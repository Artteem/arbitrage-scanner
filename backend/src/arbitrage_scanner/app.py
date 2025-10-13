from __future__ import annotations
import asyncio, json, logging, math, time
from collections import defaultdict
from typing import Iterable, Sequence

from fastapi import FastAPI, HTTPException, Query, WebSocket
from fastapi.responses import HTMLResponse
from starlette.websockets import WebSocketDisconnect

from .settings import settings
from .store import TickerStore
from .domain import Symbol, ExchangeName
from .engine.spread_calc import DEFAULT_TAKER_FEES, Row, compute_rows
from .engine.spread_history import SpreadHistory
from .connectors.base import ConnectorSpec
from .connectors.loader import load_connectors
from .connectors.discovery import discover_symbols_for_connectors
from .exchanges.limits import fetch_limits as fetch_exchange_limits
from .exchanges.history import fetch_spread_history, fetch_bid_ask_history
from .persistence import DataRecorder, Database, SpreadSnapshotRecord, TickRecord

app = FastAPI(title="Arbitrage Scanner API", version="1.2.0")

DATABASE = Database(settings.database_path)
RECORDER = DataRecorder(DATABASE)

store = TickerStore()
_tasks: list[asyncio.Task] = []
SYMBOLS: list[Symbol] = []   # наполним на старте
CONNECTOR_SYMBOLS: dict[ExchangeName, list[Symbol]] = {}

CONNECTORS: tuple[ConnectorSpec, ...] = tuple(load_connectors(settings.enabled_exchanges))
EXCHANGES: tuple[ExchangeName, ...] = tuple(c.name for c in CONNECTORS)

TAKER_FEES = {**DEFAULT_TAKER_FEES}
for connector in CONNECTORS:
    if connector.taker_fee is not None:
        TAKER_FEES[connector.name] = connector.taker_fee

FALLBACK_SYMBOLS: list[Symbol] = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]

SPREAD_HISTORY = SpreadHistory(timeframes=(60, 300, 3600), max_candles=15000)
SPREAD_REFRESH_INTERVAL = 0.1
SPREAD_EVENT: asyncio.Event = asyncio.Event()
LAST_ROWS: list[Row] = []
LAST_ROWS_TS: float = 0.0

HISTORY_LOOKBACK_DAYS = 10.0

TIMEFRAME_ALIASES: dict[str, int] = {
    "1m": 60,
    "5m": 300,
    "1h": 3600,
}

logger = logging.getLogger(__name__)


def _parse_timeframe(value: str | int) -> int:
    if isinstance(value, int):
        candidate = value
    else:
        key = str(value).strip().lower()
        candidate = TIMEFRAME_ALIASES.get(key)
        if candidate is None:
            try:
                candidate = int(key)
            except ValueError as exc:
                raise HTTPException(status_code=400, detail="Некорректный таймфрейм") from exc
    if candidate not in SPREAD_HISTORY.timeframes:
        raise HTTPException(status_code=400, detail="Таймфрейм недоступен")
    return candidate


def _current_rows() -> Sequence[Row]:
    if LAST_ROWS:
        return LAST_ROWS
    return compute_rows(
        store,
        symbols=SYMBOLS if SYMBOLS else FALLBACK_SYMBOLS,
        exchanges=EXCHANGES,
        taker_fees=TAKER_FEES,
    )


def _rows_for_symbol(symbol: Symbol) -> list[Row]:
    target = symbol.upper()
    rows = [row for row in _current_rows() if row.symbol.upper() == target]
    if rows:
        return rows
    return compute_rows(
        store,
        symbols=[target],
        exchanges=EXCHANGES,
        taker_fees=TAKER_FEES,
    )




async def _initialize_historical_data() -> None:
    if not DATABASE.is_connected:
        return
    effective_symbols = SYMBOLS if SYMBOLS else FALLBACK_SYMBOLS
    exchange_symbols: dict[ExchangeName, list[Symbol]] = {}
    for connector in CONNECTORS:
        exchange_symbols[connector.name] = list(
            dict.fromkeys(CONNECTOR_SYMBOLS.get(connector.name) or effective_symbols)
        )
    try:
        await _backfill_ticks(exchange_symbols, HISTORY_LOOKBACK_DAYS)
    except Exception:
        logger.exception("Failed to backfill tick history", exc_info=True)
    try:
        await _compose_spreads_from_ticks(effective_symbols, tuple(exchange_symbols.keys()), HISTORY_LOOKBACK_DAYS)
    except Exception:
        logger.exception("Failed to build spread history from ticks", exc_info=True)


async def _backfill_ticks(
    exchange_symbols: dict[ExchangeName, list[Symbol]], lookback_days: float
) -> None:
    for exchange, symbols in exchange_symbols.items():
        if not symbols:
            continue
        await _fetch_ticks_for_exchange(exchange, symbols, lookback_days)


async def _fetch_ticks_for_exchange(
    exchange: ExchangeName, symbols: Sequence[Symbol], lookback_days: float
) -> None:
    for symbol in dict.fromkeys(symbols):
        try:
            history = await fetch_bid_ask_history(
                exchange=exchange,
                symbol=symbol,
                timeframe_seconds=60,
                lookback_days=lookback_days,
            )
        except Exception:
            logger.exception(
                "Failed to fetch historical ticks",
                exc_info=True,
                extra={"exchange": exchange, "symbol": symbol},
            )
            continue
        if not history:
            continue
        batch: list[TickRecord] = []
        for entry in history:
            ts_ms = int(entry.ts * 1000)
            batch.append(
                TickRecord(
                    exchange=exchange,
                    symbol=symbol,
                    ts_ms=ts_ms,
                    bid=entry.bid,
                    ask=entry.ask,
                )
            )
            if len(batch) >= 500:
                await DATABASE.insert_ticks(batch)
                batch.clear()
        if batch:
            await DATABASE.insert_ticks(batch)


async def _compose_spreads_from_ticks(
    symbols: Sequence[Symbol], exchanges: Sequence[ExchangeName], lookback_days: float
) -> None:
    if not symbols or not exchanges:
        return
    end_ts = int(time.time())
    start_ts = end_ts - int(max(lookback_days, 0) * 86400)
    start_ms = start_ts * 1000
    end_ms = end_ts * 1000
    for symbol in dict.fromkeys(symbols):
        ticks = await DATABASE.fetch_ticks(
            symbol=symbol,
            start_ts_ms=start_ms,
            end_ts_ms=end_ms,
        )
        if not ticks:
            continue
        per_ts: dict[int, dict[str, TickRecord]] = defaultdict(dict)
        for record in ticks:
            ts_sec = int(record.ts_ms // 1000)
            per_ts[ts_sec][str(record.exchange).lower()] = record
        snapshots: list[SpreadSnapshotRecord] = []
        for ts, by_exchange in sorted(per_ts.items()):
            for long_exchange in exchanges:
                long_rec = by_exchange.get(str(long_exchange).lower())
                if not long_rec:
                    continue
                for short_exchange in exchanges:
                    if long_exchange == short_exchange:
                        continue
                    short_rec = by_exchange.get(str(short_exchange).lower())
                    if not short_rec:
                        continue
                    entry = _compute_entry(short_rec.bid, long_rec.ask)
                    exit_value = _compute_exit(long_rec.bid, short_rec.ask)
                    if entry is None or exit_value is None:
                        continue
                    snapshots.append(
                        SpreadSnapshotRecord(
                            symbol=symbol,
                            long_exchange=long_exchange,
                            short_exchange=short_exchange,
                            ts=ts,
                            entry_pct=entry,
                            exit_pct=exit_value,
                            long_bid=long_rec.bid,
                            long_ask=long_rec.ask,
                            short_bid=short_rec.bid,
                            short_ask=short_rec.ask,
                            long_bids=[],
                            long_asks=[],
                            short_bids=[],
                            short_asks=[],
                        )
                    )
                    if len(snapshots) >= 500:
                        await DATABASE.insert_spread_snapshots(snapshots)
                        snapshots.clear()
        if snapshots:
            await DATABASE.insert_spread_snapshots(snapshots)


def _compute_entry(short_bid: float, long_ask: float) -> float | None:
    try:
        short_bid = float(short_bid)
        long_ask = float(long_ask)
    except (TypeError, ValueError):
        return None
    if short_bid <= 0 or long_ask <= 0:
        return None
    mid = (short_bid + long_ask) / 2.0
    if mid <= 0:
        return None
    value = (short_bid - long_ask) / mid * 100.0
    return value if math.isfinite(value) else None


def _compute_exit(long_bid: float, short_ask: float) -> float | None:
    try:
        long_bid = float(long_bid)
        short_ask = float(short_ask)
    except (TypeError, ValueError):
        return None
    if long_bid <= 0 or short_ask <= 0:
        return None
    mid = (long_bid + short_ask) / 2.0
    if mid <= 0:
        return None
    value = (long_bid - short_ask) / mid * 100.0
    return value if math.isfinite(value) else None

async def _spread_loop() -> None:
    global LAST_ROWS, LAST_ROWS_TS
    while True:
        try:
            rows = compute_rows(
                store,
                symbols=SYMBOLS if SYMBOLS else FALLBACK_SYMBOLS,
                exchanges=EXCHANGES,
                taker_fees=TAKER_FEES,
            )
            ts = time.time()
            for row in rows:
                SPREAD_HISTORY.add_point(
                    symbol=row.symbol,
                    long_exchange=row.long_ex,
                    short_exchange=row.short_ex,
                    entry_value=row.entry_pct,
                    exit_value=row.exit_pct,
                    ts=ts,
                )
                RECORDER.record_spread_row(row, ts)
            LAST_ROWS = rows
            LAST_ROWS_TS = ts
            SPREAD_EVENT.set()
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.exception("Failed to compute spreads", exc_info=exc)
            LAST_ROWS = []
            LAST_ROWS_TS = 0.0
            SPREAD_EVENT.set()
        await asyncio.sleep(SPREAD_REFRESH_INTERVAL)


@app.on_event("startup")
async def startup():
    # 1) Автоматически найдём пересечение USDT-перпетуалов
    global SYMBOLS, CONNECTOR_SYMBOLS
    await DATABASE.connect()
    await RECORDER.start()
    store.set_observer(RECORDER)
    try:
        discovery = await discover_symbols_for_connectors(CONNECTORS)
        if discovery.symbols_union:
            SYMBOLS = discovery.symbols_union
            CONNECTOR_SYMBOLS = discovery.per_connector
        else:
            SYMBOLS = FALLBACK_SYMBOLS
            CONNECTOR_SYMBOLS = {spec.name: FALLBACK_SYMBOLS[:] for spec in CONNECTORS}
    except Exception:
        # Фоллбек: базовый набор
        SYMBOLS = FALLBACK_SYMBOLS
        CONNECTOR_SYMBOLS = {spec.name: FALLBACK_SYMBOLS[:] for spec in CONNECTORS}

    await _initialize_historical_data()

    # 2) Запустим ридеры бирж
    for connector in CONNECTORS:
        symbols_for_connector = CONNECTOR_SYMBOLS.get(connector.name) or SYMBOLS
        _tasks.append(asyncio.create_task(connector.run(store, symbols_for_connector)))
    _tasks.append(asyncio.create_task(_spread_loop()))


@app.on_event("shutdown")
async def shutdown():
    for t in _tasks:
        t.cancel()
    await asyncio.gather(*_tasks, return_exceptions=True)
    await RECORDER.stop()
    await DATABASE.close()


@app.get("/health")
async def health():
    return {"status": "ok", "env": settings.model_dump(), "symbols": SYMBOLS}


@app.get("/stats")
async def stats():
    snap = store.snapshot()
    return {
        "symbols_subscribed": SYMBOLS,
        "tickers_in_store": len(snap),
        "exchanges": EXCHANGES,
    }


@app.get("/ui")
async def ui():
    from .web.ui import html
    return HTMLResponse(html())


@app.get("/pair/{symbol}")
async def pair_card(symbol: str):
    from .web.pair import html as pair_html

    return HTMLResponse(pair_html(symbol))


@app.get("/api/pair/{symbol}/overview")
async def pair_overview(symbol: str):
    rows = [row.as_dict() for row in _rows_for_symbol(symbol)]
    return {"symbol": symbol.upper(), "rows": rows}


@app.get("/api/pair/{symbol}/spreads")
async def pair_spreads(
    symbol: str,
    long_exchange: str = Query(..., alias="long"),
    short_exchange: str = Query(..., alias="short"),
    timeframe: str = Query("1m"),
    metric: str = Query("entry"),
    lookback_days: float = Query(10.0, alias="days", ge=0.0),
):
    metric_key = metric.lower()
    if metric_key not in {"entry", "exit"}:
        raise HTTPException(status_code=400, detail="Неизвестный тип графика")
    tf_value = _parse_timeframe(timeframe)
    symbol_upper = symbol.upper()
    long_key = long_exchange.lower()
    short_key = short_exchange.lower()
    lookback_seconds = int(max(lookback_days, 0) * 86400)
    start_ts = int(time.time()) - lookback_seconds if lookback_seconds else 0
    db_rows = await DATABASE.fetch_spread_candles(
        metric=metric_key,
        symbol=symbol_upper,
        long_exchange=long_key,
        short_exchange=short_key,
        timeframe_seconds=tf_value,
        start_ts=start_ts,
    )
    combined: dict[int, dict] = {row.start_ts: row.to_dict() for row in db_rows}
    realtime = SPREAD_HISTORY.get_candles(
        metric_key,
        symbol=symbol_upper,
        long_exchange=long_key,
        short_exchange=short_key,
        timeframe=tf_value,
    )
    for candle in realtime:
        combined[candle.start_ts] = {
            "ts": candle.start_ts,
            "open": candle.open,
            "high": candle.high,
            "low": candle.low,
            "close": candle.close,
        }
    if not combined and lookback_days >= 1.0:
        try:
            entry_hist, exit_hist = await fetch_spread_history(
                symbol=symbol_upper,
                long_exchange=long_key,
                short_exchange=short_key,
                timeframe_seconds=tf_value,
                lookback_days=lookback_days,
            )
            source_hist = entry_hist if metric_key == "entry" else exit_hist
            for candle in source_hist:
                combined[int(candle.start_ts)] = {
                    "ts": int(candle.start_ts),
                    "open": float(candle.open),
                    "high": float(candle.high),
                    "low": float(candle.low),
                    "close": float(candle.close),
                }
        except Exception:
            logger.exception("Failed to fetch fallback spread history", exc_info=True)
    candles_list = [combined[key] for key in sorted(combined.keys())]
    if lookback_seconds:
        cutoff_ts = int(time.time()) - lookback_seconds
        candles_list = [c for c in candles_list if c["ts"] >= cutoff_ts]
    history_source = "database+realtime" if db_rows else "realtime"
    if not db_rows and combined:
        history_source = "external"
    return {
        "symbol": symbol_upper,
        "long": long_key,
        "short": short_key,
        "metric": metric_key,
        "timeframe": timeframe,
        "timeframe_seconds": tf_value,
        "candles": candles_list,
        "history_source": history_source,
    }


@app.get("/api/pair/{symbol}/limits")
async def pair_limits(symbol: str, long_exchange: str = Query(..., alias="long"), short_exchange: str = Query(..., alias="short")):
    long_limits = await fetch_exchange_limits(long_exchange, symbol)
    short_limits = await fetch_exchange_limits(short_exchange, symbol)
    return {
        "symbol": symbol.upper(),
        "long_exchange": long_exchange.lower(),
        "short_exchange": short_exchange.lower(),
        "long": long_limits,
        "short": short_limits,
    }


@app.get("/api/pair/{symbol}/realtime")
async def pair_realtime(
    symbol: str,
    long_exchange: str = Query(..., alias="long"),
    short_exchange: str = Query(..., alias="short"),
):
    rows = _rows_for_symbol(symbol)
    long_key = long_exchange.lower()
    short_key = short_exchange.lower()
    ts = LAST_ROWS_TS if LAST_ROWS else time.time()
    payload: dict[str, object] | None = None
    for row in rows:
        if row.long_ex.lower() == long_key and row.short_ex.lower() == short_key:
            payload = row.as_dict()
            if payload is not None:
                payload["_ts"] = ts
            break
    return {
        "symbol": symbol.upper(),
        "long_exchange": long_key,
        "short_exchange": short_key,
        "ts": ts,
        "row": payload,
    }


@app.websocket("/ws/spreads")
async def ws_spreads(ws: WebSocket):
    await ws.accept()
    target_symbol = (ws.query_params.get("symbol") or "").upper()
    target_long = (ws.query_params.get("long") or "").lower()
    target_short = (ws.query_params.get("short") or "").lower()
    use_filter = bool(target_symbol and target_long and target_short)
    last_payload: str | None = None
    try:
        while True:
            rows = _current_rows()
            ts = LAST_ROWS_TS if LAST_ROWS else time.time()
            payload = []
            for r in rows:
                if use_filter:
                    if (
                        r.symbol.upper() != target_symbol
                        or r.long_ex.lower() != target_long
                        or r.short_ex.lower() != target_short
                    ):
                        continue
                item = r.as_dict()
                item["_ts"] = ts
                payload.append(item)
            if payload or not use_filter:
                message = json.dumps(payload)
                if message != last_payload:
                    await ws.send_text(message)
                    last_payload = message
            if SPREAD_EVENT.is_set():
                SPREAD_EVENT.clear()
            try:
                await asyncio.wait_for(SPREAD_EVENT.wait(), timeout=SPREAD_REFRESH_INTERVAL)
            except asyncio.TimeoutError:
                continue
    except WebSocketDisconnect:
        return
    except Exception:
        try:
            await ws.close()
        except Exception:
            pass
