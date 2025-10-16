from __future__ import annotations
import asyncio, json, logging, time
from typing import Iterable, Sequence

from fastapi import FastAPI, HTTPException, Query, WebSocket
from fastapi.responses import HTMLResponse
from starlette.websockets import WebSocketDisconnect

from .settings import settings
from .store import TickerStore
from .domain import ExchangeName, Symbol
from .engine.spread_calc import DEFAULT_TAKER_FEES, Row, compute_rows
from .engine.spread_history import SpreadHistory
from .connectors.base import ConnectorSpec
from .connectors.loader import load_connectors
from .exchanges.limits import fetch_limits as fetch_exchange_limits
from .exchanges.history import fetch_spread_history
from .persistence import DataPersistence, Database, normalize_symbol
from .services.bootstrap import Bootstrapper
from .services.funding import poll_funding_loop

app = FastAPI(title="Arbitrage Scanner API", version="1.3.0")

database = Database(settings.database_path)
persistence = DataPersistence(database)
store = TickerStore(persistence=persistence)
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

DEFAULT_NOTIONAL: float | None = (
    float(settings.default_trade_volume)
    if settings.default_trade_volume and settings.default_trade_volume > 0
    else None
)

TIMEFRAME_ALIASES: dict[str, int] = {
    "1m": 60,
    "5m": 300,
    "1h": 3600,
}

logger = logging.getLogger(__name__)


def _resolve_notional(volume: float | None) -> float | None:
    if volume is None:
        return DEFAULT_NOTIONAL
    try:
        vol = float(volume)
    except (TypeError, ValueError):
        return DEFAULT_NOTIONAL
    if vol <= 0:
        return None
    return vol


def _entry_pct(bid_short: float, ask_long: float) -> float:
    mid = (bid_short + ask_long) / 2.0
    if mid == 0:
        return 0.0
    return (bid_short - ask_long) / mid * 100.0


def _exit_pct(bid_long: float, ask_short: float) -> float:
    mid = (bid_long + ask_short) / 2.0
    if mid == 0:
        return 0.0
    return (bid_long - ask_short) / mid * 100.0


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


def _current_rows(volume: float | None = None) -> Sequence[Row]:
    notional = _resolve_notional(volume)
    if notional == DEFAULT_NOTIONAL and LAST_ROWS:
        return LAST_ROWS
    return compute_rows(
        store,
        symbols=SYMBOLS if SYMBOLS else FALLBACK_SYMBOLS,
        exchanges=EXCHANGES,
        taker_fees=TAKER_FEES,
        target_notional=notional,
    )


def _rows_for_symbol(symbol: Symbol, volume: float | None = None) -> list[Row]:
    target = symbol.upper()
    rows = [row for row in _current_rows(volume) if row.symbol.upper() == target]
    if rows:
        return rows
    notional = _resolve_notional(volume)
    return compute_rows(
        store,
        symbols=[target],
        exchanges=EXCHANGES,
        taker_fees=TAKER_FEES,
        target_notional=notional,
    )


async def _backfill_from_database(
    symbol: Symbol,
    long_exchange: ExchangeName,
    short_exchange: ExchangeName,
    timeframe: int,
    start_ts: int,
) -> None:
    normalized = normalize_symbol(symbol)
    if not normalized:
        return
    end_ts = int(time.time())
    try:
        long_points = await database.load_bbo_points(long_exchange, normalized, start_ts, end_ts)
        short_points = await database.load_bbo_points(short_exchange, normalized, start_ts, end_ts)
    except Exception:
        logger.exception("Failed to load cached BBO history for %s %s/%s", symbol, long_exchange, short_exchange)
        return
    if not long_points or not short_points:
        return

    def _bucket(points: Sequence) -> dict[int, object]:
        buckets: dict[int, object] = {}
        for point in points:
            ts = int(getattr(point, "ts", 0))
            bucket = (ts // timeframe) * timeframe
            buckets[bucket] = point
        return buckets

    buckets_long = _bucket(long_points)
    buckets_short = _bucket(short_points)
    common = sorted(set(buckets_long.keys()) & set(buckets_short.keys()))
    if not common:
        return
    for bucket in common:
        lp = buckets_long[bucket]
        sp = buckets_short[bucket]
        bid_short = float(getattr(sp, "bid", 0.0))
        ask_long = float(getattr(lp, "ask", 0.0))
        bid_long = float(getattr(lp, "bid", ask_long))
        ask_short = float(getattr(sp, "ask", bid_short))
        entry_value = _entry_pct(bid_short, ask_long)
        exit_value = _exit_pct(bid_long, ask_short)
        SPREAD_HISTORY.add_point(
            symbol=symbol,
            long_exchange=long_exchange,
            short_exchange=short_exchange,
            entry_value=entry_value,
            exit_value=exit_value,
            ts=float(bucket),
        )


async def _spread_loop() -> None:
    global LAST_ROWS, LAST_ROWS_TS
    while True:
        try:
            rows = compute_rows(
                store,
                symbols=SYMBOLS if SYMBOLS else FALLBACK_SYMBOLS,
                exchanges=EXCHANGES,
                taker_fees=TAKER_FEES,
                target_notional=DEFAULT_NOTIONAL,
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
    global SYMBOLS, CONNECTOR_SYMBOLS

    await database.connect()
    await database.initialize()
    await persistence.start()
    store.attach_persistence(persistence)

    try:
        bootstrapper = Bootstrapper(
            connectors=CONNECTORS,
            store=store,
            database=database,
        )
        result = await bootstrapper.run()
        if result.symbols_union:
            SYMBOLS = result.symbols_union
        else:
            SYMBOLS = FALLBACK_SYMBOLS
        if result.per_connector:
            CONNECTOR_SYMBOLS = {ex: list(symbols) for ex, symbols in result.per_connector.items()}
        else:
            CONNECTOR_SYMBOLS = {spec.name: SYMBOLS[:] for spec in CONNECTORS}
        for exchange, fee in result.taker_fees.items():
            TAKER_FEES[exchange] = fee
    except Exception:
        logger.exception("Bootstrap failed, using fallback symbol set")
        SYMBOLS = FALLBACK_SYMBOLS
        CONNECTOR_SYMBOLS = {spec.name: FALLBACK_SYMBOLS[:] for spec in CONNECTORS}

    # Запустим ридеры бирж
    for connector in CONNECTORS:
        symbols_for_connector = CONNECTOR_SYMBOLS.get(connector.name) or SYMBOLS
        _tasks.append(asyncio.create_task(connector.run(store, symbols_for_connector)))
    _tasks.append(asyncio.create_task(_spread_loop()))

    if CONNECTOR_SYMBOLS:
        poller_payload = {ex: tuple(symbols) for ex, symbols in CONNECTOR_SYMBOLS.items()}
        _tasks.append(
            asyncio.create_task(
                poll_funding_loop(
                    exchanges=poller_payload,
                    interval=settings.funding_refresh_interval,
                    store=store,
                    persistence=persistence,
                )
            )
        )


@app.on_event("shutdown")
async def shutdown():
    for t in _tasks:
        t.cancel()
    await asyncio.gather(*_tasks, return_exceptions=True)
    await persistence.stop()
    await database.close()


@app.get("/health")
async def health():
    return {"status": "ok", "env": settings.model_dump(), "symbols": SYMBOLS}


def _stats_payload() -> dict:
    snap = store.snapshot()
    metrics = store.stats()
    return {
        "symbols_subscribed": SYMBOLS,
        "tickers_in_store": len(snap),
        "exchanges": EXCHANGES,
        "ticker_updates": metrics.get("ticker_updates", 0),
        "order_book_updates": metrics.get("order_book_updates", 0),
        "default_notional": DEFAULT_NOTIONAL,
        "database_path": settings.database_path,
    }


@app.get("/stats")
async def stats():
    return _stats_payload()


@app.get("/api/stats")
async def api_stats():
    return _stats_payload()


@app.get("/ui")
async def ui():
    from .web.ui import html
    return HTMLResponse(html())


@app.get("/pair/{symbol}")
async def pair_card(symbol: str):
    from .web.pair import html as pair_html

    return HTMLResponse(pair_html(symbol))


@app.get("/api/pair/{symbol}/overview")
async def pair_overview(symbol: str, volume: float | None = Query(None, ge=0.0)):
    rows = [row.as_dict() for row in _rows_for_symbol(symbol, volume)]
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
    candles = SPREAD_HISTORY.get_candles(
        metric_key,
        symbol=symbol_upper,
        long_exchange=long_key,
        short_exchange=short_key,
        timeframe=tf_value,
    )
    need_backfill = not candles
    if lookback_days:
        cutoff_ts = int(time.time() - lookback_days * 86400)
        if candles and candles[0].start_ts > cutoff_ts:
            need_backfill = True
    if need_backfill:
        now = time.time()
        rows = _rows_for_symbol(symbol)
        if rows:
            for row in rows:
                SPREAD_HISTORY.add_point(
                    symbol=row.symbol,
                    long_exchange=row.long_ex,
                    short_exchange=row.short_ex,
                    entry_value=row.entry_pct,
                    exit_value=row.exit_pct,
                    ts=now,
                )
        start_ts = int(time.time() - max(lookback_days, 0) * 86400)
        try:
            await _backfill_from_database(
                symbol_upper,
                long_key,
                short_key,
                tf_value,
                start_ts,
            )
        except Exception:
            logger.exception("Failed to load cached spread data", exc_info=True)
        try:
            entry_hist, exit_hist = await fetch_spread_history(
                symbol=symbol_upper,
                long_exchange=long_key,
                short_exchange=short_key,
                timeframe_seconds=tf_value,
                lookback_days=max(lookback_days, 1.0),
            )
            if entry_hist:
                SPREAD_HISTORY.merge_external(
                    "entry",
                    symbol=symbol_upper,
                    long_exchange=long_key,
                    short_exchange=short_key,
                    timeframe=tf_value,
                    candles=entry_hist,
                )
            if exit_hist:
                SPREAD_HISTORY.merge_external(
                    "exit",
                    symbol=symbol_upper,
                    long_exchange=long_key,
                    short_exchange=short_key,
                    timeframe=tf_value,
                    candles=exit_hist,
                )
        except Exception:
            logger.exception("Failed to backfill spread history", exc_info=True)
        candles = SPREAD_HISTORY.get_candles(
            metric_key,
            symbol=symbol_upper,
            long_exchange=long_key,
            short_exchange=short_key,
            timeframe=tf_value,
        )
    if lookback_days:
        cutoff_ts = int(time.time() - lookback_days * 86400)
        candles = [c for c in candles if c.start_ts >= cutoff_ts]
    return {
        "symbol": symbol_upper,
        "long": long_key,
        "short": short_key,
        "metric": metric_key,
        "timeframe": timeframe,
        "timeframe_seconds": tf_value,
        "candles": [c.to_dict() for c in candles],
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
    volume: float | None = Query(None, ge=0.0),
):
    rows = _rows_for_symbol(symbol, volume)
    long_key = long_exchange.lower()
    short_key = short_exchange.lower()
    notional = _resolve_notional(volume)
    ts = LAST_ROWS_TS if (notional == DEFAULT_NOTIONAL and LAST_ROWS) else time.time()
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
    volume_raw = ws.query_params.get("volume")
    volume_value: float | None = None
    if volume_raw is not None:
        try:
            volume_value = float(volume_raw)
        except (TypeError, ValueError):
            volume_value = None
    use_filter = bool(target_symbol and target_long and target_short)
    last_payload: str | None = None
    try:
        while True:
            rows = _current_rows(volume_value)
            notional = _resolve_notional(volume_value)
            ts = LAST_ROWS_TS if (notional == DEFAULT_NOTIONAL and LAST_ROWS) else time.time()
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
