from __future__ import annotations
import asyncio, json, logging, time
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
from .exchanges.history import fetch_spread_history

app = FastAPI(title="Arbitrage Scanner API", version="1.2.0")

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


@app.get("/health")
async def health():
    return {"status": "ok", "env": settings.model_dump(), "symbols": SYMBOLS}


@app.get("/stats")
async def stats():
    snap = store.snapshot()
    metrics = store.stats()
    return {
        "symbols_subscribed": SYMBOLS,
        "tickers_in_store": len(snap),
        "exchanges": EXCHANGES,
        "ticker_updates": metrics.get("ticker_updates", 0),
        "order_book_updates": metrics.get("order_book_updates", 0),
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
