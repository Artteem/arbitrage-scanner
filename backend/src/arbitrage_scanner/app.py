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
LAST_ROWS: list[Row] = []

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
    global LAST_ROWS
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
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.exception("Failed to compute spreads", exc_info=exc)
            LAST_ROWS = []
        await asyncio.sleep(1.0)


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
    candles = SPREAD_HISTORY.get_candles(
        metric_key,
        symbol=symbol.upper(),
        long_exchange=long_exchange.lower(),
        short_exchange=short_exchange.lower(),
        timeframe=tf_value,
    )
    if not candles:
        now = time.time()
        for row in _rows_for_symbol(symbol):
            SPREAD_HISTORY.add_point(
                symbol=row.symbol,
                long_exchange=row.long_ex,
                short_exchange=row.short_ex,
                entry_value=row.entry_pct,
                exit_value=row.exit_pct,
                ts=now,
            )
        candles = SPREAD_HISTORY.get_candles(
            metric_key,
            symbol=symbol.upper(),
            long_exchange=long_exchange.lower(),
            short_exchange=short_exchange.lower(),
            timeframe=tf_value,
        )
    if lookback_days:
        cutoff_ts = int(time.time() - lookback_days * 86400)
        candles = [c for c in candles if c.start_ts >= cutoff_ts]
    return {
        "symbol": symbol.upper(),
        "long": long_exchange.lower(),
        "short": short_exchange.lower(),
        "metric": metric_key,
        "timeframe": timeframe,
        "timeframe_seconds": tf_value,
        "candles": [c.to_dict() for c in candles],
    }


@app.websocket("/ws/spreads")
async def ws_spreads(ws: WebSocket):
    await ws.accept()
    try:
        while True:
            payload = [r.as_dict() for r in _current_rows()]
            await ws.send_text(json.dumps(payload))
            await asyncio.sleep(1.0)
    except WebSocketDisconnect:
        return
    except Exception:
        try:
            await ws.close()
        except Exception:
            pass
