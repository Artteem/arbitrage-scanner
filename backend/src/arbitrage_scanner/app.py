from __future__ import annotations
import asyncio, json, logging
from typing import Sequence, Iterable
from fastapi import FastAPI, WebSocket
from fastapi.responses import HTMLResponse, PlainTextResponse
from starlette.websockets import WebSocketDisconnect

from .settings import settings
from .store import TickerStore
from .domain import Symbol, ExchangeName
from .engine.spread_calc import compute_rows, DEFAULT_TAKER_FEES
from .connectors.base import ConnectorSpec
from .connectors.loader import load_connectors
from .connectors.discovery import discover_common_symbols, discover_symbols_for_connectors

logger = logging.getLogger(__name__)

app = FastAPI(title="Arbitrage Scanner API", version="1.1.0")

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


@app.on_event("startup")
async def startup():
    if "bingx" not in {ex.lower() for ex in settings.enabled_exchanges}:
        logger.warning(
            "BingX is not configured in ENABLED_EXCHANGES. "
            "Add 'bingx' to avoid missing production connectors."
        )

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
    # Заглушка карточки пары
    return PlainTextResponse(f"Страница пары {symbol} (в разработке)")


@app.websocket("/ws/spreads")
async def ws_spreads(ws: WebSocket):
    await ws.accept()
    try:
        while True:
            rows = compute_rows(
                store,
                symbols=SYMBOLS,
                exchanges=EXCHANGES,
                taker_fees=TAKER_FEES,
            )
            payload = [r.as_dict() for r in rows]
            await ws.send_text(json.dumps(payload))
            await asyncio.sleep(1.0)
    except WebSocketDisconnect:
        return
    except Exception:
        try:
            await ws.close()
        except Exception:
            pass
