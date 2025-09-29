from __future__ import annotations
import asyncio, json
from typing import Sequence
from fastapi import FastAPI, WebSocket
from fastapi.responses import HTMLResponse, PlainTextResponse
from starlette.websockets import WebSocketDisconnect
from .settings import settings
from .store import TickerStore
from .domain import Symbol
from .engine.spread_calc import compute_rows, DEFAULT_TAKER_FEES
from .connectors.binance_futures import run_binance
from .connectors.bybit_perp import run_bybit
from .connectors.discovery import discover_common_usdt_perp

app = FastAPI(title="Arbitrage Scanner API", version="1.1.0")

store = TickerStore()
_tasks: list[asyncio.Task] = []
SYMBOLS: list[Symbol] = []  # наполним на старте
EXCHANGES = ("binance", "bybit")

@app.on_event("startup")
async def startup():
    # 1) Авто-дискавер пересечения пар
    global SYMBOLS
    try:
        SYMBOLS = await discover_common_usdt_perp()
    except Exception:
        # Фоллбек: минимальный набор
        SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
    # 2) Запуск коннекторов
    _tasks.append(asyncio.create_task(run_binance(store, SYMBOLS)))
    _tasks.append(asyncio.create_task(run_bybit(store, SYMBOLS)))

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
    # Заглушка карточки пары (откроется в новой вкладке). Реализуем на следующем этапе.
    return PlainTextResponse(f"Страница пары {symbol} (в разработке)")

@app.websocket("/ws/spreads")
async def ws_spreads(ws: WebSocket):
    await ws.accept()
    try:
        while True:
            rows = compute_rows(store, symbols=SYMBOLS, exchanges=EXCHANGES, taker_fees=DEFAULT_TAKER_FEES)
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
