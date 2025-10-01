from __future__ import annotations
import asyncio, json, time, math
from typing import Sequence, List
import websockets
from ..domain import Ticker, Symbol
from ..store import TickerStore
from .discovery import discover_binance_usdt_perp

BOOK_WS = "wss://fstream.binance.com/stream"
MARK_WS = "wss://fstream.binance.com/stream"

MIN_SYMBOL_THRESHOLD = 5

def _stream_name_book(sym: str) -> str:
    return f"{sym.lower()}@bookTicker"

def _stream_name_mark(sym: str) -> str:
    return f"{sym.lower()}@markPrice@1s"

CHUNK = 100  # безопасный размер пакета подписки

async def run_binance(store: TickerStore, symbols: Sequence[Symbol]):
    subscribe = list(dict.fromkeys(symbols))  # сохраняем порядок без дубликатов
    if len(subscribe) < MIN_SYMBOL_THRESHOLD:
        try:
            discovered = await discover_binance_usdt_perp()
        except Exception:
            discovered = set()
        if discovered:
            subscribe = sorted(discovered)

    if not subscribe:
        return

    async def _consume(endpoint: str, params: List[str], handler):
        # отдельное соединение на каждый батч
        async for ws in _reconnect(endpoint):
            try:
                sub = {"method": "SUBSCRIBE", "params": params, "id": int(time.time())}
                await ws.send(json.dumps(sub))
                async for msg in ws:
                    data = json.loads(msg)
                    d = data.get("data")
                    if not d:
                        continue
                    await handler(d)
            except Exception:
                await asyncio.sleep(1)

    async def _handle_book(d):
        # d: { s, b, a, ... }
        s = d.get("s"); b = d.get("b"); a = d.get("a")
        if s and b and a:
            store.upsert_ticker(Ticker(exchange="binance", symbol=s, bid=float(b), ask=float(a), ts=time.time()))

    async def _handle_mark(d):
        # d: { s, r, ... }
        s = d.get("s"); r = d.get("r")
        if s is not None and r is not None:
            try:
                rate = float(r)
            except Exception:
                rate = 0.0
            store.upsert_funding("binance", s, rate=rate, interval="8h", ts=time.time())

    # батчим
    tasks = []
    for i in range(0, len(subscribe), CHUNK):
        batch = subscribe[i:i+CHUNK]
        book_params = [_stream_name_book(s) for s in batch]
        mark_params = [_stream_name_mark(s) for s in batch]
        tasks.append(asyncio.create_task(_consume(BOOK_WS, book_params, _handle_book)))
        tasks.append(asyncio.create_task(_consume(MARK_WS, mark_params, _handle_mark)))
    await asyncio.gather(*tasks)

async def _reconnect(url: str):
    while True:
        try:
            async with websockets.connect(url, ping_interval=20, ping_timeout=20, close_timeout=5) as ws:
                yield ws
        except Exception:
            await asyncio.sleep(2)
            continue
