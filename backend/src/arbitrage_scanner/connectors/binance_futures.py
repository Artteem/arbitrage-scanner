from __future__ import annotations
import asyncio, json, time, math
from typing import Sequence, List
import websockets
from ..domain import Ticker, Symbol
from ..store import TickerStore
from .discovery import discover_binance_usdt_perp

BOOK_WS = "wss://fstream.binance.com/stream"
MARK_WS = "wss://fstream.binance.com/stream"
DEPTH_WS = "wss://fstream.binance.com/stream"
TRADE_WS = "wss://fstream.binance.com/stream"

MIN_SYMBOL_THRESHOLD = 5

def _stream_name_book(sym: str) -> str:
    return f"{sym.lower()}@bookTicker"

def _stream_name_mark(sym: str) -> str:
    return f"{sym.lower()}@markPrice@1s"

def _stream_name_depth(sym: str) -> str:
    return f"{sym.lower()}@depth5@100ms"

def _stream_name_trade(sym: str) -> str:
    return f"{sym.lower()}@aggTrade"

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

    async def _handle_depth(d):
        s = d.get("s") or d.get("symbol")
        bids = d.get("bids") or d.get("b") or []
        asks = d.get("asks") or d.get("a") or []
        if not s:
            return
        now = time.time()
        def _convert(levels):
            result = []
            for level in levels[:5]:
                if isinstance(level, (list, tuple)) and len(level) >= 2:
                    try:
                        price = float(level[0])
                        size = float(level[1])
                    except (TypeError, ValueError):
                        continue
                    if price > 0 and size > 0:
                        result.append((price, size))
            return result
        bids_conv = _convert(bids)
        asks_conv = _convert(asks)
        if bids_conv or asks_conv:
            store.upsert_order_book(
                "binance",
                s,
                bids=bids_conv or None,
                asks=asks_conv or None,
                ts=now,
            )
        if bids_conv and asks_conv:
            store.upsert_ticker(
                Ticker(exchange="binance", symbol=s, bid=bids_conv[0][0], ask=asks_conv[0][0], ts=now)
            )

    async def _handle_mark(d):
        # d: { s, r, ... }
        s = d.get("s"); r = d.get("r")
        if s is not None and r is not None:
            try:
                rate = float(r)
            except Exception:
                rate = 0.0
            store.upsert_funding("binance", s, rate=rate, interval="8h", ts=time.time())

    async def _handle_trade(d):
        s = d.get("s") or d.get("symbol")
        p = d.get("p") or d.get("price")
        if not s or p is None:
            return
        try:
            price = float(p)
        except (TypeError, ValueError):
            return
        store.upsert_order_book("binance", s, last_price=price, last_price_ts=time.time())

    # батчим
    tasks = []
    for i in range(0, len(subscribe), CHUNK):
        batch = subscribe[i:i+CHUNK]
        book_params = [_stream_name_book(s) for s in batch]
        mark_params = [_stream_name_mark(s) for s in batch]
        depth_params = [_stream_name_depth(s) for s in batch]
        trade_params = [_stream_name_trade(s) for s in batch]
        tasks.append(asyncio.create_task(_consume(BOOK_WS, book_params, _handle_book)))
        tasks.append(asyncio.create_task(_consume(MARK_WS, mark_params, _handle_mark)))
        tasks.append(asyncio.create_task(_consume(DEPTH_WS, depth_params, _handle_depth)))
        tasks.append(asyncio.create_task(_consume(TRADE_WS, trade_params, _handle_trade)))
    await asyncio.gather(*tasks)

async def _reconnect(url: str):
    while True:
        try:
            async with websockets.connect(url, ping_interval=20, ping_timeout=20, close_timeout=5) as ws:
                yield ws
        except Exception:
            await asyncio.sleep(2)
            continue
