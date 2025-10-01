from __future__ import annotations
import asyncio, json, time
from typing import Sequence, List
import websockets
from ..domain import Ticker, Symbol
from ..store import TickerStore
from .discovery import discover_bybit_linear_usdt

WS = "wss://stream.bybit.com/v5/public/linear"
CHUNK = 100  # безопасный размер по числу подписок

MIN_SYMBOL_THRESHOLD = 5

async def run_bybit(store: TickerStore, symbols: Sequence[Symbol]):
    subscribe = list(dict.fromkeys(symbols))
    if len(subscribe) < MIN_SYMBOL_THRESHOLD:
        try:
            discovered = await discover_bybit_linear_usdt()
        except Exception:
            discovered = set()
        if discovered:
            subscribe = sorted(discovered)

    if not subscribe:
        return

    # По Bybit v5: подписка вида args=["tickers.BTCUSDT","tickers.ETHUSDT", ...]
    async for ws in _reconnect(WS):
        try:
            # разбиваем на чанки и шлём несколько subscribe
            for i in range(0, len(subscribe), CHUNK):
                batch = subscribe[i:i+CHUNK]
                args = [f"tickers.{s}" for s in batch]
                await ws.send(json.dumps({"op": "subscribe", "args": args}))
                await asyncio.sleep(0.05)  # маленькая пауза между сабами

            async for msg in ws:
                data = json.loads(msg)

                topic = data.get("topic", "")
                if not topic.startswith("tickers."):
                    continue

                payload = data.get("data")
                if payload is None:
                    continue

                items = payload if isinstance(payload, list) else [payload]
                for item in items:
                    sym = item.get("symbol")
                    if not sym:
                        continue

                    # best bid/ask: bid1Price/ask1Price (строки)
                    bid_s = item.get("bid1Price") or item.get("bidPrice")
                    ask_s = item.get("ask1Price") or item.get("askPrice")
                    try:
                        bid = float(bid_s) if bid_s is not None else 0.0
                        ask = float(ask_s) if ask_s is not None else 0.0
                    except Exception:
                        bid = ask = 0.0
                    if bid > 0 and ask > 0:
                        store.upsert_ticker(Ticker(exchange="bybit", symbol=sym, bid=bid, ask=ask, ts=time.time()))

                    fr = item.get("fundingRate")
                    if fr is not None:
                        try:
                            rate = float(fr)
                        except Exception:
                            rate = 0.0
                        store.upsert_funding("bybit", sym, rate=rate, interval="8h", ts=time.time())
        except Exception:
            await asyncio.sleep(1)

async def _reconnect(url: str):
    while True:
        try:
            async with websockets.connect(url, ping_interval=20, ping_timeout=20, close_timeout=5) as ws:
                yield ws
        except Exception:
            await asyncio.sleep(2)
            continue
