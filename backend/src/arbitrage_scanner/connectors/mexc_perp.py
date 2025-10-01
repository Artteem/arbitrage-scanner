from __future__ import annotations

import asyncio
import json
import time
from typing import Sequence

import websockets

from ..domain import Symbol, Ticker
from ..store import TickerStore

WS = "wss://contract.mexc.com/ws"


def _to_mexc_symbol(symbol: Symbol) -> str:
    # Биржа MEXC использует формат вида BTC_USDT, мы поддерживаем USDT-пары
    if "_" in symbol:
        return symbol
    if symbol.endswith("USDT"):
        return f"{symbol[:-4]}_USDT"
    return symbol


def _from_mexc_symbol(symbol: str | None) -> str | None:
    if not symbol:
        return None
    return symbol.replace("_", "")


async def run_mexc(store: TickerStore, symbols: Sequence[Symbol]) -> None:
    mapped = [_to_mexc_symbol(str(sym)) for sym in symbols]

    async for ws in _reconnect(WS):
        try:
            # подписываемся на тикеры и ставки финансирования
            for idx, sym in enumerate(mapped, start=1):
                await ws.send(
                    json.dumps({"method": "sub.ticker", "params": [sym], "id": idx})
                )
                await asyncio.sleep(0.05)
                await ws.send(
                    json.dumps(
                        {
                            "method": "sub.fundingRate",
                            "params": [sym],
                            "id": idx + 10_000,
                        }
                    )
                )
                await asyncio.sleep(0.05)

            async for raw in ws:
                data = json.loads(raw)

                if "ping" in data:
                    await ws.send(json.dumps({"pong": data["ping"]}))
                    continue

                channel = data.get("channel") or data.get("method")
                if channel == "push.ticker":
                    payload = data.get("data") or {}
                    sym = _from_mexc_symbol(payload.get("symbol"))
                    if not sym:
                        continue

                    bid_val = payload.get("bid1") or payload.get("bid")
                    ask_val = payload.get("ask1") or payload.get("ask")
                    try:
                        bid = float(bid_val)
                        ask = float(ask_val)
                    except (TypeError, ValueError):
                        continue
                    if bid <= 0 or ask <= 0:
                        continue
                    store.upsert_ticker(
                        Ticker(exchange="mexc", symbol=sym, bid=bid, ask=ask, ts=time.time())
                    )

                elif channel == "push.fundingRate":
                    payload = data.get("data") or {}
                    sym = _from_mexc_symbol(payload.get("symbol"))
                    if not sym:
                        continue
                    rate_val = payload.get("fundingRate") or payload.get("rate")
                    try:
                        rate = float(rate_val)
                    except (TypeError, ValueError):
                        continue
                    interval = str(payload.get("fundingInterval") or payload.get("interval") or "8h")
                    store.upsert_funding("mexc", sym, rate=rate, interval=interval, ts=time.time())

        except Exception:
            await asyncio.sleep(1)


async def _reconnect(url: str):
    while True:
        try:
            async with websockets.connect(
                url, ping_interval=20, ping_timeout=20, close_timeout=5
            ) as ws:
                yield ws
        except Exception:
            await asyncio.sleep(2)
            continue
