from __future__ import annotations

import asyncio
import gzip
import json
import time
from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Sequence, Tuple

import httpx
import websockets

from ..domain import Symbol, Ticker
from ..store import TickerStore

MIN_SYMBOL_THRESHOLD = 5

FUNDING_URL = "https://contract.mexc.com/api/v1/contract/funding_rate"
FUNDING_POLL_INTERVAL = 15.0

WS_ENDPOINT = "wss://contract.mexc.com/ws"
WS_SUB_DELAY = 0.05
WS_DEPTH_LEVELS = 50


def _as_float(value) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _to_mexc_symbol(symbol: Symbol) -> str:
    sym = str(symbol)
    if "_" in sym:
        return sym
    if sym.endswith("USDT"):
        return f"{sym[:-4]}_USDT"
    return sym


def _from_mexc_symbol(symbol: str | None) -> Symbol | None:
    if not symbol:
        return None
    return symbol.replace("_", "")


def _parse_interval(item) -> str:
    interval = item.get("fundingInterval") or item.get("interval")
    if isinstance(interval, (int, float)):
        return f"{interval}h"
    if isinstance(interval, str) and interval:
        return interval
    return "8h"


async def run_mexc(store: TickerStore, symbols: Sequence[Symbol]):
    subscribe = list(dict.fromkeys(symbols))
    if not subscribe:
        return

    tasks = [
        asyncio.create_task(_run_mexc_orderbooks(store, subscribe)),
        asyncio.create_task(_poll_mexc_funding(store, subscribe)),
    ]

    try:
        await asyncio.gather(*tasks)
    finally:
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


async def _poll_mexc_funding(store: TickerStore, symbols: Sequence[Symbol]):
    wanted = {_to_mexc_symbol(sym) for sym in symbols}
    if len(wanted) < MIN_SYMBOL_THRESHOLD:
        wanted = set()

    async with httpx.AsyncClient(timeout=15) as client:
        while True:
            now = time.time()
            try:
                funding_resp = await client.get(FUNDING_URL)
                funding_resp.raise_for_status()
                funding_items = funding_resp.json().get("data", [])
            except Exception:
                await asyncio.sleep(5.0)
                continue

            for item in funding_items:
                if not isinstance(item, dict):
                    continue

                sym_raw = item.get("symbol")
                if wanted and sym_raw not in wanted:
                    continue

                sym_common = _from_mexc_symbol(sym_raw)
                if not sym_common:
                    continue

                rate = _as_float(item.get("fundingRate") or item.get("rate"))
                interval = _parse_interval(item)
                store.upsert_funding("mexc", sym_common, rate=rate, interval=interval, ts=now)

            await asyncio.sleep(FUNDING_POLL_INTERVAL)


async def _run_mexc_orderbooks(store: TickerStore, symbols: Sequence[Symbol]):
    wanted = {_to_mexc_symbol(sym) for sym in symbols if sym}
    if not wanted:
        return

    ws_symbols = sorted(wanted)
    books: Dict[str, _MexcOrderBookState] = {}
    wanted_common = {sym.replace("_", "") for sym in wanted}

    async for ws in _reconnect_ws():
        try:
            for sym in ws_symbols:
                payload = {
                    "method": "sub.depth",
                    "params": [sym, WS_DEPTH_LEVELS],
                    "id": int(time.time() * 1_000),
                }
                await ws.send(json.dumps(payload))
                await asyncio.sleep(WS_SUB_DELAY)

            async for raw in ws:
                msg = _decode_ws_message(raw)
                if msg is None:
                    continue

                if _is_ping(msg):
                    await _reply_pong(ws, msg)
                    continue

                parsed = _extract_depth_message(msg)
                if not parsed:
                    continue

                snapshot, data, sym_raw = parsed
                if not isinstance(data, dict):
                    continue

                sym_common = _from_mexc_symbol(sym_raw)
                if not sym_common or sym_common not in wanted_common:
                    continue

                book = books.setdefault(sym_common, _MexcOrderBookState())

                bids_raw = data.get("bids") or data.get("bid") or data.get("buy")
                asks_raw = data.get("asks") or data.get("ask") or data.get("sell")

                if snapshot:
                    book.snapshot(bids_raw, asks_raw)
                else:
                    book.update(bids_raw, asks_raw)

                bids, asks = book.top_levels()
                last_price = _extract_last_price(data)
                now = time.time()

                store.upsert_order_book(
                    "mexc",
                    sym_common,
                    bids=bids or None,
                    asks=asks or None,
                    ts=now,
                    last_price=last_price,
                    last_price_ts=now if last_price is not None else None,
                )

                _maybe_update_ticker_from_book(store, sym_common, bids, asks, now)
        except asyncio.CancelledError:
            raise
        except Exception:
            await asyncio.sleep(2.0)


async def _reconnect_ws():
    while True:
        try:
            async with websockets.connect(
                WS_ENDPOINT, ping_interval=20, ping_timeout=20, close_timeout=5
            ) as ws:
                yield ws
        except asyncio.CancelledError:
            raise
        except Exception:
            await asyncio.sleep(2.0)


def _decode_ws_message(message: str | bytes) -> dict | None:
    raw: str | None = None
    if isinstance(message, (bytes, bytearray)):
        for decoder in (
            lambda b: gzip.decompress(b).decode("utf-8"),
            lambda b: gzip.decompress(b, 16 + gzip.MAX_WBITS).decode("utf-8"),
            lambda b: b.decode("utf-8"),
        ):
            try:
                raw = decoder(message)
                break
            except Exception:
                continue
    elif isinstance(message, str):
        raw = message

    if raw is None:
        return None

    try:
        return json.loads(raw)
    except Exception:
        return None


def _is_ping(message: dict) -> bool:
    if not isinstance(message, dict):
        return False
    if "ping" in message:
        return True
    method = message.get("method")
    if isinstance(method, str) and method.lower() == "ping":
        return True
    return False


async def _reply_pong(ws, message: dict) -> None:
    try:
        if "ping" in message:
            await ws.send(json.dumps({"pong": message["ping"]}))
        else:
            await ws.send(json.dumps({"id": message.get("id", 0), "method": "pong"}))
    except Exception:
        pass


def _extract_depth_message(message: dict) -> tuple[bool, dict, str] | None:
    if not isinstance(message, dict):
        return None

    method = message.get("method")
    if isinstance(method, str) and method.lower() == "depth.update":
        params = message.get("params")
        if not isinstance(params, list) or len(params) < 3:
            return None
        snapshot = bool(params[0])
        data = params[1] if isinstance(params[1], dict) else {}
        symbol = params[2]
        if not isinstance(symbol, str):
            return None
        return snapshot, data, symbol

    channel = message.get("channel") or message.get("topic")
    if isinstance(channel, str) and "depth" in channel.lower():
        data = message.get("data")
        if isinstance(data, dict):
            symbol = message.get("symbol") or data.get("symbol")
            if isinstance(symbol, str):
                return True, data, symbol

    return None


def _extract_last_price(data: dict) -> float | None:
    for key in ("lastPrice", "last", "close", "price", "markPrice"):
        val = data.get(key)
        if val is None:
            continue
        try:
            price = float(val)
        except (TypeError, ValueError):
            continue
        if price > 0:
            return price
    return None


def _iter_levels(source) -> Iterable[Tuple[float, float]]:
    if isinstance(source, dict):
        source = source.get("levels") or source.get("data") or source.get("list") or []
    if not isinstance(source, (list, tuple)):
        return []
    result: List[Tuple[float, float]] = []
    for entry in source:
        price, size = _parse_level(entry)
        if price is None or size is None:
            continue
        result.append((price, size))
    return result


def _parse_level(level) -> Tuple[float | None, float | None]:
    price = size = None
    if isinstance(level, dict):
        for key in ("price", "p", "px", "bp", "ap"):
            val = level.get(key)
            if val is None:
                continue
            try:
                price = float(val)
                break
            except Exception:
                continue
        for key in ("size", "qty", "q", "v", "volume"):
            val = level.get(key)
            if val is None:
                continue
            try:
                size = float(val)
                break
            except Exception:
                continue
    elif isinstance(level, (list, tuple)) and len(level) >= 2:
        try:
            price = float(level[0])
        except Exception:
            price = None
        try:
            size = float(level[1])
        except Exception:
            size = None
    if price is None or price <= 0:
        return None, None
    if size is None:
        return price, 0.0
    if size < 0:
        size = 0.0
    return price, size


@dataclass
class _MexcOrderBookState:
    bids: Dict[float, float] = field(default_factory=dict)
    asks: Dict[float, float] = field(default_factory=dict)

    def snapshot(self, bids, asks) -> None:
        self.bids.clear()
        self.asks.clear()
        self._apply(self.bids, bids)
        self._apply(self.asks, asks)

    def update(self, bids, asks) -> None:
        self._apply(self.bids, bids)
        self._apply(self.asks, asks)

    def top_levels(self, depth: int = 5) -> Tuple[List[Tuple[float, float]], List[Tuple[float, float]]]:
        bids_sorted = sorted(self.bids.items(), key=lambda kv: kv[0], reverse=True)[:depth]
        asks_sorted = sorted(self.asks.items(), key=lambda kv: kv[0])[:depth]
        return bids_sorted, asks_sorted

    def _apply(self, side: Dict[float, float], updates) -> None:
        if not updates:
            return
        for price, size in _iter_levels(updates):
            if size <= 0:
                side.pop(price, None)
            else:
                side[price] = size
        if len(side) > 200:
            if side is self.asks:
                ordered = sorted(side.items(), key=lambda kv: kv[0])
            else:
                ordered = sorted(side.items(), key=lambda kv: kv[0], reverse=True)
            trimmed = dict(ordered[:200])
            side.clear()
            side.update(trimmed)


def _maybe_update_ticker_from_book(
    store: TickerStore,
    symbol: Symbol,
    bids: List[Tuple[float, float]] | None,
    asks: List[Tuple[float, float]] | None,
    ts: float,
) -> None:
    if not bids or not asks:
        return

    best_bid = bids[0][0]
    best_ask = asks[0][0]
    if best_bid <= 0 or best_ask <= 0:
        return

    store.upsert_ticker(
        Ticker(
            exchange="mexc",
            symbol=symbol,
            bid=best_bid,
            ask=best_ask,
            ts=ts,
        )
    )
