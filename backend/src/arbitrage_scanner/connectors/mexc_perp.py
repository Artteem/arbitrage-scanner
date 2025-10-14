from __future__ import annotations

import asyncio
import json
import time
from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Sequence, Tuple

import websockets

from ..domain import Symbol, Ticker
from ..store import TickerStore

WS_ENDPOINT = "wss://contract.mexc.com/ws?compress=false"
WS_SUB_BATCH = 250
WS_RECONNECT_INITIAL = 1.0
WS_RECONNECT_MAX = 60.0
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


def _extract_bid(item) -> float:
    for key in ("bid1", "bestBidPrice", "bestBid"):
        val = item.get(key)
        bid = _as_float(val)
        if bid > 0:
            return bid
    return 0.0


def _extract_ask(item) -> float:
    for key in ("ask1", "bestAskPrice", "bestAsk"):
        val = item.get(key)
        ask = _as_float(val)
        if ask > 0:
            return ask
    return 0.0


def _parse_interval(item) -> str:
    interval = item.get("fundingInterval") or item.get("interval")
    if isinstance(interval, (int, float)):
        return f"{interval}h"
    if isinstance(interval, str) and interval:
        return interval
    return "8h"


def _chunk(symbols: Sequence[str], size: int) -> Iterable[Sequence[str]]:
    for idx in range(0, len(symbols), size):
        yield symbols[idx : idx + size]


async def run_mexc(store: TickerStore, symbols: Sequence[Symbol]):
    subscribe = list(dict.fromkeys(symbols))
    if not subscribe:
        return

    tasks = [
        asyncio.create_task(_run_mexc_orderbooks(store, subscribe)),
        asyncio.create_task(_run_mexc_tickers(store, subscribe)),
        asyncio.create_task(_run_mexc_funding(store, subscribe)),
    ]

    try:
        await asyncio.gather(*tasks)
    finally:
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


async def _run_mexc_orderbooks(store: TickerStore, symbols: Sequence[Symbol]):
    wanted = {_to_mexc_symbol(sym) for sym in symbols if sym}
    if not wanted:
        return

    ws_symbols = sorted(wanted)
    books: Dict[str, _MexcOrderBookState] = {}
    wanted_common = {sym.replace("_", "") for sym in wanted}

    async for ws in _reconnect_ws():
        try:
            for batch in _chunk(ws_symbols, WS_SUB_BATCH):
                for sym in batch:
                    payload = {
                        "method": "sub.depth",
                        "params": [sym, WS_DEPTH_LEVELS],
                        "id": int(time.time() * 1_000),
                    }
                    await ws.send(json.dumps(payload))

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

                bids_raw = data.get("bids") or data.get("b") or data.get("buy")
                asks_raw = data.get("asks") or data.get("a") or data.get("sell")

                book = books.setdefault(sym_common, _MexcOrderBookState())
                if snapshot:
                    book.snapshot(bids_raw, asks_raw)
                else:
                    book.update(bids_raw, asks_raw)

                bids, asks = book.top_levels()
                last_price = _extract_last_price(data)

                store.upsert_order_book(
                    "mexc",
                    sym_common,
                    bids=bids or None,
                    asks=asks or None,
                    ts=time.time(),
                    last_price=last_price,
                )

                if bids and asks:
                    best_bid = bids[0][0]
                    best_ask = asks[0][0]
                    store.upsert_ticker(
                        Ticker(
                            exchange="mexc",
                            symbol=sym_common,
                            bid=best_bid,
                            ask=best_ask,
                            ts=time.time(),
                        )
                    )
        except asyncio.CancelledError:
            raise
        except Exception:
            await asyncio.sleep(2.0)


async def _run_mexc_tickers(store: TickerStore, symbols: Sequence[Symbol]) -> None:
    wanted = {_to_mexc_symbol(sym) for sym in symbols if sym}
    if not wanted:
        return

    ws_symbols = sorted(wanted)

    async for ws in _reconnect_ws():
        try:
            for batch in _chunk(ws_symbols, WS_SUB_BATCH):
                for sym in batch:
                    payload = {"method": "sub.ticker", "params": [sym], "id": int(time.time() * 1_000)}
                    await ws.send(json.dumps(payload))

            async for raw in ws:
                msg = _decode_ws_message(raw)
                if msg is None:
                    continue

                if _is_ping(msg):
                    await _reply_pong(ws, msg)
                    continue

                now = time.time()
                for sym_raw, payload in _iter_mexc_payloads(msg):
                    if not payload:
                        continue
                    sym_common = _from_mexc_symbol(sym_raw)
                    if not sym_common:
                        continue
                    sym_exchange = _to_mexc_symbol(sym_common)
                    if sym_raw not in wanted and sym_exchange not in wanted:
                        continue

                    bid = _extract_bid(payload)
                    ask = _extract_ask(payload)
                    if bid <= 0 or ask <= 0:
                        continue

                    store.upsert_ticker(
                        Ticker(
                            exchange="mexc",
                            symbol=sym_common,
                            bid=bid,
                            ask=ask,
                            ts=now,
                        )
                    )
        except asyncio.CancelledError:
            raise
        except Exception:
            await asyncio.sleep(2.0)


async def _run_mexc_funding(store: TickerStore, symbols: Sequence[Symbol]) -> None:
    wanted = {_to_mexc_symbol(sym) for sym in symbols if sym}
    if not wanted:
        return

    ws_symbols = sorted(wanted)

    async for ws in _reconnect_ws():
        try:
            for batch in _chunk(ws_symbols, WS_SUB_BATCH):
                for sym in batch:
                    payload = {"method": "sub.funding_rate", "params": [sym], "id": int(time.time() * 1_000)}
                    await ws.send(json.dumps(payload))

            async for raw in ws:
                msg = _decode_ws_message(raw)
                if msg is None:
                    continue

                if _is_ping(msg):
                    await _reply_pong(ws, msg)
                    continue

                now = time.time()
                for sym_raw, payload in _iter_mexc_payloads(msg):
                    if not payload:
                        continue

                    sym_common = _from_mexc_symbol(sym_raw)
                    if not sym_common:
                        continue
                    sym_exchange = _to_mexc_symbol(sym_common)
                    if sym_raw not in wanted and sym_exchange not in wanted:
                        continue

                    rate = _as_float(
                        payload.get("fundingRate")
                        or payload.get("funding_rate")
                        or payload.get("rate")
                        or payload.get("value")
                    )

                    interval = _parse_interval(payload)
                    store.upsert_funding("mexc", sym_common, rate=rate, interval=interval, ts=now)
        except asyncio.CancelledError:
            raise
        except Exception:
            await asyncio.sleep(2.0)


async def _reconnect_ws():
    delay = WS_RECONNECT_INITIAL
    while True:
        try:
            async with websockets.connect(
                WS_ENDPOINT,
                ping_interval=20,
                ping_timeout=20,
                close_timeout=5,
                compression=None,
            ) as ws:
                delay = WS_RECONNECT_INITIAL
                yield ws
        except asyncio.CancelledError:
            raise
        except Exception:
            await asyncio.sleep(delay)
            delay = min(delay * 2, WS_RECONNECT_MAX)


def _decode_ws_message(message: str | bytes) -> dict | None:
    if isinstance(message, (bytes, bytearray)):
        try:
            raw = message.decode("utf-8", errors="ignore")
        except Exception:
            return None
    elif isinstance(message, str):
        raw = message
    else:
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


def _iter_mexc_payloads(message) -> Iterable[Tuple[str, dict]]:
    if not isinstance(message, dict):
        return []

    default_symbol: str | None = None

    sym_candidate = message.get("symbol") or message.get("s")
    if isinstance(sym_candidate, str) and sym_candidate:
        default_symbol = sym_candidate

    params = message.get("params")
    if isinstance(params, list):
        for item in reversed(params):
            if isinstance(item, str) and item:
                default_symbol = item
                break

    payload_candidates = []
    for key in ("data", "tick", "ticker", "tickers", "result", "payload"):
        if key in message:
            payload_candidates.append(message[key])

    if isinstance(params, list):
        payload_candidates.append(params)

    if not payload_candidates:
        payload_candidates.append(message)

    seen: set[tuple[str, int]] = set()
    items: list[tuple[str, dict]] = []
    for candidate in payload_candidates:
        for symbol, payload in _iter_payload_items(candidate, default_symbol):
            key = (symbol, id(payload))
            if key in seen:
                continue
            seen.add(key)
            items.append((symbol, payload))
    return items


def _iter_payload_items(payload, default_symbol: str | None) -> Iterable[tuple[str, dict]]:
    if payload is None:
        return []

    items: list[tuple[str, dict]] = []

    if isinstance(payload, dict):
        dict_values = list(payload.values())
        if dict_values and all(isinstance(v, dict) for v in dict_values):
            for key, value in payload.items():
                if not isinstance(value, dict):
                    continue
                symbol = _extract_symbol(value, key, default_symbol)
                if symbol:
                    items.append((symbol, value))
        else:
            symbol = _extract_symbol(payload, None, default_symbol)
            if symbol:
                items.append((symbol, payload))
        return items

    if isinstance(payload, list):
        for value in payload:
            if not isinstance(value, dict):
                continue
            symbol = _extract_symbol(value, None, default_symbol)
            if symbol:
                items.append((symbol, value))
        return items

    return items


def _extract_symbol(payload: dict, fallback_key: str | None, default_symbol: str | None) -> str | None:
    for key in ("symbol", "s", "instId", "contract", "pair", "market"):
        val = payload.get(key)
        if isinstance(val, str) and val:
            return val

    if fallback_key:
        return fallback_key

    return default_symbol


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

