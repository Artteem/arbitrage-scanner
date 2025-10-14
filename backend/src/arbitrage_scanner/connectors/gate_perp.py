from __future__ import annotations

import asyncio
import json
import time
from typing import Iterable, List, Sequence, Tuple

import websockets

from ..domain import Symbol, Ticker
from ..store import TickerStore
from .discovery import GATE_HEADERS, discover_gate_usdt_perp

WS_ENDPOINT = "wss://fx-ws.gateio.ws/v4/ws/usdt"
WS_SUB_BATCH = 100
WS_ORDERBOOK_DEPTH = 50
WS_RECONNECT_INITIAL = 2.0
WS_RECONNECT_MAX = 60.0
MIN_SYMBOL_THRESHOLD = 5
FALLBACK_SYMBOLS: tuple[Symbol, ...] = ("BTCUSDT", "ETHUSDT", "SOLUSDT")


def _as_float(value) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _to_gate_symbol(symbol: Symbol) -> str:
    sym = str(symbol).upper()
    if "_" in sym:
        return sym
    if sym.endswith("USDT"):
        return f"{sym[:-4]}_USDT"
    return sym


def _from_gate_symbol(symbol: str | None) -> Symbol | None:
    if not symbol:
        return None
    return str(symbol).replace("-", "").replace("_", "")


def _extract_price(item: dict, keys: Iterable[str]) -> float:
    for key in keys:
        val = item.get(key)
        price = _as_float(val)
        if price > 0:
            return price
    return 0.0


def _is_active_contract(item: dict) -> bool:
    state = str(item.get("state") or item.get("status") or "").strip().lower()
    if state and state not in {"open", "trading", "live"}:
        return False
    if bool(item.get("is_delisted")):
        return False
    in_delisting = item.get("in_delisting")
    if isinstance(in_delisting, str):
        if in_delisting.strip().lower() in {"true", "1"}:
            return False
    elif in_delisting:
        return False
    return True


async def run_gate(store: TickerStore, symbols: Sequence[Symbol]) -> None:
    subscribe = [sym for sym in dict.fromkeys(symbols) if sym]

    if len(subscribe) < MIN_SYMBOL_THRESHOLD:
        try:
            discovered = await discover_gate_usdt_perp()
        except Exception:
            discovered = set()
        if discovered:
            subscribe = sorted(discovered)
        else:
            subscribe = list(FALLBACK_SYMBOLS)

    if not subscribe:
        return

    tasks = []
    for chunk in _chunk_symbols(subscribe, WS_SUB_BATCH):
        tasks.append(asyncio.create_task(_run_gate_ws_chunk(store, chunk)))
        tasks.append(asyncio.create_task(_run_gate_orderbook_chunk(store, chunk)))

    try:
        await asyncio.gather(*tasks)
    finally:
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


async def _run_gate_ws_chunk(store: TickerStore, symbols: Sequence[Symbol]) -> None:
    if not symbols:
        return

    payload = [_to_gate_symbol(sym) for sym in symbols if sym]
    payload = [sym for sym in payload if sym]
    if not payload:
        return

    delay = WS_RECONNECT_INITIAL

    while True:
        try:
            async with websockets.connect(
                WS_ENDPOINT,
                ping_interval=20,
                ping_timeout=20,
                close_timeout=5,
                extra_headers=GATE_HEADERS,
            ) as ws:
                delay = WS_RECONNECT_INITIAL

                await _subscribe(ws, "futures.tickers", payload)
                await _subscribe(ws, "futures.funding_rate", payload)

                async for raw in ws:
                    try:
                        message = json.loads(raw)
                    except Exception:
                        continue

                    if await _handle_ping(ws, message):
                        continue

                    channel = str(message.get("channel") or "")
                    result = message.get("result")

                    if channel == "futures.tickers":
                        _handle_tickers(store, result)
                    elif channel == "futures.funding_rate":
                        _handle_funding(store, result)
        except asyncio.CancelledError:
            raise
        except Exception:
            await asyncio.sleep(delay)
            delay = min(delay * 2, WS_RECONNECT_MAX)


def _chunk_symbols(symbols: Sequence[Symbol], size: int) -> Iterable[Sequence[Symbol]]:
    for idx in range(0, len(symbols), size):
        yield symbols[idx : idx + size]


async def _subscribe(ws, channel: str, symbols: Sequence[str]) -> None:
    now = int(time.time())
    for symbol in symbols:
        message = {
            "time": now,
            "channel": channel,
            "event": "subscribe",
            "payload": [symbol],
        }
        try:
            await ws.send(json.dumps(message))
        except Exception:
            return


async def _run_gate_orderbook_chunk(
    store: TickerStore, symbols: Sequence[Symbol]
) -> None:
    if not symbols:
        return

    payload = [_to_gate_symbol(sym) for sym in symbols if sym]
    payload = [sym for sym in payload if sym]
    if not payload:
        return

    delay = WS_RECONNECT_INITIAL

    while True:
        try:
            async with websockets.connect(
                WS_ENDPOINT,
                ping_interval=20,
                ping_timeout=20,
                close_timeout=5,
                extra_headers=GATE_HEADERS,
            ) as ws:
                delay = WS_RECONNECT_INITIAL

                await _subscribe_orderbooks(ws, payload)

                async for raw in ws:
                    try:
                        message = json.loads(raw)
                    except Exception:
                        continue

                    if await _handle_ping(ws, message):
                        continue

                    channel = str(message.get("channel") or "")
                    result = message.get("result")

                    if channel == "futures.order_book":
                        _handle_orderbook(store, result)
        except asyncio.CancelledError:
            raise
        except Exception:
            await asyncio.sleep(delay)
            delay = min(delay * 2, WS_RECONNECT_MAX)


async def _subscribe_orderbooks(ws, symbols: Sequence[str]) -> None:
    now = int(time.time())
    payload: List[dict] = []
    for symbol in symbols:
        if not symbol:
            continue
        message = {
            "time": now,
            "channel": "futures.order_book",
            "event": "subscribe",
            "payload": [symbol, str(WS_ORDERBOOK_DEPTH), "0.1"],
        }
        payload.append(message)

    for message in payload:
        try:
            await ws.send(json.dumps(message))
        except Exception:
            return


async def _handle_ping(ws, message: dict) -> bool:
    if not isinstance(message, dict):
        return False

    event = str(message.get("event") or "").lower()
    channel = str(message.get("channel") or "")

    if event not in {"ping", "pong"} and not channel.endswith(".ping"):
        return False

    if event == "ping" or channel.endswith(".ping"):
        reply = {
            "time": int(time.time()),
            "channel": channel or "futures.ping",
            "event": "pong",
        }
        try:
            await ws.send(json.dumps(reply))
        except Exception:
            pass
        return True

    return event == "pong"


def _iter_items(payload):
    if isinstance(payload, list):
        for item in payload:
            if isinstance(item, dict):
                yield item
    elif isinstance(payload, dict):
        yield payload


def _handle_tickers(store: TickerStore, payload) -> None:
    now = time.time()
    for item in _iter_items(payload):
        contract = (
            item.get("contract")
            or item.get("name")
            or item.get("symbol")
            or item.get("s")
        )
        if not contract:
            continue

        contract_str = str(contract)
        if contract_str.count("_") > 1:
            continue

        if not _is_active_contract(item):
            continue

        sym_common = _from_gate_symbol(contract_str)
        if not sym_common:
            continue

        bid = _extract_price(
            item,
            (
                "best_bid_price",
                "highest_bid",
                "bid1",
                "best_bid",
                "bid",
            ),
        )
        ask = _extract_price(
            item,
            (
                "best_ask_price",
                "lowest_ask",
                "ask1",
                "best_ask",
                "ask",
            ),
        )
        if bid <= 0 or ask <= 0:
            continue

        store.upsert_ticker(
            Ticker(exchange="gate", symbol=sym_common, bid=bid, ask=ask, ts=now)
        )

        rate_raw = (
            item.get("funding_rate")
            or item.get("funding_rate_indicative")
            or item.get("next_funding_rate")
        )
        if rate_raw is not None:
            rate = _as_float(rate_raw)
            store.upsert_funding("gate", sym_common, rate=rate, interval="8h", ts=now)

        last_raw = (
            item.get("last")
            or item.get("last_price")
            or item.get("mark_price")
            or item.get("index_price")
        )
        last_price = _as_float(last_raw)
        if last_price > 0:
            store.upsert_order_book(
                "gate", sym_common, last_price=last_price, last_price_ts=now
            )


def _handle_funding(store: TickerStore, payload) -> None:
    now = time.time()
    for item in _iter_items(payload):
        contract = item.get("contract") or item.get("name") or item.get("symbol")
        if not contract:
            continue

        sym_common = _from_gate_symbol(str(contract))
        if not sym_common:
            continue

        rate_raw = item.get("rate") or item.get("funding_rate")
        if rate_raw is None:
            continue

        rate = _as_float(rate_raw)
        store.upsert_funding("gate", sym_common, rate=rate, interval="8h", ts=now)


def _handle_orderbook(store: TickerStore, payload) -> None:
    now = time.time()
    for item in _iter_items(payload):
        contract = (
            item.get("contract")
            or item.get("name")
            or item.get("symbol")
            or item.get("s")
        )
        if not contract:
            continue

        sym_common = _from_gate_symbol(str(contract))
        if not sym_common:
            continue

        bids_raw = item.get("bids") or item.get("bid") or item.get("buy")
        asks_raw = item.get("asks") or item.get("ask") or item.get("sell")

        bids = list(_iter_levels(bids_raw))[:20]
        asks = list(_iter_levels(asks_raw))[:20]

        last_price = _extract_price(
            item,
            (
                "last",
                "last_price",
                "mark_price",
                "index_price",
            ),
        )

        store.upsert_order_book(
            "gate",
            sym_common,
            bids=bids or None,
            asks=asks or None,
            ts=now,
            last_price=last_price if last_price > 0 else None,
            last_price_ts=now if last_price > 0 else None,
        )


def _iter_levels(source) -> Iterable[Tuple[float, float]]:
    if isinstance(source, dict):
        source = source.get("levels") or source.get("data") or source.get("list") or []
    if not isinstance(source, (list, tuple)):
        return []
    out: List[Tuple[float, float]] = []
    for entry in source:
        price, size = _parse_level(entry)
        if price is None or size is None:
            continue
        out.append((price, size))
    return out


def _parse_level(level) -> Tuple[float | None, float | None]:
    price = size = None
    if isinstance(level, dict):
        p = level.get("price") or level.get("p") or level.get("px") or level.get("bid")
        q = level.get("size") or level.get("qty") or level.get("q") or level.get("amount")
        try:
            price = float(p) if p is not None else None
        except (TypeError, ValueError):
            price = None
        try:
            size = float(q) if q is not None else None
        except (TypeError, ValueError):
            size = None
    elif isinstance(level, (list, tuple)) and len(level) >= 2:
        try:
            price = float(level[0])
        except (TypeError, ValueError):
            price = None
        try:
            size = float(level[1])
        except (TypeError, ValueError):
            size = None
    if price is None or price <= 0:
        return None, None
    if size is None:
        size = 0.0
    if size < 0:
        size = 0.0
    return price, size
