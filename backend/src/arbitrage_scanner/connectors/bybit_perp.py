from __future__ import annotations

import asyncio
import json
import os
import time
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Sequence, Tuple

import websockets

from ..domain import Symbol, Ticker
from ..store import TickerStore
from .credentials import ApiCreds
from .discovery import discover_bybit_linear_usdt
from ..settings import settings

WS_ENDPOINT = "wss://stream.bybit.com/v5/public/linear?compress=false"
WS_SUB_BATCH = 250
WS_RECONNECT_INITIAL = 1.0
WS_RECONNECT_MAX = 60.0
MIN_SYMBOL_THRESHOLD = 5


async def run_bybit(store: TickerStore, symbols: Sequence[Symbol]) -> None:
    subscribe = [sym for sym in dict.fromkeys(symbols) if sym]
    if len(subscribe) < MIN_SYMBOL_THRESHOLD:
        try:
            discovered = await discover_bybit_linear_usdt()
        except Exception:
            discovered = set()
        if discovered:
            subscribe = sorted(discovered)

    if not subscribe:
        return

    tasks = [
        asyncio.create_task(_run_bybit_chunk(store, chunk))
        for chunk in _chunk_symbols(subscribe, WS_SUB_BATCH)
    ]

    try:
        await asyncio.gather(*tasks)
    finally:
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


def _chunk_symbols(symbols: Sequence[Symbol], size: int) -> Iterable[Sequence[Symbol]]:
    for idx in range(0, len(symbols), size):
        yield symbols[idx : idx + size]


async def _run_bybit_chunk(store: TickerStore, symbols: Sequence[Symbol]) -> None:
    if not symbols:
        return

    topics = [f"tickers.{sym}" for sym in symbols]
    topics.extend(f"orderbook.50.{sym}" for sym in symbols)
    books: Dict[str, _OrderBookState] = {}

    http_proxy = settings.ws_proxy_url or settings.http_proxy
    if http_proxy:
        os.environ["HTTP_PROXY"] = http_proxy
        os.environ["http_proxy"] = http_proxy

    https_proxy = settings.ws_proxy_url or settings.https_proxy
    if https_proxy:
        os.environ["HTTPS_PROXY"] = https_proxy
        os.environ["https_proxy"] = https_proxy

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
                await ws.send(json.dumps({"op": "subscribe", "args": topics}))

                async for raw in ws:
                    try:
                        message = json.loads(raw)
                    except Exception:
                        continue

                    topic = str(message.get("topic") or "")
                    if topic.startswith("tickers."):
                        _handle_tickers(store, message.get("data"))
                    elif topic.startswith("orderbook."):
                        _handle_orderbooks(store, books, message)
        except asyncio.CancelledError:
            raise
        except Exception:
            await asyncio.sleep(delay)
            delay = min(delay * 2, WS_RECONNECT_MAX)


async def authenticate_ws(ws: Any, creds: ApiCreds | None) -> None:
    del ws, creds
    return None


def _handle_tickers(store: TickerStore, payload) -> None:
    if payload is None:
        return

    items = payload if isinstance(payload, list) else [payload]
    now = time.time()
    for item in items:
        if not isinstance(item, dict):
            continue

        symbol = item.get("symbol")
        if not symbol:
            continue

        bid_raw = item.get("bid1Price") or item.get("bidPrice")
        ask_raw = item.get("ask1Price") or item.get("askPrice")
        try:
            bid = float(bid_raw) if bid_raw is not None else 0.0
            ask = float(ask_raw) if ask_raw is not None else 0.0
        except (TypeError, ValueError):
            bid = ask = 0.0

        if bid > 0 and ask > 0:
            store.upsert_ticker(
                Ticker(exchange="bybit", symbol=symbol, bid=bid, ask=ask, ts=now)
            )

        funding_raw = item.get("fundingRate")
        if funding_raw is not None:
            try:
                rate = float(funding_raw)
            except (TypeError, ValueError):
                rate = 0.0
            store.upsert_funding("bybit", symbol, rate=rate, interval="8h", ts=now)

        last_price_raw = (
            item.get("lastPrice")
            or item.get("markPrice")
            or item.get("indexPrice")
            or item.get("prevPrice24h")
        )
        if last_price_raw is not None:
            try:
                last_price = float(last_price_raw)
            except (TypeError, ValueError):
                last_price = None
            if last_price and last_price > 0:
                store.upsert_order_book(
                    "bybit", symbol, last_price=last_price, last_price_ts=now
                )


def _handle_orderbooks(
    store: TickerStore, books: Dict[str, _OrderBookState], message: dict
) -> None:
    payload = message.get("data")
    if payload is None:
        return

    items = payload if isinstance(payload, list) else [payload]
    msg_type = str(message.get("type") or "").lower()
    now = time.time()

    for item in items:
        if not isinstance(item, dict):
            continue

        symbol = item.get("s") or item.get("symbol")
        if not symbol:
            continue

        bids = item.get("b") or item.get("bid") or item.get("bids")
        asks = item.get("a") or item.get("ask") or item.get("asks")
        if not bids and not asks:
            continue

        book = books.setdefault(symbol, _OrderBookState())
        if msg_type == "snapshot":
            book.snapshot(bids, asks, now)
        else:
            book.update(bids, asks, now)

        best_bids, best_asks = book.top_levels()
        if best_bids or best_asks:
            store.upsert_order_book(
                "bybit",
                symbol,
                bids=best_bids or None,
                asks=best_asks or None,
                ts=now,
            )


@dataclass
class _OrderBookState:
    bids: Dict[float, float] = field(default_factory=dict)
    asks: Dict[float, float] = field(default_factory=dict)
    ts: float = 0.0

    def snapshot(self, bids, asks, ts: float) -> None:
        self.bids.clear()
        self.asks.clear()
        self._apply(self.bids, bids)
        self._apply(self.asks, asks)
        self.ts = ts

    def update(self, bids, asks, ts: float) -> None:
        self._apply(self.bids, bids)
        self._apply(self.asks, asks)
        self.ts = ts

    def top_levels(
        self, depth: int = 5
    ) -> Tuple[List[Tuple[float, float]], List[Tuple[float, float]]]:
        bids_sorted = sorted(self.bids.items(), key=lambda kv: kv[0], reverse=True)[:depth]
        asks_sorted = sorted(self.asks.items(), key=lambda kv: kv[0])[:depth]
        return bids_sorted, asks_sorted

    def _apply(self, side: Dict[float, float], updates) -> None:
        if not updates:
            return
        for level in _iter_levels(updates):
            price, size = level
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


def _iter_levels(source) -> Iterable[Tuple[float, float]]:
    if isinstance(source, dict):
        source = source.get("levels") or source.get("list") or []
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
        for key in ("size", "qty", "q", "v"):
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

