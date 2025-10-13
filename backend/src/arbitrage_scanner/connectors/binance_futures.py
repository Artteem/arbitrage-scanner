from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Dict, Iterable, Sequence

import websockets

from ..domain import Symbol, Ticker
from ..store import TickerStore
from .discovery import discover_binance_usdt_perp

WS_BASE = "wss://fstream.binance.com/stream"
WS_SUB_BATCH = 250
WS_RECONNECT_INITIAL = 1.0
WS_RECONNECT_MAX = 60.0
MIN_SYMBOL_THRESHOLD = 5

STREAM_SUFFIX_BOOK = "@bookTicker"
STREAM_SUFFIX_MARK = "@markPrice@1s"
STREAM_SUFFIX_DEPTH = "@depth5@100ms"
STREAM_SUFFIX_TRADE = "@aggTrade"


logger = logging.getLogger(__name__)


async def run_binance(store: TickerStore, symbols: Sequence[Symbol]) -> None:
    subscribe = [sym for sym in dict.fromkeys(symbols) if sym]

    logger.info("run_binance received %d symbols", len(subscribe))

    if len(subscribe) < MIN_SYMBOL_THRESHOLD:
        logger.warning(
            "Binance symbol list below threshold (%d < %d), triggering discovery fallback",
            len(subscribe),
            MIN_SYMBOL_THRESHOLD,
        )
        try:
            discovered = await discover_binance_usdt_perp()
        except Exception:
            discovered = set()
        if discovered:
            subscribe = sorted(discovered)
            logger.info("Using %d symbols from discovery fallback", len(subscribe))
        else:
            logger.error("Discovery fallback did not return any Binance symbols")

    if not subscribe:
        logger.error("No Binance symbols to subscribe to — skipping run_binance startup")
        return

    logger.info("Subscribing to %d Binance symbols", len(subscribe))

    tasks = [
        asyncio.create_task(_run_binance_chunk(store, chunk))
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


def _build_stream_map(symbols: Sequence[Symbol]) -> Dict[str, tuple[str, str]]:
    mapping: Dict[str, tuple[str, str]] = {}
    for symbol in symbols:
        lower = symbol.lower()
        mapping[f"{lower}{STREAM_SUFFIX_BOOK}"] = ("book", symbol)
        mapping[f"{lower}{STREAM_SUFFIX_MARK}"] = ("mark", symbol)
        mapping[f"{lower}{STREAM_SUFFIX_DEPTH}"] = ("depth", symbol)
        mapping[f"{lower}{STREAM_SUFFIX_TRADE}"] = ("trade", symbol)
    return mapping


async def _run_binance_chunk(store: TickerStore, symbols: Sequence[Symbol]) -> None:
    if not symbols:
        return

    stream_map = _build_stream_map(symbols)
    stream_path = "/".join(stream_map.keys())
    endpoint = f"{WS_BASE}?streams={stream_path}"

    delay = WS_RECONNECT_INITIAL
    while True:
        try:
            async with websockets.connect(
                endpoint,
                ping_interval=20,
                ping_timeout=20,
                close_timeout=5,
                compression=None,
            ) as ws:
                delay = WS_RECONNECT_INITIAL
                async for raw in ws:
                    try:
                        message = json.loads(raw)
                    except Exception:
                        continue

                    stream = message.get("stream")
                    if not stream:
                        continue

                    entry = stream_map.get(stream)
                    if not entry:
                        continue

                    payload = message.get("data") or {}
                    _dispatch_stream(store, entry, payload)
        except asyncio.CancelledError:
            raise
        except Exception:
            await asyncio.sleep(delay)
            delay = min(delay * 2, WS_RECONNECT_MAX)


def _dispatch_stream(
    store: TickerStore, entry: tuple[str, str], payload: dict
) -> None:
    kind, symbol = entry
    if kind == "book":
        _handle_book(store, symbol, payload)
    elif kind == "depth":
        _handle_depth(store, symbol, payload)
    elif kind == "mark":
        _handle_mark(store, symbol, payload)
    elif kind == "trade":
        _handle_trade(store, symbol, payload)


def _handle_book(store: TickerStore, symbol: str, payload: dict) -> None:
    bid_raw = payload.get("b") or payload.get("bidPrice")
    ask_raw = payload.get("a") or payload.get("askPrice")
    if bid_raw is None or ask_raw is None:
        return

    try:
        bid = float(bid_raw)
        ask = float(ask_raw)
    except (TypeError, ValueError):
        return

    if bid <= 0 or ask <= 0:
        return

    now = time.time()
    store.upsert_ticker(
        Ticker(exchange="binance", symbol=symbol, bid=bid, ask=ask, ts=now)
    )


def _handle_depth(store: TickerStore, symbol: str, payload: dict) -> None:
    bids = payload.get("bids") or payload.get("b") or []
    asks = payload.get("asks") or payload.get("a") or []
    if not bids and not asks:
        return

    def _convert(levels):
        out = []
        for level in levels[:5]:
            if not isinstance(level, (list, tuple)) or len(level) < 2:
                continue
            try:
                price = float(level[0])
                size = float(level[1])
            except (TypeError, ValueError):
                continue
            if price > 0 and size > 0:
                out.append((price, size))
        return out

    bids_conv = _convert(bids)
    asks_conv = _convert(asks)
    if not bids_conv and not asks_conv:
        return

    now = time.time()
    store.upsert_order_book(
        "binance",
        symbol,
        bids=bids_conv or None,
        asks=asks_conv or None,
        ts=now,
    )

    if bids_conv and asks_conv:
        store.upsert_ticker(
            Ticker(
                exchange="binance",
                symbol=symbol,
                bid=bids_conv[0][0],
                ask=asks_conv[0][0],
                ts=now,
            )
        )


def _handle_mark(store: TickerStore, symbol: str, payload: dict) -> None:
    rate_raw = payload.get("r") or payload.get("fundingRate")
    if rate_raw is None:
        return
    try:
        rate = float(rate_raw)
    except (TypeError, ValueError):
        rate = 0.0
    store.upsert_funding("binance", symbol, rate=rate, interval="8h", ts=time.time())


def _handle_trade(store: TickerStore, symbol: str, payload: dict) -> None:
    price_raw = payload.get("p") or payload.get("price")
    if price_raw is None:
        return
    try:
        price = float(price_raw)
    except (TypeError, ValueError):
        return
    if price <= 0:
        return
    store.upsert_order_book(
        "binance",
        symbol,
        last_price=price,
        last_price_ts=time.time(),
    )

