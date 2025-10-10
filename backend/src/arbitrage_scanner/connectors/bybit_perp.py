from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Sequence, Tuple

import websockets
from websockets.exceptions import ConnectionClosed

from ..domain import Symbol, Ticker
from ..store import TickerStore
from .discovery import discover_bybit_linear_usdt
from .utils import pick_timestamp, now_ts

WS = "wss://stream.bybit.com/v5/public/linear"
CHUNK = 100  # безопасный размер по числу подписок

MIN_SYMBOL_THRESHOLD = 5

PING_INTERVAL = 10.0
MAX_IDLE_BEFORE_RECONNECT = 30.0


logger = logging.getLogger(__name__)


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

    tasks = [
        asyncio.create_task(_run_bybit_tickers(store, subscribe)),
        asyncio.create_task(_run_bybit_orderbooks(store, subscribe)),
    ]

    try:
        await asyncio.gather(*tasks)
    finally:
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


async def _run_bybit_tickers(store: TickerStore, subscribe: Sequence[Symbol]):
    if not subscribe:
        return

    async for ws in _reconnect(WS):
        try:
            for i in range(0, len(subscribe), CHUNK):
                batch = subscribe[i : i + CHUNK]
                args = [f"tickers.{s}" for s in batch]
                await ws.send(json.dumps({"op": "subscribe", "args": args}))
                await asyncio.sleep(0.05)

            while True:
                raw = await _recv_with_keepalive(ws)
                if raw is None:
                    break

                try:
                    data = json.loads(raw)
                except Exception:
                    logger.exception("Failed to decode Bybit ticker payload", exc_info=True)
                    continue

                if data.get("op") == "pong":
                    continue

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

                    bid_s = item.get("bid1Price") or item.get("bidPrice")
                    ask_s = item.get("ask1Price") or item.get("askPrice")
                    try:
                        bid = float(bid_s) if bid_s is not None else 0.0
                        ask = float(ask_s) if ask_s is not None else 0.0
                    except Exception:
                        bid = ask = 0.0

                    event_ts = pick_timestamp(
                        item.get("ts"),
                        item.get("time"),
                        item.get("eventTime"),
                        data.get("ts"),
                        default=now_ts(),
                    )

                    if bid > 0 and ask > 0:
                        store.upsert_ticker(
                            Ticker(exchange="bybit", symbol=sym, bid=bid, ask=ask, ts=event_ts)
                        )

                    fr = item.get("fundingRate")
                    if fr is not None:
                        try:
                            rate = float(fr)
                        except Exception:
                            rate = 0.0
                        store.upsert_funding("bybit", sym, rate=rate, interval="8h", ts=event_ts)

                    last_price_raw = (
                        item.get("lastPrice")
                        or item.get("markPrice")
                        or item.get("indexPrice")
                        or item.get("prevPrice24h")
                    )
                    if last_price_raw is not None:
                        try:
                            last_price = float(last_price_raw)
                        except Exception:
                            last_price = None
                        if last_price and last_price > 0:
                            last_price_ts = pick_timestamp(
                                item.get("ts"),
                                item.get("priceTime"),
                                item.get("updateTime"),
                                data.get("ts"),
                                default=event_ts,
                            )
                            store.upsert_order_book(
                                "bybit",
                                sym,
                                last_price=last_price,
                                last_price_ts=last_price_ts,
                            )
        except ConnectionClosed:
            logger.warning("Bybit ticker stream connection closed, reconnecting")
        except Exception:
            logger.exception("Bybit ticker stream error", exc_info=True)
        await asyncio.sleep(1)


async def _run_bybit_orderbooks(store: TickerStore, subscribe: Sequence[Symbol]):
    if not subscribe:
        return

    topics = [f"orderbook.50.{sym}" for sym in subscribe]
    books: Dict[str, _OrderBookState] = {}

    async for ws in _reconnect(WS):
        try:
            for i in range(0, len(topics), CHUNK):
                batch = topics[i : i + CHUNK]
                await ws.send(json.dumps({"op": "subscribe", "args": batch}))
                await asyncio.sleep(0.05)

            while True:
                raw = await _recv_with_keepalive(ws)
                if raw is None:
                    break

                try:
                    data = json.loads(raw)
                except Exception:
                    logger.exception("Failed to decode Bybit orderbook payload", exc_info=True)
                    continue

                if data.get("op") == "pong":
                    continue
                topic = data.get("topic", "")
                if not topic.startswith("orderbook."):
                    continue

                payload = data.get("data")
                if payload is None:
                    continue

                items = payload if isinstance(payload, list) else [payload]
                msg_type = str(data.get("type") or "").lower()

                for item in items:
                    if not isinstance(item, dict):
                        continue
                    sym = item.get("s") or item.get("symbol")
                    if not sym:
                        continue

                    bids = item.get("b") or item.get("bid") or item.get("bids")
                    asks = item.get("a") or item.get("ask") or item.get("asks")
                    if not bids and not asks:
                        continue

                    event_ts = pick_timestamp(
                        item.get("ts"),
                        item.get("time"),
                        data.get("ts"),
                        data.get("sentTime"),
                        default=now_ts(),
                    )

                    book = books.setdefault(sym, _OrderBookState())
                    if msg_type == "snapshot":
                        book.snapshot(bids, asks, event_ts)
                    else:
                        book.update(bids, asks, event_ts)

                    best_bids, best_asks = book.top_levels()
                    if best_bids or best_asks:
                        store.upsert_order_book(
                            "bybit",
                            sym,
                            bids=best_bids or None,
                            asks=best_asks or None,
                            ts=event_ts,
                        )

                    best_bid_price = best_bids[0][0] if best_bids else book.best_bid_price()
                    best_ask_price = best_asks[0][0] if best_asks else book.best_ask_price()

                    if (
                        best_bid_price is not None
                        and best_ask_price is not None
                        and best_bid_price > 0
                        and best_ask_price > 0
                    ):
                        store.upsert_ticker(
                            Ticker(
                                exchange="bybit",
                                symbol=sym,
                                bid=best_bid_price,
                                ask=best_ask_price,
                                ts=event_ts,
                            )
                        )
        except ConnectionClosed:
            logger.warning("Bybit orderbook stream connection closed, reconnecting")
        except Exception:
            logger.exception("Bybit orderbook stream error", exc_info=True)
        await asyncio.sleep(1)


async def _reconnect(url: str):
    while True:
        try:
            async with websockets.connect(
                url,
                ping_interval=None,
                close_timeout=5,
            ) as ws:
                yield ws
        except Exception:
            logger.exception("Bybit websocket reconnect failed", exc_info=True)
            await asyncio.sleep(1)


async def _recv_with_keepalive(ws: websockets.WebSocketClientProtocol) -> str | None:
    """Получить сообщение с учётом keepalive/пинга.

    Возвращает None, если соединение нужно переподключить.
    """

    idle = 0.0
    while True:
        try:
            return await asyncio.wait_for(ws.recv(), timeout=PING_INTERVAL)
        except asyncio.TimeoutError:
            idle += PING_INTERVAL
            if idle >= MAX_IDLE_BEFORE_RECONNECT:
                logger.warning(
                    "Bybit websocket idle for %.1fs, reconnecting", idle
                )
                return None
            try:
                await ws.send(json.dumps({"op": "ping"}))
            except Exception:
                logger.exception("Failed to send ping to Bybit websocket", exc_info=True)
                return None
        except ConnectionClosed:
            raise
        except Exception:
            logger.exception("Unexpected Bybit websocket receive error", exc_info=True)
            return None


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

    def top_levels(self, depth: int = 5) -> Tuple[List[Tuple[float, float]], List[Tuple[float, float]]]:
        bids_sorted = sorted(self.bids.items(), key=lambda kv: kv[0], reverse=True)[:depth]
        asks_sorted = sorted(self.asks.items(), key=lambda kv: kv[0])[:depth]
        return bids_sorted, asks_sorted

    def best_bid_price(self) -> float | None:
        if not self.bids:
            return None
        return max(self.bids.keys())

    def best_ask_price(self) -> float | None:
        if not self.asks:
            return None
        return min(self.asks.keys())

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
            # ограничиваем локальное состояние, чтобы не разрасталось
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
