from __future__ import annotations
import asyncio
import logging
from dataclasses import dataclass, field
from typing import Awaitable, Callable, Dict, Iterable, List, Tuple
from .domain import Ticker, ExchangeName, Symbol
import time


@dataclass
class OrderBookData:
    bids: List[Tuple[float, float]] = field(default_factory=list)
    asks: List[Tuple[float, float]] = field(default_factory=list)
    ts: float = 0.0
    last_price: float | None = None
    last_price_ts: float = 0.0

    def copy(self) -> "OrderBookData":
        return OrderBookData(
            bids=list(self.bids),
            asks=list(self.asks),
            ts=self.ts,
            last_price=self.last_price,
            last_price_ts=self.last_price_ts,
        )

    def to_dict(self) -> dict:
        return {
            "bids": [[price, size] for price, size in self.bids],
            "asks": [[price, size] for price, size in self.asks],
            "ts": self.ts,
            "last_price": self.last_price,
            "last_price_ts": self.last_price_ts,
        }

Key = Tuple[ExchangeName, Symbol]

class Funding:
    __slots__ = ("rate", "interval", "ts")
    def __init__(self, rate: float = 0.0, interval: str = "", ts: float = 0.0) -> None:
        self.rate = rate
        self.interval = interval  # e.g., "8h"
        self.ts = ts

    def to_dict(self) -> dict:
        return {"rate": self.rate, "interval": self.interval, "ts": self.ts}

logger = logging.getLogger(__name__)


class TickerStore:
    def __init__(self) -> None:
        self._latest: Dict[Key, Ticker] = {}
        self._funding: Dict[Key, Funding] = {}
        self._order_books: Dict[Key, OrderBookData] = {}
        self._listeners: list[Callable[[], None | Awaitable[None]]] = []
        self._latency_warnings: Dict[Key, float] = {}

    def add_listener(self, listener: Callable[[], None | Awaitable[None]]) -> None:
        self._listeners.append(listener)

    def remove_listener(self, listener: Callable[[], None | Awaitable[None]]) -> None:
        try:
            self._listeners.remove(listener)
        except ValueError:
            pass

    def _notify_listeners(self) -> None:
        if not self._listeners:
            return
        loop = None
        for listener in list(self._listeners):
            try:
                result = listener()
            except Exception:  # pragma: no cover - defensive logging
                logger.exception("TickerStore listener failed", exc_info=True)
                continue
            if asyncio.iscoroutine(result):
                if loop is None:
                    try:
                        loop = asyncio.get_running_loop()
                    except RuntimeError:
                        loop = None
                if loop is not None:
                    loop.create_task(result)

    def upsert_ticker(self, t: Ticker) -> None:
        key = (t.exchange, t.symbol)
        previous = self._latest.get(key)
        if (
            previous is not None
            and previous.bid == t.bid
            and previous.ask == t.ask
            and abs(previous.ts - t.ts) < 1e-6
        ):
            return
        self._latest[key] = t

        latency = t.latency
        if latency is not None and latency > 1.5:
            now = time.time()
            last_warn = self._latency_warnings.get(key, 0.0)
            if now - last_warn > 5.0:
                self._latency_warnings[key] = now
                logger.warning(
                    "High latency %.3fs for %s %s", latency, t.exchange, t.symbol
                )

        self._notify_listeners()

    def upsert_funding(self, exchange: ExchangeName, symbol: Symbol, rate: float, interval: str, ts: float) -> None:
        key = (exchange, symbol)
        prev = self._funding.get(key)
        if prev is not None and prev.rate == rate and prev.interval == interval:
            if prev.ts and abs(prev.ts - ts) < 1e-6:
                return
        self._funding[key] = Funding(rate=rate, interval=interval, ts=ts)
        self._notify_listeners()

    def upsert_order_book(
        self,
        exchange: ExchangeName,
        symbol: Symbol,
        *,
        bids: List[Tuple[float, float]] | None = None,
        asks: List[Tuple[float, float]] | None = None,
        ts: float | None = None,
        last_price: float | None = None,
        last_price_ts: float | None = None,
    ) -> None:
        key = (exchange, symbol)
        ob = self._order_books.get(key)
        changed = False
        if ob is None:
            ob = OrderBookData()
            changed = True
        if bids is not None:
            new_bids = [(float(price), float(size)) for price, size in bids if price and size]
            if new_bids != ob.bids:
                ob.bids = new_bids
                changed = True
        if asks is not None:
            new_asks = [(float(price), float(size)) for price, size in asks if price and size]
            if new_asks != ob.asks:
                ob.asks = new_asks
                changed = True
        if ts is not None and ts != ob.ts:
            ob.ts = ts
            changed = True
        if last_price is not None:
            new_last = float(last_price)
            if ob.last_price != new_last:
                ob.last_price = new_last
                changed = True
            if last_price_ts is None:
                last_price_ts = time.time()
        if last_price_ts is not None and last_price_ts != ob.last_price_ts:
            ob.last_price_ts = last_price_ts
            changed = True
        if changed:
            self._order_books[key] = ob
            self._notify_listeners()

    def snapshot(self) -> Dict[str, dict]:
        out: Dict[str, dict] = {}
        for (ex, sym), t in self._latest.items():
            f = self._funding.get((ex, sym))
            out_key = f"{sym}:{ex}"
            out[out_key] = {
                **t.to_dict(),
                "funding": f.to_dict() if f else {"rate": 0.0, "interval": "", "ts": 0.0}
            }
        return out

    def symbols(self) -> Iterable[Symbol]:
        return {sym for (_, sym) in self._latest.keys()}

    def exchanges(self) -> Iterable[ExchangeName]:
        return {ex for (ex, _) in self._latest.keys()}

    def get_ticker(self, exchange: ExchangeName, symbol: Symbol) -> Ticker | None:
        return self._latest.get((exchange, symbol))

    def get_funding(self, exchange: ExchangeName, symbol: Symbol) -> Funding | None:
        return self._funding.get((exchange, symbol))

    def by_symbol(self, symbol: Symbol) -> dict:
        d: Dict[ExchangeName, dict] = {}
        for (ex, sym), t in self._latest.items():
            if sym == symbol:
                f = self._funding.get((ex, sym))
                ob = self._order_books.get((ex, sym))
                d[ex] = {"ticker": t, "funding": f, "order_book": ob.copy() if ob else None}
        return d

    def get_order_book(self, exchange: ExchangeName, symbol: Symbol) -> OrderBookData | None:
        ob = self._order_books.get((exchange, symbol))
        if not ob:
            return None
        return ob.copy()
