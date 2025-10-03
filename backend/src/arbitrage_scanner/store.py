from __future__ import annotations
from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Tuple
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

class TickerStore:
    def __init__(self) -> None:
        self._latest: Dict[Key, Ticker] = {}
        self._funding: Dict[Key, Funding] = {}
        self._order_books: Dict[Key, OrderBookData] = {}

    def upsert_ticker(self, t: Ticker) -> None:
        self._latest[(t.exchange, t.symbol)] = t

    def upsert_funding(self, exchange: ExchangeName, symbol: Symbol, rate: float, interval: str, ts: float) -> None:
        self._funding[(exchange, symbol)] = Funding(rate=rate, interval=interval, ts=ts)

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
        if ob is None:
            ob = OrderBookData()
        if bids is not None:
            ob.bids = [(float(price), float(size)) for price, size in bids if price and size]
        if asks is not None:
            ob.asks = [(float(price), float(size)) for price, size in asks if price and size]
        if ts is not None:
            ob.ts = ts
        if last_price is not None:
            ob.last_price = float(last_price)
            if last_price_ts is None:
                last_price_ts = time.time()
        if last_price_ts is not None:
            ob.last_price_ts = last_price_ts
        self._order_books[key] = ob

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
