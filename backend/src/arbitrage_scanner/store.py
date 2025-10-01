from __future__ import annotations
from typing import Dict, Tuple, Iterable
from .domain import Ticker, ExchangeName, Symbol

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

    def upsert_ticker(self, t: Ticker) -> None:
        self._latest[(t.exchange, t.symbol)] = t

    def upsert_funding(self, exchange: ExchangeName, symbol: Symbol, rate: float, interval: str, ts: float) -> None:
        self._funding[(exchange, symbol)] = Funding(rate=rate, interval=interval, ts=ts)

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
                d[ex] = {"ticker": t, "funding": f}
        return d
