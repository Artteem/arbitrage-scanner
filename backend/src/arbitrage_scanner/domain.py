from __future__ import annotations
from dataclasses import dataclass, field
from typing import TypedDict
import time

# Биржа теперь задаётся строкой, чтобы можно было подключать произвольные коннекторы
# без изменения доменной модели.
ExchangeName = str
Symbol = str  # "BTCUSDT"

class TickerDict(TypedDict):
    exchange: ExchangeName
    symbol: Symbol
    bid: float
    ask: float
    ts: float  # epoch seconds

@dataclass
class Ticker:
    exchange: ExchangeName
    symbol: Symbol
    bid: float
    ask: float
    ts: float = field(default_factory=lambda: time.time())

    def __post_init__(self) -> None:
        if self.bid <= 0:
            raise ValueError("bid must be greater than 0")
        if self.ask <= 0:
            raise ValueError("ask must be greater than 0")

    @property
    def mid(self) -> float:
        return (self.bid + self.ask) / 2

    def to_dict(self) -> TickerDict:
        return TickerDict(
            exchange=self.exchange,
            symbol=self.symbol,
            bid=self.bid,
            ask=self.ask,
            ts=self.ts,
        )
