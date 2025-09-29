from __future__ import annotations
from typing import Literal, TypedDict
from pydantic import BaseModel, Field
import time

ExchangeName = Literal["binance", "bybit"]
Symbol = str  # "BTCUSDT"

class TickerDict(TypedDict):
    exchange: ExchangeName
    symbol: Symbol
    bid: float
    ask: float
    ts: float  # epoch seconds

class Ticker(BaseModel):
    exchange: ExchangeName
    symbol: Symbol
    bid: float = Field(gt=0)
    ask: float = Field(gt=0)
    ts: float = Field(default_factory=lambda: time.time())

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
