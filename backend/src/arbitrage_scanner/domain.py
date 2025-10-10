from __future__ import annotations
from typing import NotRequired, TypedDict
from pydantic import BaseModel, Field
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
    ts: float  # epoch seconds (moment the platform received the update)
    event_ts: NotRequired[float]
    latency: NotRequired[float]

class Ticker(BaseModel):
    exchange: ExchangeName
    symbol: Symbol
    bid: float = Field(gt=0)
    ask: float = Field(gt=0)
    ts: float = Field(default_factory=lambda: time.time())
    event_ts: float | None = None

    @property
    def mid(self) -> float:
        return (self.bid + self.ask) / 2

    @property
    def latency(self) -> float | None:
        """Return transport latency (seconds) if an exchange event timestamp is present."""

        if self.event_ts is None:
            return None
        return max(0.0, self.ts - float(self.event_ts))

    def to_dict(self) -> TickerDict:
        payload: TickerDict = TickerDict(
            exchange=self.exchange,
            symbol=self.symbol,
            bid=self.bid,
            ask=self.ask,
            ts=self.ts,
        )
        if self.event_ts is not None:
            payload["event_ts"] = float(self.event_ts)
            latency = self.latency
            if latency is not None:
                payload["latency"] = latency
        return payload
