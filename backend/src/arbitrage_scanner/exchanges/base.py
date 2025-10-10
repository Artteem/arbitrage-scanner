from abc import ABC, abstractmethod
from typing import AsyncIterator, NotRequired, TypedDict

class Ticker(TypedDict):
    symbol: str
    bid: float
    ask: float
    ts: float  # epoch seconds (platform receive time)
    event_ts: NotRequired[float]

class ExchangeConnector(ABC):
    name: str

    @abstractmethod
    async def tickers(self) -> AsyncIterator[Ticker]:
        """Yield normalized tickers (bid/ask) from the exchange."""
        raise NotImplementedError
