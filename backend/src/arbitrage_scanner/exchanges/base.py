from abc import ABC, abstractmethod
from typing import AsyncIterator, TypedDict

class Ticker(TypedDict):
    symbol: str
    bid: float
    ask: float
    ts: float  # epoch seconds

class ExchangeConnector(ABC):
    name: str

    @abstractmethod
    async def tickers(self) -> AsyncIterator[Ticker]:
        """Yield normalized tickers (bid/ask) from the exchange."""
        raise NotImplementedError
