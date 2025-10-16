from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Collection, Protocol, Sequence

from ..domain import ExchangeName, Symbol
from ..store import TickerStore


class SymbolDiscovery(Protocol):
    async def __call__(self) -> Collection[Symbol]:
        ...


class ConnectorRunner(Protocol):
    async def __call__(self, store: TickerStore, symbols: Sequence[Symbol]) -> None:
        ...


@dataclass(frozen=True, slots=True)
class ConnectorContract:
    """Metadata about an exchange contract used during synchronization."""

    original_symbol: str
    normalized_symbol: Symbol
    base_asset: str
    quote_asset: str
    contract_type: str = "perp"
    tick_size: float | None = None
    lot_size: float | None = None
    contract_size: float | None = None
    taker_fee: float | None = None
    funding_symbol: str | None = None
    is_active: bool = True


@dataclass(frozen=True, slots=True)
class ConnectorQuote:
    """Top-of-book quote representation returned by REST fetchers."""

    timestamp: datetime
    bid: float
    ask: float


@dataclass(frozen=True, slots=True)
class ConnectorFundingRate:
    """Historical funding rate entry returned by REST fetchers."""

    timestamp: datetime
    rate: float
    interval: str


class ContractFetcher(Protocol):
    async def __call__(self) -> Sequence[ConnectorContract]:
        ...


class TakerFeeFetcher(Protocol):
    async def __call__(self, symbol: Symbol) -> float | None:
        ...


class HistoricalQuotesFetcher(Protocol):
    async def __call__(
        self,
        symbol: Symbol,
        start: datetime,
        end: datetime,
        interval: timedelta,
    ) -> Sequence[ConnectorQuote]:
        ...


class FundingHistoryFetcher(Protocol):
    async def __call__(self, symbol: Symbol, start: datetime, end: datetime) -> Sequence[ConnectorFundingRate]:
        ...


@dataclass(frozen=True)
class ConnectorSpec:
    """Описание коннектора биржи."""

    name: ExchangeName
    run: ConnectorRunner
    discover_symbols: SymbolDiscovery | None = None
    taker_fee: float | None = None
    fetch_contracts: ContractFetcher | None = None
    fetch_taker_fee: TakerFeeFetcher | None = None
    fetch_historical_quotes: HistoricalQuotesFetcher | None = None
    fetch_funding_history: FundingHistoryFetcher | None = None

