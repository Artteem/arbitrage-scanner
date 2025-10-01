from __future__ import annotations

from dataclasses import dataclass
from typing import Collection, Protocol, Sequence

from ..domain import ExchangeName, Symbol
from ..store import TickerStore


class SymbolDiscovery(Protocol):
    async def __call__(self) -> Collection[Symbol]:
        ...


class ConnectorRunner(Protocol):
    async def __call__(self, store: TickerStore, symbols: Sequence[Symbol]) -> None:
        ...


@dataclass(frozen=True)
class ConnectorSpec:
    """Описание коннектора биржи."""

    name: ExchangeName
    run: ConnectorRunner
    discover_symbols: SymbolDiscovery | None = None
    taker_fee: float | None = None

