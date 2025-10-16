from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from types import MappingProxyType
from typing import Awaitable, Callable, Dict, Iterable, List, Mapping, Sequence

from ..connectors.base import (
    ConnectorContract,
    ConnectorFundingRate,
    ConnectorQuote,
    ConnectorSpec,
)
from ..domain import ExchangeName, Symbol
from .repositories import (
    ContractUpsert,
    FundingRateUpsert,
    QuoteCreate,
    bulk_insert_quotes,
    upsert_contracts,
    upsert_exchange,
    update_contract_taker_fee,
    upsert_funding_rates,
)
from .session import get_session
from .models import ContractType

logger = logging.getLogger(__name__)

_DEFAULT_LOOKBACK_DAYS = 10
_DEFAULT_INTERVAL = timedelta(minutes=1)
_PERIODIC_SYNC_INTERVAL = timedelta(hours=24)


@dataclass(slots=True)
class SyncedContract:
    contract_id: int
    contract: ConnectorContract


@dataclass(slots=True)
class ExchangeContractsState:
    exchange_id: int
    contracts: Dict[Symbol, SyncedContract]


@dataclass(frozen=True)
class DataSyncSummary:
    per_exchange: Mapping[ExchangeName, List[Symbol]]
    symbols_union: List[Symbol]
    contracts: Mapping[ExchangeName, Mapping[Symbol, int]]


def _to_decimal(value: float | None) -> Decimal | None:
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except Exception:  # noqa: BLE001
        return None


def _contract_type(value: str | ContractType | None) -> ContractType:
    if isinstance(value, ContractType):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"perp", "perpetual", "swap"}:
            return ContractType.PERPETUAL
        if normalized in {"spot"}:
            return ContractType.SPOT
    return ContractType.PERPETUAL


def _build_summary(states: Mapping[ExchangeName, ExchangeContractsState]) -> DataSyncSummary:
    per_exchange: Dict[ExchangeName, List[Symbol]] = {}
    per_exchange_contracts: Dict[ExchangeName, Dict[Symbol, int]] = {}
    union: set[Symbol] = set()
    for name, state in states.items():
        symbols = sorted(state.contracts.keys())
        if not symbols:
            continue
        per_exchange[name] = symbols
        union.update(symbols)
        per_exchange_contracts[name] = {
            symbol: synced.contract_id for symbol, synced in state.contracts.items()
        }
    return DataSyncSummary(
        per_exchange=MappingProxyType(per_exchange),
        symbols_union=sorted(union),
        contracts=MappingProxyType(
            {name: MappingProxyType(mapping) for name, mapping in per_exchange_contracts.items()}
        ),
    )


async def _fetch_contracts_for_connector(
    connector: ConnectorSpec,
) -> tuple[ExchangeContractsState | None, List[Symbol]]:
    if connector.fetch_contracts is None:
        return None, []
    try:
        contracts = await connector.fetch_contracts()
    except Exception:  # noqa: BLE001
        logger.exception("Failed to fetch contracts for %s", connector.name)
        return None, []
    if not contracts:
        return None, []

    taker_fee_default = (
        _to_decimal(connector.taker_fee) if connector.taker_fee is not None else None
    )

    async with get_session() as session:
        exchange_id = await upsert_exchange(
            session,
            name=connector.name,
            taker_fee=taker_fee_default,
            maker_fee=None,
        )
        upserts: List[ContractUpsert] = []
        info_by_symbol: Dict[Symbol, ConnectorContract] = {}
        for contract in contracts:
            normalized = Symbol(str(contract.normalized_symbol).upper())
            info_by_symbol[normalized] = contract
            funding_symbol = contract.funding_symbol or contract.original_symbol
            upserts.append(
                ContractUpsert(
                    exchange_id=exchange_id,
                    original_name=contract.original_symbol,
                    normalized_name=normalized,
                    base_asset=contract.base_asset.upper(),
                    quote_asset=contract.quote_asset.upper(),
                    contract_type=_contract_type(contract.contract_type),
                    is_active=contract.is_active,
                    contract_size=_to_decimal(contract.contract_size),
                    tick_size=_to_decimal(contract.tick_size),
                    lot_size=_to_decimal(contract.lot_size),
                    taker_fee=_to_decimal(contract.taker_fee),
                    funding_symbol=funding_symbol.upper() if funding_symbol else None,
                )
            )
        contract_ids = await upsert_contracts(session, upserts)
        await session.commit()

    contracts_state: Dict[Symbol, SyncedContract] = {}
    for normalized, contract_id in contract_ids.items():
        info_lookup = info_by_symbol.get(Symbol(normalized))
        if info_lookup is None:
            continue
        contracts_state[Symbol(normalized)] = SyncedContract(
            contract_id=contract_id,
            contract=info_lookup,
        )
    symbols = sorted(contracts_state.keys())
    return ExchangeContractsState(exchange_id=exchange_id, contracts=contracts_state), symbols


async def refresh_exchange_metadata(
    connectors: Sequence[ConnectorSpec],
) -> tuple[Mapping[ExchangeName, ExchangeContractsState], DataSyncSummary]:
    states: Dict[ExchangeName, ExchangeContractsState] = {}
    for connector in connectors:
        state, _symbols = await _fetch_contracts_for_connector(connector)
        if state is not None:
            states[connector.name] = state
    return states, _build_summary(states)


async def perform_initial_sync(
    connectors: Sequence[ConnectorSpec],
    *,
    lookback_days: int = _DEFAULT_LOOKBACK_DAYS,
    interval: timedelta = _DEFAULT_INTERVAL,
) -> DataSyncSummary:
    states, summary = await refresh_exchange_metadata(connectors)
    if not states:
        logger.warning("No exchange metadata could be synchronized during startup")
        return DataSyncSummary(per_exchange={}, symbols_union=[], contracts={})

    await _update_contract_taker_fees(connectors, states)

    end = datetime.now(tz=UTC)
    start = end - timedelta(days=max(lookback_days, 0))
    await _ingest_history(connectors, states, start=start, end=end, interval=interval)
    return summary


async def _update_contract_taker_fees(
    connectors: Sequence[ConnectorSpec],
    states: Mapping[ExchangeName, ExchangeContractsState],
) -> None:
    for connector in connectors:
        state = states.get(connector.name)
        if state is None or connector.fetch_taker_fee is None:
            continue
        for symbol, synced in state.contracts.items():
            try:
                fee = await connector.fetch_taker_fee(symbol)
            except Exception:  # noqa: BLE001
                logger.exception(
                    "Failed to fetch taker fee for %s:%s", connector.name, symbol
                )
                continue
            if fee is None:
                continue
            async with get_session() as session:
                await update_contract_taker_fee(
                    session, contract_id=synced.contract_id, taker_fee=_to_decimal(fee)
                )
                await session.commit()


async def _ingest_history(
    connectors: Sequence[ConnectorSpec],
    states: Mapping[ExchangeName, ExchangeContractsState],
    *,
    start: datetime,
    end: datetime,
    interval: timedelta,
) -> None:
    for connector in connectors:
        state = states.get(connector.name)
        if state is None:
            continue
        if connector.fetch_historical_quotes is None and connector.fetch_funding_history is None:
            continue
        for symbol, synced in state.contracts.items():
            await _collect_quotes(connector, symbol, synced, start, end, interval)
            await _collect_funding(connector, symbol, synced, start, end)


async def _collect_quotes(
    connector: ConnectorSpec,
    symbol: Symbol,
    synced: SyncedContract,
    start: datetime,
    end: datetime,
    interval: timedelta,
) -> None:
    if connector.fetch_historical_quotes is None:
        return
    try:
        quotes = await connector.fetch_historical_quotes(symbol, start, end, interval)
    except Exception:  # noqa: BLE001
        logger.exception(
            "Failed to fetch historical quotes for %s:%s", connector.name, symbol
        )
        return
    if not quotes:
        return

    models: List[QuoteCreate] = []
    for entry in quotes:
        if entry.bid <= 0 or entry.ask <= 0:
            continue
        bid = _to_decimal(entry.bid)
        ask = _to_decimal(entry.ask)
        if bid is None or ask is None:
            continue
        models.append(
            QuoteCreate(
                contract_id=synced.contract_id,
                timestamp=_normalize_timestamp(entry.timestamp),
                bid=bid,
                ask=ask,
            )
        )
    if not models:
        return
    async with get_session() as session:
        await bulk_insert_quotes(session, models)
        await session.commit()


async def _collect_funding(
    connector: ConnectorSpec,
    symbol: Symbol,
    synced: SyncedContract,
    start: datetime,
    end: datetime,
) -> None:
    if connector.fetch_funding_history is None:
        return
    try:
        funding = await connector.fetch_funding_history(symbol, start, end)
    except Exception:  # noqa: BLE001
        logger.exception(
            "Failed to fetch funding history for %s:%s", connector.name, symbol
        )
        return
    if not funding:
        return
    models: List[FundingRateUpsert] = []
    for entry in funding:
        rate = _to_decimal(entry.rate)
        if rate is None:
            continue
        models.append(
            FundingRateUpsert(
                contract_id=synced.contract_id,
                timestamp=_normalize_timestamp(entry.timestamp),
                rate=rate,
                interval=str(entry.interval),
            )
        )
    async with get_session() as session:
        await upsert_funding_rates(session, models)
        await session.commit()


def _normalize_timestamp(ts: datetime | float) -> datetime:
    if isinstance(ts, datetime):
        if ts.tzinfo is None:
            return ts.replace(tzinfo=UTC)
        return ts.astimezone(UTC)
    return datetime.fromtimestamp(float(ts), tz=UTC)


async def periodic_history_sync(
    connectors: Sequence[ConnectorSpec],
    *,
    interval: timedelta = _PERIODIC_SYNC_INTERVAL,
    lookback: timedelta = timedelta(days=1),
    on_summary: Callable[[DataSyncSummary], Awaitable[None]] | None = None,
) -> None:
    while True:
        try:
            states, summary = await refresh_exchange_metadata(connectors)
            if states:
                if on_summary is not None:
                    await on_summary(summary)
                await _update_contract_taker_fees(connectors, states)
                end = datetime.now(tz=UTC)
                start = end - lookback
                await _ingest_history(
                    connectors, states, start=start, end=end, interval=_DEFAULT_INTERVAL
                )
        except asyncio.CancelledError:  # pragma: no cover - shutdown handling
            raise
        except Exception:  # noqa: BLE001
            logger.exception("Periodic history synchronization failed")
        await asyncio.sleep(interval.total_seconds())
