"""Service responsible for bootstrapping exchange metadata and history."""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from typing import Iterable, Mapping

import httpx

from ..connectors.base import ConnectorSpec
from ..connectors.discovery import discover_symbols_for_connectors
from ..domain import ExchangeName, Symbol, Ticker
from ..exchanges.history import PriceCandle, fetch_price_history
from ..persistence import Database, normalize_symbol, split_symbol
from ..persistence.database import BBOPoint, FundingPoint
from ..settings import settings
from ..store import TickerStore
from .funding import fetch_historical_funding
from ..exchanges import fees as exchange_fees

logger = logging.getLogger(__name__)

_LOOKBACK_SECONDS = settings.history_lookback_days * 86400
_BBO_TIMEFRAME_SECONDS = 60  # 1 minute resolution for stored history


@dataclass(frozen=True)
class BootstrapResult:
    symbols_union: list[Symbol]
    per_connector: dict[ExchangeName, list[Symbol]]
    taker_fees: dict[ExchangeName, float]


class Bootstrapper:
    """Gather exchange metadata and backfill persistent storage."""

    def __init__(
        self,
        *,
        connectors: Iterable[ConnectorSpec],
        store: TickerStore,
        database: Database,
    ) -> None:
        self._connectors = list(connectors)
        self._store = store
        self._database = database

    async def run(self) -> BootstrapResult:
        discovery = await discover_symbols_for_connectors(tuple(self._connectors))
        symbols_union = list(discovery.symbols_union or [])
        per_connector = {ex: list(symbols) for ex, symbols in discovery.per_connector.items()}

        # Ensure contracts and aliases exist in the database.
        await self._register_contracts(per_connector)

        taker_fees = await self._collect_taker_fees()
        now = int(time.time())
        for exchange, fee in taker_fees.items():
            try:
                await self._database.record_taker_fee(exchange, fee, now)
            except Exception:
                logger.exception("Failed to persist taker fee for %s", exchange)

        await self._backfill_history(per_connector)

        return BootstrapResult(
            symbols_union=symbols_union,
            per_connector=per_connector,
            taker_fees=taker_fees,
        )

    async def _register_contracts(self, per_connector: Mapping[ExchangeName, Iterable[Symbol]]) -> None:
        for exchange, symbols in per_connector.items():
            for raw in symbols:
                normalized = split_symbol(raw)
                try:
                    await self._database.ensure_contract(normalized)
                    await self._database.ensure_alias(exchange, raw)
                except Exception:
                    logger.exception("Failed to register contract alias %s %s", exchange, raw)

    async def _collect_taker_fees(self) -> dict[ExchangeName, float]:
        results: dict[ExchangeName, float] = {}
        async with httpx.AsyncClient(timeout=httpx.Timeout(settings.http_timeout)) as client:
            for connector in self._connectors:
                exchange = connector.name
                try:
                    fee = await exchange_fees.fetch_taker_fee(exchange, client=client)
                except Exception:
                    fee = None
                if fee is None and connector.taker_fee is not None:
                    fee = connector.taker_fee
                if fee is None:
                    continue
                results[exchange] = float(fee)
        return results

    async def _backfill_history(self, per_connector: Mapping[ExchangeName, Iterable[Symbol]]) -> None:
        tasks = [
            asyncio.create_task(self._backfill_exchange(exchange, symbols))
            for exchange, symbols in per_connector.items()
        ]
        if not tasks:
            return
        await asyncio.gather(*tasks)

    async def _backfill_exchange(self, exchange: ExchangeName, symbols: Iterable[Symbol]) -> None:
        client_timeout = httpx.Timeout(settings.http_timeout * 2)
        async with httpx.AsyncClient(timeout=client_timeout) as client:
            for raw_symbol in symbols:
                normalized = normalize_symbol(raw_symbol)
                if not normalized:
                    continue
                try:
                    price_candles = await fetch_price_history(
                        exchange=exchange,
                        symbol=raw_symbol,
                        timeframe_seconds=_BBO_TIMEFRAME_SECONDS,
                        lookback_seconds=_LOOKBACK_SECONDS,
                        client=client,
                    )
                except Exception:
                    logger.exception("Failed to fetch price history for %s %s", exchange, raw_symbol)
                    price_candles = []
                points = [
                    BBOPoint(ts=candle.start_ts, bid=float(candle.low), ask=float(candle.high))
                    for candle in price_candles
                ]
                try:
                    await self._database.record_bbo(exchange, raw_symbol, normalized, points)
                except Exception:
                    logger.exception("Failed to persist BBO history for %s %s", exchange, raw_symbol)

                try:
                    funding_history = await fetch_historical_funding(
                        exchange, raw_symbol, _LOOKBACK_SECONDS, client=client
                    )
                except Exception:
                    logger.exception("Failed to fetch funding history for %s %s", exchange, raw_symbol)
                    funding_history = []
                funding_points = [
                    FundingPoint(ts=item.ts, rate=item.rate, interval=item.interval)
                    for item in funding_history
                ]
                try:
                    await self._database.record_funding(exchange, raw_symbol, normalized, funding_points)
                except Exception:
                    logger.exception("Failed to persist funding history for %s %s", exchange, raw_symbol)

                # Warm up store with last known funding value if present.
                if funding_points:
                    latest = funding_points[-1]
                    self._store.upsert_funding(
                        exchange,
                        raw_symbol,
                        rate=latest.rate,
                        interval=latest.interval,
                        ts=float(latest.ts),
                    )

                if points:
                    last_point = points[-1]
                    try:
                        self._store.upsert_ticker(
                            Ticker(
                                exchange=str(exchange),
                                symbol=str(raw_symbol),
                                bid=last_point.bid,
                                ask=last_point.ask,
                                ts=float(last_point.ts),
                            )
                        )
                    except Exception:
                        logger.exception("Failed to warm up ticker for %s %s", exchange, raw_symbol)
