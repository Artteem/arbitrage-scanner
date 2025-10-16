from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import Dict, Mapping, Sequence

from ..domain import ExchangeName, Symbol
from .models import OrderBookSide
from .repositories import (
    FundingRateUpsert,
    OrderBookEntryCreate,
    QuoteCreate,
    add_order_book_entries,
    bulk_insert_quotes,
    upsert_funding_rates,
)
from .session import get_session

logger = logging.getLogger(__name__)


def _to_decimal(value: float | int) -> Decimal | None:
    try:
        return Decimal(str(value))
    except Exception:  # noqa: BLE001 - guard against invalid numeric payloads
        return None


def _to_timestamp(value: float | int | None) -> datetime:
    if value is None:
        return datetime.now(tz=UTC)
    try:
        return datetime.fromtimestamp(float(value), tz=UTC)
    except Exception:  # noqa: BLE001 - fallback to "now" if conversion fails
        return datetime.now(tz=UTC)


@dataclass(slots=True)
class OrderBookSnapshot:
    ts: float
    bids: Sequence[tuple[float, float]]
    asks: Sequence[tuple[float, float]]


class RealtimeDatabaseSink:
    """Buffers realtime market data and flushes it to the database."""

    def __init__(
        self,
        *,
        flush_interval: float = 1.0,
        max_order_book_levels: int = 50,
    ) -> None:
        self._flush_interval = flush_interval
        self._max_order_book_levels = max_order_book_levels
        self._contract_lookup: Dict[tuple[ExchangeName, Symbol], int] = {}
        self._pending_quotes: Dict[int, tuple[float, float, float]] = {}
        self._pending_funding: Dict[int, tuple[float, float, str]] = {}
        self._pending_order_books: Dict[int, OrderBookSnapshot] = {}
        self._event: asyncio.Event = asyncio.Event()
        self._task: asyncio.Task | None = None
        self._closing = False

    def start(self) -> None:
        if self._task is None or self._task.done():
            self._closing = False
            self._task = asyncio.create_task(self._worker())

    async def close(self) -> None:
        self._closing = True
        self._event.set()
        if self._task is not None:
            await self._task
            self._task = None

    def set_contract_mapping(
        self, mapping: Mapping[tuple[ExchangeName, Symbol], int] | Mapping[ExchangeName, Mapping[Symbol, int]]
    ) -> None:
        lookup: Dict[tuple[ExchangeName, Symbol], int] = {}
        if mapping:
            first_value = next(iter(mapping.values()), None)
            if isinstance(first_value, Mapping):  # nested mapping from summary
                for exchange, symbols in mapping.items():
                    if not isinstance(symbols, Mapping):
                        continue
                    for symbol, contract_id in symbols.items():
                        lookup[(str(exchange).lower(), str(symbol).upper())] = contract_id
            else:
                for (exchange, symbol), contract_id in mapping.items():
                    lookup[(str(exchange).lower(), str(symbol).upper())] = contract_id
        self._contract_lookup = lookup

    def submit_ticker(self, exchange: ExchangeName, symbol: Symbol, bid: float, ask: float, ts: float) -> None:
        contract_id = self._contract_lookup.get((exchange.lower(), symbol.upper()))
        if contract_id is None:
            return
        self._pending_quotes[contract_id] = (ts, bid, ask)
        self._event.set()

    def submit_funding(
        self, exchange: ExchangeName, symbol: Symbol, rate: float, interval: str, ts: float
    ) -> None:
        contract_id = self._contract_lookup.get((exchange.lower(), symbol.upper()))
        if contract_id is None:
            return
        self._pending_funding[contract_id] = (ts, rate, interval)
        self._event.set()

    def submit_order_book(
        self,
        exchange: ExchangeName,
        symbol: Symbol,
        *,
        bids: Sequence[tuple[float, float]] | None,
        asks: Sequence[tuple[float, float]] | None,
        ts: float,
    ) -> None:
        contract_id = self._contract_lookup.get((exchange.lower(), symbol.upper()))
        if contract_id is None:
            return
        snapshot = OrderBookSnapshot(
            ts=ts,
            bids=list(bids[: self._max_order_book_levels]) if bids else [],
            asks=list(asks[: self._max_order_book_levels]) if asks else [],
        )
        if not snapshot.bids and not snapshot.asks:
            return
        self._pending_order_books[contract_id] = snapshot
        self._event.set()

    async def _worker(self) -> None:
        while not self._closing:
            try:
                await asyncio.wait_for(self._event.wait(), timeout=self._flush_interval)
            except asyncio.TimeoutError:
                pass
            self._event.clear()
            await self._flush_once()
        # Final flush on shutdown
        await self._flush_once()

    async def _flush_once(self) -> None:
        if not (self._pending_quotes or self._pending_funding or self._pending_order_books):
            return

        quotes_buffer, self._pending_quotes = self._pending_quotes, {}
        funding_buffer, self._pending_funding = self._pending_funding, {}
        order_book_buffer, self._pending_order_books = self._pending_order_books, {}

        quote_models: list[QuoteCreate] = []
        for contract_id, (ts, bid, ask) in quotes_buffer.items():
            bid_decimal = _to_decimal(bid)
            ask_decimal = _to_decimal(ask)
            if bid_decimal is None or ask_decimal is None:
                continue
            quote_models.append(
                QuoteCreate(
                    contract_id=contract_id,
                    timestamp=_to_timestamp(ts),
                    bid=bid_decimal,
                    ask=ask_decimal,
                )
            )

        funding_models: list[FundingRateUpsert] = []
        for contract_id, (ts, rate, interval) in funding_buffer.items():
            rate_decimal = _to_decimal(rate)
            if rate_decimal is None:
                continue
            funding_models.append(
                FundingRateUpsert(
                    contract_id=contract_id,
                    timestamp=_to_timestamp(ts),
                    rate=rate_decimal,
                    interval=str(interval),
                )
            )

        order_book_models: list[OrderBookEntryCreate] = []
        for contract_id, snapshot in order_book_buffer.items():
            ts_dt = _to_timestamp(snapshot.ts)
            for price, qty in snapshot.bids[: self._max_order_book_levels]:
                price_decimal = _to_decimal(price)
                qty_decimal = _to_decimal(qty)
                if price_decimal is None or qty_decimal is None:
                    continue
                order_book_models.append(
                    OrderBookEntryCreate(
                        contract_id=contract_id,
                        timestamp=ts_dt,
                        side=OrderBookSide.BID,
                        price=price_decimal,
                        quantity=qty_decimal,
                    )
                )
            for price, qty in snapshot.asks[: self._max_order_book_levels]:
                price_decimal = _to_decimal(price)
                qty_decimal = _to_decimal(qty)
                if price_decimal is None or qty_decimal is None:
                    continue
                order_book_models.append(
                    OrderBookEntryCreate(
                        contract_id=contract_id,
                        timestamp=ts_dt,
                        side=OrderBookSide.ASK,
                        price=price_decimal,
                        quantity=qty_decimal,
                    )
                )

        if not (quote_models or funding_models or order_book_models):
            return

        try:
            async with get_session() as session:
                if quote_models:
                    await bulk_insert_quotes(session, quote_models)
                if funding_models:
                    await upsert_funding_rates(session, funding_models)
                if order_book_models:
                    await add_order_book_entries(session, order_book_models)
                await session.commit()
        except Exception:  # noqa: BLE001 - defensive logging, do not crash worker
            logger.exception("Failed to flush realtime market data")
