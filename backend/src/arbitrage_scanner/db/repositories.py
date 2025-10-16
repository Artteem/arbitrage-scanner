from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

from sqlalchemy import Select, func, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from .models import (
    FundingRate,
    OrderBookEntry,
    OrderBookSide,
    Quote,
    Spread,
)


@dataclass(slots=True)
class QuoteCreate:
    contract_id: int
    timestamp: datetime
    bid: Decimal
    ask: Decimal


@dataclass(slots=True)
class FundingRateUpsert:
    contract_id: int
    timestamp: datetime
    rate: Decimal
    interval: str


@dataclass(slots=True)
class OrderBookEntryCreate:
    contract_id: int
    timestamp: datetime
    side: OrderBookSide
    price: Decimal
    quantity: Decimal


@dataclass(slots=True)
class SpreadCreate:
    symbol: str
    long_exchange_id: int
    short_exchange_id: int
    timestamp: datetime
    entry_pct: Decimal
    exit_pct: Decimal
    commission_pct_total: Decimal
    funding_spread: Optional[Decimal]


async def bulk_insert_quotes(session: AsyncSession, quotes: Sequence[QuoteCreate]) -> None:
    if not quotes:
        return

    stmt = insert(Quote).values(
        [
            {
                "contract_id": quote.contract_id,
                "timestamp": quote.timestamp,
                "bid": quote.bid,
                "ask": quote.ask,
            }
            for quote in quotes
        ]
    )
    stmt = stmt.on_conflict_do_update(
        constraint="uq_quote_contract_timestamp",
        set_={"bid": stmt.excluded.bid, "ask": stmt.excluded.ask},
    )
    await session.execute(stmt)


async def upsert_funding_rates(
    session: AsyncSession, funding_rates: Sequence[FundingRateUpsert]
) -> None:
    if not funding_rates:
        return

    stmt = insert(FundingRate).values(
        [
            {
                "contract_id": funding_rate.contract_id,
                "timestamp": funding_rate.timestamp,
                "rate": funding_rate.rate,
                "interval": funding_rate.interval,
            }
            for funding_rate in funding_rates
        ]
    )
    stmt = stmt.on_conflict_do_update(
        constraint="uq_funding_rate_contract_time",
        set_={"rate": stmt.excluded.rate},
    )
    await session.execute(stmt)


async def add_order_book_entries(
    session: AsyncSession, entries: Sequence[OrderBookEntryCreate]
) -> None:
    if not entries:
        return

    stmt = insert(OrderBookEntry).values(
        [
            {
                "contract_id": entry.contract_id,
                "timestamp": entry.timestamp,
                "side": entry.side,
                "price": entry.price,
                "quantity": entry.quantity,
            }
            for entry in entries
        ]
    )
    stmt = stmt.on_conflict_do_update(
        constraint="uq_order_book_entry",
        set_={"quantity": stmt.excluded.quantity},
    )
    await session.execute(stmt)


async def record_spreads(session: AsyncSession, spreads: Sequence[SpreadCreate]) -> None:
    if not spreads:
        return

    stmt = insert(Spread).values(
        [
            {
                "symbol": spread.symbol,
                "long_exchange_id": spread.long_exchange_id,
                "short_exchange_id": spread.short_exchange_id,
                "timestamp": spread.timestamp,
                "entry_pct": spread.entry_pct,
                "exit_pct": spread.exit_pct,
                "commission_pct_total": spread.commission_pct_total,
                "funding_spread": spread.funding_spread,
            }
            for spread in spreads
        ]
    )
    stmt = stmt.on_conflict_do_update(
        constraint="uq_spread_symbol_exchanges_timestamp",
        set_={
            "entry_pct": stmt.excluded.entry_pct,
            "exit_pct": stmt.excluded.exit_pct,
            "commission_pct_total": stmt.excluded.commission_pct_total,
            "funding_spread": stmt.excluded.funding_spread,
        },
    )
    await session.execute(stmt)


async def get_quotes_history(
    session: AsyncSession,
    contract_id: int,
    start: datetime,
    end: datetime,
) -> List[Quote]:
    stmt: Select[Quote] = (
        select(Quote)
        .where(Quote.contract_id == contract_id)
        .where(Quote.timestamp >= start)
        .where(Quote.timestamp <= end)
        .order_by(Quote.timestamp)
    )
    result = await session.scalars(stmt)
    return list(result)


async def get_latest_quotes(
    session: AsyncSession, contract_ids: Iterable[int]
) -> Dict[int, Quote]:
    ids = list(contract_ids)
    if not ids:
        return {}

    stmt: Select[Quote] = (
        select(Quote)
        .where(Quote.contract_id.in_(ids))
        .distinct(Quote.contract_id)
        .order_by(Quote.contract_id, Quote.timestamp.desc())
    )
    result = await session.scalars(stmt)
    quotes = result.all()
    return {quote.contract_id: quote for quote in quotes}


async def get_recent_spreads(
    session: AsyncSession, symbol: str, limit: int = 50
) -> List[Spread]:
    stmt: Select[Spread] = (
        select(Spread)
        .where(Spread.symbol == symbol)
        .order_by(Spread.timestamp.desc())
        .limit(limit)
    )
    result = await session.scalars(stmt)
    return list(result)


async def get_order_book_depth(
    session: AsyncSession,
    contract_id: int,
    side: OrderBookSide,
    target_quantity: Decimal,
) -> Tuple[List[OrderBookEntry], Decimal]:
    latest_timestamp = (
        select(func.max(OrderBookEntry.timestamp))
        .where(OrderBookEntry.contract_id == contract_id)
        .scalar_subquery()
    )

    order_column = (
        OrderBookEntry.price.desc() if side == OrderBookSide.BID else OrderBookEntry.price.asc()
    )

    stmt: Select[OrderBookEntry] = (
        select(OrderBookEntry)
        .where(OrderBookEntry.contract_id == contract_id)
        .where(OrderBookEntry.timestamp == latest_timestamp)
        .where(OrderBookEntry.side == side)
        .order_by(order_column)
    )
    result = await session.scalars(stmt)
    rows = list(result)

    depth: List[OrderBookEntry] = []
    accumulated = Decimal("0")
    for row in rows:
        depth.append(row)
        accumulated += row.quantity
        if accumulated >= target_quantity:
            break

    return depth, accumulated
