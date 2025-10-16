from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import List, Optional

from sqlalchemy import (
    Boolean,
    DateTime,
    Enum as SQLEnum,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    String,
    UniqueConstraint,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


class ContractType(str, Enum):
    PERPETUAL = "perp"
    SPOT = "spot"


class OrderBookSide(str, Enum):
    BID = "bid"
    ASK = "ask"


class Exchange(Base):
    __tablename__ = "exchanges"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(100), unique=True, nullable=False)
    taker_fee: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 6))
    maker_fee: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 6))

    contracts: Mapped[List["Contract"]] = relationship(back_populates="exchange")


class Contract(Base):
    __tablename__ = "contracts"
    __table_args__ = (
        UniqueConstraint("exchange_id", "original_name", name="uq_contract_original"),
        UniqueConstraint("exchange_id", "normalized_name", name="uq_contract_normalized"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    exchange_id: Mapped[int] = mapped_column(ForeignKey("exchanges.id", ondelete="CASCADE"), nullable=False)
    original_name: Mapped[str] = mapped_column(String(200), nullable=False)
    normalized_name: Mapped[str] = mapped_column(String(200), nullable=False)
    base_asset: Mapped[str] = mapped_column(String(50), nullable=False)
    quote_asset: Mapped[str] = mapped_column(String(50), nullable=False)
    type: Mapped[ContractType] = mapped_column(
        SQLEnum(ContractType, name="contract_type"), nullable=False
    )
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    contract_size: Mapped[Optional[Decimal]] = mapped_column(Numeric(30, 10))
    tick_size: Mapped[Optional[Decimal]] = mapped_column(Numeric(30, 10))
    lot_size: Mapped[Optional[Decimal]] = mapped_column(Numeric(30, 10))
    taker_fee: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 6))
    funding_symbol: Mapped[Optional[str]] = mapped_column(String(200))

    exchange: Mapped[Exchange] = relationship(back_populates="contracts")
    quotes: Mapped[List["Quote"]] = relationship(back_populates="contract", cascade="all, delete-orphan")
    funding_rates: Mapped[List["FundingRate"]] = relationship(back_populates="contract", cascade="all, delete-orphan")
    order_books: Mapped[List["OrderBookEntry"]] = relationship(back_populates="contract", cascade="all, delete-orphan")


class Quote(Base):
    __tablename__ = "quotes"
    __table_args__ = (
        Index("ix_quotes_contract_timestamp", "contract_id", "timestamp"),
        UniqueConstraint("contract_id", "timestamp", name="uq_quote_contract_timestamp"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    contract_id: Mapped[int] = mapped_column(ForeignKey("contracts.id", ondelete="CASCADE"), nullable=False)
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    bid: Mapped[Decimal] = mapped_column(Numeric(30, 10), nullable=False)
    ask: Mapped[Decimal] = mapped_column(Numeric(30, 10), nullable=False)

    contract: Mapped[Contract] = relationship(back_populates="quotes")


class FundingRate(Base):
    __tablename__ = "funding_rates"
    __table_args__ = (
        UniqueConstraint("contract_id", "timestamp", "interval", name="uq_funding_rate_contract_time"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    contract_id: Mapped[int] = mapped_column(ForeignKey("contracts.id", ondelete="CASCADE"), nullable=False)
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    rate: Mapped[Decimal] = mapped_column(Numeric(20, 10), nullable=False)
    interval: Mapped[str] = mapped_column(String(20), nullable=False)

    contract: Mapped[Contract] = relationship(back_populates="funding_rates")


class OrderBookEntry(Base):
    __tablename__ = "order_books"
    __table_args__ = (
        UniqueConstraint(
            "contract_id",
            "timestamp",
            "side",
            "price",
            name="uq_order_book_entry",
        ),
        Index("ix_order_books_contract_timestamp", "contract_id", "timestamp"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    contract_id: Mapped[int] = mapped_column(ForeignKey("contracts.id", ondelete="CASCADE"), nullable=False)
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    side: Mapped[OrderBookSide] = mapped_column(SQLEnum(OrderBookSide, name="order_book_side"), nullable=False)
    price: Mapped[Decimal] = mapped_column(Numeric(30, 10), nullable=False)
    quantity: Mapped[Decimal] = mapped_column(Numeric(30, 10), nullable=False)

    contract: Mapped[Contract] = relationship(back_populates="order_books")


class Spread(Base):
    __tablename__ = "spreads"
    __table_args__ = (
        Index(
            "ix_spreads_symbol_timestamp",
            "symbol",
            "timestamp",
        ),
        UniqueConstraint(
            "symbol",
            "long_exchange_id",
            "short_exchange_id",
            "timestamp",
            name="uq_spread_symbol_exchanges_timestamp",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    symbol: Mapped[str] = mapped_column(String(200), nullable=False)
    long_exchange_id: Mapped[int] = mapped_column(ForeignKey("exchanges.id", ondelete="CASCADE"), nullable=False)
    short_exchange_id: Mapped[int] = mapped_column(ForeignKey("exchanges.id", ondelete="CASCADE"), nullable=False)
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    entry_pct: Mapped[Decimal] = mapped_column(Numeric(18, 8), nullable=False)
    exit_pct: Mapped[Decimal] = mapped_column(Numeric(18, 8), nullable=False)
    commission_pct_total: Mapped[Decimal] = mapped_column(Numeric(18, 8), nullable=False)
    funding_spread: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 8), nullable=True)

    long_exchange: Mapped[Exchange] = relationship(foreign_keys=[long_exchange_id])
    short_exchange: Mapped[Exchange] = relationship(foreign_keys=[short_exchange_id])
