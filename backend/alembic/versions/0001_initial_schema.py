"""initial schema

Revision ID: 0001_initial_schema
Revises: 
Create Date: 2024-10-11 00:00:00
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0001_initial_schema"
down_revision = None
branch_labels = None
depends_on = None


contract_type_enum = sa.Enum("perp", "spot", name="contract_type")
order_book_side_enum = sa.Enum("bid", "ask", name="order_book_side")


def upgrade() -> None:
    contract_type_enum.create(op.get_bind(), checkfirst=True)
    order_book_side_enum.create(op.get_bind(), checkfirst=True)

    op.create_table(
        "exchanges",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("name", sa.String(length=100), nullable=False, unique=True),
        sa.Column("taker_fee", sa.Numeric(10, 6), nullable=True),
        sa.Column("maker_fee", sa.Numeric(10, 6), nullable=True),
    )

    op.create_table(
        "contracts",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("exchange_id", sa.Integer(), sa.ForeignKey("exchanges.id", ondelete="CASCADE"), nullable=False),
        sa.Column("original_name", sa.String(length=200), nullable=False),
        sa.Column("normalized_name", sa.String(length=200), nullable=False),
        sa.Column("base_asset", sa.String(length=50), nullable=False),
        sa.Column("quote_asset", sa.String(length=50), nullable=False),
        sa.Column("type", contract_type_enum, nullable=False),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.UniqueConstraint("exchange_id", "original_name", name="uq_contract_original"),
        sa.UniqueConstraint("exchange_id", "normalized_name", name="uq_contract_normalized"),
    )

    op.create_table(
        "quotes",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("contract_id", sa.Integer(), sa.ForeignKey("contracts.id", ondelete="CASCADE"), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("bid", sa.Numeric(30, 10), nullable=False),
        sa.Column("ask", sa.Numeric(30, 10), nullable=False),
        sa.UniqueConstraint("contract_id", "timestamp", name="uq_quote_contract_timestamp"),
    )
    op.create_index(
        "ix_quotes_contract_timestamp",
        "quotes",
        ["contract_id", "timestamp"],
    )

    op.create_table(
        "funding_rates",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("contract_id", sa.Integer(), sa.ForeignKey("contracts.id", ondelete="CASCADE"), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("rate", sa.Numeric(20, 10), nullable=False),
        sa.Column("interval", sa.String(length=20), nullable=False),
        sa.UniqueConstraint("contract_id", "timestamp", "interval", name="uq_funding_rate_contract_time"),
    )

    op.create_table(
        "order_books",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("contract_id", sa.Integer(), sa.ForeignKey("contracts.id", ondelete="CASCADE"), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("side", order_book_side_enum, nullable=False),
        sa.Column("price", sa.Numeric(30, 10), nullable=False),
        sa.Column("quantity", sa.Numeric(30, 10), nullable=False),
        sa.UniqueConstraint(
            "contract_id",
            "timestamp",
            "side",
            "price",
            name="uq_order_book_entry",
        ),
    )
    op.create_index(
        "ix_order_books_contract_timestamp",
        "order_books",
        ["contract_id", "timestamp"],
    )

    op.create_table(
        "spreads",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("symbol", sa.String(length=200), nullable=False),
        sa.Column("long_exchange_id", sa.Integer(), sa.ForeignKey("exchanges.id", ondelete="CASCADE"), nullable=False),
        sa.Column("short_exchange_id", sa.Integer(), sa.ForeignKey("exchanges.id", ondelete="CASCADE"), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("entry_pct", sa.Numeric(18, 8), nullable=False),
        sa.Column("exit_pct", sa.Numeric(18, 8), nullable=False),
        sa.Column("commission_pct_total", sa.Numeric(18, 8), nullable=False),
        sa.Column("funding_spread", sa.Numeric(18, 8), nullable=True),
        sa.UniqueConstraint(
            "symbol",
            "long_exchange_id",
            "short_exchange_id",
            "timestamp",
            name="uq_spread_symbol_exchanges_timestamp",
        ),
    )
    op.create_index(
        "ix_spreads_symbol_timestamp",
        "spreads",
        ["symbol", "timestamp"],
    )


def downgrade() -> None:
    op.drop_index("ix_spreads_symbol_timestamp", table_name="spreads")
    op.drop_table("spreads")

    op.drop_index("ix_order_books_contract_timestamp", table_name="order_books")
    op.drop_table("order_books")

    op.drop_table("funding_rates")

    op.drop_index("ix_quotes_contract_timestamp", table_name="quotes")
    op.drop_table("quotes")

    op.drop_table("contracts")
    op.drop_table("exchanges")

    order_book_side_enum.drop(op.get_bind(), checkfirst=True)
    contract_type_enum.drop(op.get_bind(), checkfirst=True)
