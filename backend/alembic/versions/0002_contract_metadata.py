"""Add contract metadata columns"""

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = "0002_contract_metadata"
down_revision = "0001_initial_schema"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("contracts", sa.Column("contract_size", sa.Numeric(30, 10), nullable=True))
    op.add_column("contracts", sa.Column("tick_size", sa.Numeric(30, 10), nullable=True))
    op.add_column("contracts", sa.Column("lot_size", sa.Numeric(30, 10), nullable=True))
    op.add_column("contracts", sa.Column("taker_fee", sa.Numeric(10, 6), nullable=True))
    op.add_column("contracts", sa.Column("funding_symbol", sa.String(length=200), nullable=True))


def downgrade() -> None:
    op.drop_column("contracts", "funding_symbol")
    op.drop_column("contracts", "taker_fee")
    op.drop_column("contracts", "lot_size")
    op.drop_column("contracts", "tick_size")
    op.drop_column("contracts", "contract_size")
