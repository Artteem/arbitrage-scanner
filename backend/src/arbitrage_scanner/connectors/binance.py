from __future__ import annotations

from .base import ConnectorSpec
from .binance_futures import run_binance
from .discovery import discover_binance_usdt_perp

connector = ConnectorSpec(
    name="binance",
    run=run_binance,
    discover_symbols=discover_binance_usdt_perp,
    taker_fee=0.0005,
)
