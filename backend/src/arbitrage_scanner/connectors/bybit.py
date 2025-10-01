from __future__ import annotations

from .base import ConnectorSpec
from .bybit_perp import run_bybit
from .discovery import discover_bybit_linear_usdt

connector = ConnectorSpec(
    name="bybit",
    run=run_bybit,
    discover_symbols=discover_bybit_linear_usdt,
    taker_fee=0.0006,
)
