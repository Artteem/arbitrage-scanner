from __future__ import annotations

from .base import ConnectorSpec
from .discovery import discover_mexc_usdt_perp
from .mexc_perp import run_mexc

connector = ConnectorSpec(
    name="mexc",
    run=run_mexc,
    discover_symbols=discover_mexc_usdt_perp,
    taker_fee=0.0007,
)
