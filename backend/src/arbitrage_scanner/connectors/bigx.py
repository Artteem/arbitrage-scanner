from __future__ import annotations

"""Backward-compatible alias for the renamed BingX connector."""

from .base import ConnectorSpec
from .bingx_perp import run_bingx
from .discovery import discover_bingx_usdt_perp

connector = ConnectorSpec(
    name="bingx",
    run=run_bingx,
    discover_symbols=discover_bingx_usdt_perp,
    taker_fee=0.0005,
)
