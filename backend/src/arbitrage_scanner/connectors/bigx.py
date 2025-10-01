from __future__ import annotations

"""Backward-compatible alias for the renamed BingX connector."""

from .base import ConnectorSpec
from .bingx_perp import run_bingx

connector = ConnectorSpec(
    name="bingx",
    run=run_bingx,
    taker_fee=0.0005,
)
