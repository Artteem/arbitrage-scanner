from __future__ import annotations

from .base import ConnectorSpec
from .gate_perp import run_gate
from .discovery import discover_gate_usdt_perp

connector = ConnectorSpec(
    name="gate",
    run=run_gate,
    discover_symbols=discover_gate_usdt_perp,
    taker_fee=0.0005,
)
