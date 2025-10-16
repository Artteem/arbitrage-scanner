from __future__ import annotations

from .base import ConnectorSpec
from .gate_perp import run_gate
from .discovery import discover_gate_usdt_perp
from .gate_rest import (
    get_gate_contracts,
    get_gate_funding_history,
    get_gate_historical_quotes,
    get_gate_taker_fee,
)

connector = ConnectorSpec(
    name="gate",
    run=run_gate,
    discover_symbols=discover_gate_usdt_perp,
    taker_fee=0.0005,
    fetch_contracts=get_gate_contracts,
    fetch_taker_fee=get_gate_taker_fee,
    fetch_historical_quotes=get_gate_historical_quotes,
    fetch_funding_history=get_gate_funding_history,
)
