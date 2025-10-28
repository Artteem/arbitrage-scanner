from __future__ import annotations

from .base import ConnectorSpec
from .bybit_perp import run_bybit
from .bybit_rest import (
    get_bybit_contracts,
    get_bybit_funding_history,
    get_bybit_historical_quotes,
    get_bybit_taker_fee,
)
from .discovery import discover_bybit_linear_usdt

connector = ConnectorSpec(
    name="bybit",
    run=run_bybit,
    discover_symbols=discover_bybit_linear_usdt,
    taker_fee=0.0006,
    fetch_contracts=get_bybit_contracts,
    fetch_taker_fee=get_bybit_taker_fee,
    fetch_historical_quotes=get_bybit_historical_quotes,
    fetch_funding_history=get_bybit_funding_history,
)
