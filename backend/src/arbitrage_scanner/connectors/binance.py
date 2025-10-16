from __future__ import annotations

from .base import ConnectorSpec
from .binance_futures import run_binance
from .binance_rest import (
    get_binance_contracts,
    get_binance_funding_history,
    get_binance_historical_quotes,
    get_binance_taker_fee,
)
from .discovery import discover_binance_usdt_perp

connector = ConnectorSpec(
    name="binance",
    run=run_binance,
    discover_symbols=discover_binance_usdt_perp,
    taker_fee=0.0005,
    fetch_contracts=get_binance_contracts,
    fetch_taker_fee=get_binance_taker_fee,
    fetch_historical_quotes=get_binance_historical_quotes,
    fetch_funding_history=get_binance_funding_history,
)
