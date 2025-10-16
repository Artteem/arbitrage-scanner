from __future__ import annotations

from .base import ConnectorSpec
from .bingx_perp import run_bingx
from .bingx_rest import (
    get_bingx_contracts,
    get_bingx_funding_history,
    get_bingx_historical_quotes,
    get_bingx_taker_fee,
)
from .discovery import discover_bingx_usdt_perp

connector = ConnectorSpec(
    name="bingx",
    run=run_bingx,
    discover_symbols=discover_bingx_usdt_perp,
    taker_fee=0.0005,
    fetch_contracts=get_bingx_contracts,
    fetch_taker_fee=get_bingx_taker_fee,
    fetch_historical_quotes=get_bingx_historical_quotes,
    fetch_funding_history=get_bingx_funding_history,
)
