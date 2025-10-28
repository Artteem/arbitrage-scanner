from __future__ import annotations

from .base import ConnectorSpec
from .discovery import discover_mexc_usdt_perp
from .mexc_perp import run_mexc
from .mexc_rest import (
    get_mexc_contracts,
    get_mexc_funding_history,
    get_mexc_historical_quotes,
    get_mexc_taker_fee,
)


connector = ConnectorSpec(
    name="mexc",
    run=run_mexc,
    discover_symbols=discover_mexc_usdt_perp,

    taker_fee=0.0006,
    fetch_contracts=get_mexc_contracts,
    fetch_taker_fee=get_mexc_taker_fee,
    fetch_historical_quotes=get_mexc_historical_quotes,
    fetch_funding_history=get_mexc_funding_history,

)
