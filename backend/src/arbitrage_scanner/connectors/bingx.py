from __future__ import annotations

from .base import ConnectorSpec
from .bingx_perp import run_bingx

connector = ConnectorSpec(
    name="bingx",
    run=run_bingx,
    taker_fee=0.0005,
)
