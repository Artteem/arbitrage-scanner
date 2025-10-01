from __future__ import annotations

from .base import ConnectorSpec
from .bigx_perp import run_bigx

connector = ConnectorSpec(
    name="bigx",
    run=run_bigx,
    taker_fee=0.0005,
)
