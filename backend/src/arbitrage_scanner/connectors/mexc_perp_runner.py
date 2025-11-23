from __future__ import annotations

import asyncio

from ..store import TickerStore
from .mexc_perp_class import MexcPerpConnector


async def main() -> None:
    store = TickerStore()
    connector = MexcPerpConnector()
    try:
        await connector.run_mexc(store, [])
    except KeyboardInterrupt:
        print("Stopping MEXC connector...")


if __name__ == "__main__":
    asyncio.run(main())
