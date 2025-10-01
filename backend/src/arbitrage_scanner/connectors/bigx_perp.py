from __future__ import annotations

import asyncio
import time
from typing import Iterable, Sequence

import httpx

from ..domain import Symbol, Ticker
from ..store import TickerStore

TICKERS_URL = "https://api.bigx.com/v1/public/market/tickers"
POLL_INTERVAL = 1.5


def _extract_price(item: dict, keys: Iterable[str]) -> float:
    for key in keys:
        val = item.get(key)
        if val is None:
            continue
        try:
            price = float(val)
        except (TypeError, ValueError):
            continue
        if price > 0:
            return price
    return 0.0


def _to_bigx_symbol(symbol: Symbol) -> str:
    sym = str(symbol).upper()
    if "-" in sym:
        sym = sym.replace("-", "_")
    if "_" in sym:
        return sym
    if sym.endswith("USDT"):
        base = sym[:-4]
        return f"{base}_USDT"
    return sym


def _from_bigx_symbol(symbol: str | None) -> Symbol | None:
    if not symbol:
        return None
    return symbol.replace("-", "").replace("_", "").upper()


async def run_bigx(store: TickerStore, symbols: Sequence[Symbol]) -> None:
    if not symbols:
        return

    wanted_common = {str(sym).upper() for sym in symbols}
    wanted_exchange = {_to_bigx_symbol(sym) for sym in wanted_common}

    params: dict[str, str] | None = {"symbols": ",".join(sorted(wanted_exchange))} if wanted_exchange else None

    async with httpx.AsyncClient(timeout=15) as client:
        while True:
            now = time.time()
            try:
                response = await client.get(TICKERS_URL, params=params)
                response.raise_for_status()
                data = response.json()
            except httpx.HTTPStatusError:
                if params is not None:
                    params = None
                    await asyncio.sleep(1.5)
                    continue
                await asyncio.sleep(2.0)
                continue
            except Exception:
                await asyncio.sleep(2.0)
                continue

            items: Iterable[dict] = []
            if isinstance(data, dict):
                for key in ("data", "result", "tickers"):
                    value = data.get(key)
                    if isinstance(value, list):
                        items = value
                        break
                    if isinstance(value, dict):
                        items = value.values()
                        break
                else:
                    value = data.get("items") if isinstance(data, dict) else None
                    if isinstance(value, list):
                        items = value
                    elif isinstance(value, dict):
                        items = value.values()
                    else:
                        items = list(data.values()) if isinstance(data, dict) else []
            elif isinstance(data, list):
                items = data

            for raw in items:
                if not isinstance(raw, dict):
                    continue

                raw_symbol = raw.get("symbol") or raw.get("market") or raw.get("instId")
                if not raw_symbol:
                    continue

                normalized_exchange_symbol = _to_bigx_symbol(raw_symbol)
                if normalized_exchange_symbol not in wanted_exchange:
                    normalized_common = _from_bigx_symbol(raw_symbol)
                    if not normalized_common or normalized_common not in wanted_common:
                        continue
                    normalized_exchange_symbol = _to_bigx_symbol(normalized_common)

                bid = _extract_price(raw, ("bestBid", "bestBidPrice", "bid", "bidPrice", "bid1"))
                ask = _extract_price(raw, ("bestAsk", "bestAskPrice", "ask", "askPrice", "ask1"))
                if bid <= 0 or ask <= 0:
                    continue

                common_symbol = _from_bigx_symbol(normalized_exchange_symbol)
                if not common_symbol or common_symbol not in wanted_common:
                    continue

                store.upsert_ticker(
                    Ticker(
                        exchange="bigx",
                        symbol=common_symbol,
                        bid=bid,
                        ask=ask,
                        ts=now,
                    )
                )

            await asyncio.sleep(POLL_INTERVAL)
