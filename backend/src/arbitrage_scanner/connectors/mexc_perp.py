from __future__ import annotations

import asyncio
import time
from typing import Sequence

import httpx

from ..domain import Symbol, Ticker
from ..store import TickerStore

MIN_SYMBOL_THRESHOLD = 5

TICKERS_URL = "https://contract.mexc.com/api/v1/contract/ticker"
FUNDING_URL = "https://contract.mexc.com/api/v1/contract/funding_rate"
POLL_INTERVAL = 1.5


def _as_float(value) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _to_mexc_symbol(symbol: Symbol) -> str:
    if "_" in symbol:
        return symbol
    if symbol.endswith("USDT"):
        return f"{symbol[:-4]}_USDT"
    return symbol


def _from_mexc_symbol(symbol: str | None) -> Symbol | None:
    if not symbol:
        return None
    return symbol.replace("_", "")


def _extract_bid(item) -> float:
    for key in ("bid1", "bestBidPrice", "bestBid"):
        val = item.get(key)
        bid = _as_float(val)
        if bid > 0:
            return bid
    return 0.0


def _extract_ask(item) -> float:
    for key in ("ask1", "bestAskPrice", "bestAsk"):
        val = item.get(key)
        ask = _as_float(val)
        if ask > 0:
            return ask
    return 0.0


def _parse_interval(item) -> str:
    interval = item.get("fundingInterval") or item.get("interval")
    if isinstance(interval, (int, float)):
        # значение в часах
        return f"{interval}h"
    if isinstance(interval, str) and interval:
        return interval
    return "8h"


async def run_mexc(store: TickerStore, symbols: Sequence[Symbol]):
    if not symbols:
        return

    wanted = {_to_mexc_symbol(sym) for sym in symbols}
    if len(wanted) < MIN_SYMBOL_THRESHOLD:
        wanted = set()

    async with httpx.AsyncClient(timeout=15) as client:
        while True:
            now = time.time()
            try:
                ticker_resp = await client.get(TICKERS_URL)
                ticker_resp.raise_for_status()
                ticker_data = ticker_resp.json().get("data", [])
            except Exception:
                await asyncio.sleep(2.0)
                continue

            funding_map: dict[str, tuple[float, str]] = {}
            try:
                funding_resp = await client.get(FUNDING_URL)
                funding_resp.raise_for_status()
                funding_items = funding_resp.json().get("data", [])
                for item in funding_items:
                    sym_raw = item.get("symbol")
                    if wanted and sym_raw not in wanted:
                        continue
                    rate = _as_float(item.get("fundingRate") or item.get("rate"))
                    interval = _parse_interval(item)
                    funding_map[sym_raw] = (rate, interval)
            except Exception:
                funding_map = {}

            for item in ticker_data:
                sym_raw = item.get("symbol")
                if wanted and sym_raw not in wanted:
                    continue

                bid = _extract_bid(item)
                ask = _extract_ask(item)
                if bid <= 0 or ask <= 0:
                    continue

                sym_common = _from_mexc_symbol(sym_raw)
                if not sym_common:
                    continue

                store.upsert_ticker(
                    Ticker(
                        exchange="mexc",
                        symbol=sym_common,
                        bid=bid,
                        ask=ask,
                        ts=now,
                    )
                )

                if sym_raw in funding_map:
                    rate, interval = funding_map[sym_raw]
                    store.upsert_funding("mexc", sym_common, rate=rate, interval=interval, ts=now)

            await asyncio.sleep(POLL_INTERVAL)
