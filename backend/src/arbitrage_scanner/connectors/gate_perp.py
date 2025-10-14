from __future__ import annotations

import asyncio
import time
from typing import Iterable, Sequence

import httpx

from ..domain import Symbol, Ticker
from ..store import TickerStore
from .discovery import GATE_HEADERS, discover_gate_usdt_perp

TICKERS_URL = "https://api.gateio.ws/api/v4/futures/usdt/tickers"
POLL_INTERVAL = 1.5
MIN_SYMBOL_THRESHOLD = 5
FALLBACK_SYMBOLS: tuple[Symbol, ...] = ("BTCUSDT", "ETHUSDT", "SOLUSDT")


def _as_float(value) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _to_gate_symbol(symbol: Symbol) -> str:
    sym = str(symbol).upper()
    if "_" in sym:
        return sym
    if sym.endswith("USDT"):
        return f"{sym[:-4]}_USDT"
    return sym


def _from_gate_symbol(symbol: str | None) -> Symbol | None:
    if not symbol:
        return None
    return str(symbol).replace("-", "").replace("_", "")


def _extract_price(item: dict, keys: Iterable[str]) -> float:
    for key in keys:
        val = item.get(key)
        price = _as_float(val)
        if price > 0:
            return price
    return 0.0


def _is_active_contract(item: dict) -> bool:
    state = str(item.get("state") or item.get("status") or "").strip().lower()
    if state and state not in {"open", "trading", "live"}:
        return False
    if bool(item.get("is_delisted")):
        return False
    in_delisting = item.get("in_delisting")
    if isinstance(in_delisting, str):
        if in_delisting.strip().lower() in {"true", "1"}:
            return False
    elif in_delisting:
        return False
    return True


async def run_gate(store: TickerStore, symbols: Sequence[Symbol]) -> None:
    subscribe = [sym for sym in dict.fromkeys(symbols) if sym]

    if len(subscribe) < MIN_SYMBOL_THRESHOLD:
        try:
            discovered = await discover_gate_usdt_perp()
        except Exception:
            discovered = set()
        if discovered:
            subscribe = sorted(discovered)
        else:
            subscribe = list(FALLBACK_SYMBOLS)

    if not subscribe:
        return

    await _poll_gate_http(store, subscribe)


async def _poll_gate_http(store: TickerStore, symbols: Sequence[Symbol]) -> None:
    wanted = {_to_gate_symbol(sym) for sym in symbols if sym}
    async with httpx.AsyncClient(timeout=15, headers=GATE_HEADERS) as client:
        delay = POLL_INTERVAL
        while True:
            now = time.time()
            try:
                response = await client.get(TICKERS_URL)
                response.raise_for_status()
                payload = response.json()
            except asyncio.CancelledError:
                raise
            except Exception:
                await asyncio.sleep(min(delay, 5.0))
                delay = min(delay * 2, 30.0)
                continue

            delay = POLL_INTERVAL
            items = _extract_items(payload)
            if not items:
                await asyncio.sleep(POLL_INTERVAL)
                continue

            for item in items:
                if not isinstance(item, dict):
                    continue

                contract = item.get("contract") or item.get("name") or item.get("symbol")
                if not contract:
                    continue

                contract_str = str(contract)
                if contract_str.count("_") > 1:
                    continue

                if wanted and contract_str not in wanted:
                    continue

                if not _is_active_contract(item):
                    continue

                sym_common = _from_gate_symbol(contract_str)
                if not sym_common:
                    continue

                bid = _extract_price(
                    item,
                    ("best_bid_price", "highest_bid", "bid1", "best_bid"),
                )
                ask = _extract_price(
                    item,
                    ("best_ask_price", "lowest_ask", "ask1", "best_ask"),
                )
                if bid <= 0 or ask <= 0:
                    continue

                store.upsert_ticker(
                    Ticker(exchange="gate", symbol=sym_common, bid=bid, ask=ask, ts=now)
                )

                rate_raw = (
                    item.get("funding_rate")
                    or item.get("funding_rate_indicative")
                    or item.get("next_funding_rate")
                )
                if rate_raw is not None:
                    rate = _as_float(rate_raw)
                    store.upsert_funding("gate", sym_common, rate=rate, interval="8h", ts=now)

                last_raw = item.get("last") or item.get("last_price") or item.get("mark_price")
                last_price = _as_float(last_raw)
                if last_price > 0:
                    store.upsert_order_book(
                        "gate", sym_common, last_price=last_price, last_price_ts=now
                    )

            await asyncio.sleep(POLL_INTERVAL)


def _extract_items(payload) -> list[dict]:
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        for key in ("tickers", "data", "items", "result"):
            value = payload.get(key)
            if isinstance(value, list):
                return value
    return []
