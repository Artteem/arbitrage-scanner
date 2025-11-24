from __future__ import annotations

import asyncio
import logging
import time
from typing import Iterable, List, Sequence, Tuple

import gate_api

from ..domain import Symbol, Ticker
from ..store import TickerStore
from .credentials import get_credentials_provider

logger = logging.getLogger(__name__)

POLL_INTERVAL = 1.5  # seconds between REST polls
ORDER_BOOK_DEPTH = 50


async def run_gate(store: TickerStore, symbols: Sequence[Symbol]) -> None:
    """
    Simple Gate.io USDT‑perp connector using the official gateapi-python package.
    Polls tickers and order books over REST and upserts into the shared store.
    """
    api = _create_api_client()
    contracts = await _resolve_contracts(api, symbols)
    if not contracts:
        logger.warning("Gate connector: no USDT perpetual contracts resolved")
        return

    logger.info("Gate connector starting", extra={"contracts": contracts})
    while True:
        start = time.time()
        for contract in contracts:
            await _process_contract(api, store, contract)
        elapsed = time.time() - start
        await asyncio.sleep(max(POLL_INTERVAL - elapsed, 0.0))


def _create_api_client() -> gate_api.FuturesApi:
    cfg = gate_api.Configuration(host="https://api.gateio.ws/api/v4")
    provider = get_credentials_provider()
    creds = provider.get("gate") if provider else None
    if creds:
        cfg.key = creds.api_key
        cfg.secret = creds.api_secret
    client = gate_api.ApiClient(cfg)
    return gate_api.FuturesApi(client)


async def _resolve_contracts(api: gate_api.FuturesApi, symbols: Sequence[Symbol]) -> list[str]:
    all_contracts = await asyncio.to_thread(api.list_futures_contracts, "usdt")
    available = {
        c.name: c for c in all_contracts if getattr(c, "quanto_multiplier", None) is not None
    }
    if symbols:
        desired = {_to_native(sym) for sym in symbols if sym}
        resolved = [sym for sym in desired if sym in available]
    else:
        resolved = list(available.keys())
    resolved.sort()
    return resolved


async def _process_contract(api: gate_api.FuturesApi, store: TickerStore, contract: str) -> None:
    ticker = await _fetch_ticker(api, contract)
    if ticker:
        _upsert_ticker(store, ticker)
    book = await _fetch_order_book(api, contract)
    if book:
        _upsert_order_book(store, contract, book)


async def _fetch_ticker(api: gate_api.FuturesApi, contract: str):
    try:
        tickers = await asyncio.to_thread(api.list_futures_tickers, "usdt", contract=contract)
        return tickers[0] if tickers else None
    except Exception as exc:
        logger.debug("Gate ticker fetch failed", exc_info=exc, extra={"contract": contract})
        return None


async def _fetch_order_book(api: gate_api.FuturesApi, contract: str):
    try:
        return await asyncio.to_thread(
            api.list_futures_order_book,
            "usdt",
            contract=contract,
            limit=ORDER_BOOK_DEPTH,
        )
    except Exception as exc:
        logger.debug("Gate order book fetch failed", exc_info=exc, extra={"contract": contract})
        return None


def _upsert_ticker(store: TickerStore, ticker) -> None:
    bid = _as_float(getattr(ticker, "highest_bid", None))
    ask = _as_float(getattr(ticker, "lowest_ask", None))
    if bid <= 0 or ask <= 0:
        return
    ts = _timestamp(getattr(ticker, "time", None) or getattr(ticker, "t", None))
    symbol = Symbol(str(getattr(ticker, "contract", "")).replace("_", ""))

    store.upsert_ticker(Ticker(exchange="gate", symbol=symbol, bid=bid, ask=ask, ts=ts))
    rate = _maybe_float(getattr(ticker, "funding_rate", None))
    if rate is not None:
        store.upsert_funding("gate", symbol, rate=rate, interval="8h", ts=ts)

    last_price = _maybe_float(getattr(ticker, "last", None))
    if last_price:
        store.set_last_price("gate", symbol, last_price=last_price, ts=ts)

def _upsert_order_book(store: TickerStore, contract: str, book) -> None:
    bids = _parse_levels(getattr(book, "bids", None))
    asks = _parse_levels(getattr(book, "asks", None))
    if not bids and not asks:
        return
    ts = _timestamp(getattr(book, "t", None))
    symbol = Symbol(str(contract).replace("_", ""))
    store.upsert_order_book("gate", symbol, bids=bids or None, asks=asks or None, ts=ts)


def _parse_levels(levels: Iterable) -> List[Tuple[float, float]]:
    parsed: List[Tuple[float, float]] = []
    if not levels:
        return parsed
    for entry in levels:
        try:
            price = float(entry.p)
            size = float(entry.s)
            if price > 0 and size > 0:
                parsed.append((price, size))
        except Exception as e:
            continue
    return parsed


def _to_native(symbol: Symbol) -> str:
    sym = str(symbol).strip().upper().replace("-", "_")
    if "_" not in sym and sym.endswith("USDT"):
        sym = f"{sym[:-4]}_USDT"
    return sym


def _timestamp(raw) -> float:
    try:
        val = float(raw)
        return val / 1000.0 if val > 1e12 else val
    except Exception:
        return time.time()


def _as_float(val) -> float:
    try:
        out = float(val)
        return out if out > 0 else 0.0
    except Exception:
        return 0.0


def _maybe_float(val):
    try:
        return float(val)
    except Exception:
        return None
