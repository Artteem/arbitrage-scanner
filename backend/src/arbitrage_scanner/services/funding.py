"""Helpers for fetching funding rates from exchange HTTP APIs."""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from typing import Iterable, List, Mapping, Sequence

import httpx

from ..domain import ExchangeName, Symbol

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class FundingSnapshot:
    symbol: Symbol
    rate: float
    interval: str
    ts: int


async def fetch_historical_funding(
    exchange: ExchangeName,
    symbol: Symbol,
    lookback_seconds: int,
    *,
    client: httpx.AsyncClient,
) -> Sequence[FundingSnapshot]:
    """Download historical funding rates for the given exchange symbol."""

    exchange_lower = str(exchange).lower()
    if exchange_lower == "binance":
        return await _fetch_binance_funding(symbol, lookback_seconds, client=client)
    if exchange_lower == "bybit":
        return await _fetch_bybit_funding(symbol, lookback_seconds, client=client)
    if exchange_lower == "mexc":
        return await _fetch_mexc_funding(symbol, lookback_seconds, client=client)
    if exchange_lower == "bingx":
        return await _fetch_bingx_funding(symbol, lookback_seconds, client=client)
    if exchange_lower == "gate":
        return await _fetch_gate_funding(symbol, lookback_seconds, client=client)
    return []


async def fetch_latest_funding(
    exchange: ExchangeName,
    symbols: Iterable[Symbol],
    *,
    client: httpx.AsyncClient,
) -> Sequence[FundingSnapshot]:
    """Fetch the most recent funding rate for a batch of symbols."""

    exchange_lower = str(exchange).lower()
    if exchange_lower == "binance":
        return await _fetch_binance_latest(symbols, client=client)
    if exchange_lower == "bybit":
        return await _fetch_bybit_latest(symbols, client=client)
    if exchange_lower == "mexc":
        return await _fetch_mexc_latest(symbols, client=client)
    if exchange_lower == "bingx":
        return await _fetch_bingx_latest(symbols, client=client)
    if exchange_lower == "gate":
        return await _fetch_gate_latest(symbols, client=client)
    return []


async def poll_funding_loop(
    *,
    exchanges: Mapping[ExchangeName, Sequence[Symbol]],
    interval: float,
    store,
    persistence,
) -> None:
    """Periodically poll exchanges for the most recent funding rates."""

    http_timeout = httpx.Timeout(10.0, connect=5.0)
    while True:
        try:
            async with httpx.AsyncClient(timeout=http_timeout) as client:
                tasks = [
                    asyncio.create_task(_fetch_and_apply(exchange, symbols, client, store, persistence))
                    for exchange, symbols in exchanges.items()
                    if symbols
                ]
                if tasks:
                    await asyncio.gather(*tasks)
        except Exception:
            logger.exception("Funding poller iteration failed")
        await asyncio.sleep(max(interval, 1.0))


async def _fetch_and_apply(exchange, symbols, client, store, persistence) -> None:
    snapshots = await fetch_latest_funding(exchange, symbols, client=client)
    now = time.time()
    for snap in snapshots:
        ts = snap.ts or int(now)
        store.upsert_funding(exchange, snap.symbol, rate=snap.rate, interval=snap.interval, ts=float(ts))
        if persistence is not None:
            persistence.submit_funding(exchange, snap.symbol, ts=float(ts), rate=snap.rate, interval=snap.interval)


# --- Exchange-specific implementations ---------------------------------------------------------


async def _fetch_binance_funding(symbol: Symbol, lookback_seconds: int, *, client: httpx.AsyncClient) -> Sequence[FundingSnapshot]:
    end_time = int(time.time() * 1000)
    start_time = end_time - max(int(lookback_seconds * 1000), 0)
    params = {
        "symbol": str(symbol).upper(),
        "startTime": start_time,
        "endTime": end_time,
        "limit": 1000,
    }
    resp = await client.get("https://fapi.binance.com/fapi/v1/fundingRate", params=params)
    resp.raise_for_status()
    payload = resp.json()
    snapshots: list[FundingSnapshot] = []
    for item in payload or []:
        try:
            ts = int(item.get("fundingTime") or item.get("time") or 0) // 1000
            rate = float(item.get("fundingRate") or 0)
        except (TypeError, ValueError):
            continue
        snapshots.append(FundingSnapshot(symbol=symbol, rate=rate, interval="8h", ts=ts))
    return snapshots


async def _fetch_binance_latest(symbols: Iterable[Symbol], *, client: httpx.AsyncClient) -> Sequence[FundingSnapshot]:
    resp = await client.get("https://fapi.binance.com/fapi/v1/premiumIndex")
    resp.raise_for_status()
    payload = resp.json()
    target = {str(symbol).upper() for symbol in symbols}
    out: list[FundingSnapshot] = []
    for item in payload or []:
        symbol = str(item.get("symbol") or "").upper()
        if symbol not in target:
            continue
        try:
            rate = float(item.get("lastFundingRate") or item.get("fundingRate") or 0)
            ts = int(item.get("time") or item.get("nextFundingTime") or time.time()) // 1000
        except (TypeError, ValueError):
            continue
        out.append(FundingSnapshot(symbol=symbol, rate=rate, interval="8h", ts=ts))
    return out


async def _fetch_bybit_funding(symbol: Symbol, lookback_seconds: int, *, client: httpx.AsyncClient) -> Sequence[FundingSnapshot]:
    end_time = int(time.time() * 1000)
    start_time = end_time - max(int(lookback_seconds * 1000), 0)
    params = {
        "category": "linear",
        "symbol": str(symbol).upper(),
        "start": start_time,
        "end": end_time,
        "limit": 200,
    }
    resp = await client.get("https://api.bybit.com/v5/market/funding/history", params=params)
    resp.raise_for_status()
    payload = resp.json()
    data = (payload.get("result") or {}).get("list") if isinstance(payload, dict) else None
    snapshots: list[FundingSnapshot] = []
    for item in data or []:
        try:
            ts = int(item.get("fundingRateTimestamp") or item.get("fundingTime") or 0) // 1000
            rate = float(item.get("fundingRate") or item.get("rate") or 0)
        except (TypeError, ValueError):
            continue
        snapshots.append(FundingSnapshot(symbol=symbol, rate=rate, interval="8h", ts=ts))
    return snapshots


async def _fetch_bybit_latest(symbols: Iterable[Symbol], *, client: httpx.AsyncClient) -> Sequence[FundingSnapshot]:
    params = {"category": "linear", "symbol": ""}
    resp = await client.get("https://api.bybit.com/v5/market/tickers", params=params)
    resp.raise_for_status()
    payload = resp.json()
    data = payload.get("result", {}).get("list", []) if isinstance(payload, dict) else []
    wanted = {str(symbol).upper() for symbol in symbols}
    out: list[FundingSnapshot] = []
    for item in data:
        symbol = str(item.get("symbol") or "").upper()
        if symbol not in wanted:
            continue
        try:
            rate = float(item.get("fundingRate") or 0)
        except (TypeError, ValueError):
            continue
        out.append(FundingSnapshot(symbol=symbol, rate=rate, interval="8h", ts=int(time.time())))
    return out


async def _fetch_mexc_funding(symbol: Symbol, lookback_seconds: int, *, client: httpx.AsyncClient) -> Sequence[FundingSnapshot]:
    params = {
        "symbol": str(symbol).upper().replace("_", "-"),
        "page_num": 1,
        "page_size": 200,
    }
    resp = await client.get("https://contract.mexc.com/api/v1/contract/fundingRate", params=params)
    resp.raise_for_status()
    payload = resp.json()
    data = payload.get("data", {}).get("records") if isinstance(payload, dict) else None
    snapshots: list[FundingSnapshot] = []
    for item in data or []:
        try:
            ts = int(item.get("fundingTime") or item.get("time") or 0) // 1000
            rate = float(item.get("fundingRate") or item.get("rate") or 0)
            interval = str(item.get("fundingInterval") or "8h")
        except (TypeError, ValueError):
            continue
        snapshots.append(FundingSnapshot(symbol=symbol, rate=rate, interval=interval, ts=ts))
    return snapshots


async def _fetch_mexc_latest(symbols: Iterable[Symbol], *, client: httpx.AsyncClient) -> Sequence[FundingSnapshot]:
    out: list[FundingSnapshot] = []
    for symbol in symbols:
        try:
            history = await _fetch_mexc_funding(symbol, 0, client=client)
        except Exception:
            continue
        if history:
            out.append(history[-1])
    return out


async def _fetch_bingx_funding(symbol: Symbol, lookback_seconds: int, *, client: httpx.AsyncClient) -> Sequence[FundingSnapshot]:
    params = {
        "symbol": str(symbol).upper(),
        "limit": 200,
    }
    resp = await client.get("https://open-api.bingx.com/openApi/swap/v3/market/fundingRate", params=params)
    resp.raise_for_status()
    payload = resp.json()
    data = payload.get("data") if isinstance(payload, dict) else None
    snapshots: list[FundingSnapshot] = []
    for item in data or []:
        try:
            ts = int(item.get("fundingTime") or item.get("time") or 0) // 1000
            rate = float(item.get("fundingRate") or item.get("rate") or 0)
        except (TypeError, ValueError):
            continue
        interval = str(item.get("fundingInterval") or item.get("interval") or "8h")
        snapshots.append(FundingSnapshot(symbol=symbol, rate=rate, interval=interval, ts=ts))
    return snapshots


async def _fetch_bingx_latest(symbols: Iterable[Symbol], *, client: httpx.AsyncClient) -> Sequence[FundingSnapshot]:
    out: list[FundingSnapshot] = []
    for symbol in symbols:
        try:
            history = await _fetch_bingx_funding(symbol, 0, client=client)
        except Exception:
            continue
        if history:
            out.append(history[-1])
    return out


async def _fetch_gate_funding(symbol: Symbol, lookback_seconds: int, *, client: httpx.AsyncClient) -> Sequence[FundingSnapshot]:
    params = {
        "contract": str(symbol).replace("_", "-"),
        "limit": 200,
    }
    resp = await client.get("https://api.gateio.ws/api/v4/futures/usdt/funding_rate", params=params)
    resp.raise_for_status()
    payload = resp.json()
    snapshots: list[FundingSnapshot] = []
    for item in payload or []:
        try:
            ts = int(item.get("t") or item.get("time") or 0)
            rate = float(item.get("r") or item.get("funding_rate") or 0)
        except (TypeError, ValueError):
            continue
        interval = str(item.get("interval") or "8h")
        snapshots.append(FundingSnapshot(symbol=symbol, rate=rate, interval=interval, ts=ts))
    return snapshots


async def _fetch_gate_latest(symbols: Iterable[Symbol], *, client: httpx.AsyncClient) -> Sequence[FundingSnapshot]:
    out: list[FundingSnapshot] = []
    for symbol in symbols:
        try:
            history = await _fetch_gate_funding(symbol, 0, client=client)
        except Exception:
            continue
        if history:
            out.append(history[-1])
    return out
