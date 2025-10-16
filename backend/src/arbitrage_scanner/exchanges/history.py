from __future__ import annotations

import asyncio
import math
import time
from dataclasses import dataclass
from typing import Sequence

import httpx

from ..domain import ExchangeName, Symbol


@dataclass(frozen=True)
class PriceCandle:
    start_ts: int
    open: float
    high: float
    low: float
    close: float


@dataclass(frozen=True)
class SpreadCandle:
    start_ts: int
    open: float
    high: float
    low: float
    close: float


class HistoryFetchError(Exception):
    """Raised when we fail to download history for an exchange."""


_TIMEFRAME_SECONDS: dict[int, str] = {60: "1m", 300: "5m", 3600: "1h"}
_BYBIT_INTERVALS: dict[int, str] = {60: "1", 300: "5", 3600: "60"}
_MEXC_INTERVALS: dict[int, str] = {60: "Min1", 300: "Min5", 3600: "Min60"}
_BINGX_INTERVALS: dict[int, str] = {60: "1m", 300: "5m", 3600: "1h"}

_BINANCE_URL = "https://fapi.binance.com/fapi/v1/klines"
_BYBIT_URL = "https://api.bybit.com/v5/market/kline"
_MEXC_URL_TEMPLATE = "https://contract.mexc.com/api/v1/contract/kline/{symbol}"
_BINGX_URL = "https://open-api.bingx.com/openApi/swap/v2/market/kline"

_DEFAULT_TIMEOUT = httpx.Timeout(10.0, connect=10.0, read=20.0, write=20.0)


async def fetch_spread_history(
    *,
    symbol: Symbol,
    long_exchange: ExchangeName,
    short_exchange: ExchangeName,
    timeframe_seconds: int,
    lookback_days: float,
    client: httpx.AsyncClient | None = None,
) -> tuple[list[SpreadCandle], list[SpreadCandle]]:
    """Fetch spread history for the given pair of exchanges."""

    timeframe = int(timeframe_seconds)
    if timeframe not in _TIMEFRAME_SECONDS:
        raise ValueError(f"Unsupported timeframe: {timeframe}")

    end_ts = int(time.time())
    start_ts = end_ts - int(max(lookback_days, 0) * 86400)

    own_client = client is None
    if own_client:
        client = httpx.AsyncClient(timeout=_DEFAULT_TIMEOUT)

    try:
        long_task = asyncio.create_task(
            _fetch_exchange_candles(client, long_exchange, symbol, timeframe, start_ts, end_ts)
        )
        short_task = asyncio.create_task(
            _fetch_exchange_candles(client, short_exchange, symbol, timeframe, start_ts, end_ts)
        )
        long_candles, short_candles = await asyncio.gather(long_task, short_task)
    finally:
        if own_client and client is not None:
            await client.aclose()

    if not long_candles or not short_candles:
        return [], []

    entry_candles, exit_candles = _compose_spread_candles(long_candles, short_candles)
    return entry_candles, exit_candles


async def _fetch_exchange_candles(
    client: httpx.AsyncClient,
    exchange: ExchangeName,
    symbol: Symbol,
    timeframe_seconds: int,
    start_ts: int,
    end_ts: int,
) -> list[PriceCandle]:
    ex = str(exchange).lower()
    if ex == "binance":
        return await _fetch_binance(client, symbol, timeframe_seconds, start_ts, end_ts)
    if ex == "bybit":
        return await _fetch_bybit(client, symbol, timeframe_seconds, start_ts, end_ts)
    if ex == "mexc":
        return await _fetch_mexc(client, symbol, timeframe_seconds, start_ts, end_ts)
    if ex == "bingx":
        return await _fetch_bingx(client, symbol, timeframe_seconds, start_ts, end_ts)
    raise HistoryFetchError(f"Exchange {exchange!r} is not supported for history fetching")


async def _fetch_binance(
    client: httpx.AsyncClient,
    symbol: Symbol,
    timeframe_seconds: int,
    start_ts: int,
    end_ts: int,
) -> list[PriceCandle]:
    interval = _TIMEFRAME_SECONDS[timeframe_seconds]
    params = {
        "symbol": str(symbol).upper(),
        "interval": interval,
        "limit": 1500,
    }
    candles: list[PriceCandle] = []
    start_ms = max(start_ts, 0) * 1000
    end_ms = max(end_ts, start_ts) * 1000
    step_ms = timeframe_seconds * 1000

    while start_ms < end_ms:
        params["startTime"] = start_ms
        params["endTime"] = end_ms
        resp = await client.get(_BINANCE_URL, params=params)
        resp.raise_for_status()
        data = resp.json()
        if not isinstance(data, list) or not data:
            break
        progressed = False
        for item in data:
            if not isinstance(item, list) or len(item) < 5:
                continue
            open_time = int(item[0]) // 1000
            open_price = _safe_float(item[1])
            high_price = _safe_float(item[2])
            low_price = _safe_float(item[3])
            close_price = _safe_float(item[4])
            if None in (open_price, high_price, low_price, close_price):
                continue
            candle = PriceCandle(
                start_ts=open_time,
                open=open_price,
                high=high_price,
                low=low_price,
                close=close_price,
            )
            candles.append(candle)
            progressed = True
            last_open = int(item[0])
        if not progressed:
            break
        start_ms = last_open + step_ms
        if len(data) < params["limit"]:
            break
    return candles


async def _fetch_bybit(
    client: httpx.AsyncClient,
    symbol: Symbol,
    timeframe_seconds: int,
    start_ts: int,
    end_ts: int,
) -> list[PriceCandle]:
    interval = _BYBIT_INTERVALS[timeframe_seconds]
    params = {
        "category": "linear",
        "symbol": str(symbol).upper(),
        "interval": interval,
        "start": start_ts * 1000,
        "end": end_ts * 1000,
        "limit": 1000,
    }
    candles: list[PriceCandle] = []
    cursor = params["start"]
    step_ms = timeframe_seconds * 1000

    while cursor < params["end"]:
        params["start"] = cursor
        resp = await client.get(_BYBIT_URL, params=params)
        resp.raise_for_status()
        payload = resp.json()
        data = payload.get("result", {}).get("list", []) if isinstance(payload, dict) else []
        if not data:
            break
        progressed = False
        for item in data:
            if not isinstance(item, (list, tuple)) or len(item) < 5:
                continue
            open_time = int(item[0]) // 1000
            open_price = _safe_float(item[1])
            high_price = _safe_float(item[2])
            low_price = _safe_float(item[3])
            close_price = _safe_float(item[4])
            if None in (open_price, high_price, low_price, close_price):
                continue
            candle = PriceCandle(
                start_ts=open_time,
                open=open_price,
                high=high_price,
                low=low_price,
                close=close_price,
            )
            candles.append(candle)
            progressed = True
            cursor = int(item[0]) + step_ms
        if not progressed or len(data) < params["limit"]:
            break
    return candles


async def _fetch_mexc(
    client: httpx.AsyncClient,
    symbol: Symbol,
    timeframe_seconds: int,
    start_ts: int,
    end_ts: int,
) -> list[PriceCandle]:
    interval = _MEXC_INTERVALS[timeframe_seconds]
    mexc_symbol = _to_mexc_symbol(str(symbol).upper())
    url = _MEXC_URL_TEMPLATE.format(symbol=mexc_symbol)
    params = {
        "interval": interval,
        "start_time": start_ts,
        "end_time": end_ts,
        "limit": 1000,
    }
    candles: list[PriceCandle] = []
    cursor = start_ts
    step = timeframe_seconds

    while cursor < end_ts:
        params["start_time"] = cursor
        resp = await client.get(url, params=params)
        resp.raise_for_status()
        payload = resp.json()
        data = payload.get("data") if isinstance(payload, dict) else None
        if not isinstance(data, list) or not data:
            break
        progressed = False
        for item in data:
            if not isinstance(item, dict):
                continue
            open_time = int(item.get("time") or item.get("t") or item.get("timestamp") or 0)
            if open_time <= 0:
                continue
            open_price = _safe_float(item.get("open") or item.get("o") or item.get("openPrice"))
            high_price = _safe_float(item.get("high") or item.get("h") or item.get("highestPrice"))
            low_price = _safe_float(item.get("low") or item.get("l") or item.get("lowestPrice"))
            close_price = _safe_float(item.get("close") or item.get("c") or item.get("closePrice"))
            if None in (open_price, high_price, low_price, close_price):
                continue
            candle = PriceCandle(
                start_ts=open_time,
                open=open_price,
                high=high_price,
                low=low_price,
                close=close_price,
            )
            candles.append(candle)
            progressed = True
            cursor = open_time + step
        if not progressed or len(data) < params["limit"]:
            break
    return candles


async def _fetch_bingx(
    client: httpx.AsyncClient,
    symbol: Symbol,
    timeframe_seconds: int,
    start_ts: int,
    end_ts: int,
) -> list[PriceCandle]:
    interval = _BINGX_INTERVALS[timeframe_seconds]
    bingx_symbol = _to_bingx_symbol(str(symbol).upper())
    params = {
        "symbol": bingx_symbol,
        "interval": interval,
        "startTime": start_ts * 1000,
        "endTime": end_ts * 1000,
        "limit": 1000,
    }
    candles: list[PriceCandle] = []
    cursor = params["startTime"]
    step_ms = timeframe_seconds * 1000

    while cursor < params["endTime"]:
        params["startTime"] = cursor
        resp = await client.get(_BINGX_URL, params=params)
        resp.raise_for_status()
        payload = resp.json()
        data = payload.get("data") if isinstance(payload, dict) else None
        if not isinstance(data, list) or not data:
            break
        progressed = False
        for item in data:
            if isinstance(item, dict):
                open_time = int(item.get("t") or item.get("time") or 0)
                open_price = _safe_float(item.get("o") or item.get("open"))
                high_price = _safe_float(item.get("h") or item.get("high"))
                low_price = _safe_float(item.get("l") or item.get("low"))
                close_price = _safe_float(item.get("c") or item.get("close"))
            elif isinstance(item, list) and len(item) >= 5:
                open_time = int(item[0])
                open_price = _safe_float(item[1])
                high_price = _safe_float(item[2])
                low_price = _safe_float(item[3])
                close_price = _safe_float(item[4])
            else:
                continue
            if open_time <= 0:
                continue
            open_sec = open_time // 1000
            if None in (open_price, high_price, low_price, close_price):
                continue
            candle = PriceCandle(
                start_ts=open_sec,
                open=open_price,
                high=high_price,
                low=low_price,
                close=close_price,
            )
            candles.append(candle)
            progressed = True
            cursor = open_time + step_ms
        if not progressed or len(data) < params["limit"]:
            break
    return candles


def _compose_spread_candles(
    long_candles: Sequence[PriceCandle],
    short_candles: Sequence[PriceCandle],
) -> tuple[list[SpreadCandle], list[SpreadCandle]]:
    long_map = {c.start_ts: c for c in long_candles}
    short_map = {c.start_ts: c for c in short_candles}
    common_ts = sorted(set(long_map.keys()) & set(short_map.keys()))
    if not common_ts:
        return [], []

    entry: list[SpreadCandle] = []
    exit_: list[SpreadCandle] = []

    for ts in common_ts:
        lc = long_map[ts]
        sc = short_map[ts]
        entry.append(
            SpreadCandle(
                start_ts=ts,
                open=_spread_pct(sc.open, lc.open),
                high=_spread_pct(sc.high, lc.low),
                low=_spread_pct(sc.low, lc.high),
                close=_spread_pct(sc.close, lc.close),
            )
        )
        exit_.append(
            SpreadCandle(
                start_ts=ts,
                open=_spread_pct(lc.open, sc.open),
                high=_spread_pct(lc.high, sc.low),
                low=_spread_pct(lc.low, sc.high),
                close=_spread_pct(lc.close, sc.close),
            )
        )

    return entry, exit_


def _spread_pct(bid: float, ask: float) -> float:
    bid = float(bid)
    ask = float(ask)
    mid = (bid + ask) / 2.0
    if not math.isfinite(bid) or not math.isfinite(ask) or mid == 0:
        return 0.0
    return (bid - ask) / mid * 100.0


def _safe_float(value) -> float | None:
    try:
        if value is None:
            return None
        result = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(result) or math.isinf(result):
        return None
    return result


def _to_mexc_symbol(symbol: str) -> str:
    if "_" in symbol:
        return symbol
    if symbol.endswith("USDT"):
        return f"{symbol[:-4]}_USDT"
    if symbol.endswith("USDC"):
        return f"{symbol[:-4]}_USDC"
    if symbol.endswith("USD"):
        return f"{symbol[:-3]}_USD"
    return symbol


def _to_bingx_symbol(symbol: str) -> str:
    if "-" in symbol:
        return symbol
    if symbol.endswith("USDT"):
        return f"{symbol[:-4]}-USDT"
    if symbol.endswith("USDC"):
        return f"{symbol[:-4]}-USDC"
    if symbol.endswith("USD"):
        return f"{symbol[:-3]}-USD"
    if symbol.endswith("BUSD"):
        return f"{symbol[:-4]}-BUSD"
    if symbol.endswith("FDUSD"):
        return f"{symbol[:-5]}-FDUSD"
    return symbol
