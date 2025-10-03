from __future__ import annotations

import asyncio
import time
from typing import Any, Dict, Optional

import httpx

from ..connectors.discovery import (
    BINANCE_EXCHANGE_INFO,
    BYBIT_INSTRUMENTS,
    MEXC_CONTRACTS,
)

_CACHE_TTL = 600.0  # seconds
_LIMIT_CACHE: Dict[tuple[str, str], tuple[float, Optional[dict[str, Any]]]] = {}
_CACHE_LOCK = asyncio.Lock()

_BINANCE_CACHE: dict[str, Any] | None = None
_BINANCE_LOCK = asyncio.Lock()

_BYBIT_CACHE: list[dict[str, Any]] | None = None
_BYBIT_LOCK = asyncio.Lock()

_MEXC_CACHE: list[dict[str, Any]] | None = None
_MEXC_LOCK = asyncio.Lock()


def _safe_float(value: Any) -> Optional[float]:
    try:
        num = float(value)
    except (TypeError, ValueError):
        return None
    if not (num is not None and num == num):
        return None
    return num


async def fetch_limits(exchange: str, symbol: str) -> Optional[dict[str, Any]]:
    """Fetch trading limits for a symbol on a specific exchange.

    Returns a dict with optional keys ("max_qty", "max_notional", "limit_desc")
    or ``None`` if the information is unavailable.
    """

    key = (exchange.lower(), symbol.upper())
    now = time.time()
    async with _CACHE_LOCK:
        cached = _LIMIT_CACHE.get(key)
        if cached and now - cached[0] < _CACHE_TTL:
            return cached[1]

    try:
        if key[0] == "binance":
            data = await _fetch_binance_limits(symbol)
        elif key[0] == "bybit":
            data = await _fetch_bybit_limits(symbol)
        elif key[0] == "mexc":
            data = await _fetch_mexc_limits(symbol)
        else:
            data = None
    except Exception:
        data = None

    async with _CACHE_LOCK:
        _LIMIT_CACHE[key] = (time.time(), data)
    return data


async def _fetch_binance_limits(symbol: str) -> Optional[dict[str, Any]]:
    sym = symbol.upper()
    async with _BINANCE_LOCK:
        global _BINANCE_CACHE
        if _BINANCE_CACHE is None or time.time() - _BINANCE_CACHE.get("ts", 0) > _CACHE_TTL:
            async with httpx.AsyncClient(timeout=15) as client:
                resp = await client.get(BINANCE_EXCHANGE_INFO)
                resp.raise_for_status()
                payload = resp.json()
            _BINANCE_CACHE = {
                "ts": time.time(),
                "symbols": payload.get("symbols", []),
            }
        entries = _BINANCE_CACHE.get("symbols", []) if _BINANCE_CACHE else []
    for item in entries:
        if item.get("symbol") != sym:
            continue
        max_qty = None
        max_notional = None
        for flt in item.get("filters", []):
            ftype = (flt.get("filterType") or "").upper()
            if ftype in {"LOT_SIZE", "MARKET_LOT_SIZE"}:
                candidate = _safe_float(flt.get("maxQty"))
                if candidate is not None:
                    max_qty = candidate
            if ftype == "NOTIONAL":
                candidate = _safe_float(flt.get("maxNotionalValue"))
                if candidate is not None:
                    max_notional = candidate
        return {
            "max_qty": max_qty,
            "max_notional": max_notional,
        }
    return None


async def _fetch_bybit_limits(symbol: str) -> Optional[dict[str, Any]]:
    sym = symbol.upper()
    async with _BYBIT_LOCK:
        global _BYBIT_CACHE
        if _BYBIT_CACHE is None or not _BYBIT_CACHE:
            async with httpx.AsyncClient(timeout=15) as client:
                resp = await client.get(BYBIT_INSTRUMENTS)
                resp.raise_for_status()
                payload = resp.json()
            result = payload.get("result") or {}
            entries = result.get("list") or []
            _BYBIT_CACHE = [entry for entry in entries if isinstance(entry, dict)]
        entries = _BYBIT_CACHE or []
    for item in entries:
        if (item.get("symbol") or "").upper() != sym:
            continue
        lot = item.get("lotSizeFilter") or {}
        max_qty = _safe_float(lot.get("maxOrderQty"))
        leverage = None
        leverage_info = item.get("leverageFilter") or {}
        lev_value = _safe_float(leverage_info.get("maxLeverage"))
        if lev_value is not None:
            leverage = f"плечо до {lev_value:g}x"
        return {
            "max_qty": max_qty,
            "max_notional": None,
            "limit_desc": leverage,
        }
    return None


async def _fetch_mexc_limits(symbol: str) -> Optional[dict[str, Any]]:
    sym = symbol.upper()
    async with _MEXC_LOCK:
        global _MEXC_CACHE
        if _MEXC_CACHE is None or not _MEXC_CACHE:
            async with httpx.AsyncClient(timeout=15) as client:
                resp = await client.get(MEXC_CONTRACTS)
                resp.raise_for_status()
                payload = resp.json()
            entries = payload.get("data") or []
            _MEXC_CACHE = [entry for entry in entries if isinstance(entry, dict)]
        entries = _MEXC_CACHE or []
    for item in entries:
        raw_symbol = item.get("symbol") or item.get("instrumentId") or ""
        if str(raw_symbol).replace("_", "").upper() != sym:
            continue
        max_qty = _safe_float(item.get("maxVol") or item.get("maxOrderAmount"))
        leverage = _safe_float(item.get("maxLeverage"))
        desc = None
        if leverage is not None:
            desc = f"плечо до {leverage:g}x"
        return {
            "max_qty": max_qty,
            "max_notional": None,
            "limit_desc": desc,
        }
    return None
