from __future__ import annotations

import asyncio
import time
from typing import Any, Dict, Optional

import httpx

from ..connectors.discovery import (
    BINANCE_EXCHANGE_INFO,
    BINANCE_HEADERS,
    BYBIT_INSTRUMENTS,
    MEXC_CONTRACTS,
)
from ..settings import settings # <--- ДОБАВЛЕНО

_CACHE_TTL = 600.0  # seconds
_LIMIT_CACHE: Dict[tuple[str, str], tuple[float, Optional[dict[str, Any]]]] = {}
_CACHE_LOCK = asyncio.Lock()

_BINANCE_CACHE: dict[str, Any] | None = None
_BINANCE_LOCK = asyncio.Lock()

_BYBIT_CACHE: list[dict[str, Any]] | None = None
_BYBIT_LOCK = asyncio.Lock()

_MEXC_CACHE: list[dict[str, Any]] | None = None
_MEXC_LOCK = asyncio.Lock()

# ИСПРАВЛЕНИЕ: Добавляем _PROXIES и helper
_PROXIES = settings.httpx_proxies

def _get_client_params(
    timeout: float = 15.0,
    headers: dict | None = None,
    http2: bool = False,
) -> dict[str, Any]:
    params: dict[str, Any] = {"timeout": timeout}
    if headers:
        params["headers"] = headers
    if http2:
        params["http2"] = http2
    if _PROXIES:
        params["proxies"] = _PROXIES
    return params


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
    entry = await _lookup_binance_symbol(sym)
    if entry is None:
        return None

    max_qty = None
    max_notional = None
    for flt in entry.get("filters", []):
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


async def _request_binance_exchange_info(symbol: str | None = None) -> list[dict[str, Any]]:
    params = {"symbol": symbol} if symbol else None
    
    # ИСПРАВЛЕНИЕ: Используем _get_client_params
    client_params = _get_client_params(timeout=15.0, headers=BINANCE_HEADERS, http2=True)
    async with httpx.AsyncClient(**client_params) as client:
        resp = await client.get(BINANCE_EXCHANGE_INFO, params=params)
        resp.raise_for_status()
        payload = resp.json()
    entries = payload.get("symbols", []) if isinstance(payload, dict) else []
    return [entry for entry in entries if isinstance(entry, dict)]


async def _lookup_binance_symbol(symbol: str) -> Optional[dict[str, Any]]:
    global _BINANCE_CACHE

    sym = symbol.upper()
    async with _BINANCE_LOCK:
        now = time.time()
        cached_symbols = []
        if _BINANCE_CACHE and now - _BINANCE_CACHE.get("ts", 0) <= _CACHE_TTL:
            cached_symbols = _BINANCE_CACHE.get("symbols", []) or []
        else:
            try:
                cached_symbols = await _request_binance_exchange_info()
            except Exception:
                cached_symbols = []
            _BINANCE_CACHE = {
                "ts": time.time(),
                "symbols": cached_symbols,
            }

        for item in cached_symbols:
            if (item.get("symbol") or "").upper() == sym:
                return item

    # Символ не нашли в общем списке — попробуем запросить его отдельно.
    try:
        entries = await _request_binance_exchange_info(sym)
    except Exception:
        return None

    if not entries:
        return None

    entry = entries[0]

    async with _BINANCE_LOCK:
        if _BINANCE_CACHE is None:
            _BINANCE_CACHE = {"ts": time.time(), "symbols": [entry]}
        else:
            symbols = _BINANCE_CACHE.get("symbols", []) or []
            if not any((item.get("symbol") or "").upper() == sym for item in symbols):
                symbols.append(entry)
                _BINANCE_CACHE = {
                    "ts": time.time(),
                    "symbols": symbols,
                }

    return entry


async def _fetch_bybit_limits(symbol: str) -> Optional[dict[str, Any]]:
    sym = symbol.upper()
    async with _BYBIT_LOCK:
        global _BYBIT_CACHE
        if _BYBIT_CACHE is None or not _BYBIT_CACHE:
            # ИСПРАВЛЕНИЕ: Используем _get_client_params
            client_params = _get_client_params(timeout=15.0)
            async with httpx.AsyncClient(**client_params) as client:
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
            # ИСПРАВЛЕНИЕ: Используем _get_client_params
            client_params = _get_client_params(timeout=15.0)
            async with httpx.AsyncClient(**client_params) as client:
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