from __future__ import annotations

from datetime import datetime, timedelta, timezone
import logging
from typing import Dict, List

import httpx

from .base import ConnectorContract, ConnectorFundingRate, ConnectorQuote
from .normalization import normalize_binance_symbol
from ..domain import Symbol

logger = logging.getLogger(__name__)

_BINANCE_EXCHANGE_INFO = "https://fapi.binance.com/fapi/v1/exchangeInfo"
_BINANCE_COMMISSION_RATE = "https://fapi.binance.com/fapi/v1/commissionRate"
_BINANCE_KLINES = "https://fapi.binance.com/fapi/v1/klines"
_BINANCE_FUNDING = "https://fapi.binance.com/fapi/v1/fundingRate"
_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    "Origin": "https://www.binance.com",
    "Referer": "https://www.binance.com/",
}

_CONTRACT_CACHE: Dict[Symbol, ConnectorContract] = {}
_DEFAULT_TIMEOUT = httpx.Timeout(20.0, connect=10.0, read=20.0, write=20.0)
_FUNDING_INTERVAL = "8h"


def _extract_filter_value(filters: list[dict], filter_type: str, key: str) -> float | None:
    for item in filters:
        if not isinstance(item, dict):
            continue
        if item.get("filterType") == filter_type and item.get(key) is not None:
            try:
                return float(item[key])
            except (TypeError, ValueError):
                return None
    return None


def _cache_contracts(contracts: List[ConnectorContract]) -> None:
    _CONTRACT_CACHE.clear()
    for contract in contracts:
        _CONTRACT_CACHE[contract.normalized_symbol] = contract


def _resolve_api_symbol(symbol: Symbol) -> str:
    contract = _CONTRACT_CACHE.get(Symbol(symbol))
    if contract:
        return contract.original_symbol.upper()
    return str(symbol).upper()


async def get_binance_contracts() -> List[ConnectorContract]:
    async with httpx.AsyncClient(timeout=_DEFAULT_TIMEOUT, headers=_HEADERS, http2=True) as client:
        response = await client.get(_BINANCE_EXCHANGE_INFO)
        response.raise_for_status()
        payload = response.json()

    contracts: List[ConnectorContract] = []
    for item in payload.get("symbols", []) if isinstance(payload, dict) else []:
        if not isinstance(item, dict):
            continue
        if item.get("status") != "TRADING":
            continue
        if item.get("contractType") != "PERPETUAL":
            continue
        if item.get("quoteAsset") != "USDT":
            continue
        original = str(item.get("symbol") or "").upper()
        normalized = normalize_binance_symbol(original)
        if not normalized:
            continue
        tick_size = _extract_filter_value(item.get("filters") or [], "PRICE_FILTER", "tickSize")
        lot_size = _extract_filter_value(item.get("filters") or [], "LOT_SIZE", "stepSize")
        contract_size = None
        try:
            contract_size_raw = item.get("contractSize")
            contract_size = float(contract_size_raw) if contract_size_raw is not None else None
        except (TypeError, ValueError):
            contract_size = None
        contract = ConnectorContract(
            original_symbol=original,
            normalized_symbol=normalized,
            base_asset=str(item.get("baseAsset") or "").upper(),
            quote_asset="USDT",
            contract_type="perp",
            tick_size=tick_size,
            lot_size=lot_size,
            contract_size=contract_size,
            funding_symbol=original,
            is_active=True,
        )
        contracts.append(contract)
    _cache_contracts(contracts)
    return contracts


async def get_binance_taker_fee(symbol: Symbol) -> float | None:
    api_symbol = _resolve_api_symbol(symbol)
    params = {"symbol": api_symbol}
    async with httpx.AsyncClient(timeout=_DEFAULT_TIMEOUT, headers=_HEADERS, http2=True) as client:
        response = await client.get(_BINANCE_COMMISSION_RATE, params=params)
        if response.status_code == 404:
            logger.warning("Binance commission rate unavailable for %s", api_symbol)
            return None
        response.raise_for_status()
        payload = response.json() or {}
    try:
        return float(payload.get("takerCommission"))
    except (TypeError, ValueError):
        return None


async def get_binance_historical_quotes(
    symbol: Symbol,
    start: datetime,
    end: datetime,
    interval: timedelta,
) -> List[ConnectorQuote]:
    api_symbol = _resolve_api_symbol(symbol)
    interval_seconds = int(interval.total_seconds())
    if interval_seconds <= 0:
        raise ValueError("Interval must be positive for historical quotes")
    params = {
        "symbol": api_symbol,
        "interval": "1m",
        "limit": 1500,
    }
    start_ms = int(start.timestamp() * 1000)
    end_ms = int(end.timestamp() * 1000)
    step_ms = max(int(interval.total_seconds() * 1000), 60_000)
    quotes: List[ConnectorQuote] = []

    async with httpx.AsyncClient(timeout=_DEFAULT_TIMEOUT, headers=_HEADERS, http2=True) as client:
        cursor = start_ms
        while cursor < end_ms:
            params["startTime"] = cursor
            params["endTime"] = end_ms
            response = await client.get(_BINANCE_KLINES, params=params)
            response.raise_for_status()
            data = response.json()
            if not isinstance(data, list) or not data:
                break
            progressed = False
            last_open = cursor
            for entry in data:
                if not isinstance(entry, list) or len(entry) < 5:
                    continue
                open_time_ms = int(entry[0])
                if open_time_ms < start_ms:
                    continue
                ts = datetime.fromtimestamp(open_time_ms / 1000, tz=timezone.utc)
                if ts >= end:
                    continue
                try:
                    low_price = float(entry[3])
                    high_price = float(entry[2])
                except (TypeError, ValueError):
                    continue
                if low_price <= 0 or high_price <= 0:
                    continue
                quotes.append(ConnectorQuote(timestamp=ts, bid=low_price, ask=high_price))
                progressed = True
                last_open = open_time_ms
            if not progressed:
                break
            cursor = last_open + step_ms
            if len(data) < params["limit"]:
                break
    return quotes


async def get_binance_funding_history(
    symbol: Symbol,
    start: datetime,
    end: datetime,
) -> List[ConnectorFundingRate]:
    api_symbol = _resolve_api_symbol(symbol)
    params = {
        "symbol": api_symbol,
        "limit": 1000,
    }
    start_ms = int(start.timestamp() * 1000)
    end_ms = int(end.timestamp() * 1000)
    funding: List[ConnectorFundingRate] = []

    async with httpx.AsyncClient(timeout=_DEFAULT_TIMEOUT, headers=_HEADERS, http2=True) as client:
        cursor = start_ms
        while cursor < end_ms:
            params["startTime"] = cursor
            params["endTime"] = end_ms
            response = await client.get(_BINANCE_FUNDING, params=params)
            response.raise_for_status()
            data = response.json()
            if not isinstance(data, list) or not data:
                break
            progressed = False
            last_time = cursor
            for entry in data:
                if not isinstance(entry, dict):
                    continue
                try:
                    funding_time = int(entry.get("fundingTime"))
                except (TypeError, ValueError):
                    continue
                if funding_time < start_ms or funding_time >= end_ms:
                    continue
                ts = datetime.fromtimestamp(funding_time / 1000, tz=timezone.utc)
                try:
                    rate = float(entry.get("fundingRate"))
                except (TypeError, ValueError):
                    continue
                interval = entry.get("fundingInterval") or _FUNDING_INTERVAL
                funding.append(
                    ConnectorFundingRate(timestamp=ts, rate=rate, interval=str(interval)),
                )
                progressed = True
                last_time = funding_time
            if not progressed:
                break
            cursor = last_time + 1
            if len(data) < params["limit"]:
                break
    return funding


__all__ = [
    "get_binance_contracts",
    "get_binance_taker_fee",
    "get_binance_historical_quotes",
    "get_binance_funding_history",
]
