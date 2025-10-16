from __future__ import annotations

from datetime import datetime, timedelta, timezone
import logging
from typing import Dict, List

import httpx

from .base import ConnectorContract, ConnectorFundingRate, ConnectorQuote
from .normalization import normalize_bybit_symbol
from ..domain import Symbol

logger = logging.getLogger(__name__)

_BYBIT_INSTRUMENTS = "https://api.bybit.com/v5/market/instruments-info"
_BYBIT_KLINE = "https://api.bybit.com/v5/market/kline"
_BYBIT_FUNDING = "https://api.bybit.com/v5/market/funding/history"
_DEFAULT_TIMEOUT = httpx.Timeout(20.0, connect=10.0, read=20.0, write=20.0)
_CATEGORY = "linear"
_DEFAULT_INTERVAL = "1"
_FUNDING_INTERVAL = "8h"

_CONTRACT_CACHE: Dict[Symbol, ConnectorContract] = {}
_TAKER_FEES: Dict[Symbol, float] = {}


def _cache_contracts(contracts: List[ConnectorContract], taker_fees: Dict[Symbol, float]) -> None:
    _CONTRACT_CACHE.clear()
    _TAKER_FEES.clear()
    for contract in contracts:
        _CONTRACT_CACHE[contract.normalized_symbol] = contract
        fee = taker_fees.get(contract.normalized_symbol)
        if fee is not None:
            _TAKER_FEES[contract.normalized_symbol] = fee


def _resolve_api_symbol(symbol: Symbol) -> str:
    contract = _CONTRACT_CACHE.get(Symbol(symbol))
    if contract:
        return contract.original_symbol
    return str(symbol).upper()


async def get_bybit_contracts() -> List[ConnectorContract]:
    params = {"category": _CATEGORY, "limit": 1000}
    async with httpx.AsyncClient(timeout=_DEFAULT_TIMEOUT) as client:
        response = await client.get(_BYBIT_INSTRUMENTS, params=params)
        response.raise_for_status()
        payload = response.json()

    contracts: List[ConnectorContract] = []
    taker_fees: Dict[Symbol, float] = {}
    result = payload.get("result", {}) if isinstance(payload, dict) else {}
    items = result.get("list", []) if isinstance(result, dict) else []
    for item in items:
        if not isinstance(item, dict):
            continue
        if str(item.get("status") or "").lower() not in {"trading", "live", "online"}:
            continue
        if str(item.get("quoteCoin") or "").upper() != "USDT":
            continue
        symbol_raw = str(item.get("symbol") or "").upper()
        normalized = normalize_bybit_symbol(symbol_raw)
        if not normalized:
            continue
        price_filter = item.get("priceFilter") or {}
        lot_filter = item.get("lotSizeFilter") or {}
        tick_size = None
        lot_size = None
        contract_size = None
        try:
            tick_size = float(price_filter.get("tickSize")) if price_filter.get("tickSize") else None
        except (TypeError, ValueError):
            tick_size = None
        try:
            lot_size = float(lot_filter.get("qtyStep")) if lot_filter.get("qtyStep") else None
        except (TypeError, ValueError):
            lot_size = None
        try:
            contract_size_raw = item.get("contractSize")
            contract_size = float(contract_size_raw) if contract_size_raw else None
        except (TypeError, ValueError):
            contract_size = None
        taker_fee_value: float | None = None
        taker_fee_raw = item.get("takerFee")
        if taker_fee_raw is None:
            taker_fee_raw = item.get("takerFeeRate")
        try:
            if taker_fee_raw is not None:
                taker_fee_value = float(taker_fee_raw)
                taker_fees[normalized] = taker_fee_value
        except (TypeError, ValueError):
            taker_fee_value = None
        contract = ConnectorContract(
            original_symbol=symbol_raw,
            normalized_symbol=normalized,
            base_asset=str(item.get("baseCoin") or "").upper(),
            quote_asset="USDT",
            contract_type="perp",
            tick_size=tick_size,
            lot_size=lot_size,
            contract_size=contract_size,
            taker_fee=taker_fee_value,
            funding_symbol=symbol_raw,
            is_active=True,
        )
        contracts.append(contract)
    _cache_contracts(contracts, taker_fees)
    return contracts


async def get_bybit_taker_fee(symbol: Symbol) -> float | None:
    normalized = Symbol(str(symbol).upper())
    fee = _TAKER_FEES.get(normalized)
    if fee is not None:
        return fee
    # Публичного эндпоинта с динамическими комиссиями нет, используем базовое значение.
    return 0.0006


async def get_bybit_historical_quotes(
    symbol: Symbol,
    start: datetime,
    end: datetime,
    interval: timedelta,
) -> List[ConnectorQuote]:
    api_symbol = _resolve_api_symbol(symbol)
    interval_minutes = max(int(interval.total_seconds() // 60), 1)
    params = {
        "category": _CATEGORY,
        "symbol": api_symbol,
        "interval": str(interval_minutes if interval_minutes in {1, 3, 5, 15, 30, 60} else 1),
        "start": int(start.timestamp() * 1000),
        "end": int(end.timestamp() * 1000),
        "limit": 1000,
    }
    quotes: List[ConnectorQuote] = []

    async with httpx.AsyncClient(timeout=_DEFAULT_TIMEOUT) as client:
        cursor = params["start"]
        while cursor < params["end"]:
            params["start"] = cursor
            response = await client.get(_BYBIT_KLINE, params=params)
            response.raise_for_status()
            payload = response.json()
            data = payload.get("result", {}).get("list", []) if isinstance(payload, dict) else []
            if not data:
                break
            progressed = False
            last_time = cursor
            for entry in data:
                if not isinstance(entry, (list, tuple)) or len(entry) < 5:
                    continue
                ts_ms = int(entry[0])
                if ts_ms < params["start"] or ts_ms >= params["end"]:
                    continue
                ts = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
                try:
                    high_price = float(entry[2])
                    low_price = float(entry[3])
                except (TypeError, ValueError):
                    continue
                if low_price <= 0 or high_price <= 0:
                    continue
                quotes.append(ConnectorQuote(timestamp=ts, bid=low_price, ask=high_price))
                progressed = True
                last_time = ts_ms
            if not progressed:
                break
            cursor = last_time + int(interval.total_seconds() * 1000)
            if len(data) < params["limit"]:
                break
    return quotes


async def get_bybit_funding_history(
    symbol: Symbol,
    start: datetime,
    end: datetime,
) -> List[ConnectorFundingRate]:
    api_symbol = _resolve_api_symbol(symbol)
    params = {
        "category": _CATEGORY,
        "symbol": api_symbol,
        "start": int(start.timestamp() * 1000),
        "end": int(end.timestamp() * 1000),
        "limit": 200,
    }
    funding: List[ConnectorFundingRate] = []

    async with httpx.AsyncClient(timeout=_DEFAULT_TIMEOUT) as client:
        cursor = params["start"]
        while cursor < params["end"]:
            params["start"] = cursor
            response = await client.get(_BYBIT_FUNDING, params=params)
            response.raise_for_status()
            payload = response.json()
            data = payload.get("result", {}).get("list", []) if isinstance(payload, dict) else []
            if not data:
                break
            progressed = False
            last_time = cursor
            for entry in data:
                if not isinstance(entry, dict):
                    continue
                try:
                    ts_ms = int(entry.get("fundingTime"))
                except (TypeError, ValueError):
                    continue
                if ts_ms < params["start"] or ts_ms >= params["end"]:
                    continue
                ts = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
                try:
                    rate = float(entry.get("fundingRate"))
                except (TypeError, ValueError):
                    continue
                interval = entry.get("fundingInterval") or _FUNDING_INTERVAL
                funding.append(ConnectorFundingRate(timestamp=ts, rate=rate, interval=str(interval)))
                progressed = True
                last_time = ts_ms
            if not progressed:
                break
            cursor = last_time + 1
            if len(data) < params["limit"]:
                break
    return funding


__all__ = [
    "get_bybit_contracts",
    "get_bybit_taker_fee",
    "get_bybit_historical_quotes",
    "get_bybit_funding_history",
]
