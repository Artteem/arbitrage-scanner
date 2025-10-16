from __future__ import annotations

from datetime import datetime, timedelta, timezone
import logging
from typing import Dict, List

import httpx

from .base import ConnectorContract, ConnectorFundingRate, ConnectorQuote
from .normalization import normalize_bingx_symbol
from ..domain import Symbol

logger = logging.getLogger(__name__)

_BINGX_CONTRACTS = "https://open-api.bingx.com/openApi/swap/v3/market/getAllContracts"
_BINGX_KLINE = "https://open-api.bingx.com/openApi/swap/v2/market/kline"
_BINGX_FUNDING = "https://open-api.bingx.com/openApi/swap/v2/market/fundingRate"
_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Origin": "https://bingx.com",
    "Referer": "https://bingx.com/",
}
_DEFAULT_TIMEOUT = httpx.Timeout(20.0, connect=10.0, read=20.0, write=20.0)
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


async def get_bingx_contracts() -> List[ConnectorContract]:
    async with httpx.AsyncClient(timeout=_DEFAULT_TIMEOUT, headers=_HEADERS) as client:
        response = await client.get(_BINGX_CONTRACTS)
        response.raise_for_status()
        payload = response.json()

    contracts: List[ConnectorContract] = []
    taker_fees: Dict[Symbol, float] = {}
    data = payload.get("data", []) if isinstance(payload, dict) else []
    for item in data:
        if not isinstance(item, dict):
            continue
        quote = str(item.get("quoteAsset") or item.get("quoteSymbol") or "").upper()
        if quote != "USDT":
            continue
        symbol_raw = str(item.get("symbol") or item.get("contractSymbol") or "").upper()
        normalized = normalize_bingx_symbol(symbol_raw)
        if not normalized:
            continue
        taker_fee_value: float | None = None
        taker_fee_raw = item.get("takerFeeRate") or item.get("takerFee")
        try:
            if taker_fee_raw is not None:
                taker_fee_value = float(taker_fee_raw)
                taker_fees[normalized] = taker_fee_value
        except (TypeError, ValueError):
            taker_fee_value = None
        try:
            tick_size = float(item.get("minPricePrecision")) if item.get("minPricePrecision") else None
        except (TypeError, ValueError):
            tick_size = None
        try:
            lot_size = float(item.get("minTradeSize")) if item.get("minTradeSize") else None
        except (TypeError, ValueError):
            lot_size = None
        try:
            contract_size = float(item.get("contractSize")) if item.get("contractSize") else None
        except (TypeError, ValueError):
            contract_size = None
        funding_symbol = str(item.get("symbol") or "").upper()
        contract = ConnectorContract(
            original_symbol=symbol_raw,
            normalized_symbol=normalized,
            base_asset=str(item.get("baseAsset") or item.get("baseSymbol") or "").upper(),
            quote_asset="USDT",
            contract_type="perp",
            tick_size=tick_size,
            lot_size=lot_size,
            contract_size=contract_size,
            taker_fee=taker_fee_value,
            funding_symbol=funding_symbol,
            is_active=True,
        )
        contracts.append(contract)
    _cache_contracts(contracts, taker_fees)
    return contracts


async def get_bingx_taker_fee(symbol: Symbol) -> float | None:
    normalized = Symbol(str(symbol).upper())
    return _TAKER_FEES.get(normalized)


async def get_bingx_historical_quotes(
    symbol: Symbol,
    start: datetime,
    end: datetime,
    interval: timedelta,
) -> List[ConnectorQuote]:
    api_symbol = _resolve_api_symbol(symbol)
    params = {
        "symbol": api_symbol,
        "interval": "1m",
        "start": int(start.timestamp() * 1000),
        "end": int(end.timestamp() * 1000),
    }
    quotes: List[ConnectorQuote] = []

    async with httpx.AsyncClient(timeout=_DEFAULT_TIMEOUT, headers=_HEADERS) as client:
        response = await client.get(_BINGX_KLINE, params=params)
        response.raise_for_status()
        payload = response.json()
    data = payload.get("data", []) if isinstance(payload, dict) else []
    for entry in data:
        if not isinstance(entry, dict):
            continue
        try:
            ts = datetime.fromtimestamp(int(entry.get("time")) / 1000, tz=timezone.utc)
            high_price = float(entry.get("high"))
            low_price = float(entry.get("low"))
        except (TypeError, ValueError):
            continue
        if ts < start or ts >= end:
            continue
        if low_price <= 0 or high_price <= 0:
            continue
        quotes.append(ConnectorQuote(timestamp=ts, bid=low_price, ask=high_price))
    quotes.sort(key=lambda q: q.timestamp)
    return quotes


async def get_bingx_funding_history(
    symbol: Symbol,
    start: datetime,
    end: datetime,
) -> List[ConnectorFundingRate]:
    api_symbol = _resolve_api_symbol(symbol)
    params = {
        "symbol": api_symbol,
        "startTime": int(start.timestamp() * 1000),
        "endTime": int(end.timestamp() * 1000),
    }
    funding: List[ConnectorFundingRate] = []

    async with httpx.AsyncClient(timeout=_DEFAULT_TIMEOUT, headers=_HEADERS) as client:
        response = await client.get(_BINGX_FUNDING, params=params)
        response.raise_for_status()
        payload = response.json()
    data = payload.get("data", []) if isinstance(payload, dict) else []
    for entry in data:
        if not isinstance(entry, dict):
            continue
        try:
            ts = datetime.fromtimestamp(int(entry.get("time")) / 1000, tz=timezone.utc)
            rate = float(entry.get("rate"))
        except (TypeError, ValueError):
            continue
        if ts < start or ts >= end:
            continue
        interval = entry.get("interval") or entry.get("fundingInterval") or _FUNDING_INTERVAL
        funding.append(ConnectorFundingRate(timestamp=ts, rate=rate, interval=str(interval)))
    funding.sort(key=lambda f: f.timestamp)
    return funding


__all__ = [
    "get_bingx_contracts",
    "get_bingx_taker_fee",
    "get_bingx_historical_quotes",
    "get_bingx_funding_history",
]
