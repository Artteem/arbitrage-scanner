from __future__ import annotations

from datetime import datetime, timedelta, timezone
import logging
from typing import Dict, List
from urllib.parse import urlparse

import httpx

from ..settings import settings
from .base import ConnectorContract, ConnectorFundingRate, ConnectorQuote
from .credentials import get_credentials_provider
from .normalization import normalize_mexc_symbol
from .signing import sign_request
from ..domain import Symbol

logger = logging.getLogger(__name__)

_MEXC_CONTRACTS = "https://contract.mexc.com/api/v1/contract/detail"
_MEXC_KLINE_TEMPLATE = "https://contract.mexc.com/api/v1/contract/kline/{symbol}"
_MEXC_FUNDING = "https://contract.mexc.com/api/v1/contract/fundingRate"
_DEFAULT_TIMEOUT = httpx.Timeout(20.0, connect=10.0, read=20.0, write=20.0)
_PROXIES = settings.httpx_proxies
_PATH_CONTRACTS = urlparse(_MEXC_CONTRACTS).path
_PATH_FUNDING = urlparse(_MEXC_FUNDING).path
_INTERVAL = "Min1"
_FUNDING_INTERVAL = "8h"

_CONTRACT_CACHE: Dict[Symbol, ConnectorContract] = {}
_TAKER_FEES: Dict[Symbol, float] = {}


async def _signed_get(
    client: httpx.AsyncClient,
    url: str,
    path: str,
    params: dict | None = None,
):
    provider = get_credentials_provider()
    creds = provider.get("mexc") if provider else None
    if not creds:
        if provider:
            logger.debug("MEXC credentials missing, using public REST endpoints")
        return await client.get(url, params=params)

    query_params = dict(params or {})
    headers, query_string = sign_request("mexc", "GET", path, query_params, None, creds)
    request_headers = dict(client.headers)
    request_headers.update(headers)
    base_url = url.split("?")[0]
    target_url = f"{base_url}?{query_string}" if query_string else base_url
    response = await client.get(target_url, headers=request_headers)
    if response.status_code in {401, 403}:
        logger.warning(
            "MEXC authenticated request failed with %s, retrying without credentials",
            response.status_code,
        )
        return await client.get(url, params=params)
    return response


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
    sym = str(symbol).upper()
    if sym.endswith("USDT"):
        return f"{sym[:-4]}_USDT"
    return sym


async def get_mexc_contracts() -> List[ConnectorContract]:
    async with httpx.AsyncClient(timeout=_DEFAULT_TIMEOUT, proxies=_PROXIES) as client:
        response = await _signed_get(client, _MEXC_CONTRACTS, _PATH_CONTRACTS)
        response.raise_for_status()
        payload = response.json()

    contracts: List[ConnectorContract] = []
    taker_fees: Dict[Symbol, float] = {}
    for item in payload.get("data", []) if isinstance(payload, dict) else []:
        if not isinstance(item, dict):
            continue
        quote = str(
            item.get("quoteCurrency")
            or item.get("quoteCoin")
            or item.get("settleCurrency")
            or "USDT"
        ).upper()
        if quote != "USDT":
            continue
        state = str(item.get("state") or item.get("status") or "").lower()
        if state and state not in {"1", "2", "trading", "online", "live"}:
            continue
        symbol_raw = str(item.get("symbol") or "").upper()
        if "_" not in symbol_raw and symbol_raw.endswith("USDT"):
            symbol_raw = f"{symbol_raw[:-4]}_USDT"
        normalized = normalize_mexc_symbol(symbol_raw)
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
            tick_size = float(item.get("priceUnit")) if item.get("priceUnit") else None
        except (TypeError, ValueError):
            tick_size = None
        try:
            lot_size = float(item.get("volUnit")) if item.get("volUnit") else None
        except (TypeError, ValueError):
            lot_size = None
        try:
            contract_size = float(item.get("contractSize")) if item.get("contractSize") else None
        except (TypeError, ValueError):
            contract_size = None
        contract = ConnectorContract(
            original_symbol=symbol_raw,
            normalized_symbol=normalized,
            base_asset=str(item.get("baseCurrency") or item.get("baseCoin") or "").upper(),
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


async def get_mexc_taker_fee(symbol: Symbol) -> float | None:
    normalized = Symbol(str(symbol).upper())
    return _TAKER_FEES.get(normalized)


async def get_mexc_historical_quotes(
    symbol: Symbol,
    start: datetime,
    end: datetime,
    interval: timedelta,
) -> List[ConnectorQuote]:
    api_symbol = _resolve_api_symbol(symbol)
    params = {
        "interval": _INTERVAL,
        "start": int(start.timestamp() * 1000),
        "end": int(end.timestamp() * 1000),
    }
    quotes: List[ConnectorQuote] = []

    url = _MEXC_KLINE_TEMPLATE.format(symbol=api_symbol)
    async with httpx.AsyncClient(timeout=_DEFAULT_TIMEOUT, proxies=_PROXIES) as client:
        response = await _signed_get(client, url, urlparse(url).path, params=params)
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


async def get_mexc_funding_history(
    symbol: Symbol,
    start: datetime,
    end: datetime,
) -> List[ConnectorFundingRate]:
    api_symbol = _resolve_api_symbol(symbol)
    params = {
        "symbol": api_symbol,
        "start": int(start.timestamp() * 1000),
        "end": int(end.timestamp() * 1000),
    }
    funding: List[ConnectorFundingRate] = []

    async with httpx.AsyncClient(timeout=_DEFAULT_TIMEOUT, proxies=_PROXIES) as client:
        response = await _signed_get(client, _MEXC_FUNDING, _PATH_FUNDING, params=params)
        response.raise_for_status()
        payload = response.json()
    data = payload.get("data", []) if isinstance(payload, dict) else []
    for entry in data:
        if not isinstance(entry, dict):
            continue
        try:
            ts = datetime.fromtimestamp(int(entry.get("fundingTime")) / 1000, tz=timezone.utc)
            rate = float(entry.get("fundingRate"))
        except (TypeError, ValueError):
            continue
        if ts < start or ts >= end:
            continue
        interval = entry.get("fundingInterval") or _FUNDING_INTERVAL
        funding.append(ConnectorFundingRate(timestamp=ts, rate=rate, interval=str(interval)))
    funding.sort(key=lambda f: f.timestamp)
    return funding


__all__ = [
    "get_mexc_contracts",
    "get_mexc_taker_fee",
    "get_mexc_historical_quotes",
    "get_mexc_funding_history",
]
