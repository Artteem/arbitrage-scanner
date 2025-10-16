from __future__ import annotations

from datetime import datetime, timedelta, timezone
import logging
from typing import Dict, List

import httpx

from .base import ConnectorContract, ConnectorFundingRate, ConnectorQuote
from .normalization import normalize_gate_symbol
from ..domain import Symbol

logger = logging.getLogger(__name__)

_GATE_CONTRACTS = "https://api.gateio.ws/api/v4/futures/usdt/contracts"
_GATE_CANDLES = "https://api.gateio.ws/api/v4/futures/usdt/candlesticks"
_GATE_FUNDING = "https://api.gateio.ws/api/v4/futures/usdt/funding_rate"
_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Origin": "https://www.gate.io",
    "Referer": "https://www.gate.io/",
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
    sym = str(symbol).upper()
    if sym.endswith("USDT"):
        return f"{sym[:-4]}_USDT"
    return sym


async def get_gate_contracts() -> List[ConnectorContract]:
    async with httpx.AsyncClient(timeout=_DEFAULT_TIMEOUT, headers=_HEADERS) as client:
        response = await client.get(_GATE_CONTRACTS)
        response.raise_for_status()
        payload = response.json()

    contracts: List[ConnectorContract] = []
    taker_fees: Dict[Symbol, float] = {}
    for item in payload if isinstance(payload, list) else []:
        if not isinstance(item, dict):
            continue
        if bool(item.get("is_delisted")):
            continue
        state = str(item.get("state") or item.get("status") or "").lower()
        if state and state not in {"trading", "open", "live"}:
            continue
        quote = str(item.get("quote") or item.get("quanto_collateral") or "USDT").upper()
        if quote != "USDT":
            continue
        symbol_raw = str(item.get("name") or item.get("contract") or "").upper()
        normalized = normalize_gate_symbol(symbol_raw)
        if not normalized:
            continue
        taker_fee_value: float | None = None
        taker_fee_raw = item.get("taker_fee") or item.get("taker_fee_rate")
        try:
            if taker_fee_raw is not None:
                taker_fee_value = float(taker_fee_raw)
                taker_fees[normalized] = taker_fee_value
        except (TypeError, ValueError):
            taker_fee_value = None
        try:
            tick_size = float(item.get("order_price_round")) if item.get("order_price_round") else None
        except (TypeError, ValueError):
            tick_size = None
        try:
            lot_size = float(item.get("order_size_round")) if item.get("order_size_round") else None
        except (TypeError, ValueError):
            lot_size = None
        try:
            contract_size = float(item.get("quanto_multiplier")) if item.get("quanto_multiplier") else None
        except (TypeError, ValueError):
            contract_size = None
        contract = ConnectorContract(
            original_symbol=symbol_raw,
            normalized_symbol=normalized,
            base_asset=str(item.get("base") or "").upper(),
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


async def get_gate_taker_fee(symbol: Symbol) -> float | None:
    normalized = Symbol(str(symbol).upper())
    fee = _TAKER_FEES.get(normalized)
    if fee is not None:
        return fee
    return None


async def get_gate_historical_quotes(
    symbol: Symbol,
    start: datetime,
    end: datetime,
    interval: timedelta,
) -> List[ConnectorQuote]:
    api_symbol = _resolve_api_symbol(symbol)
    params = {
        "contract": api_symbol,
        "interval": "1m",
        "from": int(start.timestamp()),
        "to": int(end.timestamp()),
    }
    quotes: List[ConnectorQuote] = []

    async with httpx.AsyncClient(timeout=_DEFAULT_TIMEOUT, headers=_HEADERS) as client:
        response = await client.get(_GATE_CANDLES, params=params)
        response.raise_for_status()
        data = response.json()
    if not isinstance(data, list):
        return quotes
    for entry in data:
        if not isinstance(entry, (list, tuple)) or len(entry) < 5:
            continue
        try:
            ts = datetime.fromtimestamp(float(entry[0]), tz=timezone.utc)
            high_price = float(entry[2])
            low_price = float(entry[3])
        except (TypeError, ValueError):
            continue
        if ts < start or ts >= end:
            continue
        if low_price <= 0 or high_price <= 0:
            continue
        quotes.append(ConnectorQuote(timestamp=ts, bid=low_price, ask=high_price))
    quotes.sort(key=lambda q: q.timestamp)
    return quotes


async def get_gate_funding_history(
    symbol: Symbol,
    start: datetime,
    end: datetime,
) -> List[ConnectorFundingRate]:
    api_symbol = _resolve_api_symbol(symbol)
    params = {
        "contract": api_symbol,
        "limit": 1000,
        "from": int(start.timestamp()),
        "to": int(end.timestamp()),
    }
    funding: List[ConnectorFundingRate] = []

    async with httpx.AsyncClient(timeout=_DEFAULT_TIMEOUT, headers=_HEADERS) as client:
        response = await client.get(_GATE_FUNDING, params=params)
        response.raise_for_status()
        data = response.json()
    if not isinstance(data, list):
        return funding
    for entry in data:
        if not isinstance(entry, dict):
            continue
        try:
            ts = datetime.fromtimestamp(float(entry.get("t")), tz=timezone.utc)
        except (TypeError, ValueError):
            continue
        if ts < start or ts >= end:
            continue
        try:
            rate = float(entry.get("r"))
        except (TypeError, ValueError):
            continue
        interval = entry.get("interval") or _FUNDING_INTERVAL
        funding.append(ConnectorFundingRate(timestamp=ts, rate=rate, interval=str(interval)))
    funding.sort(key=lambda f: f.timestamp)
    return funding


__all__ = [
    "get_gate_contracts",
    "get_gate_taker_fee",
    "get_gate_historical_quotes",
    "get_gate_funding_history",
]
