"""Exchange taker fee discovery."""

from __future__ import annotations

from typing import Optional

import httpx

from ..domain import ExchangeName

_BINANCE_INFO = "https://fapi.binance.com/fapi/v1/exchangeInfo"
_BYBIT_FEE = "https://api.bybit.com/v5/market/instruments-info?category=linear"
_MEXC_FEE = "https://contract.mexc.com/api/v1/contract/detail"
_BINGX_FEE = "https://open-api.bingx.com/openApi/swap/v3/market/getAllContracts"
_GATE_FEE = "https://api.gateio.ws/api/v4/futures/usdt/contracts"

_DEFAULT_FEE = 0.0006


async def fetch_taker_fee(
    exchange: ExchangeName,
    *,
    client: httpx.AsyncClient,
) -> Optional[float]:
    ex = str(exchange).lower()
    if ex == "binance":
        return await _fetch_binance_fee(client)
    if ex == "bybit":
        return await _fetch_bybit_fee(client)
    if ex == "mexc":
        return await _fetch_mexc_fee(client)
    if ex == "bingx":
        return await _fetch_bingx_fee(client)
    if ex == "gate":
        return await _fetch_gate_fee(client)
    return None


async def _fetch_binance_fee(client: httpx.AsyncClient) -> Optional[float]:
    resp = await client.get(_BINANCE_INFO)
    resp.raise_for_status()
    payload = resp.json()
    try:
        return float(payload.get("takerCommission") or 0.0005)
    except Exception:
        return 0.0005


async def _fetch_bybit_fee(client: httpx.AsyncClient) -> Optional[float]:
    resp = await client.get(_BYBIT_FEE)
    resp.raise_for_status()
    payload = resp.json()
    try:
        items = payload.get("result", {}).get("list", [])
        if not items:
            return _DEFAULT_FEE
        sample = items[0]
        return float(sample.get("takerFee") or sample.get("takerCommission") or _DEFAULT_FEE)
    except Exception:
        return _DEFAULT_FEE


async def _fetch_mexc_fee(client: httpx.AsyncClient) -> Optional[float]:
    resp = await client.get(_MEXC_FEE)
    resp.raise_for_status()
    payload = resp.json()
    try:
        data = payload.get("data") or []
        if not data:
            return _DEFAULT_FEE
        sample = data[0]
        return float(sample.get("takerFeeRate") or sample.get("takerFee") or _DEFAULT_FEE)
    except Exception:
        return _DEFAULT_FEE


async def _fetch_bingx_fee(client: httpx.AsyncClient) -> Optional[float]:
    resp = await client.get(_BINGX_FEE)
    resp.raise_for_status()
    payload = resp.json()
    try:
        data = payload.get("data") or []
        if not data:
            return _DEFAULT_FEE
        sample = data[0]
        fee_raw = sample.get("takerRate") or sample.get("takerFee") or _DEFAULT_FEE
        return float(fee_raw)
    except Exception:
        return _DEFAULT_FEE


async def _fetch_gate_fee(client: httpx.AsyncClient) -> Optional[float]:
    resp = await client.get(_GATE_FEE)
    resp.raise_for_status()
    payload = resp.json()
    if isinstance(payload, dict):
        payload = payload.get("data") or []
    try:
        sample = payload[0]
    except Exception:
        return _DEFAULT_FEE
    try:
        fee_raw = sample.get("taker_rate") or sample.get("takerFee") or sample.get("taker")
        if fee_raw is None:
            return _DEFAULT_FEE
        return float(fee_raw)
    except Exception:
        return _DEFAULT_FEE
