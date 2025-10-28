from __future__ import annotations

import logging
from typing import Dict, Tuple

import httpx

from ..settings import settings
from .credentials import get_credentials_provider
from .signing import sign_request

logger = logging.getLogger(__name__)

_AUTH_ENDPOINTS: Dict[str, Tuple[str, str]] = {
    "binance": ("https://fapi.binance.com/fapi/v1/time", "/fapi/v1/time"),
    "bybit": ("https://api.bybit.com/v5/market/time", "/v5/market/time"),
    "mexc": ("https://contract.mexc.com/api/v1/contract/ping", "/api/v1/contract/ping"),
    "gate": ("https://api.gateio.ws/api/v4/futures/usdt/funding_rate", "/api/v4/futures/usdt/funding_rate"),
    "bingx": ("https://open-api.bingx.com/openApi/swap/v2/server/time", "/openApi/swap/v2/server/time"),
}

_TIMEOUT = httpx.Timeout(10.0, connect=5.0, read=10.0, write=10.0)


async def get_auth_statuses() -> Dict[str, str]:
    provider = get_credentials_provider()
    statuses: Dict[str, str] = {}
    exchanges = settings.credentials_env_map.keys()
    if provider is None:
        return {exchange: "ABSENT" for exchange in exchanges}
    for exchange in exchanges:
        creds = provider.get(exchange)
        if not creds:
            statuses[exchange] = "ABSENT"
            continue
        endpoint = _AUTH_ENDPOINTS.get(exchange)
        if not endpoint:
            statuses[exchange] = "OK"
            continue
        url, path = endpoint
        query: dict | None = {}
        try:
            headers, query_string = sign_request(exchange, "GET", path, query, None, creds)
        except Exception:  # noqa: BLE001 - defensive logging
            logger.exception("Failed to sign auth probe for %s", exchange)
            statuses[exchange] = "INVALID"
            continue
        target_url = url
        request_kwargs = {"headers": headers}
        if exchange in {"binance", "bingx", "mexc", "gate"}:
            if query_string:
                target_url = f"{url}?{query_string}"
        elif query_string:
            request_kwargs["params"] = query
        try:
            async with httpx.AsyncClient(timeout=_TIMEOUT, proxies=settings.httpx_proxies) as client:
                response = await client.get(target_url, **request_kwargs)
        except httpx.HTTPError:
            logger.exception("Auth probe failed for %s", exchange)
            statuses[exchange] = "INVALID"
            continue
        status_code = response.status_code
        if status_code in {401, 403}:
            statuses[exchange] = "INVALID"
        elif 200 <= status_code < 300:
            statuses[exchange] = "OK"
        else:
            statuses[exchange] = "INVALID"
    return statuses


def get_rest_limit_modes() -> Dict[str, str]:
    provider = get_credentials_provider()
    exchanges = settings.credentials_env_map.keys()
    if provider is None:
        return {exchange: "public" for exchange in exchanges}
    modes: Dict[str, str] = {}
    for exchange in exchanges:
        creds = provider.get(exchange)
        modes[exchange] = "keyed" if creds else "public"
    return modes


__all__ = ["get_auth_statuses", "get_rest_limit_modes"]
