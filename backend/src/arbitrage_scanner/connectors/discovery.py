"""Discovery module for arbitrage scanner with improved exchange endpoints.

This file contains asynchronous functions used to fetch and discover the list of
USDT‑settled perpetual contracts from various exchanges. Compared to the
original project version, it includes fallback logic for BingX and MEXC
discoveries.  Some exchanges occasionally change their REST endpoints or
response structures; a single hard‑coded URL may return an empty payload even
though the exchange still supports the API.  To make the discovery more robust,
we attempt multiple endpoints in order and return the first non‑empty result.

Key changes:

* **BingX:** The original endpoint `openApi/swap/v2/market/getAllContracts` can
  return an empty response in some regions.  A newer endpoint,
  `openApi/swap/v2/quote/contracts`, appears to provide the same data and is
  currently documented on BingX’s API site【566159644440243†L178-L184】.  The
  discovery now tries the primary endpoint and falls back to the quote endpoint
  if no contracts are found.

* **MEXC:** Although `https://contract.mexc.com/api/v1/contract/detail` should
  return all contract details, some users have reported that the endpoint
  occasionally times out or returns zero items.  As a fallback, we support
  `https://contract.mexc.com/api/v1/contract/contractInfos`, which returns
  similar data but with slightly different field names.  The discovery
  function now tries the detail endpoint first and falls back to contractInfos.

These changes ensure that discovery does not silently fail and the scanner can
subscribe to markets on all supported exchanges.  The rest of the module is
copied verbatim from the project’s original `discovery.py` with minimal edits
for clarity and maintainability.
"""

from __future__ import annotations

import httpx
from collections import deque
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Sequence, Set

from .base import ConnectorContract, ConnectorSpec
from .bingx_utils import normalize_bingx_symbol
from ..domain import ExchangeName, Symbol
from ..settings import settings  # type: ignore

# ---------------------------------------------------------------------------
# Exchange endpoints
#
# Binance and Bybit endpoints are kept unchanged from the original code.  For
# BingX and MEXC, additional constants are defined for alternate endpoints.

BINANCE_EXCHANGE_INFO = "https://fapi.binance.com/fapi/v1/exchangeInfo"
BINANCE_PREMIUM_INDEX = "https://fapi.binance.com/fapi/v1/premiumIndex"
BINANCE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    "Origin": "https://www.binance.com",
    "Referer": "https://www.binance.com/",
}
_BINANCE_EXPECTED_MIN = 50
BYBIT_INSTRUMENTS = "https://api.bybit.com/v5/market/instruments-info?category=linear&limit=1000"

# BingX: primary and fallback endpoints
BINGX_CONTRACTS_PRIMARY = "https://open-api.bingx.com/openApi/swap/v2/market/getAllContracts"
BINGX_CONTRACTS_QUOTE = "https://open-api.bingx.com/openApi/swap/v2/quote/contracts"

# MEXC: primary and fallback endpoints
MEXC_CONTRACTS_PRIMARY = "https://contract.mexc.com/api/v1/contract/detail"
MEXC_CONTRACTS_ALTERNATE = "https://contract.mexc.com/api/v1/contract/contractInfos"

GATE_CONTRACTS = "https://api.gateio.ws/api/v4/futures/usdt/contracts"
GATE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Origin": "https://www.gate.io",
    "Referer": "https://www.gate.io/",
}

# ---------------------------------------------------------------------------
# Helper functions

_PROXIES = settings.httpx_proxies

def _get_client_params(
    timeout: float = 20.0,
    headers: dict | None = None,
    http2: bool = False,
) -> dict[str, Any]:
    params: dict[str, Any] = {"timeout": httpx.Timeout(timeout)}
    if headers:
        params["headers"] = headers
    if http2:
        params["http2"] = http2
    if _PROXIES:
        params["proxies"] = _PROXIES
    return params

def _extract_binance_perpetuals(payload: dict) -> Set[str]:
    out: Set[str] = set()
    for item in payload.get("symbols", []) or []:
        if not isinstance(item, dict):
            continue
        if item.get("status") != "TRADING":
            continue
        if item.get("contractType") != "PERPETUAL":
            continue
        if item.get("quoteAsset") != "USDT":
            continue
        symbol = item.get("symbol")
        if symbol:
            out.add(str(symbol))
    return out

async def _discover_binance_from_premium_index(client: httpx.AsyncClient) -> Set[str]:
    response = await client.get(BINANCE_PREMIUM_INDEX)
    response.raise_for_status()
    payload = response.json()
    if isinstance(payload, dict):
        items = payload.get("data") or payload.get("symbols") or []
    else:
        items = payload
    out: Set[str] = set()
    for entry in items or []:
        if not isinstance(entry, dict):
            continue
        symbol = entry.get("symbol")
        if not symbol:
            continue
        candidate = str(symbol).upper()
        if candidate.endswith("USDT"):
            out.add(candidate)
    return out

async def discover_binance_usdt_perp() -> Set[str]:
    primary: Set[str] = set()
    primary_error: Exception | None = None
    client_params = _get_client_params(timeout=20.0, headers=BINANCE_HEADERS, http2=True)
    async with httpx.AsyncClient(**client_params) as client:
        try:
            response = await client.get(BINANCE_EXCHANGE_INFO)
            response.raise_for_status()
            payload = response.json()
            primary = _extract_binance_perpetuals(payload)
        except Exception as exc:
            primary_error = exc

        if len(primary) >= _BINANCE_EXPECTED_MIN:
            return primary

        fallback: Set[str] = set()
        fallback_error: Exception | None = None
        try:
            fallback = await _discover_binance_from_premium_index(client)
        except Exception as exc:
            fallback_error = exc
        if fallback:
            return primary | fallback if primary else fallback
        if primary:
            return primary
        if primary_error:
            raise primary_error
        if fallback_error:
            raise fallback_error
        return set()

async def discover_bybit_linear_usdt() -> Set[str]:
    client_params = _get_client_params(timeout=20.0)
    async with httpx.AsyncClient(**client_params) as client:
        r = await client.get(BYBIT_INSTRUMENTS)
        r.raise_for_status()
        data = r.json()
    out: Set[str] = set()
    items = (data.get("result") or {}).get("list") or []
    for it in items:
        if it.get("quoteCoin") == "USDT" and str(it.get("status")).lower().startswith("trading"):
            sym = it.get("symbol")
            if sym:
                out.add(sym)
    return out

# MEXC helpers
def _mexc_symbol_to_common(symbol: str | None) -> str | None:
    if not symbol:
        return None
    return symbol.replace("_", "")

def _is_trading_state(state: str) -> bool:
    if not state:
        return True
    st = state.strip().lower()
    return st in {"1", "2", "trading", "online", "open"}

def _is_perpetual(kind: str) -> bool:
    if not kind:
        return True
    k = kind.strip().lower()
    return "perpetual" in k or "swap" in k

async def _fetch_mexc_contracts(url: str, client: httpx.AsyncClient) -> Set[str]:
    r = await client.get(url)
    r.raise_for_status()
    data = r.json()
    out: Set[str] = set()
    # Some endpoints return data under "data", others under "result".
    items = data.get("data") or data.get("result") or []
    for item in items:
        sym = _mexc_symbol_to_common(item.get("symbol") or item.get("symbolName"))
        quote = str(
            item.get("quoteCurrency")
            or item.get("quoteCoin")
            or item.get("settleCurrency")
            or item.get("settlementCurrency")
            or item.get("baseCoin")
            or ""
        ).upper()
        if quote != "USDT":
            continue
        if not _is_perpetual(str(item.get("contractType") or item.get("type") or item.get("prodType") or "")):
            continue
        if not _is_trading_state(str(item.get("state") or item.get("status") or item.get("tradingStatus") or "")):
            continue
        if sym:
            out.add(sym)
    return out

async def discover_mexc_usdt_perp() -> Set[str]:
    # Try primary endpoint first, then fallback
    client_params = _get_client_params(timeout=20.0)
    async with httpx.AsyncClient(**client_params) as client:
        out = set()
        errors: List[Exception] = []
        for url in (MEXC_CONTRACTS_PRIMARY, MEXC_CONTRACTS_ALTERNATE):
            try:
                out = await _fetch_mexc_contracts(url, client)
            except Exception as exc:
                errors.append(exc)
                out = set()
            if out:
                break
        if out:
            return out
        # If both requests failed, rethrow the first error
        if errors:
            raise errors[0]
        return set()

# Gate helpers
def _gate_symbol_to_common(symbol: str | None) -> str | None:
    if not symbol:
        return None
    sym = str(symbol).strip()
    if not sym:
        return None
    sym = sym.replace("-", "_")
    if sym.count("_") > 1:
        return None
    return sym.replace("_", "")

def _gate_is_active_contract(item: dict) -> bool:
    state = str(item.get("state") or item.get("status") or "").strip().lower()
    if state and state not in {"open", "trading", "live"}:
        return False
    if bool(item.get("is_delisted")):
        return False
    in_delisting = item.get("in_delisting")
    if isinstance(in_delisting, str):
        if in_delisting.strip().lower() in {"true", "1"}:
            return False
    elif in_delisting:
        return False
    return True

async def discover_gate_usdt_perp() -> Set[str]:
    client_params = _get_client_params(timeout=20.0, headers=GATE_HEADERS)
    async with httpx.AsyncClient(**client_params) as client:
        response = await client.get(GATE_CONTRACTS)
        response.raise_for_status()
        payload = response.json()
    if isinstance(payload, dict):
        items = None
        for key in ("data", "contracts", "items", "result"):
            val = payload.get(key)
            if isinstance(val, list):
                items = val
                break
        if items is None:
            items = []
    else:
        items = payload if isinstance(payload, list) else []
    out: Set[str] = set()
    for item in items:
        if not isinstance(item, dict):
            continue
        contract = item.get("contract") or item.get("name") or item.get("symbol")
        if not contract:
            continue
        if not _gate_is_active_contract(item):
            continue
        if str(contract).count("_") > 1:
            continue
        sym_common = _gate_symbol_to_common(contract)
        if sym_common:
            out.add(sym_common)
    return out

# BingX helpers
def _bingx_symbol_to_common(symbol: str | None) -> str | None:
    return normalize_bingx_symbol(symbol)

def _is_usdt_quote(candidate) -> bool:
    if candidate is None:
        return False
    return str(candidate).upper() == "USDT"

def _is_perpetual_contract(value) -> bool:
    if value is None:
        return True
    normalized = str(value).strip().lower()
    if not normalized:
        return True
    return any(key in normalized for key in ("perp", "perpetual", "swap"))

def _iter_bingx_contract_entries(payload) -> Iterable[dict]:
    queue: deque = deque([payload])
    while queue:
        current = queue.popleft()
        if isinstance(current, dict):
            if _looks_like_bingx_contract(current):
                yield current
            for value in current.values():
                if isinstance(value, (list, tuple, set)):
                    queue.extend(value)
                elif isinstance(value, dict):
                    queue.append(value)
        elif isinstance(current, (list, tuple, set)):
            queue.extend(current)

def _looks_like_bingx_contract(item: dict) -> bool:
    symbol_keys: Sequence[str] = (
        "symbol",
        "tradingPair",
        "instId",
        "contractId",
        "pair",
        "name",
        "symbolName",
    )
    quote_keys: Sequence[str] = (
        "quoteAsset",
        "quoteCurrency",
        "quoteCoin",
        "quote",
        "quoteAssetName",
        "settleAsset",
        "settleCurrency",
    )
    if not any(item.get(key) for key in symbol_keys):
        return False
    if not any(item.get(key) for key in quote_keys):
        return False
    return True

async def _fetch_bingx_contracts(url: str, client: httpx.AsyncClient) -> Set[str]:
    response = await client.get(url)
    response.raise_for_status()
    payload = response.json()
    out: Set[str] = set()
    for item in _iter_bingx_contract_entries(payload):
        if not isinstance(item, dict):
            continue
        quote_asset = (
            item.get("quoteAsset")
            or item.get("quoteCurrency")
            or item.get("quoteCoin")
            or item.get("quote")
            or item.get("settleAsset")
            or item.get("settleCurrency")
        )
        if not _is_usdt_quote(quote_asset):
            continue
        raw_symbol = (
            item.get("symbol")
            or item.get("tradingPair")
            or item.get("instId")
            or item.get("contractId")
            or item.get("pair")
            or item.get("name")
        )
        common = _bingx_symbol_to_common(str(raw_symbol) if raw_symbol else None)
        if not common:
            continue
        if not _is_perpetual_contract(
            item.get("contractType")
            or item.get("type")
            or item.get("productType")
        ):
            continue
        if not _is_trading_state(item.get("state") or item.get("status") or item.get("tradingStatus") or ""):
            continue
        out.add(common)
    return out

async def discover_bingx_usdt_perp() -> Set[str]:
    # Try primary and fallback endpoints sequentially
    headers = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Referer": "https://bingx.com/",
        "Origin": "https://bingx.com",
    }
    client_params = _get_client_params(timeout=20.0, headers=headers)
    async with httpx.AsyncClient(**client_params) as client:
        out = set()
        errors: List[Exception] = []
        for url in (BINGX_CONTRACTS_PRIMARY, BINGX_CONTRACTS_QUOTE):
            try:
                out = await _fetch_bingx_contracts(url, client)
            except Exception as exc:
                errors.append(exc)
                out = set()
            if out:
                break
        if out:
            return out
        if errors:
            raise errors[0]
        return set()

@dataclass(frozen=True)
class DiscoveryResult:
    """Result of auto-discovery for connectors.

    Attributes:
        symbols_union: Normalized symbol union across exchanges.
        per_connector: Mapping from exchange name to its list of normalized symbols.
        native_per_connector: Mapping from exchange name to the native (REST) symbols.
        native_to_normalized: Mapping from native symbol to normalized symbol per exchange.
    """

    symbols_union: List[Symbol]
    per_connector: Dict[ExchangeName, List[Symbol]]
    native_per_connector: Dict[ExchangeName, List[str]]
    native_to_normalized: Dict[ExchangeName, Dict[str, Symbol]]


def _extract_symbol_sets(
    contracts: Sequence[ConnectorContract],
) -> tuple[List[Symbol], List[str], Dict[str, Symbol]]:
    native_seen: set[str] = set()
    normalized_seen: set[Symbol] = set()
    normalized_list: List[Symbol] = []
    native_list: List[str] = []
    mapping: Dict[str, Symbol] = {}
    for contract in contracts:
        native = str(contract.original_symbol).strip()
        normalized = str(contract.normalized_symbol).strip().upper()
        if not native or not normalized:
            continue
        native_upper = native.upper()
        normalized_symbol = Symbol(normalized)
        mapping[native_upper] = normalized_symbol
        if native_upper not in native_seen:
            native_seen.add(native_upper)
            native_list.append(native_upper)
        if normalized_symbol not in normalized_seen:
            normalized_seen.add(normalized_symbol)
            normalized_list.append(normalized_symbol)
    return normalized_list, native_list, mapping


async def discover_symbols_for_connectors(
    connectors: Iterable[ConnectorSpec],
) -> DiscoveryResult:
    """Collect contract metadata for each connector using REST listings."""

    per_connector: Dict[ExchangeName, List[Symbol]] = {}
    native_per_connector: Dict[ExchangeName, List[str]] = {}
    native_to_normalized: Dict[ExchangeName, Dict[str, Symbol]] = {}

    for connector in connectors:
        contracts: Sequence[ConnectorContract] = []
        if connector.fetch_contracts is not None:
            try:
                contracts = await connector.fetch_contracts()
            except Exception:
                contracts = []
        if not contracts and connector.discover_symbols is not None:
            try:
                discovered = await connector.discover_symbols()
            except Exception:
                discovered = []
            contracts = [
                ConnectorContract(
                    original_symbol=str(sym),
                    normalized_symbol=Symbol(str(sym)),
                    base_asset="",
                    quote_asset="",
                )
                for sym in discovered
                if str(sym)
            ]

        if not contracts:
            continue

        normalized_list, native_list, mapping = _extract_symbol_sets(contracts)
        if not normalized_list or not native_list:
            continue
        per_connector[connector.name] = sorted(normalized_list)
        native_per_connector[connector.name] = sorted(native_list)
        native_to_normalized[connector.name] = mapping

    if not per_connector:
        return DiscoveryResult(
            symbols_union=[],
            per_connector={},
            native_per_connector={},
            native_to_normalized={},
        )

    union: set[Symbol] = set()
    for symbols in per_connector.values():
        union.update(symbols)

    return DiscoveryResult(
        symbols_union=sorted(union),
        per_connector={name: sorted(symbols) for name, symbols in per_connector.items()},
        native_per_connector={
            name: sorted(symbols) for name, symbols in native_per_connector.items()
        },
        native_to_normalized={
            name: dict(mapping) for name, mapping in native_to_normalized.items()
        },
    )


async def discover_common_symbols(
    connectors: Iterable[ConnectorSpec],
    *,
    prefetched: DiscoveryResult | None = None,
) -> List[str]:
    """Return the sorted intersection of normalized symbols across connectors."""

    result = prefetched or await discover_symbols_for_connectors(connectors)
    if not result.per_connector:
        return []
    sets = [set(items) for items in result.per_connector.values() if items]
    if not sets:
        return []
    common = set.intersection(*sets)
    return sorted(common)