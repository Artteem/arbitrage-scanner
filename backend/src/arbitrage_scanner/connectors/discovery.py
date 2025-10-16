from __future__ import annotations
import httpx
from dataclasses import dataclass
from typing import Dict, Iterable, List, Set

from .base import ConnectorSpec
from .bingx_utils import normalize_bingx_symbol
from ..domain import ExchangeName, Symbol

BINANCE_EXCHANGE_INFO = "https://fapi.binance.com/fapi/v1/exchangeInfo"
BINANCE_PREMIUM_INDEX = "https://fapi.binance.com/fapi/v1/premiumIndex"
BINANCE_HEADERS = {
    # Binance начала более агрессивно отсеивать запросы без заголовка User-Agent,
    # поэтому явно укажем типичный браузерный агент, чтобы не получать 403.
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
BINGX_CONTRACTS = "https://open-api.bingx.com/openApi/swap/v3/market/getAllContracts"
MEXC_CONTRACTS = "https://contract.mexc.com/api/v1/contract/detail"
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
    async with httpx.AsyncClient(
        timeout=httpx.Timeout(20.0, connect=10.0),
        headers=BINANCE_HEADERS,
        http2=True,
    ) as client:
        try:
            response = await client.get(BINANCE_EXCHANGE_INFO)
            response.raise_for_status()
            payload = response.json()
            primary = _extract_binance_perpetuals(payload)
        except Exception as exc:  # noqa: BLE001 - propagate only if fallback fails
            primary_error = exc

        if len(primary) >= _BINANCE_EXPECTED_MIN:
            return primary

        fallback: Set[str] = set()
        fallback_error: Exception | None = None
        try:
            fallback = await _discover_binance_from_premium_index(client)
        except Exception as exc:  # noqa: BLE001
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
    async with httpx.AsyncClient(timeout=20) as client:
        r = await client.get(BYBIT_INSTRUMENTS)
        r.raise_for_status()
        data = r.json()
    out: Set[str] = set()
    items = (data.get("result") or {}).get("list") or []
    for it in items:
        if it.get("quoteCoin") == "USDT" and str(it.get("status")).lower().startswith("trading"):
            sym = it.get("symbol")
            if sym: out.add(sym)
    return out

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

async def discover_mexc_usdt_perp() -> Set[str]:
    async with httpx.AsyncClient(timeout=20) as client:
        r = await client.get(MEXC_CONTRACTS)
        r.raise_for_status()
        data = r.json()

    out: Set[str] = set()
    for item in data.get("data", []):
        sym = _mexc_symbol_to_common(item.get("symbol"))
        quote = str(
            item.get("quoteCurrency")
            or item.get("quoteCoin")
            or item.get("settleCurrency")
            or item.get("settlementCurrency")
            or ""
        ).upper()
        if quote != "USDT":
            continue
        if not _is_perpetual(str(item.get("contractType") or item.get("type") or "")):
            continue
        if not _is_trading_state(str(item.get("state") or item.get("status") or "")):
            continue
        if sym:
            out.add(sym)
    return out


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
    async with httpx.AsyncClient(timeout=20, headers=GATE_HEADERS) as client:
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


async def discover_bingx_usdt_perp() -> Set[str]:
    headers = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Referer": "https://bingx.com/",
        "Origin": "https://bingx.com",
    }

    async with httpx.AsyncClient(timeout=20, headers=headers) as client:
        response = await client.get(BINGX_CONTRACTS)
        response.raise_for_status()
        payload = response.json()

    items: Iterable = []
    if isinstance(payload, dict):
        for key in ("data", "result", "contracts", "items"):
            val = payload.get(key)
            if isinstance(val, list):
                items = val
                break
            if isinstance(val, dict):
                items = val.values()
                break
        else:
            items = list(payload.values())
    elif isinstance(payload, list):
        items = payload

    out: Set[str] = set()
    for item in items:
        if not isinstance(item, dict):
            continue

        quote_asset = item.get("quoteAsset")
        if not _is_usdt_quote(quote_asset):
            continue

        raw_symbol = (
            item.get("symbol")
            or item.get("market")
            or item.get("tradingPair")
            or item.get("instId")
            or item.get("contractId")
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
        if not _is_trading_state(item.get("state") or item.get("status") or item.get("tradingStatus")):
            continue

        out.add(common)

    return out

@dataclass(frozen=True)
class DiscoveryResult:
    """Результат авто-обнаружения тикеров."""

    symbols_union: List[Symbol]
    per_connector: Dict[ExchangeName, List[Symbol]]


async def discover_symbols_for_connectors(connectors: Iterable[ConnectorSpec]) -> DiscoveryResult:
    """Собрать тикеры USDT-перпетуалов для каждого коннектора.

    Возвращает объединение по всем биржам и словарь вида
    ``{"binance": [...], "bybit": [...]}``.
    """

    discovered: Dict[ExchangeName, Set[Symbol]] = {}
    for connector in connectors:
        if connector.discover_symbols is None:
            continue
        try:
            symbols = await connector.discover_symbols()
        except Exception:
            # Обнаружение тикеров не должно приводить к падению всего приложения —
            # игнорируем временные ошибки конкретной биржи и продолжим с теми
            # результатами, которые удалось получить.
            continue
        symbol_set = {Symbol(str(sym)) for sym in symbols if str(sym)}
        if symbol_set:
            discovered[connector.name] = symbol_set

    if not discovered:
        return DiscoveryResult(symbols_union=[], per_connector={})

    union = sorted(set.union(*discovered.values()))
    per_connector = {name: sorted(values) for name, values in discovered.items()}
    return DiscoveryResult(symbols_union=union, per_connector=per_connector)


async def discover_common_symbols(connectors: Iterable[ConnectorSpec]) -> List[str]:
    """Вернуть отсортированное пересечение тикеров для всех коннекторов."""

    result = await discover_symbols_for_connectors(connectors)
    if not result.per_connector:
        return []

    sets = [set(items) for items in result.per_connector.values() if items]
    if not sets:
        return []

    common = set.intersection(*sets)
    return sorted(common)
