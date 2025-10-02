from __future__ import annotations
import httpx
from dataclasses import dataclass
from typing import Dict, Iterable, List, Sequence, Set, Tuple

from .base import ConnectorSpec
from .bingx_utils import normalize_bingx_symbol
from ..domain import ExchangeName, Symbol

BINANCE_EXCHANGE_INFO = "https://fapi.binance.com/fapi/v1/exchangeInfo"
BYBIT_INSTRUMENTS = "https://api.bybit.com/v5/market/instruments-info?category=linear&limit=1000"
BINGX_TICKER_ENDPOINTS: Tuple[str, ...] = (
    "https://bingx.com/api/v3/contract/tickers",
    "https://open-api.bingx.com/openApi/swap/v2/market/getLatest",
    "https://open-api.bingx.com/openApi/swap/v3/market/getLatest",
    "https://open-api.bingx.com/openApi/swap/v3/market/getAllLatest",
)
BINGX_HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://bingx.com/",
    "Origin": "https://bingx.com",
}
MEXC_CONTRACTS = "https://contract.mexc.com/api/v1/contract/detail"

async def discover_binance_usdt_perp() -> Set[str]:
    async with httpx.AsyncClient(timeout=20) as client:
        r = await client.get(BINANCE_EXCHANGE_INFO)
        r.raise_for_status()
        data = r.json()
    out: Set[str] = set()
    for s in data.get("symbols", []):
        if s.get("contractType") == "PERPETUAL" and s.get("quoteAsset") == "USDT" and s.get("status") == "TRADING":
            sym = s.get("symbol")
            if sym: out.add(sym)
    return out

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


def _bingx_symbol_to_common(symbol: str | None) -> str | None:
    return normalize_bingx_symbol(symbol)


async def _fetch_bingx_payload(client: httpx.AsyncClient) -> Iterable:
    param_candidates: Sequence[dict[str, str] | None] = (
        {"symbol": "ALL"},
        {"pair": "ALL"},
        None,
    )

    for url in BINGX_TICKER_ENDPOINTS:
        for params in param_candidates:
            try:
                response = await client.get(url, params=params)
                response.raise_for_status()
            except httpx.HTTPStatusError:
                continue
            except Exception:
                break

            try:
                payload = response.json()
            except Exception:
                continue

            items = _extract_bingx_items(payload)
            if items:
                return items

    return []


def _extract_bingx_items(payload) -> Iterable:
    if isinstance(payload, dict):
        for key in ("data", "result", "tickers", "items", "rows", "list", "dataList"):
            value = payload.get(key)
            if isinstance(value, list):
                return value
            if isinstance(value, dict):
                return value.values()
        return payload.values()
    if isinstance(payload, list):
        return payload
    return []


async def discover_bingx_usdt_perp() -> Set[str]:
    async with httpx.AsyncClient(timeout=20, headers=BINGX_HEADERS) as client:
        items = await _fetch_bingx_payload(client)

    out: Set[str] = set()
    for item in items:
        if not isinstance(item, dict):
            continue
        raw = item.get("symbol") or item.get("market") or item.get("instId")
        common = _bingx_symbol_to_common(str(raw) if raw else None)
        if not common:
            continue
        if not common.endswith("USDT"):
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
