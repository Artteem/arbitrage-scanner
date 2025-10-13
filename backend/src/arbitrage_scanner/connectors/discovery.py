from __future__ import annotations
import httpx
from dataclasses import dataclass
from typing import Dict, Iterable, List, Set

from .base import ConnectorSpec
from .bingx_utils import normalize_bingx_symbol
from ..domain import ExchangeName, Symbol

BINANCE_EXCHANGE_INFO = "https://fapi.binance.com/fapi/v1/exchangeInfo"
BINANCE_ACTIVE_STATUSES = {"LISTING"}


def _is_binance_trading_status(value) -> bool:
    if value is None:
        return False
    status = str(value).strip().upper()
    if not status:
        return False
    if status.endswith("TRADING"):
        return True
    return status in BINANCE_ACTIVE_STATUSES
BYBIT_INSTRUMENTS = "https://api.bybit.com/v5/market/instruments-info?category=linear&limit=1000"
BINGX_CONTRACTS = "https://open-api.bingx.com/openApi/swap/v3/market/getAllContracts"
MEXC_CONTRACTS = "https://contract.mexc.com/api/v1/contract/detail"

async def discover_binance_usdt_perp() -> Set[str]:
    async with httpx.AsyncClient(timeout=20) as client:
        r = await client.get(BINANCE_EXCHANGE_INFO)
        r.raise_for_status()
        data = r.json()
    out: Set[str] = set()
    for s in data.get("symbols", []):
        if (
            s.get("contractType") == "PERPETUAL"
            and s.get("quoteAsset") == "USDT"
            and _is_binance_trading_status(s.get("status"))
        ):
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
        r = await client.get(BINGX_CONTRACTS)
        r.raise_for_status()
        payload = r.json()

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

        raw_symbol = (
            item.get("symbol")
            or item.get("tradingPair")
            or item.get("instId")
            or item.get("contractId")
        )
        common = _bingx_symbol_to_common(str(raw_symbol) if raw_symbol else None)
        if not common:
            continue

        quote_candidates = (
            item.get("quoteAsset"),
            item.get("quoteCurrency"),
            item.get("quoteCoin"),
            item.get("settleAsset"),
            item.get("settleCurrency"),
            item.get("marginCoin"),
        )
        if not any(_is_usdt_quote(candidate) for candidate in quote_candidates):
            if not common.upper().endswith("USDT"):
                continue

        if not _is_perpetual_contract(
            item.get("contractType")
            or item.get("type")
            or item.get("productType")
        ):
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
