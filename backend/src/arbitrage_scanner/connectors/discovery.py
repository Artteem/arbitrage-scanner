from __future__ import annotations
import httpx
from dataclasses import dataclass
from typing import Dict, Iterable, List, Set

from .base import ConnectorSpec
from ..domain import ExchangeName, Symbol

BINANCE_EXCHANGE_INFO = "https://fapi.binance.com/fapi/v1/exchangeInfo"
BYBIT_INSTRUMENTS = "https://api.bybit.com/v5/market/instruments-info?category=linear&limit=1000"
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
        symbols = await connector.discover_symbols()
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
