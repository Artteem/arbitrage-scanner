from __future__ import annotations
import httpx
from typing import Iterable, List, Set

from .base import ConnectorSpec

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

async def discover_common_symbols(connectors: Iterable[ConnectorSpec]) -> List[str]:
    """Вернуть отсортированное пересечение тикеров для всех коннекторов."""

    discovered: List[Set[str]] = []
    for connector in connectors:
        if connector.discover_symbols is None:
            continue
        symbols = await connector.discover_symbols()
        discovered.append({str(sym) for sym in symbols})

    if not discovered:
        return []

    common = set.intersection(*discovered)
    return sorted(common)
