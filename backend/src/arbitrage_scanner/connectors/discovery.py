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


async def discover_mexc_usdt_perp() -> Set[str]:
    async with httpx.AsyncClient(timeout=20) as client:
        r = await client.get(MEXC_CONTRACTS)
        r.raise_for_status()
        data = r.json()

    out: Set[str] = set()
    items = data.get("data") or []
    for it in items:
        quote = (it.get("quoteCurrency") or it.get("settleCurrency") or "").upper()
        if quote != "USDT":
            continue

        state = str(it.get("state") or it.get("status") or "").lower()
        if state and state not in {"1", "trading", "online", "enabled", "open", "normal"}:
            continue

        sym = it.get("symbol")
        if not sym:
            continue
        norm = sym.replace("_", "")
        if norm:
            out.add(norm)
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
