"""Utilities for normalizing contract symbols across exchanges."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Iterable, Tuple

__all__ = ["normalize_symbol", "split_symbol", "NormalizedSymbol"]

_SEPARATORS_RE = re.compile(r"[^A-Za-z0-9]")
_KNOWN_QUOTES: Tuple[str, ...] = (
    "USDT",
    "USDC",
    "BUSD",
    "USD",
    "USDP",
    "DAI",
    "EUR",
    "BTC",
    "ETH",
)


@dataclass(frozen=True)
class NormalizedSymbol:
    """Represents a normalized perpetual contract symbol."""

    value: str
    base: str
    quote: str

    def as_tuple(self) -> Tuple[str, str, str]:
        return self.value, self.base, self.quote


def _strip_separators(symbol: str) -> str:
    cleaned = _SEPARATORS_RE.sub("", symbol)
    return cleaned.upper()


def _detect_quote(symbol: str) -> tuple[str, str]:
    upper = symbol.upper()
    for quote in _KNOWN_QUOTES:
        if upper.endswith(quote):
            base = upper[: -len(quote)]
            if base:
                return base, quote
    # fallback: assume USDT if nothing matched but string endswith T or D? keep whole
    return upper, ""


def normalize_symbol(raw: str | None) -> str:
    """Normalize a raw symbol string coming from an exchange."""

    if not raw:
        return ""
    value = _strip_separators(str(raw).strip())
    return value


def split_symbol(raw: str | None) -> NormalizedSymbol:
    """Normalize and split the symbol into base and quote assets."""

    norm = normalize_symbol(raw)
    if not norm:
        return NormalizedSymbol("", "", "")
    base, quote = _detect_quote(norm)
    if not quote:
        # default to USDT if nothing detected and the string is long enough
        if norm.endswith("PERP"):
            norm = norm[:-4]
        if norm.endswith("USDT"):
            base = norm[:-4]
            quote = "USDT"
        else:
            base = norm
            quote = "USDT"
    return NormalizedSymbol(norm, base, quote)


def merge_aliases(symbols: Iterable[str]) -> dict[str, set[str]]:
    """Group raw symbols by their normalized representation."""

    mapping: dict[str, set[str]] = {}
    for raw in symbols:
        normalized = normalize_symbol(raw)
        if not normalized:
            continue
        mapping.setdefault(normalized, set()).add(raw)
    return mapping
