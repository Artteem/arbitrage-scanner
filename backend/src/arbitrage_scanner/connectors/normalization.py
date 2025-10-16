from __future__ import annotations

from ..domain import Symbol

_QUOTE_TOKENS: tuple[str, ...] = (
    "USDT",
    "USDC",
    "BUSD",
    "FDUSD",
    "USD",
    "USDD",
    "DAI",
)


def _strip_separators(value: str) -> str:
    return value.replace("/", "").replace("-", "").replace("_", "")


def normalize_binance_symbol(symbol: Symbol | str | None) -> Symbol | None:
    if symbol is None:
        return None
    raw = str(symbol).strip().upper()
    if not raw:
        return None
    return Symbol(_strip_separators(raw))


def normalize_bybit_symbol(symbol: Symbol | str | None) -> Symbol | None:
    if symbol is None:
        return None
    raw = str(symbol).strip().upper()
    if not raw:
        return None
    normalized = _strip_separators(raw)
    if normalized.endswith("PERP"):
        normalized = normalized[:-4]
    return Symbol(normalized)


def normalize_bingx_symbol(symbol: Symbol | str | None) -> Symbol | None:
    if symbol is None:
        return None
    raw = str(symbol).strip().upper()
    if not raw:
        return None

    # BingX может возвращать обозначения с разными разделителями и служебными суффиксами.
    collapsed = raw.replace("/", "").replace("-", "")
    if "_" in collapsed:
        prefix, *_rest = collapsed.split("_", 1)
        if prefix:
            collapsed = prefix
    collapsed = collapsed.replace("_", "")
    if not collapsed:
        return None

    for quote in _QUOTE_TOKENS:
        idx = collapsed.find(quote)
        if idx > 0:
            base = collapsed[:idx]
            if base:
                return Symbol(base + quote)
    return Symbol(collapsed)


def normalize_mexc_symbol(symbol: Symbol | str | None) -> Symbol | None:
    if symbol is None:
        return None
    raw = str(symbol).strip().upper()
    if not raw:
        return None
    collapsed = raw.replace("/", "").replace("-", "")
    if "_" in collapsed:
        parts = [part for part in collapsed.split("_") if part]
        collapsed = "".join(parts)
    return Symbol(collapsed)


def normalize_gate_symbol(symbol: Symbol | str | None) -> Symbol | None:
    if symbol is None:
        return None
    raw = str(symbol).strip().upper()
    if not raw:
        return None
    if raw.endswith(".P"):
        raw = raw[:-2]
    collapsed = raw.replace("/", "").replace("-", "")
    if "_" in collapsed:
        parts = [part for part in collapsed.split("_") if part]
        collapsed = "".join(parts)
    return Symbol(collapsed)


__all__ = [
    "normalize_binance_symbol",
    "normalize_bybit_symbol",
    "normalize_bingx_symbol",
    "normalize_mexc_symbol",
    "normalize_gate_symbol",
]
