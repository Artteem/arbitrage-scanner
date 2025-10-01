from __future__ import annotations

from ..domain import Symbol

_QUOTE_TOKENS: tuple[str, ...] = ("FDUSD", "USDT", "USDC", "BUSD", "USD")


def normalize_bingx_symbol(symbol: Symbol | str | None) -> Symbol | None:
    """Normalize BingX instrument codes to the common ``BASEQUOTE`` form.

    BingX может возвращать тикеры в форматах ``BTC-USDT``, ``BTC_USDT`` или
    с добавочными суффиксами вроде ``BTCUSDT_UMCBL``. Для сопоставления с
    остальными биржами приводим обозначение к единому виду ``BTCUSDT``.
    """

    if symbol is None:
        return None

    raw = str(symbol).strip().upper()
    if not raw:
        return None

    # Удаляем разделители, чтобы проще выделить квоту.
    collapsed = raw.replace("/", "").replace("-", "").replace("_", "")
    if not collapsed:
        return None

    for quote in _QUOTE_TOKENS:
        idx = collapsed.find(quote)
        if idx > 0:
            base = collapsed[:idx]
            if base:
                return Symbol(base + quote)

    return Symbol(collapsed)

