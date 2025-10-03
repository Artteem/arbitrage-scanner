from __future__ import annotations

from pathlib import Path
from html import escape


def html(symbol: str) -> str:
    p = Path(__file__).with_name("pair.html")
    template = p.read_text(encoding="utf-8")
    safe_symbol = escape(symbol.upper())
    return template.replace("{{symbol}}", safe_symbol)
