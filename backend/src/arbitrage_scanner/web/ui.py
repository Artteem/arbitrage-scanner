from __future__ import annotations
from pathlib import Path

def html() -> str:
    p = Path(__file__).with_name("ui.html")
    return p.read_text(encoding="utf-8")
