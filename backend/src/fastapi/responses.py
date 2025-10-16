from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict

__all__ = ["HTMLResponse"]


@dataclass(slots=True)
class HTMLResponse:
    """Simplified HTML response used by the test client."""

    content: Any
    status_code: int = 200
    headers: Dict[str, str] | None = None

    @property
    def body(self) -> str:
        if isinstance(self.content, bytes):
            return self.content.decode("utf-8", errors="ignore")
        return str(self.content)
