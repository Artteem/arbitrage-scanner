from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from typing import Iterable, List, Sequence, Tuple

DEFAULT_EXCHANGES = "binance,bybit,mexc,bingx"
REQUIRED_EXCHANGES: Tuple[str, ...] = ("bingx",)
EXCHANGE_ALIASES: dict[str, str] = {
    "bigx": "bingx",
    "bingx-perp": "bingx",
    "bingx_perp": "bingx",
    "bingxperp": "bingx",
}

logger = logging.getLogger(__name__)


def _iter_exchanges(raw: str) -> Iterable[str]:
    for part in raw.split(","):
        name = part.strip()
        if name:
            yield name


def _normalize_exchanges(exchanges: Sequence[str]) -> tuple[list[str], list[str], list[tuple[str, str]]]:
    normalized: List[str] = []
    seen: set[str] = set()
    auto_added: list[str] = []
    aliases: list[tuple[str, str]] = []

    for raw in exchanges:
        lowered = raw.lower()
        canonical = EXCHANGE_ALIASES.get(lowered, lowered)
        if canonical != lowered:
            aliases.append((raw, canonical))
        if canonical in seen:
            continue
        normalized.append(canonical)
        seen.add(canonical)

    for req in REQUIRED_EXCHANGES:
        if req not in seen:
            normalized.append(req)
            seen.add(req)
            auto_added.append(req)

    return normalized, auto_added, aliases


@dataclass
class Settings:
    log_level: str = field(default_factory=lambda: os.getenv("LOG_LEVEL", "INFO"))
    enabled_exchanges: List[str] = field(default_factory=list)
    http_timeout: int = field(default_factory=lambda: int(os.getenv("HTTP_TIMEOUT", "10")))
    ws_connect_timeout: int = field(default_factory=lambda: int(os.getenv("WS_CONNECT_TIMEOUT", "10")))
    auto_enabled_exchanges: Tuple[str, ...] = field(init=False, default=())
    alias_mappings: Tuple[tuple[str, str], ...] = field(init=False, default=())

    def __post_init__(self) -> None:
        raw = os.getenv("ENABLED_EXCHANGES", DEFAULT_EXCHANGES)
        parsed = list(_iter_exchanges(raw))
        normalized, auto_added, aliases = _normalize_exchanges(parsed)
        self.enabled_exchanges = normalized
        self.auto_enabled_exchanges = tuple(auto_added)
        self.alias_mappings = tuple(aliases)

        os.environ["ENABLED_EXCHANGES"] = ",".join(self.enabled_exchanges)

        for original, canonical in aliases:
            logger.warning(
                "Exchange '%s' is an alias. Using canonical name '%s' instead.",
                original,
                canonical,
            )
        if auto_added:
            logger.warning(
                "Missing required exchanges: %s. Automatically enabling them.",
                ", ".join(auto_added),
            )

    def model_dump(self) -> dict:
        return {
            "log_level": self.log_level,
            "enabled_exchanges": list(self.enabled_exchanges),
            "http_timeout": self.http_timeout,
            "ws_connect_timeout": self.ws_connect_timeout,
            "auto_enabled_exchanges": list(self.auto_enabled_exchanges),
            "alias_mappings": [list(item) for item in self.alias_mappings],
        }


settings = Settings()

