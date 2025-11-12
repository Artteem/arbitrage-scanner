from __future__ import annotations

import os
from functools import cached_property
from typing import Dict, Iterable, List, Set

from pydantic import BaseModel, Field


DEFAULT_ENABLED_EXCHANGES: tuple[str, ...] = (
    "binance",
    "bybit",
    "mexc",
    "bingx",
    "gate",
)


def _normalise_exchange(value: str) -> str:
    return value.strip().lower()


def _parse_enabled_exchanges(raw: str | None) -> List[str]:
    """Parse ``ENABLED_EXCHANGES`` env value preserving order.

    The scanner historically required the environment variable to list every
    connector explicitly.  When new exchanges were added the existing
    configuration (often ``binance,bybit`` from ``.env.example``) silently
    disabled the new connectors.  As a consequence the UI rendered spreads only
    for Binance↔Bybit pairs.  To keep backwards compatibility we treat the env
    value as *customisations* on top of the default exchange set: every default
    exchange remains enabled unless it is explicitly excluded with a ``-name``
    entry.
    """

    exclusions: Set[str] = set()
    inclusions: List[str] = []

    if raw:
        for item in raw.split(","):
            if not item:
                continue
            token = _normalise_exchange(item)
            if not token:
                continue
            if token in {"all", "*"}:
                # Explicit request to include defaults; nothing else to do.
                continue
            if token.startswith("-"):
                excluded = token[1:].strip()
                if excluded:
                    exclusions.add(excluded)
                continue
            inclusions.append(token)

    seen: Set[str] = set()
    ordered: List[str] = []

    def _add(values: Iterable[str]) -> None:
        for value in values:
            normalised = _normalise_exchange(value)
            if not normalised or normalised in exclusions or normalised in seen:
                continue
            seen.add(normalised)
            ordered.append(normalised)

    _add(DEFAULT_ENABLED_EXCHANGES)
    _add(inclusions)

    return ordered


def _apply_connector_overrides(enabled: List[str]) -> List[str]:
    """Apply per-connector enable/disable overrides from environment variables."""

    toggles: dict[str, bool] = {}
    prefix = "CONNECTOR_"
    truthy = {"1", "true", "yes", "on"}
    falsy = {"0", "false", "no", "off"}

    for key, value in os.environ.items():
        if not key.startswith(prefix):
            continue
        name = key[len(prefix) :].strip().lower()
        if not name:
            continue
        val_normalized = (value or "").strip().lower()
        if val_normalized in truthy:
            toggles[name] = True
        elif val_normalized in falsy:
            toggles[name] = False

    if not toggles:
        return enabled

    seen: Set[str] = set()
    ordered: List[str] = []

    for value in enabled:
        name = value.lower()
        if name in toggles and not toggles[name]:
            continue
        if name not in seen:
            seen.add(name)
            ordered.append(name)

    for name, enabled_state in toggles.items():
        if enabled_state and name not in seen:
            ordered.append(name)
            seen.add(name)

    return ordered


def _build_enabled_exchanges() -> List[str]:
    env_value = os.getenv("ENABLED_EXCHANGES")
    enabled = _parse_enabled_exchanges(env_value)
    enabled = _apply_connector_overrides(enabled)
    if enabled:
        return enabled
    return list(DEFAULT_ENABLED_EXCHANGES)


class DatabaseSettings(BaseModel):
    url: str = os.getenv(
        "DATABASE_URL",
        "postgresql+asyncpg://arbitrage:arbitrage@localhost:5432/arbitrage",
    )
    echo: bool = os.getenv("DATABASE_ECHO", "false").lower() in {"1", "true", "yes"}
    pool_size: int = int(os.getenv("DATABASE_POOL_SIZE", "5"))
    max_overflow: int = int(os.getenv("DATABASE_MAX_OVERFLOW", "10"))
    pool_timeout: int = int(os.getenv("DATABASE_POOL_TIMEOUT", "30"))
    pool_recycle: int = int(os.getenv("DATABASE_POOL_RECYCLE", "1800"))


class Settings(BaseModel):
    log_level: str = os.getenv("LOG_LEVEL", "INFO")
    enabled_exchanges: list[str] = Field(default_factory=_build_enabled_exchanges)
    http_timeout: int = int(os.getenv("HTTP_TIMEOUT", "10"))
    ws_connect_timeout: int = int(os.getenv("WS_CONNECT_TIMEOUT", "10"))
    app_secret_key: str | None = os.getenv("APP_SECRET_KEY")
    database: DatabaseSettings = DatabaseSettings()
    http_proxy: str | None = os.getenv("HTTP_PROXY")
    https_proxy: str | None = os.getenv("HTTPS_PROXY")
    ws_proxy_url: str | None = os.getenv("WS_PROXY_URL")
    admin_token: str | None = os.getenv("ADMIN_TOKEN")
    credentials_env_map: Dict[str, Dict[str, str]] = Field(
        default_factory=lambda: {
            "binance": {"key_env": "BINANCE_API_KEY", "secret_env": "BINANCE_API_SECRET"},
            "bybit": {"key_env": "BYBIT_API_KEY", "secret_env": "BYBIT_API_SECRET"},
            "mexc": {"key_env": "MEXC_API_KEY", "secret_env": "MEXC_API_SECRET"},
            "gate": {"key_env": "GATE_API_KEY", "secret_env": "GATE_API_SECRET"},
            "bingx": {"key_env": "BINGX_API_KEY", "secret_env": "BINGX_API_SECRET"},
        }
    )

    @cached_property
    def httpx_proxies(self) -> dict[str, str] | None:
        proxies: dict[str, str] = {}
        if self.http_proxy:
            proxies["http://"] = self.http_proxy
        if self.https_proxy:
            proxies["https://"] = self.https_proxy
        return proxies or None


settings = Settings()
