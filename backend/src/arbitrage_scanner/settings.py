from __future__ import annotations

import os
from functools import cached_property
from typing import Dict

from pydantic import BaseModel, Field


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
    enabled_exchanges: list[str] = [
        ex.strip()
        for ex in os.getenv("ENABLED_EXCHANGES", "binance,bybit,mexc,bingx,gate").split(",")
        if ex.strip()
    ]
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
