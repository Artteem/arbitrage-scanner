from __future__ import annotations

import os
from pydantic import BaseModel


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
    database: DatabaseSettings = DatabaseSettings()


settings = Settings()
