from pydantic import BaseModel
import os

class Settings(BaseModel):
    log_level: str = os.getenv("LOG_LEVEL", "INFO")
    enabled_exchanges: list[str] = [
        ex.strip()
        for ex in os.getenv("ENABLED_EXCHANGES", "binance,bybit,mexc,bingx,gate").split(",")
        if ex.strip()
    ]
    http_timeout: int = int(os.getenv("HTTP_TIMEOUT", "10"))
    ws_connect_timeout: int = int(os.getenv("WS_CONNECT_TIMEOUT", "10"))
    database_path: str = os.getenv("DATABASE_PATH", os.path.join(os.getcwd(), "data", "arbitrage.db"))
    funding_refresh_interval: float = float(os.getenv("FUNDING_REFRESH_INTERVAL", "30"))
    default_trade_volume: float = float(os.getenv("DEFAULT_TRADE_VOLUME", "100"))
    history_lookback_days: int = int(os.getenv("HISTORY_LOOKBACK_DAYS", "10"))

settings = Settings()
