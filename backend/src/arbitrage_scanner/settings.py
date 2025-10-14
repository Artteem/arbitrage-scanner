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

settings = Settings()
