from __future__ import annotations
from typing import Dict, Any

"""
Back-compat shim: старые места вызывают `await fetch_limits(exchange, symbol)`.
Здесь отдаём дефолтные лимиты по бирже, не тянем ничего из discovery.
При необходимости значения легко вынести в конфиг/ENV.
"""

# Консервативные значения по умолчанию
DEFAULT_LIMITS: Dict[str, Dict[str, Any]] = {
    "mexc":  {"rest": {"rps": 5,  "burst": 10, "timeout_s": 5}, "ws": {"max_subs": 200}},
    "bingx": {"rest": {"rps": 5,  "burst": 10, "timeout_s": 5}, "ws": {"max_subs": 200}},
    "gate":  {"rest": {"rps": 5,  "burst": 10, "timeout_s": 5}, "ws": {"max_subs": 200}},
    "bybit": {"rest": {"rps": 10, "burst": 20, "timeout_s": 5}, "ws": {"max_subs": 300}},
    "binance": {"rest": {"rps": 10, "burst": 20, "timeout_s": 5}, "ws": {"max_subs": 300}},
}

async def fetch_limits(exchange: str, symbol: str) -> Dict[str, Any]:
    ex = (exchange or "").lower()
    base = DEFAULT_LIMITS.get(ex, {"rest": {"rps": 5, "burst": 10, "timeout_s": 5},
                                   "ws": {"max_subs": 200}})
    # Можно добавить несложные пер-символьные поправки при желании
    return {
        "exchange": ex,
        "symbol": symbol,
        "rest": dict(base["rest"]),
        "ws": dict(base["ws"]),
    }
