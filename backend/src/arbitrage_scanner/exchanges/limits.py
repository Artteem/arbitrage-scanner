from __future__ import annotations
from typing import Dict, Any

"""
Back-compat shim for app.py: новый discovery больше не экспортирует константы
вроде MEXC_CONTRACTS/BINGX_CONTRACTS. Раньше limits.py импортировал их —
теперь делаем модуль изолированным, чтобы импорт из app.py проходил стабильно.

Дальше можно подтянуть реальные лимиты (REST/WS) из конфигов/переменных
или вычислять их динамически. Пока — безопасные дефолты.
"""

async def fetch_limits() -> Dict[str, Any]:
    # Значения консервативные; поправим после запуска.
    return {
        "mexc":  {"rest": {"rps": 5, "burst": 10}, "ws": {"max_subs": 200}},
        "bingx": {"rest": {"rps": 5, "burst": 10}, "ws": {"max_subs": 200}},
        "gate":  {"rest": {"rps": 5, "burst": 10}, "ws": {"max_subs": 200}},
    }
