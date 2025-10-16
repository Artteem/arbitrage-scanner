import asyncio
import inspect
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))


def pytest_configure(config: pytest.Config) -> None:  # pragma: no cover - pytest hook
    config.addinivalue_line("markers", "asyncio: execute test as an asyncio coroutine")


def pytest_pyfunc_call(pyfuncitem: pytest.Function) -> bool | None:  # pragma: no cover - pytest hook
    test_function = pyfuncitem.obj
    if inspect.iscoroutinefunction(test_function):
        loop = asyncio.new_event_loop()
        try:
            loop.run_until_complete(test_function(**pyfuncitem.funcargs))
        finally:
            loop.close()
        return True
    return None
