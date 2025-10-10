from __future__ import annotations

import asyncio
import logging
import time
from typing import Any, Iterable, AsyncIterator

from websockets.exceptions import ConnectionClosed

logger = logging.getLogger(__name__)


def _normalize_numeric(value: Any) -> float | None:
    """Convert various numeric timestamp representations to seconds."""

    if isinstance(value, bool):  # guard against True/False being ints
        return None

    if isinstance(value, (int, float)):
        candidate = float(value)
    elif isinstance(value, str):
        try:
            candidate = float(value)
        except ValueError:
            return None
    else:
        return None

    if candidate <= 0:
        return None

    # Heuristics: detect ns/us/ms based on magnitude.
    if candidate > 1e18:
        candidate /= 1_000_000_000  # nanoseconds -> seconds
    elif candidate > 1e15:
        candidate /= 1_000_000  # microseconds -> seconds
    elif candidate > 1e12:
        candidate /= 1_000  # milliseconds -> seconds

    return candidate


def pick_timestamp(*candidates: Any, default: float | None = None) -> float:
    """Return the first sane timestamp (seconds) from the provided candidates."""

    for candidate in candidates:
        if isinstance(candidate, dict):
            # Occasionally exchanges wrap timestamps as {"ts": 123} or similar.
            nested = pick_timestamp(*candidate.values(), default=None)
            if nested:
                return nested
            continue
        if isinstance(candidate, Iterable) and not isinstance(candidate, (str, bytes, bytearray)):
            nested = pick_timestamp(*candidate, default=None)
            if nested:
                return nested
            continue
        normalized = _normalize_numeric(candidate)
        if normalized is not None:
            return normalized
    return default if default is not None else time.time()


def now_ts() -> float:
    """Explicit helper to obtain the current wall clock timestamp in seconds."""

    return time.time()


async def iter_ws_messages(
    ws,
    *,
    ping_interval: float = 10.0,
    max_idle: float = 30.0,
    name: str = "ws",
) -> AsyncIterator[str | bytes]:
    """Iterate websocket messages while proactively pinging to avoid stalls."""

    last_message = time.time()
    while True:
        try:
            raw = await asyncio.wait_for(ws.recv(), timeout=ping_interval)
        except asyncio.TimeoutError:
            idle = time.time() - last_message
            if idle >= max_idle:
                raise RuntimeError(f"{name} idle for {idle:.1f}s")
            try:
                pong_waiter = ws.ping()
                if pong_waiter is not None:
                    await asyncio.wait_for(pong_waiter, timeout=ping_interval)
            except Exception as exc:  # pragma: no cover - defensive logging
                logger.warning("%s ping failed: %s", name, exc)
                raise RuntimeError(f"{name} ping failed") from exc
            continue
        except ConnectionClosed:
            raise
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.warning("%s receive error: %s", name, exc)
            raise RuntimeError(f"{name} receive failed") from exc

        last_message = time.time()
        yield raw

