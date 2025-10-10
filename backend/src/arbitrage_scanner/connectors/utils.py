from __future__ import annotations

import time
from typing import Any, Iterable


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

