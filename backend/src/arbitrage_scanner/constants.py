"""Shared application constants."""

from .domain import Symbol

FALLBACK_SYMBOLS: list[Symbol] = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
"""Default list of symbols used when discovery is unavailable."""
