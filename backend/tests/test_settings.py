import os
from importlib import reload

import pytest

from arbitrage_scanner.settings import _normalize_exchanges


def make_settings(monkeypatch, value: str | None):
    if value is None:
        monkeypatch.delenv("ENABLED_EXCHANGES", raising=False)
    else:
        monkeypatch.setenv("ENABLED_EXCHANGES", value)

    from arbitrage_scanner import settings as settings_module

    reload(settings_module)
    return settings_module.settings


def test_required_exchange_is_auto_enabled(monkeypatch):
    settings = make_settings(monkeypatch, "binance,bybit,mexc")
    assert "bingx" in settings.enabled_exchanges
    if settings.auto_enabled_exchanges:
        assert tuple(settings.auto_enabled_exchanges) == ("bingx",)


def test_alias_is_normalized():
    normalized, auto_added, aliases = _normalize_exchanges(["binance", "bigx"])
    assert normalized == ["binance", "bingx"]
    assert auto_added == []
    assert ("bigx", "bingx") in aliases


def test_defaults_include_required(monkeypatch):
    settings = make_settings(monkeypatch, None)
    assert "bingx" in settings.enabled_exchanges
    assert settings.auto_enabled_exchanges == ()


def test_extra_aliases_are_canonical():
    normalized, auto_added, aliases = _normalize_exchanges(["binance", "bingx_perp", "bingx-perp", "bingxperp"])
    assert normalized == ["binance", "bingx"]
    assert auto_added == []
    assert ("bingx_perp", "bingx") in aliases
    assert ("bingx-perp", "bingx") in aliases
    assert ("bingxperp", "bingx") in aliases


def test_environment_is_synced(monkeypatch):
    settings = make_settings(monkeypatch, "binance, bybit ,mexc")
    assert settings.enabled_exchanges == ["binance", "bybit", "mexc", "bingx"]
    assert os.getenv("ENABLED_EXCHANGES") == "binance,bybit,mexc,bingx"

