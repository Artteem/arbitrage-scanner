from importlib import reload

import pytest


def make_settings(monkeypatch, value: str | None):
    if value is None:
        monkeypatch.delenv("ENABLED_EXCHANGES", raising=False)
    else:
        monkeypatch.setenv("ENABLED_EXCHANGES", value)

    from arbitrage_scanner import settings as settings_module

    reload(settings_module)
    return settings_module.Settings()


def test_required_exchange_is_auto_enabled(monkeypatch):
    settings = make_settings(monkeypatch, "binance,bybit,mexc")
    assert "bingx" in settings.enabled_exchanges
    assert tuple(settings.auto_enabled_exchanges) == ("bingx",)


def test_alias_is_normalized(monkeypatch):
    settings = make_settings(monkeypatch, "binance,bybit,mexc,bigx")
    assert settings.enabled_exchanges.count("bingx") == 1
    assert all(ex != "bigx" for ex in settings.enabled_exchanges)
    assert ("bigx", "bingx") in settings.alias_mappings


def test_defaults_include_required(monkeypatch):
    settings = make_settings(monkeypatch, None)
    assert "bingx" in settings.enabled_exchanges
    assert settings.auto_enabled_exchanges == ()

