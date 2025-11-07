from __future__ import annotations

import asyncio
import gzip
import json
import logging
import time
import re
import httpx
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Sequence, Tuple, Optional, Set

import websockets
import zlib

from ..domain import Symbol, Ticker
from ..store import TickerStore
from .credentials import ApiCreds
from .discovery import discover_mexc_usdt_perp


LOGGER = logging.getLogger("arbitrage_scanner.connectors.mexc_perp")

WS_URL = "wss://contract.mexc.com/edge"
REST_BASE = "https://contract.mexc.com"


def _utc_ms() -> int:
    return int(time.time() * 1000)


def _to_float(x: Any) -> Optional[float]:
    try:
        return float(x)
    except Exception:
        return None


def _json(obj: Any) -> str:
    return json.dumps(obj, separators=(",", ":"), ensure_ascii=False)


MULTIPLIER_PREFIXES = ("1M", "1000", "10000", "10K", "1K")


def strip_multiplier(base: str) -> str:
    for p in MULTIPLIER_PREFIXES:
        if base.startswith(p):
            return base[len(p):]
    return base


def is_delivery_or_cross(norm_sym: str) -> bool:
    s = norm_sym.upper().replace("-", "_").replace("/", "_")
    if "_USDT" not in s:
        return True
    base, _, quote = s.partition("_")
    if quote != "USDT":
        return True
    # эвристика для кроссов (ETHBTC_USDT и пр.)
    if any(k in base for k in ("BTC", "ETH", "XRP", "BNB")) and base not in ("BTC", "ETH", "XRP", "BNB"):
        return True
    # датированные окончания (напр. ETHUSDT07NOV25)
    if re.search(r"\d{2}[A-Z]{3}\d{2,4}$", base):
        return True
    return False


class MexcPerp:
    def __init__(
        self,
        store: Any,
        symbols: Optional[Iterable[str]] = None,
        ws_url: str = WS_URL,
        rest_base: str = REST_BASE,
    ) -> None:
        self.store = store
        self.ws_url = ws_url
        self.rest_base = rest_base
        self._user_symbols: Optional[Set[str]] = set(s.upper() for s in symbols) if symbols else None
        self._native_symbols: Set[str] = set()
        self._ws: Optional[websockets.WebSocketClientProtocol] = None
        self._hb_task: Optional[asyncio.Task] = None
        self._rx_task: Optional[asyncio.Task] = None
        self._running = False

    async def run(self) -> None:
        self._running = True
        while self._running:
            try:
                await self._load_contract_whitelist()
                if not self._native_symbols:
                    LOGGER.error("MEXC: no futures symbols from REST — retry in 10s")
                    await asyncio.sleep(10)
                    continue

                async with websockets.connect(self.ws_url, max_size=2**22, ping_interval=None) as ws:
                    self._ws = ws
                    LOGGER.info("MEXC: connected to %s", self.ws_url)

                    await self._subscribe_all()

                    self._hb_task = asyncio.create_task(self._heartbeat())
                    self._rx_task = asyncio.create_task(self._receiver())

                    await asyncio.wait(
                        [self._hb_task, self._rx_task],
                        return_when=asyncio.FIRST_EXCEPTION,
                    )
                    await self._cancel_tasks()

            except Exception as e:
                LOGGER.exception("MEXC: ws loop error: %s", e)

            await asyncio.sleep(3)

    async def stop(self) -> None:
        self._running = False
        await self._cancel_tasks()
        if self._ws:
            try:
                await self._ws.close()
            except Exception:
                pass
            self._ws = None

    async def _cancel_tasks(self) -> None:
        for t in (self._hb_task, self._rx_task):
            if t and not t.done():
                t.cancel()
        self._hb_task = None
        our_rx = self._rx_task
        self._rx_task = None
        # Подождём кансел, но не зависаем:
        if our_rx:
            try:
                await asyncio.sleep(0)
            except Exception:
                pass

    async def _heartbeat(self) -> None:
        try:
            while self._running and self._ws:
                await asyncio.sleep(15)
                if self._ws.closed:
                    raise ConnectionError("mexc websocket closed during heartbeat")
        except asyncio.CancelledError:
            return
        except Exception as e:
            LOGGER.error("mexc heartbeat failed\n  %s", e)
            raise

    async def _receiver(self) -> None:
        try:
            while self._running and self._ws:
                raw = await self._ws.recv()
                if isinstance(raw, (bytes, bytearray)):
                    try:
                        msg = json.loads(raw.decode("utf-8", "ignore"))
                    except Exception:
                        try:
                            msg = json.loads(raw)
                        except Exception:
                            LOGGER.debug("MEXC: skip binary frame len=%s", len(raw))
                            continue
                else:
                    msg = json.loads(raw)
                self._route_message(msg)
        except asyncio.CancelledError:
            return
        except Exception as e:
            LOGGER.exception("MEXC: receiver crashed: %s", e)
            raise

    def _route_message(self, msg: Dict[str, Any]) -> None:
        ch = msg.get("channel") or msg.get("c") or ""
        if ch == "rs.error":
            LOGGER.error("MEXC WS error: %s", msg)
            return

        if ch == "push.ticker":
            data = msg.get("data") or {}
            sym = data.get("symbol") or data.get("S")
            if not sym:
                return
            bid = _to_float(data.get("bid1") or data.get("b"))
            ask = _to_float(data.get("ask1") or data.get("a"))
            ts = int(data.get("ts") or _utc_ms())
            if bid and ask:
                self._emit_quote(sym, bid, ask, ts)
            return

        if ch == "push.depth":
            data = msg.get("data") or {}
            sym = data.get("symbol")
            if not sym:
                return
            bids = data.get("bids") or []
            asks = data.get("asks") or []
            bid = _to_float(bids[0][0]) if bids else None
            ask = _to_float(asks[0][0]) if asks else None
            ts = int(data.get("ts") or _utc_ms())
            if bid and ask:
                self._emit_quote(sym, bid, ask, ts)
            return

        if ch == "push.funding.rate":
            data = msg.get("data") or {}
            sym = data.get("symbol")
            rate = _to_float(data.get("fr") or data.get("fundingRate"))
            next_ts = int(data.get("nextFundingTime") or data.get("nft") or 0)
            if sym and rate is not None:
                self._emit_funding(sym, rate, next_ts)
            return

        if ch in ("pong", "rs.reply"):
            return

    async def _load_contract_whitelist(self) -> None:
        try:
            url = f"{self.rest_base}/api/v1/contract/detail"
            async with httpx.AsyncClient(timeout=20.0) as cli:
                r = await cli.get(url)
                r.raise_for_status()
                jd = r.json()
                data = jd.get("data") or []
                syms: Set[str] = set()
                for it in data:
                    sym = (it.get("symbol") or "").upper()
                    if not sym.endswith("_USDT"):
                        continue
                    state = (it.get("state") or it.get("status") or "NORMAL").upper()
                    if state not in ("NORMAL", "TRADING", "ON"):
                        continue
                    syms.add(sym)
                self._native_symbols = syms
                LOGGER.info("MEXC: loaded %d futures symbols", len(self._native_symbols))
        except Exception as e:
            LOGGER.exception("MEXC: failed to load contract whitelist: %s", e)
            self._native_symbols = set()

    def _resolve_symbol(self, norm_sym: str) -> Optional[str]:
        s = norm_sym.upper().replace("-", "_").replace("/", "_")
        if is_delivery_or_cross(s):
            return None
        base = s.split("_USDT", 1)[0]
        base2 = strip_multiplier(base)
        for candidate in (f"{base}_USDT", f"{base2}_USDT"):
            if candidate in self._native_symbols:
                return candidate
        return None

    async def _subscribe_all(self) -> None:
        targets: Set[str] = set()
        if self._user_symbols:
            targets = set(self._user_symbols)
        else:
            syms = None
            for name in ("symbols_for_exchange", "get_symbols_for_exchange", "get_symbols"):
                fn = getattr(self.store, name, None)
                if callable(fn):
                    try:
                        try:
                            syms = fn("MEXC")
                        except TypeError:
                            syms = fn()
                    except Exception:
                        syms = None
                if syms:
                    break
            if isinstance(syms, (list, set, tuple)):
                targets = set(str(x).upper() for x in syms)
        if not targets:
            targets = {"BTC_USDT", "ETH_USDT", "SOL_USDT"}

        ws = self._ws
        if not ws:
            return

        ok = 0
        for norm in sorted(targets):
            native = self._resolve_symbol(norm)
            if not native:
                LOGGER.debug("MEXC: skip %s (no native contract)", norm)
                continue
            for method in ("sub.ticker", "sub.depth", "sub.funding.rate"):
                msg = {"method": method, "param": {"symbol": native}, "gzip": False}
                await ws.send(_json(msg))
                await asyncio.sleep(0.01)
            ok += 1

        LOGGER.info("MEXC: subscribed %d/%d symbols", ok, len(targets))

    def _emit_quote(self, native_sym: str, bid: float, ask: float, ts_ms: int) -> None:
        for fn_name in ("upsert_best_quote", "update_l1", "set_quote", "on_best_quote"):
            fn = getattr(self.store, fn_name, None)
            if callable(fn):
                try:
                    fn("MEXC", native_sym, bid, ask, ts_ms)
                    return
                except Exception as e:
                    LOGGER.debug("store.%s error: %s", fn_name, e)
        LOGGER.debug("QUOTE MEXC %s bid=%s ask=%s ts=%s", native_sym, bid, ask, ts_ms)

    def _emit_funding(self, native_sym: str, rate: float, next_ts_ms: int) -> None:
        for fn_name in ("update_funding", "upsert_funding", "set_funding", "on_funding"):
            fn = getattr(self.store, fn_name, None)
            if callable(fn):
                try:
                    fn("MEXC", native_sym, rate, next_ts_ms)
                    return
                except Exception as e:
                    LOGGER.debug("store.%s error: %s", fn_name, e)
        LOGGER.debug("FUNDING MEXC %s rate=%s next=%s", native_sym, rate, next_ts_ms)


async def run_mexc(settings: Any, store: Any) -> None:
    symbols: Optional[Iterable[str]] = None
    for attr in ("mexc_symbols", "symbols_mexc", "symbols"):
        v = getattr(settings, attr, None)
        if v:
            symbols = v
            break
    mexc = MexcPerp(store=store, symbols=symbols)
    try:
        await mexc.run()
    finally:
        await mexc.stop()
