from __future__ import annotations

"""
Modified MEXC USDT‑perpetual connector.

This version aligns the MEXC integration with the simplified pattern used by
the Binance and Bybit connectors.  In particular it:

* Limits the number of contracts per WebSocket connection to 10.  The
  official MEXC documentation states that each WebSocket connection may
  subscribe to at most 30 topics【740259858122821†L30-L42】, and this
  connector subscribes to three topics per contract (ticker, order book depth
  and funding rate).  Therefore 10 contracts per connection ensure we never
  exceed 30 topics.
* Leaves the existing message decoding and order book management logic
  intact.  MEXC uses a bespoke WebSocket protocol with separate `sub.*`
  methods for each channel, and these routines handle the specifics.
* Adds documentation comments for maintainers on symbol formats and limits.

To add new symbols, simply pass a list of `Symbol` objects to `run_mexc`; the
connector will fall back to discovery if fewer than one symbol is provided.
"""

import asyncio
import json
import logging
import sys
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Sequence, Tuple

import websockets
from websockets.protocol import State

from ..domain import Symbol, Ticker
from ..store import TickerStore
from .credentials import ApiCreds  # noqa: F401 – reserved for future use
from .discovery import discover_mexc_usdt_perp

# MEXC futures WebSocket endpoint (contract API v1).  See
# https://www.mexc.com/api-docs/futures/websocket-api for reference.
WS_ENDPOINT = "wss://contract.mexc.com/edge"

# Reconnect backoff times.
WS_RECONNECT_INITIAL = 1.0
WS_RECONNECT_MAX = 60.0

# The maximum number of **contracts** per WebSocket connection.  MEXC allows up
# to 30 subscription topics per connection【740259858122821†L30-L42】.  Since this
# connector subscribes to three topics per contract (ticker, depth and funding
# rate), a batch of 10 contracts corresponds to 30 topics.
MAX_TOPICS_PER_CONN = 10

# Delay between successive subscription messages (in seconds).  Leaving a
# small delay helps prevent the exchange from rate limiting initial
# subscriptions.
WS_SUB_DELAY = 0.1

# Interval (in seconds) for heartbeat liveness checks.  We don't send client
# pings because the exchange pushes its own heartbeat messages.
HEARTBEAT_INTERVAL = 20.0

# If the user supplies fewer than this many symbols the connector will fall
# back to discovering all MEXC USDT perpetual symbols via
# `discover_mexc_usdt_perp`.
MIN_SYMBOL_THRESHOLD = 1

# Fallback list used when discovery fails completely.  Stored in native
# exchange format (e.g. BTC_USDT).
FALLBACK_NATIVE_SYMBOLS: tuple[Symbol, ...] = ("BTC_USDT", "ETH_USDT", "SOL_USDT")

MEXC_ORIGIN = "https://futures.mexc.com"
MEXC_USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0 Safari/537.36"
)
MEXC_HEADERS = {
    "Referer": "https://futures.mexc.com/exchange/USDT",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
}

logger = logging.getLogger(__name__)


def _ensure_logger_configured() -> None:
    """Attach a stdout handler so DEBUG logs are always visible (even after reconfig)."""
    global logger
    existing = [
        h for h in logger.handlers if getattr(h, "_mexc_stdout_handler", False)
    ]
    if not existing:
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(logging.DEBUG)
        handler.setFormatter(
            logging.Formatter("%(asctime)s %(name)s %(levelname)s: %(message)s")
        )
        handler._mexc_stdout_handler = True  # type: ignore[attr-defined]
        logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)
    logger.propagate = False


_ensure_logger_configured()


async def _resolve_mexc_symbols(symbols: Sequence[Symbol]) -> list[str]:
    """Resolve requested symbols into native MEXC contract identifiers."""

    requested: list[str] = []
    seen: set[str] = set()
    for symbol in symbols:
        if not symbol:
            continue
        native = str(symbol).strip().upper()
        if not native:
            continue
        native = native.replace("-", "_")
        if native.endswith("USDT") and "_" not in native:
            native = f"{native[:-4]}_USDT"
        if native in seen:
            continue
        seen.add(native)
        requested.append(native)

    if len(requested) >= MIN_SYMBOL_THRESHOLD:
        return requested

    try:
        discovered = await discover_mexc_usdt_perp()
    except Exception:
        logger.exception("Failed to discover MEXC symbols; falling back to configured list")
        discovered = set()

    if discovered:
        discovered_native: set[str] = set()
        for sym in discovered:
            native = _to_mexc_contract(sym)
            if not native:
                continue
            discovered_native.add(str(native).upper())
        if requested:
            filtered = [symbol for symbol in requested if symbol in discovered_native]
            if filtered:
                return filtered
            logger.warning(
                "Requested symbols are unavailable on MEXC; using discovered symbols",
                extra={"requested": requested},
            )
        if discovered_native:
            return sorted(discovered_native)

    if requested:
        logger.warning(
            "MEXC discovery unavailable; using configured symbol list",
            extra={"requested": requested},
        )
        return requested

    logger.warning(
        "MEXC discovery unavailable; using fallback symbols",
        extra={"fallback": FALLBACK_NATIVE_SYMBOLS},
    )
    return list(FALLBACK_NATIVE_SYMBOLS)


def _as_float(value) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _to_mexc_contract(symbol: Symbol) -> str:
    """Convert a common symbol into MEXC contract format (e.g. BTCUSDT → BTC_USDT)."""
    sym = str(symbol).replace('-', '').replace(' ', '')
    if "_" in sym:
        return sym
    if sym.endswith("USDT"):
        return f"{sym[:-4]}_USDT"
    return sym


def _from_mexc_symbol(symbol: str | None) -> Symbol | None:
    """Convert a MEXC contract symbol back into the common (separator‑less) form."""
    if not symbol:
        return None
    return symbol.replace("_", "")


def _extract_bid(item) -> float:
    for key in ("bid1", "bestBidPrice", "bestBid"):
        val = item.get(key)
        bid = _as_float(val)
        if bid > 0:
            return bid
    return 0.0


def _extract_ask(item) -> float:
    for key in ("ask1", "bestAskPrice", "bestAsk"):
        val = item.get(key)
        ask = _as_float(val)
        if ask > 0:
            return ask
    return 0.0


def _parse_interval(item) -> str:
    """Extract a funding interval from the payload, defaulting to 8h."""
    interval = item.get("fundingInterval") or item.get("interval")
    if isinstance(interval, (int, float)):
        return f"{interval}h"
    if isinstance(interval, str) and interval:
        return interval
    return "8h"


async def run_mexc(store: TickerStore, symbols: Sequence[Symbol]):
    """Entry point for the MEXC connector.

    Resolves symbols, splits them into connection batches and starts a worker
    for each batch.  If all workers exit normally the connector returns; if
    any worker crashes the entire set of workers is restarted.  This behaviour
    mirrors the Binance/Bybit connectors.
    """

    _ensure_logger_configured()
    #print("Running MEXC")
    subscribe_native = await _resolve_mexc_symbols(symbols)
    if not subscribe_native:
        logger.warning("No symbols resolved for MEXC connector; skipping startup")
        return

    # Build list of (common_symbol, native_symbol) tuples.
    symbol_pairs: list[tuple[str, str]] = []
    for native in subscribe_native:
        ws_symbol = _to_mexc_contract(native)
        if not ws_symbol:
            continue
        common = _from_mexc_symbol(ws_symbol)
        if not common:
            continue
        symbol_pairs.append((common, ws_symbol))

    if not symbol_pairs:
        logger.warning("MEXC connector resolved symbols but produced no native pairs")
        return

    # Split into chunks to stay within the subscription limit.  Each chunk
    # contains up to MAX_TOPICS_PER_CONN contracts (10), resulting in up to 30
    # topics per WebSocket connection.
    chunks = [
        tuple(symbol_pairs[idx : idx + MAX_TOPICS_PER_CONN])
        for idx in range(0, len(symbol_pairs), MAX_TOPICS_PER_CONN)
    ]

    if not chunks:
        return
 
    while True:
        tasks = [asyncio.create_task(_run_mexc_ws(store, chunk)) for chunk in chunks]
        try:
            await asyncio.gather(*tasks)
            return
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("MEXC websocket worker crashed; restarting all connections")
            await asyncio.sleep(WS_RECONNECT_INITIAL)
        finally:
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)


async def _run_mexc_ws(
    store: TickerStore, symbol_pairs: Sequence[tuple[str, str]]
) -> None:
    """Run a single WebSocket connection for a batch of contracts."""
    wanted_exchange = [native for _, native in symbol_pairs]

    if not wanted_exchange:
        return
    wanted_common = {common for common, _ in symbol_pairs if common}
    
    if not wanted_common:
        return
    books: Dict[str, _MexcOrderBookState] = {}
    async for ws in _reconnect_ws():
        #print("connectd")
        heartbeat: asyncio.Task | None = None
        try:
            await _send_mexc_subscriptions(ws, symbol_pairs)
            heartbeat = asyncio.create_task(
                _ws_heartbeat(ws, interval=HEARTBEAT_INTERVAL, name="mexc")
            )
            async for raw in ws:
                _log_ws_raw_frame("mexc", raw)
                msg = _decode_ws_message(raw)
                # Periodically emit a debug sample
                if int(time.time()) % 10 == 0:
                    logger.debug("WS RX sample: %s", str(msg)[:300])
                if msg is None:
                    continue
                # Ignore ack and error messages
                raw_message = str(msg)
                if _is_mexc_ack(msg):
                    logger.info("MEXC WS ack: %s", raw_message[:500])
                    continue
                if isinstance(msg, dict) and msg.get("channel") == "rs.error":
                    logger.error("MEXC WS error: %s", raw_message[:500])
                    continue
                if _is_ping(msg):
                    await _reply_pong(ws, msg)
                    continue

                # Extract order book updates
                depth_payload = _extract_depth_message(msg)
                if depth_payload:
                    snapshot, data, sym_raw = depth_payload
                    if not isinstance(data, dict):
                        continue
                    sym_common = _from_mexc_symbol(sym_raw)
                    logger.info(
                        "WS PARSE exchange=%s native=%s -> common=%s",
                        "mexc",
                        sym_raw,
                        sym_common,
                    )
                    if not sym_common:
                        logger.warning(
                            "WS DROP reason=normalize_none exchange=%s native=%s payload=%s",
                            "mexc",
                            sym_raw,
                            raw_message[:500],
                        )
                        continue
                    if sym_common not in wanted_common:
                        continue
                    bids_raw = data.get("bids") or data.get("b") or data.get("buy")
                    asks_raw = data.get("asks") or data.get("a") or data.get("sell")
                    book = books.setdefault(sym_common, _MexcOrderBookState())
                    if snapshot:
                        book.snapshot(bids_raw, asks_raw)
                    else:
                        book.update(bids_raw, asks_raw)
                    bids, asks = book.top_levels()
                    last_price = _extract_last_price(data)
                    store.upsert_order_book(
                        "mexc",
                        sym_common,
                        bids=bids or None,
                        asks=asks or None,
                        ts=time.time(),
                        last_price=last_price,
                    )
                    if bids and asks:
                        best_bid = bids[0][0]
                        best_ask = asks[0][0]
                        store.upsert_ticker(
                            Ticker(
                                exchange="mexc",
                                symbol=sym_common,
                                bid=best_bid,
                                ask=best_ask,
                                ts=time.time(),
                            )
                        )
                    continue
                # Handle other payloads: funding rate, ticker
                now = time.time()
                for sym_raw, payload in _iter_mexc_payloads(msg):
                    if not isinstance(payload, dict):
                        continue
                    sym_common = _from_mexc_symbol(sym_raw)
                    logger.info(
                        "WS PARSE exchange=%s native=%s -> common=%s",
                        "mexc",
                        sym_raw,
                        sym_common,
                    )
                    if not sym_common:
                        logger.warning(
                            "WS DROP reason=normalize_none exchange=%s native=%s payload=%s",
                            "mexc",
                            sym_raw,
                            raw_message[:500],
                        )
                        continue
                    if sym_common not in wanted_common:
                        continue
                    if _looks_like_depth(payload):
                        continue
                    rate = _extract_funding_rate(payload)
                    if rate is not None:
                        interval = _parse_interval(payload)
                        store.upsert_funding(
                            "mexc", sym_common, rate=rate, interval=interval, ts=now
                        )
                        continue
                    bid = _extract_bid(payload)
                    ask = _extract_ask(payload)
                    if bid > 0 and ask > 0:
                        store.upsert_ticker(
                            Ticker(exchange="mexc", symbol=sym_common, bid=bid, ask=ask, ts=now)
                        )
                        last_price = _extract_last_price(payload)
                        if last_price:
                            store.upsert_order_book(
                                "mexc",
                                sym_common,
                                last_price=last_price,
                                last_price_ts=now,
                            )
        except asyncio.CancelledError:
            raise
        except Exception:
            await asyncio.sleep(2.0)
        finally:
            if heartbeat is not None:
                heartbeat.cancel()
                await asyncio.gather(heartbeat, return_exceptions=True)


def _mexc_connect_kwargs() -> dict[str, Any]:
    return {
        "ping_interval": None,
        "ping_timeout": None,
        "close_timeout": 5,
        "compression": "deflate",
        "origin": MEXC_ORIGIN,
        "user_agent_header": MEXC_USER_AGENT,
        "additional_headers": MEXC_HEADERS,
    }


@asynccontextmanager
async def _mexc_ws_connection():
    """Open a websocket connection with the required MEXC headers."""
    connect_kwargs = _mexc_connect_kwargs()
    logger.debug(
        "MEXC WS connect origin=%s headers=%s",
        connect_kwargs.get("origin"),
        list((connect_kwargs.get("additional_headers") or {}).keys()),
    )
    async with websockets.connect(WS_ENDPOINT, **connect_kwargs) as ws:
        yield ws


async def _reconnect_ws():
    """Yield a connected WebSocket client, reconnecting with exponential backoff."""
    delay = WS_RECONNECT_INITIAL
    while True:
        try:
            async with _mexc_ws_connection() as ws:
                delay = WS_RECONNECT_INITIAL
                yield ws
        except asyncio.CancelledError as e:
            raise
        except Exception as e :
            logger.exception("MEXC websocket connect failed: %s", e)
            await asyncio.sleep(delay)
            delay = min(delay * 2, WS_RECONNECT_MAX)


async def _send_mexc_subscriptions(
    ws, symbols: Sequence[tuple[str, str]]
) -> None:
    """Send subscription requests for ticker, order book and funding rate topics."""
    if not symbols:
        #print("_send_mexc_subscriptions return 0")
        return
    unique: list[tuple[str, str]] = []
    seen: set[str] = set()
    for common, native in symbols:
        if not native:
            continue
        if native in seen:
            continue
        seen.add(native)
        unique.append((common, native))
    if not unique:
        #print("_send_mexc_subscriptions return 1")
        return
    for common, native in unique:
        # Subscribe to per‑contract ticker (best bid/ask, last price).
        print("print MEXC subscribe ticker -> %s (native=%s)", common, native)
        logger.debug("MEXC subscribe ticker -> %s (native=%s)", common, native)
        ticker_payload = {
            "method": "sub.ticker",
            "param": {"symbol": native},
            "gzip": False,
        }
        await _send_json_with_retry(ws, ticker_payload)
        await asyncio.sleep(WS_SUB_DELAY)
        # Subscribe to full order book depth (step0) for the symbol.
        # print("MEXC subscribe depth -> %s", native)
        logger.debug("MEXC subscribe depth -> %s", native)
        depth_payload = {
            "method": "sub.depth",
            "param": {"symbol": native},
            "gzip": False,
        }
        await _send_json_with_retry(ws, depth_payload)
        await asyncio.sleep(WS_SUB_DELAY)
        # Subscribe to funding rate updates.
        # print("MEXC subscribe funding -> %s", native)
        logger.debug("MEXC subscribe funding -> %s", native)
        funding_payload = {
            "method": "sub.funding.rate",
            "param": {"symbol": native},
            "gzip": False,
        }
        await _send_json_with_retry(ws, funding_payload)
        await asyncio.sleep(WS_SUB_DELAY)


async def _send_json_with_retry(ws, payload: dict) -> None:
    try:
        await ws.send(json.dumps(payload))
        logger.debug("MEXC WS send -> %s", json.dumps(payload)[:200])
        result = await ws.recv()
        print(result)
    except Exception as e:
        logger.exception("Failed to send MEXC subscription", extra={"payload": payload})
        # print("Exception while _send_json_with_retry(): " + str(e))


def _decode_ws_bytes(data: bytes) -> str | None:
    if not data:
        return None
    try:
        return data.decode("utf-8", errors="strict")
    except Exception:
        return None


def _log_ws_raw_frame(exchange: str, message: str | bytes | bytearray) -> None:
    if not logger.isEnabledFor(logging.DEBUG):
        return
    if isinstance(message, (bytes, bytearray)):
        print(f"{ exchange.upper()} WS RX raw frame ({len(message)} bytes)")
        return
    print(f"{exchange.upper()} WS RX raw frame: {str(message)[:512]}")


def _is_mexc_ack(message: dict) -> bool:
    if not isinstance(message, dict):
        return False
    channel = message.get("channel")
    if isinstance(channel, str) and channel in {
        "rs.sub",
        "rs.unsub",
        "rs.ping",
        "rs.pong",
        "rs.query",
    }:
        return True
    if message.get("method") in {"sub", "unsub"} and message.get("success") is True:
        return True
    if message.get("success") is True and not message.get("data"):
        return True
    return False


async def _ws_heartbeat(ws, *, interval: float, name: str) -> None:
    try:
        while True:
            await asyncio.sleep(interval)
            state = getattr(ws, "state", None)
            if state in (State.CLOSING, State.CLOSED):
                raise ConnectionError(f"{name} websocket closed during heartbeat")
    except asyncio.CancelledError:
        raise
    except Exception:
        logger.exception("%s heartbeat failed", name)
        try:
            await ws.close()
        except Exception:
            pass
        raise


def _decode_ws_message(message: str | bytes) -> dict | None:
    if isinstance(message, str):
        raw = message
    elif isinstance(message, (bytes, bytearray)):
        raw = _decode_ws_bytes(bytes(message))
    else:
        return None
    if not raw:
        return None
    raw = raw.strip()
    if not raw:
        return None
    try:
        return json.loads(raw)
    except Exception:
        return None


def _is_ping(message: dict) -> bool:
    if not isinstance(message, dict):
        return False
    if "ping" in message:
        return True
    method = message.get("method")
    if isinstance(method, str) and method.lower() == "ping":
        return True
    return False


async def _reply_pong(ws, message: dict) -> None:
    try:
        if "ping" in message:
            await ws.send(json.dumps({"pong": message["ping"]}))
        else:
            await ws.send(json.dumps({"id": message.get("id", 0), "method": "pong"}))
    except Exception:
        pass


def _extract_depth_message(message: dict) -> tuple[bool, dict, str] | None:
    if not isinstance(message, dict):
        return None
    # Legacy depth update (depth.update method).  Format: {"method":"depth.update", "params":[snapshot,data,symbol]}
    method = message.get("method")
    if isinstance(method, str) and method.lower() == "depth.update":
        params = message.get("params")
        if not isinstance(params, list) or len(params) < 3:
            return None
        snapshot = bool(params[0])
        data = params[1] if isinstance(params[1], dict) else {}
        symbol = params[2]
        if not isinstance(symbol, str):
            return None
        return snapshot, data, symbol
    # New format: channel contains "depth"
    channel = message.get("channel") or message.get("topic")
    if isinstance(channel, str) and "depth" in channel.lower():
        data = message.get("data")
        if isinstance(data, dict):
            symbol = message.get("symbol") or data.get("symbol")
            if isinstance(symbol, str):
                # Treat as snapshot for "push.depth"/"push.depth.step" messages
                return True, data, symbol
    return None


def _extract_last_price(data: dict) -> float | None:
    for key in ("lastPrice", "last", "close", "price", "markPrice"):
        val = data.get(key)
        if val is None:
            continue
        try:
            price = float(val)
        except (TypeError, ValueError):
            continue
        if price > 0:
            return price
    return None


def _iter_levels(source) -> Iterable[Tuple[float, float]]:
    if isinstance(source, dict):
        source = source.get("levels") or source.get("data") or source.get("list") or []
    if not isinstance(source, (list, tuple)):
        return []
    result: List[Tuple[float, float]] = []
    for entry in source:
        price, size = _parse_level(entry)
        if price is None or size is None:
            continue
        result.append((price, size))
    return result


def _looks_like_depth(payload: dict) -> bool:
    if not isinstance(payload, dict):
        return False
    if payload.get("bids") or payload.get("asks"):
        return True
    if payload.get("b") or payload.get("a"):
        return True
    if payload.get("buy") or payload.get("sell"):
        return True
    return False


def _extract_funding_rate(payload: dict) -> float | None:
    if not isinstance(payload, dict):
        return None
    for key in ("fundingRate", "funding_rate", "rate", "value"):
        val = payload.get(key)
        if val is None:
            continue
        try:
            rate = float(val)
        except (TypeError, ValueError):
            continue
        return rate
    return None


def _iter_mexc_payloads(message) -> Iterable[Tuple[str, dict]]:
    if not isinstance(message, dict):
        return []
    default_symbol: str | None = None
    sym_candidate = message.get("symbol") or message.get("s")
    if isinstance(sym_candidate, str) and sym_candidate:
        default_symbol = sym_candidate
    params = message.get("params")
    if isinstance(params, list):
        for item in reversed(params):
            if isinstance(item, str) and item:
                default_symbol = item
                break
    payload_candidates = []
    for key in ("data", "tick", "ticker", "tickers", "result", "payload"):
        if key in message:
            payload_candidates.append(message[key])
    if isinstance(params, list):
        payload_candidates.append(params)
    if not payload_candidates:
        payload_candidates.append(message)
    seen: set[tuple[str, int]] = set()
    items: list[tuple[str, dict]] = []
    for candidate in payload_candidates:
        for symbol, payload in _iter_payload_items(candidate, default_symbol):
            key = (symbol, id(payload))
            if key in seen:
                continue
            seen.add(key)
            items.append((symbol, payload))
    return items


def _iter_payload_items(payload, default_symbol: str | None) -> Iterable[tuple[str, dict]]:
    if payload is None:
        return []
    items: list[tuple[str, dict]] = []
    if isinstance(payload, dict):
        dict_values = list(payload.values())
        if dict_values and all(isinstance(v, dict) for v in dict_values):
            for key, value in payload.items():
                if not isinstance(value, dict):
                    continue
                symbol = _extract_symbol(value, key, default_symbol)
                if symbol:
                    items.append((symbol, value))
        else:
            symbol = _extract_symbol(payload, None, default_symbol)
            if symbol:
                items.append((symbol, payload))
        return items
    if isinstance(payload, list):
        for value in payload:
            if not isinstance(value, dict):
                continue
            symbol = _extract_symbol(value, None, default_symbol)
            if symbol:
                items.append((symbol, value))
        return items
    return items


def _extract_symbol(payload: dict, fallback_key: str | None, default_symbol: str | None) -> str | None:
    for key in ("symbol", "s", "instId", "contract", "pair", "market"):
        val = payload.get(key)
        if isinstance(val, str) and val:
            return val
    if fallback_key:
        return fallback_key
    return default_symbol


def _parse_level(level) -> Tuple[float | None, float | None]:
    price = size = None
    if isinstance(level, dict):
        for key in ("price", "p", "px", "bp", "ap"):
            val = level.get(key)
            if val is None:
                continue
            try:
                price = float(val)
                break
            except Exception:
                continue
        for key in ("size", "qty", "q", "v", "volume"):
            val = level.get(key)
            if val is None:
                continue
            try:
                size = float(val)
                break
            except Exception:
                continue
    elif isinstance(level, (list, tuple)) and len(level) >= 2:
        try:
            price = float(level[0])
        except Exception:
            price = None
        try:
            size = float(level[1])
        except Exception:
            size = None
    if price is None or price <= 0:
        return None, None
    if size is None:
        return price, 0.0
    if size < 0:
        size = 0.0
    return price, size


@dataclass
class _MexcOrderBookState:
    bids: Dict[float, float] = field(default_factory=dict)
    asks: Dict[float, float] = field(default_factory=dict)
    def snapshot(self, bids, asks) -> None:
        self.bids.clear()
        self.asks.clear()
        self._apply(self.bids, bids)
        self._apply(self.asks, asks)
    def update(self, bids, asks) -> None:
        self._apply(self.bids, bids)
        self._apply(self.asks, asks)
    def top_levels(self, depth: int = 5) -> Tuple[List[Tuple[float, float]], List[Tuple[float, float]]]:
        bids_sorted = sorted(self.bids.items(), key=lambda kv: kv[0], reverse=True)[:depth]
        asks_sorted = sorted(self.asks.items(), key=lambda kv: kv[0])[:depth]
        return bids_sorted, asks_sorted
    def _apply(self, side: Dict[float, float], updates) -> None:
        if not updates:
            return
        for price, size in _iter_levels(updates):
            if size <= 0:
                side.pop(price, None)
            else:
                side[price] = size
        if len(side) > 200:
            if side is self.asks:
                ordered = sorted(side.items(), key=lambda kv: kv[0])
            else:
                ordered = sorted(side.items(), key=lambda kv: kv[0], reverse=True)
            trimmed = dict(ordered[:200])
            side.clear()
            side.update(trimmed)


async def authenticate_ws(ws: Any, creds: ApiCreds | None) -> None:
    """Placeholder for future authenticated MEXC channels."""
    del ws, creds
    return None
