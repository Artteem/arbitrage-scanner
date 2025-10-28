from __future__ import annotations

import hashlib
import hmac
import json
import time
from typing import Any, Dict, Tuple
from urllib.parse import urlencode

from .credentials import ApiCreds


def now_ms() -> int:
    return int(time.time() * 1000)


def _json_body(body: Any) -> str:
    if body is None:
        return ""
    if isinstance(body, str):
        return body
    return json.dumps(body, separators=(",", ":"), ensure_ascii=False)


def hmac_sha256_hex(secret: str, payload: str) -> str:
    return hmac.new(secret.encode("utf-8"), payload.encode("utf-8"), hashlib.sha256).hexdigest()


def hmac_sha512_hex(secret: str, payload: str) -> str:
    return hmac.new(secret.encode("utf-8"), payload.encode("utf-8"), hashlib.sha512).hexdigest()


def sign_request(
    exchange: str,
    method: str,
    path: str,
    query: Dict[str, Any] | None,
    body: Dict[str, Any] | str | None,
    creds: ApiCreds,
) -> Tuple[Dict[str, str], str]:
    exchange_key = exchange.lower()
    if exchange_key == "binance":
        return _sign_binance(method, path, query, body, creds)
    if exchange_key == "bybit":
        return _sign_bybit(method, path, query, body, creds)
    if exchange_key == "mexc":
        return _sign_mexc(method, path, query, body, creds)
    if exchange_key == "gate":
        return _sign_gate(method, path, query, body, creds)
    if exchange_key == "bingx":
        return _sign_bingx(method, path, query, body, creds)
    raise ValueError(f"Signing for exchange '{exchange}' is not implemented")


def _encode_query(query: Dict[str, Any] | None) -> str:
    if not query:
        return ""
    items: list[tuple[str, Any]] = []
    for key, value in query.items():
        if isinstance(value, list):
            for item in value:
                items.append((key, item))
        else:
            items.append((key, value))
    return urlencode(items, doseq=True)


def _sign_binance(method: str, path: str, query: Dict[str, Any] | None, body: Any, creds: ApiCreds) -> Tuple[Dict[str, str], str]:
    params = dict(query or {})
    params.setdefault("recvWindow", "5000")
    params["timestamp"] = str(now_ms())
    query_string = _encode_query(params)
    signature = hmac_sha256_hex(creds.secret, query_string)
    signed_query = f"{query_string}&signature={signature}" if query_string else f"signature={signature}"
    headers = {"X-MBX-APIKEY": creds.key}
    return headers, signed_query


def _sign_bybit(method: str, path: str, query: Dict[str, Any] | None, body: Any, creds: ApiCreds) -> Tuple[Dict[str, str], str]:
    timestamp = str(now_ms())
    recv_window = "5000"
    query_string = _encode_query(query)
    body_string = _json_body(body)
    payload = f"{timestamp}{creds.key}{recv_window}{query_string}{body_string}"
    signature = hmac_sha256_hex(creds.secret, payload)
    headers = {
        "X-BAPI-API-KEY": creds.key,
        "X-BAPI-SIGN": signature,
        "X-BAPI-TIMESTAMP": timestamp,
        "X-BAPI-RECV-WINDOW": recv_window,
    }
    return headers, query_string


def _sign_mexc(method: str, path: str, query: Dict[str, Any] | None, body: Any, creds: ApiCreds) -> Tuple[Dict[str, str], str]:
    timestamp = str(now_ms())
    query_string = _encode_query(query)
    body_string = _json_body(body)
    payload = f"{timestamp}{method.upper()}{path}{query_string}{body_string}"
    signature = hmac_sha256_hex(creds.secret, payload)
    headers = {
        "X-MEXC-APIKEY": creds.key,
        "Timestamp": timestamp,
        "Signature": signature,
    }
    return headers, query_string


def _sign_gate(method: str, path: str, query: Dict[str, Any] | None, body: Any, creds: ApiCreds) -> Tuple[Dict[str, str], str]:
    timestamp = str(now_ms() / 1000)
    query_string = _encode_query(query)
    body_string = _json_body(body)
    payload = "\n".join([method.upper(), path, query_string, body_string, timestamp])
    signature = hmac_sha512_hex(creds.secret, payload)
    headers = {
        "KEY": creds.key,
        "Timestamp": timestamp,
        "SIGN": signature,
    }
    return headers, query_string


def _sign_bingx(method: str, path: str, query: Dict[str, Any] | None, body: Any, creds: ApiCreds) -> Tuple[Dict[str, str], str]:
    params = dict(query or {})
    params["timestamp"] = str(now_ms())
    query_string = _encode_query(params)
    signature = hmac_sha256_hex(creds.secret, query_string)
    signed_query = f"{query_string}&signature={signature}" if query_string else f"signature={signature}"
    headers = {"X-BX-APIKEY": creds.key}
    return headers, signed_query


__all__ = ["sign_request", "now_ms", "hmac_sha256_hex"]
