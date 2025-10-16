from __future__ import annotations

import inspect
import json as jsonlib
from dataclasses import dataclass
from typing import Any, Callable, Dict
from urllib.parse import urlencode, urlparse

__all__ = [
    "AsyncClient",
    "HTTPStatusError",
    "MockTransport",
    "Request",
    "Response",
    "Timeout",
]


class HTTPStatusError(Exception):
    def __init__(self, status_code: int, response: "Response") -> None:
        super().__init__(f"HTTP request failed with status {status_code}")
        self.response = response
        self.status_code = int(status_code)


@dataclass(slots=True)
class Timeout:
    timeout: float | None = None
    connect: float | None = None
    read: float | None = None
    write: float | None = None


@dataclass(slots=True)
class URL:
    scheme: str
    host: str
    port: int | None
    path: str
    query: str

    def __str__(self) -> str:
        netloc = self.host
        if self.port:
            netloc = f"{netloc}:{self.port}"
        return f"{self.scheme}://{netloc}{self.path}{('?' + self.query) if self.query else ''}"


class Request:
    def __init__(
        self,
        method: str,
        url: str,
        *,
        params: Dict[str, Any] | None = None,
        headers: Dict[str, str] | None = None,
    ) -> None:
        parsed = urlparse(url)
        query_args: Dict[str, Any] = {}
        if parsed.query:
            for item in parsed.query.split("&"):
                if not item:
                    continue
                if "=" in item:
                    key, value = item.split("=", 1)
                else:
                    key, value = item, ""
                query_args[key] = value
        if params:
            for key, value in params.items():
                query_args[str(key)] = value
        query = urlencode({k: str(v) for k, v in query_args.items()})
        self.method = method.upper()
        self.url = URL(
            scheme=parsed.scheme or "https",
            host=parsed.hostname or "",
            port=parsed.port,
            path=parsed.path or "/",
            query=query,
        )
        self.headers = {k: v for k, v in (headers or {}).items()}
        self.params = query_args


class Response:
    def __init__(self, status_code: int, *, json: Any | None = None, text: str | None = None) -> None:
        json_data = json
        self.status_code = int(status_code)
        self._json_payload = json_data
        if text is not None:
            self._text = text
        elif json_data is not None:
            try:
                self._text = jsonlib.dumps(json_data)
            except TypeError:
                self._text = jsonlib.dumps(json_data, default=str)
        else:
            self._text = ""

    def json(self) -> Any:
        if self._json_payload is not None:
            return self._json_payload
        if not self._text:
            return None
        return jsonlib.loads(self._text)

    @property
    def text(self) -> str:
        return self._text

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise HTTPStatusError(self.status_code, self)


class MockTransport:
    def __init__(self, handler: Callable[[Request], Response | Any]) -> None:
        self._handler = handler

    async def handle(self, request: Request) -> Response:
        result = self._handler(request)
        if inspect.isawaitable(result):
            result = await result
        if not isinstance(result, Response):
            raise TypeError("Mock transport handler must return httpx.Response")
        return result

    # Backwards compatibility with httpx naming
    handle_request = handle


class AsyncClient:
    def __init__(
        self,
        *,
        timeout: Timeout | float | int | None = None,
        headers: Dict[str, str] | None = None,
        transport: MockTransport | None = None,
        http2: bool | None = None,
    ) -> None:
        if isinstance(timeout, (int, float)):
            timeout = Timeout(timeout=float(timeout))
        self.timeout = timeout
        self.headers = {k: v for k, v in (headers or {}).items()}
        self._transport = transport
        self._closed = False

    async def __aenter__(self) -> "AsyncClient":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.aclose()

    async def get(
        self,
        url: str,
        *,
        params: Dict[str, Any] | None = None,
        headers: Dict[str, str] | None = None,
    ) -> Response:
        if self._closed:
            raise RuntimeError("Client is closed")
        request_headers = dict(self.headers)
        if headers:
            request_headers.update(headers)
        request = Request("GET", url, params=params, headers=request_headers)
        if self._transport is None:
            raise RuntimeError("No transport available for AsyncClient")
        return await self._transport.handle(request)

    async def aclose(self) -> None:
        self._closed = True
