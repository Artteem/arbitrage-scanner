from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Dict, Iterable, Optional

__all__ = [
    "FastAPI",
    "HTTPException",
    "Query",
    "QueryInfo",
    "WebSocket",
]


class HTTPException(Exception):
    """Minimal HTTP-style exception used by the tests."""

    def __init__(self, status_code: int, detail: Any | None = None) -> None:
        super().__init__(detail)
        self.status_code = int(status_code)
        self.detail = detail


@dataclass(slots=True)
class QueryInfo:
    default: Any = ...
    alias: str | None = None
    ge: float | None = None
    gt: float | None = None


def Query(default: Any = ..., *, alias: str | None = None, ge: float | None = None, gt: float | None = None) -> QueryInfo:
    """Return metadata describing a query parameter."""

    return QueryInfo(default=default, alias=alias, ge=ge, gt=gt)


class WebSocket:  # pragma: no cover - placeholder for compatibility
    pass


@dataclass(slots=True)
class _Route:
    method: str
    path: str
    endpoint: Callable[..., Any]
    segments: tuple[str, ...]

    def match(self, method: str, request_path: str) -> Optional[Dict[str, str]]:
        if method.upper() != self.method:
            return None
        path, *_ = request_path.split("?", 1)
        request_segments = tuple(seg for seg in path.split("/") if seg)
        if len(request_segments) != len(self.segments):
            return None
        params: Dict[str, str] = {}
        for template, actual in zip(self.segments, request_segments):
            if template.startswith("{") and template.endswith("}"):
                params[template[1:-1]] = actual
                continue
            if template != actual:
                return None
        return params


class FastAPI:
    """Tiny subset of the FastAPI API sufficient for the tests."""

    def __init__(self, title: str | None = None, version: str | None = None) -> None:
        self.title = title or ""
        self.version = version or ""
        self._routes: list[_Route] = []
        self._event_handlers: dict[str, list[Callable[[], Any]]] = {
            "startup": [],
            "shutdown": [],
        }
        self._websockets: list[_Route] = []

    # ------------------------------------------------------------------
    # Route registration helpers
    def _register_route(self, method: str, path: str, endpoint: Callable[..., Any]) -> Callable[..., Any]:
        segments = tuple(seg for seg in path.split("/") if seg)
        self._routes.append(_Route(method.upper(), path, endpoint, segments))
        return endpoint

    def get(self, path: str) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        def decorator(fn: Callable[..., Any]) -> Callable[..., Any]:
            return self._register_route("GET", path, fn)

        return decorator

    def post(self, path: str) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        def decorator(fn: Callable[..., Any]) -> Callable[..., Any]:
            return self._register_route("POST", path, fn)

        return decorator

    def websocket(self, path: str) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        def decorator(fn: Callable[..., Any]) -> Callable[..., Any]:
            segments = tuple(seg for seg in path.split("/") if seg)
            self._websockets.append(_Route("WEBSOCKET", path, fn, segments))
            return fn

        return decorator

    # ------------------------------------------------------------------
    # Event registration
    def on_event(self, event: str) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        key = event.lower()
        if key not in self._event_handlers:
            self._event_handlers[key] = []

        def decorator(fn: Callable[..., Any]) -> Callable[..., Any]:
            self._event_handlers[key].append(fn)
            return fn

        return decorator

    # ------------------------------------------------------------------
    # Introspection helpers for the TestClient
    def iter_routes(self) -> Iterable[_Route]:
        return tuple(self._routes)

    def get_event_handlers(self, event: str) -> list[Callable[..., Any]]:
        return list(self._event_handlers.get(event, ()))

    def find_route(self, method: str, path: str) -> tuple[Callable[..., Any], Dict[str, str]]:
        for route in self._routes:
            params = route.match(method, path)
            if params is not None:
                return route.endpoint, params
        raise HTTPException(status_code=404, detail="Not Found")
