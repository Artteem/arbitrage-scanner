from __future__ import annotations

import asyncio
import inspect
import json
from dataclasses import dataclass
from typing import Any, Dict, Iterable
from urllib.parse import parse_qsl, urlsplit

from . import FastAPI, HTTPException, QueryInfo
from .responses import HTMLResponse

__all__ = ["TestClient"]


@dataclass(slots=True)
class _ClientResponse:
    status_code: int
    _payload: Any = None
    text: str = ""

    def json(self) -> Any:
        if self._payload is not None:
            return self._payload
        if not self.text:
            raise ValueError("Response does not contain JSON data")
        return json.loads(self.text)


class TestClient:
    """Very small, synchronous test client used in the unit tests."""

    def __init__(self, app: FastAPI) -> None:
        self._app = app

    # ------------------------------------------------------------------
    def get(self, path: str, params: Dict[str, Any] | None = None) -> _ClientResponse:
        return self._request("GET", path, params=params)

    # ------------------------------------------------------------------
    def _request(self, method: str, url: str, params: Dict[str, Any] | None = None) -> _ClientResponse:
        clean_path, query_params = self._prepare_query(url, params)
        try:
            endpoint, path_params = self._app.find_route(method, clean_path)
        except HTTPException as exc:
            return _ClientResponse(status_code=exc.status_code, _payload={"detail": exc.detail})

        try:
            kwargs = self._build_kwargs(endpoint, path_params, query_params)
            result = endpoint(**kwargs)
            if inspect.isawaitable(result):
                result = asyncio.run(result)
        except HTTPException as exc:
            return _ClientResponse(status_code=exc.status_code, _payload={"detail": exc.detail})

        return self._to_response(result)

    # ------------------------------------------------------------------
    @staticmethod
    def _prepare_query(raw_url: str, params: Dict[str, Any] | None) -> tuple[str, Dict[str, str]]:
        parsed = urlsplit(raw_url)
        query_items: Dict[str, str] = {}
        for key, value in parse_qsl(parsed.query, keep_blank_values=True):
            query_items[key] = value
        if params:
            for key, value in params.items():
                query_items[key] = str(value)
        path = parsed.path or "/"
        return path, query_items

    # ------------------------------------------------------------------
    def _build_kwargs(
        self,
        endpoint: Any,
        path_params: Dict[str, str],
        query_params: Dict[str, str],
    ) -> Dict[str, Any]:
        signature = inspect.signature(endpoint)
        provided: Dict[str, Any] = {}
        for name, param in signature.parameters.items():
            if name in path_params:
                raw_value = path_params[name]
                query_info: QueryInfo | None = None
            else:
                raw_value, query_info = self._resolve_query_parameter(name, param, query_params)
            value = self._convert_value(param.annotation, raw_value)
            if query_info is not None:
                self._validate_query(name, query_info, value)
            provided[name] = value
        return provided

    def _resolve_query_parameter(
        self,
        name: str,
        param: inspect.Parameter,
        query_params: Dict[str, str],
    ) -> tuple[Any, QueryInfo | None]:
        default = param.default
        if isinstance(default, QueryInfo):
            info = default
            keys: Iterable[str] = (key for key in (info.alias, name) if key)
            for key in keys:
                if key in query_params:
                    return query_params.pop(key), info
            if info.default is ...:
                raise HTTPException(status_code=422, detail=f"Missing query parameter: {name}")
            return info.default, info
        if name in query_params:
            return query_params.pop(name), None
        if default is inspect._empty:
            raise HTTPException(status_code=422, detail=f"Missing parameter: {name}")
        return default, None

    @staticmethod
    def _convert_value(annotation: Any, value: Any) -> Any:
        if value is None or annotation is inspect._empty or annotation is Any:
            return value
        try:
            if annotation is bool:
                if isinstance(value, bool):
                    return value
                text = str(value).strip().lower()
                if text in {"1", "true", "t", "yes", "on"}:
                    return True
                if text in {"0", "false", "f", "no", "off"}:
                    return False
                return bool(text)
            if annotation in {int, float, str}:
                return annotation(value)
        except (TypeError, ValueError) as exc:  # pragma: no cover - defensive branch
            raise HTTPException(status_code=400, detail=f"Invalid value for {annotation}: {value!r}") from exc
        return value

    @staticmethod
    def _validate_query(name: str, info: QueryInfo, value: Any) -> None:
        if info.ge is not None:
            try:
                if float(value) < info.ge:
                    raise HTTPException(status_code=400, detail=f"{name} must be >= {info.ge}")
            except (TypeError, ValueError) as exc:  # pragma: no cover - defensive
                raise HTTPException(status_code=400, detail=f"{name} must be a number") from exc
        if info.gt is not None:
            try:
                if float(value) <= info.gt:
                    raise HTTPException(status_code=400, detail=f"{name} must be > {info.gt}")
            except (TypeError, ValueError) as exc:  # pragma: no cover - defensive
                raise HTTPException(status_code=400, detail=f"{name} must be a number") from exc

    @staticmethod
    def _to_response(result: Any) -> _ClientResponse:
        if isinstance(result, _ClientResponse):
            return result
        if isinstance(result, HTMLResponse):
            return _ClientResponse(status_code=result.status_code, text=result.body)
        if isinstance(result, tuple) and len(result) == 2:
            status, payload = result
            if isinstance(payload, (dict, list)):
                return _ClientResponse(status_code=int(status), _payload=payload)
            return _ClientResponse(status_code=int(status), text=str(payload))
        if isinstance(result, (dict, list)):
            return _ClientResponse(status_code=200, _payload=result)
        return _ClientResponse(status_code=200, text=str(result))
