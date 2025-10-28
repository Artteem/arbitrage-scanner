from __future__ import annotations

import importlib
from typing import Iterable, List

from ..settings import settings
from .base import ConnectorSpec
from .credentials import CredentialsProvider, set_credentials_provider


def load_connectors(enabled: Iterable[str]) -> List[ConnectorSpec]:
    set_credentials_provider(CredentialsProvider(settings))
    connectors: List[ConnectorSpec] = []
    for raw_name in enabled:
        name = raw_name.strip()
        if not name:
            continue
        module_name = f"{__name__.rsplit('.', 1)[0]}.{name}"
        module = importlib.import_module(module_name)
        spec = getattr(module, "connector", None)
        if not isinstance(spec, ConnectorSpec):
            raise RuntimeError(
                f"Connector module '{module_name}' must define 'connector' of type ConnectorSpec"
            )
        connectors.append(spec)
    if not connectors:
        raise RuntimeError("No connectors were loaded. Check ENABLED_EXCHANGES configuration")
    return connectors
