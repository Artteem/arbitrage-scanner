from __future__ import annotations

import importlib
import logging
from typing import Iterable, List

from ..settings import settings
from .base import ConnectorSpec
from .credentials import CredentialsProvider, set_credentials_provider


logger = logging.getLogger(__name__)


def load_connectors(enabled: Iterable[str]) -> List[ConnectorSpec]:
    set_credentials_provider(CredentialsProvider(settings))
    connectors: List[ConnectorSpec] = []
    failures: dict[str, BaseException] = {}

    for raw_name in enabled:
        name = raw_name.strip()
        if not name:
            continue
        module_name = f"{__name__.rsplit('.', 1)[0]}.{name}"
        try:
            module = importlib.import_module(module_name)
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.exception("Failed to import connector module %s", module_name)
            failures[name] = exc
            continue

        spec = getattr(module, "connector", None)
        if not isinstance(spec, ConnectorSpec):
            logger.error(
                "Connector module '%s' does not define a valid ConnectorSpec", module_name
            )
            failures[name] = RuntimeError("invalid connector spec")
            continue

        connectors.append(spec)

    if not connectors:
        raise RuntimeError(
            "No connectors were loaded. Check ENABLED_EXCHANGES configuration"
        )

    if failures:
        logger.warning(
            "Skipped %d connector(s) due to startup errors: %s",
            len(failures),
            ", ".join(sorted(failures.keys())),
        )

    return connectors
