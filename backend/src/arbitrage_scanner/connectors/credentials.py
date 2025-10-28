from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from typing import Optional

from ..settings import Settings

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class ApiCreds:
    key: str
    secret: str
    passphrase: Optional[str] = None


class CredentialsProvider:
    """Resolves API credentials for exchanges from environment or storage."""

    def __init__(self, settings: Settings):
        self._settings = settings

    def get(self, exchange: str) -> Optional[ApiCreds]:
        mapping = self._settings.credentials_env_map.get(exchange.lower())
        if not mapping:
            return None
        key_env = mapping.get("key_env")
        secret_env = mapping.get("secret_env")
        pass_env = mapping.get("passphrase_env")
        if not key_env or not secret_env:
            return None
        key = os.getenv(key_env)
        secret = os.getenv(secret_env)
        if not key or not secret:
            return None
        passphrase = os.getenv(pass_env) if pass_env else None
        return ApiCreds(key=key, secret=secret, passphrase=passphrase)


_provider: CredentialsProvider | None = None


def set_credentials_provider(provider: CredentialsProvider) -> None:
    global _provider
    _provider = provider
    available = [name for name in provider._settings.credentials_env_map if provider.get(name)]
    if available:
        logger.info("REST signing enabled for: %s", ", ".join(sorted(available)))
    else:
        logger.info("REST signing enabled for: (none)")


def get_credentials_provider() -> Optional[CredentialsProvider]:
    return _provider


__all__ = ["ApiCreds", "CredentialsProvider", "get_credentials_provider", "set_credentials_provider"]
