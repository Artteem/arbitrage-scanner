from __future__ import annotations

from contextlib import asynccontextmanager
from typing import AsyncIterator

from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine

from arbitrage_scanner.settings import settings

_engine: AsyncEngine | None = None
_session_factory: async_sessionmaker[AsyncSession] | None = None


def get_engine() -> AsyncEngine:
    global _engine
    if _engine is None:
        db_settings = settings.database
        _engine = create_async_engine(
            db_settings.url,
            echo=db_settings.echo,
            pool_size=db_settings.pool_size,
            max_overflow=db_settings.max_overflow,
            pool_timeout=db_settings.pool_timeout,
            pool_recycle=db_settings.pool_recycle,
        )
    return _engine


def get_session_factory() -> async_sessionmaker[AsyncSession]:
    global _session_factory
    if _session_factory is None:
        _session_factory = async_sessionmaker(get_engine(), expire_on_commit=False)
    return _session_factory


@asynccontextmanager
async def get_session() -> AsyncIterator[AsyncSession]:
    session = get_session_factory()()
    try:
        yield session
    finally:
        await session.close()
