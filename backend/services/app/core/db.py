from __future__ import annotations

import logging
import os
from contextlib import contextmanager
from functools import lru_cache
from typing import Generator

from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session, declarative_base, sessionmaker

from .settings import get_settings
from runtime_paths import default_control_plane_database_url, normalize_env_text, normalize_sqlite_database_url


Base = declarative_base()
logger = logging.getLogger(__name__)
_runtime_database_url_override: str | None = None
_runtime_database_fallback_reason: str = ""


def normalize_database_url(raw_url: str) -> str:
    database_url = normalize_sqlite_database_url(normalize_env_text(raw_url))
    scheme, separator, remainder = database_url.partition("://")
    if not separator:
        return database_url
    if scheme == "postgres":
        return f"postgresql+psycopg://{remainder}"
    if scheme == "postgresql":
        return f"postgresql+psycopg://{remainder}"
    return database_url


def _configured_database_url() -> str:
    return normalize_database_url(get_settings().control_plane_database_url)


def get_effective_database_url() -> str:
    if _runtime_database_url_override:
        return normalize_database_url(_runtime_database_url_override)
    return _configured_database_url()


def clear_runtime_database_fallback() -> None:
    global _runtime_database_url_override, _runtime_database_fallback_reason

    _runtime_database_url_override = None
    _runtime_database_fallback_reason = ""
    get_engine.cache_clear()
    get_session_factory.cache_clear()


def is_runtime_database_fallback_active() -> bool:
    return bool(_runtime_database_url_override)


def is_control_plane_database_persistent() -> bool:
    effective_url = get_effective_database_url()
    if effective_url.startswith("sqlite"):
        return not bool(normalize_env_text(os.getenv("VERCEL", "")))
    return True


def get_runtime_database_status() -> dict[str, str | bool]:
    effective_url = get_effective_database_url()
    scheme, separator, _ = effective_url.partition("://")
    return {
        "configured_url": _configured_database_url(),
        "effective_url": effective_url,
        "backend": scheme if separator else effective_url,
        "persistent": is_control_plane_database_persistent(),
        "fallback_active": is_runtime_database_fallback_active(),
        "fallback_reason": _runtime_database_fallback_reason,
    }


def _should_fallback_to_local_sqlite(database_url: str) -> bool:
    if is_runtime_database_fallback_active():
        return False
    if database_url.startswith("sqlite"):
        return False
    if not normalize_env_text(os.getenv("VERCEL", "")):
        return False
    return normalize_env_text(os.getenv("DATA_BACKEND_MODE", "mock")).lower() == "mock"


def _activate_runtime_database_fallback(database_url: str, exc: SQLAlchemyError) -> None:
    global _runtime_database_url_override, _runtime_database_fallback_reason

    fallback_url = normalize_database_url(default_control_plane_database_url())
    if database_url == fallback_url:
        raise exc

    _runtime_database_url_override = fallback_url
    _runtime_database_fallback_reason = str(exc)
    get_engine.cache_clear()
    get_session_factory.cache_clear()
    logger.exception("Control plane database unavailable; falling back to local runtime SQLite database.")


@lru_cache(maxsize=1)
def get_engine():
    database_url = get_effective_database_url()
    connect_args = {"check_same_thread": False} if database_url.startswith("sqlite") else {}
    return create_engine(database_url, future=True, pool_pre_ping=True, connect_args=connect_args)


@lru_cache(maxsize=1)
def get_session_factory():
    return sessionmaker(bind=get_engine(), autoflush=False, autocommit=False, future=True, expire_on_commit=False)


def init_db() -> None:
    from app.infrastructure import db_models  # noqa: F401

    try:
        Base.metadata.create_all(bind=get_engine())
    except SQLAlchemyError as exc:
        database_url = get_effective_database_url()
        if not _should_fallback_to_local_sqlite(database_url):
            raise
        _activate_runtime_database_fallback(database_url, exc)
        Base.metadata.create_all(bind=get_engine())


def get_db_session() -> Generator[Session, None, None]:
    session = get_session_factory()()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


@contextmanager
def session_scope() -> Generator[Session, None, None]:
    session = get_session_factory()()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
