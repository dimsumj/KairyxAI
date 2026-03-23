from __future__ import annotations

from contextlib import contextmanager
from functools import lru_cache
from typing import Generator

from sqlalchemy import create_engine, event
from sqlalchemy.orm import Session, declarative_base, sessionmaker

from .settings import get_settings
from runtime_paths import normalize_sqlite_database_url


Base = declarative_base()


@lru_cache(maxsize=1)
def get_engine():
    settings = get_settings()
    database_url = normalize_sqlite_database_url(settings.control_plane_database_url)
    connect_args = {}
    if database_url.startswith("sqlite"):
        connect_args = {
            "check_same_thread": False,
            "timeout": settings.sqlite_busy_timeout_seconds,
        }
    engine = create_engine(database_url, future=True, pool_pre_ping=True, connect_args=connect_args)
    if database_url.startswith("sqlite"):
        busy_timeout_ms = int(settings.sqlite_busy_timeout_seconds * 1000)

        @event.listens_for(engine, "connect")
        def _configure_sqlite_connection(dbapi_connection, _connection_record):
            cursor = dbapi_connection.cursor()
            try:
                cursor.execute(f"PRAGMA busy_timeout={busy_timeout_ms};")
                cursor.execute("PRAGMA journal_mode=WAL;")
                cursor.execute("PRAGMA synchronous=NORMAL;")
                cursor.execute("PRAGMA foreign_keys=ON;")
            finally:
                cursor.close()
    return engine


@lru_cache(maxsize=1)
def get_session_factory():
    return sessionmaker(bind=get_engine(), autoflush=False, autocommit=False, future=True, expire_on_commit=False)


def init_db() -> None:
    from app.infrastructure import db_models  # noqa: F401

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
