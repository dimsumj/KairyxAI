from __future__ import annotations

from contextlib import contextmanager
from functools import lru_cache
from typing import Generator

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, declarative_base, sessionmaker

from .settings import get_settings


Base = declarative_base()


@lru_cache(maxsize=1)
def get_engine():
    settings = get_settings()
    connect_args = {"check_same_thread": False} if settings.control_plane_database_url.startswith("sqlite") else {}
    return create_engine(settings.control_plane_database_url, future=True, pool_pre_ping=True, connect_args=connect_args)


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
