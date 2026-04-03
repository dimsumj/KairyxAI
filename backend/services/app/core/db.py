from __future__ import annotations

import logging
import os
from contextlib import contextmanager
from functools import lru_cache
from pathlib import Path
from typing import Generator

from alembic import command
from alembic.config import Config
from sqlalchemy import Integer, create_engine, event, func, select, text
from sqlalchemy import inspect as sa_inspect
from sqlalchemy.engine import Engine
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


CONTROL_PLANE_REVISION = "20260307_0001"
RESOURCE_REVISION = "20260310_0002"
MULTITENANT_REVISION = "20260322_0003"
PROJECT_ONBOARDING_REVISION = "20260324_0004"


@lru_cache(maxsize=1)
def get_engine():
    settings = get_settings()
    database_url = get_effective_database_url()
    connect_args = {}
    if database_url.startswith("sqlite"):
        connect_args = {
            "check_same_thread": False,
            "timeout": settings.sqlite_busy_timeout_seconds,
        }
    elif database_url.startswith("postgresql+psycopg://"):
        connect_args = {
            "connect_timeout": settings.control_plane_connect_timeout_seconds,
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


def _services_dir() -> Path:
    return Path(__file__).resolve().parents[2]


def _build_alembic_config(database_url: str) -> Config:
    services_dir = _services_dir()
    config = Config(str(services_dir / "alembic.ini"))
    config.set_main_option("script_location", str(services_dir / "alembic"))
    config.set_main_option("sqlalchemy.url", database_url)
    return config


def _table_has_column(engine: Engine, table_name: str, column_name: str) -> bool:
    inspector = sa_inspect(engine)
    try:
        columns = inspector.get_columns(table_name)
    except Exception:
        return False
    return any(str(column.get("name")) == column_name for column in columns)


def _infer_legacy_revision(engine: Engine) -> str | None:
    inspector = sa_inspect(engine)
    table_names = set(inspector.get_table_names())
    if not table_names or table_names == {"alembic_version"}:
        return None
    if "alembic_version" in table_names:
        return None
    if "connector_configs" in table_names and _table_has_column(engine, "connector_configs", "project_id"):
        return PROJECT_ONBOARDING_REVISION
    if "connector_configs" in table_names and _table_has_column(engine, "connector_configs", "tenant_id"):
        return MULTITENANT_REVISION
    if "control_plane_resources_v1" in table_names:
        return RESOURCE_REVISION
    legacy_control_plane_tables = {
        "connector_configs",
        "field_mappings_v2",
        "import_jobs_v2",
        "prediction_jobs_v2",
        "export_jobs_v2",
        "experiment_configs",
        "action_history_v2",
        "ingestion_checkpoints_v2",
    }
    if table_names & legacy_control_plane_tables:
        return CONTROL_PLANE_REVISION
    return None


def _run_control_plane_migrations() -> None:
    database_url = get_effective_database_url()
    engine = get_engine()
    legacy_revision = _infer_legacy_revision(engine)
    alembic_config = _build_alembic_config(database_url)
    if legacy_revision:
        command.stamp(alembic_config, legacy_revision)
    command.upgrade(alembic_config, "head")


def _initialize_schema() -> None:
    _run_control_plane_migrations()
    engine = get_engine()
    Base.metadata.create_all(bind=engine)
    _align_postgres_identity_sequences(engine)


def _align_postgres_identity_sequences(engine: Engine, *, tables=None) -> None:
    if engine.dialect.name != "postgresql":
        return
    target_tables = list(tables or Base.metadata.sorted_tables)
    if not target_tables:
        return
    with engine.begin() as connection:
        for table in target_tables:
            primary_key_columns = list(table.primary_key.columns)
            if len(primary_key_columns) != 1:
                continue
            primary_key_column = primary_key_columns[0]
            if primary_key_column.name != "id" or not isinstance(primary_key_column.type, Integer):
                continue
            qualified_table_name = ".".join(
                [part for part in (table.schema, table.name) if part]
            )
            sequence_name = connection.execute(
                text("SELECT pg_get_serial_sequence(:table_name, :column_name)"),
                {
                    "table_name": qualified_table_name,
                    "column_name": primary_key_column.name,
                },
            ).scalar_one_or_none()
            if not sequence_name:
                continue
            max_identifier = connection.execute(
                select(func.coalesce(func.max(primary_key_column), 0)).select_from(table)
            ).scalar_one()
            next_identifier = int(max_identifier or 0) + 1
            connection.execute(
                text("SELECT setval(:sequence_name, :next_identifier, false)"),
                {
                    "sequence_name": sequence_name,
                    "next_identifier": next_identifier,
                },
            )


def init_db() -> None:
    from app.infrastructure import db_models  # noqa: F401

    try:
        _initialize_schema()
    except SQLAlchemyError as exc:
        database_url = get_effective_database_url()
        if not _should_fallback_to_local_sqlite(database_url):
            raise
        _activate_runtime_database_fallback(database_url, exc)
        _initialize_schema()


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
