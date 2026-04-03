from __future__ import annotations

import logging
import sqlite3
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import Column, Integer, MetaData, String, Table
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.sql.selectable import Select
from sqlalchemy.sql.elements import TextClause

import app.main as main_module
from app.core import db as db_module
from app.core.errors import is_database_locked_error
from app.core.settings import get_settings
from app.core.logging import PredictionPollingAccessFilter
from app.main import create_app
from bigquery_service import clear_shared_bigquery_service_cache, get_shared_bigquery_service
import local_job_store
from local_job_store import list_identity_links, resolve_or_create_canonical_user_id
import runtime_paths


@pytest.fixture(autouse=True)
def _reset_runtime_database_state():
    db_module.clear_runtime_database_fallback()
    clear_shared_bigquery_service_cache()
    yield
    clear_shared_bigquery_service_cache()
    db_module.clear_runtime_database_fallback()


def test_local_job_store_accepts_sqlite_url_override(tmp_path, monkeypatch):
    target = tmp_path / "nested" / "state" / "local_jobs.db"
    monkeypatch.setenv("KAIRYX_LOCAL_DB_PATH", f"sqlite:///{target}")

    canonical = resolve_or_create_canonical_user_id("amplitude", "user-123")

    assert canonical == "uid:user-123"
    assert target.exists()
    assert list_identity_links(limit=1)[0]["canonical_user_id"] == "uid:user-123"


def test_app_startup_creates_sqlite_parent_dirs(tmp_path, monkeypatch):
    target = tmp_path / "nested" / "state" / "control_plane.db"
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("DATA_BACKEND_MODE", "mock")
    monkeypatch.setenv("CONTROL_PLANE_DATABASE_URL", f"sqlite:///{target}")
    monkeypatch.setenv("KAIRYX_LOCAL_DB_PATH", str(tmp_path / "local_jobs.db"))
    db_module.get_engine.cache_clear()
    db_module.get_session_factory.cache_clear()

    app = create_app()
    with TestClient(app) as client:
        response = client.get("/api/v1/health")

    assert response.status_code == 200
    assert target.exists()


def test_app_startup_continues_when_restart_reconciliation_fails(tmp_path, monkeypatch):
    target = tmp_path / "nested" / "state" / "control_plane.db"
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("DATA_BACKEND_MODE", "mock")
    monkeypatch.setenv("CONTROL_PLANE_DATABASE_URL", f"sqlite:///{target}")
    monkeypatch.setenv("KAIRYX_LOCAL_DB_PATH", str(tmp_path / "local_jobs.db"))
    monkeypatch.setattr(
        "app.main.ImportService.reconcile_jobs_after_restart",
        lambda self: (_ for _ in ()).throw(RuntimeError("unable to open database file")),
    )
    db_module.get_engine.cache_clear()
    db_module.get_session_factory.cache_clear()

    app = create_app()
    with TestClient(app) as client:
        response = client.get("/api/v1/health")

    assert response.status_code == 200


def test_health_snapshot_warmup_retries_transient_database_error(monkeypatch):
    calls = {
        "snapshot": 0,
        "rollback": 0,
        "commit": 0,
        "close": 0,
        "dispose": 0,
    }

    class _FakeSession:
        def rollback(self):
            calls["rollback"] += 1

        def commit(self):
            calls["commit"] += 1

        def close(self):
            calls["close"] += 1

    class _FakeEngine:
        def dispose(self):
            calls["dispose"] += 1

    class _FakeHealthMonitor:
        def __init__(self, repository, bigquery_service):
            self.repository = repository
            self.bigquery_service = bigquery_service

        def snapshot(self, *, persist=True):
            calls["snapshot"] += 1
            if calls["snapshot"] == 1:
                raise SQLAlchemyError("idle in transaction")
            return {"status": "ok"}

    monkeypatch.setattr(main_module, "get_session_factory", lambda: (lambda: _FakeSession()))
    monkeypatch.setattr(main_module, "get_engine", lambda: _FakeEngine())
    monkeypatch.setattr(main_module, "SqlAlchemyControlPlaneRepository", lambda session: object())
    monkeypatch.setattr(main_module, "HealthMonitorService", _FakeHealthMonitor)
    monkeypatch.setattr(main_module, "get_shared_bigquery_service", lambda: object())

    settings = SimpleNamespace(
        bootstrap_tenant_id="default",
        bootstrap_project_id="default",
    )

    main_module._warm_health_snapshot_with_retry(settings)

    assert calls == {
        "snapshot": 2,
        "rollback": 1,
        "commit": 1,
        "close": 2,
        "dispose": 1,
    }


def test_database_url_normalization_uses_psycopg_driver():
    assert db_module.normalize_database_url("postgres://user:pass@example.com:5432/demo") == (
        "postgresql+psycopg://user:pass@example.com:5432/demo"
    )
    assert db_module.normalize_database_url("postgresql://user:pass@example.com:5432/demo") == (
        "postgresql+psycopg://user:pass@example.com:5432/demo"
    )
    assert db_module.normalize_database_url("postgresql+psycopg://user:pass@example.com:5432/demo") == (
        "postgresql+psycopg://user:pass@example.com:5432/demo"
    )


def test_normalize_env_text_unwraps_nested_quotes_and_newlines():
    assert runtime_paths.normalize_env_text('""postgresql://user:pass@example.com/demo"\\n"') == (
        "postgresql://user:pass@example.com/demo"
    )
    assert runtime_paths.normalize_env_text('"mock\\n"') == "mock"


def test_init_db_does_not_disable_uvicorn_loggers(tmp_path, monkeypatch):
    target = tmp_path / "nested" / "state" / "control_plane.db"
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("DATA_BACKEND_MODE", "mock")
    monkeypatch.setenv("CONTROL_PLANE_DATABASE_URL", f"sqlite:///{target}")
    monkeypatch.setenv("KAIRYX_LOCAL_DB_PATH", str(tmp_path / "local_jobs.db"))
    db_module.get_engine.cache_clear()
    db_module.get_session_factory.cache_clear()

    uvicorn_logger = logging.getLogger("uvicorn.error")
    original_disabled = uvicorn_logger.disabled
    uvicorn_logger.disabled = False
    try:
        db_module.init_db()
        assert uvicorn_logger.disabled is False
    finally:
        uvicorn_logger.disabled = original_disabled


def test_align_postgres_identity_sequences_repairs_next_identifier():
    metadata = MetaData()
    target_table = Table(
        "control_plane_resource_events_v1",
        metadata,
        Column("id", Integer, primary_key=True),
    )
    Table(
        "organization_memberships_v1",
        metadata,
        Column("organization_id", String, primary_key=True),
    )

    class _ScalarResult:
        def __init__(self, value):
            self._value = value

        def scalar_one_or_none(self):
            return self._value

        def scalar_one(self):
            return self._value

    class _FakeConnection:
        def __init__(self):
            self.calls = []

        def execute(self, statement, params=None):
            if isinstance(statement, TextClause):
                sql = str(statement)
                if "pg_get_serial_sequence" in sql:
                    self.calls.append(("sequence", params))
                    return _ScalarResult("public.control_plane_resource_events_v1_id_seq")
                if "setval" in sql:
                    self.calls.append(("setval", params))
                    return _ScalarResult(None)
            if isinstance(statement, Select):
                self.calls.append(("max", statement))
                return _ScalarResult(149)
            raise AssertionError(f"Unexpected statement: {statement!r}")

    class _FakeBegin:
        def __init__(self, connection):
            self._connection = connection

        def __enter__(self):
            return self._connection

        def __exit__(self, exc_type, exc, tb):
            return False

    class _FakeEngine:
        def __init__(self, connection):
            self.dialect = SimpleNamespace(name="postgresql")
            self._connection = connection

        def begin(self):
            return _FakeBegin(self._connection)

    fake_connection = _FakeConnection()
    fake_engine = _FakeEngine(fake_connection)

    db_module._align_postgres_identity_sequences(fake_engine, tables=[target_table])

    assert fake_connection.calls[0] == (
        "sequence",
        {
            "table_name": "control_plane_resource_events_v1",
            "column_name": "id",
        },
    )
    assert fake_connection.calls[1][0] == "max"
    assert fake_connection.calls[2] == (
        "setval",
        {
            "sequence_name": "public.control_plane_resource_events_v1_id_seq",
            "next_identifier": 150,
        },
    )


def test_local_job_store_closes_sqlite_connections(tmp_path, monkeypatch):
    target = tmp_path / "tracked" / "local_jobs.db"
    monkeypatch.setenv("KAIRYX_LOCAL_DB_PATH", str(target))

    real_connect = sqlite3.connect
    open_count = 0
    close_count = 0

    class TrackingConnection:
        def __init__(self, conn):
            self._conn = conn

        def close(self):
            nonlocal close_count
            close_count += 1
            return self._conn.close()

        def __getattr__(self, name):
            return getattr(self._conn, name)

    def tracking_connect(*args, **kwargs):
        nonlocal open_count
        open_count += 1
        return TrackingConnection(real_connect(*args, **kwargs))

    monkeypatch.setattr(local_job_store.sqlite3, "connect", tracking_connect)

    resolve_or_create_canonical_user_id("amplitude", "user-1")
    resolve_or_create_canonical_user_id("amplitude", "user-2")
    list_identity_links(limit=10)

    assert open_count > 0
    assert close_count == open_count


def test_database_lock_error_detection_handles_exception_groups():
    exc = ExceptionGroup(
        "database lock group",
        [sqlite3.OperationalError("database is locked")],
    )

    assert is_database_locked_error(exc) is True


def test_shared_bigquery_service_reuses_instance_per_runtime_context(tmp_path, monkeypatch):
    clear_shared_bigquery_service_cache()

    workspace_a = tmp_path / "workspace-a"
    workspace_b = tmp_path / "workspace-b"
    workspace_a.mkdir()
    workspace_b.mkdir()

    monkeypatch.chdir(workspace_a)
    monkeypatch.setenv("DATA_BACKEND_MODE", "mock")
    service_a1 = get_shared_bigquery_service()
    service_a2 = get_shared_bigquery_service()

    monkeypatch.chdir(workspace_b)
    service_b = get_shared_bigquery_service()

    assert service_a1 is service_a2
    assert service_a1 is not service_b


def test_vercel_runtime_defaults_use_tmp_storage(tmp_path, monkeypatch):
    monkeypatch.delenv("KAIRYX_RUNTIME_DIR", raising=False)
    monkeypatch.delenv("SCHEDULER_ENABLED", raising=False)
    monkeypatch.setenv("VERCEL", "1")
    monkeypatch.setattr(runtime_paths.tempfile, "gettempdir", lambda: str(tmp_path))

    control_plane_url = runtime_paths.default_control_plane_database_url()
    local_job_db = runtime_paths.default_local_job_store_path()
    cache_path = runtime_paths.resolve_runtime_file_path(".cache/demo.jsonl", ensure_parent=True)
    settings = get_settings()

    expected_root = tmp_path / "kairyxai-runtime"
    assert control_plane_url == f"sqlite:///{expected_root / '.kairyx_control_plane.db'}"
    assert local_job_db == expected_root / ".kairyx_local.db"
    assert cache_path == expected_root / ".cache" / "demo.jsonl"
    assert settings.scheduler_enabled is False


def test_vercel_remote_control_plane_db_falls_back_to_local_sqlite(tmp_path, monkeypatch):
    runtime_dir = tmp_path / "runtime"
    local_db = tmp_path / "local_jobs.db"
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("VERCEL", "1")
    monkeypatch.setenv("DATA_BACKEND_MODE", "mock")
    monkeypatch.setenv("KAIRYX_MOCK_STORAGE_BACKEND", "database")
    monkeypatch.setenv("CONTROL_PLANE_DATABASE_URL", "postgresql://demo:demo@127.0.0.1:1/kairyx")
    monkeypatch.setenv("CONTROL_PLANE_CONNECT_TIMEOUT_SECONDS", "1")
    monkeypatch.setenv("KAIRYX_RUNTIME_DIR", str(runtime_dir))
    monkeypatch.setenv("KAIRYX_LOCAL_DB_PATH", str(local_db))

    app = create_app()
    with TestClient(app) as client:
        response = client.get("/api/v1/health")

    payload = response.json()
    fallback_db = runtime_dir / ".kairyx_control_plane.db"

    assert response.status_code == 200
    assert payload["mock_state_backend"] == "database"
    assert payload["mock_state_persistent"] is False
    assert payload["control_plane_database_backend"] == "sqlite"
    assert payload["control_plane_database_persistent"] is False
    assert payload["control_plane_database_fallback_active"] is True
    assert payload["local_cache"]["storage_backend"] == "database"
    assert payload["local_cache"]["persistent"] is False
    assert fallback_db.exists()


def test_prediction_polling_access_filter_logs_only_first_request_per_job():
    log_filter = PredictionPollingAccessFilter()

    def build_record(path: str) -> logging.LogRecord:
        return logging.LogRecord(
            name="uvicorn.access",
            level=logging.INFO,
            pathname=__file__,
            lineno=1,
            msg='%s - "%s %s HTTP/%s" %s',
            args=("127.0.0.1:12345", "GET", path, "1.1", 200),
            exc_info=None,
        )

    assert log_filter.filter(build_record("/api/v1/predictions/pred_abc123")) is True
    assert log_filter.filter(build_record("/api/v1/predictions/pred_abc123/results?page=1&page_size=500")) is False
    assert log_filter.filter(build_record("/api/v1/predictions/pred_other/results?page=1&page_size=500")) is True
    assert log_filter.filter(build_record("/northstar/v1/predictions/pred_workspace")) is True
    assert log_filter.filter(build_record("/northstar/v1/predictions/pred_workspace/results?page=1&page_size=500")) is False
    assert log_filter.filter(build_record("/api/v1/health")) is True
