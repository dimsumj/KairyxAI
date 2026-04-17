from __future__ import annotations

import sys
import threading
from types import ModuleType, SimpleNamespace

import pytest

from bigquery_service import BigQueryService, _sanitize_storage_field_name


class NotFoundError(Exception):
    pass


class UnexpectedQueryError(Exception):
    pass


def _install_fake_bigquery_module(monkeypatch, client_factory):
    google_module = ModuleType("google")
    cloud_module = ModuleType("google.cloud")
    bigquery_module = ModuleType("google.cloud.bigquery")
    bigquery_module.Client = client_factory
    bigquery_module.Dataset = lambda reference: SimpleNamespace(reference=reference, location=None)
    bigquery_module.QueryJobConfig = lambda **kwargs: SimpleNamespace(**kwargs)
    bigquery_module.ScalarQueryParameter = lambda *args, **kwargs: SimpleNamespace(args=args, kwargs=kwargs)
    cloud_module.bigquery = bigquery_module
    google_module.cloud = cloud_module

    monkeypatch.setitem(sys.modules, "google", google_module)
    monkeypatch.setitem(sys.modules, "google.cloud", cloud_module)
    monkeypatch.setitem(sys.modules, "google.cloud.bigquery", bigquery_module)


def test_gcp_bigquery_service_creates_scoped_dataset_when_missing(monkeypatch):
    class FakeClient:
        last_instance = None

        def __init__(self, project):
            self.project = project
            self.created_datasets = []
            FakeClient.last_instance = self

        def get_dataset(self, dataset_ref):
            raise NotFoundError(f"{dataset_ref} not found")

        def create_dataset(self, dataset, exists_ok=False):
            self.created_datasets.append((dataset.reference, dataset.location, exists_ok))
            return dataset

    _install_fake_bigquery_module(monkeypatch, FakeClient)
    monkeypatch.setenv("DATA_BACKEND_MODE", "gcp")
    monkeypatch.setenv("BIGQUERY_PROJECT_ID", "demo-project")
    monkeypatch.setenv("BIGQUERY_DATASET_ID", "kairyx_platform")
    monkeypatch.setenv("GCP_REGION", "us-central1")
    monkeypatch.setenv("BOOTSTRAP_TENANT_ID", "default")
    monkeypatch.setenv("BOOTSTRAP_PROJECT_ID", "default")

    service = BigQueryService()

    assert service.mode == "bigquery"
    assert FakeClient.last_instance is not None
    assert FakeClient.last_instance.created_datasets == [
        ("demo-project.kairyx_platform_default_default", "us-central1", True)
    ]


def test_get_pipeline_dead_letters_skips_query_when_bigquery_table_is_missing():
    service = BigQueryService.__new__(BigQueryService)
    service.mode = "bigquery"
    service._dead_letter_table_id = "demo.scope.pipeline_dead_letters"
    service._bigquery = SimpleNamespace(
        QueryJobConfig=lambda **kwargs: SimpleNamespace(**kwargs),
        ScalarQueryParameter=lambda *args, **kwargs: SimpleNamespace(args=args, kwargs=kwargs),
    )
    service._client = SimpleNamespace(
        get_table=lambda *args, **kwargs: (_ for _ in ()).throw(NotFoundError("table not found")),
        query=lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("Missing table should not be queried")),
    )

    assert service.get_pipeline_dead_letters(limit=25) == []


def test_list_prediction_results_returns_empty_without_query_when_bigquery_table_is_missing():
    service = BigQueryService.__new__(BigQueryService)
    service.mode = "bigquery"
    service._prediction_results_table_id = "demo.scope.prediction_results"
    service._bigquery = SimpleNamespace(
        QueryJobConfig=lambda **kwargs: SimpleNamespace(**kwargs),
        ScalarQueryParameter=lambda *args, **kwargs: SimpleNamespace(args=args, kwargs=kwargs),
    )
    service._client = SimpleNamespace(
        get_table=lambda *args, **kwargs: (_ for _ in ()).throw(NotFoundError("table not found")),
        query=lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("Missing table should not be queried")),
    )

    payload = service.list_prediction_results("job-1", page=1, page_size=50)

    assert payload == {"page": 1, "page_size": 50, "total": 0, "items": []}


def test_missing_table_guards_do_not_swallow_unexpected_bigquery_errors():
    service = BigQueryService.__new__(BigQueryService)
    service.mode = "bigquery"
    service._dead_letter_table_id = "demo.scope.pipeline_dead_letters"
    service._prediction_results_table_id = "demo.scope.prediction_results"
    service._bigquery = SimpleNamespace(
        QueryJobConfig=lambda **kwargs: SimpleNamespace(**kwargs),
        ScalarQueryParameter=lambda *args, **kwargs: SimpleNamespace(args=args, kwargs=kwargs),
    )
    service._client = SimpleNamespace(query=lambda *args, **kwargs: (_ for _ in ()).throw(UnexpectedQueryError("permission denied")))

    with pytest.raises(UnexpectedQueryError):
        service.get_pipeline_dead_letters(limit=10)

    with pytest.raises(UnexpectedQueryError):
        service.list_prediction_results("job-1")


def test_load_all_rows_from_target_skips_query_when_bigquery_table_is_missing():
    service = BigQueryService.__new__(BigQueryService)
    service.mode = "bigquery"
    service._player_latest_state_table_id = "demo.scope.player_latest_state"

    def _missing_table(table_id):
        raise NotFoundError(f"{table_id} not found")

    def _unexpected_query(*args, **kwargs):
        raise AssertionError("Missing tables should not be queried.")

    service._client = SimpleNamespace(get_table=_missing_table, query=_unexpected_query)

    assert service._load_all_rows_from_target("player_latest_state") == []


def test_get_all_player_ids_skips_missing_player_latest_state_table():
    queried_sql = []
    existing_tables = {
        "demo.scope.events_curated": object(),
        "demo.scope.events_staging": object(),
    }

    service = BigQueryService.__new__(BigQueryService)
    service.mode = "bigquery"
    service._player_latest_state_table_id = "demo.scope.player_latest_state"
    service._curated_table_id = "demo.scope.events_curated"
    service._table_id = "demo.scope.events_staging"

    def _get_table(table_id):
        if table_id in existing_tables:
            return existing_tables[table_id]
        raise NotFoundError(f"{table_id} not found")

    def _query(sql, job_config=None):
        queried_sql.append(sql)
        if "events_curated" in sql:
            return SimpleNamespace(result=lambda: [{"player_id": "player-1"}])
        if "events_staging" in sql:
            return SimpleNamespace(result=lambda: [])
        raise AssertionError("player_latest_state should be skipped when the table is missing.")

    service._client = SimpleNamespace(get_table=_get_table, query=_query)

    assert service.get_all_player_ids() == ["player-1"]
    assert all("player_latest_state" not in sql for sql in queried_sql)


def test_delete_data_for_job_skips_missing_bigquery_tables():
    queried_sql = []
    existing_tables = {
        "demo.scope.pipeline_dead_letters": object(),
    }

    service = BigQueryService.__new__(BigQueryService)
    service.mode = "bigquery"
    service._table_id = "demo.scope.processed_events"
    service._dead_letter_table_id = "demo.scope.pipeline_dead_letters"
    service._bigquery = SimpleNamespace(
        QueryJobConfig=lambda **kwargs: SimpleNamespace(**kwargs),
        ScalarQueryParameter=lambda *args, **kwargs: SimpleNamespace(args=args, kwargs=kwargs),
    )

    def _get_table(table_id):
        if table_id in existing_tables:
            return existing_tables[table_id]
        raise NotFoundError(f"{table_id} not found")

    def _query(sql, job_config=None):
        queried_sql.append(sql)
        return SimpleNamespace(result=lambda: None)

    service._client = SimpleNamespace(get_table=_get_table, query=_query)
    service.run_events_curation = lambda *args, **kwargs: {"curated_rows": 0}
    service.refresh_player_latest_state = lambda *args, **kwargs: {"players_aggregated": 0}

    service.delete_data_for_job("job-1")

    assert len(queried_sql) == 1
    assert "pipeline_dead_letters" in queried_sql[0]
    assert "processed_events" not in queried_sql[0]


def test_write_events_staging_sanitizes_invalid_bigquery_field_names_before_load():
    captured: dict[str, object] = {}

    class FakeLoadJob:
        def result(self):
            return None

    class FakeClient:
        def load_table_from_json(self, rows, table_id, job_config=None):
            captured["rows"] = rows
            captured["table_id"] = table_id
            captured["job_config"] = job_config
            return FakeLoadJob()

    service = BigQueryService.__new__(BigQueryService)
    service.mode = "bigquery"
    service._lock = threading.Lock()
    service._table_id = "demo.scope.events_staging"
    service._bigquery = SimpleNamespace(
        LoadJobConfig=lambda **kwargs: SimpleNamespace(**kwargs),
        SourceFormat=SimpleNamespace(NEWLINE_DELIMITED_JSON="NEWLINE_DELIMITED_JSON"),
        WriteDisposition=SimpleNamespace(WRITE_APPEND="WRITE_APPEND"),
        CreateDisposition=SimpleNamespace(CREATE_IF_NEEDED="CREATE_IF_NEEDED"),
    )
    service._client = FakeClient()

    service.write_events_staging(
        [
            {
                "$schema": "https://example.com/schema.json",
                "1party": "publisher",
                "event_type": "install",
                "event_properties": {
                    "campaign-name": "launch",
                },
            }
        ],
        job_id="job-1",
    )

    assert captured["table_id"] == "demo.scope.events_staging"
    rows = captured["rows"]
    assert isinstance(rows, list)
    assert len(rows) == 1
    row = rows[0]
    assert row["job_id"] == "job-1"
    assert row["job_identifier"] == "job-1"
    assert row[_sanitize_storage_field_name("$schema")] == "https://example.com/schema.json"
    assert row[_sanitize_storage_field_name("1party")] == "publisher"
    assert "$schema" not in row
    assert "1party" not in row
    assert row["event_properties"][_sanitize_storage_field_name("campaign-name")] == "launch"
