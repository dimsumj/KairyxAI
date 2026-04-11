from __future__ import annotations

import sys
from types import ModuleType, SimpleNamespace

import pytest

from bigquery_service import BigQueryService


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


def test_get_pipeline_dead_letters_returns_empty_when_bigquery_table_is_missing():
    service = BigQueryService.__new__(BigQueryService)
    service.mode = "bigquery"
    service._dead_letter_table_id = "demo.scope.pipeline_dead_letters"
    service._bigquery = SimpleNamespace(
        QueryJobConfig=lambda **kwargs: SimpleNamespace(**kwargs),
        ScalarQueryParameter=lambda *args, **kwargs: SimpleNamespace(args=args, kwargs=kwargs),
    )
    service._client = SimpleNamespace(query=lambda *args, **kwargs: (_ for _ in ()).throw(NotFoundError("table not found")))

    assert service.get_pipeline_dead_letters(limit=25) == []


def test_list_prediction_results_returns_empty_when_bigquery_table_is_missing():
    service = BigQueryService.__new__(BigQueryService)
    service.mode = "bigquery"
    service._prediction_results_table_id = "demo.scope.prediction_results"
    service._bigquery = SimpleNamespace(
        QueryJobConfig=lambda **kwargs: SimpleNamespace(**kwargs),
        ScalarQueryParameter=lambda *args, **kwargs: SimpleNamespace(args=args, kwargs=kwargs),
    )
    service._client = SimpleNamespace(query=lambda *args, **kwargs: (_ for _ in ()).throw(NotFoundError("table not found")))

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
