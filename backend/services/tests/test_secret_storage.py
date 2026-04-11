from __future__ import annotations

import pytest

from app.application.connectors import ConnectorService
from app.application.provider_connections import ProviderConnectionService
from app.application.secret_refs import (
    materialize_secret_refs,
    redact_secret_values,
    secure_inline_secret_values,
)
from secret_manager_service import SecretManagerService


SERVICE_ACCOUNT_JSON = """{
  "type": "service_account",
  "project_id": "tenant-warehouse",
  "private_key_id": "key-id-1",
  "private_key": "-----BEGIN PRIVATE KEY-----\\nabc123\\n-----END PRIVATE KEY-----\\n",
  "client_email": "warehouse-reader@tenant-warehouse.iam.gserviceaccount.com",
  "client_id": "1234567890",
  "token_uri": "https://oauth2.googleapis.com/token"
}"""


@pytest.fixture(autouse=True)
def clear_secret_manager_caches():
    SecretManagerService._get_control_plane_cipher.cache_clear()
    SecretManagerService._get_invalid_token_error.cache_clear()
    yield
    SecretManagerService._get_control_plane_cipher.cache_clear()
    SecretManagerService._get_invalid_token_error.cache_clear()


class InMemoryConnectorRepository:
    def __init__(self):
        self.records = {}

    def list_connectors(self):
        return list(self.records.values())

    def get_connector(self, name):
        return self.records.get(name)

    def upsert_connector(self, name: str, connector_type: str, config, connector_id: str | None = None):
        record = {
            "name": name,
            "type": connector_type,
            "config": dict(config or {}),
            "connector_id": connector_id or f"connector_{len(self.records) + 1}",
        }
        self.records[name] = record
        return record

    def list_import_jobs(self):
        return []


class InMemoryProviderRepository:
    def __init__(self):
        self.records = {}

    def list_resources(self, resource_type: str):
        assert resource_type == "provider_connection"
        return list(self.records.values())

    def get_resource(self, resource_type: str, resource_id: str):
        assert resource_type == "provider_connection"
        return self.records.get(resource_id)

    def upsert_resource(self, resource_type: str, resource_id: str, *, status: str, name: str, payload):
        assert resource_type == "provider_connection"
        record = {
            "resource_id": resource_id,
            "status": status,
            "name": name,
            "payload": dict(payload or {}),
        }
        self.records[resource_id] = record
        return record

    def delete_resource(self, resource_type: str, resource_id: str):
        assert resource_type == "provider_connection"
        return self.records.pop(resource_id, None) is not None

    def record_action(self, *_args, **_kwargs):
        return None


def test_secure_inline_secret_values_round_trip_and_redaction(monkeypatch):
    monkeypatch.setenv("CONTROL_PLANE_SECRET_KEY", "unit-test-secret-key")
    secured = secure_inline_secret_values(
        {
            "api_key": "top-secret",
            "service_account_info_json": {
                "type": "service_account",
                "client_email": "warehouse-reader@tenant-warehouse.iam.gserviceaccount.com",
                "private_key": "-----BEGIN PRIVATE KEY-----\\nabc123\\n-----END PRIVATE KEY-----\\n",
                "token_uri": "https://oauth2.googleapis.com/token",
            },
        }
    )

    assert "api_key" not in secured
    assert "service_account_info_json" not in secured
    assert "api_key_encrypted" in secured
    assert "service_account_info_json_encrypted" in secured

    materialized = materialize_secret_refs(secured)
    assert materialized["api_key"] == "top-secret"
    assert materialized["service_account_info_json"]["client_email"] == "warehouse-reader@tenant-warehouse.iam.gserviceaccount.com"

    redacted = redact_secret_values(secured)
    assert redacted["api_key"] is None
    assert redacted["api_key_configured"] is True
    assert "api_key_encrypted" not in redacted
    assert redacted["service_account_info_json"] is None
    assert redacted["service_account_info_json_configured"] is True
    assert "service_account_info_json_encrypted" not in redacted


def test_connector_service_encrypts_browser_entered_bigquery_credentials(monkeypatch):
    monkeypatch.setenv("APP_ENV", "prod")
    monkeypatch.setenv("CONTROL_PLANE_SECRET_KEY", "unit-test-secret-key")
    repository = InMemoryConnectorRepository()
    service = ConnectorService(repository)

    created = service.create_connector(
        "Warehouse Scores",
        "bigquery",
        {
            "project_id": "warehouse-project",
            "dataset_id": "growth_inputs",
            "service_account_json": SERVICE_ACCOUNT_JSON,
        },
    )

    saved = repository.get_connector("Warehouse Scores")
    assert saved is not None
    assert "service_account_json" not in saved["config"]
    assert "service_account_json_encrypted" in saved["config"]
    assert materialize_secret_refs(saved["config"])["service_account_json"] == SERVICE_ACCOUNT_JSON
    assert created["config"]["service_account_json"] is None
    assert created["config"]["service_account_json_configured"] is True


def test_connector_service_requires_secure_storage_for_inline_browser_secrets(monkeypatch):
    monkeypatch.setenv("APP_ENV", "prod")
    monkeypatch.delenv("CONTROL_PLANE_SECRET_KEY", raising=False)
    repository = InMemoryConnectorRepository()
    service = ConnectorService(repository)

    with pytest.raises(ValueError, match="Secure connector secret storage is not configured"):
        service.create_connector(
            "Gemini Primary",
            "google",
            {
                "api_key": "secret-value",
            },
        )


def test_provider_connection_service_encrypts_inline_api_keys(monkeypatch):
    monkeypatch.setenv("APP_ENV", "prod")
    monkeypatch.setenv("CONTROL_PLANE_SECRET_KEY", "unit-test-secret-key")
    repository = InMemoryProviderRepository()
    service = ProviderConnectionService(repository)

    created = service.create_connection(
        "Braze Primary",
        "braze",
        {
            "api_key": "braze-secret",
            "rest_endpoint": "https://rest.iad-01.braze.com",
        },
    )

    saved = repository.get_resource("provider_connection", created["provider_connection_id"])
    assert saved is not None
    saved_config = (saved.get("payload") or {}).get("config") or {}
    assert "api_key" not in saved_config
    assert "api_key_encrypted" in saved_config
    assert materialize_secret_refs(saved_config)["api_key"] == "braze-secret"
    assert created["config"]["api_key"] is None
    assert created["config"]["api_key_configured"] is True
