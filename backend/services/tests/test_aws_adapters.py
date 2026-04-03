from __future__ import annotations

import io
import json
import sys
from dataclasses import replace
from types import SimpleNamespace

import pytest

from app.core.request_context import RequestContext, request_context
from app.core.settings import Settings, get_settings, validate_runtime_settings
from gcs_service import GcsService
from pubsub_service import PubSubService
from redshift_warehouse import RedshiftWarehouseService
from secret_manager_service import SecretManagerService


@pytest.fixture(autouse=True)
def _clear_secret_caches():
    SecretManagerService._resolve_asm_secret.cache_clear()
    SecretManagerService._resolve_gsm_secret.cache_clear()
    yield
    SecretManagerService._resolve_asm_secret.cache_clear()
    SecretManagerService._resolve_gsm_secret.cache_clear()


def _base_prod_settings(**overrides) -> Settings:
    settings = Settings(
        app_env="prod",
        control_plane_database_url="postgresql+psycopg://user:pass@example.com:5432/kairyx",
        warehouse_backend="redshift",
        object_storage_backend="s3",
        message_backend="eventbridge_sqs",
        secret_backend="aws_secrets_manager",
        service_role="operator-api",
        legacy_header_auth_enabled=False,
        cors_allowed_origins=("https://operator.example.com",),
        worker_shared_token="worker-token",
        oidc_issuer="https://issuer.example.com",
        oidc_audience="kairyx",
        oidc_jwks_url="https://issuer.example.com/jwks.json",
        aws_region="us-west-2",
        redshift_workgroup_name="analytics",
        redshift_database="warehouse",
        redshift_schema="analytics",
        s3_bucket_name="kairyx-raw-prod",
        eventbridge_bus_name="kairyx-bus",
        scheduler_enabled=False,
    )
    return replace(settings, **overrides)


class _FakeSecretsManagerClient:
    def __init__(self, secrets, calls):
        self._secrets = secrets
        self._calls = calls

    def get_secret_value(self, SecretId):
        self._calls.append(SecretId)
        return self._secrets[SecretId]


class _FakeEventBridgeClient:
    def __init__(self):
        self.entries = []

    def put_events(self, Entries):
        self.entries.extend(Entries)
        return {"Entries": [{"EventId": "evt-1"}]}


class _FakeS3Paginator:
    def __init__(self, storage):
        self._storage = storage

    def paginate(self, Bucket, Prefix):
        contents = [
            {"Key": key}
            for (bucket, key), _payload in self._storage.items()
            if bucket == Bucket and key.startswith(Prefix)
        ]
        return [{"Contents": contents}]


class _FakeS3Client:
    def __init__(self):
        self.storage = {}

    def put_object(self, Bucket, Key, Body, ContentType):
        self.storage[(Bucket, Key)] = bytes(Body)

    def get_object(self, Bucket, Key):
        return {"Body": io.BytesIO(self.storage[(Bucket, Key)])}

    def delete_object(self, Bucket, Key):
        self.storage.pop((Bucket, Key), None)

    def get_paginator(self, operation_name):
        assert operation_name == "list_objects_v2"
        return _FakeS3Paginator(self.storage)


class _FakeRedshiftDataClient:
    def __init__(self):
        self.executed = []

    def execute_statement(self, **kwargs):
        statement_id = f"stmt-{len(self.executed) + 1}"
        self.executed.append({"Id": statement_id, **kwargs})
        return {"Id": statement_id}

    def describe_statement(self, Id):
        return {"Status": "FINISHED"}

    def get_statement_result(self, **kwargs):
        return {"ColumnMetadata": [], "Records": []}


def test_get_settings_infers_aws_backends_from_mode(monkeypatch):
    monkeypatch.setenv("DATA_BACKEND_MODE", "aws")
    monkeypatch.setenv("AWS_REGION", "us-west-2")
    monkeypatch.setenv("REDSHIFT_WORKGROUP_NAME", "analytics")
    monkeypatch.setenv("REDSHIFT_DATABASE", "warehouse")
    monkeypatch.setenv("REDSHIFT_SCHEMA", "analytics")
    monkeypatch.setenv("S3_BUCKET_NAME", "kairyx-raw-prod")
    monkeypatch.setenv("EVENTBRIDGE_BUS_NAME", "kairyx-bus")
    monkeypatch.setenv("SQS_IMPORT_QUEUE_URL", "https://sqs.us-west-2.amazonaws.com/123/import")

    settings = get_settings()

    assert settings.data_backend_mode == "aws"
    assert settings.warehouse_backend == "redshift"
    assert settings.object_storage_backend == "s3"
    assert settings.message_backend == "eventbridge_sqs"
    assert settings.secret_backend == "aws_secrets_manager"
    assert settings.aws_region == "us-west-2"
    assert settings.redshift_schema == "analytics"
    assert settings.s3_bucket_name == "kairyx-raw-prod"
    assert settings.eventbridge_bus_name == "kairyx-bus"


def test_validate_runtime_settings_requires_aws_worker_queue_url():
    settings = _base_prod_settings(
        service_role="import-worker",
        sqs_import_queue_url="",
    )

    with pytest.raises(RuntimeError, match="import-worker"):
        validate_runtime_settings(settings)


def test_secret_manager_resolves_asm_refs(monkeypatch):
    calls = []
    fake_boto3 = SimpleNamespace(
        client=lambda service_name, region_name=None: _FakeSecretsManagerClient(
            {"app/secret": {"SecretString": "top-secret"}},
            calls,
        )
    )
    monkeypatch.setenv("AWS_REGION", "us-west-2")
    monkeypatch.setitem(sys.modules, "boto3", fake_boto3)

    service = SecretManagerService()

    assert service.resolve_secret("asm://app/secret") == "top-secret"
    assert calls == ["app/secret"]


def test_secret_manager_resolves_plain_secret_names_in_aws_mode(monkeypatch):
    calls = []
    fake_boto3 = SimpleNamespace(
        client=lambda service_name, region_name=None: _FakeSecretsManagerClient(
            {"tenant/provider": {"SecretString": "resolved-from-aws"}},
            calls,
        )
    )
    monkeypatch.setenv("DATA_BACKEND_MODE", "aws")
    monkeypatch.setitem(sys.modules, "boto3", fake_boto3)

    service = SecretManagerService()

    assert service.resolve_secret("tenant/provider") == "resolved-from-aws"
    assert calls == ["tenant/provider"]


def test_pubsub_service_publishes_to_eventbridge_with_request_context(monkeypatch):
    fake_events = _FakeEventBridgeClient()
    fake_sqs = object()

    def _client(service_name, region_name=None):
        assert region_name == "us-west-2"
        if service_name == "events":
            return fake_events
        if service_name == "sqs":
            return fake_sqs
        raise AssertionError(f"unexpected service: {service_name}")

    monkeypatch.setenv("DATA_BACKEND_MODE", "aws")
    monkeypatch.setenv("AWS_REGION", "us-west-2")
    monkeypatch.setenv("EVENTBRIDGE_BUS_NAME", "kairyx-bus")
    monkeypatch.setitem(sys.modules, "boto3", SimpleNamespace(client=_client))

    context = RequestContext(
        actor_id="user-1",
        actor_role="admin",
        tenant_id="tenant-1",
        project_id="project-1",
        correlation_id="corr-1",
    )
    with request_context(context):
        message_id = PubSubService(topic_name="import.requested").publish({"job_id": "job-1"}, attributes={"job_type": "import"})

    assert message_id == "evt-1"
    assert len(fake_events.entries) == 1
    entry = fake_events.entries[0]
    assert entry["EventBusName"] == "kairyx-bus"
    assert entry["DetailType"] == "import.requested"
    detail = json.loads(entry["Detail"])
    assert detail["payload"] == {"job_id": "job-1"}
    assert detail["attributes"] == {
        "job_type": "import",
        "tenant_id": "tenant-1",
        "project_id": "project-1",
        "correlation_id": "corr-1",
    }


def test_pubsub_service_decodes_eventbridge_sqs_message():
    decoded = PubSubService.decode_queue_message(
        {
            "MessageId": "msg-1",
            "ReceiptHandle": "handle-1",
            "Body": json.dumps(
                {
                    "detail": json.dumps(
                        {
                            "topic_name": "prediction.requested",
                            "payload": {"job_id": "job-99"},
                            "attributes": {"job_type": "prediction"},
                        }
                    )
                }
            ),
        }
    )

    assert decoded == {
        "payload": {"job_id": "job-99"},
        "attributes": {"job_type": "prediction"},
        "topic_name": "prediction.requested",
        "body": {
            "detail": '{"topic_name": "prediction.requested", "payload": {"job_id": "job-99"}, "attributes": {"job_type": "prediction"}}'
        },
        "message_id": "msg-1",
        "receipt_handle": "handle-1",
    }


def test_gcs_service_s3_backend_round_trip_and_delete(monkeypatch):
    fake_s3 = _FakeS3Client()

    def _client(service_name, region_name=None):
        assert service_name == "s3"
        assert region_name == "us-west-2"
        return fake_s3

    monkeypatch.setenv("DATA_BACKEND_MODE", "aws")
    monkeypatch.setenv("AWS_REGION", "us-west-2")
    monkeypatch.setenv("S3_BUCKET_NAME", "kairyx-raw-prod")
    monkeypatch.setenv("BOOTSTRAP_TENANT_ID", "tenant-1")
    monkeypatch.setenv("BOOTSTRAP_PROJECT_ID", "project-1")
    monkeypatch.setitem(sys.modules, "boto3", SimpleNamespace(client=_client))

    service = GcsService()
    uri = service.upload_raw_events(
        [{"event": "install"}, {"event": "session"}],
        "raw_events/job-123/shard-0.ndjson",
    )

    assert uri == "s3://kairyx-raw-prod/tenants/tenant-1/projects/project-1/raw_events/job-123/shard-0.ndjson"
    assert service.download_raw_events(uri) == [{"event": "install"}, {"event": "session"}]

    service.delete_data_for_job("job-123")

    assert fake_s3.storage == {}


def test_redshift_warehouse_creates_schema_and_parameterized_insert(monkeypatch):
    fake_redshift = _FakeRedshiftDataClient()

    def _client(service_name, region_name=None):
        assert service_name == "redshift-data"
        assert region_name == "us-west-2"
        return fake_redshift

    monkeypatch.setenv("AWS_REGION", "us-west-2")
    monkeypatch.setenv("REDSHIFT_WORKGROUP_NAME", "analytics")
    monkeypatch.setenv("REDSHIFT_DATABASE", "warehouse")
    monkeypatch.setenv("REDSHIFT_SCHEMA", "analytics")
    monkeypatch.setitem(sys.modules, "boto3", SimpleNamespace(client=_client))

    service = RedshiftWarehouseService()
    service.append_rows(
        "events_staging",
        [{"job_id": "job-1", "job_identifier": "job-1", "event_type": "install"}],
    )

    assert service.table_id("events_staging") == "analytics.events_staging"
    executed_sql = [entry["Sql"] for entry in fake_redshift.executed]
    assert executed_sql[0] == "CREATE SCHEMA IF NOT EXISTS analytics;"
    assert "CREATE TABLE IF NOT EXISTS analytics.events_staging" in executed_sql[1]
    assert "JSON_PARSE(:payload_0)" in executed_sql[2]
    insert_parameters = {item["name"]: item["value"] for item in fake_redshift.executed[2]["Parameters"]}
    assert insert_parameters["job_id_0"] == "job-1"
    assert json.loads(insert_parameters["payload_0"])["event_type"] == "install"
