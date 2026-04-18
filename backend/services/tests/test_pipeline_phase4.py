import sqlite3
from types import SimpleNamespace

from app.core.request_context import RequestContext, request_context
from dataflow.pipeline import DataflowNormalizationRunner
from bigquery_service import BigQueryService
from gcs_service import GcsService


def test_dataflow_runner_writes_staging_and_dead_letters(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("DATA_BACKEND_MODE", "mock")
    monkeypatch.setenv("KAIRYX_LOCAL_DB_PATH", str(tmp_path / "phase4_local.db"))

    gcs_service = GcsService(bucket_name="test-bucket")
    bigquery_service = BigQueryService()
    runner = DataflowNormalizationRunner(
        gcs_service=gcs_service,
        bigquery_service=bigquery_service,
    )

    raw_events = [
        {
            "source": "dummy",
            "player_id": "player-1",
            "event_name": "session_start",
            "timestamp": "2026-03-05T00:00:00",
        },
        {
            "source": "dummy",
            "event_name": "session_start",
            "timestamp": "2026-03-05T00:05:00",
        },
        {
            "source": "dummy",
            "player_id": "player-2",
            "event_name": "session_start",
            "timestamp": "not-a-timestamp",
        },
    ]
    gcs_uri = gcs_service.upload_raw_events(raw_events, "raw_events/dummy/job-phase4/part-00001.jsonl")
    notification = {
        "job_id": "job-phase4",
        "source": "dummy",
        "gcs_path": gcs_uri,
        "event_count": len(raw_events),
        "shard_index": 1,
        "source_config_id": "Dummy Source",
        "schema_version": "v1",
    }

    stats = runner.process_notifications([notification])

    assert stats["manifests_processed"] == 1
    assert stats["raw_normalized_events"] == 3
    assert stats["events_staging_written"] == 1
    assert stats["pipeline_dead_letters_written"] == 2
    assert stats["flag_counts"]["missing_player_id"] == 1
    assert stats["flag_counts"]["invalid_event_time"] == 1

    latest = bigquery_service.get_player_latest_state("player-1")
    assert latest is not None
    assert latest["lifetime_events"] == 1

    dead_letters = bigquery_service.get_pipeline_dead_letters(job_id="job-phase4")
    assert len(dead_letters) == 2
    assert all(row["raw_gcs_uri"] == gcs_uri for row in dead_letters)


def test_dataflow_runner_falls_back_when_local_identity_store_is_unavailable(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("DATA_BACKEND_MODE", "mock")
    monkeypatch.setenv("KAIRYX_LOCAL_DB_PATH", str(tmp_path / "phase4_local.db"))
    monkeypatch.setattr(
        "dataflow.pipeline.resolve_or_create_canonical_user_id",
        lambda source, player_id: (_ for _ in ()).throw(sqlite3.OperationalError("unable to open database file")),
    )

    gcs_service = GcsService(bucket_name="test-bucket")
    bigquery_service = BigQueryService()
    runner = DataflowNormalizationRunner(
        gcs_service=gcs_service,
        bigquery_service=bigquery_service,
    )

    raw_events = [
        {
            "source": "dummy",
            "player_id": "player-1",
            "event_name": "session_start",
            "timestamp": "2026-03-05T00:00:00",
        },
        {
            "source": "dummy",
            "player_id": "player-1",
            "event_name": "purchase",
            "timestamp": "2026-03-05T00:05:00",
        },
    ]
    gcs_uri = gcs_service.upload_raw_events(raw_events, "raw_events/dummy/job-phase4-fallback/part-00001.jsonl")

    stats = runner.process_notifications(
        [
            {
                "job_id": "job-phase4-fallback",
                "source": "dummy",
                "gcs_path": gcs_uri,
                "event_count": len(raw_events),
                "shard_index": 1,
                "source_config_id": "Dummy Source",
                "schema_version": "v1",
            }
        ]
    )

    assert stats["manifests_processed"] == 1
    latest = bigquery_service.get_player_latest_state("player-1")
    assert latest is not None
    assert latest["canonical_user_id"] == "uid:player-1"


def test_dataflow_runner_continues_after_schema_load_failure_for_one_manifest(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("DATA_BACKEND_MODE", "mock")
    monkeypatch.setenv("KAIRYX_LOCAL_DB_PATH", str(tmp_path / "phase4_local.db"))

    class FlakyBigQueryService:
        def __init__(self):
            self._write_attempts = 0

        def write_events_staging(self, rows, job_id=None):
            self._write_attempts += 1
            if self._write_attempts == 1:
                raise RuntimeError(
                    "400 Provided Schema does not match Table demo.scope.processed_events. "
                    "Field event_properties.bingo_dropped_numbers has changed type from FLOAT to STRING"
                )

        def write_pipeline_dead_letters(self, rows, job_id=None):
            return None

        def run_events_curation(self):
            return {"curated_rows": 1}

        def refresh_player_latest_state(self):
            return {"profiles": 1}

    gcs_service = GcsService(bucket_name="test-bucket")
    runner = DataflowNormalizationRunner(
        gcs_service=gcs_service,
        bigquery_service=FlakyBigQueryService(),
    )

    first_uri = gcs_service.upload_raw_events(
        [
            {
                "source": "dummy",
                "player_id": "player-1",
                "event_name": "spin_completed",
                "timestamp": "2026-03-05T00:00:00",
                "event_properties": {"bingo_dropped_numbers": "07,11,19"},
            }
        ],
        "raw_events/dummy/job-phase4-partial/part-00001.jsonl",
    )
    second_uri = gcs_service.upload_raw_events(
        [
            {
                "source": "dummy",
                "player_id": "player-2",
                "event_name": "session_start",
                "timestamp": "2026-03-05T00:05:00",
            }
        ],
        "raw_events/dummy/job-phase4-partial/part-00002.jsonl",
    )

    stats = runner.process_notifications(
        [
            {
                "job_id": "job-phase4-partial",
                "source": "dummy",
                "gcs_path": first_uri,
                "event_count": 1,
                "shard_index": 1,
                "source_config_id": "Dummy Source",
                "schema_version": "v1",
            },
            {
                "job_id": "job-phase4-partial",
                "source": "dummy",
                "gcs_path": second_uri,
                "event_count": 1,
                "shard_index": 2,
                "source_config_id": "Dummy Source",
                "schema_version": "v1",
            },
        ]
    )

    assert stats["manifests_processed"] == 1
    assert stats["manifests_failed"] == 1
    assert len(stats["processed_notifications"]) == 1
    assert stats["processed_notifications"][0]["gcs_path"] == second_uri
    assert len(stats["failed_notifications"]) == 1
    assert stats["failed_notifications"][0]["notification"]["gcs_path"] == first_uri
    assert "Provided Schema does not match Table" in stats["failed_notifications"][0]["error"]
    assert stats["warehouse_stats"] == {
        "curation": {"curated_rows": 1},
        "player_latest_state": {"profiles": 1},
    }


def test_gcs_service_download_resolves_explicit_blob_scope_across_contexts(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("DATA_BACKEND_MODE", "mock")
    monkeypatch.setenv("KAIRYX_RUNTIME_DIR", str(tmp_path / "runtime"))

    gcs_service = GcsService(bucket_name="test-bucket")
    writer_scope = RequestContext(
        actor_id="writer",
        actor_role="admin",
        tenant_id="default",
        project_id="default",
        correlation_id="ctx-write",
    )
    reader_scope = RequestContext(
        actor_id="reader",
        actor_role="admin",
        tenant_id="torpedo",
        project_id="default",
        correlation_id="ctx-read",
    )
    raw_events = [
        {
            "source": "dummy",
            "player_id": "player-1",
            "event_name": "session_start",
            "timestamp": "2026-03-05T00:00:00",
        }
    ]

    with request_context(writer_scope):
        gcs_uri = gcs_service.upload_raw_events(raw_events, "raw/source=dummy/job=job-cross-scope/part-00001.jsonl")

    blob_name = gcs_uri.replace("gs://test-bucket/", "")
    with request_context(reader_scope):
        loaded_events = gcs_service.download_raw_events(blob_name)

    assert loaded_events == raw_events


def test_gcs_service_mock_bucket_path_tracks_request_scope_dynamically(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("DATA_BACKEND_MODE", "mock")
    monkeypatch.setenv("KAIRYX_RUNTIME_DIR", str(tmp_path / "runtime"))

    ctor_scope = RequestContext(
        actor_id="ctor",
        actor_role="admin",
        tenant_id="torpedo",
        project_id="default",
        correlation_id="ctx-ctor",
    )
    write_scope = RequestContext(
        actor_id="writer",
        actor_role="admin",
        tenant_id="northstar",
        project_id="alpha",
        correlation_id="ctx-write",
    )
    raw_events = [
        {
            "source": "dummy",
            "player_id": "player-1",
            "event_name": "session_start",
            "timestamp": "2026-03-05T00:00:00",
        }
    ]

    with request_context(ctor_scope):
        gcs_service = GcsService(bucket_name="test-bucket")

    with request_context(write_scope):
        gcs_uri = gcs_service.upload_raw_events(raw_events, "raw/source=dummy/job=job-dynamic-scope/part-00001.jsonl")

    blob_name = gcs_uri.replace("gs://test-bucket/", "")
    expected_path = (
        tmp_path
        / "runtime"
        / ".cache"
        / "raw"
        / "northstar"
        / "alpha"
        / "test-bucket"
        / blob_name
    )
    assert expected_path.exists()
