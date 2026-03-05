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
