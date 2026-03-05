import pathlib

from bigquery_service import BigQueryService
from data_processing_service import DataProcessingService
from gcs_service import GcsService
import ingestion_service as ingestion_service_module


def test_ingestion_service_writes_local_jsonl_shards(monkeypatch, tmp_path):
    class DummyConnector:
        def fetch_events(self, start_date, end_date):
            return [
                {"player_id": f"player-{idx}", "event_name": "session_start", "timestamp": "2026-03-05T00:00:00"}
                for idx in range(5)
            ]

    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("DATA_BACKEND_MODE", "mock")
    monkeypatch.setenv("INGEST_LOCAL_SHARD_EVENT_COUNT", "2")
    monkeypatch.setenv("KAIRYX_LOCAL_DB_PATH", str(tmp_path / "phase2_local.db"))
    monkeypatch.setattr(
        ingestion_service_module,
        "create_connector",
        lambda connector_type, connector_config: DummyConnector(),
    )

    gcs_service = GcsService(bucket_name="test-bucket")
    service = ingestion_service_module.IngestionService(
        gcs_service=gcs_service,
        connector_config={},
        connector_type="dummy",
    )

    staged = service.fetch_and_stage_events("20260301", "20260302")

    assert staged["shards_created"] == 3
    local_files = sorted(
        pathlib.Path(".cache/raw/test-bucket").rglob("*.jsonl")
    )
    assert len(local_files) == 3
    assert local_files[0].read_text().strip().startswith("{")
    assert "[" not in local_files[0].read_text()


def test_processing_pipeline_consumes_local_shards_incrementally(monkeypatch, tmp_path):
    class DummyConnector:
        def fetch_events(self, start_date, end_date):
            return [
                {
                    "source": "dummy",
                    "player_id": "player-1",
                    "event_name": "session_start",
                    "timestamp": "2026-03-05T00:00:00",
                    "source_event_id": "evt-1",
                },
                {
                    "source": "dummy",
                    "player_id": "player-1",
                    "event_name": "session_start",
                    "timestamp": "2026-03-05T00:05:00",
                    "source_event_id": "evt-2",
                },
                {
                    "source": "dummy",
                    "player_id": "player-1",
                    "event_name": "session_start",
                    "timestamp": "2026-03-05T00:05:00",
                    "source_event_id": "evt-2",
                },
                {
                    "source": "dummy",
                    "player_id": "player-1",
                    "event_name": "purchase",
                    "timestamp": "2026-03-05T00:10:00",
                    "source_event_id": "evt-3",
                    "event_properties": {"revenue": "5.00"},
                },
                {
                    "source": "dummy",
                    "player_id": "player-1",
                    "event_name": "session_start",
                    "timestamp": "2026-03-05T00:20:00",
                    "source_event_id": "evt-4",
                },
            ]

    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("DATA_BACKEND_MODE", "mock")
    monkeypatch.setenv("INGEST_LOCAL_SHARD_EVENT_COUNT", "2")
    monkeypatch.setenv("KAIRYX_LOCAL_DB_PATH", str(tmp_path / "phase2_local.db"))
    monkeypatch.setattr(
        ingestion_service_module,
        "create_connector",
        lambda connector_type, connector_config: DummyConnector(),
    )

    gcs_service = GcsService(bucket_name="test-bucket")
    ingestion_service = ingestion_service_module.IngestionService(
        gcs_service=gcs_service,
        connector_config={},
        connector_type="dummy",
    )
    published = ingestion_service.fetch_and_publish_events("20260301", "20260302")

    assert published == 5
    assert len(ingestion_service.message_queue_topic) == 3

    bigquery_service = BigQueryService()
    processing_service = DataProcessingService(
        bigquery_service=bigquery_service,
        gcs_service=gcs_service,
        job_identifier="job-phase2",
    )

    call_sizes = []
    original_normalize = processing_service.normalizer.normalize_events

    def _recording_normalize(events):
        call_sizes.append(len(events))
        return original_normalize(events)

    processing_service.normalizer.normalize_events = _recording_normalize

    stats = processing_service.run_processing_pipeline(ingestion_service)

    assert call_sizes == [2, 2, 1]
    assert stats["raw_normalized_events"] == 5
    assert stats["deduped_events"] == 4
    assert stats["duplicates_removed"] == 1

    latest = bigquery_service.get_player_latest_state("player-1")
    assert latest is not None
    assert latest["lifetime_events"] == 4
