from gcs_service import GcsService
from ingestion_service import IngestionService
import ingestion_service as ingestion_service_module
import local_job_store


def test_ingestion_service_publishes_via_mock_pubsub_and_persists_checkpoints(monkeypatch, tmp_path):
    class DummyConnector:
        def fetch_events(self, start_date, end_date):
            return [
                {"player_id": f"player-{idx}", "event_name": "session_start", "timestamp": "2026-03-05T00:00:00"}
                for idx in range(4)
            ]

    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("DATA_BACKEND_MODE", "mock")
    monkeypatch.setenv("INGEST_LOCAL_SHARD_EVENT_COUNT", "2")
    monkeypatch.setenv("KAIRYX_LOCAL_DB_PATH", str(tmp_path / "phase3_local.db"))
    monkeypatch.setattr(
        ingestion_service_module,
        "create_connector",
        lambda connector_type, connector_config: DummyConnector(),
    )
    local_job_store.init_db()

    gcs_service = GcsService(bucket_name="test-bucket")
    ingestion_service = IngestionService(
        gcs_service=gcs_service,
        connector_config={},
        connector_type="dummy",
        source_config_id="Dummy Source",
    )

    published = ingestion_service.fetch_and_publish_events(
        "20260301",
        "20260302",
        job_id="import-job-1",
    )

    assert published == 4
    assert len(ingestion_service.pubsub_service.published_messages) == 2
    assert len(ingestion_service.published_message_ids) == 2
    first_published = ingestion_service.pubsub_service.published_messages[0]
    assert first_published["payload"]["job_id"] == "import-job-1"
    assert first_published["attributes"]["shard_index"] == "1"

    checkpoints = local_job_store.list_ingestion_checkpoints("import-job-1")
    assert len(checkpoints) == 2
    assert checkpoints[0]["source_config_id"] == "Dummy Source"
    assert checkpoints[0]["publish_status"] == "published"
    assert checkpoints[0]["published_message_id"].startswith("mock-")

    local_job_store.delete_ingestion_checkpoints("import-job-1")
    assert local_job_store.list_ingestion_checkpoints("import-job-1") == []
