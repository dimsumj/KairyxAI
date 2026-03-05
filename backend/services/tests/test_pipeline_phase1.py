import json
from datetime import datetime, timedelta

from bigquery_service import BigQueryService
from event_semantic_normalizer import EventSemanticNormalizer
from gcs_service import GcsService
import ingestion_service as ingestion_service_module


def test_event_normalizer_adds_phase1_pipeline_fields():
    normalizer = EventSemanticNormalizer(event_name_map={}, property_key_map={})
    raw_event = {
        "source": "amplitude",
        "player_id": "player-1",
        "event_name": "purchase",
        "timestamp": "2026-03-05T12:34:56",
        "event_properties": {
            "revenue": "4.99",
            "currency": "usd",
            "campaign": "spring_sale",
        },
        "user_properties": {},
    }

    first = normalizer.normalize_events([raw_event])[0]
    second = normalizer.normalize_events([raw_event])[0]

    assert first["schema_version"] == "v1"
    assert first["event_date"] == "2026-03-05"
    assert first["event_fingerprint"] == second["event_fingerprint"]
    assert len(first["event_fingerprint"]) == 64


def test_bigquery_service_supports_staging_write_and_latest_state(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("DATA_BACKEND_MODE", "mock")
    monkeypatch.setenv("KAIRYX_LOCAL_DB_PATH", str(tmp_path / "phase1_local.db"))

    service = BigQueryService()
    now = datetime.utcnow()
    events = [
        {
            "player_id": "player-1",
            "canonical_user_id": "canon-1",
            "event_type": "session_start",
            "event_time": (now - timedelta(days=3)).isoformat(),
            "event_properties": {"campaign": "spring_sale", "media_source": "facebook"},
        },
        {
            "player_id": "player-1",
            "canonical_user_id": "canon-1",
            "event_type": "item_purchased",
            "event_time": (now - timedelta(days=2, minutes=10)).isoformat(),
            "event_properties": {"revenue_usd": 9.99},
        },
        {
            "player_id": "player-1",
            "canonical_user_id": "canon-1",
            "event_type": "session_start",
            "event_time": (now - timedelta(hours=2)).isoformat(),
            "event_properties": {},
        },
    ]

    service.write_events_staging(events, job_id="job-phase1")
    state = service.get_player_latest_state("canon-1")

    assert state is not None
    assert state["canonical_user_id"] == "canon-1"
    assert state["lifetime_events"] == 3
    assert state["lifetime_revenue_usd"] == 9.99
    assert state["sessions_30d"] >= 1
    assert state["last_campaign"] == "spring_sale"
    assert state["last_media_source"] == "facebook"


def test_ingestion_service_fetch_and_stage_events(monkeypatch, tmp_path):
    class DummyConnector:
        def fetch_events(self, start_date, end_date):
            return [
                {
                    "player_id": "player-1",
                    "event_name": "session_start",
                    "timestamp": "2026-03-05T00:00:00",
                }
            ]

    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("DATA_BACKEND_MODE", "mock")
    monkeypatch.setenv("KAIRYX_LOCAL_DB_PATH", str(tmp_path / "phase1_local.db"))
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

    assert staged["source"] == "dummy"
    assert staged["shards_created"] == 1
    assert staged["events_staged"] == 1
    assert len(staged["shard_manifests"]) == 1
    assert service.message_queue_topic == []

    published = service.fetch_and_publish_events("20260301", "20260302")
    assert published == 1
    assert len(service.message_queue_topic) == 1

    message = json.loads(service.message_queue_topic[0])
    assert message["source"] == "dummy"
    assert message["schema_version"] == "v1"
