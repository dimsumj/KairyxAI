from bigquery_service import BigQueryService
from player_modeling_engine import PlayerModelingEngine


def test_bigquery_service_curates_events_and_refreshes_latest_state(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("DATA_BACKEND_MODE", "mock")
    monkeypatch.setenv("KAIRYX_LOCAL_DB_PATH", str(tmp_path / "phase5_local.db"))

    service = BigQueryService()
    staging_rows = [
        {
            "job_id": "job-phase5",
            "job_identifier": "job-phase5",
            "source": "dummy",
            "player_id": "player-1",
            "canonical_user_id": "canon-1",
            "source_event_id": "evt-1",
            "event_fingerprint": "fp-1",
            "event_type": "session_start",
            "event_time": "2026-03-05T00:00:00",
            "event_properties": {"campaign": "spring_sale", "media_source": "facebook"},
            "user_properties": {"email": "player1@example.com"},
        },
        {
            "job_id": "job-phase5",
            "job_identifier": "job-phase5",
            "source": "dummy",
            "player_id": "player-1",
            "canonical_user_id": "canon-1",
            "source_event_id": "evt-1",
            "event_fingerprint": "fp-1",
            "event_type": "session_start",
            "event_time": "2026-03-05T00:00:00",
            "event_properties": {"campaign": "spring_sale", "media_source": "facebook"},
            "user_properties": {"email": "player1@example.com"},
        },
        {
            "job_id": "job-phase5",
            "job_identifier": "job-phase5",
            "source": "dummy",
            "player_id": "player-1",
            "canonical_user_id": "canon-1",
            "source_event_id": "evt-2",
            "event_fingerprint": "fp-2",
            "event_type": "item_purchased",
            "event_time": "2026-03-05T00:20:00",
            "event_properties": {"revenue_usd": 4.99},
            "user_properties": {"email": "player1@example.com"},
        },
        {
            "job_id": "job-phase5",
            "job_identifier": "job-phase5",
            "source": "dummy",
            "player_id": "player-2",
            "canonical_user_id": "canon-2",
            "source_event_id": "evt-3",
            "event_fingerprint": "fp-3",
            "event_type": "session_start",
            "event_time": "2026-03-06T00:00:00",
            "event_properties": {},
            "user_properties": {},
        },
    ]

    service.write_events_staging(staging_rows, job_id="job-phase5")
    curation_stats = service.run_events_curation(job_id="job-phase5")
    aggregate_stats = service.refresh_player_latest_state(job_id="job-phase5")

    assert curation_stats["staging_rows"] == 4
    assert curation_stats["curated_rows"] == 3
    assert curation_stats["duplicates_removed"] == 1
    assert aggregate_stats["players_aggregated"] == 2

    curated_events = service.get_player_events_curated("player-1")
    assert len(curated_events) == 2

    latest = service.get_player_latest_state("player-1")
    assert latest is not None
    assert latest["canonical_user_id"] == "canon-1"
    assert latest["email"] == "player1@example.com"
    assert latest["total_events"] == 2
    assert latest["total_sessions"] == 2
    assert latest["total_revenue"] == 4.99


def test_bigquery_service_keeps_distinct_events_when_source_event_id_is_nan_after_reload(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("DATA_BACKEND_MODE", "mock")
    monkeypatch.setenv("KAIRYX_LOCAL_DB_PATH", str(tmp_path / "phase5_nan_reload.db"))

    service = BigQueryService()
    service.write_events_staging(
        [
            {
                "job_id": "job-phase5-nan",
                "job_identifier": "job-phase5-nan",
                "source": "amplitude",
                "player_id": "player-1",
                "canonical_user_id": "canon-1",
                "source_event_id": float("nan"),
                "event_fingerprint": "fp-1",
                "event_type": "session_start",
                "event_time": "2026-03-05T00:00:00",
                "event_properties": {},
                "user_properties": {},
            },
            {
                "job_id": "job-phase5-nan",
                "job_identifier": "job-phase5-nan",
                "source": "amplitude",
                "player_id": "player-1",
                "canonical_user_id": "canon-1",
                "source_event_id": float("nan"),
                "event_fingerprint": "fp-2",
                "event_type": "item_purchased",
                "event_time": "2026-03-05T00:05:00",
                "event_properties": {"revenue_usd": 2.5},
                "user_properties": {},
            },
            {
                "job_id": "job-phase5-nan",
                "job_identifier": "job-phase5-nan",
                "source": "amplitude",
                "player_id": "player-2",
                "canonical_user_id": "canon-2",
                "source_event_id": float("nan"),
                "event_fingerprint": "fp-3",
                "event_type": "session_start",
                "event_time": "2026-03-05T00:10:00",
                "event_properties": {},
                "user_properties": {},
            },
        ],
        job_id="job-phase5-nan",
    )

    # Simulate the real import path: staging rows are written, reloaded from parquet,
    # then curated in a later step.
    service = BigQueryService()
    curation_stats = service.run_events_curation(job_id="job-phase5-nan")
    aggregate_stats = service.refresh_player_latest_state(job_id="job-phase5-nan")

    assert curation_stats["staging_rows"] == 3
    assert curation_stats["curated_rows"] == 3
    assert curation_stats["duplicates_removed"] == 0
    assert aggregate_stats["players_aggregated"] == 2

    curated_events = service.get_player_events_curated("player-1", job_id="job-phase5-nan")
    assert len(curated_events) == 2


def test_player_modeling_engine_prefers_aggregate_latest_state(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("DATA_BACKEND_MODE", "mock")
    monkeypatch.setenv("KAIRYX_LOCAL_DB_PATH", str(tmp_path / "phase5_modeling.db"))

    service = BigQueryService()
    service.write_events_staging(
        [
            {
                "job_id": "job-phase5-model",
                "job_identifier": "job-phase5-model",
                "source": "dummy",
                "player_id": "player-1",
                "canonical_user_id": "canon-1",
                "source_event_id": "evt-1",
                "event_fingerprint": "fp-1",
                "event_type": "session_start",
                "event_time": "2026-03-05T00:00:00",
                "event_properties": {"campaign": "spring_sale"},
                "user_properties": {"email": "player1@example.com"},
            }
        ],
        job_id="job-phase5-model",
    )
    service.run_events_curation(job_id="job-phase5-model")
    service.refresh_player_latest_state(job_id="job-phase5-model")

    def _should_not_be_called(player_id):
        raise AssertionError("raw event scan should not be needed when player_latest_state exists")

    monkeypatch.setattr(service, "get_events_for_player", _should_not_be_called)
    engine = PlayerModelingEngine(
        gemini_client=None,
        bigquery_service=service,
        churn_inactive_days=14,
    )

    profile = engine.build_player_profile("player-1")

    assert profile is not None
    assert profile["player_id"] == "player-1"
    assert profile["email"] == "player1@example.com"
    assert profile["total_events"] == 1
