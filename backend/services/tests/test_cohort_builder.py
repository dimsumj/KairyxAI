from __future__ import annotations

from dataclasses import replace

from fastapi.testclient import TestClient
import pytest

from app.core import db as db_module
from app.core.db import session_scope
from app.core.deps import get_settings_dependency
from app.core.settings import get_settings
from app.main import create_app
from app.infrastructure.repositories.sqlalchemy_control_plane import SqlAlchemyControlPlaneRepository
from bigquery_service import clear_shared_bigquery_service_cache, get_shared_bigquery_service


@pytest.fixture
def client(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("DATA_BACKEND_MODE", "mock")
    monkeypatch.setenv("CONTROL_PLANE_DATABASE_URL", f"sqlite:///{tmp_path / 'control_plane.db'}")
    monkeypatch.setenv("KAIRYX_LOCAL_DB_PATH", str(tmp_path / 'local_jobs.db'))
    monkeypatch.setenv("SCHEDULER_ENABLED", "false")
    db_module.get_engine.cache_clear()
    db_module.get_session_factory.cache_clear()
    clear_shared_bigquery_service_cache()
    app = create_app()
    with TestClient(app) as test_client:
        yield test_client


def _seed_prediction_job(
    job_id: str,
    *,
    source_name: str,
    history_snapshot_at: str,
    prediction_mode: str = "local",
) -> None:
    with session_scope() as session:
        repository = SqlAlchemyControlPlaneRepository(session)
        repository.create_prediction_job(
            {
                "id": job_id,
                "import_job_id": f"imp_{job_id}",
                "status": "completed",
                "spec": {
                    "import_job_id": f"imp_{job_id}",
                    "audience_scope": "source",
                    "source_name": source_name,
                    "prediction_mode": prediction_mode,
                },
                "progress": {
                    "current": 2,
                    "total": 2,
                    "pct": 100.0,
                    "details": {
                        "source_name": source_name,
                        "audience_scope": "source",
                        "prediction_mode": prediction_mode,
                        "audience_label": source_name,
                        "history_snapshot_at": history_snapshot_at,
                    },
                },
            }
        )
        session.commit()


def _append_prediction_rows(job_id: str, rows: list[dict]) -> None:
    service = get_shared_bigquery_service()
    service.append_prediction_results(job_id, rows)


def _seed_builder_prediction_data() -> None:
    _seed_prediction_job(
        "pred_source_a_old",
        source_name="Amplitude 1",
        history_snapshot_at="2026-04-01T00:00:00+00:00",
    )
    _append_prediction_rows(
        "pred_source_a_old",
        [
            {
                "prediction_job_id": "pred_source_a_old",
                "canonical_user_id": "u_old",
                "user_id": "u_old",
                "email": "old@example.com",
                "predicted_churn_risk": "high",
                "churn_state": "active",
                "prediction_source": "local",
                "suggested_action": "email",
                "days_since_last_seen": 12,
                "session_count": 1,
                "event_count": 4,
                "completed_at": "2026-04-01T00:00:00+00:00",
            }
        ],
    )

    _seed_prediction_job(
        "pred_source_a_new",
        source_name="Amplitude 1",
        history_snapshot_at="2026-04-10T00:00:00+00:00",
    )
    _append_prediction_rows(
        "pred_source_a_new",
        [
            {
                "prediction_job_id": "pred_source_a_new",
                "canonical_user_id": "u_shared",
                "user_id": "u_shared",
                "email": "shared@example.com",
                "predicted_churn_risk": "high",
                "churn_state": "active",
                "prediction_source": "local",
                "suggested_action": "email",
                "days_since_last_seen": 14,
                "session_count": 2,
                "event_count": 5,
                "completed_at": "2026-04-10T00:00:00+00:00",
            },
            {
                "prediction_job_id": "pred_source_a_new",
                "canonical_user_id": "u_a_only",
                "user_id": "u_a_only",
                "email": "aonly@example.com",
                "predicted_churn_risk": "high",
                "churn_state": "active",
                "prediction_source": "local",
                "suggested_action": "push_notification",
                "days_since_last_seen": 21,
                "session_count": 3,
                "event_count": 7,
                "completed_at": "2026-04-10T00:00:00+00:00",
            },
        ],
    )

    _seed_prediction_job(
        "pred_source_b",
        source_name="Adjust Source",
        history_snapshot_at="2026-04-11T00:00:00+00:00",
        prediction_mode="cloud",
    )
    _append_prediction_rows(
        "pred_source_b",
        [
            {
                "prediction_job_id": "pred_source_b",
                "canonical_user_id": "u_shared",
                "user_id": "u_shared",
                "email": "shared@example.com",
                "predicted_churn_risk": "medium",
                "churn_state": "active",
                "prediction_source": "cloud",
                "suggested_action": "email",
                "days_since_last_seen": 9,
                "session_count": 8,
                "event_count": 11,
                "completed_at": "2026-04-11T00:00:00+00:00",
            },
            {
                "prediction_job_id": "pred_source_b",
                "canonical_user_id": "u_b_only",
                "user_id": "u_b_only",
                "email": "bonly@example.com",
                "predicted_churn_risk": "high",
                "churn_state": "active",
                "prediction_source": "cloud",
                "suggested_action": "push_notification",
                "days_since_last_seen": 30,
                "session_count": 1,
                "event_count": 2,
                "completed_at": "2026-04-11T00:00:00+00:00",
            },
        ],
    )


def _create_bigquery_connector(client: TestClient, *, name: str, mock_tables: dict) -> dict:
    response = client.post(
        "/api/v1/connectors",
        json={
            "name": name,
            "type": "bigquery",
            "config": {
                "project_id": "tenant-warehouse",
                "dataset_id": "growth_inputs",
                "mock_tables": mock_tables,
            },
        },
    )
    assert response.status_code == 201, response.text
    return response.json()


def _replace_saved_query_sql(query_id: str, sql: str) -> None:
    with session_scope() as session:
        repository = SqlAlchemyControlPlaneRepository(session)
        record = repository.get_resource("saved_query", query_id)
        payload = dict((record or {}).get("payload") or {})
        payload["sql"] = sql
        repository.upsert_resource(
            "saved_query",
            query_id,
            status="active",
            name=payload.get("name"),
            payload=payload,
        )
        session.commit()


def test_cohort_builder_options_expose_prediction_sources_and_fields(client):
    _seed_builder_prediction_data()
    _create_bigquery_connector(
        client,
        name="Warehouse Scores",
        mock_tables={"retention_scores": [{"user_id": "u_1", "email": "u1@example.com"}]},
    )

    response = client.get("/api/v1/cohorts/builder/options")

    assert response.status_code == 200
    payload = response.json()
    assert payload["defaults"]["audience_basis"] == "prediction"
    assert payload["defaults"]["prediction_scope"] == "source"
    audience_bases = {item["id"] for item in payload["audience_bases"]}
    assert {"managed_warehouse_sql", "connector_bigquery_table"} <= audience_bases
    source_names = {item["source_name"] for item in payload["prediction_sources"]}
    assert source_names == {"Amplitude 1", "Adjust Source"}
    field_names = {item["field"] for item in payload["filter_fields"]}
    assert {"predicted_churn_risk", "days_since_last_seen", "source_name"} <= field_names
    connector_names = {item["name"] for item in payload["warehouse_connectors"]}
    assert connector_names == {"Warehouse Scores"}


def test_cohort_builder_preview_uses_latest_source_job_and_dedupes_users(client):
    _seed_builder_prediction_data()

    response = client.post(
        "/api/v1/cohorts/builder/preview",
        json={
            "name": "winback_priority",
            "audience_basis": "prediction",
            "prediction_scope": "source",
            "source_names": ["Amplitude 1", "Adjust Source"],
            "output_mode": "combined",
            "conditions": [
                {
                    "field": "predicted_churn_risk",
                    "op": "in",
                    "values": ["high", "medium"],
                }
            ],
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["member_count"] == 3
    resolved_ids = {item["prediction_job_id"] for item in payload["resolved_predictions"]}
    assert resolved_ids == {"pred_source_a_new", "pred_source_b"}
    preview_ids = {item["canonical_user_id"] for item in payload["preview_members"]}
    assert preview_ids == {"u_shared", "u_a_only", "u_b_only"}
    breakdown = {item["prediction_job_id"]: item["member_count"] for item in payload["source_breakdown"]}
    assert breakdown["pred_source_a_new"] == 2
    assert breakdown["pred_source_b"] == 2


def test_cohort_builder_create_separate_creates_one_draft_per_source(client):
    _seed_builder_prediction_data()

    response = client.post(
        "/api/v1/cohorts/builder/create",
        json={
            "name": "retention_push",
            "audience_basis": "prediction",
            "prediction_scope": "source",
            "source_names": ["Amplitude 1", "Adjust Source"],
            "output_mode": "separate",
            "conditions": [
                {
                    "field": "predicted_churn_risk",
                    "op": "=",
                    "value": "high",
                }
            ],
            "tags": ["guided-builder"],
        },
    )

    assert response.status_code == 201
    payload = response.json()
    assert payload["created_count"] == 2
    names = {item["name"] for item in payload["items"]}
    assert "retention_push__amplitude_1" in names
    assert "retention_push__adjust_source" in names
    for item in payload["items"]:
        assert item["status"] == "draft"
        assert item["definition"]["entrypoint"] == "guided_builder"
        assert len(item["definition"]["prediction_job_ids"]) == 1


def test_cohort_builder_preview_and_create_managed_warehouse_sql(client):
    response = client.post(
        "/api/v1/cohorts/builder/preview",
        json={
            "name": "warehouse_reward_users",
            "audience_basis": "managed_warehouse_sql",
            "sql": """
                SELECT 'u_1001' AS canonical_user_id, 'u1001@example.com' AS email, 'vip' AS tier
                UNION ALL
                SELECT 'u_1002' AS canonical_user_id, 'u1002@example.com' AS email, 'core' AS tier
            """,
        },
    )

    assert response.status_code == 200, response.text
    preview = response.json()
    assert preview["member_count"] == 2
    assert preview["preview_members"][0]["canonical_user_id"] == "u_1001"
    assert preview["request"]["audience_basis"] == "managed_warehouse_sql"

    create_response = client.post(
        "/api/v1/cohorts/builder/create",
        json=preview["request"],
    )

    assert create_response.status_code == 201, create_response.text
    created = create_response.json()["items"][0]
    assert created["member_count"] == 2
    assert created["definition"]["source_kind"] == "managed_warehouse_sql"
    assert created["definition"]["sql"].strip().lower().startswith("select")
    assert created["source_label"] == "Managed Warehouse"


def test_saved_query_to_cohort_freezes_sql_for_refresh(client):
    created_query = client.post(
        "/api/v1/sql-workspace/queries",
        json={
            "name": "Winback Query",
            "description": "Original reverse ETL audience.",
            "sql": "SELECT 'u_1' AS canonical_user_id, 'u1@example.com' AS email",
        },
    )
    assert created_query.status_code == 201, created_query.text
    query_id = created_query.json()["query_id"]

    created_cohort = client.post(
        f"/api/v1/sql-workspace/queries/{query_id}/cohort",
        json={
            "name": "warehouse_saved_query_cohort",
            "refresh_mode": "manual",
            "owner": "frontend_operator",
            "activate": False,
        },
    )

    assert created_cohort.status_code == 201, created_cohort.text
    cohort = created_cohort.json()
    assert cohort["member_count"] == 1
    assert cohort["definition"]["saved_query_id"] == query_id
    assert cohort["definition"]["source_kind"] == "managed_warehouse_sql"

    _replace_saved_query_sql(query_id, "SELECT 'u_2' AS canonical_user_id, 'u2@example.com' AS email")

    refreshed = client.post(f"/api/v1/cohorts/{cohort['cohort_id']}/refresh")

    assert refreshed.status_code == 200, refreshed.text
    refreshed_payload = refreshed.json()
    assert refreshed_payload["member_count"] == 1
    member_page = client.get(f"/api/v1/cohorts/{cohort['cohort_id']}/members?page=1&page_size=10")
    assert member_page.status_code == 200, member_page.text
    assert member_page.json()["items"][0]["canonical_user_id"] == "u_1"


def test_cohort_builder_connector_bigquery_table_preview_create_and_refresh(client):
    connector = _create_bigquery_connector(
        client,
        name="Warehouse Scores",
        mock_tables={
            "retention_scores": [
                {"player_id": "u_1", "email_address": "u1@example.com", "tier": "vip", "send_flag": "yes"},
                {"player_id": "u_2", "email_address": "u2@example.com", "tier": "core", "send_flag": "no"},
            ]
        },
    )
    connector_id = connector["connector_id"]

    response = client.post(
        "/api/v1/cohorts/builder/preview",
        json={
            "name": "connector_reward_users",
            "audience_basis": "connector_bigquery_table",
            "connector_id": connector_id,
            "table_name": "retention_scores",
            "selected_columns": ["player_id", "email_address", "tier", "send_flag"],
            "where_sql": "send_flag = 'yes'",
            "column_mapping": {
                "canonical_user_id": "player_id",
                "email": "email_address",
            },
        },
    )

    assert response.status_code == 200, response.text
    preview = response.json()
    assert preview["member_count"] == 1
    assert preview["preview_members"][0]["canonical_user_id"] == "u_1"
    assert preview["preview_members"][0]["email"] == "u1@example.com"

    create_response = client.post(
        "/api/v1/cohorts/builder/create",
        json=preview["request"],
    )

    assert create_response.status_code == 201, create_response.text
    created = create_response.json()["items"][0]
    assert created["definition"]["source_kind"] == "connector_bigquery_table"
    assert created["source_label"] == "BigQuery Connector"

    updated_connector = client.post(
        "/api/v1/connectors",
        json={
            "name": "Warehouse Scores",
            "type": "bigquery",
            "connector_id": connector_id,
            "config": {
                "project_id": "tenant-warehouse",
                "dataset_id": "growth_inputs",
                "mock_tables": {
                    "retention_scores": [
                        {"player_id": "u_1", "email_address": "u1@example.com", "tier": "vip", "send_flag": "yes"},
                        {"player_id": "u_3", "email_address": "u3@example.com", "tier": "vip", "send_flag": "yes"},
                    ]
                },
            },
        },
    )
    assert updated_connector.status_code == 201, updated_connector.text

    refreshed = client.post(f"/api/v1/cohorts/{created['cohort_id']}/refresh")

    assert refreshed.status_code == 200, refreshed.text
    member_page = client.get(f"/api/v1/cohorts/{created['cohort_id']}/members?page=1&page_size=10")
    assert member_page.status_code == 200, member_page.text
    member_ids = {item["canonical_user_id"] for item in member_page.json()["items"]}
    assert member_ids == {"u_1", "u_3"}


def test_cohort_builder_connector_bigquery_table_requires_canonical_user_id_mapping(client):
    connector = _create_bigquery_connector(
        client,
        name="Warehouse Scores",
        mock_tables={"retention_scores": [{"player_id": "u_1", "email_address": "u1@example.com"}]},
    )

    response = client.post(
        "/api/v1/cohorts/builder/preview",
        json={
            "name": "invalid_connector_reward_users",
            "audience_basis": "connector_bigquery_table",
            "connector_id": connector["connector_id"],
            "table_name": "retention_scores",
            "selected_columns": ["player_id", "email_address"],
            "column_mapping": {"email": "email_address"},
        },
    )

    assert response.status_code == 409
    assert "canonical_user_id" in response.json()["detail"]


def test_cohort_builder_connector_bigquery_table_rejects_unsafe_where_sql(client):
    connector = _create_bigquery_connector(
        client,
        name="Warehouse Scores",
        mock_tables={"retention_scores": [{"player_id": "u_1", "email_address": "u1@example.com"}]},
    )

    response = client.post(
        "/api/v1/cohorts/builder/preview",
        json={
            "name": "unsafe_connector_reward_users",
            "audience_basis": "connector_bigquery_table",
            "connector_id": connector["connector_id"],
            "table_name": "retention_scores",
            "column_mapping": {"canonical_user_id": "player_id"},
            "where_sql": "send_flag = 'yes'; DROP TABLE retention_scores",
        },
    )

    assert response.status_code == 409
    assert "where_sql" in response.json()["detail"].lower() or "unsafe" in response.json()["detail"].lower()


def test_cohort_builder_reverse_etl_cap_blocks_oversized_preview(client):
    settings = replace(get_settings(), max_reverse_etl_members_per_snapshot=1)
    client.app.dependency_overrides[get_settings_dependency] = lambda: settings

    response = client.post(
        "/api/v1/cohorts/builder/preview",
        json={
            "name": "oversized_warehouse_reward_users",
            "audience_basis": "managed_warehouse_sql",
            "sql": """
                SELECT 'u_1001' AS canonical_user_id, 'u1001@example.com' AS email
                UNION ALL
                SELECT 'u_1002' AS canonical_user_id, 'u1002@example.com' AS email
            """,
        },
    )

    assert response.status_code == 409
    assert "reverse etl" in response.json()["detail"].lower()
