from __future__ import annotations

from fastapi import APIRouter, Depends

from app.core.settings import get_settings
from app.core.deps import get_repository
from bigquery_service import get_shared_bigquery_service


router = APIRouter(tags=["health"])


@router.get("/health")
def health(repository=Depends(get_repository)):
    settings = get_settings()
    bigquery_service = get_shared_bigquery_service()
    cohort_events = repository.list_resource_events("cohort", event_type="cohort_refreshed", limit=5000)
    cohort_failures = repository.list_resource_events("cohort", event_type="cohort_refresh_failed", limit=5000)
    policy_logs = repository.list_resource_events("workflow", event_type="action_policy_log", limit=5000)
    action_execs = repository.list_resource_events("workflow", event_type="action_execution", limit=5000)
    decision_logs = repository.list_resource_events("experiment", event_type="decision", limit=5000)
    copilot_events = repository.list_resource_events("copilot", limit=5000)
    invalid_decisions = sum(1 for item in decision_logs if str(((item.get("payload") or {}).get("summary") or {}).get("decision") or "") == "invalid")
    insufficient_copilot = sum(1 for item in copilot_events if str((item.get("payload") or {}).get("conclusion") or "") == "insufficient_evidence")
    payload = {
        "status": "ok",
        "service": settings.app_name,
        "mode": settings.data_backend_mode,
        "data_aliases": bigquery_service.get_v1_table_aliases(),
        "operational_metrics": {
            "awaiting_mapping_count": sum(1 for job in repository.list_import_jobs() if str(job.get("status") or "") == "awaiting_mapping"),
            "cohort_refresh_failure_rate": round(len(cohort_failures) / max(1, len(cohort_events) + len(cohort_failures)), 4),
            "policy_block_rate": round(len(policy_logs) / max(1, len(policy_logs) + len(action_execs)), 4),
            "duplicate_suppressed_rate": round(
                sum(1 for item in policy_logs if str((item.get("payload") or {}).get("reason") or "") == "duplicate_suppressed") / max(1, len(policy_logs)),
                4,
            ),
            "invalid_experiment_decision_rate": round(invalid_decisions / max(1, len(decision_logs)), 4),
            "copilot_insufficient_evidence_rate": round(insufficient_copilot / max(1, len(copilot_events)), 4),
        },
    }
    if settings.data_backend_mode == "mock":
        payload["local_cache"] = bigquery_service.get_local_cache_stats()
    return payload
