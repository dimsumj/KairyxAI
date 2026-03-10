from __future__ import annotations

from fastapi import APIRouter, Depends

from app.core.settings import get_settings
from app.core.deps import get_repository
from bigquery_service import get_shared_bigquery_service


router = APIRouter(tags=["health"])


def _health_snapshot(repository, bigquery_service):
    cohort_events = repository.list_resource_events("cohort", event_type="cohort_refreshed", limit=5000)
    cohort_failures = repository.list_resource_events("cohort", event_type="cohort_refresh_failed", limit=5000)
    policy_logs = repository.list_resource_events("workflow", event_type="action_policy_log", limit=5000)
    action_execs = repository.list_resource_events("workflow", event_type="action_execution", limit=5000)
    decision_logs = repository.list_resource_events("experiment", event_type="decision", limit=5000)
    copilot_events = repository.list_resource_events("copilot", limit=5000)
    deliveries = [item.get("payload") or {} for item in repository.list_resources("workflow_delivery")]
    import_jobs = repository.list_import_jobs()
    identity_summary = bigquery_service.build_identity_summary()
    rejected_count = len(bigquery_service.get_pipeline_dead_letters(limit=5000))
    standardized_rows = len(bigquery_service.get_rows_for_alias("standardized"))
    invalid_decisions = sum(1 for item in decision_logs if str(((item.get("payload") or {}).get("summary") or {}).get("decision") or "") == "invalid")
    insufficient_copilot = sum(1 for item in copilot_events if str((item.get("payload") or {}).get("response", {}).get("conclusion") or (item.get("payload") or {}).get("conclusion") or "") == "insufficient_evidence")
    provider_failures = sum(1 for item in deliveries if str(item.get("delivery_status") or "") == "failed")
    metrics = {
        "awaiting_mapping_count": sum(1 for job in import_jobs if str(job.get("status") or "") == "awaiting_mapping"),
        "cohort_refresh_failure_rate": round(len(cohort_failures) / max(1, len(cohort_events) + len(cohort_failures)), 4),
        "policy_block_rate": round(len(policy_logs) / max(1, len(policy_logs) + len(action_execs)), 4),
        "duplicate_suppressed_rate": round(
            sum(1 for item in policy_logs if str((item.get("payload") or {}).get("reason") or "") == "duplicate_suppressed") / max(1, len(policy_logs)),
            4,
        ),
        "invalid_experiment_decision_rate": round(invalid_decisions / max(1, len(decision_logs)), 4),
        "copilot_insufficient_evidence_rate": round(insufficient_copilot / max(1, len(copilot_events)), 4),
        "canonical_user_id_coverage": round(float(identity_summary.get("canonical_user_id_coverage") or 0.0), 2),
        "reject_rate": round(rejected_count / max(1, standardized_rows + rejected_count), 4),
        "provider_failure_rate": round(provider_failures / max(1, len(deliveries)), 4),
    }
    alerts = []
    if metrics["awaiting_mapping_count"] > 0:
        alerts.append(_alert("data_core", "awaiting_mapping", "warning", metrics["awaiting_mapping_count"], "Imports are blocked awaiting mapping fixes."))
    if metrics["canonical_user_id_coverage"] < 90.0:
        alerts.append(_alert("data_core", "canonical_coverage_low", "critical", metrics["canonical_user_id_coverage"], "canonical_user_id coverage is below 90%."))
    if metrics["reject_rate"] > 0.05:
        alerts.append(_alert("data_core", "reject_rate_high", "critical", metrics["reject_rate"], "Reject rate exceeded the 5% gate."))
    if metrics["cohort_refresh_failure_rate"] > 0.05:
        alerts.append(_alert("audience_engine", "refresh_failure_high", "warning", metrics["cohort_refresh_failure_rate"], "Dynamic cohort refresh success is below 95%."))
    if metrics["provider_failure_rate"] > 0.1:
        alerts.append(_alert("action_orchestrator", "provider_failure_high", "warning", metrics["provider_failure_rate"], "Provider delivery failures are above 10%."))
    if metrics["invalid_experiment_decision_rate"] > 0.0:
        alerts.append(_alert("experiment_hub", "invalid_decisions_present", "warning", metrics["invalid_experiment_decision_rate"], "Some experiment decisions are invalid and require investigation."))
    if metrics["copilot_insufficient_evidence_rate"] > 0.25:
        alerts.append(_alert("insight_copilot", "insufficient_evidence_high", "warning", metrics["copilot_insufficient_evidence_rate"], "Copilot insufficient evidence rate is above 25%."))

    modules = {
        "data_core": _module_status("data_core", alerts, {"canonical_user_id_coverage": metrics["canonical_user_id_coverage"], "reject_rate": metrics["reject_rate"]}),
        "audience_engine": _module_status("audience_engine", alerts, {"cohort_refresh_failure_rate": metrics["cohort_refresh_failure_rate"]}),
        "action_orchestrator": _module_status("action_orchestrator", alerts, {"provider_failure_rate": metrics["provider_failure_rate"], "policy_block_rate": metrics["policy_block_rate"]}),
        "experiment_hub": _module_status("experiment_hub", alerts, {"invalid_experiment_decision_rate": metrics["invalid_experiment_decision_rate"]}),
        "insight_copilot": _module_status("insight_copilot", alerts, {"copilot_insufficient_evidence_rate": metrics["copilot_insufficient_evidence_rate"]}),
    }
    return {"operational_metrics": metrics, "alerts": alerts, "modules": modules}


def _alert(module: str, code: str, severity: str, value, message: str):
    return {
        "module": module,
        "code": code,
        "severity": severity,
        "current_value": value,
        "message": message,
    }


def _module_status(module: str, alerts: list[dict], metrics: dict):
    module_alerts = [item for item in alerts if item["module"] == module]
    return {
        "module": module,
        "status": "degraded" if module_alerts else "ok",
        "alerts": module_alerts,
        "metrics": metrics,
    }


@router.get("/health")
def health(repository=Depends(get_repository)):
    settings = get_settings()
    bigquery_service = get_shared_bigquery_service()
    snapshot = _health_snapshot(repository, bigquery_service)
    payload = {
        "status": "ok",
        "service": settings.app_name,
        "mode": settings.data_backend_mode,
        "data_aliases": bigquery_service.get_v1_table_aliases(),
        **snapshot,
    }
    if settings.data_backend_mode == "mock":
        payload["local_cache"] = bigquery_service.get_local_cache_stats()
    return payload


@router.get("/health/modules")
def health_modules(repository=Depends(get_repository)):
    snapshot = _health_snapshot(repository, get_shared_bigquery_service())
    return {"items": list(snapshot["modules"].values())}


@router.get("/health/alerts")
def health_alerts(repository=Depends(get_repository)):
    snapshot = _health_snapshot(repository, get_shared_bigquery_service())
    return {"items": snapshot["alerts"]}
