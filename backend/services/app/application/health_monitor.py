from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List

from bigquery_service import BigQueryService, get_shared_bigquery_service


class HealthMonitorService:
    def __init__(self, repository, bigquery_service: BigQueryService | None = None):
        self.repository = repository
        self.bigquery_service = bigquery_service or get_shared_bigquery_service()

    def snapshot(self, *, reference_time: str | None = None, persist: bool = True) -> Dict[str, Any]:
        snapshot = self._build_snapshot(reference_time=reference_time)
        if persist:
            self._persist_snapshot(snapshot)
        return snapshot

    def list_alerts(self, *, include_resolved: bool = False, module: str | None = None) -> List[Dict[str, Any]]:
        self.snapshot(persist=True)
        items = [
            dict(record.get("payload") or {})
            for record in self.repository.list_resources("health_alert")
        ]
        if not include_resolved:
            items = [item for item in items if str(item.get("status") or "open") == "open"]
        if module:
            items = [item for item in items if str(item.get("module") or "") == str(module)]
        return items

    def list_modules(self) -> List[Dict[str, Any]]:
        self.snapshot(persist=True)
        items = [
            dict(record.get("payload") or {})
            for record in self.repository.list_resources("health_module")
        ]
        return items

    def _build_snapshot(self, *, reference_time: str | None = None) -> Dict[str, Any]:
        resolved_at = self._parse_reference_time(reference_time)
        cohort_events = self.repository.list_resource_events("cohort", event_type="cohort_refreshed", limit=5000)
        cohort_failures = self.repository.list_resource_events("cohort", event_type="cohort_refresh_failed", limit=5000)
        policy_logs = self.repository.list_resource_events("workflow", event_type="action_policy_log", limit=5000)
        action_execs = self.repository.list_resource_events("workflow", event_type="action_execution", limit=5000)
        decision_logs = self.repository.list_resource_events("experiment", event_type="decision", limit=5000)
        copilot_events = self.repository.list_resource_events("copilot", limit=5000)
        deliveries = [item.get("payload") or {} for item in self.repository.list_resources("workflow_delivery")]
        import_jobs = self.repository.list_import_jobs()
        identity_summary = self.bigquery_service.build_identity_summary()
        rejected_count = len(self.bigquery_service.get_pipeline_dead_letters(limit=5000))
        standardized_rows = len(self.bigquery_service.get_rows_for_alias("standardized"))
        invalid_decisions = sum(
            1
            for item in decision_logs
            if str(((item.get("payload") or {}).get("summary") or {}).get("decision") or "") == "invalid"
        )
        insufficient_copilot = sum(
            1
            for item in copilot_events
            if str((item.get("payload") or {}).get("response", {}).get("conclusion") or (item.get("payload") or {}).get("conclusion") or "") == "insufficient_evidence"
        )
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
            alerts.append(self._alert("data_core", "awaiting_mapping", "warning", metrics["awaiting_mapping_count"], "Imports are blocked awaiting mapping fixes.", resolved_at))
        if metrics["canonical_user_id_coverage"] < 90.0:
            alerts.append(self._alert("data_core", "canonical_coverage_low", "critical", metrics["canonical_user_id_coverage"], "canonical_user_id coverage is below 90%.", resolved_at))
        if metrics["reject_rate"] > 0.05:
            alerts.append(self._alert("data_core", "reject_rate_high", "critical", metrics["reject_rate"], "Reject rate exceeded the 5% gate.", resolved_at))
        if metrics["cohort_refresh_failure_rate"] > 0.05:
            alerts.append(self._alert("audience_engine", "refresh_failure_high", "warning", metrics["cohort_refresh_failure_rate"], "Dynamic cohort refresh success is below 95%.", resolved_at))
        if metrics["provider_failure_rate"] > 0.1:
            alerts.append(self._alert("action_orchestrator", "provider_failure_high", "warning", metrics["provider_failure_rate"], "Provider delivery failures are above 10%.", resolved_at))
        if metrics["invalid_experiment_decision_rate"] > 0.0:
            alerts.append(self._alert("experiment_hub", "invalid_decisions_present", "warning", metrics["invalid_experiment_decision_rate"], "Some experiment decisions are invalid and require investigation.", resolved_at))
        if metrics["copilot_insufficient_evidence_rate"] > 0.25:
            alerts.append(self._alert("insight_copilot", "insufficient_evidence_high", "warning", metrics["copilot_insufficient_evidence_rate"], "Copilot insufficient evidence rate is above 25%.", resolved_at))

        modules = {
            "data_core": self._module_status("data_core", alerts, {"canonical_user_id_coverage": metrics["canonical_user_id_coverage"], "reject_rate": metrics["reject_rate"]}, resolved_at),
            "audience_engine": self._module_status("audience_engine", alerts, {"cohort_refresh_failure_rate": metrics["cohort_refresh_failure_rate"]}, resolved_at),
            "action_orchestrator": self._module_status("action_orchestrator", alerts, {"provider_failure_rate": metrics["provider_failure_rate"], "policy_block_rate": metrics["policy_block_rate"]}, resolved_at),
            "experiment_hub": self._module_status("experiment_hub", alerts, {"invalid_experiment_decision_rate": metrics["invalid_experiment_decision_rate"]}, resolved_at),
            "insight_copilot": self._module_status("insight_copilot", alerts, {"copilot_insufficient_evidence_rate": metrics["copilot_insufficient_evidence_rate"]}, resolved_at),
        }
        return {
            "evaluated_at": resolved_at.isoformat(),
            "operational_metrics": metrics,
            "alerts": alerts,
            "modules": modules,
        }

    def _persist_snapshot(self, snapshot: Dict[str, Any]) -> None:
        evaluated_at = str(snapshot.get("evaluated_at") or datetime.utcnow().isoformat())
        active_alerts = {str(item.get("alert_id") or ""): dict(item) for item in snapshot.get("alerts", [])}
        existing_alerts = {
            str(item.get("resource_id") or ""): item
            for item in self.repository.list_resources("health_alert")
        }

        for alert_id, payload in active_alerts.items():
            existing_payload = dict((existing_alerts.get(alert_id) or {}).get("payload") or {})
            first_seen_at = str(existing_payload.get("first_seen_at") or evaluated_at)
            previous_status = str(existing_payload.get("status") or "")
            next_payload = {
                **payload,
                "status": "open",
                "first_seen_at": first_seen_at,
                "last_seen_at": evaluated_at,
                "resolved_at": None,
            }
            self.repository.upsert_resource("health_alert", alert_id, status="open", name=payload.get("code"), payload=next_payload)
            event_type = "alert_opened" if previous_status != "open" else "alert_updated"
            self.repository.record_resource_event("health_alert", alert_id, event_type=event_type, payload=next_payload)

        for alert_id, record in existing_alerts.items():
            if alert_id in active_alerts:
                continue
            existing_payload = dict(record.get("payload") or {})
            if str(existing_payload.get("status") or "") == "resolved":
                continue
            resolved_payload = {
                **existing_payload,
                "status": "resolved",
                "last_seen_at": evaluated_at,
                "resolved_at": evaluated_at,
            }
            self.repository.upsert_resource("health_alert", alert_id, status="resolved", name=existing_payload.get("code"), payload=resolved_payload)
            self.repository.record_resource_event("health_alert", alert_id, event_type="alert_resolved", payload=resolved_payload)

        for module_name, payload in (snapshot.get("modules") or {}).items():
            existing_record = self.repository.get_resource("health_module", module_name)
            existing_payload = dict((existing_record or {}).get("payload") or {})
            degraded_since = existing_payload.get("degraded_since")
            if payload.get("status") == "degraded" and not degraded_since:
                degraded_since = evaluated_at
            if payload.get("status") == "ok":
                degraded_since = None
            module_payload = {
                **payload,
                "updated_at": evaluated_at,
                "degraded_since": degraded_since,
            }
            self.repository.upsert_resource("health_module", module_name, status=str(payload.get("status") or "ok"), name=module_name, payload=module_payload)
            if str(existing_payload.get("status") or "") != str(payload.get("status") or ""):
                self.repository.record_resource_event("health_module", module_name, event_type="module_status_changed", payload=module_payload)

    @staticmethod
    def _alert(module: str, code: str, severity: str, value: Any, message: str, evaluated_at: datetime) -> Dict[str, Any]:
        return {
            "alert_id": f"{module}:{code}",
            "module": module,
            "code": code,
            "severity": severity,
            "current_value": value,
            "message": message,
            "status": "open",
            "evaluated_at": evaluated_at.isoformat(),
        }

    @staticmethod
    def _module_status(module: str, alerts: List[Dict[str, Any]], metrics: Dict[str, Any], evaluated_at: datetime) -> Dict[str, Any]:
        module_alerts = [item for item in alerts if item["module"] == module]
        return {
            "module": module,
            "status": "degraded" if module_alerts else "ok",
            "alerts": module_alerts,
            "metrics": metrics,
            "updated_at": evaluated_at.isoformat(),
        }

    @staticmethod
    def _parse_reference_time(reference_time: str | None) -> datetime:
        if not reference_time:
            return datetime.utcnow()
        try:
            return datetime.fromisoformat(str(reference_time))
        except Exception:
            return datetime.utcnow()
