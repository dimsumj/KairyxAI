from __future__ import annotations

from typing import Any, Dict


HIGH_RISK_ACTION_TYPES = {
    "cohort_permanently_deleted",
    "workflow_confirmed",
    "orchestrator_kill_switch_changed",
    "experiment_decision_recorded",
    "import_job_replay_completed",
    "workflow_execution_completed",
}


class AuditService:
    def __init__(self, repository):
        self.repository = repository

    def list_actions(
        self,
        *,
        limit: int = 100,
        action_type: str | None = None,
        resource_type: str | None = None,
        resource_id: str | None = None,
        actor_role: str | None = None,
        tenant_id: str | None = None,
        project_id: str | None = None,
        high_risk_only: bool = False,
        include_all_tenants: bool = False,
    ) -> Dict[str, Any]:
        raw_items = self.repository.list_actions(
            limit=max(50, int(limit) * 10),
            tenant_id=tenant_id,
            project_id=project_id,
            include_all_tenants=include_all_tenants,
        )
        items = []
        for item in raw_items:
            payload = item.get("payload") or {}
            actor = payload.get("actor_role")
            tenant = payload.get("tenant_id")
            project = payload.get("project_id")
            is_high_risk = self._is_high_risk(item)
            if action_type and str(item.get("action_type") or "") != str(action_type):
                continue
            if resource_type and str(item.get("resource_type") or "") != str(resource_type):
                continue
            if resource_id and str(item.get("resource_id") or "") != str(resource_id):
                continue
            if actor_role and str(actor or "") != str(actor_role):
                continue
            if tenant_id and str(tenant or "default") != str(tenant_id):
                continue
            if project_id and str(project or "default") != str(project_id):
                continue
            if high_risk_only and not is_high_risk:
                continue
            items.append({**item, "high_risk": is_high_risk})
            if len(items) >= max(1, int(limit)):
                break
        return {
            "items": items,
            "summary": {
                "returned": len(items),
                "high_risk_count": sum(1 for item in items if item.get("high_risk")),
                "filters": {
                    "action_type": action_type,
                    "resource_type": resource_type,
                    "resource_id": resource_id,
                    "actor_role": actor_role,
                    "tenant_id": tenant_id,
                    "project_id": project_id,
                    "high_risk_only": bool(high_risk_only),
                },
            },
        }

    @staticmethod
    def _is_high_risk(item: Dict[str, Any]) -> bool:
        action_type = str(item.get("action_type") or "")
        if action_type in HIGH_RISK_ACTION_TYPES:
            return True
        payload = item.get("payload") or {}
        nested = payload.get("payload") if isinstance(payload.get("payload"), dict) else {}
        if nested.get("requires_confirmation") is True:
            return True
        if nested.get("status") in {"archived", "deleted"}:
            return True
        return False
