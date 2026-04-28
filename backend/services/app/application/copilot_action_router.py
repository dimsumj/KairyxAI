from __future__ import annotations

from typing import Any, Dict, List, Mapping, Protocol


class CopilotActionHandler(Protocol):
    def execute(
        self,
        parameters: Dict[str, Any],
        *,
        context: Any,
        session: Dict[str, Any],
        ui_context: Dict[str, Any],
        model_adapter: Any,
    ) -> Dict[str, Any]:
        ...


class CopilotActionRouter:
    def __init__(self, handlers: Mapping[str, CopilotActionHandler]):
        self._handlers = dict(handlers)

    def can_execute(self, action_type: str) -> bool:
        return action_type in self._handlers

    def execute(
        self,
        action_type: str,
        parameters: Dict[str, Any],
        *,
        context: Any,
        session: Dict[str, Any],
        ui_context: Dict[str, Any],
        model_adapter: Any,
    ) -> Dict[str, Any]:
        handler = self._handlers.get(action_type)
        if handler is None:
            raise KeyError(action_type)
        return handler.execute(
            parameters,
            context=context,
            session=session,
            ui_context=ui_context,
            model_adapter=model_adapter,
        )


class DashboardSummaryActionHandler:
    def __init__(self, *, repository: Any, copilot: Any, health_monitor: Any, cohorts: Any):
        self.repository = repository
        self.copilot = copilot
        self.health_monitor = health_monitor
        self.cohorts = cohorts

    def execute(
        self,
        parameters: Dict[str, Any],
        *,
        context: Any,
        session: Dict[str, Any],
        ui_context: Dict[str, Any],
        model_adapter: Any,
    ) -> Dict[str, Any]:
        overview = self.copilot.get_overview()
        health = self.health_monitor.snapshot(persist=True)
        cohorts = self.cohorts.list_cohorts()
        workflows = self.repository.list_resources("workflow")
        experiments = [item.get("payload") or {} for item in self.repository.list_resources("experiment")]
        imports = self.repository.list_import_jobs()
        active_cohorts = [item for item in cohorts if str(item.get("status") or "") == "active"]
        open_alerts = [item for item in health.get("alerts") or [] if str(item.get("status") or "open") == "open"]
        active_experiments = [item for item in experiments if str(item.get("status") or "") == "active"]
        published_workflows = [
            item
            for item in workflows
            if str((item.get("payload") or {}).get("status") or item.get("status") or "") == "published"
        ]
        blocked_imports = [item for item in imports if str(item.get("status") or "") in {"awaiting_mapping", "failed"}]
        top_risks = [
            str(item.get("message") or "")
            for item in open_alerts[:3]
            if str(item.get("message") or "").strip()
        ]
        suggested_next_steps = build_summary_next_steps(open_alerts, blocked_imports, overview)
        headline = (
            f"{len(open_alerts)} open alert(s), {len(active_cohorts)} active cohort(s), "
            f"{len(published_workflows)} published workflow(s), and {len(active_experiments)} active experiment(s)."
        )
        summary = {
            "headline": headline,
            "counts": {
                "open_alerts": len(open_alerts),
                "active_cohorts": len(active_cohorts),
                "published_workflows": len(published_workflows),
                "active_experiments": len(active_experiments),
                "blocked_imports": len(blocked_imports),
                "recent_reports": int((overview.get("report_counts") or {}).get("total") or 0),
            },
            "top_risks": top_risks,
            "suggested_next_steps": suggested_next_steps,
            "recent_reports": overview.get("recent_reports") or [],
            "recent_anomalies": overview.get("recent_anomalies") or [],
            "module_statuses": health.get("modules") or {},
            "blocked_imports": blocked_imports[:5],
            "active_cohorts": active_cohorts[:5],
        }
        return {
            "summary": headline if not top_risks else f"{headline} Top risk: {top_risks[0]}",
            "result": {"dashboard_summary": summary},
            "artifacts": [],
        }


def build_copilot_action_router(
    *,
    repository: Any,
    copilot: Any,
    health_monitor: Any,
    cohorts: Any,
) -> CopilotActionRouter:
    return CopilotActionRouter(
        {
            "summarize_dashboard": DashboardSummaryActionHandler(
                repository=repository,
                copilot=copilot,
                health_monitor=health_monitor,
                cohorts=cohorts,
            )
        }
    )


def build_summary_next_steps(
    alerts: List[Dict[str, Any]],
    blocked_imports: List[Dict[str, Any]],
    overview: Dict[str, Any],
) -> List[str]:
    steps: List[str] = []
    if blocked_imports:
        steps.append("Resolve blocked imports before relying on downstream cohorts or experiments.")
    if any(str(item.get("module") or "") == "experiment_hub" for item in alerts):
        steps.append("Review experiment integrity warnings before making rollout decisions.")
    if any(str(item.get("module") or "") == "audience_engine" for item in alerts):
        steps.append("Inspect recent cohort refresh failures and rerun any stale audience definitions.")
    if int((overview.get("report_counts") or {}).get("pending_review") or 0) > 0:
        steps.append("Review pending Copilot reports so anomalies and recommendations do not age out.")
    if not steps:
        steps.append("The workspace looks healthy; the next safe step is to prepare the next cohort or experiment draft.")
    return steps
