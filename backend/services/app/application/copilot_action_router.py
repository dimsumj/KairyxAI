from __future__ import annotations

from typing import Any, Dict, List, Mapping, Protocol

from app.application.copilot_action_artifacts import (
    artifact_for_cohort,
    artifact_for_connector,
    artifact_for_experiment,
    artifact_for_provider_connection,
    artifact_for_saved_query,
)


class CopilotActionHandler(Protocol):
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
            action_type,
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
        action_type: str,
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


class ConnectionActionHandler:
    def __init__(self, *, connectors: Any, provider_connections: Any):
        self.connectors = connectors
        self.provider_connections = provider_connections

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
        if action_type == "upsert_connector":
            connector = self.connectors.create_connector(
                parameters["name"],
                parameters["connector_type"],
                parameters["config"],
            )
            return {
                "summary": f"Created connector `{connector['name']}` for `{connector['type']}`.",
                "result": {"connector": connector},
                "artifacts": [artifact_for_connector(connector)],
            }
        if action_type == "check_connector_health":
            try:
                health = self.connectors.health_check(parameters["name"])
                health_label = health.get("message") or ("ok" if health.get("ok") else "issue")
                return {
                    "summary": f"Ran health check for `{parameters['name']}` and the connector reported `{health_label}`.",
                    "result": {"health": health},
                    "artifacts": [],
                }
            except Exception as exc:
                return {
                    "summary": f"Created the connector, but the optional health check could not complete: {exc}",
                    "result": {"health_error": str(exc)},
                    "artifacts": [],
                }
        if action_type == "upsert_provider_connection":
            existing = None
            if parameters.get("update_existing"):
                existing = next(
                    (
                        item
                        for item in self.provider_connections.list_connections()
                        if str(item.get("name") or "") == str(parameters["name"])
                        and str(item.get("provider") or "") == str(parameters["provider"])
                    ),
                    None,
                )
            if existing is not None:
                connection = self.provider_connections.update_connection(
                    existing["provider_connection_id"],
                    {"name": parameters["name"], "config": parameters["config"]},
                )
                summary = f"Updated provider connection `{connection['name']}` for `{connection['provider']}`."
            else:
                connection = self.provider_connections.create_connection(
                    parameters["name"],
                    parameters["provider"],
                    parameters["config"],
                )
                summary = f"Created provider connection `{connection['name']}` for `{connection['provider']}`."
            return {
                "summary": summary,
                "result": {"provider_connection": connection},
                "artifacts": [artifact_for_provider_connection(connection)],
            }
        raise ValueError(f"Unsupported connection action '{action_type}'.")


class CohortActionHandler:
    def __init__(self, *, cohorts: Any, sql_workspace: Any):
        self.cohorts = cohorts
        self.sql_workspace = sql_workspace

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
        if action_type == "preview_sql":
            preview = self.sql_workspace.preview(
                parameters["sql"],
                limit=int(parameters.get("limit") or 20),
                timeout_seconds=int(parameters.get("timeout_seconds") or 30),
            )
            return {
                "summary": f"Previewed the SQL query and returned {int(preview.get('row_count') or 0)} row(s).",
                "result": {"preview": preview},
                "artifacts": [],
            }
        if action_type == "save_query":
            saved_query = self.sql_workspace.create_saved_query(
                parameters["name"],
                parameters["sql"],
                parameters.get("description") or "",
            )
            return {
                "summary": f"Saved SQL query `{saved_query['name']}`.",
                "result": {"saved_query": saved_query},
                "artifacts": [artifact_for_saved_query(saved_query)],
            }
        if action_type == "create_cohort_sql":
            cohort = self.cohorts.create_cohort(
                name=parameters["name"],
                cohort_type="sql",
                definition=dict(parameters["definition"] or {}),
                refresh_mode=parameters.get("refresh_mode") or "manual",
                owner=parameters.get("owner") or context.actor_id,
                description=parameters.get("description") or "",
                tags=list(parameters.get("tags") or []),
                activate=False,
            )
            return {
                "summary": f"Created draft SQL cohort `{cohort['name']}` with {int(cohort.get('member_count') or 0)} member(s).",
                "result": {"cohort": cohort},
                "artifacts": [artifact_for_cohort(cohort)],
            }
        if action_type == "create_cohort_definition":
            cohort = self.cohorts.create_cohort(
                name=parameters["name"],
                cohort_type=parameters["cohort_type"],
                definition=dict(parameters["definition"] or {}),
                refresh_mode=parameters.get("refresh_mode") or "manual",
                owner=parameters.get("owner") or context.actor_id,
                description=parameters.get("description") or "",
                tags=list(parameters.get("tags") or []),
                activate=False,
            )
            return {
                "summary": f"Created draft {parameters['cohort_type']} cohort `{cohort['name']}`.",
                "result": {"cohort": cohort},
                "artifacts": [artifact_for_cohort(cohort)],
            }
        if action_type == "update_cohort_definition":
            cohort = self.cohorts.update_cohort(
                parameters["cohort_id"],
                {
                    "name": parameters["name"],
                    "type": parameters["cohort_type"],
                    "definition": dict(parameters["definition"] or {}),
                    "refresh_mode": parameters.get("refresh_mode") or "manual",
                    "owner": parameters.get("owner") or context.actor_id,
                    "description": parameters.get("description") or "",
                    "tags": list(parameters.get("tags") or []),
                },
            )
            return {
                "summary": f"Updated draft cohort `{cohort['name']}`.",
                "result": {"cohort": cohort},
                "artifacts": [artifact_for_cohort(cohort)],
            }
        if action_type == "activate_cohort":
            cohort = self.cohorts.activate_cohort(parameters["cohort_id"])
            return {
                "summary": f"Activated cohort `{cohort['name']}`.",
                "result": {"cohort": cohort},
                "artifacts": [artifact_for_cohort(cohort)],
            }
        if action_type == "pause_cohort":
            cohort = self.cohorts.pause_cohort(parameters["cohort_id"])
            return {
                "summary": f"Paused cohort `{cohort['name']}`.",
                "result": {"cohort": cohort},
                "artifacts": [artifact_for_cohort(cohort)],
            }
        if action_type == "archive_cohort":
            cohort = self.cohorts.archive_cohort(parameters["cohort_id"])
            return {
                "summary": f"Archived cohort `{cohort['name']}`.",
                "result": {"cohort": cohort},
                "artifacts": [artifact_for_cohort(cohort)],
            }
        if action_type == "restore_cohort":
            cohort = self.cohorts.restore_cohort(parameters["cohort_id"])
            return {
                "summary": f"Restored cohort `{cohort['name']}` to draft status.",
                "result": {"cohort": cohort},
                "artifacts": [artifact_for_cohort(cohort)],
            }
        raise ValueError(f"Unsupported cohort action '{action_type}'.")


class ExperimentActionHandler:
    def __init__(self, *, experiments: Any):
        self.experiments = experiments

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
        if action_type == "save_experiment_config":
            experiment = self.experiments.save_config(parameters, experiment_id=parameters["experiment_id"])
            return {
                "summary": f"Saved experiment config `{experiment['experiment_id']}` in a non-running state.",
                "result": {"experiment": experiment},
                "artifacts": [artifact_for_experiment(experiment)],
            }
        if action_type == "start_experiment":
            experiment = self.experiments.start(parameters["experiment_id"])
            return {
                "summary": f"Started experiment `{parameters['experiment_id']}`.",
                "result": {"experiment": experiment},
                "artifacts": [artifact_for_experiment(experiment)],
            }
        if action_type == "stop_experiment":
            experiment = self.experiments.stop(parameters["experiment_id"])
            return {
                "summary": f"Stopped experiment `{parameters['experiment_id']}`.",
                "result": {"experiment": experiment},
                "artifacts": [artifact_for_experiment(experiment)],
            }
        if action_type == "record_experiment_decision":
            decision = self.experiments.decide(
                parameters["experiment_id"],
                decided_by=parameters.get("decided_by") or context.actor_id,
            )
            return {
                "summary": f"Recorded an experiment decision for `{parameters['experiment_id']}`.",
                "result": {"decision": decision},
                "artifacts": [artifact_for_experiment({"experiment_id": parameters["experiment_id"]})],
            }
        raise ValueError(f"Unsupported experiment action '{action_type}'.")


def build_copilot_action_router(
    *,
    repository: Any,
    copilot: Any,
    health_monitor: Any,
    cohorts: Any,
    connectors: Any,
    provider_connections: Any,
    sql_workspace: Any,
    experiments: Any,
) -> CopilotActionRouter:
    connection_handler = ConnectionActionHandler(
        connectors=connectors,
        provider_connections=provider_connections,
    )
    cohort_handler = CohortActionHandler(cohorts=cohorts, sql_workspace=sql_workspace)
    experiment_handler = ExperimentActionHandler(experiments=experiments)
    return CopilotActionRouter(
        {
            "summarize_dashboard": DashboardSummaryActionHandler(
                repository=repository,
                copilot=copilot,
                health_monitor=health_monitor,
                cohorts=cohorts,
            ),
            "upsert_connector": connection_handler,
            "check_connector_health": connection_handler,
            "upsert_provider_connection": connection_handler,
            "preview_sql": cohort_handler,
            "save_query": cohort_handler,
            "create_cohort_sql": cohort_handler,
            "create_cohort_definition": cohort_handler,
            "update_cohort_definition": cohort_handler,
            "activate_cohort": cohort_handler,
            "pause_cohort": cohort_handler,
            "archive_cohort": cohort_handler,
            "restore_cohort": cohort_handler,
            "save_experiment_config": experiment_handler,
            "start_experiment": experiment_handler,
            "stop_experiment": experiment_handler,
            "record_experiment_decision": experiment_handler,
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
