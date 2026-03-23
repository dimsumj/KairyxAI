from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterable, Tuple

from fastapi import HTTPException, Request

from app.core.request_context import RequestContext, get_request_context


VALID_ROLES = {"admin", "analyst", "operator"}
PII_FIELDS = {"email", "user_email", "phone", "phone_hash", "email_hash", "webhook_token"}

ROLE_PERMISSIONS = {
    "admin": {"*"},
    "analyst": {
        "copilot.query",
        "copilot.explain",
        "copilot.report",
        "copilot.metrics.read",
        "copilot.overview.read",
        "copilot.query_log.read",
        "copilot.anomalies.read",
        "copilot.anomaly.read",
        "copilot.reports.read",
        "copilot.report.retry",
        "copilot.report.review",
        "cohorts.overview.read",
        "cohorts.metrics.read",
        "cohorts.compare.read",
        "cohorts.refresh_jobs.read",
        "audit.logs.read",
        "imports.quality.read",
        "imports.identity_links.read",
        "imports.conflicts.read",
        "imports.rejected.read",
        "imports.operations.read",
        "imports.backfills.read",
        "imports.schema.read",
        "sql_workspace.preview",
        "sql_workspace.queries.read",
        "mappings.suggestions.read",
        "audit.logs.read",
        "templates.read",
        "workflows.deliveries.read",
        "workflows.diagnostics.read",
        "workflows.versions.read",
        "experiments.read",
        "experiments.versions.read",
        "experiments.assignments.read",
        "experiments.rollout.read",
        "experiments.integrity.read",
        "health.scheduler.read",
        "predictions.models.read",
    },
    "operator": {
        "provider_connections.read",
        "provider_connections.write",
        "exports.create",
        "exports.run",
        "exports.retry",
        "exports.diagnostics.read",
        "connectors.write",
        "mappings.update",
        "mappings.rollback",
        "imports.resume",
        "imports.replay",
        "imports.quality.read",
        "imports.identity_links.read",
        "imports.conflicts.read",
        "imports.rejected.read",
        "imports.operations.read",
        "imports.backfills.read",
        "imports.backfills.create",
        "imports.schema.read",
        "cohorts.create",
        "cohorts.refresh",
        "cohorts.activate",
        "cohorts.pause",
        "cohorts.archive",
        "cohorts.restore",
        "cohorts.permanent_delete",
        "cohorts.update",
        "cohorts.overview.read",
        "cohorts.metrics.read",
        "cohorts.compare.read",
        "cohorts.refresh_jobs.read",
        "workflows.create",
        "workflows.publish",
        "workflows.pause",
        "workflows.resume",
        "workflows.execute",
        "workflows.deliveries.read",
        "workflows.diagnostics.read",
        "workflows.update",
        "workflows.versions.read",
        "workflows.confirm",
        "orchestrator.events.ingest",
        "orchestrator.thresholds.evaluate",
        "orchestrator.kill_switch",
        "activation.callbacks.ingest",
        "experiments.read",
        "experiments.config.write",
        "experiments.start",
        "experiments.stop",
        "experiments.decision",
        "experiments.outcomes.ingest",
        "experiments.versions.read",
        "experiments.assignments.read",
        "experiments.rollout.read",
        "experiments.integrity.read",
        "experiments.optimizer.run",
        "sql_workspace.preview",
        "sql_workspace.queries.read",
        "sql_workspace.queries.create",
        "sql_workspace.query_to_cohort",
        "mappings.suggestions.read",
        "audit.logs.read",
        "templates.read",
        "templates.instantiate",
        "health.scheduler.read",
        "health.scheduler.tick",
        "predictions.models.read",
        "predictions.models.train",
    },
}


@dataclass(frozen=True)
class GovernanceContext:
    actor_role: str
    actor_id: str
    tenant_id: str | None
    correlation_id: str = ""
    platform_admin: bool = False
    auth_mode: str = "system"


def get_governance_context(request: Request) -> GovernanceContext:
    current = getattr(request.state, "governance_context", None)
    if isinstance(current, GovernanceContext):
        return current

    request_context = get_request_context()
    if request_context is not None:
        actor_role = str(request_context.actor_role or "admin").strip().lower()
        if actor_role not in VALID_ROLES:
            raise HTTPException(status_code=400, detail=f"Unsupported actor_role '{actor_role}'.")
        context = GovernanceContext(
            actor_role=actor_role,
            actor_id=request_context.actor_id,
            tenant_id=request_context.tenant_id,
            correlation_id=request_context.correlation_id,
            platform_admin=bool(request_context.platform_admin),
            auth_mode=request_context.auth_mode,
        )
        request.state.governance_context = context
        return context

    actor_role = str(request.headers.get("x-actor-role") or "admin").strip().lower()
    actor_id = str(request.headers.get("x-actor-id") or actor_role).strip() or actor_role
    tenant_id = str(request.headers.get("x-tenant-id") or "default").strip() or "default"
    if actor_role not in VALID_ROLES:
        raise HTTPException(status_code=400, detail=f"Unsupported actor_role '{actor_role}'.")
    context = GovernanceContext(actor_role=actor_role, actor_id=actor_id, tenant_id=tenant_id, auth_mode="legacy_headers")
    request.state.governance_context = context
    return context


def ensure_permission(context: GovernanceContext, permission: str) -> None:
    if context.platform_admin:
        return
    allowed = ROLE_PERMISSIONS.get(context.actor_role, set())
    if "*" in allowed or permission in allowed:
        return
    raise HTTPException(status_code=403, detail=f"actor_role '{context.actor_role}' is not allowed to perform '{permission}'.")


def ensure_platform_admin(context: GovernanceContext) -> None:
    if context.platform_admin:
        return
    raise HTTPException(status_code=403, detail="Platform admin access is required.")


def record_audit(
    repository,
    context: GovernanceContext,
    *,
    action_type: str,
    resource_type: str,
    resource_id: str | None,
    payload: Dict[str, Any],
) -> int:
    event = repository.record_action(
        action_type,
        resource_type,
        resource_id,
        {
            "actor_role": context.actor_role,
            "actor_id": context.actor_id,
            "tenant_id": context.tenant_id,
            "correlation_id": context.correlation_id,
            "platform_admin": context.platform_admin,
            "auth_mode": context.auth_mode,
            "payload": payload,
        },
    )
    return int(event["id"])


def apply_masking(payload: Any, actor_role: str) -> Tuple[Any, list[str]]:
    if actor_role == "admin":
        return payload, []

    masked_fields: set[str] = set()

    def _mask_scalar(value: Any) -> Any:
        if value is None:
            return None
        if isinstance(value, str):
            if "@" in value:
                local, _, domain = value.partition("@")
                local_mask = f"{local[:1]}***" if local else "***"
                domain_mask = domain if domain else "***"
                return f"{local_mask}@{domain_mask}"
            if len(value) <= 6:
                return "***"
            return f"{value[:3]}***{value[-2:]}"
        return "***"

    def _walk(value: Any) -> Any:
        if isinstance(value, dict):
            result: Dict[str, Any] = {}
            for key, item in value.items():
                normalized = str(key).lower()
                if normalized in PII_FIELDS:
                    masked_fields.add(normalized)
                    result[key] = _mask_scalar(item)
                else:
                    result[key] = _walk(item)
            return result
        if isinstance(value, list):
            return [_walk(item) for item in value]
        if isinstance(value, tuple):
            return tuple(_walk(item) for item in value)
        return value

    return _walk(payload), sorted(masked_fields)


def build_audited_response(
    repository,
    context: GovernanceContext,
    *,
    action_type: str,
    resource_type: str,
    resource_id: str | None,
    payload: Any,
) -> Dict[str, Any]:
    masked_payload, masked_fields = apply_masking(payload, context.actor_role)
    audit_id = record_audit(
        repository,
        context,
        action_type=action_type,
        resource_type=resource_type,
        resource_id=resource_id,
        payload={"masked_fields": masked_fields, "response": masked_payload},
    )
    if isinstance(masked_payload, dict):
        return {
            **masked_payload,
            "audit_id": audit_id,
            "tenant_id": context.tenant_id,
            "correlation_id": context.correlation_id,
            "masked_fields": masked_fields,
        }
    return {
        "data": masked_payload,
        "audit_id": audit_id,
        "tenant_id": context.tenant_id,
        "correlation_id": context.correlation_id,
        "masked_fields": masked_fields,
    }


def collect_masked_fields(*field_sets: Iterable[str]) -> list[str]:
    combined: set[str] = set()
    for items in field_sets:
        combined.update(str(item) for item in items)
    return sorted(combined)
