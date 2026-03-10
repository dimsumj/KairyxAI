from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterable, Tuple

from fastapi import HTTPException, Request


VALID_ROLES = {"admin", "analyst", "operator"}
PII_FIELDS = {"email", "user_email"}

ROLE_PERMISSIONS = {
    "admin": {"*"},
    "analyst": {
        "copilot.query",
        "copilot.explain",
        "copilot.report",
        "copilot.query_log.read",
        "copilot.anomalies.read",
        "copilot.reports.read",
        "cohorts.metrics.read",
        "cohorts.compare.read",
        "imports.quality.read",
        "workflows.deliveries.read",
        "experiments.read",
    },
    "operator": {
        "exports.create",
        "exports.run",
        "exports.retry",
        "exports.diagnostics.read",
        "imports.resume",
        "imports.replay",
        "imports.quality.read",
        "cohorts.refresh",
        "cohorts.activate",
        "cohorts.metrics.read",
        "cohorts.compare.read",
        "workflows.publish",
        "workflows.pause",
        "workflows.resume",
        "workflows.execute",
        "workflows.deliveries.read",
        "activation.callbacks.ingest",
        "experiments.read",
    },
}


@dataclass(frozen=True)
class GovernanceContext:
    actor_role: str
    actor_id: str


def get_governance_context(request: Request) -> GovernanceContext:
    actor_role = str(request.headers.get("x-actor-role") or "admin").strip().lower()
    actor_id = str(request.headers.get("x-actor-id") or actor_role).strip() or actor_role
    if actor_role not in VALID_ROLES:
        raise HTTPException(status_code=400, detail=f"Unsupported actor_role '{actor_role}'.")
    return GovernanceContext(actor_role=actor_role, actor_id=actor_id)


def ensure_permission(context: GovernanceContext, permission: str) -> None:
    allowed = ROLE_PERMISSIONS.get(context.actor_role, set())
    if "*" in allowed or permission in allowed:
        return
    raise HTTPException(status_code=403, detail=f"actor_role '{context.actor_role}' is not allowed to perform '{permission}'.")


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
            "masked_fields": masked_fields,
        }
    return {
        "data": masked_payload,
        "audit_id": audit_id,
        "masked_fields": masked_fields,
    }


def collect_masked_fields(*field_sets: Iterable[str]) -> list[str]:
    combined: set[str] = set()
    for items in field_sets:
        combined.update(str(item) for item in items)
    return sorted(combined)
