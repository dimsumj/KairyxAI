from __future__ import annotations

from datetime import datetime
from typing import Any

from app.application.workflows import WorkflowService


class RecordingWorkflowRepository:
    def __init__(
        self,
        *,
        resources: dict[str, list[dict[str, Any]]] | None = None,
        events: dict[tuple[str, str | None, str | None], list[dict[str, Any]]] | None = None,
        require_scoped_delivery_lookup: bool = False,
    ) -> None:
        self.resources = dict(resources or {})
        self.events = dict(events or {})
        self.require_scoped_delivery_lookup = require_scoped_delivery_lookup
        self.list_resources_calls: list[tuple[str, str | None]] = []
        self.list_resource_events_calls: list[tuple[str, str | None, str | None, int]] = []

    def get_resource(self, resource_type: str, resource_id: str) -> dict[str, Any] | None:
        for item in self.resources.get(resource_type, []):
            if str(item.get("resource_id") or "") == resource_id:
                return item
        return None

    def list_resources(
        self,
        resource_type: str,
        *,
        name: str | None = None,
        tenant_id: str | None = None,
        project_id: str | None = None,
        include_all_tenants: bool = False,
    ) -> list[dict[str, Any]]:
        del tenant_id, project_id, include_all_tenants
        self.list_resources_calls.append((resource_type, name))
        if resource_type == "workflow_delivery" and self.require_scoped_delivery_lookup and not name:
            raise AssertionError("workflow_delivery lookups must be scoped by workflow name")
        items = list(self.resources.get(resource_type, []))
        if name is not None:
            items = [item for item in items if str(item.get("name") or "") == name]
        return items

    def list_resource_events(
        self,
        resource_type: str,
        resource_id: str | None = None,
        *,
        event_type: str | None = None,
        limit: int = 200,
        tenant_id: str | None = None,
        project_id: str | None = None,
        include_all_tenants: bool = False,
    ) -> list[dict[str, Any]]:
        del tenant_id, project_id, include_all_tenants
        self.list_resource_events_calls.append((resource_type, resource_id, event_type, limit))
        return list(self.events.get((resource_type, resource_id, event_type), []))


def _workflow_record(
    workflow_id: str,
    *,
    status: str = "published",
    trigger: dict[str, Any] | None = None,
) -> dict[str, Any]:
    resolved_trigger = dict(trigger or {"type": "daily_schedule", "hour": 10, "minute": 15})
    return {
        "resource_type": "workflow",
        "resource_id": workflow_id,
        "name": workflow_id,
        "status": status,
        "created_at": "2026-03-10T08:00:00",
        "updated_at": "2026-03-10T08:00:00",
        "tenant_id": "tenant_test",
        "project_id": "project_test",
        "payload": {
            "workflow_id": workflow_id,
            "name": workflow_id,
            "status": status,
            "current_version": 1,
            "published_version": 1 if status == "published" else None,
            "trigger": resolved_trigger,
            "policy": {},
            "budget_policy": {},
            "channel_config": {"channel": "push_notification"},
            "definition": {
                "cohort_id": "cohort_1",
                "trigger": resolved_trigger,
                "schedule": resolved_trigger,
                "policy": {},
                "budget_policy": {},
                "channel_config": {"channel": "push_notification"},
            },
        },
    }


def test_run_due_workflows_skips_runtime_summary_scans_for_non_due_workflows() -> None:
    repository = RecordingWorkflowRepository(
        resources={
            "workflow": [
                _workflow_record(
                    "wf_not_due",
                    trigger={"type": "daily_schedule", "hour": 23, "minute": 59},
                )
            ]
        }
    )
    service = WorkflowService(repository)

    payload = service.run_due_workflows(reference_time="2026-03-10T00:00:00", limit_per_workflow=10)

    assert payload["items"] == []
    assert repository.list_resource_events_calls == []


def test_runtime_summary_treats_manual_tests_as_test_history_and_next_run_is_future() -> None:
    repository = RecordingWorkflowRepository(
        resources={"workflow": [_workflow_record("wf_summary")]},
        events={
            (
                "workflow",
                "wf_summary",
                "workflow_execution",
            ): [
                {
                    "payload": {
                        "recorded_at": "2026-03-10T09:45:00",
                        "trigger_type": "manual_test",
                        "sandbox": False,
                        "triggered": 1,
                        "executed": 1,
                        "success": 1,
                        "failures": 0,
                    }
                },
                {
                    "payload": {
                        "recorded_at": "2026-03-10T10:15:00",
                        "trigger_type": "daily_schedule",
                        "sandbox": False,
                        "triggered": 2,
                        "executed": 2,
                        "success": 2,
                        "failures": 0,
                    }
                },
            ]
        },
    )
    service = WorkflowService(repository)

    workflow = service.get_workflow("wf_summary")
    summary = workflow["runtime_summary"]

    assert summary["last_run_at"] == "2026-03-10T10:15:00"
    assert summary["last_test_run_at"] == "2026-03-10T09:45:00"
    assert summary["totals"]["runs"] == 1
    assert summary["totals"]["test_runs"] == 1
    next_run_at = datetime.fromisoformat(summary["next_run_at"])
    assert (next_run_at.hour, next_run_at.minute) == (10, 15)
    assert next_run_at > datetime.utcnow()


def test_list_deliveries_uses_workflow_scoped_lookup() -> None:
    repository = RecordingWorkflowRepository(
        resources={
            "workflow": [_workflow_record("wf_delivery")],
            "workflow_delivery": [
                {
                    "resource_type": "workflow_delivery",
                    "resource_id": "delivery_1",
                    "name": "wf_delivery",
                    "payload": {"delivery_id": "delivery_1", "workflow_id": "wf_delivery"},
                },
                {
                    "resource_type": "workflow_delivery",
                    "resource_id": "delivery_2",
                    "name": "wf_other",
                    "payload": {"delivery_id": "delivery_2", "workflow_id": "wf_other"},
                },
            ],
        },
        require_scoped_delivery_lookup=True,
    )
    service = WorkflowService(repository)

    deliveries = service.list_deliveries("wf_delivery")

    assert [item["delivery_id"] for item in deliveries] == ["delivery_1"]
    assert ("workflow_delivery", "wf_delivery") in repository.list_resources_calls
