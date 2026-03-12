from __future__ import annotations

import hashlib
import uuid
from datetime import datetime, timedelta
from typing import Any, Dict, List

from engagement_executor import EngagementExecutor

from app.application.cohorts import CohortService
from app.application.experiments import ExperimentConfigService
from app.core.errors import MissingDependencyError, ResourceLockedError


class WorkflowService:
    def __init__(self, repository):
        self.repository = repository
        self.cohorts = CohortService(repository)
        self.experiments = ExperimentConfigService(repository)
        self.executor = EngagementExecutor()

    def create_workflow(
        self,
        *,
        name: str,
        cohort_id: str,
        schedule: Dict[str, Any],
        action: Dict[str, Any],
        policy: Dict[str, Any],
        experiment_id: str | None = None,
        requires_confirmation: bool = False,
        budget_policy: Dict[str, Any] | None = None,
        trigger: Dict[str, Any] | None = None,
        channel_config: Dict[str, Any] | None = None,
        steps: List[Dict[str, Any]] | None = None,
    ) -> Dict[str, Any]:
        if self.cohorts.get_cohort(cohort_id) is None:
            raise KeyError(cohort_id)
        workflow_id = f"wf_{uuid.uuid4().hex[:20]}"
        normalized_trigger = self._normalize_trigger(trigger or schedule or {"type": "daily"})
        normalized_channel = dict(channel_config or action or {})
        normalized_policy = self._normalize_policy(policy)
        resolved_budget_policy = dict(budget_policy or normalized_policy.pop("budget_policy", {}) or {})
        payload = {
            "workflow_id": workflow_id,
            "name": name,
            "status": "draft",
            "current_version": 1,
            "published_version": None,
            "trigger": normalized_trigger,
            "policy": normalized_policy,
            "budget_policy": resolved_budget_policy,
            "experiment_id": experiment_id,
            "channel_config": normalized_channel,
            "definition": {
                "cohort_id": cohort_id,
                "schedule": normalized_trigger,
                "action": normalized_channel,
                "policy": normalized_policy,
                "budget_policy": resolved_budget_policy,
                "trigger": normalized_trigger,
                "channel_config": normalized_channel,
                "steps": self._normalize_steps(steps or []),
                "experiment_id": experiment_id,
                "requires_confirmation": bool(requires_confirmation),
                "confirmation_state": None,
            },
        }
        record = self.repository.upsert_resource("workflow", workflow_id, status="draft", name=name, payload=payload)
        self.repository.create_resource_version("workflow", workflow_id, version=1, payload=payload)
        self.repository.record_resource_event("workflow", workflow_id, event_type="workflow_created", payload=payload)
        self.repository.record_action("workflow_created", "workflow", workflow_id, payload)
        return self._to_response(record)

    def update_workflow(self, workflow_id: str, patch: Dict[str, Any]) -> Dict[str, Any]:
        record = self.repository.get_resource("workflow", workflow_id)
        if record is None:
            raise KeyError(workflow_id)
        payload = dict(record.get("payload") or {})
        definition = dict(payload.get("definition") or {})
        if patch.get("cohort_id") and self.cohorts.get_cohort(str(patch["cohort_id"])) is None:
            raise KeyError(str(patch["cohort_id"]))
        if patch.get("name") is not None:
            payload["name"] = patch["name"]
        if patch.get("cohort_id") is not None:
            definition["cohort_id"] = patch["cohort_id"]
        if patch.get("policy") is not None:
            payload["policy"] = self._normalize_policy(patch["policy"])
            definition["policy"] = payload["policy"]
        if patch.get("budget_policy") is not None:
            payload["budget_policy"] = dict(patch["budget_policy"] or {})
            definition["budget_policy"] = payload["budget_policy"]
        trigger_source = patch.get("trigger") if patch.get("trigger") is not None else patch.get("schedule")
        if trigger_source is not None:
            payload["trigger"] = self._normalize_trigger(trigger_source)
            definition["trigger"] = payload["trigger"]
            definition["schedule"] = payload["trigger"]
        channel_source = patch.get("channel_config") if patch.get("channel_config") is not None else patch.get("action")
        if channel_source is not None:
            payload["channel_config"] = dict(channel_source or {})
            definition["channel_config"] = payload["channel_config"]
            definition["action"] = payload["channel_config"]
        if patch.get("experiment_id") is not None:
            payload["experiment_id"] = patch["experiment_id"]
            definition["experiment_id"] = patch["experiment_id"]
        if patch.get("requires_confirmation") is not None:
            definition["requires_confirmation"] = bool(patch["requires_confirmation"])
        if patch.get("steps") is not None:
            definition["steps"] = self._normalize_steps(patch["steps"])
        payload["definition"] = definition
        payload["current_version"] = int(payload.get("current_version") or 1) + 1
        payload["status"] = "draft"
        saved = self.repository.upsert_resource("workflow", workflow_id, status="draft", name=payload.get("name"), payload=payload)
        self.repository.create_resource_version("workflow", workflow_id, version=int(payload["current_version"]), payload=payload)
        self.repository.record_resource_event("workflow", workflow_id, event_type="workflow_updated", payload={"version": payload["current_version"], "patch": patch})
        self.repository.record_action("workflow_updated", "workflow", workflow_id, patch)
        return self._to_response(saved)

    def list_versions(self, workflow_id: str) -> Dict[str, Any]:
        if self.get_workflow(workflow_id) is None:
            raise KeyError(workflow_id)
        return {"workflow_id": workflow_id, "items": self.repository.list_resource_versions("workflow", workflow_id)}

    def confirm_workflow(self, workflow_id: str, *, note: str = "", valid_for_hours: int = 24) -> Dict[str, Any]:
        workflow = self.get_workflow(workflow_id)
        if workflow is None:
            raise KeyError(workflow_id)
        token = f"wfc_{uuid.uuid4().hex[:24]}"
        payload = {
            "workflow_id": workflow_id,
            "confirmation_token": token,
            "note": note,
            "confirmed_at": datetime.utcnow().isoformat(),
            "valid_until": (datetime.utcnow() + timedelta(hours=max(1, int(valid_for_hours)))).isoformat(),
        }
        self.repository.upsert_resource("workflow_confirmation", workflow_id, status="confirmed", name=workflow.get("name"), payload=payload)
        self.repository.record_resource_event("workflow", workflow_id, event_type="workflow_confirmed", payload=payload)
        return payload

    def get_workflow(self, workflow_id: str) -> Dict[str, Any] | None:
        record = self.repository.get_resource("workflow", workflow_id)
        return self._to_response(record) if record else None

    def list_workflows(self) -> List[Dict[str, Any]]:
        return [self._to_response(item) for item in self.repository.list_resources("workflow")]

    def publish_workflow(self, workflow_id: str) -> Dict[str, Any]:
        record = self.repository.get_resource("workflow", workflow_id)
        if record is None:
            raise KeyError(workflow_id)
        payload = dict(record.get("payload") or {})
        self._require_active_cohort(str((payload.get("definition") or {}).get("cohort_id") or ""))
        experiment_id = str(payload.get("experiment_id") or (payload.get("definition") or {}).get("experiment_id") or "").strip()
        if experiment_id:
            self._require_active_experiment(experiment_id)
        preflight = self._build_publish_preflight(payload)
        if not preflight["eligible"]:
            raise ValueError("; ".join(preflight["reasons"]))
        version = int(payload.get("current_version") or 1)
        payload["published_version"] = version
        payload["status"] = "published"
        payload["publish_preflight"] = preflight
        saved = self.repository.upsert_resource("workflow", workflow_id, status="published", name=payload.get("name"), payload=payload)
        self.repository.create_resource_version("workflow", workflow_id, version=version, payload=payload)
        self.repository.record_resource_event("workflow", workflow_id, event_type="workflow_published", payload={"version": version, "preflight": preflight})
        return self._to_response(saved)

    def pause_workflow(self, workflow_id: str) -> Dict[str, Any]:
        return self._set_status(workflow_id, "paused", "workflow_paused")

    def resume_workflow(self, workflow_id: str) -> Dict[str, Any]:
        record = self.repository.get_resource("workflow", workflow_id)
        if record is None:
            raise KeyError(workflow_id)
        payload = dict(record.get("payload") or {})
        self._require_active_cohort(str((payload.get("definition") or {}).get("cohort_id") or ""))
        experiment_id = str(payload.get("experiment_id") or (payload.get("definition") or {}).get("experiment_id") or "").strip()
        if experiment_id:
            self._require_active_experiment(experiment_id)
        preflight = self._build_publish_preflight(payload)
        if not preflight["eligible"]:
            raise ValueError("; ".join(preflight["reasons"]))
        return self._set_status(workflow_id, "published", "workflow_resumed")

    def set_kill_switch(self, enabled: bool) -> Dict[str, Any]:
        payload = {"enabled": bool(enabled), "updated_at": datetime.utcnow().isoformat()}
        record = self.repository.upsert_resource(
            "orchestrator",
            "kill_switch",
            status="enabled" if enabled else "disabled",
            name="kill_switch",
            payload=payload,
        )
        self.repository.record_action("orchestrator_kill_switch_changed", "orchestrator", "kill_switch", payload)
        return record.get("payload") or payload

    def get_kill_switch(self) -> Dict[str, Any]:
        record = self.repository.get_resource("orchestrator", "kill_switch")
        if record is None:
            return {"enabled": False}
        return record.get("payload") or {"enabled": False}

    def list_executions(self, workflow_id: str) -> List[Dict[str, Any]]:
        if self.get_workflow(workflow_id) is None:
            raise KeyError(workflow_id)
        return [item.get("payload") or {} for item in self.repository.list_resource_events("workflow", workflow_id, event_type="workflow_execution", limit=500)]

    def list_deliveries(self, workflow_id: str) -> List[Dict[str, Any]]:
        if self.get_workflow(workflow_id) is None:
            raise KeyError(workflow_id)
        items = []
        for record in self.repository.list_resources("workflow_delivery"):
            payload = record.get("payload") or {}
            if str(payload.get("workflow_id") or "") == workflow_id:
                items.append(payload)
        return items

    def get_policy_counters(self, workflow_id: str) -> Dict[str, Any]:
        if self.get_workflow(workflow_id) is None:
            raise KeyError(workflow_id)
        policy_items = [
            item.get("payload") or {}
            for item in self.repository.list_resources("workflow_policy_state")
            if str((item.get("payload") or {}).get("workflow_id") or "") == workflow_id
        ]
        budget_items = [
            item.get("payload") or {}
            for item in self.repository.list_resources("workflow_budget_state")
            if str((item.get("payload") or {}).get("workflow_id") or "") == workflow_id
        ]
        return {
            "workflow_id": workflow_id,
            "policy_state": policy_items,
            "budget_state": budget_items,
        }

    def ingest_delivery_callback(self, provider: str, callbacks: List[Dict[str, Any]]) -> Dict[str, Any]:
        ingested = 0
        duplicates = 0
        outcomes_ingested = 0
        items = []
        for callback in callbacks:
            event_type = str(
                callback.get("event_type")
                or callback.get("status")
                or callback.get("outcome_name")
                or "delivered"
            ).lower()
            callback_id = self._callback_id(provider, callback, event_type)
            if self.repository.get_resource("provider_callback", callback_id) is not None:
                duplicates += 1
                items.append({"callback_id": callback_id, "status": "duplicate"})
                continue

            delivery = self._find_delivery_for_callback(callback)
            occurred_at = str(callback.get("occurred_at") or datetime.utcnow().isoformat())
            callback_payload = {
                **callback,
                "callback_id": callback_id,
                "provider": provider,
                "event_type": event_type,
                "occurred_at": occurred_at,
            }
            self.repository.upsert_resource(
                "provider_callback",
                callback_id,
                status="ingested",
                name=provider,
                payload=callback_payload,
            )
            self.repository.record_action("provider_callback_ingested", "provider_callback", callback_id, callback_payload)
            ingested += 1

            if delivery is None:
                items.append({"callback_id": callback_id, "status": "unmatched"})
                continue

            delivery_payload = dict(delivery.get("payload") or {})
            delivery_payload["callback_count"] = int(delivery_payload.get("callback_count") or 0) + 1
            delivery_payload["last_callback_at"] = occurred_at
            delivery_payload["last_provider_event"] = event_type
            delivery_payload["provider_callback_status"] = str(callback.get("status") or event_type)
            if event_type in {"opened", "clicked", "returned", "converted"}:
                delivery_payload["delivery_status"] = "converted" if event_type in {"returned", "converted"} else event_type
            elif event_type in {"bounced", "failed", "dropped"}:
                delivery_payload["delivery_status"] = "failed"
                delivery_payload["failure_reason"] = "provider_error"
            self.repository.upsert_resource(
                "workflow_delivery",
                str(delivery_payload.get("delivery_id") or delivery.get("resource_id")),
                status=str(delivery_payload.get("delivery_status") or "delivered"),
                name=delivery_payload.get("workflow_id"),
                payload=delivery_payload,
            )
            workflow_id = str(delivery_payload.get("workflow_id") or "")
            if workflow_id:
                self.repository.record_resource_event(
                    "workflow",
                    workflow_id,
                    event_type="action_delivery_callback",
                    payload={**callback_payload, "workflow_id": workflow_id, "delivery_id": delivery_payload.get("delivery_id")},
                )

            outcome_name = self._callback_outcome_name(event_type, callback)
            experiment_id = delivery_payload.get("experiment_id")
            if experiment_id and outcome_name is not None and not bool(delivery_payload.get("sandbox")):
                self.experiments.record_outcome(
                    str(experiment_id),
                    {
                        "workflow_id": delivery_payload.get("workflow_id"),
                        "cohort_id": delivery_payload.get("cohort_id"),
                        "experiment_id": experiment_id,
                        "user_id": delivery_payload.get("user_id"),
                        "group": delivery_payload.get("group") or "treatment",
                        "action_execution_id": delivery_payload.get("action_execution_id"),
                        "occurred_at": occurred_at,
                        "outcome_name": outcome_name,
                        "source": f"{provider}_callback",
                        "metadata": dict(callback.get("metadata") or {}),
                    },
                )
                outcomes_ingested += 1
            items.append(
                {
                    "callback_id": callback_id,
                    "delivery_id": delivery_payload.get("delivery_id"),
                    "workflow_id": delivery_payload.get("workflow_id"),
                    "outcome_ingested": outcome_name is not None and bool(experiment_id),
                }
            )
        return {
            "provider": provider,
            "ingested": ingested,
            "duplicates": duplicates,
            "outcomes_ingested": outcomes_ingested,
            "items": items,
        }

    def test_run(
        self,
        workflow_id: str,
        *,
        limit: int = 20,
        confirm: bool = False,
        sandbox: bool = True,
        reference_time: str | None = None,
        confirmation_token: str | None = None,
    ) -> Dict[str, Any]:
        workflow = self.get_workflow(workflow_id)
        if workflow is None:
            raise KeyError(workflow_id)
        if workflow["status"] not in {"published", "draft"}:
            raise ValueError("Only draft or published workflows can be executed.")
        return self._execute_workflow(
            workflow,
            limit=max(1, int(limit)),
            confirm=confirm,
            sandbox=bool(sandbox),
            manual_test=True,
            reference_time=self._parse_reference_time(reference_time),
            confirmation_token=confirmation_token,
        )

    def run_due_workflows(
        self,
        *,
        reference_time: str | None = None,
        limit_per_workflow: int = 100,
        confirmation_tokens: Dict[str, str] | None = None,
    ) -> Dict[str, Any]:
        if self.get_kill_switch().get("enabled"):
            raise ValueError("Kill switch is enabled.")
        resolved_time = self._parse_reference_time(reference_time)
        action_date = resolved_time.date().isoformat()
        tokens = dict(confirmation_tokens or {})
        runs = []
        for workflow in self.list_workflows():
            if workflow.get("status") != "published":
                continue
            trigger = workflow.get("trigger") or {}
            if str(trigger.get("type") or "") != "daily_schedule":
                continue
            scheduled_hour = int(trigger.get("hour") or 0)
            scheduled_minute = int(trigger.get("minute") or 0)
            if (resolved_time.hour, resolved_time.minute) < (scheduled_hour, scheduled_minute):
                continue
            if self._already_executed_for_date(workflow["workflow_id"], action_date):
                continue
            runs.append(
                self._execute_workflow(
                    workflow,
                    limit=max(1, int(limit_per_workflow)),
                    confirm=True,
                    sandbox=False,
                    manual_test=False,
                    reference_time=resolved_time,
                    confirmation_token=tokens.get(workflow["workflow_id"]),
                )
            )
        return {"reference_time": resolved_time.isoformat(), "items": runs}

    def ingest_event(
        self,
        *,
        event_type: str,
        user_ids: List[str],
        payload: Dict[str, Any] | None = None,
        reference_time: str | None = None,
        confirmation_tokens: Dict[str, str] | None = None,
    ) -> Dict[str, Any]:
        if self.get_kill_switch().get("enabled"):
            raise ValueError("Kill switch is enabled.")
        resolved_time = self._parse_reference_time(reference_time)
        tokens = dict(confirmation_tokens or {})
        results = []
        for workflow in self.list_workflows():
            trigger = workflow.get("trigger") or {}
            if workflow.get("status") != "published" or str(trigger.get("type") or "") != "event_trigger":
                continue
            if str(trigger.get("event_type") or "") != str(event_type):
                continue
            members = self._filter_members_for_user_ids((workflow.get("definition") or {}).get("cohort_id"), user_ids)
            if not members:
                continue
            self.repository.record_resource_event(
                "workflow",
                workflow["workflow_id"],
                event_type="workflow_trigger_event",
                payload={"trigger_type": "event_trigger", "event_type": event_type, "user_ids": user_ids, "payload": payload or {}, "recorded_at": resolved_time.isoformat()},
            )
            results.append(
                self._execute_workflow(
                    workflow,
                    limit=len(members),
                    confirm=True,
                    sandbox=False,
                    manual_test=False,
                    reference_time=resolved_time,
                    members_override=members,
                    trigger_type="event_trigger",
                    confirmation_token=tokens.get(workflow["workflow_id"]),
                )
            )
        return {"event_type": event_type, "reference_time": resolved_time.isoformat(), "items": results}

    def evaluate_thresholds(
        self,
        *,
        metric_id: str,
        value: float,
        reference_time: str | None = None,
        confirmation_tokens: Dict[str, str] | None = None,
    ) -> Dict[str, Any]:
        if self.get_kill_switch().get("enabled"):
            raise ValueError("Kill switch is enabled.")
        resolved_time = self._parse_reference_time(reference_time)
        tokens = dict(confirmation_tokens or {})
        results = []
        for workflow in self.list_workflows():
            trigger = workflow.get("trigger") or {}
            if workflow.get("status") != "published" or str(trigger.get("type") or "") != "threshold_trigger":
                continue
            if str(trigger.get("metric_id") or "") != str(metric_id):
                continue
            if not self._threshold_matches(float(value), str(trigger.get("operator") or ">="), float(trigger.get("threshold") or 0.0)):
                continue
            self.repository.record_resource_event(
                "workflow",
                workflow["workflow_id"],
                event_type="workflow_trigger_event",
                payload={"trigger_type": "threshold_trigger", "metric_id": metric_id, "value": value, "recorded_at": resolved_time.isoformat()},
            )
            results.append(
                self._execute_workflow(
                    workflow,
                    limit=100,
                    confirm=True,
                    sandbox=False,
                    manual_test=False,
                    reference_time=resolved_time,
                    trigger_type="threshold_trigger",
                    confirmation_token=tokens.get(workflow["workflow_id"]),
                )
            )
        return {"metric_id": metric_id, "value": value, "reference_time": resolved_time.isoformat(), "items": results}

    def _set_status(self, workflow_id: str, status: str, event_type: str) -> Dict[str, Any]:
        record = self.repository.get_resource("workflow", workflow_id)
        if record is None:
            raise KeyError(workflow_id)
        payload = dict(record.get("payload") or {})
        payload["status"] = status
        saved = self.repository.upsert_resource("workflow", workflow_id, status=status, name=payload.get("name"), payload=payload)
        self.repository.record_resource_event("workflow", workflow_id, event_type=event_type, payload={"status": status})
        return self._to_response(saved)

    def _to_response(self, record: Dict[str, Any]) -> Dict[str, Any]:
        payload = dict(record.get("payload") or {})
        definition = payload.get("definition") or {}
        payload.setdefault("trigger", definition.get("trigger") or definition.get("schedule") or {"type": "daily_schedule"})
        payload.setdefault("policy", definition.get("policy") or {})
        payload.setdefault("budget_policy", definition.get("budget_policy") or {})
        payload.setdefault("experiment_id", definition.get("experiment_id"))
        payload.setdefault("channel_config", definition.get("channel_config") or definition.get("action") or {})
        payload.setdefault("created_at", record["created_at"])
        payload.setdefault("updated_at", record["updated_at"])
        return payload

    def _normalize_trigger(self, trigger: Dict[str, Any]) -> Dict[str, Any]:
        raw_type = str((trigger or {}).get("type") or "daily").lower()
        if raw_type == "daily":
            raw_type = "daily_schedule"
        if raw_type not in {"daily_schedule", "manual_test", "event_trigger", "threshold_trigger"}:
            raise ValueError("Supported triggers are daily_schedule, manual_test, event_trigger, and threshold_trigger.")
        payload = {
            "type": raw_type,
            "hour": int((trigger or {}).get("hour") or 0),
            "minute": int((trigger or {}).get("minute") or 0),
        }
        if raw_type == "event_trigger":
            payload["event_type"] = str((trigger or {}).get("event_type") or "").strip()
        if raw_type == "threshold_trigger":
            payload["metric_id"] = str((trigger or {}).get("metric_id") or "").strip()
            payload["operator"] = str((trigger or {}).get("operator") or ">=").strip()
            payload["threshold"] = float((trigger or {}).get("threshold") or 0.0)
        return payload

    def _normalize_policy(self, policy: Dict[str, Any]) -> Dict[str, Any]:
        payload = dict(policy or {})
        payload.setdefault("global_daily_limit", 3)
        payload.setdefault("channel_daily_limit", 2)
        payload.setdefault("cooldown_hours", 24)
        payload.setdefault("blacklist_ids", [])
        payload.setdefault("quiet_hours", {"start": 22, "end": 7})
        return payload

    def _normalize_steps(self, steps: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        if not steps:
            return []
        normalized = []
        for step in steps:
            step_type = str((step or {}).get("type") or "action").strip().lower()
            if step_type not in {"filter", "wait", "if_else", "action", "end"}:
                raise ValueError("Supported workflow step types are filter, wait, if_else, action, and end.")
            payload = {"type": step_type}
            if step_type == "filter":
                payload["conditions"] = list((step or {}).get("conditions") or [((step or {}).get("condition") or {})])
            elif step_type == "wait":
                payload["seconds"] = max(0, int((step or {}).get("seconds") or 0))
            elif step_type == "if_else":
                payload["condition"] = dict((step or {}).get("condition") or {})
                payload["then"] = dict((step or {}).get("then") or {})
                payload["else"] = dict((step or {}).get("else") or {})
            elif step_type == "action":
                payload["action"] = dict((step or {}).get("action") or {})
            normalized.append(payload)
        return normalized

    @staticmethod
    def _lookup_member_value(member: Dict[str, Any], field: str) -> Any:
        if field in member:
            return member.get(field)
        attributes = member.get("attributes")
        if isinstance(attributes, dict) and field in attributes:
            return attributes.get(field)
        for container_name in ("event_properties", "user_properties"):
            container = member.get(container_name)
            if isinstance(container, dict) and field in container:
                return container.get(field)
        return None

    def _condition_matches(self, member: Dict[str, Any], condition: Dict[str, Any], *, group: str | None = None) -> bool:
        if not condition:
            return True
        field = str(condition.get("field") or "")
        actual = group if field == "group" else self._lookup_member_value(member, field)
        op = str(condition.get("op") or "=").lower()
        expected = condition.get("value")
        if op in {"=", "=="}:
            return actual == expected
        if op == "!=":
            return actual != expected
        if op == "in":
            return actual in (expected or [])
        if op == "not in":
            return actual not in (expected or [])
        if op == "contains":
            return str(expected) in str(actual or "")
        try:
            actual_number = float(actual)
            expected_number = float(expected)
        except (TypeError, ValueError):
            return False
        if op == ">":
            return actual_number > expected_number
        if op == ">=":
            return actual_number >= expected_number
        if op == "<":
            return actual_number < expected_number
        if op == "<=":
            return actual_number <= expected_number
        return actual == expected

    def _resolve_channel_config_from_steps(
        self,
        *,
        member: Dict[str, Any],
        base_channel_config: Dict[str, Any],
        steps: List[Dict[str, Any]],
        group: str | None,
    ) -> Dict[str, Any]:
        resolved = dict(base_channel_config or {})
        trace: List[Dict[str, Any]] = []
        if not steps:
            return {"status": "ok", "channel_config": resolved, "trace": trace}
        for index, step in enumerate(steps, start=1):
            step_type = str(step.get("type") or "action")
            if step_type == "filter":
                conditions = list(step.get("conditions") or [])
                passed = all(self._condition_matches(member, condition, group=group) for condition in conditions)
                trace.append({"step": index, "type": step_type, "passed": passed, "conditions": conditions})
                if not passed:
                    return {"status": "filtered_out", "channel_config": resolved, "trace": trace}
                continue
            if step_type == "wait":
                trace.append({"step": index, "type": step_type, "seconds": int(step.get("seconds") or 0), "mode": "simulated"})
                continue
            if step_type == "if_else":
                branch = "then" if self._condition_matches(member, step.get("condition") or {}, group=group) else "else"
                branch_payload = dict(step.get(branch) or {})
                trace.append({"step": index, "type": step_type, "branch": branch, "condition": step.get("condition") or {}})
                if branch_payload.get("end") is True:
                    return {"status": "ended", "channel_config": resolved, "trace": trace}
                if isinstance(branch_payload.get("action"), dict):
                    resolved.update(branch_payload["action"])
                continue
            if step_type == "action":
                action_payload = dict(step.get("action") or {})
                resolved.update(action_payload)
                trace.append({"step": index, "type": step_type, "action": action_payload})
                continue
            if step_type == "end":
                trace.append({"step": index, "type": step_type, "ended": True})
                return {"status": "ended", "channel_config": resolved, "trace": trace}
        return {"status": "ok", "channel_config": resolved, "trace": trace}

    def _build_publish_preflight(self, workflow: Dict[str, Any]) -> Dict[str, Any]:
        reasons: List[str] = []
        definition = workflow.get("definition") or {}
        cohort = self.cohorts.get_cohort(definition.get("cohort_id"))
        if cohort is None:
            reasons.append("cohort_not_found")
        elif cohort.get("status") != "active":
            reasons.append("cohort_not_active")
        channel_config = workflow.get("channel_config") or definition.get("channel_config") or definition.get("action") or {}
        steps = list(definition.get("steps") or [])
        step_actions = [dict(step.get("action") or {}) for step in steps if str(step.get("type") or "") == "action"]
        branch_actions = []
        for step in steps:
            if str(step.get("type") or "") != "if_else":
                continue
            for branch_name in ("then", "else"):
                branch_payload = dict(step.get(branch_name) or {})
                if isinstance(branch_payload.get("action"), dict):
                    branch_actions.append(dict(branch_payload["action"]))
        has_step_channel = any(str(item.get("channel") or "").strip() for item in step_actions + branch_actions)
        if not channel_config.get("channel") and not has_step_channel:
            reasons.append("channel_missing")
        has_step_content = any(str(item.get("content") or "").strip() for item in step_actions + branch_actions)
        if not str(channel_config.get("content") or "").strip() and not has_step_content:
            reasons.append("content_missing")
        trigger = workflow.get("trigger") or definition.get("trigger") or definition.get("schedule") or {}
        trigger_type = str(trigger.get("type") or "")
        if trigger_type not in {"daily_schedule", "manual_test", "event_trigger", "threshold_trigger"}:
            reasons.append("unsupported_trigger")
        if trigger_type == "event_trigger" and not str(trigger.get("event_type") or "").strip():
            reasons.append("event_type_missing")
        if trigger_type == "threshold_trigger":
            if not str(trigger.get("metric_id") or "").strip():
                reasons.append("metric_id_missing")
            if str(trigger.get("operator") or "") not in {">", ">=", "<", "<=", "=="}:
                reasons.append("threshold_operator_invalid")
        if not workflow.get("experiment_id") and not definition.get("experiment_id"):
            reasons.append("experiment_missing")
        else:
            experiment_id = str(workflow.get("experiment_id") or definition.get("experiment_id") or "")
            experiment = self.repository.get_resource("experiment", experiment_id)
            if experiment is None:
                reasons.append("experiment_not_found")
            elif str(((experiment.get("payload") or {}).get("status") or experiment.get("status") or "")).lower() != "active":
                reasons.append("experiment_not_active")
        policy = workflow.get("policy") or definition.get("policy") or {}
        for field in ("global_daily_limit", "channel_daily_limit", "cooldown_hours"):
            if int(policy.get(field) or 0) < 0:
                reasons.append(f"invalid_{field}")
        if steps:
            has_action_step = any(str(step.get("type") or "") == "action" for step in steps)
            has_branch_action = any(str(step.get("type") or "") == "if_else" and any(isinstance((step.get(branch) or {}).get("action"), dict) for branch in ("then", "else")) for step in steps)
            if not has_action_step and not has_branch_action:
                reasons.append("workflow_steps_missing_action")
        return {"eligible": not reasons, "reasons": reasons}

    def _parse_reference_time(self, reference_time: str | None) -> datetime:
        if not reference_time:
            return datetime.utcnow()
        try:
            return datetime.fromisoformat(str(reference_time))
        except ValueError:
            return datetime.utcnow()

    def _filter_members_for_user_ids(self, cohort_id: str | None, user_ids: List[str]) -> List[Dict[str, Any]]:
        if not cohort_id:
            return []
        selected = {str(item) for item in user_ids if str(item).strip()}
        members = self.cohorts.list_members(cohort_id, page=1, page_size=1000)["items"]
        if not selected:
            return members
        return [member for member in members if str(member.get("canonical_user_id") or "") in selected]

    @staticmethod
    def _threshold_matches(value: float, operator: str, threshold: float) -> bool:
        if operator == ">":
            return value > threshold
        if operator == ">=":
            return value >= threshold
        if operator == "<":
            return value < threshold
        if operator == "<=":
            return value <= threshold
        return value == threshold

    def _already_executed_for_date(self, workflow_id: str, action_date: str) -> bool:
        for event in self.repository.list_resource_events("workflow", workflow_id, event_type="workflow_execution", limit=500):
            payload = event.get("payload") or {}
            if payload.get("action_date") == action_date and payload.get("trigger_type") == "daily_schedule":
                return True
        return False

    def _list_policy_state_for_user(self, user_id: str, action_date: str) -> List[Dict[str, Any]]:
        items = []
        for resource in self.repository.list_resources("workflow_policy_state"):
            payload = resource.get("payload") or {}
            if str(payload.get("user_id") or "") == user_id and str(payload.get("action_date") or "") == action_date:
                items.append(payload)
        return items

    def _get_budget_state(self, workflow_id: str, action_date: str) -> Dict[str, Any]:
        resource_id = f"{workflow_id}:{action_date}"
        record = self.repository.get_resource("workflow_budget_state", resource_id)
        if record is None:
            return {
                "resource_id": resource_id,
                "workflow_id": workflow_id,
                "action_date": action_date,
                "consumed": 0,
                "blocked": 0,
            }
        return record.get("payload") or {}

    def _upsert_policy_state(
        self,
        workflow_id: str,
        user_id: str,
        channel: str,
        action_date: str,
        *,
        delivered: bool = False,
        blocked_reason: str | None = None,
        duplicate_suppressed: bool = False,
        last_delivery_at: str | None = None,
    ) -> Dict[str, Any]:
        resource_id = f"{workflow_id}:{action_date}:{user_id}:{channel}"
        record = self.repository.get_resource("workflow_policy_state", resource_id)
        payload = dict((record or {}).get("payload") or {})
        payload.setdefault("workflow_id", workflow_id)
        payload.setdefault("user_id", user_id)
        payload.setdefault("channel", channel)
        payload.setdefault("action_date", action_date)
        payload["attempts"] = int(payload.get("attempts") or 0) + 1
        payload["deliveries"] = int(payload.get("deliveries") or 0) + (1 if delivered else 0)
        payload["policy_blocked"] = int(payload.get("policy_blocked") or 0) + (1 if blocked_reason == "policy_blocked" else 0)
        payload["budget_exhausted"] = int(payload.get("budget_exhausted") or 0) + (1 if blocked_reason == "budget_exhausted" else 0)
        payload["duplicate_suppressed"] = int(payload.get("duplicate_suppressed") or 0) + (1 if duplicate_suppressed else 0)
        payload["invalid_target"] = int(payload.get("invalid_target") or 0) + (1 if blocked_reason == "invalid_target" else 0)
        payload["last_decision"] = blocked_reason or ("delivered" if delivered else payload.get("last_decision"))
        if last_delivery_at:
            payload["last_delivery_at"] = last_delivery_at
        return self.repository.upsert_resource("workflow_policy_state", resource_id, status="active", name=workflow_id, payload=payload)

    def _upsert_budget_state(self, workflow_id: str, action_date: str, *, consumed: bool = False, blocked: bool = False, max_deliveries: int | None = None) -> Dict[str, Any]:
        resource_id = f"{workflow_id}:{action_date}"
        current = self._get_budget_state(workflow_id, action_date)
        current["consumed"] = int(current.get("consumed") or 0) + (1 if consumed else 0)
        current["blocked"] = int(current.get("blocked") or 0) + (1 if blocked else 0)
        if max_deliveries is not None:
            current["max_deliveries"] = int(max_deliveries)
        return self.repository.upsert_resource("workflow_budget_state", resource_id, status="active", name=workflow_id, payload=current)

    def _idempotency_key(self, workflow_id: str, snapshot_id: str, user_id: str, action_date: str) -> str:
        digest = hashlib.sha256(f"{workflow_id}:{snapshot_id}:{user_id}:{action_date}".encode("utf-8")).hexdigest()
        return f"{workflow_id}:{digest[:24]}"

    def _idempotency_exists(self, key: str) -> bool:
        return self.repository.get_resource("workflow_idempotency", key) is not None

    def _record_idempotency(self, key: str, payload: Dict[str, Any]) -> None:
        self.repository.upsert_resource("workflow_idempotency", key, status="recorded", name=payload.get("workflow_id"), payload=payload)

    def _persist_delivery(
        self,
        *,
        workflow_id: str,
        cohort_id: str | None,
        experiment_id: str | None,
        execution_payload: Dict[str, Any],
        channel_config: Dict[str, Any],
        provider_result: Dict[str, Any],
        sandbox: bool,
    ) -> Dict[str, Any]:
        delivery_id = str(provider_result.get("action_id") or execution_payload.get("action_execution_id") or f"delivery_{uuid.uuid4().hex[:16]}")
        payload = {
            "delivery_id": delivery_id,
            "action_execution_id": execution_payload.get("action_execution_id") or delivery_id,
            "workflow_id": workflow_id,
            "execution_id": execution_payload.get("execution_id"),
            "workflow_version": execution_payload.get("workflow_version"),
            "cohort_id": cohort_id,
            "cohort_snapshot_id": execution_payload.get("cohort_snapshot_id"),
            "experiment_id": experiment_id,
            "user_id": execution_payload.get("user_id"),
            "group": execution_payload.get("group"),
            "channel": execution_payload.get("channel"),
            "provider": provider_result.get("provider") or channel_config.get("provider") or execution_payload.get("channel"),
            "delivery_status": "delivered" if provider_result.get("ok") else "failed",
            "failure_reason": provider_result.get("error"),
            "provider_request": {
                "channel": channel_config.get("channel"),
                "subject": channel_config.get("subject"),
                "content": channel_config.get("content"),
            },
            "provider_response": {
                "status_code": provider_result.get("status_code"),
                "error": provider_result.get("error"),
            },
            "delivery_diagnostics": {
                "attempt_count": provider_result.get("attempt_count", 1),
                "attempts": provider_result.get("attempts", []),
                "retry_schedule_seconds": provider_result.get("retry_schedule_seconds", []),
                "failure_classification": provider_result.get("failure_classification"),
            },
            "callback_count": 0,
            "sandbox": bool(sandbox),
            "recorded_at": datetime.utcnow().isoformat(),
        }
        self.repository.upsert_resource(
            "workflow_delivery",
            delivery_id,
            status=payload["delivery_status"],
            name=workflow_id,
            payload=payload,
        )
        return payload

    def _callback_id(self, provider: str, callback: Dict[str, Any], event_type: str) -> str:
        parts = [
            str(provider),
            str(callback.get("event_id") or callback.get("message_id") or callback.get("delivery_id") or callback.get("action_execution_id") or callback.get("user_id") or "unknown"),
            str(event_type),
            str(callback.get("occurred_at") or ""),
        ]
        digest = hashlib.sha256(":".join(parts).encode("utf-8")).hexdigest()
        return f"cb_{digest[:24]}"

    def _find_delivery_for_callback(self, callback: Dict[str, Any]) -> Dict[str, Any] | None:
        delivery_id = str(callback.get("delivery_id") or callback.get("action_execution_id") or "").strip()
        if delivery_id:
            record = self.repository.get_resource("workflow_delivery", delivery_id)
            if record is not None:
                return record
        workflow_id = str(callback.get("workflow_id") or "").strip()
        user_id = str(callback.get("user_id") or "").strip()
        for record in self.repository.list_resources("workflow_delivery"):
            payload = record.get("payload") or {}
            if workflow_id and str(payload.get("workflow_id") or "") != workflow_id:
                continue
            if user_id and str(payload.get("user_id") or "") != user_id:
                continue
            if workflow_id or user_id:
                return record
        return None

    def _callback_outcome_name(self, event_type: str, callback: Dict[str, Any]) -> str | None:
        if callback.get("outcome_name"):
            return str(callback["outcome_name"]).lower()
        mapping = {
            "opened": "opened",
            "clicked": "engaged",
            "engaged": "engaged",
            "returned": "returned",
            "returned_to_game": "returned",
            "converted": "returned",
            "purchase": "returned",
        }
        return mapping.get(str(event_type).lower())

    def _validate_confirmation(self, workflow_id: str, confirmation_token: str | None) -> None:
        record = self.repository.get_resource("workflow_confirmation", workflow_id)
        if record is None:
            raise ValueError("Workflow requires confirmation before execution.")
        payload = record.get("payload") or {}
        valid_until = str(payload.get("valid_until") or "")
        try:
            valid_until_dt = datetime.fromisoformat(valid_until)
        except ValueError:
            valid_until_dt = None
        if valid_until_dt is None or valid_until_dt < datetime.utcnow():
            raise ValueError("Workflow confirmation has expired.")
        if str(payload.get("confirmation_token") or "") != str(confirmation_token or ""):
            raise ValueError("Valid confirmation token is required for workflow execution.")

    @staticmethod
    def _classify_provider_failure(provider_result: Dict[str, Any]) -> str:
        error = str(provider_result.get("error") or "").lower()
        if "unsupported_channel" in error:
            return "internal_error"
        if "timeout" in error:
            return "provider_error"
        return "provider_error"

    def _execute_action_with_retry(self, action_payload: Dict[str, Any], channel_config: Dict[str, Any]) -> Dict[str, Any]:
        retry_policy = dict(channel_config.get("retry_policy") or {})
        max_retries = max(0, int(retry_policy.get("max_retries") or 0))
        base_backoff_seconds = max(1, int(retry_policy.get("base_backoff_seconds") or 1))
        attempts: List[Dict[str, Any]] = []
        final_result: Dict[str, Any] | None = None
        for attempt in range(max_retries + 1):
            result = self.executor.execute_action_detailed(action_payload)
            attempts.append(
                {
                    "attempt": attempt + 1,
                    "status_code": result.get("status_code"),
                    "ok": bool(result.get("ok")),
                    "error": result.get("error"),
                    "backoff_seconds": 0 if result.get("ok") else (base_backoff_seconds * (2**attempt) if attempt < max_retries else 0),
                }
            )
            final_result = result
            if result.get("ok"):
                break
        resolved = dict(final_result or {})
        resolved["attempt_count"] = len(attempts)
        resolved["attempts"] = attempts
        resolved["failure_classification"] = None if resolved.get("ok") else self._classify_provider_failure(resolved)
        resolved["retry_schedule_seconds"] = [item["backoff_seconds"] for item in attempts[:-1] if item["backoff_seconds"] > 0]
        return resolved

    def _evaluate_policy(
        self,
        workflow_id: str,
        member: Dict[str, Any],
        channel_config: Dict[str, Any],
        policy: Dict[str, Any],
        budget_policy: Dict[str, Any],
        *,
        action_date: str,
        reference_time: datetime,
        snapshot_id: str,
        manual_test: bool,
    ) -> Dict[str, Any]:
        user_id = str(member.get("canonical_user_id") or "")
        channel = str(channel_config.get("channel") or "push_notification")
        if not user_id:
            return {"allowed": False, "reason": "invalid_target", "idempotency_key": None}
        key = self._idempotency_key(workflow_id, snapshot_id, user_id, action_date)
        if manual_test:
            return {"allowed": True, "reason": None, "idempotency_key": key}

        blacklist = {str(item) for item in policy.get("blacklist_ids") or []}
        if user_id in blacklist:
            return {"allowed": False, "reason": "policy_blocked", "idempotency_key": key}

        quiet = policy.get("quiet_hours") or {}
        start_hour = int(quiet.get("start", 22))
        end_hour = int(quiet.get("end", 7))
        current_hour = reference_time.hour
        if current_hour >= start_hour or current_hour < end_hour:
            return {"allowed": False, "reason": "policy_blocked", "idempotency_key": key}

        if self._idempotency_exists(key):
            return {"allowed": False, "reason": "duplicate_suppressed", "idempotency_key": key}

        user_states = self._list_policy_state_for_user(user_id, action_date)
        global_deliveries = sum(int(item.get("deliveries") or 0) for item in user_states)
        channel_deliveries = sum(int(item.get("deliveries") or 0) for item in user_states if str(item.get("channel") or "") == channel)
        workflow_states = [item for item in user_states if str(item.get("workflow_id") or "") == workflow_id]
        last_delivery_at = None
        for item in workflow_states:
            if item.get("last_delivery_at"):
                try:
                    parsed = datetime.fromisoformat(str(item["last_delivery_at"]))
                except Exception:
                    parsed = None
                if parsed is not None and (last_delivery_at is None or parsed > last_delivery_at):
                    last_delivery_at = parsed

        if global_deliveries >= int(policy.get("global_daily_limit") or 0):
            return {"allowed": False, "reason": "policy_blocked", "idempotency_key": key}
        if channel_deliveries >= int(policy.get("channel_daily_limit") or 0):
            return {"allowed": False, "reason": "policy_blocked", "idempotency_key": key}
        if last_delivery_at is not None and last_delivery_at >= reference_time - timedelta(hours=int(policy.get("cooldown_hours") or 0)):
            return {"allowed": False, "reason": "policy_blocked", "idempotency_key": key}

        max_deliveries = int(budget_policy.get("daily_delivery_limit") or 0)
        if max_deliveries > 0:
            budget_state = self._get_budget_state(workflow_id, action_date)
            if int(budget_state.get("consumed") or 0) >= max_deliveries:
                return {"allowed": False, "reason": "budget_exhausted", "idempotency_key": key}

        return {"allowed": True, "reason": None, "idempotency_key": key}

    def _execute_workflow(
        self,
        workflow: Dict[str, Any],
        *,
        limit: int,
        confirm: bool,
        sandbox: bool,
        manual_test: bool,
        reference_time: datetime,
        members_override: List[Dict[str, Any]] | None = None,
        trigger_type: str | None = None,
        confirmation_token: str | None = None,
    ) -> Dict[str, Any]:
        if self.get_kill_switch().get("enabled"):
            raise ValueError("Kill switch is enabled.")

        definition = workflow.get("definition") or {}
        if definition.get("requires_confirmation"):
            if not confirm:
                raise ValueError("Workflow requires confirmation before execution.")
            self._validate_confirmation(workflow["workflow_id"], confirmation_token)

        cohort = self._require_active_cohort(str(definition["cohort_id"]))
        experiment_id = workflow.get("experiment_id") or definition.get("experiment_id")
        if experiment_id and not bool(sandbox):
            self._require_active_experiment(str(experiment_id))
        if not manual_test and cohort.get("refresh_mode") == "daily":
            last_refreshed_at = cohort.get("last_refreshed_at")
            if not last_refreshed_at or last_refreshed_at[:10] < reference_time.date().isoformat():
                cohort = self.cohorts.refresh_cohort(definition["cohort_id"], force=True)

        members = list(members_override or self.cohorts.list_members(definition["cohort_id"], page=1, page_size=max(1, int(limit)))["items"])
        execution_id = f"run_{uuid.uuid4().hex[:20]}"
        snapshot_id = str((cohort or {}).get("latest_snapshot_id") or "snapshot_missing")
        action_date = reference_time.date().isoformat()
        summary = {
            "execution_id": execution_id,
            "workflow_id": workflow["workflow_id"],
            "workflow_version": workflow.get("published_version") or workflow.get("current_version") or 1,
            "sandbox": bool(sandbox),
            "trigger_type": trigger_type or ("manual_test" if manual_test else "daily_schedule"),
            "action_date": action_date,
            "cohort_snapshot_id": snapshot_id,
            "triggered": len(members),
            "executed": 0,
            "success": 0,
            "holdout": 0,
            "filtered_out": 0,
            "ended": 0,
            "policy_blocked": 0,
            "duplicate_suppressed": 0,
            "budget_exhausted": 0,
            "invalid_target": 0,
            "failures": 0,
            "results": [],
            "recorded_at": datetime.utcnow().isoformat(),
        }

        policy = workflow.get("policy") or definition.get("policy") or {}
        budget_policy = workflow.get("budget_policy") or definition.get("budget_policy") or {}
        channel_config = workflow.get("channel_config") or definition.get("channel_config") or definition.get("action") or {}
        workflow_steps = list(definition.get("steps") or [])
        experiment_id = workflow.get("experiment_id") or definition.get("experiment_id")

        for member in members:
            assignment = None
            group = None
            if experiment_id and not manual_test:
                assignment = self.experiments.assign_user(experiment_id, member.get("canonical_user_id"))
                group = assignment["group"]

            step_resolution = self._resolve_channel_config_from_steps(
                member=member,
                base_channel_config=channel_config,
                steps=workflow_steps,
                group=group,
            )
            resolved_channel_config = dict(step_resolution.get("channel_config") or channel_config)
            policy_result = self._evaluate_policy(
                workflow["workflow_id"],
                member,
                resolved_channel_config,
                policy,
                budget_policy,
                action_date=action_date,
                reference_time=reference_time,
                snapshot_id=snapshot_id,
                manual_test=manual_test,
            )

            execution_payload = {
                "execution_id": execution_id,
                "workflow_id": workflow["workflow_id"],
                "workflow_version": summary["workflow_version"],
                "cohort_id": definition.get("cohort_id"),
                "cohort_snapshot_id": snapshot_id,
                "user_id": member.get("canonical_user_id"),
                "channel": resolved_channel_config.get("channel", channel_config.get("channel", "push_notification")),
                "execution_status": "pending",
                "group": group,
                "sandbox": bool(sandbox),
                "trigger_type": summary["trigger_type"],
                "recorded_at": datetime.utcnow().isoformat(),
                "step_trace": step_resolution.get("trace") or [],
            }

            if step_resolution["status"] == "filtered_out":
                execution_payload["execution_status"] = "filtered_out"
                summary["filtered_out"] += 1
                self.repository.record_resource_event("workflow", workflow["workflow_id"], event_type="action_execution", payload=execution_payload)
                summary["results"].append(execution_payload)
                continue
            if step_resolution["status"] == "ended":
                execution_payload["execution_status"] = "ended"
                summary["ended"] += 1
                self.repository.record_resource_event("workflow", workflow["workflow_id"], event_type="action_execution", payload=execution_payload)
                summary["results"].append(execution_payload)
                continue

            if not policy_result["allowed"]:
                reason = str(policy_result["reason"] or "policy_blocked")
                execution_payload["execution_status"] = reason
                execution_payload["reason"] = reason
                if reason == "policy_blocked":
                    summary["policy_blocked"] += 1
                elif reason == "duplicate_suppressed":
                    summary["duplicate_suppressed"] += 1
                elif reason == "budget_exhausted":
                    summary["budget_exhausted"] += 1
                elif reason == "invalid_target":
                    summary["invalid_target"] += 1
                self.repository.record_resource_event("workflow", workflow["workflow_id"], event_type="action_policy_log", payload=execution_payload)
                if not manual_test and execution_payload.get("user_id"):
                    self._upsert_policy_state(
                        workflow["workflow_id"],
                        execution_payload["user_id"],
                        execution_payload["channel"],
                        action_date,
                        blocked_reason=reason,
                        duplicate_suppressed=(reason == "duplicate_suppressed"),
                    )
                    if reason == "budget_exhausted":
                        self._upsert_budget_state(
                            workflow["workflow_id"],
                            action_date,
                            blocked=True,
                            max_deliveries=int(budget_policy.get("daily_delivery_limit") or 0) or None,
                        )
                summary["results"].append(execution_payload)
                continue

            if group == "holdout" and not manual_test:
                execution_payload["execution_status"] = "holdout"
                summary["holdout"] += 1
                self.repository.record_resource_event("workflow", workflow["workflow_id"], event_type="action_execution", payload=execution_payload)
                self.experiments.record_exposure(
                    experiment_id,
                    {
                        **execution_payload,
                        "experiment_id": experiment_id,
                        "action_execution_id": None,
                        "exposed_at": datetime.utcnow().isoformat(),
                    },
                )
                self._record_idempotency(
                    policy_result["idempotency_key"],
                    {
                        "workflow_id": workflow["workflow_id"],
                        "cohort_snapshot_id": snapshot_id,
                        "user_id": execution_payload["user_id"],
                        "action_date": action_date,
                        "group": "holdout",
                    },
                )
                summary["results"].append(execution_payload)
                continue

            action = dict(resolved_channel_config)
            execution_payload["channel"] = action.get("channel", execution_payload["channel"])
            recipient = member.get("email") if str(action.get("channel") or "") == "email" else member.get("canonical_user_id")
            action_payload = {
                "decision": "ACT",
                "channel": action.get("channel", "push_notification"),
                "content": action.get("content", ""),
                "subject": action.get("subject", "KairyxAI"),
                "player_id": recipient or member.get("canonical_user_id"),
            }
            provider_result = self._execute_action_with_retry(action_payload, action)
            summary["executed"] += 1
            execution_payload["execution_status"] = "executed" if provider_result.get("ok") else "failed"
            execution_payload["action_execution_id"] = provider_result.get("action_id") or execution_id
            self.repository.record_resource_event("workflow", workflow["workflow_id"], event_type="action_execution", payload=execution_payload)
            delivery_payload = self._persist_delivery(
                workflow_id=workflow["workflow_id"],
                cohort_id=definition.get("cohort_id"),
                experiment_id=experiment_id,
                execution_payload=execution_payload,
                channel_config=action,
                provider_result=provider_result,
                sandbox=sandbox,
            )

            if not provider_result.get("ok"):
                summary["failures"] += 1
                failure_reason = str(provider_result.get("failure_classification") or "provider_error")
                self.repository.record_resource_event(
                    "workflow",
                    workflow["workflow_id"],
                    event_type="action_delivery",
                    payload={**execution_payload, **delivery_payload, "delivery_status": "failed", "failure_reason": failure_reason},
                )
                summary["results"].append(execution_payload)
                continue

            summary["success"] += 1
            self.repository.record_resource_event("workflow", workflow["workflow_id"], event_type="action_delivery", payload=delivery_payload)
            if not manual_test:
                self._upsert_policy_state(
                    workflow["workflow_id"],
                    execution_payload["user_id"],
                    execution_payload["channel"],
                    action_date,
                    delivered=True,
                    last_delivery_at=datetime.utcnow().isoformat(),
                )
                self._upsert_budget_state(
                    workflow["workflow_id"],
                    action_date,
                    consumed=True,
                    max_deliveries=int(budget_policy.get("daily_delivery_limit") or 0) or None,
                )
                self._record_idempotency(
                    policy_result["idempotency_key"],
                    {
                        "workflow_id": workflow["workflow_id"],
                        "cohort_snapshot_id": snapshot_id,
                        "user_id": execution_payload["user_id"],
                        "action_date": action_date,
                        "group": group or "treatment",
                        "delivery_id": delivery_payload["delivery_id"],
                    },
                )
                if experiment_id and group not in {None, "excluded"}:
                    self.experiments.record_exposure(
                        experiment_id,
                        {
                            **delivery_payload,
                            "experiment_id": experiment_id,
                            "group": group or "treatment_a",
                            "exposed_at": datetime.utcnow().isoformat(),
                        },
                    )
            summary["results"].append(delivery_payload)

        self.repository.record_resource_event("workflow", workflow["workflow_id"], event_type="workflow_execution", payload=summary)
        self.repository.record_action("workflow_execution_completed", "workflow", workflow["workflow_id"], summary)
        return summary

    def _require_active_cohort(self, cohort_id: str) -> Dict[str, Any]:
        if not cohort_id:
            raise MissingDependencyError("cohort", cohort_id or "unknown", detail="Workflow cohort reference is missing.")
        cohort = self.cohorts.get_cohort(cohort_id)
        if cohort is None:
            raise MissingDependencyError("cohort", cohort_id, detail=f"Cohort '{cohort_id}' referenced by workflow is missing.")
        cohort_status = str(cohort.get("status") or "").lower()
        if cohort_status != "active":
            raise ResourceLockedError(
                f"Cohort '{cohort_id}' is {cohort_status or 'unknown'} and is locked for workflow execution."
            )
        return cohort

    def _require_active_experiment(self, experiment_id: str) -> Dict[str, Any]:
        record = self.repository.get_resource("experiment", experiment_id)
        if record is None:
            raise MissingDependencyError(
                "experiment",
                experiment_id,
                detail=f"Experiment '{experiment_id}' referenced by workflow is missing.",
            )
        payload = record.get("payload") or {}
        experiment_status = str(payload.get("status") or record.get("status") or "").lower()
        if experiment_status != "active":
            raise ResourceLockedError(
                f"Experiment '{experiment_id}' is {experiment_status or 'unknown'} and is locked for workflow execution."
            )
        return payload
