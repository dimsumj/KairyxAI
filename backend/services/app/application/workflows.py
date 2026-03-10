from __future__ import annotations

import hashlib
import uuid
from datetime import datetime, timedelta
from typing import Any, Dict, List

from engagement_executor import EngagementExecutor

from app.application.cohorts import CohortService
from app.application.experiments import ExperimentConfigService


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
                "experiment_id": experiment_id,
                "requires_confirmation": bool(requires_confirmation),
            },
        }
        record = self.repository.upsert_resource("workflow", workflow_id, status="draft", name=name, payload=payload)
        self.repository.create_resource_version("workflow", workflow_id, version=1, payload=payload)
        self.repository.record_resource_event("workflow", workflow_id, event_type="workflow_created", payload=payload)
        self.repository.record_action("workflow_created", "workflow", workflow_id, payload)
        return self._to_response(record)

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

    def test_run(self, workflow_id: str, *, limit: int = 20, confirm: bool = False, sandbox: bool = True, reference_time: str | None = None) -> Dict[str, Any]:
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
        )

    def run_due_workflows(self, *, reference_time: str | None = None, limit_per_workflow: int = 100) -> Dict[str, Any]:
        if self.get_kill_switch().get("enabled"):
            raise ValueError("Kill switch is enabled.")
        resolved_time = self._parse_reference_time(reference_time)
        action_date = resolved_time.date().isoformat()
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
                )
            )
        return {"reference_time": resolved_time.isoformat(), "items": runs}

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
        if raw_type not in {"daily_schedule", "manual_test"}:
            raise ValueError("Phase 2 only supports daily_schedule and manual_test triggers.")
        return {
            "type": raw_type,
            "hour": int((trigger or {}).get("hour") or 0),
            "minute": int((trigger or {}).get("minute") or 0),
        }

    def _normalize_policy(self, policy: Dict[str, Any]) -> Dict[str, Any]:
        payload = dict(policy or {})
        payload.setdefault("global_daily_limit", 3)
        payload.setdefault("channel_daily_limit", 2)
        payload.setdefault("cooldown_hours", 24)
        payload.setdefault("blacklist_ids", [])
        payload.setdefault("quiet_hours", {"start": 22, "end": 7})
        return payload

    def _build_publish_preflight(self, workflow: Dict[str, Any]) -> Dict[str, Any]:
        reasons: List[str] = []
        definition = workflow.get("definition") or {}
        cohort = self.cohorts.get_cohort(definition.get("cohort_id"))
        if cohort is None:
            reasons.append("cohort_not_found")
        elif cohort.get("status") != "active":
            reasons.append("cohort_not_active")
        channel_config = workflow.get("channel_config") or definition.get("channel_config") or definition.get("action") or {}
        if not channel_config.get("channel"):
            reasons.append("channel_missing")
        if not str(channel_config.get("content") or "").strip():
            reasons.append("content_missing")
        trigger = workflow.get("trigger") or definition.get("trigger") or definition.get("schedule") or {}
        if str(trigger.get("type") or "") not in {"daily_schedule", "manual_test"}:
            reasons.append("unsupported_trigger")
        if not workflow.get("experiment_id") and not definition.get("experiment_id"):
            reasons.append("experiment_missing")
        policy = workflow.get("policy") or definition.get("policy") or {}
        for field in ("global_daily_limit", "channel_daily_limit", "cooldown_hours"):
            if int(policy.get(field) or 0) < 0:
                reasons.append(f"invalid_{field}")
        return {"eligible": not reasons, "reasons": reasons}

    def _parse_reference_time(self, reference_time: str | None) -> datetime:
        if not reference_time:
            return datetime.utcnow()
        try:
            return datetime.fromisoformat(str(reference_time))
        except ValueError:
            return datetime.utcnow()

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
    ) -> Dict[str, Any]:
        if self.get_kill_switch().get("enabled"):
            raise ValueError("Kill switch is enabled.")

        definition = workflow.get("definition") or {}
        if definition.get("requires_confirmation") and not confirm:
            raise ValueError("Workflow requires confirmation before execution.")

        cohort = self.cohorts.get_cohort(definition["cohort_id"])
        if cohort is None:
            raise KeyError(definition["cohort_id"])
        if not manual_test and cohort.get("refresh_mode") == "daily":
            last_refreshed_at = cohort.get("last_refreshed_at")
            if not last_refreshed_at or last_refreshed_at[:10] < reference_time.date().isoformat():
                cohort = self.cohorts.refresh_cohort(definition["cohort_id"], force=True)

        members = self.cohorts.list_members(definition["cohort_id"], page=1, page_size=max(1, int(limit)))["items"]
        execution_id = f"run_{uuid.uuid4().hex[:20]}"
        snapshot_id = str((cohort or {}).get("latest_snapshot_id") or "snapshot_missing")
        action_date = reference_time.date().isoformat()
        summary = {
            "execution_id": execution_id,
            "workflow_id": workflow["workflow_id"],
            "workflow_version": workflow.get("published_version") or workflow.get("current_version") or 1,
            "sandbox": bool(sandbox),
            "trigger_type": "manual_test" if manual_test else "daily_schedule",
            "action_date": action_date,
            "cohort_snapshot_id": snapshot_id,
            "triggered": len(members),
            "executed": 0,
            "success": 0,
            "holdout": 0,
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
        experiment_id = workflow.get("experiment_id") or definition.get("experiment_id")

        for member in members:
            policy_result = self._evaluate_policy(
                workflow["workflow_id"],
                member,
                channel_config,
                policy,
                budget_policy,
                action_date=action_date,
                reference_time=reference_time,
                snapshot_id=snapshot_id,
                manual_test=manual_test,
            )
            assignment = None
            group = None
            if experiment_id and not manual_test:
                assignment = self.experiments.assign_user(experiment_id, member.get("canonical_user_id"))
                group = assignment["group"]

            execution_payload = {
                "execution_id": execution_id,
                "workflow_id": workflow["workflow_id"],
                "workflow_version": summary["workflow_version"],
                "cohort_id": definition.get("cohort_id"),
                "cohort_snapshot_id": snapshot_id,
                "user_id": member.get("canonical_user_id"),
                "channel": channel_config.get("channel", "push_notification"),
                "execution_status": "pending",
                "group": group,
                "sandbox": bool(sandbox),
                "trigger_type": summary["trigger_type"],
                "recorded_at": datetime.utcnow().isoformat(),
            }

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

            action = dict(channel_config)
            recipient = member.get("email") if str(action.get("channel") or "") == "email" else member.get("canonical_user_id")
            action_payload = {
                "decision": "ACT",
                "channel": action.get("channel", "push_notification"),
                "content": action.get("content", ""),
                "subject": action.get("subject", "KairyxAI"),
                "player_id": recipient or member.get("canonical_user_id"),
            }
            delivery_id = self.executor.execute_action(action_payload)
            summary["executed"] += 1
            execution_payload["execution_status"] = "executed" if delivery_id else "failed"
            execution_payload["action_execution_id"] = delivery_id or execution_id
            self.repository.record_resource_event("workflow", workflow["workflow_id"], event_type="action_execution", payload=execution_payload)

            if not delivery_id:
                summary["failures"] += 1
                self.repository.record_resource_event(
                    "workflow",
                    workflow["workflow_id"],
                    event_type="action_delivery",
                    payload={**execution_payload, "delivery_status": "failed", "failure_reason": "provider_error"},
                )
                summary["results"].append(execution_payload)
                continue

            summary["success"] += 1
            delivery_payload = {**execution_payload, "delivery_id": delivery_id, "delivery_status": "delivered"}
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
                        "delivery_id": delivery_id,
                    },
                )
                if experiment_id:
                    self.experiments.record_exposure(
                        experiment_id,
                        {
                            **delivery_payload,
                            "experiment_id": experiment_id,
                            "group": group or "treatment",
                            "exposed_at": datetime.utcnow().isoformat(),
                        },
                    )
            summary["results"].append(delivery_payload)

        self.repository.record_resource_event("workflow", workflow["workflow_id"], event_type="workflow_execution", payload=summary)
        self.repository.record_action("workflow_execution_completed", "workflow", workflow["workflow_id"], summary)
        return summary
