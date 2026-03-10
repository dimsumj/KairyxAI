from __future__ import annotations

import uuid
from datetime import datetime, timedelta
from typing import Any, Dict, List

from engagement_executor import EngagementExecutor
from engagement_feedback import EngagementFeedback

from app.application.cohorts import CohortService
from app.application.experiments import ExperimentConfigService


class WorkflowService:
    def __init__(self, repository):
        self.repository = repository
        self.cohorts = CohortService(repository)
        self.experiments = ExperimentConfigService(repository)
        self.executor = EngagementExecutor()
        self.feedback = EngagementFeedback()

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
    ) -> Dict[str, Any]:
        if self.cohorts.get_cohort(cohort_id) is None:
            raise KeyError(cohort_id)
        workflow_id = f"wf_{uuid.uuid4().hex[:20]}"
        payload = {
            "workflow_id": workflow_id,
            "name": name,
            "status": "draft",
            "current_version": 1,
            "published_version": None,
            "definition": {
                "cohort_id": cohort_id,
                "schedule": schedule or {"type": "daily"},
                "action": action or {},
                "policy": policy or {},
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
        version = int(payload.get("current_version") or 1)
        payload["published_version"] = version
        payload["status"] = "published"
        saved = self.repository.upsert_resource("workflow", workflow_id, status="published", name=payload.get("name"), payload=payload)
        self.repository.create_resource_version("workflow", workflow_id, version=version, payload=payload)
        self.repository.record_resource_event("workflow", workflow_id, event_type="workflow_published", payload={"version": version})
        return self._to_response(saved)

    def pause_workflow(self, workflow_id: str) -> Dict[str, Any]:
        return self._set_status(workflow_id, "paused", "workflow_paused")

    def resume_workflow(self, workflow_id: str) -> Dict[str, Any]:
        return self._set_status(workflow_id, "published", "workflow_resumed")

    def set_kill_switch(self, enabled: bool) -> Dict[str, Any]:
        payload = {
            "enabled": bool(enabled),
            "updated_at": datetime.utcnow().isoformat(),
        }
        record = self.repository.upsert_resource(
            "orchestrator",
            "kill_switch",
            status="enabled" if enabled else "disabled",
            name="kill_switch",
            payload=payload,
        )
        self.repository.record_action(
            "orchestrator_kill_switch_changed",
            "orchestrator",
            "kill_switch",
            payload,
        )
        return record.get("payload") or payload

    def get_kill_switch(self) -> Dict[str, Any]:
        record = self.repository.get_resource("orchestrator", "kill_switch")
        if record is None:
            return {"enabled": False}
        return record.get("payload") or {"enabled": False}

    def list_executions(self, workflow_id: str) -> List[Dict[str, Any]]:
        return [item.get("payload") or {} for item in self.repository.list_resource_events("workflow", workflow_id, event_type="workflow_execution", limit=500)]

    def test_run(self, workflow_id: str, *, limit: int = 20, confirm: bool = False, sandbox: bool = True) -> Dict[str, Any]:
        workflow = self.get_workflow(workflow_id)
        if workflow is None:
            raise KeyError(workflow_id)
        if workflow["status"] not in {"published", "draft"}:
            raise ValueError("Only draft or published workflows can be executed.")
        if self.get_kill_switch().get("enabled"):
            raise ValueError("Kill switch is enabled.")

        definition = workflow.get("definition") or {}
        if definition.get("requires_confirmation") and not confirm:
            raise ValueError("Workflow requires confirmation before execution.")

        members = self.cohorts.list_members(definition["cohort_id"], page=1, page_size=max(1, int(limit)))["items"]
        execution_id = f"run_{uuid.uuid4().hex[:20]}"
        summary = {
            "execution_id": execution_id,
            "workflow_id": workflow_id,
            "workflow_version": workflow.get("published_version") or workflow.get("current_version") or 1,
            "sandbox": bool(sandbox),
            "triggered": len(members),
            "executed": 0,
            "success": 0,
            "holdout": 0,
            "policy_blocked": 0,
            "failures": 0,
            "results": [],
            "recorded_at": datetime.utcnow().isoformat(),
        }

        for member in members:
            policy_result = self._evaluate_policy(workflow_id, member, definition.get("action") or {}, definition.get("policy") or {}, sandbox=sandbox)
            group = None
            experiment_id = definition.get("experiment_id")
            assignment = None
            if experiment_id:
                assignment = self.experiments.assign_user(experiment_id, member.get("canonical_user_id"))
                group = assignment["group"]

            execution_payload = {
                "execution_id": execution_id,
                "workflow_id": workflow_id,
                "workflow_version": summary["workflow_version"],
                "cohort_id": definition.get("cohort_id"),
                "user_id": member.get("canonical_user_id"),
                "channel": definition.get("action", {}).get("channel", "push_notification"),
                "execution_status": "pending",
                "group": group,
                "sandbox": bool(sandbox),
                "recorded_at": datetime.utcnow().isoformat(),
            }

            if not policy_result["allowed"]:
                execution_payload["execution_status"] = "policy_blocked"
                execution_payload["reason"] = policy_result["reason"]
                summary["policy_blocked"] += 1
                self.repository.record_resource_event("workflow", workflow_id, event_type="action_policy_log", payload=execution_payload)
                if experiment_id:
                    self.experiments.record_exposure(
                        experiment_id,
                        {
                            **execution_payload,
                            "experiment_id": experiment_id,
                            "action_execution_id": None,
                        },
                    )
                summary["results"].append(execution_payload)
                continue

            if group == "holdout":
                execution_payload["execution_status"] = "holdout"
                summary["holdout"] += 1
                self.repository.record_resource_event("workflow", workflow_id, event_type="action_execution", payload=execution_payload)
                if experiment_id:
                    self.experiments.record_exposure(
                        experiment_id,
                        {
                            **execution_payload,
                            "experiment_id": experiment_id,
                            "action_execution_id": execution_payload["execution_id"],
                            "exposed_at": datetime.utcnow().isoformat(),
                        },
                    )
                summary["results"].append(execution_payload)
                continue

            action = dict(definition.get("action") or {})
            if group == "treatment_b":
                action["content"] = f"{str(action.get('content') or '').strip()} [B]"
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
            self.repository.record_resource_event("workflow", workflow_id, event_type="action_execution", payload=execution_payload)

            if not delivery_id:
                summary["failures"] += 1
                self.repository.record_resource_event(
                    "workflow",
                    workflow_id,
                    event_type="action_delivery",
                    payload={**execution_payload, "delivery_status": "failed", "failure_reason": "channel_error"},
                )
                summary["results"].append(execution_payload)
                continue

            summary["success"] += 1
            delivery_payload = {
                **execution_payload,
                "delivery_id": delivery_id,
                "delivery_status": "delivered",
            }
            self.repository.record_resource_event("workflow", workflow_id, event_type="action_delivery", payload=delivery_payload)
            if experiment_id:
                self.experiments.record_exposure(
                    experiment_id,
                    {
                        **delivery_payload,
                        "experiment_id": experiment_id,
                        "exposed_at": datetime.utcnow().isoformat(),
                    },
                )
                outcome = self.feedback.get_engagement_result(member.get("canonical_user_id"), delivery_id)
                self.experiments.record_outcome(
                    experiment_id,
                    {
                        "experiment_id": experiment_id,
                        "workflow_id": workflow_id,
                        "cohort_id": definition.get("cohort_id"),
                        "group": group or "treatment_a",
                        "user_id": member.get("canonical_user_id"),
                        "action_execution_id": delivery_id,
                        "simulated_response": outcome.get("simulated_response"),
                        "recorded_at": datetime.utcnow().isoformat(),
                    },
                )
            summary["results"].append(delivery_payload)

        self.repository.record_resource_event("workflow", workflow_id, event_type="workflow_execution", payload=summary)
        self.repository.record_action("workflow_test_run_completed", "workflow", workflow_id, summary)
        return summary

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
        payload.setdefault("created_at", record["created_at"])
        payload.setdefault("updated_at", record["updated_at"])
        return payload

    def _evaluate_policy(
        self,
        workflow_id: str,
        member: Dict[str, Any],
        action: Dict[str, Any],
        policy: Dict[str, Any],
        *,
        sandbox: bool,
    ) -> Dict[str, Any]:
        user_id = str(member.get("canonical_user_id") or "")
        if not user_id:
            return {"allowed": False, "reason": "data_missing"}
        blacklist = {str(item) for item in policy.get("blacklist_ids") or []}
        if user_id in blacklist:
            return {"allowed": False, "reason": "policy_blocked"}

        events = [item.get("payload") or {} for item in self.repository.list_resource_events("workflow", event_type="action_execution", limit=5000)]
        now = datetime.utcnow()
        last_24h = []
        same_channel = []
        same_workflow = []
        channel = str(action.get("channel") or "push_notification")
        for item in events:
            if str(item.get("user_id") or "") != user_id:
                continue
            try:
                created_at = datetime.fromisoformat(str(item.get("recorded_at") or item.get("created_at")))
            except Exception:
                created_at = now
            if created_at >= now - timedelta(hours=24):
                last_24h.append(item)
            if item.get("channel") == channel and created_at >= now - timedelta(hours=24):
                same_channel.append(item)
            if str(item.get("workflow_id") or "") == workflow_id and created_at >= now - timedelta(hours=int(policy.get("cooldown_hours") or 24)):
                same_workflow.append(item)

        if len(last_24h) >= int(policy.get("global_daily_limit") or 3):
            return {"allowed": False, "reason": "policy_blocked"}
        if len(same_channel) >= int(policy.get("channel_daily_limit") or 2):
            return {"allowed": False, "reason": "policy_blocked"}
        if same_workflow:
            return {"allowed": False, "reason": "policy_blocked"}

        if not sandbox and policy.get("quiet_hours"):
            quiet = policy.get("quiet_hours") or {}
            start_hour = int(quiet.get("start", 22))
            end_hour = int(quiet.get("end", 7))
            current_hour = now.hour
            if current_hour >= start_hour or current_hour < end_hour:
                return {"allowed": False, "reason": "policy_blocked"}
        return {"allowed": True, "reason": None}
