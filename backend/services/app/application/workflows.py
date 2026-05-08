from __future__ import annotations

import hashlib
import hmac
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List

from engagement_executor import EngagementExecutor

from app.application.cohorts import CohortService
from app.application.experiments import ExperimentConfigService
from app.application.secret_refs import contains_inline_secret, materialize_secret_refs, redact_secret_values
from app.core.errors import MissingDependencyError, ResourceLockedError
from app.core.request_context import get_request_context
from app.core.settings import get_settings


class WorkflowService:
    def __init__(self, repository):
        self.repository = repository
        self.settings = get_settings()
        self.cohorts = CohortService(repository)
        self.experiments = ExperimentConfigService(repository)
        self.executor = EngagementExecutor()

    def create_workflow(
        self,
        *,
        name: str,
        cohort_id: str | None,
        audience_mode: str | None = None,
        user_ids: List[str] | None = None,
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
        normalized_audience = self._normalize_audience_config(
            audience_mode=audience_mode,
            cohort_id=cohort_id,
            user_ids=user_ids,
        )
        workflow_id = f"wf_{uuid.uuid4().hex[:20]}"
        normalized_trigger = self._normalize_trigger(trigger or schedule or {"type": "daily"})
        normalized_channel = self._normalize_channel_config(channel_config or action or {}, workflow_name=name)
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
                "audience_mode": normalized_audience["audience_mode"],
                "cohort_id": normalized_audience["cohort_id"],
                "user_ids": normalized_audience["user_ids"],
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
        if str(payload.get("status") or "").lower() == "archived":
            raise ValueError("Archived workflows cannot be edited.")
        definition = dict(payload.get("definition") or {})
        if patch.get("name") is not None:
            payload["name"] = patch["name"]
        if any(key in patch for key in ("audience_mode", "cohort_id", "user_ids")):
            normalized_audience = self._normalize_audience_config(
                audience_mode=patch.get("audience_mode", definition.get("audience_mode")),
                cohort_id=patch.get("cohort_id", definition.get("cohort_id")),
                user_ids=patch.get("user_ids", definition.get("user_ids") or []),
            )
            definition["audience_mode"] = normalized_audience["audience_mode"]
            definition["cohort_id"] = normalized_audience["cohort_id"]
            definition["user_ids"] = normalized_audience["user_ids"]
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
            payload["channel_config"] = self._normalize_channel_config(channel_source or {}, workflow_name=str(payload.get("name") or ""))
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
        if self.repository.get_resource("workflow", workflow_id) is None:
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

    def get_workflow(self, workflow_id: str, *, include_runtime_summary: bool = True) -> Dict[str, Any] | None:
        record = self.repository.get_resource("workflow", workflow_id)
        return self._to_response(record, include_runtime_summary=include_runtime_summary) if record else None

    def list_workflows(self, *, include_runtime_summary: bool = True) -> List[Dict[str, Any]]:
        return [self._to_response(item, include_runtime_summary=include_runtime_summary) for item in self.repository.list_resources("workflow")]

    def publish_workflow(self, workflow_id: str) -> Dict[str, Any]:
        record = self.repository.get_resource("workflow", workflow_id)
        if record is None:
            raise KeyError(workflow_id)
        payload = dict(record.get("payload") or {})
        if str(payload.get("status") or "").lower() == "archived":
            raise ValueError("Archived workflows cannot be published.")
        self._assert_publishable_provider_config(payload)
        definition = payload.get("definition") or {}
        if not self._workflow_uses_provider_campaign(payload):
            self._require_active_cohort(str(definition.get("cohort_id") or ""))
            experiment_id = str(payload.get("experiment_id") or definition.get("experiment_id") or "").strip()
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

    def archive_workflow(self, workflow_id: str) -> Dict[str, Any]:
        record = self.repository.get_resource("workflow", workflow_id)
        if record is None:
            raise KeyError(workflow_id)
        payload = dict(record.get("payload") or {})
        status = str(payload.get("status") or "").lower()
        if status == "draft":
            raise ValueError("Draft workflows should be deleted instead of archived.")
        if status == "archived":
            raise ValueError("Workflow is already archived.")
        payload["status"] = "archived"
        payload["archived_at"] = datetime.utcnow().isoformat()
        saved = self.repository.upsert_resource("workflow", workflow_id, status="archived", name=payload.get("name"), payload=payload)
        self.repository.record_resource_event(
            "workflow",
            workflow_id,
            event_type="workflow_archived",
            payload={"status": "archived", "archived_at": payload["archived_at"]},
        )
        self.repository.record_action(
            "workflow_archived",
            "workflow",
            workflow_id,
            {"workflow_id": workflow_id, "archived_at": payload["archived_at"]},
        )
        return self._to_response(saved)

    def delete_workflow(self, workflow_id: str) -> bool:
        record = self.repository.get_resource("workflow", workflow_id)
        if record is None:
            return False
        payload = dict(record.get("payload") or {})
        if str(payload.get("status") or "").lower() != "draft":
            raise ValueError("Only draft workflows can be deleted.")
        deleted = self.repository.delete_resource("workflow", workflow_id)
        if deleted:
            self.repository.record_action("workflow_deleted", "workflow", workflow_id, {"workflow_id": workflow_id})
        return deleted

    def pause_workflow(self, workflow_id: str) -> Dict[str, Any]:
        return self._set_status(workflow_id, "paused", "workflow_paused")

    def resume_workflow(self, workflow_id: str) -> Dict[str, Any]:
        record = self.repository.get_resource("workflow", workflow_id)
        if record is None:
            raise KeyError(workflow_id)
        payload = dict(record.get("payload") or {})
        if str(payload.get("status") or "").lower() == "archived":
            raise ValueError("Archived workflows cannot be resumed.")
        self._assert_publishable_provider_config(payload)
        definition = payload.get("definition") or {}
        if not self._workflow_uses_provider_campaign(payload):
            self._require_active_cohort(str(definition.get("cohort_id") or ""))
            experiment_id = str(payload.get("experiment_id") or definition.get("experiment_id") or "").strip()
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
        if self.repository.get_resource("workflow", workflow_id) is None:
            raise KeyError(workflow_id)
        return [item.get("payload") or {} for item in self.repository.list_resource_events("workflow", workflow_id, event_type="workflow_execution", limit=500)]

    def list_deliveries(self, workflow_id: str) -> List[Dict[str, Any]]:
        if self.repository.get_resource("workflow", workflow_id) is None:
            raise KeyError(workflow_id)
        return [
            record.get("payload") or {}
            for record in self.repository.list_resources("workflow_delivery", name=workflow_id)
        ]

    def get_delivery_diagnostics(self, workflow_id: str) -> Dict[str, Any]:
        deliveries = self.list_deliveries(workflow_id)
        by_status: Dict[str, int] = {}
        by_provider: Dict[str, int] = {}
        by_provider_mode: Dict[str, int] = {}
        failure_classifications: Dict[str, int] = {}
        callback_latencies: List[int] = []
        simulator_count = 0
        fallback_count = 0
        retried_deliveries = 0
        for item in deliveries:
            status = str(item.get("delivery_status") or "unknown")
            provider = str(item.get("provider") or "unknown")
            provider_mode = str(item.get("provider_mode") or "unknown")
            by_status[status] = by_status.get(status, 0) + 1
            by_provider[provider] = by_provider.get(provider, 0) + 1
            by_provider_mode[provider_mode] = by_provider_mode.get(provider_mode, 0) + 1
            diagnostics = dict(item.get("delivery_diagnostics") or {})
            failure = str(diagnostics.get("failure_classification") or "").strip()
            if failure:
                failure_classifications[failure] = failure_classifications.get(failure, 0) + 1
            if int(diagnostics.get("attempt_count") or 0) > 1:
                retried_deliveries += 1
            if bool(item.get("simulated")):
                simulator_count += 1
            if provider_mode == "fallback_simulator":
                fallback_count += 1
            callback_latency_seconds = item.get("callback_latency_seconds")
            if callback_latency_seconds not in (None, ""):
                callback_latencies.append(int(callback_latency_seconds))
        total = len(deliveries)
        return {
            "workflow_id": workflow_id,
            "delivery_count": total,
            "by_status": by_status,
            "by_provider": by_provider,
            "by_provider_mode": by_provider_mode,
            "failure_classifications": failure_classifications,
            "simulator_delivery_rate": round(simulator_count / max(1, total), 4),
            "fallback_simulator_rate": round(fallback_count / max(1, total), 4),
            "retry_rate": round(retried_deliveries / max(1, total), 4),
            "callbacks_recorded": sum(1 for item in deliveries if int(item.get("callback_count") or 0) > 0),
            "callback_lag": {
                "count": len(callback_latencies),
                "avg_seconds": round(sum(callback_latencies) / len(callback_latencies), 2) if callback_latencies else 0.0,
                "max_seconds": max(callback_latencies) if callback_latencies else 0,
            },
        }

    def get_policy_counters(self, workflow_id: str) -> Dict[str, Any]:
        if self.repository.get_resource("workflow", workflow_id) is None:
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

    def ingest_delivery_callback(
        self,
        provider: str,
        callbacks: List[Dict[str, Any]],
        *,
        signature: str | None = None,
        raw_body: bytes | None = None,
    ) -> Dict[str, Any]:
        self._verify_callback_signature(provider, callbacks, signature=signature, raw_body=raw_body)
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
                "tenant_id": (delivery or {}).get("tenant_id") or callback.get("tenant_id") or (get_request_context().tenant_id if get_request_context() else None),
                "project_id": (delivery or {}).get("project_id") or callback.get("project_id") or (get_request_context().project_id if get_request_context() else None),
            }
            self.repository.upsert_resource(
                "provider_callback",
                callback_id,
                status="ingested",
                name=provider,
                payload=callback_payload,
                tenant_id=callback_payload.get("tenant_id"),
                project_id=callback_payload.get("project_id"),
            )
            self.repository.record_action(
                "provider_callback_ingested",
                "provider_callback",
                callback_id,
                callback_payload,
                tenant_id=callback_payload.get("tenant_id"),
                project_id=callback_payload.get("project_id"),
            )
            ingested += 1

            if delivery is None:
                push_dispatch = self._find_push_dispatch_for_callback(callback_payload)
                if push_dispatch is None:
                    items.append({"callback_id": callback_id, "status": "unmatched"})
                    continue
                push_dispatch_payload = dict(push_dispatch.get("payload") or {})
                push_dispatch_payload["callback_count"] = int(push_dispatch_payload.get("callback_count") or 0) + 1
                push_dispatch_payload["last_callback_at"] = occurred_at
                push_dispatch_payload["last_provider_event"] = event_type
                push_dispatch_payload["provider_callback_status"] = str(callback.get("status") or event_type)
                result_summary = dict(push_dispatch_payload.get("result_summary") or {})
                result_summary["last_callback_event"] = event_type
                result_summary["last_callback_at"] = occurred_at
                push_dispatch_payload["result_summary"] = result_summary
                self.repository.upsert_resource(
                    "push_dispatch",
                    str(push_dispatch_payload.get("push_dispatch_id") or push_dispatch.get("resource_id")),
                    status=str(push_dispatch_payload.get("status") or push_dispatch.get("status") or "sent"),
                    name=push_dispatch_payload.get("name"),
                    payload=push_dispatch_payload,
                    tenant_id=push_dispatch_payload.get("tenant_id"),
                    project_id=push_dispatch_payload.get("project_id"),
                )
                self.repository.record_resource_event(
                    "push_dispatch",
                    str(push_dispatch_payload.get("push_dispatch_id") or push_dispatch.get("resource_id")),
                    event_type="push_dispatch_callback",
                    payload={
                        **callback_payload,
                        "push_dispatch_id": push_dispatch_payload.get("push_dispatch_id"),
                        "provider_request_id": push_dispatch_payload.get("provider_request_id"),
                    },
                )
                items.append(
                    {
                        "callback_id": callback_id,
                        "push_dispatch_id": push_dispatch_payload.get("push_dispatch_id"),
                        "status": "matched_push_dispatch",
                    }
                )
                continue

            delivery_payload = dict(delivery.get("payload") or {})
            delivery_payload["callback_count"] = int(delivery_payload.get("callback_count") or 0) + 1
            delivery_payload["last_callback_at"] = occurred_at
            delivery_payload["last_provider_event"] = event_type
            delivery_payload["provider_callback_status"] = str(callback.get("status") or event_type)
            recorded_at_dt = self._parse_reference_time(str(delivery_payload.get("recorded_at") or ""))
            occurred_at_dt = self._parse_reference_time(occurred_at)
            if occurred_at_dt >= recorded_at_dt:
                delivery_payload["callback_latency_seconds"] = int((occurred_at_dt - recorded_at_dt).total_seconds())
            if event_type in {"opened", "clicked", "returned", "converted", "claimed", "purchase", "reactivated"}:
                delivery_payload["delivery_status"] = "converted" if event_type in {"returned", "converted", "claimed", "purchase", "reactivated"} else event_type
            elif event_type in {"bounced", "failed", "dropped"}:
                delivery_payload["delivery_status"] = "failed"
                delivery_payload["failure_reason"] = "provider_error"
            self.repository.upsert_resource(
                "workflow_delivery",
                str(delivery_payload.get("delivery_id") or delivery.get("resource_id")),
                status=str(delivery_payload.get("delivery_status") or "delivered"),
                name=delivery_payload.get("workflow_id"),
                payload=delivery_payload,
                tenant_id=delivery_payload.get("tenant_id"),
                project_id=delivery_payload.get("project_id"),
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
                        "user_id": callback_payload.get("user_id") or delivery_payload.get("user_id"),
                        "group": delivery_payload.get("group") or "treatment",
                        "action_execution_id": delivery_payload.get("action_execution_id"),
                        "delivery_id": delivery_payload.get("delivery_id"),
                        "provider_callback_id": callback_id,
                        "occurred_at": occurred_at,
                        "outcome_name": outcome_name,
                        "product_outcome_type": "return" if outcome_name == "returned" else ("purchase" if outcome_name == "purchase" else None),
                        "attribution_window_days": int(callback.get("attribution_window_days") or 7),
                        "variant_id": delivery_payload.get("variant_id"),
                        "template_id": delivery_payload.get("template_id"),
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
        if workflow["status"] == "archived":
            raise ValueError("Archived workflows cannot be executed.")
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
        for workflow in self.list_workflows(include_runtime_summary=False):
            if workflow.get("status") != "published":
                continue
            trigger = workflow.get("trigger") or {}
            trigger_type = str(trigger.get("type") or "")
            if trigger_type not in {"daily_schedule", "one_time_schedule"}:
                continue
            if trigger_type == "daily_schedule":
                scheduled_hour, scheduled_minute = self._resolve_scheduled_window(workflow, trigger)
                if (resolved_time.hour, resolved_time.minute) < (scheduled_hour, scheduled_minute):
                    continue
                if self._already_executed_for_date(workflow["workflow_id"], action_date):
                    continue
            else:
                scheduled_at = self._parse_reference_time(str(trigger.get("scheduled_at") or ""))
                if scheduled_at > resolved_time:
                    continue
                if self._has_execution_for_trigger(workflow["workflow_id"], "one_time_schedule"):
                    continue
            run_result = self._execute_workflow(
                workflow,
                limit=max(1, int(limit_per_workflow)),
                confirm=True,
                sandbox=False,
                manual_test=False,
                reference_time=resolved_time,
                trigger_type=trigger_type,
                confirmation_token=tokens.get(workflow["workflow_id"]),
            )
            if trigger_type == "one_time_schedule":
                self._finalize_one_time_workflow(
                    workflow["workflow_id"],
                    completed_at=resolved_time.isoformat(),
                    run_result=run_result,
                )
            runs.append(run_result)
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
        if str(payload.get("status") or "").lower() == "archived":
            raise ValueError("Archived workflows cannot change status.")
        payload["status"] = status
        saved = self.repository.upsert_resource("workflow", workflow_id, status=status, name=payload.get("name"), payload=payload)
        self.repository.record_resource_event("workflow", workflow_id, event_type=event_type, payload={"status": status})
        return self._to_response(saved)

    def _to_response(self, record: Dict[str, Any], *, include_runtime_summary: bool = True) -> Dict[str, Any]:
        payload = dict(record.get("payload") or {})
        definition = payload.get("definition") or {}
        payload.setdefault("audience_mode", definition.get("audience_mode") or ("cohort" if definition.get("cohort_id") else "provider_campaign"))
        payload.setdefault("user_ids", list(definition.get("user_ids") or []))
        payload.setdefault("trigger", definition.get("trigger") or definition.get("schedule") or {"type": "daily_schedule"})
        payload.setdefault("policy", definition.get("policy") or {})
        payload.setdefault("budget_policy", definition.get("budget_policy") or {})
        payload.setdefault("experiment_id", definition.get("experiment_id"))
        payload.setdefault("channel_config", definition.get("channel_config") or definition.get("action") or {})
        payload.setdefault("archived_at", payload.get("archived_at"))
        payload.setdefault("created_at", record["created_at"])
        payload.setdefault("updated_at", record["updated_at"])
        payload.setdefault("tenant_id", record.get("tenant_id"))
        payload.setdefault("project_id", record.get("project_id"))
        payload.setdefault("created_by", record.get("created_by") or payload.get("created_by") or "system")
        payload.setdefault("updated_by", record.get("updated_by") or payload.get("updated_by") or "system")
        payload.setdefault("correlation_id", record.get("correlation_id") or payload.get("correlation_id") or "")
        if include_runtime_summary:
            payload["runtime_summary"] = self._build_runtime_summary(payload)
        return redact_secret_values(payload)

    def _build_runtime_summary(self, workflow: Dict[str, Any]) -> Dict[str, Any]:
        workflow_id = str(workflow.get("workflow_id") or "").strip()
        default_summary = {
            "last_run_at": None,
            "last_test_run_at": None,
            "next_run_at": None,
            "last_result": {},
            "totals": {
                "runs": 0,
                "test_runs": 0,
                "triggered": 0,
                "executed": 0,
                "success": 0,
                "failures": 0,
                "holdout": 0,
                "filtered_out": 0,
                "policy_blocked": 0,
                "duplicate_suppressed": 0,
                "budget_exhausted": 0,
                "invalid_target": 0,
            },
        }
        if not workflow_id:
            return default_summary

        events = [
            item.get("payload") or {}
            for item in self.repository.list_resource_events("workflow", workflow_id, event_type="workflow_execution", limit=500)
        ]
        if not events:
            if self._workflow_has_next_run(workflow):
                default_summary["next_run_at"] = self._compute_next_run_at(workflow, None)
            return default_summary

        live_runs = [event for event in events if not self._is_test_execution(event)]
        test_runs = [event for event in events if self._is_test_execution(event)]
        latest_live = self._latest_execution_event(live_runs)
        latest_test = self._latest_execution_event(test_runs)

        totals = dict(default_summary["totals"])
        for event in live_runs:
            totals["runs"] += 1
            for key in ("triggered", "executed", "success", "failures", "holdout", "filtered_out", "policy_blocked", "duplicate_suppressed", "budget_exhausted", "invalid_target"):
                totals[key] += int(event.get(key) or 0)
        totals["test_runs"] = len(test_runs)

        return {
            "last_run_at": latest_live.get("recorded_at") if latest_live else None,
            "last_test_run_at": latest_test.get("recorded_at") if latest_test else None,
            "next_run_at": self._compute_next_run_at(workflow, latest_live),
            "last_result": self._compact_execution_summary(latest_live),
            "totals": totals,
        }

    @staticmethod
    def _latest_execution_event(events: List[Dict[str, Any]]) -> Dict[str, Any] | None:
        if not events:
            return None
        return max(
            events,
            key=lambda item: WorkflowService._parse_execution_sort_key(
                str(item.get("recorded_at") or item.get("reference_time") or "")
            ),
        )

    @staticmethod
    def _is_test_execution(event: Dict[str, Any]) -> bool:
        return bool(event.get("sandbox")) or str(event.get("trigger_type") or "").lower() == "manual_test"

    @staticmethod
    def _parse_execution_sort_key(value: str) -> datetime:
        raw = str(value or "").strip()
        if not raw:
            return datetime.min
        normalized = raw.replace("Z", "+00:00")
        try:
            parsed = datetime.fromisoformat(normalized)
            if parsed.tzinfo is not None:
                return parsed.astimezone(timezone.utc).replace(tzinfo=None)
            return parsed
        except ValueError:
            return datetime.min

    @staticmethod
    def _workflow_has_next_run(workflow: Dict[str, Any]) -> bool:
        if str(workflow.get("status") or "").lower() != "published":
            return False
        trigger_type = str((workflow.get("trigger") or {}).get("type") or "").lower()
        return trigger_type in {"daily_schedule", "one_time_schedule"}

    def _compute_next_run_at(self, workflow: Dict[str, Any], latest_live: Dict[str, Any] | None) -> str | None:
        if not self._workflow_has_next_run(workflow):
            return None
        if str(workflow.get("status") or "").lower() == "archived":
            return None
        trigger = dict(workflow.get("trigger") or {})
        trigger_type = str(trigger.get("type") or "").lower()
        if trigger_type == "one_time_schedule":
            scheduled_at = str(trigger.get("scheduled_at") or "").strip()
            if not scheduled_at:
                return None
            if latest_live:
                return None
            scheduled_time = self._parse_execution_sort_key(scheduled_at)
            return scheduled_at if scheduled_time >= datetime.utcnow() else None
        hour = int(trigger.get("hour") or 0)
        minute = int(trigger.get("minute") or 0)
        now = datetime.utcnow()
        if latest_live:
            base = self._parse_execution_sort_key(str(latest_live.get("recorded_at") or latest_live.get("reference_time") or ""))
            if base != datetime.min:
                anchor = max(base, now)
                candidate = anchor.replace(hour=hour, minute=minute, second=0, microsecond=0)
                if candidate <= anchor:
                    candidate += timedelta(days=1)
                return candidate.isoformat()
        candidate = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
        if candidate <= now:
            candidate += timedelta(days=1)
        return candidate.isoformat()

    @staticmethod
    def _compact_execution_summary(event: Dict[str, Any] | None) -> Dict[str, Any]:
        if not event:
            return {}
        return {
            "trigger_type": event.get("trigger_type"),
            "recorded_at": event.get("recorded_at"),
            "triggered": int(event.get("triggered") or 0),
            "executed": int(event.get("executed") or 0),
            "success": int(event.get("success") or 0),
            "failures": int(event.get("failures") or 0),
            "holdout": int(event.get("holdout") or 0),
            "filtered_out": int(event.get("filtered_out") or 0),
            "policy_blocked": int(event.get("policy_blocked") or 0),
            "duplicate_suppressed": int(event.get("duplicate_suppressed") or 0),
            "budget_exhausted": int(event.get("budget_exhausted") or 0),
            "invalid_target": int(event.get("invalid_target") or 0),
        }

    def _normalize_trigger(self, trigger: Dict[str, Any]) -> Dict[str, Any]:
        raw_type = str((trigger or {}).get("type") or "daily").lower()
        if raw_type == "daily":
            raw_type = "daily_schedule"
        if raw_type not in {"daily_schedule", "manual_test", "event_trigger", "threshold_trigger", "one_time_schedule"}:
            raise ValueError("Supported triggers are daily_schedule, manual_test, event_trigger, threshold_trigger, and one_time_schedule.")
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
        if raw_type == "one_time_schedule":
            scheduled_at = self._parse_iso_timestamp(str((trigger or {}).get("scheduled_at") or ""))
            if not scheduled_at:
                raise ValueError("one_time_schedule requires scheduled_at.")
            payload["scheduled_at"] = scheduled_at
            scheduled_time = datetime.fromisoformat(scheduled_at.replace("Z", "+00:00"))
            payload["hour"] = scheduled_time.hour
            payload["minute"] = scheduled_time.minute
        return payload

    def _normalize_policy(self, policy: Dict[str, Any]) -> Dict[str, Any]:
        payload = dict(policy or {})
        payload.setdefault("global_daily_limit", 3)
        payload.setdefault("channel_daily_limit", 2)
        payload.setdefault("cooldown_hours", 24)
        payload.setdefault("blacklist_ids", [])
        payload.setdefault("quiet_hours", {"start": 22, "end": 7})
        return payload

    @staticmethod
    def _optional_int(value: Any) -> int | None:
        if value in (None, ""):
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    def _resolve_budget_delivery_limit(self, budget_policy: Dict[str, Any]) -> int | None:
        return self._optional_int(
            (budget_policy or {}).get("daily_delivery_limit")
            if (budget_policy or {}).get("daily_delivery_limit") not in (None, "")
            else (budget_policy or {}).get("daily_budget_limit")
        )

    def _normalize_channel_config(self, channel_config: Dict[str, Any], *, workflow_name: str = "") -> Dict[str, Any]:
        payload = dict(channel_config or {})
        channel = str(payload.get("channel") or "push_notification").strip().lower() or "push_notification"
        payload["channel"] = channel
        if channel != "push_notification":
            return payload
        content = str(payload.get("content") or "").strip()
        body = str(payload.get("body") or "").strip()
        if body and not content:
            payload["content"] = body
        elif content and not body:
            payload["body"] = content
        campaign_name = str(payload.get("campaign_name") or "").strip()
        if not campaign_name and workflow_name:
            payload["campaign_name"] = workflow_name
        if payload.get("scheduled_at") not in (None, ""):
            payload["scheduled_at"] = self._parse_iso_timestamp(str(payload.get("scheduled_at")))
        data = payload.get("data")
        if data in (None, ""):
            payload["data"] = {}
        elif not isinstance(data, dict):
            raise ValueError("Push notification data must be an object.")
        provider_options = payload.get("provider_options")
        if provider_options in (None, ""):
            payload["provider_options"] = {}
        elif not isinstance(provider_options, dict):
            raise ValueError("Push notification provider_options must be an object.")
        return payload

    @staticmethod
    def _parse_iso_timestamp(value: str) -> str:
        raw = str(value or "").strip()
        if not raw:
            return ""
        normalized = raw.replace("Z", "+00:00")
        try:
            return datetime.fromisoformat(normalized).isoformat()
        except ValueError as exc:
            raise ValueError("scheduled_at must be a valid ISO timestamp.") from exc

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
                    resolved = self._merge_channel_config_override(resolved, branch_payload["action"])
                continue
            if step_type == "action":
                action_payload = dict(step.get("action") or {})
                resolved = self._merge_channel_config_override(resolved, action_payload)
                trace.append({"step": index, "type": step_type, "action": action_payload})
                continue
            if step_type == "end":
                trace.append({"step": index, "type": step_type, "ended": True})
                return {"status": "ended", "channel_config": resolved, "trace": trace}
        return {"status": "ok", "channel_config": resolved, "trace": trace}

    def _merge_channel_config_override(self, base_config: Dict[str, Any], override_config: Dict[str, Any]) -> Dict[str, Any]:
        resolved = dict(base_config or {})
        override = dict(override_config or {})
        channel = str(override.get("channel") or resolved.get("channel") or "").strip().lower()
        if channel == "push_notification":
            override_content = str(override.get("content") or "").strip()
            override_body = str(override.get("body") or "").strip()
            if override_content and "body" not in override:
                override["body"] = override_content
            elif override_body and "content" not in override:
                override["content"] = override_body
        resolved.update(override)
        return resolved

    @staticmethod
    def _action_message_content(action: Dict[str, Any]) -> str:
        return str(action.get("body") or action.get("content") or "").strip()

    def _resolve_provider_name(self, action: Dict[str, Any]) -> str:
        direct_provider = str(action.get("provider") or "").strip().lower()
        if direct_provider:
            return direct_provider
        provider_connection_id = str(action.get("provider_connection_id") or "").strip()
        if not provider_connection_id:
            return ""
        record = self.repository.get_resource("provider_connection", provider_connection_id)
        if record is None:
            raise ValueError(f"Provider connection '{provider_connection_id}' was not found.")
        return str((record.get("payload") or {}).get("provider") or "").strip().lower()

    def _is_live_provider_push_action(self, action: Dict[str, Any]) -> bool:
        if str(action.get("channel") or "").strip().lower() != "push_notification":
            return False
        provider_name = self._resolve_provider_name(action)
        return provider_name == "wynn_push_notifier"

    def _validate_action_for_execution(self, action: Dict[str, Any], *, workflow_name: str) -> Dict[str, Any]:
        resolved = self._normalize_channel_config(action, workflow_name=workflow_name)
        channel = str(resolved.get("channel") or "").strip().lower()
        if channel != "push_notification":
            return resolved
        body = self._action_message_content(resolved)
        if not body:
            raise ValueError("Push notification workflows require body or content.")
        resolved["body"] = body
        resolved["content"] = body
        if self._is_live_provider_push_action(resolved):
            if not str(resolved.get("title") or "").strip():
                raise ValueError("Live push workflows require title.")
            if not body:
                raise ValueError("Live push workflows require body.")
        return resolved

    def _build_publish_preflight(self, workflow: Dict[str, Any]) -> Dict[str, Any]:
        reasons: List[str] = []
        definition = workflow.get("definition") or {}
        audience_mode = str(definition.get("audience_mode") or ("cohort" if definition.get("cohort_id") else "provider_campaign")).strip().lower()
        if audience_mode == "cohort":
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
        has_step_content = any(self._action_message_content(item) for item in step_actions + branch_actions)
        if not self._action_message_content(channel_config) and not has_step_content:
            reasons.append("content_missing")
        for action in [channel_config, *step_actions, *branch_actions]:
            if not isinstance(action, dict):
                continue
            if self._is_live_provider_push_action(action):
                if not str(action.get("title") or "").strip() and "push_title_missing" not in reasons:
                    reasons.append("push_title_missing")
                if not self._action_message_content(action) and "push_body_missing" not in reasons:
                    reasons.append("push_body_missing")
        trigger = workflow.get("trigger") or definition.get("trigger") or definition.get("schedule") or {}
        trigger_type = str(trigger.get("type") or "")
        if trigger_type not in {"daily_schedule", "manual_test", "event_trigger", "threshold_trigger", "one_time_schedule"}:
            reasons.append("unsupported_trigger")
        if trigger_type == "event_trigger" and not str(trigger.get("event_type") or "").strip():
            reasons.append("event_type_missing")
        if trigger_type == "threshold_trigger":
            if not str(trigger.get("metric_id") or "").strip():
                reasons.append("metric_id_missing")
            if str(trigger.get("operator") or "") not in {">", ">=", "<", "<=", "=="}:
                reasons.append("threshold_operator_invalid")
        if trigger_type == "one_time_schedule" and not str(trigger.get("scheduled_at") or "").strip():
            reasons.append("scheduled_at_missing")
        requires_experiment = audience_mode != "provider_campaign" and str(channel_config.get("channel") or "").strip().lower() != "push_notification"
        if requires_experiment and not workflow.get("experiment_id") and not definition.get("experiment_id"):
            reasons.append("experiment_missing")
        elif requires_experiment:
            experiment_id = str(workflow.get("experiment_id") or definition.get("experiment_id") or "")
            experiment = self.repository.get_resource("experiment", experiment_id)
            if experiment is None:
                reasons.append("experiment_not_found")
            elif str(((experiment.get("payload") or {}).get("status") or experiment.get("status") or "")).lower() != "active":
                reasons.append("experiment_not_active")
        if audience_mode == "provider_campaign":
            resolved_action = self._resolve_provider_connection_config(dict(channel_config or {}))
            user_ids = list(definition.get("user_ids") or [])
            if not user_ids and not self._is_live_provider_push_action(resolved_action):
                reasons.append("provider_connection_required_for_broadcast")
        policy = workflow.get("policy") or definition.get("policy") or {}
        for field in ("global_daily_limit", "channel_daily_limit", "cooldown_hours"):
            resolved_value = self._optional_int(policy.get(field))
            if resolved_value is not None and resolved_value < 0:
                reasons.append(f"invalid_{field}")
        if steps:
            has_action_step = any(str(step.get("type") or "") == "action" for step in steps)
            has_branch_action = any(str(step.get("type") or "") == "if_else" and any(isinstance((step.get(branch) or {}).get("action"), dict) for branch in ("then", "else")) for step in steps)
            if not has_action_step and not has_branch_action:
                reasons.append("workflow_steps_missing_action")
        return {"eligible": not reasons, "reasons": reasons}

    @staticmethod
    def _normalize_user_ids(user_ids: List[Any] | None) -> List[str]:
        seen: set[str] = set()
        normalized: List[str] = []
        for value in list(user_ids or []):
            user_id = str(value or "").strip()
            if not user_id or user_id in seen:
                continue
            seen.add(user_id)
            normalized.append(user_id)
        return normalized

    def _normalize_audience_config(
        self,
        *,
        audience_mode: str | None,
        cohort_id: str | None,
        user_ids: List[Any] | None,
    ) -> Dict[str, Any]:
        normalized_mode = str(audience_mode or ("cohort" if cohort_id else "provider_campaign")).strip().lower()
        if normalized_mode not in {"cohort", "provider_campaign"}:
            raise ValueError("Workflow audience_mode must be cohort or provider_campaign.")
        normalized_cohort_id = str(cohort_id or "").strip() or None
        normalized_user_ids = self._normalize_user_ids(user_ids)
        if normalized_mode == "cohort":
            if not normalized_cohort_id:
                raise ValueError("Cohort workflows require cohort_id.")
            if self.cohorts.get_cohort(normalized_cohort_id) is None:
                raise KeyError(normalized_cohort_id)
        else:
            normalized_cohort_id = None
        return {
            "audience_mode": normalized_mode,
            "cohort_id": normalized_cohort_id,
            "user_ids": normalized_user_ids,
        }

    def _workflow_uses_provider_campaign(self, workflow: Dict[str, Any]) -> bool:
        definition = workflow.get("definition") or {}
        return str(definition.get("audience_mode") or "").strip().lower() == "provider_campaign"

    def _iter_action_configs(self, workflow: Dict[str, Any]) -> List[Dict[str, Any]]:
        definition = dict(workflow.get("definition") or {})
        items: List[Dict[str, Any]] = []
        base_action = workflow.get("channel_config") or definition.get("channel_config") or definition.get("action") or {}
        if isinstance(base_action, dict):
            items.append(dict(base_action))
        for step in list(definition.get("steps") or []):
            if str(step.get("type") or "").lower() == "action" and isinstance(step.get("action"), dict):
                items.append(dict(step["action"]))
            if str(step.get("type") or "").lower() != "if_else":
                continue
            for branch_name in ("then", "else"):
                branch_payload = dict(step.get(branch_name) or {})
                if isinstance(branch_payload.get("action"), dict):
                    items.append(dict(branch_payload["action"]))
        return items

    def _assert_publishable_provider_config(self, workflow: Dict[str, Any]) -> None:
        for action in self._iter_action_configs(workflow):
            provider_connection_id = str(action.get("provider_connection_id") or "").strip()
            if provider_connection_id and self.repository.get_resource("provider_connection", provider_connection_id) is None:
                raise ValueError(f"Provider connection '{provider_connection_id}' was not found.")
            if self.settings.app_env == "prod" and contains_inline_secret(action):
                raise ValueError("Inline provider secrets are not allowed in production workflows; use provider_connection_id or *_ref fields.")

    def _parse_reference_time(self, reference_time: str | None) -> datetime:
        if not reference_time:
            return datetime.utcnow()
        try:
            parsed = datetime.fromisoformat(str(reference_time))
            if parsed.tzinfo is not None:
                return parsed.astimezone(timezone.utc).replace(tzinfo=None)
            return parsed
        except ValueError:
            return datetime.utcnow()

    def _resolve_scheduled_window(self, workflow: Dict[str, Any], trigger: Dict[str, Any]) -> tuple[int, int]:
        experiment_id = str(workflow.get("experiment_id") or (workflow.get("definition") or {}).get("experiment_id") or "").strip()
        if not experiment_id:
            return int(trigger.get("hour") or 0), int(trigger.get("minute") or 0)
        snapshot = self.experiments.get_policy_snapshot(experiment_id) or {}
        send_window = dict(snapshot.get("variant_actions", {}).get(snapshot.get("recommended_variant_id") or "", {}).get("send_window") or {})
        if not send_window:
            send_window = dict(snapshot.get("send_window") or {})
        return int(send_window.get("hour") or trigger.get("hour") or 0), int(send_window.get("minute") or trigger.get("minute") or 0)

    def _resolve_experiment_policy(
        self,
        experiment_id: str | None,
        group: str | None,
        channel_config: Dict[str, Any],
        member: Dict[str, Any],
    ) -> Dict[str, Any]:
        if not experiment_id:
            return {
                "channel_config": dict(channel_config or {}),
                "eligibility_allowed": True,
                "eligibility_reason": None,
                "variant_id": None,
                "template_id": None,
                "policy_snapshot_id": None,
            }
        snapshot = self.experiments.get_policy_snapshot(str(experiment_id)) or {}
        resolved = dict(channel_config or {})
        baseline_score = member.get("baseline_churn_score")
        if baseline_score in (None, ""):
            baseline_score = member.get("attributes", {}).get("baseline_churn_score") if isinstance(member.get("attributes"), dict) else None
        threshold = snapshot.get("eligibility_threshold")
        eligibility_allowed = True
        eligibility_reason = None
        if baseline_score not in ("", None) and threshold not in ("", None):
            eligibility_allowed = float(baseline_score) >= float(threshold)
            eligibility_reason = (
                f"baseline_churn_score {round(float(baseline_score), 4)} "
                f"{'>=' if eligibility_allowed else '<'} threshold {round(float(threshold), 4)}"
            )
        variant_id = str(group or snapshot.get("recommended_variant_id") or "").strip() or None
        variant_payload = dict((snapshot.get("variant_actions") or {}).get(str(variant_id or ""), {}) or {})
        if variant_payload:
            resolved.update({key: value for key, value in variant_payload.items() if key not in {"variant_id", "send_window"}})
        return {
            "channel_config": resolved,
            "eligibility_allowed": eligibility_allowed,
            "eligibility_reason": eligibility_reason,
            "variant_id": variant_id,
            "template_id": variant_payload.get("template_id"),
            "policy_snapshot_id": snapshot.get("policy_snapshot_id"),
        }

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

    def _has_execution_for_trigger(self, workflow_id: str, trigger_type: str) -> bool:
        for event in self.repository.list_resource_events("workflow", workflow_id, event_type="workflow_execution", limit=500):
            payload = event.get("payload") or {}
            if str(payload.get("trigger_type") or "") == str(trigger_type or "") and not self._is_test_execution(payload):
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

    def _provider_request_id(self, workflow_id: str, execution_id: str, user_id: str, channel: str) -> str:
        digest = hashlib.sha256(f"{workflow_id}:{execution_id}:{user_id}:{channel}".encode("utf-8")).hexdigest()
        return f"pr_{digest[:24]}"

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
        recorded_at: str,
    ) -> Dict[str, Any]:
        request_context = get_request_context()
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
            "user_ids": list(execution_payload.get("user_ids") or []),
            "audience_mode": execution_payload.get("audience_mode"),
            "group": execution_payload.get("group"),
            "variant_id": execution_payload.get("variant_id"),
            "template_id": execution_payload.get("template_id"),
            "policy_snapshot_id": execution_payload.get("policy_snapshot_id"),
            "channel": execution_payload.get("channel"),
            "tenant_id": execution_payload.get("tenant_id") or (request_context.tenant_id if request_context else None),
            "project_id": execution_payload.get("project_id") or (request_context.project_id if request_context else None),
            "provider": provider_result.get("provider") or channel_config.get("provider") or execution_payload.get("channel"),
            "provider_mode": provider_result.get("provider_mode") or "live",
            "provider_backend": provider_result.get("provider_backend") or provider_result.get("provider") or execution_payload.get("channel"),
            "provider_connection_id": channel_config.get("provider_connection_id"),
            "provider_campaign_id": provider_result.get("provider_campaign_id"),
            "provider_accepted": provider_result.get("accepted"),
            "fallback_reason": provider_result.get("fallback_reason"),
            "simulated": bool(provider_result.get("simulated")),
            "delivery_status": "delivered" if provider_result.get("ok") else "failed",
            "failure_reason": provider_result.get("error"),
            "provider_request": {
                "channel": channel_config.get("channel"),
                "subject": channel_config.get("subject"),
                "content": channel_config.get("content"),
                "title": channel_config.get("title"),
                "body": channel_config.get("body") or channel_config.get("content"),
                "campaign_name": channel_config.get("campaign_name"),
                "data": channel_config.get("data"),
                "deep_link": channel_config.get("deep_link"),
                "deep_link_token": channel_config.get("deep_link_token"),
                "scheduled_at": channel_config.get("scheduled_at"),
                "provider_request_id": channel_config.get("provider_request_id"),
                "provider_options": channel_config.get("provider_options"),
                "player_ids": list(execution_payload.get("user_ids") or []),
                "audience_mode": execution_payload.get("audience_mode"),
                "template_id": channel_config.get("template_id"),
                "provider_connection_id": channel_config.get("provider_connection_id"),
                "provider_mode": provider_result.get("provider_mode") or "live",
            },
            "provider_response": {
                "status_code": provider_result.get("status_code"),
                "error": provider_result.get("error"),
                "provider_backend": provider_result.get("provider_backend"),
                "fallback_reason": provider_result.get("fallback_reason"),
                "accepted": provider_result.get("accepted"),
                "campaign_id": provider_result.get("provider_campaign_id"),
                "duplicate": provider_result.get("duplicate"),
                "scheduled_at": provider_result.get("scheduled_at"),
                "response": provider_result.get("provider_response_body"),
            },
            "delivery_diagnostics": {
                "attempt_count": provider_result.get("attempt_count", 1),
                "attempts": provider_result.get("attempts", []),
                "retry_schedule_seconds": provider_result.get("retry_schedule_seconds", []),
                "failure_classification": provider_result.get("failure_classification"),
                "provider_mode": provider_result.get("provider_mode") or "live",
            },
            "callback_count": 0,
            "callback_latency_seconds": None,
            "sandbox": bool(sandbox),
            "recorded_at": recorded_at,
        }
        self.repository.upsert_resource(
            "workflow_delivery",
            delivery_id,
            status=payload["delivery_status"],
            name=workflow_id,
            payload=payload,
            tenant_id=payload.get("tenant_id"),
            project_id=payload.get("project_id"),
        )
        return payload

    def _callback_id(self, provider: str, callback: Dict[str, Any], event_type: str) -> str:
        request_context = get_request_context()
        parts = [
            str(callback.get("tenant_id") or (request_context.tenant_id if request_context else "") or "default"),
            str(callback.get("project_id") or (request_context.project_id if request_context else "") or "default"),
            str(provider),
            str(
                callback.get("delivery_id")
                or callback.get("action_execution_id")
                or callback.get("push_dispatch_id")
                or callback.get("provider_request_id")
                or callback.get("provider_campaign_id")
                or callback.get("event_id")
                or callback.get("message_id")
                or callback.get("user_id")
                or "unknown"
            ),
            str(event_type),
            str(callback.get("occurred_at") or ""),
        ]
        digest = hashlib.sha256(":".join(parts).encode("utf-8")).hexdigest()
        return f"cb_{digest[:24]}"

    def _find_delivery_for_callback(self, callback: Dict[str, Any]) -> Dict[str, Any] | None:
        callback_provider = str(callback.get("provider") or "").strip().lower()
        delivery_id = str(callback.get("delivery_id") or callback.get("action_execution_id") or "").strip()
        provider_request_id = str(callback.get("provider_request_id") or "").strip()
        provider_campaign_id = str(
            callback.get("provider_campaign_id")
            or callback.get("campaign_id")
            or ((callback.get("metadata") or {}).get("campaign_id") if isinstance(callback.get("metadata"), dict) else "")
            or ""
        ).strip()
        tenant_id = str(callback.get("tenant_id") or "").strip()
        project_id = str(callback.get("project_id") or "").strip()
        if delivery_id:
            record = self.repository.get_resource(
                "workflow_delivery",
                delivery_id,
                tenant_id=tenant_id or None,
                project_id=project_id or None,
            )
            if record is not None:
                payload = record.get("payload") or {}
                if callback_provider and str(payload.get("provider") or "").strip().lower() not in {"", callback_provider}:
                    return None
                return record
        workflow_id = str(callback.get("workflow_id") or "").strip()
        user_id = str(callback.get("user_id") or "").strip()
        for record in self.repository.list_resources(
            "workflow_delivery",
            tenant_id=tenant_id or None,
            project_id=project_id or None,
        ):
            payload = record.get("payload") or {}
            if callback_provider and str(payload.get("provider") or "").strip().lower() not in {"", callback_provider}:
                continue
            provider_request = dict(payload.get("provider_request") or {})
            if provider_request_id and str(provider_request.get("provider_request_id") or "").strip() == provider_request_id:
                return record
            if provider_campaign_id and str(payload.get("provider_campaign_id") or "").strip() == provider_campaign_id:
                return record
            if workflow_id and str(payload.get("workflow_id") or "") != workflow_id:
                continue
            if user_id and str(payload.get("user_id") or "") != user_id:
                continue
            if workflow_id or user_id:
                return record
        return None

    def _find_push_dispatch_for_callback(self, callback: Dict[str, Any]) -> Dict[str, Any] | None:
        push_dispatch_id = str(callback.get("push_dispatch_id") or "").strip()
        provider_request_id = str(callback.get("provider_request_id") or "").strip()
        provider_campaign_id = str(
            callback.get("provider_campaign_id")
            or callback.get("campaign_id")
            or ((callback.get("metadata") or {}).get("campaign_id") if isinstance(callback.get("metadata"), dict) else "")
            or ""
        ).strip()
        tenant_id = str(callback.get("tenant_id") or "").strip()
        project_id = str(callback.get("project_id") or "").strip()
        if push_dispatch_id:
            record = self.repository.get_resource(
                "push_dispatch",
                push_dispatch_id,
                tenant_id=tenant_id or None,
                project_id=project_id or None,
            )
            if record is not None:
                return record
        for record in self.repository.list_resources(
            "push_dispatch",
            tenant_id=tenant_id or None,
            project_id=project_id or None,
        ):
            payload = dict(record.get("payload") or {})
            if provider_request_id and str(payload.get("provider_request_id") or "").strip() == provider_request_id:
                return record
            if provider_campaign_id and str(payload.get("provider_campaign_id") or "").strip() == provider_campaign_id:
                return record
        return None

    def _resolve_provider_connection_config(self, action: Dict[str, Any]) -> Dict[str, Any]:
        resolved_action = materialize_secret_refs(dict(action or {}))
        provider_connection_id = str(resolved_action.get("provider_connection_id") or "").strip()
        if not provider_connection_id:
            return resolved_action
        record = self.repository.get_resource("provider_connection", provider_connection_id)
        if record is None:
            raise ValueError(f"Provider connection '{provider_connection_id}' was not found.")
        provider_payload = dict((record.get("payload") or {}).get("config") or {})
        provider_payload = materialize_secret_refs(provider_payload)
        provider_payload["provider"] = str((record.get("payload") or {}).get("provider") or "").strip().lower()
        provider_payload["provider_connection_id"] = provider_connection_id
        return {**provider_payload, **resolved_action}

    @staticmethod
    def _build_wynn_callback_context(action: Dict[str, Any]) -> Dict[str, Any]:
        callback_url = str(action.get("callback_url") or "").strip()
        callback_bearer_token = str(action.get("callback_bearer_token") or "").strip()
        callback_signing_secret = str(action.get("callback_signing_secret") or "").strip()
        if not callback_url or not callback_bearer_token:
            return {}
        payload = {
            "url": callback_url,
            "bearer_token": callback_bearer_token,
        }
        if callback_signing_secret:
            payload["signing_secret"] = callback_signing_secret
        return payload

    def _resolve_callback_secret(self, provider: str, callback: Dict[str, Any]) -> str | None:
        provider_connection_id = str(callback.get("provider_connection_id") or "").strip()
        if not provider_connection_id:
            delivery = self._find_delivery_for_callback({**callback, "provider": provider})
            delivery_payload = dict((delivery or {}).get("payload") or {})
            provider_connection_id = str(delivery_payload.get("provider_connection_id") or "").strip()
        if not provider_connection_id:
            push_dispatch = self._find_push_dispatch_for_callback(callback)
            push_dispatch_payload = dict((push_dispatch or {}).get("payload") or {})
            provider_connection_id = str(push_dispatch_payload.get("provider_connection_id") or "").strip()
        if not provider_connection_id:
            return None
        record = self.repository.get_resource("provider_connection", provider_connection_id)
        if record is None:
            raise ValueError(f"Provider connection '{provider_connection_id}' was not found.")
        config = materialize_secret_refs(dict((record.get("payload") or {}).get("config") or {}))
        return str(
            config.get("callback_signing_secret")
            or config.get("signing_secret")
            or ""
        ).strip() or None

    def _verify_callback_signature(
        self,
        provider: str,
        callbacks: List[Dict[str, Any]],
        *,
        signature: str | None,
        raw_body: bytes | None,
    ) -> None:
        secrets = {
            secret
            for secret in (self._resolve_callback_secret(provider, callback) for callback in callbacks)
            if secret
        }
        if not secrets:
            return
        if len(secrets) > 1:
            raise ValueError("Callback batch spans multiple signing secrets; send callbacks per provider connection.")
        if not signature or raw_body is None:
            raise ValueError("Signed provider callbacks require X-Kairyx-Signature.")
        provided_signature = str(signature).strip()
        if provided_signature.startswith("sha256="):
            provided_signature = provided_signature.split("=", 1)[1]
        expected_signature = hmac.new(next(iter(secrets)).encode("utf-8"), raw_body, hashlib.sha256).hexdigest()
        if not hmac.compare_digest(provided_signature, expected_signature):
            raise ValueError("Invalid provider callback signature.")

    def _callback_outcome_name(self, event_type: str, callback: Dict[str, Any]) -> str | None:
        if callback.get("outcome_name"):
            return str(callback["outcome_name"]).lower()
        mapping = {
            "opened": "opened",
            "clicked": "engaged",
            "engaged": "engaged",
            "claimed": "purchase",
            "purchase": "purchase",
            "returned": "returned",
            "reactivated": "returned",
            "returned_to_game": "returned",
            "converted": "returned",
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
        status_code = int(provider_result.get("status_code") or 0)
        if "unsupported_channel" in error or "unsupported_provider_connection" in error or "provider_config_missing" in error:
            return "internal_error"
        if "invalid_target" in error or "invalid email" in error or status_code == 422:
            return "invalid_target"
        if "timeout" in error:
            return "provider_error"
        if status_code in {401, 403}:
            return "internal_error"
        return "provider_error"

    def _execute_action_with_retry(self, action_payload: Dict[str, Any], channel_config: Dict[str, Any]) -> Dict[str, Any]:
        retry_policy = dict(channel_config.get("retry_policy") or {})
        max_retries = max(0, int(retry_policy.get("max_retries") or 0))
        base_backoff_seconds = max(1, int(retry_policy.get("base_backoff_seconds") or 1))
        attempts: List[Dict[str, Any]] = []
        final_result: Dict[str, Any] | None = None
        for attempt in range(max_retries + 1):
            result = self.executor.execute_action_detailed(action_payload)
            failure_classification = None if result.get("ok") else self._classify_provider_failure(result)
            attempts.append(
                {
                    "attempt": attempt + 1,
                    "status_code": result.get("status_code"),
                    "ok": bool(result.get("ok")),
                    "error": result.get("error"),
                    "failure_classification": failure_classification,
                    "backoff_seconds": 0 if result.get("ok") or failure_classification in {"invalid_target", "internal_error"} else (base_backoff_seconds * (2**attempt) if attempt < max_retries else 0),
                }
            )
            final_result = result
            if result.get("ok") or failure_classification in {"invalid_target", "internal_error"}:
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
        trigger_type: str,
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

        if trigger_type == "daily_schedule":
            quiet = policy.get("quiet_hours")
            if isinstance(quiet, dict):
                start_hour = self._optional_int(quiet.get("start"))
                end_hour = self._optional_int(quiet.get("end"))
                if start_hour is not None and end_hour is not None:
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

        global_daily_limit = self._optional_int(policy.get("global_daily_limit"))
        if global_daily_limit is not None and global_daily_limit > 0 and global_deliveries >= global_daily_limit:
            return {"allowed": False, "reason": "policy_blocked", "idempotency_key": key}
        channel_daily_limit = self._optional_int(policy.get("channel_daily_limit"))
        if channel_daily_limit is not None and channel_daily_limit > 0 and channel_deliveries >= channel_daily_limit:
            return {"allowed": False, "reason": "policy_blocked", "idempotency_key": key}
        cooldown_hours = self._optional_int(policy.get("cooldown_hours"))
        if cooldown_hours is not None and cooldown_hours > 0 and last_delivery_at is not None and last_delivery_at >= reference_time - timedelta(hours=cooldown_hours):
            return {"allowed": False, "reason": "policy_blocked", "idempotency_key": key}

        max_deliveries = self._resolve_budget_delivery_limit(budget_policy)
        if max_deliveries is not None and max_deliveries > 0:
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

        if self._workflow_uses_provider_campaign(workflow):
            return self._execute_provider_campaign_workflow(
                workflow,
                limit=limit,
                sandbox=sandbox,
                manual_test=manual_test,
                reference_time=reference_time,
                trigger_type=trigger_type,
            )

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
            "tenant_id": workflow.get("tenant_id"),
            "project_id": workflow.get("project_id"),
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
            "recorded_at": reference_time.isoformat(),
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
                assignment = self.experiments.assign_user(
                    experiment_id,
                    member.get("canonical_user_id"),
                    assigned_at=reference_time.isoformat(),
                )
                group = assignment["group"]

            step_resolution = self._resolve_channel_config_from_steps(
                member=member,
                base_channel_config=channel_config,
                steps=workflow_steps,
                group=group,
            )
            resolved_channel_config = dict(step_resolution.get("channel_config") or channel_config)
            policy_resolution = self._resolve_experiment_policy(
                str(experiment_id) if experiment_id else None,
                group,
                resolved_channel_config,
                member,
            )
            resolved_channel_config = dict(policy_resolution.get("channel_config") or resolved_channel_config)
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
                trigger_type=summary["trigger_type"],
            )

            execution_payload = {
                "execution_id": execution_id,
                "workflow_id": workflow["workflow_id"],
                "workflow_version": summary["workflow_version"],
                "cohort_id": definition.get("cohort_id"),
                "cohort_snapshot_id": snapshot_id,
                "user_id": member.get("canonical_user_id"),
                "channel": resolved_channel_config.get("channel", channel_config.get("channel", "push_notification")),
                "tenant_id": workflow.get("tenant_id"),
                "project_id": workflow.get("project_id"),
                "execution_status": "pending",
                "group": group,
                "variant_id": policy_resolution.get("variant_id"),
                "template_id": policy_resolution.get("template_id") or resolved_channel_config.get("template_id"),
                "policy_snapshot_id": policy_resolution.get("policy_snapshot_id"),
                "sandbox": bool(sandbox),
                "trigger_type": summary["trigger_type"],
                "recorded_at": reference_time.isoformat(),
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

            if not policy_resolution.get("eligibility_allowed", True):
                execution_payload["execution_status"] = "filtered_out"
                execution_payload["reason"] = "eligibility_threshold"
                execution_payload["eligibility_reason"] = policy_resolution.get("eligibility_reason")
                summary["filtered_out"] += 1
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
                            max_deliveries=self._resolve_budget_delivery_limit(budget_policy),
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
                        "variant_id": execution_payload.get("variant_id"),
                        "template_id": execution_payload.get("template_id"),
                        "exposed_at": reference_time.isoformat(),
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
            action = self._resolve_provider_connection_config(action)
            action = self._validate_action_for_execution(action, workflow_name=str(workflow.get("name") or workflow["workflow_id"]))
            execution_payload["channel"] = action.get("channel", execution_payload["channel"])
            execution_payload["tenant_id"] = workflow.get("tenant_id")
            execution_payload["project_id"] = workflow.get("project_id")
            recipient = member.get("email") if str(action.get("channel") or "") == "email" else member.get("canonical_user_id")
            provider_request_id = self._provider_request_id(
                workflow["workflow_id"],
                execution_id,
                str(execution_payload.get("user_id") or ""),
                str(action.get("channel") or execution_payload["channel"]),
            )
            action["provider_request_id"] = provider_request_id
            outbound_context = {
                "workflow_id": workflow["workflow_id"],
                "execution_id": execution_id,
                "provider_connection_id": action.get("provider_connection_id"),
                "tenant_id": workflow.get("tenant_id"),
                "project_id": workflow.get("project_id"),
            }
            if self._is_live_provider_push_action(action):
                callback_context = self._build_wynn_callback_context(action)
                if callback_context:
                    outbound_context["kairyx_callback"] = callback_context
            action_payload = {
                "decision": "ACT",
                "channel": action.get("channel", "push_notification"),
                "content": action.get("content", ""),
                "title": action.get("title"),
                "body": action.get("body") or action.get("content", ""),
                "campaign_name": action.get("campaign_name") or workflow.get("name") or workflow["workflow_id"],
                "data": dict(action.get("data") or {}),
                "deep_link": action.get("deep_link"),
                "deep_link_token": action.get("deep_link_token") or action.get("default_deep_link_token"),
                "scheduled_at": action.get("scheduled_at"),
                "provider_options": dict(action.get("provider_options") or {}),
                "subject": action.get("subject", "KairyxAI"),
                "player_id": recipient or member.get("canonical_user_id"),
                "api_key": action.get("api_key"),
                "api_token": action.get("api_token"),
                "base_url": action.get("base_url"),
                "provider": action.get("provider"),
                "from_email": action.get("from_email"),
                "rest_endpoint": action.get("rest_endpoint"),
                "webhook_url": action.get("webhook_url"),
                "webhook_token": action.get("webhook_token"),
                "provider_connection_id": action.get("provider_connection_id"),
                "provider_request_id": provider_request_id,
                "workflow_id": workflow["workflow_id"],
                "execution_id": execution_id,
                "tenant_id": workflow.get("tenant_id"),
                "project_id": workflow.get("project_id"),
                "context": outbound_context,
                "metadata": dict(action.get("metadata") or {}),
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
                recorded_at=reference_time.isoformat(),
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
                    last_delivery_at=reference_time.isoformat(),
                )
                self._upsert_budget_state(
                    workflow["workflow_id"],
                    action_date,
                    consumed=True,
                    max_deliveries=self._resolve_budget_delivery_limit(budget_policy),
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
                            "variant_id": delivery_payload.get("variant_id") or execution_payload.get("variant_id"),
                            "template_id": delivery_payload.get("template_id") or execution_payload.get("template_id"),
                            "exposed_at": reference_time.isoformat(),
                        },
                    )
            summary["results"].append(delivery_payload)

        self.repository.record_resource_event("workflow", workflow["workflow_id"], event_type="workflow_execution", payload=summary)
        self.repository.record_action("workflow_execution_completed", "workflow", workflow["workflow_id"], summary)
        return summary

    def _execute_provider_campaign_workflow(
        self,
        workflow: Dict[str, Any],
        *,
        limit: int,
        sandbox: bool,
        manual_test: bool,
        reference_time: datetime,
        trigger_type: str | None,
    ) -> Dict[str, Any]:
        definition = workflow.get("definition") or {}
        channel_config = workflow.get("channel_config") or definition.get("channel_config") or definition.get("action") or {}
        action = self._resolve_provider_connection_config(dict(channel_config))
        action = self._validate_action_for_execution(action, workflow_name=str(workflow.get("name") or workflow["workflow_id"]))

        configured_user_ids = self._normalize_user_ids(definition.get("user_ids") or [])
        if not configured_user_ids and not self._is_live_provider_push_action(action):
            raise ValueError("Provider campaign broadcasts require a live Wynn PushNotifier provider connection.")

        execution_id = f"run_{uuid.uuid4().hex[:20]}"
        action_date = reference_time.date().isoformat()
        audience_mode = "explicit_user_ids" if configured_user_ids else "provider_broadcast_all_players"
        summary = {
            "execution_id": execution_id,
            "workflow_id": workflow["workflow_id"],
            "workflow_version": workflow.get("published_version") or workflow.get("current_version") or 1,
            "tenant_id": workflow.get("tenant_id"),
            "project_id": workflow.get("project_id"),
            "sandbox": bool(sandbox),
            "trigger_type": trigger_type or ("manual_test" if manual_test else "daily_schedule"),
            "action_date": action_date,
            "cohort_snapshot_id": None,
            "audience_mode": audience_mode,
            "triggered": len(configured_user_ids) if configured_user_ids else 1,
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
            "recorded_at": reference_time.isoformat(),
        }

        provider_request_id = self._provider_request_id(
            workflow["workflow_id"],
            execution_id,
            "",
            str(action.get("channel") or "push_notification"),
        )
        action["provider_request_id"] = provider_request_id
        outbound_context = {
            "workflow_id": workflow["workflow_id"],
            "execution_id": execution_id,
            "provider_connection_id": action.get("provider_connection_id"),
            "tenant_id": workflow.get("tenant_id"),
            "project_id": workflow.get("project_id"),
            "audience_mode": audience_mode,
        }
        if self._is_live_provider_push_action(action):
            callback_context = self._build_wynn_callback_context(action)
            if callback_context:
                outbound_context["kairyx_callback"] = callback_context
        action_payload = {
            "decision": "ACT",
            "channel": action.get("channel", "push_notification"),
            "content": action.get("content", ""),
            "title": action.get("title"),
            "body": action.get("body") or action.get("content", ""),
            "campaign_name": action.get("campaign_name") or workflow.get("name") or workflow["workflow_id"],
            "data": dict(action.get("data") or {}),
            "deep_link": action.get("deep_link"),
            "deep_link_token": action.get("deep_link_token") or action.get("default_deep_link_token"),
            "scheduled_at": action.get("scheduled_at"),
            "provider_options": dict(action.get("provider_options") or {}),
            "subject": action.get("subject", "KairyxAI"),
            "player_id": list(configured_user_ids),
            "player_ids": list(configured_user_ids),
            "audience_mode": audience_mode,
            "api_key": action.get("api_key"),
            "api_token": action.get("api_token"),
            "base_url": action.get("base_url"),
            "provider": action.get("provider"),
            "from_email": action.get("from_email"),
            "rest_endpoint": action.get("rest_endpoint"),
            "webhook_url": action.get("webhook_url"),
            "webhook_token": action.get("webhook_token"),
            "provider_connection_id": action.get("provider_connection_id"),
            "provider_request_id": provider_request_id,
            "workflow_id": workflow["workflow_id"],
            "execution_id": execution_id,
            "tenant_id": workflow.get("tenant_id"),
            "project_id": workflow.get("project_id"),
            "context": outbound_context,
            "metadata": dict(action.get("metadata") or {}),
        }

        provider_result = self._execute_action_with_retry(action_payload, action)
        summary["executed"] = 1
        execution_payload = {
            "execution_id": execution_id,
            "workflow_id": workflow["workflow_id"],
            "workflow_version": summary["workflow_version"],
            "cohort_id": None,
            "cohort_snapshot_id": None,
            "user_id": configured_user_ids[0] if len(configured_user_ids) == 1 else None,
            "user_ids": configured_user_ids,
            "channel": action.get("channel", "push_notification"),
            "tenant_id": workflow.get("tenant_id"),
            "project_id": workflow.get("project_id"),
            "execution_status": "executed" if provider_result.get("ok") else "failed",
            "group": None,
            "variant_id": None,
            "template_id": None,
            "policy_snapshot_id": None,
            "sandbox": bool(sandbox),
            "trigger_type": summary["trigger_type"],
            "recorded_at": reference_time.isoformat(),
            "audience_mode": audience_mode,
            "step_trace": [],
        }
        self.repository.record_resource_event("workflow", workflow["workflow_id"], event_type="action_execution", payload=execution_payload)
        delivery_payload = self._persist_delivery(
            workflow_id=workflow["workflow_id"],
            cohort_id=None,
            experiment_id=None,
            execution_payload=execution_payload,
            channel_config=action,
            provider_result=provider_result,
            sandbox=sandbox,
            recorded_at=reference_time.isoformat(),
        )

        if not provider_result.get("ok"):
            summary["failures"] = 1
            self.repository.record_resource_event(
                "workflow",
                workflow["workflow_id"],
                event_type="action_delivery",
                payload={**execution_payload, **delivery_payload, "delivery_status": "failed", "failure_reason": str(provider_result.get("failure_classification") or "provider_error")},
            )
            summary["results"].append(delivery_payload)
        else:
            summary["success"] = 1
            self.repository.record_resource_event("workflow", workflow["workflow_id"], event_type="action_delivery", payload=delivery_payload)
            summary["results"].append(delivery_payload)

        self.repository.record_resource_event("workflow", workflow["workflow_id"], event_type="workflow_execution", payload=summary)
        self.repository.record_action("workflow_execution_completed", "workflow", workflow["workflow_id"], summary)
        return summary

    def _finalize_one_time_workflow(self, workflow_id: str, *, completed_at: str, run_result: Dict[str, Any]) -> None:
        record = self.repository.get_resource("workflow", workflow_id)
        if record is None:
            return
        payload = dict(record.get("payload") or {})
        success_count = int(run_result.get("success") or 0)
        failure_count = int(run_result.get("failures") or 0)
        completion_status = "sent"
        if success_count > 0 and failure_count > 0:
            completion_status = "sent_with_errors"
        elif success_count <= 0 and failure_count > 0:
            completion_status = "failed"
        payload["status"] = completion_status
        payload["completed_at"] = completed_at
        payload["completion_reason"] = "one_time_schedule_completed"
        self.repository.upsert_resource("workflow", workflow_id, status=completion_status, name=payload.get("name"), payload=payload)
        self.repository.record_resource_event(
            "workflow",
            workflow_id,
            event_type="workflow_completed",
            payload={"status": completion_status, "completed_at": completed_at, "reason": "one_time_schedule_completed"},
        )

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
