from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any, Dict, List

from app.application.cohorts import CohortService
from app.application.copilot import CopilotService
from app.application.experiments import ExperimentConfigService
from app.application.workflows import WorkflowService


class ScenarioTemplateService:
    def __init__(self, repository):
        self.repository = repository
        self.cohorts = CohortService(repository)
        self.experiments = ExperimentConfigService(repository)
        self.workflows = WorkflowService(repository)
        self.copilot = CopilotService(repository)

    def list_templates(self) -> Dict[str, Any]:
        return {"items": [self._template_summary(item) for item in self._templates()]}

    def get_template(self, template_id: str) -> Dict[str, Any] | None:
        return next((item for item in self._templates() if item["template_id"] == template_id), None)

    def instantiate(
        self,
        template_id: str,
        *,
        name_prefix: str | None = None,
        owner: str = "system",
        activate_cohort: bool = True,
        publish_workflow: bool = False,
    ) -> Dict[str, Any]:
        template = self.get_template(template_id)
        if template is None:
            raise KeyError(template_id)
        suffix = uuid.uuid4().hex[:6]
        prefix = str(name_prefix or template["name"]).strip() or template["name"]
        cohort_name = f"{prefix}_{suffix}_cohort"
        experiment_id = f"{template_id}_{suffix}"
        workflow_name = f"{prefix}_{suffix}_workflow"

        cohort = self.cohorts.create_cohort(
            name=cohort_name,
            cohort_type=template["cohort_template"]["type"],
            definition=dict(template["cohort_template"]["definition"]),
            refresh_mode=template["cohort_template"].get("refresh_mode") or "daily",
            owner=owner,
            description=template["description"],
            tags=list(template["cohort_template"].get("tags") or [template_id]),
            activate=bool(activate_cohort),
        )
        experiment_payload = {
            **template["experiment_template"],
            "experiment_id": experiment_id,
            "cohort_id": cohort["cohort_id"],
        }
        experiment = self.experiments.save_config(experiment_payload, experiment_id=experiment_id)
        workflow = self.workflows.create_workflow(
            name=workflow_name,
            cohort_id=cohort["cohort_id"],
            schedule=template["workflow_template"]["trigger"],
            action=template["workflow_template"]["channel_config"],
            policy=template["workflow_template"].get("policy") or {},
            budget_policy=template["workflow_template"].get("budget_policy") or {},
            trigger=template["workflow_template"]["trigger"],
            channel_config=template["workflow_template"]["channel_config"],
            experiment_id=experiment_id,
            requires_confirmation=bool(template["workflow_template"].get("requires_confirmation")),
            steps=list(template["workflow_template"].get("steps") or []),
        )
        if publish_workflow and activate_cohort:
            workflow = self.workflows.publish_workflow(workflow["workflow_id"])

        report_template_id = f"reporttpl_{template_id}_{suffix}"
        report_template = {
            "report_template_id": report_template_id,
            "template_id": template_id,
            "report_type": template["copilot_template"]["report_type"],
            "time_window": template["copilot_template"]["time_window"],
            "scenario": template["name"],
            "workflow_id": workflow["workflow_id"],
            "cohort_id": cohort["cohort_id"],
            "experiment_id": experiment_id,
            "created_at": datetime.utcnow().isoformat(),
        }
        self.repository.upsert_resource(
            "copilot_report_template",
            report_template_id,
            status="ready",
            name=template["name"],
            payload=report_template,
        )
        instance_id = f"tmplinst_{uuid.uuid4().hex[:20]}"
        instance = {
            "instance_id": instance_id,
            "template_id": template_id,
            "scenario": template["name"],
            "cohort": cohort,
            "workflow": workflow,
            "experiment": experiment,
            "report_template": report_template,
            "created_at": datetime.utcnow().isoformat(),
        }
        self.repository.upsert_resource(
            "scenario_template_instance",
            instance_id,
            status="ready",
            name=template["name"],
            payload=instance,
        )
        self.repository.record_action("scenario_template_instantiated", "scenario_template", template_id, instance)
        return instance

    @staticmethod
    def _template_summary(item: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "template_id": item["template_id"],
            "name": item["name"],
            "description": item["description"],
            "status": item["status"],
            "scenario_type": item["scenario_type"],
            "channels": item["channels"],
            "report_type": item["copilot_template"]["report_type"],
        }

    @staticmethod
    def _templates() -> List[Dict[str, Any]]:
        return [
            {
                "template_id": "churn_rescue",
                "name": "Churn Rescue",
                "description": "Recover high-risk users with daily win-back messaging and weekly experiment readout.",
                "status": "recommended",
                "scenario_type": "retention",
                "channels": ["push_notification", "email"],
                "cohort_template": {
                    "type": "sql",
                    "refresh_mode": "daily",
                    "definition": {
                        "sql": (
                            "SELECT canonical_user_id, email, predicted_churn_risk, churn_state, "
                            "baseline_churn_score, recommended_template_id, recommended_variant "
                            "FROM prediction_results "
                            "WHERE predicted_churn_risk = 'high' AND COALESCE(churn_state, 'active') != 'churned'"
                        )
                    },
                    "tags": ["churn", "rescue"],
                },
                "workflow_template": {
                    "trigger": {"type": "daily_schedule", "hour": 9, "minute": 0},
                    "channel_config": {"channel": "push_notification", "content": "Return today for a win-back reward."},
                    "policy": {"global_daily_limit": 5, "channel_daily_limit": 3, "cooldown_hours": 24},
                    "budget_policy": {"daily_delivery_limit": 2000},
                    "requires_confirmation": False,
                    "steps": [
                        {"type": "filter", "conditions": [{"field": "predicted_churn_risk", "op": "=", "value": "high"}]},
                        {"type": "wait", "seconds": 900},
                        {"type": "action", "action": {"channel": "push_notification", "content": "Return today for a win-back reward."}},
                    ],
                },
                "experiment_template": {
                    "enabled": True,
                    "primary_metric": "return_rate",
                    "guardrail_metrics": ["engagement_rate", "policy_block_rate"],
                    "min_sample_size": 20,
                    "min_runtime_hours": 24,
                    "holdout_pct": 0.1,
                    "b_variant_pct": 0.0,
                    "scenario_type": "churn_rescue",
                    "optimization_mode": "fixed_ab",
                    "holdout_floor_pct": 0.1,
                    "max_daily_shift_pct": 0.1,
                    "approved_variants": [
                        {
                            "variant_id": "treatment_a",
                            "template_id": "churn_rescue_push_a",
                            "channel": "push_notification",
                            "content": "Return today for a win-back reward.",
                            "send_window": {"hour": 9, "minute": 0},
                        },
                        {
                            "variant_id": "treatment_b",
                            "template_id": "churn_rescue_push_b",
                            "channel": "push_notification",
                            "content": "Come back now to claim your comeback bonus.",
                            "send_window": {"hour": 11, "minute": 0},
                        },
                    ],
                    "rollout_policy": "conservative",
                    "multiple_comparisons_method": "none",
                },
                "copilot_template": {"report_type": "weekly", "time_window": "7d"},
            },
            {
                "template_id": "monetization_lift",
                "name": "Monetization Lift",
                "description": "Target existing payers and warm prospects with monetization nudges tied to purchase uplift.",
                "status": "beta",
                "scenario_type": "monetization",
                "channels": ["email", "braze"],
                "cohort_template": {
                    "type": "rule",
                    "refresh_mode": "daily",
                    "definition": {
                        "source_alias": "mart_user_daily",
                        "logic": "AND",
                        "conditions": [
                            {"field": "lifetime_revenue_usd", "op": ">", "value": 0},
                            {"field": "days_since_last_seen", "op": "<=", "value": 14},
                        ],
                    },
                    "tags": ["monetization", "lift"],
                },
                "workflow_template": {
                    "trigger": {"type": "daily_schedule", "hour": 12, "minute": 0},
                    "channel_config": {"channel": "email", "subject": "Limited-time offer", "content": "Claim today’s bonus pack before it expires."},
                    "policy": {"global_daily_limit": 8, "channel_daily_limit": 2, "cooldown_hours": 48},
                    "budget_policy": {"daily_delivery_limit": 1500},
                    "requires_confirmation": True,
                    "steps": [
                        {"type": "filter", "conditions": [{"field": "lifetime_revenue_usd", "op": ">", "value": 0}]},
                        {
                            "type": "if_else",
                            "condition": {"field": "days_since_last_seen", "op": "<=", "value": 3},
                            "then": {"action": {"channel": "email", "subject": "VIP pack unlocked", "content": "Your VIP pack is ready today."}},
                            "else": {"action": {"channel": "email", "subject": "Come back for bonus gems", "content": "Return now to unlock a monetization bonus."}},
                        },
                    ],
                },
                "experiment_template": {
                    "enabled": True,
                    "primary_metric": "revenue_usd",
                    "guardrail_metrics": ["engagement_rate", "policy_block_rate"],
                    "min_sample_size": 25,
                    "min_runtime_hours": 24,
                    "holdout_pct": 0.1,
                    "b_variant_pct": 0.2,
                    "scenario_type": "monetization_lift",
                    "rollout_policy": "balanced",
                    "multiple_comparisons_method": "holm_bonferroni",
                },
                "copilot_template": {"report_type": "daily", "time_window": "7d"},
            },
            {
                "template_id": "onboarding_activation",
                "name": "Onboarding Activation",
                "description": "Re-engage new users who have not completed key onboarding milestones in the first week.",
                "status": "beta",
                "scenario_type": "activation",
                "channels": ["push_notification", "braze"],
                "cohort_template": {
                    "type": "rule",
                    "refresh_mode": "daily",
                    "definition": {
                        "source_alias": "mart_user_daily",
                        "logic": "AND",
                        "conditions": [
                            {"field": "sessions_7d", "op": "<=", "value": 2},
                            {"field": "days_since_last_seen", "op": "<=", "value": 7},
                        ],
                    },
                    "tags": ["onboarding", "activation"],
                },
                "workflow_template": {
                    "trigger": {"type": "daily_schedule", "hour": 16, "minute": 0},
                    "channel_config": {"channel": "push_notification", "content": "Complete the tutorial today to unlock starter rewards."},
                    "policy": {"global_daily_limit": 5, "channel_daily_limit": 2, "cooldown_hours": 24},
                    "budget_policy": {"daily_delivery_limit": 2500},
                    "requires_confirmation": False,
                    "steps": [
                        {"type": "filter", "conditions": [{"field": "sessions_7d", "op": "<=", "value": 2}]},
                        {"type": "wait", "seconds": 300},
                        {"type": "action", "action": {"channel": "push_notification", "content": "Complete the tutorial today to unlock starter rewards."}},
                    ],
                },
                "experiment_template": {
                    "enabled": True,
                    "primary_metric": "sessions_7d",
                    "guardrail_metrics": ["engagement_rate", "policy_block_rate"],
                    "min_sample_size": 20,
                    "min_runtime_hours": 12,
                    "holdout_pct": 0.1,
                    "b_variant_pct": 0.0,
                    "scenario_type": "onboarding_activation",
                    "rollout_policy": "aggressive",
                    "multiple_comparisons_method": "none",
                },
                "copilot_template": {"report_type": "daily", "time_window": "7d"},
            },
        ]
