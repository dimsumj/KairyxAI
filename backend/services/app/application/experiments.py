from __future__ import annotations

import hashlib
from datetime import datetime
from typing import Any, Dict, List


class ExperimentConfigService:
    def __init__(self, repository):
        self.repository = repository

    def _default_config(self, experiment_id: str = "churn_engagement_v1") -> Dict[str, Any]:
        return {
            "experiment_id": experiment_id,
            "enabled": True,
            "holdout_pct": 0.10,
            "b_variant_pct": 0.0,
            "primary_metric": "return_rate",
            "guardrail_metrics": ["engagement_rate", "policy_block_rate"],
            "min_sample_size": 20,
            "min_runtime_hours": 24,
            "cohort_id": None,
            "blacklist_user_ids": [],
            "status": "draft",
        }

    def _get_experiment_record(self, experiment_id: str) -> Dict[str, Any] | None:
        return self.repository.get_resource("experiment", experiment_id)

    def _save_experiment_record(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        existing = self.repository.list_resource_versions("experiment", payload["experiment_id"])
        next_version = 1 + max((int(item.get("version") or 0) for item in existing), default=0)
        record = self.repository.upsert_resource(
            "experiment",
            payload["experiment_id"],
            status=str(payload.get("status") or "draft"),
            name=payload.get("experiment_id"),
            payload=payload,
        )
        self.repository.create_resource_version("experiment", payload["experiment_id"], version=next_version, payload=payload)
        return record

    def get_config(self, experiment_id: str = "churn_engagement_v1") -> Dict[str, Any]:
        record = self._get_experiment_record(experiment_id)
        if record is not None:
            return record.get("payload") or {}
        if experiment_id == "churn_engagement_v1":
            legacy = self.repository.get_experiment_config()
            if legacy:
                payload = {**self._default_config(experiment_id), **legacy}
                payload.pop("b_variant_pct", None)
                self._save_experiment_record(payload)
                return payload
        return self._default_config(experiment_id)

    def save_config(self, config: Dict[str, Any], experiment_id: str | None = None) -> Dict[str, Any]:
        resolved_id = str(experiment_id or config.get("experiment_id") or "churn_engagement_v1")
        payload = {**self.get_config(resolved_id), **config, "experiment_id": resolved_id}
        if payload.get("enabled") is True and payload.get("status") == "draft":
            payload["status"] = "active"
        record = self._save_experiment_record(payload)
        if resolved_id == "churn_engagement_v1":
            self.repository.save_experiment_config(payload)
        self.repository.record_action("experiment_config_saved", "experiment", resolved_id, payload)
        return record.get("payload") or payload

    def start(self, experiment_id: str) -> Dict[str, Any]:
        payload = {**self.get_config(experiment_id), "status": "active", "started_at": datetime.utcnow().isoformat()}
        return self._save_experiment_record(payload).get("payload") or payload

    def stop(self, experiment_id: str) -> Dict[str, Any]:
        payload = {**self.get_config(experiment_id), "status": "stopped", "stopped_at": datetime.utcnow().isoformat()}
        return self._save_experiment_record(payload).get("payload") or payload

    def assign_user(self, experiment_id: str, user_id: Any) -> Dict[str, Any]:
        config = self.get_config(experiment_id)
        user_text = str(user_id)
        if user_text in {str(item) for item in config.get("blacklist_user_ids") or []}:
            assignment = {
                "experiment_id": experiment_id,
                "user_id": user_text,
                "bucket": None,
                "group": "excluded",
                "assigned_at": datetime.utcnow().isoformat(),
            }
            self.repository.upsert_resource(
                "experiment_assignment",
                f"{experiment_id}:{user_text}",
                status="excluded",
                name=experiment_id,
                payload=assignment,
            )
            self.repository.record_resource_event("experiment", experiment_id, event_type="assignment", payload=assignment)
            return assignment
        key = f"{experiment_id}:{user_text}".encode("utf-8")
        bucket = int(hashlib.sha256(key).hexdigest()[:8], 16) % 10000 / 10000.0
        holdout_pct = float(config.get("holdout_pct") or 0.10)
        treatment_b_pct = max(0.0, min(1.0, float(config.get("b_variant_pct") or 0.0)))
        if bucket < holdout_pct:
            group = "holdout"
        else:
            remainder = max(0.0, 1.0 - holdout_pct)
            normalized_bucket = ((bucket - holdout_pct) / remainder) if remainder else 0.0
            group = "treatment_b" if treatment_b_pct > 0 and normalized_bucket >= (1.0 - treatment_b_pct) else "treatment_a"
        assignment = {
            "experiment_id": experiment_id,
            "user_id": user_text,
            "bucket": bucket,
            "group": group,
            "assigned_at": datetime.utcnow().isoformat(),
        }
        self.repository.upsert_resource(
            "experiment_assignment",
            f"{experiment_id}:{user_text}",
            status=group,
            name=experiment_id,
            payload=assignment,
        )
        self.repository.record_resource_event("experiment", experiment_id, event_type="assignment", payload=assignment)
        return assignment

    def record_exposure(self, experiment_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        return self.repository.record_resource_event("experiment", experiment_id, event_type="exposure", payload=payload)

    def record_outcome(self, experiment_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        return self.repository.record_resource_event("experiment", experiment_id, event_type="outcome", payload=payload)

    def ingest_outcomes(self, experiment_id: str, outcomes: List[Dict[str, Any]]) -> Dict[str, Any]:
        items = []
        for payload in outcomes:
            item = dict(payload)
            item["experiment_id"] = experiment_id
            items.append(self.record_outcome(experiment_id, item).get("payload") or item)
        self.repository.record_action("experiment_outcomes_ingested", "experiment", experiment_id, {"count": len(items)})
        return {"experiment_id": experiment_id, "ingested": len(items), "items": items}

    def list_exposures(self, experiment_id: str) -> List[Dict[str, Any]]:
        return [item.get("payload") or {} for item in self.repository.list_resource_events("experiment", experiment_id, event_type="exposure", limit=5000)]

    def list_outcomes(self, experiment_id: str) -> List[Dict[str, Any]]:
        return [item.get("payload") or {} for item in self.repository.list_resource_events("experiment", experiment_id, event_type="outcome", limit=5000)]

    def list_assignments(self, experiment_id: str) -> List[Dict[str, Any]]:
        items = []
        for record in self.repository.list_resources("experiment_assignment"):
            payload = record.get("payload") or {}
            if str(payload.get("experiment_id") or "") == experiment_id:
                items.append(payload)
        return items

    def list_versions(self, experiment_id: str) -> Dict[str, Any]:
        return {
            "experiment_id": experiment_id,
            "items": self.repository.list_resource_versions("experiment", experiment_id),
        }

    def get_summary(self, experiment_id: str) -> Dict[str, Any]:
        config = self.get_config(experiment_id)
        exposures = self.list_exposures(experiment_id)
        outcomes = self.list_outcomes(experiment_id)
        outcomes_by_key: Dict[str, List[Dict[str, Any]]] = {}
        for item in outcomes:
            key = str(item.get("action_execution_id") or item.get("user_id") or "")
            outcomes_by_key.setdefault(key, []).append(item)

        groups: Dict[str, Dict[str, Any]] = {
            "holdout": {"n": 0, "engaged": 0, "returned": 0, "policy_blocked": 0},
            "treatment_a": {"n": 0, "engaged": 0, "returned": 0, "policy_blocked": 0},
            "treatment_b": {"n": 0, "engaged": 0, "returned": 0, "policy_blocked": 0},
        }
        first_exposure_at: datetime | None = None
        for exposure in exposures:
            group = str(exposure.get("group") or "holdout")
            if group == "excluded":
                continue
            groups.setdefault(group, {"n": 0, "engaged": 0, "returned": 0, "policy_blocked": 0})
            groups[group]["n"] += 1
            if str(exposure.get("execution_status") or "") == "policy_blocked":
                groups[group]["policy_blocked"] += 1
            key = str(exposure.get("action_execution_id") or exposure.get("user_id") or "")
            for outcome in outcomes_by_key.get(key, []):
                outcome_name = str(outcome.get("outcome_name") or outcome.get("simulated_response") or "").lower()
                if outcome_name in {"opened", "engaged"}:
                    groups[group]["engaged"] += 1
                if outcome_name in {"returned", "returned_to_game"}:
                    groups[group]["returned"] += 1
            ts = exposure.get("recorded_at") or exposure.get("exposed_at")
            try:
                parsed = datetime.fromisoformat(str(ts))
            except Exception:
                parsed = None
            if parsed is not None and (first_exposure_at is None or parsed < first_exposure_at):
                first_exposure_at = parsed

        total = sum(item["n"] for item in groups.values())
        holdout_pct = float(config.get("holdout_pct") or 0.10)
        treatment_b_pct = max(0.0, min(1.0, float(config.get("b_variant_pct") or 0.0)))
        expected_treatment_b = max(0.0, (1.0 - holdout_pct) * treatment_b_pct)
        expected = {
            "holdout": holdout_pct,
            "treatment_a": max(0.0, 1.0 - holdout_pct - expected_treatment_b),
            "treatment_b": expected_treatment_b,
        }
        srm_detected = False
        if total > 0:
            for group_name, stats in groups.items():
                if expected.get(group_name, 0.0) == 0.0 and stats["n"] == 0:
                    continue
                observed = stats["n"] / total
                if abs(observed - expected.get(group_name, 0.0)) > 0.20:
                    srm_detected = True
                    break

        def _rate(numerator: int, denominator: int) -> float:
            return round((float(numerator) / denominator), 4) if denominator else 0.0

        summary_groups: Dict[str, Dict[str, Any]] = {}
        combined_treatment_n = groups["treatment_a"]["n"] + groups["treatment_b"]["n"]
        combined_treatment_returned = groups["treatment_a"]["returned"] + groups["treatment_b"]["returned"]
        combined_treatment_engaged = groups["treatment_a"]["engaged"] + groups["treatment_b"]["engaged"]
        combined_treatment_blocked = groups["treatment_a"]["policy_blocked"] + groups["treatment_b"]["policy_blocked"]
        holdout_return_rate = _rate(groups["holdout"]["returned"], groups["holdout"]["n"])
        treatment_return_rate = _rate(combined_treatment_returned, combined_treatment_n)
        for group_name, stats in groups.items():
            summary_groups[group_name] = {
                **stats,
                "engagement_rate": _rate(stats["engaged"], stats["n"]),
                "return_rate": _rate(stats["returned"], stats["n"]),
                "policy_block_rate": _rate(stats["policy_blocked"], stats["n"]),
            }
        summary_groups["treatment"] = {
            "n": combined_treatment_n,
            "engaged": combined_treatment_engaged,
            "returned": combined_treatment_returned,
            "policy_blocked": combined_treatment_blocked,
            "engagement_rate": _rate(combined_treatment_engaged, combined_treatment_n),
            "return_rate": _rate(combined_treatment_returned, combined_treatment_n),
            "policy_block_rate": _rate(combined_treatment_blocked, combined_treatment_n),
        }
        uplift = round(treatment_return_rate - holdout_return_rate, 4)

        runtime_hours = 0.0
        if first_exposure_at is not None:
            runtime_hours = max(0.0, round((datetime.utcnow() - first_exposure_at).total_seconds() / 3600.0, 2))
        min_sample = int(config.get("min_sample_size") or 20)
        min_runtime_hours = int(config.get("min_runtime_hours") or 24)

        guardrails = []
        for metric_name in list(config.get("guardrail_metrics") or [])[:2]:
            treatment_value = summary_groups.get("treatment", {}).get(metric_name, 0.0)
            holdout_value = summary_groups.get("holdout", {}).get(metric_name, 0.0)
            status = "pass"
            if metric_name == "policy_block_rate" and treatment_value > 0.25:
                status = "fail"
            elif metric_name == "engagement_rate" and treatment_value < holdout_value:
                status = "warn"
            guardrails.append(
                {
                    "metric": metric_name,
                    "treatment": treatment_value,
                    "holdout": holdout_value,
                    "status": status,
                }
            )

        decision = "neutral"
        decision_reason = "No material uplift over holdout."
        if srm_detected:
            decision = "invalid"
            decision_reason = "SRM detected between treatment and holdout."
        elif total < min_sample:
            decision = "inconclusive"
            decision_reason = "Sample size below minimum threshold."
        elif runtime_hours < min_runtime_hours:
            decision = "inconclusive"
            decision_reason = "Runtime below minimum threshold."
        elif any(item["status"] == "fail" for item in guardrails):
            decision = "invalid"
            decision_reason = "Guardrail failure detected."
        elif uplift > 0:
            decision = "winner"
            decision_reason = "Treatment outperformed holdout on return rate."

        summary = {
            "experiment_id": experiment_id,
            "status": config.get("status") or "draft",
            "primary_metric": config.get("primary_metric") or "return_rate",
            "guardrail_metrics": config.get("guardrail_metrics") or [],
            "min_sample_size": min_sample,
            "min_runtime_hours": min_runtime_hours,
            "runtime_hours": runtime_hours,
            "sample_size": total,
            "total_exposures": total,
            "srm_detected": srm_detected,
            "srm_status": "detected" if srm_detected else "ok",
            "guardrails": guardrails,
            "decision": decision,
            "decision_reason": decision_reason,
            "groups": summary_groups,
            "uplift_vs_holdout_return_rate": uplift,
            "expected_allocation": expected,
        }
        self.repository.upsert_resource(
            "experiment_summary",
            experiment_id,
            status=str(summary["decision"]),
            name=experiment_id,
            payload=summary,
        )
        return summary

    def get_rollout_suggestion(self, experiment_id: str) -> Dict[str, Any]:
        summary = self.get_summary(experiment_id)
        decision = summary["decision"]
        suggestion = "continue_experiment"
        if decision == "winner":
            suggestion = "expand_treatment_audience"
        elif decision == "neutral":
            suggestion = "pause_or_retest"
        elif decision == "invalid":
            suggestion = "stop_and_investigate"
        payload = {
            "experiment_id": experiment_id,
            "decision": decision,
            "decision_reason": summary.get("decision_reason"),
            "suggestion": suggestion,
            "risk_notes": [item["metric"] for item in summary.get("guardrails", []) if item.get("status") != "pass"],
            "summary": summary,
        }
        self.repository.upsert_resource(
            "experiment_rollout_suggestion",
            experiment_id,
            status=decision,
            name=experiment_id,
            payload=payload,
        )
        return payload

    def decide(self, experiment_id: str, *, decided_by: str = "system") -> Dict[str, Any]:
        summary = self.get_summary(experiment_id)
        rollout = self.get_rollout_suggestion(experiment_id)
        next_step = rollout["suggestion"]
        payload = {
            "experiment_id": experiment_id,
            "summary": summary,
            "next_step": next_step,
            "decision_reason": summary.get("decision_reason"),
            "decided_by": decided_by,
            "decided_at": datetime.utcnow().isoformat(),
        }
        self.repository.record_resource_event("experiment", experiment_id, event_type="decision", payload=payload)
        self.repository.record_action("experiment_decision_recorded", "experiment", experiment_id, payload)
        return payload
