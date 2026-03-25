from __future__ import annotations

import hashlib
import math
from datetime import datetime, timedelta
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
            "rollout_policy": "conservative",
            "multiple_comparisons_method": "none",
            "scenario_type": "churn_rescue",
            "optimization_mode": "fixed_ab",
            "holdout_floor_pct": 0.10,
            "max_daily_shift_pct": 0.10,
            "eligibility_threshold_steps": [0.85, 0.75, 0.65, 0.55],
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

    def assign_user(self, experiment_id: str, user_id: Any, *, assigned_at: str | None = None) -> Dict[str, Any]:
        config = self.get_config(experiment_id)
        user_text = str(user_id)
        if user_text in {str(item) for item in config.get("blacklist_user_ids") or []}:
            assignment = {
                "experiment_id": experiment_id,
                "user_id": user_text,
                "bucket": None,
                "group": "excluded",
                "assigned_at": assigned_at or datetime.utcnow().isoformat(),
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
        holdout_pct = float(config.get("holdout_pct") if config.get("holdout_pct") is not None else 0.10)
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
            "assigned_at": assigned_at or datetime.utcnow().isoformat(),
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

    def get_measurement_integrity(self, experiment_id: str) -> Dict[str, Any]:
        config = self.get_config(experiment_id)
        exposures = self.list_exposures(experiment_id)
        outcomes = self.list_outcomes(experiment_id)
        latest_outcome_at = self._latest_outcome_at(experiment_id)
        now = datetime.utcnow()
        matched_outcome_keys: set[str] = set()
        exposures_without_outcome = 0
        pending_outcomes = 0
        eligible_exposures = 0
        recent_rates: List[tuple[str, float]] = []
        outcomes_by_action = self._index_outcomes(outcomes, "action_execution_id")
        outcomes_by_delivery = self._index_outcomes(outcomes, "delivery_id")
        outcomes_by_user = self._index_outcomes(outcomes, "user_id")
        exposures_by_user = self._index_exposures_by_user(exposures)
        for exposure in exposures:
            group = str(exposure.get("group") or "holdout")
            if group == "excluded":
                continue
            exposure_time = self._parse_datetime(exposure.get("exposed_at") or exposure.get("recorded_at"))
            if exposure_time is None:
                continue
            eligible_exposures += 1
            matched = self._match_outcomes_for_exposure(
                exposure=exposure,
                outcomes_by_action=outcomes_by_action,
                outcomes_by_delivery=outcomes_by_delivery,
                outcomes_by_user=outcomes_by_user,
                exposures_by_user=exposures_by_user,
            )
            for item in matched:
                matched_outcome_keys.add(self._outcome_key(item))
            attribution_window_days = max(1, int(exposure.get("attribution_window_days") or 7))
            if matched:
                returned = any(str(item.get("outcome_name") or "").lower() in {"returned", "returned_to_game"} for item in matched)
                recent_rates.append((exposure_time.date().isoformat(), 1.0 if returned else 0.0))
                continue
            if exposure_time + timedelta(days=attribution_window_days) < now:
                exposures_without_outcome += 1
                recent_rates.append((exposure_time.date().isoformat(), 0.0))
            else:
                pending_outcomes += 1

        orphan_outcomes = [
            outcome
            for outcome in outcomes
            if self._outcome_key(outcome) not in matched_outcome_keys
        ]
        outcome_lag_seconds = 0
        if latest_outcome_at is not None:
            outcome_lag_seconds = int((now - latest_outcome_at).total_seconds())
        stale = bool(eligible_exposures) and (latest_outcome_at is None or outcome_lag_seconds > 172800)
        baseline_rate = 0.0
        recent_rate = 0.0
        drift_status = "insufficient_data"
        if recent_rates:
            grouped_rates: Dict[str, List[float]] = {}
            for date_key, rate in recent_rates:
                grouped_rates.setdefault(date_key, []).append(rate)
            ordered_days = sorted(grouped_rates.keys())
            if len(ordered_days) >= 2:
                recent_slice = ordered_days[-1:]
                baseline_slice = ordered_days[:-1]
                recent_values = [value for key in recent_slice for value in grouped_rates[key]]
                baseline_values = [value for key in baseline_slice for value in grouped_rates[key]]
                if recent_values:
                    recent_rate = round(sum(recent_values) / len(recent_values), 4)
                if baseline_values:
                    baseline_rate = round(sum(baseline_values) / len(baseline_values), 4)
                if baseline_values:
                    delta = recent_rate - baseline_rate
                    if abs(delta) >= 0.2:
                        drift_status = "drifted"
                    else:
                        drift_status = "stable"
        warnings: List[str] = []
        if stale:
            warnings.append("outcomes_stale")
        if orphan_outcomes:
            warnings.append("orphan_outcomes_present")
        if eligible_exposures and (exposures_without_outcome / max(1, eligible_exposures)) > 0.5:
            warnings.append("missing_outcomes_high")
        if drift_status == "drifted":
            warnings.append("return_rate_drift_detected")
        payload = {
            "experiment_id": experiment_id,
            "status": config.get("status") or "draft",
            "exposure_count": len(exposures),
            "outcome_count": len(outcomes),
            "eligible_exposure_count": eligible_exposures,
            "exposures_without_outcome": exposures_without_outcome,
            "pending_outcomes": pending_outcomes,
            "orphan_outcomes": len(orphan_outcomes),
            "latest_outcome_at": latest_outcome_at.isoformat() if latest_outcome_at is not None else None,
            "outcome_lag_seconds": outcome_lag_seconds,
            "stale": stale,
            "drift_status": drift_status,
            "baseline_return_rate": baseline_rate,
            "recent_return_rate": recent_rate,
            "warning_count": len(warnings),
            "warnings": warnings,
            "missing_outcome_rate": round(exposures_without_outcome / max(1, eligible_exposures), 4) if eligible_exposures else 0.0,
            "orphan_outcome_examples": orphan_outcomes[:10],
        }
        self.repository.upsert_resource(
            "experiment_measurement_integrity",
            experiment_id,
            status="warning" if warnings else "ok",
            name=experiment_id,
            payload=payload,
        )
        return payload

    def list_active_experiments(self, *, scenario_type: str | None = None) -> List[Dict[str, Any]]:
        items = []
        for record in self.repository.list_resources("experiment"):
            payload = dict(record.get("payload") or {})
            if str(payload.get("status") or record.get("status") or "").lower() != "active":
                continue
            if scenario_type and str(payload.get("scenario_type") or "") != str(scenario_type):
                continue
            items.append(payload)
        return items

    def get_policy_snapshot(self, experiment_id: str) -> Dict[str, Any] | None:
        record = self.repository.get_resource("policy_snapshot", experiment_id)
        if record is not None:
            return dict(record.get("payload") or {})
        config = self.get_config(experiment_id)
        if not config:
            return None
        return {
            "policy_snapshot_id": f"synthetic:{experiment_id}",
            "experiment_id": experiment_id,
            "status": "synthetic",
            "winner_group": "treatment_a",
            "recommended_variant_id": "treatment_a",
            "eligibility_threshold": self._default_eligibility_threshold(config),
            "variant_actions": self._resolve_variant_actions(config),
        }

    def get_latest_policy_snapshot(self) -> Dict[str, Any] | None:
        candidates = []
        for record in self.repository.list_resources("policy_snapshot"):
            payload = dict(record.get("payload") or {})
            if not payload:
                continue
            created_at = self._parse_datetime(payload.get("applied_at") or payload.get("created_at"))
            candidates.append((created_at or datetime.min, payload))
        if not candidates:
            active_experiments = self.list_active_experiments(scenario_type="churn_rescue")
            if not active_experiments:
                return None
            config = active_experiments[0]
            variants = self._resolve_variant_actions(config)
            return {
                "policy_snapshot_id": f"synthetic:{config.get('experiment_id')}",
                "experiment_id": config.get("experiment_id"),
                "status": "synthetic",
                "winner_group": "treatment_a",
                "recommended_variant_id": "treatment_a",
                "eligibility_threshold": self._default_eligibility_threshold(config),
                "variant_actions": variants,
            }
        return max(candidates, key=lambda item: item[0])[1]

    def list_optimizer_runs(self, experiment_id: str) -> List[Dict[str, Any]]:
        items = []
        for record in self.repository.list_resources("optimizer_run"):
            payload = dict(record.get("payload") or {})
            if str(payload.get("experiment_id") or "") == experiment_id:
                items.append(payload)
        items.sort(key=lambda item: str(item.get("run_at") or item.get("created_at") or ""), reverse=True)
        return items

    def get_optimizer_state(self, experiment_id: str) -> Dict[str, Any]:
        return {
            "experiment_id": experiment_id,
            "policy_snapshot": self.get_policy_snapshot(experiment_id),
            "runs": self.list_optimizer_runs(experiment_id)[:25],
        }

    def recommend_policy_action(
        self,
        *,
        baseline_churn_score: float,
        policy_snapshot: Dict[str, Any] | None = None,
    ) -> Dict[str, Any]:
        snapshot = dict(policy_snapshot or {})
        threshold = float(snapshot.get("eligibility_threshold") or 0.65)
        eligible = float(baseline_churn_score) >= threshold
        variant_actions = dict(snapshot.get("variant_actions") or {})
        winner_group = str(snapshot.get("winner_group") or snapshot.get("recommended_variant_id") or "treatment_a")
        action = dict(variant_actions.get(winner_group) or {})
        return {
            "eligible": eligible,
            "eligibility_reason": (
                f"baseline_churn_score {round(float(baseline_churn_score), 4)} "
                f"{'>=' if eligible else '<'} threshold {round(threshold, 4)}"
            ),
            "recommended_variant": winner_group if eligible else None,
            "recommended_template_id": action.get("template_id") if eligible else None,
            "channel": action.get("channel") if eligible else None,
            "content": action.get("content") if eligible else None,
            "subject": action.get("subject") if eligible else None,
            "send_window": action.get("send_window") if eligible else None,
            "policy_snapshot_id": snapshot.get("policy_snapshot_id"),
            "policy_status": snapshot.get("status") or "inactive",
        }

    def run_optimizer(
        self,
        experiment_id: str,
        *,
        reference_time: str | None = None,
        apply_changes: bool = True,
    ) -> Dict[str, Any]:
        config = self.get_config(experiment_id)
        summary = self.get_summary(experiment_id)
        resolved_time = self._parse_datetime(reference_time) or datetime.utcnow()
        latest_outcome_at = self._latest_outcome_at(experiment_id)
        stale_outcomes = latest_outcome_at is None or latest_outcome_at < (resolved_time - timedelta(hours=48))
        floor_pct = float(config.get("holdout_floor_pct") if config.get("holdout_floor_pct") is not None else 0.10)
        max_daily_shift_pct = float(config.get("max_daily_shift_pct") if config.get("max_daily_shift_pct") is not None else 0.10)
        approved_variants = self._resolve_variant_actions(config)
        current_policy = self.get_policy_snapshot(experiment_id) or {}
        current_threshold = float(
            current_policy.get("eligibility_threshold")
            if current_policy.get("eligibility_threshold") is not None
            else self._default_eligibility_threshold(config)
        )

        guardrail_failed = any(item.get("status") == "fail" for item in summary.get("guardrails") or [])
        winner_group = str(summary.get("winner_group") or "treatment_a")
        decision = str(summary.get("decision") or "neutral")
        should_pause = bool(summary.get("srm_detected")) or guardrail_failed or stale_outcomes or decision not in {"winner", "neutral"}
        total_allocations = self._current_total_allocations(config)
        target_allocations = dict(total_allocations)
        recommendation_status = "paused" if should_pause else "proposed"
        reasons: List[str] = []

        if bool(summary.get("srm_detected")):
            reasons.append("srm_detected")
        if guardrail_failed:
            reasons.append("guardrail_failure")
        if stale_outcomes:
            reasons.append("stale_outcomes")
        if decision in {"inconclusive", "invalid"}:
            reasons.append(decision)

        next_mode = str(config.get("optimization_mode") or "fixed_ab")
        next_threshold = current_threshold
        if not should_pause and decision == "winner":
            target_allocations = self._shift_allocations_toward_winner(
                allocations=total_allocations,
                winner_group=winner_group,
                floor_pct=floor_pct,
                max_daily_shift_pct=max_daily_shift_pct,
            )
            next_threshold = self._next_eligibility_threshold(config, current_threshold)
            next_mode = "guarded_rollout"
            recommendation_status = "applied" if apply_changes else "proposed"
        elif decision == "neutral":
            reasons.append("neutral_result")

        next_config = dict(config)
        next_config["holdout_pct"] = round(max(floor_pct, float(target_allocations.get("holdout", floor_pct))), 4)
        non_holdout_share = max(0.0001, 1.0 - float(next_config["holdout_pct"]))
        next_config["b_variant_pct"] = round(
            max(
                0.0,
                min(
                    1.0,
                    float(target_allocations.get("treatment_b", 0.0)) / non_holdout_share,
                ),
            ),
            4,
        )
        next_config["optimization_mode"] = next_mode

        policy_snapshot = {
            "policy_snapshot_id": f"{experiment_id}:{resolved_time.strftime('%Y%m%d%H%M%S')}",
            "experiment_id": experiment_id,
            "status": recommendation_status,
            "winner_group": winner_group if decision == "winner" else current_policy.get("winner_group"),
            "decision": decision,
            "decision_reason": summary.get("decision_reason"),
            "optimization_mode": next_mode,
            "holdout_floor_pct": floor_pct,
            "max_daily_shift_pct": max_daily_shift_pct,
            "allocations_before": total_allocations,
            "allocations_after": target_allocations,
            "eligibility_threshold": round(next_threshold, 4),
            "variant_actions": approved_variants,
            "recommended_variant_id": winner_group if decision == "winner" else current_policy.get("recommended_variant_id") or "treatment_a",
            "applied_at": resolved_time.isoformat(),
            "latest_outcome_at": latest_outcome_at.isoformat() if latest_outcome_at is not None else None,
            "stale_outcomes": stale_outcomes,
            "reasons": reasons,
        }

        optimizer_run = {
            "optimizer_run_id": f"optr_{hashlib.sha256(f'{experiment_id}:{resolved_time.isoformat()}'.encode('utf-8')).hexdigest()[:16]}",
            "experiment_id": experiment_id,
            "run_at": resolved_time.isoformat(),
            "applied": bool(apply_changes and recommendation_status == "applied"),
            "status": recommendation_status,
            "summary": summary,
            "policy_snapshot": policy_snapshot,
            "config_before": config,
            "config_after": next_config,
            "reasons": reasons,
        }

        if apply_changes and recommendation_status == "applied":
            self.save_config(next_config, experiment_id=experiment_id)
            self._upsert_policy_snapshot(experiment_id, policy_snapshot)
        else:
            self._upsert_policy_snapshot(experiment_id, policy_snapshot, status="paused" if should_pause else "proposed")
        self.repository.upsert_resource(
            "optimizer_run",
            optimizer_run["optimizer_run_id"],
            status=recommendation_status,
            name=experiment_id,
            payload=optimizer_run,
        )
        self.repository.record_resource_event("experiment", experiment_id, event_type="optimizer_run", payload=optimizer_run)
        self.repository.record_action("experiment_optimizer_run", "experiment", experiment_id, optimizer_run)
        return optimizer_run

    def get_summary(self, experiment_id: str) -> Dict[str, Any]:
        config = self.get_config(experiment_id)
        exposures = self.list_exposures(experiment_id)
        outcomes = self.list_outcomes(experiment_id)
        outcomes_by_action: Dict[str, List[Dict[str, Any]]] = {}
        outcomes_by_delivery: Dict[str, List[Dict[str, Any]]] = {}
        outcomes_by_user: Dict[str, List[Dict[str, Any]]] = {}
        for item in outcomes:
            action_execution_id = str(item.get("action_execution_id") or "").strip()
            delivery_id = str(item.get("delivery_id") or "").strip()
            user_id = str(item.get("user_id") or "").strip()
            if action_execution_id:
                outcomes_by_action.setdefault(action_execution_id, []).append(item)
            if delivery_id:
                outcomes_by_delivery.setdefault(delivery_id, []).append(item)
            if user_id:
                outcomes_by_user.setdefault(user_id, []).append(item)

        exposures_by_user: Dict[str, List[Dict[str, Any]]] = {}
        for item in exposures:
            user_id = str(item.get("user_id") or "").strip()
            if not user_id:
                continue
            exposures_by_user.setdefault(user_id, []).append(item)
        for user_id in exposures_by_user:
            exposures_by_user[user_id].sort(
                key=lambda item: self._parse_datetime(item.get("exposed_at") or item.get("recorded_at")) or datetime.min
            )

        groups: Dict[str, Dict[str, Any]] = {
            "holdout": {"n": 0, "engaged": 0, "returned": 0, "policy_blocked": 0},
            "treatment_a": {"n": 0, "engaged": 0, "returned": 0, "policy_blocked": 0},
            "treatment_b": {"n": 0, "engaged": 0, "returned": 0, "policy_blocked": 0},
        }
        first_exposure_at: datetime | None = None
        latest_outcome_at: datetime | None = None
        variant_stats: Dict[str, Dict[str, Any]] = {}
        for exposure in exposures:
            group = str(exposure.get("group") or "holdout")
            if group == "excluded":
                continue
            groups.setdefault(group, {"n": 0, "engaged": 0, "returned": 0, "policy_blocked": 0})
            groups[group]["n"] += 1
            variant_id = str(exposure.get("variant_id") or group or "holdout")
            template_id = exposure.get("template_id")
            channel = exposure.get("channel")
            variant_stats.setdefault(
                variant_id,
                {
                    "variant_id": variant_id,
                    "group": group,
                    "template_id": template_id,
                    "channel": channel,
                    "n": 0,
                    "engaged": 0,
                    "returned": 0,
                },
            )
            variant_stats[variant_id]["n"] += 1
            if str(exposure.get("execution_status") or "") == "policy_blocked":
                groups[group]["policy_blocked"] += 1
            matched_outcomes = self._match_outcomes_for_exposure(
                exposure=exposure,
                outcomes_by_action=outcomes_by_action,
                outcomes_by_delivery=outcomes_by_delivery,
                outcomes_by_user=outcomes_by_user,
                exposures_by_user=exposures_by_user,
            )
            exposure_engaged = False
            exposure_returned = False
            for outcome in matched_outcomes:
                outcome_name = str(outcome.get("outcome_name") or outcome.get("simulated_response") or "").lower()
                outcome_ts = self._parse_datetime(outcome.get("occurred_at"))
                if outcome_ts is not None and (latest_outcome_at is None or outcome_ts > latest_outcome_at):
                    latest_outcome_at = outcome_ts
                if outcome_name in {"opened", "engaged"}:
                    exposure_engaged = True
                if outcome_name in {"returned", "returned_to_game"}:
                    exposure_returned = True
            if exposure_engaged:
                groups[group]["engaged"] += 1
                variant_stats[variant_id]["engaged"] += 1
            if exposure_returned:
                groups[group]["returned"] += 1
                variant_stats[variant_id]["returned"] += 1
            ts = exposure.get("recorded_at") or exposure.get("exposed_at")
            try:
                parsed = datetime.fromisoformat(str(ts))
            except Exception:
                parsed = None
            if parsed is not None and (first_exposure_at is None or parsed < first_exposure_at):
                first_exposure_at = parsed

        total = sum(item["n"] for item in groups.values())
        holdout_pct = float(config.get("holdout_pct") if config.get("holdout_pct") is not None else 0.10)
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
        min_sample = int(config.get("min_sample_size") if config.get("min_sample_size") is not None else 20)
        min_runtime_hours = int(config.get("min_runtime_hours") if config.get("min_runtime_hours") is not None else 24)

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

        comparisons = self._build_comparisons(groups, config)
        winner_comparison = next(
            (
                item
                for item in sorted(comparisons, key=lambda item: (item.get("adjusted_p_value", 1.0), -float(item.get("uplift", 0.0))))
                if item.get("significant") is True and float(item.get("uplift") or 0.0) > 0
            ),
            None,
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
        elif winner_comparison is not None:
            decision = "winner"
            decision_reason = (
                f"{winner_comparison['group']} outperformed holdout on return rate "
                f"(uplift {winner_comparison['uplift']}, adjusted_p_value {winner_comparison['adjusted_p_value']})."
            )

        confidence_hint = "low"
        best_adjusted_p_value = min((float(item.get("adjusted_p_value") or 1.0) for item in comparisons), default=1.0)
        if decision == "winner" and total >= max(min_sample * 2, 40) and runtime_hours >= max(float(min_runtime_hours), 24.0) and best_adjusted_p_value <= 0.01:
            confidence_hint = "high"
        elif total >= min_sample and runtime_hours >= float(min_runtime_hours) and not srm_detected:
            confidence_hint = "medium"
        significance_hint = "not_significant"
        if decision == "winner" and winner_comparison is not None and float(winner_comparison.get("uplift") or 0.0) >= 0.05 and best_adjusted_p_value <= 0.05:
            significance_hint = "practical_significance_positive"
        elif decision == "winner" and winner_comparison is not None and best_adjusted_p_value <= 0.1:
            significance_hint = "directional_positive"
        multiple_comparisons_method = str(config.get("multiple_comparisons_method") or "none")
        multiple_comparisons_note = (
            "No multiple-comparisons correction applied."
            if multiple_comparisons_method == "none"
            else f"Decision should be read with {multiple_comparisons_method} correction guidance."
        )
        variant_performance = []
        for variant_id, stats in variant_stats.items():
            variant_performance.append(
                {
                    **stats,
                    "engagement_rate": _rate(int(stats.get("engaged") or 0), int(stats.get("n") or 0)),
                    "return_rate": _rate(int(stats.get("returned") or 0), int(stats.get("n") or 0)),
                    "uplift_vs_holdout_return_rate": round(
                        _rate(int(stats.get("returned") or 0), int(stats.get("n") or 0)) - holdout_return_rate,
                        4,
                    ),
                }
            )
        variant_performance.sort(key=lambda item: (float(item.get("uplift_vs_holdout_return_rate") or 0.0), float(item.get("return_rate") or 0.0)), reverse=True)

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
            "latest_outcome_at": latest_outcome_at.isoformat() if latest_outcome_at is not None else None,
            "outcome_stale": latest_outcome_at is None or latest_outcome_at < (datetime.utcnow() - timedelta(hours=48)),
            "guardrails": guardrails,
            "confidence_hint": confidence_hint,
            "significance_hint": significance_hint,
            "multiple_comparisons_method": multiple_comparisons_method,
            "multiple_comparisons_note": multiple_comparisons_note,
            "rollout_policy": str(config.get("rollout_policy") or "conservative"),
            "decision": decision,
            "decision_reason": decision_reason,
            "winner_group": winner_comparison.get("group") if winner_comparison else None,
            "groups": summary_groups,
            "comparisons": comparisons,
            "variant_performance": variant_performance,
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
        rollout_policy = str(summary.get("rollout_policy") or "conservative")
        suggestion = "continue_experiment"
        winner_group = str(summary.get("winner_group") or "treatment_a")
        if decision == "winner":
            suggestion = f"expand_{winner_group}_audience"
            if rollout_policy == "aggressive":
                suggestion = f"expand_{winner_group}_audience_fast"
            elif rollout_policy == "balanced":
                suggestion = f"expand_{winner_group}_audience_gradually"
        elif decision == "neutral":
            suggestion = "pause_or_retest"
        elif decision == "invalid":
            suggestion = "stop_and_investigate"
        payload = {
            "experiment_id": experiment_id,
            "decision": decision,
            "decision_reason": summary.get("decision_reason"),
            "suggestion": suggestion,
            "winner_group": summary.get("winner_group"),
            "rollout_policy": rollout_policy,
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

    def _upsert_policy_snapshot(self, experiment_id: str, payload: Dict[str, Any], status: str | None = None) -> Dict[str, Any]:
        saved = self.repository.upsert_resource(
            "policy_snapshot",
            experiment_id,
            status=str(status or payload.get("status") or "ready"),
            name=experiment_id,
            payload=payload,
        )
        versions = self.repository.list_resource_versions("policy_snapshot", experiment_id)
        next_version = 1 + max((int(item.get("version") or 0) for item in versions), default=0)
        self.repository.create_resource_version("policy_snapshot", experiment_id, version=next_version, payload=payload)
        return saved.get("payload") or payload

    @staticmethod
    def _parse_datetime(value: Any) -> datetime | None:
        if not value:
            return None
        try:
            return datetime.fromisoformat(str(value))
        except Exception:
            return None

    def _latest_outcome_at(self, experiment_id: str) -> datetime | None:
        latest = None
        for outcome in self.list_outcomes(experiment_id):
            occurred_at = self._parse_datetime(outcome.get("occurred_at"))
            if occurred_at is not None and (latest is None or occurred_at > latest):
                latest = occurred_at
        return latest

    @staticmethod
    def _outcome_key(outcome: Dict[str, Any]) -> str:
        return "|".join(
            [
                str(outcome.get("provider_callback_id") or ""),
                str(outcome.get("delivery_id") or ""),
                str(outcome.get("action_execution_id") or ""),
                str(outcome.get("user_id") or ""),
                str(outcome.get("occurred_at") or ""),
                str(outcome.get("outcome_name") or ""),
            ]
        )

    @staticmethod
    def _index_outcomes(items: List[Dict[str, Any]], field: str) -> Dict[str, List[Dict[str, Any]]]:
        indexed: Dict[str, List[Dict[str, Any]]] = {}
        for item in items:
            key = str(item.get(field) or "").strip()
            if not key:
                continue
            indexed.setdefault(key, []).append(item)
        return indexed

    def _index_exposures_by_user(self, exposures: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
        indexed: Dict[str, List[Dict[str, Any]]] = {}
        for exposure in exposures:
            user_id = str(exposure.get("user_id") or "").strip()
            if not user_id:
                continue
            indexed.setdefault(user_id, []).append(exposure)
        for user_id in indexed:
            indexed[user_id].sort(
                key=lambda item: self._parse_datetime(item.get("exposed_at") or item.get("recorded_at")) or datetime.min
            )
        return indexed

    def _match_outcomes_for_exposure(
        self,
        *,
        exposure: Dict[str, Any],
        outcomes_by_action: Dict[str, List[Dict[str, Any]]],
        outcomes_by_delivery: Dict[str, List[Dict[str, Any]]],
        outcomes_by_user: Dict[str, List[Dict[str, Any]]],
        exposures_by_user: Dict[str, List[Dict[str, Any]]],
    ) -> List[Dict[str, Any]]:
        exposure_time = self._parse_datetime(exposure.get("exposed_at") or exposure.get("recorded_at"))
        if exposure_time is None:
            return []
        attribution_window_days = max(1, int(exposure.get("attribution_window_days") or 7))
        window_end = exposure_time + timedelta(days=attribution_window_days)
        action_execution_id = str(exposure.get("action_execution_id") or "").strip()
        delivery_id = str(exposure.get("delivery_id") or "").strip()
        user_id = str(exposure.get("user_id") or "").strip()
        group = str(exposure.get("group") or "").strip()

        if action_execution_id and action_execution_id in outcomes_by_action:
            candidates = outcomes_by_action.get(action_execution_id, [])
        elif delivery_id and delivery_id in outcomes_by_delivery:
            candidates = outcomes_by_delivery.get(delivery_id, [])
        else:
            candidates = outcomes_by_user.get(user_id, [])

        matched: List[Dict[str, Any]] = []
        seen_matches: set[str] = set()
        for outcome in candidates:
            occurred_at = self._parse_datetime(outcome.get("occurred_at"))
            if occurred_at is None or occurred_at < exposure_time or occurred_at > window_end:
                continue
            if action_execution_id:
                outcome_action_execution_id = str(outcome.get("action_execution_id") or "").strip()
                if outcome_action_execution_id and outcome_action_execution_id != action_execution_id:
                    continue
            elif delivery_id:
                outcome_delivery_id = str(outcome.get("delivery_id") or "").strip()
                if outcome_delivery_id and outcome_delivery_id != delivery_id:
                    continue
            else:
                if str(outcome.get("user_id") or "").strip() != user_id:
                    continue
                outcome_group = str(outcome.get("group") or "").strip()
                if outcome_group and group and outcome_group != group:
                    continue
                if not self._outcome_belongs_to_exposure(
                    exposure=exposure,
                    occurred_at=occurred_at,
                    exposures=exposures_by_user.get(user_id, []),
                ):
                    continue
            dedupe_key = (
                f"{str(outcome.get('action_execution_id') or '').strip()}:"
                f"{str(outcome.get('delivery_id') or '').strip()}:"
                f"{str(outcome.get('user_id') or '').strip()}:"
                f"{str(outcome.get('occurred_at') or '').strip()}:"
                f"{str(outcome.get('outcome_name') or '').strip()}"
            )
            if dedupe_key in seen_matches:
                continue
            seen_matches.add(dedupe_key)
            matched.append(outcome)
        return matched

    def _outcome_belongs_to_exposure(
        self,
        *,
        exposure: Dict[str, Any],
        occurred_at: datetime,
        exposures: List[Dict[str, Any]],
    ) -> bool:
        current_time = self._parse_datetime(exposure.get("exposed_at") or exposure.get("recorded_at"))
        if current_time is None:
            return False
        user_id = str(exposure.get("user_id") or "").strip()
        group = str(exposure.get("group") or "").strip()
        current_key = self._exposure_match_key(exposure)
        latest_candidate_key = None
        latest_candidate_time = None
        for candidate in exposures:
            candidate_time = self._parse_datetime(candidate.get("exposed_at") or candidate.get("recorded_at"))
            if candidate_time is None or candidate_time > occurred_at:
                continue
            if str(candidate.get("user_id") or "").strip() != user_id:
                continue
            candidate_group = str(candidate.get("group") or "").strip()
            if group and candidate_group and candidate_group != group:
                continue
            window_days = max(1, int(candidate.get("attribution_window_days") or 7))
            if occurred_at > candidate_time + timedelta(days=window_days):
                continue
            if latest_candidate_time is None or candidate_time > latest_candidate_time:
                latest_candidate_time = candidate_time
                latest_candidate_key = self._exposure_match_key(candidate)
        return latest_candidate_key == current_key

    @staticmethod
    def _exposure_match_key(exposure: Dict[str, Any]) -> str:
        return "|".join(
            [
                str(exposure.get("action_execution_id") or "").strip(),
                str(exposure.get("delivery_id") or "").strip(),
                str(exposure.get("user_id") or "").strip(),
                str(exposure.get("group") or "").strip(),
                str(exposure.get("exposed_at") or exposure.get("recorded_at") or "").strip(),
            ]
        )

    @staticmethod
    def _resolve_variant_actions(config: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
        variants: Dict[str, Dict[str, Any]] = {}
        for item in list(config.get("approved_variants") or []):
            variant_id = str((item or {}).get("variant_id") or "").strip()
            if not variant_id:
                continue
            variants[variant_id] = dict(item or {})
        return variants

    @staticmethod
    def _current_total_allocations(config: Dict[str, Any]) -> Dict[str, float]:
        holdout_pct = max(0.0, min(1.0, float(config.get("holdout_pct") or 0.10)))
        b_variant_pct = max(0.0, min(1.0, float(config.get("b_variant_pct") or 0.0)))
        total_treatment = max(0.0, 1.0 - holdout_pct)
        treatment_b = round(total_treatment * b_variant_pct, 4)
        treatment_a = round(max(0.0, total_treatment - treatment_b), 4)
        return {
            "holdout": round(holdout_pct, 4),
            "treatment_a": treatment_a,
            "treatment_b": treatment_b,
        }

    @staticmethod
    def _shift_allocations_toward_winner(
        *,
        allocations: Dict[str, float],
        winner_group: str,
        floor_pct: float,
        max_daily_shift_pct: float,
    ) -> Dict[str, float]:
        updated = dict(allocations)
        updated["holdout"] = round(max(floor_pct, float(updated.get("holdout") or floor_pct)), 4)
        loser_group = "treatment_b" if winner_group == "treatment_a" else "treatment_a"
        shift = min(float(max_daily_shift_pct), float(updated.get(loser_group) or 0.0))
        updated[winner_group] = round(float(updated.get(winner_group) or 0.0) + shift, 4)
        updated[loser_group] = round(max(0.0, float(updated.get(loser_group) or 0.0) - shift), 4)
        total = round(sum(updated.values()), 4)
        if total != 1.0 and total > 0:
            diff = round(1.0 - total, 4)
            updated[winner_group] = round(float(updated[winner_group]) + diff, 4)
        return updated

    @staticmethod
    def _default_eligibility_threshold(config: Dict[str, Any]) -> float:
        steps = list(config.get("eligibility_threshold_steps") or [0.65])
        if not steps:
            return 0.65
        return float(steps[min(2, len(steps) - 1)])

    @classmethod
    def _next_eligibility_threshold(cls, config: Dict[str, Any], current_threshold: float) -> float:
        steps = sorted({float(value) for value in list(config.get("eligibility_threshold_steps") or [])}, reverse=True)
        if not steps:
            return current_threshold
        for index, value in enumerate(steps):
            if abs(value - current_threshold) < 1e-9:
                if index + 1 < len(steps):
                    return float(steps[index + 1])
                return float(value)
        for value in steps:
            if value < current_threshold:
                return float(value)
        return float(steps[-1])

    def _build_comparisons(self, groups: Dict[str, Dict[str, Any]], config: Dict[str, Any]) -> List[Dict[str, Any]]:
        comparisons: List[Dict[str, Any]] = []
        method = str(config.get("multiple_comparisons_method") or "none")
        candidates = [
            ("treatment_a", "holdout"),
            ("treatment_b", "holdout"),
        ]
        raw_values: List[float] = []
        for treatment_group, control_group in candidates:
            treatment_stats = groups.get(treatment_group) or {}
            control_stats = groups.get(control_group) or {}
            if int(treatment_stats.get("n") or 0) <= 0 or int(control_stats.get("n") or 0) <= 0:
                continue
            treatment_rate = self._rate(int(treatment_stats.get("returned") or 0), int(treatment_stats.get("n") or 0))
            control_rate = self._rate(int(control_stats.get("returned") or 0), int(control_stats.get("n") or 0))
            p_value, z_score = self._two_proportion_test(
                int(treatment_stats.get("returned") or 0),
                int(treatment_stats.get("n") or 0),
                int(control_stats.get("returned") or 0),
                int(control_stats.get("n") or 0),
            )
            comparisons.append(
                {
                    "group": treatment_group,
                    "control_group": control_group,
                    "returned": int(treatment_stats.get("returned") or 0),
                    "n": int(treatment_stats.get("n") or 0),
                    "control_returned": int(control_stats.get("returned") or 0),
                    "control_n": int(control_stats.get("n") or 0),
                    "return_rate": treatment_rate,
                    "control_return_rate": control_rate,
                    "uplift": round(treatment_rate - control_rate, 4),
                    "z_score": z_score,
                    "p_value": p_value,
                }
            )
            raw_values.append(p_value)
        adjusted = self._adjust_p_values(raw_values, method)
        for index, comparison in enumerate(comparisons):
            adjusted_p = adjusted[index] if index < len(adjusted) else float(comparison.get("p_value") or 1.0)
            comparison["adjusted_p_value"] = adjusted_p
            comparison["significant"] = adjusted_p <= 0.05 and float(comparison.get("uplift") or 0.0) > 0.0
            comparison["correction_method"] = method
        return comparisons

    @staticmethod
    def _rate(numerator: int, denominator: int) -> float:
        return round((float(numerator) / denominator), 4) if denominator else 0.0

    @staticmethod
    def _two_proportion_test(success_a: int, n_a: int, success_b: int, n_b: int) -> tuple[float, float]:
        if min(n_a, n_b) <= 0:
            return 1.0, 0.0
        rate_a = success_a / n_a
        rate_b = success_b / n_b
        pooled = (success_a + success_b) / (n_a + n_b)
        variance = pooled * (1.0 - pooled) * ((1.0 / n_a) + (1.0 / n_b))
        if variance <= 0:
            return 1.0, 0.0
        z_score = (rate_a - rate_b) / math.sqrt(variance)
        p_value = math.erfc(abs(z_score) / math.sqrt(2.0))
        return round(min(1.0, max(0.0, p_value)), 6), round(z_score, 4)

    @staticmethod
    def _adjust_p_values(p_values: List[float], method: str) -> List[float]:
        if not p_values:
            return []
        normalized = str(method or "none").lower()
        if normalized == "bonferroni":
            return [round(min(1.0, value * len(p_values)), 6) for value in p_values]
        if normalized == "holm_bonferroni":
            indexed = sorted(enumerate(p_values), key=lambda item: item[1])
            adjusted = [1.0] * len(p_values)
            running_max = 0.0
            total = len(p_values)
            for rank, (original_index, value) in enumerate(indexed, start=1):
                candidate = min(1.0, value * (total - rank + 1))
                running_max = max(running_max, candidate)
                adjusted[original_index] = round(running_max, 6)
            return adjusted
        return [round(min(1.0, max(0.0, value)), 6) for value in p_values]
