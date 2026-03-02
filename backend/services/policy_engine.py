import copy
from typing import Dict, Any, List


DEFAULT_LLM_POLICY: Dict[str, Any] = {
    "version": "2026-03-02-v1",
    "enabled": True,
    "weights": {
        "ltv_score": 0.50,
        "churn_score": 0.25,
        "confidence_gap": 0.15,
        "recency_risk": 0.10,
    },
    "ltv_tiers": {
        "whale_min": 500.0,
        "dolphin_min": 50.0,
    },
    "hard_gates": {
        "max_actions_per_player_per_week": 1,
        "respect_blacklisted_segments": True,
        "stop_when_daily_budget_exceeded": True,
        "use_decision_cache": True,
    },
    "routing_rules": [
        {
            "name": "whale_high_uncertainty",
            "if": {
                "ltv_tier_in": ["whale"],
                "churn_risk_in": ["high", "medium"],
                "min_confidence_gap": 40,
            },
            "route": "LARGE_MODEL",
        },
        {
            "name": "dolphin_high_churn",
            "if": {
                "ltv_tier_in": ["dolphin"],
                "churn_risk_in": ["high"],
            },
            "route": "SMALL_MODEL",
        },
        {
            "name": "dolphin_medium_uncertain",
            "if": {
                "ltv_tier_in": ["dolphin"],
                "churn_risk_in": ["medium"],
                "min_confidence_gap": 50,
            },
            "route": "SMALL_MODEL",
        },
    ],
    "defaults": {
        "route": "NO_LLM",
    },
}


def _deep_merge_dict(base: Dict[str, Any], patch: Dict[str, Any]) -> Dict[str, Any]:
    merged = copy.deepcopy(base)
    for key, value in patch.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge_dict(merged[key], value)
        else:
            merged[key] = value
    return merged


class LlmPolicyEngine:
    ROUTE_VALUES = {"NO_LLM", "SMALL_MODEL", "LARGE_MODEL"}

    def normalize_policy(self, patch: Dict[str, Any], base: Dict[str, Any] = None) -> Dict[str, Any]:
        merged = _deep_merge_dict(base or DEFAULT_LLM_POLICY, patch or {})
        self._validate_policy(merged)
        return merged

    def _validate_policy(self, policy: Dict[str, Any]):
        if "weights" not in policy:
            raise ValueError("Policy missing 'weights'.")
        if "defaults" not in policy or "route" not in policy["defaults"]:
            raise ValueError("Policy missing defaults.route.")
        if policy["defaults"]["route"] not in self.ROUTE_VALUES:
            raise ValueError("defaults.route must be one of NO_LLM, SMALL_MODEL, LARGE_MODEL.")

        rules: List[Dict[str, Any]] = policy.get("routing_rules", [])
        for idx, rule in enumerate(rules):
            if rule.get("route") not in self.ROUTE_VALUES:
                raise ValueError(f"routing_rules[{idx}].route is invalid.")

    def evaluate(self, policy: Dict[str, Any], context: Dict[str, Any]) -> Dict[str, Any]:
        if not policy.get("enabled", True):
            return {"route": "NO_LLM", "reason": "Policy disabled.", "priority_score": 0.0}

        hard_gates = policy.get("hard_gates", {})
        if (
            hard_gates.get("respect_blacklisted_segments", True)
            and bool(context.get("blacklisted", False))
        ):
            return {"route": "NO_LLM", "reason": "Blacklisted segment hard gate.", "priority_score": 0.0}
        if (
            hard_gates.get("stop_when_daily_budget_exceeded", True)
            and bool(context.get("daily_budget_exceeded", False))
        ):
            return {"route": "NO_LLM", "reason": "Daily budget hard gate.", "priority_score": 0.0}
        if (
            hard_gates.get("use_decision_cache", True)
            and bool(context.get("cache_hit", False))
        ):
            return {"route": "NO_LLM", "reason": "Cache hit hard gate.", "priority_score": 0.0}

        weekly_actions_count = int(context.get("weekly_actions_count", 0) or 0)
        max_actions = int(hard_gates.get("max_actions_per_player_per_week", 1) or 1)
        if weekly_actions_count >= max_actions:
            return {"route": "NO_LLM", "reason": "Frequency cap hard gate.", "priority_score": 0.0}

        ltv_tier = self._get_ltv_tier(policy, float(context.get("ltv", 0.0) or 0.0))
        churn_risk = str(context.get("churn_risk", "unknown")).lower()
        confidence_gap = float(context.get("confidence_gap", 0.0) or 0.0)
        recency_risk = float(context.get("recency_risk", 0.0) or 0.0)

        ltv_score = self._get_ltv_score(policy, float(context.get("ltv", 0.0) or 0.0))
        churn_score = self._get_churn_score(churn_risk)

        weights = policy.get("weights", {})
        priority_score = (
            float(weights.get("ltv_score", 0.0)) * ltv_score
            + float(weights.get("churn_score", 0.0)) * churn_score
            + float(weights.get("confidence_gap", 0.0)) * confidence_gap
            + float(weights.get("recency_risk", 0.0)) * recency_risk
        )

        for rule in policy.get("routing_rules", []):
            rule_if = rule.get("if", {})
            if rule_if.get("ltv_tier_in") and ltv_tier not in rule_if.get("ltv_tier_in", []):
                continue
            if rule_if.get("churn_risk_in") and churn_risk not in rule_if.get("churn_risk_in", []):
                continue
            if confidence_gap < float(rule_if.get("min_confidence_gap", 0.0) or 0.0):
                continue
            return {
                "route": rule["route"],
                "reason": f"Matched routing rule '{rule.get('name', 'unnamed')}'.",
                "priority_score": round(priority_score, 2),
                "ltv_tier": ltv_tier,
            }

        return {
            "route": policy.get("defaults", {}).get("route", "NO_LLM"),
            "reason": "No routing rule matched; using default route.",
            "priority_score": round(priority_score, 2),
            "ltv_tier": ltv_tier,
        }

    def _get_ltv_tier(self, policy: Dict[str, Any], ltv: float) -> str:
        tiers = policy.get("ltv_tiers", {})
        whale_min = float(tiers.get("whale_min", 500.0))
        dolphin_min = float(tiers.get("dolphin_min", 50.0))
        if ltv >= whale_min:
            return "whale"
        if ltv >= dolphin_min:
            return "dolphin"
        return "minnow"

    def _get_ltv_score(self, policy: Dict[str, Any], ltv: float) -> float:
        tiers = policy.get("ltv_tiers", {})
        whale_min = float(tiers.get("whale_min", 500.0))
        dolphin_min = float(tiers.get("dolphin_min", 50.0))
        if ltv >= whale_min:
            return 100.0
        if ltv >= dolphin_min:
            return 65.0
        return 20.0

    def _get_churn_score(self, churn_risk: str) -> float:
        if churn_risk == "high":
            return 90.0
        if churn_risk == "medium":
            return 60.0
        if churn_risk == "low":
            return 20.0
        return 40.0

