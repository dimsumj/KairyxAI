from __future__ import annotations

import re
from datetime import datetime
from typing import Any, Dict, List
import uuid

from bigquery_service import BigQueryService, get_shared_bigquery_service

from app.application.cohorts import CohortService
from app.application.experiments import ExperimentConfigService


class CopilotService:
    def __init__(self, repository, bigquery_service: BigQueryService | None = None):
        self.repository = repository
        self.bigquery_service = bigquery_service or get_shared_bigquery_service()
        self.cohorts = CohortService(repository, self.bigquery_service)
        self.experiments = ExperimentConfigService(repository)
        self.metric_registry: Dict[str, Dict[str, Any]] = {
            "active_users": {"label": "Active Users", "alias": "mart_user_daily", "operation": "count_rows"},
            "total_players": {"label": "Total Players", "alias": "mart_user_daily", "operation": "count_rows"},
            "payers": {"label": "Payers", "alias": "mart_user_daily", "operation": "count_positive", "field": "lifetime_revenue_usd"},
            "revenue_usd": {"label": "Revenue (USD)", "alias": "mart_user_daily", "operation": "sum", "field": "lifetime_revenue_usd"},
            "sessions_7d": {"label": "Sessions 7d", "alias": "mart_user_daily", "operation": "sum", "field": "sessions_7d"},
            "sessions_30d": {"label": "Sessions 30d", "alias": "mart_user_daily", "operation": "sum", "field": "sessions_30d"},
            "high_risk_users": {"label": "High Risk Users", "alias": "prediction_results", "operation": "count_match", "field": "predicted_churn_risk", "value": "high"},
            "medium_risk_users": {"label": "Medium Risk Users", "alias": "prediction_results", "operation": "count_match", "field": "predicted_churn_risk", "value": "medium"},
            "churned_users": {"label": "Churned Users", "alias": "prediction_results", "operation": "count_match", "field": "churn_state", "value": "churned"},
            "open_experiment_exposures": {"label": "Experiment Exposures", "alias": "prediction_results", "operation": "count_rows"},
            "low_risk_users": {"label": "Low Risk Users", "alias": "prediction_results", "operation": "count_match", "field": "predicted_churn_risk", "value": "low"},
            "already_churned": {"label": "Already Churned", "alias": "prediction_results", "operation": "count_match", "field": "predicted_churn_risk", "value": "already_churned"},
            "campaign_touches": {"label": "Campaign Touches", "alias": "fact_events_unified", "operation": "count_rows"},
            "events_total": {"label": "Events Total", "alias": "fact_events_unified", "operation": "count_rows"},
            "purchase_events": {"label": "Purchase Events", "alias": "fact_events_unified", "operation": "count_match", "field": "event_type", "value": "item_purchased"},
            "promo_views": {"label": "Promo Views", "alias": "fact_events_unified", "operation": "count_match", "field": "event_type", "value": "promo_view"},
            "return_rate": {"label": "Return Rate", "alias": "mart_user_daily", "operation": "mean_inverse_days", "field": "days_since_last_seen"},
            "payer_rate": {"label": "Payer Rate", "alias": "mart_user_daily", "operation": "ratio_positive", "field": "lifetime_revenue_usd"},
            "prediction_rows": {"label": "Prediction Rows", "alias": "prediction_results", "operation": "count_rows"},
            "delivery_success_proxy": {"label": "Delivery Success Proxy", "alias": "prediction_results", "operation": "count_rows"},
        }

    def query(self, question: str, *, time_window: str | None = None, filters: Dict[str, Any] | None = None) -> Dict[str, Any]:
        metric_id = self._match_metric(question)
        resolved_window = time_window or self._match_window(question)
        metric_value = self._compute_metric(metric_id, filters=filters or {})
        response = {
            "conclusion": f"{self.metric_registry[metric_id]['label']}: {metric_value}",
            "key_evidence": [
                {"metric_id": metric_id, "value": metric_value, "alias": self.metric_registry[metric_id]["alias"]},
                {"time_window": resolved_window},
            ],
            "impact_scope": {"records_evaluated": self._count_records(self.metric_registry[metric_id]["alias"])},
            "suggested_action": {"type": "inspect_or_recommend", "metric_id": metric_id},
            "confidence": "medium",
            "methodology": {
                "metric_id": metric_id,
                "time_window": resolved_window,
                "filters": filters or {},
                "data_sources": [self.metric_registry[metric_id]["alias"]],
            },
        }
        self.repository.record_resource_event("copilot", metric_id, event_type="query", payload={"question": question, **response})
        return response

    def explain(self, metric_id: str, *, time_window: str = "7d", dimensions: List[str] | None = None) -> Dict[str, Any]:
        metric = self.metric_registry.get(metric_id) or self.metric_registry["active_users"]
        alias = metric["alias"]
        rows = self.bigquery_service.get_rows_for_alias(alias)
        dims = list(dimensions or ["platform", "country", "campaign"])
        drivers = self._top_drivers(rows, dims)
        conclusion = f"{metric['label']} anomaly drivers identified"
        response = {
            "conclusion": conclusion,
            "key_evidence": drivers[:3],
            "impact_scope": {"users": self._count_records(alias), "time_window": time_window},
            "suggested_action": {"type": "review_top_drivers", "dimensions": dims},
            "confidence": "medium" if drivers else "low",
            "methodology": {"metric_id": metric_id, "time_window": time_window, "data_sources": [alias]},
        }
        self.repository.record_resource_event("copilot", metric_id, event_type="explain", payload=response)
        return response

    def recommend(self, insight: Dict[str, Any] | None = None, metric_context: Dict[str, Any] | None = None) -> Dict[str, Any]:
        insight = insight or {}
        metric_context = metric_context or {}
        should_target_churn = "churn" in str(insight).lower() or "high_risk" in str(metric_context).lower() or True
        cohort_draft = None
        if should_target_churn:
            cohort_name = f"copilot_high_risk_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}_{uuid.uuid4().hex[:4]}"
            cohort_draft = self.cohorts.create_cohort(
                name=cohort_name,
                cohort_type="sql",
                refresh_mode="daily",
                owner="copilot",
                activate=False,
                definition={
                    "sql": (
                        "SELECT user_id AS canonical_user_id, predicted_churn_risk, churn_state, email "
                        "FROM prediction_results "
                        "WHERE predicted_churn_risk = 'high' AND churn_state != 'churned'"
                    )
                },
            )
        response = {
            "conclusion": "Recommend a Churn Rescue push/email treatment for high-risk players.",
            "key_evidence": [
                {"signal": "high_risk_users", "value": self._compute_metric("high_risk_users")},
                {"signal": "cohort_draft", "cohort_id": (cohort_draft or {}).get("cohort_id")},
            ],
            "impact_scope": {"eligible_users": (cohort_draft or {}).get("member_count", 0)},
            "suggested_action": {
                "type": "push_notification",
                "channel": "push_notification",
                "risk": "medium",
                "cohort_draft": cohort_draft,
                "message": "We miss you. Come back today for a comeback reward.",
            },
            "confidence": "medium",
            "methodology": {
                "input_insight": insight,
                "metric_context": metric_context,
                "data_sources": ["prediction_results"],
            },
        }
        self.repository.record_resource_event("copilot", "recommendation", event_type="recommend", payload=response)
        return response

    def report(self, report_type: str = "daily", *, time_window: str = "7d") -> Dict[str, Any]:
        active_summary = self.query("active users", time_window=time_window)
        high_risk = self.query("high risk users", time_window=time_window)
        recommendation = self.recommend(metric_context={"report_type": report_type})
        response = {
            "conclusion": f"{report_type.title()} copilot report generated.",
            "key_evidence": [
                {"metric": "active_users", "value": active_summary["key_evidence"][0]["value"]},
                {"metric": "high_risk_users", "value": high_risk["key_evidence"][0]["value"]},
            ],
            "impact_scope": {"report_type": report_type, "time_window": time_window},
            "suggested_action": recommendation["suggested_action"],
            "confidence": "medium",
            "methodology": {"report_type": report_type, "time_window": time_window, "data_sources": ["mart_user_daily", "prediction_results"]},
        }
        self.repository.record_resource_event("copilot", "report", event_type="report", payload=response)
        return response

    def _match_metric(self, question: str) -> str:
        text = str(question or "").lower()
        for metric_id, candidates in (
            ("high_risk_users", ("high risk", "churn risk", "high-risk")),
            ("revenue_usd", ("revenue", "sales", "ltv")),
            ("payers", ("payer", "paid users")),
            ("purchase_events", ("purchase", "bought")),
            ("promo_views", ("promo", "campaign view")),
            ("active_users", ("active", "users", "players")),
        ):
            if any(token in text for token in candidates):
                return metric_id
        return "active_users"

    def _match_window(self, question: str) -> str:
        match = re.search(r"(\d+)\s*d", str(question or "").lower())
        if match:
            return f"{match.group(1)}d"
        return "7d"

    def _lookup_value(self, row: Dict[str, Any], field: str) -> Any:
        if field in row:
            return row.get(field)
        for container_name in ("event_properties", "user_properties"):
            container = row.get(container_name)
            if isinstance(container, dict) and field in container:
                return container.get(field)
        return None

    def _compute_metric(self, metric_id: str, *, filters: Dict[str, Any] | None = None) -> float | int:
        metric = self.metric_registry.get(metric_id) or self.metric_registry["active_users"]
        rows = self.bigquery_service.get_rows_for_alias(metric["alias"])
        if filters:
            rows = [row for row in rows if all(self._lookup_value(row, key) == value for key, value in filters.items())]
        operation = metric["operation"]
        if operation == "count_rows":
            return len(rows)
        if operation == "count_match":
            return sum(1 for row in rows if str(self._lookup_value(row, metric["field"]) or "").lower() == str(metric["value"]).lower())
        if operation == "count_positive":
            return sum(1 for row in rows if float(self._lookup_value(row, metric["field"]) or 0) > 0)
        if operation == "sum":
            return round(sum(float(self._lookup_value(row, metric["field"]) or 0) for row in rows), 2)
        if operation == "ratio_positive":
            numerator = sum(1 for row in rows if float(self._lookup_value(row, metric["field"]) or 0) > 0)
            return round(numerator / len(rows), 4) if rows else 0.0
        if operation == "mean_inverse_days":
            values = [max(0.0, 1.0 / (1.0 + float(self._lookup_value(row, metric["field"]) or 0))) for row in rows]
            return round(sum(values) / len(values), 4) if values else 0.0
        return len(rows)

    def _count_records(self, alias: str) -> int:
        return len(self.bigquery_service.get_rows_for_alias(alias))

    def _top_drivers(self, rows: List[Dict[str, Any]], dimensions: List[str]) -> List[Dict[str, Any]]:
        scores: Dict[str, Dict[str, Any]] = {}
        for row in rows:
            for dimension in dimensions:
                value = self._lookup_value(row, dimension)
                if value in (None, "", []):
                    continue
                key = f"{dimension}:{value}"
                item = scores.setdefault(key, {"dimension": dimension, "value": value, "count": 0})
                item["count"] += 1
        return sorted(scores.values(), key=lambda item: item["count"], reverse=True)
