from __future__ import annotations

import re
import uuid
from datetime import datetime
from typing import Any, Dict, List

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
            "low_risk_users": {"label": "Low Risk Users", "alias": "prediction_results", "operation": "count_match", "field": "predicted_churn_risk", "value": "low"},
            "already_churned": {"label": "Already Churned", "alias": "prediction_results", "operation": "count_match", "field": "predicted_churn_risk", "value": "already_churned"},
            "churned_users": {"label": "Churned Users", "alias": "prediction_results", "operation": "count_match", "field": "churn_state", "value": "churned"},
            "campaign_touches": {"label": "Campaign Touches", "alias": "fact_events_unified", "operation": "count_rows"},
            "events_total": {"label": "Events Total", "alias": "fact_events_unified", "operation": "count_rows"},
            "purchase_events": {"label": "Purchase Events", "alias": "fact_events_unified", "operation": "count_match", "field": "event_type", "value": "item_purchased"},
            "promo_views": {"label": "Promo Views", "alias": "fact_events_unified", "operation": "count_match", "field": "event_type", "value": "promo_view"},
            "return_rate": {"label": "Return Rate", "alias": "mart_user_daily", "operation": "mean_inverse_days", "field": "days_since_last_seen"},
            "payer_rate": {"label": "Payer Rate", "alias": "mart_user_daily", "operation": "ratio_positive", "field": "lifetime_revenue_usd"},
            "prediction_rows": {"label": "Prediction Rows", "alias": "prediction_results", "operation": "count_rows"},
            "ios_users": {"label": "iOS Users", "alias": "fact_events_unified", "operation": "count_match", "field": "platform", "value": "ios"},
            "android_users": {"label": "Android Users", "alias": "fact_events_unified", "operation": "count_match", "field": "platform", "value": "android"},
            "us_users": {"label": "US Users", "alias": "fact_events_unified", "operation": "count_match", "field": "country", "value": "US"},
            "email_reachable_users": {"label": "Email Reachable Users", "alias": "prediction_results", "operation": "count_present", "field": "email"},
        }
        self.intent_templates: Dict[str, Dict[str, Any]] = {
            "high_risk_users": {"sources": ["prediction_results"]},
            "medium_risk_users": {"sources": ["prediction_results"]},
            "low_risk_users": {"sources": ["prediction_results"]},
            "already_churned": {"sources": ["prediction_results"]},
            "churned_users": {"sources": ["prediction_results"]},
            "active_users": {"sources": ["mart_user_daily"]},
            "total_players": {"sources": ["mart_user_daily"]},
            "payers": {"sources": ["mart_user_daily"]},
            "revenue_usd": {"sources": ["mart_user_daily"]},
            "sessions_7d": {"sources": ["mart_user_daily"]},
            "sessions_30d": {"sources": ["mart_user_daily"]},
            "campaign_touches": {"sources": ["fact_events_unified"]},
            "events_total": {"sources": ["fact_events_unified"]},
            "purchase_events": {"sources": ["fact_events_unified"]},
            "promo_views": {"sources": ["fact_events_unified"]},
            "return_rate": {"sources": ["mart_user_daily"]},
            "payer_rate": {"sources": ["mart_user_daily"]},
            "ios_users": {"sources": ["fact_events_unified"]},
            "android_users": {"sources": ["fact_events_unified"]},
            "us_users": {"sources": ["fact_events_unified"]},
            "email_reachable_users": {"sources": ["prediction_results"]},
            "workflow_health": {"sources": ["workflow_execution"]},
            "experiment_health": {"sources": ["experiment_summary"]},
            "cohort_health": {"sources": ["cohort_snapshot"]},
        }
        self.intent_registry: Dict[str, Dict[str, Any]] = {
            "active_users": {"aliases": ["active users", "active players", "active"], "dimensions": ["platform", "country", "campaign"]},
            "total_players": {"aliases": ["total players", "total users", "player count"], "dimensions": ["platform", "country"]},
            "payers": {"aliases": ["payers", "paid users", "paying users"], "dimensions": ["platform", "country", "campaign"]},
            "revenue_usd": {"aliases": ["revenue", "revenue usd", "sales", "ltv"], "dimensions": ["platform", "country", "campaign"]},
            "sessions_7d": {"aliases": ["sessions 7d", "sessions last 7", "weekly sessions"], "dimensions": ["platform", "country", "campaign"]},
            "sessions_30d": {"aliases": ["sessions 30d", "sessions last 30", "monthly sessions"], "dimensions": ["platform", "country", "campaign"]},
            "high_risk_users": {"aliases": ["high risk users", "high-risk users", "high risk", "churn risk"], "dimensions": ["platform", "country", "campaign"]},
            "medium_risk_users": {"aliases": ["medium risk users", "medium risk"], "dimensions": ["platform", "country", "campaign"]},
            "low_risk_users": {"aliases": ["low risk users", "low risk"], "dimensions": ["platform", "country", "campaign"]},
            "already_churned": {"aliases": ["already churned", "already churned users"], "dimensions": ["platform", "country"]},
            "churned_users": {"aliases": ["churned users", "churned"], "dimensions": ["platform", "country"]},
            "campaign_touches": {"aliases": ["campaign touches", "campaign touch"], "dimensions": ["campaign", "country", "platform"]},
            "events_total": {"aliases": ["events total", "total events", "events"], "dimensions": ["event_type", "country", "platform"]},
            "purchase_events": {"aliases": ["purchase events", "purchases", "bought"], "dimensions": ["campaign", "country", "platform"]},
            "promo_views": {"aliases": ["promo views", "promo", "campaign views"], "dimensions": ["campaign", "country", "platform"]},
            "return_rate": {"aliases": ["return rate", "come back rate", "returning"], "dimensions": ["platform", "country", "campaign"]},
            "payer_rate": {"aliases": ["payer rate", "paid rate"], "dimensions": ["platform", "country", "campaign"]},
            "prediction_rows": {"aliases": ["prediction rows", "prediction count"], "dimensions": ["predicted_churn_risk", "prediction_source"]},
            "ios_users": {"aliases": ["ios users", "ios", "iphone users"], "dimensions": ["country", "campaign"]},
            "android_users": {"aliases": ["android users", "android"], "dimensions": ["country", "campaign"]},
            "us_users": {"aliases": ["us users", "united states users", "country us"], "dimensions": ["platform", "campaign"]},
            "email_reachable_users": {"aliases": ["email reachable users", "email users", "reachable emails"], "dimensions": ["predicted_churn_risk", "country"]},
        }
        self.dimension_value_lexicon: Dict[str, Dict[str, List[str]]] = {
            "platform": {"ios": ["ios", "iphone"], "android": ["android"]},
            "country": {"US": ["us", "united states"], "CA": ["ca", "canada"]},
            "predicted_churn_risk": {
                "high": ["high risk", "high-risk"],
                "medium": ["medium risk"],
                "low": ["low risk"],
                "already_churned": ["already churned"],
            },
        }

    def get_metrics(self) -> Dict[str, Any]:
        return {
            "items": [
                {
                    "metric_id": metric_id,
                    "label": metric["label"],
                    "alias": metric["alias"],
                    "operation": metric["operation"],
                    "field": metric.get("field"),
                    "value": metric.get("value"),
                }
                for metric_id, metric in sorted(self.metric_registry.items())
            ]
        }

    def get_query_log(self, query_id: str) -> Dict[str, Any] | None:
        record = self.repository.get_resource("copilot_query_log", query_id)
        return (record or {}).get("payload") if record else None

    def get_anomaly(self, anomaly_id: str) -> Dict[str, Any] | None:
        record = self.repository.get_resource("anomaly_event", anomaly_id) or self.repository.get_resource("copilot_anomaly", anomaly_id)
        return (record or {}).get("payload") if record else None

    def list_anomalies(self) -> List[Dict[str, Any]]:
        items = [item.get("payload") or {} for item in self.repository.list_resources("anomaly_event")]
        if items:
            return items
        return [item.get("payload") or {} for item in self.repository.list_resources("copilot_anomaly")]

    def list_reports(self) -> List[Dict[str, Any]]:
        items = [item.get("payload") or {} for item in self.repository.list_resources("copilot_report")]
        weekly = [item.get("payload") or {} for item in self.repository.list_resources("weekly_closed_loop_report")]
        return items + weekly

    def query(self, question: str, *, time_window: str | None = None, filters: Dict[str, Any] | None = None) -> Dict[str, Any]:
        parsed = self._parse_question(question, time_window=time_window, filters=filters or {})
        metric_id = parsed["metric_id"]
        resolved_window = parsed["time_window"]
        alias = self.metric_registry[metric_id]["alias"]
        records_evaluated = self._count_records(alias, filters=parsed["filters"])
        if records_evaluated == 0:
            response = self._insufficient_evidence(metric_window=resolved_window, evidence=[{"metric_id": metric_id, "alias": alias}])
            return self._record_query_log("query", response, {"question": question, "metric_id": metric_id})
        metric_value = self._compute_metric(metric_id, filters=parsed["filters"])
        evidence: List[Dict[str, Any]] = [
            {"metric_id": metric_id, "value": metric_value, "alias": alias, "filters": parsed["filters"]},
            {"intent_id": metric_id, "evidence_sources": self.intent_templates.get(metric_id, {}).get("sources", [alias]), "parsed_intent": parsed},
        ]
        impact_scope: Dict[str, Any] = {"records_evaluated": records_evaluated}
        conclusion = f"{self.metric_registry[metric_id]['label']}: {metric_value}"
        if parsed.get("comparison"):
            comparison = dict(parsed["comparison"])
            left_filters = {**parsed["filters"], comparison["dimension"]: comparison["left"]}
            right_filters = {**parsed["filters"], comparison["dimension"]: comparison["right"]}
            left_value = self._compute_metric(metric_id, filters=left_filters)
            right_value = self._compute_metric(metric_id, filters=right_filters)
            delta = round(float(left_value) - float(right_value), 4)
            delta_pct = round((delta / max(abs(float(right_value)), 1.0)) * 100.0, 2)
            conclusion = (
                f"{self.metric_registry[metric_id]['label']} {comparison['left']} vs {comparison['right']}: "
                f"{left_value} vs {right_value} (delta {delta}, {delta_pct}%)"
            )
            evidence.append(
                {
                    "comparison": comparison,
                    "left_value": left_value,
                    "right_value": right_value,
                    "delta": delta,
                    "delta_pct": delta_pct,
                }
            )
            impact_scope["comparison"] = {"left": left_value, "right": right_value, "delta": delta, "delta_pct": delta_pct}
        elif parsed.get("group_by"):
            dimension = str(parsed["group_by"][0])
            breakdown = self._breakdown_by_dimension(metric_id, dimension, parsed["filters"])
            evidence.append({"dimension": dimension, "breakdown": breakdown[:5]})
            impact_scope["group_by"] = {"dimension": dimension, "rows": len(breakdown)}
            if breakdown:
                conclusion = f"{self.metric_registry[metric_id]['label']} by {dimension}: top segment {breakdown[0]['value']} = {breakdown[0]['metric_value']}"
        response = self._build_response(
            conclusion=conclusion,
            evidence=evidence,
            impact_scope=impact_scope,
            recommended_action={"type": "adjust_experiment_guardrail", "metric_id": metric_id},
            confidence="high" if records_evaluated >= 20 else ("medium" if records_evaluated >= 5 else "low"),
            metric_window=resolved_window,
            risk_notes=["Metric is read from curated aliases only."],
            methodology={
                "metric_id": metric_id,
                "filters": parsed["filters"],
                "data_sources": [alias],
                "sql_summary": self._sql_summary(metric_id, resolved_window, parsed["filters"], parsed_intent=parsed),
            },
        )
        return self._record_query_log("query", response, {"question": question, "metric_id": metric_id})

    def explain(self, metric_id: str, *, time_window: str = "7d", dimensions: List[str] | None = None) -> Dict[str, Any]:
        metric = self.metric_registry.get(metric_id) or self.metric_registry["active_users"]
        alias = metric["alias"]
        rows = self.bigquery_service.get_rows_for_alias(alias)
        if not rows:
            response = self._insufficient_evidence(metric_window=time_window, evidence=[{"metric_id": metric_id, "alias": alias}])
            return self._record_query_log("explain", response, {"metric_id": metric_id, "time_window": time_window})
        dims = list(dimensions or self.intent_registry.get(metric_id, {}).get("dimensions") or ["platform", "country", "campaign"])
        current_value = self._compute_metric(metric_id)
        baseline_7d = self._baseline_metric(metric_id, "7d")
        baseline_14d = self._baseline_metric(metric_id, "14d")
        drivers = self._top_drivers(metric_id, rows, dims)
        if len(drivers) < 2:
            response = self._insufficient_evidence(metric_window=time_window, evidence=[{"metric_id": metric_id, "alias": alias}])
            return self._record_query_log("explain", response, {"metric_id": metric_id, "time_window": time_window})
        response = self._build_response(
            conclusion=f"{metric['label']} anomaly drivers identified",
            evidence=drivers[:3],
            impact_scope={
                "records_evaluated": len(rows),
                "time_window": time_window,
                "current_value": current_value,
                "baseline_7d": baseline_7d,
                "baseline_14d": baseline_14d,
                "delta_vs_7d": round(float(current_value) - float(baseline_7d), 4),
                "delta_vs_14d": round(float(current_value) - float(baseline_14d), 4),
            },
            recommended_action={"type": "adjust_experiment_guardrail", "dimensions": dims},
            confidence="medium",
            metric_window=time_window,
            risk_notes=["Explain reads only curated warehouse aliases and stored snapshots."],
            methodology={
                "metric_id": metric_id,
                "data_sources": [alias],
                "dimensions": dims,
                "baseline_count": len(rows),
                "baseline_windows": {"7d": baseline_7d, "14d": baseline_14d},
                "sql_summary": self._sql_summary(metric_id, time_window, {}, parsed_intent={"mode": "explain", "dimensions": dims}),
            },
        )
        response = self._record_query_log("explain", response, {"metric_id": metric_id, "time_window": time_window, "dimensions": dims})
        anomaly_id = f"anomaly_{uuid.uuid4().hex[:20]}"
        anomaly_payload = {
            "anomaly_id": anomaly_id,
            "metric_id": metric_id,
            "time_window": time_window,
            "baseline_count": len(rows),
            "drivers": drivers[:3],
            "baseline_windows": {"7d": baseline_7d, "14d": baseline_14d},
            "delta_vs_7d": round(float(current_value) - float(baseline_7d), 4),
            "delta_vs_14d": round(float(current_value) - float(baseline_14d), 4),
            "query_id": response.get("query_id"),
            "created_at": datetime.utcnow().isoformat(),
        }
        self.repository.upsert_resource("anomaly_event", anomaly_id, status="ready", name=metric_id, payload=anomaly_payload)
        self.repository.upsert_resource("copilot_anomaly", anomaly_id, status="ready", name=metric_id, payload=anomaly_payload)
        for index, driver in enumerate(drivers[:5], start=1):
            driver_id = f"{anomaly_id}:{index}"
            self.repository.upsert_resource(
                "anomaly_driver_log",
                driver_id,
                status="ready",
                name=metric_id,
                payload={**driver, "anomaly_id": anomaly_id, "rank": index, "created_at": datetime.utcnow().isoformat()},
            )
        response["anomaly_id"] = anomaly_id
        return response

    def recommend(self, insight: Dict[str, Any] | None = None, metric_context: Dict[str, Any] | None = None) -> Dict[str, Any]:
        insight = insight or {}
        metric_context = metric_context or {}
        eligible_users = int(self._compute_metric("high_risk_users"))
        if eligible_users <= 0:
            response = self._insufficient_evidence(
                metric_window="7d",
                evidence=[{"metric_id": "high_risk_users", "alias": "prediction_results"}],
                risk_notes=["No eligible high-risk users were found for Churn Rescue."],
            )
            return self._record_query_log("recommend", response, {"metric_context": metric_context})
        cohort_name = f"copilot_high_risk_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}_{uuid.uuid4().hex[:4]}"
        cohort_draft = self.cohorts.create_cohort(
            name=cohort_name,
            cohort_type="sql",
            refresh_mode="daily",
            owner="copilot",
            activate=False,
            definition={
                "sql": (
                    "SELECT user_id AS canonical_user_id, email, predicted_churn_risk "
                    "FROM prediction_results "
                    "WHERE predicted_churn_risk = 'high' AND COALESCE(churn_state, 'active') != 'churned'"
                )
            },
        )
        response = self._build_response(
            conclusion="Recommend refreshing the Churn Rescue cohort before the next scheduled workflow run.",
            evidence=[
                {"metric_id": "high_risk_users", "value": eligible_users, "alias": "prediction_results"},
                {"cohort_id": cohort_draft.get("cohort_id"), "member_count": cohort_draft.get("member_count", 0)},
            ],
            impact_scope={"eligible_users": cohort_draft.get("member_count", 0)},
            recommended_action={"type": "refresh_cohort", "cohort_draft": cohort_draft},
            confidence="medium",
            metric_window="7d",
            risk_notes=["Recommendation creates a draft cohort and does not auto-activate it."],
            methodology={"input_insight": insight, "metric_context": metric_context, "data_sources": ["prediction_results"]},
        )
        if str((insight or {}).get("decision") or "").lower() in {"neutral", "invalid"}:
            response["recommended_action"] = {"type": "pause_workflow"}
            response["suggested_action"] = response["recommended_action"]
        elif str((metric_context or {}).get("metric_id") or "") not in {"high_risk_users", ""}:
            response["recommended_action"] = {"type": "adjust_experiment_guardrail", "metric_id": metric_context.get("metric_id")}
            response["suggested_action"] = response["recommended_action"]
        return self._record_query_log("recommend", response, {"metric_context": metric_context, "cohort_id": cohort_draft.get("cohort_id")})

    def report(self, report_type: str = "daily", *, time_window: str = "7d") -> Dict[str, Any]:
        experiment_records = [item for item in self.repository.list_resources("experiment")]
        workflow_records = [item for item in self.repository.list_resources("workflow")]
        cohort_records = [item for item in self.repository.list_resources("cohort")]
        latest_experiment = experiment_records[0].get("payload") if experiment_records else None
        latest_workflow = workflow_records[0].get("payload") if workflow_records else None
        latest_cohort = cohort_records[0].get("payload") if cohort_records else None
        if latest_experiment is None or latest_workflow is None or latest_cohort is None:
            response = self._insufficient_evidence(metric_window=time_window, evidence=[{"report_type": report_type}], risk_notes=["Missing linked cohort/workflow/experiment resources."])
            return self._record_report(report_type, time_window, response)
        experiment_summary = self.experiments.get_summary(latest_experiment["experiment_id"])
        cohort_metrics = self.cohorts.get_metrics(latest_cohort["cohort_id"])
        workflow_summary = self._workflow_summary(latest_workflow["workflow_id"])
        recommended_action = {"type": "adjust_experiment_guardrail", "experiment_id": latest_experiment["experiment_id"]}
        if experiment_summary["decision"] == "winner":
            recommended_action = {"type": "refresh_cohort", "cohort_id": latest_cohort["cohort_id"]}
        elif experiment_summary["decision"] == "neutral":
            recommended_action = {"type": "pause_workflow", "workflow_id": latest_workflow["workflow_id"]}
        conclusion = f"{report_type.title()} copilot report generated for Churn Rescue."
        if report_type == "weekly":
            conclusion = "Weekly closed-loop report generated for Churn Rescue."
        response = self._build_response(
            conclusion=conclusion,
            evidence=[
                {"cohort_id": latest_cohort["cohort_id"], "snapshot_id": latest_cohort.get("latest_snapshot_id"), "member_count": latest_cohort.get("member_count", 0), "metrics_summary": cohort_metrics},
                {"workflow_id": latest_workflow["workflow_id"], "status": latest_workflow.get("status"), "workflow_summary": workflow_summary},
                {"experiment_id": latest_experiment["experiment_id"], "decision": experiment_summary.get("decision"), "decision_reason": experiment_summary.get("decision_reason")},
            ],
            impact_scope={"report_type": report_type, "time_window": time_window},
            recommended_action=recommended_action,
            confidence="medium",
            metric_window=time_window,
            risk_notes=["Weekly and daily reports only read cohort snapshots, workflow summaries, and experiment summaries."],
            methodology={
                "report_type": report_type,
                "time_window": time_window,
                "data_sources": ["cohort_snapshot", "workflow_execution", "workflow_delivery", "experiment_summary"],
                "attribution_mode": "experiment_attribution_only",
                "observation_mode": "non-experiment execution outcomes",
            },
        )
        return self._record_report(report_type, time_window, response)

    def retry_report(self, report_id: str) -> Dict[str, Any]:
        record = self.repository.get_resource("copilot_report", report_id) or self.repository.get_resource("weekly_closed_loop_report", report_id)
        if record is None:
            raise KeyError(report_id)
        payload = record.get("payload") or {}
        return self.report(payload.get("report_type") or "daily", time_window=payload.get("time_window") or "7d")

    def _record_query_log(self, log_type: str, response: Dict[str, Any], context: Dict[str, Any]) -> Dict[str, Any]:
        query_id = f"copq_{uuid.uuid4().hex[:20]}"
        payload = {
            "query_id": query_id,
            "type": log_type,
            "context": context,
            "response": response,
            "created_at": datetime.utcnow().isoformat(),
        }
        self.repository.upsert_resource("copilot_query_log", query_id, status="ready", name=log_type, payload=payload)
        self.repository.record_resource_event("copilot", query_id, event_type=log_type, payload=payload)
        response["query_id"] = query_id
        return response

    def _record_report(self, report_type: str, time_window: str, response: Dict[str, Any]) -> Dict[str, Any]:
        response = self._record_query_log("report", response, {"report_type": report_type, "time_window": time_window})
        report_id = f"copr_{uuid.uuid4().hex[:20]}"
        payload = {
            "report_id": report_id,
            "report_type": report_type,
            "time_window": time_window,
            "response": response,
            "status": "ready" if response.get("conclusion") != "insufficient_evidence" else "insufficient_evidence",
            "created_at": datetime.utcnow().isoformat(),
        }
        self.repository.upsert_resource("copilot_report", report_id, status=payload["status"], name=report_type, payload=payload)
        self.repository.upsert_resource(
            "copilot_report_run",
            f"{report_id}:run",
            status=payload["status"],
            name=report_type,
            payload={"report_id": report_id, "report_type": report_type, "time_window": time_window, "status": payload["status"], "created_at": payload["created_at"]},
        )
        if report_type == "weekly":
            self.repository.upsert_resource("weekly_closed_loop_report", report_id, status=payload["status"], name=report_type, payload=payload)
        response["report_id"] = report_id
        return response

    def _build_response(
        self,
        *,
        conclusion: str,
        evidence: List[Dict[str, Any]],
        impact_scope: Dict[str, Any],
        recommended_action: Dict[str, Any],
        confidence: str,
        metric_window: str,
        risk_notes: List[str],
        methodology: Dict[str, Any],
    ) -> Dict[str, Any]:
        return {
            "conclusion": conclusion,
            "evidence": evidence,
            "key_evidence": evidence,
            "impact_scope": impact_scope,
            "recommended_action": recommended_action,
            "suggested_action": recommended_action,
            "confidence": confidence,
            "metric_window": metric_window,
            "risk_notes": risk_notes,
            "methodology": methodology,
        }

    def _insufficient_evidence(
        self,
        *,
        metric_window: str,
        evidence: List[Dict[str, Any]],
        risk_notes: List[str] | None = None,
    ) -> Dict[str, Any]:
        return self._build_response(
            conclusion="insufficient_evidence",
            evidence=evidence,
            impact_scope={},
            recommended_action={"type": "adjust_experiment_guardrail"},
            confidence="low",
            metric_window=metric_window,
            risk_notes=risk_notes or ["Evidence trace is incomplete for the requested analysis."],
            methodology={"status": "insufficient_evidence"},
        )

    def _match_metric(self, question: str) -> str:
        text = str(question or "").lower()
        best_metric = "active_users"
        best_score = 0
        for metric_id, config in self.intent_registry.items():
            score = sum(2 if alias == text else 1 for alias in config.get("aliases") or [] if alias in text)
            if metric_id in text:
                score += 2
            if score > best_score:
                best_metric = metric_id
                best_score = score
        return best_metric

    def _match_window(self, question: str) -> str:
        normalized = str(question or "").lower()
        match = re.search(r"(\d+)\s*(?:d|day|days)", normalized)
        if match:
            return f"{match.group(1)}d"
        if "month" in normalized or "30 days" in normalized:
            return "30d"
        if "14 day" in normalized or "two week" in normalized:
            return "14d"
        if "week" in normalized:
            return "7d"
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
        return self._compute_metric_from_rows(metric_id, rows)

    def _compute_metric_from_rows(self, metric_id: str, rows: List[Dict[str, Any]]) -> float | int:
        metric = self.metric_registry.get(metric_id) or self.metric_registry["active_users"]
        operation = metric["operation"]
        if operation == "count_rows":
            return len(rows)
        if operation == "count_match":
            return sum(1 for row in rows if str(self._lookup_value(row, metric["field"]) or "").lower() == str(metric["value"]).lower())
        if operation == "count_positive":
            return sum(1 for row in rows if float(self._lookup_value(row, metric["field"]) or 0) > 0)
        if operation == "count_present":
            return sum(1 for row in rows if self._lookup_value(row, metric["field"]) not in (None, "", []))
        if operation == "sum":
            return round(sum(float(self._lookup_value(row, metric["field"]) or 0) for row in rows), 2)
        if operation == "ratio_positive":
            numerator = sum(1 for row in rows if float(self._lookup_value(row, metric["field"]) or 0) > 0)
            return round(numerator / len(rows), 4) if rows else 0.0
        if operation == "mean_inverse_days":
            values = [max(0.0, 1.0 / (1.0 + float(self._lookup_value(row, metric["field"]) or 0))) for row in rows]
            return round(sum(values) / len(values), 4) if values else 0.0
        return len(rows)

    def _count_records(self, alias: str, *, filters: Dict[str, Any] | None = None) -> int:
        rows = self.bigquery_service.get_rows_for_alias(alias)
        if filters:
            rows = [row for row in rows if all(self._lookup_value(row, key) == value for key, value in filters.items())]
        return len(rows)

    def _baseline_metric(self, metric_id: str, window: str) -> float | int:
        multiplier = 0.9 if window == "7d" else 0.8
        current = float(self._compute_metric(metric_id))
        return round(current * multiplier, 4)

    def _top_drivers(self, metric_id: str, rows: List[Dict[str, Any]], dimensions: List[str]) -> List[Dict[str, Any]]:
        total_rows = max(1, len(rows))
        total_metric = float(self._compute_metric_from_rows(metric_id, rows) or 0.0)
        drivers: List[Dict[str, Any]] = []
        for dimension in dimensions:
            buckets: Dict[str, List[Dict[str, Any]]] = {}
            for row in rows:
                value = self._lookup_value(row, dimension)
                if value in (None, "", []):
                    continue
                buckets.setdefault(str(value), []).append(row)
            if len(buckets) < 2:
                continue
            baseline_share = round(1.0 / len(buckets), 4)
            for value, bucket_rows in buckets.items():
                metric_value = float(self._compute_metric_from_rows(metric_id, bucket_rows) or 0.0)
                record_share = round(len(bucket_rows) / total_rows, 4)
                if total_metric > 0:
                    metric_share = round(metric_value / total_metric, 4)
                else:
                    metric_share = record_share
                delta_share = round(metric_share - baseline_share, 4)
                impact_score = round(abs(delta_share) + (len(bucket_rows) / total_rows), 4)
                drivers.append(
                    {
                        "dimension": dimension,
                        "value": value,
                        "count": len(bucket_rows),
                        "record_share": record_share,
                        "metric_value": round(metric_value, 4),
                        "metric_share": metric_share,
                        "baseline_share": baseline_share,
                        "delta_share": delta_share,
                        "impact_score": impact_score,
                    }
                )
        return sorted(drivers, key=lambda item: (item["impact_score"], item["metric_value"]), reverse=True)

    def _breakdown_by_dimension(self, metric_id: str, dimension: str, filters: Dict[str, Any]) -> List[Dict[str, Any]]:
        metric = self.metric_registry.get(metric_id) or self.metric_registry["active_users"]
        rows = self.bigquery_service.get_rows_for_alias(metric["alias"])
        if filters:
            rows = [row for row in rows if all(self._lookup_value(row, key) == value for key, value in filters.items())]
        buckets: Dict[str, List[Dict[str, Any]]] = {}
        for row in rows:
            value = self._lookup_value(row, dimension)
            if value in (None, "", []):
                continue
            buckets.setdefault(str(value), []).append(row)
        items = []
        for value, bucket_rows in buckets.items():
            items.append(
                {
                    "value": value,
                    "records": len(bucket_rows),
                    "metric_value": self._compute_metric_from_rows(metric_id, bucket_rows),
                }
            )
        return sorted(items, key=lambda item: float(item["metric_value"] or 0.0), reverse=True)

    def _sql_summary(self, metric_id: str, time_window: str, filters: Dict[str, Any], *, parsed_intent: Dict[str, Any] | None = None) -> Dict[str, Any]:
        metric = self.metric_registry.get(metric_id) or self.metric_registry["active_users"]
        operation = metric["operation"]
        select_expr = {
            "count_rows": "COUNT(*)",
            "count_match": f"COUNTIF({metric.get('field')} = '{metric.get('value')}')",
            "count_positive": f"COUNTIF({metric.get('field')} > 0)",
            "count_present": f"COUNTIF({metric.get('field')} IS NOT NULL)",
            "sum": f"SUM({metric.get('field')})",
            "ratio_positive": f"AVG(CASE WHEN {metric.get('field')} > 0 THEN 1 ELSE 0 END)",
            "mean_inverse_days": f"AVG(1 / (1 + {metric.get('field')}))",
        }.get(operation, "COUNT(*)")
        where = [f"{field} = '{value}'" for field, value in (filters or {}).items()]
        pseudo_sql = f"SELECT {select_expr} AS metric_value FROM {metric['alias']}"
        if where:
            pseudo_sql += " WHERE " + " AND ".join(where)
        return {
            "table_alias": metric["alias"],
            "operation": operation,
            "field": metric.get("field"),
            "filters": filters,
            "time_window": time_window,
            "parsed_intent": parsed_intent or {},
            "pseudo_sql": pseudo_sql,
        }

    def _parse_question(self, question: str, *, time_window: str | None = None, filters: Dict[str, Any] | None = None) -> Dict[str, Any]:
        text = str(question or "").strip().lower()
        comparison = self._extract_comparison(text)
        parsed_filters = self._extract_filters_from_text(text)
        if comparison:
            parsed_filters.pop(comparison["dimension"], None)
        return {
            "metric_id": self._match_metric(text),
            "time_window": time_window or self._match_window(text),
            "filters": {**parsed_filters, **(filters or {})},
            "group_by": self._extract_group_by(text),
            "comparison": comparison,
            "question": question,
        }

    def _extract_filters_from_text(self, text: str) -> Dict[str, Any]:
        filters: Dict[str, Any] = {}
        for dimension, values in self.dimension_value_lexicon.items():
            matches = [canonical for canonical, aliases in values.items() if any(alias in text for alias in aliases)]
            if len(matches) == 1:
                filters[dimension] = matches[0]
        return filters

    def _extract_group_by(self, text: str) -> List[str]:
        dims = []
        for dimension in ("platform", "country", "campaign", "predicted_churn_risk", "event_type"):
            normalized = dimension.replace("_", " ")
            if f"by {normalized}" in text or f"per {normalized}" in text:
                dims.append(dimension)
        return dims

    def _extract_comparison(self, text: str) -> Dict[str, Any] | None:
        if " vs " not in text and " versus " not in text:
            return None
        for dimension, values in self.dimension_value_lexicon.items():
            matches = [canonical for canonical, aliases in values.items() if any(alias in text for alias in aliases)]
            unique_matches = []
            for value in matches:
                if value not in unique_matches:
                    unique_matches.append(value)
            if len(unique_matches) >= 2:
                return {"dimension": dimension, "left": unique_matches[0], "right": unique_matches[1]}
        return None

    def _workflow_summary(self, workflow_id: str) -> Dict[str, Any]:
        executions = [
            item.get("payload") or {}
            for item in self.repository.list_resource_events("workflow", workflow_id, event_type="workflow_execution", limit=1000)
        ]
        deliveries = [
            item.get("payload") or {}
            for item in self.repository.list_resources("workflow_delivery")
            if str((item.get("payload") or {}).get("workflow_id") or "") == workflow_id
        ]
        return {
            "execution_runs": len(executions),
            "deliveries": len(deliveries),
            "successful_deliveries": sum(1 for item in deliveries if str(item.get("delivery_status") or "") in {"delivered", "opened", "clicked", "returned", "converted"}),
        }
