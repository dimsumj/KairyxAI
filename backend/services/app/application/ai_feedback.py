from __future__ import annotations

import re
import uuid
from datetime import datetime
from typing import Any, Dict


AI_FEEDBACK_RESOURCE_TYPE = "ai_feedback_record"
AI_FEEDBACK_EXPORT_FORMAT = "ai_feedback_record.v1"

ALLOWED_FEEDBACK_TYPES = {
    "experiment_outcome",
    "operator_approval",
    "operator_edit",
    "rating",
    "retrieval_click",
    "send_result",
    "workflow_result",
}
ALLOWED_SENTIMENTS = {"negative", "neutral", "positive"}
POSITIVE_FEEDBACK = {"operator_approval", "retrieval_click", "send_result", "workflow_result"}
NEGATIVE_FEEDBACK = {"operator_edit"}
MAX_LIST_LIMIT = 500


class AIFeedbackService:
    def __init__(self, repository):
        self.repository = repository

    def record_feedback(
        self,
        *,
        feedback_type: str,
        target_type: str,
        target_id: str | None = None,
        sentiment: str | None = None,
        weight: float | None = None,
        rating: float | None = None,
        citation_ids: list[str] | None = None,
        artifact_ids: list[str] | None = None,
        related_evaluation_id: str | None = None,
        change_summary: str | None = None,
        outcome_metrics: Dict[str, Any] | None = None,
        comments: str | None = None,
        source: str = "operator",
        metadata: Dict[str, Any] | None = None,
        recorded_by: str | None = None,
    ) -> Dict[str, Any]:
        normalized_type = _normalize_feedback_type(feedback_type)
        normalized_sentiment = _resolve_sentiment(sentiment, normalized_type, rating)
        normalized_weight = _resolve_weight(weight, normalized_sentiment, rating)
        feedback_id = f"aifb_{uuid.uuid4().hex[:20]}"
        payload = {
            "feedback_id": feedback_id,
            "feedback_type": normalized_type,
            "target_type": _normalize_token(target_type, "target_type", max_length=80),
            "target_id": _clean_text(target_id, max_length=140),
            "sentiment": normalized_sentiment,
            "weight": normalized_weight,
            "rating": _normalize_optional_score(rating, field_name="rating"),
            "citation_ids": _normalize_ids(citation_ids or []),
            "artifact_ids": _normalize_ids(artifact_ids or []),
            "related_evaluation_id": _clean_text(related_evaluation_id, max_length=140),
            "change_summary": _clean_text(change_summary, max_length=700),
            "outcome_metrics": _normalize_metrics(outcome_metrics or {}),
            "comments": _clean_text(comments, max_length=1000),
            "source": _normalize_token(source, "source", max_length=40),
            "metadata": _normalize_metadata(metadata or {}),
            "recorded_by": _clean_text(recorded_by, max_length=120) or "system",
            "status": "recorded",
            "recorded_at": datetime.utcnow().isoformat(),
            "export": {
                "format": AI_FEEDBACK_EXPORT_FORMAT,
                "resource_id": feedback_id,
                "includes": ["feedback", "target", "citations", "artifacts", "outcome_metrics"],
            },
        }
        record = self.repository.upsert_resource(
            AI_FEEDBACK_RESOURCE_TYPE,
            feedback_id,
            status="recorded",
            name=normalized_type,
            payload=payload,
        )
        self.repository.record_resource_event(
            AI_FEEDBACK_RESOURCE_TYPE,
            feedback_id,
            event_type="ai_feedback_recorded",
            payload={
                "feedback_id": feedback_id,
                "feedback_type": normalized_type,
                "target_type": payload["target_type"],
                "sentiment": normalized_sentiment,
                "weight": normalized_weight,
            },
        )
        return self._resource_to_feedback(record)

    def list_feedback(
        self,
        *,
        feedback_type: str | None = None,
        target_type: str | None = None,
        target_id: str | None = None,
        limit: int = 100,
    ) -> list[Dict[str, Any]]:
        normalized_type = _normalize_optional_feedback_type(feedback_type)
        normalized_target_type = _normalize_optional_token(target_type, max_length=80)
        normalized_target_id = _clean_text(target_id, max_length=140)
        normalized_limit = max(1, min(int(limit or 100), MAX_LIST_LIMIT))
        items = []
        for record in self.repository.list_resources(AI_FEEDBACK_RESOURCE_TYPE):
            item = self._resource_to_feedback(record)
            if normalized_type and item.get("feedback_type") != normalized_type:
                continue
            if normalized_target_type and item.get("target_type") != normalized_target_type:
                continue
            if normalized_target_id and item.get("target_id") != normalized_target_id:
                continue
            items.append(item)
            if len(items) >= normalized_limit:
                break
        return items

    def get_feedback(self, feedback_id: str) -> Dict[str, Any] | None:
        record = self.repository.get_resource(AI_FEEDBACK_RESOURCE_TYPE, feedback_id)
        return self._resource_to_feedback(record) if record is not None else None

    def export_feedback(self, feedback_id: str) -> Dict[str, Any] | None:
        feedback = self.get_feedback(feedback_id)
        if feedback is None:
            return None
        return {
            "format": AI_FEEDBACK_EXPORT_FORMAT,
            "feedback": {
                key: value
                for key, value in feedback.items()
                if key not in {"audit_id", "masked_fields"}
            },
        }

    def summarize(self, *, target_type: str | None = None) -> Dict[str, Any]:
        items = self.list_feedback(target_type=target_type, limit=MAX_LIST_LIMIT)
        sentiment_counts: Dict[str, int] = {}
        type_counts: Dict[str, int] = {}
        target_counts: Dict[str, int] = {}
        target_weights: Dict[str, float] = {}
        metric_values: Dict[str, list[float]] = {}
        for item in items:
            sentiment = str(item.get("sentiment") or "neutral")
            feedback_type = str(item.get("feedback_type") or "unknown")
            target_key = f"{item.get('target_type') or 'unknown'}:{item.get('target_id') or ''}"
            sentiment_counts[sentiment] = sentiment_counts.get(sentiment, 0) + 1
            type_counts[feedback_type] = type_counts.get(feedback_type, 0) + 1
            target_counts[target_key] = target_counts.get(target_key, 0) + 1
            target_weights[target_key] = round(target_weights.get(target_key, 0.0) + float(item.get("weight") or 0.0), 4)
            for key, value in dict(item.get("outcome_metrics") or {}).items():
                metric_values.setdefault(key, []).append(float(value))
        total = len(items)
        return {
            "total_records": total,
            "positive_rate": round(sentiment_counts.get("positive", 0) / total, 4) if total else 0.0,
            "negative_rate": round(sentiment_counts.get("negative", 0) / total, 4) if total else 0.0,
            "sentiment_counts": sentiment_counts,
            "feedback_type_counts": type_counts,
            "target_counts": target_counts,
            "target_weight_scores": dict(sorted(target_weights.items())),
            "metric_averages": {
                key: round(sum(values) / len(values), 4)
                for key, values in sorted(metric_values.items())
                if values
            },
            "latest_recorded_at": max((str(item.get("recorded_at") or "") for item in items), default=""),
        }

    @staticmethod
    def _resource_to_feedback(record: Dict[str, Any]) -> Dict[str, Any]:
        payload = dict(record.get("payload") or {})
        return {
            **payload,
            "feedback_id": payload.get("feedback_id") or record.get("resource_id"),
            "status": payload.get("status") or record.get("status") or "recorded",
            "created_at": record.get("created_at"),
            "updated_at": record.get("updated_at"),
            "tenant_id": record.get("tenant_id") or payload.get("tenant_id"),
            "project_id": record.get("project_id") or payload.get("project_id"),
            "created_by": record.get("created_by") or payload.get("created_by") or "system",
            "updated_by": record.get("updated_by") or payload.get("updated_by") or "system",
            "correlation_id": record.get("correlation_id") or payload.get("correlation_id") or "",
        }


def _normalize_feedback_type(value: str) -> str:
    normalized = _normalize_token(value, "feedback_type", max_length=80)
    if normalized not in ALLOWED_FEEDBACK_TYPES:
        raise ValueError(f"feedback_type must be one of: {', '.join(sorted(ALLOWED_FEEDBACK_TYPES))}.")
    return normalized


def _normalize_optional_feedback_type(value: str | None) -> str:
    if value is None or str(value).strip() == "":
        return ""
    return _normalize_feedback_type(value)


def _resolve_sentiment(sentiment: str | None, feedback_type: str, rating: float | None) -> str:
    if sentiment is not None and str(sentiment).strip():
        normalized = _normalize_token(sentiment, "sentiment", max_length=40)
        if normalized not in ALLOWED_SENTIMENTS:
            raise ValueError(f"sentiment must be one of: {', '.join(sorted(ALLOWED_SENTIMENTS))}.")
        return normalized
    if rating is not None:
        normalized_rating = _normalize_score_value(rating, field_name="rating")
        if normalized_rating >= 0.67:
            return "positive"
        if normalized_rating <= 0.33:
            return "negative"
    if feedback_type in POSITIVE_FEEDBACK:
        return "positive"
    if feedback_type in NEGATIVE_FEEDBACK:
        return "negative"
    return "neutral"


def _resolve_weight(weight: float | None, sentiment: str, rating: float | None) -> float:
    if weight is not None:
        return _normalize_weight(weight)
    if rating is not None:
        return round((_normalize_score_value(rating, field_name="rating") - 0.5) * 2, 4)
    if sentiment == "positive":
        return 1.0
    if sentiment == "negative":
        return -1.0
    return 0.0


def _normalize_weight(value: Any) -> float:
    try:
        normalized = float(value)
    except (TypeError, ValueError):
        raise ValueError("weight must be a number between -1 and 1.")
    if normalized < -1 or normalized > 1:
        raise ValueError("weight must be between -1 and 1.")
    return round(normalized, 4)


def _normalize_token(value: str, field_name: str, *, max_length: int) -> str:
    normalized = re.sub(r"[^a-zA-Z0-9_]+", "_", str(value or "").strip().lower()).strip("_")
    if not normalized:
        raise ValueError(f"{field_name} is required.")
    return normalized[:max_length]


def _normalize_optional_token(value: str | None, *, max_length: int) -> str:
    if value is None or str(value).strip() == "":
        return ""
    return _normalize_token(value, "filter", max_length=max_length)


def _clean_text(value: str | None, *, max_length: int) -> str:
    normalized = re.sub(r"\s+", " ", str(value or "").strip())
    return normalized[:max_length]


def _normalize_ids(values: list[str]) -> list[str]:
    normalized: list[str] = []
    seen: set[str] = set()
    for value in values:
        cleaned = _clean_text(value, max_length=140)
        if not cleaned or cleaned in seen:
            continue
        normalized.append(cleaned)
        seen.add(cleaned)
    return normalized[:50]


def _normalize_metrics(metrics: Dict[str, Any]) -> Dict[str, float]:
    normalized: Dict[str, float] = {}
    for key, value in metrics.items():
        normalized_key = _normalize_token(str(key), "metric", max_length=80)
        normalized[normalized_key] = _normalize_number(value, field_name=f"outcome_metrics.{normalized_key}")
    return dict(sorted(normalized.items()))


def _normalize_metadata(metadata: Dict[str, Any]) -> Dict[str, Any]:
    normalized: Dict[str, Any] = {}
    for key, value in metadata.items():
        normalized_key = _normalize_token(str(key), "metadata", max_length=80)
        if isinstance(value, bool) or value is None:
            normalized[normalized_key] = value
        elif isinstance(value, (int, float)):
            normalized[normalized_key] = value
        else:
            normalized[normalized_key] = _clean_text(str(value), max_length=250)
    return normalized


def _normalize_optional_score(value: float | None, *, field_name: str) -> float | None:
    if value is None:
        return None
    return _normalize_score_value(value, field_name=field_name)


def _normalize_score_value(value: Any, *, field_name: str) -> float:
    normalized = _normalize_number(value, field_name=field_name)
    if normalized < 0 or normalized > 1:
        raise ValueError(f"{field_name} must be between 0 and 1.")
    return normalized


def _normalize_number(value: Any, *, field_name: str) -> float:
    try:
        return round(float(value), 4)
    except (TypeError, ValueError):
        raise ValueError(f"{field_name} must be a number.")
