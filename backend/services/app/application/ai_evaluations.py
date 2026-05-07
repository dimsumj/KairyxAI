from __future__ import annotations

import re
import uuid
from datetime import datetime
from typing import Any, Dict


AI_EVALUATION_RESOURCE_TYPE = "ai_evaluation_record"
AI_EVALUATION_EXPORT_FORMAT = "ai_evaluation_record.v1"

ALLOWED_EVALUATION_TYPES = {
    "answer_relevance",
    "campaign_copy_usefulness",
    "citation_coverage",
    "prompt_to_artifact_completion",
    "retrieval_quality",
}
ALLOWED_OUTCOMES = {
    "accepted",
    "completed",
    "edited",
    "failed",
    "neutral",
    "not_useful",
    "rejected",
    "useful",
}
POSITIVE_OUTCOMES = {"accepted", "completed", "useful"}
NEGATIVE_OUTCOMES = {"failed", "not_useful", "rejected"}
MAX_LIST_LIMIT = 500


class AIEvaluationService:
    def __init__(self, repository):
        self.repository = repository

    def record_evaluation(
        self,
        *,
        evaluation_type: str,
        target_type: str,
        target_id: str | None = None,
        outcome: str = "neutral",
        score: float | None = None,
        dimensions: Dict[str, Any] | None = None,
        citation_ids: list[str] | None = None,
        artifact_ids: list[str] | None = None,
        prompt_summary: str | None = None,
        response_summary: str | None = None,
        comments: str | None = None,
        source: str = "operator",
        metadata: Dict[str, Any] | None = None,
        evaluated_by: str | None = None,
    ) -> Dict[str, Any]:
        normalized_type = _normalize_evaluation_type(evaluation_type)
        normalized_target_type = _normalize_token(target_type, "target_type", max_length=80)
        normalized_outcome = _normalize_outcome(outcome)
        normalized_dimensions = _normalize_dimensions(dimensions or {})
        normalized_score, score_source = _resolve_score(score, normalized_dimensions, normalized_outcome)
        evaluation_id = f"aieval_{uuid.uuid4().hex[:20]}"
        payload = {
            "evaluation_id": evaluation_id,
            "evaluation_type": normalized_type,
            "target_type": normalized_target_type,
            "target_id": _clean_text(target_id, max_length=140),
            "outcome": normalized_outcome,
            "score": normalized_score,
            "score_source": score_source,
            "dimensions": normalized_dimensions,
            "citation_ids": _normalize_ids(citation_ids or []),
            "artifact_ids": _normalize_ids(artifact_ids or []),
            "prompt_summary": _clean_text(prompt_summary, max_length=500),
            "response_summary": _clean_text(response_summary, max_length=500),
            "comments": _clean_text(comments, max_length=1000),
            "source": _normalize_token(source, "source", max_length=40),
            "metadata": _normalize_metadata(metadata or {}),
            "evaluated_by": _clean_text(evaluated_by, max_length=120) or "system",
            "status": "recorded",
            "recorded_at": datetime.utcnow().isoformat(),
            "export": {
                "format": AI_EVALUATION_EXPORT_FORMAT,
                "resource_id": evaluation_id,
                "includes": ["evaluation", "dimensions", "citations", "artifacts"],
            },
        }
        record = self.repository.upsert_resource(
            AI_EVALUATION_RESOURCE_TYPE,
            evaluation_id,
            status="recorded",
            name=normalized_type,
            payload=payload,
        )
        self.repository.record_resource_event(
            AI_EVALUATION_RESOURCE_TYPE,
            evaluation_id,
            event_type="ai_evaluation_recorded",
            payload={
                "evaluation_id": evaluation_id,
                "evaluation_type": normalized_type,
                "target_type": normalized_target_type,
                "outcome": normalized_outcome,
                "score": normalized_score,
            },
        )
        return self._resource_to_evaluation(record)

    def list_evaluations(
        self,
        *,
        evaluation_type: str | None = None,
        target_type: str | None = None,
        target_id: str | None = None,
        limit: int = 100,
    ) -> list[Dict[str, Any]]:
        normalized_type = _normalize_optional_evaluation_type(evaluation_type)
        normalized_target_type = _normalize_optional_token(target_type, max_length=80)
        normalized_target_id = _clean_text(target_id, max_length=140)
        normalized_limit = max(1, min(int(limit or 100), MAX_LIST_LIMIT))
        items = []
        for record in self.repository.list_resources(AI_EVALUATION_RESOURCE_TYPE):
            item = self._resource_to_evaluation(record)
            if normalized_type and item.get("evaluation_type") != normalized_type:
                continue
            if normalized_target_type and item.get("target_type") != normalized_target_type:
                continue
            if normalized_target_id and item.get("target_id") != normalized_target_id:
                continue
            items.append(item)
            if len(items) >= normalized_limit:
                break
        return items

    def get_evaluation(self, evaluation_id: str) -> Dict[str, Any] | None:
        record = self.repository.get_resource(AI_EVALUATION_RESOURCE_TYPE, evaluation_id)
        return self._resource_to_evaluation(record) if record is not None else None

    def export_evaluation(self, evaluation_id: str) -> Dict[str, Any] | None:
        evaluation = self.get_evaluation(evaluation_id)
        if evaluation is None:
            return None
        return {
            "format": AI_EVALUATION_EXPORT_FORMAT,
            "evaluation": {
                key: value
                for key, value in evaluation.items()
                if key not in {"audit_id", "masked_fields"}
            },
        }

    def summarize(self, *, evaluation_type: str | None = None, target_type: str | None = None) -> Dict[str, Any]:
        items = self.list_evaluations(evaluation_type=evaluation_type, target_type=target_type, limit=MAX_LIST_LIMIT)
        scores = [float(item["score"]) for item in items if item.get("score") is not None]
        outcome_counts: Dict[str, int] = {}
        type_counts: Dict[str, int] = {}
        target_type_counts: Dict[str, int] = {}
        dimension_values: Dict[str, list[float]] = {}
        for item in items:
            outcome_counts[str(item.get("outcome") or "neutral")] = outcome_counts.get(str(item.get("outcome") or "neutral"), 0) + 1
            type_counts[str(item.get("evaluation_type") or "unknown")] = type_counts.get(str(item.get("evaluation_type") or "unknown"), 0) + 1
            target_type_counts[str(item.get("target_type") or "unknown")] = target_type_counts.get(str(item.get("target_type") or "unknown"), 0) + 1
            for key, value in dict(item.get("dimensions") or {}).items():
                dimension_values.setdefault(key, []).append(float(value))
        total = len(items)
        positive_count = sum(outcome_counts.get(key, 0) for key in POSITIVE_OUTCOMES)
        negative_count = sum(outcome_counts.get(key, 0) for key in NEGATIVE_OUTCOMES)
        return {
            "total_records": total,
            "average_score": round(sum(scores) / len(scores), 4) if scores else None,
            "positive_rate": round(positive_count / total, 4) if total else 0.0,
            "negative_rate": round(negative_count / total, 4) if total else 0.0,
            "edited_rate": round(outcome_counts.get("edited", 0) / total, 4) if total else 0.0,
            "outcome_counts": outcome_counts,
            "evaluation_type_counts": type_counts,
            "target_type_counts": target_type_counts,
            "dimension_averages": {
                key: round(sum(values) / len(values), 4)
                for key, values in sorted(dimension_values.items())
                if values
            },
            "latest_recorded_at": max((str(item.get("recorded_at") or "") for item in items), default=""),
        }

    @staticmethod
    def _resource_to_evaluation(record: Dict[str, Any]) -> Dict[str, Any]:
        payload = dict(record.get("payload") or {})
        return {
            **payload,
            "evaluation_id": payload.get("evaluation_id") or record.get("resource_id"),
            "status": payload.get("status") or record.get("status") or "recorded",
            "created_at": record.get("created_at"),
            "updated_at": record.get("updated_at"),
            "tenant_id": record.get("tenant_id") or payload.get("tenant_id"),
            "project_id": record.get("project_id") or payload.get("project_id"),
            "created_by": record.get("created_by") or payload.get("created_by") or "system",
            "updated_by": record.get("updated_by") or payload.get("updated_by") or "system",
            "correlation_id": record.get("correlation_id") or payload.get("correlation_id") or "",
        }


def _normalize_evaluation_type(value: str) -> str:
    normalized = _normalize_token(value, "evaluation_type", max_length=80)
    if normalized not in ALLOWED_EVALUATION_TYPES:
        raise ValueError(f"evaluation_type must be one of: {', '.join(sorted(ALLOWED_EVALUATION_TYPES))}.")
    return normalized


def _normalize_optional_evaluation_type(value: str | None) -> str:
    if value is None or str(value).strip() == "":
        return ""
    return _normalize_evaluation_type(value)


def _normalize_outcome(value: str) -> str:
    normalized = _normalize_token(value or "neutral", "outcome", max_length=40)
    if normalized not in ALLOWED_OUTCOMES:
        raise ValueError(f"outcome must be one of: {', '.join(sorted(ALLOWED_OUTCOMES))}.")
    return normalized


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


def _normalize_dimensions(dimensions: Dict[str, Any]) -> Dict[str, float]:
    normalized: Dict[str, float] = {}
    for key, value in dimensions.items():
        normalized_key = _normalize_token(str(key), "dimension", max_length=80)
        normalized_value = _normalize_score_value(value, field_name=f"dimensions.{normalized_key}")
        normalized[normalized_key] = normalized_value
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


def _resolve_score(score: float | None, dimensions: Dict[str, float], outcome: str) -> tuple[float | None, str]:
    if score is not None:
        return _normalize_score_value(score, field_name="score"), "explicit"
    if dimensions:
        return round(sum(dimensions.values()) / len(dimensions), 4), "dimensions"
    if outcome in POSITIVE_OUTCOMES:
        return 1.0, "outcome"
    if outcome in NEGATIVE_OUTCOMES:
        return 0.0, "outcome"
    if outcome == "edited":
        return 0.5, "outcome"
    return None, "none"


def _normalize_score_value(value: Any, *, field_name: str) -> float:
    try:
        normalized = float(value)
    except (TypeError, ValueError):
        raise ValueError(f"{field_name} must be a number between 0 and 1.")
    if normalized < 0 or normalized > 1:
        raise ValueError(f"{field_name} must be between 0 and 1.")
    return round(normalized, 4)
