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
MAX_AUTO_GRADE_TEXT_CHARS = 4000
AUTO_GRADER_NAME = "deterministic_ai_grader_v1"
AUTO_GRADER_EXPORT_FORMAT = "ai_evaluation_grading.v1"
STOP_WORDS = {
    "a",
    "an",
    "and",
    "are",
    "as",
    "at",
    "be",
    "by",
    "for",
    "from",
    "how",
    "i",
    "in",
    "is",
    "it",
    "me",
    "of",
    "on",
    "or",
    "our",
    "should",
    "the",
    "this",
    "to",
    "we",
    "with",
    "you",
}
CAMPAIGN_COPY_TARGET_HINTS = {"campaign", "copy", "email", "push", "message", "workflow"}
CAMPAIGN_ACTION_TERMS = {"back", "claim", "come", "continue", "play", "return", "reward", "save", "saved", "start"}


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

    def auto_grade(
        self,
        *,
        target_type: str,
        target_id: str | None = None,
        prompt: str,
        response: str,
        citations: list[Dict[str, Any]] | None = None,
        artifacts: list[Dict[str, Any]] | None = None,
        expected_artifact_type: str | None = None,
        generated_title: str | None = None,
        generated_body: str | None = None,
        source: str = "auto_grader",
        metadata: Dict[str, Any] | None = None,
    ) -> Dict[str, Any]:
        normalized_target_type = _normalize_token(target_type, "target_type", max_length=80)
        normalized_target_id = _clean_text(target_id, max_length=140)
        normalized_prompt = _required_text(prompt, "prompt", max_length=MAX_AUTO_GRADE_TEXT_CHARS)
        normalized_response = _required_text(response, "response", max_length=MAX_AUTO_GRADE_TEXT_CHARS)
        normalized_citations = _normalize_evidence_items(citations or [])
        normalized_artifacts = _normalize_artifact_items(artifacts or [])
        normalized_expected_artifact = _clean_text(expected_artifact_type, max_length=80)
        normalized_title = _clean_text(generated_title, max_length=240)
        normalized_body = _clean_text(generated_body, max_length=2000)
        normalized_source = _normalize_token(source or "auto_grader", "source", max_length=40)
        normalized_metadata = _normalize_metadata(metadata or {})
        grading_id = f"aigrade_{uuid.uuid4().hex[:20]}"
        artifact_ids = [item["resource_id"] for item in normalized_artifacts if item.get("resource_id")]
        citation_ids = [item["citation_id"] for item in normalized_citations if item.get("citation_id")]
        base_metadata = {
            **normalized_metadata,
            "grading_id": grading_id,
            "grader": AUTO_GRADER_NAME,
            "expected_artifact_type": normalized_expected_artifact,
        }
        grade_specs = [
            _retrieval_quality_grade(normalized_prompt, normalized_response, normalized_citations),
            _citation_coverage_grade(normalized_response, normalized_citations),
            _answer_relevance_grade(normalized_prompt, normalized_response, normalized_citations),
            _prompt_to_artifact_grade(normalized_expected_artifact, normalized_artifacts),
        ]
        if _should_grade_campaign_copy(normalized_target_type, normalized_title, normalized_body):
            grade_specs.append(_campaign_copy_grade(normalized_title, normalized_body or normalized_response, normalized_citations))
        evaluations = [
            self.record_evaluation(
                evaluation_type=spec["evaluation_type"],
                target_type=normalized_target_type,
                target_id=normalized_target_id,
                outcome=_outcome_for_score(float(spec["score"])),
                score=float(spec["score"]),
                dimensions=dict(spec.get("dimensions") or {}),
                citation_ids=citation_ids,
                artifact_ids=artifact_ids,
                prompt_summary=normalized_prompt,
                response_summary=normalized_response,
                comments=str(spec.get("comments") or ""),
                source=normalized_source,
                metadata={**base_metadata, **dict(spec.get("metadata") or {})},
                evaluated_by=AUTO_GRADER_NAME,
            )
            for spec in grade_specs
        ]
        scores = [float(item["score"]) for item in evaluations if item.get("score") is not None]
        type_counts: Dict[str, int] = {}
        for item in evaluations:
            evaluation_type = str(item.get("evaluation_type") or "unknown")
            type_counts[evaluation_type] = type_counts.get(evaluation_type, 0) + 1
        return {
            "grading_id": grading_id,
            "target_type": normalized_target_type,
            "target_id": normalized_target_id,
            "evaluations": evaluations,
            "summary": {
                "evaluation_count": len(evaluations),
                "average_score": round(sum(scores) / len(scores), 4) if scores else None,
                "evaluation_type_counts": type_counts,
            },
            "export": {
                "format": AUTO_GRADER_EXPORT_FORMAT,
                "resource_id": grading_id,
                "includes": ["evaluations", "summary", "citations", "artifacts"],
            },
        }

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


def _required_text(value: str | None, field_name: str, *, max_length: int) -> str:
    normalized = _clean_text(value, max_length=max_length)
    if not normalized:
        raise ValueError(f"{field_name} is required.")
    return normalized


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


def _normalize_evidence_items(values: list[Dict[str, Any]]) -> list[Dict[str, Any]]:
    normalized: list[Dict[str, Any]] = []
    for value in values[:20]:
        if not isinstance(value, dict):
            continue
        citation_id = _clean_text(value.get("citation_id") or value.get("citation"), max_length=80)
        text = _clean_text(
            " ".join(
                [
                    str(value.get("snippet") or ""),
                    str(value.get("summary") or ""),
                    str(value.get("text") or ""),
                ]
            ),
            max_length=1200,
        )
        if not citation_id and not text:
            continue
        normalized.append(
            {
                "citation_id": citation_id,
                "text": text,
                "score": _coerce_optional_float(value.get("score")),
            }
        )
    return normalized


def _normalize_artifact_items(values: list[Dict[str, Any]]) -> list[Dict[str, str]]:
    normalized: list[Dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for value in values[:20]:
        if not isinstance(value, dict):
            continue
        resource_type = _clean_text(value.get("resource_type"), max_length=80)
        resource_id = _clean_text(value.get("resource_id") or value.get("artifact_id"), max_length=140)
        if not resource_type and not resource_id:
            continue
        key = (resource_type, resource_id)
        if key in seen:
            continue
        normalized.append({"resource_type": resource_type, "resource_id": resource_id})
        seen.add(key)
    return normalized


def _coerce_optional_float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


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


def _retrieval_quality_grade(prompt: str, response: str, citations: list[Dict[str, Any]]) -> Dict[str, Any]:
    citation_relevance = _average(
        [_term_overlap_score(prompt, str(item.get("text") or "")) for item in citations if item.get("text")]
    )
    citation_score = _average([min(max(float(item.get("score") or 0.0) / 3.0, 0.0), 1.0) for item in citations])
    response_support = _term_overlap_score(response, " ".join(str(item.get("text") or "") for item in citations))
    retrieval_quality = round((citation_relevance * 0.55) + (citation_score * 0.25) + (response_support * 0.20), 4)
    if citations and retrieval_quality == 0:
        retrieval_quality = 0.25
    return {
        "evaluation_type": "retrieval_quality",
        "score": retrieval_quality,
        "dimensions": {
            "retrieval_quality": retrieval_quality,
            "citation_relevance": citation_relevance,
            "citation_rank_quality": citation_score,
            "response_support": response_support,
        },
        "comments": "Deterministic retrieval grade from prompt/citation overlap, citation score, and response support.",
    }


def _citation_coverage_grade(response: str, citations: list[Dict[str, Any]]) -> Dict[str, Any]:
    if not citations:
        coverage = 0.0
    else:
        cited_count = sum(1 for item in citations if _response_references_citation(response, str(item.get("citation_id") or "")))
        coverage = round(cited_count / len(citations), 4)
    return {
        "evaluation_type": "citation_coverage",
        "score": coverage,
        "dimensions": {
            "citation_coverage": coverage,
            "citation_count": min(float(len(citations)) / 5.0, 1.0),
        },
        "comments": "Deterministic citation coverage grade from cited evidence ids found in the response.",
    }


def _answer_relevance_grade(prompt: str, response: str, citations: list[Dict[str, Any]]) -> Dict[str, Any]:
    prompt_overlap = _term_overlap_score(prompt, response)
    support = _term_overlap_score(response, " ".join(str(item.get("text") or "") for item in citations))
    citation_reference_score = 1.0 if any(_response_references_citation(response, str(item.get("citation_id") or "")) for item in citations) else 0.0
    hallucination_risk = round(max(0.0, 1.0 - max(prompt_overlap, support) - (citation_reference_score * 0.35)), 4)
    score = round((prompt_overlap * 0.7) + (support * 0.3), 4)
    return {
        "evaluation_type": "answer_relevance",
        "score": score,
        "dimensions": {
            "answer_relevance": score,
            "prompt_overlap": prompt_overlap,
            "citation_support": support,
            "citation_reference": citation_reference_score,
            "hallucination_risk": hallucination_risk,
        },
        "comments": "Deterministic answer grade from prompt overlap and citation support.",
    }


def _prompt_to_artifact_grade(expected_artifact_type: str, artifacts: list[Dict[str, str]]) -> Dict[str, Any]:
    artifact_types = {str(item.get("resource_type") or "") for item in artifacts}
    if expected_artifact_type:
        completion = 1.0 if expected_artifact_type in artifact_types else 0.0
    else:
        completion = 1.0 if artifacts else 0.0
    return {
        "evaluation_type": "prompt_to_artifact_completion",
        "score": completion,
        "dimensions": {
            "prompt_to_artifact_completion": completion,
            "artifact_presence": 1.0 if artifacts else 0.0,
        },
        "comments": "Deterministic artifact grade from expected artifact type presence.",
    }


def _campaign_copy_grade(title: str, body: str, citations: list[Dict[str, Any]]) -> Dict[str, Any]:
    normalized_title = _clean_text(title, max_length=240)
    normalized_body = _clean_text(body, max_length=2000)
    title_score = 1.0 if 8 <= len(normalized_title) <= 80 else 0.35 if normalized_title else 0.0
    body_score = 1.0 if 20 <= len(normalized_body) <= 240 else 0.5 if normalized_body else 0.0
    body_terms = _terms(f"{normalized_title} {normalized_body}")
    action_score = 1.0 if body_terms.intersection(CAMPAIGN_ACTION_TERMS) else 0.0
    evidence_support = _term_overlap_score(f"{normalized_title} {normalized_body}", " ".join(str(item.get("text") or "") for item in citations))
    copy_score = round((title_score * 0.25) + (body_score * 0.35) + (action_score * 0.2) + (evidence_support * 0.2), 4)
    return {
        "evaluation_type": "campaign_copy_usefulness",
        "score": copy_score,
        "dimensions": {
            "campaign_copy_usefulness": copy_score,
            "title_quality": title_score,
            "body_quality": body_score,
            "action_clarity": action_score,
            "evidence_support": evidence_support,
        },
        "comments": "Deterministic copy grade from title/body shape, action clarity, and evidence support.",
    }


def _should_grade_campaign_copy(target_type: str, title: str, body: str) -> bool:
    target_terms = set(target_type.split("_"))
    return bool(title or body or target_terms.intersection(CAMPAIGN_COPY_TARGET_HINTS))


def _response_references_citation(response: str, citation_id: str) -> bool:
    if not citation_id:
        return False
    normalized_response = str(response or "").lower()
    normalized_id = citation_id.lower()
    return normalized_id in normalized_response or f"[{normalized_id}]" in normalized_response


def _term_overlap_score(source: str, target: str) -> float:
    source_terms = _terms(source)
    if not source_terms:
        return 0.0
    target_terms = _terms(target)
    if not target_terms:
        return 0.0
    return round(len(source_terms.intersection(target_terms)) / len(source_terms), 4)


def _terms(value: str) -> set[str]:
    return {
        term
        for term in re.findall(r"[a-zA-Z0-9_]+", str(value or "").lower())
        if len(term) > 1 and term not in STOP_WORDS
    }


def _average(values: list[float]) -> float:
    return round(sum(values) / len(values), 4) if values else 0.0


def _outcome_for_score(score: float) -> str:
    if score >= 0.75:
        return "useful"
    if score <= 0.35:
        return "not_useful"
    return "neutral"


def _normalize_score_value(value: Any, *, field_name: str) -> float:
    try:
        normalized = float(value)
    except (TypeError, ValueError):
        raise ValueError(f"{field_name} must be a number between 0 and 1.")
    if normalized < 0 or normalized > 1:
        raise ValueError(f"{field_name} must be between 0 and 1.")
    return round(normalized, 4)
