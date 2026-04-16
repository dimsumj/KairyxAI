from __future__ import annotations

import asyncio
import csv
import io
import json
import os
import threading
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Optional
from urllib.parse import urlparse

import requests
import uvicorn
from fastapi import APIRouter, BackgroundTasks, HTTPException
from fastapi.responses import Response
from pydantic import BaseModel, Field

from app.main import app
from bigquery_service import BigQueryService
from gcs_service import GcsService
from gemini_client import GeminiClient
from local_job_store import delete_ingestion_checkpoints
from player_modeling_engine import PlayerModelingEngine


CACHE_DIR = ".cache"
PREDICTION_CACHE_DIR = ".cache/predictions"
AUDIT_LOG_FILE = ".audit.log.jsonl"
ACTION_HISTORY_RETENTION_DAYS = 7
ACTION_HISTORY_RETENTION = timedelta(days=ACTION_HISTORY_RETENTION_DAYS)

BIGQUERY_SERVICE_INSTANCE = BigQueryService()
GCS_SERVICE_INSTANCE = GcsService()

IMPORT_JOBS: list[dict[str, Any]] = []
PREDICTION_JOBS: list[dict[str, Any]] = []
PREDICTION_JOB_RUNNERS: dict[str, threading.Thread] = {}
PREDICTION_JOB_RUNNERS_LOCK = threading.Lock()

CHURN_CONFIG: dict[str, Any] = {
    "churn_inactive_days": 14,
    "third_party_for_active": True,
    "export_webhook_url": None,
    "export_webhook_token": None,
}
EXTERNAL_CHURN_UPDATES: dict[str, Any] = {
    "by_user_id": {},
    "by_email": {},
    "updated_at": None,
}


class IngestionRequest(BaseModel):
    start_date: str
    end_date: str
    source: str
    continue_on_source_error: bool = True
    auto_mapping: bool = False


class ChurnPredictionRequest(BaseModel):
    job_name: str
    force_recalculate: bool = False
    prediction_mode: Optional[str] = "local"


class ChurnConfigRequest(BaseModel):
    churn_inactive_days: Optional[int] = None
    third_party_for_active: Optional[bool] = None
    export_webhook_url: Optional[str] = None
    export_webhook_token: Optional[str] = None


class ChurnExportThirdPartyRequest(BaseModel):
    job_name: str
    prediction_mode: Optional[str] = "local"
    include_churned: bool = True
    include_risks: Optional[list[str]] = None
    webhook_url: Optional[str] = None
    webhook_token: Optional[str] = None


class CampaignAudienceExportRequest(BaseModel):
    job_name: str
    prediction_mode: Optional[str] = "local"
    include_churned: bool = False
    include_risks: list[str] = Field(default_factory=lambda: ["high", "medium"])
    provider: str = "webhook"
    channel: str = "push_notification"
    audience_name: Optional[str] = None
    webhook_url: Optional[str] = None
    webhook_token: Optional[str] = None


class ExternalChurnItem(BaseModel):
    user_id: Optional[str] = None
    email: Optional[str] = None
    churn_risk: str
    reason: Optional[str] = None
    source: str = "external"


class ExternalChurnUpsertRequest(BaseModel):
    items: list[ExternalChurnItem]


class ExternalChurnValidateRequest(BaseModel):
    items: list[dict[str, Any]]


def save_import_jobs_to_cache() -> None:
    return


def save_prediction_jobs_to_cache() -> None:
    return


def append_audit_log(action: str, detail: dict[str, Any]) -> None:
    record = {
        "ts": datetime.utcnow().isoformat(),
        "action": action,
        "detail": detail,
    }
    retained_records = _read_retained_audit_records()
    retained_records.append(record)
    _write_audit_records(retained_records)


def _write_audit_records(records: list[dict[str, Any]]) -> None:
    with open(AUDIT_LOG_FILE, "w", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record) + "\n")


def _parse_history_timestamp(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    text = str(value).strip()
    if not text:
        return None
    for parser in (
        datetime.fromisoformat,
        lambda raw: datetime.strptime(raw, "%Y-%m-%d %H:%M:%S,%f"),
        lambda raw: datetime.strptime(raw, "%Y-%m-%d %H:%M:%S"),
    ):
        try:
            return parser(text)
        except Exception:
            continue
    return None


def _normalize_history_datetime(value: Optional[datetime]) -> Optional[datetime]:
    if value is None:
        return None
    if value.tzinfo is not None:
        return value.astimezone(timezone.utc).replace(tzinfo=None)
    return value


def _read_retained_audit_records(now: Optional[datetime] = None) -> list[dict[str, Any]]:
    if not os.path.exists(AUDIT_LOG_FILE):
        return []

    reference_time = _normalize_history_datetime(now or datetime.utcnow()) or datetime.utcnow()
    cutoff = reference_time - ACTION_HISTORY_RETENTION
    retained_records: list[dict[str, Any]] = []
    changed = False

    with open(AUDIT_LOG_FILE, "r", encoding="utf-8") as handle:
        for line in handle:
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                changed = True
                continue

            timestamp = _normalize_history_datetime(_parse_history_timestamp(record.get("ts")))
            if timestamp is None or timestamp < cutoff:
                changed = True
                continue

            retained_records.append(record)

    if changed:
        _write_audit_records(retained_records)

    return retained_records


def _history_sort_key(item: dict[str, Any]) -> datetime:
    return _parse_history_timestamp(item.get("timestamp")) or datetime.min


def _join_history_details(*parts: Optional[str]) -> str:
    return " | ".join(str(part) for part in parts if part not in (None, "", [], {}))


def _make_history_item(
    *,
    timestamp: Optional[str],
    category: str,
    summary: str,
    status: Optional[str],
    details: Optional[str] = None,
    kind: Optional[str] = None,
    metadata: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    parsed_ts = _parse_history_timestamp(timestamp)
    return {
        "timestamp": parsed_ts.isoformat() if parsed_ts else timestamp,
        "category": category,
        "summary": summary,
        "status": status,
        "details": details or "",
        "kind": kind or category,
        "metadata": metadata or {},
    }


def _current_import_job(job_name: Optional[str]) -> Optional[dict[str, Any]]:
    if not job_name:
        return None
    return next((job for job in IMPORT_JOBS if job.get("name") == job_name), None)


def _current_prediction_job(prediction_job_id: Optional[str]) -> Optional[dict[str, Any]]:
    if not prediction_job_id:
        return None
    return next((job for job in PREDICTION_JOBS if job.get("id") == prediction_job_id), None)


def _normalize_status_label(status: Optional[str]) -> Optional[str]:
    if not status:
        return None
    return str(status).strip().lower().replace(" ", "_")


def _audit_record_to_history_item(action: str, detail: dict[str, Any], ts: Optional[str]) -> Optional[dict[str, Any]]:
    if action == "import_job_started":
        current_job = _current_import_job(detail.get("job_name"))
        return _make_history_item(
            timestamp=ts,
            category="import",
            summary=f"Start Import from {detail.get('source') or detail.get('job_name')}",
            status=_normalize_status_label(current_job.get("status")) if current_job else "started",
            details=_join_history_details(
                f"range={detail.get('start_date')} to {detail.get('end_date')}"
                if detail.get("start_date") and detail.get("end_date")
                else None,
                "manual mapping enabled" if detail.get("auto_mapping") else None,
            ),
            kind=action,
            metadata=detail,
        )

    if action == "field_mapping_updated":
        return _make_history_item(
            timestamp=ts,
            category="mapping",
            summary=f"Update Field Mapping for {detail.get('connector')}",
            status="saved",
            details=_join_history_details(
                f"keys={len(detail.get('keys') or [])}" if detail.get("keys") is not None else None,
            ),
            kind=action,
            metadata=detail,
        )

    if action in {"campaign_audience_exported", "churn_export_third_party"}:
        provider = detail.get("provider") or "webhook"
        return _make_history_item(
            timestamp=ts,
            category="campaign",
            summary=f"Push Audience to {provider.title()}" if action == "campaign_audience_exported" else "Export Churn List to Webhook",
            status="completed" if detail.get("ok", True) else "failed",
            details=_join_history_details(
                f"job={detail.get('job_name')}" if detail.get("job_name") else None,
                f"channel={detail.get('channel')}" if detail.get("channel") else None,
                f"audience={detail.get('audience_name')}" if detail.get("audience_name") else None,
                f"count={detail.get('count')}" if detail.get("count") is not None else None,
            ),
            kind=action,
            metadata=detail,
        )

    if action in {"connector_configured", "connector_deleted", "experiment_config_updated", "churn_config_updated"}:
        summary_map = {
            "connector_configured": f"Configure Connector: {detail.get('name') or detail.get('type')}",
            "connector_deleted": f"Delete Connector: {detail.get('name')}",
            "experiment_config_updated": "Update Experiment Configuration",
            "churn_config_updated": "Update Churn Configuration",
        }
        return _make_history_item(
            timestamp=ts,
            category="settings",
            summary=summary_map[action],
            status="saved",
            details=_join_history_details(
                f"type={detail.get('type')}" if detail.get("type") else None,
                f"name={detail.get('name')}" if detail.get("name") else None,
            ),
            kind=action,
            metadata=detail,
        )

    return None


def _parse_audit_history(limit: int = 200) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for record in _read_retained_audit_records():
        item = _audit_record_to_history_item(record.get("action"), record.get("detail") or {}, record.get("ts"))
        if item:
            items.append(item)
    items.sort(key=_history_sort_key, reverse=True)
    return items[: max(1, int(limit))]


def _get_import_job_storage_identifier(job: Optional[dict[str, Any]]) -> Optional[str]:
    if not job:
        return None
    warehouse_job_id = job.get("warehouse_job_id")
    if warehouse_job_id:
        return str(warehouse_job_id)
    start_date = job.get("start_date")
    end_date = job.get("end_date")
    if start_date and end_date:
        return f"{start_date}_to_{end_date}"
    job_name = job.get("name")
    if job_name:
        return str(job_name)
    return None


def _find_external_churn_match(player_profile: dict[str, Any]) -> Optional[dict[str, Any]]:
    user_id = str(player_profile.get("player_id", "")).strip()
    email = str(player_profile.get("email", "")).strip().lower()
    if user_id and user_id in EXTERNAL_CHURN_UPDATES.get("by_user_id", {}):
        return EXTERNAL_CHURN_UPDATES["by_user_id"][user_id]
    if email and email in EXTERNAL_CHURN_UPDATES.get("by_email", {}):
        return EXTERNAL_CHURN_UPDATES["by_email"][email]
    return None


def _filter_export_rows(rows: list[dict[str, Any]], include_churned: bool, include_risks: Optional[list[str]] = None) -> list[dict[str, Any]]:
    risks = set(include_risks or ["high", "medium", "low"])
    output: list[dict[str, Any]] = []
    for row in rows:
        churn_state = str(row.get("churn_state", ""))
        churn_risk = str(row.get("predicted_churn_risk", "")).lower()
        if include_churned and churn_state == "churned":
            output.append(row)
            continue
        if churn_risk in risks:
            output.append(row)
    return output


def _clean_optional_string(value: Any, lowercase: bool = False) -> Optional[str]:
    if value is None:
        return None
    cleaned = str(value).strip()
    if not cleaned:
        return None
    return cleaned.lower() if lowercase else cleaned


def _clean_optional_float(value: Any) -> Optional[float]:
    if value in (None, "", "N/A"):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _clean_optional_int(value: Any) -> Optional[int]:
    if value in (None, "", "N/A"):
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _normalize_campaign_provider(provider: str) -> str:
    normalized = str(provider or "webhook").strip().lower().replace("-", "_")
    aliases = {
        "push": "braze",
        "push_notification": "braze",
        "email": "sendgrid",
        "custom_webhook": "webhook",
    }
    return aliases.get(normalized, normalized)


def _chunked(items: list[dict[str, Any]], size: int) -> list[list[dict[str, Any]]]:
    chunk_size = max(1, int(size))
    return [items[index:index + chunk_size] for index in range(0, len(items), chunk_size)]


def _build_campaign_audience_rows(
    rows: list[dict[str, Any]],
    *,
    job_name: str,
    channel: str,
    audience_name: Optional[str] = None,
) -> list[dict[str, Any]]:
    cleaned_rows: list[dict[str, Any]] = []
    exported_at = datetime.utcnow().isoformat()

    for row in rows:
        user_id = _clean_optional_string(row.get("user_id"))
        email = _clean_optional_string(row.get("email"), lowercase=True)
        if not user_id and not email:
            continue
        cleaned_rows.append(
            {
                "user_id": user_id,
                "email": email,
                "channel": _clean_optional_string(channel) or "push_notification",
                "job_name": _clean_optional_string(job_name),
                "audience_name": _clean_optional_string(audience_name),
                "churn_state": _clean_optional_string(row.get("churn_state")),
                "predicted_churn_risk": _clean_optional_string(row.get("predicted_churn_risk"), lowercase=True),
                "prediction_source": _clean_optional_string(row.get("prediction_source")),
                "suggested_action": _clean_optional_string(row.get("suggested_action")),
                "churn_reason": _clean_optional_string(row.get("churn_reason")),
                "ltv": _clean_optional_float(row.get("ltv")),
                "session_count": _clean_optional_int(row.get("session_count")),
                "event_count": _clean_optional_int(row.get("event_count")),
                "days_since_last_seen": _clean_optional_int(row.get("days_since_last_seen")),
                "exported_at": exported_at,
            }
        )
    return cleaned_rows


def _get_first_connector_config_by_type(connector_type: str) -> Optional[dict[str, Any]]:
    del connector_type
    return None


def _default_action_suggestion(churn_state: str, churn_risk: str) -> dict[str, Any]:
    risk = str(churn_risk or "").lower()
    if churn_state == "churned" or risk in {"already_churned", "low", "unknown", "n/a"}:
        return {"decision": "NO_ACTION", "content": "No action suggested."}
    if risk == "high":
        return {"decision": "ACT", "content": "We miss you! Come back today for a special reward."}
    if risk == "medium":
        return {"decision": "ACT", "content": "Your squad is waiting. Jump back in and keep your streak alive."}
    return {"decision": "NO_ACTION", "content": "No action suggested."}


async def _estimate_churn_with_mode(
    modeling_engine: PlayerModelingEngine,
    player_id: Any,
    profile: dict[str, Any],
    prediction_mode: str,
) -> tuple[Optional[dict[str, Any]], dict[str, Any]]:
    mode = (prediction_mode or "local").lower()
    if mode not in {"local", "parallel", "cloud"}:
        raise HTTPException(status_code=400, detail="prediction_mode must be one of: local, cloud, parallel.")
    estimate = await modeling_engine.estimate_churn_risk(player_id, profile)
    selected_source = "rule" if profile.get("churn_state") == "churned" else "local"
    return estimate, {
        "mode": mode,
        "selected_source": selected_source,
        "local_estimate": estimate,
        "cloud_estimate": None,
        "cloud_error": None,
    }


async def _compute_predictions_for_job(
    job_name: str,
    force_recalculate: bool,
    prediction_mode: str = "local",
    progress_callback=None,
    stop_requested=None,
) -> list[dict[str, Any]]:
    job = next((item for item in IMPORT_JOBS if item.get("name") == job_name), None)
    if not job or job.get("status") != "Ready to Use":
        raise HTTPException(status_code=404, detail=f"Job '{job_name}' not found or not ready.")

    warehouse_job_id = _get_import_job_storage_identifier(job)
    os.makedirs(PREDICTION_CACHE_DIR, exist_ok=True)
    effective_mode = (prediction_mode or "local").lower()
    cache_path = os.path.join(PREDICTION_CACHE_DIR, f"{job_name}_{effective_mode}.json")
    if not force_recalculate and os.path.exists(cache_path):
        with open(cache_path, "r", encoding="utf-8") as handle:
            return json.load(handle)

    gemini_client = None
    if os.getenv("GOOGLE_API_KEY"):
        try:
            gemini_client = GeminiClient()
        except Exception:
            gemini_client = None

    modeling_engine = PlayerModelingEngine(
        gemini_client=gemini_client,
        bigquery_service=BIGQUERY_SERVICE_INSTANCE,
        churn_inactive_days=int(CHURN_CONFIG.get("churn_inactive_days", 14)),
        job_id=warehouse_job_id,
    )

    player_ids = modeling_engine.get_all_player_ids()
    if not player_ids:
        return []

    predictions: list[dict[str, Any]] = []
    total_players = len(player_ids)
    if callable(progress_callback):
        progress_callback(predictions, 0, total_players)

    for index, player_id in enumerate(player_ids, start=1):
        await asyncio.sleep(0)
        if callable(stop_requested) and stop_requested():
            break

        profile = modeling_engine.build_player_profile(player_id)
        if not profile:
            if callable(progress_callback):
                progress_callback(predictions, index, total_players)
            continue

        churn_estimate, churn_details = await _estimate_churn_with_mode(
            modeling_engine=modeling_engine,
            player_id=player_id,
            profile=profile,
            prediction_mode=effective_mode,
        )
        churn_state = profile.get("churn_state", "active")
        external_match = _find_external_churn_match(profile) if churn_state == "active" else None
        if external_match:
            churn_estimate = {
                "player_id": player_id,
                "churn_state": "active",
                "churn_risk": external_match.get("churn_risk", "unknown"),
                "reason": external_match.get("reason", "Third-party churn update"),
                "top_signals": [{"signal": "external_update", "value": external_match.get("source", "external")}],
            }
            churn_details = {**churn_details, "selected_source": f"external:{external_match.get('source', 'external')}"}

        churn_risk = churn_estimate.get("churn_risk", "N/A") if churn_estimate else "N/A"
        prediction_source = "rule" if churn_state == "churned" else (churn_details or {}).get("selected_source") or "local"
        next_action = _default_action_suggestion(churn_state, churn_risk)

        predictions.append(
            {
                "user_id": player_id,
                "email": profile.get("email"),
                "ltv": profile.get("total_revenue", "N/A"),
                "session_count": profile.get("total_sessions", "N/A"),
                "event_count": profile.get("total_events", "N/A"),
                "days_since_last_seen": profile.get("days_since_last_seen", "N/A"),
                "churn_state": churn_state,
                "churn_inactive_days": int(CHURN_CONFIG.get("churn_inactive_days", 14)),
                "predicted_churn_risk": churn_risk,
                "churn_reason": churn_estimate.get("reason", "N/A") if churn_estimate else "N/A",
                "top_signals": churn_estimate.get("top_signals", []) if churn_estimate else [],
                "prediction_source": prediction_source,
                "suggested_action": next_action.get("content", "No action suggested."),
                "prediction_mode": effective_mode,
                "prediction_details": churn_details,
            }
        )
        if callable(progress_callback):
            progress_callback(predictions, index, total_players)

    if predictions:
        with open(cache_path, "w", encoding="utf-8") as handle:
            json.dump(predictions, handle, indent=2)
    return predictions


def _create_prediction_job(job_name: str, force_recalculate: bool) -> dict[str, Any]:
    prediction_job = {
        "id": str(uuid.uuid4()),
        "import_job_name": job_name,
        "status": "Processing",
        "force_recalculate": force_recalculate,
        "timestamp": datetime.utcnow().isoformat(),
        "result_count": 0,
        "processed_count": 0,
        "total_count": 0,
        "stop_requested": False,
        "predictions": [],
        "error": None,
    }
    PREDICTION_JOBS.append(prediction_job)
    save_prediction_jobs_to_cache()
    return prediction_job


def _get_prediction_job(prediction_job_id: str) -> Optional[dict[str, Any]]:
    return next((job for job in PREDICTION_JOBS if job.get("id") == prediction_job_id), None)


def _register_prediction_job_runner(prediction_job_id: str, runner: threading.Thread) -> None:
    with PREDICTION_JOB_RUNNERS_LOCK:
        PREDICTION_JOB_RUNNERS[prediction_job_id] = runner


def _get_prediction_job_runner(prediction_job_id: str) -> Optional[threading.Thread]:
    with PREDICTION_JOB_RUNNERS_LOCK:
        return PREDICTION_JOB_RUNNERS.get(prediction_job_id)


def _clear_prediction_job_runner(prediction_job_id: str) -> None:
    with PREDICTION_JOB_RUNNERS_LOCK:
        PREDICTION_JOB_RUNNERS.pop(prediction_job_id, None)


async def _run_prediction_job(prediction_job_id: str, job_name: str, force_recalculate: bool) -> None:
    prediction_job = _get_prediction_job(prediction_job_id)
    if not prediction_job:
        return

    def _progress_callback(predictions: list[dict[str, Any]], processed: int, total: int) -> None:
        prediction_job["predictions"] = predictions
        prediction_job["processed_count"] = processed
        prediction_job["total_count"] = total
        prediction_job["result_count"] = len(predictions)
        prediction_job["progress_pct"] = int((processed / total) * 100) if total else 0
        save_prediction_jobs_to_cache()

    def _stop_requested() -> bool:
        return bool(prediction_job.get("stop_requested"))

    try:
        predictions = await _compute_predictions_for_job(
            job_name,
            force_recalculate,
            prediction_job.get("prediction_mode", "local"),
            progress_callback=_progress_callback,
            stop_requested=_stop_requested,
        )
        if prediction_job.get("stop_requested"):
            prediction_job["status"] = "Stopped"
        else:
            prediction_job["status"] = "Ready"
        prediction_job["predictions"] = predictions
        prediction_job["result_count"] = len(predictions)
        prediction_job["error"] = None
    except Exception as exc:
        prediction_job["status"] = "Failed"
        prediction_job["error"] = str(exc)
    finally:
        _clear_prediction_job_runner(prediction_job_id)
        save_prediction_jobs_to_cache()


def _run_prediction_job_in_thread(prediction_job_id: str, job_name: str, force_recalculate: bool) -> None:
    asyncio.run(_run_prediction_job(prediction_job_id, job_name, force_recalculate))


def run_pipeline_background(
    start_date: str,
    end_date: str,
    job_name: str,
    source: str,
    continue_on_source_error: bool = True,
    auto_mapping: bool = False,
) -> None:
    del start_date, end_date, job_name, source, continue_on_source_error, auto_mapping
    return


legacy_router = APIRouter(tags=["legacy-main-service"])


@legacy_router.get("/churn/config")
async def get_churn_config() -> dict[str, Any]:
    return {"churn": CHURN_CONFIG}


@legacy_router.post("/churn/config")
async def update_churn_config(request: ChurnConfigRequest) -> dict[str, Any]:
    payload = request.model_dump(exclude_none=True)
    if "churn_inactive_days" in payload and int(payload["churn_inactive_days"]) < 1:
        raise HTTPException(status_code=400, detail="churn_inactive_days must be >= 1")
    CHURN_CONFIG.update(payload)
    append_audit_log(
        "churn_config_updated",
        {key: ("***" if "token" in key else value) for key, value in CHURN_CONFIG.items()},
    )
    return {"churn": {**CHURN_CONFIG, "export_webhook_token": "***" if CHURN_CONFIG.get("export_webhook_token") else None}}


@legacy_router.post("/churn/external-updates/validate")
async def validate_external_churn_updates(request: ExternalChurnValidateRequest) -> dict[str, Any]:
    valid = 0
    invalid = 0
    preview: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []

    for index, row in enumerate(request.items or []):
        user_id = str(row.get("user_id", "") or "").strip()
        email = str(row.get("email", "") or "").strip().lower()
        churn_risk = str(row.get("churn_risk", "") or "").strip().lower()
        row_errors: list[str] = []
        if not user_id and not email:
            row_errors.append("missing user_id/email")
        if churn_risk not in {"low", "medium", "high", "already_churned", "unknown"}:
            row_errors.append("invalid churn_risk")
        if row_errors:
            invalid += 1
            errors.append({"index": index, "errors": row_errors, "row": row})
            continue
        valid += 1
        if len(preview) < 10:
            preview.append(
                {
                    "user_id": user_id or None,
                    "email": email or None,
                    "churn_risk": churn_risk,
                    "reason": row.get("reason"),
                    "source": row.get("source", "external"),
                }
            )

    return {
        "total": len(request.items or []),
        "valid": valid,
        "invalid": invalid,
        "preview": preview,
        "errors": errors[:50],
    }


@legacy_router.post("/churn/external-updates")
async def upsert_external_churn_updates(request: ExternalChurnUpsertRequest) -> dict[str, Any]:
    items = request.items or []
    if not items:
        raise HTTPException(status_code=400, detail="items cannot be empty")

    matched_user_id = 0
    matched_email = 0
    skipped = 0

    for item in items:
        if not item.user_id and not item.email:
            skipped += 1
            continue
        record = {
            "user_id": item.user_id,
            "email": item.email,
            "churn_risk": item.churn_risk,
            "reason": item.reason or "Third-party churn update",
            "source": item.source,
            "updated_at": datetime.utcnow().isoformat(),
        }
        if item.user_id:
            EXTERNAL_CHURN_UPDATES.setdefault("by_user_id", {})[str(item.user_id)] = record
            matched_user_id += 1
        if item.email:
            EXTERNAL_CHURN_UPDATES.setdefault("by_email", {})[str(item.email).lower()] = record
            matched_email += 1

    EXTERNAL_CHURN_UPDATES["updated_at"] = datetime.utcnow().isoformat()
    append_audit_log(
        "external_churn_updates_upserted",
        {
            "count": len(items),
            "matched_user_id": matched_user_id,
            "matched_email": matched_email,
            "skipped": skipped,
        },
    )
    return {
        "message": "External churn updates ingested.",
        "count": len(items),
        "matched_user_id": matched_user_id,
        "matched_email": matched_email,
        "unmatched": max(0, len(items) - matched_user_id - matched_email + skipped),
        "skipped": skipped,
        "updated_at": EXTERNAL_CHURN_UPDATES.get("updated_at"),
    }


@legacy_router.post("/predict-churn-for-import")
async def predict_churn_for_import(request: ChurnPredictionRequest) -> dict[str, Any]:
    predictions = await _compute_predictions_for_job(
        request.job_name,
        request.force_recalculate,
        request.prediction_mode or "local",
    )
    return {"predictions": predictions}


@legacy_router.get("/churn/export/estimate")
async def estimate_churn_export(
    job_name: str,
    prediction_mode: str = "local",
    include_churned: bool = True,
    include_risks: Optional[str] = "high,medium,low",
) -> dict[str, Any]:
    rows = await _compute_predictions_for_job(job_name, force_recalculate=False, prediction_mode=prediction_mode)
    risk_list = [value.strip().lower() for value in (include_risks or "").split(",") if value.strip()]
    filtered = _filter_export_rows(rows, include_churned=include_churned, include_risks=risk_list)
    breakdown = {"churned": 0, "high": 0, "medium": 0, "low": 0, "other": 0}
    for row in filtered:
        if str(row.get("churn_state", "")) == "churned":
            breakdown["churned"] += 1
            continue
        risk = str(row.get("predicted_churn_risk", "")).lower()
        if risk in {"high", "medium", "low"}:
            breakdown[risk] += 1
        else:
            breakdown["other"] += 1
    return {
        "job_name": job_name,
        "prediction_mode": prediction_mode,
        "include_churned": include_churned,
        "include_risks": risk_list,
        "count": len(filtered),
        "breakdown": breakdown,
    }


@legacy_router.get("/churn/export/csv")
async def export_churn_csv(
    job_name: str,
    prediction_mode: str = "local",
    include_churned: bool = True,
    include_risks: Optional[str] = "high,medium,low",
) -> Response:
    rows = await _compute_predictions_for_job(job_name, force_recalculate=False, prediction_mode=prediction_mode)
    risk_list = [value.strip().lower() for value in (include_risks or "").split(",") if value.strip()]
    filtered = _filter_export_rows(rows, include_churned=include_churned, include_risks=risk_list)
    buffer = io.StringIO()
    writer = csv.writer(buffer)
    writer.writerow(
        [
            "user_id",
            "email",
            "churn_state",
            "predicted_churn_risk",
            "prediction_source",
            "days_since_last_seen",
            "ltv",
            "session_count",
            "event_count",
            "churn_reason",
        ]
    )
    for row in filtered:
        writer.writerow(
            [
                row.get("user_id"),
                row.get("email"),
                row.get("churn_state"),
                row.get("predicted_churn_risk"),
                row.get("prediction_source"),
                row.get("days_since_last_seen"),
                row.get("ltv"),
                row.get("session_count"),
                row.get("event_count"),
                row.get("churn_reason"),
            ]
        )
    filename = f"churn_export_{job_name}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.csv"
    return Response(
        content=buffer.getvalue(),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )


@legacy_router.post("/churn/export/third-party")
async def export_churn_to_third_party(request: ChurnExportThirdPartyRequest) -> dict[str, Any]:
    rows = await _compute_predictions_for_job(
        request.job_name,
        force_recalculate=False,
        prediction_mode=request.prediction_mode or "local",
    )
    filtered = _filter_export_rows(rows, include_churned=request.include_churned, include_risks=request.include_risks)
    webhook_url = request.webhook_url or CHURN_CONFIG.get("export_webhook_url")
    webhook_token = request.webhook_token or CHURN_CONFIG.get("export_webhook_token")
    if not webhook_url:
        raise HTTPException(status_code=400, detail="Missing webhook_url (request or churn config)")
    headers = {"Content-Type": "application/json"}
    if webhook_token:
        headers["Authorization"] = f"Bearer {webhook_token}"
    payload = {
        "job_name": request.job_name,
        "prediction_mode": request.prediction_mode or "local",
        "count": len(filtered),
        "rows": filtered,
        "sent_at": datetime.utcnow().isoformat(),
    }
    response = requests.post(webhook_url, json=payload, headers=headers, timeout=30)
    ok = 200 <= response.status_code < 300
    append_audit_log(
        "churn_export_third_party",
        {
            "job_name": request.job_name,
            "provider": "webhook",
            "count": len(filtered),
            "status_code": response.status_code,
            "ok": ok,
            "destination_host": urlparse(webhook_url).netloc or None,
        },
    )
    if not ok:
        raise HTTPException(status_code=502, detail=f"Third-party export failed: {response.status_code} {response.text[:300]}")
    return {"message": "Exported to third-party successfully.", "count": len(filtered), "status_code": response.status_code}


def _post_campaign_audience_webhook(request: CampaignAudienceExportRequest, audience_rows: list[dict[str, Any]]) -> dict[str, Any]:
    webhook_url = request.webhook_url or CHURN_CONFIG.get("export_webhook_url")
    webhook_token = request.webhook_token or CHURN_CONFIG.get("export_webhook_token")
    if not webhook_url:
        raise HTTPException(status_code=400, detail="Missing webhook_url (request or churn config)")
    headers = {"Content-Type": "application/json"}
    if webhook_token:
        headers["Authorization"] = f"Bearer {webhook_token}"
    payload = {
        "job_name": request.job_name,
        "prediction_mode": request.prediction_mode or "local",
        "provider": "webhook",
        "channel": request.channel,
        "audience_name": request.audience_name,
        "count": len(audience_rows),
        "fields": list(audience_rows[0].keys()) if audience_rows else [],
        "rows": audience_rows,
        "sent_at": datetime.utcnow().isoformat(),
    }
    response = requests.post(webhook_url, json=payload, headers=headers, timeout=30)
    if not 200 <= response.status_code < 300:
        raise HTTPException(status_code=502, detail=f"Webhook audience export failed: {response.status_code} {response.text[:300]}")
    return {
        "message": "Audience pushed to webhook successfully.",
        "provider": "webhook",
        "channel": request.channel,
        "count": len(audience_rows),
        "status_code": response.status_code,
        "fields": payload["fields"],
    }


def _put_campaign_audience_sendgrid(request: CampaignAudienceExportRequest, audience_rows: list[dict[str, Any]]) -> dict[str, Any]:
    connector = _get_first_connector_config_by_type("sendgrid") or {}
    api_key = connector.get("api_key") or os.getenv("SENDGRID_API_KEY")
    if not api_key:
        raise HTTPException(status_code=400, detail="SendGrid is not configured. Set a SendGrid API key first.")
    contacts: list[dict[str, Any]] = []
    skipped_missing_email = 0
    for row in audience_rows:
        email = row.get("email")
        if not email:
            skipped_missing_email += 1
            continue
        contact = {"email": email, "external_id": row.get("user_id")}
        contacts.append({key: value for key, value in contact.items() if value is not None})
    if not contacts:
        raise HTTPException(status_code=400, detail="SendGrid export requires at least one audience row with email.")
    response = requests.put(
        "https://api.sendgrid.com/v3/marketing/contacts",
        headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
        json={"contacts": contacts},
        timeout=30,
    )
    if not 200 <= response.status_code < 300:
        raise HTTPException(status_code=502, detail=f"SendGrid audience export failed: {response.status_code} {response.text[:300]}")
    return {
        "message": "Audience pushed to SendGrid successfully.",
        "provider": "sendgrid",
        "channel": request.channel,
        "count": len(contacts),
        "skipped_missing_email": skipped_missing_email,
        "status_code": response.status_code,
        "fields": list(audience_rows[0].keys()) if audience_rows else [],
    }


@legacy_router.post("/campaigns/export-audience")
async def export_campaign_audience(request: CampaignAudienceExportRequest) -> dict[str, Any]:
    rows = await _compute_predictions_for_job(
        request.job_name,
        force_recalculate=False,
        prediction_mode=request.prediction_mode or "local",
    )
    filtered = _filter_export_rows(rows, include_churned=request.include_churned, include_risks=request.include_risks)
    audience_rows = _build_campaign_audience_rows(
        filtered,
        job_name=request.job_name,
        channel=request.channel,
        audience_name=request.audience_name,
    )
    if not audience_rows:
        raise HTTPException(status_code=400, detail="No audience rows matched the selected export filters.")
    provider = _normalize_campaign_provider(request.provider)
    if provider == "webhook":
        result = _post_campaign_audience_webhook(request, audience_rows)
    elif provider == "sendgrid":
        result = _put_campaign_audience_sendgrid(request, audience_rows)
    else:
        raise HTTPException(status_code=400, detail="provider must be one of: webhook, braze, sendgrid")
    append_audit_log(
        "campaign_audience_exported",
        {
            "job_name": request.job_name,
            "provider": provider,
            "channel": request.channel,
            "audience_name": request.audience_name,
            "count": result.get("count", len(audience_rows)),
            "status_code": result.get("status_code"),
        },
    )
    return result


@legacy_router.get("/action-history")
async def get_action_history(limit: int = 200) -> dict[str, Any]:
    return {"action_history": _parse_audit_history(limit=limit)}


@legacy_router.post("/ingest-and-process-data")
async def ingest_and_process_data(request: IngestionRequest, background_tasks: BackgroundTasks) -> dict[str, Any]:
    start_date = str(request.start_date)
    end_date = str(request.end_date)
    job_timestamp = datetime.utcnow()
    expiration_timestamp = job_timestamp + timedelta(days=3)
    job_name = f"{job_timestamp.strftime('%Y%m%d-%H%M%S')}-{request.source.capitalize()}"
    IMPORT_JOBS.append(
        {
            "name": job_name,
            "status": "Processing",
            "current_step": "Queued",
            "progress_pct": 0,
            "timestamp": job_timestamp.isoformat(),
            "creation_timestamp": job_timestamp.isoformat(),
            "expiration_timestamp": expiration_timestamp.isoformat(),
            "start_date": start_date,
            "end_date": end_date,
            "warehouse_job_id": job_name,
        }
    )
    append_audit_log(
        "import_job_started",
        {
            "job_name": job_name,
            "source": request.source,
            "start_date": start_date,
            "end_date": end_date,
            "continue_on_source_error": request.continue_on_source_error,
            "auto_mapping": request.auto_mapping,
        },
    )
    background_tasks.add_task(
        run_pipeline_background,
        start_date,
        end_date,
        job_name,
        request.source,
        request.continue_on_source_error,
        request.auto_mapping,
    )
    save_import_jobs_to_cache()
    return {"message": f"Data import '{job_name}' started. It will be processed in the background."}


@legacy_router.post("/predict-churn-for-import-async")
async def predict_churn_for_import_async(request: ChurnPredictionRequest) -> dict[str, Any]:
    prediction_job = _create_prediction_job(request.job_name, request.force_recalculate)
    prediction_job["prediction_mode"] = request.prediction_mode or "local"
    save_prediction_jobs_to_cache()
    append_audit_log(
        "prediction_job_started",
        {
            "prediction_job_id": prediction_job["id"],
            "import_job_name": request.job_name,
            "force_recalculate": request.force_recalculate,
            "prediction_mode": prediction_job["prediction_mode"],
        },
    )
    runner = threading.Thread(
        target=_run_prediction_job_in_thread,
        args=(prediction_job["id"], request.job_name, request.force_recalculate),
        name=f"prediction-job-{prediction_job['id'][:8]}",
        daemon=True,
    )
    _register_prediction_job_runner(prediction_job["id"], runner)
    runner.start()
    return {
        "message": "Prediction job started.",
        "prediction_job_id": prediction_job["id"],
        "import_job_name": request.job_name,
    }


@legacy_router.get("/prediction-jobs")
async def list_prediction_jobs() -> dict[str, Any]:
    return {"prediction_jobs": sorted(PREDICTION_JOBS, key=lambda item: item.get("timestamp", ""), reverse=True)}


@legacy_router.get("/prediction-job/{prediction_job_id}")
async def get_prediction_job_status(prediction_job_id: str) -> dict[str, Any]:
    prediction_job = _get_prediction_job(prediction_job_id)
    if not prediction_job:
        raise HTTPException(status_code=404, detail=f"Prediction job '{prediction_job_id}' not found.")
    return {"prediction_job": prediction_job}


@legacy_router.post("/prediction-job/{prediction_job_id}/stop")
async def stop_prediction_job(prediction_job_id: str) -> dict[str, Any]:
    prediction_job = _get_prediction_job(prediction_job_id)
    if not prediction_job:
        raise HTTPException(status_code=404, detail=f"Prediction job '{prediction_job_id}' not found.")
    if prediction_job.get("status") != "Processing":
        return {"message": "Prediction job is not running.", "prediction_job": prediction_job}
    prediction_job["stop_requested"] = True
    append_audit_log(
        "prediction_job_stop_requested",
        {
            "prediction_job_id": prediction_job_id,
            "import_job_name": prediction_job.get("import_job_name"),
            "prediction_mode": prediction_job.get("prediction_mode", "local"),
        },
    )
    save_prediction_jobs_to_cache()
    runner = _get_prediction_job_runner(prediction_job_id)
    if (not runner or not runner.is_alive()) and prediction_job.get("status") == "Processing":
        prediction_job["status"] = "Stopped"
        save_prediction_jobs_to_cache()
    return {"message": "Stop requested.", "prediction_job": prediction_job}


@legacy_router.delete("/job/{job_name}")
async def delete_job_cache(job_name: str) -> dict[str, Any]:
    global IMPORT_JOBS, PREDICTION_JOBS
    job_to_delete = next((job for job in IMPORT_JOBS if job.get("name") == job_name), None)
    if not job_to_delete:
        raise HTTPException(status_code=404, detail=f"Job '{job_name}' not found.")
    start_date = job_to_delete.get("start_date")
    end_date = job_to_delete.get("end_date")
    raw_data_identifier = f"{str(start_date).replace('-', '')}_to_{str(end_date).replace('-', '')}"
    warehouse_job_identifier = _get_import_job_storage_identifier(job_to_delete) or raw_data_identifier
    if start_date and end_date:
        cache_filename = os.path.join(CACHE_DIR, f"{start_date}_{end_date}.json")
        if os.path.exists(cache_filename):
            os.remove(cache_filename)
    GCS_SERVICE_INSTANCE.delete_data_for_job(raw_data_identifier)
    BIGQUERY_SERVICE_INSTANCE.delete_data_for_job(warehouse_job_identifier)
    delete_ingestion_checkpoints(job_name)
    if os.path.isdir(PREDICTION_CACHE_DIR):
        prefix = f"{job_name}_"
        for filename in os.listdir(PREDICTION_CACHE_DIR):
            if filename == f"{job_name}.json" or filename.startswith(prefix):
                os.remove(os.path.join(PREDICTION_CACHE_DIR, filename))
    removed_prediction_jobs = [job for job in PREDICTION_JOBS if job.get("import_job_name") == job_name]
    if removed_prediction_jobs:
        PREDICTION_JOBS = [job for job in PREDICTION_JOBS if job.get("import_job_name") != job_name]
        with PREDICTION_JOB_RUNNERS_LOCK:
            for prediction_job in removed_prediction_jobs:
                PREDICTION_JOB_RUNNERS.pop(prediction_job.get("id"), None)
        save_prediction_jobs_to_cache()
    IMPORT_JOBS = [job for job in IMPORT_JOBS if job.get("name") != job_name]
    save_import_jobs_to_cache()
    return {"message": f"Job '{job_name}' and its cache have been deleted."}


if not getattr(app.state, "legacy_main_service_routes_registered", False):
    app.include_router(legacy_router)
    routes = list(app.router.routes)
    legacy_routes = [
        route
        for route in routes
        if getattr(getattr(route, "endpoint", None), "__module__", None) == __name__
    ]
    non_legacy_routes = [route for route in routes if route not in legacy_routes]
    organization_route_index = next(
        (
            index
            for index, route in enumerate(non_legacy_routes)
            if getattr(route, "path", None) == "/{organization_id}"
        ),
        len(non_legacy_routes),
    )
    app.router.routes = (
        non_legacy_routes[:organization_route_index]
        + legacy_routes
        + non_legacy_routes[organization_route_index:]
    )
    app.state.legacy_main_service_routes_registered = True


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
