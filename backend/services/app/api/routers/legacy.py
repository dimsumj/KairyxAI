from __future__ import annotations

import csv
import io
import json
import threading
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List

from fastapi import APIRouter, Depends, HTTPException, Query, Response
from pydantic import BaseModel
from sqlalchemy import delete
from sqlalchemy.orm import Session

from app.application.connectors import ConnectorService
from app.application.experiments import ExperimentConfigService
from app.application.exports import ExportService
from app.application.imports import ImportService
from app.application.mappings import MappingService
from app.application.predictions import PredictionService
from app.core.db import session_scope
from app.core.deps import (
    get_connector_service,
    get_experiment_service,
    get_export_service,
    get_import_service,
    get_mapping_service,
    get_prediction_service,
    get_repository,
    get_settings_dependency,
)
from app.core.settings import Settings
from app.infrastructure.db_models import ExportJobModel, ImportJobModel, IngestionCheckpointModel, PredictionJobModel
from app.infrastructure.repositories.sqlalchemy_control_plane import SqlAlchemyControlPlaneRepository
from bigquery_service import BigQueryService
from connectors.normalizer import canonical_attribution_event
from gcs_service import GcsService
from growth_decision_engine import GrowthDecisionEngine
from local_job_store import init_db as init_local_store, list_identity_links
from player_modeling_engine import PlayerModelingEngine
from gemini_client import GeminiClient


router = APIRouter(tags=["legacy"])

_INGESTION_CONNECTOR_TYPES = {"amplitude", "adjust", "appsflyer"}
_CONNECTOR_LABELS = {
    "amplitude": "Amplitude",
    "adjust": "Adjust",
    "appsflyer": "AppsFlyer",
    "google": "Google Gemini",
    "bigquery": "BigQuery",
    "sendgrid": "SendGrid",
    "braze": "Braze",
}
_CONNECTOR_REQUIRED_KEYS = {
    "amplitude": ("api_key", "secret_key"),
    "adjust": ("api_token",),
    "appsflyer": ("api_token", "app_id"),
    "google": ("api_key",),
    "bigquery": ("project_id",),
    "sendgrid": ("api_key",),
    "braze": ("api_key", "rest_endpoint"),
}
_ACTION_LABELS = {
    "connector_configured": ("connector", "saved"),
    "field_mapping_updated": ("mapping", "saved"),
    "import_job_created": ("import", "started"),
    "import_job_completed": ("import", "completed"),
    "import_job_stopped": ("import", "stopped"),
    "import_job_deleted": ("import", "completed"),
    "prediction_job_created": ("prediction", "started"),
    "prediction_job_completed": ("prediction", "completed"),
    "prediction_job_stopped": ("prediction", "stopped"),
    "export_job_completed": ("campaign", "completed"),
}

_PREDICTION_RUNNERS: dict[str, threading.Thread] = {}
_PREDICTION_STOP_EVENTS: dict[str, threading.Event] = {}
_PREDICTION_LOCK = threading.Lock()


class ConfigureAmplitudeRequest(BaseModel):
    api_key: str
    secret_key: str


class ConfigureAdjustRequest(BaseModel):
    api_token: str
    api_url: str | None = None


class ConfigureAppsFlyerRequest(BaseModel):
    api_token: str
    app_id: str
    pull_api_url: str | None = None


class ConfigureGoogleRequest(BaseModel):
    api_key: str
    model_name: str | None = None


class ConfigureBigQueryRequest(BaseModel):
    project_id: str


class ConfigureSendGridRequest(BaseModel):
    api_key: str


class ConfigureBrazeRequest(BaseModel):
    api_key: str
    rest_endpoint: str


class LegacyImportRequest(BaseModel):
    start_date: str
    end_date: str
    source: str
    continue_on_source_error: bool = True
    auto_mapping: bool = False


class LegacyPredictionRequest(BaseModel):
    job_name: str
    force_recalculate: bool = False
    prediction_mode: str = "local"


class LegacyExportRequest(BaseModel):
    job_name: str
    prediction_mode: str = "local"
    include_churned: bool = False
    include_risks: list[str] | None = None
    provider: str | None = None
    channel: str | None = None
    audience_name: str | None = None
    webhook_url: str | None = None
    webhook_token: str | None = None


class ChurnConfigRequest(BaseModel):
    churn_inactive_days: int = 14
    third_party_for_active: bool = True


class ExternalUpdatesRequest(BaseModel):
    items: list[dict[str, Any]]


class MappingPreviewRequest(BaseModel):
    connector_name: str
    sample_record: dict[str, Any]


class MappingCoverageRequest(BaseModel):
    connector_name: str
    sample_records: list[dict[str, Any]]


def _now_iso() -> str:
    return datetime.utcnow().isoformat()


def _prediction_status(status: str) -> str:
    normalized = str(status or "").lower()
    return {
        "queued": "Queued",
        "running": "Processing",
        "completed": "Ready",
        "failed": "Failed",
        "stopped": "Stopped",
    }.get(normalized, normalized.title() or "Queued")


def _import_status(status: str) -> str:
    normalized = str(status or "").lower()
    return {
        "queued": "Processing",
        "running": "Processing",
        "completed": "Ready to Use",
        "failed": "Failed",
        "stopped": "Interrupted",
        "interrupted": "Interrupted",
    }.get(normalized, normalized.title() or "Processing")


def _parse_iso(value: str | None) -> datetime:
    if not value:
        return datetime.utcnow()
    text = str(value).replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(text).replace(tzinfo=None)
    except Exception:
        return datetime.utcnow()


def _next_connector_name(repository: SqlAlchemyControlPlaneRepository, connector_type: str) -> str:
    base_name = _CONNECTOR_LABELS.get(connector_type, connector_type.title())
    numbers = []
    for connector in repository.list_connectors():
        name = str(connector.get("name", ""))
        if not name.startswith(base_name):
            continue
        suffix = name[len(base_name):].strip()
        if suffix.isdigit():
            numbers.append(int(suffix))
            continue
        if not suffix:
            numbers.append(0)
    next_index = (max(numbers) + 1) if numbers else 1
    return f"{base_name} {next_index}"


def _simple_connector_health(connector: Dict[str, Any]) -> Dict[str, Any]:
    connector_type = str(connector.get("type") or "")
    config = connector.get("config") or {}
    required = _CONNECTOR_REQUIRED_KEYS.get(connector_type, ())
    missing = [key for key in required if not config.get(key)]
    return {
        "ok": len(missing) == 0,
        "message": "configured" if not missing else f"missing {', '.join(missing)}",
    }


def _legacy_connector(connector: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "type": connector.get("type"),
        "name": connector.get("name"),
        "details": "Configured",
    }


def _legacy_source(connector: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "id": connector.get("name"),
        "name": connector.get("name"),
        "type": connector.get("type"),
    }


def _legacy_import_job(job: Dict[str, Any], repository: SqlAlchemyControlPlaneRepository) -> Dict[str, Any]:
    spec = job.get("spec") or {}
    progress = job.get("progress") or {}
    details = progress.get("details") or {}
    source_name = spec.get("source_name") or job.get("source_name")
    connector = repository.get_connector(str(source_name)) if source_name else None
    processing_stats = details.get("processing") or {}
    return {
        "name": job["id"],
        "status": _import_status(job.get("status", "")),
        "current_step": details.get("current_step") or _import_status(job.get("status", "")),
        "progress_pct": float(progress.get("pct", 0.0) or 0.0),
        "timestamp": job.get("created_at") or _now_iso(),
        "start_date": spec.get("start_date"),
        "end_date": spec.get("end_date"),
        "expiration_timestamp": (_parse_iso(job.get("updated_at")) + timedelta(days=7)).isoformat(),
        "warehouse_job_id": job["id"],
        "source_stats": [
            {
                "source": source_name,
                "type": spec.get("connector_type") or (connector or {}).get("type"),
                "ingested_events": int(details.get("events_staged") or progress.get("current") or 0),
                "status": _import_status(job.get("status", "")),
                "error": job.get("error"),
            }
        ] if source_name else [],
        "processing_stats": processing_stats,
    }


def _list_all_prediction_rows(job_id: str, page_size: int = 500) -> List[Dict[str, Any]]:
    service = BigQueryService()
    page = 1
    rows: List[Dict[str, Any]] = []
    while True:
        payload = service.list_prediction_results(job_id=job_id, page=page, page_size=page_size)
        items = payload.get("items") or []
        rows.extend(items)
        total = int(payload.get("total", 0) or 0)
        if len(rows) >= total or not items:
            return rows
        page += 1


def _legacy_prediction_job(job: Dict[str, Any], rows: List[Dict[str, Any]] | None = None) -> Dict[str, Any]:
    progress = job.get("progress") or {}
    details = progress.get("details") or {}
    return {
        "id": job["id"],
        "status": _prediction_status(job.get("status", "")),
        "stop_requested": bool(details.get("stop_requested", False) or _prediction_stop_requested(job["id"])),
        "processed_count": int(progress.get("current", 0) or 0),
        "total_count": int(progress.get("total", 0) or 0),
        "progress_pct": float(progress.get("pct", 0.0) or 0.0),
        "predictions": rows if rows is not None else [],
        "error": job.get("error"),
        "prediction_mode": (job.get("spec") or {}).get("prediction_mode", "local"),
    }


def _prediction_stop_requested(job_id: str) -> bool:
    with _PREDICTION_LOCK:
        event = _PREDICTION_STOP_EVENTS.get(job_id)
        return bool(event and event.is_set())


def _record_action(repository: SqlAlchemyControlPlaneRepository, action_type: str, resource_type: str, resource_id: str | None, payload: Dict[str, Any]) -> None:
    repository.record_action(action_type, resource_type, resource_id, payload)


def _action_history_items(repository: SqlAlchemyControlPlaneRepository, limit: int) -> List[Dict[str, Any]]:
    items = []
    for action in repository.list_actions(limit=limit):
        payload = action.get("payload") or {}
        action_type = str(action.get("action_type") or "")
        category, status = _ACTION_LABELS.get(action_type, ("system", "completed"))
        summary = action_type.replace("_", " ").title()
        details = ""

        if action_type == "connector_configured":
            summary = f"Configure Connector: {payload.get('name') or action.get('resource_id')}"
            details = f"type={payload.get('type', 'unknown')}"
        elif action_type == "field_mapping_updated":
            summary = f"Update Field Mapping for {payload.get('connector_name') or action.get('resource_id')}"
            keys = sorted((payload.get("mapping") or {}).keys())
            details = f"keys={', '.join(keys)}" if keys else "keys=none"
        elif action_type == "import_job_created":
            source_name = payload.get("source_name") or (payload.get("spec") or {}).get("source_name")
            spec = payload.get("spec") or {}
            summary = f"Start Import from {source_name}"
            details = f"range={spec.get('start_date')} to {spec.get('end_date')}"
        elif action_type == "import_job_completed":
            summary = f"Import Ready: {payload.get('id') or action.get('resource_id')}"
            progress = payload.get("progress") or {}
            rows = ((progress.get("details") or {}).get("events_staged")) or progress.get("current") or 0
            details = f"events={rows}"
        elif action_type == "prediction_job_created":
            spec = payload.get("spec") or {}
            summary = f"Run Prediction for {spec.get('import_job_id')}"
            details = f"mode={spec.get('prediction_mode', 'local')}"
        elif action_type == "prediction_job_completed":
            progress = payload.get("progress") or {}
            summary = f"Prediction Ready for {(payload.get('spec') or {}).get('import_job_id')}"
            details = f"rows={((progress.get('details') or {}).get('rows_written') or progress.get('current') or 0)}"
        elif action_type == "prediction_job_stopped":
            summary = f"Prediction Stopped for {(payload.get('spec') or {}).get('import_job_id')}"
            details = f"processed={((payload.get('progress') or {}).get('current') or 0)}"
        elif action_type == "export_job_completed":
            progress = payload.get("progress") or {}
            export_details = progress.get("details") or {}
            provider = str(export_details.get("provider") or (payload.get("spec") or {}).get("provider") or "provider").title()
            summary = f"Push Audience to {provider}"
            details = f"count={export_details.get('count', 0)}, channel={(payload.get('spec') or {}).get('channel', 'push_notification')}"

        items.append(
            {
                "timestamp": action.get("created_at"),
                "category": category,
                "summary": summary,
                "status": status,
                "details": details,
            }
        )
    return items


def _read_jsonl(path: Path, limit: int, *, job_identifier: str | None = None, source: str | None = None) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    rows: List[Dict[str, Any]] = []
    for line in reversed(path.read_text(encoding="utf-8").splitlines()):
        if not line.strip():
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        if job_identifier and str(payload.get("job_identifier")) != str(job_identifier):
            continue
        if source:
            source_value = (
                payload.get("source")
                or payload.get("source_a")
                or payload.get("event", {}).get("source")
            )
            if str(source_value) != str(source):
                continue
        rows.append(payload)
        if len(rows) >= max(1, int(limit)):
            break
    return rows


def _load_generic_config(repository: SqlAlchemyControlPlaneRepository, key: str, default: Dict[str, Any]) -> Dict[str, Any]:
    stored = repository.get_experiment_config(key=key)
    if not stored:
        return dict(default)
    return stored


def _save_generic_config(repository: SqlAlchemyControlPlaneRepository, key: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    return repository.save_experiment_config(payload, key=key)


def _filter_prediction_rows(rows: List[Dict[str, Any]], include_churned: bool, include_risks: List[str] | None) -> List[Dict[str, Any]]:
    risk_set = {str(item).strip().lower() for item in (include_risks or []) if str(item).strip()} or {"high", "medium"}
    filtered = []
    for row in rows:
        churn_state = str(row.get("churn_state", "")).lower()
        risk = str(row.get("predicted_churn_risk", "")).lower()
        if include_churned and churn_state == "churned":
            filtered.append(row)
            continue
        if risk in risk_set:
            filtered.append(row)
    return filtered


def _csv_for_rows(rows: List[Dict[str, Any]]) -> str:
    fields = [
        "user_id",
        "email",
        "churn_state",
        "predicted_churn_risk",
        "prediction_source",
        "churn_reason",
        "ltv",
        "session_count",
        "event_count",
        "days_since_last_seen",
        "suggested_action",
    ]
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=fields)
    writer.writeheader()
    for row in rows:
        writer.writerow({field: row.get(field) for field in fields})
    return buffer.getvalue()


def _normalize_external_update(item: Dict[str, Any]) -> Dict[str, Any]:
    normalized = dict(item)
    if normalized.get("email"):
        normalized["email"] = str(normalized["email"]).strip().lower()
    if normalized.get("user_id"):
        normalized["user_id"] = str(normalized["user_id"]).strip()
    normalized["churn_risk"] = str(normalized.get("churn_risk") or "").strip().lower()
    normalized["updated_at"] = _now_iso()
    return normalized


def _validate_external_items(items: List[Dict[str, Any]]) -> Dict[str, Any]:
    errors = []
    valid = 0
    for index, item in enumerate(items):
        normalized = _normalize_external_update(item)
        if not normalized.get("churn_risk"):
            errors.append({"index": index, "error": "churn_risk is required"})
            continue
        if not normalized.get("user_id") and not normalized.get("email"):
            errors.append({"index": index, "error": "user_id or email is required"})
            continue
        valid += 1
    return {"total": len(items), "valid": valid, "invalid": len(errors), "errors": errors}


def _load_external_updates(repository: SqlAlchemyControlPlaneRepository) -> Dict[str, Any]:
    stored = _load_generic_config(repository, "external_churn_updates", {"updated_at": None, "items": []})
    items = stored.get("items") or []
    by_user_id = [item for item in items if item.get("user_id")]
    by_email = [item for item in items if item.get("email") and not item.get("user_id")]
    return {
        "updated_at": stored.get("updated_at"),
        "items": items,
        "by_user_id": by_user_id,
        "by_email": by_email,
    }


def _save_external_updates(repository: SqlAlchemyControlPlaneRepository, items: List[Dict[str, Any]]) -> Dict[str, Any]:
    normalized_items = []
    merged: Dict[str, Dict[str, Any]] = {}
    matched_user_id = 0
    matched_email = 0
    skipped = 0

    for item in items:
        normalized = _normalize_external_update(item)
        if not normalized.get("churn_risk") or (not normalized.get("user_id") and not normalized.get("email")):
            skipped += 1
            continue
        key = normalized.get("user_id") or f"email:{normalized.get('email')}"
        if normalized.get("user_id"):
            matched_user_id += 1
        else:
            matched_email += 1
        merged[str(key)] = normalized

    normalized_items = list(merged.values())
    payload = {
        "updated_at": _now_iso(),
        "items": normalized_items,
    }
    saved = _save_generic_config(repository, "external_churn_updates", payload)
    current = _load_external_updates(repository)
    current["updated_at"] = saved.get("updated_at")
    current["count"] = len(normalized_items)
    current["matched_user_id"] = matched_user_id
    current["matched_email"] = matched_email
    current["skipped"] = skipped
    return current


def _lookup_import_job(service: ImportService, job_name: str) -> Dict[str, Any]:
    job = service.get_job(job_name)
    if job is None:
        raise HTTPException(status_code=404, detail=f"Import job '{job_name}' not found.")
    return job


def _latest_completed_prediction_job(repository: SqlAlchemyControlPlaneRepository, import_job_id: str, prediction_mode: str) -> Dict[str, Any] | None:
    for job in repository.list_prediction_jobs():
        spec = job.get("spec") or {}
        if spec.get("import_job_id") != import_job_id:
            continue
        if str(spec.get("prediction_mode", "local")).lower() != str(prediction_mode).lower():
            continue
        if str(job.get("status", "")).lower() == "completed":
            return job
    return None


def _run_prediction_job(repository: SqlAlchemyControlPlaneRepository, settings: Settings, job_id: str, stop_event: threading.Event | None = None) -> Dict[str, Any]:
    service = PredictionService(repository, settings)
    job = repository.get_prediction_job(job_id)
    if job is None:
        raise KeyError(job_id)
    import_job_id = (job.get("spec") or {}).get("import_job_id")
    import_job = repository.get_import_job(import_job_id)
    if import_job is None:
        raise KeyError(import_job_id)

    mode = str((job.get("spec") or {}).get("prediction_mode", "local")).lower()
    repository.update_prediction_job(job_id, {"status": "running", "error": None})

    churn_config = _load_generic_config(
        repository,
        "churn",
        {"churn_inactive_days": 14, "third_party_for_active": True},
    )
    gemini_client = None
    try:
        gemini_client = GeminiClient()
    except Exception:
        gemini_client = None

    bigquery_service = BigQueryService()
    modeling_engine = PlayerModelingEngine(
        gemini_client=gemini_client,
        bigquery_service=bigquery_service,
        churn_inactive_days=max(1, int(churn_config.get("churn_inactive_days", 14))),
        job_id=import_job_id,
    )
    decision_engine = GrowthDecisionEngine(gemini_client)
    player_ids = modeling_engine.get_all_player_ids()
    total = len(player_ids)
    rows: List[Dict[str, Any]] = []
    bigquery_service.replace_prediction_results(job_id=job_id, rows=[])

    try:
        for index, player_id in enumerate(player_ids, start=1):
            if stop_event is not None and stop_event.is_set():
                bigquery_service.replace_prediction_results(job_id=job_id, rows=rows)
                stopped = repository.update_prediction_job(
                    job_id,
                    {
                        "status": "stopped",
                        "progress": {
                            "current": len(rows),
                            "total": total,
                            "pct": (len(rows) / total * 100.0) if total else 100.0,
                            "details": {
                                "rows_written": len(rows),
                                "import_job_id": import_job_id,
                                "stop_requested": True,
                            },
                        },
                    },
                )
                _record_action(repository, "prediction_job_stopped", "prediction_job", job_id, stopped)
                return stopped

            profile = modeling_engine.build_player_profile(player_id)
            if profile:
                churn_estimate, prediction_source = service._estimate_prediction(mode, modeling_engine, player_id, profile)
                next_action = decision_engine.decide_next_action(profile, churn_estimate, "reduce_churn") or {
                    "content": "No action suggested.",
                }
                rows.append(
                    {
                        "prediction_job_id": job_id,
                        "import_job_id": import_job_id,
                        "user_id": str(player_id),
                        "email": profile.get("email"),
                        "ltv": profile.get("total_revenue", 0.0),
                        "session_count": profile.get("total_sessions", 0),
                        "event_count": profile.get("total_events", 0),
                        "days_since_last_seen": profile.get("days_since_last_seen", 0),
                        "churn_state": churn_estimate.get("churn_state", profile.get("churn_state", "active")),
                        "predicted_churn_risk": churn_estimate.get("churn_risk", "unknown"),
                        "churn_reason": churn_estimate.get("reason", "unknown"),
                        "top_signals": churn_estimate.get("top_signals", []),
                        "prediction_source": prediction_source,
                        "suggested_action": next_action.get("content", "No action suggested."),
                    }
                )

            repository.update_prediction_job(
                job_id,
                {
                    "progress": {
                        "current": index,
                        "total": total,
                        "pct": (index / total * 100.0) if total else 100.0,
                        "details": {
                            "rows_buffered": len(rows),
                            "import_job_id": import_job_id,
                            "stop_requested": bool(stop_event and stop_event.is_set()),
                        },
                    }
                },
            )
            if index == total or index % 25 == 0:
                bigquery_service.replace_prediction_results(job_id=job_id, rows=rows)

        bigquery_service.replace_prediction_results(job_id=job_id, rows=rows)
        completed = repository.update_prediction_job(
            job_id,
            {
                "status": "completed",
                "progress": {
                    "current": total,
                    "total": total,
                    "pct": 100.0,
                    "details": {
                        "rows_written": len(rows),
                        "import_job_id": import_job_id,
                        "stop_requested": bool(stop_event and stop_event.is_set()),
                    },
                },
            },
        )
        _record_action(repository, "prediction_job_completed", "prediction_job", job_id, completed)
        return completed
    except Exception as exc:
        failed = repository.update_prediction_job(
            job_id,
            {
                "status": "failed",
                "error": str(exc),
                "progress": {
                    "current": len(rows),
                    "total": total,
                    "pct": (len(rows) / total * 100.0) if total else 0.0,
                    "details": {"rows_written": len(rows), "import_job_id": import_job_id},
                },
            },
        )
        _record_action(repository, "prediction_job_failed", "prediction_job", job_id, failed)
        raise


def _prediction_runner_main(job_id: str) -> None:
    try:
        with session_scope() as session:
            repository = SqlAlchemyControlPlaneRepository(session)
            settings = get_settings_dependency()
            stop_event = _PREDICTION_STOP_EVENTS.get(job_id)
            _run_prediction_job(repository, settings, job_id, stop_event=stop_event)
    finally:
        with _PREDICTION_LOCK:
            _PREDICTION_RUNNERS.pop(job_id, None)


def _start_prediction_runner(job_id: str) -> None:
    with _PREDICTION_LOCK:
        runner = _PREDICTION_RUNNERS.get(job_id)
        if runner and runner.is_alive():
            return
        stop_event = _PREDICTION_STOP_EVENTS.setdefault(job_id, threading.Event())
        stop_event.clear()
        runner = threading.Thread(target=_prediction_runner_main, args=(job_id,), daemon=True, name=f"kairyx-pred-{job_id}")
        _PREDICTION_RUNNERS[job_id] = runner
        runner.start()


def _ensure_prediction_results(
    repository: SqlAlchemyControlPlaneRepository,
    settings: Settings,
    import_job_id: str,
    prediction_mode: str,
    force_recalculate: bool,
) -> tuple[Dict[str, Any], List[Dict[str, Any]]]:
    job = None if force_recalculate else _latest_completed_prediction_job(repository, import_job_id, prediction_mode)
    if job is None:
        job = PredictionService(repository, settings).create_job(import_job_id, prediction_mode)
        job = _run_prediction_job(repository, settings, job["id"])
    rows = _list_all_prediction_rows(job["id"])
    return job, rows


def _mapping_hits(sample_records: List[Dict[str, Any]], path: str | None) -> tuple[int, int]:
    if not path:
        return 0, len(sample_records)
    hits = 0
    for record in sample_records:
        current: Any = record
        for part in str(path).split("."):
            if not isinstance(current, dict):
                current = None
                break
            current = current.get(part)
        if current not in (None, ""):
            hits += 1
    return hits, len(sample_records)


def _build_mapping_coverage(mapping: Dict[str, Any], sample_records: List[Dict[str, Any]]) -> Dict[str, Any]:
    records = sample_records or []
    fields = ["canonical_user_id", "event_name", "event_time", "source_event_id", "campaign", "adset", "media_source"]
    coverage: Dict[str, Any] = {}
    required_scores = []
    for field in fields:
        hits, total = _mapping_hits(records, mapping.get(field))
        hit_rate = (hits / total) if total else 0.0
        coverage[field] = {
            "path": mapping.get(field),
            "hits": hits,
            "total": total,
            "hit_rate": hit_rate,
        }
        if field in {"canonical_user_id", "event_name", "event_time"}:
            required_scores.append(hit_rate)
    return {
        "coverage": coverage,
        "required_coverage_score": (sum(required_scores) / len(required_scores)) if required_scores else 0.0,
    }


def _job_sample_records(repository: SqlAlchemyControlPlaneRepository, job_id: str, limit: int = 25) -> List[Dict[str, Any]]:
    checkpoints = repository.list_checkpoints(job_id)
    if not checkpoints:
        return []
    gcs = GcsService()
    sample_records: List[Dict[str, Any]] = []
    prefix = f"gs://{gcs.bucket_name}/"
    for checkpoint in checkpoints:
        gcs_uri = checkpoint.get("gcs_uri")
        if not gcs_uri:
            continue
        blob_name = str(gcs_uri).replace(prefix, "")
        try:
            sample_records.extend(gcs.download_raw_events(blob_name))
        except FileNotFoundError:
            continue
        if len(sample_records) >= limit:
            break
    return sample_records[:limit]


@router.get("/health")
def legacy_health(settings: Settings = Depends(get_settings_dependency)):
    return {
        "status": "ok",
        "service": settings.app_name,
        "mode": settings.data_backend_mode,
    }


@router.get("/services-health")
def services_health(
    settings: Settings = Depends(get_settings_dependency),
    repository: SqlAlchemyControlPlaneRepository = Depends(get_repository),
):
    return {
        "operator_api": {"status": "ok", "details": settings.app_name},
        "control_plane": {"status": "ok", "details": settings.control_plane_database_url},
        "analytics_warehouse": {"status": "ok", "details": f"mode={settings.data_backend_mode}"},
        "connectors": {"status": "ok", "details": f"{len(repository.list_connectors())} configured"},
    }


@router.get("/connectors/list")
def legacy_list_connectors(service: ConnectorService = Depends(get_connector_service)):
    return {"connectors": [_legacy_connector(connector) for connector in service.list_connectors()]}


@router.get("/list-configured-sources")
def legacy_list_configured_sources(service: ConnectorService = Depends(get_connector_service)):
    connectors = [connector for connector in service.list_connectors() if connector.get("type") in _INGESTION_CONNECTOR_TYPES]
    return {"sources": [_legacy_source(connector) for connector in connectors]}


@router.get("/connector-health/{connector_name}")
def legacy_connector_health(
    connector_name: str,
    repository: SqlAlchemyControlPlaneRepository = Depends(get_repository),
):
    connector = repository.get_connector(connector_name)
    if connector is None:
        raise HTTPException(status_code=404, detail=f"Connector '{connector_name}' not found.")
    health = _simple_connector_health(connector)
    return {"connector": connector_name, "type": connector.get("type"), "health": health}


@router.get("/connector-freshness")
def connector_freshness(repository: SqlAlchemyControlPlaneRepository = Depends(get_repository)):
    imports = repository.list_import_jobs()
    freshness: Dict[str, Any] = {}
    for connector in repository.list_connectors():
        name = connector["name"]
        relevant = [job for job in imports if (job.get("spec") or {}).get("source_name") == name]
        latest_attempt = relevant[0] if relevant else None
        latest_success = next((job for job in relevant if str(job.get("status", "")).lower() == "completed"), None)
        latest_error = next((job for job in relevant if job.get("error")), None)
        latest_success_details = ((latest_success or {}).get("progress") or {}).get("details") or {}
        freshness[name] = {
            "connector_name": name,
            "connector_type": connector.get("type"),
            "last_attempt_at": (latest_attempt or {}).get("created_at"),
            "last_success_at": (latest_success or {}).get("updated_at"),
            "last_ingested_events": latest_success_details.get("events_staged"),
            "last_error": (latest_error or {}).get("error"),
        }
    return {"connectors": freshness}


def _configure_connector(
    connector_type: str,
    config: Dict[str, Any],
    success_message: str,
    service: ConnectorService,
    repository: SqlAlchemyControlPlaneRepository,
):
    name = _next_connector_name(repository, connector_type)
    service.create_connector(name, connector_type, config)
    _record_action(repository, "connector_configured", "connector", name, {"name": name, "type": connector_type, "config": config})
    return {"message": success_message, "connector": {"name": name, "type": connector_type}}


@router.post("/configure-amplitude-keys")
def configure_amplitude(
    request: ConfigureAmplitudeRequest,
    service: ConnectorService = Depends(get_connector_service),
    repository: SqlAlchemyControlPlaneRepository = Depends(get_repository),
):
    return _configure_connector(
        "amplitude",
        {"api_key": request.api_key, "secret_key": request.secret_key},
        "Amplitude credentials configured and cached.",
        service,
        repository,
    )


@router.post("/configure-adjust-credentials")
def configure_adjust(
    request: ConfigureAdjustRequest,
    service: ConnectorService = Depends(get_connector_service),
    repository: SqlAlchemyControlPlaneRepository = Depends(get_repository),
):
    return _configure_connector(
        "adjust",
        {"api_token": request.api_token, "api_url": request.api_url},
        "Adjust credentials configured and cached.",
        service,
        repository,
    )


@router.post("/configure-appsflyer")
def configure_appsflyer(
    request: ConfigureAppsFlyerRequest,
    service: ConnectorService = Depends(get_connector_service),
    repository: SqlAlchemyControlPlaneRepository = Depends(get_repository),
):
    return _configure_connector(
        "appsflyer",
        {"api_token": request.api_token, "app_id": request.app_id, "pull_api_url": request.pull_api_url},
        "AppsFlyer credentials configured and cached.",
        service,
        repository,
    )


@router.post("/configure-google-key")
def configure_google(
    request: ConfigureGoogleRequest,
    service: ConnectorService = Depends(get_connector_service),
    repository: SqlAlchemyControlPlaneRepository = Depends(get_repository),
):
    return _configure_connector(
        "google",
        {"api_key": request.api_key, "model_name": request.model_name},
        "Google credentials configured and cached.",
        service,
        repository,
    )


@router.post("/configure-bigquery")
def configure_bigquery(
    request: ConfigureBigQueryRequest,
    service: ConnectorService = Depends(get_connector_service),
    repository: SqlAlchemyControlPlaneRepository = Depends(get_repository),
):
    return _configure_connector(
        "bigquery",
        {"project_id": request.project_id},
        "BigQuery project configured and cached.",
        service,
        repository,
    )


@router.post("/configure-sendgrid-key")
def configure_sendgrid(
    request: ConfigureSendGridRequest,
    service: ConnectorService = Depends(get_connector_service),
    repository: SqlAlchemyControlPlaneRepository = Depends(get_repository),
):
    return _configure_connector(
        "sendgrid",
        {"api_key": request.api_key},
        "SendGrid credentials configured and cached.",
        service,
        repository,
    )


@router.post("/configure-braze")
def configure_braze(
    request: ConfigureBrazeRequest,
    service: ConnectorService = Depends(get_connector_service),
    repository: SqlAlchemyControlPlaneRepository = Depends(get_repository),
):
    return _configure_connector(
        "braze",
        {"api_key": request.api_key, "rest_endpoint": request.rest_endpoint},
        "Braze credentials configured and cached.",
        service,
        repository,
    )


@router.delete("/connector/{connector_name}")
def legacy_delete_connector(
    connector_name: str,
    service: ConnectorService = Depends(get_connector_service),
):
    deleted = service.delete_connector(connector_name)
    if not deleted:
        raise HTTPException(status_code=404, detail=f"Connector '{connector_name}' not found.")
    return {"message": f"Connector '{connector_name}' deleted."}


@router.post("/field-mapping/preview")
def field_mapping_preview(request: MappingPreviewRequest, service: MappingService = Depends(get_mapping_service)):
    mapping = service.get_mapping(request.connector_name)
    preview = canonical_attribution_event(request.connector_name, request.sample_record, mapping)
    return {"connector": request.connector_name, "mapping": mapping, "preview": preview}


@router.post("/field-mapping/coverage")
def field_mapping_coverage(request: MappingCoverageRequest, service: MappingService = Depends(get_mapping_service)):
    mapping = service.get_mapping(request.connector_name)
    payload = _build_mapping_coverage(mapping, request.sample_records)
    return {"connector": request.connector_name, "mapping": mapping, **payload}


@router.get("/field-mapping/{connector_name}")
def legacy_get_field_mapping(connector_name: str, service: MappingService = Depends(get_mapping_service)):
    return {"connector": connector_name, "mapping": service.get_mapping(connector_name)}


@router.post("/field-mapping/{connector_name}")
def legacy_save_field_mapping(
    connector_name: str,
    payload: Dict[str, Any],
    service: MappingService = Depends(get_mapping_service),
    repository: SqlAlchemyControlPlaneRepository = Depends(get_repository),
):
    mapping = payload.get("mapping") or {}
    saved = service.save_mapping(connector_name, mapping)
    _record_action(repository, "field_mapping_updated", "field_mapping", connector_name, saved)
    return {"message": f"Field mapping saved for {connector_name}.", "connector": connector_name, "mapping": mapping}


@router.get("/job/{job_name}/mapping-coverage")
def job_mapping_coverage(
    job_name: str,
    connector_name: str,
    service: MappingService = Depends(get_mapping_service),
    repository: SqlAlchemyControlPlaneRepository = Depends(get_repository),
):
    mapping = service.get_mapping(connector_name)
    sample_records = _job_sample_records(repository, job_name)
    payload = _build_mapping_coverage(mapping, sample_records)
    return {"connector": connector_name, "job_name": job_name, "mapping": mapping, **payload}


@router.get("/list-imports")
def legacy_list_imports(
    service: ImportService = Depends(get_import_service),
    repository: SqlAlchemyControlPlaneRepository = Depends(get_repository),
):
    return {"imports": [_legacy_import_job(job, repository) for job in service.list_jobs()]}


@router.post("/ingest-and-process-data")
def ingest_and_process_data(
    request: LegacyImportRequest,
    service: ImportService = Depends(get_import_service),
    repository: SqlAlchemyControlPlaneRepository = Depends(get_repository),
):
    try:
        job = service.create_job(request.source, request.start_date, request.end_date)
        job = service.run_job(job["id"])
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Connector '{request.source}' not found.")
    return {
        "message": f"Import job '{job['id']}' completed and is ready to use.",
        "job": _legacy_import_job(job, repository),
    }


@router.post("/job/{job_name}/process-after-mapping")
def process_after_mapping(
    job_name: str,
    service: ImportService = Depends(get_import_service),
    repository: SqlAlchemyControlPlaneRepository = Depends(get_repository),
):
    job = _lookup_import_job(service, job_name)
    legacy_job = _legacy_import_job(job, repository)
    return {
        "message": f"Import job '{job_name}' is ready for use.",
        "processing_stats": legacy_job.get("processing_stats") or {},
    }


@router.post("/job/{job_name}/stop")
def stop_import_job(
    job_name: str,
    service: ImportService = Depends(get_import_service),
    repository: SqlAlchemyControlPlaneRepository = Depends(get_repository),
):
    job = _lookup_import_job(service, job_name)
    progress = job.get("progress") or {}
    details = progress.get("details") or {}
    details["stop_requested"] = True
    stopped = repository.update_import_job(
        job_name,
        {
            "status": "stopped",
            "progress": {
                "current": int(progress.get("current", 0) or 0),
                "total": int(progress.get("total", 0) or 0),
                "pct": float(progress.get("pct", 0.0) or 0.0),
                "details": details,
            },
        },
    )
    _record_action(repository, "import_job_stopped", "import_job", job_name, stopped)
    return {"message": f"Import job '{job_name}' stopped.", "job": _legacy_import_job(stopped, repository)}


@router.delete("/job/{job_name}")
def delete_import_job(job_name: str, repository: SqlAlchemyControlPlaneRepository = Depends(get_repository)):
    job = repository.get_import_job(job_name)
    if job is None:
        raise HTTPException(status_code=404, detail=f"Import job '{job_name}' not found.")

    prediction_ids = [
        prediction_job["id"]
        for prediction_job in repository.list_prediction_jobs()
        if (prediction_job.get("spec") or {}).get("import_job_id") == job_name
    ]

    BigQueryService().delete_data_for_job(job_name)
    GcsService().delete_data_for_job(job_name)
    for prediction_id in prediction_ids:
        BigQueryService().replace_prediction_results(prediction_id, [])
        with _PREDICTION_LOCK:
            _PREDICTION_RUNNERS.pop(prediction_id, None)
            _PREDICTION_STOP_EVENTS.pop(prediction_id, None)

    if prediction_ids:
        repository.session.execute(delete(ExportJobModel).where(ExportJobModel.prediction_job_id.in_(prediction_ids)))
        repository.session.execute(delete(PredictionJobModel).where(PredictionJobModel.id.in_(prediction_ids)))
    repository.session.execute(delete(IngestionCheckpointModel).where(IngestionCheckpointModel.job_id == job_name))
    repository.session.execute(delete(ImportJobModel).where(ImportJobModel.id == job_name))
    _record_action(repository, "import_job_deleted", "import_job", job_name, {"name": job_name})
    return {"message": f"Import job '{job_name}' deleted."}


@router.post("/predict-churn-for-import")
def legacy_predict_for_import(
    request: LegacyPredictionRequest,
    service: ImportService = Depends(get_import_service),
    repository: SqlAlchemyControlPlaneRepository = Depends(get_repository),
    settings: Settings = Depends(get_settings_dependency),
):
    _lookup_import_job(service, request.job_name)
    job, rows = _ensure_prediction_results(repository, settings, request.job_name, request.prediction_mode, request.force_recalculate)
    return {"prediction_job_id": job["id"], "predictions": rows}


@router.post("/predict-churn-for-import-async")
def legacy_predict_for_import_async(
    request: LegacyPredictionRequest,
    service: ImportService = Depends(get_import_service),
    prediction_service: PredictionService = Depends(get_prediction_service),
):
    _lookup_import_job(service, request.job_name)
    job = prediction_service.create_job(request.job_name, request.prediction_mode)
    _start_prediction_runner(job["id"])
    return {"prediction_job_id": job["id"]}


@router.get("/prediction-job/{job_id}")
def legacy_get_prediction_job(job_id: str, service: PredictionService = Depends(get_prediction_service)):
    job = service.get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail=f"Prediction job '{job_id}' not found.")
    rows = _list_all_prediction_rows(job_id)
    return {"prediction_job": _legacy_prediction_job(job, rows=rows)}


@router.post("/prediction-job/{job_id}/stop")
def legacy_stop_prediction_job(
    job_id: str,
    repository: SqlAlchemyControlPlaneRepository = Depends(get_repository),
):
    job = repository.get_prediction_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail=f"Prediction job '{job_id}' not found.")
    with _PREDICTION_LOCK:
        stop_event = _PREDICTION_STOP_EVENTS.setdefault(job_id, threading.Event())
        stop_event.set()

    progress = job.get("progress") or {}
    details = progress.get("details") or {}
    details["stop_requested"] = True
    repository.update_prediction_job(
        job_id,
        {
            "progress": {
                "current": int(progress.get("current", 0) or 0),
                "total": int(progress.get("total", 0) or 0),
                "pct": float(progress.get("pct", 0.0) or 0.0),
                "details": details,
            }
        },
    )
    updated = repository.get_prediction_job(job_id) or job
    rows = _list_all_prediction_rows(job_id)
    return {"prediction_job": _legacy_prediction_job(updated, rows=rows)}


@router.get("/churn/export/estimate")
def churn_export_estimate(
    job_name: str,
    prediction_mode: str = "local",
    include_churned: bool = False,
    include_risks: str = "high,medium",
    service: ImportService = Depends(get_import_service),
    repository: SqlAlchemyControlPlaneRepository = Depends(get_repository),
    settings: Settings = Depends(get_settings_dependency),
):
    _lookup_import_job(service, job_name)
    _, rows = _ensure_prediction_results(repository, settings, job_name, prediction_mode, False)
    filtered = _filter_prediction_rows(rows, include_churned, include_risks.split(","))
    return {"job_name": job_name, "count": len(filtered)}


@router.get("/churn/export/csv")
def churn_export_csv(
    job_name: str,
    prediction_mode: str = "local",
    include_churned: bool = False,
    include_risks: str = "high,medium",
    service: ImportService = Depends(get_import_service),
    repository: SqlAlchemyControlPlaneRepository = Depends(get_repository),
    settings: Settings = Depends(get_settings_dependency),
):
    _lookup_import_job(service, job_name)
    _, rows = _ensure_prediction_results(repository, settings, job_name, prediction_mode, False)
    filtered = _filter_prediction_rows(rows, include_churned, include_risks.split(","))
    csv_text = _csv_for_rows(filtered)
    return Response(
        content=csv_text,
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{job_name}_churn_export.csv"'},
    )


def _run_export_job_for_import(
    repository: SqlAlchemyControlPlaneRepository,
    settings: Settings,
    export_service: ExportService,
    payload: LegacyExportRequest,
) -> Dict[str, Any]:
    prediction_job, _ = _ensure_prediction_results(repository, settings, payload.job_name, payload.prediction_mode, False)
    export_job = export_service.create_job(
        {
            "prediction_job_id": prediction_job["id"],
            "provider": payload.provider or "webhook",
            "channel": payload.channel or "push_notification",
            "audience_name": payload.audience_name,
            "include_churned": payload.include_churned,
            "include_risks": payload.include_risks or ["high", "medium"],
            "webhook_url": payload.webhook_url,
            "webhook_token": payload.webhook_token,
        }
    )
    export_job = export_service.run_job(export_job["id"])
    details = ((export_job.get("progress") or {}).get("details") or {})
    return {
        "message": f"Audience exported to {(payload.provider or 'webhook')}.",
        "provider": details.get("provider", payload.provider or "webhook"),
        "count": details.get("count", 0),
        "status_code": details.get("status_code"),
        "export_job_id": export_job["id"],
    }


@router.post("/churn/export/third-party")
def churn_export_third_party(
    request: LegacyExportRequest,
    service: ImportService = Depends(get_import_service),
    export_service: ExportService = Depends(get_export_service),
    repository: SqlAlchemyControlPlaneRepository = Depends(get_repository),
    settings: Settings = Depends(get_settings_dependency),
):
    _lookup_import_job(service, request.job_name)
    payload = request.model_copy(update={"provider": request.provider or "webhook"})
    return _run_export_job_for_import(repository, settings, export_service, payload)


@router.post("/campaigns/export-audience")
def campaign_export_audience(
    request: LegacyExportRequest,
    service: ImportService = Depends(get_import_service),
    export_service: ExportService = Depends(get_export_service),
    repository: SqlAlchemyControlPlaneRepository = Depends(get_repository),
    settings: Settings = Depends(get_settings_dependency),
):
    _lookup_import_job(service, request.job_name)
    return _run_export_job_for_import(repository, settings, export_service, request)


@router.get("/action-history")
def action_history(
    limit: int = Query(200, ge=1, le=2000),
    repository: SqlAlchemyControlPlaneRepository = Depends(get_repository),
):
    return {"action_history": _action_history_items(repository, limit)}


@router.get("/experiments/config")
def legacy_get_experiment_config(service: ExperimentConfigService = Depends(get_experiment_service)):
    return {"experiment": service.get_config()}


@router.post("/experiments/config")
def legacy_save_experiment_config(
    payload: Dict[str, Any],
    service: ExperimentConfigService = Depends(get_experiment_service),
):
    current = service.get_config()
    current.update(payload)
    return {"experiment": service.save_config(current)}


@router.get("/experiments/summary")
def legacy_get_experiment_summary(
    experiment_id: str = Query("churn_engagement_v1"),
    service: ExperimentConfigService = Depends(get_experiment_service),
):
    return service.get_summary(experiment_id)


@router.get("/churn/config")
def get_churn_config(repository: SqlAlchemyControlPlaneRepository = Depends(get_repository)):
    churn = _load_generic_config(repository, "churn", {"churn_inactive_days": 14, "third_party_for_active": True})
    return {"churn": churn}


@router.post("/churn/config")
def save_churn_config(request: ChurnConfigRequest, repository: SqlAlchemyControlPlaneRepository = Depends(get_repository)):
    payload = request.model_dump()
    churn = _save_generic_config(repository, "churn", payload)
    return {"churn": churn}


@router.get("/identity-links")
def get_identity_links(limit: int = Query(200, ge=1, le=2000)):
    init_local_store()
    return {"identity_links": list_identity_links(limit=limit)}


@router.get("/cleanup/rejected-events")
def cleanup_rejected_events(
    limit: int = Query(200, ge=1, le=2000),
    job_identifier: str | None = None,
    source: str | None = None,
):
    path = Path(".cache/rejected_events.jsonl")
    return {"rejected_events": _read_jsonl(path, limit, job_identifier=job_identifier, source=source)}


@router.get("/cleanup/conflicts")
def cleanup_conflicts(
    limit: int = Query(200, ge=1, le=2000),
    job_identifier: str | None = None,
    source: str | None = None,
):
    path = Path(".cache/conflict_log.jsonl")
    return {"conflicts": _read_jsonl(path, limit, job_identifier=job_identifier, source=source)}


@router.get("/churn/external-updates")
def get_external_updates(
    limit: int = Query(200, ge=1, le=2000),
    repository: SqlAlchemyControlPlaneRepository = Depends(get_repository),
):
    payload = _load_external_updates(repository)
    return {
        "updated_at": payload.get("updated_at"),
        "by_user_id": (payload.get("by_user_id") or [])[:limit],
        "by_email": (payload.get("by_email") or [])[:limit],
    }


@router.post("/churn/external-updates/validate")
def validate_external_updates(request: ExternalUpdatesRequest):
    return _validate_external_items(request.items)


@router.post("/churn/external-updates")
def upsert_external_updates(
    request: ExternalUpdatesRequest,
    repository: SqlAlchemyControlPlaneRepository = Depends(get_repository),
):
    return _save_external_updates(repository, request.items)


@router.get("/data-sandbox/glance")
def data_sandbox_glance(repository: SqlAlchemyControlPlaneRepository = Depends(get_repository)):
    imports = repository.list_import_jobs()
    if not imports:
        return {"filename": "No data imported yet", "sample": [], "event_counts": {}}

    latest_job = imports[0]
    sample = _job_sample_records(repository, latest_job["id"], limit=25)
    event_counts: Dict[str, int] = {}
    for record in sample:
        event_name = str(record.get("event_type") or record.get("event_name") or "unknown")
        event_counts[event_name] = event_counts.get(event_name, 0) + 1
    ordered_counts = dict(sorted(event_counts.items(), key=lambda item: item[1], reverse=True)[:10])
    return {
        "filename": latest_job["id"],
        "sample": sample[:5],
        "event_counts": ordered_counts,
    }
