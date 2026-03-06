# main_service.py

import asyncio
import os
import re
import csv
import uvicorn
import shutil
import uuid
import requests
import zipfile
import io
import gzip
import json
import threading
from typing import Any, Optional, Dict
from urllib.parse import urlparse
from datetime import (
    datetime,
    timedelta,
)
from fastapi import FastAPI, HTTPException, Request, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from starlette.responses import FileResponse, Response
from event_semantic_normalizer import EventSemanticNormalizer
from player_modeling_engine import PlayerModelingEngine
from growth_decision_engine import GrowthDecisionEngine
from player_cohort_service import PlayerCohortService
from gemini_client import GeminiClient
from engagement_executor import EngagementExecutor
from churn_reporter import ChurnReporter
from fastapi.staticfiles import StaticFiles
from ingestion_service import IngestionService
from data_processing_service import DataProcessingService
from connectors import create_connector
from connectors.normalizer import canonical_attribution_event
from bigquery_service import BigQueryService
from gcs_service import GcsService
from amplitude_service import AmplitudeService
from engagement_feedback import EngagementFeedback
from policy_engine import LlmPolicyEngine, DEFAULT_LLM_POLICY
from cloud_churn_service import CloudChurnService
from experiment_service import ExperimentService
from pydantic import BaseModel, Field
from local_job_store import (
    init_db as init_local_job_db,
    save_import_jobs as save_import_jobs_db,
    load_import_jobs as load_import_jobs_db,
    save_prediction_jobs as save_prediction_jobs_db,
    load_prediction_jobs as load_prediction_jobs_db,
    resolve_or_create_canonical_user_id,
    list_identity_links,
    save_field_mapping,
    get_field_mapping,
    list_field_mappings,
    list_ingestion_checkpoints,
    delete_ingestion_checkpoints,
)

KEYS_CACHE_FILE = ".api_keys_cache.json"

# In-memory cache for normalization maps. In a production scenario,
# this would be replaced by a persistent database (e.g., Redis, PostgreSQL).
NORMALIZATION_MAPS = {
    "event_name_map": {},
    "property_key_map": {}
}
NORMALIZATION_CACHE_FILE = ".normalization_maps.json"
IMPORT_JOBS_CACHE_FILE = ".import_jobs.json"
PREDICTION_JOBS_CACHE_FILE = ".prediction_jobs.json"
PREDICTION_CACHE_DIR = ".cache/predictions"
CACHE_DIR = ".cache"
LLM_POLICY_CACHE_FILE = ".llm_policy.json"
SAFETY_RAILS_CACHE_FILE = ".safety_rails.json"
AUDIT_LOG_FILE = ".audit.log.jsonl"
CONNECTOR_STATUS_CACHE_FILE = ".connector_status.json"
EXTERNAL_CHURN_CACHE_FILE = ".external_churn_updates.json"
DEFAULT_SAFETY_RAILS = {
    "AI_DAILY_TOKEN_LIMIT": 500000,
    "AI_MONTHLY_TOKEN_LIMIT": 10000000,
    "AI_DAILY_BUDGET_LIMIT_USD": 25.0,
    "AI_MONTHLY_BUDGET_LIMIT_USD": 500.0,
    "AI_RESPONSE_CACHE_TTL_SEC": 21600,
    "AI_COST_PER_1K_TOKENS_USD": 0.002,
    "AI_LLM_TIMEOUT_SEC": 20,
    "AI_LLM_MAX_RETRIES": 2,
    "AI_LLM_RETRY_BACKOFF_SEC": 1.0,
    "AI_LLM_CIRCUIT_FAILURE_THRESHOLD": 5,
    "AI_LLM_CIRCUIT_OPEN_SEC": 60,
}
DATA_BACKEND_MODE = os.getenv("DATA_BACKEND_MODE", "mock").strip().lower()

# Global instances of our new services. In a microservices architecture,
# these would be independent, deployed services. Here, we instantiate them
# globally to simulate their persistence.
BIGQUERY_SERVICE_INSTANCE = BigQueryService()
GCS_SERVICE_INSTANCE = GcsService()

# In-memory list to track import jobs. In production, this would be a database.
IMPORT_JOBS = []
PREDICTION_JOBS = []
PREDICTION_JOB_RUNNERS: Dict[str, threading.Thread] = {}
PREDICTION_JOB_RUNNERS_LOCK = threading.Lock()
LLM_POLICY_ENGINE = LlmPolicyEngine()
LLM_POLICY_CONFIG = DEFAULT_LLM_POLICY.copy()
CONNECTOR_STATUS: Dict[str, Any] = {}
EXPERIMENT_SERVICE = ExperimentService(base_dir=".")
EXPERIMENT_CONFIG = {
    "experiment_id": "churn_engagement_v1",
    "enabled": True,
    "holdout_pct": 0.10,
    "b_variant_pct": 0.50,
}
CHURN_CONFIG = {
    "churn_inactive_days": 14,
    "third_party_for_active": True,
    "export_webhook_url": None,
    "export_webhook_token": None,
}
EXTERNAL_CHURN_UPDATES = {
    "by_user_id": {},
    "by_email": {},
    "updated_at": None,
}

def _normalize_date_for_amplitude(date_str: str) -> str:
    """
    Normalizes incoming date strings to Amplitude's expected 'YYYYMMDD' format.
    Accepts either 'YYYY-MM-DD' or 'YYYYMMDD'.
    """
    for fmt in ("%Y-%m-%d", "%Y%m%d"):
        try:
            return datetime.strptime(date_str, fmt).strftime("%Y%m%d")
        except ValueError:
            continue
    raise HTTPException(
        status_code=400,
        detail=f"Invalid date '{date_str}'. Use 'YYYY-MM-DD' or 'YYYYMMDD'.",
    )

def clear_cache_on_startup():
    """Clears the data cache directory."""
    if os.path.exists(CACHE_DIR):
        print(f"Clearing cache directory: {CACHE_DIR}")
        shutil.rmtree(CACHE_DIR)
    print(f"Creating cache directory: {CACHE_DIR}")
    os.makedirs(PREDICTION_CACHE_DIR, exist_ok=True)
    os.makedirs(CACHE_DIR, exist_ok=True)
    
def clear_api_key_cache_on_startup():
    """Deletes the API key cache file if it exists."""
    if os.path.exists(KEYS_CACHE_FILE):
        print(f"Clearing API key cache file: {KEYS_CACHE_FILE}")
        os.remove(KEYS_CACHE_FILE)

def save_import_jobs_to_cache():
    """Saves the current IMPORT_JOBS list to local sqlite + legacy json cache."""
    save_import_jobs_db(IMPORT_JOBS)
    with open(IMPORT_JOBS_CACHE_FILE, 'w') as f:
        json.dump(IMPORT_JOBS, f, indent=2)


def load_import_jobs_from_cache():
    """Loads IMPORT_JOBS from local sqlite first, then falls back to legacy json cache."""
    IMPORT_JOBS.clear()
    try:
        jobs = load_import_jobs_db()
        if jobs:
            IMPORT_JOBS.extend(jobs)
            return
    except Exception as e:
        print(f"Warning: Could not load import jobs from sqlite: {e}")

    if os.path.exists(IMPORT_JOBS_CACHE_FILE):
        with open(IMPORT_JOBS_CACHE_FILE, 'r') as f:
            try:
                jobs = json.load(f)
                IMPORT_JOBS.extend(jobs)
            except json.JSONDecodeError:
                print(f"Warning: Could not decode {IMPORT_JOBS_CACHE_FILE}")


def save_prediction_jobs_to_cache():
    """Saves current PREDICTION_JOBS list to local sqlite + legacy json cache."""
    save_prediction_jobs_db(PREDICTION_JOBS)
    with open(PREDICTION_JOBS_CACHE_FILE, 'w') as f:
        json.dump(PREDICTION_JOBS, f, indent=2)


def load_prediction_jobs_from_cache():
    """Loads PREDICTION_JOBS from local sqlite first, then falls back to legacy json cache."""
    PREDICTION_JOBS.clear()
    try:
        jobs = load_prediction_jobs_db()
        if jobs:
            PREDICTION_JOBS.extend(jobs)
            return
    except Exception as e:
        print(f"Warning: Could not load prediction jobs from sqlite: {e}")

    if os.path.exists(PREDICTION_JOBS_CACHE_FILE):
        with open(PREDICTION_JOBS_CACHE_FILE, 'r') as f:
            try:
                jobs = json.load(f)
                PREDICTION_JOBS.extend(jobs)
            except json.JSONDecodeError:
                print(f"Warning: Could not decode {PREDICTION_JOBS_CACHE_FILE}")

def cleanup_expired_jobs():
    """
    Removes expired jobs and jobs stuck in 'Processing' state from the cache
    on application startup.
    """
    global IMPORT_JOBS
    
    # Filter out jobs stuck in 'Processing' from a previous run
    processing_cleared_jobs = [job for job in IMPORT_JOBS if job.get("status") != "Processing"]
    if len(processing_cleared_jobs) < len(IMPORT_JOBS):
        print("Clearing jobs stuck in 'Processing' state from previous session.")
    
    # From the remaining jobs, filter out the expired ones
    now = datetime.utcnow()
    final_jobs = []
    for job in processing_cleared_jobs:
        # Keep all 'Ready to Use' jobs, regardless of expiration, to ensure they persist across restarts.
        if job.get("status") == "Ready to Use":
            final_jobs.append(job)
        else:
            # For other statuses, check for expiration.
            expiration = datetime.fromisoformat(job.get("expiration_timestamp"))
            if now < expiration:
                final_jobs.append(job)
            else:
                print(f"Job '{job['name']}' has expired and is not 'Ready to Use'. Removing from list.")
    
    IMPORT_JOBS = final_jobs
    save_import_jobs_to_cache()


def load_keys_from_cache():
    """Load API keys from the cache file into environment variables if the file exists."""
    if os.path.exists(KEYS_CACHE_FILE):
        print(f"Loading connector configurations from cache file: {KEYS_CACHE_FILE}")
        with open(KEYS_CACHE_FILE, 'r') as f:
            try:
                cached_data = json.load(f)
                connectors = cached_data.get("connectors", {})
                # For services that depend on os.environ, load the *first* available key.
                # This maintains backward compatibility for core functions while allowing UI to list all.
                if "amplitude" in connectors and connectors["amplitude"]:
                    os.environ["AMPLITUDE_API_KEY"] = connectors["amplitude"][0].get("api_key")
                    os.environ["AMPLITUDE_SECRET_KEY"] = connectors["amplitude"][0].get("secret_key")
                if "google" in connectors and connectors["google"]:
                    os.environ["GOOGLE_API_KEY"] = connectors["google"][0].get("api_key")
                    os.environ["GOOGLE_GEMINI_MODEL"] = connectors["google"][0].get("model_name")
                if "adjust" in connectors and connectors["adjust"]:
                    os.environ["ADJUST_API_TOKEN"] = connectors["adjust"][0].get("api_token")
                    if connectors["adjust"][0].get("api_url"):
                        os.environ["ADJUST_API_URL"] = connectors["adjust"][0].get("api_url")
                if "appsflyer" in connectors and connectors["appsflyer"]:
                    os.environ["APPSFLYER_API_TOKEN"] = connectors["appsflyer"][0].get("api_token")
                    os.environ["APPSFLYER_APP_ID"] = connectors["appsflyer"][0].get("app_id")
                    if connectors["appsflyer"][0].get("pull_api_url"):
                        os.environ["APPSFLYER_PULL_API_URL"] = connectors["appsflyer"][0].get("pull_api_url")
                if "sendgrid" in connectors and connectors["sendgrid"]:
                    os.environ["SENDGRID_API_KEY"] = connectors["sendgrid"][0].get("api_key")
                if "bigquery" in connectors and connectors["bigquery"]:
                     os.environ["BIGQUERY_PROJECT_ID"] = connectors["bigquery"][0].get("project_id")
                if "cloud_churn" in connectors and connectors["cloud_churn"]:
                    os.environ["CHURN_API_URL"] = connectors["cloud_churn"][0].get("api_url")
                    if connectors["cloud_churn"][0].get("api_token"):
                        os.environ["CHURN_API_TOKEN"] = connectors["cloud_churn"][0].get("api_token")
                print("Primary API keys loaded into environment.")
            except json.JSONDecodeError:
                print(f"Warning: Could not decode JSON from {KEYS_CACHE_FILE}. File might be corrupt.")
    
    if os.path.exists(NORMALIZATION_CACHE_FILE):
        print(f"Loading normalization maps from cache file: {NORMALIZATION_CACHE_FILE}")
        with open(NORMALIZATION_CACHE_FILE, 'r') as f:
            try:
                maps = json.load(f)
                NORMALIZATION_MAPS["event_name_map"].update(maps.get("event_name_map", {}))
                NORMALIZATION_MAPS["property_key_map"].update(maps.get("property_key_map", {}))
            except json.JSONDecodeError:
                print(f"Warning: Could not decode JSON from {NORMALIZATION_CACHE_FILE}. File might be corrupt.")

def save_maps_to_cache():
    """Save normalization maps to the cache file."""
    with open(NORMALIZATION_CACHE_FILE, 'w') as f:
        json.dump(NORMALIZATION_MAPS, f, indent=2)


def load_llm_policy_from_cache():
    """Loads LLM routing policy from cache if it exists."""
    global LLM_POLICY_CONFIG
    if not os.path.exists(LLM_POLICY_CACHE_FILE):
        return
    with open(LLM_POLICY_CACHE_FILE, 'r') as f:
        try:
            cached_policy = json.load(f)
            LLM_POLICY_CONFIG = LLM_POLICY_ENGINE.normalize_policy(cached_policy)
            print(f"Loaded LLM policy from {LLM_POLICY_CACHE_FILE}.")
        except (json.JSONDecodeError, ValueError) as e:
            print(f"Warning: Could not load LLM policy cache. Using defaults. Reason: {e}")


def save_llm_policy_to_cache():
    """Saves current LLM routing policy to cache file."""
    with open(LLM_POLICY_CACHE_FILE, 'w') as f:
        json.dump(LLM_POLICY_CONFIG, f, indent=2)


def load_safety_rails_from_cache():
    """Loads AI safety-rail limits from cache into environment variables."""
    if not os.path.exists(SAFETY_RAILS_CACHE_FILE):
        return
    with open(SAFETY_RAILS_CACHE_FILE, 'r') as f:
        try:
            data = json.load(f)
            for key, value in data.items():
                if value is None:
                    continue
                os.environ[key] = str(value)
            print(f"Loaded safety rails from {SAFETY_RAILS_CACHE_FILE}.")
        except json.JSONDecodeError as e:
            print(f"Warning: Could not decode safety rails cache. Reason: {e}")


def save_safety_rails_to_cache(settings: Dict[str, Any]):
    """Persists AI safety-rail limits to local cache."""
    merged = {}
    if os.path.exists(SAFETY_RAILS_CACHE_FILE):
        with open(SAFETY_RAILS_CACHE_FILE, 'r') as f:
            try:
                merged = json.load(f)
            except json.JSONDecodeError:
                merged = {}
    merged.update(settings)
    with open(SAFETY_RAILS_CACHE_FILE, 'w') as f:
        json.dump(merged, f, indent=2)


def apply_default_safety_rails():
    """Applies default LLM safety settings when env vars are not already set."""
    for key, value in DEFAULT_SAFETY_RAILS.items():
        os.environ.setdefault(key, str(value))


def append_audit_log(action: str, detail: Dict[str, Any]):
    """Append lightweight local audit trail (local-demo friendly)."""
    record = {
        "ts": datetime.utcnow().isoformat(),
        "action": action,
        "detail": detail,
    }
    with open(AUDIT_LOG_FILE, "a") as f:
        f.write(json.dumps(record) + "\n")


def _parse_history_timestamp(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    text = str(value).strip()
    if not text:
        return None
    for parser in (
        datetime.fromisoformat,
        lambda v: datetime.strptime(v, "%Y-%m-%d %H:%M:%S,%f"),
        lambda v: datetime.strptime(v, "%Y-%m-%d %H:%M:%S"),
    ):
        try:
            return parser(text)
        except Exception:
            continue
    return None


def _history_sort_key(item: Dict[str, Any]) -> datetime:
    parsed = _parse_history_timestamp(item.get("timestamp"))
    return parsed or datetime.min


def _join_history_details(*parts: Optional[str]) -> str:
    return " | ".join([str(part) for part in parts if part not in (None, "", [], {})])


def _make_history_item(
    *,
    timestamp: Optional[str],
    category: str,
    summary: str,
    status: Optional[str],
    details: Optional[str] = None,
    kind: Optional[str] = None,
    metadata: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
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


def _current_import_job(job_name: Optional[str]) -> Optional[Dict[str, Any]]:
    if not job_name:
        return None
    return next((job for job in IMPORT_JOBS if job.get("name") == job_name), None)


def _current_prediction_job(prediction_job_id: Optional[str]) -> Optional[Dict[str, Any]]:
    if not prediction_job_id:
        return None
    return next((job for job in PREDICTION_JOBS if job.get("id") == prediction_job_id), None)


def _normalize_status_label(status: Optional[str]) -> Optional[str]:
    if not status:
        return None
    return str(status).strip().lower().replace(" ", "_")


def _parse_audit_history(limit: int = 200) -> list[Dict[str, Any]]:
    if not os.path.exists(AUDIT_LOG_FILE):
        return []

    items: list[Dict[str, Any]] = []
    with open(AUDIT_LOG_FILE, "r") as f:
        for line in f:
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                continue
            action = record.get("action")
            detail = record.get("detail") or {}
            ts = record.get("ts")
            item = _audit_record_to_history_item(action, detail, ts)
            if item:
                items.append(item)

    items.sort(key=_history_sort_key, reverse=True)
    return items[: max(1, int(limit))]


def _audit_record_to_history_item(action: str, detail: Dict[str, Any], ts: Optional[str]) -> Optional[Dict[str, Any]]:
    if action == "import_job_started":
        current_job = _current_import_job(detail.get("job_name"))
        source = detail.get("source")
        date_range = _join_history_details(
            f"range={detail.get('start_date')} to {detail.get('end_date')}" if detail.get("start_date") and detail.get("end_date") else None,
            "manual mapping enabled" if detail.get("auto_mapping") else None,
        )
        return _make_history_item(
            timestamp=ts,
            category="import",
            summary=f"Start Import from {source or detail.get('job_name')}",
            status=_normalize_status_label(current_job.get("status")) if current_job else "started",
            details=date_range,
            kind=action,
            metadata=detail,
        )

    if action == "process_after_mapping_requested":
        current_job = _current_import_job(detail.get("job_name"))
        return _make_history_item(
            timestamp=ts,
            category="mapping",
            summary=f"Resume Import After Mapping for {detail.get('job_name')}",
            status=_normalize_status_label(current_job.get("status")) if current_job else "processing",
            details=_join_history_details(
                f"source={detail.get('source')}" if detail.get("source") else None,
                f"range={detail.get('start_date')} to {detail.get('end_date')}" if detail.get("start_date") and detail.get("end_date") else None,
            ),
            kind=action,
            metadata=detail,
        )

    if action in {"import_job_stop_requested"}:
        current_job = _current_import_job(detail.get("job_name"))
        status_map = {
            "import_job_stop_requested": "stopped",
        }
        summary_map = {
            "import_job_stop_requested": "Stop Import",
        }
        details = _join_history_details(
            f"source={detail.get('source')}" if detail.get("source") else None,
            f"range={detail.get('start_date')} to {detail.get('end_date')}" if detail.get("start_date") and detail.get("end_date") else None,
        )
        return _make_history_item(
            timestamp=ts,
            category="import",
            summary=f"{summary_map[action]} for {detail.get('job_name')}",
            status=_normalize_status_label(current_job.get("status")) if current_job else status_map[action],
            details=details,
            kind=action,
            metadata=detail,
        )

    if action in {"prediction_job_started", "prediction_job_stop_requested"}:
        current_job = _current_prediction_job(detail.get("prediction_job_id"))
        status_map = {
            "prediction_job_started": "started",
            "prediction_job_stop_requested": "stopping",
        }
        summary_map = {
            "prediction_job_started": "Start Churn Prediction",
            "prediction_job_stop_requested": "Stop Churn Prediction",
        }
        details = _join_history_details(
            f"job={detail.get('import_job_name')}" if detail.get("import_job_name") else None,
            f"mode={detail.get('prediction_mode')}" if detail.get("prediction_mode") else None,
        )
        return _make_history_item(
            timestamp=ts,
            category="prediction",
            summary=f"{summary_map[action]} for {detail.get('import_job_name') or detail.get('prediction_job_id')}",
            status=_normalize_status_label(current_job.get("status")) if current_job else status_map[action],
            details=details,
            kind=action,
            metadata=detail,
        )

    if action in {"campaign_audience_exported", "churn_export_third_party"}:
        provider = detail.get("provider") or "webhook"
        details = _join_history_details(
            f"job={detail.get('job_name')}" if detail.get("job_name") else None,
            f"channel={detail.get('channel')}" if detail.get("channel") else None,
            f"audience={detail.get('audience_name')}" if detail.get("audience_name") else None,
            f"count={detail.get('count')}" if detail.get("count") is not None else None,
        )
        return _make_history_item(
            timestamp=ts,
            category="campaign",
            summary=f"Push Audience to {provider.title()}" if action == "campaign_audience_exported" else "Export Churn List to Webhook",
            status="completed" if detail.get("ok", True) else "failed",
            details=details,
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


def load_connector_status_from_cache():
    global CONNECTOR_STATUS
    if os.path.exists(CONNECTOR_STATUS_CACHE_FILE):
        try:
            with open(CONNECTOR_STATUS_CACHE_FILE, "r") as f:
                CONNECTOR_STATUS = json.load(f)
        except json.JSONDecodeError:
            CONNECTOR_STATUS = {}


def save_connector_status_to_cache():
    with open(CONNECTOR_STATUS_CACHE_FILE, "w") as f:
        json.dump(CONNECTOR_STATUS, f, indent=2)


def load_external_churn_from_cache():
    global EXTERNAL_CHURN_UPDATES
    if os.path.exists(EXTERNAL_CHURN_CACHE_FILE):
        try:
            with open(EXTERNAL_CHURN_CACHE_FILE, "r") as f:
                EXTERNAL_CHURN_UPDATES = json.load(f)
        except json.JSONDecodeError:
            EXTERNAL_CHURN_UPDATES = {"by_user_id": {}, "by_email": {}, "updated_at": None}


def save_external_churn_to_cache():
    with open(EXTERNAL_CHURN_CACHE_FILE, "w") as f:
        json.dump(EXTERNAL_CHURN_UPDATES, f, indent=2)


def update_connector_status(connector_name: str, connector_type: str, success: bool, ingested_events: int = 0, error: Optional[str] = None):
    now = datetime.utcnow().isoformat()
    existing = CONNECTOR_STATUS.get(connector_name, {})
    record = {
        **existing,
        "connector_name": connector_name,
        "connector_type": connector_type,
        "last_attempt_at": now,
        "last_ingested_events": ingested_events,
        "last_error": error,
    }
    if success:
        record["last_success_at"] = now
    CONNECTOR_STATUS[connector_name] = record
    save_connector_status_to_cache()


def _transition_job_status(job: Dict[str, Any], next_status: str, allowed: Dict[str, set], kind: str):
    current = job.get("status")
    if next_status == current:
        return
    allowed_next = allowed.get(current, set())
    if next_status not in allowed_next:
        raise ValueError(f"Invalid {kind} job status transition: {current} -> {next_status}")
    job["status"] = next_status
    append_audit_log(
        "job_status_transition",
        {"kind": kind, "id": job.get("id") or job.get("name"), "from": current, "to": next_status},
    )


IMPORT_JOB_ALLOWED_TRANSITIONS = {
    "Processing": {"Awaiting Mapping", "Ready to Use", "Failed", "Interrupted"},
    "Awaiting Mapping": {"Processing", "Ready to Use", "Failed", "Interrupted"},
    "Failed": set(),
    "Interrupted": set(),
    "Ready to Use": set(),
}

PREDICTION_JOB_ALLOWED_TRANSITIONS = {
    "Processing": {"Ready", "Failed", "Stopped"},
    "Stopped": set(),
    "Failed": set(),
    "Ready": set(),
}

# Load any cached API keys on application startup
# clear_cache_on_startup()
# clear_api_key_cache_on_startup()
init_local_job_db()
load_import_jobs_from_cache()
load_prediction_jobs_from_cache()
load_keys_from_cache()
load_connector_status_from_cache()
load_external_churn_from_cache()
load_llm_policy_from_cache()
load_safety_rails_from_cache()
apply_default_safety_rails()
cleanup_expired_jobs()
print(f"Data backend mode: {DATA_BACKEND_MODE}")

app = FastAPI()

# Add CORS middleware to allow the frontend to communicate with the backend.
# This is configured for local development and allows all origins.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allows all origins
    allow_credentials=True,
    allow_methods=["*"],  # Allows all methods
    allow_headers=["*"],  # Allows all headers
)

class AmplitudeApiKeys(BaseModel):
    """Request model for setting Amplitude API keys."""
    amplitude_api_key: str = Field(..., alias='api_key')
    amplitude_secret_key: str = Field(..., alias='secret_key')

class GoogleApiKey(BaseModel):
    """Request model for setting Google API key."""
    google_api_key: str = Field(..., alias='api_key')
    model_name: Optional[str] = None

class BigQueryCredentials(BaseModel):
    """Request model for setting BigQuery credentials."""
    project_id: str

class AdjustApiKey(BaseModel):
    """Request model for setting Adjust credentials."""
    adjust_api_token: str = Field(..., alias='api_token')
    api_url: Optional[str] = None


class AppsFlyerCredentials(BaseModel):
    """Request model for AppsFlyer pull API credentials."""
    api_token: str
    app_id: str
    pull_api_url: Optional[str] = None


class FieldMappingRequest(BaseModel):
    mapping: Dict[str, str]


class MappingPreviewRequest(BaseModel):
    connector_name: str
    sample_record: Dict[str, Any]


class MappingCoverageRequest(BaseModel):
    connector_name: str
    sample_records: list[Dict[str, Any]]

class SendGridApiKey(BaseModel):
    """Request model for setting the SendGrid API key."""
    sendgrid_api_key: str = Field(..., alias='api_key')

class BrazeCredentials(BaseModel):
    """Request model for setting Braze API key and endpoint."""
    api_key: str
    rest_endpoint: str

class ChurnReportRequest(BaseModel):
    """Request model for generating a churn report."""
    start_date: str
    end_date: str

class CohortCreationRequest(BaseModel):
    """Request model for creating player cohorts."""
    start_date: str
    end_date: str

class SafetyRailsRequest(BaseModel):
    """Request model for setting safety rails."""
    ai_token_limit: Optional[int] = None
    ai_budget_limit: Optional[float] = None
    ai_daily_token_limit: Optional[int] = None
    ai_monthly_token_limit: Optional[int] = None
    ai_daily_budget_limit_usd: Optional[float] = None
    ai_monthly_budget_limit_usd: Optional[float] = None
    ai_response_cache_ttl_sec: Optional[int] = None
    ai_cost_per_1k_tokens_usd: Optional[float] = None

class ChurnReportResponse(BaseModel):
    """Response model for the churn report generation."""
    message: str
    report_path: str

class PlayerAnalysisRequest(BaseModel):
    """Request model for analyzing a single player."""
    player_id: str
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    prediction_mode: Optional[str] = "local"

class DataSandboxRequest(BaseModel):
    """Request model for the data sandbox."""
    raw_name: str
    normalized_name: str

class IngestionRequest(BaseModel):
    """Request model for triggering data ingestion."""
    start_date: str
    end_date: str
    source: str
    continue_on_source_error: bool = True
    auto_mapping: bool = False
class ChurnPredictionRequest(BaseModel):
    """Request model for running churn prediction on an imported dataset."""
    job_name: str
    force_recalculate: bool = False
    prediction_mode: Optional[str] = "local"


class CloudChurnConfig(BaseModel):
    """Request model for configuring cloud churn prediction provider."""
    api_url: str
    api_token: Optional[str] = None


class LlmPolicyUpdateRequest(BaseModel):
    """Request model for updating the LLM routing policy."""
    policy: Dict[str, Any]


class ExperimentConfigRequest(BaseModel):
    experiment_id: Optional[str] = None
    enabled: Optional[bool] = None
    holdout_pct: Optional[float] = None
    b_variant_pct: Optional[float] = None


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
    items: list[Dict[str, Any]]
# Serve the frontend application
# This assumes the 'frontend' directory is two levels up from this script's location.
# Use an absolute path to make serving robust, regardless of the current working directory.
backend_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(backend_dir, '..', '..'))
frontend_dir = os.path.join(project_root, 'frontend')

app.mount("/static", StaticFiles(directory=frontend_dir), name="static")

@app.get("/")
async def serve_index():
    """Serves the main index.html file from the frontend directory."""
    response = FileResponse(os.path.join(frontend_dir, 'index.html'))
    # Add headers to prevent caching during development
    response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    return response


@app.get("/health")
async def health_check():
    """A simple health check endpoint that confirms the server is running."""
    return {"status": "ok"}
@app.post("/configure-amplitude-keys")
async def configure_amplitude_keys(keys: AmplitudeApiKeys):
    """
    An API endpoint to set the Amplitude API keys for the session.
    **Security Warning:** This is insecure for production. API keys should be
    set as environment variables on the server.
    """
    os.environ["AMPLITUDE_API_KEY"] = keys.amplitude_api_key
    os.environ["AMPLITUDE_SECRET_KEY"] = keys.amplitude_secret_key
    
    new_config = {
        "api_key": keys.amplitude_api_key,
        "secret_key": keys.amplitude_secret_key
    }
    _add_connector_config("amplitude", "Amplitude", new_config)
    append_audit_log("connector_configured", {"type": "amplitude", "name": "Amplitude"})

    return {"message": "Amplitude API keys have been configured and cached."}

@app.post("/configure-google-key")
async def configure_google_key(key: GoogleApiKey):
    """
    An API endpoint to set the Google API key for the session.
    **Security Warning:** This is insecure for production. API keys should be
    set as environment variables on the server.
    Note: This now directly configures the google.generativeai library upon being called.
    """
    os.environ["GOOGLE_API_KEY"] = key.google_api_key
    new_config = {"api_key": key.google_api_key}
    if key.model_name:
        os.environ["GOOGLE_GEMINI_MODEL"] = key.model_name
        new_config["model_name"] = key.model_name
    _add_connector_config("google", "Google Gemini", new_config)
    append_audit_log("connector_configured", {"type": "google", "name": "Google Gemini"})

    try:
        import google.generativeai as genai
        genai.configure(api_key=key.google_api_key)
        return {"message": "Google API settings have been configured and cached."}
    except ImportError:
        raise HTTPException(status_code=500, detail="The 'google-generativeai' library is not installed. Please check requirements.txt.")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to configure Google Gemini: {e}")

@app.post("/configure-bigquery")
async def configure_bigquery(creds: BigQueryCredentials):
    """
    An API endpoint to set the Google BigQuery Project ID.
    """
    os.environ["BIGQUERY_PROJECT_ID"] = creds.project_id
    new_config = {"project_id": creds.project_id}
    _add_connector_config("bigquery", "Google BigQuery", new_config)
    append_audit_log("connector_configured", {"type": "bigquery", "name": "Google BigQuery"})
    # In a real app, you might also initialize the BigQuery client here to verify credentials.
    return {"message": "BigQuery Project ID has been configured and cached."}

@app.post("/configure-sendgrid-key")
async def configure_sendgrid_key(key: SendGridApiKey):
    """
    An API endpoint to set the SendGrid API key for the session.
    """
    os.environ["SENDGRID_API_KEY"] = key.sendgrid_api_key
    new_config = {"api_key": key.sendgrid_api_key}
    _add_connector_config("sendgrid", "SendGrid", new_config)
    append_audit_log("connector_configured", {"type": "sendgrid", "name": "SendGrid"})
    return {"message": "SendGrid API key has been configured and cached."}


@app.post("/configure-braze")
async def configure_braze(creds: BrazeCredentials):
    """Configures Braze REST credentials."""
    os.environ["BRAZE_API_KEY"] = creds.api_key
    os.environ["BRAZE_REST_ENDPOINT"] = creds.rest_endpoint
    new_config = {"api_key": creds.api_key, "rest_endpoint": creds.rest_endpoint}
    _add_connector_config("braze", "Braze", new_config)
    append_audit_log("connector_configured", {"type": "braze", "name": "Braze"})
    return {"message": "Braze credentials have been configured and cached."}


@app.post("/configure-cloud-churn")
async def configure_cloud_churn(config: CloudChurnConfig):
    """
    Configures cloud churn prediction provider endpoint/token.
    """
    os.environ["CHURN_API_URL"] = config.api_url
    new_config = {"api_url": config.api_url}
    if config.api_token:
        os.environ["CHURN_API_TOKEN"] = config.api_token
        new_config["api_token"] = config.api_token
    _add_connector_config("cloud_churn", "Cloud Churn", new_config)
    append_audit_log("connector_configured", {"type": "cloud_churn", "name": "Cloud Churn"})
    return {"message": "Cloud churn provider configured and cached."}

@app.get("/connector-health/{connector_name}")
async def connector_health(connector_name: str):
    """Runs connector-level health check using saved connector config."""
    config, ctype = _get_connector_config(connector_name)
    if not config or not ctype:
        raise HTTPException(status_code=404, detail=f"Connector '{connector_name}' not found.")

    try:
        connector = create_connector(ctype, config)
        return {"connector": connector_name, "type": ctype, "health": connector.health_check()}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Connector health check failed: {e}")


@app.get("/connector-freshness")
async def connector_freshness():
    """Returns connector freshness metadata (last attempt/success/error)."""
    return {"connectors": CONNECTOR_STATUS}


@app.get("/identity-links")
async def get_identity_links(limit: int = 200):
    return {"identity_links": list_identity_links(limit=limit)}


@app.get("/field-mappings")
async def get_all_field_mappings():
    return {"mappings": list_field_mappings()}


@app.get("/field-mapping/{connector_name}")
async def get_connector_field_mapping(connector_name: str):
    return {"connector": connector_name, "mapping": get_field_mapping(connector_name)}


@app.post("/field-mapping/{connector_name}")
async def save_connector_field_mapping(connector_name: str, request: FieldMappingRequest):
    save_field_mapping(connector_name, request.mapping or {})
    append_audit_log("field_mapping_updated", {"connector": connector_name, "keys": list((request.mapping or {}).keys())})
    return {"message": "Field mapping saved.", "connector": connector_name, "mapping": request.mapping}


@app.post("/field-mapping/preview")
async def preview_field_mapping(request: MappingPreviewRequest):
    mapping = get_field_mapping(request.connector_name)
    # Infer type from connector name prefix; fallback to 'custom'
    conn_type = "custom"
    if request.connector_name.lower().startswith("appsflyer"):
        conn_type = "appsflyer"
    elif request.connector_name.lower().startswith("adjust"):
        conn_type = "adjust"
    elif request.connector_name.lower().startswith("amplitude"):
        conn_type = "amplitude"

    preview = canonical_attribution_event(conn_type, request.sample_record, mapping)
    return {"connector": request.connector_name, "mapping": mapping, "preview": preview}


@app.post("/field-mapping/coverage")
async def mapping_coverage(request: MappingCoverageRequest):
    mapping = get_field_mapping(request.connector_name)

    required_fields = ["canonical_user_id", "event_name", "event_time"]
    optional_fields = ["source_event_id", "campaign", "adset", "media_source"]

    def _get_path(raw: Dict[str, Any], path: str):
        cur: Any = raw
        for part in (path or "").split('.'):
            if not isinstance(cur, dict):
                return None
            cur = cur.get(part)
        return cur

    records = request.sample_records or []
    total = len(records)
    coverage = {}

    for field in required_fields + optional_fields:
        path = mapping.get(field)
        if not path:
            coverage[field] = {"mapped": False, "path": None, "hit_rate": 0.0, "hits": 0, "total": total}
            continue
        hits = 0
        for r in records:
            if _get_path(r, path) is not None:
                hits += 1
        coverage[field] = {
            "mapped": True,
            "path": path,
            "hits": hits,
            "total": total,
            "hit_rate": (hits / total) if total > 0 else 0.0,
        }

    required_avg = 0.0
    if required_fields:
        required_avg = sum(coverage[f]["hit_rate"] for f in required_fields) / len(required_fields)

    return {
        "connector": request.connector_name,
        "required_fields": required_fields,
        "optional_fields": optional_fields,
        "coverage": coverage,
        "required_coverage_score": required_avg,
    }


@app.get("/job/{job_name}/mapping-coverage")
async def job_mapping_coverage(job_name: str, connector_name: str):
    job = next((j for j in IMPORT_JOBS if j["name"] == job_name), None)
    if not job:
        raise HTTPException(status_code=404, detail=f"Job '{job_name}' not found.")

    pending_notifications = job.get("pending_notifications") or []
    if not pending_notifications:
        return {"connector": connector_name, "required_coverage_score": 0.0, "coverage": {}}

    mapping = get_field_mapping(connector_name)
    _, connector_type = _get_connector_config(connector_name)

    sample_records = []
    for msg in pending_notifications[:20]:
        try:
            n = json.loads(msg)
            gcs_path = n.get("gcs_path")
            source = n.get("source")
            if connector_type and source and source != connector_type:
                continue
            if not gcs_path:
                continue
            blob_name = gcs_path.replace(f"gs://{GCS_SERVICE_INSTANCE.bucket_name}/", "")
            raw_events = GCS_SERVICE_INSTANCE.download_raw_events(blob_name)
            sample_records.extend(raw_events[:20])
        except Exception:
            continue

    req = MappingCoverageRequest(connector_name=connector_name, sample_records=sample_records[:100])
    return await mapping_coverage(req)


@app.get("/list-configured-sources")
async def list_configured_sources():
    """
    Returns a list of data sources that have been correctly configured.
    """
    sources = []
    if os.path.exists(KEYS_CACHE_FILE):
        with open(KEYS_CACHE_FILE, 'r') as f:
            try:
                cached_data = json.load(f)
                connectors = cached_data.get("connectors", {})
                for ctype in ["amplitude", "adjust", "appsflyer"]:
                    if connectors.get(ctype):
                        for config in connectors[ctype]:
                            sources.append({"id": config["name"], "name": config["name"]})
            except json.JSONDecodeError:
                pass # No sources to return
    return {"sources": sources}

@app.get("/connectors/list")
async def list_connectors():
    """
    Returns a list of all configured connectors with some details.
    """
    configured_connectors = []
    if os.path.exists(KEYS_CACHE_FILE):
        with open(KEYS_CACHE_FILE, 'r') as f:
            try:
                cached_data = json.load(f)
                connectors_data = cached_data.get("connectors", {})
                for conn_type, configs in connectors_data.items():
                    for config in configs:
                        configured_connectors.append({
                            "type": conn_type,
                            "name": config.get("name"),
                            "details": "Configured"
                        })
            except json.JSONDecodeError:
                pass # Return empty list

    return {"connectors": configured_connectors}

@app.delete("/connector/{connector_name}")
async def delete_connector(connector_name: str):
    """
    Deletes a specific configured connector by its unique name.
    """
    if not os.path.exists(KEYS_CACHE_FILE):
        raise HTTPException(status_code=404, detail="No connectors configured.")

    with open(KEYS_CACHE_FILE, 'r+') as f:
        cached_data = json.load(f)
        connectors = cached_data.get("connectors", {})
        
        found_and_deleted = False
        for conn_type, configs in connectors.items():
            original_count = len(configs)
            connectors[conn_type] = [c for c in configs if c.get("name") != connector_name]
            if len(connectors[conn_type]) < original_count:
                found_and_deleted = True
                break
        
        if not found_and_deleted:
            raise HTTPException(status_code=404, detail=f"Connector '{connector_name}' not found.")

        cached_data["connectors"] = connectors
        f.seek(0)
        f.truncate()
        json.dump(cached_data, f, indent=2)

    # Note: This does not remove keys from os.environ. A restart or re-configuration
    # would be needed to update the environment-level primary keys.
    append_audit_log("connector_deleted", {"name": connector_name})
    return {"message": f"Connector '{connector_name}' has been deleted successfully."}

def _add_connector_config(conn_type: str, base_name: str, config: dict):
    """Helper function to add a new connector configuration and save to cache."""
    cached_data = {"connectors": {}}
    if os.path.exists(KEYS_CACHE_FILE):
        with open(KEYS_CACHE_FILE, 'r') as f:
            try:
                cached_data = json.load(f)
                if "connectors" not in cached_data:
                    cached_data["connectors"] = {}
            except json.JSONDecodeError:
                pass # Will create a new file

    if conn_type not in cached_data["connectors"]:
        cached_data["connectors"][conn_type] = []

    # Determine the next sequence number
    next_num = len(cached_data["connectors"][conn_type]) + 1
    config["name"] = f"{base_name} {next_num}"
    
    cached_data["connectors"][conn_type].append(config)

    with open(KEYS_CACHE_FILE, 'w') as f:
        json.dump(cached_data, f, indent=2)

def _get_connector_config(connector_name: str) -> tuple[Optional[Dict[str, Any]], Optional[str]]:
    """Retrieves a specific connector's configuration by its unique name."""
    if not os.path.exists(KEYS_CACHE_FILE):
        return None, None
    with open(KEYS_CACHE_FILE, 'r') as f:
        try:
            cached_data = json.load(f)
            connectors = cached_data.get("connectors", {})
            for conn_type, configs in connectors.items():
                for config in configs:
                    if config.get("name") == connector_name:
                        return config, conn_type
        except json.JSONDecodeError:
            pass
    return None, None


def _get_first_connector_config_by_type(conn_type: str) -> Optional[Dict[str, Any]]:
    """Returns the first configured connector entry for a connector type."""
    if not os.path.exists(KEYS_CACHE_FILE):
        return None
    with open(KEYS_CACHE_FILE, 'r') as f:
        try:
            cached_data = json.load(f)
            connectors = cached_data.get("connectors", {})
            configs = connectors.get(conn_type) or []
            return configs[0] if configs else None
        except json.JSONDecodeError:
            return None
@app.get("/list-imports")
async def list_imports():
    """
    Returns a list of all data import jobs and their statuses.
    """
    return {"imports": sorted(IMPORT_JOBS, key=lambda x: x['timestamp'], reverse=True)}

@app.get("/services-health")
async def get_services_health():
    """
    Checks the configuration status of all integrated services.
    """
    services_status = {
        "backend_api": {
            "status": "ok",
            "details": "The backend API is running."
        },
        "amplitude": {
            "status": "ok" if os.getenv("AMPLITUDE_API_KEY") and os.getenv("AMPLITUDE_SECRET_KEY") else "error",
            "details": "Configured" if os.getenv("AMPLITUDE_API_KEY") and os.getenv("AMPLITUDE_SECRET_KEY") else "Not Configured"
        },
        "adjust": {
            "status": "ok" if os.getenv("ADJUST_API_TOKEN") else "error",
            "details": "Configured" if os.getenv("ADJUST_API_TOKEN") else "Not Configured"
        },
        "appsflyer": {
            "status": "ok" if os.getenv("APPSFLYER_API_TOKEN") and os.getenv("APPSFLYER_APP_ID") else "warning",
            "details": "Configured" if os.getenv("APPSFLYER_API_TOKEN") and os.getenv("APPSFLYER_APP_ID") else "Not Configured"
        },
        "google_gemini": {
            "status": "ok" if os.getenv("GOOGLE_API_KEY") else "error",
            "details": "Configured" if os.getenv("GOOGLE_API_KEY") else "Not Configured"
        },
        "cloud_churn_prediction": {
            "status": "ok" if os.getenv("CHURN_API_URL") else "warning",
            "details": "Configured" if os.getenv("CHURN_API_URL") else "Not Configured (optional)"
        },
        "google_cloud_services": {
            "status": "ok" if os.getenv("BIGQUERY_PROJECT_ID") else "warning",
            "details": "Project ID Configured" if os.getenv("BIGQUERY_PROJECT_ID") else "Project ID Not Configured (required for GCS & BigQuery)"
        },
        "sendgrid": {
            "status": "ok" if os.getenv("SENDGRID_API_KEY") else "error",
            "details": "Configured" if os.getenv("SENDGRID_API_KEY") else "Not Configured"
        },
        "braze": {
            "status": "ok" if os.getenv("BRAZE_API_KEY") and os.getenv("BRAZE_REST_ENDPOINT") else "warning",
            "details": "Configured" if os.getenv("BRAZE_API_KEY") and os.getenv("BRAZE_REST_ENDPOINT") else "Not Configured (optional)"
        }
    }
    return services_status

@app.get("/observability/local-events")
async def get_local_observability_events(limit: int = 200):
    """Returns recent local audit and dead-letter events for debugging in demo mode."""
    events = []

    if os.path.exists(AUDIT_LOG_FILE):
        with open(AUDIT_LOG_FILE, "r") as f:
            for line in f.readlines()[-limit:]:
                try:
                    events.append({"stream": "audit", "record": json.loads(line)})
                except json.JSONDecodeError:
                    continue

    dlq_file = os.getenv("INGEST_DLQ_FILE", ".cache/ingest_dlq.jsonl")
    if os.path.exists(dlq_file):
        with open(dlq_file, "r") as f:
            for line in f.readlines()[-limit:]:
                try:
                    events.append({"stream": "ingest_dlq", "record": json.loads(line)})
                except json.JSONDecodeError:
                    continue

    events = sorted(events, key=lambda x: x["record"].get("ts") or x["record"].get("timestamp", ""), reverse=True)
    return {"events": events[:limit]}


@app.get("/cleanup/rejected-events")
async def get_rejected_events(limit: int = 200, job_identifier: Optional[str] = None, source: Optional[str] = None):
    path = ".cache/rejected_events.jsonl"
    out = []
    if os.path.exists(path):
        with open(path, "r") as f:
            for line in f.readlines()[-max(limit * 5, limit) :]:
                try:
                    rec = json.loads(line)
                    if job_identifier and rec.get("job_identifier") != job_identifier:
                        continue
                    rec_source = ((rec.get("event") or {}).get("source"))
                    if source and rec_source != source:
                        continue
                    out.append(rec)
                except json.JSONDecodeError:
                    continue
    return {"rejected_events": out[-limit:]}


@app.get("/cleanup/conflicts")
async def get_cleanup_conflicts(limit: int = 200, job_identifier: Optional[str] = None, source: Optional[str] = None):
    path = ".cache/conflict_log.jsonl"
    out = []
    if os.path.exists(path):
        with open(path, "r") as f:
            for line in f.readlines()[-max(limit * 5, limit) :]:
                try:
                    rec = json.loads(line)
                    if job_identifier and rec.get("job_identifier") != job_identifier:
                        continue
                    if source and rec.get("source_a") != source and rec.get("source_b") != source:
                        continue
                    out.append(rec)
                except json.JSONDecodeError:
                    continue
    return {"conflicts": out[-limit:]}


@app.post("/configure-adjust-credentials")
async def configure_adjust_credentials(key: AdjustApiKey):
    """
    An API endpoint to set Adjust connector credentials for the session.
    """
    os.environ["ADJUST_API_TOKEN"] = key.adjust_api_token
    new_config = {"api_token": key.adjust_api_token}
    if key.api_url:
        os.environ["ADJUST_API_URL"] = key.api_url
        new_config["api_url"] = key.api_url
    _add_connector_config("adjust", "Adjust", new_config)
    append_audit_log("connector_configured", {"type": "adjust", "name": "Adjust"})
    return {"message": "Adjust credentials have been configured and cached."}


@app.post("/configure-appsflyer")
async def configure_appsflyer(creds: AppsFlyerCredentials):
    """Configure AppsFlyer pull API credentials for connector ingestion."""
    os.environ["APPSFLYER_API_TOKEN"] = creds.api_token
    os.environ["APPSFLYER_APP_ID"] = creds.app_id
    new_config = {"api_token": creds.api_token, "app_id": creds.app_id}
    if creds.pull_api_url:
        os.environ["APPSFLYER_PULL_API_URL"] = creds.pull_api_url
        new_config["pull_api_url"] = creds.pull_api_url
    _add_connector_config("appsflyer", "AppsFlyer", new_config)
    append_audit_log("connector_configured", {"type": "appsflyer", "name": "AppsFlyer"})
    return {"message": "AppsFlyer credentials configured and cached."}

@app.post("/configure-safety-rails")
async def configure_safety_rails(request: SafetyRailsRequest):
    """
    Configures AI token/budget/cache limits used by GeminiClient.
    """
    payload = request.dict(exclude_none=True)
    env_updates: Dict[str, Any] = {}

    # Backward-compatible aliases (treated as daily limits).
    if "ai_token_limit" in payload:
        env_updates["AI_DAILY_TOKEN_LIMIT"] = int(payload["ai_token_limit"])
    if "ai_budget_limit" in payload:
        env_updates["AI_DAILY_BUDGET_LIMIT_USD"] = float(payload["ai_budget_limit"])

    if "ai_daily_token_limit" in payload:
        env_updates["AI_DAILY_TOKEN_LIMIT"] = int(payload["ai_daily_token_limit"])
    if "ai_monthly_token_limit" in payload:
        env_updates["AI_MONTHLY_TOKEN_LIMIT"] = int(payload["ai_monthly_token_limit"])
    if "ai_daily_budget_limit_usd" in payload:
        env_updates["AI_DAILY_BUDGET_LIMIT_USD"] = float(payload["ai_daily_budget_limit_usd"])
    if "ai_monthly_budget_limit_usd" in payload:
        env_updates["AI_MONTHLY_BUDGET_LIMIT_USD"] = float(payload["ai_monthly_budget_limit_usd"])
    if "ai_response_cache_ttl_sec" in payload:
        env_updates["AI_RESPONSE_CACHE_TTL_SEC"] = int(payload["ai_response_cache_ttl_sec"])
    if "ai_cost_per_1k_tokens_usd" in payload:
        env_updates["AI_COST_PER_1K_TOKENS_USD"] = float(payload["ai_cost_per_1k_tokens_usd"])

    for key, value in env_updates.items():
        os.environ[key] = str(value)

    if env_updates:
        save_safety_rails_to_cache(env_updates)

    return {"message": "Safety rails configured.", "settings": env_updates}


@app.get("/safety-rails")
async def get_safety_rails():
    """Returns active LLM safety settings (env-backed with defaults)."""
    settings = {}
    for key, default_value in DEFAULT_SAFETY_RAILS.items():
        raw_value = os.getenv(key, str(default_value))
        settings[key] = raw_value
    return {"settings": settings}


@app.get("/ai-usage")
async def get_ai_usage():
    """Returns current AI usage counters and active limits."""
    try:
        gemini_client = GeminiClient()
        return {"usage": gemini_client.get_usage_snapshot()}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/llm-policy")
async def get_llm_policy():
    """Returns current LLM routing policy."""
    return {"policy": LLM_POLICY_CONFIG}


@app.get("/experiments/config")
async def get_experiment_config():
    return {"experiment": EXPERIMENT_CONFIG}


@app.post("/experiments/config")
async def update_experiment_config(request: ExperimentConfigRequest):
    payload = request.dict(exclude_none=True)
    if "holdout_pct" in payload and not (0.0 <= payload["holdout_pct"] <= 0.9):
        raise HTTPException(status_code=400, detail="holdout_pct must be between 0.0 and 0.9")
    if "b_variant_pct" in payload and not (0.0 <= payload["b_variant_pct"] <= 1.0):
        raise HTTPException(status_code=400, detail="b_variant_pct must be between 0.0 and 1.0")
    EXPERIMENT_CONFIG.update(payload)
    append_audit_log("experiment_config_updated", EXPERIMENT_CONFIG)
    return {"experiment": EXPERIMENT_CONFIG}


@app.get("/experiments/summary")
async def get_experiment_summary(experiment_id: Optional[str] = None):
    exp_id = experiment_id or EXPERIMENT_CONFIG.get("experiment_id", "churn_engagement_v1")
    return EXPERIMENT_SERVICE.summary(exp_id)


@app.get("/churn/config")
async def get_churn_config():
    return {"churn": CHURN_CONFIG}


def _find_external_churn_match(player_profile: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    user_id = str(player_profile.get("player_id", "")).strip()
    email = str(player_profile.get("email", "")).strip().lower()
    if user_id and user_id in EXTERNAL_CHURN_UPDATES.get("by_user_id", {}):
        return EXTERNAL_CHURN_UPDATES["by_user_id"][user_id]
    if email and email in EXTERNAL_CHURN_UPDATES.get("by_email", {}):
        return EXTERNAL_CHURN_UPDATES["by_email"][email]
    return None


@app.post("/churn/config")
async def update_churn_config(request: ChurnConfigRequest):
    payload = request.dict(exclude_none=True)
    if "churn_inactive_days" in payload and payload["churn_inactive_days"] < 1:
        raise HTTPException(status_code=400, detail="churn_inactive_days must be >= 1")
    CHURN_CONFIG.update(payload)
    append_audit_log("churn_config_updated", {k: ("***" if "token" in k else v) for k, v in CHURN_CONFIG.items()})
    return {"churn": {**CHURN_CONFIG, "export_webhook_token": "***" if CHURN_CONFIG.get("export_webhook_token") else None}}


def _filter_export_rows(rows: list[dict], include_churned: bool, include_risks: Optional[list[str]] = None) -> list[dict]:
    risks = set((include_risks or ["high", "medium", "low"]))
    out = []
    for r in rows:
        churn_state = str(r.get("churn_state", ""))
        churn_risk = str(r.get("predicted_churn_risk", "")).lower()
        if include_churned and churn_state == "churned":
            out.append(r)
            continue
        if churn_risk in risks:
            out.append(r)
    return out


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


def _chunked(items: list[dict], size: int) -> list[list[dict]]:
    chunk_size = max(1, int(size))
    return [items[idx:idx + chunk_size] for idx in range(0, len(items), chunk_size)]


def _build_campaign_audience_rows(
    rows: list[dict],
    *,
    job_name: str,
    channel: str,
    audience_name: Optional[str] = None,
) -> list[dict]:
    cleaned_rows = []
    export_time = datetime.utcnow().isoformat()

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
                "exported_at": export_time,
            }
        )

    return cleaned_rows


def _post_campaign_audience_webhook(request: CampaignAudienceExportRequest, audience_rows: list[dict]) -> dict:
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

    resp = requests.post(webhook_url, json=payload, headers=headers, timeout=30)
    ok = 200 <= resp.status_code < 300
    if not ok:
        raise HTTPException(status_code=502, detail=f"Webhook audience export failed: {resp.status_code} {resp.text[:300]}")

    return {
        "message": "Audience pushed to webhook successfully.",
        "provider": "webhook",
        "channel": request.channel,
        "count": len(audience_rows),
        "status_code": resp.status_code,
        "fields": payload["fields"],
    }


def _post_campaign_audience_braze(request: CampaignAudienceExportRequest, audience_rows: list[dict]) -> dict:
    connector = _get_first_connector_config_by_type("braze") or {}
    api_key = connector.get("api_key") or os.getenv("BRAZE_API_KEY")
    rest_endpoint = (connector.get("rest_endpoint") or os.getenv("BRAZE_REST_ENDPOINT") or "").rstrip("/")
    if not api_key or not rest_endpoint:
        raise HTTPException(status_code=400, detail="Braze is not configured. Set Braze API key and REST endpoint first.")

    attributes = []
    skipped_missing_user_id = 0
    for row in audience_rows:
        external_id = row.get("user_id")
        if not external_id:
            skipped_missing_user_id += 1
            continue

        attr = {
            "external_id": external_id,
            "kairyx_job_name": row.get("job_name"),
            "kairyx_audience_name": row.get("audience_name"),
            "kairyx_channel": row.get("channel"),
            "kairyx_churn_state": row.get("churn_state"),
            "kairyx_predicted_churn_risk": row.get("predicted_churn_risk"),
            "kairyx_prediction_source": row.get("prediction_source"),
            "kairyx_suggested_action": row.get("suggested_action"),
            "kairyx_churn_reason": row.get("churn_reason"),
            "kairyx_ltv": row.get("ltv"),
            "kairyx_session_count": row.get("session_count"),
            "kairyx_event_count": row.get("event_count"),
            "kairyx_days_since_last_seen": row.get("days_since_last_seen"),
            "kairyx_exported_at": row.get("exported_at"),
        }
        if row.get("email"):
            attr["email"] = row["email"]
        attributes.append({k: v for k, v in attr.items() if v is not None})

    if not attributes:
        raise HTTPException(status_code=400, detail="Braze export requires at least one audience row with user_id.")

    status_codes = []
    url = f"{rest_endpoint}/users/track"
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    for chunk in _chunked(attributes, 75):
        resp = requests.post(url, headers=headers, json={"attributes": chunk}, timeout=30)
        ok = 200 <= resp.status_code < 300
        status_codes.append(resp.status_code)
        if not ok:
            raise HTTPException(status_code=502, detail=f"Braze audience export failed: {resp.status_code} {resp.text[:300]}")

    return {
        "message": "Audience pushed to Braze successfully.",
        "provider": "braze",
        "channel": request.channel,
        "count": len(attributes),
        "skipped_missing_user_id": skipped_missing_user_id,
        "request_count": len(status_codes),
        "status_code": status_codes[-1] if status_codes else 200,
        "fields": list(audience_rows[0].keys()) if audience_rows else [],
    }


def _put_campaign_audience_sendgrid(request: CampaignAudienceExportRequest, audience_rows: list[dict]) -> dict:
    connector = _get_first_connector_config_by_type("sendgrid") or {}
    api_key = connector.get("api_key") or os.getenv("SENDGRID_API_KEY")
    if not api_key:
        raise HTTPException(status_code=400, detail="SendGrid is not configured. Set a SendGrid API key first.")

    contacts = []
    skipped_missing_email = 0
    for row in audience_rows:
        email = row.get("email")
        if not email:
            skipped_missing_email += 1
            continue

        contact = {
            "email": email,
            "external_id": row.get("user_id"),
        }
        contacts.append({k: v for k, v in contact.items() if v is not None})

    if not contacts:
        raise HTTPException(status_code=400, detail="SendGrid export requires at least one audience row with email.")

    resp = requests.put(
        "https://api.sendgrid.com/v3/marketing/contacts",
        headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
        json={"contacts": contacts},
        timeout=30,
    )
    ok = 200 <= resp.status_code < 300
    if not ok:
        raise HTTPException(status_code=502, detail=f"SendGrid audience export failed: {resp.status_code} {resp.text[:300]}")

    return {
        "message": "Audience pushed to SendGrid successfully.",
        "provider": "sendgrid",
        "channel": request.channel,
        "count": len(contacts),
        "skipped_missing_email": skipped_missing_email,
        "status_code": resp.status_code,
        "fields": list(audience_rows[0].keys()) if audience_rows else [],
    }


def _default_action_suggestion(churn_state: str, churn_risk: str) -> Dict[str, Any]:
    risk = (churn_risk or "").lower()
    if churn_state == "churned" or risk in {"already_churned", "low", "unknown", "n/a"}:
        return {"decision": "NO_ACTION", "content": "No action suggested."}
    if risk == "high":
        return {"decision": "ACT", "content": "We miss you! Come back today for a special reward 🎁"}
    if risk == "medium":
        return {"decision": "ACT", "content": "Your squad is waiting—jump back in and keep your streak alive ⚡"}
    return {"decision": "NO_ACTION", "content": "No action suggested."}


@app.get("/churn/export/estimate")
async def estimate_churn_export(job_name: str, prediction_mode: str = "local", include_churned: bool = True, include_risks: Optional[str] = "high,medium,low"):
    rows = await _compute_predictions_for_job(job_name, force_recalculate=False, prediction_mode=prediction_mode)
    risk_list = [x.strip().lower() for x in (include_risks or "").split(",") if x.strip()]
    filtered = _filter_export_rows(rows, include_churned=include_churned, include_risks=risk_list)

    breakdown = {
        "churned": 0,
        "high": 0,
        "medium": 0,
        "low": 0,
        "other": 0,
    }
    for r in filtered:
        if str(r.get("churn_state", "")) == "churned":
            breakdown["churned"] += 1
        else:
            risk = str(r.get("predicted_churn_risk", "")).lower()
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


@app.get("/churn/export/csv")
async def export_churn_csv(job_name: str, prediction_mode: str = "local", include_churned: bool = True, include_risks: Optional[str] = "high,medium,low"):
    rows = await _compute_predictions_for_job(job_name, force_recalculate=False, prediction_mode=prediction_mode)
    risk_list = [x.strip().lower() for x in (include_risks or "").split(",") if x.strip()]
    filtered = _filter_export_rows(rows, include_churned=include_churned, include_risks=risk_list)

    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow([
        "user_id", "email", "churn_state", "predicted_churn_risk", "prediction_source",
        "days_since_last_seen", "ltv", "session_count", "event_count", "churn_reason"
    ])
    for r in filtered:
        writer.writerow([
            r.get("user_id"),
            r.get("email"),
            r.get("churn_state"),
            r.get("predicted_churn_risk"),
            r.get("prediction_source"),
            r.get("days_since_last_seen"),
            r.get("ltv"),
            r.get("session_count"),
            r.get("event_count"),
            r.get("churn_reason"),
        ])

    csv_text = buf.getvalue()
    filename = f"churn_export_{job_name}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.csv"
    return Response(content=csv_text, media_type="text/csv", headers={"Content-Disposition": f"attachment; filename={filename}"})


@app.post("/churn/export/third-party")
async def export_churn_to_third_party(request: ChurnExportThirdPartyRequest):
    rows = await _compute_predictions_for_job(request.job_name, force_recalculate=False, prediction_mode=request.prediction_mode or "local")
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

    resp = requests.post(webhook_url, json=payload, headers=headers, timeout=30)
    ok = 200 <= resp.status_code < 300
    append_audit_log(
        "churn_export_third_party",
        {
            "job_name": request.job_name,
            "provider": "webhook",
            "count": len(filtered),
            "status_code": resp.status_code,
            "ok": ok,
            "destination_host": urlparse(webhook_url).netloc or None,
        },
    )

    if not ok:
        raise HTTPException(status_code=502, detail=f"Third-party export failed: {resp.status_code} {resp.text[:300]}")

    return {"message": "Exported to third-party successfully.", "count": len(filtered), "status_code": resp.status_code}


@app.post("/campaigns/export-audience")
async def export_campaign_audience(request: CampaignAudienceExportRequest):
    rows = await _compute_predictions_for_job(
        request.job_name,
        force_recalculate=False,
        prediction_mode=request.prediction_mode or "local",
    )
    filtered = _filter_export_rows(
        rows,
        include_churned=request.include_churned,
        include_risks=request.include_risks,
    )
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
    elif provider == "braze":
        result = _post_campaign_audience_braze(request, audience_rows)
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


@app.get("/churn/external-updates")
async def get_external_churn_updates(limit: int = 200):
    by_user = list(EXTERNAL_CHURN_UPDATES.get("by_user_id", {}).values())
    by_email = list(EXTERNAL_CHURN_UPDATES.get("by_email", {}).values())
    return {
        "updated_at": EXTERNAL_CHURN_UPDATES.get("updated_at"),
        "by_user_id": by_user[:limit],
        "by_email": by_email[:limit],
    }


@app.post("/churn/external-updates/validate")
async def validate_external_churn_updates(request: ExternalChurnValidateRequest):
    items = request.items or []
    valid = 0
    invalid = 0
    errors = []
    preview = []

    for idx, row in enumerate(items):
        uid = str(row.get("user_id", "") or "").strip()
        email = str(row.get("email", "") or "").strip().lower()
        risk = str(row.get("churn_risk", "") or "").strip().lower()

        row_errors = []
        if not uid and not email:
            row_errors.append("missing user_id/email")
        if risk not in {"low", "medium", "high", "already_churned", "unknown"}:
            row_errors.append("invalid churn_risk")

        if row_errors:
            invalid += 1
            errors.append({"index": idx, "errors": row_errors, "row": row})
        else:
            valid += 1
            if len(preview) < 10:
                preview.append({
                    "user_id": uid or None,
                    "email": email or None,
                    "churn_risk": risk,
                    "reason": row.get("reason"),
                    "source": row.get("source", "external"),
                })

    return {
        "total": len(items),
        "valid": valid,
        "invalid": invalid,
        "preview": preview,
        "errors": errors[:50],
    }


@app.post("/churn/external-updates")
async def upsert_external_churn_updates(request: ExternalChurnUpsertRequest):
    items = request.items or []
    if not items:
        raise HTTPException(status_code=400, detail="items cannot be empty")

    matched_user_id = 0
    matched_email = 0
    skipped = 0

    for i in items:
        if not i.user_id and not i.email:
            skipped += 1
            continue

        rec = {
            "user_id": i.user_id,
            "email": i.email,
            "churn_risk": i.churn_risk,
            "reason": i.reason or "Third-party churn update",
            "source": i.source,
            "updated_at": datetime.utcnow().isoformat(),
        }
        if i.user_id:
            EXTERNAL_CHURN_UPDATES.setdefault("by_user_id", {})[str(i.user_id)] = rec
            matched_user_id += 1
        if i.email:
            EXTERNAL_CHURN_UPDATES.setdefault("by_email", {})[str(i.email).lower()] = rec
            matched_email += 1

    EXTERNAL_CHURN_UPDATES["updated_at"] = datetime.utcnow().isoformat()
    save_external_churn_to_cache()
    append_audit_log("external_churn_updates_upserted", {
        "count": len(items),
        "matched_user_id": matched_user_id,
        "matched_email": matched_email,
        "skipped": skipped,
    })

    return {
        "message": "External churn updates ingested.",
        "count": len(items),
        "matched_user_id": matched_user_id,
        "matched_email": matched_email,
        "unmatched": max(0, len(items) - matched_user_id - matched_email + skipped),
        "skipped": skipped,
        "updated_at": EXTERNAL_CHURN_UPDATES.get("updated_at"),
    }


@app.post("/configure-llm-policy")
async def configure_llm_policy(request: LlmPolicyUpdateRequest):
    """Merges and validates incoming LLM policy patch, then persists it."""
    global LLM_POLICY_CONFIG
    try:
        updated_policy = LLM_POLICY_ENGINE.normalize_policy(request.policy, LLM_POLICY_CONFIG)
        LLM_POLICY_CONFIG = updated_policy
        save_llm_policy_to_cache()
        return {"message": "LLM policy updated.", "policy": LLM_POLICY_CONFIG}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/generate-churn-report", response_model=ChurnReportResponse)
async def generate_churn_report(request: ChurnReportRequest):
    """
    An API endpoint to generate a churn prediction report.
    """
    try:
        start_date = _normalize_date_for_amplitude(request.start_date)
        end_date = _normalize_date_for_amplitude(request.end_date)

        amplitude_client = AmplitudeService()

        # Fetch the events
        events_data = amplitude_client.export_events(start_date, end_date)

        if not events_data:
            raise HTTPException(status_code=404, detail="No events found for the given date range.")

        # 1. Define normalization rules
        event_map = {
            "start_session": "session_started",
            "purchase": "item_purchased"
        }
        prop_map = {
            "item_ID": "item_id",
            "value": "revenue_usd"
        }

        # 2. Initialize and run the normalizer
        normalizer = EventSemanticNormalizer(event_name_map=event_map, property_key_map=prop_map)
        normalized_data = normalizer.normalize_events(events_data)

        # 3. Initialize the Gemini client (requires GOOGLE_API_KEY env var)
        gemini_client = GeminiClient()

        # 4. Persist normalized events into the simulated warehouse for profiling.
        job_identifier = f"{start_date}_to_{end_date}_report"
        BIGQUERY_SERVICE_INSTANCE.write_processed_events(normalized_data, job_identifier)

        # 5. Initialize the modeling engine with the warehouse client and the AI client
        modeling_engine = PlayerModelingEngine(
            gemini_client=gemini_client,
            bigquery_service=BIGQUERY_SERVICE_INSTANCE,
            churn_inactive_days=int(CHURN_CONFIG.get("churn_inactive_days", 14)),
        )

        # 6. Use only players present in this report payload.
        player_ids = sorted({event.get("player_id") for event in normalized_data if event.get("player_id") is not None})

        # 7. Initialize the decision engine
        decision_engine = GrowthDecisionEngine(gemini_client)

        # 8. Initialize reporter and generate final CSV report for at-risk players.
        reporter = ChurnReporter(modeling_engine, decision_engine)
        report_filename = f"churn_predictions_report_{datetime.utcnow().strftime('%Y%m%d-%H%M%S')}.csv"
        if player_ids:
            await reporter.generate_report(player_ids, report_filename)
            return {"message": "Churn report generated successfully.", "report_path": report_filename}
        else:
            raise HTTPException(status_code=404, detail="No players found to generate a report for.")

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred: {str(e)}")

@app.post("/create-cohorts")
async def create_cohorts(request: CohortCreationRequest):
    """
    Analyzes player data and groups them into cohorts.
    """
    try:
        # This endpoint now depends on data being in BigQuery.
        # We will simulate this by ensuring the processing pipeline has run.
        # For this example, we'll just use the global BQ instance.
        # A real implementation might trigger a fresh analysis or read from a snapshot.        
        if not IMPORT_JOBS:
            raise HTTPException(status_code=404, detail="No data has been imported yet. Please import data before creating cohorts.")
        
        latest_job = sorted(IMPORT_JOBS, key=lambda x: x['timestamp'], reverse=True)[0]
        events_data = await get_events_with_caching(latest_job['start_date'], latest_job['end_date'])

        gemini_client = GeminiClient()
        modeling_engine = PlayerModelingEngine(
            gemini_client=gemini_client,
            bigquery_service=BIGQUERY_SERVICE_INSTANCE,
            churn_inactive_days=int(CHURN_CONFIG.get("churn_inactive_days", 14)),
        )
        cohort_service = PlayerCohortService(modeling_engine)

        cohorts = await cohort_service.create_player_cohorts()


        # Extract a sample for the data sandbox glance
        data_glance = events_data[:3] # Get the first 3 events as a sample
        unique_event_names = sorted(list({event.get('event_type', 'N/A') for event in events_data}))

        return {
            "message": "Cohorts created successfully", 
            "cohorts": cohorts,
            "data_glance": data_glance,
            "event_names": unique_event_names
        }

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred: {str(e)}")

@app.get("/action-history")
async def get_action_history(limit: int = 200):
    """
    Retrieves a timeline of human-triggered operational actions.
    """
    try:
        history = _parse_audit_history(limit=limit)
        return {"action_history": history[: max(1, int(limit))]}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to read action history: {str(e)}")

@app.get("/data-sandbox/raw-events")
async def get_raw_events_sample(start_date: str, end_date: str):
    """
    Fetches a small sample of raw events for display in the data sandbox.
    """
    start_date = _normalize_date_for_amplitude(start_date)
    end_date = _normalize_date_for_amplitude(end_date)
    amplitude_client = AmplitudeService()
    events_data = amplitude_client.export_events(start_date, end_date)
    # Return a sample of 10 events to keep the response light
    return {"raw_events_sample": events_data[:10]}

@app.post("/data-sandbox/tag-event")
async def tag_event(request: DataSandboxRequest):
    """
    Adds a new normalization rule for an event name.
    This simulates the 'tagging' feature from the Data Sandbox.
    """
    if not request.raw_name or not request.normalized_name:
        raise HTTPException(status_code=400, detail="Both raw_name and normalized_name are required.")
    
    NORMALIZATION_MAPS["event_name_map"][request.raw_name] = request.normalized_name
    save_maps_to_cache() # Persist the new mapping
    
    return {"message": f"Rule created: '{request.raw_name}' will now be normalized to '{request.normalized_name}'.", "current_rules": NORMALIZATION_MAPS["event_name_map"]}

@app.get("/data-sandbox/glance")
async def get_data_sandbox_glance():
    """
    Provides a glance of the most recent import for the data sandbox,
    including a data sample and event counts.
    """
    if not IMPORT_JOBS:
        raise HTTPException(status_code=404, detail="No data has been imported yet.")

    # IMPORT_JOBS is append-only and may be loaded from cache in arbitrary order,
    # so sort explicitly to guarantee we pick the newest job.
    latest_job = max(
        IMPORT_JOBS,
        key=lambda job: datetime.fromisoformat(job.get("timestamp", datetime.min.isoformat())),
    )
    start_date = latest_job.get("start_date")
    end_date = latest_job.get("end_date")

    if not start_date or not end_date:
        raise HTTPException(status_code=404, detail="Latest import job is missing date range information.")

    # Fetch the cached raw data for the latest import
    events_data = await get_events_with_caching(start_date, end_date)
    if not events_data:
        raise HTTPException(status_code=404, detail="No raw data found for the latest import.")

    # Calculate event counts
    event_counts = {}
    for event in events_data:
        event_type = event.get("event_type", "N/A")
        event_counts[event_type] = event_counts.get(event_type, 0) + 1

    # Sort by count and take top 10
    sorted_event_counts = dict(sorted(event_counts.items(), key=lambda item: item[1], reverse=True)[:10])

    return {
        "filename": latest_job["name"],
        "sample": events_data[:1],
        "event_counts": sorted_event_counts,
    }

async def get_events_with_caching(start_date: str, end_date: str) -> list[dict]:
    """
    Fetches events from Amplitude, using a local file cache to avoid redundant API calls.
    """
    start_date = _normalize_date_for_amplitude(start_date)
    end_date = _normalize_date_for_amplitude(end_date)
    cache_filename = os.path.join(CACHE_DIR, f"{start_date}_{end_date}.json")
    
    if os.path.exists(cache_filename):
        print(f"Loading events from cache file: {cache_filename}")
        with open(cache_filename, 'r') as f:
            return json.load(f)

    # Fetch from Amplitude if not cached
    print("Fetching events from Amplitude API...")
    amplitude_client = AmplitudeService()
    events_data = amplitude_client.export_events(start_date, end_date)
    
    # Save to cache
    if events_data:
        print(f"Saving {len(events_data)} events to cache file: {cache_filename}")
        with open(cache_filename, 'w') as f:
            json.dump(events_data, f)
            
    return events_data

@app.delete("/job/{job_name}")
async def delete_job_cache(job_name: str):
    """
    Deletes a job from the import list and removes its associated cache file.
    """
    global IMPORT_JOBS, PREDICTION_JOBS
    job_to_delete = next((j for j in IMPORT_JOBS if j["name"] == job_name), None)

    if not job_to_delete:
        raise HTTPException(status_code=404, detail=f"Job '{job_name}' not found.")

    start_date = job_to_delete.get("start_date")
    end_date = job_to_delete.get("end_date")

    # Construct the base path/identifier for the job's data
    job_data_identifier = f"{start_date.replace('-', '')}_to_{end_date.replace('-', '')}"

    # 1. Delete raw cache file if it exists
    if start_date and end_date:
        cache_filename = os.path.join(CACHE_DIR, f"{start_date}_{end_date}.json")
        if os.path.exists(cache_filename):
            os.remove(cache_filename)
            print(f"Deleted cache file: {cache_filename}")

    # 2. Delete data from simulated GCS
    GCS_SERVICE_INSTANCE.delete_data_for_job(job_data_identifier)

    # 3. Delete data from simulated BigQuery
    BIGQUERY_SERVICE_INSTANCE.delete_data_for_job(job_data_identifier)

    # 3b. Delete ingestion checkpoints associated with this import job
    delete_ingestion_checkpoints(job_name)

    # 4. Delete all prediction cache variants for this import job.
    if os.path.isdir(PREDICTION_CACHE_DIR):
        prefix = f"{job_name}_"
        for filename in os.listdir(PREDICTION_CACHE_DIR):
            if filename == f"{job_name}.json" or filename.startswith(prefix):
                prediction_cache_file = os.path.join(PREDICTION_CACHE_DIR, filename)
                os.remove(prediction_cache_file)
                print(f"Deleted prediction cache file: {prediction_cache_file}")

    # 5. Remove prediction job records tied to this import job.
    removed_prediction_jobs = [j for j in PREDICTION_JOBS if j.get("import_job_name") == job_name]
    if removed_prediction_jobs:
        PREDICTION_JOBS = [j for j in PREDICTION_JOBS if j.get("import_job_name") != job_name]
        with PREDICTION_JOB_RUNNERS_LOCK:
            for prediction_job in removed_prediction_jobs:
                PREDICTION_JOB_RUNNERS.pop(prediction_job.get("id"), None)
        save_prediction_jobs_to_cache()

    IMPORT_JOBS = [j for j in IMPORT_JOBS if j["name"] != job_name]
    save_import_jobs_to_cache()
    return {"message": f"Job '{job_name}' and its cache have been deleted."}

def run_pipeline_background(start_date: str, end_date: str, job_name: str, source: str, continue_on_source_error: bool = True, auto_mapping: bool = False):
    """The actual data processing logic that runs in the background."""
    try:
        job = next((j for j in IMPORT_JOBS if j["name"] == job_name), None)

        start_date = _normalize_date_for_amplitude(start_date)
        end_date = _normalize_date_for_amplitude(end_date)
        job_identifier = f"{start_date}_to_{end_date}"

        source_names = [s.strip() for s in (source or "").split(",") if s.strip()]
        if not source_names:
            raise ValueError("At least one source connector name is required.")

        aggregate_ingestion = type("AggregateIngestion", (), {"message_queue_topic": []})()
        source_stats = []

        if job:
            job["current_step"] = "Starting import"
            job["progress_pct"] = 5
            save_import_jobs_to_cache()

        for idx, source_name in enumerate(source_names, start=1):
            try:
                if job:
                    job["current_step"] = f"Importing source {idx}/{len(source_names)}: {source_name}"
                    job["progress_pct"] = min(70, 10 + int((idx - 1) / max(1, len(source_names)) * 50))
                    save_import_jobs_to_cache()
                connector_config, conn_type = _get_connector_config(source_name)
                if not connector_config:
                    raise ValueError(f"Connector configuration for '{source_name}' not found.")

                connector_config = dict(connector_config)
                connector_config["field_mapping"] = get_field_mapping(source_name)

                ingestion_service = IngestionService(
                    gcs_service=GCS_SERVICE_INSTANCE,
                    connector_config=connector_config,
                    connector_type=conn_type,
                    source_config_id=source_name,
                )
                ingested_count = ingestion_service.fetch_and_publish_events(start_date, end_date, job_id=job_name)
                aggregate_ingestion.message_queue_topic.extend(ingestion_service.message_queue_topic)
                source_stats.append({
                    "source": source_name,
                    "type": conn_type,
                    "ingested_events": ingested_count,
                    "shards_created": len(ingestion_service.staged_shards),
                    "published_messages": len(ingestion_service.published_message_ids),
                    "status": "ok",
                })
                append_audit_log(
                    "import_source_ingested",
                    {
                        "job_name": job_name,
                        "source": source_name,
                        "connector_type": conn_type,
                        "ingested_events": ingested_count,
                        "shards_created": len(ingestion_service.staged_shards),
                        "published_messages": len(ingestion_service.published_message_ids),
                        "status": "ok",
                    },
                )
                update_connector_status(source_name, conn_type, success=True, ingested_events=ingested_count)
            except Exception as source_error:
                source_stats.append({
                    "source": source_name,
                    "type": "unknown",
                    "ingested_events": 0,
                    "status": "failed",
                    "error": str(source_error),
                })
                append_audit_log(
                    "import_source_ingested",
                    {
                        "job_name": job_name,
                        "source": source_name,
                        "connector_type": "unknown",
                        "ingested_events": 0,
                        "status": "failed",
                        "error": str(source_error),
                    },
                )
                update_connector_status(source_name, "unknown", success=False, ingested_events=0, error=str(source_error))
                if not continue_on_source_error:
                    raise

        if not aggregate_ingestion.message_queue_topic:
            raise ValueError("No source produced data for this import window.")

        if job:
            job["current_step"] = "Import complete, preparing processing"
            job["progress_pct"] = 75
            save_import_jobs_to_cache()

        if auto_mapping:
            if job and job.get("status") != "Interrupted":
                job["source_stats"] = source_stats
                job["ingestion_checkpoints"] = list_ingestion_checkpoints(job_name)
                job["pending_notifications"] = list(aggregate_ingestion.message_queue_topic)
                job["current_step"] = "Awaiting manual mapping"
                job["progress_pct"] = 85
                _transition_job_status(job, "Awaiting Mapping", IMPORT_JOB_ALLOWED_TRANSITIONS, "import")
                save_import_jobs_to_cache()
                append_audit_log(
                    "import_job_awaiting_mapping",
                    {
                        "job_name": job_name,
                        "source": ",".join(source_names),
                        "start_date": start_date,
                        "end_date": end_date,
                        "count": len(aggregate_ingestion.message_queue_topic),
                    },
                )
                print(f"Job '{job_name}' is awaiting manual mapping.")
            return

        if job:
            job["current_step"] = "Processing and normalizing events"
            job["progress_pct"] = 90
            save_import_jobs_to_cache()

        processing_service = DataProcessingService(
            bigquery_service=BIGQUERY_SERVICE_INSTANCE,
            gcs_service=GCS_SERVICE_INSTANCE,
            job_identifier=job_identifier,
        )
        processing_stats = processing_service.run_processing_pipeline(aggregate_ingestion)
        warehouse_stats = {
            "curation": BIGQUERY_SERVICE_INSTANCE.run_events_curation(job_id=job_identifier),
            "player_latest_state": BIGQUERY_SERVICE_INSTANCE.refresh_player_latest_state(job_id=job_identifier),
        }

        if job and job.get("status") != "Interrupted":
            job["source_stats"] = source_stats
            job["ingestion_checkpoints"] = list_ingestion_checkpoints(job_name)
            job["processing_stats"] = processing_stats
            job["warehouse_stats"] = warehouse_stats
            job["current_step"] = "Completed"
            job["progress_pct"] = 100
            _transition_job_status(job, "Ready to Use", IMPORT_JOB_ALLOWED_TRANSITIONS, "import")
            save_import_jobs_to_cache()
            append_audit_log(
                "import_job_completed",
                {
                    "job_name": job_name,
                    "source": ",".join(source_names),
                    "start_date": start_date,
                    "end_date": end_date,
                    "raw_normalized_events": processing_stats.get("raw_normalized_events"),
                    "deduped_events": processing_stats.get("deduped_events"),
                    "duplicates_removed": processing_stats.get("duplicates_removed"),
                },
            )
            print(f"Job '{job_name}' completed successfully.")

    except Exception as e:
        print(f"Error processing job '{job_name}': {e}")
        if job and job.get("status") != "Interrupted":
            job["last_error"] = str(e)
            job["current_step"] = "Failed"
            job["progress_pct"] = job.get("progress_pct", 0)
            _transition_job_status(job, "Failed", IMPORT_JOB_ALLOWED_TRANSITIONS, "import")
            save_import_jobs_to_cache()
            append_audit_log(
                "import_job_failed",
                {
                    "job_name": job_name,
                    "source": source,
                    "start_date": start_date,
                    "end_date": end_date,
                    "error": str(e),
                },
            )

@app.post("/ingest-and-process-data")
async def ingest_and_process_data(request: IngestionRequest, background_tasks: BackgroundTasks):
    """
    Triggers the simulated data pipeline: Ingestion -> Processing -> Storage.
    The actual processing is run as a background task.
    """
    try:
        start_date = _normalize_date_for_amplitude(request.start_date)
        end_date = _normalize_date_for_amplitude(request.end_date)
        job_timestamp = datetime.utcnow()
        expiration_timestamp = job_timestamp + timedelta(days=3)
        job_name = f"{job_timestamp.strftime('%Y%m%d-%H%M%S')}-{request.source.capitalize()}"
        IMPORT_JOBS.append({
            "name": job_name,
            "status": "Processing",
            "current_step": "Queued",
            "progress_pct": 0,
            "timestamp": job_timestamp.isoformat(),
            "creation_timestamp": job_timestamp.isoformat(),
            "expiration_timestamp": expiration_timestamp.isoformat(),
            "start_date": start_date,
            "end_date": end_date
        })
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
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred during the pipeline execution: {str(e)}")

@app.post("/job/{job_name}/stop")
async def stop_job(job_name: str):
    """
    Stops a currently processing job by marking its status as 'Interrupted'.
    """
    global IMPORT_JOBS
    job_to_stop = next((j for j in IMPORT_JOBS if j["name"] == job_name), None)

    if not job_to_stop:
        raise HTTPException(status_code=404, detail=f"Job '{job_name}' not found.")

    if job_to_stop.get("status") == "Processing":
        append_audit_log("import_job_stop_requested", {"job_name": job_name, "status": job_to_stop.get("status")})
        _transition_job_status(job_to_stop, "Interrupted", IMPORT_JOB_ALLOWED_TRANSITIONS, "import")
        save_import_jobs_to_cache()
        return {"message": f"Job '{job_name}' has been interrupted."}
    else:
        raise HTTPException(status_code=400, detail=f"Job '{job_name}' is not currently processing.")

@app.post("/job/{job_name}/process-after-mapping")
async def process_after_mapping(job_name: str):
    """Continue processing for a job paused at 'Awaiting Mapping'."""
    job = next((j for j in IMPORT_JOBS if j["name"] == job_name), None)
    if not job:
        raise HTTPException(status_code=404, detail=f"Job '{job_name}' not found.")
    if job.get("status") != "Awaiting Mapping":
        raise HTTPException(status_code=400, detail=f"Job '{job_name}' is not awaiting mapping.")

    pending_notifications = job.get("pending_notifications") or []
    if not pending_notifications:
        raise HTTPException(status_code=400, detail="No pending notifications to process.")

    start_date = job.get("start_date")
    end_date = job.get("end_date")
    if not start_date or not end_date:
        raise HTTPException(status_code=400, detail="Job date range metadata missing.")

    job_identifier = f"{start_date}_to_{end_date}"
    aggregate_ingestion = type("AggregateIngestion", (), {"message_queue_topic": pending_notifications})()

    append_audit_log(
        "process_after_mapping_requested",
        {
            "job_name": job_name,
            "source": ",".join([s.get("source") for s in (job.get("source_stats") or []) if s.get("source")]) or None,
            "start_date": start_date,
            "end_date": end_date,
        },
    )
    job["current_step"] = "Processing after manual mapping"
    job["progress_pct"] = 90
    save_import_jobs_to_cache()

    processing_service = DataProcessingService(
        bigquery_service=BIGQUERY_SERVICE_INSTANCE,
        gcs_service=GCS_SERVICE_INSTANCE,
        job_identifier=job_identifier,
    )
    processing_stats = processing_service.run_processing_pipeline(aggregate_ingestion)
    warehouse_stats = {
        "curation": BIGQUERY_SERVICE_INSTANCE.run_events_curation(job_id=job_identifier),
        "player_latest_state": BIGQUERY_SERVICE_INSTANCE.refresh_player_latest_state(job_id=job_identifier),
    }

    job["ingestion_checkpoints"] = list_ingestion_checkpoints(job_name)
    job["processing_stats"] = processing_stats
    job["warehouse_stats"] = warehouse_stats
    job["current_step"] = "Completed"
    job["progress_pct"] = 100
    job.pop("pending_notifications", None)
    _transition_job_status(job, "Ready to Use", IMPORT_JOB_ALLOWED_TRANSITIONS, "import")
    save_import_jobs_to_cache()
    append_audit_log(
        "import_job_completed_after_mapping",
        {
            "job_name": job_name,
            "source": ",".join([s.get("source") for s in (job.get("source_stats") or []) if s.get("source")]) or None,
            "start_date": start_date,
            "end_date": end_date,
            "raw_normalized_events": processing_stats.get("raw_normalized_events"),
            "deduped_events": processing_stats.get("deduped_events"),
            "duplicates_removed": processing_stats.get("duplicates_removed"),
        },
    )

    return {"message": f"Job '{job_name}' processed after mapping.", "processing_stats": processing_stats}


@app.post("/predict-churn-for-import")
async def predict_churn_for_import(request: ChurnPredictionRequest):
    """
    Runs churn prediction for all players in a specified imported dataset.
    """
    try:
        predictions = await _compute_predictions_for_job(request.job_name, request.force_recalculate, request.prediction_mode or "local")
        return {"predictions": predictions}
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error during churn prediction for job '{request.job_name}': {e}")
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred during churn prediction: {str(e)}")


def _create_prediction_job(job_name: str, force_recalculate: bool) -> dict:
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


def _get_prediction_job(prediction_job_id: str) -> Optional[Dict[str, Any]]:
    return next((j for j in PREDICTION_JOBS if j.get("id") == prediction_job_id), None)


def _register_prediction_job_runner(prediction_job_id: str, runner: threading.Thread) -> None:
    with PREDICTION_JOB_RUNNERS_LOCK:
        PREDICTION_JOB_RUNNERS[prediction_job_id] = runner


def _get_prediction_job_runner(prediction_job_id: str) -> Optional[threading.Thread]:
    with PREDICTION_JOB_RUNNERS_LOCK:
        return PREDICTION_JOB_RUNNERS.get(prediction_job_id)


def _clear_prediction_job_runner(prediction_job_id: str) -> None:
    with PREDICTION_JOB_RUNNERS_LOCK:
        PREDICTION_JOB_RUNNERS.pop(prediction_job_id, None)


async def _estimate_churn_with_mode(
    modeling_engine: PlayerModelingEngine,
    player_id: Any,
    profile: Dict[str, Any],
    prediction_mode: str,
):
    mode = (prediction_mode or "local").lower()
    local_estimate = None
    cloud_estimate = None
    cloud_error = None

    if mode not in {"local", "cloud", "parallel"}:
        raise HTTPException(status_code=400, detail="prediction_mode must be one of: local, cloud, parallel.")

    # Already-churned users are resolved by inactivity rule only.
    if profile.get("churn_state") == "churned":
        local_estimate = await modeling_engine.estimate_churn_risk(player_id, profile)
        return local_estimate, {
            "mode": mode,
            "selected_source": "rule",
            "local_estimate": local_estimate,
            "cloud_estimate": None,
            "cloud_error": None,
        }

    if mode in {"local", "parallel"}:
        local_estimate = await modeling_engine.estimate_churn_risk(player_id, profile)

    if mode in {"cloud", "parallel"}:
        try:
            cloud_service = CloudChurnService()
            cloud_estimate = cloud_service.estimate_churn_risk(player_id, profile)
        except Exception as e:
            cloud_error = str(e)

    if mode == "cloud":
        if cloud_estimate:
            return cloud_estimate, {"mode": mode, "local_estimate": None, "cloud_estimate": cloud_estimate, "cloud_error": None}
        # Cloud-only mode with failure should fail fast.
        raise HTTPException(status_code=502, detail=f"Cloud churn prediction failed: {cloud_error or 'unknown error'}")

    if mode == "parallel":
        # Prefer cloud estimate when available, otherwise local fallback.
        selected = cloud_estimate or local_estimate
        return selected, {
            "mode": mode,
            "selected_source": "cloud" if cloud_estimate else "local",
            "local_estimate": local_estimate,
            "cloud_estimate": cloud_estimate,
            "cloud_error": cloud_error,
        }

    return local_estimate, {"mode": mode, "local_estimate": local_estimate, "cloud_estimate": None, "cloud_error": None}


async def _compute_predictions_for_job(job_name: str, force_recalculate: bool, prediction_mode: str = "local", progress_callback=None, stop_requested=None) -> list[dict]:
    job = next((j for j in IMPORT_JOBS if j["name"] == job_name), None)
    if not job or job.get("status") != "Ready to Use":
        raise HTTPException(status_code=404, detail=f"Job '{job_name}' not found or not ready.")

    os.makedirs(PREDICTION_CACHE_DIR, exist_ok=True)
    effective_mode = (prediction_mode or "local").lower()
    if CHURN_CONFIG.get("third_party_for_active", True) and effective_mode == "local" and os.getenv("CHURN_API_URL"):
        effective_mode = "parallel"
    mode_key = effective_mode
    prediction_cache_file = os.path.join(PREDICTION_CACHE_DIR, f"{job_name}_{mode_key}.json")
    if not force_recalculate and os.path.exists(prediction_cache_file):
        print(f"Loading churn predictions from cache: {prediction_cache_file}")
        with open(prediction_cache_file, 'r') as f:
            return json.load(f)

    gemini_client = None
    try:
        if os.getenv("GOOGLE_API_KEY"):
            gemini_client = GeminiClient()
    except Exception as e:
        print(f"Warning: Gemini unavailable, falling back to heuristic mode: {e}")

    modeling_engine = PlayerModelingEngine(
            gemini_client=gemini_client,
            bigquery_service=BIGQUERY_SERVICE_INSTANCE,
            churn_inactive_days=int(CHURN_CONFIG.get("churn_inactive_days", 14)),
        )
    decision_engine = GrowthDecisionEngine(gemini_client)

    player_ids = modeling_engine.get_all_player_ids()
    if not player_ids:
        return []

    total_players = len(player_ids)
    predictions = []
    skipped_players = 0

    if callable(progress_callback):
        progress_callback(predictions, 0, total_players)

    for idx, player_id in enumerate(player_ids, start=1):
        # Yield between players so stop requests can be observed promptly.
        await asyncio.sleep(0)
        if callable(stop_requested) and stop_requested():
            print("Prediction stop requested. Exiting early.")
            break
        try:
            profile = modeling_engine.build_player_profile(player_id)
            if not profile:
                continue

            churn_estimate, churn_details = await _estimate_churn_with_mode(
                modeling_engine=modeling_engine,
                player_id=player_id,
                profile=profile,
                prediction_mode=effective_mode,
            )
            churn_state = churn_estimate.get("churn_state", profile.get("churn_state", "active")) if churn_estimate else profile.get("churn_state", "active")

            external_match = None
            if churn_state == "active":
                external_match = _find_external_churn_match(profile)
                if external_match:
                    churn_estimate = {
                        "player_id": player_id,
                        "churn_state": "active",
                        "churn_risk": external_match.get("churn_risk", "unknown"),
                        "reason": external_match.get("reason", "Third-party churn update"),
                        "top_signals": [{"signal": "external_update", "value": external_match.get("source", "external")}],
                    }

            churn_risk = churn_estimate.get("churn_risk", "N/A") if churn_estimate else "N/A"
            recency_risk = min(max(float(profile.get("days_since_last_seen", 0) or 0) * 3.0, 0.0), 100.0)

            if churn_state == "churned":
                policy_eval = {"route": "NO_ACTION", "reason": "Player already churned by inactivity threshold."}
            else:
                policy_eval = LLM_POLICY_ENGINE.evaluate(
                    LLM_POLICY_CONFIG,
                    {
                        "ltv": float(profile.get("total_revenue", 0.0) or 0.0),
                        "churn_risk": churn_risk,
                        "confidence_gap": 50.0,
                        "recency_risk": recency_risk,
                        "weekly_actions_count": 0,
                        "blacklisted": False,
                        "daily_budget_exceeded": False,
                        "cache_hit": False,
                    },
                )

            if churn_state == "churned":
                next_action = {
                    "player_id": player_id,
                    "decision": "NO_ACTION",
                    "reason": "Already churned by inactivity threshold.",
                }
            elif policy_eval.get("route") == "NO_LLM":
                fallback = _default_action_suggestion(churn_state, churn_risk)
                next_action = {
                    "player_id": player_id,
                    "decision": fallback.get("decision", "NO_ACTION"),
                    "content": fallback.get("content"),
                    "reason": policy_eval.get("reason", "Policy routed to NO_LLM."),
                }
            else:
                next_action = decision_engine.decide_next_action(profile, churn_estimate, "reduce_churn")

            churn_reason = churn_estimate.get("reason", "N/A") if churn_estimate else "N/A"
            prediction_source = "unknown"
            if churn_state == "churned":
                prediction_source = "rule"
            elif external_match:
                prediction_source = f"external:{external_match.get('source', 'external')}"
            else:
                prediction_source = (churn_details or {}).get("selected_source") or (effective_mode if effective_mode != "parallel" else "parallel-selected")

            predictions.append({
                "user_id": player_id,
                "email": profile.get("email"),
                "ltv": profile.get("total_revenue", "N/A"),
                "session_count": profile.get("total_sessions", "N/A"),
                "event_count": profile.get("total_events", "N/A"),
                "days_since_last_seen": profile.get("days_since_last_seen", "N/A"),
                "churn_state": churn_state,
                "churn_inactive_days": int(CHURN_CONFIG.get("churn_inactive_days", 14)),
                "predicted_churn_risk": churn_risk,
                "churn_reason": churn_reason,
                "top_signals": churn_estimate.get("top_signals", []) if churn_estimate else [],
                "prediction_source": prediction_source,
                "suggested_action": next_action.get("content", "No action suggested.") if next_action else "No action suggested.",
                "llm_route": policy_eval.get("route"),
                "policy_reason": policy_eval.get("reason"),
                "prediction_mode": effective_mode,
                "prediction_details": churn_details,
            })

            if callable(progress_callback):
                progress_callback(predictions, idx, total_players)
        except Exception as per_player_error:
            skipped_players += 1
            print(f"Skipping player {player_id} due to prediction error: {per_player_error}")
            if callable(progress_callback):
                progress_callback(predictions, idx, total_players)
            continue

    if predictions:
        print(f"Saving churn predictions to cache: {prediction_cache_file}")
        with open(prediction_cache_file, 'w') as f:
            json.dump(predictions, f, indent=2)

    return predictions


async def _run_prediction_job(prediction_job_id: str, job_name: str, force_recalculate: bool):
    prediction_job = _get_prediction_job(prediction_job_id)
    if not prediction_job:
        return

    def _progress_callback(preds, processed, total):
        prediction_job["predictions"] = preds
        prediction_job["processed_count"] = processed
        prediction_job["total_count"] = total
        prediction_job["result_count"] = len(preds)
        prediction_job["progress_pct"] = int((processed / total) * 100) if total else 0
        save_prediction_jobs_to_cache()

    def _stop_requested():
        return bool(prediction_job.get("stop_requested"))

    try:
        prediction_mode = prediction_job.get("prediction_mode", "local")
        predictions = await _compute_predictions_for_job(
            job_name,
            force_recalculate,
            prediction_mode,
            progress_callback=_progress_callback,
            stop_requested=_stop_requested,
        )
        if prediction_job.get("stop_requested"):
            _transition_job_status(prediction_job, "Stopped", PREDICTION_JOB_ALLOWED_TRANSITIONS, "prediction")
            append_audit_log(
                "prediction_job_stopped",
                {
                    "prediction_job_id": prediction_job_id,
                    "import_job_name": job_name,
                    "prediction_mode": prediction_mode,
                    "processed_count": prediction_job.get("processed_count"),
                    "total_count": prediction_job.get("total_count"),
                    "result_count": len(predictions),
                },
            )
        else:
            _transition_job_status(prediction_job, "Ready", PREDICTION_JOB_ALLOWED_TRANSITIONS, "prediction")
            append_audit_log(
                "prediction_job_completed",
                {
                    "prediction_job_id": prediction_job_id,
                    "import_job_name": job_name,
                    "prediction_mode": prediction_mode,
                    "processed_count": prediction_job.get("processed_count"),
                    "total_count": prediction_job.get("total_count"),
                    "result_count": len(predictions),
                },
            )
        prediction_job["predictions"] = predictions
        prediction_job["result_count"] = len(predictions)
        prediction_job["error"] = None
    except asyncio.CancelledError:
        prediction_job["stop_requested"] = True
        if prediction_job.get("status") == "Processing":
            _transition_job_status(prediction_job, "Stopped", PREDICTION_JOB_ALLOWED_TRANSITIONS, "prediction")
        append_audit_log(
            "prediction_job_stopped",
            {
                "prediction_job_id": prediction_job_id,
                "import_job_name": job_name,
                "prediction_mode": prediction_job.get("prediction_mode", "local"),
                "processed_count": prediction_job.get("processed_count"),
                "total_count": prediction_job.get("total_count"),
                "result_count": prediction_job.get("result_count"),
            },
        )
        prediction_job["error"] = None
    except Exception as e:
        prediction_job["error"] = str(e)
        _transition_job_status(prediction_job, "Failed", PREDICTION_JOB_ALLOWED_TRANSITIONS, "prediction")
        append_audit_log(
            "prediction_job_failed",
            {
                "prediction_job_id": prediction_job_id,
                "import_job_name": job_name,
                "prediction_mode": prediction_job.get("prediction_mode", "local"),
                "processed_count": prediction_job.get("processed_count"),
                "total_count": prediction_job.get("total_count"),
                "result_count": prediction_job.get("result_count"),
                "error": str(e),
            },
        )
    finally:
        _clear_prediction_job_runner(prediction_job_id)
        save_prediction_jobs_to_cache()


def _run_prediction_job_in_thread(prediction_job_id: str, job_name: str, force_recalculate: bool) -> None:
    asyncio.run(_run_prediction_job(prediction_job_id, job_name, force_recalculate))


@app.post("/predict-churn-for-import-async")
async def predict_churn_for_import_async(request: ChurnPredictionRequest, background_tasks: BackgroundTasks):
    """
    Starts churn prediction in the background and returns a prediction job id.
    """
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


@app.get("/prediction-jobs")
async def list_prediction_jobs():
    """Lists all background prediction jobs."""
    return {"prediction_jobs": sorted(PREDICTION_JOBS, key=lambda x: x.get("timestamp", ""), reverse=True)}


@app.get("/prediction-job/{prediction_job_id}")
async def get_prediction_job(prediction_job_id: str):
    """Returns a specific prediction job status."""
    prediction_job = _get_prediction_job(prediction_job_id)
    if not prediction_job:
        raise HTTPException(status_code=404, detail=f"Prediction job '{prediction_job_id}' not found.")
    return {"prediction_job": prediction_job}


@app.post("/prediction-job/{prediction_job_id}/stop")
async def stop_prediction_job(prediction_job_id: str):
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
        _transition_job_status(prediction_job, "Stopped", PREDICTION_JOB_ALLOWED_TRANSITIONS, "prediction")
        save_prediction_jobs_to_cache()
    return {"message": "Stop requested.", "prediction_job": prediction_job}

@app.post("/analyze-and-engage-player")
async def analyze_and_engage_player(request: PlayerAnalysisRequest):
    """
    Analyzes a single player for churn risk, executes an engagement action if needed,
    and simulates feedback. This creates a full, closed-loop process.
    """
    try:
        if not request.player_id:
            raise ValueError("Player ID cannot be empty.")

        # Initialize clients and engines.
        # The modeling engine now uses the BigQuery service to get data.
        gemini_client = None
        try:
            if os.getenv("GOOGLE_API_KEY"):
                gemini_client = GeminiClient()
        except Exception as e:
            print(f"Warning: Gemini unavailable, falling back to heuristic mode: {e}")

        modeling_engine = PlayerModelingEngine(
            gemini_client=gemini_client,
            bigquery_service=BIGQUERY_SERVICE_INSTANCE,
            churn_inactive_days=int(CHURN_CONFIG.get("churn_inactive_days", 14)),
        )
        decision_engine = GrowthDecisionEngine(gemini_client)
        executor = EngagementExecutor()
        feedback_service = EngagementFeedback()

        player_id = request.player_id

        # 3. Build profile and estimate churn
        player_profile = modeling_engine.build_player_profile(player_id)
        if not player_profile:
            raise HTTPException(status_code=404, detail=f"Player with ID '{player_id}' not found.")

        effective_mode = (request.prediction_mode or "local").lower()
        if CHURN_CONFIG.get("third_party_for_active", True) and effective_mode == "local" and os.getenv("CHURN_API_URL"):
            effective_mode = "parallel"

        churn_estimate, churn_details = await _estimate_churn_with_mode(
            modeling_engine=modeling_engine,
            player_id=player_id,
            profile=player_profile,
            prediction_mode=effective_mode,
        )

        churn_state = churn_estimate.get("churn_state", player_profile.get("churn_state", "active")) if churn_estimate else player_profile.get("churn_state", "active")

        external_match = None
        if churn_state == "active":
            external_match = _find_external_churn_match(player_profile)
            if external_match:
                churn_estimate = {
                    "player_id": player_id,
                    "churn_state": "active",
                    "churn_risk": external_match.get("churn_risk", "unknown"),
                    "reason": external_match.get("reason", "Third-party churn update"),
                    "top_signals": [{"signal": "external_update", "value": external_match.get("source", "external")}],
                }

        churn_risk = churn_estimate.get("churn_risk", "N/A") if churn_estimate else "N/A"
        recency_risk = min(max(float(player_profile.get("days_since_last_seen", 0) or 0) * 3.0, 0.0), 100.0)

        if churn_state == "churned":
            policy_eval = {"route": "NO_ACTION", "reason": "Player already churned by inactivity threshold."}
        else:
            policy_eval = LLM_POLICY_ENGINE.evaluate(
                LLM_POLICY_CONFIG,
                {
                    "ltv": float(player_profile.get("total_revenue", 0.0) or 0.0),
                    "churn_risk": churn_risk,
                    "confidence_gap": 50.0,
                    "recency_risk": recency_risk,
                    "weekly_actions_count": 0,
                    "blacklisted": False,
                    "daily_budget_exceeded": False,
                    "cache_hit": False,
                },
            )

        # 4. Decide and execute next best action
        if churn_state == "churned":
            next_action = {
                "player_id": player_id,
                "decision": "NO_ACTION",
                "reason": "Already churned by inactivity threshold.",
            }
        elif policy_eval.get("route") == "NO_LLM":
            fallback = _default_action_suggestion(churn_state, churn_risk)
            next_action = {
                "player_id": player_id,
                "decision": fallback.get("decision", "NO_ACTION"),
                "content": fallback.get("content"),
                "reason": policy_eval.get("reason", "Policy routed to NO_LLM."),
            }
        else:
            next_action = decision_engine.decide_next_action(player_profile, churn_estimate, "reduce_churn")

        assignment = None
        if EXPERIMENT_CONFIG.get("enabled", True):
            assignment = EXPERIMENT_SERVICE.assign(
                experiment_id=EXPERIMENT_CONFIG.get("experiment_id", "churn_engagement_v1"),
                player_id=player_id,
                holdout_pct=float(EXPERIMENT_CONFIG.get("holdout_pct", 0.10)),
                b_variant_pct=float(EXPERIMENT_CONFIG.get("b_variant_pct", 0.50)),
            )
            if assignment["group"] == "holdout":
                next_action = {
                    "player_id": player_id,
                    "decision": "NO_ACTION",
                    "reason": "Experiment holdout group",
                }
            elif assignment["group"] == "treatment_b" and next_action and next_action.get("decision") == "ACT":
                # Simple variant tweak for local experimenting
                next_action["content"] = f"{next_action.get('content', '')} [Variant B]"

        action_id = executor.execute_action(next_action)

        exposure = {
            "ts": datetime.utcnow().isoformat(),
            "experiment_id": EXPERIMENT_CONFIG.get("experiment_id", "churn_engagement_v1"),
            "player_id": str(player_id),
            "group": assignment.get("group") if assignment else "disabled",
            "decision": next_action.get("decision") if next_action else "NONE",
            "action_id": action_id,
        }
        EXPERIMENT_SERVICE.record_exposure(exposure)

        # 5. Simulate and record feedback
        feedback = None
        if action_id:
            feedback = feedback_service.get_engagement_result(player_id, action_id)
            if assignment:
                EXPERIMENT_SERVICE.record_outcome({
                    "ts": datetime.utcnow().isoformat(),
                    "experiment_id": EXPERIMENT_CONFIG.get("experiment_id", "churn_engagement_v1"),
                    "player_id": str(player_id),
                    "group": assignment.get("group"),
                    "action_id": action_id,
                    "simulated_response": feedback.get("simulated_response"),
                })

        prediction_source = "rule" if churn_state == "churned" else (f"external:{external_match.get('source', 'external')}" if external_match else ((churn_details or {}).get("selected_source") or (effective_mode if effective_mode != "parallel" else "parallel-selected")))

        return {
            "player_profile": player_profile,
            "churn_estimate": churn_estimate,
            "churn_prediction_details": churn_details,
            "prediction_source": prediction_source,
            "policy_eval": policy_eval,
            "experiment_assignment": assignment,
            "action_taken": next_action,
            "feedback": feedback,
        }

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred: {str(e)}")

if __name__ == '__main__':
    # To run this API server:
    # 1. Make sure you have fastapi and uvicorn installed:
    #    pip install fastapi "uvicorn[standard]"
    # 2. Set your environment variables: AMPLITUDE_API_KEY, AMPLITUDE_SECRET_KEY, GOOGLE_API_KEY
    # 3. Choose data backend mode (optional):
    #    mock (default): DATA_BACKEND_MODE=mock
    #    gcp: DATA_BACKEND_MODE=gcp (requires BIGQUERY_PROJECT_ID + ADC + GCS bucket access)
    # 4. Run the server:
    #    uvicorn main_service:app --reload
    
    print("Starting API server. Run with: uvicorn main_service:app --reload")
    uvicorn.run(app, host="0.0.0.0", port=8000)
