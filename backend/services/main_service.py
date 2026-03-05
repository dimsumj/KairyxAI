# main_service.py

import os
import re
import uvicorn
import shutil
import uuid
import requests
import zipfile
import io
import gzip
import json
from typing import Any, Optional, Dict
from datetime import (
    datetime,
    timedelta,
)
from fastapi import FastAPI, HTTPException, Request, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from starlette.responses import FileResponse
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
from bigquery_service import BigQueryService
from gcs_service import GcsService
from amplitude_service import AmplitudeService
from engagement_feedback import EngagementFeedback
from policy_engine import LlmPolicyEngine, DEFAULT_LLM_POLICY
from cloud_churn_service import CloudChurnService
from pydantic import BaseModel, Field
from local_job_store import (
    init_db as init_local_job_db,
    save_import_jobs as save_import_jobs_db,
    load_import_jobs as load_import_jobs_db,
    save_prediction_jobs as save_prediction_jobs_db,
    load_prediction_jobs as load_prediction_jobs_db,
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
LLM_POLICY_ENGINE = LlmPolicyEngine()
LLM_POLICY_CONFIG = DEFAULT_LLM_POLICY.copy()


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

# Load any cached API keys on application startup
# clear_cache_on_startup()
# clear_api_key_cache_on_startup()
init_local_job_db()
load_import_jobs_from_cache()
load_prediction_jobs_from_cache()
load_keys_from_cache()
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
    """Request model for setting the Adjust API token."""
    adjust_api_token: str = Field(..., alias='api_token')

class SendGridApiKey(BaseModel):
    """Request model for setting the SendGrid API key."""
    sendgrid_api_key: str = Field(..., alias='api_key')

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
                if connectors.get("amplitude"):
                    for config in connectors["amplitude"]:
                        sources.append({"id": config["name"], "name": config["name"]})
                # In the future, you could add other source types here
                # if connectors.get("some_other_source"):
                #    ...
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
        }
    }
    return services_status

@app.post("/configure-adjust-credentials")
async def configure_adjust_credentials(key: AdjustApiKey):
    """
    An API endpoint to set the Adjust API token for the session.
    **Security Warning:** This is insecure for production. API keys should be
    set as environment variables on the server.
    """
    os.environ["ADJUST_API_TOKEN"] = key.adjust_api_token
    new_config = {"api_token": key.adjust_api_token}
    _add_connector_config("adjust", "Adjust", new_config)
    append_audit_log("connector_configured", {"type": "adjust", "name": "Adjust"})
    return {"message": "Adjust API token has been configured and cached."}

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
        modeling_engine = PlayerModelingEngine(gemini_client=gemini_client, bigquery_service=BIGQUERY_SERVICE_INSTANCE)

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
        modeling_engine = PlayerModelingEngine(gemini_client=gemini_client, bigquery_service=BIGQUERY_SERVICE_INSTANCE)
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
async def get_action_history():
    """
    Retrieves the history of engagement actions taken.
    """
    try:
        with open('engagement_actions.log', 'r') as f:
            logs = f.readlines()
        
        history = []
        # Example log line: 2026-01-16 03:48:42,420 - INFO - Action Sent - ActionID: ..., PlayerID: ..., Channel: ..., Content: '...'
        log_pattern = re.compile(r"^(?P<timestamp>[\d\- ,:]+) - INFO - Action Sent - ActionID: (?P<action_id>[^,]+), PlayerID: (?P<player_id>[^,]+), Channel: (?P<channel>[^,]+), Content: '(?P<content>.+)'$")
        for log in logs:
            match = log_pattern.match(log.strip())
            if match:
                history.append(match.groupdict())
        
        return {"action_history": history}
    except FileNotFoundError:
        return {"action_history": []}
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
    global IMPORT_JOBS
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

    # 4. Delete prediction cache file if it exists
    prediction_cache_file = os.path.join(PREDICTION_CACHE_DIR, f"{job_name}.json")
    if os.path.exists(prediction_cache_file):
        os.remove(prediction_cache_file)
        print(f"Deleted prediction cache file: {prediction_cache_file}")

    IMPORT_JOBS = [j for j in IMPORT_JOBS if j["name"] != job_name]
    save_import_jobs_to_cache()
    return {"message": f"Job '{job_name}' and its cache have been deleted."}

def run_pipeline_background(start_date: str, end_date: str, job_name: str, source: str):
    """The actual data processing logic that runs in the background."""
    try:
        # Find the job to update its status later
        job = next((j for j in IMPORT_JOBS if j["name"] == job_name), None)

        # 1. Ingestion: Fetch from source, upload to GCS, and publish notification
        # Construct the job_identifier consistently for GCS and BigQuery
        start_date = _normalize_date_for_amplitude(start_date)
        end_date = _normalize_date_for_amplitude(end_date)
        job_identifier = f"{start_date}_to_{end_date}"

        connector_config, conn_type = _get_connector_config(source) # 'source' is now the connector name
        if not connector_config:
            raise ValueError(f"Connector configuration for '{source}' not found.")

        ingestion_service = IngestionService(gcs_service=GCS_SERVICE_INSTANCE, connector_config=connector_config, connector_type=conn_type)
        # For now, we assume only amplitude is a valid ingestion source.
        if conn_type == 'amplitude':
            ingestion_service.fetch_and_publish_events(start_date, end_date)

        # 2. Processing: Consume from queue, normalize, and write to BigQuery
        processing_service = DataProcessingService(bigquery_service=BIGQUERY_SERVICE_INSTANCE, gcs_service=GCS_SERVICE_INSTANCE, job_identifier=job_identifier)
        processing_service.run_processing_pipeline(ingestion_service)

        if job and job.get("status") != "Interrupted":
            job["status"] = "Ready to Use"
            save_import_jobs_to_cache()
            print(f"Job '{job_name}' completed successfully.")

    except Exception as e:
        print(f"Error processing job '{job_name}': {e}")
        if job and job.get("status") != "Interrupted":
            job["status"] = "Failed"
            save_import_jobs_to_cache()

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
            "timestamp": job_timestamp.isoformat(),
            "creation_timestamp": job_timestamp.isoformat(),
            "expiration_timestamp": expiration_timestamp.isoformat(),
            "start_date": start_date,
            "end_date": end_date
        })
        background_tasks.add_task(run_pipeline_background, start_date, end_date, job_name, request.source)
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
        job_to_stop["status"] = "Interrupted"
        save_import_jobs_to_cache()
        return {"message": f"Job '{job_name}' has been interrupted."}
    else:
        raise HTTPException(status_code=400, detail=f"Job '{job_name}' is not currently processing.")

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
        "error": None,
    }
    PREDICTION_JOBS.append(prediction_job)
    save_prediction_jobs_to_cache()
    return prediction_job


def _get_prediction_job(prediction_job_id: str) -> Optional[Dict[str, Any]]:
    return next((j for j in PREDICTION_JOBS if j.get("id") == prediction_job_id), None)


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


async def _compute_predictions_for_job(job_name: str, force_recalculate: bool, prediction_mode: str = "local") -> list[dict]:
    job = next((j for j in IMPORT_JOBS if j["name"] == job_name), None)
    if not job or job.get("status") != "Ready to Use":
        raise HTTPException(status_code=404, detail=f"Job '{job_name}' not found or not ready.")

    os.makedirs(PREDICTION_CACHE_DIR, exist_ok=True)
    mode_key = (prediction_mode or "local").lower()
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

    modeling_engine = PlayerModelingEngine(gemini_client=gemini_client, bigquery_service=BIGQUERY_SERVICE_INSTANCE)
    decision_engine = GrowthDecisionEngine(gemini_client)

    player_ids = modeling_engine.get_all_player_ids()
    if not player_ids:
        return []

    predictions = []
    for player_id in player_ids:
        profile = modeling_engine.build_player_profile(player_id)
        if not profile:
            continue

        churn_estimate, churn_details = await _estimate_churn_with_mode(
            modeling_engine=modeling_engine,
            player_id=player_id,
            profile=profile,
            prediction_mode=prediction_mode,
        )
        churn_risk = churn_estimate.get("churn_risk", "N/A") if churn_estimate else "N/A"
        recency_risk = min(max(float(profile.get("days_since_last_seen", 0) or 0) * 3.0, 0.0), 100.0)
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

        if policy_eval.get("route") == "NO_LLM":
            next_action = {
                "player_id": player_id,
                "decision": "NO_ACTION",
                "reason": policy_eval.get("reason", "Policy routed to NO_LLM."),
            }
        else:
            next_action = decision_engine.decide_next_action(profile, churn_estimate, "reduce_churn")

        churn_reason = churn_estimate.get("reason", "N/A") if churn_estimate else "N/A"
        predictions.append({
            "user_id": player_id,
            "ltv": profile.get("total_revenue", "N/A"),
            "session_count": profile.get("total_sessions", "N/A"),
            "event_count": profile.get("total_events", "N/A"),
            "predicted_churn_risk": churn_risk,
            "churn_reason": churn_reason,
            "suggested_action": next_action.get("content", "No action suggested.") if next_action else "No action suggested.",
            "llm_route": policy_eval.get("route"),
            "policy_reason": policy_eval.get("reason"),
            "prediction_mode": prediction_mode,
            "prediction_details": churn_details,
        })

    if predictions:
        print(f"Saving churn predictions to cache: {prediction_cache_file}")
        with open(prediction_cache_file, 'w') as f:
            json.dump(predictions, f, indent=2)

    return predictions


async def _run_prediction_job(prediction_job_id: str, job_name: str, force_recalculate: bool):
    prediction_job = _get_prediction_job(prediction_job_id)
    if not prediction_job:
        return
    try:
        prediction_mode = prediction_job.get("prediction_mode", "local")
        predictions = await _compute_predictions_for_job(job_name, force_recalculate, prediction_mode)
        prediction_job["status"] = "Ready"
        prediction_job["result_count"] = len(predictions)
        prediction_job["error"] = None
    except Exception as e:
        prediction_job["status"] = "Failed"
        prediction_job["error"] = str(e)
    finally:
        save_prediction_jobs_to_cache()


@app.post("/predict-churn-for-import-async")
async def predict_churn_for_import_async(request: ChurnPredictionRequest, background_tasks: BackgroundTasks):
    """
    Starts churn prediction in the background and returns a prediction job id.
    """
    prediction_job = _create_prediction_job(request.job_name, request.force_recalculate)
    prediction_job["prediction_mode"] = request.prediction_mode or "local"
    save_prediction_jobs_to_cache()
    background_tasks.add_task(_run_prediction_job, prediction_job["id"], request.job_name, request.force_recalculate)
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
        gemini_client = GeminiClient()
        modeling_engine = PlayerModelingEngine(gemini_client=gemini_client, bigquery_service=BIGQUERY_SERVICE_INSTANCE)
        decision_engine = GrowthDecisionEngine(gemini_client)
        executor = EngagementExecutor()
        feedback_service = EngagementFeedback()

        player_id = request.player_id

        # 3. Build profile and estimate churn
        player_profile = modeling_engine.build_player_profile(player_id)
        if not player_profile:
            raise HTTPException(status_code=404, detail=f"Player with ID '{player_id}' not found.")

        churn_estimate, churn_details = await _estimate_churn_with_mode(
            modeling_engine=modeling_engine,
            player_id=player_id,
            profile=player_profile,
            prediction_mode=request.prediction_mode or "local",
        )

        churn_risk = churn_estimate.get("churn_risk", "N/A") if churn_estimate else "N/A"
        recency_risk = min(max(float(player_profile.get("days_since_last_seen", 0) or 0) * 3.0, 0.0), 100.0)
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
        if policy_eval.get("route") == "NO_LLM":
            next_action = {
                "player_id": player_id,
                "decision": "NO_ACTION",
                "reason": policy_eval.get("reason", "Policy routed to NO_LLM."),
            }
        else:
            next_action = decision_engine.decide_next_action(player_profile, churn_estimate, "reduce_churn")
        action_id = executor.execute_action(next_action)

        # 5. Simulate and record feedback
        feedback = None
        if action_id:
            feedback = feedback_service.get_engagement_result(player_id, action_id)

        return {
            "player_profile": player_profile,
            "churn_estimate": churn_estimate,
            "churn_prediction_details": churn_details,
            "policy_eval": policy_eval,
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
