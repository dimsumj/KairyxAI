# main_service.py

import os
import re
import uvicorn
import shutil
import requests
import zipfile
import io
import gzip
import json
from typing import List, Dict, Any
from datetime import datetime
from fastapi import FastAPI, HTTPException, Request
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
from engagement_feedback import EngagementFeedback
from pydantic import BaseModel, Field

KEYS_CACHE_FILE = ".api_keys_cache.json"

# In-memory cache for normalization maps. In a production scenario,
# this would be replaced by a persistent database (e.g., Redis, PostgreSQL).
NORMALIZATION_MAPS = {
    "event_name_map": {},
    "property_key_map": {}
}
NORMALIZATION_CACHE_FILE = ".normalization_maps.json"
CACHE_DIR = ".cache"

def clear_cache_on_startup():
    """Clears the data cache directory."""
    if os.path.exists(CACHE_DIR):
        print(f"Clearing cache directory: {CACHE_DIR}")
        shutil.rmtree(CACHE_DIR)
    print(f"Creating cache directory: {CACHE_DIR}")
    os.makedirs(CACHE_DIR, exist_ok=True)
    

def load_keys_from_cache():
    """Load API keys from the cache file into environment variables if the file exists."""
    if os.path.exists(KEYS_CACHE_FILE):
        print(f"Loading API keys from cache file: {KEYS_CACHE_FILE}")
        with open(KEYS_CACHE_FILE, 'r') as f:
            try:
                keys = json.load(f)
                for key, value in keys.items():
                    if value:
                        os.environ[key] = value
                print("API keys loaded from cache.")
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

def save_keys_to_cache(new_keys: dict):
    """Save API keys to the cache file."""
    keys = {}
    if os.path.exists(KEYS_CACHE_FILE):
        with open(KEYS_CACHE_FILE, 'r') as f:
            try:
                keys = json.load(f)
            except json.JSONDecodeError:
                pass  # Overwrite if corrupt
    keys.update(new_keys)
    with open(KEYS_CACHE_FILE, 'w') as f:
        json.dump(keys, f, indent=2)

def save_maps_to_cache():
    """Save normalization maps to the cache file."""
    with open(NORMALIZATION_CACHE_FILE, 'w') as f:
        json.dump(NORMALIZATION_MAPS, f, indent=2)

# Load any cached API keys on application startup
# clear_cache_on_startup()
load_keys_from_cache()

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
    model_name: str | None = None

class AdjustApiKey(BaseModel):
    """Request model for setting the Adjust API token."""
    adjust_api_token: str = Field(..., alias='api_token')

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
    ai_token_limit: int | None = None
    ai_budget_limit: float | None = None

class ChurnReportResponse(BaseModel):
    """Response model for the churn report generation."""
    message: str
    report_path: str

class PlayerAnalysisRequest(BaseModel):
    """Request model for analyzing a single player."""
    player_id: str
    start_date: str | None = None
    end_date: str | None = None

class DataSandboxRequest(BaseModel):
    """Request model for the data sandbox."""
    raw_name: str
    normalized_name: str


class AmplitudeService:
    """
    A service to connect to the Amplitude API and fetch event data.
    """
    API_URL = "https://amplitude.com/api/2/export"


    def __init__(self):
        """
        Initializes the AmplitudeService, retrieving API keys from environment variables.
        """
        self.api_key = os.getenv("AMPLITUDE_API_KEY")
        self.secret_key = os.getenv("AMPLITUDE_SECRET_KEY")

        if not self.api_key or not self.secret_key:
            raise ValueError(
                "AMPLITUDE_API_KEY and AMPLITUDE_SECRET_KEY environment variables must be set."
            )

    def export_events(self, start_date: str, end_date: str) -> list[dict]:
        """
        Fetches event data from Amplitude for a given date range.

        Args:
            start_date: The start date in 'YYYYMMDD' format (e.g., '20240101').
            end_date: The end date in 'YYYYMMDD' format (e.g., '20240131').

        Returns:
            A list of event data dictionaries.
        """
        print(f"Requesting data export from {start_date} to {end_date}...")

        # The API expects dates in YYYYMMDDTHH format, we'll use midnight.
        params = {
            "start": f"{start_date}T00",
            "end": f"{end_date}T23",
        }

        try:
            response = requests.get(
                self.API_URL,
                params=params,
                auth=(self.api_key, self.secret_key),
                stream=True
            )
            response.raise_for_status()  # Raises an HTTPError for bad responses (4xx or 5xx)

            print("Export successful. Unzipping and processing data...")

            # The data is returned as a zip archive in memory
            with zipfile.ZipFile(io.BytesIO(response.content)) as z:
                # The archive contains one or more gzipped JSON line files.
                # We can process them more concisely with a list comprehension.
                all_events = [json.loads(line) for filename in z.namelist() for line in gzip.open(z.open(filename), 'rt')]
            
            print(f"Successfully processed {len(all_events)} events.")
            return all_events

        except requests.exceptions.HTTPError as http_err:
            print(f"HTTP error occurred: {http_err}")
            print(f"Response content: {response.text}")
            raise
        except Exception as err:
            print(f"An other error occurred: {err}")
            raise

# Serve the frontend application
# This assumes the 'frontend' directory is two levels up from this script's location.
frontend_dir = os.path.join(os.path.dirname(__file__), '..', '..', 'frontend')
app.mount("/static", StaticFiles(directory=frontend_dir), name="static")

@app.get("/")
async def serve_index():
    """Serves the main index.html file from the frontend directory."""
    return FileResponse(os.path.join(frontend_dir, 'index.html'))


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
    save_keys_to_cache({
        "AMPLITUDE_API_KEY": keys.amplitude_api_key,
        "AMPLITUDE_SECRET_KEY": keys.amplitude_secret_key
    })
    return {"message": "Amplitude API keys have been configured and cached."}

@app.post("/configure-google-key")
async def configure_google_key(key: GoogleApiKey):
    """
    An API endpoint to set the Google API key for the session.
    **Security Warning:** This is insecure for production. API keys should be
    set as environment variables on the server.
    """
    os.environ["GOOGLE_API_KEY"] = key.google_api_key
    keys_to_cache = {"GOOGLE_API_KEY": key.google_api_key}
    if key.model_name:
        os.environ["GOOGLE_GEMINI_MODEL"] = key.model_name
        keys_to_cache["GOOGLE_GEMINI_MODEL"] = key.model_name
    save_keys_to_cache(keys_to_cache)
    return {"message": "Google API settings have been configured and cached."}

@app.post("/configure-adjust-credentials")
async def configure_adjust_credentials(key: AdjustApiKey):
    """
    An API endpoint to set the Adjust API token for the session.
    **Security Warning:** This is insecure for production. API keys should be
    set as environment variables on the server.
    """
    os.environ["ADJUST_API_TOKEN"] = key.adjust_api_token
    save_keys_to_cache({"ADJUST_API_TOKEN": key.adjust_api_token})
    return {"message": "Adjust API token has been configured and cached."}

@app.post("/configure-safety-rails")
async def configure_safety_rails(request: SafetyRailsRequest):
    """
    An API endpoint to set safety limits for AI usage.
    This is a placeholder and doesn't enforce limits yet.
    """
    return {"message": "Safety rails configured.", "settings": request.dict()}

@app.post("/generate-churn-report", response_model=ChurnReportResponse)
async def generate_churn_report(request: ChurnReportRequest):
    """
    An API endpoint to generate a churn prediction report.
    """
    try:
        amplitude_client = AmplitudeService()

        # Fetch the events
        events_data = amplitude_client.export_events(request.start_date, request.end_date)

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

        # 4. Initialize the modeling engine with the clean data and the AI client
        modeling_engine = PlayerModelingEngine(normalized_data, gemini_client)

        # 5. Get a list of all players
        player_ids = modeling_engine.get_all_player_ids()

        # 6. Initialize the decision engine
        decision_engine = GrowthDecisionEngine(gemini_client)

        # 7. Initialize the churn reporter
        reporter = ChurnReporter(modeling_engine, decision_engine) # 8. Generate the final CSV report for all at-risk players
        report_filename = f"{datetime.utcnow().strftime('%Y%m%d-%H%M%S')}.csv"

        # 8. Generate the final CSV report for all at-risk players
        report_filename = 'churn_predictions_report.csv'
        if player_ids:
            reporter.generate_report(player_ids, report_filename)
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
        amplitude_client = AmplitudeService()
        events_data = amplitude_client.export_events(request.start_date, request.end_date)
        
        # Check for cached data first
        cache_filename = os.path.join(CACHE_DIR, f"{request.start_date}_{request.end_date}.json")
        if os.path.exists(cache_filename):
            print(f"Loading events from cache file: {cache_filename}")
            with open(cache_filename, 'r') as f:
                events_data = json.load(f)
        else:
            # Fetch from Amplitude if not cached
            events_data = amplitude_client.export_events(request.start_date, request.end_date)
            # Save to cache
            if events_data:
                print(f"Saving {len(events_data)} events to cache file: {cache_filename}")
                with open(cache_filename, 'w') as f:
                    json.dump(events_data, f)

        
        if not events_data:
            raise HTTPException(status_code=404, detail="No events found for the given date range.")

        normalizer = EventSemanticNormalizer(event_name_map=NORMALIZATION_MAPS["event_name_map"], property_key_map=NORMALIZATION_MAPS["property_key_map"])
        normalized_data = normalizer.normalize_events(events_data)

        gemini_client = GeminiClient()
        modeling_engine = PlayerModelingEngine(normalized_data, gemini_client)
        cohort_service = PlayerCohortService(modeling_engine)

        cohorts = await cohort_service.create_player_cohorts()

        return {"message": "Cohorts created successfully", "cohorts": cohorts}
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


@app.post("/analyze-and-engage-player")
async def analyze_and_engage_player(request: PlayerAnalysisRequest):
    """
    Analyzes a single player for churn risk, executes an engagement action if needed,
    and simulates feedback. This creates a full, closed-loop process.
    """
    try:
        # For a single player analysis, we might fetch data over a longer period
        # to build a more robust profile.
        # Use provided dates or default to a wide range.
        start_date = request.start_date if request.start_date else "20240101"
        end_date = request.end_date if request.end_date else datetime.utcnow().strftime('%Y%m%d')

        if not request.player_id:
            raise ValueError("Player ID cannot be empty.")

        amplitude_client = AmplitudeService()
        events_data = amplitude_client.export_events(start_date, end_date)

        if not events_data:
            raise HTTPException(status_code=404, detail="No events found for the given date range.")

        # 1. Normalize events
        normalizer = EventSemanticNormalizer(event_name_map=NORMALIZATION_MAPS["event_name_map"], property_key_map=NORMALIZATION_MAPS["property_key_map"])
        normalized_data = normalizer.normalize_events(events_data)

        # 2. Initialize clients and engines
        gemini_client = GeminiClient()
        modeling_engine = PlayerModelingEngine(normalized_data, gemini_client)
        decision_engine = GrowthDecisionEngine(gemini_client)
        executor = EngagementExecutor()
        feedback_service = EngagementFeedback()

        player_id = request.player_id

        # 3. Build profile and estimate churn
        player_profile = modeling_engine.build_player_profile(player_id)
        if not player_profile:
            raise HTTPException(status_code=404, detail=f"Player with ID '{player_id}' not found.")

        churn_estimate = await modeling_engine.estimate_churn_risk(player_id)

        # 4. Decide and execute next best action
        next_action = decision_engine.decide_next_action(player_profile, churn_estimate, "reduce_churn")
        action_id = executor.execute_action(next_action)

        # 5. Simulate and record feedback
        feedback = None
        if action_id:
            feedback = feedback_service.get_engagement_result(player_id, action_id)

        return {"player_profile": player_profile, "churn_estimate": churn_estimate, "action_taken": next_action, "feedback": feedback}

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred: {str(e)}")

if __name__ == '__main__':
    # To run this API server:
    # 1. Make sure you have fastapi and uvicorn installed:
    #    pip install fastapi "uvicorn[standard]"
    # 2. Set your environment variables: AMPLITUDE_API_KEY, AMPLITUDE_SECRET_KEY, GOOGLE_API_KEY
    # 3. Run the server:
    #    uvicorn main_service:app --reload
    
    print("Starting API server. Run with: uvicorn main_service:app --reload")
    uvicorn.run(app, host="0.0.0.0", port=8000)