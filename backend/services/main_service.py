# main_service.py

import os
import uvicorn
import requests
import zipfile
import io
import gzip
import json
from datetime import datetime
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from starlette.responses import FileResponse
from event_semantic_normalizer import EventSemanticNormalizer
from player_modeling_engine import PlayerModelingEngine
from growth_decision_engine import GrowthDecisionEngine
from gemini_client import GeminiClient
from engagement_executor import EngagementExecutor
from churn_reporter import ChurnReporter
from engagement_feedback import EngagementFeedback

app = FastAPI()

class AmplitudeApiKeys(BaseModel):
    """Request model for setting Amplitude API keys."""
    amplitude_api_key: str
    amplitude_secret_key: str

class GoogleApiKey(BaseModel):
    """Request model for setting Google API key."""
    google_api_key: str

class AdjustApiKey(BaseModel):
    """Request model for setting the Adjust API token."""
    adjust_api_token: str

class ChurnReportRequest(BaseModel):
    """Request model for generating a churn report."""
    start_date: str
    end_date: str

class ChurnReportResponse(BaseModel):
    """Response model for the churn report generation."""
    message: str
    report_path: str
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
@app.post("/configure-amplitude-keys")
async def configure_amplitude_keys(keys: AmplitudeApiKeys):
    """
    An API endpoint to set the Amplitude API keys for the session.
    **Security Warning:** This is insecure for production. API keys should be
    set as environment variables on the server.
    """
    os.environ["AMPLITUDE_API_KEY"] = keys.amplitude_api_key
    os.environ["AMPLITUDE_SECRET_KEY"] = keys.amplitude_secret_key
    return {"message": "Amplitude API keys have been configured for this session."}

@app.post("/configure-google-key")
async def configure_google_key(key: GoogleApiKey):
    """
    An API endpoint to set the Google API key for the session.
    **Security Warning:** This is insecure for production. API keys should be
    set as environment variables on the server.
    """
    os.environ["GOOGLE_API_KEY"] = key.google_api_key
    return {"message": "Google API key has been configured for this session."}

@app.post("/configure-adjust-credentials")
async def configure_adjust_credentials(key: AdjustApiKey):
    """
    An API endpoint to set the Adjust API token for the session.
    **Security Warning:** This is insecure for production. API keys should be
    set as environment variables on the server.
    """
    os.environ["ADJUST_API_TOKEN"] = key.adjust_api_token
    return {"message": "Adjust API token has been configured for this session."}


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
        reporter = ChurnReporter(modeling_engine, decision_engine)

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

if __name__ == '__main__':
    # To run this API server:
    # 1. Make sure you have fastapi and uvicorn installed:
    #    pip install fastapi "uvicorn[standard]"
    # 2. Set your environment variables: AMPLITUDE_API_KEY, AMPLITUDE_SECRET_KEY, GOOGLE_API_KEY
    # 3. Run the server:
    #    uvicorn main_service:app --reload
    print("Starting API server. Run with: uvicorn main_service:app --reload")
    uvicorn.run(app, host="0.0.0.0", port=8000)