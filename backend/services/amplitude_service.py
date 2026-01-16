# amplitude_service.py

import os
import requests
import zipfile
import io
import gzip
import json

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