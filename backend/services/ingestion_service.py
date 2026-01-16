# ingestion_service.py

import json
from typing import List, Dict, Any
from datetime import datetime
from gcs_service import GcsService
from amplitude_service import AmplitudeService

class IngestionService:
    """
    Simulates a data ingestion service that fetches data from a source
    and publishes it to a message queue like Google Cloud Pub/Sub.
    """

    def __init__(self, gcs_service: GcsService):
        """
        In a real application, this would initialize a Pub/Sub client.
        """
        self.amplitude_client = AmplitudeService()
        self.gcs_service = gcs_service
        # In a real scenario, this would be a Pub/Sub topic of messages.
        self.message_queue_topic = [] 
        print("IngestionService initialized (simulating Pub/Sub publisher).")

    def fetch_and_publish_events(self, start_date: str, end_date: str) -> int:
        """
        Fetches events from Amplitude and publishes them to the simulated queue.

        Args:
            start_date: The start date for the data fetch.
            end_date: The end date for the data fetch.

        Returns:
            The number of events published.
        """
        print(f"Fetching events from Amplitude for {start_date} to {end_date}...")
        raw_events = self.amplitude_client.export_events(start_date, end_date)

        if raw_events:
            # 1. Save raw data to Google Cloud Storage
            timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
            blob_name = f"raw_events/{start_date}_to_{end_date}/{timestamp}.json"
            gcs_path = self.gcs_service.upload_raw_events(raw_events, blob_name)

            # 2. Publish a notification with the GCS path to the message queue
            notification = {"gcs_path": gcs_path, "event_count": len(raw_events)}
            self.message_queue_topic.append(json.dumps(notification))
            print(f"Published notification for {len(raw_events)} events to the message queue.")
        
        return len(raw_events)