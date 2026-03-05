# ingestion_service.py

import json
import os
import time
from pathlib import Path
from typing import Dict, Any
from datetime import datetime

from gcs_service import GcsService
from connectors import create_connector


class IngestionService:
    """
    Simulates a data ingestion service that fetches data from a source
    and publishes it to a message queue like Google Cloud Pub/Sub.

    Reliability additions (P0):
    - retry with backoff for external fetch
    - dead-letter record for failed fetches
    """

    def __init__(self, gcs_service: GcsService, connector_config: Dict[str, Any], connector_type: str):
        self.gcs_service = gcs_service
        self.connector_type = connector_type
        self.connector = create_connector(connector_type, connector_config)

        self.message_queue_topic = []
        self.retry_max_attempts = int(os.getenv("INGEST_RETRY_MAX_ATTEMPTS", "3"))
        self.retry_backoff_sec = float(os.getenv("INGEST_RETRY_BACKOFF_SEC", "1.5"))
        self.dlq_path = Path(os.getenv("INGEST_DLQ_FILE", ".cache/ingest_dlq.jsonl"))
        self.dlq_path.parent.mkdir(parents=True, exist_ok=True)
        print(f"IngestionService initialized for connector={connector_type} (simulating Pub/Sub publisher).")

    def _record_dead_letter(self, start_date: str, end_date: str, error_text: str):
        record = {
            "timestamp": datetime.utcnow().isoformat(),
            "source": self.connector_type,
            "start_date": start_date,
            "end_date": end_date,
            "error": error_text,
        }
        with self.dlq_path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(record) + "\n")

    def _fetch_events_with_retry(self, start_date: str, end_date: str):
        last_error = None
        for attempt in range(1, self.retry_max_attempts + 1):
            try:
                print(f"[Ingestion:{self.connector_type}] Attempt {attempt}/{self.retry_max_attempts} for {start_date} -> {end_date}")
                return self.connector.fetch_events(start_date, end_date)
            except Exception as e:
                last_error = e
                if attempt < self.retry_max_attempts:
                    sleep_sec = self.retry_backoff_sec * (2 ** (attempt - 1))
                    print(f"[Ingestion:{self.connector_type}] attempt {attempt} failed: {e}. Retrying in {sleep_sec:.1f}s...")
                    time.sleep(sleep_sec)
                else:
                    print(f"[Ingestion:{self.connector_type}] final attempt failed: {e}")

        self._record_dead_letter(start_date, end_date, str(last_error))
        raise RuntimeError(f"Ingestion failed after retries: {last_error}")

    def fetch_and_publish_events(self, start_date: str, end_date: str) -> int:
        """
        Fetches events from connector and publishes notifications to the simulated queue.
        """
        print(f"Fetching events from {self.connector_type} for {start_date} to {end_date}...")
        raw_events = self._fetch_events_with_retry(start_date, end_date)

        if raw_events:
            timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
            blob_name = f"raw_events/{self.connector_type}/{start_date}_to_{end_date}/{timestamp}.json"
            gcs_path = self.gcs_service.upload_raw_events(raw_events, blob_name)

            notification = {"gcs_path": gcs_path, "event_count": len(raw_events), "source": self.connector_type}
            self.message_queue_topic.append(json.dumps(notification))
            print(f"Published notification for {len(raw_events)} events to the message queue.")

        return len(raw_events)
