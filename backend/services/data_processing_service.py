# data_processing_service.py

import json
from typing import List
from event_semantic_normalizer import EventSemanticNormalizer
from bigquery_service import BigQueryService
from gcs_service import GcsService
from ingestion_service import IngestionService

class DataProcessingService:
    """
    Simulates a stream/batch data processing pipeline (e.g., Google Cloud Dataflow).
    This service consumes from a message queue, processes the data, and writes it
    to a data warehouse.
    """

    def __init__(self, bigquery_service: BigQueryService, gcs_service: GcsService):
        """
        Initializes the processing service.

        Args:
            bigquery_service: The service for writing to our data warehouse.
            gcs_service: The service for reading from our data lake (GCS).
        """
        # In a real system, these maps would come from a persistent config store.
        self.normalizer = EventSemanticNormalizer(event_name_map={}, property_key_map={})
        self.gcs_service = gcs_service
        self.bigquery_service = bigquery_service
        print("DataProcessingService initialized (simulating Dataflow).")

    def run_processing_pipeline(self, ingestion_service: IngestionService):
        """
        Simulates the execution of the processing pipeline.

        Args:
            ingestion_service: The service holding the raw data in its queue.
        """
        # In a real system, this would be a continuously running Dataflow job
        # listening to a Pub/Sub subscription.
        print("Starting data processing pipeline...")
        notifications = [json.loads(msg) for msg in ingestion_service.message_queue_topic]

        for notification in notifications:
            gcs_path = notification.get("gcs_path")
            if not gcs_path:
                continue
            
            # 1. Download raw data from GCS
            blob_name = gcs_path.replace(f"gs://{self.gcs_service.bucket_name}/", "")
            raw_events = self.gcs_service.download_raw_events(blob_name)
            
            # 2. Normalize and 3. Write to BigQuery
            normalized_events = self.normalizer.normalize_events(raw_events)
            self.bigquery_service.write_processed_events(normalized_events)
        print("Data processing pipeline finished.")