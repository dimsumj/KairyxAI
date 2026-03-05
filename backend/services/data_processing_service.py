# data_processing_service.py

import json
from typing import List, Dict, Any
from event_semantic_normalizer import EventSemanticNormalizer
from bigquery_service import BigQueryService
from gcs_service import GcsService
from ingestion_service import IngestionService
from local_job_store import resolve_or_create_canonical_user_id

class DataProcessingService:
    """
    Simulates a stream/batch data processing pipeline (e.g., Google Cloud Dataflow).
    This service consumes from a message queue, processes the data, and writes it
    to a data warehouse.
    """

    def __init__(self, bigquery_service: BigQueryService, gcs_service: GcsService, job_identifier: str):
        """
        Initializes the processing service.

        Args:
            bigquery_service: The service for writing to our data warehouse.
            gcs_service: The service for reading from our data lake (GCS).
            job_identifier: The identifier for the current job (e.g., 'YYYYMMDD_to_YYYYMMDD').
        """
        # In a real system, these maps would come from a persistent config store.
        self.normalizer = EventSemanticNormalizer(event_name_map={}, property_key_map={})
        self.gcs_service = gcs_service
        self.bigquery_service = bigquery_service
        self.job_identifier = job_identifier
        print("DataProcessingService initialized (simulating Dataflow).")

    def run_processing_pipeline(self, ingestion_service: IngestionService):
        """
        Simulates the execution of the processing pipeline.

        Returns processing stats including dedupe effect.
        """
        print("Starting data processing pipeline...")
        notifications = [json.loads(msg) for msg in ingestion_service.message_queue_topic]

        all_normalized: List[Dict[str, Any]] = []

        for notification in notifications:
            gcs_path = notification.get("gcs_path")
            if not gcs_path:
                continue

            blob_name = gcs_path.replace(f"gs://{self.gcs_service.bucket_name}/", "")
            raw_events = self.gcs_service.download_raw_events(blob_name)
            normalized_events = self.normalizer.normalize_events(raw_events)
            all_normalized.extend(normalized_events)

        dedupe_map: Dict[tuple, Dict[str, Any]] = {}
        for e in all_normalized:
            source = str(e.get("source", "unknown"))
            player_id = str(e.get("player_id", "unknown_user"))
            canonical_user_id = resolve_or_create_canonical_user_id(source, player_id)
            e["canonical_user_id"] = canonical_user_id

            source_event_id = e.get("source_event_id")
            key = (
                "srcid",
                str(source),
                str(source_event_id),
            ) if source_event_id else (
                "fallback",
                str(canonical_user_id),
                str(e.get("event_type")),
                str(e.get("event_time")),
                str(source),
            )
            dedupe_map[key] = e

        deduped_events = list(dedupe_map.values())
        self.bigquery_service.write_processed_events(deduped_events, self.job_identifier)

        flag_counts: Dict[str, int] = {}
        rows_with_flags = 0
        for e in deduped_events:
            flags = e.get("data_quality_flags") or []
            if flags:
                rows_with_flags += 1
            for f in flags:
                flag_counts[f] = flag_counts.get(f, 0) + 1

        stats = {
            "raw_normalized_events": len(all_normalized),
            "deduped_events": len(deduped_events),
            "duplicates_removed": max(0, len(all_normalized) - len(deduped_events)),
            "quality": {
                "rows_with_flags": rows_with_flags,
                "rows_clean": max(0, len(deduped_events) - rows_with_flags),
                "flag_counts": flag_counts,
            },
        }
        print(f"Data processing pipeline finished. Stats: {stats}")
        return stats