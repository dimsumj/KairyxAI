# ingestion_service.py

import json
import os
import time
from pathlib import Path
from typing import Dict, Any, Optional, Iterable, List
from datetime import datetime

from gcs_service import GcsService
from connectors import create_connector
from pipeline_models import ShardManifest
from pubsub_service import PubSubService
from local_job_store import save_ingestion_checkpoint


class IngestionService:
    """
    Simulates a data ingestion service that fetches data from a source
    and publishes it to a message queue like Google Cloud Pub/Sub.

    Reliability additions (P0):
    - retry with backoff for external fetch
    - dead-letter record for failed fetches
    """

    def __init__(
        self,
        gcs_service: GcsService,
        connector_config: Dict[str, Any],
        connector_type: str,
        source_config_id: Optional[str] = None,
        pubsub_service: Optional[PubSubService] = None,
    ):
        self.gcs_service = gcs_service
        self.connector_type = connector_type
        self.connector = create_connector(connector_type, connector_config)
        self.source_config_id = source_config_id or connector_type
        self.pubsub_service = pubsub_service or PubSubService()

        self.message_queue_topic = []
        self.staged_shards = []
        self.published_message_ids = []
        self.local_shard_event_count = max(1, int(os.getenv("INGEST_LOCAL_SHARD_EVENT_COUNT", "5000")))
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

    def _iter_event_pages(self, start_date: str, end_date: str) -> Iterable[List[Dict[str, Any]]]:
        page_size = self.local_shard_event_count
        if hasattr(self.connector, "iter_event_pages"):
            try:
                yielded = False
                for page in self.connector.iter_event_pages(start_date, end_date, page_size=page_size):
                    yielded = True
                    if page:
                        yield page
                if yielded:
                    return
            except TypeError:
                pass

        if hasattr(self.connector, "fetch_events_page"):
            cursor = None
            while True:
                page = self.connector.fetch_events_page(
                    start_date,
                    end_date,
                    cursor=cursor,
                    page_size=page_size,
                )
                events = page.get("events") or []
                if not events:
                    break
                yield events
                if not page.get("has_more"):
                    break
                cursor = page.get("next_cursor")
            return

        raw_events = self._fetch_events_with_retry(start_date, end_date)
        for chunk in self._chunk_events(raw_events):
            yield chunk

    def _chunk_events(self, raw_events):
        if self.gcs_service.mode != "mock":
            return [raw_events]

        return [
            raw_events[index:index + self.local_shard_event_count]
            for index in range(0, len(raw_events), self.local_shard_event_count)
        ]

    def _save_checkpoint(
        self,
        manifest: Dict[str, Any],
        publish_status: str,
        published_message_id: Optional[str] = None,
    ) -> None:
        checkpoint = dict(manifest)
        checkpoint["publish_status"] = publish_status
        checkpoint["published_message_id"] = published_message_id
        checkpoint["checkpointed_at"] = datetime.utcnow().isoformat()
        save_ingestion_checkpoint(
            checkpoint["job_id"],
            checkpoint["source"],
            checkpoint["shard_index"],
            checkpoint,
        )

    def fetch_and_stage_events(self, start_date: str, end_date: str, job_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Fetches events from the connector, writes them to raw storage, and returns
        shard metadata without publishing queue notifications.
        """
        print(f"Fetching events from {self.connector_type} for {start_date} to {end_date}...")
        timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
        resolved_job_id = job_id or f"{self.connector_type}_{start_date}_{end_date}_{timestamp}"
        shard_manifests = []
        total_events = 0

        for index, shard_events in enumerate(self._iter_event_pages(start_date, end_date), start=1):
            total_events += len(shard_events)
            blob_name = (
                f"raw/source={self.connector_type}/job={resolved_job_id}/"
                f"part-{index:05d}.jsonl"
            )
            gcs_path = self.gcs_service.upload_raw_events(shard_events, blob_name)
            manifest = ShardManifest(
                job_id=resolved_job_id,
                source=self.connector_type,
                gcs_uri=gcs_path,
                event_count=len(shard_events),
                start_date=start_date,
                end_date=end_date,
                shard_index=index,
                source_config_id=self.source_config_id,
            )
            manifest_dict = manifest.to_dict()
            self.staged_shards.append(manifest_dict)
            shard_manifests.append(manifest_dict)
            self._save_checkpoint(manifest_dict, publish_status="staged")

        if shard_manifests:
            return {
                "job_id": resolved_job_id,
                "source": self.connector_type,
                "shards_created": len(shard_manifests),
                "events_staged": total_events,
                "last_checkpoint": {
                    "gcs_uri": shard_manifests[-1]["gcs_uri"],
                    "event_count": shard_manifests[-1]["event_count"],
                    "start_date": start_date,
                    "end_date": end_date,
                },
                "shard_manifests": shard_manifests,
            }

        return {
            "job_id": resolved_job_id,
            "source": self.connector_type,
            "shards_created": 0,
            "events_staged": 0,
            "last_checkpoint": None,
            "shard_manifests": [],
        }

    def fetch_and_publish_events(self, start_date: str, end_date: str, job_id: Optional[str] = None) -> int:
        """
        Fetches events from connector and publishes notifications to the simulated queue.
        """
        staged = self.fetch_and_stage_events(start_date, end_date, job_id=job_id)
        for manifest in staged["shard_manifests"]:
            notification = {
                "gcs_path": manifest["gcs_uri"],
                "event_count": manifest["event_count"],
                "source": manifest["source"],
                "job_id": manifest["job_id"],
                "schema_version": manifest["schema_version"],
                "shard_index": manifest["shard_index"],
                "source_config_id": manifest["source_config_id"],
            }
            self.message_queue_topic.append(json.dumps(notification))
            message_id = self.pubsub_service.publish(
                notification,
                attributes={
                    "job_id": manifest["job_id"],
                    "source": manifest["source"],
                    "shard_index": manifest["shard_index"],
                    "schema_version": manifest["schema_version"],
                },
            )
            self.published_message_ids.append(message_id)
            self._save_checkpoint(manifest, publish_status="published", published_message_id=message_id)
            print(f"Published notification for {manifest['event_count']} events to the message queue.")

        return int(staged["events_staged"])
