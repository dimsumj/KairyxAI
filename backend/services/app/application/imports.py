from __future__ import annotations

import json
import uuid
from typing import Any, Dict, List

from app.domain.jobs import CheckpointStatus, JobStatus
from dataflow.pipeline import DataflowNormalizationRunner
from gcs_service import GcsService
from ingestion_service import IngestionService
from pubsub_service import PubSubService
from bigquery_service import BigQueryService


class ImportService:
    def __init__(self, repository, settings):
        self.repository = repository
        self.settings = settings

    def create_job(self, source_name: str, start_date: str, end_date: str, page_size: int | None = None) -> Dict[str, Any]:
        connector = self.repository.get_connector(source_name)
        if connector is None:
            raise KeyError(source_name)
        job = self.repository.create_import_job(
            {
                "id": f"imp_{uuid.uuid4().hex[:20]}",
                "source_name": source_name,
                "status": JobStatus.QUEUED.value,
                "spec": {
                    "source_name": source_name,
                    "connector_type": connector["type"],
                    "start_date": start_date,
                    "end_date": end_date,
                    "page_size": int(page_size or self.settings.worker_page_size),
                },
                "progress": {"current": 0, "total": 0, "pct": 0.0, "details": {}},
            }
        )
        self.repository.record_action("import_job_created", "import_job", job["id"], job)
        PubSubService(topic_name=self.settings.import_command_topic).publish({"job_id": job["id"]}, attributes={"job_type": "import"})
        return job

    def list_jobs(self) -> List[Dict[str, Any]]:
        return self.repository.list_import_jobs()

    def get_job(self, job_id: str) -> Dict[str, Any] | None:
        return self.repository.get_import_job(job_id)

    def run_job(self, job_id: str) -> Dict[str, Any]:
        job = self.repository.get_import_job(job_id)
        if job is None:
            raise KeyError(job_id)
        connector_record = self.repository.get_connector(job["spec"]["source_name"])
        if connector_record is None:
            raise KeyError(job["spec"]["source_name"])

        self.repository.update_import_job(job_id, {"status": JobStatus.RUNNING.value})
        gcs_service = GcsService()
        raw_pubsub = PubSubService(topic_name=self.settings.raw_shard_topic)
        ingestion_service = IngestionService(
            gcs_service=gcs_service,
            connector_config=connector_record["config"],
            connector_type=connector_record["type"],
            source_config_id=connector_record["name"],
            pubsub_service=raw_pubsub,
        )

        staged = ingestion_service.fetch_and_stage_events(
            job["spec"]["start_date"],
            job["spec"]["end_date"],
            job_id=job_id,
        )

        manifests: List[Dict[str, Any]] = []
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
            message_id = raw_pubsub.publish(
                notification,
                attributes={
                    "job_id": manifest["job_id"],
                    "source": manifest["source"],
                    "shard_index": manifest["shard_index"],
                    "schema_version": manifest["schema_version"],
                },
            )
            checkpoint_payload = {
                "job_id": job_id,
                "shard_index": manifest["shard_index"],
                "source_name": connector_record["name"],
                "status": CheckpointStatus.PUBLISHED.value,
                "cursor": str(manifest["shard_index"]),
                "gcs_uri": manifest["gcs_uri"],
                "message_id": message_id,
                "manifest": manifest,
            }
            self.repository.upsert_checkpoint(checkpoint_payload)
            manifests.append(notification)

        processing_stats: Dict[str, Any] = {}
        if self.settings.data_backend_mode == "mock":
            runner = DataflowNormalizationRunner(gcs_service=gcs_service, bigquery_service=BigQueryService())
            processing_stats = runner.process_notifications(manifests)

        completed = self.repository.update_import_job(
            job_id,
            {
                "status": JobStatus.COMPLETED.value,
                "progress": {
                    "current": int(staged["events_staged"]),
                    "total": int(staged["events_staged"]),
                    "pct": 100.0,
                    "details": {
                        "source": connector_record["name"],
                        "connector_type": connector_record["type"],
                        "events_staged": staged["events_staged"],
                        "shards_created": staged["shards_created"],
                        "processing": processing_stats,
                    },
                },
            },
        )
        self.repository.record_action("import_job_completed", "import_job", job_id, completed)
        return completed
