from __future__ import annotations

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

    def _commit_session(self) -> None:
        session = getattr(self.repository, "session", None)
        if session is not None:
            try:
                session.commit()
            except Exception:
                session.rollback()
                raise

    def rollback_session(self) -> None:
        session = getattr(self.repository, "session", None)
        if session is not None:
            session.rollback()

    def _safe_get_import_job(self, job_id: str) -> Dict[str, Any] | None:
        try:
            return self.repository.get_import_job(job_id)
        except Exception:
            self.rollback_session()
            return None

    def _is_stop_requested(self, job_id: str) -> bool:
        job = self.repository.get_import_job(job_id)
        if job is None:
            return False
        return str(job.get("status") or "").lower() in {JobStatus.STOPPING.value, JobStatus.STOPPED.value}

    def _mark_stopped(self, job_id: str, reason: str = "Stopped by user.") -> Dict[str, Any]:
        job = self.repository.get_import_job(job_id)
        if job is None:
            raise KeyError(job_id)
        if str(job.get("status") or "").lower() == JobStatus.STOPPED.value:
            return job

        progress = job.get("progress") or {}
        details = dict(progress.get("details") or {})
        details.pop("stop_requested", None)
        details["stop_reason"] = reason
        stopped = self.repository.update_import_job(
            job_id,
            {
                "status": JobStatus.STOPPED.value,
                "error": None,
                "progress": {
                    "current": int(progress.get("current", 0) or 0),
                    "total": int(progress.get("total", 0) or 0),
                    "pct": float(progress.get("pct", 0.0) or 0.0),
                    "details": details,
                },
            },
        )
        self.repository.record_action("import_job_stopped", "import_job", job_id, stopped)
        self._commit_session()
        return stopped

    def _update_stage_progress(
        self,
        job_id: str,
        connector_record: Dict[str, Any],
        current_events: int,
        shards_created: int,
    ) -> Dict[str, Any]:
        current_job = self.repository.get_import_job(job_id)
        if current_job is None:
            raise KeyError(job_id)
        progress = current_job.get("progress") or {}
        details = dict(progress.get("details") or {})
        details.update(
            {
                "source": connector_record["name"],
                "connector_type": connector_record["type"],
                "events_staged": int(current_events),
                "shards_created": int(shards_created),
            }
        )
        updated = self.repository.update_import_job(
            job_id,
            {
                "progress": {
                    "current": int(current_events),
                    "total": max(int(progress.get("total", 0) or 0), int(current_events)),
                    "pct": float(progress.get("pct", 0.0) or 0.0),
                    "details": details,
                }
            },
        )
        self._commit_session()
        return updated

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

    def reconcile_jobs_after_restart(self) -> int:
        reconciled_count = 0
        for job in self.repository.list_import_jobs():
            status = str(job.get("status") or "").lower()
            if status != JobStatus.STOPPING.value:
                continue

            progress = job.get("progress") or {}
            details = dict(progress.get("details") or {})
            details.pop("stop_requested", None)
            details["stop_reason"] = details.get("stop_reason") or "Stopped after server restart."
            stopped = self.repository.update_import_job(
                job["id"],
                {
                    "status": JobStatus.STOPPED.value,
                    "error": None,
                    "progress": {
                        "current": int(progress.get("current", 0) or 0),
                        "total": int(progress.get("total", 0) or 0),
                        "pct": float(progress.get("pct", 0.0) or 0.0),
                        "details": details,
                    },
                },
            )
            self.repository.record_action("import_job_reconciled_after_restart", "import_job", job["id"], stopped)
            reconciled_count += 1

        if reconciled_count:
            self._commit_session()
        return reconciled_count

    def list_jobs(self) -> List[Dict[str, Any]]:
        return self.repository.list_import_jobs()

    def get_job(self, job_id: str) -> Dict[str, Any] | None:
        return self.repository.get_import_job(job_id)

    def stop_job(self, job_id: str) -> Dict[str, Any]:
        job = self.repository.get_import_job(job_id)
        if job is None:
            raise KeyError(job_id)

        status = str(job.get("status") or "").lower()
        if status == JobStatus.STOPPED.value:
            return job
        if status == JobStatus.QUEUED.value:
            return self._mark_stopped(job_id)
        if status == JobStatus.RUNNING.value:
            progress = job.get("progress") or {}
            details = dict(progress.get("details") or {})
            details["stop_requested"] = True
            stopping = self.repository.update_import_job(
                job_id,
                {
                    "status": JobStatus.STOPPING.value,
                    "progress": {
                        "current": int(progress.get("current", 0) or 0),
                        "total": int(progress.get("total", 0) or 0),
                        "pct": float(progress.get("pct", 0.0) or 0.0),
                        "details": details,
                    },
                },
            )
            self.repository.record_action("import_job_stop_requested", "import_job", job_id, stopping)
            self._commit_session()
            return stopping
        if status == JobStatus.STOPPING.value:
            return job
        raise ValueError("Only queued or running import jobs can be stopped.")

    def delete_job(self, job_id: str) -> None:
        job = self.repository.get_import_job(job_id)
        if job is None:
            raise KeyError(job_id)

        status = str(job.get("status") or "").lower()
        if status in {JobStatus.QUEUED.value, JobStatus.RUNNING.value, JobStatus.STOPPING.value}:
            raise ValueError("Stop the import before deleting it.")

        if self.repository.delete_import_job(job_id):
            self.repository.record_action("import_job_deleted", "import_job", job_id, job)
            self._commit_session()

    def run_job(self, job_id: str) -> Dict[str, Any]:
        job = self.repository.get_import_job(job_id)
        if job is None:
            raise KeyError(job_id)
        if str(job.get("status") or "").lower() == JobStatus.STOPPED.value:
            return job
        if str(job.get("status") or "").lower() == JobStatus.STOPPING.value:
            return self._mark_stopped(job_id)
        connector_record = self.repository.get_connector(job["spec"]["source_name"])
        if connector_record is None:
            raise KeyError(job["spec"]["source_name"])

        latest_job = self.repository.get_import_job(job_id)
        if latest_job is None:
            raise KeyError(job_id)
        if str(latest_job.get("status") or "").lower() == JobStatus.STOPPED.value:
            return latest_job
        if str(latest_job.get("status") or "").lower() == JobStatus.STOPPING.value:
            return self._mark_stopped(job_id)

        self.repository.update_import_job(job_id, {"status": JobStatus.RUNNING.value})
        self._commit_session()
        try:
            page_size = int(job["spec"].get("page_size") or self.settings.worker_page_size)
            gcs_service = GcsService()
            raw_pubsub = PubSubService(topic_name=self.settings.raw_shard_topic)
            ingestion_service = IngestionService(
                gcs_service=gcs_service,
                connector_config=connector_record["config"],
                connector_type=connector_record["type"],
                source_config_id=connector_record["name"],
                pubsub_service=raw_pubsub,
            )
            ingestion_service.local_shard_event_count = page_size
            staged = ingestion_service.fetch_and_stage_events(
                job["spec"]["start_date"],
                job["spec"]["end_date"],
                job_id=job_id,
                page_size=page_size,
                should_stop=lambda: self._is_stop_requested(job_id),
                progress_callback=lambda current_events, shards_created, _: self._update_stage_progress(
                    job_id,
                    connector_record,
                    current_events,
                    shards_created,
                ),
            )
            if staged.get("stopped"):
                return self._mark_stopped(job_id, staged.get("stop_reason") or "Stopped by user.")

            manifests: List[Dict[str, Any]] = []
            for manifest in staged["shard_manifests"]:
                if self._is_stop_requested(job_id):
                    return self._mark_stopped(job_id)
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
                self._commit_session()
                manifests.append(notification)

            processing_stats: Dict[str, Any] = {}
            if self._is_stop_requested(job_id):
                return self._mark_stopped(job_id)
            if self.settings.data_backend_mode == "mock":
                runner = DataflowNormalizationRunner(gcs_service=gcs_service, bigquery_service=BigQueryService())
                processing_stats = runner.process_notifications(manifests)

            completed = self.repository.update_import_job(
                job_id,
                {
                    "status": JobStatus.COMPLETED.value,
                    "error": None,
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
            self._commit_session()
            return completed
        except Exception as exc:
            self.rollback_session()
            try:
                if self._is_stop_requested(job_id):
                    return self._mark_stopped(job_id)
            except Exception:
                self.rollback_session()
            failed_job = self._safe_get_import_job(job_id) or job
            progress = failed_job.get("progress") or {}
            progress_details = dict(progress.get("details") or {})
            progress_details["failure_reason"] = str(exc)
            try:
                failed = self.repository.update_import_job(
                    job_id,
                    {
                        "status": JobStatus.FAILED.value,
                        "error": str(exc),
                        "progress": {
                            "current": int(progress.get("current", 0) or 0),
                            "total": int(progress.get("total", 0) or 0),
                            "pct": float(progress.get("pct", 0.0) or 0.0),
                            "details": progress_details,
                        },
                    },
                )
                self.repository.record_action("import_job_failed", "import_job", job_id, failed)
                self._commit_session()
            except Exception:
                self.rollback_session()
            raise
