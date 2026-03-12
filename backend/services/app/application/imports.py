from __future__ import annotations

import logging
import uuid
from datetime import datetime, timedelta
from typing import Any, Dict, List

from app.application.mappings import MappingService
from app.core.errors import MissingDependencyError, ResourceLockedError
from app.domain.jobs import CheckpointStatus, JobStatus
from dataflow.pipeline import DataflowNormalizationRunner
from gcs_service import GcsService
from ingestion_service import IngestionService
from pubsub_service import PubSubService
from bigquery_service import BigQueryService, get_shared_bigquery_service


logger = logging.getLogger(__name__)


class ImportService:
    def __init__(self, repository, settings, bigquery_service: BigQueryService | None = None):
        self.repository = repository
        self.settings = settings
        self.bigquery_service = bigquery_service or get_shared_bigquery_service()

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

    @staticmethod
    def _canonical_aliases() -> Dict[str, str]:
        return {
            "standardized": "events_staging",
            "fact_events_unified": "events_curated",
            "mart_user_daily": "player_latest_state",
            "prediction_results": "prediction_results",
            "pipeline_dead_letters": "pipeline_dead_letters",
        }

    def _mapping_coverage(self, source_name: str, *, job_id: str | None = None) -> float:
        try:
            mapping = MappingService(self.repository).get_effective_mapping(source_name, job_id=job_id)
            return MappingService._required_coverage(mapping)
        except Exception:
            self.rollback_session()
            return 100.0

    def _build_quality_report(
        self,
        processing_stats: Dict[str, Any],
        *,
        mapping_coverage: float | None = None,
        identity_summary: Dict[str, Any] | None = None,
        top20_field_quality: Dict[str, Any] | None = None,
    ) -> Dict[str, Any]:
        raw = max(0, int(processing_stats.get("raw_normalized_events", 0) or 0))
        dead_letters = max(0, int(processing_stats.get("pipeline_dead_letters_written", 0) or 0))
        flag_counts = processing_stats.get("flag_counts") or {}
        invalid_time = int(flag_counts.get("invalid_event_time", 0) or 0)
        missing_player = int(flag_counts.get("missing_player_id", 0) or 0)
        source_required_coverage = 100.0
        if raw > 0:
            player_id_coverage = max(0.0, (raw - missing_player) / raw * 100.0)
            event_time_coverage = max(0.0, (raw - invalid_time) / raw * 100.0)
            source_required_coverage = round(min(player_id_coverage, event_time_coverage, 100.0), 2)
        warehouse_stats = processing_stats.get("warehouse_stats") or {}
        curation = warehouse_stats.get("curation") or {}
        staging_rows = int(curation.get("staging_rows", 0) or 0)
        duplicates_removed = int(curation.get("duplicates_removed", 0) or 0)
        dedupe_rate = round((duplicates_removed / staging_rows * 100.0), 2) if staging_rows else 0.0
        reject_rate = round((dead_letters / raw * 100.0), 2) if raw else 0.0
        canonical_coverage = float((identity_summary or {}).get("canonical_user_id_coverage") or 0.0)
        if identity_summary is None and int(processing_stats.get("events_staging_written", 0) or 0) > 0:
            canonical_coverage = 100.0
        resolved_mapping_coverage = round(float(mapping_coverage if mapping_coverage is not None else source_required_coverage), 2)
        return {
            "required_mapping_coverage": resolved_mapping_coverage,
            "source_required_field_coverage": source_required_coverage,
            "canonical_user_id_coverage": round(canonical_coverage, 2),
            "reject_rate": reject_rate,
            "dedupe_rate": dedupe_rate,
            "flag_counts": flag_counts,
            "top20_field_coverage": top20_field_quality or {"rows_evaluated": 0, "fields": {}},
        }

    def _identity_summary(self, job_id: str) -> Dict[str, Any]:
        summary = self.bigquery_service.build_identity_summary(job_id=job_id)
        self.repository.upsert_resource(
            "identity_summary",
            job_id,
            status="ready",
            name=job_id,
            payload=summary,
        )
        self.repository.record_resource_event(
            "identity_summary",
            job_id,
            event_type="identity_summary_refreshed",
            payload=summary,
        )
        return summary

    def _top20_field_quality(self, job_id: str) -> Dict[str, Any]:
        return self.bigquery_service.top20_field_quality(job_id=job_id, alias="standardized")

    def _safe_get_import_job(self, job_id: str) -> Dict[str, Any] | None:
        try:
            return self.repository.get_import_job(job_id)
        except Exception:
            self.rollback_session()
            return None

    def _merge_progress(
        self,
        job: Dict[str, Any],
        *,
        current: int | None = None,
        total: int | None = None,
        pct: float | None = None,
        details_patch: Dict[str, Any] | None = None,
    ) -> Dict[str, Any]:
        progress = job.get("progress") or {}
        details = dict(progress.get("details") or {})
        if details_patch:
            details.update(details_patch)
        return {
            "current": int(progress.get("current", 0) if current is None else current),
            "total": int(progress.get("total", 0) if total is None else total),
            "pct": float(progress.get("pct", 0.0) if pct is None else pct),
            "details": details,
        }

    def _record_status_transition(
        self,
        job_id: str,
        *,
        from_status: str,
        to_status: str,
        reason: str,
        metadata: Dict[str, Any] | None = None,
    ) -> None:
        payload = {
            "job_id": job_id,
            "from_status": from_status,
            "to_status": to_status,
            "reason": reason,
            "metadata": metadata or {},
            "recorded_at": datetime.utcnow().isoformat(),
        }
        self.repository.record_action("import_job_status_changed", "import_job", job_id, payload)

    def _set_job_status(
        self,
        job_id: str,
        status: str,
        *,
        reason: str,
        error: str | None = None,
        details_patch: Dict[str, Any] | None = None,
    ) -> Dict[str, Any]:
        job = self.repository.get_import_job(job_id)
        if job is None:
            raise KeyError(job_id)
        updated = self.repository.update_import_job(
            job_id,
            {
                "status": status,
                "error": error,
                "progress": self._merge_progress(job, details_patch=details_patch),
            },
        )
        self._record_status_transition(
            job_id,
            from_status=str(job.get("status") or ""),
            to_status=status,
            reason=reason,
            metadata=details_patch,
        )
        return updated

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

        stopped = self.repository.update_import_job(
            job_id,
            {
                "status": JobStatus.STOPPED.value,
                "error": None,
                "progress": self._merge_progress(job, details_patch={"stop_reason": reason, "stop_requested": False}),
            },
        )
        self._record_status_transition(
            job_id,
            from_status=str(job.get("status") or ""),
            to_status=JobStatus.STOPPED.value,
            reason=reason,
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
        page_size: int,
    ) -> Dict[str, Any]:
        current_job = self.repository.get_import_job(job_id)
        if current_job is None:
            raise KeyError(job_id)
        updated = self.repository.update_import_job(
            job_id,
            {
                "progress": self._merge_progress(
                    current_job,
                    current=current_events,
                    details_patch={
                        "source": connector_record["name"],
                        "connector_type": connector_record["type"],
                        "events_staged": int(current_events),
                        "shards_created": int(shards_created),
                        "page_size": int(page_size),
                        "phase": "staging",
                        "canonical_aliases": self._canonical_aliases(),
                        "checkpoint_state": self._summarize_checkpoints(job_id),
                    },
                ),
            },
        )
        self._commit_session()
        return updated

    def _update_processing_progress(
        self,
        job_id: str,
        connector_record: Dict[str, Any],
        total_events: int,
        processed_manifests: int,
        total_manifests: int,
        summary: Dict[str, Any],
    ) -> Dict[str, Any]:
        current_job = self.repository.get_import_job(job_id)
        if current_job is None:
            raise KeyError(job_id)
        current = int(summary.get("raw_normalized_events", 0) or 0)
        total = max(0, int(total_events or 0))
        pct = (current / total * 100.0) if total else 0.0
        updated = self.repository.update_import_job(
            job_id,
            {
                "progress": self._merge_progress(
                    current_job,
                    current=current,
                    total=total,
                    pct=pct,
                    details_patch={
                        "source": connector_record["name"],
                        "connector_type": connector_record["type"],
                        "phase": "processing",
                        "events_staged": int(total_events),
                        "normalized_events": int(summary.get("raw_normalized_events", 0) or 0),
                        "events_written": int(summary.get("events_staging_written", 0) or 0),
                        "dead_letters_written": int(summary.get("pipeline_dead_letters_written", 0) or 0),
                        "processed_manifests": int(processed_manifests),
                        "total_manifests": int(total_manifests),
                        "checkpoint_state": self._summarize_checkpoints(job_id),
                    },
                ),
            },
        )
        self._commit_session()
        return updated

    def _parse_timestamp(self, value: Any) -> datetime | None:
        if not value:
            return None
        try:
            return datetime.fromisoformat(str(value))
        except ValueError:
            return None

    def _summarize_checkpoints(self, job_id: str) -> Dict[str, Any]:
        items = self.repository.list_checkpoints(job_id)
        counts: Dict[str, int] = {}
        last_cursor = None
        for item in items:
            status = str(item.get("status") or "unknown")
            counts[status] = counts.get(status, 0) + 1
            if item.get("cursor") is not None:
                last_cursor = item.get("cursor")
        processed = counts.get(CheckpointStatus.PROCESSED.value, 0)
        published = counts.get(CheckpointStatus.PUBLISHED.value, 0)
        staged = counts.get(CheckpointStatus.STAGED.value, 0)
        failed = counts.get(CheckpointStatus.FAILED.value, 0)
        return {
            "total": len(items),
            "processed": processed,
            "published": published,
            "staged": staged,
            "failed": failed,
            "pending": max(0, len(items) - processed),
            "last_cursor": last_cursor,
            "counts": counts,
        }

    def _checkpoint_notifications(
        self,
        checkpoints: List[Dict[str, Any]],
        *,
        pending_only: bool = False,
    ) -> List[Dict[str, Any]]:
        notifications: List[Dict[str, Any]] = []
        for item in checkpoints:
            status = str(item.get("status") or "")
            if pending_only and status == CheckpointStatus.PROCESSED.value:
                continue
            manifest = dict(item.get("manifest") or {})
            notifications.append(
                {
                    "gcs_path": item.get("gcs_uri") or manifest.get("gcs_uri"),
                    "event_count": int(item.get("event_count") or manifest.get("event_count") or 0),
                    "source": manifest.get("source") or item.get("source") or item.get("source_name"),
                    "job_id": item.get("job_id"),
                    "schema_version": manifest.get("schema_version"),
                    "shard_index": int(item.get("shard_index") or manifest.get("shard_index") or 0),
                    "source_config_id": manifest.get("source_config_id") or item.get("source_name"),
                    "start_date": manifest.get("start_date"),
                    "end_date": manifest.get("end_date"),
                }
            )
        return notifications

    def _mark_checkpoint_status(
        self,
        job_id: str,
        source_name: str,
        notifications: List[Dict[str, Any]],
        *,
        status: str,
    ) -> None:
        for notification in notifications:
            self.repository.upsert_checkpoint(
                {
                    "job_id": job_id,
                    "shard_index": int(notification.get("shard_index") or 0),
                    "source_name": source_name,
                    "status": status,
                    "cursor": str(notification.get("shard_index") or ""),
                    "gcs_uri": notification.get("gcs_path"),
                    "message_id": notification.get("message_id"),
                    "manifest": notification,
                    "event_count": int(notification.get("event_count") or 0),
                }
            )

    def _process_notifications(
        self,
        job_id: str,
        *,
        connector_record: Dict[str, Any],
        notifications: List[Dict[str, Any]],
        staged_events: int,
        mode: str,
    ) -> Dict[str, Any]:
        processing_stats: Dict[str, Any] = {}
        if notifications and self.settings.data_backend_mode == "mock":
            runner = DataflowNormalizationRunner(gcs_service=GcsService(), bigquery_service=self.bigquery_service)
            processing_stats = runner.process_notifications(
                notifications,
                progress_callback=lambda processed_manifests, total_manifests, summary: self._update_processing_progress(
                    job_id,
                    connector_record,
                    staged_events,
                    processed_manifests,
                    total_manifests,
                    summary,
                ),
            )
        self._mark_checkpoint_status(job_id, connector_record["name"], notifications, status=CheckpointStatus.PROCESSED.value)
        mapping_coverage = self._mapping_coverage(connector_record["name"], job_id=job_id)
        identity_summary = self._identity_summary(job_id)
        quality_report = self._build_quality_report(
            processing_stats,
            mapping_coverage=mapping_coverage,
            identity_summary=identity_summary,
            top20_field_quality=self._top20_field_quality(job_id),
        )
        final_status = JobStatus.COMPLETED.value if quality_report["required_mapping_coverage"] >= 95.0 else JobStatus.AWAITING_MAPPING.value
        current_job = self.repository.get_import_job(job_id)
        if current_job is None:
            raise KeyError(job_id)
        completed = self.repository.update_import_job(
            job_id,
            {
                "status": final_status,
                "error": None,
                "progress": self._merge_progress(
                    current_job,
                    current=staged_events,
                    total=staged_events,
                    pct=100.0,
                    details_patch={
                        "source": connector_record["name"],
                        "connector_type": connector_record["type"],
                        "events_staged": staged_events,
                        "phase": "completed",
                        "processing": processing_stats,
                        "quality_report": quality_report,
                        "identity_summary": identity_summary,
                        "top20_field_quality": self._top20_field_quality(job_id),
                        "mapping_coverage": mapping_coverage,
                        "checkpoint_state": self._summarize_checkpoints(job_id),
                        "canonical_aliases": self._canonical_aliases(),
                        "resume_mode": mode,
                    },
                ),
            },
        )
        self._record_status_transition(
            job_id,
            from_status=str(current_job.get("status") or ""),
            to_status=final_status,
            reason="Import processing completed.",
            metadata={"quality_report": quality_report, "resume_mode": mode},
        )
        self.repository.record_action("import_job_completed", "import_job", job_id, completed)
        self._commit_session()
        return completed

    def cleanup_expired_jobs(self) -> int:
        cutoff = datetime.utcnow() - timedelta(days=max(1, int(self.settings.job_retention_days)))
        removed_count = 0

        for job in self.repository.list_import_jobs():
            status = str(job.get("status") or "").lower()
            if status not in {JobStatus.COMPLETED.value, JobStatus.FAILED.value, JobStatus.STOPPED.value, JobStatus.CANCELLED.value}:
                continue
            updated_at = self._parse_timestamp(job.get("updated_at"))
            if updated_at is None or updated_at > cutoff:
                continue
            if self._active_prediction_dependencies(job["id"]):
                continue

            try:
                self.bigquery_service.delete_data_for_job(job["id"])
                if self.repository.delete_import_job(job["id"]):
                    payload = {
                        "id": job["id"],
                        "status": job.get("status"),
                        "cleanup_reason": f"Removed after {self.settings.job_retention_days} day retention window.",
                        "updated_at": job.get("updated_at"),
                    }
                    self.repository.record_action("import_job_retention_deleted", "import_job", job["id"], payload)
                    self._commit_session()
                    removed_count += 1
            except Exception as exc:
                self.rollback_session()
                logger.exception("Unable to delete expired import job %s. error=%s", job.get("id"), exc)

        return removed_count

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
                    "display_name": f"{source_name}-{datetime.utcnow().strftime('%Y%m%d-%H%M%S')}",
                    "connector_type": connector["type"],
                    "start_date": start_date,
                    "end_date": end_date,
                    "page_size": int(page_size or self.settings.worker_page_size),
                },
                "progress": {
                    "current": 0,
                    "total": 0,
                    "pct": 0.0,
                    "details": {
                        "canonical_aliases": self._canonical_aliases(),
                        "mapping_coverage": self._mapping_coverage(source_name),
                        "checkpoint_state": {"total": 0, "processed": 0, "pending": 0, "counts": {}},
                    },
                },
            }
        )
        self.repository.record_action("import_job_created", "import_job", job["id"], job)
        PubSubService(topic_name=self.settings.import_command_topic).publish({"job_id": job["id"]}, attributes={"job_type": "import"})
        return job

    def _discard_job_after_restart(self, job: Dict[str, Any], reason: str) -> None:
        deleted = self.repository.delete_import_job(job["id"])
        if not deleted:
            return
        payload = {
            "id": job["id"],
            "type": job["type"],
            "status": job["status"],
            "spec": job.get("spec") or {},
            "progress": job.get("progress") or {},
            "error": reason,
            "discard_reason": reason,
            "created_at": job.get("created_at"),
            "updated_at": job.get("updated_at"),
        }
        self.repository.record_action("import_job_discarded_after_restart", "import_job", job["id"], payload)
        self._commit_session()

    def reconcile_jobs_after_restart(self) -> int:
        reconciled_count = 0
        for job in self.repository.list_import_jobs():
            status = str(job.get("status") or "").lower()
            if status not in {JobStatus.QUEUED.value, JobStatus.RUNNING.value, JobStatus.STOPPING.value, JobStatus.READY.value}:
                continue

            try:
                self._discard_job_after_restart(job, "Discarded after server restart before completion.")
            except Exception as exc:
                self.rollback_session()
                logger.exception(
                    "Unable to discard incomplete import job %s during restart reconciliation. error=%s",
                    job["id"],
                    exc,
                )
            reconciled_count += 1

        return reconciled_count

    def list_jobs(self) -> List[Dict[str, Any]]:
        return self.repository.list_import_jobs()

    def get_job(self, job_id: str) -> Dict[str, Any] | None:
        return self.repository.get_import_job(job_id)

    def get_quality(self, job_id: str) -> Dict[str, Any]:
        job = self.repository.get_import_job(job_id)
        if job is None:
            raise KeyError(job_id)
        details = (job.get("progress") or {}).get("details") or {}
        mapping_coverage = float(details.get("mapping_coverage") or self._mapping_coverage(job["spec"]["source_name"], job_id=job_id))
        identity_summary = details.get("identity_summary") or self._identity_summary(job_id)
        quality_report = details.get("quality_report") or self._build_quality_report(
            {},
            mapping_coverage=mapping_coverage,
            identity_summary=identity_summary,
            top20_field_quality=details.get("top20_field_quality") or self._top20_field_quality(job_id),
        )
        checkpoint_state = details.get("checkpoint_state") or self._summarize_checkpoints(job_id)
        conflicts = self.bigquery_service.get_field_conflicts(job_id=job_id, limit=200)
        rejected = self.bigquery_service.get_rejected_event_explanations(job_id=job_id, limit=200)
        return {
            "job_id": job_id,
            "status": job["status"],
            "quality_report": quality_report,
            "identity_summary": identity_summary,
            "mapping_coverage": mapping_coverage,
            "checkpoint_state": checkpoint_state,
            "canonical_aliases": self._canonical_aliases(),
            "source_of_truth": identity_summary.get("source_of_truth_decisions") or [],
            "conflict_summary": {"count": len(conflicts), "items": conflicts[:25]},
            "rejected_summary": {"count": len(rejected), "items": rejected[:25]},
        }

    def get_identity_links(self, job_id: str) -> Dict[str, Any]:
        job = self.repository.get_import_job(job_id)
        if job is None:
            raise KeyError(job_id)
        return {
            "job_id": job_id,
            "items": self.bigquery_service.get_identity_links(job_id=job_id, limit=500),
        }

    def get_conflicts(self, job_id: str) -> Dict[str, Any]:
        job = self.repository.get_import_job(job_id)
        if job is None:
            raise KeyError(job_id)
        return {
            "job_id": job_id,
            "items": self.bigquery_service.get_field_conflicts(job_id=job_id, limit=500),
        }

    def get_rejected(self, job_id: str) -> Dict[str, Any]:
        job = self.repository.get_import_job(job_id)
        if job is None:
            raise KeyError(job_id)
        return {
            "job_id": job_id,
            "items": self.bigquery_service.get_rejected_event_explanations(job_id=job_id, limit=500),
        }

    def stop_job(self, job_id: str) -> Dict[str, Any]:
        job = self.repository.get_import_job(job_id)
        if job is None:
            raise KeyError(job_id)

        status = str(job.get("status") or "").lower()
        if status == JobStatus.STOPPED.value:
            return job
        if status in {JobStatus.QUEUED.value, JobStatus.READY.value, JobStatus.AWAITING_MAPPING.value}:
            return self._mark_stopped(job_id)
        if status == JobStatus.RUNNING.value:
            stopping = self.repository.update_import_job(
                job_id,
                {
                    "status": JobStatus.STOPPING.value,
                    "progress": self._merge_progress(job, details_patch={"stop_requested": True}),
                },
            )
            self._record_status_transition(
                job_id,
                from_status=status,
                to_status=JobStatus.STOPPING.value,
                reason="Stop requested for running import job.",
            )
            self.repository.record_action("import_job_stop_requested", "import_job", job_id, stopping)
            self._commit_session()
            return stopping
        if status == JobStatus.STOPPING.value:
            return job
        raise ValueError("Only queued, ready, awaiting_mapping, or running import jobs can be stopped.")

    def delete_job(self, job_id: str) -> None:
        job = self.repository.get_import_job(job_id)
        if job is None:
            raise KeyError(job_id)

        status = str(job.get("status") or "").lower()
        if status in {JobStatus.QUEUED.value, JobStatus.RUNNING.value, JobStatus.STOPPING.value, JobStatus.READY.value}:
            raise ResourceLockedError("Stop the import before deleting it.")
        blocking_predictions = self._active_prediction_dependencies(job_id)
        if blocking_predictions:
            raise ResourceLockedError(
                f"Import job '{job_id}' is locked by prediction jobs: {', '.join(blocking_predictions[:5])}."
            )

        if self.repository.delete_import_job(job_id):
            self.repository.record_action("import_job_deleted", "import_job", job_id, job)
            self._commit_session()

    def resume_job(self, job_id: str) -> Dict[str, Any]:
        job = self.repository.get_import_job(job_id)
        if job is None:
            raise KeyError(job_id)
        status = str(job.get("status") or "").lower()
        if status not in {JobStatus.AWAITING_MAPPING.value, JobStatus.STOPPED.value, JobStatus.FAILED.value}:
            raise ValueError("Only awaiting_mapping, stopped, or failed jobs can be resumed.")

        connector_record = self.repository.get_connector(job["spec"]["source_name"])
        if connector_record is None:
            raise KeyError(job["spec"]["source_name"])

        mapping_coverage = self._mapping_coverage(connector_record["name"], job_id=job_id)
        if mapping_coverage < 95.0:
            awaiting = self._set_job_status(
                job_id,
                JobStatus.AWAITING_MAPPING.value,
                reason="Resume blocked until mapping coverage reaches 95%.",
                details_patch={
                    "mapping_coverage": mapping_coverage,
                    "quality_report": self._build_quality_report(
                        {},
                        mapping_coverage=mapping_coverage,
                        top20_field_quality=self._top20_field_quality(job_id),
                    ),
                    "checkpoint_state": self._summarize_checkpoints(job_id),
                },
            )
            self._commit_session()
            raise ValueError(f"Mapping coverage is {mapping_coverage:.2f}%; resume requires at least 95%.")

        checkpoints = self.repository.list_checkpoints(job_id)
        pending_notifications = self._checkpoint_notifications(checkpoints, pending_only=True)
        if pending_notifications:
            running = self._set_job_status(
                job_id,
                JobStatus.RUNNING.value,
                reason="Resuming import processing from persisted checkpoints.",
                details_patch={"resume_requested_at": datetime.utcnow().isoformat(), "resume_mode": "checkpoint"},
            )
            self._commit_session()
            staged_events = int((running.get("progress") or {}).get("details", {}).get("events_staged") or sum(int(item.get("event_count") or 0) for item in pending_notifications))
            return self._process_notifications(
                job_id,
                connector_record=connector_record,
                notifications=pending_notifications,
                staged_events=staged_events,
                mode="checkpoint_resume",
            )

        ready = self._set_job_status(
            job_id,
            JobStatus.READY.value,
            reason="Resume requested; job ready for rerun.",
            details_patch={"resume_requested_at": datetime.utcnow().isoformat(), "resume_mode": "full_rerun"},
        )
        self._commit_session()
        return self.run_job(job_id)

    def replay_job(self, job_id: str) -> Dict[str, Any]:
        job = self.repository.get_import_job(job_id)
        if job is None:
            raise KeyError(job_id)
        dead_letters = self.bigquery_service.get_pipeline_dead_letters(job_id=job_id, limit=5000)
        existing_rows = self.bigquery_service.get_rows_for_alias("standardized") + self.bigquery_service.get_rows_for_alias("fact_events_unified")
        existing_fingerprints = {
            str(row.get("event_fingerprint") or "")
            for row in existing_rows
            if row.get("event_fingerprint")
        }
        replayable_rows: List[Dict[str, Any]] = []
        skipped_rows: List[Dict[str, Any]] = []
        for dead_letter in dead_letters:
            event = dict(dead_letter.get("normalized_event") or {})
            if not event:
                skipped_rows.append({"reason": "missing_normalized_event", "dead_letter": dead_letter})
                continue
            flags = {str(flag) for flag in event.get("data_quality_flags") or []}
            if {"missing_player_id", "invalid_event_time"} & flags:
                skipped_rows.append({"reason": "critical_quality_failure", "event_fingerprint": event.get("event_fingerprint")})
                continue
            fingerprint = str(event.get("event_fingerprint") or "")
            if fingerprint and fingerprint in existing_fingerprints:
                skipped_rows.append({"reason": "duplicate_suppressed", "event_fingerprint": fingerprint})
                continue
            event.setdefault("job_id", job_id)
            event.setdefault("job_identifier", job_id)
            replayable_rows.append(event)
            if fingerprint:
                existing_fingerprints.add(fingerprint)

        warehouse_stats: Dict[str, Any] = {}
        if replayable_rows:
            self.bigquery_service.write_events_staging(replayable_rows, job_id=job_id)
            warehouse_stats = {
                "curation": self.bigquery_service.run_events_curation(job_id=job_id),
                "player_latest_state": self.bigquery_service.refresh_player_latest_state(job_id=job_id),
            }

        current_job = self.repository.get_import_job(job_id)
        if current_job is None:
            raise KeyError(job_id)
        replay_summary = {
            "attempted_rows": len(dead_letters),
            "replayed_rows": len(replayable_rows),
            "skipped_rows": len(skipped_rows),
            "warehouse_stats": warehouse_stats,
            "replayed_at": datetime.utcnow().isoformat(),
        }
        identity_summary = self._identity_summary(job_id)
        updated = self.repository.update_import_job(
            job_id,
            {
                "progress": self._merge_progress(
                    current_job,
                    details_patch={
                        "checkpoint_state": self._summarize_checkpoints(job_id),
                        "replay_summary": replay_summary,
                        "identity_summary": identity_summary,
                        "top20_field_quality": self._top20_field_quality(job_id),
                        "quality_report": self._build_quality_report(
                            {},
                            mapping_coverage=float(((current_job.get("progress") or {}).get("details") or {}).get("mapping_coverage") or 100.0),
                            identity_summary=identity_summary,
                            top20_field_quality=self._top20_field_quality(job_id),
                        ),
                        "canonical_aliases": self._canonical_aliases(),
                    },
                )
            },
        )
        self.repository.record_action("import_job_replay_completed", "import_job", job_id, replay_summary)
        self._commit_session()
        return {
            "job_id": job_id,
            "status": updated["status"],
            "replayed_rows": len(replayable_rows),
            "skipped_rows": len(skipped_rows),
            "warehouse_stats": warehouse_stats,
            "checkpoint_state": self._summarize_checkpoints(job_id),
        }

    def run_job(self, job_id: str) -> Dict[str, Any]:
        job = self.repository.get_import_job(job_id)
        if job is None:
            raise KeyError(job_id)
        current_status = str(job.get("status") or "").lower()
        if current_status == JobStatus.STOPPED.value:
            return job
        if current_status == JobStatus.STOPPING.value:
            return self._mark_stopped(job_id)

        connector_record = self.repository.get_connector(job["spec"]["source_name"])
        if connector_record is None:
            raise MissingDependencyError(
                "connector",
                str(job["spec"]["source_name"]),
                detail=f"Connector '{job['spec']['source_name']}' required by import job '{job_id}' is missing.",
            )

        mapping_coverage = self._mapping_coverage(connector_record["name"], job_id=job_id)
        if mapping_coverage < 95.0:
            awaiting = self._set_job_status(
                job_id,
                JobStatus.AWAITING_MAPPING.value,
                reason="Required mapping coverage below 95%; import blocked before staging.",
                details_patch={
                    "mapping_coverage": mapping_coverage,
                    "quality_report": self._build_quality_report(
                        {},
                        mapping_coverage=mapping_coverage,
                        top20_field_quality=self._top20_field_quality(job_id),
                    ),
                    "checkpoint_state": self._summarize_checkpoints(job_id),
                    "canonical_aliases": self._canonical_aliases(),
                },
            )
            self._commit_session()
            return awaiting

        latest_job = self.repository.get_import_job(job_id)
        if latest_job is None:
            raise KeyError(job_id)
        if str(latest_job.get("status") or "").lower() == JobStatus.STOPPED.value:
            return latest_job
        if str(latest_job.get("status") or "").lower() == JobStatus.STOPPING.value:
            return self._mark_stopped(job_id)

        running = self._set_job_status(
            job_id,
            JobStatus.RUNNING.value,
            reason="Import execution started.",
            details_patch={"phase": "starting", "mapping_coverage": mapping_coverage, "canonical_aliases": self._canonical_aliases()},
        )
        self._commit_session()
        try:
            page_size = int(job["spec"].get("page_size") or self.settings.worker_page_size)
            gcs_service = GcsService()
            raw_pubsub = PubSubService(topic_name=self.settings.raw_shard_topic)
            connector_config = dict(connector_record["config"] or {})
            connector_config["field_mapping"] = MappingService(self.repository).get_effective_mapping(
                connector_record["name"],
                job_id=job_id,
            )
            ingestion_service = IngestionService(
                gcs_service=gcs_service,
                connector_config=connector_config,
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
                    page_size,
                ),
            )
            if staged.get("stopped"):
                return self._mark_stopped(job_id, staged.get("stop_reason") or "Stopped by user.")

            notifications: List[Dict[str, Any]] = []
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
                    "start_date": manifest.get("start_date"),
                    "end_date": manifest.get("end_date"),
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
                notification["message_id"] = message_id
                self.repository.upsert_checkpoint(
                    {
                        "job_id": job_id,
                        "shard_index": manifest["shard_index"],
                        "source_name": connector_record["name"],
                        "status": CheckpointStatus.PUBLISHED.value,
                        "cursor": str(manifest["shard_index"]),
                        "gcs_uri": manifest["gcs_uri"],
                        "message_id": message_id,
                        "manifest": manifest,
                        "event_count": int(manifest["event_count"] or 0),
                    }
                )
                self._commit_session()
                notifications.append(notification)

            if self._is_stop_requested(job_id):
                return self._mark_stopped(job_id)

            return self._process_notifications(
                job_id,
                connector_record=connector_record,
                notifications=notifications,
                staged_events=int(staged["events_staged"]),
                mode="initial_run",
            )
        except Exception as exc:
            self.rollback_session()
            try:
                if self._is_stop_requested(job_id):
                    return self._mark_stopped(job_id)
            except Exception:
                self.rollback_session()
            failed_job = self._safe_get_import_job(job_id) or job
            try:
                failed = self.repository.update_import_job(
                    job_id,
                    {
                        "status": JobStatus.FAILED.value,
                        "error": str(exc),
                        "progress": self._merge_progress(
                            failed_job,
                            details_patch={
                                "failure_reason": str(exc),
                                "checkpoint_state": self._summarize_checkpoints(job_id),
                            },
                        ),
                    },
                )
                self._record_status_transition(
                    job_id,
                    from_status=str(failed_job.get("status") or ""),
                    to_status=JobStatus.FAILED.value,
                    reason="Import execution failed.",
                    metadata={"error": str(exc)},
                )
                self.repository.record_action("import_job_failed", "import_job", job_id, failed)
                self._commit_session()
            except Exception:
                self.rollback_session()
            raise

    def _active_prediction_dependencies(self, job_id: str) -> List[str]:
        blocking_statuses = {JobStatus.QUEUED.value, JobStatus.RUNNING.value, JobStatus.STOPPING.value}
        return sorted(
            [
                str(item.get("id"))
                for item in self.repository.list_prediction_jobs()
                if str(item.get("import_job_id") or (item.get("spec") or {}).get("import_job_id") or "") == job_id
                and str(item.get("status") or "").lower() in blocking_statuses
            ]
        )
