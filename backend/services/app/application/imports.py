from __future__ import annotations

import logging
import queue
import threading
import time
import uuid
from datetime import datetime, timedelta
from typing import Any, Dict, List

from app.application.mappings import MappingService
from app.core.db import session_scope
from app.application.secret_refs import materialize_secret_refs
from app.core.errors import MissingDependencyError, ResourceLockedError
from app.core.request_context import RequestContext, get_request_context, request_context
from app.core.runtime import is_shutdown_requested
from app.domain.jobs import CheckpointStatus, JobStatus
from app.infrastructure.repositories.sqlalchemy_control_plane import SqlAlchemyControlPlaneRepository
from dataflow.pipeline import DataflowNormalizationRunner
from gcs_service import GcsService
from ingestion_service import IngestionService
from pubsub_service import PubSubService
from bigquery_service import BigQueryService, get_shared_bigquery_service


logger = logging.getLogger(__name__)
_IMPORT_RUN_THREADS: dict[str, threading.Thread] = {}
_IMPORT_RUN_THREADS_LOCK = threading.Lock()


class ImportInterruptedError(RuntimeError):
    """Raised when an import run is stopped while work is in flight."""


class ImportTimeoutError(RuntimeError):
    """Raised when an import run exceeds the configured external-call budget."""


class ImportDeletedError(ImportInterruptedError):
    """Raised when an import job is deleted while work is in flight."""


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

    def _get_import_run_thread(self, job_id: str) -> threading.Thread | None:
        with _IMPORT_RUN_THREADS_LOCK:
            thread = _IMPORT_RUN_THREADS.get(job_id)
            if thread is not None and not thread.is_alive():
                _IMPORT_RUN_THREADS.pop(job_id, None)
                return None
            return thread

    def _set_import_run_thread(self, job_id: str, thread: threading.Thread) -> None:
        with _IMPORT_RUN_THREADS_LOCK:
            _IMPORT_RUN_THREADS[job_id] = thread

    def _clear_import_run_thread(self, job_id: str, thread: threading.Thread | None = None) -> None:
        with _IMPORT_RUN_THREADS_LOCK:
            existing = _IMPORT_RUN_THREADS.get(job_id)
            if existing is None:
                return
            if thread is None or existing is thread:
                _IMPORT_RUN_THREADS.pop(job_id, None)

    def _wait_for_import_run_thread(self, job_id: str, timeout_seconds: float = 5.0) -> bool:
        thread = self._get_import_run_thread(job_id)
        if thread is None:
            return True
        thread.join(max(0.1, float(timeout_seconds)))
        if thread.is_alive():
            return False
        self._clear_import_run_thread(job_id, thread)
        return True

    def start_job_background(
        self,
        job_id: str,
        *,
        request_scope: RequestContext | None = None,
    ) -> Dict[str, Any]:
        job = self.repository.get_import_job(job_id)
        if job is None:
            raise KeyError(job_id)

        active_thread = self._get_import_run_thread(job_id)
        if active_thread is not None:
            return {"job": job, "started": False}

        captured_context = request_scope or get_request_context()

        def _worker(captured_job_id: str, captured_request_scope: RequestContext | None) -> None:
            current_thread = threading.current_thread()
            try:
                with request_context(captured_request_scope):
                    with session_scope() as session:
                        repository = SqlAlchemyControlPlaneRepository(session)
                        service = ImportService(repository, self.settings, get_shared_bigquery_service())
                        service.run_job(captured_job_id)
            except Exception:
                logger.exception("Background import execution failed for job %s.", captured_job_id)
            finally:
                self._clear_import_run_thread(captured_job_id, current_thread)

        worker = threading.Thread(
            target=_worker,
            args=(job_id, captured_context),
            name=f"import-job-{job_id}",
            daemon=True,
        )
        self._set_import_run_thread(job_id, worker)
        worker.start()
        refreshed_job = self.repository.get_import_job(job_id) or job
        return {"job": refreshed_job, "started": True}

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
        try:
            with session_scope() as session:
                repository = SqlAlchemyControlPlaneRepository(session)
                job = repository.get_import_job(job_id)
        except Exception:
            return False
        if job is None:
            return False
        return str(job.get("status") or "").lower() in {JobStatus.STOPPING.value, JobStatus.STOPPED.value}

    def _require_live_job_for_progress(self, job_id: str) -> Dict[str, Any]:
        current_job = self.repository.get_import_job(job_id)
        if current_job is None:
            raise ImportDeletedError(f"Import job '{job_id}' was deleted.")
        status = str(current_job.get("status") or "").lower()
        if status in {JobStatus.STOPPING.value, JobStatus.STOPPED.value}:
            raise ImportInterruptedError(self._stop_reason())
        return current_job

    def _should_stop(self, job_id: str) -> bool:
        return self._is_stop_requested(job_id) or is_shutdown_requested()

    def _stop_reason(self, default_reason: str = "Stopped by user.") -> str:
        if is_shutdown_requested():
            return "Stopped during server shutdown."
        return default_reason

    def _interruptible_call(
        self,
        job_id: str,
        *,
        operation: str,
        callback,
        timeout_seconds: float | None = None,
        activity_timestamp_getter=None,
    ) -> Any:
        result_queue: queue.Queue[tuple[str, Any]] = queue.Queue(maxsize=1)

        def _runner() -> None:
            try:
                result_queue.put(("result", callback()))
            except Exception as exc:
                result_queue.put(("error", exc))

        worker = threading.Thread(
            target=_runner,
            name=f"import-{job_id}-{operation}",
            daemon=True,
        )
        worker.start()

        resolved_timeout = max(
            0.1,
            float(timeout_seconds if timeout_seconds is not None else self.settings.import_network_timeout_seconds),
        )
        started_at = time.monotonic()
        last_activity_at = started_at
        poll_interval = max(0.05, float(self.settings.import_stop_poll_interval_seconds))
        startup_grace_seconds = max(poll_interval, min(0.25, resolved_timeout * 0.5))
        observed_progress = False

        while True:
            if self._should_stop(job_id):
                raise ImportInterruptedError(self._stop_reason())
            if callable(activity_timestamp_getter):
                try:
                    candidate_activity = activity_timestamp_getter()
                except Exception:
                    candidate_activity = None
                if candidate_activity is not None:
                    try:
                        candidate_activity_at = float(candidate_activity)
                        observed_progress = observed_progress or candidate_activity_at > started_at
                        last_activity_at = max(last_activity_at, candidate_activity_at)
                    except (TypeError, ValueError):
                        pass
            timeout_deadline = last_activity_at + resolved_timeout
            if not observed_progress:
                timeout_deadline += startup_grace_seconds
            remaining = timeout_deadline - time.monotonic()
            if remaining <= 0:
                raise ImportTimeoutError(
                    f"{operation.replace('_', ' ')} timed out after {resolved_timeout:.1f}s without progress. "
                    f"Increase IMPORT_NETWORK_TIMEOUT_SECONDS for slower connectors."
                )
            try:
                state, payload = result_queue.get(timeout=min(poll_interval, remaining))
            except queue.Empty:
                continue
            if state == "error":
                raise payload
            return payload

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
        current_job = self._require_live_job_for_progress(job_id)
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
        current_job = self._require_live_job_for_progress(job_id)
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
            resource_id = f"{job_id}:{int(notification.get('shard_index') or 0)}"
            existing = self.repository.get_resource("import_manifest", resource_id)
            payload = dict((existing or {}).get("payload") or {})
            payload.update(
                {
                    "manifest_id": resource_id,
                    "job_id": job_id,
                    "source_name": source_name,
                    "status": status,
                    "gcs_uri": notification.get("gcs_path"),
                    "message_id": notification.get("message_id"),
                    "event_count": int(notification.get("event_count") or 0),
                    "manifest": notification,
                    "schema_version": notification.get("schema_version"),
                    "shard_index": int(notification.get("shard_index") or 0),
                }
            )
            self.repository.upsert_resource("import_manifest", resource_id, status=status, name=job_id, payload=payload)

    def _mark_pending_checkpoints_failed(self, job_id: str, *, reason: str) -> None:
        checkpoints = self.repository.list_checkpoints(job_id)
        failed_at = datetime.utcnow().isoformat()
        for checkpoint in checkpoints:
            status = str(checkpoint.get("status") or "").lower()
            if status == CheckpointStatus.PROCESSED.value:
                continue
            shard_index = int(checkpoint.get("shard_index") or 0)
            source_name = str(checkpoint.get("source_name") or "")
            payload = dict(checkpoint)
            payload.update(
                {
                    "failure_reason": reason,
                    "failed_at": failed_at,
                }
            )
            self.repository.upsert_checkpoint(
                {
                    "job_id": job_id,
                    "shard_index": shard_index,
                    "source_name": source_name,
                    "status": CheckpointStatus.FAILED.value,
                    "cursor": checkpoint.get("cursor"),
                    "gcs_uri": checkpoint.get("gcs_uri"),
                    "message_id": checkpoint.get("message_id"),
                    "manifest": checkpoint.get("manifest"),
                    "event_count": int(checkpoint.get("event_count") or 0),
                    "failure_reason": reason,
                    "failed_at": failed_at,
                }
            )
            resource_id = f"{job_id}:{shard_index}"
            existing = self.repository.get_resource("import_manifest", resource_id)
            resource_payload = dict((existing or {}).get("payload") or {})
            resource_payload.update(payload)
            resource_payload.update(
                {
                    "manifest_id": resource_id,
                    "job_id": job_id,
                    "source_name": source_name,
                    "status": CheckpointStatus.FAILED.value,
                    "gcs_uri": checkpoint.get("gcs_uri"),
                    "message_id": checkpoint.get("message_id"),
                    "shard_index": shard_index,
                    "event_count": int(checkpoint.get("event_count") or 0),
                    "failure_reason": reason,
                    "failed_at": failed_at,
                }
            )
            self.repository.upsert_resource(
                "import_manifest",
                resource_id,
                status=CheckpointStatus.FAILED.value,
                name=job_id,
                payload=resource_payload,
            )

    def _failure_stage(self, job: Dict[str, Any] | None) -> str:
        details = ((job or {}).get("progress") or {}).get("details") or {}
        phase = str(details.get("phase") or "").strip().lower()
        if phase:
            return phase
        status = str((job or {}).get("status") or "").strip().lower()
        return status or "unknown"

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
        active_jobs = [
            job
            for job in self.repository.list_import_jobs()
            if str(job.get("status") or "").lower() not in {JobStatus.COMPLETED.value, JobStatus.FAILED.value, JobStatus.STOPPED.value, JobStatus.CANCELLED.value}
        ]
        if len(active_jobs) >= int(self.settings.max_import_jobs_per_tenant):
            raise ValueError(f"Import job limit reached for tenant; max active jobs is {self.settings.max_import_jobs_per_tenant}.")
        connector = self.repository.get_connector(source_name)
        if connector is None:
            raise KeyError(source_name)
        connector_name = str(connector.get("name") or source_name)
        connector_id = str(connector.get("connector_id") or source_name)
        job = self.repository.create_import_job(
            {
                "id": f"imp_{uuid.uuid4().hex[:20]}",
                "source_name": connector_name,
                "status": JobStatus.QUEUED.value,
                "spec": {
                    "source_name": connector_name,
                    "connector_id": connector_id,
                    "display_name": f"{connector_name}-{datetime.utcnow().strftime('%Y%m%d-%H%M%S')}",
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
                        "mapping_coverage": self._mapping_coverage(connector_name),
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
        schema_contracts = [
            self.bigquery_service.get_schema_contract("standardized", job_id=job_id),
            self.bigquery_service.get_schema_contract("fact_events_unified", job_id=job_id),
        ]
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
            "schema_contracts": schema_contracts,
        }

    def list_manifests(self, job_id: str) -> Dict[str, Any]:
        job = self.repository.get_import_job(job_id)
        if job is None:
            raise KeyError(job_id)
        items = []
        for record in self.repository.list_resources("import_manifest"):
            payload = dict(record.get("payload") or {})
            if str(payload.get("job_id") or "") != job_id:
                continue
            items.append(
                {
                    "manifest_id": record.get("resource_id"),
                    "status": record.get("status"),
                    "created_at": record.get("created_at"),
                    "updated_at": record.get("updated_at"),
                    **payload,
                }
            )
        if not items:
            for checkpoint in self.repository.list_checkpoints(job_id):
                manifest = dict(checkpoint.get("manifest") or {})
                items.append(
                    {
                        "manifest_id": f"{job_id}:{checkpoint.get('shard_index')}",
                        "status": checkpoint.get("status"),
                        "created_at": checkpoint.get("updated_at"),
                        "updated_at": checkpoint.get("updated_at"),
                        "job_id": job_id,
                        "source_name": checkpoint.get("source_name"),
                        "event_count": checkpoint.get("event_count"),
                        "schema_version": manifest.get("schema_version"),
                        "shard_index": checkpoint.get("shard_index"),
                        "gcs_uri": checkpoint.get("gcs_uri"),
                        "message_id": checkpoint.get("message_id"),
                        "manifest": manifest,
                    }
                )
        items.sort(key=lambda item: int(item.get("shard_index") or 0))
        return {"job_id": job_id, "items": items}

    def list_schema_contracts(self) -> Dict[str, Any]:
        return {"items": self.bigquery_service.list_schema_contracts()}

    def get_schema_contract(self, alias: str, *, job_id: str | None = None) -> Dict[str, Any]:
        return self.bigquery_service.get_schema_contract(alias, job_id=job_id)

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

    def _classify_dead_letter_replay(self, job_id: str) -> Dict[str, Any]:
        dead_letters = self.bigquery_service.get_pipeline_dead_letters(job_id=job_id, limit=5000)
        existing_rows = self.bigquery_service.get_rows_for_alias("standardized") + self.bigquery_service.get_rows_for_alias("fact_events_unified")
        existing_fingerprints = {
            str(row.get("event_fingerprint") or "")
            for row in existing_rows
            if row.get("event_fingerprint")
        }
        replayable_rows: List[Dict[str, Any]] = []
        skipped_rows: List[Dict[str, Any]] = []
        reason_counts: Dict[str, int] = {}

        for dead_letter in dead_letters:
            event = dict(dead_letter.get("normalized_event") or {})
            if not event:
                reason = "missing_normalized_event"
                skipped_rows.append({"reason": reason, "dead_letter": dead_letter})
                reason_counts[reason] = reason_counts.get(reason, 0) + 1
                continue

            flags = {str(flag) for flag in event.get("data_quality_flags") or []}
            if {"missing_player_id", "invalid_event_time"} & flags:
                reason = "critical_quality_failure"
                skipped_rows.append({"reason": reason, "event_fingerprint": event.get("event_fingerprint")})
                reason_counts[reason] = reason_counts.get(reason, 0) + 1
                continue

            fingerprint = str(event.get("event_fingerprint") or "")
            if fingerprint and fingerprint in existing_fingerprints:
                reason = "duplicate_suppressed"
                skipped_rows.append({"reason": reason, "event_fingerprint": fingerprint})
                reason_counts[reason] = reason_counts.get(reason, 0) + 1
                continue

            event.setdefault("job_id", job_id)
            event.setdefault("job_identifier", job_id)
            replayable_rows.append(event)
            if fingerprint:
                existing_fingerprints.add(fingerprint)

        return {
            "dead_letters": dead_letters,
            "replayable_rows": replayable_rows,
            "skipped_rows": skipped_rows,
            "skip_reason_counts": reason_counts,
        }

    @staticmethod
    def _normalize_date_key(value: Any) -> str:
        text = str(value or "").strip().replace("-", "")
        return text

    def _job_in_backfill_scope(self, job: Dict[str, Any], *, source_name: str, start_date: str, end_date: str) -> bool:
        spec = job.get("spec") or {}
        if str(spec.get("source_name") or "") != str(source_name):
            return False
        job_start = self._normalize_date_key(spec.get("start_date"))
        job_end = self._normalize_date_key(spec.get("end_date"))
        range_start = self._normalize_date_key(start_date)
        range_end = self._normalize_date_key(end_date)
        if not job_start or not job_end or not range_start or not range_end:
            return False
        return not (job_end < range_start or job_start > range_end)

    def get_operations(self, job_id: str) -> Dict[str, Any]:
        job = self.repository.get_import_job(job_id)
        if job is None:
            raise KeyError(job_id)

        details = (job.get("progress") or {}).get("details") or {}
        replay_analysis = self._classify_dead_letter_replay(job_id)
        dead_letter_summary = self.bigquery_service.get_dead_letter_summary(job_id=job_id)
        lag_summary = self.bigquery_service.get_pipeline_lag_summary(job_id=job_id)
        quality = self.get_quality(job_id)

        status = str(job.get("status") or "").lower()
        can_resume = status in {JobStatus.AWAITING_MAPPING.value, JobStatus.STOPPED.value, JobStatus.FAILED.value}
        can_replay = len(replay_analysis["replayable_rows"]) > 0 and float(quality.get("mapping_coverage") or 0.0) >= 95.0

        recommended_action = "no_action_required"
        if status == JobStatus.AWAITING_MAPPING.value:
            recommended_action = "fix_mapping_and_resume"
        elif can_replay:
            recommended_action = "replay_rejected_rows"
        elif int((lag_summary.get("table_counts") or {}).get("dead_letter_rows") or 0) > 0:
            recommended_action = "review_dead_letters"

        return {
            "job_id": job_id,
            "status": job["status"],
            "source_name": (job.get("spec") or {}).get("source_name"),
            "processing_contract": {
                "mode": "manifest-driven",
                "runtime_path": (
                    "gcp"
                    if self.settings.data_backend_mode == "gcp"
                    else ("aws" if self.settings.data_backend_mode == "aws" else "local_demo")
                ),
                "checkpoint_unit": "shard",
                "replay_unit": "shard",
                "canonical_aliases": self._canonical_aliases(),
            },
            "checkpoint_state": details.get("checkpoint_state") or self._summarize_checkpoints(job_id),
            "quality_report": quality.get("quality_report") or {},
            "manifest_summary": self.list_manifests(job_id),
            "schema_contracts": quality.get("schema_contracts") or [],
            "lag_summary": lag_summary,
            "dead_letters": {
                **dead_letter_summary,
                "replayable_count": len(replay_analysis["replayable_rows"]),
                "blocked_count": len(replay_analysis["skipped_rows"]),
                "blocked_reasons": replay_analysis["skip_reason_counts"],
            },
            "remediation": {
                "can_resume": can_resume,
                "can_replay": can_replay,
                "backfill_supported": True,
                "recommended_action": recommended_action,
                "suggested_backfill_scope": {
                    "source_name": (job.get("spec") or {}).get("source_name"),
                    "start_date": (job.get("spec") or {}).get("start_date"),
                    "end_date": (job.get("spec") or {}).get("end_date"),
                    "mode": "replay_rejected_rows",
                },
            },
        }

    def create_backfill(self, *, source_name: str, start_date: str, end_date: str, mode: str = "replay_rejected_rows", limit_jobs: int = 50) -> Dict[str, Any]:
        backfill_id = f"backfill_{uuid.uuid4().hex[:20]}"
        candidate_jobs = [
            job
            for job in self.repository.list_import_jobs()
            if self._job_in_backfill_scope(job, source_name=source_name, start_date=start_date, end_date=end_date)
        ]
        candidate_jobs = candidate_jobs[: max(1, int(limit_jobs))]

        items: List[Dict[str, Any]] = []
        total_replayed_rows = 0
        total_skipped_rows = 0

        for job in candidate_jobs:
            job_id = str(job["id"])
            status = str(job.get("status") or "").lower()
            if status in {JobStatus.QUEUED.value, JobStatus.RUNNING.value, JobStatus.STOPPING.value, JobStatus.READY.value}:
                items.append({"job_id": job_id, "status": "locked", "reason": "active_job"})
                continue

            mapping_coverage = float((((job.get("progress") or {}).get("details") or {}).get("mapping_coverage") or self._mapping_coverage(source_name, job_id=job_id)))
            if mapping_coverage < 95.0:
                items.append({"job_id": job_id, "status": "blocked", "reason": "mapping_below_gate", "mapping_coverage": mapping_coverage})
                continue

            replay_analysis = self._classify_dead_letter_replay(job_id)
            if not replay_analysis["dead_letters"]:
                items.append({"job_id": job_id, "status": "skipped", "reason": "no_dead_letters"})
                continue
            if not replay_analysis["replayable_rows"]:
                items.append(
                    {
                        "job_id": job_id,
                        "status": "skipped",
                        "reason": "no_replayable_rows",
                        "blocked_reasons": replay_analysis["skip_reason_counts"],
                    }
                )
                continue

            try:
                replay_result = self.replay_job(job_id)
                total_replayed_rows += int(replay_result.get("replayed_rows") or 0)
                total_skipped_rows += int(replay_result.get("skipped_rows") or 0)
                items.append({"job_id": job_id, "status": "completed", **replay_result})
            except Exception as exc:
                self.rollback_session()
                items.append({"job_id": job_id, "status": "failed", "reason": str(exc)})

        completed_jobs = sum(1 for item in items if item.get("status") == "completed")
        failed_jobs = sum(1 for item in items if item.get("status") == "failed")
        resource_status = "completed"
        if failed_jobs and completed_jobs:
            resource_status = "partial"
        elif failed_jobs and not completed_jobs:
            resource_status = "failed"
        elif not items:
            resource_status = "empty"

        payload = {
            "backfill_id": backfill_id,
            "source_name": source_name,
            "start_date": start_date,
            "end_date": end_date,
            "mode": mode,
            "matched_jobs": len(candidate_jobs),
            "completed_jobs": completed_jobs,
            "failed_jobs": failed_jobs,
            "replayed_rows": total_replayed_rows,
            "skipped_rows": total_skipped_rows,
            "items": items,
            "executed_at": datetime.utcnow().isoformat(),
        }
        self.repository.upsert_resource("import_backfill", backfill_id, status=resource_status, name=f"{source_name}:{start_date}-{end_date}", payload=payload)
        self.repository.record_resource_event("import_backfill", backfill_id, event_type="import_backfill_completed", payload=payload)
        self.repository.record_action("import_backfill_created", "import_backfill", backfill_id, payload)
        self._commit_session()
        return {
            "backfill_id": backfill_id,
            "status": resource_status,
            **payload,
        }

    def list_backfills(self) -> Dict[str, Any]:
        items = []
        for resource in self.repository.list_resources("import_backfill"):
            payload = dict(resource.get("payload") or {})
            items.append(
                {
                    "backfill_id": resource.get("resource_id"),
                    "status": resource.get("status"),
                    "name": resource.get("name"),
                    "created_at": resource.get("created_at"),
                    "updated_at": resource.get("updated_at"),
                    **payload,
                }
            )
        return {"items": items}

    def get_backfill(self, backfill_id: str) -> Dict[str, Any]:
        resource = self.repository.get_resource("import_backfill", backfill_id)
        if resource is None:
            raise KeyError(backfill_id)
        return {
            "backfill_id": resource.get("resource_id"),
            "status": resource.get("status"),
            "name": resource.get("name"),
            "created_at": resource.get("created_at"),
            "updated_at": resource.get("updated_at"),
            **dict(resource.get("payload") or {}),
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
            self.repository.record_action(
                "import_job_stop_requested",
                "import_job",
                job_id,
                {"job_id": job_id, "status": status, "requested_at": datetime.utcnow().isoformat()},
            )
            self._commit_session()
            return self._mark_stopped(job_id, self._stop_reason())
        if status == JobStatus.STOPPING.value:
            return self._mark_stopped(job_id, self._stop_reason())
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

        if not self._wait_for_import_run_thread(job_id):
            raise ResourceLockedError(f"Import job '{job_id}' is still shutting down; retry deleting it in a moment.")

        gcs_service = GcsService()
        gcs_service.delete_data_for_job(job_id)
        self.bigquery_service.delete_data_for_job(job_id)
        self.repository.delete_resource("identity_summary", job_id)
        for resource in self.repository.list_resources("import_manifest"):
            payload = dict(resource.get("payload") or {})
            resource_id = str(resource.get("resource_id") or "")
            if (
                str(resource.get("name") or "") == job_id
                or str(payload.get("job_id") or "") == job_id
                or resource_id.startswith(f"{job_id}:")
            ):
                self.repository.delete_resource("import_manifest", resource_id)

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

        connector_record = self.repository.get_connector(
            job["spec"].get("connector_id") or job["spec"]["source_name"],
            tenant_id=job.get("tenant_id"),
            project_id=job.get("project_id"),
        )
        if connector_record is None:
            raise KeyError(job["spec"].get("connector_id") or job["spec"]["source_name"])

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
        replay_analysis = self._classify_dead_letter_replay(job_id)
        dead_letters = replay_analysis["dead_letters"]
        replayable_rows = replay_analysis["replayable_rows"]
        skipped_rows = replay_analysis["skipped_rows"]

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

        connector_record = self.repository.get_connector(
            job["spec"].get("connector_id") or job["spec"]["source_name"],
            tenant_id=job.get("tenant_id"),
            project_id=job.get("project_id"),
        )
        if connector_record is None:
            raise MissingDependencyError(
                "connector",
                str(job["spec"].get("connector_id") or job["spec"]["source_name"]),
                detail=f"Connector '{job['spec'].get('connector_id') or job['spec']['source_name']}' required by import job '{job_id}' is missing.",
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
            stage_progress = {"last_activity_at": time.monotonic()}
            gcs_service = GcsService()
            raw_pubsub = PubSubService(topic_name=self.settings.raw_shard_topic)
            connector_config = materialize_secret_refs(dict(connector_record["config"] or {}))
            connector_config["field_mapping"] = MappingService(self.repository).get_effective_mapping(
                connector_record["name"],
                job_id=job_id,
            )
            ingestion_service = IngestionService(
                gcs_service=gcs_service,
                connector_config=connector_config,
                connector_type=connector_record["type"],
                source_config_id=connector_record.get("connector_id") or connector_record["name"],
                pubsub_service=raw_pubsub,
            )
            ingestion_service.local_shard_event_count = page_size

            def _stage_progress_callback(current_events: int, shards_created: int, manifest: Dict[str, Any]) -> Dict[str, Any]:
                stage_progress["last_activity_at"] = time.monotonic()
                return self._update_stage_progress(
                    job_id,
                    connector_record,
                    current_events,
                    shards_created,
                    page_size,
                )

            staged = self._interruptible_call(
                job_id,
                operation="fetch_and_stage_events",
                callback=lambda: ingestion_service.fetch_and_stage_events(
                    job["spec"]["start_date"],
                    job["spec"]["end_date"],
                    job_id=job_id,
                    page_size=page_size,
                    should_stop=lambda: self._should_stop(job_id),
                    progress_callback=_stage_progress_callback,
                ),
                activity_timestamp_getter=lambda: stage_progress["last_activity_at"],
            )
            if staged.get("stopped"):
                return self._mark_stopped(job_id, staged.get("stop_reason") or self._stop_reason())

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
                resource_id = f"{job_id}:{int(manifest['shard_index'])}"
                self.repository.upsert_resource(
                    "import_manifest",
                    resource_id,
                    status=CheckpointStatus.PUBLISHED.value,
                    name=job_id,
                    payload={
                        "manifest_id": resource_id,
                        "job_id": job_id,
                        "source_name": connector_record["name"],
                        "event_count": int(manifest["event_count"] or 0),
                        "schema_version": manifest["schema_version"],
                        "shard_index": int(manifest["shard_index"]),
                        "gcs_uri": manifest["gcs_uri"],
                        "message_id": message_id,
                        "manifest": manifest,
                    },
                )
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
            failed_job = self._safe_get_import_job(job_id)
            if isinstance(exc, ImportInterruptedError):
                if failed_job is None:
                    logger.info("Import job %s stopped after deletion. reason=%s", job_id, exc)
                    return {
                        "id": job_id,
                        "status": JobStatus.STOPPED.value,
                        "progress": {"details": {"stop_reason": str(exc)}},
                    }
                try:
                    if self._should_stop(job_id):
                        return self._mark_stopped(job_id, str(exc) or self._stop_reason())
                except Exception:
                    self.rollback_session()
            try:
                if self._should_stop(job_id):
                    return self._mark_stopped(job_id, self._stop_reason())
            except Exception:
                self.rollback_session()
            failed_job = failed_job or job
            failure_stage = self._failure_stage(failed_job)
            logger.exception("Import job %s failed during %s. error=%s", job_id, failure_stage, exc)
            try:
                self._mark_pending_checkpoints_failed(job_id, reason=str(exc))
                self._commit_session()
            except Exception:
                self.rollback_session()
                logger.exception("Unable to mark pending checkpoints failed for import job %s.", job_id)
            failed_job = self._safe_get_import_job(job_id) or failed_job
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
                                "failure_stage": failure_stage,
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
                    metadata={"error": str(exc), "failure_stage": failure_stage},
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
