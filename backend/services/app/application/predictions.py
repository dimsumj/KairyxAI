from __future__ import annotations

import logging
import os
import queue
import threading
import time
import uuid
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, List, Tuple

from app.application.churn_models import LocalChurnModelService
from app.application.experiments import ExperimentConfigService
from app.core.db import session_scope
from app.core.errors import MissingDependencyError, ResourceLockedError
from app.core.request_context import RequestContext, get_request_context, request_context
from app.domain.jobs import JobStatus
from app.core.runtime import is_shutdown_requested
from app.infrastructure.repositories.sqlalchemy_control_plane import SqlAlchemyControlPlaneRepository
from bigquery_service import BigQueryService, get_shared_bigquery_service
from cloud_churn_service import CloudChurnRequestError, CloudChurnService
from gemini_client import GeminiClient
from growth_decision_engine import GrowthDecisionEngine
from player_modeling_engine import PlayerModelingEngine
from pubsub_service import PubSubService


logger = logging.getLogger(__name__)

_LOCAL_MODEL_TRAINING_THREADS: dict[str, threading.Thread] = {}
_LOCAL_MODEL_TRAINING_THREADS_LOCK = threading.Lock()


class PredictionInterruptedError(RuntimeError):
    pass


class PredictionTimeoutError(RuntimeError):
    pass


class PredictionService:
    def __init__(self, repository, settings, bigquery_service: BigQueryService | None = None):
        self.repository = repository
        self.settings = settings
        self.bigquery_service = bigquery_service or get_shared_bigquery_service()
        self.local_models = LocalChurnModelService(repository, self.bigquery_service)
        self.experiments = ExperimentConfigService(repository)

    def _local_model_training_scope_key(self) -> str:
        return self.local_models._training_scope_key()

    def _get_local_model_training_thread(self) -> threading.Thread | None:
        scope_key = self._local_model_training_scope_key()
        with _LOCAL_MODEL_TRAINING_THREADS_LOCK:
            thread = _LOCAL_MODEL_TRAINING_THREADS.get(scope_key)
            if thread is not None and not thread.is_alive():
                _LOCAL_MODEL_TRAINING_THREADS.pop(scope_key, None)
                return None
            return thread

    def _set_local_model_training_thread(self, thread: threading.Thread) -> None:
        with _LOCAL_MODEL_TRAINING_THREADS_LOCK:
            _LOCAL_MODEL_TRAINING_THREADS[self._local_model_training_scope_key()] = thread

    def _clear_local_model_training_thread(self, thread: threading.Thread | None = None) -> None:
        scope_key = self._local_model_training_scope_key()
        with _LOCAL_MODEL_TRAINING_THREADS_LOCK:
            existing = _LOCAL_MODEL_TRAINING_THREADS.get(scope_key)
            if existing is None:
                return
            if thread is None or existing is thread:
                _LOCAL_MODEL_TRAINING_THREADS.pop(scope_key, None)

    def _reconcile_stale_training_status(self) -> Dict[str, Any]:
        training_status = self.local_models.get_training_status()
        if not training_status:
            return {}
        status = str(training_status.get("status") or "").lower()
        if status in {"running", "stopping"} and self._get_local_model_training_thread() is None:
            return self.local_models.mark_training_stopped(reason="Training stopped when the server restarted.")
        return training_status

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

    def _safe_get_prediction_job(self, job_id: str) -> Dict[str, Any] | None:
        try:
            return self.repository.get_prediction_job(job_id)
        except Exception:
            self.rollback_session()
            return None

    def _is_stop_requested(self, job_id: str) -> bool:
        job = self.repository.get_prediction_job(job_id)
        if job is None:
            return False
        return str(job.get("status") or "").lower() in {JobStatus.STOPPING.value, JobStatus.STOPPED.value}

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
        callback: Callable[[], Any],
        timeout_seconds: float | None = None,
    ) -> Any:
        result_queue: queue.Queue[tuple[str, Any]] = queue.Queue(maxsize=1)

        def _runner() -> None:
            try:
                result_queue.put(("result", callback()))
            except Exception as exc:
                result_queue.put(("error", exc))

        worker = threading.Thread(
            target=_runner,
            name=f"prediction-{job_id}-{operation}",
            daemon=True,
        )
        worker.start()

        deadline = time.monotonic() + max(
            0.1,
            float(timeout_seconds if timeout_seconds is not None else self.settings.prediction_network_timeout_seconds),
        )
        poll_interval = max(0.05, float(self.settings.prediction_stop_poll_interval_seconds))

        while True:
            if self._should_stop(job_id):
                raise PredictionInterruptedError(self._stop_reason())
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise PredictionTimeoutError(
                    f"{operation.replace('_', ' ')} timed out after "
                    f"{float(timeout_seconds if timeout_seconds is not None else self.settings.prediction_network_timeout_seconds):.1f}s."
                )
            try:
                state, payload = result_queue.get(timeout=min(poll_interval, remaining))
            except queue.Empty:
                continue
            if state == "error":
                raise payload
            return payload

    def _mark_stopped(self, job_id: str, reason: str = "Stopped by user.") -> Dict[str, Any]:
        job = self.repository.get_prediction_job(job_id)
        if job is None:
            raise KeyError(job_id)
        if str(job.get("status") or "").lower() == JobStatus.STOPPED.value:
            return self._decorate_prediction_job(job)

        progress = job.get("progress") or {}
        details = dict(progress.get("details") or {})
        details.pop("stop_requested", None)
        details["stop_reason"] = reason
        stopped = self.repository.update_prediction_job(
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
        self.repository.record_action("prediction_job_stopped", "prediction_job", job_id, stopped)
        self._commit_session()
        return self._decorate_prediction_job(stopped)

    @staticmethod
    def _normalize_audience_scope(audience_scope: str | None, *, import_job_id: str | None = None, source_name: str | None = None) -> str:
        candidate = str(audience_scope or "").strip().lower()
        if candidate in {"import", "source"}:
            return candidate
        return "source" if str(source_name or "").strip() else "import"

    def _get_import_job_source_name(self, import_job: Dict[str, Any] | None) -> str:
        if not import_job:
            return ""
        spec = import_job.get("spec") or {}
        return str(spec.get("source_name") or import_job.get("source_name") or "").strip()

    def _get_import_job_display_name(self, import_job: Dict[str, Any] | None) -> str:
        if not import_job:
            return ""
        spec = import_job.get("spec") or {}
        return str(spec.get("display_name") or "").strip()

    def _sort_jobs_by_latest_timestamp(self, jobs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        return sorted(
            jobs,
            key=lambda item: (
                self._parse_timestamp(item.get("updated_at") or item.get("created_at")) or datetime.min,
                str(item.get("id") or ""),
            ),
            reverse=True,
        )

    def _resolve_latest_completed_import_for_source(self, source_name: str) -> Dict[str, Any]:
        normalized_source_name = str(source_name or "").strip()
        if not normalized_source_name:
            raise MissingDependencyError("import source", source_name or "", detail="Prediction source_name is required.")

        matching_imports = [
            job
            for job in self.repository.list_import_jobs()
            if str(job.get("status") or "").lower() == JobStatus.COMPLETED.value
            and self._get_import_job_source_name(job) == normalized_source_name
        ]
        if not matching_imports:
            raise MissingDependencyError(
                "completed import job",
                normalized_source_name,
                detail=f"No completed import job is available for source '{normalized_source_name}'.",
            )
        return self._sort_jobs_by_latest_timestamp(matching_imports)[0]

    def _resolve_prediction_target(
        self,
        *,
        import_job_id: str | None = None,
        source_name: str | None = None,
        audience_scope: str | None = None,
    ) -> Dict[str, Any]:
        resolved_scope = self._normalize_audience_scope(audience_scope, import_job_id=import_job_id, source_name=source_name)
        if resolved_scope == "source":
            import_job = self._resolve_latest_completed_import_for_source(str(source_name or "").strip())
        else:
            import_job = self._require_completed_import(str(import_job_id or "").strip())
        resolved_source_name = self._get_import_job_source_name(import_job)
        resolved_import_job_id = str(import_job["id"])
        resolved_import_display_name = self._get_import_job_display_name(import_job)
        audience_label = resolved_source_name if resolved_scope == "source" else (resolved_import_display_name or resolved_import_job_id)
        return {
            "audience_scope": resolved_scope,
            "source_name": resolved_source_name,
            "import_job": import_job,
            "import_job_id": resolved_import_job_id,
            "resolved_import_display_name": resolved_import_display_name,
            "audience_label": audience_label,
        }

    @staticmethod
    def _build_prediction_audience_details(resolved_target: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "audience_scope": str(resolved_target.get("audience_scope") or "import"),
            "source_name": str(resolved_target.get("source_name") or "").strip(),
            "audience_label": str(resolved_target.get("audience_label") or "").strip(),
            "resolved_import_display_name": str(resolved_target.get("resolved_import_display_name") or "").strip(),
        }

    def create_job(
        self,
        import_job_id: str | None = None,
        prediction_mode: str = "local",
        *,
        source_name: str | None = None,
        audience_scope: str | None = None,
    ) -> Dict[str, Any]:
        resolved_target = self._resolve_prediction_target(
            import_job_id=import_job_id,
            source_name=source_name,
            audience_scope=audience_scope,
        )
        import_job_id = resolved_target["import_job_id"]
        local_model_readiness = self.local_models.get_model_readiness()
        execution_details = self._prediction_execution_details(
            prediction_mode,
            gemini_available=self._has_configured_gemini(),
        )
        job = self.repository.create_prediction_job(
            {
                "id": f"pred_{uuid.uuid4().hex[:20]}",
                "import_job_id": import_job_id,
                "status": JobStatus.QUEUED.value,
                "spec": {
                    "import_job_id": import_job_id,
                    "audience_scope": resolved_target["audience_scope"],
                    "source_name": resolved_target["source_name"],
                    "prediction_mode": prediction_mode,
                },
                "progress": {
                    "current": 0,
                    "total": 0,
                    "pct": 0.0,
                    "details": {
                        "import_job_id": import_job_id,
                        **self._build_prediction_audience_details(resolved_target),
                        "prediction_mode": str(prediction_mode or "local").lower(),
                        "history_scope": "tenant_merged",
                        "history_snapshot_at": None,
                        "stale": False,
                        "stale_reason": "",
                        **self._build_local_model_details(local_model_readiness),
                        **execution_details,
                    },
                },
            }
        )
        self.repository.record_action("prediction_job_created", "prediction_job", job["id"], job)
        PubSubService(topic_name=self.settings.prediction_command_topic).publish({"job_id": job["id"]}, attributes={"job_type": "prediction"})
        return self._decorate_prediction_job(job)

    def list_jobs(self) -> List[Dict[str, Any]]:
        return [self._decorate_prediction_job(job) for job in self.repository.list_prediction_jobs()]

    def get_job(self, job_id: str) -> Dict[str, Any] | None:
        job = self.repository.get_prediction_job(job_id)
        return self._decorate_prediction_job(job) if job else None

    def list_results(self, job_id: str, page: int, page_size: int) -> Dict[str, Any]:
        return self.bigquery_service.list_prediction_results(job_id=job_id, page=page, page_size=page_size)

    def train_local_model(self, *, reference_time: str | None = None, min_rows: int = 12) -> Dict[str, Any]:
        payload = self.local_models.train_model(reference_time=reference_time, min_rows=min_rows)
        return self.local_models.sanitize_payload(payload) or {}

    def start_local_model_training(self, *, reference_time: str | None = None, min_rows: int = 12) -> Dict[str, Any]:
        current_status = self._reconcile_stale_training_status()
        current_state = str(current_status.get("status") or "").lower()
        active_thread = self._get_local_model_training_thread()
        if current_state in {"running", "stopping"} and active_thread is not None:
            return {
                "training_status": current_status,
                "readiness": self.local_models.get_model_readiness(min_rows=min_rows),
                "started": False,
            }

        resolved_reference_time = reference_time or datetime.utcnow().isoformat()
        min_rows_required = max(6, int(min_rows))
        request_scope = get_request_context()
        self.local_models.mark_training_started(reference_time=resolved_reference_time, min_rows=min_rows_required)

        def _worker(captured_context: RequestContext | None, captured_reference_time: str, captured_min_rows: int) -> None:
            current_thread = threading.current_thread()
            try:
                with request_context(captured_context):
                    with session_scope() as session:
                        repository = SqlAlchemyControlPlaneRepository(session)
                        service = LocalChurnModelService(repository, get_shared_bigquery_service())
                        service.train_model(
                            reference_time=captured_reference_time,
                            min_rows=captured_min_rows,
                            should_stop=service.is_stop_requested,
                            persist_initial_status=False,
                        )
            except Exception:
                logger.exception("Background local churn model training failed.")
            finally:
                with _LOCAL_MODEL_TRAINING_THREADS_LOCK:
                    scope_key = str((captured_context.tenant_id if captured_context is not None else None) or os.getenv("BOOTSTRAP_TENANT_ID", "default")).strip() or "default"
                    existing = _LOCAL_MODEL_TRAINING_THREADS.get(scope_key)
                    if existing is current_thread:
                        _LOCAL_MODEL_TRAINING_THREADS.pop(scope_key, None)

        worker = threading.Thread(
            target=_worker,
            args=(request_scope, resolved_reference_time, min_rows_required),
            name=f"local-churn-model-training-{self._local_model_training_scope_key()}",
            daemon=True,
        )
        self._set_local_model_training_thread(worker)
        worker.start()
        return {
            "training_status": self.local_models.get_training_status(),
            "readiness": self.local_models.get_model_readiness(min_rows=min_rows_required),
            "started": True,
        }

    def stop_local_model_training(self) -> Dict[str, Any]:
        current_status = self._reconcile_stale_training_status()
        status = str(current_status.get("status") or "").lower()
        active_thread = self._get_local_model_training_thread()
        if status not in {"running", "stopping"}:
            raise ValueError("Only running local model training can be stopped.")
        if active_thread is None:
            training_status = self.local_models.mark_training_stopped(reason="Stopped by user.")
        else:
            training_status = self.local_models.request_stop_training(reason="Stopped by user.")
        return {
            "training_status": training_status,
            "readiness": self.local_models.get_model_readiness(),
            "stopped": True,
        }

    def get_latest_model(self) -> Dict[str, Any] | None:
        payload = self.local_models.get_latest_model_payload()
        return self.local_models.sanitize_payload(payload) if payload else None

    def list_model_versions(self) -> List[Dict[str, Any]]:
        return [self.local_models.sanitize_payload(item.get("payload") or {}) or {} for item in self.local_models.list_model_versions()]

    def get_model_training_status(self) -> Dict[str, Any]:
        return self._reconcile_stale_training_status()

    def get_model_readiness(self) -> Dict[str, Any]:
        return self.local_models.get_model_readiness()

    def _parse_timestamp(self, value: Any) -> datetime | None:
        if not value:
            return None
        try:
            return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except ValueError:
            return None

    def _compute_prediction_staleness(self, job: Dict[str, Any]) -> tuple[bool, str]:
        if not job:
            return False, ""

        spec = job.get("spec") or {}
        progress = job.get("progress") or {}
        details = progress.get("details") or {}
        import_job_id = str(job.get("import_job_id") or spec.get("import_job_id") or "").strip()
        if not import_job_id:
            return False, ""

        snapshot_at = details.get("history_snapshot_at") or job.get("updated_at") or job.get("created_at")
        snapshot_dt = self._parse_timestamp(snapshot_at)
        if snapshot_dt is None:
            return False, ""

        for import_job in self.repository.list_import_jobs():
            if str(import_job.get("status") or "").lower() != JobStatus.COMPLETED.value:
                continue
            if str(import_job.get("id") or "") == import_job_id:
                continue
            completed_at = self._parse_timestamp(import_job.get("updated_at") or import_job.get("created_at"))
            if completed_at is None or completed_at <= snapshot_dt:
                continue
            completed_at_text = completed_at.isoformat()
            return True, (
                f"Newer import {import_job.get('id')} completed at {completed_at_text}, "
                "so cached merged-history predictions are stale."
            )
        return False, ""

    def _decorate_prediction_job(self, job: Dict[str, Any] | None) -> Dict[str, Any] | None:
        if job is None:
            return None

        decorated = dict(job)
        progress = dict(decorated.get("progress") or {})
        details = dict(progress.get("details") or {})
        details.setdefault("history_scope", "tenant_merged")
        details.setdefault("history_snapshot_at", None)
        stale, stale_reason = self._compute_prediction_staleness(decorated)
        details["stale"] = stale
        details["stale_reason"] = stale_reason
        progress["details"] = details
        decorated["progress"] = progress
        return decorated

    def cleanup_expired_jobs(self) -> int:
        cutoff = datetime.utcnow() - timedelta(days=max(1, int(self.settings.job_retention_days)))
        removed_count = 0

        for job in self.repository.list_prediction_jobs():
            status = str(job.get("status") or "").lower()
            if status not in {JobStatus.COMPLETED.value, JobStatus.FAILED.value, JobStatus.STOPPED.value}:
                continue
            updated_at = self._parse_timestamp(job.get("updated_at"))
            if updated_at is None or updated_at > cutoff:
                continue
            if self._active_export_dependencies(job["id"]):
                continue

            try:
                self.bigquery_service.delete_prediction_results(job["id"])
                if self.repository.delete_prediction_job(job["id"]):
                    payload = {
                        "id": job["id"],
                        "status": job.get("status"),
                        "cleanup_reason": f"Removed after {self.settings.job_retention_days} day retention window.",
                        "updated_at": job.get("updated_at"),
                    }
                    self.repository.record_action("prediction_job_retention_deleted", "prediction_job", job["id"], payload)
                    self._commit_session()
                    removed_count += 1
            except Exception as exc:
                self.rollback_session()
                logger.exception("Unable to delete expired prediction job %s. error=%s", job.get("id"), exc)

        return removed_count

    def stop_job(self, job_id: str) -> Dict[str, Any]:
        job = self.repository.get_prediction_job(job_id)
        if job is None:
            raise KeyError(job_id)

        status = str(job.get("status") or "").lower()
        if status == JobStatus.STOPPED.value:
            return self._decorate_prediction_job(job)
        if status == JobStatus.QUEUED.value:
            return self._mark_stopped(job_id)
        if status == JobStatus.RUNNING.value:
            progress = job.get("progress") or {}
            details = dict(progress.get("details") or {})
            details["stop_requested"] = True
            stopping = self.repository.update_prediction_job(
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
            self.repository.record_action("prediction_job_stop_requested", "prediction_job", job_id, stopping)
            self._commit_session()
            return self._decorate_prediction_job(stopping)
        if status == JobStatus.STOPPING.value:
            return self._decorate_prediction_job(job)
        raise ValueError("Only queued or running prediction jobs can be stopped.")

    def run_job(self, job_id: str) -> Dict[str, Any]:
        job = self.repository.get_prediction_job(job_id)
        if job is None:
            raise KeyError(job_id)
        if is_shutdown_requested():
            return self._mark_stopped(job_id, self._stop_reason())
        if str(job.get("status") or "").lower() == JobStatus.STOPPED.value:
            return self._decorate_prediction_job(job)
        if str(job.get("status") or "").lower() == JobStatus.STOPPING.value:
            return self._mark_stopped(job_id, self._stop_reason())
        spec = job.get("spec") or {}
        resolved_target = self._resolve_prediction_target(
            import_job_id=spec.get("import_job_id"),
            source_name=spec.get("source_name"),
            audience_scope=spec.get("audience_scope"),
        )
        import_job_id = resolved_target["import_job_id"]

        mode = str(job["spec"].get("prediction_mode", "local")).lower()
        history_snapshot_at = datetime.utcnow().isoformat()
        self.repository.update_prediction_job(
            job_id,
            {
                "status": JobStatus.RUNNING.value,
                "error": None,
                "import_job_id": import_job_id,
                "spec": {
                    **spec,
                    "import_job_id": import_job_id,
                    "audience_scope": resolved_target["audience_scope"],
                    "source_name": resolved_target["source_name"],
                },
            },
        )
        self._commit_session()

        try:
            gemini_client = (
                self._build_gemini_client(job_id, reset_circuit=True)
                if self._mode_uses_gemini(mode)
                else None
            )
            self.bigquery_service.replace_prediction_results(job_id=job_id, rows=[])
            execution_details = self._prediction_execution_details(mode, gemini_available=gemini_client is not None)
            active_model = self.local_models.get_latest_model_payload()
            local_model_readiness = self.local_models.get_model_readiness()
            policy_snapshot = self.experiments.get_latest_policy_snapshot()

            modeling_engine = PlayerModelingEngine(
                gemini_client=gemini_client,
                bigquery_service=self.bigquery_service,
                job_id=None,
            )
            decision_engine = GrowthDecisionEngine(gemini_client)
            player_ids = self.bigquery_service.get_import_roster_player_ids(import_job_id)
            total = len(player_ids)
            rows_written = 0

            self.repository.update_prediction_job(
                job_id,
                {
                    "progress": {
                        "current": 0,
                        "total": total,
                        "pct": 0.0,
                        "details": {
                            "rows_written": 0,
                            "import_job_id": import_job_id,
                            **self._build_prediction_audience_details(resolved_target),
                            "prediction_mode": mode,
                            "history_scope": "tenant_merged",
                            "history_snapshot_at": history_snapshot_at,
                            "stale": False,
                            "stale_reason": "",
                            **self._build_local_model_details(local_model_readiness),
                            **execution_details,
                        },
                    }
                },
            )
            self._commit_session()

            for index, player_id in enumerate(player_ids, start=1):
                if self._should_stop(job_id):
                    return self._mark_stopped(job_id, self._stop_reason())

                profile = modeling_engine.build_player_profile(player_id)
                if not profile:
                    self.repository.update_prediction_job(
                        job_id,
                        {
                            "progress": {
                                "current": index,
                                "total": total,
                                "pct": (index / total * 100.0) if total else 100.0,
                                "details": {
                                    "rows_written": rows_written,
                                    "import_job_id": import_job_id,
                                    **self._build_prediction_audience_details(resolved_target),
                                    "prediction_mode": mode,
                                    "history_scope": "tenant_merged",
                                    "history_snapshot_at": history_snapshot_at,
                                    "stale": False,
                                    "stale_reason": "",
                                    "last_user_id": str(player_id),
                                    **self._build_local_model_details(local_model_readiness),
                                    **execution_details,
                                },
                            }
                        },
                    )
                    self._commit_session()
                    continue

                model_score = self.local_models.score_profile(profile)
                churn_estimate, prediction_source = self._estimate_prediction(job_id, mode, modeling_engine, player_id, profile)
                churn_estimate = self._merge_model_score(churn_estimate, model_score)
                if prediction_source == "local" and model_score.model_status == "active":
                    prediction_source = "local_model"
                if self._should_stop(job_id):
                    return self._mark_stopped(job_id, self._stop_reason())

                recommendation = self.experiments.recommend_policy_action(
                    baseline_churn_score=model_score.baseline_churn_score,
                    policy_snapshot=policy_snapshot,
                )
                next_action = (
                    self._build_recommended_action_from_policy(recommendation)
                    or
                    self._interruptible_call(
                        job_id,
                        operation="next_action",
                        callback=lambda: decision_engine.decide_next_action(profile, churn_estimate, "reduce_churn"),
                    )
                    or {"content": "No action suggested."}
                )
                if self._should_stop(job_id):
                    return self._mark_stopped(job_id, self._stop_reason())

                row = {
                    "prediction_job_id": job_id,
                    "import_job_id": import_job_id,
                    "completed_at": datetime.utcnow().isoformat(),
                    "user_id": str(player_id),
                    "canonical_user_id": str(profile.get("canonical_user_id") or player_id),
                    "email": profile.get("email"),
                    "ltv": profile.get("total_revenue", 0.0),
                    "session_count": profile.get("total_sessions", 0),
                    "event_count": profile.get("total_events", 0),
                    "days_since_last_seen": profile.get("days_since_last_seen", 0),
                    "churn_state": churn_estimate.get("churn_state", profile.get("churn_state", "active")),
                    "predicted_churn_risk": churn_estimate.get("churn_risk", "unknown"),
                    "churn_reason": churn_estimate.get("reason", "unknown"),
                    "top_signals": churn_estimate.get("top_signals", []),
                    "prediction_source": prediction_source,
                    "suggested_action": next_action.get("content", "No action suggested."),
                    "baseline_churn_score": round(model_score.baseline_churn_score, 4),
                    "model_version": model_score.model_version,
                    "score_timestamp": model_score.score_timestamp,
                    "effective_local_model_version": local_model_readiness.get("using_model_version"),
                    "effective_local_model_state": local_model_readiness.get("state"),
                    "eligibility_reason": recommendation.get("eligibility_reason"),
                    "recommended_template_id": recommendation.get("recommended_template_id"),
                    "recommended_variant": recommendation.get("recommended_variant"),
                    "policy_snapshot_id": recommendation.get("policy_snapshot_id"),
                    "policy_status": recommendation.get("policy_status"),
                }
                self.bigquery_service.append_prediction_results(job_id=job_id, rows=[row])
                rows_written += 1
                self.repository.update_prediction_job(
                    job_id,
                    {
                        "progress": {
                            "current": index,
                            "total": total,
                            "pct": (index / total * 100.0) if total else 100.0,
                            "details": {
                                "rows_written": rows_written,
                                "import_job_id": import_job_id,
                                **self._build_prediction_audience_details(resolved_target),
                                "prediction_mode": mode,
                                "history_scope": "tenant_merged",
                                "history_snapshot_at": history_snapshot_at,
                                "stale": False,
                                "stale_reason": "",
                                "last_user_id": str(player_id),
                                "model_version": model_score.model_version,
                                "policy_snapshot_id": recommendation.get("policy_snapshot_id"),
                                **self._build_local_model_details(local_model_readiness),
                                **execution_details,
                            },
                        }
                    },
                )
                self._commit_session()

            completed = self.repository.update_prediction_job(
                job_id,
                {
                    "status": JobStatus.COMPLETED.value,
                    "progress": {
                        "current": total,
                        "total": total,
                        "pct": 100.0,
                        "details": {
                            "rows_written": rows_written,
                            "import_job_id": import_job_id,
                            **self._build_prediction_audience_details(resolved_target),
                            "prediction_mode": mode,
                            "history_scope": "tenant_merged",
                            "history_snapshot_at": history_snapshot_at,
                            "stale": False,
                            "stale_reason": "",
                            "model_version": str((active_model or {}).get("model_version") or "heuristic_v1"),
                            "policy_snapshot_id": str((policy_snapshot or {}).get("policy_snapshot_id") or ""),
                            **self._build_local_model_details(local_model_readiness),
                            **execution_details,
                        },
                    },
                },
            )
            self.repository.record_action("prediction_job_completed", "prediction_job", job_id, completed)
            self._commit_session()
            return self._decorate_prediction_job(completed)
        except Exception as exc:
            self.rollback_session()
            try:
                if self._should_stop(job_id) or isinstance(exc, PredictionInterruptedError):
                    return self._mark_stopped(job_id, self._stop_reason())
            except Exception:
                self.rollback_session()
            failed_job = self._safe_get_prediction_job(job_id) or job
            progress = failed_job.get("progress") or {}
            progress_details = dict(progress.get("details") or {})
            progress_details["failure_reason"] = str(exc)
            try:
                failed = self.repository.update_prediction_job(
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
                self.repository.record_action("prediction_job_failed", "prediction_job", job_id, failed)
                self._commit_session()
            except Exception:
                self.rollback_session()
                logger.exception("Unable to mark prediction job %s failed.", job_id)
            raise

    def _require_completed_import(self, import_job_id: str) -> Dict[str, Any]:
        import_job = self.repository.get_import_job(import_job_id)
        if import_job is None:
            raise MissingDependencyError(
                "import job",
                import_job_id,
                detail=f"Import job '{import_job_id}' required for prediction is missing.",
            )
        import_status = str(import_job.get("status") or "").lower()
        if import_status != JobStatus.COMPLETED.value:
            raise ResourceLockedError(
                f"Import job '{import_job_id}' is {import_status or 'unknown'} and cannot be used for prediction until completed."
            )
        return import_job

    def _active_export_dependencies(self, prediction_job_id: str) -> List[str]:
        blocking_statuses = {JobStatus.QUEUED.value, JobStatus.READY.value, JobStatus.RUNNING.value}
        return sorted(
            [
                str(item.get("id"))
                for item in self.repository.list_export_jobs()
                if str(item.get("prediction_job_id") or (item.get("spec") or {}).get("prediction_job_id") or "") == prediction_job_id
                and str(item.get("status") or "").lower() in blocking_statuses
            ]
        )

    def _has_configured_gemini(self) -> bool:
        if self._select_google_connector() is not None:
            return True
        return bool(str(os.getenv("GOOGLE_API_KEY") or "").strip())

    def _mode_uses_gemini(self, mode: str) -> bool:
        return str(mode or "local").lower() in {"ai", "parallel"}

    def _prediction_execution_details(self, mode: str, *, gemini_available: bool) -> Dict[str, str]:
        normalized_mode = str(mode or "local").lower()
        if normalized_mode == "cloud":
            return {"execution_mode": "cloud", "execution_label": "Cloud"}
        if normalized_mode == "parallel":
            return {
                "execution_mode": "parallel",
                "execution_label": "AI + Cloud" if gemini_available else "Parallel",
            }
        if normalized_mode == "ai" and gemini_available:
            return {"execution_mode": "ai", "execution_label": "AI"}
        return {"execution_mode": "local_model", "execution_label": "Local Model"}

    def _build_gemini_client(self, job_id: str, *, reset_circuit: bool = False) -> GeminiClient | None:
        connector = self._select_google_connector()
        if connector is not None:
            config = connector.get("config") or {}
            api_key = str(config.get("api_key") or "").strip()
            model_name = str(config.get("model_name") or "").strip() or None
            if api_key:
                try:
                    client = GeminiClient(
                        api_key=api_key,
                        model_name=model_name,
                        stop_checker=lambda: self._should_stop(job_id),
                        circuit_namespace="predictions",
                    )
                    if reset_circuit:
                        client.reset_circuit_breaker()
                    return client
                except Exception:
                    return None

        try:
            client = GeminiClient(
                stop_checker=lambda: self._should_stop(job_id),
                circuit_namespace="predictions",
            )
            if reset_circuit:
                client.reset_circuit_breaker()
            return client
        except Exception:
            return None

    def _select_google_connector(self) -> Dict[str, Any] | None:
        google_connectors = [
            connector
            for connector in self.repository.list_connectors()
            if str(connector.get("type") or "").lower() == "google"
            and str((connector.get("config") or {}).get("api_key") or "").strip()
        ]
        if not google_connectors:
            return None
        return max(google_connectors, key=self._connector_sort_key)

    def _connector_sort_key(self, connector: Dict[str, Any]) -> datetime:
        for field in ("updated_at", "created_at"):
            raw_value = connector.get(field)
            if not raw_value:
                continue
            try:
                return datetime.fromisoformat(str(raw_value))
            except ValueError:
                continue
        return datetime.min

    def _estimate_prediction(
        self,
        job_id: str,
        mode: str,
        modeling_engine: PlayerModelingEngine,
        player_id: Any,
        profile: Dict[str, Any],
    ) -> Tuple[Dict[str, Any], str]:
        local_estimate = None
        if mode in {"local", "parallel"}:
            local_estimate = self._run_local_estimate(job_id, modeling_engine, player_id, profile)
        if mode == "local":
            return local_estimate, "local"
        cloud_estimate = None
        if mode in {"cloud", "parallel"}:
            try:
                cloud_estimate = self._interruptible_call(
                    job_id,
                    operation="cloud_churn_prediction",
                    callback=lambda: CloudChurnService(
                        timeout_sec=self.settings.prediction_network_timeout_seconds
                    ).estimate_churn_risk(player_id, profile),
                )
            except CloudChurnRequestError:
                if mode == "cloud":
                    raise
                cloud_estimate = None
            except PredictionTimeoutError:
                if mode == "cloud":
                    raise
                cloud_estimate = None
        if mode == "cloud" and cloud_estimate:
            return cloud_estimate, "cloud"
        if cloud_estimate:
            return cloud_estimate, "cloud"
        return local_estimate or self._run_local_estimate(job_id, modeling_engine, player_id, profile), "local"

    def _run_local_estimate(
        self,
        job_id: str,
        modeling_engine: PlayerModelingEngine,
        player_id: Any,
        profile: Dict[str, Any],
    ) -> Dict[str, Any]:
        import asyncio

        if is_shutdown_requested():
            raise RuntimeError("Prediction interrupted by server shutdown.")
        return self._interruptible_call(
            job_id,
            operation="local_churn_prediction",
            callback=lambda: asyncio.run(modeling_engine.estimate_churn_risk(player_id, profile)),
        )

    @staticmethod
    def _merge_model_score(churn_estimate: Dict[str, Any], model_score) -> Dict[str, Any]:
        payload = dict(churn_estimate or {})
        payload["baseline_churn_score"] = round(float(model_score.baseline_churn_score), 4)
        payload["model_version"] = model_score.model_version
        payload["score_timestamp"] = model_score.score_timestamp
        if model_score.model_status == "active":
            payload["churn_risk"] = model_score.predicted_churn_risk
            payload["reason"] = (
                f"Local supervised baseline model predicted {round(model_score.baseline_churn_score, 4)} "
                f"for 7d non-return. {payload.get('reason', '')}".strip()
            )
        return payload

    @staticmethod
    def _build_recommended_action_from_policy(recommendation: Dict[str, Any]) -> Dict[str, Any] | None:
        if recommendation.get("eligible") is not True:
            return None
        content = str(recommendation.get("content") or "").strip()
        if not content:
            return None
        return {
            "channel": recommendation.get("channel") or "push_notification",
            "content": content,
            "subject": recommendation.get("subject"),
            "template_id": recommendation.get("recommended_template_id"),
            "variant_id": recommendation.get("recommended_variant"),
        }

    @staticmethod
    def _build_local_model_details(readiness: Dict[str, Any] | None) -> Dict[str, Any]:
        payload = dict(readiness or {})
        return {
            "effective_local_model_version": str(payload.get("using_model_version") or "heuristic_v1"),
            "effective_local_model_state": str(payload.get("state") or "untrained"),
            "effective_local_model_reason": str(payload.get("reason") or "").strip(),
        }
