from __future__ import annotations

import logging
import os
import uuid
from datetime import datetime
from typing import Any, Dict, List, Tuple

from app.domain.jobs import JobStatus
from app.core.runtime import is_shutdown_requested
from bigquery_service import BigQueryService
from cloud_churn_service import CloudChurnService
from gemini_client import GeminiClient
from growth_decision_engine import GrowthDecisionEngine
from player_modeling_engine import PlayerModelingEngine
from pubsub_service import PubSubService


logger = logging.getLogger(__name__)


class PredictionService:
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

    def _mark_stopped(self, job_id: str, reason: str = "Stopped by user.") -> Dict[str, Any]:
        job = self.repository.get_prediction_job(job_id)
        if job is None:
            raise KeyError(job_id)
        if str(job.get("status") or "").lower() == JobStatus.STOPPED.value:
            return job

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
        return stopped

    def create_job(self, import_job_id: str, prediction_mode: str = "local") -> Dict[str, Any]:
        import_job = self.repository.get_import_job(import_job_id)
        if import_job is None:
            raise KeyError(import_job_id)
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
                    "prediction_mode": prediction_mode,
                },
                "progress": {
                    "current": 0,
                    "total": 0,
                    "pct": 0.0,
                    "details": {
                        "import_job_id": import_job_id,
                        "prediction_mode": str(prediction_mode or "local").lower(),
                        **execution_details,
                    },
                },
            }
        )
        self.repository.record_action("prediction_job_created", "prediction_job", job["id"], job)
        PubSubService(topic_name=self.settings.prediction_command_topic).publish({"job_id": job["id"]}, attributes={"job_type": "prediction"})
        return job

    def list_jobs(self) -> List[Dict[str, Any]]:
        return self.repository.list_prediction_jobs()

    def get_job(self, job_id: str) -> Dict[str, Any] | None:
        return self.repository.get_prediction_job(job_id)

    def list_results(self, job_id: str, page: int, page_size: int) -> Dict[str, Any]:
        service = BigQueryService()
        return service.list_prediction_results(job_id=job_id, page=page, page_size=page_size)

    def stop_job(self, job_id: str) -> Dict[str, Any]:
        job = self.repository.get_prediction_job(job_id)
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
            return stopping
        if status == JobStatus.STOPPING.value:
            return job
        raise ValueError("Only queued or running prediction jobs can be stopped.")

    def run_job(self, job_id: str) -> Dict[str, Any]:
        job = self.repository.get_prediction_job(job_id)
        if job is None:
            raise KeyError(job_id)
        if is_shutdown_requested():
            return self._mark_stopped(job_id, self._stop_reason())
        if str(job.get("status") or "").lower() == JobStatus.STOPPED.value:
            return job
        if str(job.get("status") or "").lower() == JobStatus.STOPPING.value:
            return self._mark_stopped(job_id, self._stop_reason())
        import_job_id = job["spec"]["import_job_id"]
        import_job = self.repository.get_import_job(import_job_id)
        if import_job is None:
            raise KeyError(import_job_id)

        mode = str(job["spec"].get("prediction_mode", "local")).lower()
        self.repository.update_prediction_job(job_id, {"status": JobStatus.RUNNING.value, "error": None})
        self._commit_session()

        try:
            gemini_client = self._build_gemini_client()
            bigquery_service = BigQueryService()
            bigquery_service.replace_prediction_results(job_id=job_id, rows=[])
            execution_details = self._prediction_execution_details(mode, gemini_available=gemini_client is not None)

            modeling_engine = PlayerModelingEngine(
                gemini_client=gemini_client,
                bigquery_service=bigquery_service,
                job_id=import_job_id,
            )
            decision_engine = GrowthDecisionEngine(gemini_client)
            player_ids = modeling_engine.get_all_player_ids()
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
                            "prediction_mode": mode,
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
                                    "prediction_mode": mode,
                                    "last_user_id": str(player_id),
                                    **execution_details,
                                },
                            }
                        },
                    )
                    self._commit_session()
                    continue

                churn_estimate, prediction_source = self._estimate_prediction(mode, modeling_engine, player_id, profile)
                if self._should_stop(job_id):
                    return self._mark_stopped(job_id, self._stop_reason())

                next_action = decision_engine.decide_next_action(profile, churn_estimate, "reduce_churn") or {
                    "content": "No action suggested.",
                }
                if self._should_stop(job_id):
                    return self._mark_stopped(job_id, self._stop_reason())

                row = {
                    "prediction_job_id": job_id,
                    "import_job_id": import_job_id,
                    "completed_at": datetime.utcnow().isoformat(),
                    "user_id": str(player_id),
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
                }
                bigquery_service.append_prediction_results(job_id=job_id, rows=[row])
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
                                "prediction_mode": mode,
                                "last_user_id": str(player_id),
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
                            "prediction_mode": mode,
                            **execution_details,
                        },
                    },
                },
            )
            self.repository.record_action("prediction_job_completed", "prediction_job", job_id, completed)
            self._commit_session()
            return completed
        except Exception as exc:
            self.rollback_session()
            try:
                if self._should_stop(job_id):
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

    def _has_configured_gemini(self) -> bool:
        if self._select_google_connector() is not None:
            return True
        return bool(str(os.getenv("GOOGLE_API_KEY") or "").strip())

    def _prediction_execution_details(self, mode: str, *, gemini_available: bool) -> Dict[str, str]:
        normalized_mode = str(mode or "local").lower()
        if normalized_mode == "cloud":
            return {"execution_mode": "cloud", "execution_label": "Cloud"}
        if normalized_mode == "parallel":
            return {
                "execution_mode": "parallel",
                "execution_label": "AI + Cloud" if gemini_available else "Parallel",
            }
        if gemini_available:
            return {"execution_mode": "ai", "execution_label": "AI"}
        return {"execution_mode": "local", "execution_label": "Local"}

    def _build_gemini_client(self) -> GeminiClient | None:
        connector = self._select_google_connector()
        if connector is not None:
            config = connector.get("config") or {}
            api_key = str(config.get("api_key") or "").strip()
            model_name = str(config.get("model_name") or "").strip() or None
            if api_key:
                try:
                    return GeminiClient(api_key=api_key, model_name=model_name)
                except Exception:
                    return None

        try:
            return GeminiClient()
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
        mode: str,
        modeling_engine: PlayerModelingEngine,
        player_id: Any,
        profile: Dict[str, Any],
    ) -> Tuple[Dict[str, Any], str]:
        local_estimate = None
        if mode in {"local", "parallel"}:
            local_estimate = self._run_local_estimate(modeling_engine, player_id, profile)
        if mode == "local":
            return local_estimate, "local"
        cloud_estimate = None
        if mode in {"cloud", "parallel"}:
            try:
                cloud_estimate = CloudChurnService().estimate_churn_risk(player_id, profile)
            except Exception:
                cloud_estimate = None
        if mode == "cloud" and cloud_estimate:
            return cloud_estimate, "cloud"
        if cloud_estimate:
            return cloud_estimate, "cloud"
        return local_estimate or self._run_local_estimate(modeling_engine, player_id, profile), "local"

    def _run_local_estimate(self, modeling_engine: PlayerModelingEngine, player_id: Any, profile: Dict[str, Any]) -> Dict[str, Any]:
        import asyncio

        if is_shutdown_requested():
            raise RuntimeError("Prediction interrupted by server shutdown.")
        return asyncio.run(modeling_engine.estimate_churn_risk(player_id, profile))
