from __future__ import annotations

import uuid
from typing import Any, Dict, List, Tuple

from app.domain.jobs import JobStatus
from bigquery_service import BigQueryService
from cloud_churn_service import CloudChurnService
from gemini_client import GeminiClient
from growth_decision_engine import GrowthDecisionEngine
from player_modeling_engine import PlayerModelingEngine
from pubsub_service import PubSubService


class PredictionService:
    def __init__(self, repository, settings):
        self.repository = repository
        self.settings = settings

    def create_job(self, import_job_id: str, prediction_mode: str = "local") -> Dict[str, Any]:
        import_job = self.repository.get_import_job(import_job_id)
        if import_job is None:
            raise KeyError(import_job_id)
        job = self.repository.create_prediction_job(
            {
                "id": f"pred_{uuid.uuid4().hex[:20]}",
                "import_job_id": import_job_id,
                "status": JobStatus.QUEUED.value,
                "spec": {
                    "import_job_id": import_job_id,
                    "prediction_mode": prediction_mode,
                },
                "progress": {"current": 0, "total": 0, "pct": 0.0, "details": {}},
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

    def run_job(self, job_id: str) -> Dict[str, Any]:
        job = self.repository.get_prediction_job(job_id)
        if job is None:
            raise KeyError(job_id)
        import_job_id = job["spec"]["import_job_id"]
        import_job = self.repository.get_import_job(import_job_id)
        if import_job is None:
            raise KeyError(import_job_id)

        mode = str(job["spec"].get("prediction_mode", "local")).lower()
        self.repository.update_prediction_job(job_id, {"status": JobStatus.RUNNING.value})

        gemini_client = None
        try:
            gemini_client = GeminiClient()
        except Exception:
            gemini_client = None

        bigquery_service = BigQueryService()
        modeling_engine = PlayerModelingEngine(
            gemini_client=gemini_client,
            bigquery_service=bigquery_service,
            job_id=import_job_id,
        )
        decision_engine = GrowthDecisionEngine(gemini_client)
        player_ids = modeling_engine.get_all_player_ids()

        rows: List[Dict[str, Any]] = []
        total = len(player_ids)
        for index, player_id in enumerate(player_ids, start=1):
            profile = modeling_engine.build_player_profile(player_id)
            if not profile:
                continue
            churn_estimate, prediction_source = self._estimate_prediction(mode, modeling_engine, player_id, profile)
            next_action = decision_engine.decide_next_action(profile, churn_estimate, "reduce_churn") or {
                "content": "No action suggested.",
            }
            rows.append(
                {
                    "prediction_job_id": job_id,
                    "import_job_id": import_job_id,
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
            )
            self.repository.update_prediction_job(
                job_id,
                {
                    "progress": {
                        "current": index,
                        "total": total,
                        "pct": (index / total * 100.0) if total else 100.0,
                        "details": {"rows_buffered": len(rows)},
                    }
                },
            )

        bigquery_service.replace_prediction_results(job_id=job_id, rows=rows)
        completed = self.repository.update_prediction_job(
            job_id,
            {
                "status": JobStatus.COMPLETED.value,
                "progress": {
                    "current": total,
                    "total": total,
                    "pct": 100.0,
                    "details": {"rows_written": len(rows), "import_job_id": import_job_id},
                },
            },
        )
        self.repository.record_action("prediction_job_completed", "prediction_job", job_id, completed)
        return completed

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

        return asyncio.run(modeling_engine.estimate_churn_risk(player_id, profile))
