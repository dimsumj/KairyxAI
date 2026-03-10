from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Dict, List

from bigquery_service import BigQueryService, get_shared_bigquery_service

from app.application.copilot import CopilotService
from app.application.health_monitor import HealthMonitorService
from app.application.workflows import WorkflowService
from app.core.settings import Settings


class ControlLoopService:
    def __init__(self, repository, settings: Settings, bigquery_service: BigQueryService | None = None):
        self.repository = repository
        self.settings = settings
        self.bigquery_service = bigquery_service or get_shared_bigquery_service()
        self.health = HealthMonitorService(repository, self.bigquery_service)
        self.workflows = WorkflowService(repository)
        self.copilot = CopilotService(repository, self.bigquery_service)

    def ensure_default_jobs(self) -> List[Dict[str, Any]]:
        definitions = [
            {
                "job_id": "health_refresh",
                "name": "Health Refresh",
                "job_type": "health_refresh",
                "schedule": {"type": "interval", "seconds": self.settings.scheduler_interval_seconds},
            },
            {
                "job_id": "due_workflow_runner",
                "name": "Due Workflow Runner",
                "job_type": "workflow_run_due",
                "schedule": {"type": "interval", "seconds": self.settings.scheduler_interval_seconds},
            },
            {
                "job_id": "daily_copilot_report",
                "name": "Daily Copilot Report",
                "job_type": "copilot_report",
                "report_type": "daily",
                "time_window": "7d",
                "schedule": {"type": "daily", "hour": self.settings.scheduler_daily_report_hour, "minute": 0},
            },
            {
                "job_id": "weekly_closed_loop_report",
                "name": "Weekly Closed-Loop Report",
                "job_type": "copilot_report",
                "report_type": "weekly",
                "time_window": "7d",
                "schedule": {
                    "type": "weekly",
                    "weekday": self.settings.scheduler_weekly_report_weekday,
                    "hour": self.settings.scheduler_weekly_report_hour,
                    "minute": 0,
                },
            },
        ]
        created = []
        for definition in definitions:
            existing = self.repository.get_resource("scheduler_job", definition["job_id"])
            payload = dict((existing or {}).get("payload") or {})
            if not payload:
                payload = {
                    **definition,
                    "status": "ready",
                    "last_run_at": None,
                    "last_status": None,
                    "last_result_summary": {},
                }
            else:
                payload.update({key: value for key, value in definition.items() if key not in {"job_id"}})
            payload["next_run_hint"] = self._next_run_hint(payload, self._parse_reference_time(None))
            record = self.repository.upsert_resource(
                "scheduler_job",
                definition["job_id"],
                status=str(payload.get("status") or "ready"),
                name=definition["name"],
                payload=payload,
            )
            created.append(record.get("payload") or payload)
        return created

    def list_jobs(self) -> List[Dict[str, Any]]:
        self.ensure_default_jobs()
        return [item.get("payload") or {} for item in self.repository.list_resources("scheduler_job")]

    def tick(self, *, reference_time: str | None = None) -> Dict[str, Any]:
        resolved_time = self._parse_reference_time(reference_time)
        self.ensure_default_jobs()
        results = [
            self._run_health_refresh(resolved_time),
            self._run_due_workflow_job(resolved_time),
            self._run_report_job("daily_copilot_report", resolved_time),
            self._run_report_job("weekly_closed_loop_report", resolved_time),
        ]
        return {
            "executed_at": resolved_time.isoformat(),
            "items": results,
        }

    def _run_health_refresh(self, resolved_time: datetime) -> Dict[str, Any]:
        snapshot = self.health.snapshot(reference_time=resolved_time.isoformat(), persist=True)
        return self._mark_job_run(
            "health_refresh",
            resolved_time,
            status="completed",
            result_summary={
                "alerts_open": len([item for item in snapshot.get("alerts", []) if str(item.get("status") or "open") == "open"]),
                "modules": len(snapshot.get("modules") or {}),
            },
        )

    def _run_due_workflow_job(self, resolved_time: datetime) -> Dict[str, Any]:
        job = self._get_job_payload("due_workflow_runner")
        if not self._interval_due(job, resolved_time):
            return self._skip_job(job, resolved_time, "not_due")
        payload = self.workflows.run_due_workflows(reference_time=resolved_time.isoformat(), limit_per_workflow=100)
        return self._mark_job_run(
            "due_workflow_runner",
            resolved_time,
            status="completed",
            result_summary={"workflow_runs": len(payload.get("items") or [])},
        )

    def _run_report_job(self, job_id: str, resolved_time: datetime) -> Dict[str, Any]:
        job = self._get_job_payload(job_id)
        if not self._schedule_due(job, resolved_time):
            return self._skip_job(job, resolved_time, "not_due")
        response = self.copilot.report(str(job.get("report_type") or "daily"), time_window=str(job.get("time_window") or "7d"))
        return self._mark_job_run(
            job_id,
            resolved_time,
            status="completed" if response.get("conclusion") != "insufficient_evidence" else "insufficient_evidence",
            result_summary={"report_id": response.get("report_id"), "conclusion": response.get("conclusion")},
        )

    def _mark_job_run(self, job_id: str, resolved_time: datetime, *, status: str, result_summary: Dict[str, Any]) -> Dict[str, Any]:
        payload = self._get_job_payload(job_id)
        payload["status"] = "ready"
        payload["last_status"] = status
        payload["last_run_at"] = resolved_time.isoformat()
        payload["last_result_summary"] = result_summary
        payload["next_run_hint"] = self._next_run_hint(payload, resolved_time)
        self.repository.upsert_resource("scheduler_job", job_id, status="ready", name=payload.get("name"), payload=payload)
        self.repository.record_resource_event(
            "scheduler_job",
            job_id,
            event_type="job_run",
            payload={"job_id": job_id, "status": status, "executed_at": resolved_time.isoformat(), "result_summary": result_summary},
        )
        return {
            "job_id": job_id,
            "status": status,
            "executed_at": resolved_time.isoformat(),
            "result_summary": result_summary,
        }

    def _skip_job(self, job: Dict[str, Any], resolved_time: datetime, reason: str) -> Dict[str, Any]:
        job_id = str(job.get("job_id") or "")
        next_hint = self._next_run_hint(job, resolved_time)
        if job_id:
            job["next_run_hint"] = next_hint
            self.repository.upsert_resource("scheduler_job", job_id, status="ready", name=job.get("name"), payload=job)
        return {
            "job_id": job_id,
            "status": "skipped",
            "reason": reason,
            "executed_at": resolved_time.isoformat(),
            "next_run_hint": next_hint,
        }

    def _get_job_payload(self, job_id: str) -> Dict[str, Any]:
        record = self.repository.get_resource("scheduler_job", job_id)
        if record is None:
            self.ensure_default_jobs()
            record = self.repository.get_resource("scheduler_job", job_id)
        return dict((record or {}).get("payload") or {})

    @staticmethod
    def _parse_reference_time(reference_time: str | None) -> datetime:
        if not reference_time:
            return datetime.utcnow()
        try:
            return datetime.fromisoformat(str(reference_time))
        except Exception:
            return datetime.utcnow()

    @staticmethod
    def _parse_last_run(payload: Dict[str, Any]) -> datetime | None:
        raw_value = payload.get("last_run_at")
        if not raw_value:
            return None
        try:
            return datetime.fromisoformat(str(raw_value))
        except Exception:
            return None

    def _interval_due(self, payload: Dict[str, Any], resolved_time: datetime) -> bool:
        last_run = self._parse_last_run(payload)
        interval_seconds = int(((payload.get("schedule") or {}).get("seconds") or self.settings.scheduler_interval_seconds))
        if last_run is None:
            return True
        return (resolved_time - last_run).total_seconds() >= max(5, interval_seconds)

    def _schedule_due(self, payload: Dict[str, Any], resolved_time: datetime) -> bool:
        schedule = dict(payload.get("schedule") or {})
        schedule_type = str(schedule.get("type") or "interval")
        last_run = self._parse_last_run(payload)
        if schedule_type == "interval":
            return self._interval_due(payload, resolved_time)
        if schedule_type == "daily":
            target_hour = int(schedule.get("hour") or 0)
            target_minute = int(schedule.get("minute") or 0)
            if (resolved_time.hour, resolved_time.minute) < (target_hour, target_minute):
                return False
            return last_run is None or last_run.date() < resolved_time.date()
        if schedule_type == "weekly":
            target_weekday = int(schedule.get("weekday") or 0)
            target_hour = int(schedule.get("hour") or 0)
            target_minute = int(schedule.get("minute") or 0)
            if resolved_time.weekday() != target_weekday:
                return False
            if (resolved_time.hour, resolved_time.minute) < (target_hour, target_minute):
                return False
            if last_run is None:
                return True
            return (last_run.isocalendar().week, last_run.year) != (resolved_time.isocalendar().week, resolved_time.year)
        return False

    def _next_run_hint(self, payload: Dict[str, Any], resolved_time: datetime) -> str:
        schedule = dict(payload.get("schedule") or {})
        schedule_type = str(schedule.get("type") or "interval")
        if schedule_type == "interval":
            next_run = resolved_time + timedelta(seconds=int(schedule.get("seconds") or self.settings.scheduler_interval_seconds))
            return next_run.isoformat()
        if schedule_type == "daily":
            next_run = resolved_time.replace(hour=int(schedule.get("hour") or 0), minute=int(schedule.get("minute") or 0), second=0, microsecond=0)
            if next_run <= resolved_time:
                next_run += timedelta(days=1)
            return next_run.isoformat()
        if schedule_type == "weekly":
            next_run = resolved_time.replace(hour=int(schedule.get("hour") or 0), minute=int(schedule.get("minute") or 0), second=0, microsecond=0)
            target_weekday = int(schedule.get("weekday") or 0)
            days_ahead = (target_weekday - next_run.weekday()) % 7
            if days_ahead == 0 and next_run <= resolved_time:
                days_ahead = 7
            next_run += timedelta(days=days_ahead)
            return next_run.isoformat()
        return resolved_time.isoformat()
