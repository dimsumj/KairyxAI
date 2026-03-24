from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any, Dict, List

import requests

from app.application.secret_refs import materialize_secret_refs
from app.core.errors import MissingDependencyError, ResourceLockedError
from app.domain.jobs import JobStatus
from bigquery_service import BigQueryService, get_shared_bigquery_service
from pubsub_service import PubSubService


class ExportService:
    def __init__(self, repository, settings, bigquery_service: BigQueryService | None = None):
        self.repository = repository
        self.settings = settings
        self.bigquery_service = bigquery_service or get_shared_bigquery_service()

    def create_job(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        self._require_completed_prediction(str(payload["prediction_job_id"]))
        active_jobs = [
            job
            for job in self.repository.list_export_jobs()
            if str(job.get("status") or "").lower() not in {JobStatus.COMPLETED.value, JobStatus.FAILED.value, JobStatus.STOPPED.value, JobStatus.CANCELLED.value}
        ]
        if len(active_jobs) >= int(self.settings.max_export_jobs_per_tenant):
            raise ValueError(f"Export job limit reached for tenant; max active jobs is {self.settings.max_export_jobs_per_tenant}.")
        if self.settings.app_env == "prod" and payload.get("webhook_token"):
            raise ValueError("Inline webhook_token is not allowed in production; use provider_connection_id with secret refs.")
        job = self.repository.create_export_job(
            {
                "id": f"exp_{uuid.uuid4().hex[:20]}",
                "prediction_job_id": payload["prediction_job_id"],
                "status": JobStatus.QUEUED.value,
                "spec": payload,
                "progress": {"current": 0, "total": 0, "pct": 0.0, "details": {}},
            }
        )
        self.repository.record_action("export_job_created", "export_job", job["id"], job)
        PubSubService(topic_name=self.settings.export_command_topic).publish({"job_id": job["id"]}, attributes={"job_type": "export"})
        return job

    def list_jobs(self) -> List[Dict[str, Any]]:
        return self.repository.list_export_jobs()

    def get_job(self, job_id: str) -> Dict[str, Any] | None:
        return self.repository.get_export_job(job_id)

    def list_diagnostics(self, job_id: str) -> List[Dict[str, Any]]:
        if self.repository.get_export_job(job_id) is None:
            raise KeyError(job_id)
        items = []
        for record in self.repository.list_resources("export_diagnostic"):
            payload = record.get("payload") or {}
            if str(payload.get("job_id") or "") == job_id:
                items.append(payload)
        return items

    def retry_job(self, job_id: str) -> Dict[str, Any]:
        job = self.repository.get_export_job(job_id)
        if job is None:
            raise KeyError(job_id)
        if str(job.get("status") or "").lower() == JobStatus.RUNNING.value:
            raise ValueError("Cannot retry a running export job.")
        current_details = ((job.get("progress") or {}).get("details") or {})
        current_details["retry_requested_at"] = datetime.utcnow().isoformat()
        self.repository.update_export_job(
            job_id,
            {
                "status": JobStatus.READY.value,
                "progress": {
                    **(job.get("progress") or {}),
                    "details": current_details,
                },
                "error": None,
            },
        )
        return self.run_job(job_id)

    def run_job(self, job_id: str) -> Dict[str, Any]:
        job = self.repository.get_export_job(job_id)
        if job is None:
            raise KeyError(job_id)
        prediction_job_id = str((job.get("spec") or {}).get("prediction_job_id") or "")
        if prediction_job_id:
            self._require_completed_prediction(prediction_job_id)
        existing_details = (((job.get("progress") or {}).get("details")) or {})
        attempt = int(existing_details.get("attempt") or 0) + 1
        self.repository.update_export_job(job_id, {"status": JobStatus.RUNNING.value})

        spec = job["spec"]
        page = 1
        all_rows: List[Dict[str, Any]] = []
        while True:
            batch = self.bigquery_service.list_prediction_results(
                job_id=spec["prediction_job_id"],
                page=page,
                page_size=self.settings.export_batch_size,
            )
            rows = batch["items"]
            if not rows:
                break
            all_rows.extend(rows)
            if len(all_rows) >= batch["total"]:
                break
            page += 1

        filtered_rows = self._filter_rows(all_rows, spec.get("include_churned", False), spec.get("include_risks") or [])
        diagnostic_request = {
            "provider": str(spec.get("provider", "webhook")).lower(),
            "channel": spec.get("channel", "push_notification"),
            "audience_name": spec.get("audience_name"),
            "count": len(filtered_rows),
            "sample_user_ids": [row.get("user_id") for row in filtered_rows[:5] if row.get("user_id")],
        }
        try:
            result = self._dispatch_export(spec, filtered_rows)
        except Exception as exc:
            diagnostic = self._record_diagnostic(
                job_id,
                attempt=attempt,
                request_payload=diagnostic_request,
                response_payload={"error": str(exc)},
                status="failed",
            )
            failed = self.repository.update_export_job(
                job_id,
                {
                    "status": JobStatus.FAILED.value,
                    "error": str(exc),
                    "progress": {
                        "current": 0,
                        "total": len(filtered_rows),
                        "pct": 0.0,
                        "details": {
                            **existing_details,
                            "attempt": attempt,
                            "latest_diagnostic_id": diagnostic["diagnostic_id"],
                        },
                    },
                },
            )
            self.repository.record_action("export_job_failed", "export_job", job_id, failed)
            return failed
        diagnostic = self._record_diagnostic(
            job_id,
            attempt=attempt,
            request_payload=diagnostic_request,
            response_payload=result,
            status="completed",
        )

        completed = self.repository.update_export_job(
            job_id,
            {
                "status": JobStatus.COMPLETED.value,
                "progress": {
                    "current": len(filtered_rows),
                    "total": len(filtered_rows),
                    "pct": 100.0,
                    "details": {
                        **result,
                        "attempt": attempt,
                        "latest_diagnostic_id": diagnostic["diagnostic_id"],
                    },
                },
            },
        )
        self.repository.record_action("export_job_completed", "export_job", job_id, completed)
        return completed

    def _record_diagnostic(
        self,
        job_id: str,
        *,
        attempt: int,
        request_payload: Dict[str, Any],
        response_payload: Dict[str, Any],
        status: str,
    ) -> Dict[str, Any]:
        diagnostic_id = f"expdiag_{uuid.uuid4().hex[:20]}"
        payload = {
            "diagnostic_id": diagnostic_id,
            "job_id": job_id,
            "attempt": int(attempt),
            "status": status,
            "request": request_payload,
            "response": response_payload,
            "recorded_at": datetime.utcnow().isoformat(),
        }
        self.repository.upsert_resource(
            "export_diagnostic",
            diagnostic_id,
            status=status,
            name=job_id,
            payload=payload,
        )
        return payload

    def _filter_rows(self, rows: List[Dict[str, Any]], include_churned: bool, include_risks: List[str]) -> List[Dict[str, Any]]:
        risk_set = {str(value).lower() for value in include_risks} or {"high", "medium"}
        filtered = []
        for row in rows:
            churn_state = str(row.get("churn_state", "")).lower()
            risk = str(row.get("predicted_churn_risk", "")).lower()
            if include_churned and churn_state == "churned":
                filtered.append(row)
                continue
            if risk in risk_set:
                filtered.append(row)
        return filtered

    def _dispatch_export(self, spec: Dict[str, Any], rows: List[Dict[str, Any]]) -> Dict[str, Any]:
        provider = str(spec.get("provider", "webhook")).lower()
        payload = {
            "provider": provider,
            "channel": spec.get("channel", "push_notification"),
            "audience_name": spec.get("audience_name"),
            "count": len(rows),
            "rows": rows,
        }
        if provider == "webhook":
            provider_connection = self._resolve_provider_connection(spec)
            webhook_url = spec.get("webhook_url") or provider_connection.get("webhook_url")
            if not webhook_url:
                raise ValueError("webhook_url is required for webhook exports")
            headers = {"Content-Type": "application/json"}
            webhook_token = spec.get("webhook_token") or provider_connection.get("webhook_token")
            if webhook_token:
                headers["Authorization"] = f"Bearer {webhook_token}"
            response = requests.post(webhook_url, json=payload, headers=headers, timeout=30)
            response.raise_for_status()
            return {"provider": provider, "status_code": response.status_code, "count": len(rows)}

        provider_connection = self._resolve_provider_connection(spec)
        connector_name = "SendGrid" if provider == "sendgrid" else "Braze"
        connector = self.repository.get_connector(connector_name)
        if connector is None:
            connector = next((item for item in self.repository.list_connectors() if item.get("type") == provider), None)
        connection_config = materialize_secret_refs(dict(connector["config"] or {})) if connector is not None else {}
        connection_config.update(provider_connection)

        if provider == "sendgrid":
            api_key = connection_config.get("api_key")
            contacts = [{"email": row.get("email"), "external_id": row.get("user_id")} for row in rows if row.get("email")]
            response = requests.put(
                "https://api.sendgrid.com/v3/marketing/contacts",
                headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
                json={"contacts": contacts},
                timeout=30,
            )
            response.raise_for_status()
            return {"provider": provider, "status_code": response.status_code, "count": len(contacts)}

        rest_endpoint = str(connection_config.get("rest_endpoint", "")).rstrip("/")
        api_key = connection_config.get("api_key")
        attributes = [
            {
                "external_id": row.get("user_id"),
                "email": row.get("email"),
                "kairyx_predicted_churn_risk": row.get("predicted_churn_risk"),
                "kairyx_suggested_action": row.get("suggested_action"),
            }
            for row in rows
            if row.get("user_id")
        ]
        response = requests.post(
            f"{rest_endpoint}/users/track",
            headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
            json={"attributes": attributes},
            timeout=30,
        )
        response.raise_for_status()
        return {"provider": provider, "status_code": response.status_code, "count": len(attributes)}

    def _require_completed_prediction(self, prediction_job_id: str) -> Dict[str, Any]:
        prediction_job = self.repository.get_prediction_job(prediction_job_id)
        if prediction_job is None:
            raise MissingDependencyError(
                "prediction job",
                prediction_job_id,
                detail=f"Prediction job '{prediction_job_id}' required for export is missing.",
            )
        prediction_status = str(prediction_job.get("status") or "").lower()
        if prediction_status != JobStatus.COMPLETED.value:
            raise ResourceLockedError(
                f"Prediction job '{prediction_job_id}' is {prediction_status or 'unknown'} and cannot be used for export until completed."
            )
        return prediction_job

    def _resolve_provider_connection(self, spec: Dict[str, Any]) -> Dict[str, Any]:
        provider_connection_id = str(spec.get("provider_connection_id") or "").strip()
        if not provider_connection_id:
            return materialize_secret_refs(spec)
        record = self.repository.get_resource("provider_connection", provider_connection_id)
        if record is None:
            raise ValueError(f"Provider connection '{provider_connection_id}' was not found.")
        payload = dict((record.get("payload") or {}).get("config") or {})
        payload["provider_connection_id"] = provider_connection_id
        return materialize_secret_refs(payload)
