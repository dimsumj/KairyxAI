from __future__ import annotations

import uuid
from typing import Any, Dict, List

import requests

from app.domain.jobs import JobStatus
from bigquery_service import BigQueryService
from pubsub_service import PubSubService


class ExportService:
    def __init__(self, repository, settings):
        self.repository = repository
        self.settings = settings

    def create_job(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        prediction_job = self.repository.get_prediction_job(payload["prediction_job_id"])
        if prediction_job is None:
            raise KeyError(payload["prediction_job_id"])
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

    def run_job(self, job_id: str) -> Dict[str, Any]:
        job = self.repository.get_export_job(job_id)
        if job is None:
            raise KeyError(job_id)
        self.repository.update_export_job(job_id, {"status": JobStatus.RUNNING.value})

        spec = job["spec"]
        page = 1
        all_rows: List[Dict[str, Any]] = []
        while True:
            batch = BigQueryService().list_prediction_results(job_id=spec["prediction_job_id"], page=page, page_size=self.settings.export_batch_size)
            rows = batch["items"]
            if not rows:
                break
            all_rows.extend(rows)
            if len(all_rows) >= batch["total"]:
                break
            page += 1

        filtered_rows = self._filter_rows(all_rows, spec.get("include_churned", False), spec.get("include_risks") or [])
        result = self._dispatch_export(spec, filtered_rows)

        completed = self.repository.update_export_job(
            job_id,
            {
                "status": JobStatus.COMPLETED.value,
                "progress": {
                    "current": len(filtered_rows),
                    "total": len(filtered_rows),
                    "pct": 100.0,
                    "details": result,
                },
            },
        )
        self.repository.record_action("export_job_completed", "export_job", job_id, completed)
        return completed

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
            webhook_url = spec.get("webhook_url")
            if not webhook_url:
                raise ValueError("webhook_url is required for webhook exports")
            headers = {"Content-Type": "application/json"}
            if spec.get("webhook_token"):
                headers["Authorization"] = f"Bearer {spec['webhook_token']}"
            response = requests.post(webhook_url, json=payload, headers=headers, timeout=30)
            response.raise_for_status()
            return {"provider": provider, "status_code": response.status_code, "count": len(rows)}

        connector_name = "SendGrid" if provider == "sendgrid" else "Braze"
        connector = self.repository.get_connector(connector_name)
        if connector is None:
            connector = next(
                (item for item in self.repository.list_connectors() if item.get("type") == provider),
                None,
            )
        if connector is None:
            raise ValueError(f"{connector_name} connector is not configured")

        if provider == "sendgrid":
            api_key = connector["config"].get("api_key")
            contacts = [{"email": row.get("email"), "external_id": row.get("user_id")} for row in rows if row.get("email")]
            response = requests.put(
                "https://api.sendgrid.com/v3/marketing/contacts",
                headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
                json={"contacts": contacts},
                timeout=30,
            )
            response.raise_for_status()
            return {"provider": provider, "status_code": response.status_code, "count": len(contacts)}

        rest_endpoint = str(connector["config"].get("rest_endpoint", "")).rstrip("/")
        api_key = connector["config"].get("api_key")
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
