from __future__ import annotations

from typing import Any, Dict

from app.application.secret_refs import redact_secret_values

from .common import JobProgress, JobResponse


def build_job_response(job: Dict[str, Any], *, base_path: str, extra_links: Dict[str, str] | None = None) -> JobResponse:
    links = {"self": f"{base_path}/{job['id']}"}
    if extra_links:
        links.update(extra_links)
    progress = job.get("progress") or {}
    details = redact_secret_values(progress.get("details") or {})
    return JobResponse(
        id=job["id"],
        type=job["type"],
        status=job["status"],
        tenant_id=job.get("tenant_id"),
        created_by=str(job.get("created_by") or "system"),
        updated_by=str(job.get("updated_by") or "system"),
        correlation_id=str(job.get("correlation_id") or ""),
        created_at=job["created_at"],
        updated_at=job["updated_at"],
        progress=JobProgress(
            current=int(progress.get("current", 0) or 0),
            total=int(progress.get("total", 0) or 0),
            pct=float(progress.get("pct", 0.0) or 0.0),
            details=details,
        ),
        error=job.get("error"),
        links=links,
        spec=redact_secret_values(job.get("spec") or {}),
        quality_report=details.get("quality_report") or {},
        checkpoint_state=details.get("checkpoint_state") or {},
        mapping_coverage=float(details.get("mapping_coverage") or 0.0),
    )
