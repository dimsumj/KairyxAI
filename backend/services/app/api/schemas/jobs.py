from __future__ import annotations

from typing import Any, Dict

from .common import JobProgress, JobResponse


def build_job_response(job: Dict[str, Any], *, base_path: str, extra_links: Dict[str, str] | None = None) -> JobResponse:
    links = {"self": f"{base_path}/{job['id']}"}
    if extra_links:
        links.update(extra_links)
    progress = job.get("progress") or {}
    return JobResponse(
        id=job["id"],
        type=job["type"],
        status=job["status"],
        created_at=job["created_at"],
        updated_at=job["updated_at"],
        progress=JobProgress(
            current=int(progress.get("current", 0) or 0),
            total=int(progress.get("total", 0) or 0),
            pct=float(progress.get("pct", 0.0) or 0.0),
            details=progress.get("details") or {},
        ),
        error=job.get("error"),
        links=links,
        spec=job.get("spec") or {},
    )
