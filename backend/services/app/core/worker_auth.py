from __future__ import annotations

import secrets

from fastapi import HTTPException, Request

from .settings import get_settings


def _authorization_bearer_token(request: Request) -> str:
    authorization = str(request.headers.get("authorization") or "").strip()
    if not authorization.lower().startswith("bearer "):
        return ""
    return authorization.split(" ", 1)[1].strip()


def require_worker_auth(request: Request) -> None:
    settings = get_settings()
    expected_token = str(settings.worker_shared_token or "").strip()
    if not expected_token:
        raise HTTPException(status_code=503, detail="WORKER_SHARED_TOKEN is not configured.")

    # Query parameter takes precedence so provider-managed Authorization headers
    # can coexist with a shared worker token on the request URL.
    provided_token = str(request.query_params.get("token") or "").strip() or _authorization_bearer_token(request)
    if not provided_token or not secrets.compare_digest(provided_token, expected_token):
        raise HTTPException(status_code=401, detail="Worker token is missing or invalid.")
