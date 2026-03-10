from __future__ import annotations

from fastapi import APIRouter

from app.core.settings import get_settings
from bigquery_service import get_shared_bigquery_service


router = APIRouter(tags=["health"])


@router.get("/health")
def health():
    settings = get_settings()
    bigquery_service = get_shared_bigquery_service()
    payload = {
        "status": "ok",
        "service": settings.app_name,
        "mode": settings.data_backend_mode,
        "data_aliases": bigquery_service.get_v1_table_aliases(),
    }
    if settings.data_backend_mode == "mock":
        payload["local_cache"] = bigquery_service.get_local_cache_stats()
    return payload
