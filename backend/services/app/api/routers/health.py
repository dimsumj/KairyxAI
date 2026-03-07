from __future__ import annotations

from fastapi import APIRouter

from app.core.settings import get_settings


router = APIRouter(tags=["health"])


@router.get("/health")
def health():
    settings = get_settings()
    return {
        "status": "ok",
        "service": settings.app_name,
        "mode": settings.data_backend_mode,
    }
