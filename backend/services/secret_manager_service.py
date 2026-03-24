from __future__ import annotations

import os
from functools import lru_cache


class SecretManagerService:
    def resolve_secret(self, secret_ref: str | None) -> str | None:
        if not secret_ref:
            return None
        ref = str(secret_ref).strip()
        if not ref:
            return None
        if ref.startswith("env://"):
            return os.getenv(ref[len("env://"):])
        if ref.startswith("gsm://"):
            return self._resolve_gsm_secret(ref[len("gsm://"):])
        if ref.startswith("projects/"):
            return self._resolve_gsm_secret(ref)
        return ref

    @lru_cache(maxsize=128)
    def _resolve_gsm_secret(self, ref: str) -> str:
        try:
            from google.cloud import secretmanager
        except ImportError as exc:
            raise RuntimeError("google-cloud-secret-manager is required for gsm:// secret references.") from exc
        name = ref
        if not name.startswith("projects/"):
            project_id = os.getenv("GCP_SECRET_PROJECT_ID") or os.getenv("GCP_PROJECT_ID") or os.getenv("GOOGLE_CLOUD_PROJECT")
            if not project_id:
                raise RuntimeError("GCP secret project is not configured for gsm:// secret references.")
            name = f"projects/{project_id}/secrets/{ref}/versions/latest"
        client = secretmanager.SecretManagerServiceClient()
        response = client.access_secret_version(request={"name": name})
        return response.payload.data.decode("utf-8")
