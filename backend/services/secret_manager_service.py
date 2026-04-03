from __future__ import annotations

import os
from functools import lru_cache

from provider_backends import resolve_secret_backend


class SecretManagerService:
    def resolve_secret(self, secret_ref: str | None) -> str | None:
        if not secret_ref:
            return None
        ref = str(secret_ref).strip()
        if not ref:
            return None
        if ref.startswith("env://"):
            return os.getenv(ref[len("env://"):])
        if ref.startswith("asm://"):
            return self._resolve_asm_secret(ref[len("asm://"):])
        if ref.startswith("gsm://"):
            return self._resolve_gsm_secret(ref[len("gsm://"):])
        if ref.startswith("projects/"):
            return self._resolve_gsm_secret(ref)
        if ref.startswith("arn:aws:secretsmanager:"):
            return self._resolve_asm_secret(ref)
        if resolve_secret_backend() == "aws_secrets_manager" and "/" in ref and "://" not in ref:
            return self._resolve_asm_secret(ref)
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

    @lru_cache(maxsize=128)
    def _resolve_asm_secret(self, ref: str) -> str:
        try:
            import boto3
        except ImportError as exc:
            raise RuntimeError("boto3 is required for asm:// secret references.") from exc

        region_name = os.getenv("AWS_REGION") or None
        client = boto3.client("secretsmanager", region_name=region_name)
        secret_id = str(ref).strip()
        if not secret_id:
            raise RuntimeError("AWS Secrets Manager secret reference is empty.")
        response = client.get_secret_value(SecretId=secret_id)
        secret_string = response.get("SecretString")
        if secret_string is not None:
            return str(secret_string)
        secret_binary = response.get("SecretBinary")
        if secret_binary is None:
            raise RuntimeError(f"AWS Secrets Manager secret '{secret_id}' has no SecretString or SecretBinary value.")
        return secret_binary.decode("utf-8")
