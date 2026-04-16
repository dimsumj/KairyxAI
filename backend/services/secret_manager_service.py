from __future__ import annotations

import base64
import hashlib
import json
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

    def encrypt_secret_value(self, value):
        cipher = self._get_control_plane_cipher()
        payload = json.dumps({"value": value}, separators=(",", ":"), sort_keys=True)
        return cipher.encrypt(payload.encode("utf-8")).decode("utf-8")

    def decrypt_secret_value(self, token: str):
        cipher = self._get_control_plane_cipher()
        try:
            payload = cipher.decrypt(str(token).encode("utf-8")).decode("utf-8")
        except self._get_invalid_token_error() as exc:
            raise RuntimeError("Encrypted control-plane secret could not be decrypted.") from exc
        try:
            parsed = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise RuntimeError("Encrypted control-plane secret payload is invalid JSON.") from exc
        return parsed.get("value")

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

    @staticmethod
    @lru_cache(maxsize=1)
    def _get_control_plane_cipher():
        raw_key = str(os.getenv("CONTROL_PLANE_SECRET_KEY") or "").strip()
        if not raw_key:
            raise RuntimeError(
                "CONTROL_PLANE_SECRET_KEY must be configured to securely store web-entered connector secrets."
            )
        try:
            from cryptography.fernet import Fernet
        except ImportError as exc:
            raise RuntimeError("cryptography is required for encrypted control-plane secret storage.") from exc
        derived_key = base64.urlsafe_b64encode(hashlib.sha256(raw_key.encode("utf-8")).digest())
        return Fernet(derived_key)

    @staticmethod
    @lru_cache(maxsize=1)
    def _get_invalid_token_error():
        try:
            from cryptography.fernet import InvalidToken
        except ImportError as exc:
            raise RuntimeError("cryptography is required for encrypted control-plane secret storage.") from exc
        return InvalidToken
