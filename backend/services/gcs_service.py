# gcs_service.py

import os
import json
import shutil
from pathlib import Path
from typing import List, Dict, Any

from app.core.request_context import get_request_context
from provider_backends import resolve_object_storage_backend
from runtime_paths import resolve_runtime_file_path


class GcsService:
    """
    Object storage service with multi-backend support:
    - mock: local filesystem bucket (.gcs_bucket)
    - gcs: Google Cloud Storage bucket
    - s3: Amazon S3 bucket
    """

    def __init__(self, bucket_name: str = "kairyx_ai_raw_data_bucket"):
        self.backend = resolve_object_storage_backend()
        self.mode = "mock" if self.backend == "mock" else self.backend
        self.uri_scheme = "gs" if self.backend in {"mock", "gcs"} else "s3"
        self.bucket_name = (
            os.getenv("S3_BUCKET_NAME")
            if self.backend == "s3"
            else os.getenv("GCS_BUCKET_NAME")
        ) or bucket_name
        if self.backend == "gcs":
            self._init_gcp_backend()
            print(f"GcsService initialized in GCP mode (bucket: {self.bucket_name}).")
        elif self.backend == "s3":
            self._init_s3_backend()
            print(f"GcsService initialized in AWS S3 mode (bucket: {self.bucket_name}).")
        else:
            self._init_mock_backend()
            print(f"GcsService initialized in MOCK mode (bucket root: {self._mock_root}).")

    def _init_gcp_backend(self):
        try:
            from google.cloud import storage
        except ImportError as e:
            raise RuntimeError(
                "google-cloud-storage is required for DATA_BACKEND_MODE=gcp."
            ) from e

        self._storage = storage
        self._client = storage.Client()
        self._bucket = self._client.bucket(self.bucket_name)

    def _init_s3_backend(self):
        try:
            import boto3
        except ImportError as e:
            raise RuntimeError(
                "boto3 is required for OBJECT_STORAGE_BACKEND=s3."
            ) from e

        self._boto3 = boto3
        self._client = boto3.client("s3", region_name=os.getenv("AWS_REGION") or None)

    def _init_mock_backend(self):
        self._mock_root = Path(resolve_runtime_file_path(Path(".cache") / "raw", ensure_parent=True))
        self._legacy_bucket_path = str(
            resolve_runtime_file_path(Path(".gcs_bucket") / self.bucket_name, ensure_parent=True)
        )
        os.makedirs(self._mock_root, exist_ok=True)

    def _normalize_scope_component(self, raw_value: str | None, default: str = "default") -> str:
        normalized = "".join(char if char.isalnum() or char in {"-", "_"} else "_" for char in str(raw_value or default).strip())
        return normalized or default

    def _tenant_scope_key(self) -> str:
        context = get_request_context()
        raw_value = context.tenant_id if context is not None else os.getenv("BOOTSTRAP_TENANT_ID", "default")
        return self._normalize_scope_component(raw_value)

    def _project_scope_key(self) -> str:
        context = get_request_context()
        raw_value = context.project_id if context is not None else os.getenv("BOOTSTRAP_PROJECT_ID", "default")
        return self._normalize_scope_component(raw_value)

    def _scope_prefix(self, tenant_scope: str | None = None, project_scope: str | None = None) -> str:
        return f"tenants/{self._normalize_scope_component(tenant_scope or self._tenant_scope_key())}/projects/{self._normalize_scope_component(project_scope or self._project_scope_key())}"

    def _tenant_blob_name(self, blob_name: str) -> str:
        normalized = str(blob_name).lstrip("/")
        if normalized.startswith("tenants/"):
            return normalized
        prefix = self._scope_prefix()
        if normalized.startswith(prefix):
            return normalized
        return f"{prefix}/{normalized}"

    def _extract_blob_scope(self, blob_name: str) -> tuple[str, str] | None:
        segments = [segment for segment in str(blob_name or "").split("/") if segment]
        if len(segments) < 4:
            return None
        if segments[0] != "tenants" or segments[2] != "projects":
            return None
        return (
            self._normalize_scope_component(segments[1]),
            self._normalize_scope_component(segments[3]),
        )

    def _mock_bucket_path(self, tenant_scope: str | None = None, project_scope: str | None = None) -> str:
        bucket_path = self._mock_root / self._normalize_scope_component(tenant_scope or self._tenant_scope_key()) / self._normalize_scope_component(project_scope or self._project_scope_key()) / self.bucket_name
        os.makedirs(bucket_path, exist_ok=True)
        return str(bucket_path)

    def _encode_raw_events(self, events: List[Dict[str, Any]]) -> str:
        return "\n".join(json.dumps(event) for event in events)

    def _decode_raw_events(self, payload: str) -> List[Dict[str, Any]]:
        stripped = payload.strip()
        if not stripped:
            return []

        if stripped.startswith("["):
            parsed = json.loads(stripped)
            return parsed if isinstance(parsed, list) else []

        events = []
        for line in stripped.splitlines():
            line = line.strip()
            if not line:
                continue
            events.append(json.loads(line))
        return events

    def _resolve_mock_blob_path(self, blob_name: str) -> str:
        candidate_paths = []
        explicit_scope = self._extract_blob_scope(blob_name)
        if explicit_scope is not None:
            candidate_paths.append(os.path.join(self._mock_bucket_path(*explicit_scope), blob_name))
        candidate_paths.append(os.path.join(self._mock_bucket_path(), blob_name))
        candidate_paths.append(os.path.join(self._legacy_bucket_path, blob_name))
        candidate_paths = list(dict.fromkeys(candidate_paths))
        for path in candidate_paths:
            if os.path.exists(path):
                return path
        return candidate_paths[0]

    def object_uri_for_blob_name(self, blob_name: str) -> str:
        normalized_blob_name = self._tenant_blob_name(blob_name)
        return f"{self.uri_scheme}://{self.bucket_name}/{normalized_blob_name}"

    def resolve_blob_name(self, object_uri_or_blob_name: str) -> str:
        candidate = str(object_uri_or_blob_name or "").strip()
        if not candidate:
            return ""
        for prefix in (f"gs://{self.bucket_name}/", f"s3://{self.bucket_name}/"):
            if candidate.startswith(prefix):
                return candidate[len(prefix):]
        if candidate.startswith("gs://") or candidate.startswith("s3://"):
            parts = candidate.split("/", 3)
            if len(parts) >= 4:
                return parts[3]
        return candidate

    def upload_raw_events(self, events: List[Dict[str, Any]], destination_blob_name: str) -> str:
        if not events:
            return ""

        payload = self._encode_raw_events(events)
        destination_blob_name = self._tenant_blob_name(destination_blob_name)
        if self.backend == "gcs":
            blob = self._bucket.blob(destination_blob_name)
            blob.upload_from_string(payload, content_type="application/x-ndjson")
            object_uri = self.object_uri_for_blob_name(destination_blob_name)
            print(f"Uploaded {len(events)} events to GCS at: {object_uri}")
            return object_uri
        if self.backend == "s3":
            self._client.put_object(
                Bucket=self.bucket_name,
                Key=destination_blob_name,
                Body=payload.encode("utf-8"),
                ContentType="application/x-ndjson",
            )
            object_uri = self.object_uri_for_blob_name(destination_blob_name)
            print(f"Uploaded {len(events)} events to S3 at: {object_uri}")
            return object_uri

        scoped_bucket_path = self._mock_bucket_path(*(self._extract_blob_scope(destination_blob_name) or (self._tenant_scope_key(), self._project_scope_key())))
        file_path = os.path.join(scoped_bucket_path, destination_blob_name)
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, "w") as f:
            f.write(payload)

        object_uri = self.object_uri_for_blob_name(destination_blob_name)
        print(f"Uploaded {len(events)} events to local object-storage mock at: {object_uri}")
        return object_uri

    def download_raw_events(self, blob_name: str) -> List[Dict[str, Any]]:
        blob_name = self._tenant_blob_name(self.resolve_blob_name(blob_name))
        if self.backend == "gcs":
            blob = self._bucket.blob(blob_name)
            if not blob.exists():
                raise FileNotFoundError(f"Blob not found in GCS: {blob_name}")
            return self._decode_raw_events(blob.download_as_text())
        if self.backend == "s3":
            try:
                response = self._client.get_object(Bucket=self.bucket_name, Key=blob_name)
            except Exception as exc:
                raise FileNotFoundError(f"Blob not found in S3: {blob_name}") from exc
            return self._decode_raw_events(response["Body"].read().decode("utf-8"))

        file_path = self._resolve_mock_blob_path(blob_name)
        with open(file_path, "r") as f:
            return self._decode_raw_events(f.read())

    def delete_data_for_job(self, job_identifier: str):
        """
        Deletes all blobs associated with a specific job identifier.
        """
        job_fragment = f"/{job_identifier}/"
        tenant_prefix = self._tenant_blob_name("raw_events/")
        if self.backend == "gcs":
            for blob in self._client.list_blobs(self.bucket_name, prefix=tenant_prefix):
                if job_fragment not in f"/{blob.name}":
                    continue
                blob.delete()
                print(f"Deleted blob '{blob.name}' from GCS.")
            return
        if self.backend == "s3":
            paginator = self._client.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=self.bucket_name, Prefix=tenant_prefix):
                for item in page.get("Contents", []):
                    key = str(item.get("Key") or "")
                    if job_fragment not in f"/{key}":
                        continue
                    self._client.delete_object(Bucket=self.bucket_name, Key=key)
                    print(f"Deleted blob '{key}' from S3.")
            return

        for root in (str(self._mock_root), self._legacy_bucket_path):
            if not os.path.isdir(root):
                continue
            for current_root, _, filenames in os.walk(root):
                for filename in filenames:
                    file_to_delete = os.path.join(current_root, filename)
                    rel_path = os.path.relpath(file_to_delete, root)
                    if job_fragment not in f"/{rel_path}":
                        continue
                    os.remove(file_to_delete)
                    print(f"Deleted blob '{rel_path}' from local GCS mock.")

    def delete_project_scope(self) -> None:
        tenant_scope = self._tenant_scope_key()
        project_scope = self._project_scope_key()
        prefix = self._scope_prefix(tenant_scope, project_scope).rstrip("/") + "/"
        if self.backend == "gcs":
            for blob in self._client.list_blobs(self.bucket_name, prefix=prefix):
                blob.delete()
                print(f"Deleted blob '{blob.name}' from GCS.")
            return
        if self.backend == "s3":
            paginator = self._client.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=self.bucket_name, Prefix=prefix):
                for item in page.get("Contents", []):
                    key = str(item.get("Key") or "")
                    if not key:
                        continue
                    self._client.delete_object(Bucket=self.bucket_name, Key=key)
                    print(f"Deleted blob '{key}' from S3.")
            return

        scoped_bucket_path = Path(self._mock_bucket_path(tenant_scope, project_scope)).resolve()
        if scoped_bucket_path.exists():
            shutil.rmtree(scoped_bucket_path, ignore_errors=True)
        legacy_prefix = Path(self._legacy_bucket_path) / prefix
        if legacy_prefix.exists():
            if legacy_prefix.is_dir():
                shutil.rmtree(legacy_prefix, ignore_errors=True)
            else:
                legacy_prefix.unlink(missing_ok=True)
