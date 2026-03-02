# gcs_service.py

import os
import json
from typing import List, Dict, Any


class GcsService:
    """
    GCS service with dual backend support:
    - mock: local filesystem bucket (.gcs_bucket)
    - gcp: real Google Cloud Storage bucket
    """

    def __init__(self, bucket_name: str = "kairyx_ai_raw_data_bucket"):
        self.mode = os.getenv("DATA_BACKEND_MODE", "mock").strip().lower()
        if self.mode not in {"mock", "gcp"}:
            raise ValueError("DATA_BACKEND_MODE must be 'mock' or 'gcp'.")

        self.bucket_name = os.getenv("GCS_BUCKET_NAME", bucket_name)
        if self.mode == "gcp":
            self._init_gcp_backend()
            print(f"GcsService initialized in GCP mode (bucket: {self.bucket_name}).")
        else:
            self._init_mock_backend()
            print(f"GcsService initialized in MOCK mode (bucket path: {self._bucket_path}).")

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

    def _init_mock_backend(self):
        self._bucket_path = os.path.join(".gcs_bucket", self.bucket_name)
        os.makedirs(self._bucket_path, exist_ok=True)

    def upload_raw_events(self, events: List[Dict[str, Any]], destination_blob_name: str) -> str:
        if not events:
            return ""

        payload = json.dumps(events)
        if self.mode == "gcp":
            blob = self._bucket.blob(destination_blob_name)
            blob.upload_from_string(payload, content_type="application/json")
            gcs_path = f"gs://{self.bucket_name}/{destination_blob_name}"
            print(f"Uploaded {len(events)} events to GCS at: {gcs_path}")
            return gcs_path

        file_path = os.path.join(self._bucket_path, destination_blob_name)
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, "w") as f:
            f.write(payload)

        gcs_path = f"gs://{self.bucket_name}/{destination_blob_name}"
        print(f"Uploaded {len(events)} events to local GCS mock at: {gcs_path}")
        return gcs_path

    def download_raw_events(self, blob_name: str) -> List[Dict[str, Any]]:
        if self.mode == "gcp":
            blob = self._bucket.blob(blob_name)
            if not blob.exists():
                raise FileNotFoundError(f"Blob not found in GCS: {blob_name}")
            return json.loads(blob.download_as_text())

        file_path = os.path.join(self._bucket_path, blob_name)
        with open(file_path, "r") as f:
            return json.load(f)

    def delete_data_for_job(self, job_identifier: str):
        """
        Deletes all blobs associated with a specific job identifier.
        """
        prefix = f"raw_events/{job_identifier}/"
        if self.mode == "gcp":
            for blob in self._client.list_blobs(self.bucket_name, prefix=prefix):
                blob.delete()
                print(f"Deleted blob '{blob.name}' from GCS.")
            return

        job_dir = os.path.join(self._bucket_path, "raw_events", job_identifier)
        if os.path.isdir(job_dir):
            for filename in os.listdir(job_dir):
                file_to_delete = os.path.join(job_dir, filename)
                os.remove(file_to_delete)
                print(f"Deleted blob '{os.path.join('raw_events', job_identifier, filename)}' from local GCS mock.")

