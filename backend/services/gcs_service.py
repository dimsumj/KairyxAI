# gcs_service.py

import os
import json
from typing import List, Dict, Any

class GcsService:
    """
    Simulates a service for interacting with Google Cloud Storage.
    In a real-world application, this would use the google-cloud-storage library.
    """

    def __init__(self, bucket_name: str = "kairyx_ai_raw_data_bucket"):
        """
        Initializes the service. In a real app, this would set up the GCS client.
        For this simulation, we'll use a local directory as the "bucket".
        """
        self.bucket_name = bucket_name
        self._bucket_path = os.path.join(".gcs_bucket", bucket_name)
        os.makedirs(self._bucket_path, exist_ok=True)
        print(f"GcsService initialized (simulating GCS bucket at: {self._bucket_path}).")

    def upload_raw_events(self, events: List[Dict[str, Any]], destination_blob_name: str) -> str:
        """
        Simulates uploading a list of raw events as a JSON file to GCS.

        Args:
            events: A list of raw event dictionaries.
            destination_blob_name: The "path" or name for the file in the bucket.

        Returns:
            The GCS path to the uploaded file.
        """
        if not events:
            return ""

        file_path = os.path.join(self._bucket_path, destination_blob_name)
        os.makedirs(os.path.dirname(file_path), exist_ok=True)

        with open(file_path, 'w') as f:
            json.dump(events, f)
        
        gcs_path = f"gs://{self.bucket_name}/{destination_blob_name}"
        print(f"Uploaded {len(events)} events to GCS at: {gcs_path}")
        return gcs_path

    def download_raw_events(self, blob_name: str) -> List[Dict[str, Any]]:
        """
        Simulates downloading and reading a raw events JSON file from GCS.

        Args:
            blob_name: The "path" or name of the file in the bucket.

        Returns:
            A list of raw event dictionaries.
        """
        file_path = os.path.join(self._bucket_path, blob_name)
        with open(file_path, 'r') as f:
            return json.load(f)