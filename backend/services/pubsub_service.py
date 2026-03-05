from __future__ import annotations

import json
import os
from typing import Any, Dict, List, Optional


class PubSubService:
    """
    Pub/Sub publisher with dual backend support:
    - mock: in-memory published message log
    - gcp: real Google Cloud Pub/Sub publisher
    """

    def __init__(self, topic_name: str = "kairyx-raw-shards"):
        self.mode = os.getenv("DATA_BACKEND_MODE", "mock").strip().lower()
        if self.mode not in {"mock", "gcp"}:
            raise ValueError("DATA_BACKEND_MODE must be 'mock' or 'gcp'.")

        self.topic_name = os.getenv("PUBSUB_TOPIC_NAME", topic_name)
        self.published_messages: List[Dict[str, Any]] = []
        if self.mode == "gcp":
            self._init_gcp_backend()
        else:
            self._next_message_id = 1

    def _init_gcp_backend(self):
        try:
            from google.cloud import pubsub_v1
        except ImportError as e:
            raise RuntimeError(
                "google-cloud-pubsub is required for DATA_BACKEND_MODE=gcp."
            ) from e

        project_id = (
            os.getenv("GCP_PROJECT_ID")
            or os.getenv("GOOGLE_CLOUD_PROJECT")
            or os.getenv("BIGQUERY_PROJECT_ID")
        )
        if not project_id:
            raise ValueError(
                "GCP_PROJECT_ID, GOOGLE_CLOUD_PROJECT, or BIGQUERY_PROJECT_ID must be set for DATA_BACKEND_MODE=gcp."
            )

        self._pubsub_v1 = pubsub_v1
        self._publisher = pubsub_v1.PublisherClient()
        self._topic_path = self._publisher.topic_path(project_id, self.topic_name)

    def publish(self, payload: Dict[str, Any], attributes: Optional[Dict[str, Any]] = None) -> str:
        safe_attributes = {k: str(v) for k, v in (attributes or {}).items() if v is not None}
        if self.mode == "gcp":
            data = json.dumps(payload).encode("utf-8")
            future = self._publisher.publish(self._topic_path, data, **safe_attributes)
            return future.result(timeout=30)

        message_id = f"mock-{self._next_message_id}"
        self._next_message_id += 1
        self.published_messages.append(
            {
                "message_id": message_id,
                "payload": payload,
                "attributes": safe_attributes,
            }
        )
        return message_id

    def publish_many(
        self,
        payloads: List[Dict[str, Any]],
        attributes_list: Optional[List[Optional[Dict[str, Any]]]] = None,
    ) -> List[str]:
        message_ids = []
        for index, payload in enumerate(payloads):
            attrs = None
            if attributes_list and index < len(attributes_list):
                attrs = attributes_list[index]
            message_ids.append(self.publish(payload, attrs))
        return message_ids
