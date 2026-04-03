from __future__ import annotations

import json
import os
from typing import Any, Dict, List, Optional

from app.core.request_context import get_request_context
from provider_backends import resolve_message_backend


class PubSubService:
    """
    Message publisher/consumer with multi-backend support:
    - mock: in-memory published message log
    - pubsub: Google Cloud Pub/Sub publisher
    - eventbridge_sqs: Amazon EventBridge publisher + SQS consumer
    """

    def __init__(self, topic_name: str = "kairyx-raw-shards"):
        self.backend = resolve_message_backend()
        self.mode = "mock" if self.backend == "mock" else self.backend

        if topic_name == "kairyx-raw-shards":
            self.topic_name = os.getenv("PUBSUB_TOPIC_NAME", topic_name)
        else:
            self.topic_name = topic_name
        self.published_messages: List[Dict[str, Any]] = []
        if self.backend == "pubsub":
            self._init_gcp_backend()
        elif self.backend == "eventbridge_sqs":
            self._init_aws_backend()
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

    def _init_aws_backend(self):
        try:
            import boto3
        except ImportError as e:
            raise RuntimeError(
                "boto3 is required for MESSAGE_BACKEND=eventbridge_sqs."
            ) from e

        region_name = os.getenv("AWS_REGION") or None
        self._eventbridge = boto3.client("events", region_name=region_name)
        self._sqs = boto3.client("sqs", region_name=region_name)
        self._event_bus_name = os.getenv("EVENTBRIDGE_BUS_NAME", "default")

    def publish(self, payload: Dict[str, Any], attributes: Optional[Dict[str, Any]] = None) -> str:
        safe_attributes = {k: str(v) for k, v in (attributes or {}).items() if v is not None}
        context = get_request_context()
        if context is not None:
            safe_attributes.setdefault("tenant_id", str(context.tenant_id or ""))
            safe_attributes.setdefault("project_id", str(context.project_id or ""))
            safe_attributes.setdefault("correlation_id", context.correlation_id)
        if self.backend == "pubsub":
            data = json.dumps(payload).encode("utf-8")
            future = self._publisher.publish(self._topic_path, data, **safe_attributes)
            return future.result(timeout=30)
        if self.backend == "eventbridge_sqs":
            detail = json.dumps(
                {
                    "topic_name": self.topic_name,
                    "payload": payload,
                    "attributes": safe_attributes,
                }
            )
            response = self._eventbridge.put_events(
                Entries=[
                    {
                        "Source": "kairyx.ai",
                        "DetailType": self.topic_name,
                        "EventBusName": self._event_bus_name,
                        "Detail": detail,
                    }
                ]
            )
            entry = (response.get("Entries") or [{}])[0]
            error_code = entry.get("ErrorCode")
            if error_code:
                raise RuntimeError(
                    f"EventBridge publish failed for topic '{self.topic_name}': {error_code} {entry.get('ErrorMessage') or ''}".strip()
                )
            return str(entry.get("EventId") or "")

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

    @staticmethod
    def queue_url_for_service_role(service_role: str) -> str:
        mapping = {
            "import-worker": os.getenv("SQS_IMPORT_QUEUE_URL", ""),
            "prediction-worker": os.getenv("SQS_PREDICTION_QUEUE_URL", ""),
            "export-worker": os.getenv("SQS_EXPORT_QUEUE_URL", ""),
            "scheduler-worker": os.getenv("SQS_SCHEDULER_QUEUE_URL", ""),
        }
        return mapping.get(str(service_role or "").strip().lower(), "")

    def receive_messages(
        self,
        queue_url: str,
        *,
        max_number: int = 1,
        wait_time_seconds: int = 20,
        visibility_timeout: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        if self.backend != "eventbridge_sqs":
            return []
        kwargs: Dict[str, Any] = {
            "QueueUrl": queue_url,
            "MaxNumberOfMessages": max(1, min(10, int(max_number))),
            "WaitTimeSeconds": max(0, min(20, int(wait_time_seconds))),
            "AttributeNames": ["All"],
            "MessageAttributeNames": ["All"],
        }
        if visibility_timeout is not None:
            kwargs["VisibilityTimeout"] = max(0, int(visibility_timeout))
        response = self._sqs.receive_message(**kwargs)
        return list(response.get("Messages") or [])

    def delete_message(self, queue_url: str, receipt_handle: str) -> None:
        if self.backend != "eventbridge_sqs":
            return
        self._sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)

    @staticmethod
    def decode_queue_message(message: Dict[str, Any]) -> Dict[str, Any]:
        raw_body = str(message.get("Body") or "").strip()
        if not raw_body:
            return {"payload": {}, "attributes": {}, "body": {}, "message_id": message.get("MessageId"), "receipt_handle": message.get("ReceiptHandle")}
        parsed_body: Any
        try:
            parsed_body = json.loads(raw_body)
        except json.JSONDecodeError:
            parsed_body = {"payload": {}}

        detail = parsed_body.get("detail") if isinstance(parsed_body, dict) else None
        if isinstance(detail, str):
            try:
                detail = json.loads(detail)
            except json.JSONDecodeError:
                detail = {"payload": {}}

        if isinstance(detail, dict) and "payload" in detail:
            payload = detail.get("payload") or {}
            attributes = detail.get("attributes") or {}
            topic_name = detail.get("topic_name")
        elif isinstance(parsed_body, dict) and "payload" in parsed_body:
            payload = parsed_body.get("payload") or {}
            attributes = parsed_body.get("attributes") or {}
            topic_name = parsed_body.get("topic_name")
        else:
            payload = parsed_body if isinstance(parsed_body, dict) else {}
            attributes = {}
            topic_name = None

        return {
            "payload": payload if isinstance(payload, dict) else {},
            "attributes": attributes if isinstance(attributes, dict) else {},
            "topic_name": topic_name,
            "body": parsed_body if isinstance(parsed_body, dict) else {},
            "message_id": message.get("MessageId"),
            "receipt_handle": message.get("ReceiptHandle"),
        }
