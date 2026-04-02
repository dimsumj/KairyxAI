from __future__ import annotations

import logging
import threading
import time
from typing import Callable, Dict

from app.core.runtime import is_shutdown_requested
from app.core.settings import get_settings
from pubsub_service import PubSubService


logger = logging.getLogger(__name__)


def start_queue_poller(
    app,
    *,
    service_role: str,
    handler: Callable[[Dict[str, object], Dict[str, object]], None],
) -> None:
    settings = get_settings()
    if settings.message_backend != "eventbridge_sqs":
        return

    queue_url = PubSubService.queue_url_for_service_role(service_role)
    if not queue_url:
        logger.info("Skipping SQS poller startup for %s because no queue URL is configured.", service_role)
        return

    stop_event = threading.Event()
    app.state.queue_poller_stop_event = stop_event

    def _poll_loop() -> None:
        service = PubSubService()
        logger.info("Starting SQS poller for %s.", service_role)
        while not stop_event.is_set() and not is_shutdown_requested():
            try:
                messages = service.receive_messages(queue_url, max_number=1, wait_time_seconds=20)
            except Exception:
                logger.exception("SQS receive failed for %s.", service_role)
                time.sleep(5)
                continue
            if not messages:
                continue

            for raw_message in messages:
                decoded = service.decode_queue_message(raw_message)
                payload = decoded.get("payload") or {}
                attributes = decoded.get("attributes") or {}
                receipt_handle = decoded.get("receipt_handle")
                try:
                    handler(dict(payload), dict(attributes))
                except Exception:
                    logger.exception("Worker handler failed for %s.", service_role)
                    continue
                if receipt_handle:
                    try:
                        service.delete_message(queue_url, str(receipt_handle))
                    except Exception:
                        logger.exception("Failed to delete processed SQS message for %s.", service_role)

    thread = threading.Thread(target=_poll_loop, name=f"{service_role}-sqs-poller", daemon=True)
    app.state.queue_poller_thread = thread
    thread.start()


def stop_queue_poller(app) -> None:
    stop_event = getattr(app.state, "queue_poller_stop_event", None)
    thread = getattr(app.state, "queue_poller_thread", None)
    if stop_event is not None:
        stop_event.set()
    if thread is not None and thread.is_alive():
        thread.join(timeout=1.0)
