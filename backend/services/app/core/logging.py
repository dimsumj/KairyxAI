from __future__ import annotations

import logging
import re
import threading


class PredictionPollingAccessFilter(logging.Filter):
    """Allow only the first repeated prediction polling access log per job."""

    _prediction_status_pattern = re.compile(r"^/api/v1/predictions/([^/]+)$")
    _prediction_results_pattern = re.compile(r"^/api/v1/predictions/([^/]+)/results$")

    def __init__(self) -> None:
        super().__init__()
        self._seen_jobs: set[str] = set()
        self._lock = threading.Lock()

    def filter(self, record: logging.LogRecord) -> bool:
        args = getattr(record, "args", ())
        if not isinstance(args, tuple) or len(args) < 5:
            return True

        _client_addr, method, raw_path, _http_version, _status_code = args[:5]
        if str(method).upper() != "GET":
            return True

        job_id = self._extract_prediction_job_id(str(raw_path))
        if not job_id:
            return True

        with self._lock:
            if job_id in self._seen_jobs:
                return False
            self._seen_jobs.add(job_id)
        return True

    def _extract_prediction_job_id(self, raw_path: str) -> str | None:
        path = raw_path.split("?", 1)[0]
        for pattern in (self._prediction_status_pattern, self._prediction_results_pattern):
            match = pattern.match(path)
            if match:
                return match.group(1)
        return None


def configure_access_log_filters() -> None:
    access_logger = logging.getLogger("uvicorn.access")
    if any(isinstance(item, PredictionPollingAccessFilter) for item in access_logger.filters):
        return
    access_logger.addFilter(PredictionPollingAccessFilter())
