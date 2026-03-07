from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    app_name: str = "KairyxAI Operator API"
    api_v1_prefix: str = "/api/v1"
    control_plane_database_url: str = ""
    data_backend_mode: str = "mock"
    import_command_topic: str = "kairyx-import-jobs"
    prediction_command_topic: str = "kairyx-prediction-jobs"
    export_command_topic: str = "kairyx-export-jobs"
    raw_shard_topic: str = "kairyx-raw-shards"
    default_prediction_page_size: int = 100
    max_prediction_page_size: int = 1000
    worker_page_size: int = 1000
    export_batch_size: int = 500
    export_retry_attempts: int = 3


def get_settings() -> Settings:
    database_url = (
        os.getenv("CONTROL_PLANE_DATABASE_URL")
        or os.getenv("DATABASE_URL")
        or "sqlite:///./.kairyx_control_plane.db"
    )
    return Settings(
        control_plane_database_url=database_url,
        data_backend_mode=os.getenv("DATA_BACKEND_MODE", "mock").strip().lower(),
        import_command_topic=os.getenv("IMPORT_COMMAND_TOPIC", "kairyx-import-jobs"),
        prediction_command_topic=os.getenv("PREDICTION_COMMAND_TOPIC", "kairyx-prediction-jobs"),
        export_command_topic=os.getenv("EXPORT_COMMAND_TOPIC", "kairyx-export-jobs"),
        raw_shard_topic=os.getenv("PUBSUB_TOPIC_NAME", "kairyx-raw-shards"),
        default_prediction_page_size=max(1, int(os.getenv("DEFAULT_PREDICTION_PAGE_SIZE", "100"))),
        max_prediction_page_size=max(1, int(os.getenv("MAX_PREDICTION_PAGE_SIZE", "1000"))),
        worker_page_size=max(1, int(os.getenv("WORKER_PAGE_SIZE", "1000"))),
        export_batch_size=max(1, int(os.getenv("EXPORT_BATCH_SIZE", "500"))),
        export_retry_attempts=max(1, int(os.getenv("EXPORT_RETRY_ATTEMPTS", "3"))),
    )
