from __future__ import annotations

from enum import Enum


class JobType(str, Enum):
    IMPORT = "import"
    PREDICTION = "prediction"
    EXPORT = "export"


class JobStatus(str, Enum):
    CREATED = "created"
    QUEUED = "queued"
    READY = "ready"
    RUNNING = "running"
    AWAITING_MAPPING = "awaiting_mapping"
    PAUSED = "paused"
    STOPPING = "stopping"
    STOPPED = "stopped"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class CheckpointStatus(str, Enum):
    STAGED = "staged"
    PUBLISHED = "published"
    PROCESSED = "processed"
    FAILED = "failed"
