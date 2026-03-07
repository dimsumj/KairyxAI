from __future__ import annotations

from enum import Enum


class JobType(str, Enum):
    IMPORT = "import"
    PREDICTION = "prediction"
    EXPORT = "export"


class JobStatus(str, Enum):
    QUEUED = "queued"
    RUNNING = "running"
    STOPPING = "stopping"
    STOPPED = "stopped"
    COMPLETED = "completed"
    FAILED = "failed"


class CheckpointStatus(str, Enum):
    STAGED = "staged"
    PUBLISHED = "published"
    PROCESSED = "processed"
    FAILED = "failed"
