from __future__ import annotations

from datetime import datetime

from sqlalchemy import DateTime, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from app.core.db import Base


class ConnectorConfigModel(Base):
    __tablename__ = "connector_configs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(255), unique=True, index=True)
    connector_type: Mapped[str] = mapped_column(String(128), index=True)
    config_json: Mapped[str] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class FieldMappingModel(Base):
    __tablename__ = "field_mappings_v2"

    connector_name: Mapped[str] = mapped_column(String(255), primary_key=True)
    mapping_json: Mapped[str] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class ImportJobModel(Base):
    __tablename__ = "import_jobs_v2"

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    source_name: Mapped[str] = mapped_column(String(255), index=True)
    status: Mapped[str] = mapped_column(String(64), index=True)
    spec_json: Mapped[str] = mapped_column(Text)
    progress_json: Mapped[str] = mapped_column(Text, default="{}")
    error: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class PredictionJobModel(Base):
    __tablename__ = "prediction_jobs_v2"

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    import_job_id: Mapped[str] = mapped_column(String(64), index=True)
    status: Mapped[str] = mapped_column(String(64), index=True)
    spec_json: Mapped[str] = mapped_column(Text)
    progress_json: Mapped[str] = mapped_column(Text, default="{}")
    error: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class ExportJobModel(Base):
    __tablename__ = "export_jobs_v2"

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    prediction_job_id: Mapped[str | None] = mapped_column(String(64), index=True, nullable=True)
    status: Mapped[str] = mapped_column(String(64), index=True)
    spec_json: Mapped[str] = mapped_column(Text)
    progress_json: Mapped[str] = mapped_column(Text, default="{}")
    error: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class ExperimentConfigModel(Base):
    __tablename__ = "experiment_configs"

    config_key: Mapped[str] = mapped_column(String(64), primary_key=True)
    config_json: Mapped[str] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class ActionHistoryModel(Base):
    __tablename__ = "action_history_v2"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    action_type: Mapped[str] = mapped_column(String(128), index=True)
    resource_type: Mapped[str] = mapped_column(String(128), index=True)
    resource_id: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)
    payload_json: Mapped[str] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)


class IngestionCheckpointModel(Base):
    __tablename__ = "ingestion_checkpoints_v2"
    __table_args__ = (UniqueConstraint("job_id", "shard_index", name="uq_ingestion_checkpoint_job_shard"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    job_id: Mapped[str] = mapped_column(String(64), index=True)
    shard_index: Mapped[int] = mapped_column(Integer, index=True)
    source_name: Mapped[str] = mapped_column(String(255), index=True)
    status: Mapped[str] = mapped_column(String(64), index=True)
    cursor_value: Mapped[str | None] = mapped_column(String(255), nullable=True)
    gcs_uri: Mapped[str | None] = mapped_column(Text, nullable=True)
    message_id: Mapped[str | None] = mapped_column(String(255), nullable=True)
    payload_json: Mapped[str] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
