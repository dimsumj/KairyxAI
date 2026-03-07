from __future__ import annotations

from fastapi import Depends
from sqlalchemy.orm import Session

from app.application.connectors import ConnectorService
from app.application.experiments import ExperimentConfigService
from app.application.exports import ExportService
from app.application.imports import ImportService
from app.application.mappings import MappingService
from app.application.predictions import PredictionService
from app.core.db import get_db_session
from app.core.settings import Settings, get_settings
from app.infrastructure.repositories.sqlalchemy_control_plane import SqlAlchemyControlPlaneRepository


def get_settings_dependency() -> Settings:
    return get_settings()


def get_repository(session: Session = Depends(get_db_session)) -> SqlAlchemyControlPlaneRepository:
    return SqlAlchemyControlPlaneRepository(session)


def get_connector_service(
    repository: SqlAlchemyControlPlaneRepository = Depends(get_repository),
) -> ConnectorService:
    return ConnectorService(repository)


def get_mapping_service(
    repository: SqlAlchemyControlPlaneRepository = Depends(get_repository),
) -> MappingService:
    return MappingService(repository)


def get_import_service(
    repository: SqlAlchemyControlPlaneRepository = Depends(get_repository),
    settings: Settings = Depends(get_settings_dependency),
) -> ImportService:
    return ImportService(repository, settings)


def get_prediction_service(
    repository: SqlAlchemyControlPlaneRepository = Depends(get_repository),
    settings: Settings = Depends(get_settings_dependency),
) -> PredictionService:
    return PredictionService(repository, settings)


def get_export_service(
    repository: SqlAlchemyControlPlaneRepository = Depends(get_repository),
    settings: Settings = Depends(get_settings_dependency),
) -> ExportService:
    return ExportService(repository, settings)


def get_experiment_service(
    repository: SqlAlchemyControlPlaneRepository = Depends(get_repository),
) -> ExperimentConfigService:
    return ExperimentConfigService(repository)
