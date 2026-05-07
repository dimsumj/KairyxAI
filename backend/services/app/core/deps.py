from __future__ import annotations

from fastapi import Depends
from sqlalchemy.orm import Session

from app.application.audit import AuditService
from app.application.agent_model_profiles import AgentModelProfileService
from app.application.ai_evaluations import AIEvaluationService
from app.application.braze_provider import BrazeProviderService
from app.application.cohorts import CohortService
from app.application.copilot import CopilotService
from app.application.copilot_agent import CopilotAgentService
from app.application.connectors import ConnectorService
from app.application.control_loop import ControlLoopService
from app.application.email_campaigns import EmailCampaignService
from app.application.experiments import ExperimentConfigService
from app.application.exports import ExportService
from app.application.health_monitor import HealthMonitorService
from app.application.imports import ImportService
from app.application.knowledge import KnowledgeService
from app.application.mappings import MappingService
from app.application.predictions import PredictionService
from app.application.projects import ProjectWorkspaceService
from app.application.push_dispatches import PushDispatchService
from app.application.sendgrid_provider import SendGridProviderService
from app.application.sql_workspace import SqlWorkspaceService
from app.application.templates import ScenarioTemplateService
from app.application.workflows import WorkflowService
from app.core.db import get_db_session
from app.core.settings import Settings, get_settings
from app.infrastructure.repositories.sqlalchemy_control_plane import SqlAlchemyControlPlaneRepository
from bigquery_service import get_shared_bigquery_service


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
    return MappingService(repository, get_shared_bigquery_service())


def get_import_service(
    repository: SqlAlchemyControlPlaneRepository = Depends(get_repository),
    settings: Settings = Depends(get_settings_dependency),
) -> ImportService:
    return ImportService(repository, settings)


def get_knowledge_service(
    repository: SqlAlchemyControlPlaneRepository = Depends(get_repository),
) -> KnowledgeService:
    return KnowledgeService(repository)


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


def get_ai_evaluation_service(
    repository: SqlAlchemyControlPlaneRepository = Depends(get_repository),
) -> AIEvaluationService:
    return AIEvaluationService(repository)


def get_cohort_service(
    repository: SqlAlchemyControlPlaneRepository = Depends(get_repository),
    settings: Settings = Depends(get_settings_dependency),
) -> CohortService:
    return CohortService(repository, settings=settings)


def get_sql_workspace_service(
    repository: SqlAlchemyControlPlaneRepository = Depends(get_repository),
    settings: Settings = Depends(get_settings_dependency),
) -> SqlWorkspaceService:
    return SqlWorkspaceService(repository, settings)


def get_workflow_service(
    repository: SqlAlchemyControlPlaneRepository = Depends(get_repository),
) -> WorkflowService:
    return WorkflowService(repository)


def get_email_campaign_service(
    repository: SqlAlchemyControlPlaneRepository = Depends(get_repository),
    settings: Settings = Depends(get_settings_dependency),
) -> EmailCampaignService:
    return EmailCampaignService(repository, settings)


def get_push_dispatch_service(
    repository: SqlAlchemyControlPlaneRepository = Depends(get_repository),
) -> PushDispatchService:
    return PushDispatchService(repository)


def get_sendgrid_provider_service(
    repository: SqlAlchemyControlPlaneRepository = Depends(get_repository),
) -> SendGridProviderService:
    return SendGridProviderService(repository)


def get_braze_provider_service(
    repository: SqlAlchemyControlPlaneRepository = Depends(get_repository),
) -> BrazeProviderService:
    return BrazeProviderService(repository)


def get_copilot_service(
    repository: SqlAlchemyControlPlaneRepository = Depends(get_repository),
    settings: Settings = Depends(get_settings_dependency),
) -> CopilotService:
    return CopilotService(repository, settings)


def get_copilot_agent_service(
    repository: SqlAlchemyControlPlaneRepository = Depends(get_repository),
    settings: Settings = Depends(get_settings_dependency),
) -> CopilotAgentService:
    return CopilotAgentService(repository, settings)


def get_agent_model_profile_service(
    repository: SqlAlchemyControlPlaneRepository = Depends(get_repository),
) -> AgentModelProfileService:
    return AgentModelProfileService(repository)


def get_health_monitor_service(
    repository: SqlAlchemyControlPlaneRepository = Depends(get_repository),
) -> HealthMonitorService:
    return HealthMonitorService(repository)


def get_control_loop_service(
    repository: SqlAlchemyControlPlaneRepository = Depends(get_repository),
    settings: Settings = Depends(get_settings_dependency),
) -> ControlLoopService:
    return ControlLoopService(repository, settings)


def get_audit_service(
    repository: SqlAlchemyControlPlaneRepository = Depends(get_repository),
) -> AuditService:
    return AuditService(repository)


def get_template_service(
    repository: SqlAlchemyControlPlaneRepository = Depends(get_repository),
) -> ScenarioTemplateService:
    return ScenarioTemplateService(repository)


def get_project_workspace_service(
    repository: SqlAlchemyControlPlaneRepository = Depends(get_repository),
) -> ProjectWorkspaceService:
    return ProjectWorkspaceService(repository)
