from __future__ import annotations

import json
import os
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional

from sqlalchemy import delete, desc, select
from sqlalchemy.orm import Session

from app.core.request_context import get_request_context
from app.infrastructure.db_models import (
    ActionHistoryModel,
    ConnectorConfigModel,
    ControlPlaneResourceEventModel,
    ControlPlaneResourceModel,
    ControlPlaneResourceVersionModel,
    ExperimentConfigModel,
    ExportJobModel,
    FieldMappingModel,
    ImportJobModel,
    IngestionCheckpointModel,
    PlatformUserModel,
    PredictionJobModel,
    TenantMembershipModel,
    TenantModel,
)


def _to_json_text(value: Dict[str, Any]) -> str:
    return json.dumps(value, sort_keys=True, default=str)


def _from_json_text(value: str) -> Dict[str, Any]:
    if not value:
        return {}
    return json.loads(value)


class SqlAlchemyControlPlaneRepository:
    def __init__(self, session: Session):
        self.session = session

    def _bootstrap_tenant_id(self) -> str:
        return str(os.getenv("BOOTSTRAP_TENANT_ID", "default")).strip() or "default"

    def _current_tenant_id(self) -> str | None:
        context = get_request_context()
        if context is None:
            return None
        return str(context.tenant_id or "").strip() or None

    def _resolve_tenant_id(self, tenant_id: str | None = None, *, fallback_to_bootstrap: bool = False) -> str | None:
        resolved = str(tenant_id or "").strip() or self._current_tenant_id()
        if resolved:
            return resolved
        if fallback_to_bootstrap:
            return self._bootstrap_tenant_id()
        return None

    def _metadata(self, tenant_id: str | None = None) -> Dict[str, str]:
        context = get_request_context()
        resolved_tenant_id = self._resolve_tenant_id(tenant_id, fallback_to_bootstrap=True) or self._bootstrap_tenant_id()
        actor_id = context.actor_id if context is not None else "system"
        correlation_id = context.correlation_id if context is not None else ""
        return {
            "tenant_id": resolved_tenant_id,
            "actor_id": actor_id,
            "correlation_id": correlation_id,
        }

    def _augment_payload(self, payload: Dict[str, Any], *, tenant_id: str, created_by: str | None = None, updated_by: str | None = None, correlation_id: str = "") -> Dict[str, Any]:
        return {
            **payload,
            "tenant_id": tenant_id,
            "created_by": created_by or payload.get("created_by") or "system",
            "updated_by": updated_by or payload.get("updated_by") or created_by or payload.get("updated_by") or "system",
            "correlation_id": correlation_id or payload.get("correlation_id") or "",
        }

    def ensure_tenant(self, tenant_id: str, name: str | None = None, *, status: str = "active") -> Dict[str, Any]:
        resolved_id = str(tenant_id).strip() or self._bootstrap_tenant_id()
        row = self.session.get(TenantModel, resolved_id)
        if row is None:
            row = TenantModel(tenant_id=resolved_id, name=name or resolved_id, status=status)
            self.session.add(row)
        else:
            row.name = name or row.name
            row.status = status or row.status
            row.updated_at = datetime.utcnow()
        self.session.flush()
        return self._tenant_to_dict(row)

    def list_tenants(self) -> List[Dict[str, Any]]:
        rows = self.session.execute(select(TenantModel).order_by(TenantModel.tenant_id.asc())).scalars().all()
        return [self._tenant_to_dict(row) for row in rows]

    def get_tenant(self, tenant_id: str) -> Optional[Dict[str, Any]]:
        row = self.session.get(TenantModel, tenant_id)
        return self._tenant_to_dict(row) if row else None

    def upsert_platform_user(self, user_id: str, *, email: str | None = None, display_name: str | None = None) -> Dict[str, Any]:
        resolved_user_id = str(user_id).strip()
        row = self.session.get(PlatformUserModel, resolved_user_id)
        if row is None:
            row = PlatformUserModel(user_id=resolved_user_id, email=email, display_name=display_name)
            self.session.add(row)
        else:
            row.email = email or row.email
            row.display_name = display_name or row.display_name
            row.updated_at = datetime.utcnow()
        self.session.flush()
        return self._platform_user_to_dict(row)

    def get_platform_user(self, user_id: str) -> Optional[Dict[str, Any]]:
        row = self.session.get(PlatformUserModel, user_id)
        return self._platform_user_to_dict(row) if row else None

    def upsert_tenant_membership(self, tenant_id: str, user_id: str, *, role: str, status: str = "active") -> Dict[str, Any]:
        row = self.session.execute(
            select(TenantMembershipModel).where(
                TenantMembershipModel.tenant_id == str(tenant_id),
                TenantMembershipModel.user_id == str(user_id),
            )
        ).scalar_one_or_none()
        if row is None:
            row = TenantMembershipModel(
                tenant_id=str(tenant_id),
                user_id=str(user_id),
                role=str(role),
                status=str(status or "active"),
            )
            self.session.add(row)
        else:
            row.role = str(role)
            row.status = str(status or row.status)
            row.updated_at = datetime.utcnow()
        self.session.flush()
        return self._tenant_membership_to_dict(row)

    def get_tenant_membership(self, tenant_id: str, user_id: str) -> Optional[Dict[str, Any]]:
        row = self.session.execute(
            select(TenantMembershipModel).where(
                TenantMembershipModel.tenant_id == str(tenant_id),
                TenantMembershipModel.user_id == str(user_id),
            )
        ).scalar_one_or_none()
        return self._tenant_membership_to_dict(row) if row else None

    def list_tenant_memberships(self, tenant_id: str | None = None) -> List[Dict[str, Any]]:
        query = select(TenantMembershipModel)
        if tenant_id:
            query = query.where(TenantMembershipModel.tenant_id == str(tenant_id))
        rows = self.session.execute(query.order_by(TenantMembershipModel.tenant_id.asc(), TenantMembershipModel.user_id.asc())).scalars().all()
        return [self._tenant_membership_to_dict(row) for row in rows]

    def list_connectors(self, *, include_all_tenants: bool = False, tenant_id: str | None = None) -> List[Dict[str, Any]]:
        query = select(ConnectorConfigModel)
        resolved_tenant_id = None if include_all_tenants else self._resolve_tenant_id(tenant_id)
        if resolved_tenant_id:
            query = query.where(ConnectorConfigModel.tenant_id == resolved_tenant_id)
        rows = self.session.execute(query.order_by(ConnectorConfigModel.name.asc())).scalars().all()
        return [self._connector_to_dict(row) for row in rows]

    def get_connector(self, ref: str, *, tenant_id: str | None = None, include_all_tenants: bool = False) -> Optional[Dict[str, Any]]:
        query = select(ConnectorConfigModel).where(
            (ConnectorConfigModel.name == ref) | (ConnectorConfigModel.connector_id == ref)
        )
        resolved_tenant_id = None if include_all_tenants else self._resolve_tenant_id(tenant_id)
        if resolved_tenant_id:
            query = query.where(ConnectorConfigModel.tenant_id == resolved_tenant_id)
        row = self.session.execute(query.order_by(ConnectorConfigModel.updated_at.desc())).scalars().first()
        return self._connector_to_dict(row) if row else None

    def upsert_connector(self, name: str, connector_type: str, config: Dict[str, Any], *, connector_id: str | None = None, tenant_id: str | None = None) -> Dict[str, Any]:
        metadata = self._metadata(tenant_id)
        row = self.session.execute(
            select(ConnectorConfigModel).where(
                ConnectorConfigModel.tenant_id == metadata["tenant_id"],
                (ConnectorConfigModel.name == name) | (ConnectorConfigModel.connector_id == str(connector_id or "")),
            )
        ).scalar_one_or_none()
        resolved_connector_id = str(connector_id or (row.connector_id if row is not None else f"conn_{uuid.uuid4().hex[:20]}")).strip()
        if row is None:
            row = ConnectorConfigModel(
                tenant_id=metadata["tenant_id"],
                connector_id=resolved_connector_id,
                name=name,
                connector_type=connector_type,
                config_json=_to_json_text(self._augment_payload(config, tenant_id=metadata["tenant_id"], created_by=metadata["actor_id"], updated_by=metadata["actor_id"], correlation_id=metadata["correlation_id"])),
                created_by=metadata["actor_id"],
                updated_by=metadata["actor_id"],
                correlation_id=metadata["correlation_id"],
            )
            self.session.add(row)
        else:
            row.name = name
            row.connector_type = connector_type
            row.connector_id = resolved_connector_id
            row.config_json = _to_json_text(self._augment_payload(config, tenant_id=metadata["tenant_id"], created_by=row.created_by, updated_by=metadata["actor_id"], correlation_id=metadata["correlation_id"]))
            row.updated_by = metadata["actor_id"]
            row.correlation_id = metadata["correlation_id"]
            row.updated_at = datetime.utcnow()
        self.session.flush()
        return self._connector_to_dict(row)

    def delete_connector(self, ref: str, *, tenant_id: str | None = None) -> bool:
        query = select(ConnectorConfigModel).where(
            (ConnectorConfigModel.name == ref) | (ConnectorConfigModel.connector_id == ref)
        )
        resolved_tenant_id = self._resolve_tenant_id(tenant_id)
        if resolved_tenant_id:
            query = query.where(ConnectorConfigModel.tenant_id == resolved_tenant_id)
        row = self.session.execute(query.order_by(ConnectorConfigModel.updated_at.desc())).scalars().first()
        if row is None:
            return False
        self.session.delete(row)
        self.session.flush()
        return True

    def get_field_mapping(self, connector_name: str, *, tenant_id: str | None = None) -> Dict[str, Any]:
        resolved_tenant_id = self._resolve_tenant_id(tenant_id, fallback_to_bootstrap=True)
        row = self.session.execute(
            select(FieldMappingModel).where(
                FieldMappingModel.tenant_id == resolved_tenant_id,
                FieldMappingModel.connector_name == connector_name,
            )
        ).scalar_one_or_none()
        return _from_json_text(row.mapping_json) if row else {}

    def save_field_mapping(self, connector_name: str, mapping: Dict[str, Any], *, tenant_id: str | None = None) -> Dict[str, Any]:
        metadata = self._metadata(tenant_id)
        row = self.session.execute(
            select(FieldMappingModel).where(
                FieldMappingModel.tenant_id == metadata["tenant_id"],
                FieldMappingModel.connector_name == connector_name,
            )
        ).scalar_one_or_none()
        payload = self._augment_payload(mapping, tenant_id=metadata["tenant_id"], created_by=metadata["actor_id"], updated_by=metadata["actor_id"], correlation_id=metadata["correlation_id"])
        if row is None:
            row = FieldMappingModel(
                tenant_id=metadata["tenant_id"],
                connector_name=connector_name,
                mapping_json=_to_json_text(payload),
                created_by=metadata["actor_id"],
                updated_by=metadata["actor_id"],
                correlation_id=metadata["correlation_id"],
            )
            self.session.add(row)
        else:
            payload["created_by"] = row.created_by
            payload["updated_by"] = metadata["actor_id"]
            row.mapping_json = _to_json_text(payload)
            row.updated_by = metadata["actor_id"]
            row.correlation_id = metadata["correlation_id"]
            row.updated_at = datetime.utcnow()
        self.session.flush()
        return {"connector_name": connector_name, "mapping": payload}

    def create_import_job(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        metadata = self._metadata(payload.get("tenant_id"))
        spec = self._augment_payload(payload.get("spec", {}), tenant_id=metadata["tenant_id"], created_by=metadata["actor_id"], updated_by=metadata["actor_id"], correlation_id=metadata["correlation_id"])
        row = ImportJobModel(
            id=payload["id"],
            tenant_id=metadata["tenant_id"],
            source_name=payload["source_name"],
            status=payload["status"],
            spec_json=_to_json_text(spec),
            progress_json=_to_json_text(payload.get("progress", {})),
            error=payload.get("error"),
            created_by=metadata["actor_id"],
            updated_by=metadata["actor_id"],
            correlation_id=metadata["correlation_id"],
        )
        self.session.add(row)
        self.session.flush()
        return self._job_to_dict(row, "import")

    def list_import_jobs(self, *, tenant_id: str | None = None, include_all_tenants: bool = False) -> List[Dict[str, Any]]:
        query = select(ImportJobModel)
        resolved_tenant_id = None if include_all_tenants else self._resolve_tenant_id(tenant_id)
        if resolved_tenant_id:
            query = query.where(ImportJobModel.tenant_id == resolved_tenant_id)
        rows = self.session.execute(query.order_by(desc(ImportJobModel.created_at))).scalars().all()
        return [self._job_to_dict(row, "import") for row in rows]

    def get_import_job(self, job_id: str, *, tenant_id: str | None = None, include_all_tenants: bool = False) -> Optional[Dict[str, Any]]:
        query = select(ImportJobModel).where(ImportJobModel.id == job_id)
        resolved_tenant_id = None if include_all_tenants else self._resolve_tenant_id(tenant_id)
        if resolved_tenant_id:
            query = query.where(ImportJobModel.tenant_id == resolved_tenant_id)
        row = self.session.execute(query).scalar_one_or_none()
        return self._job_to_dict(row, "import") if row else None

    def update_import_job(self, job_id: str, patch: Dict[str, Any], *, tenant_id: str | None = None) -> Dict[str, Any]:
        row = self.session.execute(
            select(ImportJobModel).where(
                ImportJobModel.id == job_id,
                *self._job_scope_conditions(ImportJobModel, tenant_id),
            )
        ).scalar_one_or_none()
        if row is None:
            raise KeyError(job_id)
        self._apply_job_patch(row, patch, tenant_id=tenant_id)
        self.session.flush()
        return self._job_to_dict(row, "import")

    def delete_import_job(self, job_id: str, *, tenant_id: str | None = None) -> bool:
        row = self.session.execute(
            select(ImportJobModel).where(
                ImportJobModel.id == job_id,
                *self._job_scope_conditions(ImportJobModel, tenant_id),
            )
        ).scalar_one_or_none()
        if row is None:
            return False
        self.session.execute(
            delete(IngestionCheckpointModel).where(
                IngestionCheckpointModel.job_id == job_id,
                IngestionCheckpointModel.tenant_id == row.tenant_id,
            )
        )
        self.session.delete(row)
        self.session.flush()
        return True

    def create_prediction_job(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        metadata = self._metadata(payload.get("tenant_id"))
        spec = self._augment_payload(payload.get("spec", {}), tenant_id=metadata["tenant_id"], created_by=metadata["actor_id"], updated_by=metadata["actor_id"], correlation_id=metadata["correlation_id"])
        row = PredictionJobModel(
            id=payload["id"],
            tenant_id=metadata["tenant_id"],
            import_job_id=payload["import_job_id"],
            status=payload["status"],
            spec_json=_to_json_text(spec),
            progress_json=_to_json_text(payload.get("progress", {})),
            error=payload.get("error"),
            created_by=metadata["actor_id"],
            updated_by=metadata["actor_id"],
            correlation_id=metadata["correlation_id"],
        )
        self.session.add(row)
        self.session.flush()
        return self._job_to_dict(row, "prediction")

    def list_prediction_jobs(self, *, tenant_id: str | None = None, include_all_tenants: bool = False) -> List[Dict[str, Any]]:
        query = select(PredictionJobModel)
        resolved_tenant_id = None if include_all_tenants else self._resolve_tenant_id(tenant_id)
        if resolved_tenant_id:
            query = query.where(PredictionJobModel.tenant_id == resolved_tenant_id)
        rows = self.session.execute(query.order_by(desc(PredictionJobModel.created_at))).scalars().all()
        return [self._job_to_dict(row, "prediction") for row in rows]

    def get_prediction_job(self, job_id: str, *, tenant_id: str | None = None, include_all_tenants: bool = False) -> Optional[Dict[str, Any]]:
        query = select(PredictionJobModel).where(PredictionJobModel.id == job_id)
        resolved_tenant_id = None if include_all_tenants else self._resolve_tenant_id(tenant_id)
        if resolved_tenant_id:
            query = query.where(PredictionJobModel.tenant_id == resolved_tenant_id)
        row = self.session.execute(query).scalar_one_or_none()
        return self._job_to_dict(row, "prediction") if row else None

    def update_prediction_job(self, job_id: str, patch: Dict[str, Any], *, tenant_id: str | None = None) -> Dict[str, Any]:
        row = self.session.execute(
            select(PredictionJobModel).where(
                PredictionJobModel.id == job_id,
                *self._job_scope_conditions(PredictionJobModel, tenant_id),
            )
        ).scalar_one_or_none()
        if row is None:
            raise KeyError(job_id)
        self._apply_job_patch(row, patch, tenant_id=tenant_id)
        self.session.flush()
        return self._job_to_dict(row, "prediction")

    def delete_prediction_job(self, job_id: str, *, tenant_id: str | None = None) -> bool:
        row = self.session.execute(
            select(PredictionJobModel).where(
                PredictionJobModel.id == job_id,
                *self._job_scope_conditions(PredictionJobModel, tenant_id),
            )
        ).scalar_one_or_none()
        if row is None:
            return False
        self.session.execute(
            delete(ExportJobModel).where(
                ExportJobModel.prediction_job_id == job_id,
                ExportJobModel.tenant_id == row.tenant_id,
            )
        )
        self.session.delete(row)
        self.session.flush()
        return True

    def create_export_job(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        metadata = self._metadata(payload.get("tenant_id"))
        spec = self._augment_payload(payload.get("spec", {}), tenant_id=metadata["tenant_id"], created_by=metadata["actor_id"], updated_by=metadata["actor_id"], correlation_id=metadata["correlation_id"])
        row = ExportJobModel(
            id=payload["id"],
            tenant_id=metadata["tenant_id"],
            prediction_job_id=payload.get("prediction_job_id"),
            status=payload["status"],
            spec_json=_to_json_text(spec),
            progress_json=_to_json_text(payload.get("progress", {})),
            error=payload.get("error"),
            created_by=metadata["actor_id"],
            updated_by=metadata["actor_id"],
            correlation_id=metadata["correlation_id"],
        )
        self.session.add(row)
        self.session.flush()
        return self._job_to_dict(row, "export")

    def list_export_jobs(self, *, tenant_id: str | None = None, include_all_tenants: bool = False) -> List[Dict[str, Any]]:
        query = select(ExportJobModel)
        resolved_tenant_id = None if include_all_tenants else self._resolve_tenant_id(tenant_id)
        if resolved_tenant_id:
            query = query.where(ExportJobModel.tenant_id == resolved_tenant_id)
        rows = self.session.execute(query.order_by(desc(ExportJobModel.created_at))).scalars().all()
        return [self._job_to_dict(row, "export") for row in rows]

    def get_export_job(self, job_id: str, *, tenant_id: str | None = None, include_all_tenants: bool = False) -> Optional[Dict[str, Any]]:
        query = select(ExportJobModel).where(ExportJobModel.id == job_id)
        resolved_tenant_id = None if include_all_tenants else self._resolve_tenant_id(tenant_id)
        if resolved_tenant_id:
            query = query.where(ExportJobModel.tenant_id == resolved_tenant_id)
        row = self.session.execute(query).scalar_one_or_none()
        return self._job_to_dict(row, "export") if row else None

    def update_export_job(self, job_id: str, patch: Dict[str, Any], *, tenant_id: str | None = None) -> Dict[str, Any]:
        row = self.session.execute(
            select(ExportJobModel).where(
                ExportJobModel.id == job_id,
                *self._job_scope_conditions(ExportJobModel, tenant_id),
            )
        ).scalar_one_or_none()
        if row is None:
            raise KeyError(job_id)
        self._apply_job_patch(row, patch, tenant_id=tenant_id)
        self.session.flush()
        return self._job_to_dict(row, "export")

    def upsert_checkpoint(self, payload: Dict[str, Any], *, tenant_id: str | None = None) -> Dict[str, Any]:
        metadata = self._metadata(tenant_id)
        query = select(IngestionCheckpointModel).where(
            IngestionCheckpointModel.tenant_id == metadata["tenant_id"],
            IngestionCheckpointModel.job_id == payload["job_id"],
            IngestionCheckpointModel.shard_index == int(payload["shard_index"]),
        )
        row = self.session.execute(query).scalar_one_or_none()
        enriched_payload = self._augment_payload(payload, tenant_id=metadata["tenant_id"], created_by=metadata["actor_id"], updated_by=metadata["actor_id"], correlation_id=metadata["correlation_id"])
        if row is None:
            row = IngestionCheckpointModel(
                tenant_id=metadata["tenant_id"],
                job_id=payload["job_id"],
                shard_index=int(payload["shard_index"]),
                source_name=payload["source_name"],
                status=payload["status"],
                cursor_value=payload.get("cursor"),
                gcs_uri=payload.get("gcs_uri"),
                message_id=payload.get("message_id"),
                payload_json=_to_json_text(enriched_payload),
                created_by=metadata["actor_id"],
                updated_by=metadata["actor_id"],
                correlation_id=metadata["correlation_id"],
            )
            self.session.add(row)
        else:
            row.source_name = payload["source_name"]
            row.status = payload["status"]
            row.cursor_value = payload.get("cursor")
            row.gcs_uri = payload.get("gcs_uri")
            row.message_id = payload.get("message_id")
            enriched_payload["created_by"] = row.created_by
            enriched_payload["updated_by"] = metadata["actor_id"]
            row.payload_json = _to_json_text(enriched_payload)
            row.updated_by = metadata["actor_id"]
            row.correlation_id = metadata["correlation_id"]
            row.updated_at = datetime.utcnow()
        self.session.flush()
        return self._checkpoint_to_dict(row)

    def list_checkpoints(self, job_id: str, *, tenant_id: str | None = None, include_all_tenants: bool = False) -> List[Dict[str, Any]]:
        query = select(IngestionCheckpointModel).where(IngestionCheckpointModel.job_id == job_id)
        resolved_tenant_id = None if include_all_tenants else self._resolve_tenant_id(tenant_id)
        if resolved_tenant_id:
            query = query.where(IngestionCheckpointModel.tenant_id == resolved_tenant_id)
        rows = self.session.execute(
            query.order_by(IngestionCheckpointModel.shard_index.asc())
        ).scalars().all()
        return [self._checkpoint_to_dict(row) for row in rows]

    def record_action(self, action_type: str, resource_type: str, resource_id: Optional[str], payload: Dict[str, Any], *, tenant_id: str | None = None) -> Dict[str, Any]:
        metadata = self._metadata(tenant_id)
        enriched_payload = {
            "tenant_id": metadata["tenant_id"],
            "actor_id": metadata["actor_id"],
            "correlation_id": metadata["correlation_id"],
            **payload,
        }
        row = ActionHistoryModel(
            tenant_id=metadata["tenant_id"],
            actor_id=metadata["actor_id"],
            correlation_id=metadata["correlation_id"],
            action_type=action_type,
            resource_type=resource_type,
            resource_id=resource_id,
            payload_json=_to_json_text(enriched_payload),
        )
        self.session.add(row)
        self.session.flush()
        return self._action_to_dict(row)

    def list_actions(self, limit: int = 200, *, tenant_id: str | None = None, include_all_tenants: bool = False) -> List[Dict[str, Any]]:
        query = select(ActionHistoryModel)
        resolved_tenant_id = None if include_all_tenants else self._resolve_tenant_id(tenant_id)
        if resolved_tenant_id:
            query = query.where(ActionHistoryModel.tenant_id == resolved_tenant_id)
        rows = self.session.execute(
            query.order_by(desc(ActionHistoryModel.created_at)).limit(max(1, int(limit)))
        ).scalars().all()
        return [self._action_to_dict(row) for row in rows]

    def get_experiment_config(self, key: str = "default", *, tenant_id: str | None = None) -> Dict[str, Any]:
        resolved_tenant_id = self._resolve_tenant_id(tenant_id, fallback_to_bootstrap=True)
        row = self.session.execute(
            select(ExperimentConfigModel).where(
                ExperimentConfigModel.tenant_id == resolved_tenant_id,
                ExperimentConfigModel.config_key == key,
            )
        ).scalar_one_or_none()
        if row is None:
            if key != "default":
                return {}
            return {
                "experiment_id": "churn_engagement_v1",
                "enabled": True,
                "holdout_pct": 0.10,
                "tenant_id": resolved_tenant_id,
            }
        return _from_json_text(row.config_json)

    def save_experiment_config(self, config: Dict[str, Any], key: str = "default", *, tenant_id: str | None = None) -> Dict[str, Any]:
        metadata = self._metadata(tenant_id)
        row = self.session.execute(
            select(ExperimentConfigModel).where(
                ExperimentConfigModel.tenant_id == metadata["tenant_id"],
                ExperimentConfigModel.config_key == key,
            )
        ).scalar_one_or_none()
        payload = self._augment_payload(config, tenant_id=metadata["tenant_id"], created_by=metadata["actor_id"], updated_by=metadata["actor_id"], correlation_id=metadata["correlation_id"])
        if row is None:
            row = ExperimentConfigModel(
                tenant_id=metadata["tenant_id"],
                config_key=key,
                config_json=_to_json_text(payload),
                created_by=metadata["actor_id"],
                updated_by=metadata["actor_id"],
                correlation_id=metadata["correlation_id"],
            )
            self.session.add(row)
        else:
            payload["created_by"] = row.created_by
            payload["updated_by"] = metadata["actor_id"]
            row.config_json = _to_json_text(payload)
            row.updated_by = metadata["actor_id"]
            row.correlation_id = metadata["correlation_id"]
            row.updated_at = datetime.utcnow()
        self.session.flush()
        return _from_json_text(row.config_json)

    def get_resource(self, resource_type: str, resource_id: str, *, tenant_id: str | None = None, include_all_tenants: bool = False) -> Optional[Dict[str, Any]]:
        query = select(ControlPlaneResourceModel).where(
            ControlPlaneResourceModel.resource_type == resource_type,
            ControlPlaneResourceModel.resource_id == resource_id,
        )
        resolved_tenant_id = None if include_all_tenants else self._resolve_tenant_id(tenant_id)
        if resolved_tenant_id:
            query = query.where(ControlPlaneResourceModel.tenant_id == resolved_tenant_id)
        row = self.session.execute(query).scalar_one_or_none()
        return self._resource_to_dict(row) if row else None

    def list_resources(self, resource_type: str, *, tenant_id: str | None = None, include_all_tenants: bool = False) -> List[Dict[str, Any]]:
        query = select(ControlPlaneResourceModel).where(ControlPlaneResourceModel.resource_type == resource_type)
        resolved_tenant_id = None if include_all_tenants else self._resolve_tenant_id(tenant_id)
        if resolved_tenant_id:
            query = query.where(ControlPlaneResourceModel.tenant_id == resolved_tenant_id)
        rows = self.session.execute(
            query.order_by(ControlPlaneResourceModel.updated_at.desc())
        ).scalars().all()
        return [self._resource_to_dict(row) for row in rows]

    def upsert_resource(
        self,
        resource_type: str,
        resource_id: str,
        *,
        status: str,
        payload: Dict[str, Any],
        name: str | None = None,
        tenant_id: str | None = None,
    ) -> Dict[str, Any]:
        metadata = self._metadata(tenant_id)
        row = self.session.execute(
            select(ControlPlaneResourceModel).where(
                ControlPlaneResourceModel.tenant_id == metadata["tenant_id"],
                ControlPlaneResourceModel.resource_type == resource_type,
                ControlPlaneResourceModel.resource_id == resource_id,
            )
        ).scalar_one_or_none()
        enriched_payload = self._augment_payload(payload, tenant_id=metadata["tenant_id"], created_by=metadata["actor_id"], updated_by=metadata["actor_id"], correlation_id=metadata["correlation_id"])
        if row is None:
            row = ControlPlaneResourceModel(
                tenant_id=metadata["tenant_id"],
                resource_type=resource_type,
                resource_id=resource_id,
                name=name,
                status=status,
                payload_json=_to_json_text(enriched_payload),
                created_by=metadata["actor_id"],
                updated_by=metadata["actor_id"],
                correlation_id=metadata["correlation_id"],
            )
            self.session.add(row)
        else:
            enriched_payload["created_by"] = row.created_by
            enriched_payload["updated_by"] = metadata["actor_id"]
            row.name = name if name is not None else row.name
            row.status = status
            row.payload_json = _to_json_text(enriched_payload)
            row.updated_by = metadata["actor_id"]
            row.correlation_id = metadata["correlation_id"]
            row.updated_at = datetime.utcnow()
        self.session.flush()
        return self._resource_to_dict(row)

    def delete_resource(self, resource_type: str, resource_id: str, *, tenant_id: str | None = None) -> bool:
        resolved_tenant_id = self._resolve_tenant_id(tenant_id)
        query = select(ControlPlaneResourceModel).where(
            ControlPlaneResourceModel.resource_type == resource_type,
            ControlPlaneResourceModel.resource_id == resource_id,
        )
        if resolved_tenant_id:
            query = query.where(ControlPlaneResourceModel.tenant_id == resolved_tenant_id)
        row = self.session.execute(query).scalar_one_or_none()
        if row is None:
            return False
        self.session.execute(
            delete(ControlPlaneResourceVersionModel).where(
                ControlPlaneResourceVersionModel.tenant_id == row.tenant_id,
                ControlPlaneResourceVersionModel.resource_type == resource_type,
                ControlPlaneResourceVersionModel.resource_id == resource_id,
            )
        )
        self.session.execute(
            delete(ControlPlaneResourceEventModel).where(
                ControlPlaneResourceEventModel.tenant_id == row.tenant_id,
                ControlPlaneResourceEventModel.resource_type == resource_type,
                ControlPlaneResourceEventModel.resource_id == resource_id,
            )
        )
        self.session.delete(row)
        self.session.flush()
        return True

    def create_resource_version(
        self,
        resource_type: str,
        resource_id: str,
        *,
        version: int,
        payload: Dict[str, Any],
        tenant_id: str | None = None,
    ) -> Dict[str, Any]:
        metadata = self._metadata(tenant_id)
        row = self.session.execute(
            select(ControlPlaneResourceVersionModel).where(
                ControlPlaneResourceVersionModel.tenant_id == metadata["tenant_id"],
                ControlPlaneResourceVersionModel.resource_type == resource_type,
                ControlPlaneResourceVersionModel.resource_id == resource_id,
                ControlPlaneResourceVersionModel.version == int(version),
            )
        ).scalar_one_or_none()
        enriched_payload = self._augment_payload(payload, tenant_id=metadata["tenant_id"], created_by=metadata["actor_id"], updated_by=metadata["actor_id"], correlation_id=metadata["correlation_id"])
        if row is None:
            row = ControlPlaneResourceVersionModel(
                tenant_id=metadata["tenant_id"],
                resource_type=resource_type,
                resource_id=resource_id,
                version=int(version),
                payload_json=_to_json_text(enriched_payload),
                created_by=metadata["actor_id"],
                correlation_id=metadata["correlation_id"],
            )
            self.session.add(row)
        else:
            enriched_payload["created_by"] = row.created_by
            row.payload_json = _to_json_text(enriched_payload)
            row.correlation_id = metadata["correlation_id"]
        self.session.flush()
        return self._resource_version_to_dict(row)

    def list_resource_versions(self, resource_type: str, resource_id: str, *, tenant_id: str | None = None, include_all_tenants: bool = False) -> List[Dict[str, Any]]:
        query = select(ControlPlaneResourceVersionModel).where(
            ControlPlaneResourceVersionModel.resource_type == resource_type,
            ControlPlaneResourceVersionModel.resource_id == resource_id,
        )
        resolved_tenant_id = None if include_all_tenants else self._resolve_tenant_id(tenant_id)
        if resolved_tenant_id:
            query = query.where(ControlPlaneResourceVersionModel.tenant_id == resolved_tenant_id)
        rows = self.session.execute(
            query.order_by(ControlPlaneResourceVersionModel.version.desc())
        ).scalars().all()
        return [self._resource_version_to_dict(row) for row in rows]

    def record_resource_event(
        self,
        resource_type: str,
        resource_id: str,
        *,
        event_type: str,
        payload: Dict[str, Any],
        tenant_id: str | None = None,
    ) -> Dict[str, Any]:
        metadata = self._metadata(tenant_id)
        enriched_payload = self._augment_payload(payload, tenant_id=metadata["tenant_id"], created_by=metadata["actor_id"], updated_by=metadata["actor_id"], correlation_id=metadata["correlation_id"])
        row = ControlPlaneResourceEventModel(
            tenant_id=metadata["tenant_id"],
            resource_type=resource_type,
            resource_id=resource_id,
            event_type=event_type,
            payload_json=_to_json_text(enriched_payload),
            created_by=metadata["actor_id"],
            correlation_id=metadata["correlation_id"],
        )
        self.session.add(row)
        self.session.flush()
        return self._resource_event_to_dict(row)

    def list_resource_events(
        self,
        resource_type: str,
        resource_id: str | None = None,
        *,
        event_type: str | None = None,
        limit: int = 200,
        tenant_id: str | None = None,
        include_all_tenants: bool = False,
    ) -> List[Dict[str, Any]]:
        query = select(ControlPlaneResourceEventModel).where(ControlPlaneResourceEventModel.resource_type == resource_type)
        resolved_tenant_id = None if include_all_tenants else self._resolve_tenant_id(tenant_id)
        if resolved_tenant_id:
            query = query.where(ControlPlaneResourceEventModel.tenant_id == resolved_tenant_id)
        if resource_id is not None:
            query = query.where(ControlPlaneResourceEventModel.resource_id == resource_id)
        if event_type is not None:
            query = query.where(ControlPlaneResourceEventModel.event_type == event_type)
        rows = self.session.execute(
            query.order_by(desc(ControlPlaneResourceEventModel.created_at)).limit(max(1, int(limit)))
        ).scalars().all()
        return [self._resource_event_to_dict(row) for row in rows]

    def _job_scope_conditions(self, model: Any, tenant_id: str | None) -> List[Any]:
        resolved_tenant_id = self._resolve_tenant_id(tenant_id)
        return [model.tenant_id == resolved_tenant_id] if resolved_tenant_id else []

    def _tenant_to_dict(self, row: TenantModel) -> Dict[str, Any]:
        return {
            "tenant_id": row.tenant_id,
            "name": row.name,
            "status": row.status,
            "created_at": row.created_at.isoformat(),
            "updated_at": row.updated_at.isoformat(),
        }

    def _platform_user_to_dict(self, row: PlatformUserModel) -> Dict[str, Any]:
        return {
            "user_id": row.user_id,
            "email": row.email,
            "display_name": row.display_name,
            "created_at": row.created_at.isoformat(),
            "updated_at": row.updated_at.isoformat(),
        }

    def _tenant_membership_to_dict(self, row: TenantMembershipModel) -> Dict[str, Any]:
        return {
            "id": row.id,
            "tenant_id": row.tenant_id,
            "user_id": row.user_id,
            "role": row.role,
            "status": row.status,
            "created_at": row.created_at.isoformat(),
            "updated_at": row.updated_at.isoformat(),
        }

    def _connector_to_dict(self, row: ConnectorConfigModel) -> Dict[str, Any]:
        payload = _from_json_text(row.config_json)
        return {
            "tenant_id": row.tenant_id,
            "connector_id": row.connector_id,
            "name": row.name,
            "type": row.connector_type,
            "config": payload,
            "created_by": row.created_by,
            "updated_by": row.updated_by,
            "correlation_id": row.correlation_id,
            "created_at": row.created_at.isoformat(),
            "updated_at": row.updated_at.isoformat(),
        }

    def _job_to_dict(self, row: Any, job_type: str) -> Dict[str, Any]:
        spec = _from_json_text(row.spec_json)
        progress = _from_json_text(row.progress_json)
        return {
            "tenant_id": row.tenant_id,
            "id": row.id,
            "type": job_type,
            "status": row.status,
            "spec": spec,
            "progress": progress,
            "error": row.error,
            "created_by": row.created_by,
            "updated_by": row.updated_by,
            "correlation_id": row.correlation_id,
            "created_at": row.created_at.isoformat(),
            "updated_at": row.updated_at.isoformat(),
        }

    def _checkpoint_to_dict(self, row: IngestionCheckpointModel) -> Dict[str, Any]:
        payload = _from_json_text(row.payload_json)
        payload.setdefault("tenant_id", row.tenant_id)
        payload.setdefault("job_id", row.job_id)
        payload.setdefault("shard_index", row.shard_index)
        payload.setdefault("source_name", row.source_name)
        payload.setdefault("status", row.status)
        payload.setdefault("cursor", row.cursor_value)
        payload.setdefault("gcs_uri", row.gcs_uri)
        payload.setdefault("message_id", row.message_id)
        payload.setdefault("created_by", row.created_by)
        payload.setdefault("updated_by", row.updated_by)
        payload.setdefault("correlation_id", row.correlation_id)
        payload["created_at"] = row.created_at.isoformat()
        payload["updated_at"] = row.updated_at.isoformat()
        return payload

    def _action_to_dict(self, row: ActionHistoryModel) -> Dict[str, Any]:
        return {
            "id": row.id,
            "tenant_id": row.tenant_id,
            "actor_id": row.actor_id,
            "correlation_id": row.correlation_id,
            "action_type": row.action_type,
            "resource_type": row.resource_type,
            "resource_id": row.resource_id,
            "payload": _from_json_text(row.payload_json),
            "created_at": row.created_at.isoformat(),
        }

    def _resource_to_dict(self, row: ControlPlaneResourceModel) -> Dict[str, Any]:
        payload = _from_json_text(row.payload_json)
        return {
            "tenant_id": row.tenant_id,
            "resource_type": row.resource_type,
            "resource_id": row.resource_id,
            "name": row.name,
            "status": row.status,
            "payload": payload,
            "created_by": row.created_by,
            "updated_by": row.updated_by,
            "correlation_id": row.correlation_id,
            "created_at": row.created_at.isoformat(),
            "updated_at": row.updated_at.isoformat(),
        }

    def _resource_version_to_dict(self, row: ControlPlaneResourceVersionModel) -> Dict[str, Any]:
        return {
            "id": row.id,
            "tenant_id": row.tenant_id,
            "resource_type": row.resource_type,
            "resource_id": row.resource_id,
            "version": row.version,
            "payload": _from_json_text(row.payload_json),
            "created_by": row.created_by,
            "correlation_id": row.correlation_id,
            "created_at": row.created_at.isoformat(),
        }

    def _resource_event_to_dict(self, row: ControlPlaneResourceEventModel) -> Dict[str, Any]:
        return {
            "id": row.id,
            "tenant_id": row.tenant_id,
            "resource_type": row.resource_type,
            "resource_id": row.resource_id,
            "event_type": row.event_type,
            "payload": _from_json_text(row.payload_json),
            "created_by": row.created_by,
            "correlation_id": row.correlation_id,
            "created_at": row.created_at.isoformat(),
        }

    def _apply_job_patch(self, row: Any, patch: Dict[str, Any], *, tenant_id: str | None = None) -> None:
        metadata = self._metadata(tenant_id or getattr(row, "tenant_id", None))
        if "status" in patch:
            row.status = patch["status"]
        if "spec" in patch:
            spec_payload = self._augment_payload(
                patch["spec"],
                tenant_id=metadata["tenant_id"],
                created_by=getattr(row, "created_by", "system"),
                updated_by=metadata["actor_id"],
                correlation_id=metadata["correlation_id"],
            )
            row.spec_json = _to_json_text(spec_payload)
        if "progress" in patch:
            row.progress_json = _to_json_text(patch["progress"])
        if "error" in patch:
            row.error = patch["error"]
        row.updated_by = metadata["actor_id"]
        row.correlation_id = metadata["correlation_id"]
        row.updated_at = datetime.utcnow()
