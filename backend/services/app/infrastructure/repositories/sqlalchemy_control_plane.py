from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Dict, List, Optional

from sqlalchemy import delete, desc, select
from sqlalchemy.orm import Session

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
    PredictionJobModel,
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

    def list_connectors(self) -> List[Dict[str, Any]]:
        rows = self.session.execute(select(ConnectorConfigModel).order_by(ConnectorConfigModel.name.asc())).scalars().all()
        return [self._connector_to_dict(row) for row in rows]

    def get_connector(self, name: str) -> Optional[Dict[str, Any]]:
        row = self.session.execute(select(ConnectorConfigModel).where(ConnectorConfigModel.name == name)).scalar_one_or_none()
        return self._connector_to_dict(row) if row else None

    def upsert_connector(self, name: str, connector_type: str, config: Dict[str, Any]) -> Dict[str, Any]:
        row = self.session.execute(select(ConnectorConfigModel).where(ConnectorConfigModel.name == name)).scalar_one_or_none()
        if row is None:
            row = ConnectorConfigModel(name=name, connector_type=connector_type, config_json=_to_json_text(config))
            self.session.add(row)
        else:
            row.connector_type = connector_type
            row.config_json = _to_json_text(config)
            row.updated_at = datetime.utcnow()
        self.session.flush()
        return self._connector_to_dict(row)

    def delete_connector(self, name: str) -> bool:
        row = self.session.execute(select(ConnectorConfigModel).where(ConnectorConfigModel.name == name)).scalar_one_or_none()
        if row is None:
            return False
        self.session.delete(row)
        self.session.flush()
        return True

    def get_field_mapping(self, connector_name: str) -> Dict[str, Any]:
        row = self.session.get(FieldMappingModel, connector_name)
        return _from_json_text(row.mapping_json) if row else {}

    def save_field_mapping(self, connector_name: str, mapping: Dict[str, Any]) -> Dict[str, Any]:
        row = self.session.get(FieldMappingModel, connector_name)
        if row is None:
            row = FieldMappingModel(connector_name=connector_name, mapping_json=_to_json_text(mapping))
            self.session.add(row)
        else:
            row.mapping_json = _to_json_text(mapping)
            row.updated_at = datetime.utcnow()
        self.session.flush()
        return {"connector_name": connector_name, "mapping": mapping}

    def create_import_job(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        row = ImportJobModel(
            id=payload["id"],
            source_name=payload["source_name"],
            status=payload["status"],
            spec_json=_to_json_text(payload.get("spec", {})),
            progress_json=_to_json_text(payload.get("progress", {})),
            error=payload.get("error"),
        )
        self.session.add(row)
        self.session.flush()
        return self._job_to_dict(row, "import")

    def list_import_jobs(self) -> List[Dict[str, Any]]:
        rows = self.session.execute(select(ImportJobModel).order_by(desc(ImportJobModel.created_at))).scalars().all()
        return [self._job_to_dict(row, "import") for row in rows]

    def get_import_job(self, job_id: str) -> Optional[Dict[str, Any]]:
        row = self.session.get(ImportJobModel, job_id)
        return self._job_to_dict(row, "import") if row else None

    def update_import_job(self, job_id: str, patch: Dict[str, Any]) -> Dict[str, Any]:
        row = self.session.get(ImportJobModel, job_id)
        if row is None:
            raise KeyError(job_id)
        self._apply_job_patch(row, patch)
        self.session.flush()
        return self._job_to_dict(row, "import")

    def delete_import_job(self, job_id: str) -> bool:
        row = self.session.get(ImportJobModel, job_id)
        if row is None:
            return False
        self.session.execute(delete(IngestionCheckpointModel).where(IngestionCheckpointModel.job_id == job_id))
        self.session.delete(row)
        self.session.flush()
        return True

    def create_prediction_job(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        row = PredictionJobModel(
            id=payload["id"],
            import_job_id=payload["import_job_id"],
            status=payload["status"],
            spec_json=_to_json_text(payload.get("spec", {})),
            progress_json=_to_json_text(payload.get("progress", {})),
            error=payload.get("error"),
        )
        self.session.add(row)
        self.session.flush()
        return self._job_to_dict(row, "prediction")

    def list_prediction_jobs(self) -> List[Dict[str, Any]]:
        rows = self.session.execute(select(PredictionJobModel).order_by(desc(PredictionJobModel.created_at))).scalars().all()
        return [self._job_to_dict(row, "prediction") for row in rows]

    def get_prediction_job(self, job_id: str) -> Optional[Dict[str, Any]]:
        row = self.session.get(PredictionJobModel, job_id)
        return self._job_to_dict(row, "prediction") if row else None

    def update_prediction_job(self, job_id: str, patch: Dict[str, Any]) -> Dict[str, Any]:
        row = self.session.get(PredictionJobModel, job_id)
        if row is None:
            raise KeyError(job_id)
        self._apply_job_patch(row, patch)
        self.session.flush()
        return self._job_to_dict(row, "prediction")

    def delete_prediction_job(self, job_id: str) -> bool:
        row = self.session.get(PredictionJobModel, job_id)
        if row is None:
            return False
        self.session.execute(delete(ExportJobModel).where(ExportJobModel.prediction_job_id == job_id))
        self.session.delete(row)
        self.session.flush()
        return True

    def create_export_job(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        row = ExportJobModel(
            id=payload["id"],
            prediction_job_id=payload.get("prediction_job_id"),
            status=payload["status"],
            spec_json=_to_json_text(payload.get("spec", {})),
            progress_json=_to_json_text(payload.get("progress", {})),
            error=payload.get("error"),
        )
        self.session.add(row)
        self.session.flush()
        return self._job_to_dict(row, "export")

    def list_export_jobs(self) -> List[Dict[str, Any]]:
        rows = self.session.execute(select(ExportJobModel).order_by(desc(ExportJobModel.created_at))).scalars().all()
        return [self._job_to_dict(row, "export") for row in rows]

    def get_export_job(self, job_id: str) -> Optional[Dict[str, Any]]:
        row = self.session.get(ExportJobModel, job_id)
        return self._job_to_dict(row, "export") if row else None

    def update_export_job(self, job_id: str, patch: Dict[str, Any]) -> Dict[str, Any]:
        row = self.session.get(ExportJobModel, job_id)
        if row is None:
            raise KeyError(job_id)
        self._apply_job_patch(row, patch)
        self.session.flush()
        return self._job_to_dict(row, "export")

    def upsert_checkpoint(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        query = select(IngestionCheckpointModel).where(
            IngestionCheckpointModel.job_id == payload["job_id"],
            IngestionCheckpointModel.shard_index == int(payload["shard_index"]),
        )
        row = self.session.execute(query).scalar_one_or_none()
        if row is None:
            row = IngestionCheckpointModel(
                job_id=payload["job_id"],
                shard_index=int(payload["shard_index"]),
                source_name=payload["source_name"],
                status=payload["status"],
                cursor_value=payload.get("cursor"),
                gcs_uri=payload.get("gcs_uri"),
                message_id=payload.get("message_id"),
                payload_json=_to_json_text(payload),
            )
            self.session.add(row)
        else:
            row.source_name = payload["source_name"]
            row.status = payload["status"]
            row.cursor_value = payload.get("cursor")
            row.gcs_uri = payload.get("gcs_uri")
            row.message_id = payload.get("message_id")
            row.payload_json = _to_json_text(payload)
            row.updated_at = datetime.utcnow()
        self.session.flush()
        return self._checkpoint_to_dict(row)

    def list_checkpoints(self, job_id: str) -> List[Dict[str, Any]]:
        rows = self.session.execute(
            select(IngestionCheckpointModel)
            .where(IngestionCheckpointModel.job_id == job_id)
            .order_by(IngestionCheckpointModel.shard_index.asc())
        ).scalars().all()
        return [self._checkpoint_to_dict(row) for row in rows]

    def record_action(self, action_type: str, resource_type: str, resource_id: Optional[str], payload: Dict[str, Any]) -> Dict[str, Any]:
        row = ActionHistoryModel(
            action_type=action_type,
            resource_type=resource_type,
            resource_id=resource_id,
            payload_json=_to_json_text(payload),
        )
        self.session.add(row)
        self.session.flush()
        return self._action_to_dict(row)

    def list_actions(self, limit: int = 200) -> List[Dict[str, Any]]:
        rows = self.session.execute(
            select(ActionHistoryModel).order_by(desc(ActionHistoryModel.created_at)).limit(max(1, int(limit)))
        ).scalars().all()
        return [self._action_to_dict(row) for row in rows]

    def get_experiment_config(self, key: str = "default") -> Dict[str, Any]:
        row = self.session.get(ExperimentConfigModel, key)
        if row is None:
            if key != "default":
                return {}
            return {
                "experiment_id": "churn_engagement_v1",
                "enabled": True,
                "holdout_pct": 0.10,
                "b_variant_pct": 0.50,
            }
        return _from_json_text(row.config_json)

    def save_experiment_config(self, config: Dict[str, Any], key: str = "default") -> Dict[str, Any]:
        row = self.session.get(ExperimentConfigModel, key)
        if row is None:
            row = ExperimentConfigModel(config_key=key, config_json=_to_json_text(config))
            self.session.add(row)
        else:
            row.config_json = _to_json_text(config)
            row.updated_at = datetime.utcnow()
        self.session.flush()
        return _from_json_text(row.config_json)

    def get_resource(self, resource_type: str, resource_id: str) -> Optional[Dict[str, Any]]:
        row = self.session.execute(
            select(ControlPlaneResourceModel).where(
                ControlPlaneResourceModel.resource_type == resource_type,
                ControlPlaneResourceModel.resource_id == resource_id,
            )
        ).scalar_one_or_none()
        return self._resource_to_dict(row) if row else None

    def list_resources(self, resource_type: str) -> List[Dict[str, Any]]:
        rows = self.session.execute(
            select(ControlPlaneResourceModel)
            .where(ControlPlaneResourceModel.resource_type == resource_type)
            .order_by(ControlPlaneResourceModel.updated_at.desc())
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
    ) -> Dict[str, Any]:
        row = self.session.execute(
            select(ControlPlaneResourceModel).where(
                ControlPlaneResourceModel.resource_type == resource_type,
                ControlPlaneResourceModel.resource_id == resource_id,
            )
        ).scalar_one_or_none()
        if row is None:
            row = ControlPlaneResourceModel(
                resource_type=resource_type,
                resource_id=resource_id,
                name=name,
                status=status,
                payload_json=_to_json_text(payload),
            )
            self.session.add(row)
        else:
            row.name = name if name is not None else row.name
            row.status = status
            row.payload_json = _to_json_text(payload)
            row.updated_at = datetime.utcnow()
        self.session.flush()
        return self._resource_to_dict(row)

    def delete_resource(self, resource_type: str, resource_id: str) -> bool:
        row = self.session.execute(
            select(ControlPlaneResourceModel).where(
                ControlPlaneResourceModel.resource_type == resource_type,
                ControlPlaneResourceModel.resource_id == resource_id,
            )
        ).scalar_one_or_none()
        if row is None:
            return False
        self.session.execute(
            delete(ControlPlaneResourceVersionModel).where(
                ControlPlaneResourceVersionModel.resource_type == resource_type,
                ControlPlaneResourceVersionModel.resource_id == resource_id,
            )
        )
        self.session.execute(
            delete(ControlPlaneResourceEventModel).where(
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
    ) -> Dict[str, Any]:
        row = self.session.execute(
            select(ControlPlaneResourceVersionModel).where(
                ControlPlaneResourceVersionModel.resource_type == resource_type,
                ControlPlaneResourceVersionModel.resource_id == resource_id,
                ControlPlaneResourceVersionModel.version == int(version),
            )
        ).scalar_one_or_none()
        if row is None:
            row = ControlPlaneResourceVersionModel(
                resource_type=resource_type,
                resource_id=resource_id,
                version=int(version),
                payload_json=_to_json_text(payload),
            )
            self.session.add(row)
        else:
            row.payload_json = _to_json_text(payload)
        self.session.flush()
        return self._resource_version_to_dict(row)

    def list_resource_versions(self, resource_type: str, resource_id: str) -> List[Dict[str, Any]]:
        rows = self.session.execute(
            select(ControlPlaneResourceVersionModel)
            .where(
                ControlPlaneResourceVersionModel.resource_type == resource_type,
                ControlPlaneResourceVersionModel.resource_id == resource_id,
            )
            .order_by(ControlPlaneResourceVersionModel.version.desc())
        ).scalars().all()
        return [self._resource_version_to_dict(row) for row in rows]

    def record_resource_event(
        self,
        resource_type: str,
        resource_id: str,
        *,
        event_type: str,
        payload: Dict[str, Any],
    ) -> Dict[str, Any]:
        row = ControlPlaneResourceEventModel(
            resource_type=resource_type,
            resource_id=resource_id,
            event_type=event_type,
            payload_json=_to_json_text(payload),
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
    ) -> List[Dict[str, Any]]:
        query = select(ControlPlaneResourceEventModel).where(ControlPlaneResourceEventModel.resource_type == resource_type)
        if resource_id is not None:
            query = query.where(ControlPlaneResourceEventModel.resource_id == resource_id)
        if event_type is not None:
            query = query.where(ControlPlaneResourceEventModel.event_type == event_type)
        rows = self.session.execute(
            query.order_by(desc(ControlPlaneResourceEventModel.created_at)).limit(max(1, int(limit)))
        ).scalars().all()
        return [self._resource_event_to_dict(row) for row in rows]

    def _connector_to_dict(self, row: ConnectorConfigModel) -> Dict[str, Any]:
        return {
            "name": row.name,
            "type": row.connector_type,
            "config": _from_json_text(row.config_json),
            "created_at": row.created_at.isoformat(),
            "updated_at": row.updated_at.isoformat(),
        }

    def _job_to_dict(self, row: Any, job_type: str) -> Dict[str, Any]:
        spec = _from_json_text(row.spec_json)
        progress = _from_json_text(row.progress_json)
        return {
            "id": row.id,
            "type": job_type,
            "status": row.status,
            "spec": spec,
            "progress": progress,
            "error": row.error,
            "created_at": row.created_at.isoformat(),
            "updated_at": row.updated_at.isoformat(),
        }

    def _checkpoint_to_dict(self, row: IngestionCheckpointModel) -> Dict[str, Any]:
        payload = _from_json_text(row.payload_json)
        payload.setdefault("job_id", row.job_id)
        payload.setdefault("shard_index", row.shard_index)
        payload.setdefault("source_name", row.source_name)
        payload.setdefault("status", row.status)
        payload.setdefault("cursor", row.cursor_value)
        payload.setdefault("gcs_uri", row.gcs_uri)
        payload.setdefault("message_id", row.message_id)
        payload["created_at"] = row.created_at.isoformat()
        payload["updated_at"] = row.updated_at.isoformat()
        return payload

    def _action_to_dict(self, row: ActionHistoryModel) -> Dict[str, Any]:
        return {
            "id": row.id,
            "action_type": row.action_type,
            "resource_type": row.resource_type,
            "resource_id": row.resource_id,
            "payload": _from_json_text(row.payload_json),
            "created_at": row.created_at.isoformat(),
        }

    def _resource_to_dict(self, row: ControlPlaneResourceModel) -> Dict[str, Any]:
        payload = _from_json_text(row.payload_json)
        return {
            "resource_type": row.resource_type,
            "resource_id": row.resource_id,
            "name": row.name,
            "status": row.status,
            "payload": payload,
            "created_at": row.created_at.isoformat(),
            "updated_at": row.updated_at.isoformat(),
        }

    def _resource_version_to_dict(self, row: ControlPlaneResourceVersionModel) -> Dict[str, Any]:
        return {
            "id": row.id,
            "resource_type": row.resource_type,
            "resource_id": row.resource_id,
            "version": row.version,
            "payload": _from_json_text(row.payload_json),
            "created_at": row.created_at.isoformat(),
        }

    def _resource_event_to_dict(self, row: ControlPlaneResourceEventModel) -> Dict[str, Any]:
        return {
            "id": row.id,
            "resource_type": row.resource_type,
            "resource_id": row.resource_id,
            "event_type": row.event_type,
            "payload": _from_json_text(row.payload_json),
            "created_at": row.created_at.isoformat(),
        }

    def _apply_job_patch(self, row: Any, patch: Dict[str, Any]) -> None:
        if "status" in patch:
            row.status = patch["status"]
        if "spec" in patch:
            row.spec_json = _to_json_text(patch["spec"])
        if "progress" in patch:
            row.progress_json = _to_json_text(patch["progress"])
        if "error" in patch:
            row.error = patch["error"]
        row.updated_at = datetime.utcnow()
