from __future__ import annotations

from typing import Any, Dict, List, Optional, Protocol


class ConnectorConfigRepository(Protocol):
    def list_connectors(self) -> List[Dict[str, Any]]:
        ...

    def get_connector(self, name: str) -> Optional[Dict[str, Any]]:
        ...

    def upsert_connector(self, name: str, connector_type: str, config: Dict[str, Any]) -> Dict[str, Any]:
        ...

    def delete_connector(self, name: str) -> bool:
        ...


class FieldMappingRepository(Protocol):
    def get_field_mapping(self, connector_name: str) -> Dict[str, Any]:
        ...

    def save_field_mapping(self, connector_name: str, mapping: Dict[str, Any]) -> Dict[str, Any]:
        ...


class ImportJobRepository(Protocol):
    def create_import_job(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        ...

    def list_import_jobs(self) -> List[Dict[str, Any]]:
        ...

    def get_import_job(self, job_id: str) -> Optional[Dict[str, Any]]:
        ...

    def update_import_job(self, job_id: str, patch: Dict[str, Any]) -> Dict[str, Any]:
        ...


class PredictionJobRepository(Protocol):
    def create_prediction_job(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        ...

    def list_prediction_jobs(self) -> List[Dict[str, Any]]:
        ...

    def get_prediction_job(self, job_id: str) -> Optional[Dict[str, Any]]:
        ...

    def update_prediction_job(self, job_id: str, patch: Dict[str, Any]) -> Dict[str, Any]:
        ...


class ExportJobRepository(Protocol):
    def create_export_job(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        ...

    def list_export_jobs(self) -> List[Dict[str, Any]]:
        ...

    def get_export_job(self, job_id: str) -> Optional[Dict[str, Any]]:
        ...

    def update_export_job(self, job_id: str, patch: Dict[str, Any]) -> Dict[str, Any]:
        ...


class CheckpointRepository(Protocol):
    def upsert_checkpoint(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        ...

    def list_checkpoints(self, job_id: str) -> List[Dict[str, Any]]:
        ...


class ActionHistoryRepository(Protocol):
    def record_action(self, action_type: str, resource_type: str, resource_id: Optional[str], payload: Dict[str, Any]) -> Dict[str, Any]:
        ...

    def list_actions(self, limit: int = 200) -> List[Dict[str, Any]]:
        ...


class ExperimentConfigRepository(Protocol):
    def get_experiment_config(self, key: str = "default") -> Dict[str, Any]:
        ...

    def save_experiment_config(self, config: Dict[str, Any], key: str = "default") -> Dict[str, Any]:
        ...
