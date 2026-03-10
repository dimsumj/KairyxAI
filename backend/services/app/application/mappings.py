from __future__ import annotations

from typing import Any, Dict


class MappingService:
    def __init__(self, repository):
        self.repository = repository

    @staticmethod
    def _mapping_key(connector_name: str, scope_type: str = "source", scope_key: str | None = None) -> str:
        resolved_scope = str(scope_type or "source").strip().lower()
        if resolved_scope == "global":
            return "__global__"
        if resolved_scope == "job":
            return f"job:{scope_key or connector_name}"
        return str(scope_key or connector_name)

    @staticmethod
    def _required_coverage(mapping: Dict[str, Any]) -> float:
        # Empty mappings still inherit the built-in connector normalizer defaults.
        if not mapping:
            return 100.0

        required_keys = ("canonical_user_id", "event_name", "event_time")
        hits = sum(1 for key in required_keys if str(mapping.get(key) or "").strip())
        return round(hits / len(required_keys) * 100.0, 2)

    def get_mapping(
        self,
        connector_name: str,
        *,
        scope_type: str = "source",
        scope_key: str | None = None,
    ) -> Dict[str, Any]:
        key = self._mapping_key(connector_name, scope_type=scope_type, scope_key=scope_key)
        mapping = self.repository.get_field_mapping(key)
        return {
            "connector_name": connector_name,
            "scope_type": scope_type,
            "scope_key": scope_key,
            "mapping": mapping,
            "required_coverage": self._required_coverage(mapping),
            "effective_mapping": self.get_effective_mapping(connector_name, job_id=scope_key if scope_type == "job" else None),
        }

    def get_effective_mapping(self, connector_name: str, *, job_id: str | None = None) -> Dict[str, Any]:
        effective: Dict[str, Any] = {}
        for key in ("__global__", connector_name):
            effective.update(self.repository.get_field_mapping(key))
        if job_id:
            effective.update(self.repository.get_field_mapping(f"job:{job_id}"))
        return effective

    def list_versions(
        self,
        connector_name: str,
        *,
        scope_type: str = "source",
        scope_key: str | None = None,
    ) -> Dict[str, Any]:
        key = self._mapping_key(connector_name, scope_type=scope_type, scope_key=scope_key)
        return {
            "connector_name": connector_name,
            "scope_type": scope_type,
            "scope_key": scope_key,
            "items": self.repository.list_resource_versions("mapping", key),
        }

    def save_mapping(
        self,
        connector_name: str,
        mapping: Dict[str, Any],
        *,
        scope_type: str = "source",
        scope_key: str | None = None,
        changed_by: str = "system",
    ) -> Dict[str, Any]:
        key = self._mapping_key(connector_name, scope_type=scope_type, scope_key=scope_key)
        saved = self.repository.save_field_mapping(key, mapping)
        latest_versions = self.repository.list_resource_versions("mapping", key)
        next_version = 1 + max((int(item.get("version") or 0) for item in latest_versions), default=0)
        version_payload = {
            "connector_name": connector_name,
            "scope_type": scope_type,
            "scope_key": scope_key,
            "mapping": mapping,
            "required_coverage": self._required_coverage(mapping),
            "changed_by": changed_by,
        }
        self.repository.upsert_resource(
            "mapping",
            key,
            status="active",
            name=connector_name,
            payload=version_payload,
        )
        self.repository.create_resource_version("mapping", key, version=next_version, payload=version_payload)
        self.repository.record_resource_event(
            "mapping",
            key,
            event_type="mapping_updated",
            payload={"version": next_version, **version_payload},
        )
        self.repository.record_action("mapping_updated", "mapping", key, version_payload)
        return {
            "connector_name": connector_name,
            "scope_type": scope_type,
            "scope_key": scope_key,
            "mapping": saved["mapping"],
            "required_coverage": self._required_coverage(mapping),
            "effective_mapping": self.get_effective_mapping(connector_name, job_id=scope_key if scope_type == "job" else None),
        }

    def rollback(
        self,
        connector_name: str,
        version: int,
        *,
        scope_type: str = "source",
        scope_key: str | None = None,
        changed_by: str = "system",
    ) -> Dict[str, Any]:
        key = self._mapping_key(connector_name, scope_type=scope_type, scope_key=scope_key)
        version_items = self.repository.list_resource_versions("mapping", key)
        selected = next((item for item in version_items if int(item.get("version") or 0) == int(version)), None)
        if selected is None:
            raise KeyError(version)
        payload = dict(selected.get("payload") or {})
        mapping = payload.get("mapping") or {}
        rolled = self.save_mapping(
            connector_name,
            mapping,
            scope_type=scope_type,
            scope_key=scope_key,
            changed_by=changed_by,
        )
        self.repository.record_resource_event(
            "mapping",
            key,
            event_type="mapping_rolled_back",
            payload={"rolled_back_to_version": int(version), "changed_by": changed_by},
        )
        return rolled
