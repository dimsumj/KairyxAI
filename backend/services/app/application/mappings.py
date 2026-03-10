from __future__ import annotations

from typing import Any, Dict

from bigquery_service import BigQueryService, get_shared_bigquery_service


class MappingService:
    def __init__(self, repository, bigquery_service: BigQueryService | None = None):
        self.repository = repository
        self.bigquery_service = bigquery_service or get_shared_bigquery_service()

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

    def suggestions(
        self,
        connector_name: str,
        *,
        scope_type: str = "source",
        scope_key: str | None = None,
    ) -> Dict[str, Any]:
        current = self.get_effective_mapping(connector_name, job_id=scope_key if scope_type == "job" else None)
        observed_paths = self._observed_paths()
        suggestions = []
        candidates = {
            "canonical_user_id": [
                ("player_id", 0.92, "player_id is the most common canonical candidate in v1"),
                ("event_properties.player_id", 0.84, "Found common nested player_id pattern"),
                ("user_properties.player_id", 0.72, "Fallback user profile player id"),
            ],
            "event_name": [
                ("event_name", 0.93, "event_name aligns with canonical event_type"),
                ("event_type", 0.88, "event_type is commonly used as source event name"),
            ],
            "event_time": [
                ("timestamp", 0.94, "timestamp is the most common source event time field"),
                ("event_time", 0.89, "event_time already matches canonical naming"),
                ("created_at", 0.71, "created_at is a common fallback timestamp"),
            ],
            "campaign": [
                ("event_properties.campaign", 0.81, "campaign usually arrives inside event_properties"),
                ("campaign", 0.74, "campaign is also common as a top-level field"),
            ],
            "media_source": [
                ("event_properties.media_source", 0.8, "media_source typically lives in attribution payloads"),
                ("media_source", 0.7, "media_source as a flat field is a common fallback"),
            ],
        }
        for field, options in candidates.items():
            if str(current.get(field) or "").strip():
                continue
            ranked_options = []
            for path, confidence, rationale in options:
                sample_values = observed_paths.get(path) or []
                if sample_values:
                    ranked_options.append((path, min(0.99, confidence + 0.03), f"{rationale}; backed by observed samples", sample_values))
                else:
                    ranked_options.append((path, confidence, rationale, []))
            ranked_options.sort(key=lambda item: (item[1], len(item[3])), reverse=True)
            path, confidence, rationale, sample_values = ranked_options[0]
            suggestions.append(
                {
                    "field": field,
                    "suggested_path": path,
                    "confidence": confidence,
                    "rationale": rationale,
                    "sample_values": sample_values[:3],
                    "alternatives": [
                        {
                            "path": alt_path,
                            "confidence": alt_confidence,
                            "rationale": alt_rationale,
                            "sample_values": alt_samples[:3],
                        }
                        for alt_path, alt_confidence, alt_rationale, alt_samples in ranked_options[1:]
                    ],
                }
            )
        return {
            "connector_name": connector_name,
            "scope_type": scope_type,
            "scope_key": scope_key,
            "suggestions": suggestions,
            "effective_mapping": current,
        }

    def _observed_paths(self) -> Dict[str, list[str]]:
        rows = self.bigquery_service.get_rows_for_alias("standardized")[:100]
        observed: Dict[str, list[str]] = {}
        for row in rows:
            self._collect_paths("", row, observed)
        return observed

    def _collect_paths(self, prefix: str, value: Any, observed: Dict[str, list[str]]) -> None:
        if isinstance(value, dict):
            for key, item in value.items():
                next_prefix = f"{prefix}.{key}" if prefix else str(key)
                self._collect_paths(next_prefix, item, observed)
            return
        if value in (None, "", [], {}):
            return
        bucket = observed.setdefault(prefix, [])
        text = str(value)
        if text not in bucket and len(bucket) < 5:
            bucket.append(text)
