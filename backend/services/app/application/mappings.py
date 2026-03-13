from __future__ import annotations

import json
import os
from typing import Any, Dict

from bigquery_service import BigQueryService, get_shared_bigquery_service
from gemini_client import GeminiClient


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
        suggestions = self._heuristic_suggestions(current, observed_paths)
        engine = "heuristic"
        ai_model = None
        ai_suggestions = self._ai_suggestions(connector_name, current, observed_paths, suggestions)
        if ai_suggestions:
            suggestions = ai_suggestions["suggestions"]
            engine = "ai_assisted"
            ai_model = ai_suggestions.get("model_name")
        return {
            "connector_name": connector_name,
            "scope_type": scope_type,
            "scope_key": scope_key,
            "engine": engine,
            "model_name": ai_model,
            "suggestions": suggestions,
            "effective_mapping": current,
        }

    def _heuristic_suggestions(self, current: Dict[str, Any], observed_paths: Dict[str, list[str]]) -> list[Dict[str, Any]]:
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
        return suggestions

    def _ai_suggestions(
        self,
        connector_name: str,
        current: Dict[str, Any],
        observed_paths: Dict[str, list[str]],
        heuristic_suggestions: list[Dict[str, Any]],
    ) -> Dict[str, Any] | None:
        client = self._build_gemini_client()
        if client is None or not heuristic_suggestions:
            return None
        prompt = {
            "task": "Choose the best source field path for each missing canonical mapping field in a game event connector.",
            "connector_name": connector_name,
            "current_mapping": current,
            "observed_paths": {path: values[:3] for path, values in list(observed_paths.items())[:80]},
            "heuristic_suggestions": heuristic_suggestions,
            "instructions": {
                "return_format": [
                    {
                        "field": "canonical_user_id",
                        "suggested_path": "player_id",
                        "confidence": 0.97,
                        "rationale": "why this path is best",
                    }
                ],
                "rules": [
                    "Prefer deterministic identifier fields for canonical_user_id.",
                    "Use event_name/event_type for event_name.",
                    "Use timestamp-like fields for event_time.",
                    "Return only JSON.",
                ],
            },
        }
        try:
            raw_response = client.generate_content(json.dumps(prompt))
            parsed = self._extract_json_object(raw_response)
            items = parsed if isinstance(parsed, list) else parsed.get("suggestions") or []
            if not isinstance(items, list) or not items:
                return None
            merged = self._merge_ai_suggestions(heuristic_suggestions, items)
            return {"suggestions": merged, "model_name": getattr(client, "model_name", None)}
        except Exception:
            return None

    def _build_gemini_client(self) -> GeminiClient | None:
        connector = self._select_google_connector()
        if connector is not None:
            config = connector.get("config") or {}
            api_key = str(config.get("api_key") or "").strip()
            model_name = str(config.get("model_name") or "").strip() or None
            if api_key:
                try:
                    return GeminiClient(api_key=api_key, model_name=model_name, circuit_namespace="mappings")
                except Exception:
                    return None
        if str(os.getenv("GOOGLE_API_KEY") or "").strip():
            try:
                return GeminiClient(circuit_namespace="mappings")
            except Exception:
                return None
        return None

    def _select_google_connector(self) -> Dict[str, Any] | None:
        google_connectors = [
            connector
            for connector in self.repository.list_connectors()
            if str(connector.get("type") or "").lower() == "google"
            and str((connector.get("config") or {}).get("api_key") or "").strip()
        ]
        if not google_connectors:
            return None
        return max(google_connectors, key=lambda connector: str(connector.get("updated_at") or connector.get("created_at") or ""))

    @staticmethod
    def _extract_json_object(raw_response: Any) -> Any:
        text = str(raw_response or "").strip()
        if not text:
            return {}
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            start = min((index for index in (text.find("["), text.find("{")) if index >= 0), default=-1)
            end = max(text.rfind("]"), text.rfind("}"))
            if start >= 0 and end > start:
                return json.loads(text[start : end + 1])
        return {}

    @staticmethod
    def _merge_ai_suggestions(heuristic_suggestions: list[Dict[str, Any]], ai_items: list[Dict[str, Any]]) -> list[Dict[str, Any]]:
        by_field = {str(item.get("field") or ""): dict(item) for item in heuristic_suggestions}
        for ai_item in ai_items:
            field = str(ai_item.get("field") or "")
            if field not in by_field:
                continue
            merged = dict(by_field[field])
            suggested_path = str(ai_item.get("suggested_path") or "").strip()
            confidence = float(ai_item.get("confidence") or merged.get("confidence") or 0.0)
            rationale = str(ai_item.get("rationale") or "").strip()
            if suggested_path:
                merged["suggested_path"] = suggested_path
            merged["confidence"] = max(0.0, min(0.99, confidence))
            if rationale:
                merged["rationale"] = f"{rationale} (AI-assisted)"
            merged["engine"] = "ai_assisted"
            by_field[field] = merged
        return list(by_field.values())

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
