from __future__ import annotations

import json
from typing import Any, Dict, List

from app.application.text_model_runtime import TextModelRuntimeResolver
from bigquery_service import BigQueryService, get_shared_bigquery_service
from gcs_service import GcsService


class MappingService:
    def __init__(self, repository, bigquery_service: BigQueryService | None = None):
        self.repository = repository
        self.bigquery_service = bigquery_service or get_shared_bigquery_service()
        self.gcs_service = GcsService()
        self.model_runtime_resolver = TextModelRuntimeResolver(repository, circuit_namespace="mappings")

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

    def field_candidates(
        self,
        connector_name: str,
        *,
        job_id: str | None = None,
    ) -> Dict[str, Any]:
        effective_mapping = self.get_effective_mapping(connector_name, job_id=job_id)
        sample_events = self._load_job_sample_events(job_id) if job_id else []
        observed_paths = self._observed_paths(sample_events or None)
        suggestions = self._heuristic_suggestions(effective_mapping, observed_paths)
        fields = [
            {
                "path": path,
                "sample_values": values[:3],
            }
            for path, values in sorted(observed_paths.items(), key=lambda item: item[0])
            if path
        ]
        return {
            "connector_name": connector_name,
            "job_id": job_id,
            "effective_mapping": effective_mapping,
            "fields": fields,
            "suggestions": suggestions,
            "sample_events": sample_events[:3],
        }

    def _heuristic_suggestions(self, current: Dict[str, Any], observed_paths: Dict[str, list[str]]) -> list[Dict[str, Any]]:
        suggestions = []
        fallback_candidates = {
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
        for field, options in fallback_candidates.items():
            if str(current.get(field) or "").strip():
                continue
            ranked_options = self._match_observed_paths(field, observed_paths)
            if not ranked_options:
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

    def _match_observed_paths(
        self,
        field: str,
        observed_paths: Dict[str, list[str]],
    ) -> list[tuple[str, float, str, list[str]]]:
        aliases = {
            "canonical_user_id": [
                "canonical_user_id",
                "player_id",
                "playerid",
                "user_id",
                "userid",
                "uid",
                "pid",
                "customer_user_id",
                "customeruserid",
                "external_user_id",
                "externaluserid",
                "distinct_id",
                "distinctid",
                "appsflyer_id",
                "appsflyerid",
            ],
            "event_name": ["event_name", "eventname", "event_type", "eventtype", "name"],
            "event_time": ["event_time", "eventtime", "timestamp", "time", "created_at", "client_event_time", "server_upload_time"],
            "campaign": ["campaign", "campaign_name", "campaignname", "utm_campaign"],
            "media_source": ["media_source", "mediasource", "network", "channel", "source", "publisher"],
        }
        ranked: list[tuple[str, float, str, list[str]]] = []
        normalized_aliases = aliases.get(field, [])
        for path, sample_values in observed_paths.items():
            tokens = [self._normalize_token(token) for token in path.split(".") if token]
            if not tokens:
                continue
            path_score = self._score_path_tokens(tokens, normalized_aliases)
            if path_score <= 0:
                continue
            confidence = min(0.99, 0.6 + path_score)
            rationale = f"Observed raw field path resembles {field}"
            if sample_values:
                rationale += " and includes sample values"
            ranked.append((path, confidence, rationale, sample_values[:3]))
        return ranked

    @staticmethod
    def _normalize_token(value: str) -> str:
        return "".join(char for char in str(value or "").lower() if char.isalnum())

    def _score_path_tokens(self, tokens: List[str], aliases: List[str]) -> float:
        if not aliases:
            return 0.0
        score = 0.0
        last_token = tokens[-1]
        for alias in aliases:
            normalized_alias = self._normalize_token(alias)
            if not normalized_alias:
                continue
            if last_token == normalized_alias:
                score = max(score, 0.34)
            elif last_token.endswith(normalized_alias) or normalized_alias.endswith(last_token):
                score = max(score, 0.28)
            elif normalized_alias in tokens:
                score = max(score, 0.22)
            elif any(normalized_alias in token or token in normalized_alias for token in tokens):
                score = max(score, 0.15)
        return score

    def _ai_suggestions(
        self,
        connector_name: str,
        current: Dict[str, Any],
        observed_paths: Dict[str, list[str]],
        heuristic_suggestions: list[Dict[str, Any]],
    ) -> Dict[str, Any] | None:
        selection = self.model_runtime_resolver.resolve()
        runtime = selection.runtime
        if runtime is None or not heuristic_suggestions:
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
            raw_response = runtime.request_text(prompt)
            parsed = self._extract_json_object(raw_response)
            items = parsed if isinstance(parsed, list) else parsed.get("suggestions") or []
            if not isinstance(items, list) or not items:
                return None
            merged = self._merge_ai_suggestions(heuristic_suggestions, items)
            return {"suggestions": merged, "model_name": selection.model_name or runtime.model_name}
        except Exception:
            return None

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

    def _observed_paths(self, rows: List[Dict[str, Any]] | None = None) -> Dict[str, list[str]]:
        rows = rows if rows is not None else self.bigquery_service.get_rows_for_alias("standardized")[:100]
        observed: Dict[str, list[str]] = {}
        for row in rows:
            self._collect_paths("", row, observed)
        return observed

    def _load_job_sample_events(self, job_id: str | None, max_records: int = 50) -> List[Dict[str, Any]]:
        if not job_id:
            return []
        records: List[Dict[str, Any]] = []
        manifests = self._list_job_manifests(job_id)
        for manifest in manifests:
            blob_name = str(manifest.get("gcs_uri") or "").strip()
            if not blob_name:
                continue
            try:
                events = self.gcs_service.download_raw_events(blob_name)
            except FileNotFoundError:
                continue
            for event in events:
                if isinstance(event, dict):
                    records.append(event)
                if len(records) >= max_records:
                    return records
        return records

    def _list_job_manifests(self, job_id: str) -> List[Dict[str, Any]]:
        items: List[Dict[str, Any]] = []
        for record in self.repository.list_resources("import_manifest"):
            payload = dict(record.get("payload") or {})
            if str(payload.get("job_id") or "") != job_id:
                continue
            items.append(payload)
        if items:
            items.sort(key=lambda item: int(item.get("shard_index") or 0))
            return items
        checkpoints = []
        try:
            checkpoints = self.repository.list_checkpoints(job_id)
        except Exception:
            checkpoints = []
        for checkpoint in checkpoints:
            manifest = dict(checkpoint.get("manifest") or {})
            if not manifest:
                manifest = {
                    "job_id": job_id,
                    "gcs_uri": checkpoint.get("gcs_uri"),
                    "shard_index": checkpoint.get("shard_index"),
                }
            items.append(manifest)
        items.sort(key=lambda item: int(item.get("shard_index") or 0))
        return items

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
