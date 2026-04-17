from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Dict, List

from app.application.text_model_runtime import TextModelRuntimeResolver
from bigquery_service import BigQueryService, get_shared_bigquery_service
from gcs_service import GcsService

MAPPING_MEMORY_RESOURCE_TYPE = "mapping_memory"
MAPPING_METADATA_KEYS = {"tenant_id", "project_id", "created_by", "updated_by", "correlation_id"}


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

    def get_mapping_memory(self, connector_name: str) -> Dict[str, Any]:
        resource = self.repository.get_resource(MAPPING_MEMORY_RESOURCE_TYPE, connector_name)
        payload = dict(resource.get("payload") or {}) if resource else {}
        fields = payload.get("fields")
        return {
            "connector_name": connector_name,
            "fields": dict(fields or {}) if isinstance(fields, dict) else {},
        }

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
        previous_mapping = self.repository.get_field_mapping(key)
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
        self.learn_from_mapping(
            connector_name,
            mapping,
            reason="manual_save",
            job_id=scope_key if scope_type == "job" else None,
            previous_mapping=previous_mapping,
        )
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
        sample_events = self._load_job_sample_events(scope_key) if scope_type == "job" else self._load_source_sample_events(connector_name)
        analysis_rows = list(sample_events or self._fallback_analysis_rows(connector_name))
        observed_paths = self._observed_paths(analysis_rows) if analysis_rows else {}
        suggestions = self._heuristic_suggestions(
            current,
            observed_paths,
            path_profiles=self._build_path_profiles(analysis_rows),
            memory=self.get_mapping_memory(connector_name),
        )
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
        sample_events = self._load_job_sample_events(job_id) if job_id else self._load_source_sample_events(connector_name)
        observed_paths = self._observed_paths(sample_events)
        path_profiles = self._build_path_profiles(sample_events)
        suggestions = self._heuristic_suggestions(
            effective_mapping,
            observed_paths,
            path_profiles=path_profiles,
            memory=self.get_mapping_memory(connector_name),
        )
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

    def learn_from_mapping(
        self,
        connector_name: str,
        mapping: Dict[str, Any],
        *,
        reason: str,
        job_id: str | None = None,
        previous_mapping: Dict[str, Any] | None = None,
    ) -> Dict[str, Any]:
        normalized_mapping = {
            str(field): str(path).strip()
            for field, path in dict(mapping or {}).items()
            if str(path or "").strip() and str(field or "").strip() not in MAPPING_METADATA_KEYS
        }
        prior_mapping = {
            str(field): str(path).strip()
            for field, path in dict(previous_mapping or {}).items()
            if str(path or "").strip() and str(field or "").strip() not in MAPPING_METADATA_KEYS
        }
        if not normalized_mapping and not prior_mapping:
            return self.get_mapping_memory(connector_name)
        sample_events = self._load_job_sample_events(job_id) if job_id else self._load_source_sample_events(connector_name, max_records=25)
        path_profiles = self._build_path_profiles(sample_events)
        current_memory = self.get_mapping_memory(connector_name)
        fields = {
            str(field): {
                "paths": {
                    str(path): dict(stats)
                    for path, stats in dict((field_payload or {}).get("paths") or {}).items()
                }
            }
            for field, field_payload in dict(current_memory.get("fields") or {}).items()
        }
        learned_at = datetime.utcnow().isoformat()
        changed_fields = sorted(set(normalized_mapping) | set(prior_mapping))
        correction_weight = 2 if reason == "manual_save" else 1
        for field in changed_fields:
            field_bucket = fields.setdefault(field, {"paths": {}})
            current_path = normalized_mapping.get(field)
            prior_path = prior_mapping.get(field)
            if prior_path and prior_path != current_path:
                prior_stats = dict(field_bucket["paths"].get(prior_path) or {})
                prior_stats["manual_confirmation_count"] = int(prior_stats.get("manual_confirmation_count") or 0)
                prior_stats["successful_import_count"] = int(prior_stats.get("successful_import_count") or 0)
                prior_stats["correction_count"] = int(prior_stats.get("correction_count") or 0) + correction_weight
                prior_stats["last_corrected_at"] = learned_at
                field_bucket["paths"][prior_path] = prior_stats
            if not current_path:
                fields[field] = field_bucket
                continue
            stats = dict(field_bucket["paths"].get(current_path) or {})
            profile = dict(path_profiles.get(current_path) or {})
            prior_observations = int(stats.get("observations") or 0)
            stats["manual_confirmation_count"] = int(stats.get("manual_confirmation_count") or 0) + (1 if reason == "manual_save" else 0)
            stats["successful_import_count"] = int(stats.get("successful_import_count") or 0) + (1 if reason == "successful_import" else 0)
            stats["correction_count"] = int(stats.get("correction_count") or 0)
            stats["observations"] = prior_observations + 1
            stats["last_seen_at"] = learned_at
            if job_id:
                stats["last_job_id"] = job_id
            for metric in ("row_coverage", "event_type_coverage", "distinct_ratio"):
                metric_value = profile.get(metric)
                if metric_value is None:
                    continue
                prior_average = float(stats.get(metric) or 0.0)
                stats[metric] = round(
                    ((prior_average * prior_observations) + float(metric_value)) / max(1, prior_observations + 1),
                    4,
                )
            stats["sample_values"] = self._merge_sample_values(stats.get("sample_values"), profile.get("sample_values") or [])
            field_bucket["paths"][current_path] = stats
            field_bucket["last_selected_path"] = current_path
            fields[field] = field_bucket
        payload = {
            "connector_name": connector_name,
            "fields": fields,
            "updated_at": learned_at,
        }
        self.repository.upsert_resource(
            MAPPING_MEMORY_RESOURCE_TYPE,
            connector_name,
            status="active",
            name=connector_name,
            payload=payload,
        )
        self.repository.record_resource_event(
            MAPPING_MEMORY_RESOURCE_TYPE,
            connector_name,
            event_type="mapping_memory_updated",
            payload={
                "connector_name": connector_name,
                "job_id": job_id,
                "reason": reason,
                "mapping": normalized_mapping,
                "previous_mapping": prior_mapping,
                "updated_at": learned_at,
            },
        )
        return payload

    def _heuristic_suggestions(
        self,
        current: Dict[str, Any],
        observed_paths: Dict[str, list[str]],
        *,
        path_profiles: Dict[str, Dict[str, Any]] | None = None,
        memory: Dict[str, Any] | None = None,
    ) -> list[Dict[str, Any]]:
        path_profiles = path_profiles or {}
        memory = memory or {"fields": {}}
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
            ranked_options = self._merge_ranked_candidates(
                self._rank_memory_candidates(field, observed_paths, path_profiles, memory),
                self._match_observed_paths(field, observed_paths, path_profiles),
            )
            if not ranked_options:
                ranked_options = []
                for path, confidence, rationale in options:
                    sample_values = observed_paths.get(path) or []
                    ranked_options.append(
                        self._candidate_option(
                            path=path,
                            confidence=min(0.99, confidence + (0.03 if sample_values else 0.0)),
                            rationale=f"{rationale}; backed by observed samples" if sample_values else rationale,
                            sample_values=sample_values[:3],
                            profile=path_profiles.get(path) or {},
                        )
                    )
            ranked_options.sort(
                key=lambda item: (
                    float(item.get("confidence") or 0.0),
                    int(item.get("manual_confirmation_count") or 0),
                    int(item.get("successful_import_count") or 0),
                    len(item.get("sample_values") or []),
                ),
                reverse=True,
            )
            top_option = ranked_options[0]
            suggestions.append(
                {
                    "field": field,
                    "suggested_path": top_option["path"],
                    "confidence": top_option["confidence"],
                    "rationale": top_option["rationale"],
                    "sample_values": top_option["sample_values"][:3],
                    "manual_confirmation_count": int(top_option.get("manual_confirmation_count") or 0),
                    "successful_import_count": int(top_option.get("successful_import_count") or 0),
                    "profile": dict(top_option.get("profile") or {}),
                    "alternatives": [
                        {
                            "path": alt_option["path"],
                            "confidence": alt_option["confidence"],
                            "rationale": alt_option["rationale"],
                            "sample_values": alt_option["sample_values"][:3],
                        }
                        for alt_option in ranked_options[1:]
                    ],
                }
            )
        return suggestions

    def _match_observed_paths(
        self,
        field: str,
        observed_paths: Dict[str, list[str]],
        path_profiles: Dict[str, Dict[str, Any]] | None = None,
    ) -> list[Dict[str, Any]]:
        path_profiles = path_profiles or {}
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
        ranked: list[Dict[str, Any]] = []
        normalized_aliases = aliases.get(field, [])
        for path, sample_values in observed_paths.items():
            tokens = [self._normalize_token(token) for token in path.split(".") if token]
            if not tokens:
                continue
            path_score = self._score_path_tokens(tokens, normalized_aliases)
            if path_score <= 0:
                continue
            profile = dict(path_profiles.get(path) or {})
            confidence = min(0.99, 0.6 + path_score + self._profile_confidence_boost(field, profile))
            rationale = f"Observed raw field path resembles {field}"
            if sample_values:
                rationale += " and includes sample values"
            if field == "canonical_user_id" and profile:
                if float(profile.get("row_coverage") or 0.0) >= 0.8:
                    rationale += "; present in most sampled rows"
                if float(profile.get("event_type_coverage") or 0.0) >= 0.6:
                    rationale += "; stable across multiple event types"
            ranked.append(
                self._candidate_option(
                    path=path,
                    confidence=confidence,
                    rationale=rationale,
                    sample_values=sample_values[:3],
                    profile=profile,
                )
            )
        return ranked

    def _rank_memory_candidates(
        self,
        field: str,
        observed_paths: Dict[str, list[str]],
        path_profiles: Dict[str, Dict[str, Any]],
        memory: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        ranked: List[Dict[str, Any]] = []
        field_memory = dict((((memory or {}).get("fields") or {}).get(field) or {}).get("paths") or {})
        for path, stats in field_memory.items():
            if observed_paths and path not in observed_paths:
                continue
            path_profile = dict(path_profiles.get(path) or {})
            manual_confirmation_count = int((stats or {}).get("manual_confirmation_count") or 0)
            successful_import_count = int((stats or {}).get("successful_import_count") or 0)
            correction_count = int((stats or {}).get("correction_count") or 0)
            confidence = 0.58 + min(0.2, manual_confirmation_count * 0.07) + min(0.12, successful_import_count * 0.04) - min(0.18, correction_count * 0.06)
            confidence += self._profile_confidence_boost(field, path_profile or dict(stats or {}))
            rationale_bits = []
            if manual_confirmation_count:
                rationale_bits.append(f"confirmed manually {manual_confirmation_count} time{'s' if manual_confirmation_count != 1 else ''}")
            if successful_import_count:
                rationale_bits.append(f"seen in {successful_import_count} successful import{'s' if successful_import_count != 1 else ''}")
            if correction_count:
                rationale_bits.append(f"corrected away {correction_count} time{'s' if correction_count != 1 else ''}")
            if float((path_profile or stats).get("event_type_coverage") or 0.0) >= 0.6:
                rationale_bits.append("stable across event types")
            rationale = "Learned from prior successful mappings"
            if rationale_bits:
                rationale += f": {', '.join(rationale_bits)}"
            ranked.append(
                self._candidate_option(
                    path=path,
                    confidence=min(0.99, confidence),
                    rationale=rationale,
                    sample_values=(observed_paths.get(path) or (stats or {}).get("sample_values") or [])[:3],
                    manual_confirmation_count=manual_confirmation_count,
                    successful_import_count=successful_import_count,
                    profile=path_profile or {
                        "row_coverage": float((stats or {}).get("row_coverage") or 0.0),
                        "event_type_coverage": float((stats or {}).get("event_type_coverage") or 0.0),
                        "distinct_ratio": float((stats or {}).get("distinct_ratio") or 0.0),
                    },
                )
            )
        return ranked

    @staticmethod
    def _candidate_option(
        *,
        path: str,
        confidence: float,
        rationale: str,
        sample_values: List[str] | None = None,
        manual_confirmation_count: int = 0,
        successful_import_count: int = 0,
        profile: Dict[str, Any] | None = None,
    ) -> Dict[str, Any]:
        return {
            "path": str(path or ""),
            "confidence": max(0.0, min(0.99, float(confidence or 0.0))),
            "rationale": str(rationale or "").strip(),
            "sample_values": [str(value) for value in list(sample_values or [])[:3]],
            "manual_confirmation_count": int(manual_confirmation_count or 0),
            "successful_import_count": int(successful_import_count or 0),
            "profile": dict(profile or {}),
        }

    def _merge_ranked_candidates(self, *candidate_groups: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        merged: Dict[str, Dict[str, Any]] = {}
        for group in candidate_groups:
            for item in group or []:
                path = str(item.get("path") or "").strip()
                if not path:
                    continue
                current = merged.get(path)
                if current is None:
                    merged[path] = dict(item)
                    continue
                current["confidence"] = max(float(current.get("confidence") or 0.0), float(item.get("confidence") or 0.0))
                current["manual_confirmation_count"] = max(
                    int(current.get("manual_confirmation_count") or 0),
                    int(item.get("manual_confirmation_count") or 0),
                )
                current["successful_import_count"] = max(
                    int(current.get("successful_import_count") or 0),
                    int(item.get("successful_import_count") or 0),
                )
                current["sample_values"] = self._merge_sample_values(current.get("sample_values"), item.get("sample_values"))
                current["profile"] = {**dict(current.get("profile") or {}), **dict(item.get("profile") or {})}
                rationale_bits = [str(current.get("rationale") or "").strip(), str(item.get("rationale") or "").strip()]
                current["rationale"] = "; ".join(bit for bit in rationale_bits if bit)
        return list(merged.values())

    @staticmethod
    def _merge_sample_values(existing: Any, incoming: Any, *, limit: int = 5) -> List[str]:
        values: List[str] = []
        seen = set()
        for candidate in list(existing or []) + list(incoming or []):
            text = str(candidate or "").strip()
            if not text or text in seen:
                continue
            seen.add(text)
            values.append(text)
            if len(values) >= limit:
                break
        return values

    @staticmethod
    def _profile_confidence_boost(field: str, profile: Dict[str, Any]) -> float:
        if not profile:
            return 0.0
        row_coverage = float(profile.get("row_coverage") or 0.0)
        event_type_coverage = float(profile.get("event_type_coverage") or 0.0)
        distinct_ratio = float(profile.get("distinct_ratio") or 0.0)
        boost = 0.0
        if row_coverage >= 0.8:
            boost += 0.07
        elif row_coverage >= 0.5:
            boost += 0.03
        if field == "canonical_user_id":
            if event_type_coverage >= 0.6:
                boost += 0.08
            if row_coverage >= 0.9 and event_type_coverage >= 0.9:
                boost += 0.05
            if distinct_ratio >= 0.2:
                boost += 0.04
        elif field == "event_time" and row_coverage >= 0.9:
            boost += 0.03
        elif field == "event_name" and event_type_coverage >= 0.6:
            boost += 0.02
        return boost

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

    def _fallback_analysis_rows(self, connector_name: str, *, limit: int = 100) -> List[Dict[str, Any]]:
        rows = list(self.bigquery_service.get_rows_for_alias("standardized")[: max(limit * 2, limit)] or [])
        if not rows:
            return []
        connector = self.repository.get_connector(connector_name)
        connector_name_key = str(connector_name or "").strip().lower()
        connector_type_key = str((connector or {}).get("type") or "").strip().lower()
        matching_job_ids = {
            str(item.get("id") or "").strip()
            for item in self.repository.list_import_jobs()
            if str(item.get("source_name") or "").strip().lower() == connector_name_key
        }
        exact_matches = [
            row for row in rows
            if (
                str(row.get("source_config_id") or "").strip().lower() == connector_name_key
                or str(row.get("source") or "").strip().lower() == connector_name_key
                or str(row.get("job_id") or "").strip() in matching_job_ids
            )
        ]
        if exact_matches:
            return exact_matches[:limit]
        if connector_type_key:
            typed_matches = [
                row for row in rows
                if str(row.get("source") or "").strip().lower() == connector_type_key
            ]
            if typed_matches:
                return typed_matches[:limit]
        return []

    def _load_source_sample_events(self, connector_name: str, max_records: int = 50) -> List[Dict[str, Any]]:
        records: List[Dict[str, Any]] = []
        for manifest in self._list_connector_manifests(connector_name):
            for sample_event in self._manifest_sample_events(manifest):
                raw_event = self._extract_original_raw_event(sample_event)
                if isinstance(raw_event, dict):
                    records.append(raw_event)
                if len(records) >= max_records:
                    return records
            blob_name = str(manifest.get("gcs_uri") or "").strip()
            if not blob_name:
                continue
            try:
                events = self.gcs_service.download_raw_events(blob_name)
            except FileNotFoundError:
                continue
            for event in events:
                raw_event = self._extract_original_raw_event(event)
                if isinstance(raw_event, dict):
                    records.append(raw_event)
                if len(records) >= max_records:
                    return records
        return records

    def _load_job_sample_events(self, job_id: str | None, max_records: int = 50) -> List[Dict[str, Any]]:
        if not job_id:
            return []
        records: List[Dict[str, Any]] = []
        manifests = self._list_job_manifests(job_id)
        for manifest in manifests:
            for sample_event in self._manifest_sample_events(manifest):
                raw_event = self._extract_original_raw_event(sample_event)
                if isinstance(raw_event, dict):
                    records.append(raw_event)
                if len(records) >= max_records:
                    return records
            blob_name = str(manifest.get("gcs_uri") or "").strip()
            if not blob_name:
                continue
            try:
                events = self.gcs_service.download_raw_events(blob_name)
            except FileNotFoundError:
                continue
            for event in events:
                raw_event = self._extract_original_raw_event(event)
                if isinstance(raw_event, dict):
                    records.append(raw_event)
                if len(records) >= max_records:
                    return records
        return records

    def _list_connector_manifests(self, connector_name: str) -> List[Dict[str, Any]]:
        items: List[Dict[str, Any]] = []
        for record in self.repository.list_resources("import_manifest"):
            payload = dict(record.get("payload") or {})
            source_name = str(payload.get("source_name") or payload.get("source") or "").strip()
            manifest = dict(payload.get("manifest") or {})
            source_config_id = str(manifest.get("source_config_id") or "").strip()
            if connector_name not in {source_name, source_config_id}:
                continue
            items.append(payload)
        return items

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

    def _extract_original_raw_event(self, event: Any) -> Dict[str, Any] | None:
        if not isinstance(event, dict):
            return None
        event_properties = event.get("event_properties")
        if isinstance(event_properties, dict):
            nested_raw = event_properties.get("raw")
            if isinstance(nested_raw, dict):
                return nested_raw
        direct_raw = event.get("raw")
        if isinstance(direct_raw, dict):
            return direct_raw
        return event

    def _manifest_sample_events(self, manifest: Dict[str, Any]) -> List[Dict[str, Any]]:
        candidates: List[Dict[str, Any]] = []
        for key in ("sample_raw_events", "sample_events"):
            values = manifest.get(key)
            if isinstance(values, list):
                candidates.extend(item for item in values if isinstance(item, dict))
            nested_values = dict(manifest.get("manifest") or {}).get(key)
            if isinstance(nested_values, list):
                candidates.extend(item for item in nested_values if isinstance(item, dict))
        return candidates

    def _build_path_profiles(self, rows: List[Dict[str, Any]] | None) -> Dict[str, Dict[str, Any]]:
        normalized_rows = [row for row in list(rows or []) if isinstance(row, dict)]
        if not normalized_rows:
            return {}
        total_rows = len(normalized_rows)
        total_event_types = {
            self._event_type_from_raw(row)
            for row in normalized_rows
            if self._event_type_from_raw(row)
        }
        profiles: Dict[str, Dict[str, Any]] = {}
        for row in normalized_rows:
            event_type = self._event_type_from_raw(row)
            row_paths: Dict[str, list[str]] = {}
            self._collect_paths("", row, row_paths)
            for path, sample_values in row_paths.items():
                profile = profiles.setdefault(
                    path,
                    {
                        "rows_with_value": 0,
                        "event_types": set(),
                        "distinct_values": set(),
                        "sample_values": [],
                    },
                )
                profile["rows_with_value"] += 1
                if event_type:
                    profile["event_types"].add(event_type)
                for sample_value in sample_values[:3]:
                    if len(profile["sample_values"]) < 5 and sample_value not in profile["sample_values"]:
                        profile["sample_values"].append(sample_value)
                    if len(profile["distinct_values"]) < 50:
                        profile["distinct_values"].add(str(sample_value))
        finalized: Dict[str, Dict[str, Any]] = {}
        total_event_type_count = max(1, len(total_event_types))
        for path, profile in profiles.items():
            rows_with_value = int(profile.get("rows_with_value") or 0)
            distinct_values = profile.get("distinct_values") or set()
            event_types = profile.get("event_types") or set()
            finalized[path] = {
                "rows_evaluated": total_rows,
                "rows_with_value": rows_with_value,
                "row_coverage": round(rows_with_value / total_rows, 4) if total_rows else 0.0,
                "event_type_coverage": round(len(event_types) / total_event_type_count, 4) if total_event_type_count else 0.0,
                "event_type_count": len(event_types),
                "distinct_ratio": round(len(distinct_values) / rows_with_value, 4) if rows_with_value else 0.0,
                "sample_values": list(profile.get("sample_values") or [])[:3],
            }
        return finalized

    @staticmethod
    def _event_type_from_raw(row: Dict[str, Any]) -> str:
        for key in ("event_type", "event_name", "eventName", "name"):
            value = row.get(key)
            if value not in (None, ""):
                return str(value)
        return ""

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
