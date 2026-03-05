from __future__ import annotations

import argparse
import json
from typing import Any, Dict, Iterable, List, Optional

from bigquery_service import BigQueryService
from event_semantic_normalizer import EventSemanticNormalizer
from gcs_service import GcsService
from local_job_store import resolve_or_create_canonical_user_id
from pipeline_models import PIPELINE_SCHEMA_VERSION, build_event_fingerprint, derive_event_date


def _coerce_manifest(payload: Dict[str, Any]) -> Dict[str, Any]:
    gcs_uri = payload.get("gcs_uri") or payload.get("gcs_path")
    return {
        "job_id": str(payload.get("job_id") or "unknown_job"),
        "source": str(payload.get("source") or "unknown"),
        "gcs_uri": str(gcs_uri or ""),
        "event_count": int(payload.get("event_count") or 0),
        "start_date": str(payload.get("start_date") or ""),
        "end_date": str(payload.get("end_date") or ""),
        "shard_index": int(payload.get("shard_index") or 1),
        "source_config_id": str(payload.get("source_config_id") or payload.get("source") or "default"),
        "schema_version": str(payload.get("schema_version") or PIPELINE_SCHEMA_VERSION),
    }


class DataflowNormalizationRunner:
    """
    Production-shaped normalization runner that consumes shard manifests,
    normalizes events, writes valid rows to staging, and routes invalid rows
    to a dead-letter sink.
    """

    def __init__(
        self,
        gcs_service: Optional[GcsService] = None,
        bigquery_service: Optional[BigQueryService] = None,
        event_name_map: Optional[Dict[str, str]] = None,
        property_key_map: Optional[Dict[str, str]] = None,
        use_local_identity_store: Optional[bool] = None,
    ):
        self.gcs_service = gcs_service or GcsService()
        self.bigquery_service = bigquery_service or BigQueryService()
        self.normalizer = EventSemanticNormalizer(
            event_name_map=event_name_map or {},
            property_key_map=property_key_map or {},
        )
        self.use_local_identity_store = (
            self.gcs_service.mode == "mock"
            if use_local_identity_store is None
            else bool(use_local_identity_store)
        )

    def _blob_name_from_gcs_uri(self, gcs_uri: str) -> str:
        return gcs_uri.replace(f"gs://{self.gcs_service.bucket_name}/", "")

    def _resolve_canonical_user_id(self, source: str, player_id: Any) -> str:
        player_text = str(player_id or "unknown_user")
        if self.use_local_identity_store:
            return resolve_or_create_canonical_user_id(source, player_text)
        return f"uid:{player_text}"

    def _build_dead_letter_row(self, event: Dict[str, Any], manifest: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "job_id": manifest["job_id"],
            "job_identifier": manifest["job_id"],
            "source": manifest["source"],
            "source_config_id": manifest["source_config_id"],
            "shard_index": manifest["shard_index"],
            "raw_gcs_uri": manifest["gcs_uri"],
            "player_id": str(event.get("player_id") or "unknown_user"),
            "event_type": str(event.get("event_type") or "unknown_event"),
            "event_time": event.get("event_time"),
            "event_date": event.get("event_date"),
            "data_quality_flags": list(event.get("data_quality_flags") or []),
            "rejection_reason": event.get("rejection_reason") or "critical_quality_failure",
            "normalized_event": event,
        }

    def normalize_manifest(self, manifest_payload: Dict[str, Any]) -> Dict[str, Any]:
        manifest = _coerce_manifest(manifest_payload)
        if not manifest["gcs_uri"]:
            raise ValueError("Manifest is missing gcs_uri/gcs_path.")

        blob_name = self._blob_name_from_gcs_uri(manifest["gcs_uri"])
        raw_events = self.gcs_service.download_raw_events(blob_name)
        normalized_events = self.normalizer.normalize_events(raw_events)

        valid_rows: List[Dict[str, Any]] = []
        dead_letters: List[Dict[str, Any]] = []
        flag_counts: Dict[str, int] = {}

        for event in normalized_events:
            source = str(event.get("source") or manifest["source"])
            player_id = str(event.get("player_id") or "unknown_user")
            event["source"] = source
            event["job_id"] = manifest["job_id"]
            event["job_identifier"] = manifest["job_id"]
            event["source_config_id"] = manifest["source_config_id"]
            event["raw_gcs_uri"] = manifest["gcs_uri"]
            event["shard_index"] = manifest["shard_index"]
            event["schema_version"] = event.get("schema_version") or manifest["schema_version"]
            event["event_date"] = event.get("event_date") or derive_event_date(event.get("event_time"))
            event["canonical_user_id"] = self._resolve_canonical_user_id(source, player_id)
            event["event_fingerprint"] = build_event_fingerprint(
                event,
                canonical_user_id=event["canonical_user_id"],
            )

            flags = list(event.get("data_quality_flags") or [])
            for flag in flags:
                flag_counts[flag] = flag_counts.get(flag, 0) + 1

            if "missing_player_id" in flags or "invalid_event_time" in flags:
                event["rejection_reason"] = "critical_quality_failure"
                dead_letters.append(self._build_dead_letter_row(event, manifest))
            else:
                valid_rows.append(event)

        return {
            "manifest": manifest,
            "valid_rows": valid_rows,
            "dead_letters": dead_letters,
            "stats": {
                "raw_normalized_events": len(normalized_events),
                "events_staging_written": len(valid_rows),
                "pipeline_dead_letters_written": len(dead_letters),
                "flag_counts": flag_counts,
            },
        }

    def process_manifest(self, manifest_payload: Dict[str, Any]) -> Dict[str, Any]:
        result = self.normalize_manifest(manifest_payload)
        manifest = result["manifest"]
        self.bigquery_service.write_events_staging(result["valid_rows"], job_id=manifest["job_id"])
        self.bigquery_service.write_pipeline_dead_letters(result["dead_letters"], job_id=manifest["job_id"])
        return result["stats"]

    def process_manifests(self, manifest_payloads: Iterable[Dict[str, Any]]) -> Dict[str, Any]:
        summary = {
            "manifests_processed": 0,
            "raw_normalized_events": 0,
            "events_staging_written": 0,
            "pipeline_dead_letters_written": 0,
            "flag_counts": {},
            "warehouse_stats": {},
        }
        for payload in manifest_payloads:
            stats = self.process_manifest(payload)
            summary["manifests_processed"] += 1
            summary["raw_normalized_events"] += stats["raw_normalized_events"]
            summary["events_staging_written"] += stats["events_staging_written"]
            summary["pipeline_dead_letters_written"] += stats["pipeline_dead_letters_written"]
            for flag, count in stats["flag_counts"].items():
                summary["flag_counts"][flag] = summary["flag_counts"].get(flag, 0) + count
        if summary["manifests_processed"] > 0:
            summary["warehouse_stats"] = {
                "curation": self.bigquery_service.run_events_curation(),
                "player_latest_state": self.bigquery_service.refresh_player_latest_state(),
            }
        return summary

    def process_notifications(self, notifications: Iterable[Dict[str, Any]]) -> Dict[str, Any]:
        return self.process_manifests(_coerce_manifest(notification) for notification in notifications)


def _load_jsonl(path: str) -> List[Dict[str, Any]]:
    records: List[Dict[str, Any]] = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            records.append(json.loads(line))
    return records


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Run the KairyxAI Dataflow normalization pipeline locally.")
    parser.add_argument("--manifests-jsonl", required=True, help="Path to newline-delimited shard manifest JSON.")
    args = parser.parse_args(argv)

    runner = DataflowNormalizationRunner()
    manifests = _load_jsonl(args.manifests_jsonl)
    summary = runner.process_manifests(manifests)
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
