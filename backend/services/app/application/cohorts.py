from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List

from app.application.experiments import ExperimentConfigService
from app.core.errors import ResourceLockedError
from bigquery_service import BigQueryService, get_shared_bigquery_service


class CohortService:
    def __init__(self, repository, bigquery_service: BigQueryService | None = None):
        self.repository = repository
        self.bigquery_service = bigquery_service or get_shared_bigquery_service()
        self.experiments = ExperimentConfigService(repository)

    def _commit_session(self) -> None:
        session = getattr(self.repository, "session", None)
        if session is not None:
            session.commit()

    def create_cohort(
        self,
        *,
        name: str,
        cohort_type: str,
        definition: Dict[str, Any],
        refresh_mode: str = "manual",
        owner: str = "system",
        tags: List[str] | None = None,
        description: str = "",
        activate: bool = False,
    ) -> Dict[str, Any]:
        if any(item.get("name") == name for item in self.repository.list_resources("cohort")):
            raise ValueError(f"Cohort '{name}' already exists.")
        cohort_id = f"cohort_{uuid.uuid4().hex[:20]}"
        refresh_policy = self._build_refresh_policy(refresh_mode)
        payload = {
            "cohort_id": cohort_id,
            "name": name,
            "type": str(cohort_type).lower(),
            "status": "draft",
            "refresh_mode": refresh_mode,
            "refresh_policy": refresh_policy,
            "owner": owner,
            "description": description,
            "deleted_at": None,
            "version": 1,
            "version_id": 1,
            "definition": definition,
            "tags": list(tags or []),
            "member_count": 0,
            "preview_members": [],
            "latest_members": [],
            "latest_snapshot_id": None,
            "delta": {"added": 0, "removed": 0, "unchanged": 0},
            "last_refreshed_at": None,
            "last_refresh_status": "not_started",
            "last_refresh_error": None,
            "refresh_failures": 0,
            "metrics_summary": self._empty_metrics_summary(),
            "activation_preflight": self._build_activation_preflight([], False, None),
        }
        record = self.repository.upsert_resource("cohort", cohort_id, status="draft", name=name, payload=payload)
        self._create_definition_version(cohort_id, 1, payload)
        self.repository.record_resource_event("cohort", cohort_id, event_type="cohort_created", payload=payload)
        self.repository.record_action("cohort_created", "cohort", cohort_id, payload)
        refreshed = self.refresh_cohort(cohort_id, force=True)
        if activate:
            refreshed = self.activate_cohort(cohort_id, force=False)
        return refreshed

    def list_cohorts(self) -> List[Dict[str, Any]]:
        return [self._to_response(item) for item in self.repository.list_resources("cohort")]

    def get_cohort(self, cohort_id: str) -> Dict[str, Any] | None:
        record = self.repository.get_resource("cohort", cohort_id)
        return self._to_response(record) if record else None

    def update_cohort(self, cohort_id: str, patch: Dict[str, Any]) -> Dict[str, Any]:
        record = self.repository.get_resource("cohort", cohort_id)
        if record is None:
            raise KeyError(cohort_id)
        payload = dict(record.get("payload") or {})
        definition_changed = False
        for field in ("name", "owner", "description"):
            if field in patch and patch.get(field) is not None:
                payload[field] = patch[field]
        if patch.get("tags") is not None:
            payload["tags"] = list(patch.get("tags") or [])
        for field in ("type", "definition", "refresh_mode"):
            if field in patch and patch.get(field) is not None and patch.get(field) != payload.get(field):
                payload[field] = patch[field]
                definition_changed = True
        if definition_changed:
            next_version = int(payload.get("version_id") or payload.get("version") or 1) + 1
            payload["version"] = next_version
            payload["version_id"] = next_version
            payload["refresh_policy"] = self._build_refresh_policy(payload.get("refresh_mode") or "manual")
        saved = self.repository.upsert_resource(
            "cohort",
            cohort_id,
            status=str(payload.get("status") or "draft"),
            name=payload.get("name"),
            payload=payload,
        )
        if definition_changed:
            self._create_definition_version(cohort_id, int(payload["version_id"]), payload)
        self.repository.record_resource_event("cohort", cohort_id, event_type="cohort_updated", payload={"patch": patch, "definition_changed": definition_changed})
        return self._to_response(saved)

    def list_versions(self, cohort_id: str) -> Dict[str, Any]:
        record = self.repository.get_resource("cohort", cohort_id)
        if record is None:
            raise KeyError(cohort_id)
        return {"items": self.repository.list_resource_versions("cohort", cohort_id)}

    def list_members(self, cohort_id: str, *, page: int = 1, page_size: int = 100) -> Dict[str, Any]:
        cohort = self.get_cohort(cohort_id)
        if cohort is None:
            raise KeyError(cohort_id)
        members = list(cohort.get("latest_members") or [])
        page = max(1, int(page))
        page_size = max(1, int(page_size))
        offset = (page - 1) * page_size
        return {
            "page": page,
            "page_size": page_size,
            "total": len(members),
            "items": members[offset: offset + page_size],
        }

    def get_metrics(self, cohort_id: str) -> Dict[str, Any]:
        cohort = self.get_cohort(cohort_id)
        if cohort is None:
            raise KeyError(cohort_id)
        metrics = self._calculate_metrics(cohort)
        resource_id = f"{cohort_id}:{datetime.utcnow().date().isoformat()}"
        self.repository.upsert_resource(
            "cohort_metrics_daily",
            resource_id,
            status="ready",
            name=cohort.get("name"),
            payload=metrics,
        )
        for experiment_id in metrics.get("experiment_ids") or []:
            self.repository.upsert_resource(
                "cohort_experiment_link",
                f"{cohort_id}:{experiment_id}",
                status="linked",
                name=cohort.get("name"),
                payload={
                    "cohort_id": cohort_id,
                    "experiment_id": experiment_id,
                    "workflow_ids": metrics.get("workflow_ids") or [],
                    "updated_at": datetime.utcnow().isoformat(),
                },
            )
        record = self.repository.get_resource("cohort", cohort_id)
        if record is not None:
            payload = dict(record.get("payload") or {})
            payload["metrics_summary"] = metrics
            self.repository.upsert_resource("cohort", cohort_id, status=payload.get("status") or "draft", name=payload.get("name"), payload=payload)
        return metrics

    def get_overview(self, cohort_id: str) -> Dict[str, Any]:
        cohort = self.get_cohort(cohort_id)
        if cohort is None:
            raise KeyError(cohort_id)
        metrics = self.get_metrics(cohort_id)
        refresh_jobs = self.list_refresh_jobs(cohort_id).get("items") or []
        current_version = int(cohort.get("version_id") or cohort.get("version") or 1)
        compare = None
        if current_version > 1:
            try:
                compare = self.compare_versions(cohort_id, base_version=current_version - 1, target_version=current_version)
            except KeyError:
                compare = None
        linked_workflows = self._linked_workflows(cohort_id)
        linked_experiments = self._linked_experiments(cohort_id, metrics)
        failed_refreshes = [item for item in refresh_jobs if str(item.get("status") or "") == "failed"]
        return {
            "cohort": cohort,
            "metrics": metrics,
            "measurement_state": metrics.get("measurement_state") or {},
            "linked_workflows": linked_workflows,
            "linked_experiments": linked_experiments,
            "refresh_summary": {
                "total_runs": len(refresh_jobs),
                "failed_runs": len(failed_refreshes),
                "last_status": refresh_jobs[0].get("status") if refresh_jobs else None,
                "latest_snapshot_id": cohort.get("latest_snapshot_id"),
                "consecutive_failures": int(cohort.get("refresh_failures") or 0),
                "auto_pause_after_failures": int((cohort.get("refresh_policy") or {}).get("auto_pause_after_failures") or 2),
            },
            "recent_refresh_jobs": refresh_jobs[:5],
            "latest_compare": compare,
        }

    def compare_versions(self, cohort_id: str, *, base_version: int, target_version: int) -> Dict[str, Any]:
        cohort = self.get_cohort(cohort_id)
        if cohort is None:
            raise KeyError(cohort_id)
        version_items = self.repository.list_resource_versions("cohort", cohort_id)
        base_version_payload = next((item.get("payload") or {} for item in version_items if int(item.get("version") or 0) == int(base_version)), None)
        target_version_payload = next((item.get("payload") or {} for item in version_items if int(item.get("version") or 0) == int(target_version)), None)
        snapshots = [
            item.get("payload") or {}
            for item in self.repository.list_resources("cohort_snapshot")
            if str((item.get("payload") or {}).get("cohort_id") or "") == cohort_id
        ]
        base_snapshot = self._latest_snapshot_for_version(snapshots, base_version)
        target_snapshot = self._latest_snapshot_for_version(snapshots, target_version)
        if base_snapshot is None or target_snapshot is None:
            raise KeyError("snapshot_missing")
        base_ids = {self._member_identity(item) for item in base_snapshot.get("members") or [] if self._member_identity(item)}
        target_ids = {self._member_identity(item) for item in target_snapshot.get("members") or [] if self._member_identity(item)}
        base_metrics = self._calculate_metrics_for_snapshot(cohort_id, base_snapshot)
        target_metrics = self._calculate_metrics_for_snapshot(cohort_id, target_snapshot)
        return {
            "cohort_id": cohort_id,
            "base_version": int(base_version),
            "target_version": int(target_version),
            "base_snapshot_id": base_snapshot.get("snapshot_id"),
            "target_snapshot_id": target_snapshot.get("snapshot_id"),
            "base_member_count": int(base_snapshot.get("member_count") or len(base_ids)),
            "target_member_count": int(target_snapshot.get("member_count") or len(target_ids)),
            "member_delta": {
                "added": len(target_ids - base_ids),
                "removed": len(base_ids - target_ids),
                "unchanged": len(base_ids & target_ids),
            },
            "definition_diff": self._definition_diff(base_version_payload or {}, target_version_payload or {}),
            "metrics_delta": self._metrics_delta(base_metrics, target_metrics),
        }

    def refresh_cohort(self, cohort_id: str, *, force: bool = False) -> Dict[str, Any]:
        record = self.repository.get_resource("cohort", cohort_id)
        if record is None:
            raise KeyError(cohort_id)
        payload = dict(record.get("payload") or {})
        refresh_job_id = f"crj_{uuid.uuid4().hex[:20]}"
        refresh_payload = {
            "refresh_job_id": refresh_job_id,
            "cohort_id": cohort_id,
            "status": "running",
            "requested_at": datetime.utcnow().isoformat(),
            "force": bool(force),
            "version_id": int(payload.get("version_id") or payload.get("version") or 1),
        }
        self.repository.upsert_resource("cohort_refresh_job", refresh_job_id, status="running", name=payload.get("name"), payload=refresh_payload)
        previous_members = list(payload.get("latest_members") or [])
        attempts = 2 if str(payload.get("refresh_mode") or "").lower() == "daily" else 1
        last_error: Exception | None = None
        members: List[Dict[str, Any]] = []
        for _ in range(attempts):
            try:
                members = self._materialize_members(payload.get("type") or "rule", payload.get("definition") or {})
                last_error = None
                break
            except Exception as exc:
                last_error = exc
        if last_error is not None:
            failures = int(payload.get("refresh_failures") or 0) + 1
            payload["refresh_failures"] = failures
            payload["last_refresh_status"] = "failed"
            payload["last_refresh_error"] = str(last_error)
            if failures >= int((payload.get("refresh_policy") or {}).get("auto_pause_after_failures") or 2):
                payload["status"] = "paused"
            payload["activation_preflight"] = self._build_activation_preflight(
                previous_members,
                False,
                payload.get("latest_snapshot_id"),
                refresh_error=str(last_error),
            )
            saved = self.repository.upsert_resource(
                "cohort",
                cohort_id,
                status=payload.get("status") or "draft",
                name=payload.get("name"),
                payload=payload,
            )
            self.repository.record_resource_event(
                "cohort",
                cohort_id,
                event_type="cohort_refresh_failed",
                payload={"error": str(last_error), "refresh_failures": failures, "force": force},
            )
            refresh_payload["status"] = "failed"
            refresh_payload["error"] = str(last_error)
            refresh_payload["completed_at"] = datetime.utcnow().isoformat()
            self.repository.upsert_resource("cohort_refresh_job", refresh_job_id, status="failed", name=payload.get("name"), payload=refresh_payload)
            self._commit_session()
            raise ValueError(str(last_error)) from last_error

        previous_ids = {self._member_identity(item) for item in previous_members if self._member_identity(item)}
        current_ids = {self._member_identity(item) for item in members if self._member_identity(item)}
        snapshot_id = self._persist_snapshot(cohort_id, payload, members)
        payload["latest_members"] = members
        payload["preview_members"] = members[:20]
        payload["member_count"] = len(members)
        payload["latest_snapshot_id"] = snapshot_id
        payload["last_refreshed_at"] = datetime.utcnow().isoformat()
        payload["last_refresh_status"] = "success"
        payload["last_refresh_error"] = None
        payload["refresh_failures"] = 0
        payload["delta"] = {
            "added": len(current_ids - previous_ids),
            "removed": len(previous_ids - current_ids),
            "unchanged": len(current_ids & previous_ids),
        }
        payload["activation_preflight"] = self._build_activation_preflight(members, True, snapshot_id)
        if str(payload.get("status") or "") == "archived":
            next_status = "archived"
        else:
            next_status = str(payload.get("status") or "draft")
        payload["status"] = next_status
        saved = self.repository.upsert_resource(
            "cohort",
            cohort_id,
            status=payload["status"],
            name=payload.get("name"),
            payload=payload,
        )
        self.repository.record_resource_event(
            "cohort",
            cohort_id,
            event_type="cohort_refreshed",
            payload={"member_count": len(members), "delta": payload["delta"], "snapshot_id": snapshot_id, "force": force},
        )
        refresh_payload["status"] = "completed"
        refresh_payload["snapshot_id"] = snapshot_id
        refresh_payload["member_count"] = len(members)
        refresh_payload["delta"] = payload["delta"]
        refresh_payload["completed_at"] = datetime.utcnow().isoformat()
        self.repository.upsert_resource("cohort_refresh_job", refresh_job_id, status="completed", name=payload.get("name"), payload=refresh_payload)
        return self._to_response(saved)

    def activate_cohort(self, cohort_id: str, *, force: bool = False) -> Dict[str, Any]:
        record = self.repository.get_resource("cohort", cohort_id)
        if record is None:
            raise KeyError(cohort_id)
        payload = dict(record.get("payload") or {})
        if not payload.get("latest_snapshot_id") or force:
            self.refresh_cohort(cohort_id, force=force)
            record = self.repository.get_resource("cohort", cohort_id)
            payload = dict((record or {}).get("payload") or {})
        preflight = payload.get("activation_preflight") or self._build_activation_preflight(
            payload.get("latest_members") or [],
            str(payload.get("last_refresh_status") or "") == "success",
            payload.get("latest_snapshot_id"),
        )
        if not preflight.get("eligible"):
            self.repository.record_resource_event(
                "cohort",
                cohort_id,
                event_type="cohort_activation_blocked",
                payload={"activation_preflight": preflight},
            )
            self._commit_session()
            raise ValueError("Cohort activation preflight failed.")
        payload["status"] = "active"
        payload["activation_preflight"] = preflight
        saved = self.repository.upsert_resource("cohort", cohort_id, status="active", name=payload.get("name"), payload=payload)
        self.repository.record_resource_event(
            "cohort",
            cohort_id,
            event_type="cohort_activation_audit",
            payload={"activation_preflight": preflight, "snapshot_id": payload.get("latest_snapshot_id")},
        )
        self.repository.record_resource_event("cohort", cohort_id, event_type="cohort_activated", payload={"member_count": payload.get("member_count", 0)})
        return self._to_response(saved)

    def pause_cohort(self, cohort_id: str) -> Dict[str, Any]:
        return self._set_status(cohort_id, "paused", "cohort_paused")

    def archive_cohort(self, cohort_id: str) -> Dict[str, Any]:
        blocking_workflows = self._referencing_workflows(cohort_id, statuses={"published"})
        if blocking_workflows:
            raise ResourceLockedError(
                f"Cohort '{cohort_id}' is locked by published workflows: {', '.join(blocking_workflows[:5])}."
            )
        archived_at = datetime.utcnow().isoformat()
        return self._set_status(
            cohort_id,
            "archived",
            "cohort_archived",
            extra_payload={"archived_at": archived_at, "deleted_at": archived_at},
        )

    def restore_cohort(self, cohort_id: str) -> Dict[str, Any]:
        record = self.repository.get_resource("cohort", cohort_id)
        if record is None:
            raise KeyError(cohort_id)
        payload = dict(record.get("payload") or {})
        if str(payload.get("status") or "") != "archived":
            raise ValueError("Only archived cohorts can be restored.")
        payload["archived_at"] = None
        payload["deleted_at"] = None
        payload["status"] = "draft"
        saved = self.repository.upsert_resource("cohort", cohort_id, status="draft", name=payload.get("name"), payload=payload)
        self.repository.record_resource_event("cohort", cohort_id, event_type="cohort_restored", payload={"status": "draft"})
        return self._to_response(saved)

    def list_refresh_jobs(self, cohort_id: str) -> Dict[str, Any]:
        cohort = self.get_cohort(cohort_id)
        if cohort is None:
            raise KeyError(cohort_id)
        items = []
        for record in self.repository.list_resources("cohort_refresh_job"):
            payload = record.get("payload") or {}
            if str(payload.get("cohort_id") or "") == cohort_id:
                items.append(payload)
        items = sorted(items, key=lambda item: str(item.get("requested_at") or item.get("completed_at") or ""), reverse=True)
        return {"cohort_id": cohort_id, "items": items}

    def permanent_delete(self, cohort_id: str) -> Dict[str, Any]:
        cohort = self.get_cohort(cohort_id)
        if cohort is None:
            raise KeyError(cohort_id)
        blocking_workflows = self._referencing_workflows(cohort_id)
        if blocking_workflows:
            raise ResourceLockedError(
                f"Cohort '{cohort_id}' is locked by workflows: {', '.join(blocking_workflows[:5])}."
            )
        for resource_type in ("cohort_snapshot", "cohort_metrics_daily", "cohort_experiment_link", "cohort_refresh_job"):
            for record in self.repository.list_resources(resource_type):
                payload = record.get("payload") or {}
                if str(payload.get("cohort_id") or "") == cohort_id:
                    self.repository.delete_resource(resource_type, record["resource_id"])
        deleted = self.repository.delete_resource("cohort", cohort_id)
        if deleted:
            self.repository.record_action("cohort_permanently_deleted", "cohort", cohort_id, {"cohort_id": cohort_id})
        return {"cohort_id": cohort_id, "deleted": bool(deleted)}

    def rollback_cohort(self, cohort_id: str, version: int) -> Dict[str, Any]:
        record = self.repository.get_resource("cohort", cohort_id)
        if record is None:
            raise KeyError(cohort_id)
        version_items = self.repository.list_resource_versions("cohort", cohort_id)
        selected = next((item for item in version_items if int(item.get("version") or 0) == int(version)), None)
        if selected is None:
            raise KeyError(version)
        current_payload = dict(record.get("payload") or {})
        version_payload = dict(selected.get("payload") or {})
        next_version = 1 + max((int(item.get("version") or 0) for item in version_items), default=0)
        current_payload["definition"] = version_payload.get("definition") or {}
        current_payload["type"] = version_payload.get("type") or current_payload.get("type") or "rule"
        current_payload["tags"] = list(version_payload.get("tags") or current_payload.get("tags") or [])
        current_payload["refresh_mode"] = version_payload.get("refresh_mode") or current_payload.get("refresh_mode") or "manual"
        current_payload["refresh_policy"] = version_payload.get("refresh_policy") or self._build_refresh_policy(current_payload["refresh_mode"])
        current_payload["version"] = next_version
        current_payload["version_id"] = next_version
        saved = self.repository.upsert_resource(
            "cohort",
            cohort_id,
            status=str(current_payload.get("status") or "draft"),
            name=current_payload.get("name"),
            payload=current_payload,
        )
        self._create_definition_version(cohort_id, next_version, current_payload)
        self.repository.record_resource_event(
            "cohort",
            cohort_id,
            event_type="cohort_rolled_back",
            payload={"rolled_back_to_version": int(version), "new_version": next_version},
        )
        return self.refresh_cohort(cohort_id, force=True)

    def _set_status(
        self,
        cohort_id: str,
        status: str,
        event_type: str,
        *,
        extra_payload: Dict[str, Any] | None = None,
    ) -> Dict[str, Any]:
        record = self.repository.get_resource("cohort", cohort_id)
        if record is None:
            raise KeyError(cohort_id)
        payload = dict(record.get("payload") or {})
        payload["status"] = status
        if extra_payload:
            payload.update(extra_payload)
        saved = self.repository.upsert_resource("cohort", cohort_id, status=status, name=payload.get("name"), payload=payload)
        self.repository.record_resource_event("cohort", cohort_id, event_type=event_type, payload={"status": status, **(extra_payload or {})})
        return self._to_response(saved)

    def _create_definition_version(self, cohort_id: str, version: int, payload: Dict[str, Any]) -> None:
        version_payload = {
            "cohort_id": cohort_id,
            "version": int(version),
            "version_id": int(version),
            "name": payload.get("name"),
            "type": payload.get("type"),
            "definition": payload.get("definition") or {},
            "refresh_mode": payload.get("refresh_mode"),
            "refresh_policy": payload.get("refresh_policy") or {},
            "tags": list(payload.get("tags") or []),
        }
        self.repository.create_resource_version("cohort", cohort_id, version=int(version), payload=version_payload)

    def _persist_snapshot(self, cohort_id: str, payload: Dict[str, Any], members: List[Dict[str, Any]]) -> str:
        snapshot_id = f"csnap_{uuid.uuid4().hex[:20]}"
        snapshot_payload = {
            "snapshot_id": snapshot_id,
            "cohort_id": cohort_id,
            "version_id": int(payload.get("version_id") or payload.get("version") or 1),
            "member_count": len(members),
            "members": members,
            "created_at": datetime.utcnow().isoformat(),
        }
        self.repository.upsert_resource("cohort_snapshot", snapshot_id, status="ready", name=payload.get("name"), payload=snapshot_payload)
        self.repository.record_resource_event("cohort", cohort_id, event_type="cohort_snapshot_created", payload={"snapshot_id": snapshot_id, "member_count": len(members)})
        return snapshot_id

    def _to_response(self, record: Dict[str, Any]) -> Dict[str, Any]:
        payload = dict(record.get("payload") or {})
        payload.setdefault("version", int(payload.get("version_id") or payload.get("version") or 1))
        payload.setdefault("version_id", int(payload.get("version") or payload.get("version_id") or 1))
        payload.setdefault("refresh_policy", self._build_refresh_policy(payload.get("refresh_mode") or "manual"))
        payload.setdefault("description", "")
        payload.setdefault("deleted_at", payload.get("archived_at"))
        payload.setdefault("metrics_summary", self._empty_metrics_summary())
        payload.setdefault(
            "activation_preflight",
            self._build_activation_preflight(
                payload.get("latest_members") or [],
                str(payload.get("last_refresh_status") or "") == "success",
                payload.get("latest_snapshot_id"),
            ),
        )
        payload.setdefault("created_at", record["created_at"])
        payload.setdefault("updated_at", record["updated_at"])
        payload.setdefault("tenant_id", record.get("tenant_id"))
        payload.setdefault("created_by", record.get("created_by") or payload.get("created_by") or "system")
        payload.setdefault("updated_by", record.get("updated_by") or payload.get("updated_by") or "system")
        payload.setdefault("correlation_id", record.get("correlation_id") or payload.get("correlation_id") or "")
        return payload

    @staticmethod
    def _build_refresh_policy(refresh_mode: str) -> Dict[str, Any]:
        resolved = str(refresh_mode or "manual").lower()
        return {
            "mode": resolved,
            "retry_limit": 1 if resolved == "daily" else 0,
            "auto_pause_after_failures": 2,
        }

    @staticmethod
    def _empty_metrics_summary() -> Dict[str, Any]:
        return {
            "member_count": 0,
            "delivered_users": 0,
            "reach_rate": 0.0,
            "conversion_users": 0,
            "conversion_rate": 0.0,
            "workflow_ids": [],
            "experiment_ids": [],
        }

    @staticmethod
    def _definition_diff(base_version_payload: Dict[str, Any], target_version_payload: Dict[str, Any]) -> Dict[str, Any]:
        diff: Dict[str, Any] = {}
        for field in ("type", "refresh_mode", "definition", "tags"):
            if (base_version_payload or {}).get(field) != (target_version_payload or {}).get(field):
                diff[field] = {
                    "base": (base_version_payload or {}).get(field),
                    "target": (target_version_payload or {}).get(field),
                }
        return diff

    @staticmethod
    def _metrics_delta(base_metrics: Dict[str, Any], target_metrics: Dict[str, Any]) -> Dict[str, Any]:
        result = {}
        for field in ("member_count", "delivered_users", "reach_rate", "conversion_users", "conversion_rate"):
            result[field] = {
                "base": base_metrics.get(field, 0),
                "target": target_metrics.get(field, 0),
                "delta": round(float(target_metrics.get(field, 0)) - float(base_metrics.get(field, 0)), 4),
            }
        return result

    def _latest_snapshot_for_version(self, snapshots: List[Dict[str, Any]], version: int) -> Dict[str, Any] | None:
        candidates = [item for item in snapshots if int(item.get("version_id") or 0) == int(version)]
        if not candidates:
            return None
        return sorted(candidates, key=lambda item: str(item.get("created_at") or ""), reverse=True)[0]

    def _calculate_metrics(self, cohort: Dict[str, Any]) -> Dict[str, Any]:
        cohort_id = cohort["cohort_id"]
        snapshot_id = cohort.get("latest_snapshot_id")
        if snapshot_id:
            snapshot_payload = {"snapshot_id": snapshot_id, "cohort_id": cohort_id, "version_id": int(cohort.get("version_id") or 1)}
            return self._calculate_metrics_for_snapshot(cohort_id, snapshot_payload, default_member_count=int(cohort.get("member_count") or 0))
        return self._empty_metrics_summary() | {"cohort_id": cohort_id}

    def _calculate_metrics_for_snapshot(
        self,
        cohort_id: str,
        snapshot: Dict[str, Any],
        *,
        default_member_count: int | None = None,
    ) -> Dict[str, Any]:
        snapshot_id = snapshot.get("snapshot_id")
        deliveries = [
            item.get("payload") or {}
            for item in self.repository.list_resources("workflow_delivery")
            if str((item.get("payload") or {}).get("cohort_id") or "") == cohort_id
            and (snapshot_id is None or str((item.get("payload") or {}).get("cohort_snapshot_id") or "") == str(snapshot_id))
            and not bool((item.get("payload") or {}).get("sandbox"))
        ]
        delivered_users = {
            str(item.get("user_id"))
            for item in deliveries
            if str(item.get("delivery_status") or "") in {"delivered", "opened", "clicked", "returned", "converted"}
            and item.get("user_id")
        }
        workflow_ids = sorted(
            {
                str(item.get("workflow_id"))
                for item in deliveries
                if item.get("workflow_id")
            }
        )
        delivery_ids = {
            str(item.get("action_execution_id"))
            for item in deliveries
            if item.get("action_execution_id")
        }
        outcomes = []
        for item in self.repository.list_resource_events("experiment", event_type="outcome", limit=5000):
            payload = item.get("payload") or {}
            if str(payload.get("cohort_id") or "") != cohort_id:
                continue
            if delivery_ids and payload.get("action_execution_id") and str(payload.get("action_execution_id")) not in delivery_ids:
                continue
            outcomes.append(payload)
        conversion_users = {
            str(item.get("user_id"))
            for item in outcomes
            if str(item.get("outcome_name") or "").lower() in {"returned", "returned_to_game", "converted", "purchase"}
            and item.get("user_id")
        }
        experiment_ids = sorted(
            {
                str(item.get("experiment_id"))
                for item in outcomes
                if item.get("experiment_id")
            }
            | {
                str((item.get("payload") or {}).get("experiment_id"))
                for item in self.repository.list_resources("experiment")
                if str(((item.get("payload") or {}).get("cohort_id") or "")) == cohort_id
            }
        )
        experiment_summaries = []
        for experiment_id in experiment_ids:
            summary = self.experiments.get_summary(experiment_id)
            experiment_summaries.append(
                {
                    "experiment_id": experiment_id,
                    "decision": summary.get("decision"),
                    "decision_reason": summary.get("decision_reason"),
                    "sample_size": summary.get("sample_size"),
                    "runtime_hours": summary.get("runtime_hours"),
                }
            )
        member_count = int(default_member_count if default_member_count is not None else snapshot.get("member_count") or 0)
        measurement_state = self._build_measurement_state(
            member_count=member_count,
            delivered_users=len(delivered_users),
            conversion_users=len(conversion_users),
            workflow_ids=workflow_ids,
            experiment_summaries=experiment_summaries,
        )
        return {
            "cohort_id": cohort_id,
            "snapshot_id": snapshot_id,
            "version_id": int(snapshot.get("version_id") or 1),
            "member_count": member_count,
            "delivered_users": len(delivered_users),
            "reach_rate": round((len(delivered_users) / member_count), 4) if member_count else 0.0,
            "conversion_users": len(conversion_users),
            "conversion_rate": round((len(conversion_users) / member_count), 4) if member_count else 0.0,
            "workflow_ids": workflow_ids,
            "experiment_ids": experiment_ids,
            "experiment_summaries": experiment_summaries,
            "measurement_state": measurement_state,
            "last_calculated_at": datetime.utcnow().isoformat(),
        }

    @staticmethod
    def _build_measurement_state(
        *,
        member_count: int,
        delivered_users: int,
        conversion_users: int,
        workflow_ids: List[str],
        experiment_summaries: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        warnings = []
        delivery_status = "ready" if delivered_users > 0 else ("missing" if workflow_ids else "not_configured")
        outcome_status = "ready" if conversion_users > 0 else ("missing" if delivered_users > 0 else "not_observed")
        experiment_status = "ready" if experiment_summaries else "missing"
        if member_count > 0 and workflow_ids and delivered_users == 0:
            warnings.append("delivery_signal_missing")
        if delivered_users > 0 and conversion_users == 0:
            warnings.append("outcome_signal_missing")
        if workflow_ids and not experiment_summaries:
            warnings.append("experiment_summary_missing")
        return {
            "delivery_signal_status": delivery_status,
            "outcome_signal_status": outcome_status,
            "experiment_summary_status": experiment_status,
            "warnings": warnings,
        }

    def _linked_workflows(self, cohort_id: str) -> List[Dict[str, Any]]:
        items = []
        for record in self.repository.list_resources("workflow"):
            payload = record.get("payload") or {}
            definition = payload.get("definition") or {}
            if str(definition.get("cohort_id") or payload.get("cohort_id") or "") != cohort_id:
                continue
            items.append(
                {
                    "workflow_id": payload.get("workflow_id") or record.get("resource_id"),
                    "name": payload.get("name"),
                    "status": payload.get("status") or record.get("status"),
                    "experiment_id": definition.get("experiment_id") or payload.get("experiment_id"),
                    "trigger_type": ((definition.get("trigger") or {}).get("type") or (payload.get("trigger") or {}).get("type")),
                }
            )
        return sorted(items, key=lambda item: str(item.get("workflow_id") or ""))

    def _linked_experiments(self, cohort_id: str, metrics: Dict[str, Any]) -> List[Dict[str, Any]]:
        items = list(metrics.get("experiment_summaries") or [])
        known_ids = {str(item.get("experiment_id") or "") for item in items if item.get("experiment_id")}
        for record in self.repository.list_resources("experiment"):
            payload = record.get("payload") or {}
            experiment_id = str(payload.get("experiment_id") or record.get("resource_id") or "")
            if not experiment_id or experiment_id in known_ids:
                continue
            if str(payload.get("cohort_id") or "") != cohort_id:
                continue
            summary = self.experiments.get_summary(experiment_id)
            items.append(
                {
                    "experiment_id": experiment_id,
                    "decision": summary.get("decision"),
                    "decision_reason": summary.get("decision_reason"),
                    "sample_size": summary.get("sample_size"),
                    "runtime_hours": summary.get("runtime_hours"),
                }
            )
        return sorted(items, key=lambda item: str(item.get("experiment_id") or ""))

    def _referencing_workflows(self, cohort_id: str, *, statuses: set[str] | None = None) -> List[str]:
        items: List[str] = []
        for record in self.repository.list_resources("workflow"):
            payload = record.get("payload") or {}
            definition = payload.get("definition") or {}
            if str(definition.get("cohort_id") or payload.get("cohort_id") or "") != cohort_id:
                continue
            workflow_status = str(payload.get("status") or record.get("status") or "").lower()
            if statuses and workflow_status not in statuses:
                continue
            workflow_id = str(payload.get("workflow_id") or record.get("resource_id") or "")
            if workflow_id:
                items.append(workflow_id)
        return sorted(items)

    def _build_activation_preflight(
        self,
        members: List[Dict[str, Any]],
        refresh_success: bool,
        snapshot_id: str | None,
        *,
        refresh_error: str | None = None,
    ) -> Dict[str, Any]:
        total = len(members)
        resolved = sum(1 for item in members if item.get("canonical_user_id"))
        checks = {
            "non_empty": total > 0,
            "canonical_user_id_coverage": 100.0 if total and resolved == total else (round((resolved / total) * 100.0, 2) if total else 0.0),
            "refresh_success": bool(refresh_success),
            "snapshot_ready": bool(snapshot_id),
        }
        reasons = []
        if not checks["non_empty"]:
            reasons.append("empty_cohort")
        if checks["canonical_user_id_coverage"] < 100.0:
            reasons.append("canonical_user_id_incomplete")
        if not checks["refresh_success"]:
            reasons.append(refresh_error or "refresh_not_successful")
        if not checks["snapshot_ready"]:
            reasons.append("snapshot_missing")
        return {
            "eligible": not reasons,
            "checks": checks,
            "reasons": reasons,
            "snapshot_id": snapshot_id,
        }

    @staticmethod
    def _member_identity(item: Dict[str, Any]) -> str | None:
        for field in ("canonical_user_id", "user_id", "player_id"):
            value = item.get(field)
            if value is not None and str(value).strip():
                return str(value)
        return None

    def _normalize_member(self, row: Dict[str, Any]) -> Dict[str, Any]:
        member = dict(row)
        canonical = member.get("canonical_user_id") or member.get("user_id") or member.get("player_id")
        if canonical is not None:
            member["canonical_user_id"] = str(canonical)
        return member

    def _materialize_members(self, cohort_type: str, definition: Dict[str, Any]) -> List[Dict[str, Any]]:
        resolved_type = str(cohort_type or "rule").lower()
        if resolved_type == "list":
            items = definition.get("members") or definition.get("member_ids") or []
            members = []
            for item in items:
                if isinstance(item, dict):
                    members.append(self._normalize_member(item))
                else:
                    members.append({"canonical_user_id": str(item)})
            return members
        if resolved_type == "sql":
            sql = str(definition.get("sql") or "").strip()
            result = self.bigquery_service.run_readonly_query(sql, limit=max(1000, int(definition.get("limit") or 1000)))
            return [self._normalize_member(row) for row in result.get("rows") or [] if self._normalize_member(row).get("canonical_user_id")]
        return self._materialize_rule(definition)

    def _materialize_rule(self, definition: Dict[str, Any]) -> List[Dict[str, Any]]:
        source_alias = str(definition.get("source_alias") or "prediction_results")
        rows = self.bigquery_service.get_rows_for_alias(source_alias)
        conditions = list(definition.get("conditions") or [])
        logic = str(definition.get("logic") or "AND").upper()
        if not conditions:
            return [self._normalize_member(row) for row in rows if self._normalize_member(row).get("canonical_user_id")]

        if any(str(cond.get("type") or "").lower() == "event_count" for cond in conditions):
            rows = self._build_event_count_rows(rows)

        filtered = []
        for row in rows:
            normalized = self._normalize_member(row)
            if not normalized.get("canonical_user_id"):
                continue
            outcomes = [self._matches_condition(normalized, condition) for condition in conditions]
            passed = all(outcomes) if logic != "OR" else any(outcomes)
            if passed:
                filtered.append(normalized)
        return filtered

    def _build_event_count_rows(self, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        now = datetime.now(timezone.utc)
        aggregates: Dict[str, Dict[str, Any]] = {}
        for row in rows:
            member = self._normalize_member(row)
            canonical = member.get("canonical_user_id")
            if not canonical:
                continue
            aggregate = aggregates.setdefault(str(canonical), {"canonical_user_id": str(canonical)})
            event_type = str(member.get("event_type") or "")
            event_time = member.get("event_time")
            try:
                parsed = datetime.fromisoformat(str(event_time)).astimezone(timezone.utc)
            except Exception:
                parsed = now
            age_days = max(0, int((now - parsed).total_seconds() // 86400))
            for window_days in (7, 14, 30):
                if age_days <= window_days:
                    key = f"{event_type}_{window_days}d_count"
                    aggregate[key] = int(aggregate.get(key, 0) or 0) + 1
        return list(aggregates.values())

    def _lookup_value(self, row: Dict[str, Any], field: str) -> Any:
        if field in row:
            return row.get(field)
        for container_name in ("event_properties", "user_properties"):
            container = row.get(container_name)
            if isinstance(container, dict) and field in container:
                return container.get(field)
        return None

    def _matches_condition(self, row: Dict[str, Any], condition: Dict[str, Any]) -> bool:
        cond_type = str(condition.get("type") or "").lower()
        if cond_type == "event_count":
            event_name = str(condition.get("event") or "").strip()
            window_days = int(condition.get("window_days") or 14)
            field = f"{event_name}_{window_days}d_count"
        else:
            field = str(condition.get("field") or "")
        actual = self._lookup_value(row, field)
        op = str(condition.get("op") or "=").lower()
        expected = condition.get("value")

        if op in {"=", "=="}:
            return actual == expected
        if op == "!=":
            return actual != expected
        if op == "in":
            return actual in (expected or [])
        if op == "not in":
            return actual not in (expected or [])
        if op == "contains":
            return str(expected) in str(actual or "")
        if op in {">", ">=", "<", "<="}:
            try:
                actual_value = float(actual)
                expected_value = float(expected)
            except (TypeError, ValueError):
                return False
            if op == ">":
                return actual_value > expected_value
            if op == ">=":
                return actual_value >= expected_value
            if op == "<":
                return actual_value < expected_value
            return actual_value <= expected_value
        if op == "between":
            try:
                start, end = expected
                actual_value = float(actual)
                return float(start) <= actual_value <= float(end)
            except Exception:
                return False
        if op == "within_last":
            try:
                days = int(expected)
                if actual is None:
                    return False
                parsed = datetime.fromisoformat(str(actual).replace("Z", "+00:00"))
                if parsed.tzinfo is None:
                    parsed = parsed.replace(tzinfo=timezone.utc)
                return parsed >= datetime.now(timezone.utc) - timedelta(days=days)
            except Exception:
                return False
        return False
