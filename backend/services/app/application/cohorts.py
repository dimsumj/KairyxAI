from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List

from bigquery_service import BigQueryService, get_shared_bigquery_service


class CohortService:
    def __init__(self, repository, bigquery_service: BigQueryService | None = None):
        self.repository = repository
        self.bigquery_service = bigquery_service or get_shared_bigquery_service()

    def create_cohort(
        self,
        *,
        name: str,
        cohort_type: str,
        definition: Dict[str, Any],
        refresh_mode: str = "manual",
        owner: str = "system",
        tags: List[str] | None = None,
        activate: bool = False,
    ) -> Dict[str, Any]:
        if any(item.get("name") == name for item in self.repository.list_resources("cohort")):
            raise ValueError(f"Cohort '{name}' already exists.")
        cohort_id = f"cohort_{uuid.uuid4().hex[:20]}"
        payload = {
            "cohort_id": cohort_id,
            "name": name,
            "type": str(cohort_type).lower(),
            "status": "draft",
            "refresh_mode": refresh_mode,
            "owner": owner,
            "version": 1,
            "definition": definition,
            "tags": list(tags or []),
            "member_count": 0,
            "preview_members": [],
            "latest_members": [],
            "delta": {"added": 0, "removed": 0, "unchanged": 0},
            "last_refreshed_at": None,
        }
        record = self.repository.upsert_resource("cohort", cohort_id, status="draft", name=name, payload=payload)
        self.repository.create_resource_version("cohort", cohort_id, version=1, payload=payload)
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

    def refresh_cohort(self, cohort_id: str, *, force: bool = False) -> Dict[str, Any]:
        record = self.repository.get_resource("cohort", cohort_id)
        if record is None:
            raise KeyError(cohort_id)
        payload = dict(record.get("payload") or {})
        previous_members = list(payload.get("latest_members") or [])
        members = self._materialize_members(payload.get("type") or "rule", payload.get("definition") or {})
        previous_ids = {self._member_identity(item) for item in previous_members if self._member_identity(item)}
        current_ids = {self._member_identity(item) for item in members if self._member_identity(item)}
        payload["latest_members"] = members
        payload["preview_members"] = members[:20]
        payload["member_count"] = len(members)
        payload["last_refreshed_at"] = datetime.utcnow().isoformat()
        payload["delta"] = {
            "added": len(current_ids - previous_ids),
            "removed": len(previous_ids - current_ids),
            "unchanged": len(current_ids & previous_ids),
        }
        if members and str(record.get("status") or "") not in {"active", "paused"}:
            payload["status"] = "ready"
        elif not members and str(record.get("status") or "") == "active":
            payload["status"] = "draft"
        else:
            payload["status"] = str(record.get("status") or payload.get("status") or "draft")
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
            payload={"member_count": len(members), "delta": payload["delta"], "force": force},
        )
        return self._to_response(saved)

    def activate_cohort(self, cohort_id: str, *, force: bool = False) -> Dict[str, Any]:
        record = self.repository.get_resource("cohort", cohort_id)
        if record is None:
            raise KeyError(cohort_id)
        payload = dict(record.get("payload") or {})
        if not payload.get("latest_members"):
            payload = self.refresh_cohort(cohort_id, force=force)
        record = self.repository.get_resource("cohort", cohort_id)
        payload = dict(record.get("payload") or {})
        members = list(payload.get("latest_members") or [])
        if not members:
            raise ValueError("Empty cohorts cannot be activated.")
        if any(not item.get("canonical_user_id") for item in members):
            raise ValueError("Every cohort member must resolve to canonical_user_id before activation.")
        payload["status"] = "active"
        saved = self.repository.upsert_resource("cohort", cohort_id, status="active", name=payload.get("name"), payload=payload)
        self.repository.record_resource_event("cohort", cohort_id, event_type="cohort_activated", payload={"member_count": len(members)})
        return self._to_response(saved)

    def pause_cohort(self, cohort_id: str) -> Dict[str, Any]:
        record = self.repository.get_resource("cohort", cohort_id)
        if record is None:
            raise KeyError(cohort_id)
        payload = dict(record.get("payload") or {})
        payload["status"] = "paused"
        saved = self.repository.upsert_resource("cohort", cohort_id, status="paused", name=payload.get("name"), payload=payload)
        self.repository.record_resource_event("cohort", cohort_id, event_type="cohort_paused", payload={"status": "paused"})
        return self._to_response(saved)

    def _to_response(self, record: Dict[str, Any]) -> Dict[str, Any]:
        payload = dict(record.get("payload") or {})
        payload.setdefault("created_at", record["created_at"])
        payload.setdefault("updated_at", record["updated_at"])
        return payload

    @staticmethod
    def _member_identity(item: Dict[str, Any]) -> str | None:
        for field in ("canonical_user_id", "user_id", "player_id"):
            value = item.get(field)
            if value is not None and str(value).strip():
                return str(value)
        return None

    def _normalize_member(self, row: Dict[str, Any]) -> Dict[str, Any]:
        member = dict(row)
        canonical = (
            member.get("canonical_user_id")
            or member.get("user_id")
            or member.get("player_id")
        )
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
