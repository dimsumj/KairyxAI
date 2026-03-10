from __future__ import annotations

import uuid
from typing import Any, Dict, List

from bigquery_service import BigQueryService, get_shared_bigquery_service


class SqlWorkspaceService:
    def __init__(self, repository, bigquery_service: BigQueryService | None = None):
        self.repository = repository
        self.bigquery_service = bigquery_service or get_shared_bigquery_service()

    def preview(self, sql: str, *, limit: int = 50, timeout_seconds: int = 30) -> Dict[str, Any]:
        return self.bigquery_service.run_readonly_query(
            sql,
            limit=limit,
            timeout_seconds=timeout_seconds,
        )

    def create_saved_query(self, name: str, sql: str, description: str = "") -> Dict[str, Any]:
        query_id = f"sql_{uuid.uuid4().hex[:20]}"
        payload = {
            "query_id": query_id,
            "name": name,
            "description": description,
            "sql": sql,
        }
        record = self.repository.upsert_resource(
            "saved_query",
            query_id,
            status="active",
            name=name,
            payload=payload,
        )
        self.repository.create_resource_version("saved_query", query_id, version=1, payload=payload)
        self.repository.record_action("saved_query_created", "saved_query", query_id, payload)
        return self._resource_to_query(record)

    def list_saved_queries(self) -> List[Dict[str, Any]]:
        return [self._resource_to_query(item) for item in self.repository.list_resources("saved_query")]

    def get_saved_query(self, query_id: str) -> Dict[str, Any] | None:
        record = self.repository.get_resource("saved_query", query_id)
        return self._resource_to_query(record) if record else None

    def run_saved_query(self, query_id: str, *, limit: int = 50, timeout_seconds: int = 30) -> Dict[str, Any]:
        record = self.get_saved_query(query_id)
        if record is None:
            raise KeyError(query_id)
        return self.preview(record["sql"], limit=limit, timeout_seconds=timeout_seconds)

    def _resource_to_query(self, record: Dict[str, Any]) -> Dict[str, Any]:
        payload = record.get("payload") or {}
        return {
            "query_id": payload.get("query_id") or record["resource_id"],
            "name": payload.get("name") or record.get("name") or "",
            "description": payload.get("description") or "",
            "sql": payload.get("sql") or "",
            "created_at": record["created_at"],
            "updated_at": record["updated_at"],
        }
