from __future__ import annotations

import json
import os
import re
from datetime import datetime
from typing import Any, Dict, List

from runtime_paths import normalize_env_text


_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_UNSAFE_FILTER_PATTERN = re.compile(r"(;|--|/\*|\*/|\b(insert|update|delete|merge|drop|alter|create)\b)", re.IGNORECASE)


class BigQueryConnector:
    connector_type = "bigquery"

    def __init__(self, config: Dict[str, Any]):
        self.config = dict(config or {})
        self.project_id = str(self.config.get("gcp_project_id") or self.config.get("project_id") or "").strip()
        self.dataset_id = str(self.config.get("dataset_id") or "").strip()
        self.location = str(self.config.get("location") or "").strip()
        self.mock_tables = dict(self.config.get("mock_tables") or {})
        self._client = None
        self._bigquery = None

    def health_check(self) -> Dict[str, Any]:
        if not self.project_id or not self.dataset_id:
            return {
                "ok": False,
                "connector": self.connector_type,
                "message": "missing project_id or dataset_id",
            }
        if self._is_mock_mode():
            return {
                "ok": True,
                "connector": self.connector_type,
                "message": f"configured dataset {self.project_id}.{self.dataset_id}",
            }
        try:
            client = self._get_client()
            client.get_dataset(f"{self.project_id}.{self.dataset_id}")
        except Exception as exc:
            return {"ok": False, "connector": self.connector_type, "message": str(exc)}
        return {
            "ok": True,
            "connector": self.connector_type,
            "message": f"configured dataset {self.project_id}.{self.dataset_id}",
        }

    def list_tables(self) -> List[Dict[str, Any]]:
        if not self.project_id or not self.dataset_id:
            raise ValueError("BigQuery connector requires project_id and dataset_id.")
        if self._is_mock_mode():
            items = []
            for name, value in sorted(self.mock_tables.items()):
                if isinstance(value, dict):
                    rows = list(value.get("rows") or [])
                    table_type = str(value.get("table_type") or "table").lower()
                else:
                    rows = list(value or [])
                    table_type = "table"
                items.append(
                    {
                        "table_name": str(name),
                        "table_type": table_type,
                        "row_count": len(rows),
                    }
                )
            return items

        client = self._get_client()
        dataset_ref = f"{self.project_id}.{self.dataset_id}"
        items = []
        try:
            for table in client.list_tables(dataset_ref):
                items.append(
                    {
                        "table_name": str(table.table_id),
                        "table_type": str(getattr(table, "table_type", "table") or "table").lower(),
                        "row_count": None,
                    }
                )
        except Exception as exc:
            raise ValueError(f"Unable to list BigQuery tables for dataset {dataset_ref}: {exc}") from exc
        return items

    def fetch_table_rows_page(
        self,
        table_name: str,
        *,
        cursor: str | None = None,
        page_size: int | None = None,
        selected_columns: List[str] | None = None,
        where_sql: str | None = None,
        timestamp_column: str | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> Dict[str, Any]:
        resolved_table = self._validate_identifier(table_name, field_name="table_name")
        resolved_columns = [self._validate_identifier(item, field_name="selected_columns") for item in list(selected_columns or [])]
        resolved_timestamp_column = (
            self._validate_identifier(timestamp_column, field_name="timestamp_column")
            if str(timestamp_column or "").strip()
            else None
        )
        resolved_where_sql = self._validate_where_sql(where_sql)
        resolved_page_size = max(1, int(page_size or 1000))
        page_number = max(0, int(cursor or "0"))

        if self._is_mock_mode():
            table_spec = self.mock_tables.get(resolved_table)
            if table_spec is None:
                raise ValueError(f"BigQuery table '{resolved_table}' was not found.")
            if isinstance(table_spec, dict):
                rows = [dict(item) for item in list(table_spec.get("rows") or [])]
            else:
                rows = [dict(item) for item in list(table_spec or [])]
            rows = self._filter_mock_rows(
                rows,
                resolved_columns=resolved_columns,
                timestamp_column=resolved_timestamp_column,
                start_date=start_date,
                end_date=end_date,
            )
            total = len(rows)
            offset = page_number * resolved_page_size
            page_rows = rows[offset: offset + resolved_page_size]
            next_cursor = str(page_number + 1) if (offset + resolved_page_size) < total else None
            return {"rows": page_rows, "total": total, "next_cursor": next_cursor, "has_more": next_cursor is not None}

        client = self._get_client()
        select_clause = ", ".join(resolved_columns) if resolved_columns else "*"
        table_ref = f"`{self.project_id}.{self.dataset_id}.{resolved_table}`"
        where_clauses: List[str] = []
        query_parameters: List[Any] = []
        if resolved_where_sql:
            where_clauses.append(f"({resolved_where_sql})")
        if resolved_timestamp_column and start_date and end_date:
            where_clauses.append(f"DATE({resolved_timestamp_column}) BETWEEN @start_date AND @end_date")
            query_parameters.extend(
                [
                    self._bigquery.ScalarQueryParameter("start_date", "DATE", str(start_date)),
                    self._bigquery.ScalarQueryParameter("end_date", "DATE", str(end_date)),
                ]
            )
        where_clause = f" WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
        count_query = f"SELECT COUNT(*) AS total FROM {table_ref}{where_clause}"
        row_query = f"SELECT {select_clause} FROM {table_ref}{where_clause} LIMIT @limit OFFSET @offset"
        query_parameters.extend(
            [
                self._bigquery.ScalarQueryParameter("limit", "INT64", resolved_page_size),
                self._bigquery.ScalarQueryParameter("offset", "INT64", page_number * resolved_page_size),
            ]
        )
        job_config = self._bigquery.QueryJobConfig(query_parameters=query_parameters)
        count_job_config = self._bigquery.QueryJobConfig(query_parameters=query_parameters[: len(query_parameters) - 2])
        total = int(next(iter(client.query(count_query, job_config=count_job_config).result()))["total"])
        rows = [dict(item.items()) for item in client.query(row_query, job_config=job_config).result()]
        next_cursor = str(page_number + 1) if ((page_number + 1) * resolved_page_size) < total else None
        return {"rows": rows, "total": total, "next_cursor": next_cursor, "has_more": next_cursor is not None}

    def _get_client(self):
        if self._client is not None:
            return self._client
        if self._is_mock_mode():
            return None
        try:
            from google.cloud import bigquery
            from google.oauth2 import service_account
        except ImportError as exc:
            raise RuntimeError("google-cloud-bigquery is required for BigQuery connector support.") from exc
        self._bigquery = bigquery
        credentials = None
        raw_service_account = self.config.get("service_account_json") or self.config.get("service_account_info_json")
        if raw_service_account:
            credentials = service_account.Credentials.from_service_account_info(
                self.parse_service_account_info(raw_service_account)
            )
        self._client = bigquery.Client(project=self.project_id, location=self.location or None, credentials=credentials)
        return self._client

    @staticmethod
    def parse_service_account_info(raw_value: Any) -> Dict[str, Any]:
        if isinstance(raw_value, dict):
            return dict(raw_value)
        text = str(raw_value or "").strip()
        if not text:
            raise ValueError("BigQuery service account JSON is required.")
        try:
            payload = json.loads(text)
        except json.JSONDecodeError as exc:
            raise ValueError("BigQuery service account JSON must be valid JSON.") from exc
        if not isinstance(payload, dict):
            raise ValueError("BigQuery service account JSON must decode to an object.")
        return payload

    @staticmethod
    def _validate_identifier(value: str | None, *, field_name: str) -> str:
        text = str(value or "").strip()
        if not text:
            raise ValueError(f"{field_name} is required.")
        if not _IDENTIFIER_PATTERN.fullmatch(text):
            raise ValueError(f"{field_name} contains unsupported characters.")
        return text

    @staticmethod
    def _validate_where_sql(value: str | None) -> str:
        text = str(value or "").strip()
        if not text:
            return ""
        if _UNSAFE_FILTER_PATTERN.search(text):
            raise ValueError("where_sql contains unsupported SQL.")
        return text

    @staticmethod
    def _filter_mock_rows(
        rows: List[Dict[str, Any]],
        *,
        resolved_columns: List[str],
        timestamp_column: str | None,
        start_date: str | None,
        end_date: str | None,
    ) -> List[Dict[str, Any]]:
        filtered = rows
        if timestamp_column and start_date and end_date:
            start_text = str(start_date)
            end_text = str(end_date)

            def _in_window(item: Dict[str, Any]) -> bool:
                raw_value = item.get(timestamp_column)
                if raw_value in (None, ""):
                    return False
                try:
                    parsed = datetime.fromisoformat(str(raw_value).replace("Z", "+00:00"))
                except Exception:
                    return False
                day = parsed.date().isoformat()
                return start_text <= day <= end_text

            filtered = [item for item in filtered if _in_window(item)]
        if not resolved_columns:
            return filtered
        projected = []
        for item in filtered:
            projected.append({field: item.get(field) for field in resolved_columns})
        return projected

    @staticmethod
    def _is_mock_mode() -> bool:
        return normalize_env_text(os.getenv("DATA_BACKEND_MODE", "mock")).lower() == "mock"
