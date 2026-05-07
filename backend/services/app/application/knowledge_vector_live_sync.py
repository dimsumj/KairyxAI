from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Dict

from app.application.secret_refs import redact_secret_values
from secret_manager_service import SecretManagerService


PGVECTOR_STORE = "pgvector"
DEFAULT_PGVECTOR_SCHEMA = "public"
DEFAULT_PGVECTOR_TABLE = "kairyx_knowledge_vectors"
PG_DSN_PREFIXES = ("postgresql://", "postgres://", "postgresql+psycopg://")
MAX_SYNC_ERROR_CHARS = 180


@dataclass(frozen=True)
class PgvectorTarget:
    dsn: str
    schema: str
    table: str

    @property
    def table_ref(self) -> str:
        return f"{self.schema}.{self.table}"


def sync_knowledge_vector_record(
    config: Dict[str, Any],
    *,
    operation: str,
    vector_record: Dict[str, Any],
) -> Dict[str, Any]:
    """Best-effort provider live sync.

    The control-plane vector record remains the source of truth for local retrieval.
    Provider failures are represented as sanitized adapter receipt patches.
    """

    normalized_store = str(config.get("vector_store") or "").strip().lower()
    if normalized_store != PGVECTOR_STORE:
        return {}
    try:
        target = _resolve_pgvector_target(config)
        if target is None:
            return {}
        if operation == "archive":
            rows_affected = _archive_pgvector_record(target, vector_record)
        else:
            rows_affected = _upsert_pgvector_record(target, config, vector_record)
        return _success_receipt(config, target, operation=operation, vector_record=vector_record, rows_affected=rows_affected)
    except Exception as exc:
        return _failure_receipt(config, operation=operation, exc=exc)


def _resolve_pgvector_target(config: Dict[str, Any]) -> PgvectorTarget | None:
    secret_ref = str(config.get("secret_ref") or "").strip()
    if not secret_ref:
        return None
    try:
        resolved = SecretManagerService().resolve_secret(secret_ref)
    except Exception as exc:
        raise RuntimeError(_sanitize_exception(exc)) from exc
    if not resolved:
        raise RuntimeError("vector secret reference did not resolve to a value")
    if not _looks_like_pgvector_target(resolved):
        return None
    target_payload = _parse_target_payload(str(resolved))
    dsn = _normalize_dsn(str(target_payload.get("dsn") or ""))
    if not dsn:
        raise RuntimeError("pgvector sync secret must resolve to a PostgreSQL DSN")
    return PgvectorTarget(
        dsn=dsn,
        schema=_safe_identifier(target_payload.get("schema") or DEFAULT_PGVECTOR_SCHEMA, fallback=DEFAULT_PGVECTOR_SCHEMA),
        table=_safe_identifier(target_payload.get("table") or DEFAULT_PGVECTOR_TABLE, fallback=DEFAULT_PGVECTOR_TABLE),
    )


def _looks_like_pgvector_target(value: Any) -> bool:
    raw = str(value or "").strip()
    if raw.startswith(PG_DSN_PREFIXES):
        return True
    if not raw.startswith("{"):
        return False
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        return False
    if not isinstance(parsed, dict):
        return False
    dsn = str(parsed.get("dsn") or parsed.get("database_url") or parsed.get("connection_url") or "").strip()
    return dsn.startswith(PG_DSN_PREFIXES)


def _parse_target_payload(value: str) -> Dict[str, Any]:
    raw = str(value or "").strip()
    if raw.startswith("{"):
        parsed = json.loads(raw)
        if not isinstance(parsed, dict):
            raise RuntimeError("pgvector sync secret JSON must be an object")
        return {
            "dsn": parsed.get("dsn") or parsed.get("database_url") or parsed.get("connection_url") or "",
            "schema": parsed.get("schema") or DEFAULT_PGVECTOR_SCHEMA,
            "table": parsed.get("table") or DEFAULT_PGVECTOR_TABLE,
        }
    return {"dsn": raw, "schema": DEFAULT_PGVECTOR_SCHEMA, "table": DEFAULT_PGVECTOR_TABLE}


def _normalize_dsn(value: str) -> str:
    dsn = str(value or "").strip()
    if dsn.startswith("postgresql+psycopg://"):
        return "postgresql://" + dsn[len("postgresql+psycopg://") :]
    return dsn


def _safe_identifier(value: Any, *, fallback: str) -> str:
    normalized = re.sub(r"[^a-zA-Z0-9_]+", "_", str(value or "").strip()).strip("_")
    if not normalized or not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]{0,62}$", normalized):
        return fallback
    return normalized


def _upsert_pgvector_record(target: PgvectorTarget, config: Dict[str, Any], vector_record: Dict[str, Any]) -> int:
    from psycopg import connect, sql

    dimensions = int(vector_record.get("dimensions") or config.get("dimensions") or 1024)
    vector = list(vector_record.get("vector") or [])
    if not vector:
        raise RuntimeError("vector record has no embedding vector")
    vector_literal = "[" + ",".join(f"{float(item):.6f}" for item in vector) + "]"
    metadata = _provider_metadata(config, vector_record)
    with connect(target.dsn, connect_timeout=5) as connection:
        with connection.cursor() as cursor:
            cursor.execute("CREATE EXTENSION IF NOT EXISTS vector")
            cursor.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(target.schema)))
            cursor.execute(
                sql.SQL(
                    """
                    CREATE TABLE IF NOT EXISTS {} (
                        vector_record_id TEXT PRIMARY KEY,
                        index_id TEXT NOT NULL,
                        vector_namespace TEXT NOT NULL,
                        chunk_id TEXT NOT NULL,
                        document_id TEXT NOT NULL,
                        source_id TEXT NOT NULL,
                        source_type TEXT NOT NULL,
                        source_title TEXT NOT NULL,
                        tags JSONB NOT NULL DEFAULT '[]'::jsonb,
                        vector_hash TEXT NOT NULL,
                        embedding vector({}) NOT NULL,
                        metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
                        status TEXT NOT NULL DEFAULT 'active',
                        materialized_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                        archived_at TIMESTAMPTZ NULL
                    )
                    """
                ).format(
                    sql.Identifier(target.schema, target.table),
                    sql.Literal(dimensions),
                )
            )
            cursor.execute(
                sql.SQL(
                    """
                    INSERT INTO {} (
                        vector_record_id,
                        index_id,
                        vector_namespace,
                        chunk_id,
                        document_id,
                        source_id,
                        source_type,
                        source_title,
                        tags,
                        vector_hash,
                        embedding,
                        metadata,
                        status,
                        materialized_at,
                        archived_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s::vector, %s::jsonb, 'active', now(), NULL)
                    ON CONFLICT (vector_record_id) DO UPDATE SET
                        index_id = EXCLUDED.index_id,
                        vector_namespace = EXCLUDED.vector_namespace,
                        chunk_id = EXCLUDED.chunk_id,
                        document_id = EXCLUDED.document_id,
                        source_id = EXCLUDED.source_id,
                        source_type = EXCLUDED.source_type,
                        source_title = EXCLUDED.source_title,
                        tags = EXCLUDED.tags,
                        vector_hash = EXCLUDED.vector_hash,
                        embedding = EXCLUDED.embedding,
                        metadata = EXCLUDED.metadata,
                        status = 'active',
                        materialized_at = now(),
                        archived_at = NULL
                    """
                ).format(sql.Identifier(target.schema, target.table)),
                (
                    str(vector_record.get("vector_record_id") or ""),
                    str(vector_record.get("index_id") or config.get("index_id") or ""),
                    str(config.get("vector_namespace") or "default"),
                    str(vector_record.get("chunk_id") or ""),
                    str(vector_record.get("document_id") or ""),
                    str(vector_record.get("source_id") or ""),
                    str(vector_record.get("source_type") or ""),
                    str(vector_record.get("source_title") or ""),
                    json.dumps(list(vector_record.get("tags") or []), separators=(",", ":")),
                    str(vector_record.get("vector_hash") or ""),
                    vector_literal,
                    json.dumps(metadata, separators=(",", ":"), sort_keys=True),
                ),
            )
            return int(cursor.rowcount or 0)


def _archive_pgvector_record(target: PgvectorTarget, vector_record: Dict[str, Any]) -> int:
    from psycopg import connect, sql

    vector_record_id = str(vector_record.get("vector_record_id") or "").strip()
    if not vector_record_id:
        raise RuntimeError("vector record id is required for archive sync")
    with connect(target.dsn, connect_timeout=5) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                sql.SQL(
                    """
                    UPDATE {}
                    SET status = 'archived',
                        archived_at = now(),
                        metadata = COALESCE(metadata, '{}'::jsonb) || %s::jsonb
                    WHERE vector_record_id = %s
                    """
                ).format(sql.Identifier(target.schema, target.table)),
                (
                    json.dumps({"archived_by": "kairyx_control_plane"}, separators=(",", ":")),
                    vector_record_id,
                ),
            )
            return int(cursor.rowcount or 0)


def _provider_metadata(config: Dict[str, Any], vector_record: Dict[str, Any]) -> Dict[str, Any]:
    embedding = dict(vector_record.get("embedding") or {})
    metadata = {
        "embedding_provider": config.get("embedding_provider"),
        "embedding_model": config.get("embedding_model"),
        "vector_store": config.get("vector_store"),
        "vector_namespace": config.get("vector_namespace"),
        "chunk_id": vector_record.get("chunk_id"),
        "document_id": vector_record.get("document_id"),
        "source_id": vector_record.get("source_id"),
        "source_name": vector_record.get("source_name"),
        "source_type": vector_record.get("source_type"),
        "source_title": vector_record.get("source_title"),
        "vector_ref": embedding.get("vector_ref"),
        "synced_by": "kairyx_control_plane",
    }
    return redact_secret_values({key: value for key, value in metadata.items() if value not in {None, ""}})


def _success_receipt(
    config: Dict[str, Any],
    target: PgvectorTarget,
    *,
    operation: str,
    vector_record: Dict[str, Any],
    rows_affected: int,
) -> Dict[str, Any]:
    vector_record_id = str(vector_record.get("vector_record_id") or "")
    sync_status = "live_archive_synced" if operation == "archive" else "live_synced"
    readiness_status = "archived" if operation == "archive" else "live_synced"
    return {
        "operation": operation,
        "sync_status": sync_status,
        "readiness_status": readiness_status,
        "external_vector_ref": f"pgvector://{target.table_ref}/{vector_record_id}",
        "secret_ref_configured": bool(config.get("secret_ref_configured")),
        "capabilities": ["upsert", "query", "archive", "export_shadow", "secret_ref_required", "live_sync"],
        "warnings": [],
        "live_sync": {
            "provider": PGVECTOR_STORE,
            "status": "synced",
            "operation": operation,
            "table": target.table_ref,
            "rows_affected": rows_affected,
            "recorded_at": _utcnow_iso(),
        },
    }


def _failure_receipt(
    config: Dict[str, Any],
    *,
    operation: str,
    exc: Exception,
) -> Dict[str, Any]:
    return {
        "operation": operation,
        "sync_status": "live_sync_failed",
        "readiness_status": "ready_for_live_sync",
        "secret_ref_configured": bool(config.get("secret_ref_configured")),
        "capabilities": ["upsert", "query", "archive", "export_shadow", "secret_ref_required", "live_sync"],
        "warnings": ["pgvector live sync failed; local control-plane retrieval remains available."],
        "live_sync": {
            "provider": PGVECTOR_STORE,
            "status": "failed",
            "operation": operation,
            "error": _sanitize_exception(exc),
            "recorded_at": _utcnow_iso(),
        },
    }


def _sanitize_exception(exc: Exception) -> str:
    message = re.sub(r"\s+", " ", str(exc or "")).strip() or exc.__class__.__name__
    message = re.sub(r"(postgres(?:ql)?://)[^@\s]+@", r"\1***@", message, flags=re.IGNORECASE)
    message = re.sub(r"password=[^\s]+", "password=***", message, flags=re.IGNORECASE)
    if len(message) > MAX_SYNC_ERROR_CHARS:
        return message[: MAX_SYNC_ERROR_CHARS - 1].rstrip() + "..."
    return message


def _utcnow_iso() -> str:
    return datetime.now(UTC).replace(tzinfo=None).isoformat()
