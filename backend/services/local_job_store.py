import json
import os
import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Optional

from runtime_paths import default_local_job_store_path, resolve_sqlite_file_path


DB_PATH = default_local_job_store_path()


def _db_path() -> Path:
    override = os.getenv("KAIRYX_LOCAL_DB_PATH")
    return resolve_sqlite_file_path(override or DB_PATH, ensure_parent=True)


def _conn() -> sqlite3.Connection:
    db_path = _db_path()
    try:
        conn = sqlite3.connect(db_path)
    except sqlite3.OperationalError as exc:
        raise sqlite3.OperationalError(f"unable to open local SQLite store at {db_path}") from exc
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    return conn


def init_db() -> None:
    with _conn() as c:
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS import_jobs (
                name TEXT PRIMARY KEY,
                payload TEXT NOT NULL,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS prediction_jobs (
                id TEXT PRIMARY KEY,
                payload TEXT NOT NULL,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS identity_links (
                source TEXT NOT NULL,
                source_user_id TEXT NOT NULL,
                canonical_user_id TEXT NOT NULL,
                confidence REAL DEFAULT 1.0,
                method TEXT DEFAULT 'deterministic',
                first_seen_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                last_seen_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (source, source_user_id)
            )
            """
        )
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS field_mappings (
                connector_name TEXT PRIMARY KEY,
                payload TEXT NOT NULL,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS ingestion_checkpoints (
                job_id TEXT NOT NULL,
                source TEXT NOT NULL,
                shard_index INTEGER NOT NULL,
                payload TEXT NOT NULL,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (job_id, source, shard_index)
            )
            """
        )


def save_import_jobs(jobs: List[Dict[str, Any]]) -> None:
    with _conn() as c:
        c.execute("DELETE FROM import_jobs")
        for j in jobs:
            c.execute(
                "INSERT OR REPLACE INTO import_jobs(name, payload) VALUES (?, ?)",
                (j.get("name"), json.dumps(j)),
            )


def load_import_jobs() -> List[Dict[str, Any]]:
    with _conn() as c:
        rows = c.execute("SELECT payload FROM import_jobs ORDER BY updated_at DESC").fetchall()
    return [json.loads(r[0]) for r in rows]


def save_prediction_jobs(jobs: List[Dict[str, Any]]) -> None:
    with _conn() as c:
        c.execute("DELETE FROM prediction_jobs")
        for j in jobs:
            c.execute(
                "INSERT OR REPLACE INTO prediction_jobs(id, payload) VALUES (?, ?)",
                (j.get("id"), json.dumps(j)),
            )


def load_prediction_jobs() -> List[Dict[str, Any]]:
    with _conn() as c:
        rows = c.execute("SELECT payload FROM prediction_jobs ORDER BY updated_at DESC").fetchall()
    return [json.loads(r[0]) for r in rows]


def resolve_or_create_canonical_user_id(source: str, source_user_id: str) -> str:
    source_user_id = str(source_user_id or "unknown_user")
    init_db()
    with _conn() as c:
        row = c.execute(
            "SELECT canonical_user_id FROM identity_links WHERE source=? AND source_user_id=?",
            (source, source_user_id),
        ).fetchone()
        if row:
            c.execute(
                "UPDATE identity_links SET last_seen_at=CURRENT_TIMESTAMP WHERE source=? AND source_user_id=?",
                (source, source_user_id),
            )
            return row[0]

        # deterministic cross-source link by exact source_user_id match
        existing = c.execute(
            "SELECT canonical_user_id FROM identity_links WHERE source_user_id=? LIMIT 1",
            (source_user_id,),
        ).fetchone()
        canonical = existing[0] if existing else f"uid:{source_user_id}"

        c.execute(
            """
            INSERT OR REPLACE INTO identity_links(source, source_user_id, canonical_user_id, confidence, method)
            VALUES (?, ?, ?, ?, ?)
            """,
            (source, source_user_id, canonical, 1.0, "deterministic_exact_id"),
        )
        return canonical


def list_identity_links(limit: int = 200) -> List[Dict[str, Any]]:
    with _conn() as c:
        rows = c.execute(
            """
            SELECT source, source_user_id, canonical_user_id, confidence, method, first_seen_at, last_seen_at
            FROM identity_links
            ORDER BY last_seen_at DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    return [
        {
            "source": r[0],
            "source_user_id": r[1],
            "canonical_user_id": r[2],
            "confidence": r[3],
            "method": r[4],
            "first_seen_at": r[5],
            "last_seen_at": r[6],
        }
        for r in rows
    ]


def save_field_mapping(connector_name: str, payload: Dict[str, Any]) -> None:
    with _conn() as c:
        c.execute(
            "INSERT OR REPLACE INTO field_mappings(connector_name, payload) VALUES (?, ?)",
            (connector_name, json.dumps(payload)),
        )


def get_field_mapping(connector_name: str) -> Dict[str, Any]:
    with _conn() as c:
        row = c.execute("SELECT payload FROM field_mappings WHERE connector_name=?", (connector_name,)).fetchone()
    if not row:
        return {}
    try:
        return json.loads(row[0])
    except json.JSONDecodeError:
        return {}


def list_field_mappings() -> Dict[str, Any]:
    with _conn() as c:
        rows = c.execute("SELECT connector_name, payload FROM field_mappings").fetchall()
    out: Dict[str, Any] = {}
    for name, payload in rows:
        try:
            out[name] = json.loads(payload)
        except json.JSONDecodeError:
            out[name] = {}
    return out


def save_ingestion_checkpoint(job_id: str, source: str, shard_index: int, payload: Dict[str, Any]) -> None:
    init_db()
    with _conn() as c:
        c.execute(
            """
            INSERT OR REPLACE INTO ingestion_checkpoints(job_id, source, shard_index, payload, updated_at)
            VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
            """,
            (job_id, source, int(shard_index), json.dumps(payload)),
        )


def list_ingestion_checkpoints(job_id: Optional[str] = None) -> List[Dict[str, Any]]:
    init_db()
    query = """
        SELECT job_id, source, shard_index, payload
        FROM ingestion_checkpoints
    """
    params: tuple[Any, ...] = ()
    if job_id:
        query += " WHERE job_id=?"
        params = (job_id,)
    query += " ORDER BY job_id, source, shard_index"

    with _conn() as c:
        rows = c.execute(query, params).fetchall()

    checkpoints: List[Dict[str, Any]] = []
    for row_job_id, source, shard_index, payload in rows:
        try:
            checkpoint = json.loads(payload)
        except json.JSONDecodeError:
            checkpoint = {}
        checkpoint.setdefault("job_id", row_job_id)
        checkpoint.setdefault("source", source)
        checkpoint.setdefault("shard_index", shard_index)
        checkpoints.append(checkpoint)
    return checkpoints


def delete_ingestion_checkpoints(job_id: str) -> None:
    init_db()
    with _conn() as c:
        c.execute("DELETE FROM ingestion_checkpoints WHERE job_id=?", (job_id,))
