import json
import sqlite3
from pathlib import Path
from typing import Any, Dict, List

DB_PATH = Path(__file__).resolve().parent / ".kairyx_local.db"


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
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
