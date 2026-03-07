from __future__ import annotations

import os
from pathlib import Path

from sqlalchemy.engine import make_url


SERVICES_DIR = Path(__file__).resolve().parent


def services_dir() -> Path:
    return SERVICES_DIR


def _resolve_filesystem_path(raw_path: str | os.PathLike[str]) -> Path:
    expanded = os.path.expandvars(os.path.expanduser(os.fspath(raw_path)))
    path = Path(expanded)
    if not path.is_absolute():
        path = SERVICES_DIR / path
    return path.resolve()


def resolve_sqlite_file_path(raw_value: str | os.PathLike[str], *, ensure_parent: bool = False) -> Path:
    text = os.fspath(raw_value).strip()
    if not text:
        raise ValueError("SQLite path value cannot be empty.")

    if text.startswith("sqlite:"):
        url = make_url(text)
        database = url.database
        if not database or database == ":memory:":
            raise ValueError("SQLite path value must reference a file-backed database.")
        path = _resolve_filesystem_path(database)
    else:
        path = _resolve_filesystem_path(text)

    if ensure_parent:
        path.parent.mkdir(parents=True, exist_ok=True)
    return path


def default_control_plane_database_url() -> str:
    default_path = resolve_sqlite_file_path(SERVICES_DIR / ".kairyx_control_plane.db", ensure_parent=True)
    return f"sqlite:///{default_path}"


def default_local_job_store_path() -> Path:
    return resolve_sqlite_file_path(SERVICES_DIR / ".kairyx_local.db", ensure_parent=True)


def normalize_sqlite_database_url(raw_url: str) -> str:
    if not raw_url:
        return raw_url

    url_text = raw_url.strip()
    if not url_text.startswith("sqlite"):
        return url_text

    url = make_url(url_text)
    database = url.database
    if not database or database == ":memory:":
        return url_text

    path = resolve_sqlite_file_path(database, ensure_parent=True)
    return url.set(database=str(path)).render_as_string(hide_password=False)
