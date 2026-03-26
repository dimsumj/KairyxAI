from __future__ import annotations

import os
import tempfile
from pathlib import Path

from sqlalchemy.engine import make_url


SERVICES_DIR = Path(__file__).resolve().parent


def normalize_env_text(raw_value: str | os.PathLike[str] | None) -> str:
    text = str(raw_value or "")
    text = text.replace("\\n", "\n").replace("\\r", "\r").strip()
    while len(text) >= 2 and text[0] == text[-1] and text[0] in {'"', "'"}:
        unwrapped = text[1:-1].strip()
        if unwrapped == text:
            break
        text = unwrapped
    return text


def services_dir() -> Path:
    return SERVICES_DIR


def _resolve_filesystem_path(raw_path: str | os.PathLike[str]) -> Path:
    expanded = os.path.expandvars(os.path.expanduser(os.fspath(raw_path)))
    path = Path(expanded)
    if not path.is_absolute():
        path = SERVICES_DIR / path
    return path.resolve()


def _resolve_path_from_cwd(raw_path: str | os.PathLike[str]) -> Path:
    expanded = os.path.expandvars(os.path.expanduser(os.fspath(raw_path)))
    path = Path(expanded)
    if not path.is_absolute():
        path = Path.cwd() / path
    return path.resolve()


def _runtime_storage_root() -> Path | None:
    override = normalize_env_text(os.getenv("KAIRYX_RUNTIME_DIR", ""))
    if override:
        path = _resolve_path_from_cwd(override)
        path.mkdir(parents=True, exist_ok=True)
        return path

    if normalize_env_text(os.getenv("KAIRYX_PLATFORM_SURFACE", "")).lower() == "vercel_demo":
        path = (Path(tempfile.gettempdir()) / "kairyxai-runtime").resolve()
        path.mkdir(parents=True, exist_ok=True)
        return path

    return None


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


def resolve_runtime_file_path(raw_value: str | os.PathLike[str], *, ensure_parent: bool = False) -> Path:
    text = os.fspath(raw_value).strip()
    if not text:
        raise ValueError("Runtime path value cannot be empty.")

    runtime_root = _runtime_storage_root()
    if runtime_root is None:
        path = _resolve_path_from_cwd(text)
    else:
        candidate = Path(os.path.expandvars(os.path.expanduser(text)))
        if candidate.is_absolute():
            path = candidate.resolve()
        else:
            normalized = candidate.as_posix()
            if normalized.startswith("./"):
                normalized = normalized[2:]
            path = (runtime_root / normalized.lstrip("/")).resolve()

    if ensure_parent:
        path.parent.mkdir(parents=True, exist_ok=True)
    return path


def default_control_plane_database_url() -> str:
    runtime_root = _runtime_storage_root()
    if runtime_root is None:
        default_path = resolve_sqlite_file_path(SERVICES_DIR / ".kairyx_control_plane.db", ensure_parent=True)
    else:
        default_path = resolve_runtime_file_path(".kairyx_control_plane.db", ensure_parent=True)
    return f"sqlite:///{default_path}"


def default_local_job_store_path() -> Path:
    runtime_root = _runtime_storage_root()
    if runtime_root is None:
        return resolve_sqlite_file_path(SERVICES_DIR / ".kairyx_local.db", ensure_parent=True)
    return resolve_runtime_file_path(".kairyx_local.db", ensure_parent=True)


def normalize_sqlite_database_url(raw_url: str) -> str:
    if not raw_url:
        return raw_url

    url_text = normalize_env_text(raw_url)
    if not url_text.startswith("sqlite"):
        return url_text

    url = make_url(url_text)
    database = url.database
    if not database or database == ":memory:":
        return url_text

    path = resolve_sqlite_file_path(database, ensure_parent=True)
    return url.set(database=str(path)).render_as_string(hide_password=False)
