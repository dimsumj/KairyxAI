from __future__ import annotations

import os
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SERVICES_DIR = PROJECT_ROOT / "backend" / "services"

os.environ.setdefault("KAIRYX_PLATFORM_SURFACE", "vercel_demo")
os.environ.setdefault("APP_ENV", "demo")
os.environ.setdefault("DATA_BACKEND_MODE", "mock")
os.environ.setdefault("SCHEDULER_ENABLED", "false")
os.environ.setdefault("LEGACY_HEADER_AUTH_ENABLED", "false")
os.environ.setdefault("KAIRYX_MOCK_STORAGE_BACKEND", "database")

if str(SERVICES_DIR) not in sys.path:
    sys.path.insert(0, str(SERVICES_DIR))

from app.main import app  # noqa: E402
