from __future__ import annotations

import sys
from pathlib import Path

import pytest


TESTS_DIR = Path(__file__).resolve().parent
BACKEND_SERVICES_DIR = TESTS_DIR.parent

if str(BACKEND_SERVICES_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_SERVICES_DIR))

from secret_manager_service import SecretManagerService


@pytest.fixture(autouse=True)
def configure_control_plane_secret_key(monkeypatch):
    monkeypatch.setenv("CONTROL_PLANE_SECRET_KEY", "test-control-plane-secret-key")
    SecretManagerService._get_control_plane_cipher.cache_clear()
    SecretManagerService._get_invalid_token_error.cache_clear()
    yield
    SecretManagerService._get_control_plane_cipher.cache_clear()
    SecretManagerService._get_invalid_token_error.cache_clear()
