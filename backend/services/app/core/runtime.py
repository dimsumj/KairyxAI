from __future__ import annotations

import threading


_shutdown_requested = threading.Event()


def clear_shutdown_requested() -> None:
    _shutdown_requested.clear()


def mark_shutdown_requested() -> None:
    _shutdown_requested.set()


def is_shutdown_requested() -> bool:
    return _shutdown_requested.is_set()
