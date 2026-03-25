from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar, Token
from dataclasses import dataclass, field
from typing import Any, Iterator


@dataclass(frozen=True)
class RequestContext:
    actor_id: str
    actor_role: str
    tenant_id: str | None
    correlation_id: str
    platform_admin: bool = False
    auth_mode: str = "system"
    claims: dict[str, Any] = field(default_factory=dict)


_request_context: ContextVar[RequestContext | None] = ContextVar("kairyx_request_context", default=None)


def get_request_context() -> RequestContext | None:
    return _request_context.get()


def set_request_context(context: RequestContext | None) -> Token[RequestContext | None]:
    return _request_context.set(context)


def reset_request_context(token: Token[RequestContext | None]) -> None:
    _request_context.reset(token)


@contextmanager
def request_context(context: RequestContext | None) -> Iterator[None]:
    token = set_request_context(context)
    try:
        yield
    finally:
        reset_request_context(token)
