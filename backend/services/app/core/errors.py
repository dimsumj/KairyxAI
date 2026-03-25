from __future__ import annotations


class ResourceLockedError(RuntimeError):
    """Raised when a resource cannot transition because it is still in use."""


class MissingDependencyError(KeyError):
    """Raised when a referenced upstream resource is missing."""

    def __init__(self, resource_type: str, resource_id: str, detail: str | None = None):
        self.resource_type = str(resource_type)
        self.resource_id = str(resource_id)
        self.detail = detail or f"{self.resource_type.title()} '{self.resource_id}' not found."
        super().__init__(self.resource_id)


def is_database_locked_error(exc: BaseException) -> bool:
    pending: list[BaseException] = [exc]
    seen: set[int] = set()
    while pending:
        current = pending.pop()
        if id(current) in seen:
            continue
        seen.add(id(current))
        message = str(current).lower()
        if "database is locked" in message or "database table is locked" in message:
            return True
        nested = getattr(current, "exceptions", None)
        if isinstance(nested, tuple):
            pending.extend(item for item in nested if isinstance(item, BaseException))
        next_exc = getattr(current, "orig", None)
        if isinstance(next_exc, BaseException):
            pending.append(next_exc)
        cause = getattr(current, "__cause__", None)
        if isinstance(cause, BaseException):
            pending.append(cause)
    return False
