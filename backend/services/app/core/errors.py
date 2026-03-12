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
