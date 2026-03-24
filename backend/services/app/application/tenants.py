from __future__ import annotations

from typing import Any, Dict, List


class TenantService:
    def __init__(self, repository):
        self.repository = repository

    def list_tenants(self) -> List[Dict[str, Any]]:
        return self.repository.list_tenants()

    def create_tenant(self, tenant_id: str, name: str, status: str = "active") -> Dict[str, Any]:
        return self.repository.ensure_tenant(tenant_id, name, status=status)

    def list_memberships(self, tenant_id: str) -> Dict[str, Any]:
        return {"items": self.repository.list_tenant_memberships(tenant_id)}

    def upsert_membership(
        self,
        tenant_id: str,
        user_id: str,
        *,
        role: str,
        status: str = "active",
        email: str | None = None,
        display_name: str | None = None,
    ) -> Dict[str, Any]:
        self.repository.ensure_tenant(tenant_id)
        user = self.repository.upsert_platform_user(user_id, email=email, display_name=display_name)
        membership = self.repository.upsert_tenant_membership(tenant_id, user_id, role=role, status=status)
        return {"user": user, "membership": membership}
