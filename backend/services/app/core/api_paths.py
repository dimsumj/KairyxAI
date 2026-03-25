from __future__ import annotations

import re
from urllib.parse import quote

from fastapi import Request

_ORG_SLUG_PATTERN = re.compile(r"^[a-z0-9][a-z0-9-]{0,63}$")


def apply_org_scoped_api_alias(request: Request, api_v1_prefix: str) -> None:
    original_path = str(request.scope.get("path") or "/")
    request.state.external_request_path = original_path
    normalized_prefix = api_v1_prefix.rstrip("/")
    request.state.scoped_api_prefix = normalized_prefix
    request.state.path_scoped_tenant_id = None

    if original_path.startswith(normalized_prefix):
        return

    segments = [segment for segment in original_path.split("/") if segment]
    if len(segments) < 2 or segments[1] != "v1":
        return

    tenant_id = str(segments[0]).strip()
    if not tenant_id or _ORG_SLUG_PATTERN.fullmatch(tenant_id) is None:
        return

    suffix = ""
    if len(segments) > 2:
        suffix = "/" + "/".join(segments[2:])
    request.scope["path"] = f"{normalized_prefix}{suffix}"
    request.state.path_scoped_tenant_id = tenant_id
    request.state.scoped_api_prefix = f"/{quote(tenant_id, safe='')}/v1"


def get_external_request_path(request: Request) -> str:
    return str(getattr(request.state, "external_request_path", "") or request.url.path)


def get_request_api_prefix(request: Request, default_api_v1_prefix: str = "/api/v1") -> str:
    return str(getattr(request.state, "scoped_api_prefix", "") or default_api_v1_prefix.rstrip("/"))


def build_request_api_path(request: Request, resource_path: str, default_api_v1_prefix: str = "/api/v1") -> str:
    prefix = get_request_api_prefix(request, default_api_v1_prefix=default_api_v1_prefix).rstrip("/")
    suffix = "/" + str(resource_path or "").lstrip("/")
    return f"{prefix}{suffix}"


def get_path_scoped_tenant_id(request: Request) -> str | None:
    tenant_id = str(getattr(request.state, "path_scoped_tenant_id", "") or "").strip()
    return tenant_id or None
