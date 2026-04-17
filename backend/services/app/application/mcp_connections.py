from __future__ import annotations

import asyncio
import ipaddress
import secrets
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from html import escape
from typing import Any, Dict, List, Sequence
from urllib.parse import urlencode, urlparse

import httpx
from fastapi import HTTPException

from app.application.cohorts import CohortService
from app.application.secret_refs import (
    materialize_secret_refs,
    secure_inline_secret_values,
)
from app.core.settings import Settings


MCP_CONNECTION_RESOURCE_TYPE = "mcp_connection"
MCP_AUTHORIZATION_RESOURCE_TYPE = "mcp_connection_authorization"
MCP_OAUTH_STATE_RESOURCE_TYPE = "mcp_oauth_state"
MCP_RESULT_SNAPSHOT_RESOURCE_TYPE = "mcp_result_snapshot"

MCP_SECRET_FIELDS = {
    "access_token",
    "refresh_token",
    "client_secret",
    "id_token",
}
SAFE_TOOL_PREFIXES = ("get", "list", "query", "search", "read", "fetch")
BLOCKED_TOOL_PREFIXES = ("create", "edit", "update", "save", "delete", "archive", "restore", "write", "set")
IDENTIFIER_FIELDS = ("canonical_user_id", "player_id", "user_id")
MCP_TOOL_LOOP_LIMIT = 3
MCP_PRESETS = {
    "amplitude_us": {
        "label": "Amplitude (US)",
        "endpoint_url": "https://mcp.amplitude.com/mcp",
    },
    "amplitude_eu": {
        "label": "Amplitude (EU)",
        "endpoint_url": "https://mcp.eu.amplitude.com/mcp",
    },
    "custom": {
        "label": "Custom Remote MCP",
        "endpoint_url": "",
    },
}


@dataclass(frozen=True)
class McpPromptStep:
    thought: str
    done: bool
    tool_name: str = ""
    arguments: Dict[str, Any] | None = None
    answer: str = ""


class McpConnectionService:
    def __init__(self, repository, settings: Settings, *, cohort_service: CohortService | None = None):
        self.repository = repository
        self.settings = settings
        self.cohorts = cohort_service or CohortService(repository)

    def list_connections(self, *, actor_id: str) -> List[Dict[str, Any]]:
        return [self._to_connection_response(record, actor_id=actor_id) for record in self.repository.list_resources(MCP_CONNECTION_RESOURCE_TYPE)]

    def list_snapshots(self) -> List[Dict[str, Any]]:
        return [self._to_snapshot_response(record) for record in self.repository.list_resources(MCP_RESULT_SNAPSHOT_RESOURCE_TYPE)]

    def get_connection(self, mcp_connection_id: str, *, actor_id: str) -> Dict[str, Any] | None:
        record = self.repository.get_resource(MCP_CONNECTION_RESOURCE_TYPE, mcp_connection_id)
        return self._to_connection_response(record, actor_id=actor_id) if record else None

    def get_snapshot(self, snapshot_id: str) -> Dict[str, Any] | None:
        record = self.repository.get_resource(MCP_RESULT_SNAPSHOT_RESOURCE_TYPE, snapshot_id)
        return self._to_snapshot_response(record) if record else None

    def validate_endpoint(self, endpoint_url: str, *, preset_key: str | None = None) -> Dict[str, Any]:
        normalized_preset = self._normalize_preset_key(preset_key)
        resolved_url = self._resolve_endpoint_url(endpoint_url, preset_key=normalized_preset)
        self._validate_remote_endpoint(resolved_url)
        return {
            "endpoint_url": resolved_url,
            "preset_key": normalized_preset,
            "transport_type": "streamable_http",
            "auth_mode": "oauth_authorization_code",
            "is_valid": True,
            "notes": [],
        }

    def create_connection(self, *, name: str, preset_key: str, endpoint_url: str | None) -> Dict[str, Any]:
        normalized_preset = self._normalize_preset_key(preset_key)
        resolved_url = self._resolve_endpoint_url(endpoint_url, preset_key=normalized_preset)
        self._validate_remote_endpoint(resolved_url)
        connection_id = f"mcpc_{uuid.uuid4().hex[:20]}"
        now = datetime.utcnow().isoformat()
        payload = {
            "mcp_connection_id": connection_id,
            "name": str(name or "").strip(),
            "preset_key": normalized_preset,
            "endpoint_url": resolved_url,
            "transport_type": "streamable_http",
            "auth_mode": "oauth_authorization_code",
            "status": "active",
            "allowed_tools": [],
            "discovered_tools": [],
            "last_discovered_at": None,
            "last_validated_at": now,
            "client_info": {},
            "oauth_metadata": {},
            "protected_resource_metadata": {},
        }
        record = self.repository.upsert_resource(
            MCP_CONNECTION_RESOURCE_TYPE,
            connection_id,
            status="active",
            name=payload["name"],
            payload=payload,
        )
        self.repository.record_action("mcp_connection_created", MCP_CONNECTION_RESOURCE_TYPE, connection_id, payload)
        actor_id = str((record.get("payload") or {}).get("updated_by") or "system")
        return self._to_connection_response(record, actor_id=actor_id)

    def update_connection(self, mcp_connection_id: str, patch: Dict[str, Any], *, actor_id: str) -> Dict[str, Any]:
        record = self.repository.get_resource(MCP_CONNECTION_RESOURCE_TYPE, mcp_connection_id)
        if record is None:
            raise KeyError(mcp_connection_id)
        payload = dict(record.get("payload") or {})
        if patch.get("name") is not None:
            payload["name"] = str(patch["name"] or "").strip()
        normalized_preset = self._normalize_preset_key(patch.get("preset_key") or payload.get("preset_key"))
        resolved_url = self._resolve_endpoint_url(patch.get("endpoint_url"), preset_key=normalized_preset, fallback=payload.get("endpoint_url"))
        self._validate_remote_endpoint(resolved_url)
        payload["preset_key"] = normalized_preset
        payload["endpoint_url"] = resolved_url
        if patch.get("status") is not None:
            payload["status"] = str(patch["status"] or "active").strip().lower() or "active"
        payload["last_validated_at"] = datetime.utcnow().isoformat()
        saved = self.repository.upsert_resource(
            MCP_CONNECTION_RESOURCE_TYPE,
            mcp_connection_id,
            status=str(payload.get("status") or "active"),
            name=payload.get("name"),
            payload=payload,
        )
        self.repository.record_action("mcp_connection_updated", MCP_CONNECTION_RESOURCE_TYPE, mcp_connection_id, patch)
        return self._to_connection_response(saved, actor_id=actor_id)

    def delete_connection(self, mcp_connection_id: str) -> bool:
        deleted = self.repository.delete_resource(MCP_CONNECTION_RESOURCE_TYPE, mcp_connection_id)
        if not deleted:
            return False
        for record in self.repository.list_resources(MCP_AUTHORIZATION_RESOURCE_TYPE):
            payload = dict(record.get("payload") or {})
            if str(payload.get("mcp_connection_id") or "") == mcp_connection_id:
                self.repository.delete_resource(MCP_AUTHORIZATION_RESOURCE_TYPE, str(record.get("resource_id") or ""))
        for record in self.repository.list_resources(MCP_OAUTH_STATE_RESOURCE_TYPE):
            payload = dict(record.get("payload") or {})
            if str(payload.get("mcp_connection_id") or "") == mcp_connection_id:
                self.repository.delete_resource(MCP_OAUTH_STATE_RESOURCE_TYPE, str(record.get("resource_id") or ""))
        self.repository.record_action("mcp_connection_deleted", MCP_CONNECTION_RESOURCE_TYPE, mcp_connection_id, {"mcp_connection_id": mcp_connection_id})
        return True

    def start_oauth_connection(
        self,
        mcp_connection_id: str,
        *,
        actor_id: str,
        callback_url: str,
    ) -> Dict[str, Any]:
        connection = self._get_connection_payload(mcp_connection_id)
        oauth_context = asyncio.run(self._prepare_oauth_context(connection, callback_url=callback_url))
        state_id = f"mcpstate_{uuid.uuid4().hex[:20]}"
        payload = {
            "state_id": state_id,
            "mcp_connection_id": mcp_connection_id,
            "actor_id": actor_id,
            "tenant_id": connection.get("tenant_id"),
            "project_id": connection.get("project_id"),
            "endpoint_url": connection["endpoint_url"],
            "callback_url": callback_url,
            "state": oauth_context["state"],
            "code_verifier": oauth_context["code_verifier"],
            "client_id": oauth_context["client_id"],
            "token_endpoint": oauth_context["token_endpoint"],
            "resource": oauth_context["resource"],
            "authorization_url": oauth_context["authorization_url"],
            "created_at": datetime.utcnow().isoformat(),
            "expires_at": (datetime.now(timezone.utc) + timedelta(minutes=10)).isoformat(),
        }
        self.repository.upsert_resource(
            MCP_OAUTH_STATE_RESOURCE_TYPE,
            state_id,
            status="pending",
            name=mcp_connection_id,
            payload=payload,
        )
        self.repository.record_action("mcp_connection_auth_started", MCP_CONNECTION_RESOURCE_TYPE, mcp_connection_id, {"actor_id": actor_id, "state_id": state_id})
        return {
            "authorization_url": oauth_context["authorization_url"],
            "state_id": state_id,
            "popup_title": f"Authorize {connection.get('name') or 'MCP Connection'}",
        }

    def complete_oauth_connection(self, *, state: str, code: str | None, error: str | None = None) -> Dict[str, Any]:
        state_record = self._get_oauth_state_by_value(state)
        state_payload = dict(state_record.get("payload") or {})
        tenant_id = str(state_payload.get("tenant_id") or state_record.get("tenant_id") or "").strip() or None
        project_id = str(state_payload.get("project_id") or state_record.get("project_id") or "").strip() or None
        connection = self._get_connection_payload(
            str(state_payload.get("mcp_connection_id") or ""),
            tenant_id=tenant_id,
            project_id=project_id,
        )
        actor_id = str(state_payload.get("actor_id") or "")
        if error:
            self._upsert_authorization(
                mcp_connection_id=connection["mcp_connection_id"],
                actor_id=actor_id,
                patch={"status": "error", "last_error": error},
                tenant_id=tenant_id,
                project_id=project_id,
            )
            self.repository.upsert_resource(
                MCP_OAUTH_STATE_RESOURCE_TYPE,
                str(state_payload.get("state_id") or state_record.get("resource_id") or ""),
                status="failed",
                name=connection["mcp_connection_id"],
                payload={**state_payload, "error": error},
                tenant_id=tenant_id,
                project_id=project_id,
            )
            return {
                "ok": False,
                "actor_id": actor_id,
                "mcp_connection_id": connection["mcp_connection_id"],
                "message": f"Authorization failed: {error}",
            }
        if not code:
            raise HTTPException(status_code=400, detail="Missing OAuth authorization code.")
        token_payload = asyncio.run(self._exchange_oauth_code(connection, state_payload=state_payload, code=code))
        self._upsert_authorization(
            mcp_connection_id=connection["mcp_connection_id"],
            actor_id=actor_id,
            patch=token_payload,
            tenant_id=tenant_id,
            project_id=project_id,
        )
        self.repository.upsert_resource(
            MCP_OAUTH_STATE_RESOURCE_TYPE,
            str(state_payload.get("state_id") or state_record.get("resource_id") or ""),
            status="completed",
            name=connection["mcp_connection_id"],
            payload={**state_payload, "completed_at": datetime.utcnow().isoformat()},
            tenant_id=tenant_id,
            project_id=project_id,
        )
        refreshed = self.refresh_tools(
            connection["mcp_connection_id"],
            actor_id=actor_id,
            tenant_id=tenant_id,
            project_id=project_id,
        )
        return {
            "ok": True,
            "actor_id": actor_id,
            "mcp_connection_id": connection["mcp_connection_id"],
            "message": f"Authorized {connection.get('name') or connection['mcp_connection_id']}.",
            "discovered_tool_count": int(refreshed.get("discovered_tool_count") or 0),
        }

    def disconnect(self, mcp_connection_id: str, *, actor_id: str) -> Dict[str, Any]:
        authorization_id = self._authorization_id(mcp_connection_id, actor_id)
        deleted = self.repository.delete_resource(MCP_AUTHORIZATION_RESOURCE_TYPE, authorization_id)
        self.repository.record_action(
            "mcp_connection_disconnected",
            MCP_CONNECTION_RESOURCE_TYPE,
            mcp_connection_id,
            {"actor_id": actor_id, "deleted": deleted},
        )
        connection = self.get_connection(mcp_connection_id, actor_id=actor_id)
        if connection is None:
            raise KeyError(mcp_connection_id)
        return connection

    def refresh_tools(
        self,
        mcp_connection_id: str,
        *,
        actor_id: str,
        tenant_id: str | None = None,
        project_id: str | None = None,
    ) -> Dict[str, Any]:
        connection = self._get_connection_payload(mcp_connection_id, tenant_id=tenant_id, project_id=project_id)
        auth_payload = self._resolve_authorization(
            mcp_connection_id,
            actor_id=actor_id,
            require_active=True,
            tenant_id=tenant_id,
            project_id=project_id,
        )
        discovered_tools = asyncio.run(self._list_remote_tools(connection, auth_payload=auth_payload))
        allowed_tools = [item["name"] for item in discovered_tools if bool(item.get("allowed"))]
        connection["discovered_tools"] = discovered_tools
        connection["allowed_tools"] = allowed_tools
        connection["last_discovered_at"] = datetime.utcnow().isoformat()
        connection["last_validated_at"] = connection["last_discovered_at"]
        saved = self.repository.upsert_resource(
            MCP_CONNECTION_RESOURCE_TYPE,
            mcp_connection_id,
            status=str(connection.get("status") or "active"),
            name=connection.get("name"),
            payload=connection,
            tenant_id=tenant_id,
            project_id=project_id,
        )
        self.repository.record_action(
            "mcp_connection_tools_refreshed",
            MCP_CONNECTION_RESOURCE_TYPE,
            mcp_connection_id,
            {"actor_id": actor_id, "allowed_tools": allowed_tools},
        )
        return self._to_connection_response(saved, actor_id=actor_id)

    def run_prompt(
        self,
        mcp_connection_id: str,
        *,
        actor_id: str,
        question: str,
        model_adapter,
        session_state: Dict[str, Any],
        ui_context: Dict[str, Any],
    ) -> Dict[str, Any]:
        connection = self._get_connection_payload(mcp_connection_id)
        auth_payload = self._resolve_authorization(mcp_connection_id, actor_id=actor_id, require_active=True)
        discovered_tools = list(connection.get("discovered_tools") or [])
        if not discovered_tools:
            connection = self.refresh_tools(mcp_connection_id, actor_id=actor_id)
            discovered_tools = list(connection.get("discovered_tools") or [])
        allowed_tools = [item for item in discovered_tools if item.get("allowed")]
        if not allowed_tools:
            raise HTTPException(status_code=409, detail="No read-only MCP tools are available for this connection.")
        result = asyncio.run(
            self._run_prompt_async(
                connection=dict(connection),
                auth_payload=auth_payload,
                question=question,
                model_adapter=model_adapter,
                session_state=session_state,
                ui_context=ui_context,
                allowed_tools=allowed_tools,
            )
        )
        return result

    def import_snapshot(
        self,
        mcp_connection_id: str,
        *,
        name: str | None,
        query_result: Dict[str, Any],
    ) -> Dict[str, Any]:
        connection = self._get_connection_payload(mcp_connection_id)
        rows = [dict(item) for item in list(query_result.get("rows") or []) if isinstance(item, dict)]
        snapshot_id = f"mcps_{uuid.uuid4().hex[:20]}"
        identifier_fields = self._detect_identifier_fields(rows)
        payload = {
            "snapshot_id": snapshot_id,
            "mcp_connection_id": connection["mcp_connection_id"],
            "name": str(name or f"{connection.get('name') or 'mcp'} snapshot {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}").strip(),
            "question": str(query_result.get("question") or "").strip(),
            "answer": str(query_result.get("answer") or "").strip(),
            "rows": rows,
            "tool_calls": list(query_result.get("tool_calls") or []),
            "result": dict(query_result.get("result") or {}),
            "row_count": len(rows),
            "identifier_fields": identifier_fields,
            "status": "ready",
        }
        record = self.repository.upsert_resource(
            MCP_RESULT_SNAPSHOT_RESOURCE_TYPE,
            snapshot_id,
            status="ready",
            name=payload["name"],
            payload=payload,
        )
        self.repository.record_action("mcp_result_snapshot_created", MCP_RESULT_SNAPSHOT_RESOURCE_TYPE, snapshot_id, payload)
        return self._to_snapshot_response(record)

    def create_cohort_from_snapshot(
        self,
        snapshot_id: str,
        *,
        name: str,
        description: str = "",
        tags: Sequence[str] | None = None,
    ) -> Dict[str, Any]:
        snapshot = self.get_snapshot(snapshot_id)
        if snapshot is None:
            raise KeyError(snapshot_id)
        snapshot_record = self.repository.get_resource(MCP_RESULT_SNAPSHOT_RESOURCE_TYPE, snapshot_id)
        rows = list((snapshot_record.get("payload") or {}).get("rows") or []) if snapshot_record else []
        members = [self._normalize_snapshot_member(item) for item in rows if self._normalize_snapshot_member(item).get("canonical_user_id")]
        if not members:
            raise HTTPException(status_code=409, detail="This snapshot does not contain canonical_user_id, user_id, or player_id rows.")
        cohort = self.cohorts.create_cohort(
            name=name,
            cohort_type="list",
            definition={"members": members},
            refresh_mode="manual",
            description=description or f"Created from MCP snapshot {snapshot_id}.",
            tags=list(tags or ["mcp", "snapshot"]),
            activate=False,
        )
        return cohort

    def oauth_callback_html(self, payload: Dict[str, Any]) -> str:
        message = escape(str(payload.get("message") or "Authorization complete."))
        connection_id = escape(str(payload.get("mcp_connection_id") or ""))
        ok = "true" if bool(payload.get("ok")) else "false"
        return (
            "<!DOCTYPE html><html><head><meta charset='utf-8'><title>MCP Authorization</title></head>"
            "<body style='font-family: sans-serif; padding: 24px;'>"
            f"<h1>{'Connected' if payload.get('ok') else 'Authorization failed'}</h1>"
            f"<p>{message}</p>"
            "<p>You can close this window.</p>"
            "<script>"
            "if (window.opener) {"
            f"window.opener.postMessage({{type:'kairyx:mcp-oauth-complete', ok:{ok}, mcpConnectionId:'{connection_id}'}}, window.location.origin);"
            "}"
            "setTimeout(function(){ window.close(); }, 250);"
            "</script>"
            "</body></html>"
        )

    async def _prepare_oauth_context(self, connection: Dict[str, Any], *, callback_url: str) -> Dict[str, Any]:
        mcp_auth = self._import_mcp_auth()
        prm, oauth_metadata = await self._discover_oauth_metadata(str(connection.get("endpoint_url") or ""))
        client_info = self._materialize_client_info(connection)
        if client_info is None:
            client_metadata = mcp_auth["OAuthClientMetadata"](
                redirect_uris=[callback_url],
                client_name="KairyxAI",
                grant_types=["authorization_code", "refresh_token"],
                response_types=["code"],
                token_endpoint_auth_method="none",
            )
            request = mcp_auth["create_client_registration_request"](
                oauth_metadata,
                client_metadata,
                self._authorization_base_url(str(connection.get("endpoint_url") or "")),
            )
            async with httpx.AsyncClient(timeout=20.0, follow_redirects=True) as client:
                response = await client.send(request)
            client_info = await mcp_auth["handle_registration_response"](response)
            self._persist_client_info(connection["mcp_connection_id"], client_info=client_info, oauth_metadata=oauth_metadata, protected_resource_metadata=prm)
        scopes = mcp_auth["get_client_metadata_scopes"](None, prm, oauth_metadata) or ""
        pkce_params = mcp_auth["PKCEParameters"].generate()
        state = secrets.token_urlsafe(32)
        resource = mcp_auth["resource_url_from_server_url"](str(connection.get("endpoint_url") or ""))
        authorization_endpoint = str(
            getattr(oauth_metadata, "authorization_endpoint", "") or f"{self._authorization_base_url(str(connection.get('endpoint_url') or ''))}/authorize"
        )
        auth_params = {
            "response_type": "code",
            "client_id": str(client_info.client_id),
            "redirect_uri": callback_url,
            "state": state,
            "code_challenge": str(pkce_params.code_challenge),
            "code_challenge_method": "S256",
        }
        if resource:
            auth_params["resource"] = resource
        if scopes:
            auth_params["scope"] = scopes
        authorization_url = f"{authorization_endpoint}?{urlencode(auth_params)}"
        return {
            "authorization_url": authorization_url,
            "client_id": str(client_info.client_id),
            "state": state,
            "code_verifier": str(pkce_params.code_verifier),
            "token_endpoint": str(getattr(oauth_metadata, "token_endpoint", "") or f"{self._authorization_base_url(str(connection.get('endpoint_url') or ''))}/token"),
            "resource": resource,
        }

    async def _exchange_oauth_code(self, connection: Dict[str, Any], *, state_payload: Dict[str, Any], code: str) -> Dict[str, Any]:
        mcp_auth = self._import_mcp_auth()
        client_info = self._materialize_client_info(connection)
        if client_info is None:
            raise HTTPException(status_code=409, detail="Missing MCP OAuth client registration.")
        token_data = {
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": str(state_payload.get("callback_url") or ""),
            "client_id": str(state_payload.get("client_id") or client_info.client_id),
            "code_verifier": str(state_payload.get("code_verifier") or ""),
        }
        resource = str(state_payload.get("resource") or "").strip()
        if resource:
            token_data["resource"] = resource
        if getattr(client_info, "token_endpoint_auth_method", None) == "client_secret_post" and getattr(client_info, "client_secret", None):
            token_data["client_secret"] = str(client_info.client_secret)
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        if getattr(client_info, "token_endpoint_auth_method", None) == "client_secret_basic" and getattr(client_info, "client_secret", None):
            context = mcp_auth["OAuthContext"](
                server_url=str(connection.get("endpoint_url") or ""),
                client_metadata=mcp_auth["OAuthClientMetadata"](
                    redirect_uris=[str(state_payload.get("callback_url") or "")],
                    client_name="KairyxAI",
                    grant_types=["authorization_code", "refresh_token"],
                    response_types=["code"],
                ),
                storage=_NullTokenStorage(),
                redirect_handler=None,
                callback_handler=None,
            )
            context.client_info = client_info
            token_data, headers = context.prepare_token_auth(token_data, headers)
        async with httpx.AsyncClient(timeout=20.0, follow_redirects=True) as client:
            response = await client.post(str(state_payload.get("token_endpoint") or ""), data=token_data, headers=headers)
        token = await mcp_auth["handle_token_response_scopes"](response)
        expires_at = None
        if getattr(token, "expires_in", None):
            expires_at = (datetime.now(timezone.utc) + timedelta(seconds=int(token.expires_in))).isoformat()
        return {
            "status": "authorized",
            "authorized_at": datetime.utcnow().isoformat(),
            "expires_at": expires_at,
            "last_error": "",
            "tokens": token.model_dump(mode="json"),
        }

    async def _discover_oauth_metadata(self, endpoint_url: str):
        mcp_auth = self._import_mcp_auth()
        protected_resource_metadata = None
        oauth_metadata = None
        async with httpx.AsyncClient(timeout=20.0, follow_redirects=True) as client:
            discovery_urls = mcp_auth["build_protected_resource_metadata_discovery_urls"](None, endpoint_url)
            for url in discovery_urls:
                response = await client.send(mcp_auth["create_oauth_metadata_request"](url))
                protected_resource_metadata = await mcp_auth["handle_protected_resource_response"](response)
                if protected_resource_metadata is not None:
                    break
            auth_server_url = (
                str((protected_resource_metadata.authorization_servers or [None])[0])
                if protected_resource_metadata and getattr(protected_resource_metadata, "authorization_servers", None)
                else None
            )
            oauth_urls = mcp_auth["build_oauth_authorization_server_metadata_discovery_urls"](auth_server_url, endpoint_url)
            for url in oauth_urls:
                response = await client.send(mcp_auth["create_oauth_metadata_request"](url))
                ok, oauth_metadata = await mcp_auth["handle_auth_metadata_response"](response)
                if oauth_metadata is not None or not ok:
                    break
        if oauth_metadata is None:
            raise HTTPException(status_code=409, detail="Could not discover MCP OAuth metadata for this server.")
        return protected_resource_metadata, oauth_metadata

    async def _list_remote_tools(self, connection: Dict[str, Any], *, auth_payload: Dict[str, Any]) -> List[Dict[str, Any]]:
        async with self._open_mcp_session(connection, auth_payload=auth_payload) as session:
            listed = await session.list_tools()
        tools = []
        for tool in list(getattr(listed, "tools", []) or []):
            name = str(getattr(tool, "name", "") or "")
            description = str(getattr(tool, "description", "") or "")
            classification = self._classify_tool(name)
            tools.append(
                {
                    "name": name,
                    "description": description,
                    "allowed": classification == "read_only",
                    "classification": classification,
                    "input_schema": getattr(tool, "inputSchema", None) or {},
                }
            )
        return tools

    async def _run_prompt_async(
        self,
        *,
        connection: Dict[str, Any],
        auth_payload: Dict[str, Any],
        question: str,
        model_adapter,
        session_state: Dict[str, Any],
        ui_context: Dict[str, Any],
        allowed_tools: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        tool_catalog = [
            {
                "name": item["name"],
                "description": item.get("description") or "",
                "input_schema": item.get("input_schema") or {},
            }
            for item in allowed_tools
        ]
        step_results: List[Dict[str, Any]] = []
        final_answer = ""
        async with self._open_mcp_session(connection, auth_payload=auth_payload) as session:
            for _ in range(MCP_TOOL_LOOP_LIMIT):
                step = self._plan_mcp_step(
                    question,
                    tool_catalog=tool_catalog,
                    previous_steps=step_results,
                    model_adapter=model_adapter,
                    session_state=session_state,
                    ui_context=ui_context,
                )
                if step.done:
                    final_answer = step.answer.strip()
                    break
                if not step.tool_name:
                    break
                result = await session.call_tool(step.tool_name, step.arguments or {})
                serialized = result.model_dump(mode="json")
                step_results.append(
                    {
                        "thought": step.thought,
                        "tool_name": step.tool_name,
                        "arguments": step.arguments or {},
                        "result": serialized,
                    }
                )
            if not final_answer:
                final_answer = self._summarize_tool_results(
                    question,
                    tool_catalog=tool_catalog,
                    step_results=step_results,
                    model_adapter=model_adapter,
                    session_state=session_state,
                    ui_context=ui_context,
                )
        rows = self._extract_rows_from_tool_results(step_results)
        query_result = {
            "query_id": f"mcpq_{uuid.uuid4().hex[:20]}",
            "mcp_connection_id": str(connection.get("mcp_connection_id") or ""),
            "question": question,
            "answer": final_answer,
            "rows": rows,
            "tool_calls": step_results,
            "result": {"tool_calls": step_results},
        }
        return query_result

    def _plan_mcp_step(
        self,
        question: str,
        *,
        tool_catalog: List[Dict[str, Any]],
        previous_steps: List[Dict[str, Any]],
        model_adapter,
        session_state: Dict[str, Any],
        ui_context: Dict[str, Any],
    ) -> McpPromptStep:
        if hasattr(model_adapter, "plan_mcp_step"):
            planned = model_adapter.plan_mcp_step(
                question,
                session_state=session_state,
                ui_context=ui_context,
                hint={"tool_catalog": tool_catalog, "previous_steps": previous_steps},
            )
            if isinstance(planned, dict):
                return McpPromptStep(
                    thought=str(planned.get("thought") or ""),
                    done=bool(planned.get("done")),
                    tool_name=str(planned.get("tool_name") or ""),
                    arguments=dict(planned.get("arguments") or {}),
                    answer=str(planned.get("answer") or ""),
                )
        if previous_steps:
            return McpPromptStep(thought="Summarize the tool results.", done=True, answer="")
        search_tool = next((item for item in tool_catalog if str(item.get("name") or "") == "search"), None)
        if search_tool is not None:
            return McpPromptStep(
                thought="Use the search tool to locate the closest matching Amplitude objects first.",
                done=False,
                tool_name="search",
                arguments={"query": question},
            )
        context_tool = next((item for item in tool_catalog if str(item.get("name") or "") == "get_context"), None)
        if context_tool is not None:
            return McpPromptStep(
                thought="Start with account context when search is not available.",
                done=False,
                tool_name="get_context",
                arguments={},
            )
        first_tool = tool_catalog[0] if tool_catalog else {}
        return McpPromptStep(
            thought="Use the first available read-only tool.",
            done=False,
            tool_name=str(first_tool.get("name") or ""),
            arguments={},
        )

    def _summarize_tool_results(
        self,
        question: str,
        *,
        tool_catalog: List[Dict[str, Any]],
        step_results: List[Dict[str, Any]],
        model_adapter,
        session_state: Dict[str, Any],
        ui_context: Dict[str, Any],
    ) -> str:
        if hasattr(model_adapter, "summarize_mcp_results"):
            result = model_adapter.summarize_mcp_results(
                question,
                session_state=session_state,
                ui_context=ui_context,
                hint={"tool_catalog": tool_catalog, "step_results": step_results},
            )
            if isinstance(result, str) and result.strip():
                return result.strip()
        if not step_results:
            return "The MCP connection is configured, but the read-only tool run did not produce a usable result."
        latest = step_results[-1]
        rows = self._extract_rows_from_tool_results(step_results)
        if rows:
            sample = rows[0]
            sample_bits = ", ".join(f"{key}={sample.get(key)!r}" for key in list(sample.keys())[:4])
            return f"Ran `{latest.get('tool_name')}` and extracted {len(rows)} row(s). Sample: {sample_bits}."
        return f"Ran `{latest.get('tool_name')}` and captured a structured MCP response."

    @staticmethod
    def _extract_rows_from_tool_results(step_results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        for step in step_results:
            extracted = McpConnectionService._find_first_rowset(step.get("result"))
            if extracted:
                return extracted[:200]
        return []

    @staticmethod
    def _find_first_rowset(payload: Any) -> List[Dict[str, Any]]:
        if isinstance(payload, list):
            if payload and all(isinstance(item, dict) for item in payload):
                return [dict(item) for item in payload]
            for item in payload:
                extracted = McpConnectionService._find_first_rowset(item)
                if extracted:
                    return extracted
            return []
        if isinstance(payload, dict):
            for value in payload.values():
                extracted = McpConnectionService._find_first_rowset(value)
                if extracted:
                    return extracted
        return []

    @staticmethod
    def _normalize_snapshot_member(row: Dict[str, Any]) -> Dict[str, Any]:
        member = dict(row or {})
        canonical = member.get("canonical_user_id") or member.get("user_id") or member.get("player_id")
        if canonical is not None and str(canonical).strip():
            member["canonical_user_id"] = str(canonical)
        return member

    @staticmethod
    def _detect_identifier_fields(rows: Sequence[Dict[str, Any]]) -> List[str]:
        identifiers: List[str] = []
        for field in IDENTIFIER_FIELDS:
            if any(str((row or {}).get(field) or "").strip() for row in rows):
                identifiers.append(field)
        return identifiers

    @staticmethod
    def _authorization_base_url(endpoint_url: str) -> str:
        parsed = urlparse(str(endpoint_url or ""))
        return f"{parsed.scheme}://{parsed.netloc}"

    def _normalize_preset_key(self, preset_key: str | None) -> str:
        normalized = str(preset_key or "amplitude_us").strip().lower() or "amplitude_us"
        if normalized not in MCP_PRESETS:
            raise ValueError(f"Unsupported MCP preset '{normalized}'.")
        return normalized

    def _resolve_endpoint_url(self, endpoint_url: str | None, *, preset_key: str, fallback: str | None = None) -> str:
        raw = str(endpoint_url or "").strip() or str(fallback or "").strip() or str(MCP_PRESETS[preset_key]["endpoint_url"] or "").strip()
        if not raw:
            raise ValueError("endpoint_url is required.")
        return raw

    @staticmethod
    def _validate_remote_endpoint(endpoint_url: str) -> None:
        parsed = urlparse(str(endpoint_url or "").strip())
        if parsed.scheme != "https":
            raise ValueError("MCP connections require a public https:// endpoint.")
        hostname = str(parsed.hostname or "").strip().lower()
        if not hostname:
            raise ValueError("MCP connections require a valid hostname.")
        if hostname in {"localhost", "127.0.0.1", "::1"} or hostname.endswith(".localhost"):
            raise ValueError("Localhost endpoints are not allowed for MCP connections.")
        if hostname.endswith((".local", ".internal", ".lan", ".corp", ".home", ".test")):
            raise ValueError("Private-network MCP endpoints are not allowed.")
        try:
            ip = ipaddress.ip_address(hostname)
        except ValueError:
            ip = None
        if ip is not None and (ip.is_private or ip.is_loopback or ip.is_link_local or ip.is_reserved or ip.is_unspecified):
            raise ValueError("Private-network MCP endpoints are not allowed.")

    def _authorization_id(self, mcp_connection_id: str, actor_id: str) -> str:
        return f"{mcp_connection_id}:{actor_id}"

    def _resolve_authorization(
        self,
        mcp_connection_id: str,
        *,
        actor_id: str,
        require_active: bool,
        tenant_id: str | None = None,
        project_id: str | None = None,
    ) -> Dict[str, Any]:
        authorization = self._get_authorization_record(
            mcp_connection_id,
            actor_id=actor_id,
            tenant_id=tenant_id,
            project_id=project_id,
        )
        if authorization is None:
            raise HTTPException(status_code=409, detail="The current actor has not authorized this MCP connection yet.")
        payload = dict(authorization.get("payload") or {})
        if require_active and str(payload.get("status") or "") != "authorized":
            raise HTTPException(status_code=409, detail="The current actor must re-authorize this MCP connection.")
        return payload

    def _get_authorization_record(
        self,
        mcp_connection_id: str,
        *,
        actor_id: str,
        tenant_id: str | None = None,
        project_id: str | None = None,
    ) -> Dict[str, Any] | None:
        return self.repository.get_resource(
            MCP_AUTHORIZATION_RESOURCE_TYPE,
            self._authorization_id(mcp_connection_id, actor_id),
            tenant_id=tenant_id,
            project_id=project_id,
        )

    def _upsert_authorization(
        self,
        *,
        mcp_connection_id: str,
        actor_id: str,
        patch: Dict[str, Any],
        tenant_id: str | None = None,
        project_id: str | None = None,
    ) -> Dict[str, Any]:
        current = dict(
            (
                self._get_authorization_record(
                    mcp_connection_id,
                    actor_id=actor_id,
                    tenant_id=tenant_id,
                    project_id=project_id,
                )
                or {}
            ).get("payload")
            or {}
        )
        merged = {**current, **dict(patch or {})}
        merged["mcp_connection_id"] = mcp_connection_id
        merged["actor_id"] = actor_id
        if merged.get("tokens") is not None:
            merged["tokens"] = secure_inline_secret_values(dict(merged.get("tokens") or {}), secret_fields=MCP_SECRET_FIELDS)
        record = self.repository.upsert_resource(
            MCP_AUTHORIZATION_RESOURCE_TYPE,
            self._authorization_id(mcp_connection_id, actor_id),
            status=str(merged.get("status") or "authorized"),
            name=mcp_connection_id,
            payload=merged,
            tenant_id=tenant_id,
            project_id=project_id,
        )
        return dict(record.get("payload") or {})

    def _get_oauth_state_by_value(self, state: str) -> Dict[str, Any]:
        for record in self.repository.list_resources(MCP_OAUTH_STATE_RESOURCE_TYPE, include_all_tenants=True):
            payload = dict(record.get("payload") or {})
            if str(payload.get("state") or "") == state:
                return record
        raise HTTPException(status_code=404, detail="The OAuth state is invalid or expired.")

    def _get_connection_payload(
        self,
        mcp_connection_id: str,
        *,
        tenant_id: str | None = None,
        project_id: str | None = None,
    ) -> Dict[str, Any]:
        record = self.repository.get_resource(
            MCP_CONNECTION_RESOURCE_TYPE,
            mcp_connection_id,
            tenant_id=tenant_id,
            project_id=project_id,
        )
        if record is None:
            raise KeyError(mcp_connection_id)
        return dict(record.get("payload") or {})

    def _to_connection_response(self, record: Dict[str, Any] | None, *, actor_id: str) -> Dict[str, Any]:
        if record is None:
            return {}
        payload = dict(record.get("payload") or {})
        auth_record = self._get_authorization_record(str(payload.get("mcp_connection_id") or record.get("resource_id") or ""), actor_id=actor_id)
        auth_payload = dict((auth_record or {}).get("payload") or {})
        token_payload = materialize_secret_refs(dict(auth_payload.get("tokens") or {}), secret_fields=MCP_SECRET_FIELDS) if auth_payload.get("tokens") else {}
        discovered_tools = list(payload.get("discovered_tools") or [])
        return {
            "mcp_connection_id": payload.get("mcp_connection_id") or record.get("resource_id"),
            "name": payload.get("name") or record.get("name"),
            "preset_key": payload.get("preset_key") or "custom",
            "endpoint_url": payload.get("endpoint_url") or "",
            "transport_type": payload.get("transport_type") or "streamable_http",
            "auth_mode": payload.get("auth_mode") or "oauth_authorization_code",
            "status": record.get("status") or payload.get("status") or "active",
            "allowed_tools": list(payload.get("allowed_tools") or []),
            "discovered_tools": discovered_tools,
            "discovered_tool_count": len(discovered_tools),
            "authorization": {
                "actor_id": actor_id,
                "status": str(auth_payload.get("status") or "not_authorized"),
                "authorized_at": auth_payload.get("authorized_at"),
                "expires_at": auth_payload.get("expires_at"),
                "has_refresh_token": bool(str(token_payload.get("refresh_token") or "").strip()),
                "last_error": str(auth_payload.get("last_error") or ""),
            },
            "last_discovered_at": payload.get("last_discovered_at"),
            "last_validated_at": payload.get("last_validated_at"),
            "tenant_id": record.get("tenant_id") or payload.get("tenant_id"),
            "project_id": record.get("project_id") or payload.get("project_id"),
            "created_by": record.get("created_by") or payload.get("created_by") or "system",
            "updated_by": record.get("updated_by") or payload.get("updated_by") or "system",
            "correlation_id": record.get("correlation_id") or payload.get("correlation_id") or "",
            "created_at": record.get("created_at"),
            "updated_at": record.get("updated_at"),
        }

    def _to_snapshot_response(self, record: Dict[str, Any]) -> Dict[str, Any]:
        payload = dict(record.get("payload") or {})
        rows = [dict(item) for item in list(payload.get("rows") or []) if isinstance(item, dict)]
        return {
            "snapshot_id": payload.get("snapshot_id") or record.get("resource_id"),
            "mcp_connection_id": payload.get("mcp_connection_id") or "",
            "name": payload.get("name") or record.get("name"),
            "question": payload.get("question") or "",
            "answer": payload.get("answer") or "",
            "row_count": int(payload.get("row_count") or len(rows)),
            "identifier_fields": list(payload.get("identifier_fields") or []),
            "rows_preview": rows[:10],
            "status": record.get("status") or payload.get("status") or "ready",
            "tenant_id": record.get("tenant_id") or payload.get("tenant_id"),
            "project_id": record.get("project_id") or payload.get("project_id"),
            "created_by": record.get("created_by") or payload.get("created_by") or "system",
            "updated_by": record.get("updated_by") or payload.get("updated_by") or "system",
            "correlation_id": record.get("correlation_id") or payload.get("correlation_id") or "",
            "created_at": record.get("created_at"),
            "updated_at": record.get("updated_at"),
        }

    def _persist_client_info(self, mcp_connection_id: str, *, client_info, oauth_metadata, protected_resource_metadata) -> None:
        record = self.repository.get_resource(MCP_CONNECTION_RESOURCE_TYPE, mcp_connection_id)
        if record is None:
            raise KeyError(mcp_connection_id)
        payload = dict(record.get("payload") or {})
        tenant_id = str(record.get("tenant_id") or payload.get("tenant_id") or "").strip() or None
        project_id = str(record.get("project_id") or payload.get("project_id") or "").strip() or None
        client_info_payload = secure_inline_secret_values(client_info.model_dump(mode="json"), secret_fields=MCP_SECRET_FIELDS)
        payload["client_info"] = client_info_payload
        payload["oauth_metadata"] = oauth_metadata.model_dump(mode="json")
        payload["protected_resource_metadata"] = (
            protected_resource_metadata.model_dump(mode="json")
            if protected_resource_metadata is not None
            else {}
        )
        self.repository.upsert_resource(
            MCP_CONNECTION_RESOURCE_TYPE,
            mcp_connection_id,
            status=str(payload.get("status") or "active"),
            name=payload.get("name"),
            payload=payload,
            tenant_id=tenant_id,
            project_id=project_id,
        )

    def _materialize_client_info(self, connection: Dict[str, Any]):
        raw = dict(connection.get("client_info") or {})
        if not raw:
            return None
        resolved = materialize_secret_refs(raw, secret_fields=MCP_SECRET_FIELDS)
        mcp_auth = self._import_mcp_auth()
        return mcp_auth["OAuthClientInformationFull"].model_validate(resolved)

    @staticmethod
    def _classify_tool(tool_name: str) -> str:
        normalized = str(tool_name or "").strip().lower()
        if not normalized:
            return "blocked"
        if normalized.startswith(BLOCKED_TOOL_PREFIXES):
            return "blocked"
        if normalized.startswith(SAFE_TOOL_PREFIXES):
            return "read_only"
        return "blocked"

    def _import_mcp_auth(self) -> Dict[str, Any]:
        try:
            from mcp.client.auth import OAuthClientProvider, PKCEParameters  # noqa: F401
            from mcp.client.auth.oauth2 import OAuthContext
            from mcp.client.auth.utils import (
                build_oauth_authorization_server_metadata_discovery_urls,
                build_protected_resource_metadata_discovery_urls,
                create_client_registration_request,
                create_oauth_metadata_request,
                get_client_metadata_scopes,
                handle_auth_metadata_response,
                handle_protected_resource_response,
                handle_registration_response,
                handle_token_response_scopes,
            )
            from mcp.shared.auth import OAuthClientInformationFull, OAuthClientMetadata
            from mcp.shared.auth_utils import resource_url_from_server_url
        except ModuleNotFoundError as exc:  # pragma: no cover
            raise HTTPException(status_code=500, detail="The Python `mcp` SDK is not installed.") from exc
        return {
            "OAuthClientMetadata": OAuthClientMetadata,
            "OAuthClientInformationFull": OAuthClientInformationFull,
            "OAuthContext": OAuthContext,
            "OAuthClientProvider": OAuthClientProvider,
            "PKCEParameters": PKCEParameters,
            "build_protected_resource_metadata_discovery_urls": build_protected_resource_metadata_discovery_urls,
            "build_oauth_authorization_server_metadata_discovery_urls": build_oauth_authorization_server_metadata_discovery_urls,
            "create_client_registration_request": create_client_registration_request,
            "create_oauth_metadata_request": create_oauth_metadata_request,
            "get_client_metadata_scopes": get_client_metadata_scopes,
            "handle_auth_metadata_response": handle_auth_metadata_response,
            "handle_protected_resource_response": handle_protected_resource_response,
            "handle_registration_response": handle_registration_response,
            "handle_token_response_scopes": handle_token_response_scopes,
            "resource_url_from_server_url": resource_url_from_server_url,
        }

    @asynccontextmanager
    async def _open_mcp_session(self, connection: Dict[str, Any], *, auth_payload: Dict[str, Any]):
        mcp_auth = self._import_mcp_auth()
        try:
            from mcp.client import ClientSession
            from mcp.client.streamable_http import streamable_http_client
        except ModuleNotFoundError as exc:  # pragma: no cover
            raise HTTPException(status_code=500, detail="The Python `mcp` SDK is not installed.") from exc

        tenant_id = str(connection.get("tenant_id") or "").strip() or None
        project_id = str(connection.get("project_id") or "").strip() or None
        connection_payload = self._get_connection_payload(
            str(connection.get("mcp_connection_id") or ""),
            tenant_id=tenant_id,
            project_id=project_id,
        )
        callback_url = "https://example.invalid/oauth/callback"
        client_metadata = mcp_auth["OAuthClientMetadata"](
            redirect_uris=[callback_url],
            client_name="KairyxAI",
            grant_types=["authorization_code", "refresh_token"],
            response_types=["code"],
            token_endpoint_auth_method="none",
        )
        storage = _RepositoryTokenStorage(
            repository=self.repository,
            connection_payload=connection_payload,
            auth_payload=auth_payload,
            mcp_connection_id=str(connection.get("mcp_connection_id") or ""),
            actor_id=str(auth_payload.get("actor_id") or ""),
            tenant_id=tenant_id,
            project_id=project_id,
        )
        auth = mcp_auth["OAuthClientProvider"](
            str(connection.get("endpoint_url") or ""),
            client_metadata=client_metadata,
            storage=storage,
            redirect_handler=None,
            callback_handler=None,
        )
        async with httpx.AsyncClient(auth=auth, follow_redirects=True, timeout=30.0) as http_client:
            async with streamable_http_client(str(connection.get("endpoint_url") or ""), http_client=http_client) as transport:
                read_stream, write_stream, *_ = transport
                async with ClientSession(read_stream, write_stream) as session:
                    await session.initialize()
                    yield session


class _RepositoryTokenStorage:
    def __init__(
        self,
        *,
        repository,
        connection_payload: Dict[str, Any],
        auth_payload: Dict[str, Any],
        mcp_connection_id: str,
        actor_id: str,
        tenant_id: str | None = None,
        project_id: str | None = None,
    ):
        self.repository = repository
        self.connection_payload = dict(connection_payload or {})
        self.auth_payload = dict(auth_payload or {})
        self.mcp_connection_id = str(mcp_connection_id or "")
        self.actor_id = str(actor_id or "")
        self.tenant_id = str(tenant_id or "").strip() or None
        self.project_id = str(project_id or "").strip() or None

    async def get_tokens(self):
        tokens = dict(self.auth_payload.get("tokens") or {})
        if not tokens:
            return None
        materialized = materialize_secret_refs(tokens, secret_fields=MCP_SECRET_FIELDS)
        from mcp.shared.auth import OAuthToken

        return OAuthToken.model_validate(materialized)

    async def set_tokens(self, tokens) -> None:
        current = dict(
            (
                self.repository.get_resource(
                    MCP_AUTHORIZATION_RESOURCE_TYPE,
                    f"{self.mcp_connection_id}:{self.actor_id}",
                    tenant_id=self.tenant_id,
                    project_id=self.project_id,
                )
                or {}
            ).get("payload")
            or {}
        )
        current.update(
            {
                "mcp_connection_id": self.mcp_connection_id,
                "actor_id": self.actor_id,
                "status": "authorized",
                "authorized_at": current.get("authorized_at") or datetime.utcnow().isoformat(),
                "last_error": "",
                "tokens": secure_inline_secret_values(tokens.model_dump(mode="json"), secret_fields=MCP_SECRET_FIELDS),
            }
        )
        self.repository.upsert_resource(
            MCP_AUTHORIZATION_RESOURCE_TYPE,
            f"{self.mcp_connection_id}:{self.actor_id}",
            status="authorized",
            name=self.mcp_connection_id,
            payload=current,
            tenant_id=self.tenant_id,
            project_id=self.project_id,
        )
        self.auth_payload = current

    async def get_client_info(self):
        raw = dict(self.connection_payload.get("client_info") or {})
        if not raw:
            return None
        materialized = materialize_secret_refs(raw, secret_fields=MCP_SECRET_FIELDS)
        from mcp.shared.auth import OAuthClientInformationFull

        return OAuthClientInformationFull.model_validate(materialized)

    async def set_client_info(self, client_info) -> None:
        current = dict(
            (
                self.repository.get_resource(
                    MCP_CONNECTION_RESOURCE_TYPE,
                    self.mcp_connection_id,
                    tenant_id=self.tenant_id,
                    project_id=self.project_id,
                )
                or {}
            ).get("payload")
            or {}
        )
        current["client_info"] = secure_inline_secret_values(client_info.model_dump(mode="json"), secret_fields=MCP_SECRET_FIELDS)
        self.repository.upsert_resource(
            MCP_CONNECTION_RESOURCE_TYPE,
            self.mcp_connection_id,
            status=str(current.get("status") or "active"),
            name=current.get("name"),
            payload=current,
            tenant_id=self.tenant_id,
            project_id=self.project_id,
        )
        self.connection_payload = current
