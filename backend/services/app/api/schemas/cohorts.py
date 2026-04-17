from __future__ import annotations

from typing import Any, Dict, List

from pydantic import BaseModel, Field


class CohortCreateRequest(BaseModel):
    name: str
    type: str
    definition: Dict[str, Any] = Field(default_factory=dict)
    refresh_mode: str = "manual"
    owner: str = "system"
    description: str = ""
    tags: List[str] = Field(default_factory=list)
    activate: bool = False


class CohortActionRequest(BaseModel):
    force: bool = False


class CohortUpdateRequest(BaseModel):
    name: str | None = None
    type: str | None = None
    definition: Dict[str, Any] | None = None
    refresh_mode: str | None = None
    owner: str | None = None
    description: str | None = None
    tags: List[str] | None = None


class CohortResponse(BaseModel):
    cohort_id: str
    name: str
    type: str
    status: str
    tenant_id: str | None = None
    project_id: str | None = None
    created_by: str = "system"
    updated_by: str = "system"
    correlation_id: str = ""
    refresh_mode: str
    owner: str
    description: str = ""
    version: int
    version_id: int
    member_count: int = 0
    last_refreshed_at: str | None = None
    deleted_at: str | None = None
    definition: Dict[str, Any] = Field(default_factory=dict)
    tags: List[str] = Field(default_factory=list)
    preview_members: List[Dict[str, Any]] = Field(default_factory=list)
    delta: Dict[str, int] = Field(default_factory=dict)
    refresh_policy: Dict[str, Any] = Field(default_factory=dict)
    activation_preflight: Dict[str, Any] = Field(default_factory=dict)
    metrics_summary: Dict[str, Any] = Field(default_factory=dict)
    source_kind: str | None = None
    source_label: str | None = None
    source_summary: Dict[str, Any] = Field(default_factory=dict)
    created_at: str
    updated_at: str


class CohortMemberPage(BaseModel):
    page: int
    page_size: int
    total: int
    items: List[Dict[str, Any]] = Field(default_factory=list)


class CohortVersionListResponse(BaseModel):
    items: List[Dict[str, Any]] = Field(default_factory=list)


class CohortBuilderCondition(BaseModel):
    field: str
    op: str = "="
    value: Any = None
    values: List[Any] = Field(default_factory=list)
    value_type: str | None = None


class CohortBuilderRequest(BaseModel):
    name: str = ""
    audience_basis: str = "prediction"
    prediction_scope: str = "source"
    source_names: List[str] = Field(default_factory=list)
    prediction_job_ids: List[str] = Field(default_factory=list)
    output_mode: str = "combined"
    refresh_mode: str = "manual"
    owner: str = "system"
    description: str = ""
    tags: List[str] = Field(default_factory=list)
    logic: str = "AND"
    conditions: List[CohortBuilderCondition] = Field(default_factory=list)
    members: List[Any] = Field(default_factory=list)
    sql: str = ""
    saved_query_id: str = ""
    connector_id: str = ""
    table_name: str = ""
    selected_columns: List[str] = Field(default_factory=list)
    where_sql: str = ""
    column_mapping: Dict[str, str] = Field(default_factory=dict)
    activate: bool = False
