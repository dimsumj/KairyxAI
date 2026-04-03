from __future__ import annotations

from typing import Any, Dict, Iterable, List


SUPPORTED_OPERATOR_TASKS = (
    "Summarize the dashboard",
    "Set up a cohort",
    "Set up an A/B test",
    "Set up a connection",
)


MODULE_LABELS = {
    "data-core": "Data Core",
    "audience-engine": "Audience Engine",
    "action-orchestrator": "Action Orchestrator",
    "experiment-hub": "Experiment Hub",
    "insight-copilot": "Insight Copilot",
}

PAGE_LABELS = {
    "operator-hub": "Data Core -> Churn Rescue",
    "player-cohorts": "Data Core -> Imports",
    "connectors": "Data Core -> Connectors",
    "data-sandbox": "Data Core -> Mappings",
    "action-history": "Data Core -> Audit Trail",
    "scenario-templates": "Data Core -> Templates",
    "service-health": "Data Core -> Health",
    "safety-rails": "Data Core -> Governance",
    "audience-engine": "Audience Engine",
    "action-orchestrator": "Action Orchestrator",
    "experiment-hub": "Experiment Hub",
    "insight-copilot": "Insight Copilot",
}

SAMPLE_TERMS = (
    "sample",
    "example",
    "payload",
    "json",
    "sql",
    "prompt",
)

TROUBLESHOOTING_TERMS = (
    "failing",
    "failed",
    "failure",
    "error",
    "not working",
    "issue",
    "403",
    "forbidden",
    "why is",
    "why did",
    "troubleshoot",
)

LOCATION_TERMS = (
    "where do i",
    "where can i",
    "where should i",
    "which page",
    "what page",
)

PAGE_GUIDANCE_TERMS = (
    "how do i use this",
    "how do i use this page",
    "what does this page do",
    "what does this do",
    "what is this page",
)

HELP_CATALOG: List[Dict[str, Any]] = [
    {
        "entry_id": "global_overview",
        "title": "End-to-end operator flow",
        "keywords": ("overview", "start", "flow", "operator", "console", "dashboard"),
        "overview": (
            "KairyxAI works best in this order: connect data, import and map it, build a cohort, attach the cohort to a workflow, "
            "configure an experiment, and use the AI assistant for summaries, setup help, and safe setup execution."
        ),
        "where_to_go": "Data Core -> Connectors / Imports, Audience Engine, Action Orchestrator, Experiment Hub, and the global AI assistant",
        "steps": (
            "Create upstream connectors or downstream provider connections in Data Core.",
            "Run imports and finish any blocked mappings before relying on downstream work.",
            "Create a draft cohort in Audience Engine and validate the member preview.",
            "Bind that cohort to a workflow in Action Orchestrator and publish only after preflight passes.",
            "Save the experiment config in Experiment Hub, inspect integrity, and start it only after review.",
        ),
        "example_prompts": (
            "Summarize the dashboard.",
            "Set up a connection for amplitude named prod_amplitude with api_key: demo_api_key and secret_key: demo_secret_key.",
            "Set up a SQL cohort named high_risk_users with SQL: SELECT user_id AS canonical_user_id, email FROM prediction_results WHERE predicted_churn_risk = 'high'",
            "Set up an A/B test named churn_holdout for cohort_id: cohort_demo with primary metric: return_rate",
        ),
    },
    {
        "entry_id": "role_guide",
        "title": "Role and permission guide",
        "keywords": ("role", "roles", "admin", "analyst", "operator", "403", "permission", "forbidden"),
        "overview": "Permissions are role-aware. Admin has full access, Analyst is read-heavy, and Operator is execution-heavy for safe operational tasks.",
        "where_to_go": "Settings and the active role switcher in the shell",
        "troubleshooting": (
            "If a request fails with 403, the active role is missing the permission for that operation.",
            "Use Admin while testing if you need unmasked data or full write access.",
            "Analyst can read summaries and metrics, but write actions such as connector setup can be blocked.",
        ),
    },
    {
        "entry_id": "data_core_connectors",
        "title": "Connector and provider connection setup",
        "modules": ("data-core",),
        "pages": ("connectors",),
        "keywords": (
            "connector",
            "connection",
            "amplitude",
            "adjust",
            "appsflyer",
            "bigquery",
            "google",
            "provider connection",
            "braze",
            "sendgrid",
            "webhook",
            "simulator",
        ),
        "overview": (
            "Use this page to create upstream data connectors such as Amplitude, Adjust, AppsFlyer, BigQuery, or Google, "
            "and downstream provider connections such as Braze, SendGrid, webhook, or simulator."
        ),
        "where_to_go": "Data Core -> Connectors",
        "steps": (
            "Decide whether you need an upstream connector or a downstream provider connection.",
            "Enter a stable name and the provider-specific required fields.",
            "Save the connection. Upstream connectors can also run a health check after creation.",
            "Use Imports for ingest sources, or Action Orchestrator and exports for downstream delivery once the connection is ready.",
        ),
        "troubleshooting": (
            "A connector health check failure usually means the credentials or endpoint are missing or invalid.",
            "If the save is blocked with 403, the current role is missing connectors or provider connection write access.",
            "If Imports does not show the source, verify the connector was created in the active project and tenant.",
        ),
        "samples": (
            {
                "title": "Amplitude connector sample",
                "language": "json",
                "code": '{\n  "name": "prod_amplitude",\n  "connection_scope": "connector",\n  "connection_type": "amplitude",\n  "config": {\n    "api_key": "demo_api_key",\n    "secret_key": "demo_secret_key"\n  }\n}',
            },
            {
                "title": "Webhook provider connection sample",
                "language": "json",
                "code": '{\n  "name": "lifecycle_webhook",\n  "connection_scope": "provider_connection",\n  "connection_type": "webhook",\n  "config": {\n    "webhook_url": "https://example.com/hooks/churn"\n  }\n}',
            },
        ),
        "example_prompts": (
            "How do I create an Amplitude connector here?",
            "Give me a sample payload for a webhook provider connection.",
            "Set up an amplitude connector named prod_amplitude with api_key: demo_api_key and secret_key: demo_secret_key.",
        ),
    },
    {
        "entry_id": "data_core_imports",
        "title": "Imports and mappings",
        "modules": ("data-core",),
        "pages": ("player-cohorts", "data-sandbox"),
        "keywords": ("import", "imports", "mapping", "mappings", "awaiting mapping", "dataset"),
        "overview": (
            "Imports bring source data into the workspace. If an import stops in awaiting mapping, finish the mapping coverage in Data Core -> Mappings before retrying the job."
        ),
        "where_to_go": "Data Core -> Imports and Data Core -> Mappings",
        "steps": (
            "Choose the source and import range in Data Core -> Imports.",
            "If the import pauses in awaiting mapping, open Data Core -> Mappings and complete the JSON field mapping.",
            "Retry the import once mapping coverage is complete.",
        ),
        "troubleshooting": (
            "No processed datasets usually means there is no completed import yet.",
            "If the import is blocked on mapping, downstream cohort or experiment work will be incomplete.",
        ),
    },
    {
        "entry_id": "audience_engine",
        "title": "Cohort setup and audience workflow",
        "modules": ("audience-engine",),
        "pages": ("audience-engine",),
        "keywords": ("cohort", "audience", "sql cohort", "rule cohort", "list cohort", "members", "refresh", "sql workspace"),
        "overview": (
            "Audience Engine creates draft cohorts from SQL, rule definitions, or explicit member lists. SQL cohorts should preview the query first, save it if needed, and then create the cohort as a draft."
        ),
        "where_to_go": "Audience Engine -> Create Cohort or Audience Engine -> SQL Workspace",
        "steps": (
            "Choose whether the cohort should be SQL, rule-based, or a member list.",
            "For SQL cohorts, preview the SQL first and save the query if you want the definition persisted.",
            "Create the cohort as a draft and validate the member count before activation.",
            "Activate the cohort only when you are ready to use it in workflows or experiments.",
        ),
        "troubleshooting": (
            "If cohort activation or workflow publish fails, check member_count, activation_preflight, and whether the cohort is still draft or paused.",
            "If a refresh looks stale, inspect the latest cohort metrics and rerun the relevant SQL or import pipeline.",
        ),
        "samples": (
            {
                "title": "SQL cohort sample",
                "language": "sql",
                "code": "SELECT user_id AS canonical_user_id, email\nFROM prediction_results\nWHERE predicted_churn_risk = 'high'\n  AND COALESCE(churn_state, 'active') != 'churned'",
            },
            {
                "title": "Rule cohort sample",
                "language": "json",
                "code": '{\n  "source_alias": "mart_user_daily",\n  "logic": "AND",\n  "conditions": [\n    { "field": "days_since_last_seen", "op": ">=", "value": 3 },\n    { "field": "sessions_7d", "op": "<=", "value": 2 }\n  ]\n}',
            },
            {
                "title": "List cohort sample",
                "language": "json",
                "code": '{\n  "members": [\n    { "canonical_user_id": "u_1001", "email": "u1001@example.com" },\n    { "canonical_user_id": "u_1002", "email": "u1002@example.com" }\n  ]\n}',
            },
        ),
        "example_prompts": (
            "Give me a SQL cohort sample for high-risk players.",
            "How do I create a rule cohort here?",
            "Set up a SQL cohort named high_risk_users with SQL: SELECT user_id AS canonical_user_id, email FROM prediction_results WHERE predicted_churn_risk = 'high'",
        ),
    },
    {
        "entry_id": "action_orchestrator",
        "title": "Workflow setup and runtime controls",
        "modules": ("action-orchestrator",),
        "pages": ("action-orchestrator",),
        "keywords": ("workflow", "publish", "runtime", "delivery", "manual_test", "daily_schedule", "callback", "outcome"),
        "overview": (
            "Action Orchestrator binds a cohort to a delivery workflow, lets you test it in sandbox, and records deliveries and provider callbacks into durable execution logs."
        ),
        "where_to_go": "Action Orchestrator -> Workflow Studio and Runtime Controls",
        "steps": (
            "Create the workflow with a cohort, experiment_id, trigger, action payload, and policy.",
            "Test the workflow in sandbox before publishing it.",
            "Publish only when preflight passes and the linked cohort is active.",
            "Use Runtime Controls and Deliveries to inspect execution and callback state.",
        ),
        "troubleshooting": (
            "Publish preflight requires an active cohort, non-empty content, a supported trigger type, and an experiment id.",
            "Run Due Workflows returns nothing unless the workflow is already published and the scheduled time is due.",
        ),
        "samples": (
            {
                "title": "Workflow sample",
                "language": "json",
                "code": '{\n  "name": "daily_churn_rescue",\n  "cohort_id": "cohort_xxx",\n  "experiment_id": "churn_rescue_v1",\n  "trigger": { "type": "daily_schedule", "hour": 10, "minute": 0 },\n  "action": {\n    "channel": "push_notification",\n    "content": "Come back for a reward."\n  },\n  "policy": {\n    "global_daily_limit": 5,\n    "channel_daily_limit": 5,\n    "cooldown_hours": 24,\n    "blacklist_ids": [],\n    "quiet_hours": { "start": 22, "end": 7 }\n  },\n  "budget_policy": {\n    "daily_budget_limit": 25\n  },\n  "requires_confirmation": false\n}',
            },
            {
                "title": "Outcome ingestion sample",
                "language": "json",
                "code": '{\n  "outcomes": [\n    {\n      "workflow_id": "wf_xxx",\n      "cohort_id": "cohort_xxx",\n      "experiment_id": "churn_rescue_v1",\n      "user_id": "u_1001",\n      "occurred_at": "2026-03-10T11:00:00",\n      "group": "treatment",\n      "outcome_name": "returned",\n      "source": "internal_writeback",\n      "metadata": { "channel": "push_notification" }\n    }\n  ]\n}',
            },
        ),
    },
    {
        "entry_id": "experiment_hub",
        "title": "Experiment configuration and decision flow",
        "modules": ("experiment-hub",),
        "pages": ("experiment-hub",),
        "keywords": ("experiment", "a/b", "ab test", "holdout", "guardrail", "sample size", "runtime", "decision", "integrity"),
        "overview": (
            "Experiment Hub saves explicit treatment-vs-holdout experiment configs, surfaces summary and integrity checks, and records decisions after the runtime and sample thresholds are met."
        ),
        "where_to_go": "Experiment Hub -> Experiment Control and Summary",
        "steps": (
            "Provide experiment_id, cohort_id, primary metric, guardrails, runtime threshold, sample size threshold, and split settings.",
            "Save the config in a non-running state first.",
            "Review the summary and integrity panels before starting the experiment.",
            "Record a decision only after the sample, runtime, guardrail, and SRM checks are acceptable.",
        ),
        "troubleshooting": (
            "If the experiment stays inconclusive, inspect sample_size, runtime_hours, SRM status, and guardrail results.",
            "Start and stop actions are confirmation-gated because they are high risk operational changes.",
        ),
        "samples": (
            {
                "title": "Experiment config sample",
                "language": "json",
                "code": '{\n  "experiment_id": "churn_rescue_v1",\n  "cohort_id": "cohort_xxx",\n  "primary_metric": "return_rate",\n  "guardrail_metrics": ["engagement_rate", "policy_block_rate"],\n  "min_sample_size": 500,\n  "min_runtime_hours": 72,\n  "holdout_pct": 0.1,\n  "b_variant_pct": 0.4\n}',
            },
        ),
        "example_prompts": (
            "How do I set up an A/B test here?",
            "Give me a sample experiment config payload.",
            "Set up an A/B test named churn_holdout for cohort_id: cohort_demo with primary metric: return_rate",
        ),
    },
    {
        "entry_id": "copilot_manual_tools",
        "title": "Manual Copilot tools",
        "modules": ("insight-copilot",),
        "pages": ("insight-copilot",),
        "keywords": ("copilot", "query", "explain", "recommend", "report", "evidence", "query log"),
        "overview": (
            "The global assistant bubble is the primary AI surface. The Insight Copilot page now acts as the advanced manual fallback for direct Query, Explain, Recommend, Report, and Evidence & Logs workflows."
        ),
        "where_to_go": "Insight Copilot -> Query, Explain, Recommend, Report, and Evidence & Logs",
        "steps": (
            "Use the global assistant when you want guided setup, grounded help, or safe task execution from any page.",
            "Use Query when you want a direct natural-language metric request.",
            "Use Explain, Recommend, and Report when you want the raw analytical workflows and evidence envelope.",
        ),
        "samples": (
            {
                "title": "Query examples",
                "language": "text",
                "code": "how many high risk users do we have in 7d?\nhow many payers do we have?\nwhat is total revenue in 7d?",
            },
            {
                "title": "Explain example",
                "language": "text",
                "code": "metric_id: promo_views\ntime_window: 7d\ndimensions: campaign,country,platform",
            },
        ),
        "example_prompts": (
            "What manual tool should I use for this question?",
            "Give me sample prompts for Query or Explain.",
        ),
    },
    {
        "entry_id": "common_issues",
        "title": "Common issues",
        "keywords": ("failing", "failed", "issue", "issues", "troubleshoot", "broken", "403", "masked"),
        "overview": "The most common failures are blocked imports, inactive cohorts, publish preflight gaps, masked data during testing, and insufficient experiment evidence.",
        "troubleshooting": (
            "No processed datasets in Data Core usually means the import did not finish or the mapping coverage is incomplete.",
            "Cohort activation or workflow publish failures usually come from activation_preflight issues or a still-draft cohort.",
            "Masked delivery payloads usually mean the active role is Analyst or Operator instead of Admin.",
            "Copilot evidence gaps usually mean the warehouse aliases or curated source data are still incomplete.",
        ),
    },
]


def build_help_support_answer(message: str, *, ui_context: Dict[str, Any] | None = None) -> str:
    lowered = str(message or "").strip().lower()
    context = dict(ui_context or {})
    entries = _match_help_entries(lowered, context)
    primary = entries[0] if entries else _entry_by_id("global_overview")
    current_context = _current_context_label(context)
    wants_samples = any(term in lowered for term in SAMPLE_TERMS)
    wants_troubleshooting = any(term in lowered for term in TROUBLESHOOTING_TERMS)
    wants_location = any(term in lowered for term in LOCATION_TERMS)
    wants_page_guidance = any(term in lowered for term in PAGE_GUIDANCE_TERMS)

    sections: List[str] = []
    if current_context:
        sections.append(f"You are currently on `{current_context}`.")
    if primary.get("where_to_go") and (wants_location or wants_page_guidance or primary.get("pages")):
        sections.append(f"Use `{primary['where_to_go']}` for this workflow.")
    if primary.get("overview"):
        sections.append(str(primary["overview"]))

    steps = list(primary.get("steps") or [])
    if steps and (wants_page_guidance or wants_location or "how do i" in lowered or "how to" in lowered):
        sections.append(_render_bullet_block("Recommended steps", steps[:5]))

    samples = _pick_samples(primary, lowered)
    if wants_samples and samples:
        sections.append(_render_samples(samples[:2]))

    troubleshooting = _merge_troubleshooting(primary, entries[1:] if len(entries) > 1 else [])
    if wants_troubleshooting and troubleshooting:
        sections.append(_render_bullet_block("If it fails", troubleshooting[:4]))

    prompts = list(primary.get("example_prompts") or [])
    if (not steps and not wants_samples) or "prompt" in lowered or "example" in lowered:
        if prompts:
            sections.append(_render_bullet_block("Example prompts", [f"`{item}`" for item in prompts[:4]]))

    if not sections:
        sections.append("I can help with grounded product guidance, troubleshooting, and safe setup tasks.")

    if not any("Example prompts" in section for section in sections):
        sections.append(
            _render_bullet_block(
                "You can also ask",
                [f"`{item}`" for item in SUPPORTED_OPERATOR_TASKS],
            )
        )
    return "\n\n".join(section for section in sections if section.strip())


def _entry_by_id(entry_id: str) -> Dict[str, Any]:
    for entry in HELP_CATALOG:
        if entry.get("entry_id") == entry_id:
            return entry
    return HELP_CATALOG[0]


def _current_context_label(ui_context: Dict[str, Any]) -> str:
    page_id = str(ui_context.get("active_page_id") or "").strip()
    module_id = str(ui_context.get("active_module_id") or "").strip()
    if page_id and page_id in PAGE_LABELS:
        return PAGE_LABELS[page_id]
    return MODULE_LABELS.get(module_id, "")


def _match_help_entries(lowered_message: str, ui_context: Dict[str, Any]) -> List[Dict[str, Any]]:
    active_module = str(ui_context.get("active_module_id") or "").strip()
    active_page = str(ui_context.get("active_page_id") or "").strip()
    scored: List[tuple[int, Dict[str, Any]]] = []
    for entry in HELP_CATALOG:
        score = 0
        if active_module and active_module in entry.get("modules", ()):
            score += 18
        if active_page and active_page in entry.get("pages", ()):
            score += 26
        for keyword in entry.get("keywords", ()):
            if keyword and keyword in lowered_message:
                score += 8
        if not lowered_message and (active_page in entry.get("pages", ()) or active_module in entry.get("modules", ())):
            score += 12
        if score > 0:
            scored.append((score, entry))
    scored.sort(key=lambda item: (-item[0], str(item[1].get("title") or "")))
    if not scored:
        return [_entry_by_id("global_overview"), _entry_by_id("common_issues")]
    ordered = [item[1] for item in scored[:3]]
    if ordered[0].get("entry_id") != "global_overview":
        ordered.append(_entry_by_id("global_overview"))
    return ordered


def _pick_samples(entry: Dict[str, Any], lowered_message: str) -> List[Dict[str, Any]]:
    samples = list(entry.get("samples") or [])
    if not samples:
        return []
    filtered = [
        sample
        for sample in samples
        if any(keyword in lowered_message for keyword in (str(sample.get("title") or "").lower(), str(sample.get("language") or "").lower()))
    ]
    return filtered or samples


def _merge_troubleshooting(primary: Dict[str, Any], secondary_entries: Iterable[Dict[str, Any]]) -> List[str]:
    combined: List[str] = [str(item) for item in primary.get("troubleshooting") or [] if str(item).strip()]
    for entry in secondary_entries:
        for item in entry.get("troubleshooting") or []:
            text = str(item).strip()
            if text and text not in combined:
                combined.append(text)
    if not combined:
        common = _entry_by_id("common_issues")
        combined.extend(str(item) for item in common.get("troubleshooting") or [] if str(item).strip())
    return combined


def _render_bullet_block(title: str, items: Iterable[str]) -> str:
    lines = [f"{title}:"]
    for item in items:
        text = str(item).strip()
        if text:
            lines.append(f"- {text}")
    return "\n".join(lines)


def _render_samples(samples: Iterable[Dict[str, Any]]) -> str:
    rendered: List[str] = []
    for sample in samples:
        title = str(sample.get("title") or "Sample").strip()
        language = str(sample.get("language") or "").strip() or "text"
        code = str(sample.get("code") or "").strip()
        if not code:
            continue
        rendered.append(f"{title}:\n```{language}\n{code}\n```")
    return "\n\n".join(rendered)
