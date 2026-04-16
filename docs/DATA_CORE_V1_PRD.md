# KairyxAI Data Core v1 PRD

## 4.0 Connector Management (Ownership Clarification)

### Goal
Own connector configuration, health checks, freshness, and latest-ingestion status under Data Core as the entry control plane for all ingestion, mapping, and SQL capabilities.

### Functional Requirements
- Save multiple connector configurations
- List available connectors and data sources
- Support connector deletion
- Support health checks
- Show freshness, `last_ingestion_status`, and `last_ingestion_at`
- Surface connector setup as the first-run entry action from the main workbench and the first item inside Data Core navigation

### Ownership Notes
- In the current repository, connector control-plane functionality and source freshness belong to Data Core
- Production-grade governance for connector secrets is still owned by the Master PRD's Production Readiness workstream

---

## 4.1 Direct Warehouse Query and Data-Source Access

### Goal
After connecting a Data Lake or database such as BigQuery, allow users to query directly at the warehouse layer and materialize query results into cohorts with one click.

### Functional Requirements

#### FR-4.1.1 Direct Data-Source Connectivity
- Support configuring and validating BigQuery connections, with Snowflake and Redshift as future expansion targets
- Support selecting project, dataset, and table or view
- Support read-only mode with least privilege
- BigQuery browser setup must accept tenant-provided service account credentials via upload or paste instead of assuming shared backend credentials for connector reads

#### FR-4.1.2 SQL Query Workspace
- Provide an executable SQL workspace with templates and parameters
- Support `where/filter`, time windows, and aggregation conditions
- Support result preview for the first N rows and row-count estimation
- Support saved query templates

#### FR-4.1.3 Query to Cohort Generation
- Query results must contain a unified primary key, either `canonical_user_id` or `player_id`
- Support one-click write into a cohort with name, description, source SQL, and creation time
- Support both static cohorts (snapshot) and dynamic cohorts (scheduled refresh)

#### FR-4.1.4 Cohort Management
- Cohort list, audience size, and most recent refresh time
- Versioning support for iterative updates of the same cohort name
- Export and downstream invocation for Engage and Experiment

#### FR-4.1.5 Security and Governance
- SQL whitelist and read-only restrictions that block DDL and DML
- Query timeout and scan-limit controls to avoid excessive query cost
- Audit logs showing who ran which SQL and generated which cohort

### v1 Definition of Done
1. After BigQuery connectivity is configured, SQL can be executed in the UI and return a preview
2. SQL results containing user identifiers can be turned into a cohort with one click
3. The resulting cohort can be consumed directly by `predict / engage / experiment`
4. At least one dynamic cohort supports scheduled refresh, such as once per day
5. Query audit logs and failure traceability are available

### 4.1.6 Backend Runtime and Persistence Ownership
- Under `operator-api`, `/api/v1/connectors`, `/mappings`, `/imports`, and `/predictions` form the main Data Core backend control-plane surface
- `import-worker` owns connector paging, checkpoint-aware import execution, raw-shard publishing, and recovery
- `dataflow` owns normalization from `raw shard -> manifest -> standardized -> unified`
- `prediction-worker` owns predictions on top of the unified aggregate layer and writes results into BigQuery-backed serving / result storage
- Prediction-result reads must support pagination to avoid unbounded reads
- Data Core owns at least these persisted control-plane entities:
  - `connector configuration`
  - `field mapping`
  - `import job`
  - `prediction job`
  - `ingestion checkpoint`

### 4.1.7 Local Model Readiness + Baseline Strategy

#### Goal
Keep the `Local Model` prediction path always available and interpretable within the v1 batch / nearline and human-in-the-loop boundaries, while making it explicit whether the operator is using a learned local model or the `heuristic_v1` baseline.

#### Strategy Boundaries
- `heuristic_v1` is the default and always-available general local baseline model in v1
- The local supervised churn model retrains in batch from accumulated holdout / untreated rows and observed return / purchase outcomes
- A newly trained model can only be promoted to active when `validation_accuracy >= heuristic_accuracy`
- No online incremental learning is introduced in v1
- `prediction_mode=local` remains runnable when the model is not ready, but it must be explicitly marked as `heuristic_v1` fallback

#### Control-plane / API Contract
- `GET /api/v1/predictions/models/runs` must return a normalized `readiness` object:
  - `state`: `untrained | learning | fallback | ready`
  - `using_model_version`
  - `reason`
  - `last_trained_at`
  - `baseline_rows`
  - `min_rows_required`
  - `class_balance`
  - `validation_accuracy`
  - `heuristic_accuracy`
- The same endpoint must also return `training_status`, including:
  - `status`
  - `stage`
  - `started_at`
  - `trained_at`
  - `row_count`
  - `class_balance`
  - `min_rows_required`
- `prediction job` progress details and completed metadata must explicitly include:
  - `effective_local_model_version`
  - `effective_local_model_state`
- Prediction result storage must retain the effective local model metadata for UI rendering, exports, and audit lookup

#### Module Ownership Boundaries
- Data Core owns:
  - control-plane persistence for local churn model artifacts, versions, and training status
  - the readiness API contract
  - the prediction job / result metadata contract
- Experiment Hub provides:
  - holdout / treatment exposure history
  - outcome logging and attributed return / purchase signals
- Audience / Action consume prediction results and do not own readiness inference logic

#### Operator UX Contract (v1)
- The Operator Console must show a local-model status badge beside the prediction engine selector: `Ready / Learning / Fallback`
- When `Local Model` is not ready, the UI must clearly warn that `heuristic_v1` fallback is being used
- The Operator Console must provide an explicit `Train Local Model` control that triggers local batch retraining without leaving the churn workbench
- The Operator Console must provide a `Refresh Model Status` control and an inline training-status line showing the latest training outcome, labeled-row count, class balance, and last update time
- Completed prediction jobs must show the actual local model version and state used so cached results remain interpretable

#### Definition of Done
1. `Local Model` remains runnable without an active learned model and falls back to `heuristic_v1`
2. Readiness state is available through a single API contract, without frontend inference
3. Prediction jobs and result rows expose the actual local model version and state used
4. The operator UI clearly distinguishes `learning` and `fallback` from a truly `ready` local model
5. Operators can manually trigger and inspect local model retraining from the workbench

### 4.1.8 Source-First Prediction Audience Selection

#### Goal
Reduce operator friction in the churn workbench by allowing prediction to run against a source such as `Amplitude 1`, while preserving a concrete resolved import snapshot for reproducibility, exports, and audit.

#### Strategy Boundaries
- `Source` is the default operator audience mode in v1
- `Import` remains available as an explicit override for debugging, audit review, and backfill comparison
- `Source` mode resolves to the latest completed import for that source when the prediction job starts
- Prediction roster selection still comes from the resolved import, not from unioning every historical import for the source
- Merged tenant history remains the feature source for churn scoring after the roster is selected

#### Control-plane / API Contract
- `POST /api/v1/predictions` accepts either:
  - `import_job_id` with `audience_scope=import`
  - `source_name` with `audience_scope=source`
- Prediction jobs must store:
  - `audience_scope`
  - `source_name` when source mode is used
  - the resolved `import_job_id` actually used for scoring
- Prediction progress metadata must expose:
  - `audience_scope`
  - `source_name`
  - `resolved_import_display_name`
- Source-mode jobs may update their resolved `import_job_id` at run start if a newer completed import exists for the same source

#### Operator UX Contract (v1)
- The churn workbench must show a `Prediction Target` selector with `Source` and `Import`
- In `Source` mode, the selector shows available sources backed by at least one completed import
- In `Import` mode, the selector shows completed imports directly
- The UI must explain that source mode resolves to the latest completed import when the job starts
- Cached completed predictions remain viewable, but stale jobs must still require explicit rerun confirmation

#### Definition of Done
1. Operators can run prediction by source without manually choosing among repeated imports from that source
2. Prediction jobs still record the concrete import snapshot that was scored
3. Import-mode prediction remains available and behaviorally unchanged for explicit import selection
4. Source-mode caching, active-job recovery, and export lookup all key off `audience_scope + audience_key`

### 4.1.9 Import Diagnostics On-Demand Loading

#### Goal
Keep the Imports page responsive after backend restart by deferring expensive diagnostics until the operator explicitly asks for them, while preserving full operational detail when needed.

#### Operator UX Contract (v1)
- Opening the Imports page must load the import list without automatically fetching `operations`, `quality`, `manifests`, or full schema-contract detail
- Import diagnostics load only when the operator clicks `Load Operations`, `Load Quality`, `Load Manifests`, `Load Contract`, or `List All`
- Automatic polling on the Imports page continues only while at least one import job is active (`queued`, `running`, or `stopping`)
- If the control plane is temporarily busy immediately after restart, the UI must surface a retryable busy message rather than an opaque failure

#### Runtime / Error-Handling Contract
- Startup-time SQLite lock contention must degrade to a retryable busy response for request paths that touch the control plane, including import diagnostics
- On-demand diagnostics remain auditable and do not change the underlying import-control-plane contract

#### Definition of Done
1. Initial Imports page load does not automatically trigger heavy import diagnostics
2. Completed-only import lists stop polling automatically
3. Busy-after-restart import detail reads degrade to a retryable busy experience instead of an opaque 500

### 4.1.10 Copilot Prediction Draft Integration

#### Goal
Let `Insight Copilot` use Data Core prediction jobs as the first step of a prompt-driven operator flow, while keeping prediction ownership, async job state, and result metadata inside Data Core.

#### Data Core Ownership
- Prediction jobs remain the system of record for prompt-driven prediction runs, whether the operator starts them from the churn workbench or from `Ask AI`
- Copilot may create or reuse prediction jobs, but it must do so through the existing prediction control-plane contract
- Prediction jobs must remain deep-linkable and resumable through artifact metadata rather than through browser-side AI state

#### Runtime / API Contract
- Copilot-created prediction jobs use the same `source` or `import` audience rules as manual prediction jobs
- The prediction job artifact exposed back to Copilot must include enough metadata for resume and deep linking:
  - `resource_type = prediction_job`
  - `resource_id`
  - `resume_ready`
  - `resume_message`
  - `status_detail`
- Copilot sessions may hold a pending flow while a prediction job is queued or running, but Data Core still owns the underlying job lifecycle and result persistence
- Draft SQL generation defaults to `prediction_results` semantics and requires `canonical_user_id` in the previewed result before downstream cohort creation can proceed

#### Definition of Done
1. Copilot can reuse or start prediction jobs without introducing a second prediction orchestration API
2. Prediction job artifacts are sufficient for deep-linking and async continuation in the Ask AI drawer
3. Prompt-driven SQL-to-cohort creation fails closed when the preview does not expose `canonical_user_id`

---

## 4.2 Multi-Source Ingestion and Stitching (P0)

> Priority: **P0 (Immediate)**

### Goal
Solve heterogeneous data ingestion, standardization, merge, dedupe, and identity stitching across multiple exported data sources, and produce a unified event layer that can be consumed directly by segmentation and strategy execution.

### 4.2.1 Canonical Event Contract
After all sources such as analytics platforms, MMPs, and game backends enter the standardized layer, they must map to a unified structure:
- `job_id`
- `source`
- `source_event_id`
- `player_id`
- `canonical_user_id` (filled after stitching)
- `event_type`
- `event_time`
- `event_properties`
- `user_properties`
- `ingested_at`
- `data_quality_flags`

### 4.2.2 Three-Layer Ingestion Architecture (P0)

#### Layer A: Raw Landing
- Store raw JSON by `source/date/job_id`
- Append-only, never overwrite, to support replay and audit

#### Layer B: Standardized
- Normalize field mapping, timezone and timestamp formats, and amount/currency conventions
- Mark quality problems such as `missing_player_id` and `invalid_event_time`
- Produce `stg_events_standardized`

#### Layer C: Unified
- Run dedupe and identity stitch
- Produce `fact_events_unified` as the only upstream source for higher layers

#### 4.2.2.1 Dual Runtime Modes (Overall Design)
Data Core must support two runtime modes over the long term. They share schema contracts and service interfaces, but do not have to share exactly the same runtime implementation:

- `Local demo mode`
  - Used for local UI debugging, connector mock flow, and end-to-end demos without cloud infrastructure
  - Local raw storage, in-process or local queue simulation, and parquet/sqlite persistence are acceptable
  - The current synchronous FastAPI-driven developer experience may remain, but scale is not the optimization target
- `Production GCP mode`
  - Used for high-volume connector fetch, replayable ingestion, distributed normalization, and warehouse-backed dedupe and serving
  - Runtime shape is fixed as `GCS + Pub/Sub + Dataflow + BigQuery`
  - The goal is replayable, observable, idempotent, and memory-controlled execution

#### 4.2.2.2 Production Data Plane (Scalable Ingestion Blueprint)
In production mode, the ingestion data plane is fixed as:
1. Connector fetcher pulls external events page by page
2. Each page is written as a bounded raw shard in compressed JSONL
3. Only shard metadata manifests are published, not raw event arrays
4. Dataflow consumes the manifest and performs canonical normalization
5. Valid events are written to `events_staging`
6. Invalid events are written to `pipeline_dead_letters`
7. BigQuery SQL produces `events_curated` and serving / aggregate tables
8. Upper-layer APIs, prediction, and decision services read curated / aggregate tables by default rather than scanning raw events

#### 4.2.2.3 Raw Shard and Manifest Contract (P0)
Raw shard path convention:
- `gs://<bucket>/raw/source=<source>/dt=YYYY-MM-DD/hour=HH/job=<job_id>/part-000123.jsonl.gz`
- Format: gzip-compressed newline-delimited JSON, one source event per line

Shard manifest must include at least:
- `job_id`
- `source`
- `source_config_id`
- `gcs_uri`
- `event_count`
- `start_date`
- `end_date`
- `schema_version`
- `published_at`

Constraints:
- Pub/Sub only carries shard metadata and never full event arrays
- Checkpoints must be traceable to `job_id + source + shard_index`
- Replay and resume operate at shard granularity instead of full-job reruns

#### 4.2.2.4 Canonical Event and Warehouse Contract (P0)
In addition to the core event fields, the standardized canonical event must explicitly hold:
- `schema_version`
- `source_config_id`
- `raw_gcs_uri`
- `event_date`
- `event_fingerprint`
- `campaign`
- `adset`
- `media_source`

Recommended table layout:
- `raw_ingestion_audit`
- `events_staging` (standardized layer)
- `pipeline_dead_letters`
- `events_curated` (unified / curated layer)
- `identity_links`
- `player_daily_metrics`
- `player_latest_state`
- `player_churn_features`

Current v1 naming and canonical-alias alignment:
- `events_staging` -> `stg_events_standardized`
- `events_curated` -> `fact_events_unified`
- `player_latest_state` -> `mart_user_daily`

Design requirements:
- `events_staging` acts as an append-only landing table for replay and debugging
- `events_curated` acts as the deduped and cleaned source of truth for downstream consumption
- Churn, profile, and actioning paths should read `player_latest_state` and `player_churn_features` first by default
- Drill-down use cases may read curated event history as needed

#### 4.2.2.5 Component Responsibility Boundaries (P0)
- `IngestionService`
  - `mock`: keep the current local development flow, where local shard and queue simulation are acceptable
  - `gcp`: page through connectors, write GCS shards, publish Pub/Sub manifests, and persist checkpoints
- `DataProcessingService / dataflow`
  - `mock`: keep local shard-by-shard processing plus rejected/conflict logs
  - `gcp`: move normalization into Dataflow; FastAPI request paths must never process large jobs inline
- `BigQueryService`
  - Evolve into the warehouse facade with explicit staging, dead-letter, curated, and latest-state methods
  - `mock` mode may continue to write parquet or sqlite, but public methods should match production concepts
- `connectors/normalizer.py`
  - Only owns deterministic field extraction, timestamp coercion, schema versioning, fingerprinting, and required-field validation
  - Does not own full-history dedupe, cross-job reconciliation, or full in-memory state

### 4.2.3 Stitch Rule Engine (P0)
v1 follows a deterministic-first stitching strategy with the following priority:
1. `internal_account_id / game_uid`
2. `login_user_id`
3. `device_id + login binding behavior`
4. `email_hash / phone_hash`
5. Fallback: `source:source_user_id`

Output `identity_links`:
- `source`
- `source_user_id`
- `canonical_user_id`
- `method`
- `confidence`
- `first_seen_at`
- `last_seen_at`

Requirement: every stitch relationship must be traceable and explainable.

### 4.2.4 Dedupe Rules (P0)
- Primary rule: `(source, source_event_id)`
- Fallback rule: `(canonical_or_source_user_id, event_type, event_time_rounded, source)`

Output metrics:
- `raw_normalized_events`
- `deduped_events`
- `duplicates_removed`
- `dedupe_rate`

### 4.2.5 Source-of-Truth Matrix (P0)
Field-level source-of-truth precedence is defined per field family rather than globally:

#### Identity Fields
Priority: `game backend > analytics SDK > MMP`

Applicable fields:
- `internal_account_id`
- `game_uid`
- `login_user_id`
- `player_id` as a candidate ID before canonicalization

#### Attribution Fields
Priority: `analytics SDK > MMP`

Applicable fields:
- `campaign`
- `adset`
- `media_source`
- `channel`

Note: because of privacy-related signal loss and delay on some MMP paths, v1 treats Analytics SDK as the preferred attribution source.

### 4.2.6 Conflict and Exception Handling (P0)
- Record conflict logs for cross-source field conflicts such as `campaign / adset / media_source`
- Field overwrite must record audit info: `old_value / new_value / source / ts / rule_id`
- Events with severe quality issues enter a rejected queue and do not enter the unified layer
- All rejected rows and conflicts must be queryable by `job_id / source`

### 4.2.7 Mapping Strengthening (P0)

#### 4.2.7.1 Layered Mapping System
- `Global Mapping`: shared defaults across sources
- `Source Mapping`: source-level mapping for analytics platforms, MMPs, or game-backend sources
- `Job Override`: one-off override for a single import job

Priority: `Job Override > Source Mapping > Global Mapping`

#### 4.2.7.2 Required-Field Hard Gate
The following fields are mandatory before data can enter the unified layer:
- `player_id` or a canonical-candidate ID
- `event_type`
- `event_time`

Rule: if required mapping coverage is below 95%, the job automatically enters `Awaiting Mapping` and cannot write into unified.

#### 4.2.7.3 Mapping Quality Report Enhancements
In addition to hit rate, the mapping-quality report must include:
- null rate
- type mismatch rate
- sample values (first N rows)
- impacted row count

#### 4.2.7.4 Mapping Versioning and Rollback
Each mapping change records:
- `mapping_version`
- `changed_by`
- `changed_at`
- `diff`

Support one-click rollback to a previous version.

#### 4.2.7.5 AI-Assisted Suggestions (Recommendation-Only by Default)
- Generate suggested mappings from field-name similarity and sample-value patterns
- Suggestions remain recommendation-only until the user confirms them

#### 4.2.7.6 Replay After Mapping Fix
- After a mapping fix, support reprocessing from `standardized -> unified`
- Do not re-fetch source data, so reruns stay cheaper and faster

### 4.2.8 v1 Definition of Done
1. At least 2 sources can stably ingest into `fact_events_unified`
2. `canonical_user_id` coverage is above 90%
3. Duplicate rates for key events such as login and purchase are explainable and traceable
4. Dedupe and stitch statistics are queryable by job
5. Cohorts can be generated directly from the unified layer without source-specific SQL
6. The required mapping-coverage gate is enforced, with jobs below 95% automatically entering `Awaiting Mapping`
7. Mapping versions are auditable and reversible, and replay after mapping fixes is supported

### 4.2.9 Top 20 Mapping Priority List (P0)

> Goal: prioritize the minimum field set needed for segmentation, attribution, and strategy execution.

#### A. Identity and Device
1. `player_id` (unified candidate primary key)
2. `internal_account_id`
3. `game_uid`
4. `login_user_id`
5. `anonymous_id`
6. `device_id`
7. `idfa / idfv / gaid` (ad identifiers, nullable by privacy policy)
8. `email_hash / phone_hash`

#### B. Event Core
9. `event_type`
10. `event_time`
11. `source_event_id`
12. `session_id`
13. `app_version`
14. `platform` (`ios / android / web`)

#### C. Attribution and Channel
15. `campaign`
16. `adset`
17. `media_source`
18. `channel`

#### D. Business and Geography
19. `revenue_usd`
20. `country / region`

### 4.2.10 Top-20 Mapping Acceptance Gates (P0)
- Overall Top-20 field coverage >= 90%
- Critical field coverage (`player_id`, `event_type`, `event_time`) >= 95%
- Attribution field coverage (`campaign / adset / media_source / channel`) >= 85%
- Type-validity rate for `revenue_usd` >= 98%
- If any critical field falls below threshold, the job automatically enters `Awaiting Mapping`

### 4.2.11 Scalable Ingestion Evolution Phases (Overall Design)
#### Phase 1: Interface Refactor Without Behavior Break
- Introduce production-shaped interfaces without breaking the current local demo
- Add shard-manifest models, `fetch_and_stage_events()`, and explicit `event_fingerprint`

#### Phase 2: Local Shard Processing
- Change local mode to write per-shard local JSONL and process shard-by-shard
- Remove full job-level in-memory accumulation so local mode more closely resembles production

#### Phase 3: GCP Ingestion Path
- Default connector fetch writes to `GCS + Pub/Sub`
- Formalize checkpoint persistence and failure recovery

#### Phase 4: Dataflow Normalization Path
- Move normalization out of the FastAPI request path
- Let Dataflow consume manifests, write `events_staging`, and write invalid rows to the dead-letter table

#### Phase 5: Curated and Aggregate Serving
- Build `events_curated`, `player_latest_state`, and `player_churn_features`
- Make player modeling, churn, and decision services read aggregate-first by default

### 4.2.12 Non-Goals of the First Scale-Up Refactor
- No full real-time identity-graph resolution
- No online feature store
- No guarantee of exactly-once semantics for all external connectors
- No full statistical experiment engine as part of ingestion scale-up

---

## 4.3 Audience / Cohort Engine (P0)

> Priority: **P0 (in parallel with 4.2)**

### Goal
Turn the unified data layer directly into reusable audience assets that can be consumed consistently by strategy execution, experimentation, and prediction modules.

### 4.3.1 Cohort Types
- Static cohort (snapshot)
- Dynamic cohort (rule-based with scheduled refresh)
- SQL cohort (generated directly from a query)

### 4.3.2 Unified Cohort Object Model
Every cohort must contain at least:
- `cohort_id`
- `name`
- `type` (`static / dynamic / sql`)
- `definition` (rule JSON or SQL)
- `refresh_mode` (`manual / daily / hourly`)
- `status` (`draft / validating / ready / materializing / active / paused / failed`)
- `member_count`
- `last_refreshed_at`
- `version`
- `owner`
- `source_job_ids`

### 4.3.3 Cohort Generation Definition (P0)

#### Entry Points (3)
- Rule Builder (no-code condition builder)
- SQL Builder (advanced query path)
- Import List (upload of `user_id` / `canonical_user_id` lists)

#### Unified Generation Flow
1. Input definition (rule / SQL / list)
2. Pre-validation (field legality, primary-key completeness, scan-cost estimation)
3. Preview (sample + estimated audience size)
4. Execute materialization (snapshot)
5. Output metadata (`member_count`, `version`, `source`)
6. Optional immediate activation for Engage / Experiment

#### Hard Checks
- Result must contain `canonical_user_id` or be mappable to it
- Empty cohorts cannot be activated, though drafts may still be saved
- Oversized queries must be blocked and return optimization guidance

#### Generated Artifacts
- `cohort_definition`
- `cohort_snapshot`
- `cohort_stats`

### 4.3.4 Cohort Storage and Naming Management (P0)

#### Storage Requirements
- Persist metadata, definition, and the latest snapshot after cohort generation
- Support lookup by `cohort_id` and `name`
- Support soft delete with `deleted_at` and restore

#### Naming and Catalog Management
- `name` must be globally unique, or unique within a workspace/project scope
- Support rename with historical name-change logs
- Support tags and folders
- Support search and filters by name, type, owner, tag, and status

#### Lifecycle Operations (CRUD)
- Create: create cohort as draft or active
- Read: inspect definition, audience size, and recent refresh state
- Update: change definition, refresh policy, name, and tags
- Delete: soft delete plus restore; privileged permanent delete must be audited

### 4.3.5 Cohort Refresh and Storage Strategy (P0)
- Use a hybrid model: persist the definition permanently and keep the latest snapshot
- Dynamic cohorts support both scheduled and manual refresh
- Record audience delta after refresh, including added and dropped members

### 4.3.5 Cohort Consumption Interfaces (Internal)
- Prediction module: pull user sets by cohort
- Strategy module: trigger actions by cohort
- Experiment module: run A/B/Holdout allocation by cohort

Suggested interfaces:
- `POST /cohorts`
- `GET /cohorts/{id}`
- `GET /cohorts/{id}/members`
- `POST /cohorts/{id}/refresh`
- `POST /cohorts/{id}/activate`
- `POST /cohorts/{id}/pause`

### 4.3.6 Cohort Quality Gates (P0)
- Member variance on rerun for the same cohort and time window must be <= 2%
- Dynamic cohort refresh failures must support retry and alerting
- Every cohort member must have a `canonical_user_id`

### 4.3.7 Rule Builder (P0)

#### v1 Syntax Scope (Controlled DSL)
Rule Builder follows a "rule JSON -> SQL compile and execute" pattern. v1 supports:
- Condition combinators: `AND / OR` with up to 3 levels of nesting
- User-attribute conditions such as `country / platform / app_version / payer_status`
- Event-behavior conditions such as `event_type`, `count(event)`, and `last_event_time`
- Numeric metric conditions such as `revenue / session_count / ltv / last_active_days`
- Time-window conditions such as `within_last / before / after`

Supported operators:
- Text: `=`, `!=`, `in`, `not in`, `contains`
- Numeric: `>`, `>=`, `<`, `<=`, `between`
- Time: `within_last`, `before`, `after`

#### Built-In Rule Templates (v1)
1. Active users in the last 7 days
2. Users not logged in during the last 14 days
3. Users with payments in the last 30 days
4. High-value users (`LTV > X`)
5. New users (`registration <= N days`)
6. High-churn-risk users (`risk = high`)
7. Users who viewed a promotion but did not purchase
8. Newly acquired users from a specific channel (`campaign / media_source`)

#### Interaction and Execution Requirements
- Support visual rule editing through condition groups and logical operators
- Show audience-size estimates before execution
- Provide sample previews for the first N users
- Support read-only display of compiled SQL
- Support one-click save as cohort with optional immediate activation

#### Guardrails and Limits
- Maximum condition count: 30
- Maximum nesting depth: 3
- Automatically block queries that exceed timeout or scan limits
- Empty cohorts cannot be activated, though drafts may be saved

### 4.3.8 Rule DSL Examples (Appendix, P0)

#### Example A: Active in the last 7 days + no payment in the last 30 days + high churn risk

Rule JSON (DSL):
```json
{
  "name": "active_7d_no_pay_30d_high_risk",
  "logic": "AND",
  "conditions": [
    {
      "type": "metric",
      "field": "last_active_days",
      "op": "<=",
      "value": 7
    },
    {
      "type": "metric",
      "field": "last_30d_revenue",
      "op": "=",
      "value": 0
    },
    {
      "type": "property",
      "field": "churn_risk",
      "op": "in",
      "value": ["high"]
    }
  ],
  "window": {
    "timezone": "America/Los_Angeles",
    "as_of": "now"
  }
}
```

Compiled SQL (example):
```sql
SELECT
  canonical_user_id
FROM mart_user_daily
WHERE
  last_active_days <= 7
  AND last_30d_revenue = 0
  AND churn_risk IN ('high');
```

#### Example B: Viewed a promotion but did not purchase in the last 14 days

Rule JSON (DSL):
```json
{
  "name": "view_promo_no_purchase_14d",
  "logic": "AND",
  "conditions": [
    {
      "type": "event_count",
      "event": "promo_view",
      "window_days": 14,
      "op": ">=",
      "value": 1
    },
    {
      "type": "event_count",
      "event": "purchase_success",
      "window_days": 14,
      "op": "=",
      "value": 0
    }
  ]
}
```

Compiled SQL (example):
```sql
WITH base AS (
  SELECT
    canonical_user_id,
    SUM(CASE WHEN event_type = 'promo_view'
              AND event_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 14 DAY)
             THEN 1 ELSE 0 END) AS promo_views_14d,
    SUM(CASE WHEN event_type = 'purchase_success'
              AND event_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 14 DAY)
             THEN 1 ELSE 0 END) AS purchases_14d
  FROM fact_events_unified
  GROUP BY canonical_user_id
)
SELECT canonical_user_id
FROM base
WHERE promo_views_14d >= 1
  AND purchases_14d = 0;
```

### 4.3.9 v1 Acceptance Criteria
1. Support static, dynamic, and SQL cohorts
2. All three entry points (Rule / SQL / List) can generate cohorts
3. Rule Builder can generate executable SQL and return estimated audience size
4. Preview and audience-size estimation exist before generation
5. At least one dynamic cohort supports stable daily auto-refresh
6. Cohorts can be consumed directly by predict, engage, and experiment
7. The entire cohort generation flow is auditable with creator, definition, time, and version

### 4.3.10 Detailed Design Reference
Audience Engine's detailed scope, module design, and launch gates are maintained in a dedicated document:
- `docs/AUDIENCE_ENGINE_V1_PRD.md`

The master and Data Core PRDs keep only goals, core capabilities, and high-level acceptance criteria.

---

## 4.4 Data Quality and Observability (P0)

> Priority: **P0 (required before launch)**

### Goal
Build full-chain observability from source to cohort so that data issues can be detected, diagnosed, and replayed.

### 4.4.1 Job-Level Observability
- Source ingest success/failure statistics
- Processing-stage latency
- Dedupe / stitch / reject / conflict metrics
- Current step plus progress percentage

### 4.4.2 Data-Quality Monitoring Metrics
- Required mapping coverage
- `canonical_user_id` coverage
- Valid-rate of `event_time`
- Type-validity rate of `revenue_usd`
- Conflict rate and reject rate
- Events ingested per connector
- Shard creation latency
- Pub/Sub backlog age
- Dataflow processing latency
- Dead-letter volume
- Duplicate rate
- BigQuery staging-to-curated lag
- Aggregate refresh lag

### 4.4.3 Observability Sinks (P0)
- Production default sinks:
  - BigQuery audit tables
  - Cloud Logging
  - Cloud Monitoring alerts
- `mock` mode continues to keep local JSONL and file logs for debugging and replay

### 4.4.4 Alert Rules (P0)
- Required coverage < 95%
- Canonical coverage < 90%
- Reject rate > 5%
- Dynamic cohort refresh failure
- Abnormal increase in dead-letter volume
- Staging-to-curated lag breaches threshold
- Aggregate refresh lag breaches threshold

### 4.4.5 Traceability and Replay
- Keep traces across raw, standardized, and unified layers
- Support replay by `job_id / source`
- Keep audit logs for configuration changes, field overrides, and rule hits

### 4.4.6 v1 Acceptance Criteria
1. Every job has a complete source, processing, and quality report
2. Alert rules can trigger automatically and be recorded
3. Key issues such as mapping errors and conflicts can be traced to source and job within 15 minutes
4. Replay-based recovery can be triggered by job

---

## 4.5 Data Governance and Access Control (P0)

> Priority: **P0 (minimum governance capability)**

### Goal
Establish a minimum viable governance and access-control baseline in v1 to satisfy security, compliance, and team collaboration needs.

### 4.5.1 Access Control
- SQL read-only permission with DDL/DML blocked
- Role-based permissions: `Admin / Analyst / Operator`
- High-risk actions such as mapping override and bulk replay must be audited

### 4.5.2 Data Classification and Masking
- PII fields are masked or hashed by default, such as email and phone
- Exported cohort field visibility is controlled by permission
- External exports are audited

### 4.5.3 Configuration Governance
- Source configs, mappings, and stitch rules are all versioned
- Diff and rollback are supported
- Critical configuration changes automatically write audit logs

### 4.5.4 Cost and Resource Governance
- Query timeout and scan-limit controls
- Replay concurrency limits
- Dynamic cohort refresh-frequency limits

### 4.5.5 v1 Acceptance Criteria
1. Role-based access control and action audit are available
2. PII fields can be masked according to policy
3. Configuration changes are traceable and reversible
4. Queries and replays have foundational resource protection

### 4.6 Default Settings (v1)

> The following defaults act as the v1 environment baseline and may be overridden at the project level.

#### 4.6.1 Data-Quality Thresholds
- Required mapping coverage: `95%`
- `canonical_user_id` coverage: `90%`
- Reject-rate alert line: `5%`

#### 4.6.2 Cohort Refresh Defaults
- Dynamic cohort `refresh_mode`: `daily`
- Static cohort `refresh_mode`: `manual`

#### 4.6.3 Replay and Resource Controls
- Replay concurrency limit: `1~2 jobs`
- Query timeout: enabled and project-configurable
- Scan limit: enabled and project-configurable

#### 4.6.4 Permissions and Audit Defaults
- Role model: `Admin / Analyst / Operator`
- High-risk operations such as rule changes, bulk replay, and permanent delete require audit by default

---

## 5. P0 Implementation Checklist (Prioritized)

### P0-1 Highest Priority: Multi-Source Ingestion Main Path Available
- Goal: make at least 2 sources ingest stably into `fact_events_unified`
- Key deliverables:
  - source configuration and connectivity checks
  - raw -> standardized -> unified three-layer path
  - foundational dedupe and stitch statistics
- Completion criteria:
  - regular imports can run successfully
  - job states are complete (`Processing / Awaiting Mapping / Ready / Failed`)

### P0-2 Mapping Gates and Versioning
- Goal: keep critical field-mapping quality controllable and reversible
- Key deliverables:
  - required-coverage gate with `<95% => Awaiting Mapping`
  - mapping version, diff, and rollback
  - replay after mapping fixes
- Completion criteria:
  - critical field-mapping quality reaches threshold stably
  - broken mappings can be recovered quickly

### P0-3 Identity Stitch and Source-of-Truth in Production
- Goal: establish explainable primary identity and attribution conventions
- Key deliverables:
  - deterministic stitch rules
  - source-of-truth matrix (`backend > analytics SDK > MMP` for identity, `analytics SDK > MMP` for attribution)
  - conflict log (`old / new / source / rule`)
- Completion criteria:
  - `canonical_user_id` coverage >= 90%
  - conflicts are traceable and explainable

### P0-4 Cohort Generation and Management (Rule / SQL / List)
- Goal: turn data capability into executable audience assets
- Key deliverables:
  - Rule Builder / SQL Builder / Import List entry points
  - cohort persistence, naming management, soft delete, and restore
  - direct cohort consumption by engage, experiment, and predict
- Completion criteria:
  - generation, lookup, refresh, and activation all work

### P0-5 Data Quality and Observability
- Goal: make issues detectable, diagnosable, and replayable
- Key deliverables:
  - dashboards for job, source, and quality
  - P0 alerts for coverage, canonical coverage, and reject rate
  - replay and audit chain
- Completion criteria:
  - critical quality issues can be traced to a specific job and source within 15 minutes

### P0-6 Governance and Access Control
- Goal: establish the minimum governance loop in v1
- Key deliverables:
  - RBAC (`Admin / Analyst / Operator`)
  - PII-masked output policy
  - audit coverage for high-risk operations
- Completion criteria:
  - critical actions are fully auditable
  - export and query paths have permission boundaries

### 5.1 Suggested Execution Order (Two-Week Sprint Version)
- Week 1: `P0-1 + P0-2 + P0-3`
- Week 2: `P0-4 + P0-5 + P0-6`

### 5.2 Launch Gates (Go/No-Go)
- Required mapping coverage >= 95%
- `canonical_user_id` coverage >= 90%
- Reject rate <= 5%
- At least one dynamic cohort refreshes reliably every day
- High-risk action audit is enabled and queryable

---

## 6. Current Gap Register (Based on the 2026-03 Repository State Review)

### 6.1 Already Implemented
- Import job state machine, quality gate, and resume / replay already exist
- Mapping versioning, rollback, suggestions, and quality-coverage reporting already exist
- SQL workspace, saved queries, query audit, and query-to-cohort already exist
- Identity summary, conflict, rejected-row queries, and health alerts already exist
- The foundational runtime shape of `operator-api + import-worker + prediction-worker + dataflow` already exists
- SQLAlchemy + Alembic control-plane persistence already exists, with local SQLite fallback and a production Postgres target reflected in the code structure
- Paged connector ingestion, ingestion checkpoints, BigQuery-backed prediction-result storage, and paginated reads already exist
- `Local Model` readiness contract, `heuristic_v1` fallback semantics, effective model metadata, and operator badge/warning are implemented
- Manual local-model retraining controls, inline training-status surfacing, and on-demand import diagnostics are implemented in the operator console

### 6.2 Remaining Gaps

#### Gap-D1 Manifest-Driven Processing Is Not Yet the Default Path
- Current state:
  - The import main path already has raw-shard, standardized, and unified structure
  - The default operating model is still job-driven orchestration at the application layer
- Remaining work:
  - Promote manifest-driven processing to the default entry path
  - Standardize scheduling semantics for `raw shard -> manifest -> standardized -> unified`

#### Gap-D2 Replay and Backfill Tooling Is Incomplete
- Current state:
  - Replay after mapping fixes already exists
  - Replay from a job and rejected-row reprocessing already exist
- Remaining work:
  - Build general raw-shard backfill and replay tooling for source, date, and job ranges
  - Provide a bulk replay control plane that does not require refetching the source

#### Gap-D3 Warehouse Schema Contract Is Not Yet Formalized
- Current state:
  - `events_staging`, `events_curated`, and `player_latest_state` are already usable
  - Canonical aliases already exist
- Remaining work:
  - Schema-version contracts for serving and experimentation tables are not yet formalized
  - Upstream/downstream compatibility rules and change gates are not explicit enough

#### Gap-D4 Dead-Letter and Quality Observability Still Feels Too Engineering-Oriented
- Current state:
  - Rejected events, health alerts, and identity summaries are already queryable
- Remaining work:
  - An operator-facing dead-letter remediation flow is still missing
  - Stable dashboards and escalation alerts for DLQ, quality gate, and source freshness are still missing

#### Gap-D5 GCP-Shaped Mode Is Still Partial
- Current state:
  - GCS, Pub/Sub, Dataflow, and BigQuery abstractions already exist
  - Foundational entrypoints for `import-worker`, `prediction-worker`, and `dataflow` already exist
  - Mock remains the default primary runtime path
- Remaining work:
  - The production runtime contract, failure recovery, and observability are not yet fully aligned

#### Gap-D6 Secret and Access Boundary Is Not Production-Grade Yet
- Current state:
  - Connector and warehouse access still rely mainly on local config and environment variables
- Remaining work:
  - A formal secret manager is still missing
  - Production-grade access boundaries and credential-rotation strategy for warehouse and data connectors are still missing

#### Gap-D7 Data Core Console and Contract Hardening Is Not Complete
- Current state:
  - Connector, import, mapping, SQL, and quality capabilities all already have APIs and single-page entry points
- Remaining work:
  - Data Sandbox, connector, import, and SQL workspace still lack dedicated E2E contract coverage
  - Freshness, quality, DLQ, and mapping-remediation views still need clearer backend view models and UI contracts

### 6.3 Next-Phase Ownership Held by This Document
- `Phase 3 Data Platform Completion`
- Data, connector, and warehouse permission boundaries under `Phase 5 Production Readiness`

### 6.4 V1 Backlog

#### P0 Finish-Up
1. `Manifest-Driven Default Path`
   - Make `raw shard -> manifest -> standardized -> unified` the default processing semantic
   - Reduce the degree to which application-layer job orchestration dominates the main path
2. `Replay / Backfill Tooling`
   - Complete raw-shard backfill and replay across source, date, and job-range dimensions
   - Support bulk replay without refetching from source
3. `Warehouse Schema Contract`
   - Formalize schema versions and compatibility gates for `events_staging / events_curated / player_latest_state / player_churn_features`
   - Define explicit upstream write, downstream consumption, and change-review rules
4. `Dead-Letter / Quality Remediation`
   - Provide an operator-facing dead-letter remediation flow
   - Deliver stable dashboards and escalation alerts for freshness, quality gate, dead-letter, and lag

#### P1
1. `GCP-Shaped Runtime Default`
   - Push the partially implemented GCS / PubSub / Dataflow / BigQuery path toward the production-default runtime contract
   - Complete failure recovery and observability
2. `Secret / Access Productionization`
   - Introduce a formal secret manager and connector / warehouse permission rotation
   - Strengthen warehouse and data-connector access boundaries
3. `Data Core Console Hardening`
   - Add dedicated E2E coverage for connector, import, mapping, SQL, and quality pages
   - Tighten the Data Core backend view model and frontend contract
