**Why KairyxAI Exists**

As a Product Manager in the mobile gaming industry for over 7-8 years, I have been feeling the pain regularly regarding the disconnection between data analyze and marketing/retargeting efforts. 

It has been a messy process for each of the team members that they have to do manual process regularly (create rules, visualize data, make the decision, download data from one place, upload data to another place, execute the decisions etc etc). A lot of the time the data gets swamped in tasks and not synced up across teams (marketing, live ops, CS folks, PMs, Engs) so the result is on and off. Teams have to do caliberations again and again to make sure the rule is setup correctly, the data is cleaned up properly and uploaded without any issue before do the property marketing efforts. It just take TOO MUCH Time!

I want to stitch the splited data from different sources again with the AI powered decision making system and automate the whole process (at least majority of it for now).


Modern game teams already have:

- MMPs (AppsFlyer, Adjust)

- Analytics (Firebase, Amplitude, Mixpanel)

- Messaging tools (Braze, SendGrid, push providers)

What they don’t have is a system that:

- Understands messy, inconsistent event data automatically

- Learns player behavior patterns without manual segmentation

- Decides when not to message as much as when to engage

- Continuously optimizes for retention and monetization outcomes


_KairyxAI is built to be that AI decision layer._


**What KairyxAI Does**

At a high level, KairyxAI:

1. Connects to existing data sources via APIs and webhooks

2. Automatically normalizes and tags events using AI

3. Builds player-level behavioral models in near real time

4. Decides the best action (or no action) for each player

5. Executes engagement via email, push notification, or in-game hooks

6. Learns from outcomes to improve future decisions

All without requiring teams to define funnels, segments, or rules upfront.


**Core Principles**

- MMP-agnostic – Bring your existing stack

- Decision-first – Analytics exist only to drive actions

- Respectful engagement – Fewer, smarter messages

- Continuous learning – Every action improves the system

- Explainable AI – Every decision has a reason


**System Architecture (Conceptual)**


        Data Sources (MMPs / Analytics / Game Events)
               
                ↓
        
        AI Data Normalization
              
                ↓
      
        Player Modeling & Embeddings
              
                ↓
        
        Decision & Optimization Engine
               
                ↓
    
        Execution (Email / Push / In-Game)
              
                ↓
          
        Outcome Feedback Loop

        

**What This Repository Is**

This repo focuses on:

- Core system modules and interfaces

- AI agent prompts and system instructions

- Data normalization and decision logic

- Reference implementations for connectors and execution

**Current Status**

🚧 Early-stage / active development

Initial focus:

- API-based data ingestion

- AI-driven event normalization

- Player modeling and segmentation

- Automated churn-prevention actions

**Who This Is For**

- Game engineers and data engineers

- Product managers working on growth / live ops

- AI engineers interested in applied decision systems

- Founders building tooling for games or consumer apps

**Disclaimer**

KairyxAI is an experimental project.

Expect rapid iteration, architectural changes, and evolving abstractions.

=========================================

Test locally (no full infra required):

### Fast start (recommended)
```bash
./run_local_demo.sh
```
This starts:
- Backend: `http://localhost:8000`
- Frontend: `http://localhost:5173`

Default mode is local mock data backend (`DATA_BACKEND_MODE=mock`).

### Manual start
1. Checkout the project
2. browse to `/backend/services/`
3. run: `pip install -r requirements.txt`
4. choose data backend mode:
   - Mock mode (dev/qa): `export DATA_BACKEND_MODE=mock`
   - GCP mode (prod-like): `export DATA_BACKEND_MODE=gcp`
5. for GCP mode, set:
   - `export BIGQUERY_PROJECT_ID=<your_project_id>`
   - `export GCS_BUCKET_NAME=<your_bucket_name>`
   - configure ADC (Application Default Credentials)
6. run backend: `uvicorn main_service:app --reload --host 0.0.0.0 --port 8000 --reload-dir ../../frontend`
7. run frontend in `/frontend`: `npm install && npm run dev`

Gemini API key is optional now: if unavailable, churn scoring/action uses local heuristic fallback mode.

Local reliability/security defaults added:
- Ingestion retry + backoff for external source pulls
- Dead-letter log for failed ingestion: `.cache/ingest_dlq.jsonl`
- Local audit log for connector changes: `.audit.log.jsonl`
- Local SQLite job persistence: `backend/services/.kairyx_local.db`

P1 execution adapters (local-friendly):
- `email` channel routes via SendGrid when configured, otherwise local simulation
- `braze` channel routes via Braze REST when configured, otherwise push simulation
- Configure Braze via `POST /configure-braze` with `{ api_key, rest_endpoint }`

P1 experimentation (A/B + holdout, local):
- Experiment config endpoint: `GET/POST /experiments/config`
- Summary endpoint: `GET /experiments/summary?experiment_id=...`
- Exposure log: `.experiments_exposure.jsonl`
- Outcome log: `.experiments_outcome.jsonl`
- Default setup: 10% holdout, remaining users split A/B 50:50

## Experiment Framework: Setup & Usage

This project includes a built-in local experimentation framework for churn engagement decisions.

### 1) Start the app
```bash
./run_local_demo.sh
```

### 2) Configure experiment parameters
Use the Backend Workbench UI (Experiment Control section), or call API directly.

Get current config:
```bash
curl http://localhost:8000/experiments/config
```

Update config:
```bash
curl -X POST http://localhost:8000/experiments/config \
  -H "Content-Type: application/json" \
  -d '{
    "experiment_id": "churn_engagement_v1",
    "enabled": true,
    "holdout_pct": 0.10,
    "b_variant_pct": 0.50
  }'
```

Config meanings:
- `experiment_id`: logical name for one experiment run
- `enabled`: enable/disable experiment routing
- `holdout_pct`: % of users receiving no action (control)
- `b_variant_pct`: split of non-holdout traffic between A/B

### 3) Generate experiment exposures
Run player analysis flow (UI: Player Inspector / Analyze & Engage) or API:
```bash
curl -X POST http://localhost:8000/analyze-and-engage-player \
  -H "Content-Type: application/json" \
  -d '{"player_id":"p_8921","prediction_mode":"local"}'
```

What happens automatically:
- user is deterministically assigned to `holdout`, `treatment_a`, or `treatment_b`
- holdout users get `NO_ACTION`
- treatment B gets a variant-marked message
- exposure and outcome are logged locally

### 4) Inspect results and uplift
```bash
curl "http://localhost:8000/experiments/summary?experiment_id=churn_engagement_v1"
```

Summary includes per-group:
- sample size `n`
- `engagement_rate`
- `return_rate`
- `uplift_vs_holdout_return_rate` (for treatment groups)

### 5) Local files used
- Exposures: `.experiments_exposure.jsonl`
- Outcomes: `.experiments_outcome.jsonl`

### Notes
- This is a local/demo experimentation pipeline (not a full statistical engine yet).
- Assignment is stable for a given `(experiment_id, player_id)`.
- For clean reruns, change `experiment_id` or archive/remove local experiment log files.

## Multi-Source Connector Layer (Adjust / AppsFlyer / Amplitude)

KairyxAI now uses a connector registry under `backend/services/connectors/`:
- `amplitude_connector.py`
- `adjust_connector.py`
- `appsflyer_connector.py`
- `registry.py` (connector routing)

Ingestion pipeline uses the selected connector automatically via connector type.

### Configure connectors
- Amplitude: `POST /configure-amplitude-keys`
- Adjust: `POST /configure-adjust-credentials` (`api_token`, optional `api_url`)
- AppsFlyer: `POST /configure-appsflyer` (`api_token`, `app_id`, optional `pull_api_url`)

AppsFlyer configure example:
```bash
curl -X POST http://localhost:8000/configure-appsflyer \
  -H "Content-Type: application/json" \
  -d '{"api_token":"YOUR_TOKEN","app_id":"com.your.game"}'
```

### Import from connector sources
Once configured, sources appear in the workbench import source selector (Amplitude/Adjust/AppsFlyer).
You can import from multiple sources in one job by passing comma-separated connector names:

```bash
curl -X POST http://localhost:8000/ingest-and-process-data \
  -H "Content-Type: application/json" \
  -d '{"start_date":"20250101","end_date":"20250103","source":"Amplitude 1,AppsFlyer 1,Adjust 1","continue_on_source_error":true,"auto_mapping":false}'
```

`auto_mapping` behavior:
- default: `false` (unchecked)
- when `true`: job pulls data then pauses at `Awaiting Mapping` status
- Workbench now shows a guided banner for paused jobs and auto-focuses mapping panel to the first pending source
- after you complete manual mapping, call:

```bash
curl -X POST http://localhost:8000/job/<job_name>/process-after-mapping
```

### Connector health check
```bash
curl http://localhost:8000/connector-health/<connector_name>
```
Example:
```bash
curl http://localhost:8000/connector-health/AppsFlyer%201
```

### Source freshness / last success
```bash
curl http://localhost:8000/connector-freshness
```
Returns per-connector:
- `last_attempt_at`
- `last_success_at`
- `last_ingested_events`
- `last_error`

### Local demo behavior
In `DATA_BACKEND_MODE=mock`, Adjust and AppsFlyer return deterministic mock attribution events so the full pipeline can be tested locally without external infra.

### Merge + dedupe strategy (current)
During processing, events from all selected sources are normalized and merged.

Identity resolution (phase 1-2):
- build deterministic identity links by `(source, source_user_id) -> canonical_user_id`
- if same `source_user_id` appears across sources, it links to the same canonical id
- inspect with: `GET /identity-links`

Dedupe priority:
1. `source_event_id` if present
2. fallback key: `(canonical_user_id, event_type, event_time, source)`

Connector API response parsing supports common wrappers (`data`, `results`, `items`, `rows`, `events`, nested `data.records`) to reduce integration friction.

Per-job import output now includes:
- `source_stats`: ingested event counts per source
- `processing_stats`: normalized count, deduped count, duplicates removed
- `processing_stats.quality`: clean/flagged rows and data-quality flag counts

Cleanup P1 currently performs:
- timestamp normalization to ISO with `invalid_event_time` flag
- player ID fallback (`unknown_user`) with `missing_player_id` flag
- revenue type coercion (`revenue_usd`) with `malformed_revenue` flag
- currency normalization to uppercase

Cleanup P2 adds:
- rejection policy: critically bad rows (missing player id / invalid event time) are isolated to `.cache/rejected_events.jsonl`
- conflict resolution logging: cross-source conflicts on `campaign/adset/media_source` for same canonical event are logged to `.cache/conflict_log.jsonl`
- job stats include `rejected_events` and `conflicts_logged`

APIs:
- `GET /cleanup/rejected-events` (supports `limit`, `job_identifier`, `source`)
- `GET /cleanup/conflicts` (supports `limit`, `job_identifier`, `source`)

P2.5 frontend observability:
- Workbench includes a Cleanup Observability panel
- Filter rejected/conflict logs by job/source
- Table views for quick triage

### Manual field mapping (canonical override)
Use this when sources use different field names (e.g., `PID`, `uid`, `user_id`).

API:
- `GET /field-mapping/{connector_name}`
- `POST /field-mapping/{connector_name}` with `{ "mapping": { ... } }`
- `POST /field-mapping/preview` with a sample raw record

Example mapping:
```json
{
  "canonical_user_id": "event_properties.PID",
  "event_name": "event_type",
  "event_time": "timestamp",
  "source_event_id": "insert_id",
  "campaign": "campaign_name",
  "adset": "adgroup",
  "media_source": "network"
}
```

Frontend workbench now includes:
- Manual Field Mapping section (load/save/preview)
- Identity Links table for cross-source user matching visibility

## Churn prediction model split (active vs already churned)

Churn is now evaluated in 2 layers:
1. **Already churned** (rule-based):
   - configurable `churn_inactive_days` (default 14)
   - if `days_since_last_seen >= churn_inactive_days` => `churn_state = churned`
2. **Likely to churn** (risk scoring):
   - only evaluated for `churn_state = active`
   - returns `churn_risk`, `reason`, and `top_signals`

API:
- `GET /churn/config`
- `POST /churn/config` with `{ "churn_inactive_days": 14, "third_party_for_active": true }`

Notes:
- `third_party_for_active=true` makes active-user churn prediction use local+cloud (parallel) mode by default.
- Already-churned users still use rule-based result only (no third-party call).

