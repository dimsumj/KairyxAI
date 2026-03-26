# KairyxAI Product User Guide

> GitHub Wiki source document. Keep this file aligned with the live operator console, `README.md`, and any user-facing product changes.

## 1) What This Guide Covers
This guide explains how to use the current KairyxAI operator console module by module.

It is written against the current backend-served React operator shell and covers:
- every primary module in the sidebar
- every wired button and text box in the console
- representative sample input and output for the main workflows
- current placeholder controls that exist in the UI but are not yet wired

Unless otherwise stated, example payloads are representative. IDs, timestamps, counts, and exact status text will vary in real environments.
Current v1 resource and job responses include both `tenant_id` and `project_id`.

---

## 2) Before You Start

### 2.1 Global Shell Controls

| Control | Type | How to use it | Sample input | Expected result |
| --- | --- | --- | --- | --- |
| Sidebar collapse button | Button | Shrinks the desktop sidebar to a tight icon rail and expands it again when clicked a second time. In collapsed mode the site brand is hidden, the rail keeps only the module icons, and hovering or focusing an icon opens that module's section list in a right-side popout above the page. The desktop shell also auto-collapses this rail when the viewport drops below `1200px`. | None | Navigation uses less horizontal space while still exposing the current module sections from the icon rail. |
| `Switcher` | Button | Opens the full-screen workspace selector overlay from `Settings -> Organization`. | None | Lets you choose an organization space and project before entering the app. |
| `New Project` | Button | Opens the new-project overlay from `Settings -> Organization`. | None | Creates a new project and switches into it after success. |
| Sidebar profile chip | Footer button | Shows the current signed-in identity at the bottom-left of the sidebar. Click it to open the account menu and use `Log out`. | `Studio Operator` | Opens the account menu, and `Log out` clears the app session then returns the shell to the organization URL gate. |
| Top bar search | Search box | Enter a module title or section label to jump directly to it. The top bar keeps the search field on the left and the theme selector on the right. | `settings` | The matching module or section opens and the matching page becomes active. |
| Theme mode selector | Three-button segmented control | Use the header buttons to follow the system theme or force light or dark mode. The preference is stored in local storage for the current browser. | `Dark` | The shell and module pages immediately switch to the selected theme mode. |
| Sidebar module links | Navigation buttons | Hover or focus a module to expand its section list downward in the full sidebar. Click the module button to open that module's first section by default. Click the same already-open module again in the expanded sidebar to collapse its section list while keeping the current page active. In collapsed mode, hovering or focusing an icon opens that section list in a right-side popout. The `Settings` module is the exception: it opens directly into the Settings page without a sidebar submenu. | `Audience Engine` | The first section under that module becomes active and the matching page content loads, and a repeated click on that same open module collapses the inline section list. |
| Sidebar section list | Inline submenu or collapsed popout | Click any section button in the expanded list under a module, or in the collapsed right-side popout, to jump directly to that section. | `Versions & Comparison` | The matching section becomes active and its content scrolls into view. |
| Workspace startup status | Status line | Read-only. Visible in the full-screen onboarding or workspace gate even when the sidebar is hidden. | `Application start completed (mock)` | Confirms that the application finished startup and the backend health check passed. |

### 2.2 Recommended First-Time Path
1. Use `Continue with Google`.
2. If this is your first login after Google sign-in, complete the onboarding wizard:
   - enter the `Organization URL`
   - continue to the `Project Name`
   - create the first workspace
3. If you already belong to more than one organization space or project, use the `Switcher` button from `Settings` to choose the active workspace.
4. Go to `Data Core -> Connectors` and create at least one connector.
5. Go to `Data Core -> Imports` and run an import.
6. Go to `Audience Engine` and create or refresh a cohort.
7. Go to `Action Orchestrator` and create a workflow.
8. Go to `Experiment Hub` and save the linked experiment config.
9. Go to `Insight Copilot` for query, explain, recommend, and report flows.
10. Go to `Settings` if you want to switch between light mode and dark mode, manage login state, review application startup status, use the shell-level `Switcher` and `New Project` shortcuts, or view the placeholder account-management layouts.

### 2.3 Onboarding And Workspace Overlays

#### Google login gate

| Control | Type | How to use it | Sample input | Expected result |
| --- | --- | --- | --- | --- |
| `Continue with Google` | Button | Starts the Google PKCE login flow before any onboarding or workspace selection is shown. | None | Browser redirects to Google, then returns with an authenticated bearer token. |
| Workspace startup status | Status line | Read-only. Visible before login. | `Application start completed (mock)` | Confirms the backend is up before the user signs in. |

Every user now passes through the Google login gate first. After successful sign-in, the console does one of two things automatically:
- opens the organization-space onboarding wizard if the user has no memberships yet
- enters the existing organization and project, or opens the workspace selector if the user has more than one choice

#### Organization-space onboarding wizard

| Control | Type | How to use it | Sample input | Expected result |
| --- | --- | --- | --- | --- |
| `Organization URL` | Text box | Enter the URL slug that should appear after the base URL. | `northstar` | The console stores this as the internal `organization_id` and uses it in the org-scoped path. |
| `Continue` | Button | Moves from the organization URL step to the project step. | None | The console keeps the generated organization id internally and opens the project form. |
| `Project Name` | Text box | Enter the display name for the first project. | `Live Ops` | The name is shown in the project selector. |
| `Create Project` | Button | Creates the organization space, first project, owner membership, and project-admin membership. | None | The wizard closes and the new workspace becomes active. |

The console now asks for the org URL directly and generates the internal organization display name from that slug. It still generates the internal `project_id` automatically from the project name you type. The backend still stores the organization id internally as `tenant_id`, but that internal field is no longer part of the visible login or workspace UI.

#### Sample onboarding request
```json
{
  "organization_id": "northstar",
  "organization_name": "North Star Games",
  "project_id": "liveops",
  "project_name": "Live Ops",
  "project_description": "Primary production lifecycle execution"
}
```

#### Sample onboarding response
```json
{
  "organization_space": {
    "organization_id": "northstar",
    "tenant_id": "northstar",
    "name": "North Star Games",
    "status": "active"
  },
  "project": {
    "organization_id": "northstar",
    "tenant_id": "northstar",
    "project_id": "liveops",
    "name": "Live Ops",
    "description": "Primary production lifecycle execution",
    "status": "active"
  }
}
```

#### Workspace switcher overlay

| Control | Type | How to use it | Sample input | Expected result |
| --- | --- | --- | --- | --- |
| `Organization URL` | Text box | Type the organization URL you want to open. | `northstar` | The console resolves that organization, loads its projects, and moves to the project step. |
| `Continue` | Button | Resolves the typed organization URL. | None | The project list for that organization loads. |
| `Existing Project` | Select | Choose a project that already exists inside the selected organization space. | `sandbox` | The selected project becomes the active console context after continue. |
| `Use Existing Project` | Button | Confirms the selected existing project. | None | The gate closes and the console reloads data for that org/project. |
| `New Project Name` | Text box | Enter a new project name if you want to create another project in the selected organization space. | `Growth Sandbox` | The console generates the internal project id automatically. |
| `Add New Project` or `Create First Project` | Button | Creates a new project inside the selected organization space. | None | The project is created, the creator becomes a project admin, and the console switches into it. |

#### New-project overlay

| Control | Type | How to use it | Sample input | Expected result |
| --- | --- | --- | --- | --- |
| `Project Name` | Text box | Enter the display name for the new project. | `Growth Sandbox` | The project is created with this name. |
| `Create Project` | Button | Creates the project in the selected organization space. | None | The project is created, the creator becomes a project admin, and the console switches into it. |
| `Cancel` | Button | Closes the new-project overlay. | None | Returns to the prior workspace selection state. |

As in onboarding, the current new-project UI generates the internal `project_id` automatically from the typed project name and keeps the id field hidden.

#### Invite redemption behavior
- If the browser opens a URL containing `invite_code`, the console stores that invite locally before Google login.
- After successful Google login, the console redeems the invite automatically by calling `POST /api/v1/project-invites/redeem` first, then switches normal authenticated traffic to the org-scoped path shape `/{organization_id}/v1/...`.
- On success, the active organization space and project switch to the invite target.

---

## 3) Data Core

### 3.1 Churn Rescue Workbench
This page is the quickest end-to-end operator view for running prediction and exporting a churn-rescue audience.

#### Controls

| Control | Type | How to use it | Sample input | Expected result |
| --- | --- | --- | --- | --- |
| `Prediction Target` | Select | Choose whether to run prediction by `Source` or by explicit `Import`. | `Source` | The audience selector switches between source-level and import-level options. |
| `Select Source` / `Select Import` | Select | In `Source` mode, choose a source such as `Amplitude 1`. In `Import` mode, choose a specific completed import. | `Amplitude 1` | Source mode resolves to the latest completed import for that source when the job starts; import mode uses the selected import directly. |
| `Prediction Engine` | Select | Choose the prediction execution mode. | `AI + Cloud` | The request uses the selected prediction mode. |
| Local model status badge | Badge | Read the current readiness of the `Local Model` path before running prediction. | `Learning` | Shows whether local prediction is `Ready`, `Learning`, or `Fallback`. |
| `Train Local Model` | Button | Manually trigger a local batch retrain from the workbench. | None | Starts a local training run and updates the inline training status when complete. |
| `Refresh Model Status` | Button | Reload the latest local-model readiness and training status without starting a run. | None | Refreshes the badge, readiness details, and inline training status. |
| Local model training status | Inline status text | Read the latest training state, labeled-row count, class balance, and last update time. | `Fallback · 42/12 labeled rows` | Shows the most recent local model training outcome and supporting detail. |
| `Predict Churn` | Button | Starts prediction for the selected source or import. | None | A prediction job is created and results populate the table when complete. |
| `Provider` | Select | Choose the audience export target. | `Braze` | Export request uses Braze provider settings. |
| `Channel` | Select | Choose the downstream delivery channel. | `Push Notification` | Export metadata is tagged with the selected channel. |
| `Risk Filters` | Text box | Comma-separated predicted risk levels to include. | `high,medium` | Only those risk levels are exported. |
| `Audience Name` | Text box | Optional export label for the downstream audience. | `churn_push_high` | Export payload uses this audience name. |
| `Webhook URL` | Text box | Optional direct override for webhook exports. Leave blank to use configured connection settings. | `https://hooks.example.com/churn` | Webhook exports target this URL. |
| `Webhook Token` | Password box | Optional bearer token for direct webhook exports. | `secret-token-123` | Sent as a bearer token when using direct webhook mode. |
| `Include already churned users` | Checkbox | Include users whose churn state is already `churned`. | Checked | Export includes users already marked as churned. |
| `Push Audience` | Button | Sends the current audience selection to the configured provider. | None | Creates an export job and updates the export status message. |
| `Show` | Select | Changes rows per page in the result table. | `50` | Table pagination updates to 50 rows per page. |
| Pagination controls | Buttons | Move through paginated prediction results. | `Next` | The next page of prediction rows is displayed. |

#### Sample prediction input
```text
Prediction Target: Source
Source: Amplitude 1
Prediction Engine: AI + Cloud
```

#### Local model readiness behavior
- `Local Model` always remains runnable, even when no trained supervised model is active yet.
- When the badge shows `Learning` or `Fallback`, the console warns that `heuristic_v1` fallback is being used.
- When the badge shows `Ready`, local predictions are using the active learned churn model.
- `Train Local Model` uses the local batch trainer and refreshes the same readiness contract used by the workbench.
- Completed prediction jobs may also show the effective local model version and state used for that run.
- In `Source` mode, the workbench resolves to the latest completed import when the prediction job starts, and the resolved import remains recorded on the job for audit.

#### Sample prediction output
```json
[
  {
    "user_id": "u_1001",
    "ltv": 240.35,
    "session_count": 2,
    "event_count": 18,
    "predicted_churn_risk": "high",
    "churn_reason": "7d session drop, no purchase in 14d",
    "suggested_action": "Send reward reminder"
  },
  {
    "user_id": "u_1048",
    "ltv": 71.50,
    "session_count": 3,
    "event_count": 21,
    "predicted_churn_risk": "medium",
    "churn_reason": "engagement weakening",
    "suggested_action": "Push tutorial recap"
  }
]
```

#### Sample audience export input
```json
{
  "provider": "braze",
  "channel": "push_notification",
  "risk_filters": ["high", "medium"],
  "audience_name": "churn_push_high",
  "include_churned": false
}
```

#### Sample audience export output
```json
{
  "job_id": "export_20260322_110201",
  "status": "queued",
  "provider": "braze",
  "audience_name": "churn_push_high",
  "exported_users": 128,
  "tenant_id": "default",
  "project_id": "default"
}
```

### 3.2 Import Control Plane

#### Controls

| Control | Type | How to use it | Sample input | Expected result |
| --- | --- | --- | --- | --- |
| `Import Source` | Select | Choose the configured ingestion source. | `amplitude` | Import request uses that connector/source. |
| `Start Date` | Date | Beginning of the import window. | `2026-03-01` | Request converts to `20260301`. |
| `End Date` | Date | End of the import window. | `2026-03-07` | Request converts to `20260307`. |
| `Import Data` | Button | Creates a new import job. | None | Import job appears in the imported data list. |
| Import row `Stop` | Row button | Stops a queued or running import. | None | Job moves toward `stopping` then `stopped`. |
| Import row `Delete` | Row button | Deletes a completed, failed, or stopped import. | None | Import disappears from the list after confirmation. |
| `Import Job` | Select | Choose an import job for detail views. | `import_20260322_101500` | Detail actions apply to the selected import. |
| `Load Operations` | Button | Loads import operational detail on demand. | None | Operations JSON appears in the detail output. |
| `Load Quality` | Button | Loads import quality detail on demand. | None | Quality JSON appears in the detail output. |
| `Load Manifests` | Button | Loads manifest detail for the selected import on demand. | None | Manifest JSON and list appear. |
| `Alias` | Select | Choose a warehouse contract alias. | `standardized` | Contract detail request targets that alias. |
| `Load Contract` | Button | Loads the selected schema contract on demand. | None | Contract JSON appears in the schema output. |
| `List All` | Button | Lists all available schema contracts on demand. | None | Contract list is displayed for all aliases. |

#### Sample import input
```json
{
  "source": "amplitude",
  "start_date": "20260301",
  "end_date": "20260307"
}
```

#### Sample import output
```json
{
  "id": "import_20260322_101500",
  "name": "Amplitude 1-20260322-101500",
  "status": "queued",
  "current_step": "Processing",
  "progress_pct": 0,
  "start_date": "20260301",
  "end_date": "20260307",
  "tenant_id": "default",
  "project_id": "default"
}
```

#### Sample operations output
```json
{
  "job_id": "import_20260322_101500",
  "status": "completed",
  "events_staged": 18240,
  "events_processed": 18002,
  "manifests_created": 8,
  "checkpoint": "2026-03-07T23:59:59Z"
}
```

#### Import diagnostics behavior
- The Imports page no longer auto-loads heavy diagnostics on first render.
- Operations, quality, manifests, and schema-contract detail load only when you request them.
- Import polling continues automatically only while at least one import job is still active.
- Right after backend restart, a transient control-plane busy response may appear; retry the detail load if prompted.

### 3.3 Connectors
Use this page to register upstream ingestion sources and downstream service credentials.

#### Shared controls

| Control | Type | How to use it | Sample input | Expected result |
| --- | --- | --- | --- | --- |
| `Add New Connector` | Button | Opens the connector creation form. | None | The connector type form becomes visible. |
| `Select Connector Source` | Select | Chooses which connector form to render. | `Amplitude` | Connector-specific fields appear. |
| `Save Connector` | Button | Saves the connector after the required fields are filled. | None | Connector is created and shown in the saved connector list. |
| `Cancel` | Button | Resets the form and returns to the empty state. | None | Dynamic fields disappear and the add card returns. |
| Saved connector `Delete` | Row button | Deletes the connector after confirmation. | None | Connector is removed from the saved list. |

#### Connector-specific fields

| Connector type | Fields | Sample input |
| --- | --- | --- |
| `Amplitude` | `Amplitude API Key`, `Amplitude Secret Key` | `api_key=amp_public_123`, `secret_key=amp_secret_456` |
| `Adjust` | `Adjust API Token`, `Adjust API URL (optional)` | `api_token=adj_token_123`, `api_url=https://dash.adjust.com/control-center/reports-service` |
| `AppsFlyer` | `AppsFlyer API Token`, `AppsFlyer App ID`, `AppsFlyer Pull API URL (optional)` | `api_token=af_token_123`, `app_id=id123456789`, `pull_api_url=https://hq1.appsflyer.com/api/raw-data/export/app` |
| `Google Gemini` | `Google API Key`, `Gemini Model Version` | `api_key=google_key_123`, `model_name=gemini-flash-latest` |
| `BigQuery` | `Google Cloud Project ID` | `project_id=my-prod-project` |
| `SendGrid` | `SendGrid API Key` | `api_key=SG.xxxxx` |
| `Braze` | `Braze API Key`, `Braze REST Endpoint` | `api_key=braze_key_123`, `rest_endpoint=https://rest.iad-01.braze.com` |

#### Sample connector output
```json
{
  "name": "Amplitude 1",
  "type": "amplitude",
  "details": "Configured",
  "tenant_id": "default",
  "project_id": "default",
  "created_by": "admin"
}
```

### 3.4 Mappings
Use the mapping sandbox when an import is waiting on field mapping or when you want to preview how a raw record will normalize.

#### Controls

| Control | Type | How to use it | Sample input | Expected result |
| --- | --- | --- | --- | --- |
| `Connector` | Select | Choose which connector mapping to load or edit. | `Amplitude 1` | The mapping actions target this connector. |
| `Awaiting Mapping Job` | Select | Choose a paused job that is waiting on mapping. | `import_20260322_101500` | `Process After Mapping` targets this job. |
| `Load Mapping` | Button | Loads the current saved field mapping. | None | Mapping JSON fills the editor. |
| `Save Mapping` | Button | Persists the current mapping JSON. | None | Mapping is saved for the selected connector. |
| `Preview Mapping` | Button | Applies the mapping to the sample raw event locally. | None | Preview result JSON is generated. |
| `Coverage` | Button | Calculates mapping coverage against the selected connector. | None | Coverage summary appears. |
| `Process After Mapping` | Button | Resumes the selected waiting job after mapping is ready. | None | Import processing continues. |
| `Mapping JSON` | Text area | The canonical mapping definition. | `{"events":[...],"users":[...]}` | Saved and previewed as JSON. |
| `Sample Raw Event` | Text area | A raw source event used for local preview. | `{"user_id":"u_1001","event":"purchase"}` | Preview result shows normalized fields. |

#### Sample mapping input
```json
{
  "event_name": "event",
  "event_time": "timestamp",
  "user_id": "user_id",
  "email": "email"
}
```

#### Sample mapping preview output
```json
{
  "canonical_user_id": "u_1001",
  "event_name": "purchase",
  "occurred_at": "2026-03-22T09:31:00Z",
  "email": "u1001@example.com"
}
```

### 3.5 Audit Trail

#### Controls

| Control | Type | How to use it | Sample input | Expected result |
| --- | --- | --- | --- | --- |
| `Resource Type` | Text box | Filter audit records by resource type. | `workflow` | Only workflow-related actions are returned. |
| `Action Type` | Text box | Filter audit records by action type. | `workflow_execution_completed` | Only matching actions are shown. |
| `Show` | Select | Changes audit rows per page. | `50` | More or fewer records render per page. |
| `High Risk Only` | Checkbox | Restricts results to high-risk actions. | Checked | Only high-risk records remain. |
| `Refresh Audit Log` | Button | Reloads the audit table using the current filters. | None | Filtered audit rows refresh. |
| `Show Full Text` / `Show Less` | Row button | Expands or collapses long details text. | None | The details cell expands or collapses. |

#### Sample audit output
```json
{
  "created_at": "2026-03-22T11:02:15Z",
  "action_type": "workflow_published",
  "resource_type": "workflow",
  "resource_id": "wf_20260322_1101",
  "high_risk": true,
  "payload": {
    "workflow_id": "wf_20260322_1101",
    "experiment_id": "churn_rescue_v1"
  }
}
```

### 3.6 Templates
Templates let you instantiate a prebuilt scenario into concrete cohort and workflow assets.

#### Controls

| Control | Type | How to use it | Sample input | Expected result |
| --- | --- | --- | --- | --- |
| `Refresh` | Button | Reloads the template catalog. | None | Template list refreshes. |
| Template name button | Row button | Selects a template from the list. | `Churn Rescue` | Template detail JSON loads. |
| `Owner` | Text box | Sets the owner field for instantiated assets. | `frontend_operator` | Created resources use this owner. |
| `Name Prefix` | Text box | Optional prefix for generated resource names. | `pilot_` | Cohort and workflow names start with `pilot_`. |
| `Activate cohort` | Checkbox | Activates the cohort immediately after creation. | Checked | Created cohort becomes active. |
| `Publish workflow` | Checkbox | Publishes the workflow after it is created. | Checked | Workflow is created and published. |
| `Instantiate Template` | Button | Creates concrete assets from the selected template. | None | Instantiation output JSON appears. |

#### Sample instantiation output
```json
{
  "template_id": "churn_rescue",
  "cohort_id": "cohort_20260322_1110",
  "workflow_id": "wf_20260322_1110",
  "activate_cohort": true,
  "publish_workflow": true
}
```

### 3.7 Health
Use Health to inspect system status and manually run one scheduler tick.

#### Controls

| Control | Type | How to use it | Sample input | Expected result |
| --- | --- | --- | --- | --- |
| `Refresh` | Button | Reloads the health payload, module table, alert list, and scheduler list. | None | Health panels refresh. |
| `Run Scheduler Tick` | Button | Triggers one scheduler tick immediately. | None | Tick output appears and health is reloaded. |
| Health output panel | JSON output | Read-only. | None | Shows `/api/v1/health`, `/{organization_id}/v1/health`, or tick output depending on the active session context. |
| Module status table | Read-only table | Read the current module status and metrics. | None | Modules show status and metrics. |
| Alerts table | Read-only table | Inspect persisted alerts. | None | Alerts list with severity and message. |
| Scheduler jobs table | Read-only table | Inspect current scheduler configuration and timing. | None | Scheduler jobs show schedule, last run, next run. |

#### Sample health output
```json
{
  "status": "ok",
  "service": "KairyxAI Operator API",
  "mode": "gcp",
  "alerts_open": 1,
  "modules": 5
}
```

### 3.8 Governance
This page currently contains a visible form but the `Save Limits` button is not wired in the current JavaScript.

#### Visible controls

| Control | Type | How to use it | Sample input | Current behavior |
| --- | --- | --- | --- | --- |
| `AI Token Limit (per month)` | Number box | Planned monthly token cap input. | `1000000` | Visible only; not persisted by the current frontend. |
| `Budget Limit (USD per month)` | Number box | Planned monthly budget cap input. | `500` | Visible only; not persisted by the current frontend. |
| `Save Limits` | Button | Placeholder control for future governance limit persistence. | None | No wired action in the current frontend. |

---

## 4) Audience Engine

### 4.1 Create Cohort

| Control | Type | How to use it | Sample input | Expected result |
| --- | --- | --- | --- | --- |
| `Name` | Text box | Unique cohort name. | `churn_rescue_high_risk` | Used as the cohort display name. |
| `Type` | Select | Choose `SQL`, `Rule`, or `List`. | `SQL` | Backend interprets the definition accordingly. |
| `Refresh Mode` | Select | Choose `Daily` or `Manual`. | `Manual` | Controls automatic refresh behavior. |
| `Owner` | Text box | Sets the cohort owner. | `frontend_operator` | Saved in cohort metadata. |
| `Tags (comma separated)` | Text box | Free-form tags for organization. | `churn,rescue,high-risk` | Stored as tag array. |
| `Description` | Text box | Describe the cohort purpose. | `High-risk users for rescue workflow` | Stored as cohort description. |
| `Definition JSON` | Text area | Enter SQL, rule, or list definition JSON. | See sample below | Used to build the cohort. |
| `Activate after create` | Checkbox | Activate the cohort immediately after creation. | Checked | Cohort status becomes active after create. |
| `Create Cohort` | Button | Submits the cohort create request. | None | Cohort is created and status text updates. |

#### Sample cohort definitions

SQL definition:
```json
{
  "sql": "SELECT user_id AS canonical_user_id, email FROM prediction_results WHERE predicted_churn_risk = 'high'"
}
```

Rule definition:
```json
{
  "source_alias": "mart_user_daily",
  "logic": "AND",
  "conditions": [
    { "field": "days_since_last_seen", "op": ">=", "value": 3 },
    { "field": "sessions_7d", "op": "<=", "value": 2 }
  ]
}
```

List definition:
```json
{
  "members": [
    { "canonical_user_id": "u_1001", "email": "u1001@example.com" },
    { "canonical_user_id": "u_1002", "email": "u1002@example.com" }
  ]
}
```

#### Sample cohort output
```json
{
  "cohort_id": "cohort_20260322_1200",
  "name": "churn_rescue_high_risk",
  "type": "sql",
  "status": "active",
  "version": 1,
  "member_count": 128
}
```

### 4.2 SQL Workspace

| Control | Type | How to use it | Sample input | Expected result |
| --- | --- | --- | --- | --- |
| `Saved Query Name` | Text box | Name the saved query. | `High risk users` | Stored with the query. |
| `Preview Limit` | Number box | Max rows to preview. | `20` | Preview is limited to 20 rows. |
| `Timeout (seconds)` | Number box | Read-only query timeout. | `30` | Query is cut off after 30 seconds if needed. |
| `Description` | Text box | Describe why the query exists. | `High-risk audience seed query` | Stored as query description. |
| `SQL` | Text area | Enter the read-only warehouse query. | See sample below | Used for preview/save/cohort creation. |
| `Preview` | Button | Runs a preview against the current SQL. | None | Preview JSON appears. |
| `Save Query` | Button | Saves the query and metadata. | None | Query appears in the saved query list. |
| `Query to Cohort` | Button | Converts the current SQL into a cohort. | None | A new cohort is created from the SQL. |
| Saved query `Preview` | Row button | Loads the saved SQL and previews it. | None | SQL text and preview output refresh. |
| Saved query `To Cohort` | Row button | Creates a cohort from the saved query. | None | Cohort is created from that saved query id. |

#### Sample SQL input
```sql
SELECT user_id AS canonical_user_id, email
FROM prediction_results
WHERE predicted_churn_risk = 'high'
  AND COALESCE(churn_state, 'active') != 'churned'
```

#### Sample preview output
```json
{
  "rows": [
    { "canonical_user_id": "u_1001", "email": "u1001@example.com" },
    { "canonical_user_id": "u_1002", "email": "u1002@example.com" }
  ],
  "returned": 2,
  "limit": 20
}
```

### 4.3 Cohort List And Detail

| Control | Type | How to use it | Sample input | Expected result |
| --- | --- | --- | --- | --- |
| `Refresh` | Button | Reloads the cohort list and detail state. | None | The cohort list refreshes. |
| Cohort row `View` | Row button | Loads the selected cohort detail. | None | Detail panel changes to the selected cohort. |
| Cohort row `Refresh` | Row button | Re-runs the cohort refresh. | None | Cohort version or metrics may change. |
| Cohort row `Activate` | Row button | Activates a draft or paused cohort. | None | Cohort status becomes active. |
| Cohort row `Pause` | Row button | Pauses an active cohort. | None | Cohort status becomes paused. |
| Cohort row `Archive` | Row button | Archives the cohort. | None | Cohort status becomes archived. |
| Cohort row `Restore` | Row button | Restores an archived cohort. | None | Cohort returns to a usable lifecycle state. |
| `Load Members` | Button | Loads cohort member preview rows. | None | Member table appears. |
| `Load Versions` | Button | Loads cohort version history. | None | Version table appears. |
| `Load Metrics` | Button | Loads cohort metrics JSON. | None | Metrics JSON appears. |
| `Base Version` | Number box | Choose the base version for compare or rollback. | `1` | Used in compare and rollback actions. |
| `Target Version` | Number box | Choose the compare target version. | `2` | Used in version comparison. |
| `Compare Versions` | Button | Compares the two selected versions. | None | Comparison JSON appears. |
| `Rollback to Base` | Button | Rolls the cohort back to the base version. | None | Cohort version reverts to the selected base. |

#### Sample compare output
```json
{
  "base_version": 1,
  "target_version": 2,
  "member_delta": 14,
  "added_members": 20,
  "removed_members": 6
}
```

---

## 5) Action Orchestrator

### 5.1 Create Workflow

| Control | Type | How to use it | Sample input | Expected result |
| --- | --- | --- | --- | --- |
| `Name` | Text box | Workflow name. | `daily_churn_rescue` | Stored as workflow name. |
| `Cohort` | Select | Choose the source cohort. | `cohort_20260322_1200` | Workflow binds to that cohort. |
| `Experiment ID` | Text box | Link the workflow to an experiment id. | `churn_rescue_v1` | Publish and measurement use this experiment id. |
| `Trigger Type` | Select | Choose `daily_schedule` or `manual_test`. | `daily_schedule` | Trigger config uses the selected type. |
| `Hour` | Number box | Scheduled run hour. | `10` | Daily run executes at 10:00. |
| `Minute` | Number box | Scheduled run minute. | `0` | Daily run executes at `:00`. |
| `Channel` | Select | Choose message channel. | `push_notification` | Action uses the selected channel. |
| `Global Daily Limit` | Number box | Max sends per day across workflow. | `5` | Policy blocks sends beyond five. |
| `Channel Daily Limit` | Number box | Max sends per channel per day. | `5` | Policy applies per channel. |
| `Cooldown Hours` | Number box | Cooldown per user. | `24` | Same user is skipped for 24 hours. |
| `Quiet Hours Start` | Number box | Start of no-send window. | `22` | Sends are blocked after 22:00. |
| `Quiet Hours End` | Number box | End of no-send window. | `7` | Sends resume at 07:00. |
| `Daily Budget Limit` | Number box | Workflow budget cap. | `25` | Policy blocks sends beyond budget. |
| `Blacklist IDs (comma separated)` | Text box | Users who should never receive this workflow. | `user_1,user_2` | Those users are always skipped. |
| `Content` | Text area | Message body. | `Come back for a reward.` | Used as workflow content. |
| `Requires manual confirmation` | Checkbox | Require manual confirmation before high-risk execution. | Checked | Workflow is marked confirmation-required. |
| `Create Workflow` | Button | Creates the workflow draft. | None | Workflow appears in the workflow list. |

#### Sample workflow output
```json
{
  "workflow_id": "wf_20260322_1215",
  "name": "daily_churn_rescue",
  "status": "draft",
  "cohort_id": "cohort_20260322_1200",
  "experiment_id": "churn_rescue_v1"
}
```

### 5.2 Runtime Controls

| Control | Type | How to use it | Sample input | Expected result |
| --- | --- | --- | --- | --- |
| `Kill Switch On` | Button | Immediately blocks new sends. | None | Orchestrator kill switch is enabled. |
| `Kill Switch Off` | Button | Re-enables sends after mitigation. | None | Orchestrator kill switch is disabled. |
| `Run Due Reference Time` | Text box | Optional ISO timestamp for scheduled execution simulation. | `2026-03-10T10:00:00` | Due-workflow evaluation uses this time. |
| `Limit Per Workflow` | Number box | Max items to execute per workflow in a run. | `100` | Scheduler run caps execution per workflow. |
| `Run Due Workflows` | Button | Runs currently due workflows. | None | Due executions are created and shown. |
| `Callback Provider` | Select | Choose callback provider parser. | `braze` | Callback ingestion treats payload as Braze callbacks. |
| `Callback Payload` | Text area | JSON body for callback ingestion. | See sample below | Callback events are ingested. |
| `Ingest Callback` | Button | Sends callbacks into the activation endpoint. | None | Ingestion status and output update. |
| `Export Job` | Select | Choose an export job for diagnostics or retry. | `export_20260322_1220` | Diagnostics actions target that export. |
| `Load Diagnostics` | Button | Loads export diagnostics for the selected export. | None | Diagnostics JSON appears. |
| `Retry Export` | Button | Retries the selected export job. | None | Export retry request is issued. |

#### Sample callback input
```json
{
  "callbacks": [
    {
      "provider": "braze",
      "delivery_id": "dlv_1001",
      "workflow_id": "wf_20260322_1215",
      "user_id": "u_1001",
      "status": "delivered",
      "occurred_at": "2026-03-22T12:25:00Z"
    }
  ]
}
```

#### Sample due-run output
```json
{
  "items": [
    {
      "workflow_id": "wf_20260322_1215",
      "execution_id": "exec_20260322_1225",
      "status": "completed",
      "delivered": 5
    }
  ]
}
```

### 5.3 Workflow List, Executions, And Deliveries

| Control | Type | How to use it | Sample input | Expected result |
| --- | --- | --- | --- | --- |
| `Refresh` | Button | Reloads workflows and detail panels. | None | Workflow list refreshes. |
| Workflow row `View` | Row button | Loads selected workflow detail. | None | Execution, delivery, and policy panels refresh. |
| Workflow row `Publish` | Row button | Publishes the workflow after preflight checks. | None | Workflow status becomes published. |
| Workflow row `Pause` | Row button | Pauses a published workflow. | None | Workflow status becomes paused. |
| Workflow row `Resume` | Row button | Resumes a paused workflow. | None | Workflow status becomes published or active again. |
| Workflow row `Test Run` | Row button | Runs the workflow in test mode. | None | Test run output appears in runtime output panel. |
| `Load Diagnostics` | Button | Loads delivery diagnostics for the selected workflow. | None | Diagnostics JSON appears. |

#### Sample workflow diagnostics output
```json
{
  "workflow_id": "wf_20260322_1215",
  "deliveries": 5,
  "callbacks_recorded": 3,
  "callback_lag": {
    "count": 3,
    "avg_seconds": 14.2,
    "max_seconds": 21
  }
}
```

---

## 6) Experiment Hub

### 6.1 Experiment Control

| Control | Type | How to use it | Sample input | Expected result |
| --- | --- | --- | --- | --- |
| `Experiment ID` | Text box | Unique experiment identifier. | `churn_rescue_v1` | Used as the main experiment key. |
| `Primary Metric` | Text box | Main metric for decisioning. | `return_rate` | Summary and decisioning use this metric. |
| `Cohort ID` | Text box | Optional linked cohort. | `cohort_20260322_1200` | Saved in config metadata. |
| `Guardrails (comma separated)` | Text box | Guardrail metrics. | `engagement_rate,policy_block_rate` | Stored as guardrail list. |
| `Min Sample Size` | Number box | Minimum sample size for decisioning. | `20` | Summary remains inconclusive below 20. |
| `Min Runtime Hours` | Number box | Minimum runtime before decisioning. | `24` | Summary remains inconclusive before 24 hours. |
| `Holdout %` | Number box | Holdout allocation percentage. | `0.1` | 10 percent holdout allocation. |
| `B Variant %` | Number box | B variant allocation percentage. | `0.5` | 50 percent to B variant within treatment. |
| `Decision Actor` | Text box | Operator recording the decision. | `frontend_operator` | Recorded in decision metadata. |
| `Enabled` | Checkbox | Enables or disables the experiment. | Checked | Saved config reflects enabled state. |
| `Load Config` | Button | Loads config, summary, integrity, exposures, and outcomes. | None | Experiment workspace fills with current data. |
| `Save Config` | Button | Saves the current config form. | None | Config is stored or updated. |
| `Start` | Button | Starts the experiment lifecycle. | None | Experiment moves to running state. |
| `Stop` | Button | Stops the experiment lifecycle. | None | Experiment moves to stopped state. |
| `Record Decision` | Button | Records an experiment decision using the current summary. | None | Summary updates with decision result. |
| `Refresh Summary` | Button | Reloads the summary. | None | Summary output refreshes. |
| `Load Integrity` | Button | Reloads integrity details only. | None | Integrity JSON refreshes. |

#### Sample config output
```json
{
  "experiment_id": "churn_rescue_v1",
  "enabled": true,
  "primary_metric": "return_rate",
  "guardrail_metrics": ["engagement_rate", "policy_block_rate"],
  "min_sample_size": 20,
  "min_runtime_hours": 24,
  "holdout_pct": 0.1,
  "b_variant_pct": 0.5
}
```

### 6.2 Exposures, Outcomes, And Outcome Ingestion

| Control | Type | How to use it | Sample input | Expected result |
| --- | --- | --- | --- | --- |
| Exposures table | Read-only table | Inspect exposure assignments. | None | Exposure list shows group and user rows. |
| Outcomes table | Read-only table | Inspect recorded outcomes. | None | Outcome list shows outcome name and timestamp. |
| `Outcome Payload` | Text area | JSON batch of outcomes to ingest. | See sample below | Outcome ingestion request uses this body. |
| `Ingest Outcomes` | Button | Writes outcomes into the experiment. | None | Outcome status confirms ingested count. |

#### Sample outcome ingestion input
```json
{
  "outcomes": [
    {
      "workflow_id": "wf_20260322_1215",
      "cohort_id": "cohort_20260322_1200",
      "experiment_id": "churn_rescue_v1",
      "user_id": "u_1001",
      "occurred_at": "2026-03-22T12:35:00Z",
      "group": "treatment",
      "outcome_name": "returned",
      "source": "internal_writeback",
      "metadata": {
        "channel": "push_notification"
      }
    }
  ]
}
```

#### Sample summary output
```json
{
  "experiment_id": "churn_rescue_v1",
  "decision": "winner",
  "sample_size": 52,
  "runtime_hours": 31,
  "primary_metric": {
    "name": "return_rate",
    "treatment": 0.29,
    "holdout": 0.18,
    "delta": 0.11
  },
  "guardrails_ok": true
}
```

---

## 7) Insight Copilot

### 7.1 Query

| Control | Type | How to use it | Sample input | Expected result |
| --- | --- | --- | --- | --- |
| `Question` | Text box | Natural-language business question. | `how many high risk users do we have in 7d?` | Copilot runs a query against the evidence layer. |
| `Time Window` | Text box | Query time window. | `7d` | Request uses a seven-day window. |
| `Filters JSON` | Text area | Optional filter object. | `{}` | Filters are applied when present. |
| `Run Query` | Button | Sends the query request. | None | Latest response panel updates with query result. |

#### Sample query output
```json
{
  "query_id": "query_20260322_1230",
  "conclusion": "There are 128 high risk users in the last 7 days.",
  "evidence": [
    {
      "metric": "high_risk_users",
      "value": 128,
      "time_window": "7d"
    }
  ],
  "confidence": 0.94
}
```

### 7.2 Explain

| Control | Type | How to use it | Sample input | Expected result |
| --- | --- | --- | --- | --- |
| `Metric ID` | Text box | Metric to explain. | `promo_views` | Copilot explains that metric. |
| `Time Window` | Text box | Explanation time window. | `7d` | Explanation uses seven days. |
| `Dimensions` | Text box | Comma-separated dimensions. | `campaign,country,platform` | Dimension list is parsed into an array. |
| `Explain Metric` | Button | Runs the explanation request. | None | Latest response panel updates. |

#### Sample explain output
```json
{
  "conclusion": "Promo views dropped because Campaign A weakened in US iOS traffic.",
  "evidence": [
    { "dimension": "campaign", "value": "Campaign A", "delta": -0.21 },
    { "dimension": "country", "value": "US", "delta": -0.14 }
  ],
  "risk_notes": ["Low sample size on Android tablets"]
}
```

### 7.3 Recommend

| Control | Type | How to use it | Sample input | Expected result |
| --- | --- | --- | --- | --- |
| `Insight JSON` | Text area | Raw insight context for recommendation. | `{}` | Parsed into `insight`. |
| `Metric Context JSON` | Text area | Metric metadata used for recommendation. | `{"metric_id":"high_risk_users"}` | Parsed into `metric_context`. |
| `Generate Recommendation` | Button | Produces an actionable recommendation. | None | Latest response panel updates. |

#### Sample recommend input
```json
{
  "insight": {
    "issue": "high_risk_users increased"
  },
  "metric_context": {
    "metric_id": "high_risk_users"
  }
}
```

#### Sample recommend output
```json
{
  "recommended_action": "Create a churn rescue push workflow for high-risk users.",
  "impact_scope": "128 users",
  "confidence": 0.88,
  "risk_notes": ["Respect quiet hours and cooldown policies"]
}
```

### 7.4 Report

| Control | Type | How to use it | Sample input | Expected result |
| --- | --- | --- | --- | --- |
| `Report Type` | Select | Choose `daily` or `weekly`. | `daily` | Request uses the selected report cadence. |
| `Time Window` | Text box | Report time window. | `7d` | Report covers the last seven days. |
| `Generate Report` | Button | Creates the report. | None | Latest response and reports list update. |

#### Sample report output
```json
{
  "report_id": "report_20260322_1240",
  "report_type": "daily",
  "time_window": "7d",
  "conclusion": "Churn rescue performance improved over the last 7 days."
}
```

### 7.5 Query Logs, Anomalies, And Reports

| Control | Type | How to use it | Sample input | Expected result |
| --- | --- | --- | --- | --- |
| `Query log ID` | Text box | Enter a query id from a prior result. | `query_20260322_1230` | Used to load that query log. |
| `Load Query Log` | Button | Loads the query log by id. | None | Query log output panel updates. |
| `Refresh` | Button | Reloads anomalies and reports metadata. | None | Anomalies and report lists refresh. |

#### Sample query log output
```json
{
  "query_id": "query_20260322_1230",
  "question": "how many high risk users do we have in 7d?",
  "sql": "SELECT COUNT(*) FROM ...",
  "result": 128
}
```

---

## 8) Settings
The `Settings` module is now a tabbed page. The left sidebar opens `Settings` directly, and the in-page tab strip controls the visible placeholder or live settings surface.

### 8.1 Settings Tab Strip

| Control | Type | How to use it | Sample input | Expected result |
| --- | --- | --- | --- | --- |
| `Profile` | Tab button | Opens the profile placeholder layout. | None | The profile information and password placeholder cards become visible. |
| `Organization` | Tab button | Opens the organization workspace tab. | None | Live workspace and session controls become visible. |
| `Projects` | Tab button | Opens the projects placeholder layout. | None | Project placeholder rows become visible. |
| `Teams` | Tab button | Opens the teams placeholder layout. | None | Team placeholder rows become visible. |
| `Notifications` | Tab button | Opens the notifications tab. | None | Notification placeholder rows become visible. |
| `Billing` | Tab button | Opens the billing placeholder layout. | None | Billing placeholder rows become visible. |

### 8.2 Profile

The `Profile` tab is currently a placeholder layout styled to match the new SaaS settings reference.

| Control | Type | How to use it | Sample input | Expected result |
| --- | --- | --- | --- | --- |
| `Change Avatar` | Placeholder button | Visible for layout only. It does not upload an image yet. | None | No backend action occurs yet. |
| `First Name` | Placeholder text box | Shows example profile content. | `John` | Layout only. No save is performed yet. |
| `Last Name` | Placeholder text box | Shows example profile content. | `Doe` | Layout only. No save is performed yet. |
| `Email` | Placeholder text box | Shows example profile content. | `john@company.com` | Layout only. No save is performed yet. |
| `Job Title` | Placeholder text box | Shows example profile content. | `Product Manager` | Layout only. No save is performed yet. |
| `Bio` | Placeholder text area | Shows example profile content. | `Experienced product manager with a passion for building great products` | Layout only. No save is performed yet. |
| `Save Changes` | Placeholder button | Visible for layout only. | None | No backend action occurs yet. |
| `Current Password` | Placeholder password box | Visible for layout only. | `placeholder-password` | No password update occurs yet. |
| `New Password` | Placeholder password box | Visible for layout only. | `placeholder-password` | No password update occurs yet. |
| `Confirm Password` | Placeholder password box | Visible for layout only. | `placeholder-password` | No password update occurs yet. |
| `Update Password` | Placeholder button | Visible for layout only. | None | No backend action occurs yet. |

### 8.3 Organization

The `Organization` tab holds the live shell controls that still drive workspace and session behavior.

| Control | Type | How to use it | Sample input | Expected result |
| --- | --- | --- | --- | --- |
| `Switcher` | Button | Opens the full-screen workspace selector overlay from inside Settings. | None | Lets you switch organization space or project. |
| `New Project` | Button | Opens the create-project overlay from inside Settings. | None | Creates a new project in the current organization space after success. |
| Current workspace card | Read-only summary | Shows the active organization space and project. | `North Star Games / Live Ops` | Confirms the live context before using shell shortcuts. |
| Session state card | Read-only summary | Shows the current login or demo state. | `Google alice@example.com @ northstar / liveops` | Confirms the current authenticated session. |
| Auth session card | Read-only summary | Shows the current login or local/demo state from inside Settings. | `Google alice@example.com @ northstar / liveops` | Confirms the current authenticated session before you switch workspaces or log out. |
| `Continue with Google` | Button | Starts the Google PKCE login flow from inside Settings. | None | Browser redirects to Google and returns with a bearer token. |
| `Logout` | Button | Clears the current bearer token and ends the authenticated session. | None | Session returns to the organization URL gate so the next sign-in starts from org selection. |
| `API Key` | Password box | Optional legacy/demo API key entry. This stays hidden when Google login is configured or an OIDC bearer session is active. | `local-demo-key` | Local/demo requests reuse the stored API key in the browser. |
| Application startup status | Read-only status line | Shows the latest startup or health result from inside Settings. | `Application start completed (mock)` | Confirms whether the backend is reachable from the console. |

### 8.4 Projects, Teams, And Billing

The `Projects`, `Teams`, and `Billing` tabs are placeholder layouts only. Their visible rows are present for design and navigation structure, but they do not execute backend actions yet.

### 8.5 Notifications

The `Notifications` tab is currently a placeholder layout only. Theme mode now lives in the top-right header selector instead of inside Settings.

| Control | Type | How to use it | Sample input | Expected result |
| --- | --- | --- | --- | --- |
| Notification rows | Placeholder rows | Visible for layout only. | None | No backend action occurs yet. |

---

## 9) Help Page
The `Help` module is a built-in quick reference. It does not contain action buttons that mutate backend state. Use it for:
- recommended end-to-end operator order
- role guide
- copy-paste audience definition samples
- workflow and experiment sample JSON
- copilot example prompts
- common issue troubleshooting

Recommended use:
1. Open `Help` when you need starter payloads.
2. Copy a sample payload into the relevant form in another module.
3. Return to the target module and execute the live action there.

---

## 10) Representative End-To-End Example

### Goal
Create a high-risk churn cohort, bind it to a workflow, measure it with an experiment, and review the result in Copilot.

### Step-by-step
1. In `Data Core -> Connectors`, save an `Amplitude` connector.
2. In `Data Core -> Imports`, choose `Amplitude`, set `Start Date=2026-03-01`, `End Date=2026-03-07`, then click `Import Data`.
3. Wait for the import to reach a completed or ready state.
4. In `Audience Engine`, create a SQL cohort:
   - `Name`: `churn_rescue_high_risk`
   - `Type`: `SQL`
   - `Definition JSON`:
     ```json
     {
       "sql": "SELECT user_id AS canonical_user_id, email FROM prediction_results WHERE predicted_churn_risk = 'high'"
     }
     ```
   - Click `Create Cohort`.
5. In `Action Orchestrator`, create a workflow:
   - `Name`: `daily_churn_rescue`
   - `Cohort`: select the new cohort
   - `Experiment ID`: `churn_rescue_v1`
   - `Trigger Type`: `daily_schedule`
   - `Hour`: `10`
   - `Minute`: `0`
   - `Content`: `Come back for a reward.`
   - Click `Create Workflow`.
6. In the workflow table, click `Publish`.
7. In `Experiment Hub`, load `churn_rescue_v1`, review the summary, and click `Start` if the experiment is still inactive.
8. After executions and outcomes accumulate, click `Record Decision`.
9. In `Insight Copilot`, run:
   - `Question`: `how many high risk users do we have in 7d?`
   - Click `Run Query`.
10. Review the latest Copilot response and, if needed, generate a recommendation or report.

---

## 11) Current Known UI Caveats
- `Data Core -> Governance -> Save Limits` is currently a placeholder and is not wired in the frontend JavaScript.
- `Webhook URL` and `Webhook Token` on the churn export panel are mainly relevant when the provider is `webhook`.
- Some lists and selectors require prior data. Examples:
  - workflow cohort selector needs cohorts loaded first
  - import detail selector needs imports loaded first
  - export diagnostics selector needs export jobs present first
  - query log loader needs a real `query_id`

---

## 12) Documentation Maintenance Rule
When any user-facing function, button label, form field, workflow, or sample payload changes:
1. Update this file.
2. Update `README.md`.
3. Update any module PRD or runbook if the behavior change is cross-cutting or production-relevant.
4. Call out placeholder or not-yet-wired controls explicitly instead of documenting them as working features.
