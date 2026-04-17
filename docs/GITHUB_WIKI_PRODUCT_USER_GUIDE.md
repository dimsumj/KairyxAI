# KairyxAI Product User Guide

> GitHub Wiki source document. Keep this file aligned with the live operator console, `README.md`, and any user-facing product changes. `README.md` is intentionally brief; this guide is the canonical detailed reference for modules, controls, workflows, sample input, and representative output.

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
| Sidebar collapse button | Button | Shrinks the desktop sidebar to a tight icon rail and expands it again when clicked a second time. In collapsed mode the site brand is hidden, the rail keeps only the module icons, and hovering or focusing an icon opens that module's section list in a right-side popout above the page. The popout stays reachable while you move the pointer from the icon into that section list. Clicking a collapsed icon routes to that module's first section and closes the temporary popout. The desktop shell also auto-collapses this rail when the viewport drops below `1200px`. | None | Navigation uses less horizontal space while still exposing the current module sections from the icon rail. |
| `Switcher` | Button | Opens the full-screen workspace selector overlay from `Settings -> Organization`, returns the browser to the base gateway URL, and lets you type a different organization URL without reusing the currently active org. | None | Lets you choose an organization and project before entering the app. |
| `New Project` | Button | Opens the new-project overlay from `Settings -> Organization`. | None | Creates a new project and switches into it after success. |
| Sidebar profile chip | Footer button | Shows the current signed-in identity at the bottom-left of the sidebar. Click it to open the account menu and use `Log out`. | `Studio Operator` | Opens the account menu, and `Log out` clears the app session then returns the shell to the organization URL gate. |
| Top bar search | Search box | Enter a module title or section label to jump directly to it. The top bar keeps the search field on the left and the theme selector on the right. | `settings` | The matching module or section opens and the matching page becomes active. |
| Theme mode selector | Three-button segmented control | Use the header buttons to follow the system theme or force light or dark mode. The preference is stored in local storage for the current browser. | `Dark` | The shell and module pages immediately switch to the selected theme mode. |
| Sidebar module links | Navigation buttons | Hover or focus a module to expand its section list downward in the full sidebar. Click the module button to open that module's first section by default. Click the same already-open module again in the expanded sidebar to collapse its section list while keeping the current page active. In collapsed mode, hovering or focusing an icon opens that section list in a right-side popout, and clicking the icon routes to the first section then dismisses the popout. The `Settings` module is the exception: it opens directly into the Settings page without a sidebar submenu. | `Audience Engine` | The first section under that module becomes active and the matching page content loads, and a repeated click on that same open module collapses the inline section list. |
| Sidebar section list | Inline submenu or collapsed popout | Click any section button in the expanded list under a module, or in the collapsed right-side popout, to jump directly to that section. In collapsed mode, the popout closes after the navigation fires. | `Versions & Comparison` | The matching section becomes active and its content scrolls into view. |
| Workspace startup status | Status line | Read-only. Visible in the full-screen onboarding or workspace gate even when the sidebar is hidden. | `Application start completed (mock)` | Confirms that the application finished startup and the backend health check passed. |

### 2.2 Recommended First-Time Path
1. Use `Continue with Google`.
2. After Google sign-in, follow the gateway state that matches your account:
   - if you belong to `0` organizations, create a new organization and its first project
   - if you belong to `1` organization with `1` active project, the console enters that workspace directly
   - if you belong to `1` organization with multiple active projects, continue to project selection for that org
   - if you belong to `2+` organizations, choose the organization you want to enter first
3. In the project step, either choose an existing project or create a new project if your org role allows it.
4. Go to `Data Core -> Connectors` and create at least one data connector. If you want Ask AI to use Gemini, LM Studio, Ollama, or another OpenAI-compatible runtime, also save an entry under `AI Agents & Models`.
5. Go to `Data Core -> Imports` and run an import.
6. Go to `Audience Engine` and create or refresh a cohort.
7. Go to `Data Core -> Connectors`, click `Connect Campaign Provider`, save a SendGrid, Braze, or Wynn PushNotifier provider connection, then go to `Action Orchestrator` to either draft an email campaign in `Email Campaigns`, draft or update a push workflow in `Push Notifications`, or manage the resulting schedules in `Workflow Studio`.
8. Go to `Experiment Hub` and save the linked experiment config.
9. Use the global `Ask AI` bubble from any page for dashboard summary, cohort setup, experiment setup, connection setup, product help, and sample payloads, then open `Insight Copilot` only when you want the manual query, explain, recommend, and report tools directly.
10. Go to `Settings` if you want to manage login state, review application startup status, switch organizations or projects, create or delete projects, manage organization members, or review the lighter placeholder profile, notification, and billing layouts.

### 2.3 Deployment Surface Notes

- `Local demo mode` keeps mock state in local filesystem cache files by default.
- `Vercel demo mode` is an isolated demo adapter. It keeps `/` as the gateway page, keeps the main app on `/{organization_id}`, and uses database-backed mock persistence only on that adapter.
- `Cloud Run / GCP production` remains the real production deployment path. It should not use the Vercel demo adapter, runtime SQLite fallback, or database-backed mock demo storage.
- Health payloads now expose deployment-safe runtime diagnostics:
  - `control_plane_database_backend`
  - `control_plane_database_persistent`
  - `control_plane_database_fallback_active`
  - `mock_state_backend`
  - `mock_state_persistent`

### 2.4 Onboarding And Workspace Overlays

#### Google login gate

| Control | Type | How to use it | Sample input | Expected result |
| --- | --- | --- | --- | --- |
| `Continue with Google` | Button | Opens Google's browser popup sign-in flow before any onboarding or workspace selection is shown. If the account chooser is dismissed, the same button can be used again immediately. | None | Google opens in a browser popup, returns a Google ID token-backed bearer session, and the console keeps the browser on the base app URL until the organization and project are resolved. |
| Workspace startup status | Status line | Read-only. Visible before login. | `Application start completed (mock)` | Confirms the backend is up before the user signs in. |

Every user now passes through the Google login gate first. After successful sign-in, the console keeps the browser on the base gateway URL and then does one of these things:
- opens the organization onboarding wizard if the user has no org memberships yet
- enters the workspace immediately if the user belongs to exactly one organization and that organization has exactly one active project
- opens project selection if the user belongs to exactly one organization and that organization has multiple active projects
- opens the organization gateway plus an accessible-organization list if the user belongs to two or more organizations
- rewrites the browser URL to `https://<base-url>/<organization_id>` only after an active organization and project are chosen or created

If the user typed an organization URL before Google sign-in, the gateway carries that value into the next step after login. A first-time user still lands on the onboarding wizard, but the organization URL field is prefilled with the value they already entered.

Across the console, protected module pages now wait for a resolved organization and project workspace before they load live data. During Google-login session handoff, stale workspace recovery, or a just-created workspace becoming active, the UI stays in a neutral waiting state instead of rendering raw backend membership errors inside module cards.

In deployed Google-login environments, the base URL itself is only the gateway page. The main operator experience is shown only after the browser is on the organization path such as `https://<base-url>/northstar`. Direct organization paths are authoritative, so opening `/<organization_id>` does not inherit a different org from stale browser storage.

#### Organization onboarding wizard

| Control | Type | How to use it | Sample input | Expected result |
| --- | --- | --- | --- | --- |
| `Organization URL` | Text box | Enter the URL slug that should appear after the base URL. New organization URLs must use lowercase letters and numbers only, can be at most 16 characters long, and must be globally unique. | `northstar` | The console stores this as the internal `organization_id` and uses it in the org-scoped path. |
| `Continue` | Button | Moves from the organization URL step to the project step. | None | The console keeps the generated organization id internally and opens the project form. |
| `Project Name` | Text box | Enter the display name for the first project. | `Live Ops` | The name is shown in the project selector. |
| `Create Project` | Button | Creates the organization, first project, and the creator's `owner` role in that organization. | None | The wizard closes, the new workspace becomes active by default, the first project becomes the default project for that org, and the browser URL becomes `/<organization_id>`. |

The console now asks for the org URL directly and generates the internal organization display name from that slug. New organization URLs are limited to lowercase letters and numbers only, with a maximum length of 16 characters, and the chosen URL must not already exist anywhere in the product. It still generates the internal `project_id` automatically from the project name you type. The backend still stores the organization id internally as `tenant_id`, but that internal field is no longer part of the visible login or workspace UI. Google sign-in always returns to the base app URL first; if the user has no memberships, the gateway advances directly into the organization URL onboarding step, reusing any org URL the user already entered before sign-in. Once the session is validated and a workspace exists, the console rewrites the page URL to the active organization path, and the creator is placed into the newly created organization and project automatically.

Gateway validation rules for the org step are:
- if the typed org does not exist, the user can create it
- if the typed org already exists and the signed-in Google account belongs to it, the user continues into direct entry or project selection for that org depending on how many active projects it has
- if the typed org already exists and the signed-in Google account does not belong to it, the gateway must show an explicit error that the org exists but this account is not a member
- if the user tries to create an organization URL that already exists, creation fails with an explicit already-exists error instead of reusing the existing org

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
| `Your organizations` | Select or list | When the signed-in Google account belongs to two or more orgs, choose one of the accessible organizations first. | `North Star Games` | The console loads the projects for that organization and keeps the gateway on `/` until a project is confirmed. |
| `Organization URL` | Text box | Type the organization URL you want to open. | `northstar` | The console resolves that organization, loads its projects, and moves to the project step. |
| `Cancel` | Button | Closes the workspace switcher overlay without applying a new workspace. This button sits inline in the main action row beside the primary continue action. | None | Returns to the prior app state. |
| `Continue` | Button | Resolves the typed organization URL. | None | The project list for that organization loads. |
| `Existing Project` | Select | Choose a project that already exists inside the selected organization. If multiple active projects exist, the oldest active project is preselected as the default. | `sandbox` | The selected project becomes the active console context after continue. |
| `Use Existing Project` | Button | Confirms the selected existing project. Available to any member of the selected organization. | None | The gate closes, the console reloads data for that org/project, and the browser URL becomes `/<organization_id>`. |
| `New Project Name` | Text box | Enter a new project name if you want to create another project in the selected organization. | `Growth Sandbox` | The console generates the internal project id automatically. |
| `Create New Project` or `Create First Project` | Button | Creates a new project inside the selected organization. Available only to organization `owner` and `admin` users. | None | The project is created, the console switches into it, and the browser URL stays on `/<organization_id>`. |

When the typed organization already exists and the signed-in Google user has access to it, the gateway now stays on `/` and explicitly offers the two choices required for that org:
- `Use Existing Project`
- `Create New Project`

If the same signed-in user wants a different organization instead, they can use `Switcher`, return to the base gateway, type a new organization URL, and continue into the create-org flow from the same base gateway page. The gateway now preserves that newly typed organization URL through session validation instead of snapping back to the previously active org, and once the first project is created the browser lands on the new `/{organization_id}` path.

The switcher overlay does not use the shared footer `Close` button. In this mode the red inline `Cancel` button sits in the same main action row as `Continue`, matching the alignment used in the create-project overlay.

All org members can access all active projects in that organization. Project selection is a workspace choice, not a project-membership permission check.

#### New-project overlay

| Control | Type | How to use it | Sample input | Expected result |
| --- | --- | --- | --- | --- |
| `Project Name` | Text box | Enter the display name for the new project. | `Growth Sandbox` | The project is created with this name. |
| `Create Project` | Button | Creates the project in the selected organization. Available only to `owner` and `admin` users. | None | The project is created, it joins the org-wide project list, and the console switches into it. |
| `Cancel` | Button | Closes the new-project overlay. This button uses red styling to distinguish it from `Create Project`. | None | Returns to the prior workspace selection state. |

The create-project overlay does not show the shared footer `Close` button. In this mode the only exit action is the inline red `Cancel` button beside `Create Project`.

As in onboarding, the current new-project UI generates the internal `project_id` automatically from the typed project name and keeps the id field hidden.

#### Invite redemption behavior
- Organization invites are email-based and organization-level. The default invited role is `member`.
- Admins and owners can optionally copy a shareable invite link, but the actual access grant still belongs to the invited Google email.
- If the browser opens a URL containing `invite_code`, the console stores that invite locally before Google login.
- After successful Google login, the console first auto-activates any pending organization invite whose email matches the signed-in Google account, and the explicit invite-redeem call remains idempotent for that same user.
- On success, the user chooses or creates a project inside the invited org, and the browser URL becomes `/<organization_id>`.

---

## 3) Data Core

### 3.1 Churn Rescue Workbench
This page is the quickest end-to-end operator view for running prediction and exporting a churn-rescue audience.

#### Controls

| Control | Type | How to use it | Sample input | Expected result |
| --- | --- | --- | --- | --- |
| `Connect Data Source` | Button | Opens the connector setup flow from the main workbench so operators can configure data access before imports or prediction. | None | Routes to `Connectors` and opens the connector form. |
| `Prediction Target` | Select | Choose whether to run prediction by `Source` or by explicit `Import`. | `Source` | The audience selector switches between source-level and import-level options. |
| `Select Source` / `Select Import` | Select | In `Source` mode, choose a source such as `Amplitude 1`. In `Import` mode, choose a specific completed import. | `Amplitude 1` | Source mode resolves to the latest completed import for that source when the job starts; import mode uses the selected import directly. |
| `Prediction Engine` | Select | Choose the prediction execution mode. | `AI + Cloud` | The request uses the selected prediction mode. |
| Local model status badge | Badge | Read the current readiness of the `Local Model` path before running prediction. | `Learning` | Shows whether local prediction is `Ready`, `Learning`, or `Fallback`. |
| `Train Local Model` | Button | Manually trigger a local batch retrain from the workbench. | None | Starts a local training run and updates the inline training status when complete. |
| `Refresh Model Status` | Button | Reload the latest local-model readiness and training status without starting a run. | None | Refreshes the badge, readiness details, and inline training status. |
| Local model training status | Inline status text | Read the latest training state, labeled-row count, class balance, and last update time. | `Fallback - 42/12 labeled rows` | Shows the most recent local model training outcome and supporting detail. |
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

The import source form and imported-data list now wait for a resolved organization and project workspace before they load. During Google-login session handoff or workspace switching, the page stays in a neutral waiting state instead of replacing the import form with a raw membership error.

Import failure help tooltips now render above nearby controls so the failure reason stays readable even when the imported-data table sits above other cards and form fields.

#### Controls

| Control | Type | How to use it | Sample input | Expected result |
| --- | --- | --- | --- | --- |
| `Import Source` | Select | Choose a configured ingestion source or BigQuery connector. | `Warehouse Scores` | Import request uses that connector/source. |
| `Start Date` | Date | Beginning of the import window for event connectors. For BigQuery table imports, this is an optional filter that is only sent when both dates are provided and a timestamp column is mapped. | `2026-03-01` | Event imports send `20260301`; BigQuery imports send `2026-03-01`. |
| `End Date` | Date | End of the import window for event connectors. For BigQuery table imports, this is an optional filter that is only sent when both dates are provided and a timestamp column is mapped. | `2026-03-07` | Event imports send `20260307`; BigQuery imports send `2026-03-07`. |
| `Browse Tables` | Select | For BigQuery sources, pick a discovered dataset table to prefill the table name. When the connector has not fetched an exact count yet, the picker shows `unknown rows` instead of a fake `0`. | `prediction_scores` | Table name field is populated from the discovered table list. |
| `Refresh Tables` | Button | Reloads the BigQuery table list from the selected connector. | None | Updated table list is shown in the status copy. |
| `Fetch Row Count` | Button | Runs an exact row-count lookup for the selected or manually entered BigQuery table without reloading the whole table list. If table metadata access is blocked but the connector can still run queries, the count falls back to a direct `COUNT(*)` query. | `prediction_scores` | Status copy shows the exact count, and the discovered table label is updated for that table. |
| `Table Name` | Text | Enter the BigQuery table to import when you do not want to use the discovered table list. Letters, numbers, and underscores only. | `prediction_scores` | Request targets that BigQuery table. |
| `Import Type` | Select | Choose whether the selected table becomes external prediction scores or a churn list. | `external_prediction_scores` | Mapping fields and validation switch to the selected import type. |
| `WHERE Filter (optional)` | Text | Add a safe SQL filter expression for the table read. Semicolons, comments, and write statements are rejected in the browser before submit. | `country = 'US'` | BigQuery import reads only the filtered rows. |
| `Canonical User ID Column` | Text | Required BigQuery mapping for the stable player identifier. | `player_id` | Request includes `column_mapping.canonical_user_id`. |
| `Prediction score mappings` | Text inputs | For `external_prediction_scores`, map score, risk, timestamp, and optional enrichment columns. At least `Predicted Risk Column` or `Score Column` is required. | `score`, `risk`, `scored_at` | Request includes BigQuery score mappings. |
| `Churn list mappings` | Text inputs | For `churn_list`, map reason, segment, and optional `as_of_timestamp` columns. | `reason`, `segment`, `as_of_timestamp` | Request includes BigQuery churn-list mappings. |
| `Activate Cohort` | Checkbox | For `churn_list`, immediately make the imported churn roster available as a cohort. | Checked | Backend activates the generated cohort after import completion. |
| `Cohort Name` | Text | Optional display name for an activated churn-list cohort. | `High Risk APAC` | Activated cohort uses that name instead of the default. |
| `Import Data` / `Import BigQuery Table` | Button | Creates a new import job. In mock-mode deployed environments, the run is kicked off in the background immediately after creation. | None | Import job appears in the imported data list and the page polls for status updates instead of waiting on one long request. |
| Import row `Stop` | Row button | Stops a queued or running import. | None | Job moves toward `stopping` then `stopped`. |
| Import row `Delete` | Row button | Deletes a completed, failed, or stopped import. | None | Import disappears from the list after confirmation, and the backend also removes that import's temporary raw file objects, job-scoped staging rows, and derived sanitized state. |
| `Import Job` | Select | Choose a non-failed import job for detail views. Failed imports remain visible in the imported-data table, but they are excluded from downstream selectors such as `Import Operations`. | `import_20260322_101500` | Detail actions apply to the selected non-failed import. |
| `Load Operations` | Button | Loads import operational detail on demand. | None | Operations JSON appears in the detail output. |
| `Load Quality` | Button | Loads import quality detail on demand. | None | Quality JSON appears in the detail output. |
| `Load Manifests` | Button | Loads manifest detail for the selected import on demand. | None | Manifest JSON and list appear. |
| `Alias` | Select | Choose a warehouse contract alias. | `standardized` | Contract detail request targets that alias. |
| `Load Contract` | Button | Loads the selected schema contract on demand. | None | Contract JSON appears in the schema output. |
| `List All` | Button | Lists all available schema contracts on demand. | None | Contract list is displayed for all aliases. |

#### Sample import input
```json
{
  "source_name": "amplitude",
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
- Active import rows now expose a status `?` tooltip with the current step, such as connecting to the source, staging events, processing manifests, or reading rows from a BigQuery table.
- Completed imports clear old failure and timeout metadata when a rerun succeeds, so `Ready to Use` rows no longer keep stale timeout tooltips after refresh.
- In mock-mode deployed environments, clicking `Import Data` starts the run in the background so the browser does not sit on a long import request until completion.
- Right after backend restart, a transient control-plane busy response may appear; retry the detail load if prompted.

#### BigQuery table import browser flow
- Selecting a BigQuery connector in `Import Source` switches the form into table-import mode.
- The browser can browse discovered tables, import one table at a time, and submit either `external_prediction_scores` or `churn_list` payloads through the same `/api/v1/imports` endpoint.
- BigQuery date filters remain optional and only work when the matching timestamp column is mapped:
  - `external_prediction_scores` uses `score_timestamp`
  - `churn_list` uses `as_of_timestamp`
- Column names and table names are validated in the browser to simple BigQuery identifiers before submit.
- `WHERE Filter` is passed through only when it remains a read-only filter expression.

#### Sample BigQuery external prediction import request
```json
{
  "source_name": "Warehouse Scores",
  "table_name": "prediction_scores",
  "resource_kind": "external_prediction_scores",
  "column_mapping": {
    "canonical_user_id": "player_id",
    "user_id": "player_id",
    "email": "email",
    "predicted_churn_risk": "risk",
    "score": "score",
    "score_timestamp": "scored_at"
  },
  "start_date": "2026-04-01",
  "end_date": "2026-04-03"
}
```

#### Sample BigQuery churn-list import request
```json
{
  "source_name": "Warehouse Lists",
  "table_name": "churned_users",
  "resource_kind": "churn_list",
  "activate_cohort": true,
  "cohort_name": "vip_churn_list",
  "column_mapping": {
    "canonical_user_id": "player_id",
    "user_id": "player_id",
    "email": "email",
    "reason": "reason",
    "segment": "segment",
    "as_of_timestamp": "as_of"
  }
}
```

#### Sample BigQuery import completion detail
```json
{
  "id": "imp_20260406_101500",
  "status": "completed",
  "progress": {
    "details": {
      "rows_seen": 3,
      "rows_loaded": 2,
      "duplicate_rows": 1,
      "linked_prediction_job_id": "pred_20260406_101700",
      "bigquery_table_import": {
        "table_name": "prediction_scores",
        "resource_kind": "external_prediction_scores",
        "row_count": 3,
        "duplicate_rows": 1
      }
    }
  }
}
```

BigQuery table import behavior:
- `resource_kind="external_prediction_scores"` requires `canonical_user_id` and either `predicted_churn_risk` or `score`.
- `resource_kind="churn_list"` requires `canonical_user_id` and can materialize a list cohort directly.
- `score_timestamp` or `as_of_timestamp` only becomes required when `start_date` and `end_date` are supplied.
- Duplicate rows are suppressed by `canonical_user_id`; later rows win and duplicate counts are recorded in the import detail.
- Completed prediction imports create a linked external prediction job and native prediction results.
- Completed churn-list imports can create and activate a linked list cohort.

### 3.3 Connectors
Use this page to register upstream ingestion sources, campaign-provider credentials, and the backend-managed runtimes that Ask AI uses. OpenAI-compatible runtime URLs are called by the backend, so the saved endpoint must be reachable from the backend runtime. `LM Studio` and `Ollama` localhost presets are intended for self-hosted or local deployments.

#### AI Agents & Models

| Control | Type | How to use it | Sample input | Expected result |
| --- | --- | --- | --- | --- |
| `Connect Ask AI Runtime` | Button | Opens the model-profile form used by Ask AI. | None | The runtime setup form becomes visible. |
| `Runtime Type` | Select | Chooses which Ask AI preset to configure. | `LM Studio` | The preset-specific model and endpoint fields appear. |
| `Profile Name` | Text box | Sets the label shown in the AI runtime table and Ask AI model selector. | `LM Studio Local` | The runtime is saved under this name. |
| `Gemini Model` | Select | Appears when `Runtime Type` is `Gemini`. | `gemini-2.5-flash` | The saved runtime targets the selected Gemini model. |
| `Google API Key` | Password box | Appears when `Runtime Type` is `Gemini`. | `AIza...` | The backend-managed Gemini profile stores the key securely. |
| `Model Name` | Text box | Appears for `LM Studio`, `Ollama`, and `Custom OpenAI-compatible`. | `llama3.1` | Ask AI sends requests to that OpenAI-compatible model name. |
| `API Key / Token` | Password box | Appears for `LM Studio`, `Ollama`, and `Custom OpenAI-compatible`. Leave it blank when the local or hosted OpenAI-compatible endpoint does not require bearer auth. | `sk-live-key` | The saved profile sends bearer auth only when a token is configured. |
| `Base URL` | Text box | Appears for `LM Studio`, `Ollama`, and `Custom OpenAI-compatible`. Base URLs with or without a trailing `/v1` both work, but the endpoint must be reachable from the backend runtime. | `http://127.0.0.1:11434/v1` | Kairyx targets the OpenAI-compatible chat-completions path correctly for that endpoint. |
| `Use this runtime as the Ask AI default` | Checkbox | Makes the saved runtime the default model profile for new Ask AI sessions. | Checked | Ask AI uses this runtime unless the operator selects another profile for the session. |
| `Save Runtime` / `Update Runtime` | Button | Saves the runtime. Leave the API key blank while editing if you want to keep the existing secret. | None | The runtime appears in the `AI Agents & Models` table. |
| `Refresh` | Button | Reloads the current runtime list. | None | The runtime table refreshes from the control plane. |
| Runtime row `Edit` | Row button | Loads the saved runtime back into the form. | None | The form switches to update mode. |
| Runtime row `Set Default` | Row button | Makes a non-default runtime the Ask AI default. | None | New Ask AI sessions use that runtime by default. |
| Runtime row `Delete` | Row button | Deletes a saved non-system runtime after confirmation. | None | The runtime disappears from the list. |

Credential storage behavior:
- Browser-entered runtime secrets are accepted on save, encrypted before persistence, and redacted from subsequent API responses.
- Runtime reads return `null` for raw secret fields and expose only the matching `*_configured` metadata flag.
- API clients can still use `*_ref` values when the team prefers an external secret manager instead of control-plane encrypted storage.

Runtime presets shipped in the current frontend:

| Runtime preset | Backend provider | Fields / behavior | Sample input |
| --- | --- | --- | --- |
| `Gemini` | `gemini` | `Google API Key`, `Gemini Model` | `api_key=google_key_123`, `model_name=gemini-2.5-flash` |
| `LM Studio` | `openai` | `Model Name`, optional `API Key / Token`, `Base URL` with default `http://127.0.0.1:1234/v1` | `model_name=local-model`, `base_url=http://127.0.0.1:1234/v1` |
| `Ollama` | `openai` | `Model Name`, optional `API Key / Token`, `Base URL` with default `http://127.0.0.1:11434/v1` | `model_name=llama3.1`, `base_url=http://127.0.0.1:11434/v1` |
| `Custom OpenAI-compatible` | `openai` | `Model Name`, optional `API Key / Token`, `Base URL` | `model_name=gpt-4.1-mini`, `base_url=https://api.openai.com/v1` |

Existing Anthropic profiles still render in the AI runtime list and Ask AI model selector when they were created through the API, but the current Connectors form does not create new Anthropic profiles.

Deployment note:
- `LM Studio` and `Ollama` presets use localhost-style defaults for self-hosted or local deployments where the backend can reach that machine or network.
- Hosted production deployments reject localhost and other private-network runtime URLs. Use a publicly reachable HTTPS endpoint or a self-hosted deployment in that case.

#### Data Source Connectors

| Control | Type | How to use it | Sample input | Expected result |
| --- | --- | --- | --- | --- |
| `Connect Data Source` | Button | Opens the source-connector creation form. | None | The connector type form becomes visible. |
| `Display Name` | Text box | Sets the connector name shown in the saved connector list and used by downstream flows. | `Warehouse Scores` | The connector is saved under this name. |
| `Select Connector Source` | Select | Chooses which connector form to render. | `Amplitude` | Connector-specific fields appear. |
| `Save Connector` | Button | Saves the connector after the required fields are filled. | None | Connector is created and shown in the saved connector list. |
| `Cancel` | Button | Resets the form and returns to the source-connector list. | None | Dynamic fields disappear and the form hides. |
| Saved connector `Delete` | Row button | Deletes the connector after confirmation. | None | Connector is removed from the saved list. |

#### Data-source connector fields

| Connector type | Fields / API payload | Sample input |
| --- | --- | --- |
| `Amplitude` | `Amplitude API Key`, `Amplitude Secret Key` | `api_key=amp_public_123`, `secret_key=amp_secret_456` |
| `Adjust` | `Adjust API Token`, `Adjust API URL (optional)` | `api_token=adj_token_123`, `api_url=https://dash.adjust.com/control-center/reports-service` |
| `AppsFlyer` | `AppsFlyer API Token`, `AppsFlyer App ID`, `AppsFlyer Pull API URL (optional)` | `api_token=af_token_123`, `app_id=id123456789`, `pull_api_url=https://hq1.appsflyer.com/api/raw-data/export/app` |
| `BigQuery` | Browser form: `Google Cloud Project ID`, `BigQuery Dataset ID`, `BigQuery Location (optional)`, `How do you want to enter service account credentials?`, and either `Service Account JSON File` or `Service Account JSON`; API also supports `service_account_json`, `service_account_info_json`, and the matching `*_ref` fields | `project_id=my-prod-project`, `dataset_id=growth_inputs`, `location=US`, `Upload JSON file` |
| `SendGrid` | `SendGrid API Key` | `api_key=SG.xxxxx` |
| `Braze` | `Braze API Key`, `Braze REST Endpoint` | `api_key=braze_key_123`, `rest_endpoint=https://rest.iad-01.braze.com` |


Legacy `Google Gemini` connector records are no longer created from this generic connector form. Ask AI runtimes are managed through `AI Agents & Models` instead.

For lifecycle email campaigns and Wynn-backed push workflows, use `Data Core -> Connectors -> Campaign Provider Connections` to save tenant-scoped SendGrid, Braze, or Wynn PushNotifier accounts. SendGrid and Braze provider connections are used by `Action Orchestrator -> Email Campaigns`. Wynn PushNotifier provider connections are used by `Action Orchestrator -> Create Workflow` when a `push_notification` workflow should create a visible campaign in Wynn PushNotifier instead of using the simulator. The legacy `Data Core -> Connectors` SendGrid and Braze connector cards remain basic connector records and do not browse campaign assets for the new email campaign builder.

#### Campaign Provider Connections

Use the dedicated provider-connection card on `Data Core -> Connectors` to manage credentials for lifecycle email campaigns and Wynn-backed push workflows. Browser-entered API keys and bearer tokens are encrypted before storage and later reads expose only the matching `*_configured` flag. The credential form stays hidden until you click `Connect Campaign Provider`.

| Control | Type | How to use it | Sample input | Expected result |
| --- | --- | --- | --- | --- |
| `Connect Campaign Provider` | Button | Opens the provider-connection form when you want to add a new SendGrid, Braze, or Wynn PushNotifier account. | None | The provider selector and credential fields appear. |
| `Provider` | Select | Switches the form between SendGrid, Braze, and Wynn PushNotifier account setup. | `Wynn PushNotifier` | The provider-specific credential fields change immediately. |
| `Connection Name` | Text box | Sets the label that appears later in the email campaign builder or workflow composer. | `Wynn PushNotifier Production` | The provider connection is listed under that name. |
| SendGrid fields | Email box, text boxes, password box | When `Provider` is `SendGrid`, fill `Default From Email`, optional `Default From Name`, optional `Base URL`, and `SendGrid API Key`. | `rewards@example.com`, `KairyxAI Rewards`, `SG.xxxxx` | The SendGrid account can browse dynamic templates and send campaigns. |
| Braze fields | Text box, password box | When `Provider` is `Braze`, fill `Braze REST Endpoint` and `Braze API Key`. | `https://rest.iad-01.braze.com`, `braze_key_123` | The Braze account can browse API-triggered campaigns and execute them. |
| Wynn PushNotifier fields | Text box, password boxes | When `Provider` is `Wynn PushNotifier`, fill `PushNotifier Base URL`, `PushNotifier API Token`, optional `Default Deep Link Token`, and optional `Callback Signing Secret`. | `https://push.example.com`, `push-secret-token`, `campaign-default-token` | The connection becomes selectable for push workflows and Kairyx can create visible Wynn PushNotifier campaigns for explicit player IDs. |
| `Save Provider Connection` / `Update Provider Connection` | Button | Creates a new provider connection or updates the selected one. Leave the API key or API token blank while editing if you want to keep the existing secret. | None | The provider connection is saved and becomes selectable in the email campaign builder or workflow composer. |
| `Cancel` | Button | Hides the provider-connection form without saving changes. | None | The connector page returns to the provider list view. |
| `Refresh` | Button | Reloads the provider-connection list from the control plane. | None | The connector page reflects the latest saved connections. |
| Provider row `Edit` | Row button | Loads the selected provider connection into the form for editing. | None | The form switches to update mode for that row. |
| Provider row `Use in Campaign` | Row button | Visible for SendGrid and Braze rows. Jumps to `Action Orchestrator -> Email Campaigns`, sets the provider switch, selects that provider connection, and loads its assets. | None | The email campaign builder is preloaded for that provider account. |
| Provider row `Use in Workflow` | Row button | Visible for Wynn PushNotifier rows. Jumps to `Action Orchestrator -> Push Notifications` and preselects that provider connection in the push workflow composer. | None | The push workflow builder is preloaded for Wynn-backed `push_notification` delivery. |
| Provider row `Delete` | Row button | Removes the saved provider connection when it is no longer needed. Kairyx blocks the delete if any draft, scheduled, or sending campaign still references that provider connection. | None | The provider connection is deleted or the UI returns a guardrail error explaining which campaigns must be cancelled or removed first. |

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

#### BigQuery connector API
Once a BigQuery connector is saved with `project_id`, `dataset_id`, and tenant-scoped service account credentials, operators can use the connector health and dataset-discovery routes directly. If the runtime has `CONTROL_PLANE_SECRET_KEY`, browser-entered BigQuery credentials are encrypted before storage. Connector responses redact the saved credential payload and expose only the `*_configured` metadata flag.

#### Sample BigQuery connector request
```json
{
  "name": "Warehouse Scores",
  "type": "bigquery",
  "config": {
    "project_id": "warehouse-project",
    "dataset_id": "growth_inputs",
    "location": "US",
    "service_account_json": "{\"type\":\"service_account\",\"client_email\":\"warehouse-reader@tenant-warehouse.iam.gserviceaccount.com\",\"private_key\":\"-----BEGIN PRIVATE KEY-----\\n...\\n-----END PRIVATE KEY-----\\n\",\"token_uri\":\"https://oauth2.googleapis.com/token\"}"
  }
}
```

Teams that keep warehouse credentials in Secret Manager can still use:

```json
{
  "name": "Warehouse Scores",
  "type": "bigquery",
  "config": {
    "project_id": "warehouse-project",
    "dataset_id": "growth_inputs",
    "service_account_json_ref": "gsm://tenant-connectors/warehouse-scores"
  }
}
```

#### Sample BigQuery table listing output
```json
{
  "name": "Warehouse Scores",
  "type": "bigquery",
  "items": [
    {
      "table_name": "prediction_scores",
      "table_type": "table",
      "row_count": 120034
    },
    {
      "table_name": "churned_users_view",
      "table_type": "view",
      "row_count": null
    }
  ]
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

### 5.1 Email Campaigns

Use this section to build one-time lifecycle email campaigns across SendGrid and Braze. Provider connections are managed in `Data Core -> Connectors -> Campaign Provider Connections`; this page reuses those saved accounts, lets the operator switch providers, and loads the matching messaging assets for the selected provider connection.

#### 5.1.1 Provider Connections

Provider connections are no longer created inside `Action Orchestrator`. Create or update them in `Data Core -> Connectors`, then return here to build campaigns.

Provider behavior:
- `SendGrid` provider connections expose dynamic transactional templates and use the saved sender defaults.
- `Braze` provider connections expose API-triggered Braze campaigns and use Braze-side campaign configuration for sender behavior.

#### 5.1.2 Campaign Builder

| Control | Type | How to use it | Sample input | Expected result |
| --- | --- | --- | --- | --- |
| `Campaign Name` | Text box | Friendly name for the campaign record. | `spring_winback_reward` | Stored as the campaign name. |
| `Campaign Provider` | Select | Switches the builder between SendGrid and Braze mode. The provider switch filters the provider-connection list and changes the recipient identifier field that is shown. | `Braze` | The builder shows only Braze provider connections and Braze campaign assets. |
| `Provider Connection` | Select | Chooses which saved SendGrid or Braze account to use. | `Lifecycle Braze` | Asset browsing and send execution use that provider connection. |
| `Dynamic Template` / `Braze API Campaign` | Select | Chooses the provider-specific messaging asset. SendGrid loads dynamic templates; Braze loads API-triggered campaigns only. | `Winback Reward` | The campaign stores the selected `template_id` plus an asset summary snapshot. |
| `Refresh Assets` | Button | Reloads templates or Braze campaigns for the selected provider connection. | None | The asset select refreshes from the current provider account. |
| `Audience Source` | Select | Switches the audience input between prediction jobs and saved cohorts. | `Cohort` | The builder shows the matching audience selector and payload shape. |
| `Prediction Audience` | Select | Chooses the prediction job that provides recipient rows. The label now prefers the prediction audience label or resolved import name instead of the raw job id. | `High Risk Winback Import (completed)` | Campaign execution resolves recipients from that prediction job at send time. |
| `Cohort Audience` | Select | Chooses a saved cohort whose latest members already contain identifiers such as `user_id`, `canonical_user_id`, or `email`. | `VIP Returners (active)` | Campaign execution resolves recipients from that cohort at send time. |
| `Risk Filters` | Text box | Prediction-only filter for risk values to keep. Leave it blank to send to all non-churned prediction rows. | blank | All non-churned prediction rows are eligible at send time. |
| `Recipient Email Field` | Select | Visible when `Campaign Provider` is `SendGrid`. The options are sampled from JSON keys found in the selected audience rows. | `email` | Each SendGrid personalization uses that field as the `to` email. |
| `Recipient External ID Field` | Select | Visible when `Campaign Provider` is `Braze`. The options are sampled from JSON keys found in the selected audience rows. | `braze_external_id` | Each Braze recipient uses that field as `external_user_id`. |
| `Include already churned users` | Checkbox | Includes rows whose churn state is already marked as churned. | Checked | Churned rows are allowed into the send audience. |
| `Template Deeplink Variable` | Text box | Variable name that receives the final deeplink URL in the provider payload. | `deeplink_url` | SendGrid receives it in `dynamic_template_data`; Braze receives it in `trigger_properties`. |
| `Audience Deeplink Override Field (optional)` | Text box | If present on a row, this field wins over the campaign deeplink template. | `reward_deeplink_url` | Matching rows use the row-level deeplink directly. |
| `Campaign Deeplink Template (optional)` | Text box | URL template with `{field_name}` placeholders resolved from the audience row and campaign context. | `mygame://reward?user_id={user_id}&reward_id={reward_id}&campaign={campaign_id}` | Rows without an override field receive a rendered deeplink URL. |
| `Merge Fields JSON` | Text area | Maps provider template variables to row fields or literals. | See sample below | SendGrid builds `dynamic_template_data`; Braze builds `trigger_properties`. |
| `Schedule For (optional)` | Date/time picker | Sets the one-time scheduled send time in the operator's local timezone. | `2026-04-15 11:00` | Campaign status becomes `scheduled`. |
| `Clear` | Button | Clears the selected campaign and resets the builder to a new draft. | None | The form is ready for a new campaign record. |
| `Save Draft` | Button | Creates or updates the campaign in `draft` status. | None | Campaign saves without a schedule. |
| `Schedule Campaign` | Button | Creates or updates the campaign in `scheduled` status. | None | Campaign becomes editable scheduled work. |
| `Send Now` | Button | Runs the selected campaign immediately. If no campaign is selected yet, the console saves a draft first and then sends it. | None | Campaign executes through the selected provider and moves to `sent`, `sent_with_errors`, or `failed`. |

Audience behavior:
- Prediction audiences keep the existing risk-filter and include-churned controls.
- Cohort audiences skip prediction-only risk filtering and use the saved cohort member payload directly.
- Recipient field selects are populated from sampled audience JSON keys so operators can choose a field instead of typing raw paths blind.

#### Sample SendGrid email campaign request
```json
{
  "name": "spring_winback_reward",
  "provider": "sendgrid",
  "provider_connection_id": "pc_1234567890abcdef",
  "template_id": "d-1234567890abcdef1234567890abcdef",
  "audience": {
    "prediction_job_id": "pred_20260410_0900",
    "include_risks": ["high", "medium"],
    "include_churned": false
  },
  "recipient_email_field": "email",
  "merge_fields": {
    "first_name": { "source": "field", "value": "first_name" },
    "reward_name": { "source": "literal", "value": "Welcome Back Pack" }
  },
  "deeplink_template_field": "deeplink_url",
  "deeplink_override_field": "reward_deeplink_url",
  "deeplink_template": "mygame://reward?user_id={user_id}&reward_id={reward_id}&campaign={campaign_id}",
  "schedule_at": "2026-04-15T18:00:00Z"
}
```

#### Sample Braze email campaign request
```json
{
  "name": "spring_winback_braze",
  "provider": "braze",
  "provider_connection_id": "pc_braze_1234567890",
  "template_id": "cmp_api_1234567890",
  "audience": {
    "prediction_job_id": "pred_20260410_0900",
    "include_risks": ["high", "medium"],
    "include_churned": false
  },
  "recipient_external_id_field": "user_id",
  "merge_fields": {
    "first_name": { "source": "field", "value": "first_name" },
    "reward_name": { "source": "literal", "value": "Welcome Back Pack" }
  },
  "deeplink_template_field": "deeplink_url",
  "deeplink_template": "mygame://reward?user_id={user_id}&reward_id={reward_id}&campaign={campaign_id}"
}
```

#### Sample email campaign response
```json
{
  "email_campaign_id": "ec_1234567890abcdef",
  "name": "spring_winback_reward",
  "status": "scheduled",
  "provider": "sendgrid",
  "provider_connection_id": "pc_1234567890abcdef",
  "template_id": "d-1234567890abcdef1234567890abcdef",
  "template_summary": {
    "id": "d-1234567890abcdef1234567890abcdef",
    "name": "Winback Reward",
    "generation": "dynamic",
    "active_version": {
      "id": "ver_active",
      "subject": "Come back for a reward",
      "active": true
    }
  },
  "result_summary": {}
}
```

#### 5.1.3 Upcoming And Past Campaigns

| Control | Type | How to use it | Sample input | Expected result |
| --- | --- | --- | --- | --- |
| `Refresh` | Button | Reloads provider connections, prediction jobs, provider-specific messaging assets, and campaign lists. | None | The action-orchestrator campaign workspace refreshes. |
| Upcoming row `Edit` | Row button | Loads the selected draft or scheduled campaign into the builder. | None | The builder switches to that campaign. |
| Upcoming row `Send Now` | Row button | Executes the selected draft or scheduled campaign immediately. | None | Campaign leaves the editable queue and records execution results. |
| Upcoming row `Cancel` | Row button | Cancels a scheduled campaign. | None | Campaign status becomes `cancelled`. |
| Upcoming row `Delete` | Row button | Deletes a draft campaign. | None | Draft is removed from the list. |
| Past row `View` | Row button | Loads a sent, failed, or cancelled campaign into the detail panel. | None | The JSON detail panel shows the stored campaign snapshot and result summary. |

State rules:
- Only `draft` and `scheduled` campaigns are editable.
- Only `scheduled` campaigns can be cancelled.
- Only `draft` campaigns can be deleted.
- `sent`, `sent_with_errors`, `failed`, and `cancelled` campaigns stay read-only in the browser list and detail view.

### 5.2 Push Notifications

Use this section to create or update Wynn-backed push workflows. The push builder reuses the existing workflow backend, keeps the simulator fallback when no provider connection is selected, and writes draft workflow versions that later move through `Workflow Studio`.

| Control | Type | How to use it | Sample input | Expected result |
| --- | --- | --- | --- | --- |
| `Name` | Text box | Workflow name. | `daily_churn_rescue` | Stored as the workflow name. |
| `Cohort` | Select | Choose the source cohort. | `cohort_20260322_1200` | Workflow binds to that cohort. |
| `Experiment ID` | Text box | Link the workflow to an experiment id. | `churn_rescue_v1` | Publish and measurement use this experiment id. |
| `Trigger Type` | Select | Choose `daily_schedule` or `manual_test`. | `daily_schedule` | Trigger config uses the selected type. |
| `Hour` | Number box | Scheduled run hour. | `10` | Daily run executes at 10:00. |
| `Minute` | Number box | Scheduled run minute. | `15` | Daily run executes at `:15`. |
| `Channel` | Select | Fixed to `Push Notification` for this builder. | `Push Notification` | Workflow action stays on the push path. |
| `Global Daily Limit` | Number box | Max sends per day across workflow. | `5` | Policy blocks sends beyond five. |
| `Channel Daily Limit` | Number box | Max push sends per day. | `5` | Policy applies at the push-channel level. |
| `Cooldown Hours` | Number box | Cooldown per user. | `24` | Same user is skipped for 24 hours. |
| `Quiet Hours Start` | Number box | Start of no-send window. | `22` | Sends are blocked after 22:00. |
| `Quiet Hours End` | Number box | End of no-send window. | `7` | Sends resume at 07:00. |
| `Daily Budget Limit` | Number box | Workflow budget cap. | `25` | Policy blocks sends beyond budget. |
| `Blacklist IDs (comma separated)` | Text box | Users who should never receive this workflow. | `user_1,user_2` | Those users are always skipped. |
| `Provider Connection` | Select | Leave blank to keep simulator delivery, or choose a Wynn PushNotifier connection for live Wynn delivery. | `Wynn PushNotifier Production` | Execution uses the selected provider connection. |
| `Campaign Name` | Text box | Sets the Wynn campaign label created for the workflow send. | `winback_push` | The outbound payload includes `campaign_name`. |
| `Title` | Text box | Required for live Wynn delivery. | `Come back` | The outbound payload includes `title`. |
| `Schedule Override (optional)` | Date/time picker | Converts the local date and time into an ISO timestamp for Wynn PushNotifier. | `2026-04-16 11:30` | The outbound payload includes `scheduled_at`. |
| `Body` | Text area | Required for live Wynn delivery and also used as simulator content when no provider connection is selected. | `Rewards are waiting for you.` | The outbound payload includes `body`. |
| `Deep Link (optional)` | Text box | Sets the deep link metadata stored on the push request. | `wynn://promotions/welcome-back` | The outbound payload includes `deep_link`. |
| `Deep Link Token (optional)` | Text box | Overrides the provider connection's default deep link token for this workflow. | `campaign-default-token` | The outbound payload includes `deep_link_token`. |
| `Push Data JSON` | Text area | Must be valid JSON object text. | `{"reward_id":"reward_pack"}` | The outbound payload includes `data`. |
| `Advanced Provider Options` | Disclosure section | Expands the provider-specific options editor for push workflows. | None | The advanced JSON field becomes visible. |
| `Provider Options JSON` | Text area | Must be valid JSON object text. | `{"priority":"high"}` | The outbound payload includes `provider_options`. |
| `Requires manual confirmation` | Checkbox | Require manual confirmation before high-risk execution. | Checked | Workflow is marked confirmation-required. |
| `Create Push Workflow` / `Update Push Workflow` | Button | Creates a new draft workflow or updates the selected workflow back into a draft version. | None | The workflow is saved and becomes manageable in `Workflow Studio`. |
| `Clear` | Button | Clears the selected workflow and resets the builder to a new draft. | None | The form is ready for a new push workflow. |

Push builder behavior:
- Leaving `Provider Connection` blank keeps the legacy simulator path for push workflows.
- Selecting a Wynn PushNotifier provider connection switches the workflow to live Wynn delivery. Kairyx sends explicit cohort member `canonical_user_id` values as Wynn `player_ids`.
- Live Wynn push workflows require both `Title` and `Body`.
- `Push Data JSON` and `Provider Options JSON` must parse as JSON objects before the workflow can be saved.
- Editing a published or paused workflow creates a new draft version on that same workflow record.

#### Sample push workflow request
```json
{
  "name": "daily_churn_rescue_push",
  "cohort_id": "cohort_20260322_1200",
  "experiment_id": "churn_rescue_v1",
  "schedule": { "type": "daily" },
  "trigger": {
    "type": "daily_schedule",
    "hour": 10,
    "minute": 15
  },
  "action": {
    "channel": "push_notification",
    "provider_connection_id": "pc_wynn_1234567890",
    "campaign_name": "winback_push",
    "title": "Come back",
    "body": "Rewards are waiting for you.",
    "deep_link": "wynn://promotions/welcome-back",
    "deep_link_token": "campaign-default-token",
    "scheduled_at": "2026-04-16T18:30:00Z",
    "data": {
      "reward_id": "reward_pack"
    },
    "provider_options": {
      "priority": "high"
    }
  },
  "channel_config": {
    "channel": "push_notification",
    "provider_connection_id": "pc_wynn_1234567890",
    "campaign_name": "winback_push",
    "title": "Come back",
    "body": "Rewards are waiting for you.",
    "deep_link": "wynn://promotions/welcome-back",
    "deep_link_token": "campaign-default-token",
    "scheduled_at": "2026-04-16T18:30:00Z",
    "data": {
      "reward_id": "reward_pack"
    },
    "provider_options": {
      "priority": "high"
    }
  }
}
```

#### Sample workflow response
```json
{
  "workflow_id": "wf_20260322_1215",
  "name": "daily_churn_rescue_push",
  "status": "draft",
  "archived_at": null,
  "experiment_id": "churn_rescue_v1",
  "runtime_summary": {
    "last_run_at": null,
    "last_test_run_at": null,
    "next_run_at": null,
    "last_result": {},
    "totals": {
      "runs": 0,
      "test_runs": 0,
      "triggered": 0,
      "executed": 0,
      "success": 0,
      "failures": 0
    }
  }
}
```

### 5.3 Workflow Studio

Use this section to schedule email campaigns, manage push workflows, and review a unified operational summary. The table merges both resource types and adds a selected-item panel so operators can edit, schedule, archive, or inspect the current record without switching pages.

| Control | Type | How to use it | Sample input | Expected result |
| --- | --- | --- | --- | --- |
| `Refresh` | Button | Reloads email campaigns, push workflows, and the selected detail panel. | None | The studio list and detail state refresh. |
| `All` / `Email Campaigns` / `Push Workflows` / `Scheduled` / `Archived` | Filter buttons | Narrow the studio table to the relevant resource set. | `Scheduled` | Only scheduled email campaigns and published push workflows remain visible. |
| `Name` | Table column | Read-only. Shows the resource name plus the underlying id. | `daily_churn_rescue_push` | Operators can identify the exact campaign or workflow record. |
| `Type` | Table column | Read-only. Distinguishes `Email Campaign` from `Push Workflow`. | `Push Workflow` | The operator can see which builder to use for edits. |
| `Provider` | Table column | Read-only. Shows SendGrid, Braze, Wynn PushNotifier, simulator, or workflow channel fallback. | `Wynn PushNotifier` | The delivery target is visible from the list. |
| `Status` | Table column | Read-only. Shows draft, scheduled, published, paused, sent, failed, cancelled, or archived state. | `archived` | The lifecycle state is visible from the list. |
| `Last Run` | Table column | Read-only. Uses workflow `runtime_summary.last_run_at` or campaign send timestamps. | `2026-03-10 10:15` | Operators can see the last live execution time. |
| `Next Run` | Table column | Read-only. Uses workflow `runtime_summary.next_run_at` or campaign `schedule_at`. | `2026-03-11 10:15` | Operators can see the next due time. |
| `Last Results` | Table column | Read-only. Shows compact counts from the latest execution or send. | `ok 42 · fail 3` | The most recent outcome is summarized without opening raw JSON. |
| `Total Results` | Table column | Read-only. Shows aggregate run or send counts. | `runs 7 · success 255` | Operators can gauge cumulative performance quickly. |
| `Selected Item` | Detail panel | Shows the current campaign or workflow JSON plus resource-specific actions. | None | Detail JSON and action buttons appear for the selected item. |
| `Schedule Email Campaign` | Date/time picker | Visible only when an email campaign is selected. Sets the campaign schedule directly from Workflow Studio. | `2026-04-20 09:30` | The selected email campaign moves to `scheduled`. |
| Email item `View` | Row or detail button | Loads the selected email campaign into the detail panel. | None | Campaign detail JSON appears. |
| Email item `Edit` | Row or detail button | Opens the campaign in `Email Campaigns`. | None | The email builder loads the selected campaign. |
| Email item `Schedule` | Row or detail button | Uses the detail-panel schedule picker to create or update `schedule_at`. | None | The email campaign becomes scheduled. |
| Email item `Send Now` | Row or detail button | Executes the selected draft or scheduled campaign immediately. | None | Campaign runs and records result counts. |
| Email item `Cancel` | Row or detail button | Cancels a scheduled campaign. | None | Campaign status becomes `cancelled`. |
| Email item `Delete` | Row or detail button | Deletes a draft campaign. | None | Draft campaign is removed. |
| Push item `View` | Row or detail button | Loads the selected workflow into the detail panel. | None | Workflow detail JSON appears. |
| Push item `Edit` | Row or detail button | Opens the workflow in `Push Notifications`. | None | The push builder loads the selected workflow. |
| Push item `Publish` | Row or detail button | Publishes a draft workflow after preflight checks. | None | Workflow status becomes `published`. |
| Push item `Pause` | Row or detail button | Pauses a published workflow. | None | Workflow status becomes `paused`. |
| Push item `Resume` | Row or detail button | Resumes a paused workflow. | None | Workflow status becomes `published`. |
| Push item `Test Run` | Row or detail button | Runs the workflow in sandbox mode. | None | Test-run output appears in the runtime output panel and `last_test_run_at` updates after refresh. |
| Push item `Archive` | Row or detail button | Archives a non-draft workflow so it remains visible but cannot run again. | None | Workflow status becomes `archived`. |
| Push item `Delete` | Row or detail button | Deletes a draft workflow only. | None | Draft workflow is removed. |

Workflow Studio behavior:
- Archived workflows remain visible in the `Archived` filter and in historical detail views, but they are excluded from due-run execution, resume, publish, and test-run actions.
- Push workflow totals come from `runtime_summary.totals` and count live runs separately from sandbox test runs.
- Email campaign totals reuse the campaign `result_summary`.

### 5.4 Runtime Controls

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

### 5.5 Executions And Deliveries

| Control | Type | How to use it | Sample input | Expected result |
| --- | --- | --- | --- | --- |
| `Selected workflow` badge | Read-only badge | Shows which push workflow the execution and delivery panels are currently inspecting. | `daily_churn_rescue_push` | Operators can confirm the execution scope. |
| `Executions & Policy Counters` | Detail panel | Read-only list of workflow execution records and policy aggregates for the selected workflow. | None | Execution history and policy JSON update. |
| `Load Diagnostics` | Button | Loads delivery diagnostics for the selected workflow. | None | Diagnostics JSON appears. |
| `Deliveries` | Detail panel | Read-only list of workflow deliveries for the selected workflow. | None | Delivery rows and diagnostics update. |

Delivery detail behavior:
- Push workflow deliveries created through Wynn PushNotifier keep the selected `provider_connection_id`.
- The delivery record stores `provider_campaign_id` and `provider_accepted` when Wynn accepts the request.
- Delivery detail JSON also records the normalized `provider_request` and `provider_response` payloads for operator inspection.
- In v1, Wynn delivery success means the push campaign request was accepted and created in PushNotifier. It does not mean the device-level notification has already been delivered.

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

### 7.1 Global AI Assistant

The bottom-right `Ask AI` bubble is now the primary AI surface. It stays available while you move across `Data Core`, `Audience Engine`, `Action Orchestrator`, `Experiment Hub`, and the manual `Insight Copilot` page.

The assistant can:
- answer grounded product-help questions for the page you are currently viewing
- give sample SQL, JSON payloads, and example prompts
- summarize the current dashboard
- set up low-risk draft cohorts, experiment configs, connectors, and provider connections
- reuse or start prediction jobs, draft SQL from prediction context, and turn the result into a saved query plus draft cohort
- select an existing SendGrid template or Braze API campaign and create a draft email campaign
- create a draft workflow linked to the cohort and optional email campaign
- stop high-risk actions at explicit confirmation

The drawer now behaves like a normal chat room:
- one transcript from top to bottom
- one message box at the bottom
- the first message box stays disabled with `Getting Agents Ready...` only until the first session-create call completes
- reopening the drawer keeps the existing session usable while the transcript refreshes in the background
- the user message appears immediately after send
- the assistant shows a thinking animation until the answer or next required action is ready
- inline clarification, confirmation, and artifact cards only when they are relevant
- no persistent side panels for agent workflow state

The `Insight Copilot` page now acts as the advanced/manual fallback for direct `Query`, `Explain`, `Recommend`, `Report`, and `Evidence & Logs` usage.

| Control | Type | How to use it | Sample input | Expected result |
| --- | --- | --- | --- | --- |
| `Ask AI` | Floating launcher | Opens the global assistant drawer from any app page after workspace resolution. | None | The assistant drawer opens without leaving the current page. |
| `Agent Model` | Select | Choose which backend-managed model profile the agent should use for the next session. Configure these profiles in `Data Core -> Connectors -> AI Agents & Models`. Gemini stays the default when a default Gemini profile or system Gemini configuration exists. | `LM Studio Local` | The next session runs with the selected provider and model metadata. |
| `Model status` | Status line | Read-only. Shows the effective provider, model name, and whether the choice came from a saved profile, system default, or deterministic fallback. | `Gemini - gemini-2.5-flash` | Confirms which model the agent is using. |
| `Session status` | Status line | Read-only. Shows the current session id, intent, status, and async continuation state when a prediction-backed flow is waiting. | `Session cpa_... - waiting_for_prediction` | Confirms the active agent session and whether a follow-up `Continue` is possible. |
| `New Session` | Button | Starts a fresh operator-agent session. | None | Prior conversation is left behind and a new empty session is created. |
| `Message` | Text area | Wait until the placeholder changes from `Getting Agents Ready...` to the normal prompt, then ask how to use the current page, request a sample payload, or tell the agent to perform a supported setup task. Press `Enter` to send or `Shift+Enter` for a new line. | `How do I create an Amplitude connector here? Give me a sample payload.` | The assistant blocks only the initial first message while the first session is created, then returns grounded guidance or executes the supported setup flow. |
| `Send` | Button | Sends the current message to the assistant after the drawer is ready. The button stays disabled only during initial session bootstrap or when workspace access is blocked. | None | The transcript updates with the latest answer or task state. |
| Inline thinking row | Temporary status row | Appears after you send a message and disappears when the assistant responds. | None | Shows that the agent is working before the final answer or next action appears. |
| Inline clarification card | Conditional form | Fill only the missing inputs requested by the agent directly in the transcript. | `connection_scope: connector` | The agent continues the task without restarting the session. |
| Inline confirmation card | Conditional action card | Review high-risk actions that were prepared but not executed automatically. | `Start experiment` | A confirm button appears inline instead of auto-running the action. |
| `Confirm Action` | Button | Explicitly approves a risky prepared action from the inline confirmation card. | None | The held action executes and the conversation updates. |
| Inline artifact card | Conditional resource card | Opens the created or updated prediction job, cohort, experiment, connector, provider connection, saved query, email campaign, or workflow in the right module. | `cohort_...` | The console navigates to the linked resource view. |
| `Continue` on artifact card | Conditional button | Appears when the agent is waiting for a background prediction to complete before it can finish the remaining setup steps. | None | Sends the stored resume message and continues the pending prediction-backed flow after completion. |
| `Open Assistant` on `Insight Copilot` | Button | Opens the same global assistant from the manual Copilot page. | None | You keep the same session and return to the same drawer experience. |

#### Supported v1 agent tasks

- `Summarize the dashboard`
- `Set up a cohort`
- `Set up an A/B test`
- `Set up a connection`
- `Run prediction for Source X`
- `Draft SQL for the high-risk audience`
- `Set up a draft email campaign with SendGrid or Braze`
- `Set up a draft workflow`
- `Set up the whole prediction -> cohort -> email campaign -> workflow flow`
- grounded product help such as `How do I use this page?`, `Where do I do X?`, `Give me a sample payload`, or `Why is this failing?`

The v1 agent executes only low-risk reads and draft/setup actions automatically. It does not auto-run destructive deletes. Cohort activation, experiment start/stop, experiment decision logging, and similar risky follow-up actions are held for explicit confirmation.

Prediction-backed flows are asynchronous. If the agent starts a fresh prediction job, the drawer stays on the same session, exposes the prediction job as an artifact, and waits for you to click `Continue` after the prediction completes.

#### Sample operator-flow prompt
```text
Run churn prediction for high-risk players from source Amplitude 1, create a cohort, use SendGrid template tmpl_winback, and set up a draft workflow.
provider_connection_id: pc_sendgrid123
campaign_name: april_winback_campaign
workflow_name: april_winback_workflow
cohort_name: april_high_risk_cohort
saved_query_name: april_high_risk_query
```

#### Sample agent response output
```json
{
  "assistant_message": "Started the prediction job. Continue when the prediction completes to build the saved query, cohort, campaign, and workflow drafts.",
  "session_state": {
    "session_id": "cpa_20260401_1200",
    "status": "waiting_for_prediction",
    "current_intent": "setup_operator_flow",
    "model_profile_id": "amp_123",
    "effective_provider": "gemini",
    "effective_model_name": "gemini-2.5-flash",
    "async_status": "waiting_for_prediction"
  },
  "completed_actions": [
    {
      "action_type": "setup_operator_flow",
      "status": "running",
      "is_async": true
    }
  ],
  "artifacts": [
    {
      "resource_type": "prediction_job",
      "resource_id": "pred_123",
      "resume_ready": false,
      "resume_message": "Continue with the prediction results."
    }
  ]
}
```

### 7.2 Query

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

### 7.3 Explain

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

### 7.4 Recommend

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

### 7.5 Report

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

### 7.6 Query Logs, Anomalies, And Reports

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
| `Projects` | Tab button | Opens the live project-management tab for the active organization. | None | Project rows, default-project marker, create-project action, and delete flow become visible. |
| `Teams` | Tab button | Opens the live team-management tab for the active organization. | None | Organization member rows, joined dates, invite controls, role-management controls, member removal, and owner-transfer confirmation become visible. |
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
| `Switcher` | Button | Opens the full-screen workspace selector overlay from inside Settings. | None | Lets you switch organization or project. |
| `New Project` | Button | Opens the create-project overlay from inside Settings. | None | Creates a new project in the current organization after success. |
| Current workspace card | Read-only summary | Shows the active organization and project. | `North Star Games / Live Ops` | Confirms the live context before using shell shortcuts. |
| Session state card | Read-only summary | Shows the current login state and the org role that applies across all projects in the active organization. | `Google alice@example.com @ northstar / liveops (owner)` | Confirms the current authenticated session. |
| Auth session card | Read-only summary | Shows the current login or local/demo state from inside Settings. | `Google alice@example.com @ northstar / liveops (admin)` | Confirms the current authenticated session before you switch workspaces or log out. |
| `Continue with Google` | Button | Opens Google's browser popup sign-in flow from inside Settings. | None | Google opens in a browser popup and returns with a Google ID token-backed bearer session. |
| `Logout` | Button | Clears the current bearer token and ends the authenticated session. | None | Session returns to the organization URL gate so the next sign-in starts from org selection. |
| `API Key` | Password box | Optional legacy/demo API key entry. This stays hidden when Google login is configured or an OIDC bearer session is active. | `local-demo-key` | Local/demo requests reuse the stored API key in the browser. |
| Application startup status | Read-only status line | Shows the latest startup or health result from inside Settings. | `Application start completed (mock)` | Confirms whether the backend is reachable from the console. |

### 8.4 Projects

The `Projects` tab manages the active organization's projects. Project access is org-wide, so every org member can enter every active project. The default project is the oldest active project in that organization.

| Control | Type | How to use it | Sample input | Expected result |
| --- | --- | --- | --- | --- |
| Project list | Read-only table | Shows every active project in the current organization. | `Live Ops`, `Sandbox` | Confirms which projects exist before switching or deleting. |
| Default badge | Read-only badge | Marks the oldest active project in the org. | `Default` | Shows which project is auto-preselected when multiple projects exist. |
| `Create Project` | Button | Opens the create-project form or overlay. Available only to `owner` and `admin` users. | None | A new project can be created inside the current org. |
| `Project Name` | Text box | Enter the new project's display name. | `Growth Sandbox` | The console generates the internal project id automatically. |
| `Delete Project` | Row button | Starts the permanent delete flow. Available only to `owner` and `admin` users. | None | Opens the delete confirmation modal for that project. |
| Delete confirmation text box | Text box | Type the exact confirmation keyword. | `delete` | Enables the final delete action. |
| Final delete confirmation | Modal warning | Read-only warning in the delete modal. | `This permanently deletes the project and its data. This cannot be recovered.` | Explains that deletion removes project-scoped data permanently. |
| `Delete Project` | Button | Permanently deletes the project after the confirmation text matches. | None | Project is removed. If it was the current project, the console moves to the next default project or to create-first-project state if no projects remain. |

Project deletion is hard delete only. It removes the selected project's connectors, imports, data layers, workflows, cohorts, predictions, AI agent state, tools, experiments, exports, and project-scoped audit history. Organization metadata and the shared team member list remain intact.

#### Sample project-delete confirmation input
```json
{
  "confirmation": "delete"
}
```

#### Sample project-delete result
```json
{
  "organization_id": "northstar",
  "deleted_project_id": "sandbox",
  "remaining_projects": [
    {
      "organization_id": "northstar",
      "project_id": "liveops",
      "name": "Live Ops",
      "description": "Primary production project",
      "status": "active",
      "role": "admin",
      "is_default": true
    }
  ],
  "next_default_project_id": "liveops"
}
```

### 8.5 Teams

The `Teams` tab manages organization-level access. Team membership is shared across every project in the active organization.

| Control | Type | How to use it | Sample input | Expected result |
| --- | --- | --- | --- | --- |
| Member list | Read-only table | Shows every current organization member plus any pending org invite rows that have not activated yet. | `alice@example.com`, `member` | Confirms who already has access and who is still pending. |
| Joined date text | Read-only row text | Read the `Joined YYYY-MM-DD` or `Invited YYYY-MM-DD` label next to each member's name. | `Joined 2026-04-03` | Confirms when the member or pending invite entered the organization roster. |
| `Add Team Member` | Button | Uses the email field and adds that Google account to the organization as a `member`. Available only to `owner` and `admin` users. | None | Lets the admin pre-authorize a Google email for org access. |
| `Google Email` | Text box | Enter the Google account email to invite into the org. | `teammate@example.com` | Creates an org-level invite or pre-authorization record. |
| Default role note | Read-only inline note | Read-only reminder below the email field. | `New team members join as Member by default.` | Confirms that owners and admins promote later from the roster instead of assigning `admin` during add-member creation. |
| `Generate Invite Link` | Button | Uses the email field to create or refresh an optional organization invite link in the shared invite section. Available only to `owner` and `admin` users. | None | The latest invite link is refreshed in the shared invite card, not on individual member rows. |
| `Copy Invite Link` | Top-level button | Copies the most recently generated invite link from the invite-link field. | None | The current invite link is placed on the clipboard. |
| Role selector | Row select | Promote or demote a current member between `admin` and `member`. Owners can also choose `owner` on another non-owner row to start an ownership transfer. Administrators can demote themselves to `member`, but they cannot change any owner row. | `admin` | Stages a role change for that member. |
| `Save` | Row button | Writes the staged role change for that row. Available only when the row has an unsaved change. | None | The role update is sent and the status line shows `Changes are saved.` on success. |
| `Remove Member` | Row button | Opens the removal confirmation flow for a non-owner member. Available only to `owner` and `admin` users. | None | Attempts to remove that member from the organization. |
| Owner badge | Read-only badge | Marks the organization creator. | `Owner` | Indicates the only role that cannot be reassigned through the normal team-management flow. |

Role contract:
- `owner`: the creator of the org; also has admin privileges; only the owner can transfer ownership by changing another row to `owner` and confirming the popup
- `admin`: can add members, create projects, delete projects, remove non-owner members, promote or demote between `admin` and `member`, and can demote themselves to `member`
- `member`: can enter the org and use all its projects, but cannot manage team or project lifecycle actions

Current UI limitations:
- new team members are always created as `member`; promote them later from the roster
- administrators cannot transfer ownership through the UI
- after an admin demotes themselves to `member`, the page refreshes their org role and removes their management controls immediately

#### Sample add-member input
```json
{
  "email": "teammate@example.com",
  "role": "member"
}
```

#### Sample add-member result
```json
{
  "member": null,
  "invite": {
    "organization_id": "northstar",
    "email": "teammate@example.com",
    "role": "member",
    "status": "pending",
    "invite_url": "/?invite_code=oinv_123&organization_id=northstar"
  }
}
```

When the invited Google account signs in:
- if the verified email matches the org invite, the user gains org membership automatically
- the user then selects or creates a project inside that organization
- if the email does not match, the invite does not grant access

### 8.6 Billing

The `Billing` tab is still a placeholder layout only. Its visible rows are present for design and navigation structure, but they do not execute backend actions yet.

### 8.7 Notifications

The `Notifications` tab is currently a placeholder layout only. Theme mode now lives in the top-right header selector instead of inside Settings.

| Control | Type | How to use it | Sample input | Expected result |
| --- | --- | --- | --- | --- |
| Notification rows | Placeholder rows | Visible for layout only. | None | No backend action occurs yet. |

---

## 9) Representative End-To-End Example

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
5. In `Action Orchestrator -> Push Notifications`, create a push workflow:
   - `Name`: `daily_churn_rescue_push`
   - `Cohort`: select the new cohort
   - `Experiment ID`: `churn_rescue_v1`
   - `Trigger Type`: `daily_schedule`
   - `Hour`: `10`
   - `Minute`: `15`
   - `Campaign Name`: `winback_push`
   - `Title`: `Come back`
   - `Body`: `Rewards are waiting for you.`
   - Click `Create Push Workflow`.
6. In `Action Orchestrator -> Workflow Studio`, find the new workflow and click `Publish`.
7. In `Experiment Hub`, load `churn_rescue_v1`, review the summary, and click `Start` if the experiment is still inactive.
8. After executions and outcomes accumulate, click `Record Decision`.
9. Open the global `Ask AI` bubble and run:
   - `Summarize the dashboard.`
   - or `How do I create an Amplitude connector here? Give me a sample payload.`
10. If you need the raw analytical tools, open `Insight Copilot` and run:
   - `Question`: `how many high risk users do we have in 7d?`
   - Click `Run Query`.
11. Review the latest Copilot response and, if needed, generate a recommendation or report.

---

## 10) Current Known UI Caveats
- `Data Core -> Governance -> Save Limits` is currently a placeholder and is not wired in the frontend JavaScript.
- `Webhook URL` and `Webhook Token` on the churn export panel are mainly relevant when the provider is `webhook`.
- Some lists and selectors require prior data. Examples:
  - workflow cohort selector needs cohorts loaded first
  - import detail selector needs imports loaded first
  - export diagnostics selector needs export jobs present first
  - query log loader needs a real `query_id`

---

## 11) Documentation Maintenance Rule
When any user-facing function, button label, form field, workflow, or sample payload changes:
1. Update this file.
2. Update `README.md`.
3. Update any module PRD or runbook if the behavior change is cross-cutting or production-relevant.
4. Call out placeholder or not-yet-wired controls explicitly instead of documenting them as working features.
