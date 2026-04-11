# KairyxAI GCP Dev Environment Bootstrap Runbook

## 1) Purpose
This runbook explains how to bootstrap one internal-only GCP dev environment for KairyxAI using the repository's Cloud Run deployment path.

Use this runbook when you want:
- one dedicated dev GCP project
- Google Workspace login on the gateway
- the same five-service runtime topology used by the production-shaped deployment path
- repo-supported bootstrap scripts instead of a manual click-by-click setup

This runbook does not provision `qa` or `prod`. It is intentionally scoped to one dev environment.

## 2) Recommended Dev Shape

### 2.1 Deployment model
- One dedicated dev GCP project, for example `kairyx-dev`
- Cloud Run services:
  - `operator-api`
  - `import-worker`
  - `prediction-worker`
  - `export-worker`
  - `scheduler-worker`
- Cloud SQL PostgreSQL for the control plane
- Pub/Sub for import, prediction, export, and raw-shard topics
- Cloud Scheduler for `scheduler-worker`
- BigQuery and Cloud Storage for production-shaped data services
- Secret Manager for the database URL and worker shared token

### 2.2 Runtime settings
The dev environment should stay production-shaped:

| Variable | Value |
| --- | --- |
| `APP_ENV` | `prod` |
| `DATA_BACKEND_MODE` | `gcp` |
| `WAREHOUSE_BACKEND` | `bigquery` |
| `OBJECT_STORAGE_BACKEND` | `gcs` |
| `MESSAGE_BACKEND` | `pubsub` |
| `SECRET_BACKEND` | `gcp_secret_manager` |
| `LEGACY_HEADER_AUTH_ENABLED` | `false` |
| `KAIRYX_PLATFORM_SURFACE` | unset |

### 2.3 Auth shape
- Use Google login
- Keep the base URL `/` as the gateway
- Keep the active app on `/{organization_id}`
- Restrict login to your Google Workspace domain through `GOOGLE_OIDC_HOSTED_DOMAIN`

## 3) Repo Entry Points
Use these checked-in assets:

- `deploy/gcp/dev.env.example`
- `deploy/gcp/bootstrap_dev_foundation.sh`
- `deploy/gcp/deploy_cloud_run.sh`
- `deploy/gcp/configure_dev_eventing.sh`
- `.github/workflows/`

What each one does:
- `deploy/gcp/dev.env.example`
  - canonical example env file for the dev environment
- `deploy/gcp/bootstrap_dev_foundation.sh`
  - creates or verifies the dev project foundation resources
- `deploy/gcp/deploy_cloud_run.sh`
  - builds the image and deploys the five Cloud Run services
- `deploy/gcp/configure_dev_eventing.sh`
  - wires Pub/Sub push subscriptions and the Cloud Scheduler HTTP job after the services exist
- `.github/workflows/`
  - runs validation and then auto-deploys the shared dev environment from pushes to `main`

## 4) GitHub Actions Dev Auto-Deploy

### 4.1 Deployment behavior
The shared dev environment deploys from GitHub Actions:

- pushes to `main` run validation first
- after validation succeeds, GitHub Actions deploys the current `main` revision to the shared GCP dev environment
- the GitHub environment name for this deployment is `dev`

The CI deployment path does not source `deploy/gcp/dev.env`. That file stays local and manual for bootstrap, operator-driven redeploys, and eventing setup from a developer machine.

### 4.2 GitHub `dev` environment contract
The GitHub environment named `dev` should provide the values that the deploy job passes to `deploy/gcp/deploy_cloud_run.sh`.

Required environment variables:

- `GCP_PROJECT_ID`
- `GCP_REGION`
- `GCP_ARTIFACT_REGISTRY_REPOSITORY`
- `GCP_IMAGE_NAME`
- `GCP_DEPLOYMENT_TIER=dev`
- `GCP_SERVICE_PREFIX=dev`
- `GCP_CLOUD_SQL_CONNECTION_NAME`
- `CONTROL_PLANE_DATABASE_URL_SECRET`
- `WORKER_SHARED_TOKEN_SECRET`
- `CORS_ALLOWED_ORIGINS`
- `OIDC_ISSUER`
- `OIDC_AUDIENCE`
- `OIDC_JWKS_URL`
- `OIDC_CLIENT_ID`
- `OIDC_AUTHORIZE_URL`
- `OIDC_TOKEN_URL`
- `GOOGLE_OIDC_CLIENT_ID`
- `GOOGLE_OIDC_HOSTED_DOMAIN`
- `GCS_BUCKET_NAME`
- `GCP_BIGQUERY_DATASET_ID`
- `IMPORT_COMMAND_TOPIC`
- `PREDICTION_COMMAND_TOPIC`
- `EXPORT_COMMAND_TOPIC`
- `PUBSUB_TOPIC_NAME`
- `BOOTSTRAP_TENANT_ID`
- `BOOTSTRAP_TENANT_NAME`
- `BOOTSTRAP_PROJECT_ID`
- `BOOTSTRAP_PROJECT_NAME`

Required network variables:

- either `GCP_RUN_NETWORK` and `GCP_RUN_SUBNET`
- or `GCP_VPC_CONNECTOR`

Optional environment variables:

- `GCP_RELEASE_TAG` if the workflow does not generate a release tag itself
- `GCP_VPC_EGRESS`
- `GCP_SECRET_PROJECT_ID`
- `OIDC_LOGOUT_URL`
- `API_ACCESS_KEY_SECRET`
- `OIDC_JWT_SIGNING_SECRET_SECRET`
- the service-account override variables supported by `deploy/gcp/dev.env.example`

Required environment secrets:

- `GCP_WORKLOAD_IDENTITY_PROVIDER`
- `GCP_DEPLOY_SERVICE_ACCOUNT`

`deploy/gcp/dev.env` remains a local operator file and is not read by CI.

### 4.3 One-time Workload Identity Federation setup
Set up GitHub Actions authentication to GCP with `google-github-actions/auth`:

1. Create one deploy service account in the dev GCP project.
2. Create one Workload Identity Pool and one OIDC provider that trust this GitHub repository.
3. Restrict the provider to this repository and branch `refs/heads/main`.
4. Grant the GitHub principal `roles/iam.workloadIdentityUser` on the deploy service account.
5. Grant the deploy service account the runtime deploy roles it actually needs:
   - `roles/run.admin`
   - `roles/artifactregistry.writer`
   - `roles/pubsub.admin`
   - `roles/cloudscheduler.admin`
   - `roles/secretmanager.secretAccessor` on `WORKER_SHARED_TOKEN_SECRET`
6. Grant `roles/iam.serviceAccountUser` on the runtime service accounts used by:
   - `operator-api`
   - `import-worker`
   - `prediction-worker`
   - `export-worker`
   - `scheduler-worker`
7. Grant service-account policy edit rights only where the eventing step needs them:
   - enough permission to manage IAM bindings on `pubsub-push-invoker`
   - enough permission to manage IAM bindings on `scheduler-invoker`
8. Keep the deploy service account scoped to the dev project only.
9. Store the provider resource name in the GitHub `dev` environment as `GCP_WORKLOAD_IDENTITY_PROVIDER`.
10. Store the deploy service account email in the GitHub `dev` environment as `GCP_DEPLOY_SERVICE_ACCOUNT`.

The deploy job should authenticate with `google-github-actions/auth` using the GitHub `dev` environment values instead of a JSON key.

## 5) Prerequisites

### 5.1 Operator prerequisites
- temporary IAM admin or equivalent bootstrap access in the dev GCP project
- `gcloud`
- `bq`
- `docker`
- `python3`

### 5.2 Project prerequisites
- billing enabled
- one dedicated dev GCP project
- a Google OAuth client for the dev console origin
- your Google Workspace domain

## 6) Bootstrap Sequence

### 6.1 Prepare the private env file
Copy the checked-in template into a private env file that is not committed:

```bash
cp deploy/gcp/dev.env.example deploy/gcp/dev.env
```

Fill the real values for:
- `GCP_PROJECT_ID`
- `GCP_REGION`
- `GCP_ARTIFACT_REGISTRY_REPOSITORY`
- `GCP_IMAGE_NAME`
- `GCP_RELEASE_TAG`
- `CORS_ALLOWED_ORIGINS`
- `OIDC_AUDIENCE`
- `OIDC_CLIENT_ID`
- `GOOGLE_OIDC_CLIENT_ID`
- `GOOGLE_OIDC_HOSTED_DOMAIN`
- any naming overrides required by your organization

Do not store real secret values in git. The scripts generate the expected Secret Manager secrets when they are missing.

### 6.2 Bootstrap the dev foundation
Run:

```bash
bash deploy/gcp/bootstrap_dev_foundation.sh deploy/gcp/dev.env
```

This script enables required APIs and creates or verifies:
- Artifact Registry repository
- VPC and subnet
- Private Service Access range and connection
- Cloud SQL PostgreSQL instance, database, and app user
- Secret Manager secrets:
  - `CONTROL_PLANE_DATABASE_URL_SECRET`
  - `WORKER_SHARED_TOKEN_SECRET`
- Cloud Storage bucket
- BigQuery datasets:
  - the base dataset from `GCP_BIGQUERY_DATASET_ID`
  - the bootstrap-scoped dataset for `BOOTSTRAP_TENANT_ID` and `BOOTSTRAP_PROJECT_ID`
- the bootstrap `pipeline_dead_letters` table inside the bootstrap-scoped dataset
- Pub/Sub topics
- runtime and invoker service accounts
- baseline IAM bindings

Important constraints:
- the script is dev-only and expects `GCP_DEPLOYMENT_TIER=dev` when that variable is set
- service-account overrides must stay in the same GCP project
- existing secrets are reused; the script does not rotate a secret just because it already exists
- the bootstrap-scoped BigQuery dataset follows the same normalization as the runtime, for example `kairyx_platform_default_default`

### 6.3 Deploy the runtime
Run:

```bash
bash deploy/gcp/deploy_cloud_run.sh deploy/gcp/dev.env
```

This deploys:
- `operator-api`
- `import-worker`
- `prediction-worker`
- `export-worker`
- `scheduler-worker`

### 6.4 Wire Pub/Sub and Scheduler
After the services exist, run:

```bash
bash deploy/gcp/configure_dev_eventing.sh deploy/gcp/dev.env
```

This script:
- grants `roles/run.invoker` on the worker services to the dedicated Pub/Sub and Scheduler caller identities
- creates or updates the import, prediction, and export push subscriptions
- creates or updates the Scheduler HTTP job for `scheduler-worker`

The script reads `WORKER_SHARED_TOKEN` from Secret Manager and uses it in the callback URLs. It does not print the token or the full URLs, but anyone who can inspect the resulting Pub/Sub subscription or Scheduler job configuration can still read the token from those stored URIs.

## 7) Required Variables In `deploy/gcp/dev.env`

### 7.1 Foundation and deploy
These values must be set for the bootstrap and deploy flow:

- `GCP_PROJECT_ID`
- `GCP_REGION`
- `GCP_ARTIFACT_REGISTRY_REPOSITORY`
- `GCP_IMAGE_NAME`
- `GCP_RELEASE_TAG`
- `GCP_RUN_NETWORK`
- `GCP_RUN_SUBNET`
- `GCP_SQL_INSTANCE`
- `GCP_SQL_DATABASE`
- `GCP_SQL_USER`
- `GCP_CLOUD_SQL_CONNECTION_NAME`
- `CONTROL_PLANE_DATABASE_URL_SECRET`
- `WORKER_SHARED_TOKEN_SECRET`
- `CORS_ALLOWED_ORIGINS`
- `OIDC_ISSUER`
- `OIDC_AUDIENCE`
- `OIDC_JWKS_URL`
- `OIDC_CLIENT_ID`
- `OIDC_AUTHORIZE_URL`
- `OIDC_TOKEN_URL`
- `GOOGLE_OIDC_CLIENT_ID`
- `GOOGLE_OIDC_HOSTED_DOMAIN`
- `GCS_BUCKET_NAME`
- `IMPORT_COMMAND_TOPIC`
- `PREDICTION_COMMAND_TOPIC`
- `EXPORT_COMMAND_TOPIC`
- `PUBSUB_TOPIC_NAME`

### 7.2 Optional overrides
These stay optional:
- `GCP_VPC_CONNECTOR`
- `GCP_VPC_EGRESS`
- `GCP_PRIVATE_SERVICE_RANGE_NAME`
- `GCP_PRIVATE_SERVICE_RANGE_PREFIX_LENGTH`
- `GCP_BIGQUERY_DATASET_ID`
- `GCP_STORAGE_CLASS`
- `IMPORT_COMMAND_SUBSCRIPTION`
- `PREDICTION_COMMAND_SUBSCRIPTION`
- `EXPORT_COMMAND_SUBSCRIPTION`
- `SCHEDULER_JOB_NAME`
- `GCP_SCHEDULER_CRON`
- `GCP_SCHEDULER_TIME_ZONE`
- `OPERATOR_API_SERVICE_ACCOUNT`
- `IMPORT_WORKER_SERVICE_ACCOUNT`
- `PREDICTION_WORKER_SERVICE_ACCOUNT`
- `EXPORT_WORKER_SERVICE_ACCOUNT`
- `SCHEDULER_WORKER_SERVICE_ACCOUNT`
- `PUBSUB_PUSH_INVOKER_SERVICE_ACCOUNT`
- `SCHEDULER_INVOKER_SERVICE_ACCOUNT`
- `GCP_RELEASE_TAG`
- `GCP_SECRET_PROJECT_ID`
- `OIDC_LOGOUT_URL`
- `API_ACCESS_KEY_SECRET`
- `OIDC_JWT_SIGNING_SECRET_SECRET`
- `GCP_EXTRA_ENV_FILE`

Important:

- `deploy/gcp/dev.env` is for local operator use only
- GitHub Actions deploys dev from the GitHub `dev` environment contract instead of sourcing `deploy/gcp/dev.env`

## 8) Validation Checklist

### 8.1 Infrastructure checks
- required APIs enabled
- Cloud SQL instance exists
- bucket exists
- BigQuery dataset exists
- Pub/Sub topics exist
- expected Secret Manager secrets exist
- runtime and invoker service accounts exist

### 8.2 Deployment checks
- all five Cloud Run services are deployed
- worker services stay authenticated-only
- `operator-api` is reachable
- pushes to `main` auto-deploy the dev environment after validation passes

### 8.3 App checks
- `GET /health/live` succeeds
- Google login appears on `/`
- Workspace-domain restriction works
- the user can sign in and reach `/{organization_id}`

### 8.4 Functional smoke
- enter or create an organization
- enter or create a project
- create a connector
- start an import
- confirm project-scoped pages load without membership errors

### 8.5 Operational checks
- logs appear in Cloud Logging
- Pub/Sub push hits the worker services successfully
- Cloud Scheduler can invoke `scheduler-worker`
- Secret Manager access works only for the intended service accounts

## 9) Security And Access Cleanup
After the first successful bootstrap and deploy:
- remove broad human IAM admin from normal operations
- keep only the deployment principal and least-privilege runtime service accounts
- do not commit `deploy/gcp/dev.env`
- do not print or paste the actual database URL or worker token into docs, tickets, or git history
- restrict read access to Pub/Sub subscription and Cloud Scheduler job configuration because those resources store the worker token in the callback URI

## 10) Relationship To The Production Runbook
Use this dev runbook for the first internal dev environment only.

Use `docs/GCP_PRODUCTION_DEPLOYMENT_RUNBOOK.md` for:
- production sizing and quotas
- Cloud Run production hardening
- production IAM posture
- multi-environment rollout planning
