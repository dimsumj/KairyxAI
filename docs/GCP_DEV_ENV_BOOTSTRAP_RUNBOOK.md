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

What each one does:
- `deploy/gcp/dev.env.example`
  - canonical example env file for the dev environment
- `deploy/gcp/bootstrap_dev_foundation.sh`
  - creates or verifies the dev project foundation resources
- `deploy/gcp/deploy_cloud_run.sh`
  - builds the image and deploys the five Cloud Run services
- `deploy/gcp/configure_dev_eventing.sh`
  - wires Pub/Sub push subscriptions and the Cloud Scheduler HTTP job after the services exist

## 4) Prerequisites

### 4.1 Operator prerequisites
- temporary IAM admin or equivalent bootstrap access in the dev GCP project
- `gcloud`
- `bq`
- `docker`
- `python3`

### 4.2 Project prerequisites
- billing enabled
- one dedicated dev GCP project
- a Google OAuth client for the dev console origin
- your Google Workspace domain

## 5) Bootstrap Sequence

### 5.1 Prepare the private env file
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

### 5.2 Bootstrap the dev foundation
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

### 5.3 Deploy the runtime
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

### 5.4 Wire Pub/Sub and Scheduler
After the services exist, run:

```bash
bash deploy/gcp/configure_dev_eventing.sh deploy/gcp/dev.env
```

This script:
- grants `roles/run.invoker` on the worker services to the dedicated Pub/Sub and Scheduler caller identities
- creates or updates the import, prediction, and export push subscriptions
- creates or updates the Scheduler HTTP job for `scheduler-worker`

The script reads `WORKER_SHARED_TOKEN` from Secret Manager and uses it in the callback URLs. It does not print the token or the full URLs, but anyone who can inspect the resulting Pub/Sub subscription or Scheduler job configuration can still read the token from those stored URIs.

## 6) Required Variables In `deploy/gcp/dev.env`

### 6.1 Foundation and deploy
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
- `GOOGLE_OIDC_HOSTED_DOMAIN`
- `GCS_BUCKET_NAME`
- `IMPORT_COMMAND_TOPIC`
- `PREDICTION_COMMAND_TOPIC`
- `EXPORT_COMMAND_TOPIC`
- `PUBSUB_TOPIC_NAME`

### 6.2 Optional overrides
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

## 7) Validation Checklist

### 7.1 Infrastructure checks
- required APIs enabled
- Cloud SQL instance exists
- bucket exists
- BigQuery dataset exists
- Pub/Sub topics exist
- expected Secret Manager secrets exist
- runtime and invoker service accounts exist

### 7.2 Deployment checks
- all five Cloud Run services are deployed
- worker services stay authenticated-only
- `operator-api` is reachable

### 7.3 App checks
- `GET /health/live` succeeds
- Google login appears on `/`
- Workspace-domain restriction works
- the user can sign in and reach `/{organization_id}`

### 7.4 Functional smoke
- enter or create an organization
- enter or create a project
- create a connector
- start an import
- confirm project-scoped pages load without membership errors

### 7.5 Operational checks
- logs appear in Cloud Logging
- Pub/Sub push hits the worker services successfully
- Cloud Scheduler can invoke `scheduler-worker`
- Secret Manager access works only for the intended service accounts

## 8) Security And Access Cleanup
After the first successful bootstrap and deploy:
- remove broad human IAM admin from normal operations
- keep only the deployment principal and least-privilege runtime service accounts
- do not commit `deploy/gcp/dev.env`
- do not print or paste the actual database URL or worker token into docs, tickets, or git history
- restrict read access to Pub/Sub subscription and Cloud Scheduler job configuration because those resources store the worker token in the callback URI

## 9) Relationship To The Production Runbook
Use this dev runbook for the first internal dev environment only.

Use `docs/GCP_PRODUCTION_DEPLOYMENT_RUNBOOK.md` for:
- production sizing and quotas
- Cloud Run production hardening
- production IAM posture
- multi-environment rollout planning
