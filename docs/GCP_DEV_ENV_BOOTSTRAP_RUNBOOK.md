# KairyxAI GCP Dev Environment Bootstrap Runbook

## 1) Purpose
This runbook is the canonical operator guide for bringing up the shared KairyxAI GCP dev environment and wiring GitHub Actions to deploy it automatically from `main`.

Use this runbook when you need to:
- create or verify the dedicated dev GCP project
- install the local Bash tooling required to run the repo-supported scripts
- bootstrap the GCP foundation resources
- deploy the five Cloud Run services manually the first time
- configure Pub/Sub push and Cloud Scheduler eventing
- set up GitHub Actions Workload Identity Federation and the `dev` environment variables

This runbook is intentionally scoped to the dev environment. Use `docs/GCP_PRODUCTION_DEPLOYMENT_RUNBOOK.md` for production sizing, hardening, and rollout policy.

## 2) Recommended Dev Shape

### 2.1 Deployment model
- One dedicated dev GCP project
- Cloud Run services:
  - `dev-operator-api`
  - `dev-import-worker`
  - `dev-prediction-worker`
  - `dev-export-worker`
  - `dev-scheduler-worker`
- Cloud SQL PostgreSQL for the control plane
- Pub/Sub for import, prediction, export, and raw-shard topics
- Cloud Scheduler for `dev-scheduler-worker`
- BigQuery and Cloud Storage for production-shaped data services
- Secret Manager for the database URL, control-plane secret encryption key, and worker shared token

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
| `GCP_DEPLOYMENT_TIER` | `dev` |
| `GCP_SERVICE_PREFIX` | `dev` |

### 2.3 Auth shape
- Use Google login
- Keep `/` as the gateway
- Keep the active app on `/{organization_id}`
- Restrict login to your Google Workspace domain through `GOOGLE_OIDC_HOSTED_DOMAIN`
- If you use the Cloud Run default hostname, the OAuth client must allow that exact `run.app` origin and the same origin with a trailing slash as the redirect URI

## 3) Repo Entry Points
The supported dev bootstrap and deploy surface is:

- `deploy/gcp/dev.env.example`
- `deploy/gcp/bootstrap_dev_foundation.sh`
- `deploy/gcp/deploy_cloud_run.sh`
- `deploy/gcp/configure_dev_eventing.sh`
- `deploy/gcp/render_ci_env.py`
- `.github/workflows/deploy-dev.yml`

What each one does:
- `deploy/gcp/dev.env.example`
  - canonical local template for the dev environment
- `deploy/gcp/bootstrap_dev_foundation.sh`
  - creates or verifies the dev GCP foundation resources
- `deploy/gcp/deploy_cloud_run.sh`
  - builds the Docker image, pushes it to Artifact Registry, and deploys the five Cloud Run services
- `deploy/gcp/configure_dev_eventing.sh`
  - configures Pub/Sub push subscriptions and the Cloud Scheduler job after the services exist
- `deploy/gcp/render_ci_env.py`
  - validates and renders the GitHub Actions `dev` environment contract into a temporary deploy env file
- `.github/workflows/deploy-dev.yml`
  - validates the repo on pushes to `main`, authenticates to GCP with WIF, deploys the shared dev environment, configures eventing, and runs the `operator-api` health smoke check

## 4) Local Tooling And Authentication

### 4.1 Required local tools
For the repo-supported Bash scripts, install:
- `gcloud`
- `bq`
- `docker`
- `python3`

If you want to reproduce the GitHub validation job locally, also install:
- `node`
- `npm`
- `pip`

### 4.2 Example Bash installs
Example on macOS with Homebrew:

```bash
brew install --cask google-cloud-sdk
brew install --cask docker
brew install python@3.14
brew install node
```

If you use another package manager or Linux distribution, install equivalent packages instead. The important part is that the commands below exist in your shell.

### 4.3 Verify the local toolchain
Run:

```bash
gcloud --version
bq version
docker --version
python3 --version
node --version
npm --version
```

### 4.4 Authenticate the workstation
Before running any local bootstrap or deploy script:

```bash
gcloud auth login
gcloud auth application-default login
gcloud config set project YOUR_DEV_PROJECT_ID
```

If the workstation is using a service account instead of a user account, activate it explicitly before running the scripts.

## 5) Sample Dev Naming And Values
Use these sample values while reading the commands in this runbook:

```bash
export GCP_PROJECT_ID="your-dev-project-id"
export GCP_PROJECT_NUMBER="123456789012"
export GCP_REGION="us-central1"
export GCP_ARTIFACT_REGISTRY_REPOSITORY="kairyx"
export GCP_IMAGE_NAME="kairyxai"
export GCP_RELEASE_TAG="dev-2026-04-10-r1"
export GCP_DEPLOYMENT_TIER="dev"
export GCP_SERVICE_PREFIX="dev"
export GCP_RUN_NETWORK="dev-vpc"
export GCP_RUN_SUBNET="dev-serverless"
export GCP_SQL_INSTANCE="kairyx-dev-db"
export GCP_SQL_DATABASE="kairyx"
export GCP_SQL_USER="kairyx_app"
export GCP_CLOUD_SQL_CONNECTION_NAME="${GCP_PROJECT_ID}:${GCP_REGION}:${GCP_SQL_INSTANCE}"
export GCP_BIGQUERY_DATASET_ID="kairyx_platform"
export CONTROL_PLANE_DATABASE_URL_SECRET="dev-control-plane-db-url"
export CONTROL_PLANE_SECRET_KEY_SECRET="dev-control-plane-secret-key"
export WORKER_SHARED_TOKEN_SECRET="dev-worker-shared-token"
export GCS_BUCKET_NAME="kairyx-dev-data"
export IMPORT_COMMAND_TOPIC="kairyx-dev-import-jobs"
export PREDICTION_COMMAND_TOPIC="kairyx-dev-prediction-jobs"
export EXPORT_COMMAND_TOPIC="kairyx-dev-export-jobs"
export PUBSUB_TOPIC_NAME="kairyx-dev-raw-shards"
export BOOTSTRAP_TENANT_ID="default"
export BOOTSTRAP_TENANT_NAME="Default Tenant"
export BOOTSTRAP_PROJECT_ID="default"
export BOOTSTRAP_PROJECT_NAME="Default Project"
export OIDC_CLIENT_ID="your-google-client-id.apps.googleusercontent.com"
export GOOGLE_OIDC_HOSTED_DOMAIN="example.com"
```

Replace them with your real dev values before running anything.

## 6) GCP Project Setup

### 6.1 Create or select the dev project
If the project does not exist yet:

```bash
gcloud projects create "${GCP_PROJECT_ID}" --name="KairyxAI Dev"
gcloud config set project "${GCP_PROJECT_ID}"
gcloud beta billing projects link "${GCP_PROJECT_ID}" \
  --billing-account="REPLACE_WITH_BILLING_ACCOUNT_ID"
```

If the project already exists:

```bash
gcloud config set project "${GCP_PROJECT_ID}"
export GCP_PROJECT_NUMBER="$(gcloud projects describe "${GCP_PROJECT_ID}" --format='value(projectNumber)')"
```

### 6.2 APIs that must be enabled
`deploy/gcp/bootstrap_dev_foundation.sh` enables these APIs automatically, but you should know the expected set for verification and troubleshooting:

- `run.googleapis.com`
- `artifactregistry.googleapis.com`
- `cloudbuild.googleapis.com`
- `sqladmin.googleapis.com`
- `secretmanager.googleapis.com`
- `pubsub.googleapis.com`
- `cloudscheduler.googleapis.com`
- `monitoring.googleapis.com`
- `logging.googleapis.com`
- `bigquery.googleapis.com`
- `storage.googleapis.com`
- `compute.googleapis.com`
- `servicenetworking.googleapis.com`

Manual equivalent:

```bash
gcloud services enable \
  run.googleapis.com \
  artifactregistry.googleapis.com \
  cloudbuild.googleapis.com \
  sqladmin.googleapis.com \
  secretmanager.googleapis.com \
  pubsub.googleapis.com \
  cloudscheduler.googleapis.com \
  monitoring.googleapis.com \
  logging.googleapis.com \
  bigquery.googleapis.com \
  storage.googleapis.com \
  compute.googleapis.com \
  servicenetworking.googleapis.com \
  --project "${GCP_PROJECT_ID}"
```

### 6.3 Google OAuth client configuration
Create or verify one Google OAuth client for the dev console:

- Client type: `Web application`
- Authorized JavaScript origin:
  - your current dev console origin, for example `https://dev-operator-api-xxxxxx-uc.a.run.app`
- Authorized redirect URI:
  - the same origin with a trailing slash, for example `https://dev-operator-api-xxxxxx-uc.a.run.app/`
- Audience mode:
  - prefer `Internal` if the project belongs to your Google Workspace organization
- Hosted-domain restriction:
  - set `GOOGLE_OIDC_HOSTED_DOMAIN` to your Workspace domain

The deployed app expects:
- `OIDC_ISSUER=https://accounts.google.com`
- `OIDC_JWKS_URL=https://www.googleapis.com/oauth2/v3/certs`
- `OIDC_AUTHORIZE_URL=https://accounts.google.com/o/oauth2/v2/auth`
- `OIDC_TOKEN_URL=https://oauth2.googleapis.com/token`
- `OIDC_AUDIENCE`, `OIDC_CLIENT_ID`, and `GOOGLE_OIDC_CLIENT_ID` equal to the OAuth client ID

## 7) Service Accounts And IAM

### 7.1 Service accounts used in dev
Runtime and invoker service accounts are created automatically by `deploy/gcp/bootstrap_dev_foundation.sh` unless you override them in `deploy/gcp/dev.env`:

- `operator-api@PROJECT_ID.iam.gserviceaccount.com`
- `import-worker@PROJECT_ID.iam.gserviceaccount.com`
- `prediction-worker@PROJECT_ID.iam.gserviceaccount.com`
- `export-worker@PROJECT_ID.iam.gserviceaccount.com`
- `scheduler-worker@PROJECT_ID.iam.gserviceaccount.com`
- `pubsub-push-invoker@PROJECT_ID.iam.gserviceaccount.com`
- `scheduler-invoker@PROJECT_ID.iam.gserviceaccount.com`

The GitHub CI deploy path uses one additional service account that you create manually:

- `kairyx-dev-deploy-service@PROJECT_ID.iam.gserviceaccount.com`

### 7.2 Temporary bootstrap operator permissions
The easiest safe bootstrap path is a temporary human or workstation identity with admin-level coverage for the dev project. One workable role set is:

- `roles/serviceusage.serviceUsageAdmin`
- `roles/artifactregistry.admin`
- `roles/compute.networkAdmin`
- `roles/cloudsql.admin`
- `roles/secretmanager.admin`
- `roles/storage.admin`
- `roles/bigquery.admin`
- `roles/pubsub.admin`
- `roles/cloudscheduler.admin`
- `roles/iam.serviceAccountAdmin`
- `roles/resourcemanager.projectIamAdmin`

This is not the long-term runtime posture. It is the bootstrap posture used to create the dev foundation and then step back to least privilege.

### 7.3 Runtime IAM applied by the foundation script
`deploy/gcp/bootstrap_dev_foundation.sh` grants these baseline runtime permissions:

| Service account | Project roles | Bucket roles |
| --- | --- | --- |
| `operator-api` | `roles/cloudsql.client`, `roles/secretmanager.secretAccessor`, `roles/pubsub.publisher`, `roles/bigquery.jobUser`, `roles/bigquery.dataEditor` | `roles/storage.objectAdmin` on `gs://GCS_BUCKET_NAME` |
| `import-worker` | `roles/cloudsql.client`, `roles/secretmanager.secretAccessor`, `roles/pubsub.publisher`, `roles/bigquery.jobUser`, `roles/bigquery.dataEditor` | `roles/storage.objectAdmin` on `gs://GCS_BUCKET_NAME` |
| `prediction-worker` | `roles/cloudsql.client`, `roles/secretmanager.secretAccessor`, `roles/bigquery.jobUser`, `roles/bigquery.dataEditor` | none |
| `export-worker` | `roles/cloudsql.client`, `roles/secretmanager.secretAccessor`, `roles/bigquery.jobUser`, `roles/bigquery.dataViewer` | `roles/storage.objectAdmin` on `gs://GCS_BUCKET_NAME` |
| `scheduler-worker` | `roles/cloudsql.client`, `roles/secretmanager.secretAccessor`, `roles/bigquery.jobUser`, `roles/bigquery.dataViewer` | none |

The foundation script also grants the Pub/Sub service agent `roles/iam.serviceAccountTokenCreator` on `pubsub-push-invoker@PROJECT_ID.iam.gserviceaccount.com`.

### 7.4 Create the GitHub deploy service account
Create the CI deploy service account once:

```bash
gcloud iam service-accounts create kairyx-dev-deploy-service \
  --project "${GCP_PROJECT_ID}" \
  --display-name "KairyxAI Dev GitHub Deploy"
```

### 7.5 CI deploy service account permissions
Grant the GitHub deploy service account the permissions required by `.github/workflows/deploy-dev.yml`, `deploy/gcp/deploy_cloud_run.sh`, and `deploy/gcp/configure_dev_eventing.sh`.

Project-level roles:

```bash
export DEPLOY_SA="kairyx-dev-deploy-service@${GCP_PROJECT_ID}.iam.gserviceaccount.com"

for role in \
  roles/run.admin \
  roles/artifactregistry.writer \
  roles/pubsub.admin \
  roles/cloudscheduler.admin
do
  gcloud projects add-iam-policy-binding "${GCP_PROJECT_ID}" \
    --member="serviceAccount:${DEPLOY_SA}" \
    --role="${role}"
done
```

Secret-level access for eventing:

```bash
gcloud secrets add-iam-policy-binding "${WORKER_SHARED_TOKEN_SECRET}" \
  --project "${GCP_PROJECT_ID}" \
  --member="serviceAccount:${DEPLOY_SA}" \
  --role="roles/secretmanager.secretAccessor"
```

Service-account impersonation for Cloud Run deploy and eventing:

```bash
for sa in \
  operator-api \
  import-worker \
  prediction-worker \
  export-worker \
  scheduler-worker \
  pubsub-push-invoker \
  scheduler-invoker
do
  gcloud iam service-accounts add-iam-policy-binding \
    "${sa}@${GCP_PROJECT_ID}.iam.gserviceaccount.com" \
    --project "${GCP_PROJECT_ID}" \
    --member="serviceAccount:${DEPLOY_SA}" \
    --role="roles/iam.serviceAccountUser"
done
```

Service-account policy edit rights for the Pub/Sub push invoker, because `configure_dev_eventing.sh` grants the Pub/Sub service agent `roles/iam.serviceAccountTokenCreator` on it:

```bash
gcloud iam service-accounts add-iam-policy-binding \
  "pubsub-push-invoker@${GCP_PROJECT_ID}.iam.gserviceaccount.com" \
  --project "${GCP_PROJECT_ID}" \
  --member="serviceAccount:${DEPLOY_SA}" \
  --role="roles/iam.serviceAccountAdmin"
```

## 8) Create The Local `deploy/gcp/dev.env`

### 8.1 Copy the template
Do not edit `deploy/gcp/dev.env.example` directly. Copy it to a private local file:

```bash
cp deploy/gcp/dev.env.example deploy/gcp/dev.env
```

`deploy/gcp/*.env` is already ignored by git.

### 8.2 Sample `deploy/gcp/dev.env`
Use this sample as a starting point:

```bash
GCP_DEPLOYMENT_TIER=dev
GCP_SERVICE_PREFIX=dev

GCP_PROJECT_ID=your-dev-project-id
GCP_REGION=us-central1
GCP_ARTIFACT_REGISTRY_REPOSITORY=kairyx
GCP_IMAGE_NAME=kairyxai
GCP_RELEASE_TAG=dev-2026-04-10-r1

GCP_RUN_NETWORK=dev-vpc
GCP_RUN_SUBNET=dev-serverless
# GCP_VPC_CONNECTOR=projects/your-dev-project-id/locations/us-central1/connectors/dev-serverless
GCP_VPC_EGRESS=private-ranges-only

GCP_SQL_INSTANCE=kairyx-dev-db
GCP_SQL_DATABASE=kairyx
GCP_SQL_USER=kairyx_app
GCP_SQL_TIER=db-custom-2-8192
GCP_SQL_STORAGE_SIZE_GB=50
GCP_SQL_AVAILABILITY_TYPE=ZONAL
GCP_SQL_DATABASE_VERSION=POSTGRES_16
GCP_CLOUD_SQL_CONNECTION_NAME=${GCP_PROJECT_ID}:${GCP_REGION}:${GCP_SQL_INSTANCE}
GCP_BIGQUERY_DATASET_ID=kairyx_platform

CONTROL_PLANE_DATABASE_URL_SECRET=dev-control-plane-db-url
CONTROL_PLANE_SECRET_KEY_SECRET=dev-control-plane-secret-key
WORKER_SHARED_TOKEN_SECRET=dev-worker-shared-token

CORS_ALLOWED_ORIGINS=https://dev-console.example.internal
OIDC_ISSUER=https://accounts.google.com
OIDC_AUDIENCE=your-google-client-id.apps.googleusercontent.com
OIDC_JWKS_URL=https://www.googleapis.com/oauth2/v3/certs
OIDC_CLIENT_ID=your-google-client-id.apps.googleusercontent.com
GOOGLE_OIDC_CLIENT_ID=your-google-client-id.apps.googleusercontent.com
OIDC_AUTHORIZE_URL=https://accounts.google.com/o/oauth2/v2/auth
OIDC_TOKEN_URL=https://oauth2.googleapis.com/token
OIDC_LOGOUT_URL=
GOOGLE_OIDC_HOSTED_DOMAIN=example.com

GCP_SECRET_PROJECT_ID=${GCP_PROJECT_ID}
GCS_BUCKET_NAME=kairyx-dev-data
IMPORT_COMMAND_TOPIC=kairyx-dev-import-jobs
PREDICTION_COMMAND_TOPIC=kairyx-dev-prediction-jobs
EXPORT_COMMAND_TOPIC=kairyx-dev-export-jobs
PUBSUB_TOPIC_NAME=kairyx-dev-raw-shards

BOOTSTRAP_TENANT_ID=default
BOOTSTRAP_TENANT_NAME=Default Tenant
BOOTSTRAP_PROJECT_ID=default
BOOTSTRAP_PROJECT_NAME=Default Project

# Optional overrides:
# IMPORT_COMMAND_SUBSCRIPTION=dev-import-worker-push
# PREDICTION_COMMAND_SUBSCRIPTION=dev-prediction-worker-push
# EXPORT_COMMAND_SUBSCRIPTION=dev-export-worker-push
# SCHEDULER_JOB_NAME=dev-scheduler-worker
# GCP_SCHEDULER_CRON=* * * * *
# GCP_SCHEDULER_TIME_ZONE=UTC
# OPERATOR_API_SERVICE_ACCOUNT=operator-api@your-dev-project-id.iam.gserviceaccount.com
# IMPORT_WORKER_SERVICE_ACCOUNT=import-worker@your-dev-project-id.iam.gserviceaccount.com
# PREDICTION_WORKER_SERVICE_ACCOUNT=prediction-worker@your-dev-project-id.iam.gserviceaccount.com
# EXPORT_WORKER_SERVICE_ACCOUNT=export-worker@your-dev-project-id.iam.gserviceaccount.com
# SCHEDULER_WORKER_SERVICE_ACCOUNT=scheduler-worker@your-dev-project-id.iam.gserviceaccount.com
# PUBSUB_PUSH_INVOKER_SERVICE_ACCOUNT=pubsub-push-invoker@your-dev-project-id.iam.gserviceaccount.com
# SCHEDULER_INVOKER_SERVICE_ACCOUNT=scheduler-invoker@your-dev-project-id.iam.gserviceaccount.com
# API_ACCESS_KEY_SECRET=dev-api-access-key
# OIDC_JWT_SIGNING_SECRET_SECRET=dev-oidc-jwt-signing-secret
# GCP_EXTRA_ENV_FILE=deploy/gcp/dev.extra.env.yaml
```

Important:
- `GCP_DEPLOYMENT_TIER=dev` and `GCP_SERVICE_PREFIX=dev` are required for the current GitHub deploy flow
- if `GCP_SERVICE_PREFIX` is missing, the CI smoke check will target `operator-api` instead of `dev-operator-api`
- do not put real secret values in this file; keep only the Secret Manager secret IDs

## 9) Manual Bootstrap And First Deploy

### 9.1 Bootstrap the foundation
Run:

```bash
bash deploy/gcp/bootstrap_dev_foundation.sh deploy/gcp/dev.env
```

This script creates or verifies:
- Artifact Registry repository
- VPC and subnet
- Private Service Access range and connection
- Cloud SQL PostgreSQL instance, database, and app user
- Secret Manager secrets:
  - `CONTROL_PLANE_DATABASE_URL_SECRET`
  - `CONTROL_PLANE_SECRET_KEY_SECRET`
  - `WORKER_SHARED_TOKEN_SECRET`
- Cloud Storage bucket
- BigQuery base dataset from `GCP_BIGQUERY_DATASET_ID`
- bootstrap-scoped BigQuery dataset and `pipeline_dead_letters` table
- Pub/Sub topics
- runtime and invoker service accounts
- baseline runtime IAM bindings

Important behavior:
- the script is dev-only and rejects other deployment tiers
- service-account overrides must stay in the same GCP project
- if `CONTROL_PLANE_DATABASE_URL_SECRET` already exists, the script uses that password as the source of truth and resets the Cloud SQL user password to match it
- if `CONTROL_PLANE_SECRET_KEY_SECRET` already exists, the script reuses it instead of rotating it
- if `WORKER_SHARED_TOKEN_SECRET` already exists, the script reuses it instead of rotating it
- the bootstrap script seeds only the bootstrap-scoped BigQuery dataset and `pipeline_dead_letters` table; later org/project-scoped datasets are created lazily at runtime
- for those later org/project scopes, the runtime creates the scoped dataset automatically, skips dead-letter reads when `pipeline_dead_letters` is still absent, and lets the table be created lazily on first dead-letter write

### 9.2 Deploy the five Cloud Run services
Run:

```bash
bash deploy/gcp/deploy_cloud_run.sh deploy/gcp/dev.env
```

This deploys:
- `dev-operator-api`
- `dev-import-worker`
- `dev-prediction-worker`
- `dev-export-worker`
- `dev-scheduler-worker`

### 9.3 Configure Pub/Sub push and Cloud Scheduler
Run:

```bash
bash deploy/gcp/configure_dev_eventing.sh deploy/gcp/dev.env
```

This script:
- resolves the deployed worker URLs
- grants `roles/run.invoker` on worker services to the dedicated Pub/Sub and Scheduler caller identities
- creates or updates the push subscriptions
- creates or updates the Cloud Scheduler HTTP job

Default generated names when `GCP_SERVICE_PREFIX=dev`:
- import subscription: `dev-import-worker-push`
- prediction subscription: `dev-prediction-worker-push`
- export subscription: `dev-export-worker-push`
- scheduler job: `dev-scheduler-worker`

The script fails fast if an existing subscription name points at the wrong Pub/Sub topic.

### 9.4 Validate the eventing contract without changing resources
Run:

```bash
bash deploy/gcp/configure_dev_eventing.sh --validate-only deploy/gcp/dev.env
```

Use this when checking local config or matching the GitHub CI preflight behavior.

## 10) GitHub Actions Dev Auto-Deploy

### 10.1 Workflow behavior
`.github/workflows/deploy-dev.yml` does the following on every push to `main`:

1. validates the repo
2. validates the GitHub `dev` environment contract
3. authenticates to GCP with WIF
4. renders a temporary deploy env file
5. validates the dev eventing contract
6. deploys the five Cloud Run services
7. configures Pub/Sub push and Cloud Scheduler
8. waits for the latest Cloud Run revision to become ready
9. smoke-checks `operator-api` at `/health/live`

The GitHub workflow does not read `deploy/gcp/dev.env`. It uses GitHub `Environment -> dev -> Variables`.

### 10.2 Create the Workload Identity Pool and Provider
Run these once with an admin-capable GCP identity:

```bash
export PROJECT_ID="your-dev-project-id"
export PROJECT_NUMBER="$(gcloud projects describe "${PROJECT_ID}" --format='value(projectNumber)')"
export POOL_ID="github-actions"
export PROVIDER_ID="github-main"
export REPO="dimsumj/KairyxAI"
export DEPLOY_SA="kairyx-dev-deploy-service@${PROJECT_ID}.iam.gserviceaccount.com"

gcloud iam workload-identity-pools create "${POOL_ID}" \
  --project="${PROJECT_ID}" \
  --location="global" \
  --display-name="GitHub Actions"

gcloud iam workload-identity-pools providers create-oidc "${PROVIDER_ID}" \
  --project="${PROJECT_ID}" \
  --location="global" \
  --workload-identity-pool="${POOL_ID}" \
  --display-name="GitHub main branch" \
  --issuer-uri="https://token.actions.githubusercontent.com" \
  --attribute-mapping="google.subject=assertion.sub,attribute.repository=assertion.repository,attribute.repository_owner=assertion.repository_owner,attribute.ref=assertion.ref" \
  --attribute-condition="assertion.repository=='${REPO}' && assertion.ref=='refs/heads/main'"

gcloud iam service-accounts add-iam-policy-binding "${DEPLOY_SA}" \
  --project="${PROJECT_ID}" \
  --role="roles/iam.workloadIdentityUser" \
  --member="principalSet://iam.googleapis.com/projects/${PROJECT_NUMBER}/locations/global/workloadIdentityPools/${POOL_ID}/attribute.repository/${REPO}"
```

Get the exact provider resource name for GitHub:

```bash
gcloud iam workload-identity-pools providers describe "${PROVIDER_ID}" \
  --project="${PROJECT_ID}" \
  --location="global" \
  --workload-identity-pool="${POOL_ID}" \
  --format='value(name)'
```

That output becomes `GCP_WORKLOAD_IDENTITY_PROVIDER` in GitHub.

### 10.3 GitHub `Environment -> dev -> Variables`
Create a GitHub environment named `dev`. The current workflow expects these values as environment variables, not secrets.

Required variables with sample values:

| Variable | Sample value |
| --- | --- |
| `GCP_PROJECT_ID` | `your-dev-project-id` |
| `GCP_REGION` | `us-central1` |
| `GCP_ARTIFACT_REGISTRY_REPOSITORY` | `kairyx` |
| `GCP_IMAGE_NAME` | `kairyxai` |
| `GCP_DEPLOYMENT_TIER` | `dev` |
| `GCP_SERVICE_PREFIX` | `dev` |
| `GCP_CLOUD_SQL_CONNECTION_NAME` | `your-dev-project-id:us-central1:kairyx-dev-db` |
| `GCP_RUN_NETWORK` | `dev-vpc` |
| `GCP_RUN_SUBNET` | `dev-serverless` |
| `CONTROL_PLANE_DATABASE_URL_SECRET` | `dev-control-plane-db-url` |
| `CONTROL_PLANE_SECRET_KEY_SECRET` | `dev-control-plane-secret-key` |
| `WORKER_SHARED_TOKEN_SECRET` | `dev-worker-shared-token` |
| `CORS_ALLOWED_ORIGINS` | `https://dev-console.example.internal` |
| `OIDC_ISSUER` | `https://accounts.google.com` |
| `OIDC_AUDIENCE` | `your-google-client-id.apps.googleusercontent.com` |
| `OIDC_JWKS_URL` | `https://www.googleapis.com/oauth2/v3/certs` |
| `OIDC_CLIENT_ID` | `your-google-client-id.apps.googleusercontent.com` |
| `GOOGLE_OIDC_CLIENT_ID` | `your-google-client-id.apps.googleusercontent.com` |
| `OIDC_AUTHORIZE_URL` | `https://accounts.google.com/o/oauth2/v2/auth` |
| `OIDC_TOKEN_URL` | `https://oauth2.googleapis.com/token` |
| `GOOGLE_OIDC_HOSTED_DOMAIN` | `example.com` |
| `GCP_SECRET_PROJECT_ID` | `your-dev-project-id` |
| `GCP_BIGQUERY_DATASET_ID` | `kairyx_platform` |
| `GCS_BUCKET_NAME` | `kairyx-dev-data` |
| `IMPORT_COMMAND_TOPIC` | `kairyx-dev-import-jobs` |
| `PREDICTION_COMMAND_TOPIC` | `kairyx-dev-prediction-jobs` |
| `EXPORT_COMMAND_TOPIC` | `kairyx-dev-export-jobs` |
| `PUBSUB_TOPIC_NAME` | `kairyx-dev-raw-shards` |
| `BOOTSTRAP_TENANT_ID` | `default` |
| `BOOTSTRAP_TENANT_NAME` | `Default Tenant` |
| `BOOTSTRAP_PROJECT_ID` | `default` |
| `BOOTSTRAP_PROJECT_NAME` | `Default Project` |
| `GCP_WORKLOAD_IDENTITY_PROVIDER` | `projects/123456789012/locations/global/workloadIdentityPools/github-actions/providers/github-main` |
| `GCP_DEPLOY_SERVICE_ACCOUNT` | `kairyx-dev-deploy-service@your-dev-project-id.iam.gserviceaccount.com` |

Optional variables:

| Variable | Sample value |
| --- | --- |
| `GCP_VPC_CONNECTOR` | `projects/your-dev-project-id/locations/us-central1/connectors/dev-serverless` |
| `GCP_VPC_EGRESS` | `private-ranges-only` |
| `OIDC_LOGOUT_URL` | empty |
| `API_ACCESS_KEY_SECRET` | `dev-api-access-key` |
| `OIDC_JWT_SIGNING_SECRET_SECRET` | `dev-oidc-jwt-signing-secret` |
| `GCP_EXTRA_ENV_FILE` | `deploy/gcp/dev.extra.env.yaml` |
| `OPERATOR_API_SERVICE_ACCOUNT` | `operator-api@your-dev-project-id.iam.gserviceaccount.com` |
| `IMPORT_WORKER_SERVICE_ACCOUNT` | `import-worker@your-dev-project-id.iam.gserviceaccount.com` |
| `PREDICTION_WORKER_SERVICE_ACCOUNT` | `prediction-worker@your-dev-project-id.iam.gserviceaccount.com` |
| `EXPORT_WORKER_SERVICE_ACCOUNT` | `export-worker@your-dev-project-id.iam.gserviceaccount.com` |
| `SCHEDULER_WORKER_SERVICE_ACCOUNT` | `scheduler-worker@your-dev-project-id.iam.gserviceaccount.com` |
| `PUBSUB_PUSH_INVOKER_SERVICE_ACCOUNT` | `pubsub-push-invoker@your-dev-project-id.iam.gserviceaccount.com` |
| `SCHEDULER_INVOKER_SERVICE_ACCOUNT` | `scheduler-invoker@your-dev-project-id.iam.gserviceaccount.com` |

Important:
- `GCP_WORKLOAD_IDENTITY_PROVIDER` and `GCP_DEPLOY_SERVICE_ACCOUNT` are GitHub environment variables in the current workflow, not GitHub secrets
- if `GCP_DEPLOYMENT_TIER` or `GCP_SERVICE_PREFIX` is missing, the workflow fails preflight before Google auth or deploy
- if `GCP_SERVICE_PREFIX` is blank, the smoke check will build the wrong service name

### 10.4 Optional: verify the CI contract locally
You can verify the GitHub environment contract locally by exporting the same variables in your shell and running:

```bash
python3 deploy/gcp/render_ci_env.py --check-only
```

## 11) Validation Checklist

### 11.1 Foundation validation
- the required APIs are enabled
- Artifact Registry repository exists
- VPC, subnet, and Private Service Access are configured
- Cloud SQL instance, database, and user exist
- Secret Manager contains:
  - `CONTROL_PLANE_DATABASE_URL_SECRET`
  - `CONTROL_PLANE_SECRET_KEY_SECRET`
  - `WORKER_SHARED_TOKEN_SECRET`
- Cloud Storage bucket exists
- BigQuery base dataset exists
- bootstrap-scoped BigQuery dataset exists
- `pipeline_dead_letters` exists in the bootstrap-scoped dataset
- newly created org/project scopes create their BigQuery dataset lazily at runtime, and first-read health or import analysis checks skip `pipeline_dead_letters` until the first dead-letter write creates it
- Pub/Sub topics exist
- runtime and invoker service accounts exist

### 11.2 Runtime validation
- all five Cloud Run services are deployed
- worker services remain authenticated-only
- `operator-api` is reachable
- `curl https://YOUR_OPERATOR_API_URL/health/live` returns `200`
- `curl https://YOUR_OPERATOR_API_URL/health` returns `200`
- `curl https://YOUR_OPERATOR_API_URL/api/v1/health` returns `401` without a bearer token

### 11.3 Eventing validation
- `bash deploy/gcp/configure_dev_eventing.sh --validate-only deploy/gcp/dev.env` succeeds
- the import, prediction, and export subscriptions exist
- each subscription points at the correct worker topic and worker URL
- the Cloud Scheduler job exists and points at `dev-scheduler-worker`
- the Pub/Sub and Scheduler caller identities have `roles/run.invoker` on the expected services

### 11.4 GitHub CI validation
- the GitHub environment name is exactly `dev`
- `deploy-dev` succeeds on a push to `main`
- the workflow summary shows the deployed release tag and the `operator-api` health URL
- the smoke step checks the prefixed service URL, for example `https://dev-operator-api-.../health/live`

## 12) Security And Cleanup
- remove broad human admin access after the first successful bootstrap
- keep the deploy service account scoped to the dev project only
- keep runtime service accounts least-privilege
- never commit `deploy/gcp/dev.env`
- never paste the real database URL or worker shared token into docs, tickets, or git history
- restrict who can inspect Pub/Sub subscription and Cloud Scheduler job configuration because those resources store the worker token in the callback URI

## 13) Relationship To The Production Runbook
Use this runbook for:
- the first internal dev environment
- ongoing manual dev redeploys
- GitHub `main` auto-deploy setup

Use `docs/GCP_PRODUCTION_DEPLOYMENT_RUNBOOK.md` for:
- production sizing and quotas
- Cloud Run production hardening
- production IAM posture
- multi-environment rollout planning
