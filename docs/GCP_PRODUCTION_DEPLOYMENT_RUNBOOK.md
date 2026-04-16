# KairyxAI GCP Production Deployment Runbook

## 1) Purpose
This runbook describes how to deploy the current KairyxAI multi-tenant SaaS stack to production on Google Cloud Platform (GCP).

It is written for the repository state as of `2026-04-06` and assumes the production topology defined in the multi-tenant production-readiness PRD:
- `operator-api` on Cloud Run
- `import-worker` on Cloud Run
- `prediction-worker` on Cloud Run
- `export-worker` on Cloud Run
- `scheduler-worker` on Cloud Run
- `Postgres` on Cloud SQL
- `Pub/Sub` for worker dispatch
- `Cloud Scheduler` for periodic control-loop execution
- `BigQuery` and `Cloud Storage` for tenant-scoped data
- `Secret Manager` for secret references

---

## 2) Recommended Production Shape

### 2.1 Region And Network
- Use one primary region for the full v1 stack. Good defaults are `us-central1` or `us-east1`.
- Keep Cloud Run, Cloud SQL, Pub/Sub subscriptions, Artifact Registry, and Cloud Scheduler in the same region unless there is a strict business reason not to.
- Use private connectivity from Cloud Run to Cloud SQL.
- Use one dedicated production project, separate from staging and development.
- For internal `dev` and `qa`, either:
  - use separate non-prod projects, or
  - keep one shared non-prod project and set `GCP_SERVICE_PREFIX` so service names do not collide, for example `dev-operator-api` and `qa-operator-api`.

### 2.2 Public Versus Private Services
- `operator-api`
  - Public HTTPS endpoint
  - App-level auth is handled by OIDC bearer tokens on org-scoped paths such as `/{organization_id}/v1/...` plus `X-Kairyx-Project`
  - Optional but recommended later: external HTTPS load balancer, Cloud Armor, and managed certificate
- `import-worker`, `prediction-worker`, `export-worker`, `scheduler-worker`
  - Authenticated invocation only
  - Invoked by Pub/Sub push or Cloud Scheduler with authenticated service accounts
  - No public unauthenticated access
  - If your organization supports an internal-only event path, prefer restricted ingress; otherwise public HTTPS with authenticated invocation is acceptable for v1

### 2.3 Important Runtime Note
- Cloud Run does not "restart because of load." Under load, it scales out by creating more instances.
- Restarts should be used for unhealthy instances only, through liveness probes or process crashes.
- For expected spikes, autoscaling plus minimum instances is the correct control plane, not forced restarts.

---

## 3) Required GCP Services

| Service | Required | Purpose | Baseline recommendation |
| --- | --- | --- | --- |
| Cloud Run | Yes | Host API and workers | 5 services, regional |
| Artifact Registry | Yes | Store container images | 1 Docker repo in the production region |
| Cloud Build | Optional | Alternative build system if you do not build from a workstation or CI runner with Docker | Use only if your release process prefers managed remote builds |
| Cloud SQL for PostgreSQL | Yes | Control-plane system of record | Regional HA instance |
| Pub/Sub | Yes | Import, prediction, and export command delivery | 4 primary topics plus DLQ topics |
| Cloud Scheduler | Yes | Trigger `scheduler-worker` | 1 production cron job |
| Secret Manager | Yes | Store DB URL, provider credentials, connector credentials, callback secrets | Separate production secrets, least privilege |
| BigQuery | Yes | Tenant-scoped warehouse and reporting | On-demand at first |
| Cloud Storage | Yes | Raw shard staging, exports, backups, tenant prefixes | Regional standard buckets |
| Cloud Monitoring | Yes | Alerts and dashboards | Production alert policies and notification channels |
| Cloud Logging | Yes | Structured application and audit logs | Retention aligned to policy |
| IAM | Yes | Service identities and least-privilege bindings | One service account per runtime role |
| VPC + Private Service Access | Yes | Private Cloud SQL connectivity | One production VPC |
| Serverless VPC Access or Direct VPC egress | Yes | Cloud Run private egress path | Prefer Direct VPC egress; use connector only if required |

---

## 4) Recommended Starting Capacity

### 4.1 Cloud Run Service Sizing

These are starting values for the current codebase, not final permanent limits. Re-test them under load before general availability.

| Service | CPU | Memory | Concurrency | Min instances | Max instances | Timeout | Recommendation notes |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `operator-api` | `2` | `4Gi` | `40` | `2` | `20` | `300s` | Good baseline for FastAPI + Gunicorn + frontend shell |
| `import-worker` | `2` | `4Gi` | `1` | `1` | `20` | `3600s` | Keep concurrency at `1` because one push request maps to one job |
| `prediction-worker` | `2` | `8Gi` | `1` | `0` | `10` | `3600s` | More memory than API; raise CPU only if load tests prove benefit |
| `export-worker` | `2` | `4Gi` | `1` | `0` | `20` | `1800s` | Export work is job-oriented, not request-parallel |
| `scheduler-worker` | `1` | `1Gi` | `1` | `1` | `2` | `300s` | Keep one warm instance because it runs on a schedule |

### 4.2 Cloud SQL Sizing

Use Cloud SQL PostgreSQL with HA enabled from day one.

| Environment size | Machine | Storage | HA | Recommended use |
| --- | --- | --- | --- | --- |
| First production / pilot | `db-custom-4-16384` | `200 GB` SSD or Hyperdisk Balanced | Yes | First real multi-tenant production cutover |
| Growth | `db-custom-8-32768` | `500 GB` SSD or Hyperdisk Balanced | Yes | Higher tenant count and heavier job overlap |
| Large v1 footprint | `db-custom-16-65536` | `1 TB+` | Yes + read replica(s) | Heavy reporting and stronger DR posture |

Important constraints:
- Do not use shared-core production shapes such as `db-f1-micro` or `db-g1-small`.
- Enable automatic storage increase.
- Set a storage auto-increase limit so temporary spikes do not grow the instance without bound.
- Enable automated backups, PITR, deletion protection, and a maintenance window.

### 4.3 BigQuery And Storage Sizing
- BigQuery
  - Start with on-demand query pricing.
  - Do not buy reservations until you have 2 to 4 weeks of actual query-cost data.
  - Enforce app-side per-tenant query limits using the existing settings.
- Cloud Storage
  - Use regional standard storage.
  - Create business-critical buckets with soft delete enabled.
  - If you create a separate short-lived temp bucket, you may disable soft delete there to avoid temporary-object cost amplification.

---

## 5) Required Production Settings From This Repository

The following runtime settings are required by the current code in production:

| Variable | Required production value |
| --- | --- |
| `APP_ENV` | `prod` |
| `SERVICE_ROLE` | One of `operator-api`, `import-worker`, `prediction-worker`, `export-worker`, `scheduler-worker` |
| `CONTROL_PLANE_DATABASE_URL` | Postgres URL, not SQLite |
| `DATA_BACKEND_MODE` | `gcp` |
| `WAREHOUSE_BACKEND` | `bigquery` |
| `OBJECT_STORAGE_BACKEND` | `gcs` |
| `MESSAGE_BACKEND` | `pubsub` |
| `SECRET_BACKEND` | `gcp_secret_manager` |
| `KAIRYX_PLATFORM_SURFACE` | Unset |
| `KAIRYX_MOCK_STORAGE_BACKEND` | Unset or `local_files`, never `database` |
| `KAIRYX_RUNTIME_DIR` | Unset unless you have a separate non-demo operational need |
| `LEGACY_HEADER_AUTH_ENABLED` | `false` |
| `CORS_ALLOWED_ORIGINS` | Explicit production origins, never `*` |
| `WORKER_SHARED_TOKEN` | Required on worker services and used in Pub/Sub / Scheduler callback URLs |
| `OIDC_ISSUER` | Real production issuer |
| `OIDC_AUDIENCE` | Real production audience |
| `OIDC_JWKS_URL` | Real JWKS URL unless you intentionally use local signing secret mode |
| `OIDC_JWKS_TIMEOUT_SECONDS` | Optional fail-fast timeout for JWKS retrieval; keep it low to avoid long auth hangs |
| `OIDC_CLIENT_ID` | Real console client ID |
| `GOOGLE_OIDC_CLIENT_ID` | Optional alias for `OIDC_CLIENT_ID` if you prefer Google-named env templates |
| `OIDC_AUTHORIZE_URL` | Real IdP authorize URL |
| `OIDC_TOKEN_URL` | Real IdP token URL |
| `OIDC_LOGOUT_URL` | Real IdP logout URL |
| `GOOGLE_OIDC_HOSTED_DOMAIN` | Optional Google hosted-domain hint |
| `GCP_PROJECT_ID` | Production project ID |
| `GCP_SECRET_PROJECT_ID` | Production secret project ID |
| `GCS_BUCKET_NAME` | Production Cloud Storage bucket for raw shards and exports |
| `IMPORT_COMMAND_TOPIC` | Production import command topic |
| `PREDICTION_COMMAND_TOPIC` | Production prediction command topic |
| `EXPORT_COMMAND_TOPIC` | Production export command topic |
| `PUBSUB_TOPIC_NAME` | Production raw shard topic |
| `SCHEDULER_ENABLED` | `false` on `operator-api`, `true` on `scheduler-worker` |
| `BOOTSTRAP_TENANT_ID` | Production bootstrap tenant ID |
| `BOOTSTRAP_TENANT_NAME` | Production bootstrap tenant name |

The app already rejects the following in `APP_ENV=prod`:
- SQLite
- `DATA_BACKEND_MODE=mock`
- `LEGACY_HEADER_AUTH_ENABLED=true`
- wildcard CORS
- missing OIDC issuer, audience, and JWKS/signing settings
- `operator-api` running the in-process scheduler

The production Cloud Run path must also stay off the Vercel demo adapter:
- do not set `KAIRYX_PLATFORM_SURFACE=vercel_demo`
- do not rely on runtime SQLite fallback
- do not use database-backed mock storage

Important repository note:
- `backend/services/.env.example` currently defaults to the AWS-native backend stack.
- Do not copy that file into GCP production unchanged.
- The GCP deploy script in this repository explicitly overrides the backend selectors to `bigquery + gcs + pubsub + gcp_secret_manager`.

---

## 6) IAM And Service Accounts

Create separate service accounts at minimum:
- `operator-api@PROJECT_ID.iam.gserviceaccount.com`
- `import-worker@PROJECT_ID.iam.gserviceaccount.com`
- `prediction-worker@PROJECT_ID.iam.gserviceaccount.com`
- `export-worker@PROJECT_ID.iam.gserviceaccount.com`
- `scheduler-worker@PROJECT_ID.iam.gserviceaccount.com`
- `pubsub-push-invoker@PROJECT_ID.iam.gserviceaccount.com`
- `scheduler-invoker@PROJECT_ID.iam.gserviceaccount.com`

Recommended bindings:

| Principal | Minimum practical roles |
| --- | --- |
| `operator-api` | `roles/cloudsql.client`, `roles/secretmanager.secretAccessor`, `roles/pubsub.publisher`, BigQuery dataset access, Cloud Storage bucket access |
| `import-worker` | `roles/cloudsql.client`, `roles/secretmanager.secretAccessor`, `roles/pubsub.publisher`, BigQuery dataset access, Cloud Storage bucket access |
| `prediction-worker` | `roles/cloudsql.client`, `roles/secretmanager.secretAccessor`, BigQuery dataset access |
| `export-worker` | `roles/cloudsql.client`, `roles/secretmanager.secretAccessor`, BigQuery dataset access, Cloud Storage bucket access |
| `scheduler-worker` | `roles/cloudsql.client`, `roles/secretmanager.secretAccessor`, BigQuery dataset access if scheduled jobs require it |
| `pubsub-push-invoker` | `roles/run.invoker` on worker services |
| `scheduler-invoker` | `roles/run.invoker` on `scheduler-worker` |

Use resource-level permissions where possible instead of project-wide owner/editor roles.

---

## 7) Step-By-Step Deployment

Use the following sample values as placeholders while reading the examples in this section:

```bash
export GCP_PROJECT_ID="kairyx-prod"
export GCP_PROJECT_NUMBER="123456789012"
export GCP_REGION="us-central1"
export GCP_ZONE="us-central1-a"
export GCP_ARTIFACT_REGISTRY_REPOSITORY="kairyx"
export GCP_IMAGE_NAME="kairyxai"
export GCP_RELEASE_TAG="2026-04-06-r1"
export GCP_NETWORK="kairyx-prod-vpc"
export GCP_SUBNET="kairyx-prod-serverless"
export GCP_SQL_INSTANCE="kairyx-prod-db"
export GCP_SQL_DATABASE="kairyx"
export GCP_SQL_USER="kairyx_app"
export GCP_SQL_CONNECTION_NAME="${GCP_PROJECT_ID}:${GCP_REGION}:${GCP_SQL_INSTANCE}"
export GCS_BUCKET_NAME="kairyx-prod-data"
export IMPORT_COMMAND_TOPIC="kairyx-import-jobs"
export PREDICTION_COMMAND_TOPIC="kairyx-prediction-jobs"
export EXPORT_COMMAND_TOPIC="kairyx-export-jobs"
export RAW_SHARD_TOPIC="kairyx-raw-shards"
```

Replace them with your real values before running anything.

### 7.1 Create The Production Project
1. Create a dedicated production GCP project.
2. Set billing, organization policies, labels, and deletion protection rules.
3. Set the default region that will host Cloud Run, Cloud SQL, Artifact Registry, and Scheduler.

Example:

```bash
gcloud projects create "${GCP_PROJECT_ID}" --name="KairyxAI Production"

gcloud config set project "${GCP_PROJECT_ID}"

gcloud beta billing projects link "${GCP_PROJECT_ID}" \
  --billing-account="REPLACE_WITH_BILLING_ACCOUNT_ID"
```

After project creation:
- enable required org labels and policies using your organization's standard platform tooling
- confirm the project number because some later IAM bindings use the Pub/Sub service agent derived from it

### 7.2 Enable Required APIs
Enable at least:
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
- `vpcaccess.googleapis.com`
- `iam.googleapis.com`
- `iamcredentials.googleapis.com`

Example:

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
  vpcaccess.googleapis.com \
  iam.googleapis.com \
  iamcredentials.googleapis.com \
  --project="${GCP_PROJECT_ID}"
```

### 7.3 Create Artifact Registry
1. Create one Docker repository in the production region, for example `kairyx`.
2. Grant the build identity permission to push images.
3. Reuse one shared immutable image digest for all Cloud Run services.
4. Promote that digest across environments instead of rebuilding separate role-specific images.

Inference from the repo:
- One shared immutable image is operationally cleaner because all five services use the same repo-root Dockerfile and differ only by `SERVICE_ROLE` plus role-specific env.

Example:

```bash
gcloud artifacts repositories create "${GCP_ARTIFACT_REGISTRY_REPOSITORY}" \
  --project="${GCP_PROJECT_ID}" \
  --repository-format=docker \
  --location="${GCP_REGION}" \
  --description="KairyxAI runtime images"
```

### 7.4 Create The Production VPC
1. Create a dedicated production VPC and subnet in the same region.
2. Configure Private Service Access for Cloud SQL private IP.
3. Prefer Direct VPC egress for Cloud Run to reach private IP resources.
4. Use Serverless VPC Access only if organization policy or network design requires it.

Example:

```bash
gcloud compute networks create "${GCP_NETWORK}" \
  --project="${GCP_PROJECT_ID}" \
  --subnet-mode=custom

gcloud compute networks subnets create "${GCP_SUBNET}" \
  --project="${GCP_PROJECT_ID}" \
  --network="${GCP_NETWORK}" \
  --region="${GCP_REGION}" \
  --range="10.20.0.0/24"

gcloud compute addresses create google-managed-services-"${GCP_NETWORK}" \
  --project="${GCP_PROJECT_ID}" \
  --global \
  --purpose=VPC_PEERING \
  --prefix-length=16 \
  --network="${GCP_NETWORK}"

gcloud services vpc-peerings connect \
  --project="${GCP_PROJECT_ID}" \
  --service=servicenetworking.googleapis.com \
  --ranges=google-managed-services-"${GCP_NETWORK}" \
  --network="${GCP_NETWORK}"
```

### 7.5 Provision Cloud SQL PostgreSQL
1. Create a regional PostgreSQL instance with HA enabled.
2. Recommended starting shape: `db-custom-4-16384`, `200 GB`, SSD or Hyperdisk Balanced.
3. Use private IP only.
4. Enable:
   - automated backups
   - PITR
   - deletion protection
   - maintenance window
   - Query Insights
   - automatic storage increase
   - storage auto-increase limit
5. Create:
   - database `kairyx`
   - least-privilege application user
6. Store the connection URL in Secret Manager.

Example:

```bash
gcloud sql instances create "${GCP_SQL_INSTANCE}" \
  --project="${GCP_PROJECT_ID}" \
  --database-version=POSTGRES_16 \
  --region="${GCP_REGION}" \
  --availability-type=REGIONAL \
  --tier=db-custom-4-16384 \
  --storage-type=SSD \
  --storage-size=200GB \
  --network="projects/${GCP_PROJECT_ID}/global/networks/${GCP_NETWORK}" \
  --no-assign-ip \
  --backup-start-time=03:00 \
  --enable-point-in-time-recovery \
  --maintenance-window-day=7 \
  --maintenance-window-hour=4 \
  --deletion-protection

gcloud sql databases create "${GCP_SQL_DATABASE}" \
  --project="${GCP_PROJECT_ID}" \
  --instance="${GCP_SQL_INSTANCE}"

gcloud sql users create "${GCP_SQL_USER}" \
  --project="${GCP_PROJECT_ID}" \
  --instance="${GCP_SQL_INSTANCE}"
```

Set the database user's password through your approved admin workflow before building `CONTROL_PLANE_DATABASE_URL`.

Recommended secret names:
- `control-plane-db-url`
- `worker-shared-token`
- `oidc-client-secret` if your IdP flow requires it outside the app
- `provider-sendgrid-api-key`
- `provider-braze-api-key`
- provider callback signing secrets
- connector credentials per integration

### 7.5.1 Build `CONTROL_PLANE_DATABASE_URL`
For the current Cloud Run deployment path in this repository, use the Cloud SQL Unix socket form:

```text
postgresql+psycopg://DB_USER:DB_PASSWORD@/DB_NAME?host=/cloudsql/PROJECT_ID:REGION:INSTANCE_NAME
```

Example:

```text
postgresql+psycopg://kairyx_app:REPLACE_ME@/kairyx?host=/cloudsql/kairyx-prod:us-central1:kairyx-prod-db
```

How to fill it in:
- `DB_USER`: the least-privilege Postgres application user you created for KairyxAI
- `DB_PASSWORD`: the password for that user
- `DB_NAME`: usually `kairyx`
- `PROJECT_ID`: the GCP project that owns the Cloud SQL instance
- `REGION`: the Cloud SQL region, for example `us-central1`
- `INSTANCE_NAME`: the Cloud SQL instance name, for example `kairyx-prod-db`

Important details:
- Use the `/cloudsql/PROJECT:REGION:INSTANCE` host form for this repo's Cloud Run path.
- Do not use SQLite in production.
- If the password contains characters such as `@`, `:`, `/`, or `?`, URL-encode the password before building the URL.

Store that URL in Secret Manager and reference the secret ID through `CONTROL_PLANE_DATABASE_URL_SECRET`.

Example secret creation:

```bash
printf '%s' 'postgresql+psycopg://kairyx_app:REPLACE_ME@/kairyx?host=/cloudsql/kairyx-prod:us-central1:kairyx-prod-db' \
  | gcloud secrets create control-plane-db-url --data-file=-
```

If the secret already exists, add a new version instead:

```bash
printf '%s' 'postgresql+psycopg://kairyx_app:REPLACE_ME@/kairyx?host=/cloudsql/kairyx-prod:us-central1:kairyx-prod-db' \
  | gcloud secrets versions add control-plane-db-url --data-file=-
```

### 7.6 Create BigQuery Datasets
1. Create the control datasets or dataset prefixes used by the product in the production project.
2. Keep tenant isolation at the dataset or dataset-prefix level according to the multi-tenant PRD.
3. Apply labels for environment, owner, data-classification, and product.
4. Grant BigQuery access only to the service accounts that need it.

Example:

```bash
bq --location="${GCP_REGION}" mk \
  --dataset \
  --label=environment:prod \
  --label=product:kairyx \
  "${GCP_PROJECT_ID}:kairyx_platform"
```

If you use multiple datasets for tenant or workload separation, repeat that pattern with your actual dataset naming convention.

### 7.7 Create Cloud Storage Buckets
1. Create regional buckets in the same region as the runtime.
2. Use uniform bucket-level access.
3. Keep tenant data under prefixes such as:
   - `tenants/<tenant_id>/raw/`
   - `tenants/<tenant_id>/exports/`
   - `tenants/<tenant_id>/backups/`
4. Enable soft delete for business-critical buckets.
5. If you create a separate short-lived staging bucket, document whether soft delete is disabled there for cost reasons.

Example:

```bash
gcloud storage buckets create "gs://${GCS_BUCKET_NAME}" \
  --project="${GCP_PROJECT_ID}" \
  --location="${GCP_REGION}" \
  --uniform-bucket-level-access
```

Example prefix layout after the bucket exists:

```text
gs://kairyx-prod-data/tenants/default/raw/
gs://kairyx-prod-data/tenants/default/exports/
gs://kairyx-prod-data/tenants/default/backups/
```

### 7.8 Create Secret Manager Secrets
1. Create production-only secrets in Secret Manager.
2. Use least-privilege secret IAM.
3. Prefer stable secret IDs and rotate by adding versions.
4. Create a dedicated `CONTROL_PLANE_SECRET_KEY` secret for browser-entered connector and provider credentials so the control plane can encrypt them before persistence.
5. For this repo, use `gsm://SECRET_NAME` references when you want the code to resolve the latest version automatically.
6. If you need strict release control for a high-risk secret, use a full Secret Manager version path because the current secret resolver also accepts `projects/.../secrets/.../versions/...`.

Example:

```bash
printf '%s' 'replace-me-with-a-long-random-control-plane-secret-key' \
  | gcloud secrets create control-plane-secret-key \
      --project="${GCP_PROJECT_ID}" \
      --data-file=-

printf '%s' 'replace-me-with-a-long-random-worker-token' \
  | gcloud secrets create worker-shared-token \
      --project="${GCP_PROJECT_ID}" \
      --data-file=-

printf '%s' 'replace-me-with-sendgrid-key' \
  | gcloud secrets create provider-sendgrid-api-key \
      --project="${GCP_PROJECT_ID}" \
      --data-file=-
```

If a secret already exists:

```bash
printf '%s' 'new-secret-value' \
  | gcloud secrets versions add worker-shared-token \
      --project="${GCP_PROJECT_ID}" \
      --data-file=-
```

### 7.9 Create Service Accounts And IAM Bindings
1. Create the runtime service accounts listed in Section 6.
2. Grant only the minimum roles needed.
3. Grant `roles/run.invoker` to:
   - `pubsub-push-invoker` on `import-worker`, `prediction-worker`, and `export-worker`
   - `scheduler-invoker` on `scheduler-worker`
4. For Pub/Sub authenticated push, grant the Pub/Sub service agent `roles/iam.serviceAccountTokenCreator` on the push auth service account.
5. For Cloud Scheduler authenticated HTTP, grant the scheduler caller service account `roles/run.invoker` on `scheduler-worker`.

Example:

```bash
for sa in operator-api import-worker prediction-worker export-worker scheduler-worker pubsub-push-invoker scheduler-invoker; do
  gcloud iam service-accounts create "${sa}" \
    --project="${GCP_PROJECT_ID}" \
    --display-name="${sa}"
done
```

Example role bindings:

```bash
gcloud projects add-iam-policy-binding "${GCP_PROJECT_ID}" \
  --member="serviceAccount:operator-api@${GCP_PROJECT_ID}.iam.gserviceaccount.com" \
  --role="roles/cloudsql.client"

gcloud projects add-iam-policy-binding "${GCP_PROJECT_ID}" \
  --member="serviceAccount:operator-api@${GCP_PROJECT_ID}.iam.gserviceaccount.com" \
  --role="roles/secretmanager.secretAccessor"

gcloud projects add-iam-policy-binding "${GCP_PROJECT_ID}" \
  --member="serviceAccount:operator-api@${GCP_PROJECT_ID}.iam.gserviceaccount.com" \
  --role="roles/pubsub.publisher"
```

Apply the Cloud Run service-level `roles/run.invoker` bindings after the worker services exist. The later Pub/Sub and Scheduler steps show where those bindings fit operationally.

### 7.10 Dev Bootstrap Versus Production Deploy
Use the dedicated dev bootstrap path when you are standing up the first internal GCP dev environment:
- `deploy/gcp/bootstrap_dev_foundation.sh`
- `deploy/gcp/configure_dev_eventing.sh`
- `docs/GCP_DEV_ENV_BOOTSTRAP_RUNBOOK.md`

That path is intentionally dev-only and creates or verifies the project foundation before the Cloud Run deploy happens.

Use this production runbook when you already have the GCP foundation resources in place and want the production-shaped Cloud Run deploy flow.

### 7.11 Build The Production Image
Use the checked-in deploy script:
- `deploy/gcp/deploy_cloud_run.sh`
- `deploy/gcp/dev.env.example`
- `deploy/gcp/qa.env.example`

What the script does:
1. load one operator-provided env file
2. build the repo-root Docker image
3. push the tagged image to Artifact Registry
4. resolve the pushed tag to an immutable digest
5. deploy `operator-api`, `import-worker`, `prediction-worker`, `export-worker`, and `scheduler-worker` from that same digest

The script intentionally does not provision the full GCP foundation. It assumes your project, Artifact Registry repository, Cloud SQL instance, VPC path, service accounts, Secret Manager secrets, Pub/Sub topics, and Scheduler caller identity already exist.

### 7.12 Prepare Runtime Configuration
Create one deploy env file, for example `deploy/gcp/production.env`, and pass it to the script.

Checked-in non-prod templates:
- `deploy/gcp/dev.env.example`
- `deploy/gcp/qa.env.example`

Recommended usage:
- production: copy the production example from this runbook into a private env file that is not committed
- dev: start from `deploy/gcp/dev.env.example`
- qa: start from `deploy/gcp/qa.env.example`

Required deploy-script variables:

| Variable | Meaning |
| --- | --- |
| `GCP_PROJECT_ID` | Target deploy project |
| `GCP_REGION` | Cloud Run and Artifact Registry region |
| `GCP_ARTIFACT_REGISTRY_REPOSITORY` | Artifact Registry Docker repository name |
| `GCP_IMAGE_NAME` | Image name inside Artifact Registry |
| `GCP_RELEASE_TAG` | Release tag to build and push |
| `GCP_CLOUD_SQL_CONNECTION_NAME` | `PROJECT:REGION:INSTANCE` Cloud SQL connection name |
| `CONTROL_PLANE_DATABASE_URL_SECRET` | Secret Manager secret ID injected into `CONTROL_PLANE_DATABASE_URL` |
| `CONTROL_PLANE_SECRET_KEY_SECRET` | Secret Manager secret ID injected into `CONTROL_PLANE_SECRET_KEY` for encrypted-at-rest browser-entered connector and provider secrets |
| `WORKER_SHARED_TOKEN_SECRET` | Secret Manager secret ID injected into worker `WORKER_SHARED_TOKEN` |
| `CORS_ALLOWED_ORIGINS` | Production console origins |
| `OIDC_ISSUER` | Production OIDC issuer |
| `OIDC_AUDIENCE` | Production OIDC audience |
| `OIDC_JWKS_URL` | Production JWKS URL |
| `OIDC_JWKS_TIMEOUT_SECONDS` | Optional JWKS fetch timeout in seconds |
| `OIDC_CLIENT_ID` | Browser/client OIDC client ID |
| `OIDC_AUTHORIZE_URL` | Authorize URL for the IdP |
| `OIDC_TOKEN_URL` | Token URL for the IdP |
| `GCS_BUCKET_NAME` | Runtime raw-shard/export bucket |

Recommended optional deploy-script variables:

| Variable | Meaning |
| --- | --- |
| `GCP_DEPLOYMENT_TIER` | Optional deploy profile: `prod`, `qa`, or `dev`; defaults to `prod` |
| `GCP_SERVICE_PREFIX` | Optional service-name prefix such as `dev` or `qa` |
| `GCP_SECRET_PROJECT_ID` | Secret project used by app-resolved `gsm://` refs; defaults to `GCP_PROJECT_ID` |
| `GOOGLE_OIDC_CLIENT_ID` | Optional alias; defaults to `OIDC_CLIENT_ID` |
| `GOOGLE_OIDC_HOSTED_DOMAIN` | Optional hosted-domain restriction |
| `OIDC_LOGOUT_URL` | Optional logout URL |
| `API_ACCESS_KEY_SECRET` | Optional Secret Manager secret ID injected into `API_ACCESS_KEY` |
| `OIDC_JWT_SIGNING_SECRET_SECRET` | Optional Secret Manager secret ID injected into `OIDC_JWT_SIGNING_SECRET` |
| `GCP_RUN_NETWORK` and `GCP_RUN_SUBNET` | Preferred Direct VPC egress path for private Cloud SQL |
| `GCP_VPC_CONNECTOR` | Alternative to Direct VPC egress if your org requires Serverless VPC Access |
| `GCP_VPC_EGRESS` | VPC egress mode; defaults to `private-ranges-only` |
| `OPERATOR_API_SERVICE_ACCOUNT` | Optional override; defaults to `operator-api@PROJECT_ID.iam.gserviceaccount.com` |
| `IMPORT_WORKER_SERVICE_ACCOUNT` | Optional override |
| `PREDICTION_WORKER_SERVICE_ACCOUNT` | Optional override |
| `EXPORT_WORKER_SERVICE_ACCOUNT` | Optional override |
| `SCHEDULER_WORKER_SERVICE_ACCOUNT` | Optional override |
| `IMPORT_COMMAND_TOPIC` | Defaults to `kairyx-import-jobs` |
| `PREDICTION_COMMAND_TOPIC` | Defaults to `kairyx-prediction-jobs` |
| `EXPORT_COMMAND_TOPIC` | Defaults to `kairyx-export-jobs` |
| `PUBSUB_TOPIC_NAME` | Defaults to `kairyx-raw-shards` |
| `BOOTSTRAP_TENANT_ID` | Defaults to `default` |
| `BOOTSTRAP_TENANT_NAME` | Defaults to `Default Tenant` |
| `BOOTSTRAP_PROJECT_ID` | Defaults to `default` |
| `BOOTSTRAP_PROJECT_NAME` | Defaults to `Default Project` |
| `GCP_EXTRA_ENV_FILE` | Optional YAML fragment appended to the generated Cloud Run env-vars file |

Tier defaults built into the script:

| Tier | Use case | Service naming | Sizing intent |
| --- | --- | --- | --- |
| `prod` | Production traffic | no prefix by default | current production-sized defaults |
| `qa` | Internal validation, staging-like verification | usually `qa-` prefix | reduced but production-shaped capacity |
| `dev` | Internal development, smoke tests, feature checks | usually `dev-` prefix | smallest shared-test capacity |

Example `deploy/gcp/production.env`:

```bash
GCP_PROJECT_ID=kairyx-prod
GCP_REGION=us-central1
GCP_ARTIFACT_REGISTRY_REPOSITORY=kairyx
GCP_IMAGE_NAME=kairyxai
GCP_RELEASE_TAG=2026-04-06-r1
GCP_CLOUD_SQL_CONNECTION_NAME=kairyx-prod:us-central1:kairyx-prod-db
GCP_RUN_NETWORK=prod-vpc
GCP_RUN_SUBNET=prod-serverless

CONTROL_PLANE_DATABASE_URL_SECRET=control-plane-db-url
CONTROL_PLANE_SECRET_KEY_SECRET=control-plane-secret-key
WORKER_SHARED_TOKEN_SECRET=worker-shared-token

CORS_ALLOWED_ORIGINS=https://console.example.com
OIDC_ISSUER=https://accounts.google.com
OIDC_AUDIENCE=your-google-client-id.apps.googleusercontent.com
OIDC_JWKS_URL=https://www.googleapis.com/oauth2/v3/certs
OIDC_JWKS_TIMEOUT_SECONDS=5
OIDC_CLIENT_ID=your-google-client-id.apps.googleusercontent.com
GOOGLE_OIDC_CLIENT_ID=your-google-client-id.apps.googleusercontent.com
OIDC_AUTHORIZE_URL=https://accounts.google.com/o/oauth2/v2/auth
OIDC_TOKEN_URL=https://oauth2.googleapis.com/token
OIDC_LOGOUT_URL=
GOOGLE_OIDC_HOSTED_DOMAIN=

GCP_SECRET_PROJECT_ID=kairyx-prod
GCS_BUCKET_NAME=kairyx-prod-data
IMPORT_COMMAND_TOPIC=kairyx-import-jobs
PREDICTION_COMMAND_TOPIC=kairyx-prediction-jobs
EXPORT_COMMAND_TOPIC=kairyx-export-jobs
PUBSUB_TOPIC_NAME=kairyx-raw-shards
BOOTSTRAP_TENANT_ID=default
BOOTSTRAP_TENANT_NAME=Default Tenant
BOOTSTRAP_PROJECT_ID=default
BOOTSTRAP_PROJECT_NAME=Default Project
```

Example non-prod usage:

```bash
cp deploy/gcp/dev.env.example deploy/gcp/dev.env
bash deploy/gcp/deploy_cloud_run.sh deploy/gcp/dev.env

cp deploy/gcp/qa.env.example deploy/gcp/qa.env
bash deploy/gcp/deploy_cloud_run.sh deploy/gcp/qa.env
```

What the non-prod templates already do:
- set `GCP_DEPLOYMENT_TIER=dev` or `qa`
- set `GCP_SERVICE_PREFIX=dev` or `qa`
- use environment-specific topic names and bucket names
- keep `APP_ENV=prod` semantics so runtime validation stays production-shaped
- keep the default service-account names unless you explicitly override `OPERATOR_API_SERVICE_ACCOUNT`, `IMPORT_WORKER_SERVICE_ACCOUNT`, `PREDICTION_WORKER_SERVICE_ACCOUNT`, `EXPORT_WORKER_SERVICE_ACCOUNT`, or `SCHEDULER_WORKER_SERVICE_ACCOUNT`

Secrets expected by the script:
- `CONTROL_PLANE_DATABASE_URL_SECRET`, `CONTROL_PLANE_SECRET_KEY_SECRET`, and `WORKER_SHARED_TOKEN_SECRET` must be secret IDs that Cloud Run can inject at deploy time.
- In practice, keep those secrets in the same project you deploy Cloud Run into.
- `GCP_SECRET_PROJECT_ID` is still useful for app-resolved `gsm://...` connector/provider refs, but Cloud Run deploy-time secret injection should stay simple and same-project.

### 7.13 Deploy `operator-api`
Run the deploy script from the repository root:

```bash
bash deploy/gcp/deploy_cloud_run.sh deploy/gcp/production.env
```

For non-prod internal environments:

```bash
bash deploy/gcp/deploy_cloud_run.sh deploy/gcp/dev.env
bash deploy/gcp/deploy_cloud_run.sh deploy/gcp/qa.env
```

The script deploys the following Cloud Run defaults for `operator-api`:
- public ingress
- service account `operator-api`
- CPU `2`
- memory `4Gi`
- concurrency `40`
- min instances `2`
- max instances `20`
- timeout `300s`
- startup CPU boost enabled
- explicit `APP_ENV=prod`
- explicit `CORS_ALLOWED_ORIGINS`

Recommended post-deploy hardening:
- add startup and liveness probes on `/health/live`
- capture those probe settings either in the checked-in Cloud Run YAML manifests or a follow-up `gcloud run services update`

Recommended probe settings:
- startup probe
  - path: `/health/live`
  - period: `10s`
  - timeout: `2s`
  - failure threshold: `12`
- liveness probe
  - path: `/health/live`
  - period: `30s`
  - timeout: `2s`
  - failure threshold: `3`

### 7.14 Deploy The Workers

The script deploys each worker as a separate Cloud Run service using the same image digest and the role-aware entrypoint driven by `SERVICE_ROLE`:
- `import-worker`: `SERVICE_ROLE=import-worker`
- `prediction-worker`: `SERVICE_ROLE=prediction-worker`
- `export-worker`: `SERVICE_ROLE=export-worker`
- `scheduler-worker`: `SERVICE_ROLE=scheduler-worker`

Recommended worker settings:
- no unauthenticated access
- startup CPU boost enabled
- `WORKER_SHARED_TOKEN` injected through a secret reference
- add startup and liveness probes on `/health/live` as a follow-up hardening step if you want parity with the production YAML pattern in Section 9

Worker-specific sizing:
- `import-worker`
  - CPU `2`, memory `4Gi`, concurrency `1`, min `1`, max `20`, timeout `3600s`
- `prediction-worker`
  - CPU `2`, memory `8Gi`, concurrency `1`, min `0`, max `10`, timeout `3600s`
- `export-worker`
  - CPU `2`, memory `4Gi`, concurrency `1`, min `0`, max `20`, timeout `1800s`
- `scheduler-worker`
  - CPU `1`, memory `1Gi`, concurrency `1`, min `1`, max `2`, timeout `300s`

Important limit:
- Cloud Run service requests top out at `3600s`. If imports or predictions regularly approach that ceiling, split the work into smaller jobs or move that execution class to a different runtime model before production scale-out.

### 7.15 Create Pub/Sub Topics And Subscriptions
Create at minimum:
- `kairyx-raw-shards`
- `kairyx-import-jobs`
- `kairyx-prediction-jobs`
- `kairyx-export-jobs`

Recommended subscriptions:
- one push subscription per worker command topic
- one dead-letter topic per worker subscription
- exponential backoff retry policy instead of immediate redelivery

Example topic creation:

```bash
gcloud pubsub topics create kairyx-raw-shards --project="${GCP_PROJECT_ID}"
gcloud pubsub topics create kairyx-import-jobs --project="${GCP_PROJECT_ID}"
gcloud pubsub topics create kairyx-prediction-jobs --project="${GCP_PROJECT_ID}"
gcloud pubsub topics create kairyx-export-jobs --project="${GCP_PROJECT_ID}"
gcloud pubsub topics create kairyx-import-jobs-dlq --project="${GCP_PROJECT_ID}"
gcloud pubsub topics create kairyx-prediction-jobs-dlq --project="${GCP_PROJECT_ID}"
gcloud pubsub topics create kairyx-export-jobs-dlq --project="${GCP_PROJECT_ID}"
```

Recommended settings per worker subscription:
- push endpoint
  - `import-worker`: `https://import-worker-.../pubsub/push?token=WORKER_SHARED_TOKEN`
  - `prediction-worker`: `https://prediction-worker-.../pubsub/push?token=WORKER_SHARED_TOKEN`
  - `export-worker`: `https://export-worker-.../pubsub/push?token=WORKER_SHARED_TOKEN`
- authentication enabled
- push auth service account: `pubsub-push-invoker`
- audience set to the target Cloud Run URL
- dead-letter max delivery attempts: start with `10`
- retry backoff: start with `min 10s`, `max 600s`

Example push-subscription creation:

```bash
PROJECT_NUMBER="$(gcloud projects describe "${GCP_PROJECT_ID}" --format='value(projectNumber)')"
PUBSUB_SERVICE_AGENT="service-${PROJECT_NUMBER}@gcp-sa-pubsub.iam.gserviceaccount.com"

gcloud iam service-accounts add-iam-policy-binding \
  "pubsub-push-invoker@${GCP_PROJECT_ID}.iam.gserviceaccount.com" \
  --member="serviceAccount:${PUBSUB_SERVICE_AGENT}" \
  --role="roles/iam.serviceAccountTokenCreator" \
  --project="${GCP_PROJECT_ID}"

IMPORT_WORKER_URL="$(gcloud run services describe import-worker --region="${GCP_REGION}" --project="${GCP_PROJECT_ID}" --format='value(status.url)')"

gcloud pubsub subscriptions create kairyx-import-jobs-sub \
  --project="${GCP_PROJECT_ID}" \
  --topic="kairyx-import-jobs" \
  --push-endpoint="${IMPORT_WORKER_URL}/pubsub/push?token=REPLACE_WITH_WORKER_SHARED_TOKEN" \
  --push-auth-service-account="pubsub-push-invoker@${GCP_PROJECT_ID}.iam.gserviceaccount.com" \
  --push-auth-token-audience="${IMPORT_WORKER_URL}" \
  --dead-letter-topic="kairyx-import-jobs-dlq" \
  --max-delivery-attempts=10 \
  --min-retry-delay=10s \
  --max-retry-delay=600s
```

Repeat that pattern for `prediction-worker` and `export-worker`.

If you set `GCP_SERVICE_PREFIX`, resolve the prefixed service names instead:
- `dev-import-worker`
- `qa-import-worker`
- `dev-prediction-worker`
- `qa-prediction-worker`
- `dev-export-worker`
- `qa-export-worker`

### 7.16 Create The Scheduler Job
1. Create one Cloud Scheduler HTTP job pointed at `scheduler-worker` `/run?token=WORKER_SHARED_TOKEN`.
2. Use OIDC auth, not unauthenticated calls.
3. Use service account `scheduler-invoker`.
4. Set the audience to the scheduler-worker URL.
5. Configure retry policy.

Recommended starting schedule:
- every `1` minute if you need nearline control-loop checks

Recommended retry policy:
- max retry attempts `5`
- max retry duration `300s`
- min backoff `10s`
- max backoff `300s`
- max doublings `3`

Example job creation:

```bash
SCHEDULER_WORKER_URL="$(gcloud run services describe scheduler-worker --region="${GCP_REGION}" --project="${GCP_PROJECT_ID}" --format='value(status.url)')"

gcloud scheduler jobs create http kairyx-scheduler-worker \
  --project="${GCP_PROJECT_ID}" \
  --location="${GCP_REGION}" \
  --schedule="*/1 * * * *" \
  --time-zone="Etc/UTC" \
  --uri="${SCHEDULER_WORKER_URL}/run?token=REPLACE_WITH_WORKER_SHARED_TOKEN" \
  --http-method=POST \
  --oidc-service-account-email="scheduler-invoker@${GCP_PROJECT_ID}.iam.gserviceaccount.com" \
  --oidc-token-audience="${SCHEDULER_WORKER_URL}" \
  --max-retry-attempts=5 \
  --max-retry-duration=300s \
  --min-backoff=10s \
  --max-backoff=300s \
  --max-doublings=3
```

If you set `GCP_SERVICE_PREFIX`, use the prefixed scheduler service name, for example `dev-scheduler-worker` or `qa-scheduler-worker`.

### 7.17 Run Schema Migration
1. Run Alembic against production Cloud SQL.
2. Execute the multi-tenant migration before enabling live production traffic.
3. Validate:
   - bootstrap tenant exists
   - all control-plane tables have expected row counts
   - tenant-scoped unique constraints behave correctly

### 7.18 Bootstrap Production Tenancy
1. Use the platform-admin API to create the initial tenant.
2. Create at least one tenant membership.
3. If operators will enter connector or provider credentials through the web UI, make sure Cloud Run is injecting `CONTROL_PLANE_SECRET_KEY`; otherwise keep using API `*_ref` values backed by Secret Manager.
4. Run the auth smoke flow with a real bearer token against an org-scoped URL such as `/{organization_id}/v1/auth/me` and include `X-Kairyx-Project` when project selection is required.

### 7.19 Configure Monitoring And Alerts
Create notification channels first, then alert policies for:
- Cloud Run API error rate
- Cloud Run worker error rate
- Cloud Run p95 latency
- Cloud Run instance saturation or repeated cold-start pressure
- Pub/Sub DLQ message count
- Pub/Sub backlog growth
- Cloud SQL CPU and memory pressure
- Cloud SQL connection saturation
- Cloud SQL failover or restart events
- auth failure spikes
- callback failure spikes
- outcome lag

### 7.20 Smoke Test Before Traffic
Validate the following in staging first, then production:
1. `GET /health/live`
2. `GET /api/v1/health`
3. `GET /{organization_id}/v1/auth/me` with a real JWT and `X-Kairyx-Project` when project selection is required
4. create connector
5. run import
6. create cohort
7. create workflow draft
8. publish workflow
9. create experiment
10. verify worker job delivery and logs

---

## 8) Recommended Autoscaling, Restart, And Recovery Settings

### 8.1 Cloud Run Autoscaling
Use these settings on every service:
- explicit min instances
- explicit max instances
- explicit concurrency
- startup CPU boost

Recommended defaults:
- `operator-api`
  - apply scaling at the service level
  - keep `minScale=2`
  - set `maxScale=20`
  - concurrency `40`
- job workers
  - apply scaling at the service level
  - concurrency `1`
  - max scale sized to downstream capacity, not just traffic burst

Capacity rule:
- Max Cloud Run scale must not exceed what Cloud SQL, BigQuery quotas, provider APIs, or per-tenant app limits can safely absorb.

### 8.2 Cloud Run Restart Behavior
Use health probes so unhealthy instances are restarted automatically:
- startup probe protects slower application boot
- liveness probe forces restart for deadlocked or broken instances

Recommended probe path for the current repo:
- `/health/live`

### 8.3 Pub/Sub Failure Recovery
Use all three:
- authenticated push
- exponential backoff retry
- dead-letter topics

Why:
- immediate redelivery can amplify a broken worker state
- exponential backoff gives the app time to recover
- DLQ keeps poison messages from retrying forever

### 8.4 Cloud Scheduler Recovery
Enable retry policy on the scheduler job. Use authenticated HTTP and do not allow unauthenticated access to the target.

### 8.5 Cloud SQL Recovery
Enable all of the following:
- HA regional instance
- automated backups
- PITR
- deletion protection
- automatic storage increase
- maintenance window

Recommended operational rule:
- if Cloud SQL performs repeated restarts because of memory exhaustion or connection saturation, scale the instance vertically before raising Cloud Run max scale further

---

## 9) Example Cloud Run YAML Pattern

Use this as the baseline pattern when you turn the current minimal manifests into production-ready manifests:

```yaml
apiVersion: serving.knative.dev/v1
kind: Service
metadata:
  name: operator-api
spec:
  template:
    metadata:
      annotations:
        autoscaling.knative.dev/minScale: "2"
        autoscaling.knative.dev/maxScale: "20"
        run.googleapis.com/startup-cpu-boost: "true"
    spec:
      containerConcurrency: 40
      timeoutSeconds: 300
      serviceAccountName: operator-api
      containers:
        - image: us-docker.pkg.dev/PROJECT_ID/kairyx/platform:RELEASE_TAG
          resources:
            limits:
              cpu: "2"
              memory: "4Gi"
          env:
            - name: APP_ENV
              value: prod
            - name: SERVICE_ROLE
              value: operator-api
            - name: DATA_BACKEND_MODE
              value: gcp
            - name: WAREHOUSE_BACKEND
              value: bigquery
            - name: OBJECT_STORAGE_BACKEND
              value: gcs
            - name: MESSAGE_BACKEND
              value: pubsub
            - name: SECRET_BACKEND
              value: gcp_secret_manager
            - name: LEGACY_HEADER_AUTH_ENABLED
              value: "false"
            - name: SCHEDULER_ENABLED
              value: "false"
          startupProbe:
            httpGet:
              path: /health/live
              port: 8080
            timeoutSeconds: 2
            failureThreshold: 12
            periodSeconds: 10
          livenessProbe:
            httpGet:
              path: /health/live
              port: 8080
            timeoutSeconds: 2
            failureThreshold: 3
            periodSeconds: 30
```

Use the same pattern for workers, changing:
- service name
- service account
- command and args
- CPU, memory, timeout
- concurrency
- `minScale`
- `maxScale`
- `SERVICE_ROLE`

---

## 10) Rollback And Incident Controls

### 10.1 Safe Release Pattern
1. Deploy a new revision with no traffic.
2. Run health and smoke checks.
3. Shift a small percentage of traffic to the new revision.
4. Watch error rate, latency, and DLQ growth.
5. Move to full traffic only after the release stays stable.

### 10.2 Immediate Rollback Triggers
- auth failures spike after deploy
- p95 latency doubles and stays elevated
- Pub/Sub DLQ starts filling
- callback signature failures increase
- Cloud SQL restart/failover appears during deploy

### 10.3 Fastest Rollback Actions
- route traffic back to the prior Cloud Run revision
- pause scheduler if the issue is periodic-job driven
- stop worker subscriptions temporarily if one worker class is poisoning the queue
- enable the product kill switch for outbound sends if activation is affected

---

## 11) Production Go-Live Checklist
- Production project is isolated from staging and dev
- Cloud SQL is HA-enabled and private-IP only
- Backups, PITR, and deletion protection are enabled
- All Cloud Run services have explicit CPU, memory, concurrency, min scale, and max scale
- Worker services are private and authenticated
- Scheduler job is authenticated
- Pub/Sub push subscriptions use authenticated push, exponential backoff, and DLQ
- Secret Manager secrets exist for all production credentials
- `APP_ENV=prod` validation passes on every service
- OIDC login works end to end
- Tenant bootstrap and membership flows work
- Monitoring alerts and notification channels are active
- Smoke tests pass in staging and production

---

## 12) Related Documents
- `docs/MULTITENANT_PRODUCTION_READINESS_V1_PRD.md`
- `docs/RUNBOOKS_MULTITENANT_GCP.md`
- `docs/KAIRYXAI_V1_MASTER_PRD.md`
