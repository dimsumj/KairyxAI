#!/usr/bin/env bash

set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  deploy/gcp/bootstrap_dev_foundation.sh <env-file>

What this script does:
  1. Loads the dev environment file.
  2. Enables the required GCP APIs.
  3. Provisions the recommended dev foundation:
     - Artifact Registry repository
     - VPC + subnet + Private Service Access range
     - Cloud SQL PostgreSQL instance, database, and app user
     - Secret Manager secrets for control-plane DB URL and worker token
     - Cloud Storage bucket
     - BigQuery dataset
     - Pub/Sub topics
     - Runtime and invoker service accounts
     - Baseline IAM bindings

Required env values:
  GCP_PROJECT_ID
  GCP_REGION
  GCP_ARTIFACT_REGISTRY_REPOSITORY
  GCP_RUN_NETWORK
  GCP_RUN_SUBNET
  GCP_SQL_INSTANCE
  GCP_SQL_DATABASE
  GCP_SQL_USER
  GCP_CLOUD_SQL_CONNECTION_NAME
  CONTROL_PLANE_DATABASE_URL_SECRET
  WORKER_SHARED_TOKEN_SECRET
  GCS_BUCKET_NAME
  IMPORT_COMMAND_TOPIC
  PREDICTION_COMMAND_TOPIC
  EXPORT_COMMAND_TOPIC
  PUBSUB_TOPIC_NAME

Optional env values:
  GCP_SQL_TIER
  GCP_SQL_STORAGE_SIZE_GB
  GCP_SQL_AVAILABILITY_TYPE
  GCP_SQL_DATABASE_VERSION
  GCP_BIGQUERY_DATASET_ID
  GCP_STORAGE_CLASS
  GCP_PRIVATE_SERVICE_RANGE_NAME
  GCP_PRIVATE_SERVICE_RANGE_PREFIX_LENGTH
  OPERATOR_API_SERVICE_ACCOUNT
  IMPORT_WORKER_SERVICE_ACCOUNT
  PREDICTION_WORKER_SERVICE_ACCOUNT
  EXPORT_WORKER_SERVICE_ACCOUNT
  SCHEDULER_WORKER_SERVICE_ACCOUNT
  PUBSUB_PUSH_INVOKER_SERVICE_ACCOUNT
  SCHEDULER_INVOKER_SERVICE_ACCOUNT

Notes:
  - This script is intentionally dev-only. It expects GCP_DEPLOYMENT_TIER=dev when set.
  - It supports the recommended Direct VPC egress path. If your org requires a pre-created VPC connector,
    bootstrap the network path separately and keep using deploy/gcp/deploy_cloud_run.sh for service deploys.
  - Secret values are generated only when the expected secret does not already have a latest version.
EOF
}

log() {
  printf '[bootstrap/gcp-dev] %s\n' "$*"
}

die() {
  printf '[bootstrap/gcp-dev] ERROR: %s\n' "$*" >&2
  exit 1
}

require_command() {
  command -v "$1" >/dev/null 2>&1 || die "Required command not found: $1"
}

require_env() {
  local name="$1"
  [[ -n "${!name:-}" ]] || die "Required env var is missing: $name"
}

load_env_file() {
  local env_file="$1"
  [[ -f "$env_file" ]] || die "Env file not found: $env_file"
  set -a
  # shellcheck disable=SC1090
  source "$env_file"
  set +a
}

default_service_account() {
  local service_name="$1"
  printf '%s@%s.iam.gserviceaccount.com' "$service_name" "$GCP_PROJECT_ID"
}

validate_same_project_service_account() {
  local env_name="$1"
  local email="$2"
  local expected_suffix="@${GCP_PROJECT_ID}.iam.gserviceaccount.com"

  [[ "$email" == *"${expected_suffix}" ]] || die "${env_name} must be a same-project service account ending with ${expected_suffix}"
}

service_account_local_name() {
  local email="$1"
  printf '%s' "${email%@*}"
}

secret_exists() {
  local secret_id="$1"
  gcloud secrets describe "$secret_id" --project="$GCP_PROJECT_ID" >/dev/null 2>&1
}

secret_has_latest_version() {
  local secret_id="$1"
  gcloud secrets versions access latest --secret "$secret_id" --project "$GCP_PROJECT_ID" >/dev/null 2>&1
}

create_or_update_secret() {
  local secret_id="$1"
  local value="$2"

  if secret_exists "$secret_id"; then
    printf '%s' "$value" | gcloud secrets versions add "$secret_id" --project "$GCP_PROJECT_ID" --data-file=- >/dev/null
    return
  fi

  printf '%s' "$value" | gcloud secrets create "$secret_id" --project "$GCP_PROJECT_ID" --replication-policy=automatic --data-file=- >/dev/null
}

read_secret_latest() {
  local secret_id="$1"
  gcloud secrets versions access latest --secret "$secret_id" --project "$GCP_PROJECT_ID"
}

generate_random_secret() {
  python3 - <<'PY'
import secrets
print(secrets.token_urlsafe(32))
PY
}

url_encode() {
  local raw_value="$1"
  python3 - "$raw_value" <<'PY'
import sys
from urllib.parse import quote
print(quote(sys.argv[1], safe=""))
PY
}

extract_db_password_from_url() {
  local database_url="$1"
  python3 - "$database_url" <<'PY'
import sys
from urllib.parse import unquote, urlparse

parsed = urlparse(sys.argv[1])
if parsed.password is None:
    raise SystemExit("Database URL secret does not contain a password.")
print(unquote(parsed.password))
PY
}

ensure_api_enabled() {
  local api_name="$1"
  gcloud services enable "$api_name" --project="$GCP_PROJECT_ID" >/dev/null
}

ensure_artifact_registry_repository() {
  if gcloud artifacts repositories describe "$GCP_ARTIFACT_REGISTRY_REPOSITORY" \
    --project="$GCP_PROJECT_ID" \
    --location="$GCP_REGION" >/dev/null 2>&1; then
    log "Artifact Registry repository already exists: $GCP_ARTIFACT_REGISTRY_REPOSITORY"
    return
  fi

  gcloud artifacts repositories create "$GCP_ARTIFACT_REGISTRY_REPOSITORY" \
    --project="$GCP_PROJECT_ID" \
    --location="$GCP_REGION" \
    --repository-format=docker \
    --description="KairyxAI dev images" \
    >/dev/null
}

ensure_network() {
  if ! gcloud compute networks describe "$GCP_RUN_NETWORK" --project="$GCP_PROJECT_ID" >/dev/null 2>&1; then
    gcloud compute networks create "$GCP_RUN_NETWORK" \
      --project="$GCP_PROJECT_ID" \
      --subnet-mode=custom \
      >/dev/null
  fi

  if ! gcloud compute networks subnets describe "$GCP_RUN_SUBNET" \
    --project="$GCP_PROJECT_ID" \
    --region="$GCP_REGION" >/dev/null 2>&1; then
    gcloud compute networks subnets create "$GCP_RUN_SUBNET" \
      --project="$GCP_PROJECT_ID" \
      --network="$GCP_RUN_NETWORK" \
      --region="$GCP_REGION" \
      --range="${GCP_RUN_SUBNET_RANGE:-10.20.0.0/28}" \
      >/dev/null
  fi
}

ensure_private_service_access() {
  local range_name="${GCP_PRIVATE_SERVICE_RANGE_NAME:-google-managed-services-${GCP_RUN_NETWORK}}"
  local prefix_length="${GCP_PRIVATE_SERVICE_RANGE_PREFIX_LENGTH:-16}"
  local peering_state

  if ! gcloud compute addresses describe "$range_name" \
    --project="$GCP_PROJECT_ID" \
    --global >/dev/null 2>&1; then
    gcloud compute addresses create "$range_name" \
      --project="$GCP_PROJECT_ID" \
      --global \
      --purpose=VPC_PEERING \
      --prefix-length="$prefix_length" \
      --network="$GCP_RUN_NETWORK" \
      >/dev/null
  fi

  peering_state="$(gcloud services vpc-peerings list \
    --project="$GCP_PROJECT_ID" \
    --network="$GCP_RUN_NETWORK" \
    --service=servicenetworking.googleapis.com \
    --format='value(state)' 2>/dev/null | head -n1 || true)"
  if [[ -n "$peering_state" ]]; then
    log "Private Service Access already configured for ${GCP_RUN_NETWORK}"
    return
  fi

  gcloud services vpc-peerings connect \
    --project="$GCP_PROJECT_ID" \
    --service=servicenetworking.googleapis.com \
    --ranges="$range_name" \
    --network="$GCP_RUN_NETWORK" \
    >/dev/null
}

ensure_cloud_sql_instance() {
  if gcloud sql instances describe "$GCP_SQL_INSTANCE" --project="$GCP_PROJECT_ID" >/dev/null 2>&1; then
    log "Cloud SQL instance already exists: $GCP_SQL_INSTANCE"
    return
  fi

  gcloud sql instances create "$GCP_SQL_INSTANCE" \
    --project="$GCP_PROJECT_ID" \
    --database-version="${GCP_SQL_DATABASE_VERSION:-POSTGRES_16}" \
    --tier="${GCP_SQL_TIER:-db-custom-2-8192}" \
    --region="$GCP_REGION" \
    --availability-type="${GCP_SQL_AVAILABILITY_TYPE:-ZONAL}" \
    --storage-type=SSD \
    --storage-size="${GCP_SQL_STORAGE_SIZE_GB:-50}" \
    --storage-auto-increase \
    --network="projects/${GCP_PROJECT_ID}/global/networks/${GCP_RUN_NETWORK}" \
    --no-assign-ip \
    --quiet \
    >/dev/null
}

ensure_cloud_sql_database() {
  if gcloud sql databases describe "$GCP_SQL_DATABASE" \
    --project="$GCP_PROJECT_ID" \
    --instance="$GCP_SQL_INSTANCE" >/dev/null 2>&1; then
    return
  fi

  gcloud sql databases create "$GCP_SQL_DATABASE" \
    --project="$GCP_PROJECT_ID" \
    --instance="$GCP_SQL_INSTANCE" \
    >/dev/null
}

sql_user_exists() {
  gcloud sql users list \
    --project="$GCP_PROJECT_ID" \
    --instance="$GCP_SQL_INSTANCE" \
    --format='value(name)' | grep -Fx "$GCP_SQL_USER" >/dev/null 2>&1
}

ensure_database_url_secret() {
  local db_auth_value=""
  local db_auth_value_encoded=""
  local database_url=""

  if secret_has_latest_version "$CONTROL_PLANE_DATABASE_URL_SECRET"; then
    database_url="$(read_secret_latest "$CONTROL_PLANE_DATABASE_URL_SECRET")"
    db_auth_value="$(extract_db_password_from_url "$database_url")"
    if sql_user_exists; then
      gcloud sql users set-password "$GCP_SQL_USER" \
        --project="$GCP_PROJECT_ID" \
        --instance="$GCP_SQL_INSTANCE" \
        --password "$db_auth_value" \
        >/dev/null
    else
      gcloud sql users create "$GCP_SQL_USER" \
        --project="$GCP_PROJECT_ID" \
        --instance="$GCP_SQL_INSTANCE" \
        --password "$db_auth_value" \
        >/dev/null
    fi
    return
  fi

  db_auth_value="$(generate_random_secret)"
  if sql_user_exists; then
    gcloud sql users set-password "$GCP_SQL_USER" \
      --project="$GCP_PROJECT_ID" \
      --instance="$GCP_SQL_INSTANCE" \
      --password "$db_auth_value" \
      >/dev/null
  else
    gcloud sql users create "$GCP_SQL_USER" \
      --project="$GCP_PROJECT_ID" \
      --instance="$GCP_SQL_INSTANCE" \
      --password "$db_auth_value" \
      >/dev/null
  fi

  db_auth_value_encoded="$(url_encode "$db_auth_value")"
  database_url="postgresql+psycopg://${GCP_SQL_USER}:${db_auth_value_encoded}@/${GCP_SQL_DATABASE}?host=/cloudsql/${GCP_CLOUD_SQL_CONNECTION_NAME}"
  create_or_update_secret "$CONTROL_PLANE_DATABASE_URL_SECRET" "$database_url"
}

ensure_worker_shared_token_secret() {
  local worker_auth_value=""
  if secret_has_latest_version "$WORKER_SHARED_TOKEN_SECRET"; then
    return
  fi

  worker_auth_value="$(generate_random_secret)"
  create_or_update_secret "$WORKER_SHARED_TOKEN_SECRET" "$worker_auth_value"
}

ensure_bucket() {
  if gcloud storage buckets describe "gs://${GCS_BUCKET_NAME}" >/dev/null 2>&1; then
    return
  fi

  gcloud storage buckets create "gs://${GCS_BUCKET_NAME}" \
    --project="$GCP_PROJECT_ID" \
    --location="$GCP_REGION" \
    --default-storage-class="${GCP_STORAGE_CLASS:-STANDARD}" \
    --uniform-bucket-level-access \
    >/dev/null
}

ensure_bigquery_dataset() {
  local dataset_id="${GCP_BIGQUERY_DATASET_ID:-kairyx_platform}"
  if bq --project_id="$GCP_PROJECT_ID" show --dataset "${GCP_PROJECT_ID}:${dataset_id}" >/dev/null 2>&1; then
    return
  fi

  bq --project_id="$GCP_PROJECT_ID" --location="$GCP_REGION" mk \
    --dataset \
    --label=environment:dev \
    --label=product:kairyx \
    "${GCP_PROJECT_ID}:${dataset_id}" \
    >/dev/null
}

ensure_topic() {
  local topic_name="$1"
  if gcloud pubsub topics describe "$topic_name" --project="$GCP_PROJECT_ID" >/dev/null 2>&1; then
    return
  fi

  gcloud pubsub topics create "$topic_name" --project="$GCP_PROJECT_ID" >/dev/null
}

ensure_service_account() {
  local service_account_name="$1"
  if gcloud iam service-accounts describe "${service_account_name}@${GCP_PROJECT_ID}.iam.gserviceaccount.com" \
    --project="$GCP_PROJECT_ID" >/dev/null 2>&1; then
    return
  fi

  gcloud iam service-accounts create "$service_account_name" \
    --project="$GCP_PROJECT_ID" \
    --display-name="$service_account_name" \
    >/dev/null
}

ensure_project_binding() {
  local member="$1"
  local role="$2"
  gcloud projects add-iam-policy-binding "$GCP_PROJECT_ID" \
    --member="$member" \
    --role="$role" \
    --quiet \
    >/dev/null
}

ensure_bucket_binding() {
  local member="$1"
  local role="$2"
  gcloud storage buckets add-iam-policy-binding "gs://${GCS_BUCKET_NAME}" \
    --member="$member" \
    --role="$role" \
    >/dev/null
}

ensure_pubsub_push_token_creator_binding() {
  local project_number="$1"
  local push_invoker_email="$2"
  gcloud iam service-accounts add-iam-policy-binding "$push_invoker_email" \
    --project="$GCP_PROJECT_ID" \
    --member="serviceAccount:service-${project_number}@gcp-sa-pubsub.iam.gserviceaccount.com" \
    --role="roles/iam.serviceAccountTokenCreator" \
    >/dev/null
}

grant_runtime_permissions() {
  local service_role="$1"
  local service_account_email="$2"
  local member="serviceAccount:${service_account_email}"

  ensure_project_binding "$member" "roles/cloudsql.client"
  ensure_project_binding "$member" "roles/secretmanager.secretAccessor"

  case "$service_role" in
    operator-api)
      ensure_project_binding "$member" "roles/pubsub.publisher"
      ensure_project_binding "$member" "roles/bigquery.jobUser"
      ensure_project_binding "$member" "roles/bigquery.dataEditor"
      ensure_bucket_binding "$member" "roles/storage.objectAdmin"
      ;;
    import-worker)
      ensure_project_binding "$member" "roles/pubsub.publisher"
      ensure_project_binding "$member" "roles/bigquery.jobUser"
      ensure_project_binding "$member" "roles/bigquery.dataEditor"
      ensure_bucket_binding "$member" "roles/storage.objectAdmin"
      ;;
    prediction-worker)
      ensure_project_binding "$member" "roles/bigquery.jobUser"
      ensure_project_binding "$member" "roles/bigquery.dataEditor"
      ;;
    export-worker)
      ensure_project_binding "$member" "roles/bigquery.jobUser"
      ensure_project_binding "$member" "roles/bigquery.dataViewer"
      ensure_bucket_binding "$member" "roles/storage.objectAdmin"
      ;;
    scheduler-worker)
      ;;
    *)
      die "Unsupported service role for IAM grants: ${service_role}"
      ;;
  esac
}

validate_configuration() {
  [[ -z "${GCP_DEPLOYMENT_TIER:-}" || "${GCP_DEPLOYMENT_TIER}" == "dev" ]] || die "bootstrap_dev_foundation.sh only supports GCP_DEPLOYMENT_TIER=dev"
  require_env GCP_PROJECT_ID
  require_env GCP_REGION
  require_env GCP_ARTIFACT_REGISTRY_REPOSITORY
  require_env GCP_RUN_NETWORK
  require_env GCP_RUN_SUBNET
  require_env GCP_SQL_INSTANCE
  require_env GCP_SQL_DATABASE
  require_env GCP_SQL_USER
  require_env GCP_CLOUD_SQL_CONNECTION_NAME
  require_env CONTROL_PLANE_DATABASE_URL_SECRET
  require_env WORKER_SHARED_TOKEN_SECRET
  require_env GCS_BUCKET_NAME
  require_env IMPORT_COMMAND_TOPIC
  require_env PREDICTION_COMMAND_TOPIC
  require_env EXPORT_COMMAND_TOPIC
  require_env PUBSUB_TOPIC_NAME
}

main() {
  local env_file="${1:-}"
  local project_number=""
  local sa_name=""
  local sa_email=""
  local operator_api_email=""
  local import_worker_email=""
  local prediction_worker_email=""
  local export_worker_email=""
  local scheduler_worker_email=""
  local pubsub_push_invoker_email=""
  local scheduler_invoker_email=""

  if [[ -z "$env_file" || "$env_file" == "-h" || "$env_file" == "--help" ]]; then
    usage
    exit 0
  fi

  require_command gcloud
  require_command bq
  require_command python3

  ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
  cd "$ROOT_DIR"

  load_env_file "$env_file"
  validate_configuration

  gcloud config set project "$GCP_PROJECT_ID" >/dev/null

  operator_api_email="${OPERATOR_API_SERVICE_ACCOUNT:-$(default_service_account operator-api)}"
  import_worker_email="${IMPORT_WORKER_SERVICE_ACCOUNT:-$(default_service_account import-worker)}"
  prediction_worker_email="${PREDICTION_WORKER_SERVICE_ACCOUNT:-$(default_service_account prediction-worker)}"
  export_worker_email="${EXPORT_WORKER_SERVICE_ACCOUNT:-$(default_service_account export-worker)}"
  scheduler_worker_email="${SCHEDULER_WORKER_SERVICE_ACCOUNT:-$(default_service_account scheduler-worker)}"
  pubsub_push_invoker_email="${PUBSUB_PUSH_INVOKER_SERVICE_ACCOUNT:-$(default_service_account pubsub-push-invoker)}"
  scheduler_invoker_email="${SCHEDULER_INVOKER_SERVICE_ACCOUNT:-$(default_service_account scheduler-invoker)}"

  validate_same_project_service_account OPERATOR_API_SERVICE_ACCOUNT "$operator_api_email"
  validate_same_project_service_account IMPORT_WORKER_SERVICE_ACCOUNT "$import_worker_email"
  validate_same_project_service_account PREDICTION_WORKER_SERVICE_ACCOUNT "$prediction_worker_email"
  validate_same_project_service_account EXPORT_WORKER_SERVICE_ACCOUNT "$export_worker_email"
  validate_same_project_service_account SCHEDULER_WORKER_SERVICE_ACCOUNT "$scheduler_worker_email"
  validate_same_project_service_account PUBSUB_PUSH_INVOKER_SERVICE_ACCOUNT "$pubsub_push_invoker_email"
  validate_same_project_service_account SCHEDULER_INVOKER_SERVICE_ACCOUNT "$scheduler_invoker_email"

  log "Enabling required APIs"
  for api in \
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
    servicenetworking.googleapis.com; do
    ensure_api_enabled "$api"
  done

  log "Ensuring Artifact Registry repository"
  ensure_artifact_registry_repository

  log "Ensuring VPC, subnet, and Private Service Access"
  ensure_network
  ensure_private_service_access

  log "Ensuring Cloud SQL instance, database, and application user"
  ensure_cloud_sql_instance
  ensure_cloud_sql_database
  ensure_database_url_secret
  ensure_worker_shared_token_secret

  log "Ensuring bucket, dataset, and Pub/Sub topics"
  ensure_bucket
  ensure_bigquery_dataset
  ensure_topic "$PUBSUB_TOPIC_NAME"
  ensure_topic "$IMPORT_COMMAND_TOPIC"
  ensure_topic "$PREDICTION_COMMAND_TOPIC"
  ensure_topic "$EXPORT_COMMAND_TOPIC"

  log "Ensuring service accounts"
  for sa_email in \
    "$operator_api_email" \
    "$import_worker_email" \
    "$prediction_worker_email" \
    "$export_worker_email" \
    "$scheduler_worker_email" \
    "$pubsub_push_invoker_email" \
    "$scheduler_invoker_email"; do
    sa_name="$(service_account_local_name "$sa_email")"
    ensure_service_account "$sa_name"
  done

  project_number="$(gcloud projects describe "$GCP_PROJECT_ID" --format='value(projectNumber)')"

  log "Applying baseline IAM bindings"
  grant_runtime_permissions "operator-api" "$operator_api_email"
  grant_runtime_permissions "import-worker" "$import_worker_email"
  grant_runtime_permissions "prediction-worker" "$prediction_worker_email"
  grant_runtime_permissions "export-worker" "$export_worker_email"
  grant_runtime_permissions "scheduler-worker" "$scheduler_worker_email"

  ensure_pubsub_push_token_creator_binding "$project_number" "$pubsub_push_invoker_email"

  cat <<EOF

Dev foundation bootstrap finished.

Provisioned or verified:
  - Artifact Registry repository: ${GCP_ARTIFACT_REGISTRY_REPOSITORY}
  - VPC/subnet: ${GCP_RUN_NETWORK} / ${GCP_RUN_SUBNET}
  - Cloud SQL: ${GCP_SQL_INSTANCE}
  - Cloud Storage bucket: ${GCS_BUCKET_NAME}
  - BigQuery dataset: ${GCP_BIGQUERY_DATASET_ID:-kairyx_platform}
  - Pub/Sub topics:
      * ${PUBSUB_TOPIC_NAME}
      * ${IMPORT_COMMAND_TOPIC}
      * ${PREDICTION_COMMAND_TOPIC}
      * ${EXPORT_COMMAND_TOPIC}
  - Secret Manager secrets:
      * ${CONTROL_PLANE_DATABASE_URL_SECRET}
      * ${WORKER_SHARED_TOKEN_SECRET}

Next steps:
  1. Copy the template if needed:
       cp deploy/gcp/dev.env.example deploy/gcp/dev.env
  2. Fill any remaining placeholder values in deploy/gcp/dev.env.
  3. Deploy the runtime:
       bash deploy/gcp/deploy_cloud_run.sh deploy/gcp/dev.env
  4. Configure Pub/Sub push and Cloud Scheduler:
       bash deploy/gcp/configure_dev_eventing.sh deploy/gcp/dev.env

Notes:
  - This script does not write real secret values into git.
  - Event wiring is intentionally separate so rerunning foundation bootstrap does not rotate callback tokens or recreate subscriptions.
EOF
}

main "$@"
