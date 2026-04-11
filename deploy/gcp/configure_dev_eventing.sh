#!/usr/bin/env bash

set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  deploy/gcp/configure_dev_eventing.sh <env-file>
  deploy/gcp/configure_dev_eventing.sh --validate-only <env-file>

What this script does:
  1. Loads the dev environment file.
  2. Resolves the deployed Cloud Run worker URLs.
  3. Grants service-level run.invoker to the dedicated Pub/Sub and Scheduler caller identities.
  4. Creates or updates Pub/Sub push subscriptions for import, prediction, and export workers.
  5. Creates or updates the Cloud Scheduler HTTP job for scheduler-worker.

Required env values:
  GCP_PROJECT_ID
  GCP_REGION
  WORKER_SHARED_TOKEN_SECRET
  IMPORT_COMMAND_TOPIC
  PREDICTION_COMMAND_TOPIC
  EXPORT_COMMAND_TOPIC

Optional env values:
  GCP_DEPLOYMENT_TIER
  GCP_SERVICE_PREFIX
  IMPORT_COMMAND_SUBSCRIPTION
  PREDICTION_COMMAND_SUBSCRIPTION
  EXPORT_COMMAND_SUBSCRIPTION
  SCHEDULER_JOB_NAME
  GCP_SCHEDULER_CRON
  GCP_SCHEDULER_TIME_ZONE
  PUBSUB_PUSH_INVOKER_SERVICE_ACCOUNT
  SCHEDULER_INVOKER_SERVICE_ACCOUNT

Notes:
  - This script is intentionally dev-only.
  - It reads the latest worker shared token from Secret Manager in order to configure callback URLs.
  - It does not print the token or the full callback URLs, but the token remains visible to operators who can read the resulting Pub/Sub subscription or Scheduler job configuration.
EOF
}

log() {
  printf '[configure/gcp-dev-eventing] %s\n' "$*"
}

die() {
  printf '[configure/gcp-dev-eventing] ERROR: %s\n' "$*" >&2
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

normalize_service_prefix() {
  local prefix="${GCP_SERVICE_PREFIX:-}"
  if [[ -z "$prefix" ]]; then
    printf ''
    return
  fi
  if [[ "$prefix" == *- ]]; then
    printf '%s' "$prefix"
    return
  fi
  printf '%s-' "$prefix"
}

service_name_for_role() {
  local service_role="$1"
  printf '%s%s' "${SERVICE_PREFIX}" "${service_role}"
}

ensure_service_invoker() {
  local service_name="$1"
  local member="$2"
  gcloud run services add-iam-policy-binding "$service_name" \
    --project="$GCP_PROJECT_ID" \
    --region="$GCP_REGION" \
    --member="$member" \
    --role="roles/run.invoker" \
    --quiet \
    >/dev/null
}

ensure_pubsub_token_creator_binding() {
  local project_number="$1"
  local push_invoker_email="$2"
  gcloud iam service-accounts add-iam-policy-binding "$push_invoker_email" \
    --project="$GCP_PROJECT_ID" \
    --member="serviceAccount:service-${project_number}@gcp-sa-pubsub.iam.gserviceaccount.com" \
    --role="roles/iam.serviceAccountTokenCreator" \
    >/dev/null
}

service_url() {
  local service_name="$1"
  gcloud run services describe "$service_name" \
    --project="$GCP_PROJECT_ID" \
    --region="$GCP_REGION" \
    --format='value(status.url)'
}

read_worker_auth_value() {
  gcloud secrets versions access latest \
    --project "$GCP_PROJECT_ID" \
    --secret "$WORKER_SHARED_TOKEN_SECRET"
}

ensure_push_subscription() {
  local topic_name="$1"
  local subscription_name="$2"
  local worker_url="$3"
  local push_invoker_email="$4"
  local worker_auth_value="$5"
  local expected_topic="projects/${GCP_PROJECT_ID}/topics/${topic_name}"
  local current_topic=""

  if gcloud pubsub subscriptions describe "$subscription_name" --project="$GCP_PROJECT_ID" >/dev/null 2>&1; then
    current_topic="$(gcloud pubsub subscriptions describe "$subscription_name" --project="$GCP_PROJECT_ID" --format='value(topic)')"
    [[ "$current_topic" == "$expected_topic" ]] || die "Subscription ${subscription_name} already exists but points at ${current_topic}. Delete and recreate it, or choose a new subscription name for topic ${expected_topic}."
    gcloud pubsub subscriptions update "$subscription_name" \
      --project="$GCP_PROJECT_ID" \
      --push-endpoint="${worker_url}/pubsub/push?token=${worker_auth_value}" \
      --push-auth-service-account="$push_invoker_email" \
      --push-auth-token-audience="$worker_url" \
      --ack-deadline=600 \
      --min-retry-delay=10s \
      --max-retry-delay=600s \
      >/dev/null
    return
  fi

  gcloud pubsub subscriptions create "$subscription_name" \
    --project="$GCP_PROJECT_ID" \
    --topic="$topic_name" \
    --push-endpoint="${worker_url}/pubsub/push?token=${worker_auth_value}" \
    --push-auth-service-account="$push_invoker_email" \
    --push-auth-token-audience="$worker_url" \
    --ack-deadline=600 \
    --min-retry-delay=10s \
    --max-retry-delay=600s \
    >/dev/null
}

ensure_scheduler_job() {
  local job_name="$1"
  local scheduler_url="$2"
  local scheduler_invoker_email="$3"
  local worker_auth_value="$4"
  local cron="${GCP_SCHEDULER_CRON:-* * * * *}"
  local timezone="${GCP_SCHEDULER_TIME_ZONE:-UTC}"

  if gcloud scheduler jobs describe "$job_name" \
    --project="$GCP_PROJECT_ID" \
    --location="$GCP_REGION" >/dev/null 2>&1; then
    gcloud scheduler jobs update http "$job_name" \
      --project="$GCP_PROJECT_ID" \
      --location="$GCP_REGION" \
      --schedule="$cron" \
      --time-zone="$timezone" \
      --uri="${scheduler_url}/run?token=${worker_auth_value}" \
      --http-method=POST \
      --oidc-service-account-email="$scheduler_invoker_email" \
      --oidc-token-audience="$scheduler_url" \
      >/dev/null
    return
  fi

  gcloud scheduler jobs create http "$job_name" \
    --project="$GCP_PROJECT_ID" \
    --location="$GCP_REGION" \
    --schedule="$cron" \
    --time-zone="$timezone" \
    --uri="${scheduler_url}/run?token=${worker_auth_value}" \
    --http-method=POST \
    --oidc-service-account-email="$scheduler_invoker_email" \
    --oidc-token-audience="$scheduler_url" \
    >/dev/null
}

validate_configuration() {
  [[ -z "${GCP_DEPLOYMENT_TIER:-}" || "${GCP_DEPLOYMENT_TIER}" == "dev" ]] || die "configure_dev_eventing.sh only supports GCP_DEPLOYMENT_TIER=dev"
  require_env GCP_PROJECT_ID
  require_env GCP_REGION
  require_env WORKER_SHARED_TOKEN_SECRET
  require_env IMPORT_COMMAND_TOPIC
  require_env PREDICTION_COMMAND_TOPIC
  require_env EXPORT_COMMAND_TOPIC
}

main() {
  local validate_only=0
  local env_file="${1:-}"
  local project_number=""
  local worker_auth_value=""
  local push_invoker_email=""
  local scheduler_invoker_email=""
  local import_service=""
  local prediction_service=""
  local export_service=""
  local scheduler_service=""
  local import_url=""
  local prediction_url=""
  local export_url=""
  local scheduler_url=""
  local import_subscription=""
  local prediction_subscription=""
  local export_subscription=""
  local scheduler_job=""

  if [[ "$env_file" == "--validate-only" ]]; then
    validate_only=1
    env_file="${2:-}"
  fi

  if [[ -z "$env_file" || "$env_file" == "-h" || "$env_file" == "--help" ]]; then
    usage
    exit 0
  fi

  ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
  cd "$ROOT_DIR"

  load_env_file "$env_file"
  validate_configuration
  if [[ "$validate_only" -eq 1 ]]; then
    log "Validated dev eventing env contract."
    return 0
  fi

  require_command gcloud
  SERVICE_PREFIX="$(normalize_service_prefix)"

  gcloud config set project "$GCP_PROJECT_ID" >/dev/null

  push_invoker_email="${PUBSUB_PUSH_INVOKER_SERVICE_ACCOUNT:-$(default_service_account pubsub-push-invoker)}"
  scheduler_invoker_email="${SCHEDULER_INVOKER_SERVICE_ACCOUNT:-$(default_service_account scheduler-invoker)}"
  validate_same_project_service_account PUBSUB_PUSH_INVOKER_SERVICE_ACCOUNT "$push_invoker_email"
  validate_same_project_service_account SCHEDULER_INVOKER_SERVICE_ACCOUNT "$scheduler_invoker_email"
  import_service="$(service_name_for_role import-worker)"
  prediction_service="$(service_name_for_role prediction-worker)"
  export_service="$(service_name_for_role export-worker)"
  scheduler_service="$(service_name_for_role scheduler-worker)"
  import_subscription="${IMPORT_COMMAND_SUBSCRIPTION:-${import_service}-push}"
  prediction_subscription="${PREDICTION_COMMAND_SUBSCRIPTION:-${prediction_service}-push}"
  export_subscription="${EXPORT_COMMAND_SUBSCRIPTION:-${export_service}-push}"
  scheduler_job="${SCHEDULER_JOB_NAME:-${scheduler_service}}"

  project_number="$(gcloud projects describe "$GCP_PROJECT_ID" --format='value(projectNumber)')"
  worker_auth_value="$(read_worker_auth_value)"

  import_url="$(service_url "$import_service")"
  prediction_url="$(service_url "$prediction_service")"
  export_url="$(service_url "$export_service")"
  scheduler_url="$(service_url "$scheduler_service")"

  log "Applying service-level run.invoker bindings"
  ensure_service_invoker "$import_service" "serviceAccount:${push_invoker_email}"
  ensure_service_invoker "$prediction_service" "serviceAccount:${push_invoker_email}"
  ensure_service_invoker "$export_service" "serviceAccount:${push_invoker_email}"
  ensure_service_invoker "$scheduler_service" "serviceAccount:${scheduler_invoker_email}"
  ensure_pubsub_token_creator_binding "$project_number" "$push_invoker_email"

  log "Creating or updating Pub/Sub push subscriptions"
  ensure_push_subscription "$IMPORT_COMMAND_TOPIC" "$import_subscription" "$import_url" "$push_invoker_email" "$worker_auth_value"
  ensure_push_subscription "$PREDICTION_COMMAND_TOPIC" "$prediction_subscription" "$prediction_url" "$push_invoker_email" "$worker_auth_value"
  ensure_push_subscription "$EXPORT_COMMAND_TOPIC" "$export_subscription" "$export_url" "$push_invoker_email" "$worker_auth_value"

  log "Creating or updating Cloud Scheduler job"
  ensure_scheduler_job "$scheduler_job" "$scheduler_url" "$scheduler_invoker_email" "$worker_auth_value"

  cat <<EOF

Dev eventing configuration finished.

Configured:
  - Pub/Sub push subscriptions:
      * ${import_subscription}
      * ${prediction_subscription}
      * ${export_subscription}
  - Cloud Scheduler job:
      * ${scheduler_job}

Cloud Run services targeted:
  - ${import_service}
  - ${prediction_service}
  - ${export_service}
  - ${scheduler_service}

Notes:
  - Callback URLs use the latest value from ${WORKER_SHARED_TOKEN_SECRET}, but the token itself is never printed by this script.
  - Anyone who can inspect the resulting Pub/Sub subscription or Cloud Scheduler job can still read the token from the stored URI configuration.
  - Re-running this script updates callback and auth settings in place only when the subscription names already point at the expected topics.
EOF
}

main "$@"
