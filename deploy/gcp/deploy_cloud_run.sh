#!/usr/bin/env bash

set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  deploy/gcp/deploy_cloud_run.sh <env-file>

What this script does:
  1. Loads deployment configuration from the provided env file.
  2. Builds the repo-root Docker image.
  3. Pushes the image to Artifact Registry.
  4. Resolves the pushed tag to an immutable digest.
  5. Deploys operator-api, import-worker, prediction-worker, export-worker, and scheduler-worker to Cloud Run.

Required env file values:
  GCP_PROJECT_ID
  GCP_REGION
  GCP_ARTIFACT_REGISTRY_REPOSITORY
  GCP_IMAGE_NAME
  GCP_RELEASE_TAG
  GCP_CLOUD_SQL_CONNECTION_NAME
  CONTROL_PLANE_DATABASE_URL_SECRET
  CONTROL_PLANE_SECRET_KEY_SECRET
  WORKER_SHARED_TOKEN_SECRET
  CORS_ALLOWED_ORIGINS
  OIDC_ISSUER
  OIDC_AUDIENCE
  OIDC_JWKS_URL
  OIDC_CLIENT_ID
  OIDC_AUTHORIZE_URL
  OIDC_TOKEN_URL
  GCS_BUCKET_NAME

Optional deployment values:
  GCP_DEPLOYMENT_TIER
  GCP_SERVICE_PREFIX
  GCP_SECRET_PROJECT_ID
  GOOGLE_OIDC_CLIENT_ID
  GOOGLE_OIDC_HOSTED_DOMAIN
  OIDC_LOGOUT_URL
  API_ACCESS_KEY_SECRET
  OIDC_JWT_SIGNING_SECRET_SECRET
  GCP_RUN_NETWORK
  GCP_RUN_SUBNET
  GCP_VPC_CONNECTOR
  GCP_VPC_EGRESS
  OPERATOR_API_SERVICE_ACCOUNT
  IMPORT_WORKER_SERVICE_ACCOUNT
  PREDICTION_WORKER_SERVICE_ACCOUNT
  EXPORT_WORKER_SERVICE_ACCOUNT
  SCHEDULER_WORKER_SERVICE_ACCOUNT
  IMPORT_COMMAND_TOPIC
  PREDICTION_COMMAND_TOPIC
  EXPORT_COMMAND_TOPIC
  PUBSUB_TOPIC_NAME
  BOOTSTRAP_TENANT_ID
  BOOTSTRAP_TENANT_NAME
  BOOTSTRAP_PROJECT_ID
  BOOTSTRAP_PROJECT_NAME
  GCP_EXTRA_ENV_FILE

Notes:
  - The script expects Cloud Run-injected secrets to live in the same GCP project as the deploy target.
  - Use GCP_RUN_NETWORK + GCP_RUN_SUBNET for Direct VPC egress, or GCP_VPC_CONNECTOR for Serverless VPC Access.
  - GCP_DEPLOYMENT_TIER defaults to prod. Use dev or qa for lighter internal-test sizing.
  - GCP_SERVICE_PREFIX lets one project host multiple KairyxAI environments, for example dev-operator-api and qa-operator-api.
  - For a fresh internal dev project, run deploy/gcp/bootstrap_dev_foundation.sh before the first deploy.
  - For the dev tier, run deploy/gcp/configure_dev_eventing.sh after deploy to wire Pub/Sub push subscriptions and Cloud Scheduler.
EOF
}

log() {
  printf '[deploy/gcp] %s\n' "$*"
}

die() {
  printf '[deploy/gcp] ERROR: %s\n' "$*" >&2
  exit 1
}

require_command() {
  command -v "$1" >/dev/null 2>&1 || die "Required command not found: $1"
}

require_env() {
  local name="$1"
  [[ -n "${!name:-}" ]] || die "Required env var is missing: $name"
}

safe_value() {
  local value="${1:-}"
  value="${value//\'/\'\'}"
  printf "'%s'" "$value"
}

env_file_line() {
  local key="$1"
  local value="${2:-}"
  printf '%s: %s\n' "$key" "$(safe_value "$value")"
}

default_service_account() {
  local service_name="$1"
  printf '%s@%s.iam.gserviceaccount.com' "$service_name" "$GCP_PROJECT_ID"
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

resolve_deployment_tier() {
  local tier="${GCP_DEPLOYMENT_TIER:-prod}"
  case "$tier" in
    prod|qa|dev)
      printf '%s' "$tier"
      ;;
    *)
      die "GCP_DEPLOYMENT_TIER must be one of: prod, qa, dev"
      ;;
  esac
}

service_name_for_role() {
  local service_role="$1"
  printf '%s%s' "${SERVICE_PREFIX}" "${service_role}"
}

tier_setting() {
  local service_role="$1"
  local field="$2"

  case "${DEPLOYMENT_TIER}:${service_role}:${field}" in
    prod:operator-api:cpu) printf '2' ;;
    prod:operator-api:memory) printf '4Gi' ;;
    prod:operator-api:concurrency) printf '40' ;;
    prod:operator-api:min) printf '2' ;;
    prod:operator-api:max) printf '20' ;;
    prod:operator-api:timeout) printf '300' ;;

    prod:import-worker:cpu) printf '2' ;;
    prod:import-worker:memory) printf '4Gi' ;;
    prod:import-worker:concurrency) printf '1' ;;
    prod:import-worker:min) printf '1' ;;
    prod:import-worker:max) printf '20' ;;
    prod:import-worker:timeout) printf '3600' ;;

    prod:prediction-worker:cpu) printf '2' ;;
    prod:prediction-worker:memory) printf '8Gi' ;;
    prod:prediction-worker:concurrency) printf '1' ;;
    prod:prediction-worker:min) printf '0' ;;
    prod:prediction-worker:max) printf '10' ;;
    prod:prediction-worker:timeout) printf '3600' ;;

    prod:export-worker:cpu) printf '2' ;;
    prod:export-worker:memory) printf '4Gi' ;;
    prod:export-worker:concurrency) printf '1' ;;
    prod:export-worker:min) printf '0' ;;
    prod:export-worker:max) printf '20' ;;
    prod:export-worker:timeout) printf '1800' ;;

    prod:scheduler-worker:cpu) printf '1' ;;
    prod:scheduler-worker:memory) printf '1Gi' ;;
    prod:scheduler-worker:concurrency) printf '1' ;;
    prod:scheduler-worker:min) printf '1' ;;
    prod:scheduler-worker:max) printf '2' ;;
    prod:scheduler-worker:timeout) printf '300' ;;

    qa:operator-api:cpu) printf '1' ;;
    qa:operator-api:memory) printf '2Gi' ;;
    qa:operator-api:concurrency) printf '20' ;;
    qa:operator-api:min) printf '1' ;;
    qa:operator-api:max) printf '5' ;;
    qa:operator-api:timeout) printf '300' ;;

    qa:import-worker:cpu) printf '1' ;;
    qa:import-worker:memory) printf '2Gi' ;;
    qa:import-worker:concurrency) printf '1' ;;
    qa:import-worker:min) printf '0' ;;
    qa:import-worker:max) printf '5' ;;
    qa:import-worker:timeout) printf '3600' ;;

    qa:prediction-worker:cpu) printf '1' ;;
    qa:prediction-worker:memory) printf '4Gi' ;;
    qa:prediction-worker:concurrency) printf '1' ;;
    qa:prediction-worker:min) printf '0' ;;
    qa:prediction-worker:max) printf '4' ;;
    qa:prediction-worker:timeout) printf '3600' ;;

    qa:export-worker:cpu) printf '1' ;;
    qa:export-worker:memory) printf '2Gi' ;;
    qa:export-worker:concurrency) printf '1' ;;
    qa:export-worker:min) printf '0' ;;
    qa:export-worker:max) printf '5' ;;
    qa:export-worker:timeout) printf '1800' ;;

    qa:scheduler-worker:cpu) printf '1' ;;
    qa:scheduler-worker:memory) printf '1Gi' ;;
    qa:scheduler-worker:concurrency) printf '1' ;;
    qa:scheduler-worker:min) printf '0' ;;
    qa:scheduler-worker:max) printf '1' ;;
    qa:scheduler-worker:timeout) printf '300' ;;

    dev:operator-api:cpu) printf '1' ;;
    dev:operator-api:memory) printf '1Gi' ;;
    dev:operator-api:concurrency) printf '10' ;;
    dev:operator-api:min) printf '0' ;;
    dev:operator-api:max) printf '3' ;;
    dev:operator-api:timeout) printf '300' ;;

    dev:import-worker:cpu) printf '1' ;;
    dev:import-worker:memory) printf '1Gi' ;;
    dev:import-worker:concurrency) printf '1' ;;
    dev:import-worker:min) printf '0' ;;
    dev:import-worker:max) printf '3' ;;
    dev:import-worker:timeout) printf '3600' ;;

    dev:prediction-worker:cpu) printf '1' ;;
    dev:prediction-worker:memory) printf '2Gi' ;;
    dev:prediction-worker:concurrency) printf '1' ;;
    dev:prediction-worker:min) printf '0' ;;
    dev:prediction-worker:max) printf '2' ;;
    dev:prediction-worker:timeout) printf '3600' ;;

    dev:export-worker:cpu) printf '1' ;;
    dev:export-worker:memory) printf '1Gi' ;;
    dev:export-worker:concurrency) printf '1' ;;
    dev:export-worker:min) printf '0' ;;
    dev:export-worker:max) printf '3' ;;
    dev:export-worker:timeout) printf '1800' ;;

    dev:scheduler-worker:cpu) printf '1' ;;
    dev:scheduler-worker:memory) printf '1Gi' ;;
    dev:scheduler-worker:concurrency) printf '1' ;;
    dev:scheduler-worker:min) printf '0' ;;
    dev:scheduler-worker:max) printf '1' ;;
    dev:scheduler-worker:timeout) printf '300' ;;

    *)
      die "No deployment tier setting found for ${DEPLOYMENT_TIER}/${service_role}/${field}"
      ;;
  esac
}

configure_network_flags() {
  NETWORK_FLAGS=()
  local vpc_egress="${GCP_VPC_EGRESS:-private-ranges-only}"
  if [[ -n "${GCP_RUN_NETWORK:-}" || -n "${GCP_RUN_SUBNET:-}" ]]; then
    [[ -n "${GCP_RUN_NETWORK:-}" ]] || die "GCP_RUN_NETWORK is required when GCP_RUN_SUBNET is set."
    [[ -n "${GCP_RUN_SUBNET:-}" ]] || die "GCP_RUN_SUBNET is required when GCP_RUN_NETWORK is set."
    NETWORK_FLAGS=(
      "--network=${GCP_RUN_NETWORK}"
      "--subnet=${GCP_RUN_SUBNET}"
      "--vpc-egress=${vpc_egress}"
    )
    return
  fi
  if [[ -n "${GCP_VPC_CONNECTOR:-}" ]]; then
    NETWORK_FLAGS=(
      "--vpc-connector=${GCP_VPC_CONNECTOR}"
      "--vpc-egress=${vpc_egress}"
    )
  fi
}

load_env_file() {
  local env_file="$1"
  [[ -f "$env_file" ]] || die "Env file not found: $env_file"
  set -a
  # shellcheck disable=SC1090
  source "$env_file"
  set +a
}

validate_configuration() {
  require_env GCP_PROJECT_ID
  require_env GCP_REGION
  require_env GCP_ARTIFACT_REGISTRY_REPOSITORY
  require_env GCP_IMAGE_NAME
  require_env GCP_RELEASE_TAG
  require_env GCP_CLOUD_SQL_CONNECTION_NAME
  require_env CONTROL_PLANE_DATABASE_URL_SECRET
  require_env CONTROL_PLANE_SECRET_KEY_SECRET
  require_env WORKER_SHARED_TOKEN_SECRET
  require_env CORS_ALLOWED_ORIGINS
  require_env OIDC_ISSUER
  require_env OIDC_AUDIENCE
  require_env OIDC_JWKS_URL
  require_env OIDC_CLIENT_ID
  require_env OIDC_AUTHORIZE_URL
  require_env OIDC_TOKEN_URL
  require_env GCS_BUCKET_NAME
}

build_image() {
  local registry_host="${GCP_REGION}-docker.pkg.dev"
  IMAGE_BASE="${registry_host}/${GCP_PROJECT_ID}/${GCP_ARTIFACT_REGISTRY_REPOSITORY}/${GCP_IMAGE_NAME}"
  IMAGE_TAGGED="${IMAGE_BASE}:${GCP_RELEASE_TAG}"

  log "Configuring Docker auth for ${registry_host}"
  gcloud auth configure-docker "${registry_host}" --quiet >/dev/null

  log "Building ${IMAGE_TAGGED}"
  docker build --platform "linux/amd64" -t "${IMAGE_TAGGED}" .

  log "Pushing ${IMAGE_TAGGED}"
  docker push "${IMAGE_TAGGED}" >/dev/null

  log "Resolving immutable digest for ${IMAGE_TAGGED}"
  IMAGE_DIGEST="$(gcloud artifacts docker images describe "${IMAGE_TAGGED}" --format='value(image_summary.digest)')"
  [[ -n "${IMAGE_DIGEST}" ]] || die "Unable to resolve Artifact Registry digest for ${IMAGE_TAGGED}"
  IMAGE_REFERENCE="${IMAGE_BASE}@${IMAGE_DIGEST}"
  log "Using image digest ${IMAGE_REFERENCE}"
}

write_env_vars_file() {
  local file_path="$1"
  local service_role="$2"
  local scheduler_enabled="$3"
  local google_client_id="${GOOGLE_OIDC_CLIENT_ID:-${OIDC_CLIENT_ID}}"

  : >"${file_path}"
  env_file_line APP_ENV "prod" >>"${file_path}"
  env_file_line SERVICE_ROLE "${service_role}" >>"${file_path}"
  env_file_line CONTROL_PLANE_CONNECT_TIMEOUT_SECONDS "${CONTROL_PLANE_CONNECT_TIMEOUT_SECONDS:-3}" >>"${file_path}"
  env_file_line DATA_BACKEND_MODE "gcp" >>"${file_path}"
  env_file_line WAREHOUSE_BACKEND "bigquery" >>"${file_path}"
  env_file_line OBJECT_STORAGE_BACKEND "gcs" >>"${file_path}"
  env_file_line MESSAGE_BACKEND "pubsub" >>"${file_path}"
  env_file_line SECRET_BACKEND "gcp_secret_manager" >>"${file_path}"
  env_file_line LEGACY_HEADER_AUTH_ENABLED "false" >>"${file_path}"
  env_file_line KAIRYX_PLATFORM_SURFACE "" >>"${file_path}"
  env_file_line CORS_ALLOWED_ORIGINS "${CORS_ALLOWED_ORIGINS}" >>"${file_path}"
  env_file_line OIDC_ISSUER "${OIDC_ISSUER}" >>"${file_path}"
  env_file_line OIDC_AUDIENCE "${OIDC_AUDIENCE}" >>"${file_path}"
  env_file_line OIDC_JWKS_URL "${OIDC_JWKS_URL}" >>"${file_path}"
  env_file_line OIDC_CLIENT_ID "${OIDC_CLIENT_ID}" >>"${file_path}"
  env_file_line GOOGLE_OIDC_CLIENT_ID "${google_client_id}" >>"${file_path}"
  env_file_line OIDC_AUTHORIZE_URL "${OIDC_AUTHORIZE_URL}" >>"${file_path}"
  env_file_line OIDC_TOKEN_URL "${OIDC_TOKEN_URL}" >>"${file_path}"
  env_file_line OIDC_LOGOUT_URL "${OIDC_LOGOUT_URL:-}" >>"${file_path}"
  env_file_line GOOGLE_OIDC_HOSTED_DOMAIN "${GOOGLE_OIDC_HOSTED_DOMAIN:-}" >>"${file_path}"
  env_file_line GCP_PROJECT_ID "${GCP_PROJECT_ID}" >>"${file_path}"
  env_file_line GCP_SECRET_PROJECT_ID "${GCP_SECRET_PROJECT_ID:-${GCP_PROJECT_ID}}" >>"${file_path}"
  env_file_line BIGQUERY_PROJECT_ID "${BIGQUERY_PROJECT_ID:-${GCP_PROJECT_ID}}" >>"${file_path}"
  env_file_line BIGQUERY_DATASET_ID "${BIGQUERY_DATASET_ID:-${GCP_BIGQUERY_DATASET_ID:-kairyx_platform}}" >>"${file_path}"
  env_file_line GCS_BUCKET_NAME "${GCS_BUCKET_NAME}" >>"${file_path}"
  env_file_line IMPORT_COMMAND_TOPIC "${IMPORT_COMMAND_TOPIC:-kairyx-import-jobs}" >>"${file_path}"
  env_file_line PREDICTION_COMMAND_TOPIC "${PREDICTION_COMMAND_TOPIC:-kairyx-prediction-jobs}" >>"${file_path}"
  env_file_line EXPORT_COMMAND_TOPIC "${EXPORT_COMMAND_TOPIC:-kairyx-export-jobs}" >>"${file_path}"
  env_file_line PUBSUB_TOPIC_NAME "${PUBSUB_TOPIC_NAME:-kairyx-raw-shards}" >>"${file_path}"
  env_file_line BOOTSTRAP_TENANT_ID "${BOOTSTRAP_TENANT_ID:-default}" >>"${file_path}"
  env_file_line BOOTSTRAP_TENANT_NAME "${BOOTSTRAP_TENANT_NAME:-Default Tenant}" >>"${file_path}"
  env_file_line BOOTSTRAP_PROJECT_ID "${BOOTSTRAP_PROJECT_ID:-default}" >>"${file_path}"
  env_file_line BOOTSTRAP_PROJECT_NAME "${BOOTSTRAP_PROJECT_NAME:-Default Project}" >>"${file_path}"
  env_file_line SCHEDULER_ENABLED "${scheduler_enabled}" >>"${file_path}"
  env_file_line SCHEDULER_INTERVAL_SECONDS "${SCHEDULER_INTERVAL_SECONDS:-60}" >>"${file_path}"
  env_file_line WEB_CONCURRENCY "${WEB_CONCURRENCY:-4}" >>"${file_path}"
  env_file_line GUNICORN_TIMEOUT "${GUNICORN_TIMEOUT:-300}" >>"${file_path}"
  env_file_line MAX_SQL_PREVIEW_ROWS_PER_TENANT "${MAX_SQL_PREVIEW_ROWS_PER_TENANT:-1000}" >>"${file_path}"
  env_file_line MAX_IMPORT_JOBS_PER_TENANT "${MAX_IMPORT_JOBS_PER_TENANT:-10}" >>"${file_path}"
  env_file_line MAX_EXPORT_JOBS_PER_TENANT "${MAX_EXPORT_JOBS_PER_TENANT:-20}" >>"${file_path}"
  env_file_line MAX_COPILOT_REPORTS_PER_TENANT "${MAX_COPILOT_REPORTS_PER_TENANT:-50}" >>"${file_path}"

  if [[ -n "${GCP_EXTRA_ENV_FILE:-}" ]]; then
    [[ -f "${GCP_EXTRA_ENV_FILE}" ]] || die "GCP_EXTRA_ENV_FILE not found: ${GCP_EXTRA_ENV_FILE}"
    cat "${GCP_EXTRA_ENV_FILE}" >>"${file_path}"
  fi
}

build_secret_bindings() {
  local service_role="$1"
  SECRET_BINDINGS=(
    "CONTROL_PLANE_DATABASE_URL=${CONTROL_PLANE_DATABASE_URL_SECRET}:latest"
    "CONTROL_PLANE_SECRET_KEY=${CONTROL_PLANE_SECRET_KEY_SECRET}:latest"
  )

  if [[ "${service_role}" != "operator-api" ]]; then
    SECRET_BINDINGS+=("WORKER_SHARED_TOKEN=${WORKER_SHARED_TOKEN_SECRET}:latest")
  fi
  if [[ -n "${API_ACCESS_KEY_SECRET:-}" ]]; then
    SECRET_BINDINGS+=("API_ACCESS_KEY=${API_ACCESS_KEY_SECRET}:latest")
  fi
  if [[ -n "${OIDC_JWT_SIGNING_SECRET_SECRET:-}" ]]; then
    SECRET_BINDINGS+=("OIDC_JWT_SIGNING_SECRET=${OIDC_JWT_SIGNING_SECRET_SECRET}:latest")
  fi
}

deploy_service() {
  local service_name="$1"
  local service_role="$2"
  local scheduler_enabled="$3"
  local allow_flag="$4"
  local service_account="$5"
  local cpu
  local memory
  local concurrency
  local min_instances
  local max_instances
  local timeout
  local env_file
  local url

  cpu="$(tier_setting "${service_role}" cpu)"
  memory="$(tier_setting "${service_role}" memory)"
  concurrency="$(tier_setting "${service_role}" concurrency)"
  min_instances="$(tier_setting "${service_role}" min)"
  max_instances="$(tier_setting "${service_role}" max)"
  timeout="$(tier_setting "${service_role}" timeout)"
  env_file="$(mktemp)"
  write_env_vars_file "${env_file}" "${service_role}" "${scheduler_enabled}"
  build_secret_bindings "${service_role}"

  log "Deploying ${service_name} (${service_role})"
  gcloud run deploy "${service_name}" \
    --project "${GCP_PROJECT_ID}" \
    --region "${GCP_REGION}" \
    --image "${IMAGE_REFERENCE}" \
    --service-account "${service_account}" \
    --cpu "${cpu}" \
    --memory "${memory}" \
    --concurrency "${concurrency}" \
    --min-instances "${min_instances}" \
    --max-instances "${max_instances}" \
    --timeout "${timeout}" \
    --port 8080 \
    --ingress all \
    --cpu-boost \
    --add-cloudsql-instances "${GCP_CLOUD_SQL_CONNECTION_NAME}" \
    --env-vars-file "${env_file}" \
    --set-secrets "$(IFS=,; echo "${SECRET_BINDINGS[*]}")" \
    "${NETWORK_FLAGS[@]}" \
    "${allow_flag}" \
    --quiet

  url="$(gcloud run services describe "${service_name}" --project "${GCP_PROJECT_ID}" --region "${GCP_REGION}" --format='value(status.url)')"
  log "${service_name} URL: ${url}"
  rm -f "${env_file}"
}

main() {
  local env_file="${1:-}"
  if [[ -z "${env_file}" || "${env_file}" == "-h" || "${env_file}" == "--help" ]]; then
    usage
    exit 0
  fi

  require_command docker
  require_command gcloud

  ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
  cd "${ROOT_DIR}"

  load_env_file "${env_file}"
  validate_configuration
  configure_network_flags
  DEPLOYMENT_TIER="$(resolve_deployment_tier)"
  SERVICE_PREFIX="$(normalize_service_prefix)"

  gcloud config set project "${GCP_PROJECT_ID}" >/dev/null

  OPERATOR_API_SERVICE_ACCOUNT="${OPERATOR_API_SERVICE_ACCOUNT:-$(default_service_account operator-api)}"
  IMPORT_WORKER_SERVICE_ACCOUNT="${IMPORT_WORKER_SERVICE_ACCOUNT:-$(default_service_account import-worker)}"
  PREDICTION_WORKER_SERVICE_ACCOUNT="${PREDICTION_WORKER_SERVICE_ACCOUNT:-$(default_service_account prediction-worker)}"
  EXPORT_WORKER_SERVICE_ACCOUNT="${EXPORT_WORKER_SERVICE_ACCOUNT:-$(default_service_account export-worker)}"
  SCHEDULER_WORKER_SERVICE_ACCOUNT="${SCHEDULER_WORKER_SERVICE_ACCOUNT:-$(default_service_account scheduler-worker)}"

  build_image

  deploy_service "$(service_name_for_role operator-api)" "operator-api" "false" "--allow-unauthenticated" "${OPERATOR_API_SERVICE_ACCOUNT}"
  deploy_service "$(service_name_for_role import-worker)" "import-worker" "false" "--no-allow-unauthenticated" "${IMPORT_WORKER_SERVICE_ACCOUNT}"
  deploy_service "$(service_name_for_role prediction-worker)" "prediction-worker" "false" "--no-allow-unauthenticated" "${PREDICTION_WORKER_SERVICE_ACCOUNT}"
  deploy_service "$(service_name_for_role export-worker)" "export-worker" "false" "--no-allow-unauthenticated" "${EXPORT_WORKER_SERVICE_ACCOUNT}"
  deploy_service "$(service_name_for_role scheduler-worker)" "scheduler-worker" "true" "--no-allow-unauthenticated" "${SCHEDULER_WORKER_SERVICE_ACCOUNT}"

  cat <<EOF

Deployment finished.

Image:
  ${IMAGE_REFERENCE}

Deployment tier:
  ${DEPLOYMENT_TIER}

Service prefix:
  ${SERVICE_PREFIX:-<none>}

Next steps:
  1. If this deploy is for a fresh dev project and foundation bootstrap has not been run yet, stop here and run:
       bash deploy/gcp/bootstrap_dev_foundation.sh ${env_file}
  2. Configure Pub/Sub push subscriptions and the Cloud Scheduler job:
       bash deploy/gcp/configure_dev_eventing.sh ${env_file}
  3. Run smoke checks:
     - GET operator-api /health/live
     - GET operator-api /api/v1/health
     - one authenticated call to each worker endpoint
EOF
}

main "$@"
