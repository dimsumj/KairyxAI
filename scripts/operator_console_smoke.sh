#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${1:-${BASE_URL:-http://127.0.0.1:8000}}"
PWCLI="${PWCLI:-}"
SESSION="${PLAYWRIGHT_CLI_SESSION:-kairyx-operator-smoke}"
ARTIFACT_DIR="${ARTIFACT_DIR:-output/playwright}"
LOG_FILE="$ARTIFACT_DIR/operator_console_smoke.log"

if [[ -z "$PWCLI" ]]; then
  printf '%s\n' "PWCLI is required. Set it to your Playwright CLI wrapper before running this script." >&2
  exit 1
fi

mkdir -p "$ARTIFACT_DIR"
: >"$LOG_FILE"

run_pw() {
  "$PWCLI" --session "$SESSION" "$@"
}

log_step() {
  printf '%s\n' "$1" | tee -a "$LOG_FILE"
}

open_and_wait() {
  run_pw open "$BASE_URL" >>"$LOG_FILE"
  run_pw run-code "await page.waitForLoadState('networkidle')" >>"$LOG_FILE"
}

assert_auth_shell() {
  log_step "Checking Google login shell"
  run_pw run-code "(() => {
    const login = document.getElementById('oidc-login-btn');
    const logout = document.getElementById('oidc-logout-btn');
    const workspaceInput = document.getElementById('workspace-org-url-input');
    const status = document.getElementById('auth-status-text');
    if (!login || !logout || !workspaceInput || !status) throw new Error('Missing auth controls');
    return {
      loginLabel: login.textContent || '',
      logoutLabel: logout.textContent || '',
      workspacePlaceholder: workspaceInput.placeholder || '',
      status: status.textContent || '',
    };
  })()" >>"$LOG_FILE"
}

assert_module() {
  local module="$1"
  local selector="$2"
  log_step "Checking module: $module"
  run_pw run-code "(() => {
    const link = document.querySelector('[data-module=\"$module\"]');
    if (!link) throw new Error('Missing module link: $module');
    link.click();
    return document.getElementById('module-title')?.textContent || '';
  })()" >>"$LOG_FILE"
  run_pw run-code "await page.waitForTimeout(600)" >>"$LOG_FILE"
  run_pw run-code "(() => {
    const element = document.querySelector('$selector');
    if (!element) throw new Error('Missing selector for $module: $selector');
    return {
      module: '$module',
      title: document.getElementById('module-title')?.textContent || '',
      selector: '$selector',
      visible: !!element,
    };
  })()" >>"$LOG_FILE"
}

cleanup() {
  run_pw close >>"$LOG_FILE" 2>/dev/null || true
}

trap cleanup EXIT

log_step "Opening $BASE_URL"
open_and_wait
assert_auth_shell
assert_module "data-core" "#import-detail-output"
assert_module "audience-engine" "#audience-cohort-list"
assert_module "action-orchestrator" "#workflow-delivery-diagnostics-output"
assert_module "experiment-hub" "#experiment-integrity-output"
assert_module "insight-copilot" "#copilot-response-output"
assert_module "help" "#help"
log_step "Operator console smoke completed."
