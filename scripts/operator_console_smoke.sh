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

exercise_copilot_agent() {
  log_step "Exercising global AI assistant"
  run_pw run-code "async () => {
    const connectorsLink = document.querySelector('[data-item=\"data-core-connectors\"]');
    if (!connectorsLink) throw new Error('Missing Data Core -> Connectors navigation');
    connectorsLink.click();
    await page.waitForTimeout(700);

    const launcher = document.getElementById('copilot-agent-launcher-btn');
    if (!launcher) throw new Error('Missing global assistant launcher');
    launcher.click();
    await page.waitForTimeout(500);

    const textarea = document.getElementById('copilot-agent-message-input');
    const sendButton = document.getElementById('copilot-agent-send-btn');
    const status = document.getElementById('copilot-agent-session-status');
    if (!textarea || !sendButton || !status) throw new Error('Missing copilot agent controls');

    textarea.value = 'How do I create an Amplitude connector here? Give me a sample payload.';
    sendButton.click();
    await page.waitForTimeout(900);

    const threadAfterHelp = document.getElementById('copilot-agent-thread')?.textContent || '';
    if (!threadAfterHelp.toLowerCase().includes('amplitude') || !threadAfterHelp.includes('demo_api_key')) {
      throw new Error('Expected grounded connector help answer with sample content');
    }

    textarea.value = 'Set up a connection.';
    sendButton.click();
    await page.waitForTimeout(900);

    const clarifications = document.getElementById('copilot-agent-clarifications')?.textContent || '';
    if (!clarifications.toLowerCase().includes('connection')) {
      throw new Error('Expected connection clarification prompt');
    }

    const connectorName = 'agent_smoke_' + Date.now();
    textarea.value = [
      'Set up a connection',
      'connection_scope: connector',
      'connection_type: amplitude',
      'name: ' + connectorName,
      'api_key: demo_api_key',
      'secret_key: demo_secret_key'
    ].join('\n');
    sendButton.click();
    await page.waitForTimeout(1200);

    const artifacts = document.getElementById('copilot-agent-artifacts')?.textContent || '';
    if (!artifacts.includes(connectorName)) {
      throw new Error('Expected connector artifact after safe setup flow');
    }

    textarea.value = 'Start experiment smoke_agent_exp';
    sendButton.click();
    await page.waitForTimeout(900);

    const confirmations = document.getElementById('copilot-agent-confirmations')?.textContent || '';
    if (!confirmations.toLowerCase().includes('start experiment')) {
      throw new Error('Expected pending confirmation for experiment start');
    }

    const copilotNav = document.querySelector('[data-module=\"insight-copilot\"]');
    if (!copilotNav) throw new Error('Missing Insight Copilot module link');
    copilotNav.click();
    await page.waitForTimeout(700);

    const persistedConfirmations = document.getElementById('copilot-agent-confirmations')?.textContent || '';
    if (!persistedConfirmations.toLowerCase().includes('start experiment')) {
      throw new Error('Expected pending confirmation to persist across navigation');
    }

    return {
      sessionStatus: status.textContent || '',
      connectorName,
      threadAfterHelp,
      clarifications,
      artifacts,
      confirmations,
      persistedConfirmations,
    };
  }" >>"$LOG_FILE"
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
assert_module "insight-copilot" "#copilot-query-section"
exercise_copilot_agent
log_step "Operator console smoke completed."
