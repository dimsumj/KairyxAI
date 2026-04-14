#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${1:-${BASE_URL:-http://127.0.0.1:8000}}"
PWCLI="${PWCLI:-}"
SESSION="${PLAYWRIGHT_CLI_SESSION:-kxs-$$_${RANDOM}}"
ARTIFACT_DIR="${ARTIFACT_DIR:-output/playwright}"
LOG_FILE="$ARTIFACT_DIR/operator_console_smoke.log"

if [[ -z "$PWCLI" ]]; then
  printf '%s\n' "PWCLI is required. Set it to your Playwright CLI wrapper before running this script." >&2
  exit 1
fi

mkdir -p "$ARTIFACT_DIR"
: >"$LOG_FILE"

run_pw() {
  local output_file
  output_file="$(mktemp)"
  if ! "$PWCLI" --session "$SESSION" "$@" >"$output_file" 2>&1; then
    cat "$output_file" | tee -a "$LOG_FILE" >&2
    rm -f "$output_file"
    return 1
  fi
  cat "$output_file" >>"$LOG_FILE"
  if grep -q '^### Error' "$output_file"; then
    cat "$output_file" | tee -a "$LOG_FILE" >&2
    rm -f "$output_file"
    return 1
  fi
  rm -f "$output_file"
}

log_step() {
  printf '%s\n' "$1" | tee -a "$LOG_FILE"
}

open_and_wait() {
  run_pw open "$BASE_URL"
  run_pw run-code "async (page) => {
    await page.waitForLoadState('networkidle');
  }"
}

assert_base_url_ready() {
  log_step "Checking base URL: $BASE_URL"
  if ! curl --silent --show-error --fail "$BASE_URL" >/dev/null; then
    printf '%s\n' "Smoke target is unavailable: $BASE_URL" | tee -a "$LOG_FILE" >&2
    exit 1
  fi
}

assert_auth_shell() {
  log_step "Checking Google login shell"
  run_pw eval "() => {
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
  }"
}

assert_module() {
  local module="$1"
  local selector="$2"
  log_step "Checking module: $module"
  run_pw eval "() => {
    const link = document.querySelector('[data-module=\"$module\"]');
    if (!link) throw new Error('Missing module link: $module');
    link.click();
    return document.getElementById('module-title')?.textContent || '';
  }"
  run_pw run-code "async (page) => {
    await page.waitForTimeout(600);
  }"
  run_pw eval "() => {
    const element = document.querySelector('$selector');
    if (!element) throw new Error('Missing selector for $module: $selector');
    return {
      module: '$module',
      title: document.getElementById('module-title')?.textContent || '',
      selector: '$selector',
      visible: !!element,
    };
  }"
}

assert_bigquery_connector_ui() {
  log_step "Checking BigQuery connector UI"
  run_pw run-code "async (page) => {
    const connectorsLink = page.locator('[data-item=\"data-core-connectors\"]');
    if (await connectorsLink.count() === 0) throw new Error('Missing Data Core -> Connectors navigation');
    await connectorsLink.first().click();
    await page.waitForTimeout(700);

    const connectButton = page.locator('#add-connector-btn');
    if (await connectButton.count() === 0) throw new Error('Missing Connect Data Source button');
    await connectButton.first().click();
    await page.waitForTimeout(250);

    const typeSelect = page.locator('#connector-type');
    if (await typeSelect.count() === 0) throw new Error('Missing connector type select');
    await typeSelect.first().selectOption('bigquery');
    await page.waitForTimeout(250);

    const modeSelect = page.locator('#bigquery_credentials_entry_mode');
    const uploadInput = page.locator('#bigquery_service_account_file');
    const pasteTextarea = page.locator('#bigquery_service_account_json');
    if (await modeSelect.count() === 0 || await uploadInput.count() === 0 || await pasteTextarea.count() === 0) {
      throw new Error('Missing latest BigQuery connector credential controls');
    }

    await modeSelect.first().selectOption('paste');
    await page.waitForTimeout(150);

    const uploadStyle = await uploadInput.evaluate((element) => window.getComputedStyle(element.closest('.form-group')).display);
    const pasteStyle = await pasteTextarea.evaluate((element) => window.getComputedStyle(element.closest('.form-group')).display);
    if (uploadStyle !== 'none' || pasteStyle === 'none') {
      throw new Error('BigQuery credential mode toggle did not switch to paste mode');
    }

    return {
      title: await page.locator('#add-connector-form-container h2').textContent() || '',
      typeValue: await typeSelect.inputValue(),
      modeValue: await modeSelect.inputValue(),
      uploadStyle,
      pasteStyle,
    };
  }"
}

assert_email_campaign_ui() {
  log_step "Checking provider-aware email campaign UI"
  run_pw run-code "async (page) => {
    const connectorsLink = page.locator('[data-item=\"data-core-connectors\"]');
    if (await connectorsLink.count() === 0) throw new Error('Missing Data Core -> Connectors navigation');
    await connectorsLink.first().click();
    await page.waitForTimeout(700);

    const connectProviderButton = page.locator('#add-provider-connection-btn');
    const providerFormContainer = page.locator('#provider-connection-form-container');
    const providerType = page.locator('#provider-connection-type-select');
    const providerName = page.locator('#provider-connection-name-input');
    const providerSave = page.locator('#provider-connection-save-btn');
    const providerCancel = page.locator('#provider-connection-cancel-btn');
    if (await connectProviderButton.count() === 0 || await providerFormContainer.count() === 0) {
      throw new Error('Missing Connect Campaign Provider entry point');
    }
    const providerFormDisplay = await providerFormContainer.evaluate((element) => window.getComputedStyle(element).display);
    if (providerFormDisplay !== 'none') {
      throw new Error('Campaign provider form should be hidden until Connect Campaign Provider is clicked');
    }

    await connectProviderButton.first().click();
    await page.waitForTimeout(200);

    if (
      await providerType.count() === 0
      || await providerName.count() === 0
      || await providerSave.count() === 0
      || await providerCancel.count() === 0
    ) {
      throw new Error('Missing campaign provider connection controls in Connectors');
    }
    const visibleProviderFormDisplay = await providerFormContainer.evaluate((element) => window.getComputedStyle(element).display);
    if (visibleProviderFormDisplay === 'none') {
      throw new Error('Campaign provider form did not open after clicking Connect Campaign Provider');
    }

    await providerType.selectOption('braze');
    await page.waitForTimeout(200);
    const brazeApiKey = page.locator('#provider-connection-braze-api-key-input');
    const brazeEndpoint = page.locator('#provider-connection-braze-rest-endpoint-input');
    if (await brazeApiKey.count() === 0 || await brazeEndpoint.count() === 0) {
      throw new Error('Missing Braze provider connection fields');
    }

    const orchestratorLink = page.locator('[data-module=\"action-orchestrator\"]');
    if (await orchestratorLink.count() === 0) throw new Error('Missing Action Orchestrator navigation');
    await orchestratorLink.first().click();
    await page.waitForTimeout(700);

    const campaignProviderType = page.locator('#email-campaign-provider-type-select');
    const campaignProvider = page.locator('#email-campaign-provider-select');
    const campaignTemplate = page.locator('#email-campaign-template-select');
    const campaignAudienceType = page.locator('#email-campaign-audience-type-select');
    const campaignPredictionAudience = page.locator('#email-campaign-prediction-job-select');
    const campaignCohortAudience = page.locator('#email-campaign-cohort-select');
    const campaignSend = page.locator('#email-campaign-send-now-btn');
    if (
      await campaignProviderType.count() === 0
      || await campaignProvider.count() === 0
      || await campaignTemplate.count() === 0
      || await campaignAudienceType.count() === 0
      || await campaignPredictionAudience.count() === 0
      || await campaignCohortAudience.count() === 0
      || await campaignSend.count() === 0
    ) {
      throw new Error('Missing email campaign builder controls');
    }

    await campaignProviderType.selectOption('braze');
    await page.waitForTimeout(200);

    const emailFieldGroup = page.locator('#email-campaign-recipient-email-field-group');
    const externalIdFieldGroup = page.locator('#email-campaign-recipient-external-id-field-group');
    const emailFieldSelect = page.locator('#email-campaign-recipient-email-field-select');
    const externalIdFieldSelect = page.locator('#email-campaign-recipient-external-id-field-select');
    const emailFieldDisplay = await emailFieldGroup.evaluate((element) => window.getComputedStyle(element).display);
    const externalIdFieldDisplay = await externalIdFieldGroup.evaluate((element) => window.getComputedStyle(element).display);
    if (
      emailFieldDisplay !== 'none'
      || externalIdFieldDisplay === 'none'
      || await emailFieldSelect.count() === 0
      || await externalIdFieldSelect.count() === 0
    ) {
      throw new Error('Campaign provider switch did not toggle recipient field groups');
    }

    await campaignAudienceType.selectOption('cohort');
    await page.waitForTimeout(200);
    const cohortGroupDisplay = await page.locator('#email-campaign-cohort-group').evaluate((element) => window.getComputedStyle(element).display);
    const predictionGroupDisplay = await page.locator('#email-campaign-prediction-job-group').evaluate((element) => window.getComputedStyle(element).display);
    if (cohortGroupDisplay === 'none' || predictionGroupDisplay !== 'none') {
      throw new Error('Campaign audience source switch did not toggle prediction vs cohort selectors');
    }

    return {
      providerButtonLabel: await connectProviderButton.first().textContent() || '',
      providerTypeValue: await providerType.inputValue(),
      providerSaveLabel: await providerSave.textContent() || '',
      campaignProviderTypeValue: await campaignProviderType.inputValue(),
      campaignAudienceTypeValue: await campaignAudienceType.inputValue(),
      campaignTemplatePlaceholder: await campaignTemplate.first().locator('option').first().textContent() || '',
      campaignSendLabel: await campaignSend.textContent() || '',
      externalIdFieldDisplay,
    };
  }"
}

exercise_copilot_agent() {
  log_step "Exercising global AI assistant"
  run_pw run-code "async (page) => {
    const connectorsLink = page.locator('[data-item=\"data-core-connectors\"]');
    if (await connectorsLink.count() === 0) throw new Error('Missing Data Core -> Connectors navigation');
    await connectorsLink.first().click();
    await page.waitForTimeout(700);

    const launcher = page.locator('#copilot-agent-launcher-btn');
    if (await launcher.count() === 0) throw new Error('Missing global assistant launcher');
    await launcher.first().click();
    await page.waitForTimeout(500);

    const textarea = page.locator('#copilot-agent-message-input');
    const sendButton = page.locator('#copilot-agent-send-btn');
    const status = page.locator('#copilot-agent-session-status');
    if (await textarea.count() === 0 || await sendButton.count() === 0 || await status.count() === 0) {
      throw new Error('Missing copilot agent controls');
    }
    await page.waitForFunction(() => {
      const input = document.getElementById('copilot-agent-message-input');
      const send = document.getElementById('copilot-agent-send-btn');
      return !!input && !!send && !input.disabled && !send.disabled;
    }, { timeout: 5000 });

    await textarea.fill('How do I create an Amplitude connector here? Give me a sample payload.');
    await sendButton.click();
    await page.waitForTimeout(900);

    const threadAfterHelp = await page.locator('#copilot-agent-thread').textContent() || '';
    if (!threadAfterHelp.toLowerCase().includes('amplitude') || !threadAfterHelp.includes('demo_api_key')) {
      throw new Error('Expected grounded connector help answer with sample content');
    }

    await textarea.fill('Set up a connection.');
    await sendButton.click();
    await page.waitForTimeout(900);

    const clarifications = await page.locator('#copilot-agent-thread').textContent() || '';
    if (!clarifications.toLowerCase().includes('connection')) {
      throw new Error('Expected connection clarification prompt');
    }

    const connectorName = 'agent_smoke_' + Date.now();
    await textarea.fill([
      'Set up a connection',
      'connection_scope: connector',
      'connection_type: amplitude',
      'name: ' + connectorName,
      'api_key: demo_api_key',
      'secret_key: demo_secret_key'
    ].join('\n'));
    await sendButton.click();
    await page.waitForTimeout(1200);

    const artifacts = await page.locator('#copilot-agent-thread').textContent() || '';
    const artifactButton = page.locator('[data-copilot-agent-artifact-index]');
    if (!artifacts.includes(connectorName) || await artifactButton.count() === 0) {
      throw new Error('Expected connector artifact after safe setup flow');
    }

    await textarea.fill('Start experiment smoke_agent_exp');
    await sendButton.click();
    await page.waitForTimeout(900);

    const confirmations = await page.locator('#copilot-agent-thread').textContent() || '';
    const confirmationButton = page.locator('[data-copilot-agent-confirm]');
    if (!confirmations.toLowerCase().includes('start experiment') || await confirmationButton.count() === 0) {
      throw new Error('Expected pending confirmation for experiment start');
    }

    const closeDrawerButton = page.locator('#copilot-agent-close-btn');
    if (await closeDrawerButton.count() === 0) throw new Error('Missing assistant drawer close button');
    await closeDrawerButton.first().click();
    await page.waitForTimeout(250);

    const copilotNav = page.locator('[data-module=\"insight-copilot\"]');
    if (await copilotNav.count() === 0) throw new Error('Missing Insight Copilot module link');
    await copilotNav.first().click();
    await page.waitForTimeout(700);

    await launcher.first().click();
    await page.waitForTimeout(500);

    const persistedConfirmations = await page.locator('#copilot-agent-thread').textContent() || '';
    const persistedConfirmationButton = page.locator('[data-copilot-agent-confirm]');
    if (!persistedConfirmations.toLowerCase().includes('start experiment') || await persistedConfirmationButton.count() === 0) {
      throw new Error('Expected pending confirmation to persist across navigation');
    }

    return {
      sessionStatus: await status.textContent() || '',
      connectorName,
      threadAfterHelp,
      clarifications,
      artifacts,
      confirmations,
      persistedConfirmations,
    };
  }"
}

cleanup() {
  "$PWCLI" --session "$SESSION" close >>"$LOG_FILE" 2>/dev/null || true
}

trap cleanup EXIT

log_step "Opening $BASE_URL"
assert_base_url_ready
open_and_wait
assert_auth_shell
assert_module "data-core" "#import-detail-output"
assert_bigquery_connector_ui
assert_module "audience-engine" "#audience-cohort-list"
assert_module "action-orchestrator" "#workflow-delivery-diagnostics-output"
assert_email_campaign_ui
assert_module "experiment-hub" "#experiment-integrity-output"
assert_module "insight-copilot" "#copilot-query-section"
exercise_copilot_agent
log_step "Operator console smoke completed."
