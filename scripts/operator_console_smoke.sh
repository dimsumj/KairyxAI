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

assert_ai_first_operator_ui() {
  log_step "Checking AI-first operator console surfaces"
  run_pw run-code "async (page) => {
    const checks = [];
    const modules = [
      ['data-core', '#data-sandbox', '[data-ai-command-center=\"data-core\"]', '#data-sandbox-advanced-json-panel'],
      ['audience-engine', '#audience-engine', '[data-ai-command-center=\"audience-engine\"]', '#audience-builder-manual-list-group details'],
      ['action-orchestrator', '#action-orchestrator', '[data-ai-command-center=\"action-orchestrator\"]', '#push-dispatch-data-input'],
      ['experiment-hub', '#experiment-hub', '[data-ai-command-center=\"experiment-hub\"]', '#experiment-outcomes-json'],
      ['insight-copilot', '#insight-copilot', '[data-ai-command-center=\"insight-copilot\"]', '#copilot-manual-tools-panel'],
    ];
    for (const [moduleId, pageSelector, aiSelector, advancedSelector] of modules) {
      const nav = page.locator('[data-module=\"' + moduleId + '\"]');
      if (await nav.count() === 0) throw new Error('Missing module nav for ' + moduleId);
      await nav.first().click();
      await page.waitForTimeout(500);
      if (await page.locator(pageSelector).count() === 0) throw new Error('Missing page for ' + moduleId);
      const aiPanel = page.locator(aiSelector);
      if (await aiPanel.count() === 0) throw new Error('Missing AI starter panel for ' + moduleId);
      const starterCount = await aiPanel.locator('[data-agent-starter-message]').count();
      if (starterCount === 0 && moduleId !== 'insight-copilot') throw new Error('Missing starter prompts for ' + moduleId);
      checks.push({ moduleId, starterCount });
    }

    await page.locator('[data-module=\"data-core\"]').first().click();
    await page.waitForTimeout(300);
    const dataSandboxLink = page.locator('[data-item=\"data-core-mappings\"]');
    if (await dataSandboxLink.count() > 0) {
      await dataSandboxLink.first().click();
      await page.waitForTimeout(300);
    }
    const visibleJsonInputs = await page.locator([
      '#data-sandbox-mapping-json',
      '#data-sandbox-sample-json',
      '#sql-workspace-textarea',
      '#email-campaign-merge-fields-input',
      '#push-dispatch-data-input',
      '#push-dispatch-provider-options-input',
      '#push-dispatch-wynn-filters-input',
      '#workflow-push-data-input',
      '#workflow-push-provider-options-input',
      '#activation-callbacks-json',
      '#experiment-outcomes-json',
      '#copilot-query-filters-json',
      '#copilot-insight-json',
      '#copilot-metric-context-json',
    ].join(',')).evaluateAll((elements) => elements
      .filter((element) => {
        const style = window.getComputedStyle(element);
        const rect = element.getBoundingClientRect();
        return style.display !== 'none' && style.visibility !== 'hidden' && rect.width > 0 && rect.height > 0;
      })
      .map((element) => element.id));
    if (visibleJsonInputs.length > 0) {
      throw new Error('JSON input fields should be hidden from the primary operator UI: ' + visibleJsonInputs.join(', '));
    }
    const nonStateTechnicalInputs = await page.locator([
      '#data-sandbox-mapping-json',
      '#data-sandbox-sample-json',
      '#sql-workspace-textarea',
      '#email-campaign-merge-fields-input',
      '#push-dispatch-data-input',
      '#push-dispatch-provider-options-input',
      '#push-dispatch-wynn-filters-input',
      '#workflow-push-data-input',
      '#workflow-push-provider-options-input',
      '#activation-callbacks-json',
      '#experiment-outcomes-json',
      '#copilot-query-filters-json',
      '#copilot-insight-json',
      '#copilot-metric-context-json',
    ].join(',')).evaluateAll((elements) => elements
      .filter((element) => element.tagName.toLowerCase() === 'textarea' || String(element.getAttribute('type') || '').toLowerCase() !== 'hidden')
      .map((element) => element.id));
    if (nonStateTechnicalInputs.length > 0) {
      throw new Error('Technical JSON/code setup fields must be hidden state inputs with export artifacts: ' + nonStateTechnicalInputs.join(', '));
    }

    const requiredArtifactButtons = [
      '#data-sandbox-mapping-json-artifact-export-json-btn',
      '#sql-workspace-artifact-export-json-btn',
      '#push-dispatch-data-artifact-export-json-btn',
      '#experiment-outcomes-artifact-export-json-btn',
    ];
    for (const selector of requiredArtifactButtons) {
      if (await page.locator(selector).count() === 0) {
        throw new Error('Missing setup artifact export button: ' + selector);
      }
    }

    await page.locator('[data-module=\"action-orchestrator\"]').first().click();
    await page.waitForTimeout(500);
    const pushAdvancedOpen = await page.locator('#push-dispatch-data-input').evaluate((element) => element.closest('details')?.open);
    if (pushAdvancedOpen) throw new Error('Push data JSON should not be visible as an expanded editor');

    await page.locator('[data-module=\"experiment-hub\"]').first().click();
    await page.waitForTimeout(500);
    const outcomeAdvancedOpen = await page.locator('#experiment-outcomes-json').evaluate((element) => element.closest('details')?.open);
    if (outcomeAdvancedOpen) throw new Error('Outcome payload JSON should not be visible as an expanded editor');

    await page.locator('[data-module=\"insight-copilot\"]').first().click();
    await page.waitForTimeout(500);
    const manualPanelOpen = await page.locator('#copilot-manual-tools-panel').evaluate((element) => element.open);
    const queryVisible = await page.locator('#copilot-query-section').isVisible();
    if (manualPanelOpen || queryVisible) {
      throw new Error('Manual Copilot forms should not be primary visible controls');
    }

    return checks;
  }"
}

assert_audience_builder_ui() {
  log_step "Checking guided audience builder UI"
  run_pw run-code "async (page) => {
    const audienceLink = page.locator('[data-module=\"audience-engine\"]');
    if (await audienceLink.count() === 0) throw new Error('Missing Audience Engine navigation');
    await audienceLink.first().click();
    await page.waitForTimeout(700);

    const basisSelect = page.locator('#audience-builder-basis-select');
    const sourceSelect = page.locator('#audience-builder-source-select');
    const previewButton = page.locator('#audience-builder-preview-btn');
    const aiDraftButton = page.locator('#audience-builder-ai-draft-btn');
    const advancedSql = page.locator('#audience-sql-section');
    if (
      await basisSelect.count() === 0
      || await sourceSelect.count() === 0
      || await previewButton.count() === 0
      || await aiDraftButton.count() === 0
      || await advancedSql.count() === 0
    ) {
      throw new Error('Missing guided audience builder controls');
    }

    await basisSelect.first().selectOption('managed_warehouse_sql');
    await page.waitForTimeout(250);

    const sqlOpen = await advancedSql.evaluate((element) => element.hasAttribute('open'));
    if (!sqlOpen) {
      throw new Error('Managed Warehouse SQL section did not open when selecting managed_warehouse_sql basis');
    }

    const savedQuerySelect = page.locator('#audience-builder-saved-query-select');
    if (await savedQuerySelect.count() === 0) {
      throw new Error('Missing saved query selector for managed warehouse audiences');
    }

    await basisSelect.first().selectOption('connector_bigquery_table');
    await page.waitForTimeout(250);

    const connectorSelect = page.locator('#audience-builder-connector-select');
    const connectorTableSelect = page.locator('#audience-builder-connector-table-select');
    const canonicalFieldInput = page.locator('#audience-builder-canonical-user-id-field-input');
    if (
      await connectorSelect.count() === 0
      || await connectorTableSelect.count() === 0
      || await canonicalFieldInput.count() === 0
    ) {
      throw new Error('Missing BigQuery connector cohort controls');
    }

    await basisSelect.first().selectOption('manual_list');
    await page.waitForTimeout(250);
    const manualListGroup = page.locator('#audience-builder-manual-list-group');
    const manualListDisplay = await manualListGroup.evaluate((element) => window.getComputedStyle(element).display);
    if (manualListDisplay === 'none') {
      throw new Error('Manual list input did not appear for manual_list basis');
    }

    await basisSelect.first().selectOption('prediction');
    await page.waitForTimeout(250);
    const predictionControlsDisplay = await page.locator('#audience-builder-prediction-controls').evaluate((element) => window.getComputedStyle(element).display);
    if (predictionControlsDisplay === 'none') {
      throw new Error('Prediction controls should be visible for prediction basis');
    }

    return {
      basisValue: await basisSelect.inputValue(),
      previewLabel: await previewButton.textContent() || '',
      aiDraftLabel: await aiDraftButton.textContent() || '',
      sqlOpen,
      savedQuerySelectorVisible: await savedQuerySelect.count(),
      connectorSelectorVisible: await connectorSelect.count(),
      manualListDisplay,
    };
  }"
}

assert_bigquery_connector_ui() {
  log_step "Checking BigQuery connector UI"
  run_pw run-code "async (page) => {
    const dataCoreModuleLink = page.locator('[data-module=\"data-core\"]');
    const connectorsLink = page.locator('[data-item=\"data-core-connectors\"]');
    if (await dataCoreModuleLink.count() === 0 || await connectorsLink.count() === 0) {
      throw new Error('Missing Data Core -> Connectors navigation');
    }
    const dataCoreActive = await dataCoreModuleLink.first().evaluate((element) => element.classList.contains('active'));
    const dataCoreExpanded = (await dataCoreModuleLink.first().getAttribute('aria-expanded')) === 'true';
    if (!dataCoreActive || !dataCoreExpanded) {
      await dataCoreModuleLink.first().click();
      await page.waitForTimeout(300);
    }
    await connectorsLink.first().click();
    await page.waitForTimeout(700);

    const runtimeButton = page.locator('#add-ai-model-profile-btn');
    if (await runtimeButton.count() === 0) throw new Error('Missing Connect Ask AI Runtime button');
    await runtimeButton.first().click();
    await page.waitForTimeout(250);

    const runtimeSelect = page.locator('#ai-model-profile-runtime-select');
    const geminiApiKey = page.locator('#ai-model-profile-gemini-api-key-input');
    const geminiApiKeyRef = page.locator('#ai-model-profile-gemini-api-key-ref-input');
    if (await runtimeSelect.count() === 0 || await geminiApiKey.count() === 0 || await geminiApiKeyRef.count() === 0) {
      throw new Error('Missing secure Gemini runtime credential fields');
    }
    const geminiPlaceholder = await geminiApiKey.first().getAttribute('placeholder') || '';
    if (!geminiPlaceholder.includes('CONTROL_PLANE_SECRET_KEY')) {
      throw new Error('Gemini API key field should explain production inline secret requirements');
    }

    await runtimeSelect.first().selectOption('openai_compatible');
    await page.waitForTimeout(250);
    const openaiApiKey = page.locator('#ai-model-profile-openai-api-key-input');
    const openaiApiKeyRef = page.locator('#ai-model-profile-openai-api-key-ref-input');
    if (await openaiApiKey.count() === 0 || await openaiApiKeyRef.count() === 0) {
      throw new Error('Missing secure OpenAI-compatible runtime credential fields');
    }
    const openaiPlaceholder = await openaiApiKey.first().getAttribute('placeholder') || '';
    if (!openaiPlaceholder.includes('CONTROL_PLANE_SECRET_KEY')) {
      throw new Error('OpenAI-compatible API key field should explain production inline secret requirements');
    }
    await page.locator('#ai-model-profile-cancel-btn').click();
    await page.waitForTimeout(150);

    const connectButton = page.locator('#add-connector-btn');
    if (await connectButton.count() === 0) throw new Error('Missing Connect Data Source button');
    await connectButton.first().click();
    await page.waitForTimeout(250);

    const typeSelect = page.locator('#connector-type');
    if (await typeSelect.count() === 0) throw new Error('Missing connector type select');
    await typeSelect.first().selectOption('bigquery');
    await page.waitForTimeout(250);

    const uploadInput = page.locator('#bigquery_service_account_file');
    const pasteTextarea = page.locator('#bigquery_service_account_json');
    if (await uploadInput.count() === 0) {
      throw new Error('Missing BigQuery connector credential file upload');
    }
    if (await pasteTextarea.count() > 0) {
      throw new Error('BigQuery connector should use file upload or secret refs instead of a JSON paste textarea');
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
    const dataCoreModuleLink = page.locator('[data-module=\"data-core\"]');
    const connectorsLink = page.locator('[data-item=\"data-core-connectors\"]');
    if (await dataCoreModuleLink.count() === 0 || await connectorsLink.count() === 0) {
      throw new Error('Missing Data Core -> Connectors navigation');
    }
    const dataCoreActive = await dataCoreModuleLink.first().evaluate((element) => element.classList.contains('active'));
    const dataCoreExpanded = (await dataCoreModuleLink.first().getAttribute('aria-expanded')) === 'true';
    if (!dataCoreActive || !dataCoreExpanded) {
      await dataCoreModuleLink.first().click();
      await page.waitForTimeout(300);
    }
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

    const workflowFilters = await page.locator('#workflow-studio-filters [data-workflow-studio-filter]').evaluateAll((elements) => elements.map((element) => ({
      filter: element.getAttribute('data-workflow-studio-filter'),
      text: element.textContent.trim(),
      active: element.classList.contains('active'),
    })));
    const expectedWorkflowFilters = ['scheduled:Scheduled', 'sent:Sent', 'archived:Archived', 'all:All'];
    const actualWorkflowFilters = workflowFilters.map((item) => `${item.filter}:${item.text}`);
    if (JSON.stringify(actualWorkflowFilters) !== JSON.stringify(expectedWorkflowFilters)) {
      throw new Error(`Unexpected Workflow Studio filters: ${actualWorkflowFilters.join(', ')}`);
    }
    if (workflowFilters.find((item) => item.filter === 'email_campaign' || item.filter === 'workflow')) {
      throw new Error('Workflow Studio still exposes type-specific filter tabs');
    }

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
    const dataCoreModuleLink = page.locator('[data-module=\"data-core\"]');
    const connectorsLink = page.locator('[data-item=\"data-core-connectors\"]');
    if (await dataCoreModuleLink.count() === 0 || await connectorsLink.count() === 0) {
      throw new Error('Missing Data Core -> Connectors navigation');
    }
    const dataCoreActive = await dataCoreModuleLink.first().evaluate((element) => element.classList.contains('active'));
    const dataCoreExpanded = (await dataCoreModuleLink.first().getAttribute('aria-expanded')) === 'true';
    if (!dataCoreActive || !dataCoreExpanded) {
      await dataCoreModuleLink.first().click();
      await page.waitForTimeout(300);
    }
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
    await textarea.fill('Set up a BigQuery connector named ' + connectorName + ' with project_id: analytics-prod');
    await sendButton.click();
    await page.waitForTimeout(900);

    const securePrompt = await page.locator('#copilot-agent-thread').textContent() || '';
    const secureButton = page.locator('[data-copilot-agent-secure-inputs]');
    if (!securePrompt.toLowerCase().includes('service account') || await secureButton.count() === 0) {
      throw new Error('Expected secure credential prompt for BigQuery setup');
    }
    const datasetClarification = page.locator('[data-clarification-key=\"dataset_id\"]');
    if (await datasetClarification.count() === 0) {
      throw new Error('Expected non-sensitive dataset clarification before secure setup');
    }
    await datasetClarification.fill('game_events');
    await page.locator('[data-copilot-agent-submit-clarifications]').first().click();
    await page.waitForTimeout(900);

    await page.locator('[data-copilot-agent-secure-inputs]').first().click();
    await page.waitForTimeout(250);

    const secureDialog = page.locator('#copilot-agent-secure-input-dialog');
    if (await secureDialog.count() === 0 || !(await secureDialog.isVisible())) {
      throw new Error('Secure input dialog did not open');
    }
    const serviceAccountJson = JSON.stringify({
      type: 'service_account',
      project_id: 'analytics-prod',
      client_email: 'svc@analytics-prod.iam.gserviceaccount.com',
      private_key: '-----BEGIN PRIVATE KEY-----\\nsmoke-private-key\\n-----END PRIVATE KEY-----\\n',
      token_uri: 'https://oauth2.googleapis.com/token',
    });
    await page.locator('[data-secure-input-key=\"service_account_json\"]').fill(serviceAccountJson);
    await page.locator('#copilot-agent-secure-input-submit-btn').click();
    await page.waitForTimeout(1400);

    const secureDialogHidden = await secureDialog.evaluate((element) => element.classList.contains('hidden'));
    if (!secureDialogHidden) {
      throw new Error('Secure input dialog did not close after submission');
    }

    const artifacts = await page.locator('#copilot-agent-thread').textContent() || '';
    const artifactButton = page.locator('[data-copilot-agent-artifact-index]');
    if (!artifacts.includes(connectorName) || artifacts.includes('smoke-private-key') || await artifactButton.count() === 0) {
      throw new Error('Expected redacted connector artifact after secure setup flow');
    }

    await artifactButton.first().click();
    await page.waitForTimeout(600);
    const activeDataCore = await page.locator('[data-module=\"data-core\"]').first().evaluate((element) => element.classList.contains('active'));
    if (!activeDataCore) {
      throw new Error('Artifact navigation did not return to Data Core');
    }

    await textarea.fill('Schedule a single push notification to user_id: smoke_user in half an hour from now. Draft copy to call players back to the game.');
    await sendButton.click();
    await page.waitForTimeout(900);

    const pushHandoff = await page.locator('#copilot-agent-thread').textContent() || '';
    const confirmationButton = page.locator('[data-copilot-agent-confirm]');
    const handoffButton = page.locator('[data-copilot-agent-handoff-index]');
    if (!pushHandoff.toLowerCase().includes('did not send') || !pushHandoff.toLowerCase().includes('draft') || await confirmationButton.count() !== 0 || await handoffButton.count() === 0) {
      throw new Error('Expected drafted push handoff without a chat confirmation button');
    }
    await handoffButton.first().click();
    await page.waitForTimeout(700);
    const preparedPushTitle = await page.locator('#push-dispatch-title-input').inputValue();
    const preparedPushBody = await page.locator('#push-dispatch-body-input').inputValue();
    const preparedPushUserIds = await page.locator('#push-dispatch-user-id-input').inputValue();
    const preparedPushTiming = await page.locator('#push-dispatch-single-timing-select').inputValue();
    const preparedPushSchedule = await page.locator('#push-dispatch-schedule-once-input').inputValue();
    if (!preparedPushTitle || !preparedPushBody || !preparedPushUserIds.includes('smoke_user') || preparedPushTiming !== 'schedule_once' || !preparedPushSchedule) {
      throw new Error('Prepared push copy handoff did not load into scheduled Push Notifications');
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

    const persistedPushHandoff = await page.locator('#copilot-agent-thread').textContent() || '';
    const persistedConfirmationButton = page.locator('[data-copilot-agent-confirm]');
    if (!persistedPushHandoff.toLowerCase().includes('did not send') || await persistedConfirmationButton.count() !== 0) {
      throw new Error('Expected push handoff to persist without a chat confirmation button');
    }

    return {
      sessionStatus: await status.textContent() || '',
      connectorName,
      threadAfterHelp,
      clarifications,
      artifacts,
      pushHandoff,
      persistedPushHandoff,
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
assert_ai_first_operator_ui
assert_module "data-core" "#import-detail-output"
assert_bigquery_connector_ui
assert_module "audience-engine" "#audience-cohort-list"
assert_audience_builder_ui
assert_module "action-orchestrator" "#workflow-delivery-diagnostics-output"
assert_email_campaign_ui
assert_module "experiment-hub" "#experiment-integrity-output"
assert_module "insight-copilot" "#copilot-manual-tools-panel"
exercise_copilot_agent
log_step "Operator console smoke completed."
