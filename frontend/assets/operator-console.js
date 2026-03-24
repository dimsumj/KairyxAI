// KairyxAI operator console runtime. Extracted from index.html for frontend hardening.
        document.addEventListener('DOMContentLoaded', function () {
            const navLinks = document.querySelectorAll('.nav-link');
            const pages = document.querySelectorAll('.page');
            const moduleTitle = document.getElementById('module-title');
            const moduleSubtitle = document.getElementById('module-subtitle');
            const moduleSubnav = document.getElementById('module-subnav');
            const actorRoleSelect = document.getElementById('actor-role-select');
            const actorIdInput = document.getElementById('actor-id-input');
            const tenantIdInput = document.getElementById('tenant-id-input');
            const authStatusText = document.getElementById('auth-status-text');
            const oidcLoginBtn = document.getElementById('oidc-login-btn');
            const oidcLogoutBtn = document.getElementById('oidc-logout-btn');
            const apiKeyInput = document.getElementById('api-key-input');
            const ACTOR_ROLE_STORAGE_KEY = 'kairyx.actorRole';
            const ACTOR_ID_STORAGE_KEY = 'kairyx.actorId';
            const TENANT_ID_STORAGE_KEY = 'kairyx.tenantId';
            const API_KEY_STORAGE_KEY = 'kairyx.apiKey';
            const ACCESS_TOKEN_STORAGE_KEY = 'kairyx.accessToken';
            const OIDC_CODE_VERIFIER_STORAGE_KEY = 'kairyx.oidcCodeVerifier';
            let activeModuleId = 'data-core';
            let activePageId = 'operator-hub';
            let oidcConfig = null;
            let accessToken = '';
            const moduleConfigs = {
                'data-core': {
                    title: 'Data Core',
                    subtitle: 'Manage connectors, imports, mappings, governance checks, and health signals that power the closed-loop lifecycle.',
                    pages: [
                        { id: 'operator-hub', label: 'Churn Rescue' },
                        { id: 'player-cohorts', label: 'Imports' },
                        { id: 'connectors', label: 'Connectors' },
                        { id: 'data-sandbox', label: 'Mappings' },
                        { id: 'action-history', label: 'Audit Trail' },
                        { id: 'scenario-templates', label: 'Templates' },
                        { id: 'service-health', label: 'Health' },
                        { id: 'safety-rails', label: 'Governance' },
                    ],
                },
                'audience-engine': {
                    title: 'Audience Engine',
                    subtitle: 'Build and operate cohorts with SQL workspace support, refresh controls, member previews, metrics, and version-aware rollback.',
                    pages: [{ id: 'audience-engine', label: 'Audience Console' }],
                },
                'action-orchestrator': {
                    title: 'Action Orchestrator',
                    subtitle: 'Configure workflow runtime, publish and test journeys, inspect deliveries, and reconcile provider callbacks into durable execution logs.',
                    pages: [{ id: 'action-orchestrator', label: 'Workflow Runtime' }],
                },
                'experiment-hub': {
                    title: 'Experiment Hub',
                    subtitle: 'Operate treatment-vs-holdout experiments with explicit configuration, exposure and outcome inspection, summary gates, and decision logging.',
                    pages: [{ id: 'experiment-hub', label: 'Experiment Console' }],
                },
                'insight-copilot': {
                    title: 'Insight Copilot',
                    subtitle: 'Run query, explain, recommend, and report flows against curated evidence with query logs, anomaly tracking, and archived reports.',
                    pages: [{ id: 'insight-copilot', label: 'Copilot Workspace' }],
                },
                'help': {
                    title: 'Help',
                    subtitle: 'Read the current v1 manual, follow the end-to-end operator path, and copy sample SQL or JSON payloads that match the live UI.',
                    pages: [{ id: 'help', label: 'Manual & Samples' }],
                },
            };

            function readStoredActorContext() {
                try {
                    return {
                        role: localStorage.getItem(ACTOR_ROLE_STORAGE_KEY) || 'admin',
                        actorId: localStorage.getItem(ACTOR_ID_STORAGE_KEY) || 'admin',
                        tenantId: localStorage.getItem(TENANT_ID_STORAGE_KEY) || 'default',
                        apiKey: localStorage.getItem(API_KEY_STORAGE_KEY) || '',
                    };
                } catch (error) {
                    return { role: 'admin', actorId: 'admin', tenantId: 'default', apiKey: '' };
                }
            }

            function persistActorContext() {
                try {
                    localStorage.setItem(ACTOR_ROLE_STORAGE_KEY, actorRoleSelect.value || 'admin');
                    localStorage.setItem(ACTOR_ID_STORAGE_KEY, actorIdInput.value || actorRoleSelect.value || 'admin');
                    localStorage.setItem(TENANT_ID_STORAGE_KEY, tenantIdInput.value || 'default');
                    localStorage.setItem(API_KEY_STORAGE_KEY, apiKeyInput.value || '');
                } catch (error) {
                    console.warn('Unable to persist actor context:', error);
                }
            }

            const storedActorContext = readStoredActorContext();
            actorRoleSelect.value = storedActorContext.role;
            actorIdInput.value = storedActorContext.actorId;
            tenantIdInput.value = storedActorContext.tenantId;
            apiKeyInput.value = storedActorContext.apiKey;
            try {
                accessToken = localStorage.getItem(ACCESS_TOKEN_STORAGE_KEY) || '';
            } catch (error) {
                accessToken = '';
            }
            if (authStatusText) {
                authStatusText.textContent = accessToken ? 'Validating OIDC session…' : 'Legacy local session';
            }

            actorRoleSelect.addEventListener('change', () => {
                if (!actorIdInput.value.trim() || actorIdInput.value.trim() === readStoredActorContext().role) {
                    actorIdInput.value = actorRoleSelect.value;
                }
                persistActorContext();
                if (activePageId) {
                    activatePage(activePageId);
                }
            });
            actorIdInput.addEventListener('change', () => {
                persistActorContext();
                if (activePageId) {
                    activatePage(activePageId);
                }
            });
            tenantIdInput.addEventListener('change', () => {
                persistActorContext();
                if (accessToken) {
                    hydrateAuthSession().catch((error) => setAuthStatus(error.message || 'Tenant switch failed.'));
                }
                if (activePageId) {
                    activatePage(activePageId);
                }
            });
            apiKeyInput.addEventListener('change', persistActorContext);

            function clearPageIntervals() {
                if (importListInterval) {
                    clearInterval(importListInterval);
                    importListInterval = null;
                }
            }

            function loadPageData(pageId) {
                if (pageId === 'operator-hub') {
                    loadReadyImportsForOperatorHub();
                }
                if (pageId === 'player-cohorts') {
                    loadConfiguredSources();
                    loadImportedDataList();
                    loadImportSchemaContracts(true);
                    importListInterval = setInterval(loadImportedDataList, 3000);
                }
                if (pageId === 'action-history') {
                    loadActionHistory();
                }
                if (pageId === 'scenario-templates') {
                    loadScenarioTemplates();
                }
                if (pageId === 'service-health') {
                    loadServiceHealthStatus();
                }
                if (pageId === 'connectors') {
                    loadSavedConnectors();
                }
                if (pageId === 'data-sandbox') {
                    loadDataSandboxGlance();
                    loadDataSandboxMappingControls();
                }
                if (pageId === 'audience-engine') {
                    loadAudienceEngine();
                }
                if (pageId === 'action-orchestrator') {
                    loadActionOrchestrator();
                }
                if (pageId === 'experiment-hub') {
                    loadExperimentHub();
                }
                if (pageId === 'insight-copilot') {
                    loadInsightCopilot();
                }
            }

            function activatePage(pageId) {
                clearPageIntervals();
                activePageId = pageId;
                pages.forEach((page) => page.classList.remove('active'));
                const page = document.getElementById(pageId);
                if (page) {
                    page.classList.add('active');
                    loadPageData(pageId);
                }
                Array.from(moduleSubnav.querySelectorAll('button')).forEach((button) => {
                    button.classList.toggle('active', button.dataset.page === pageId);
                });
            }

            function renderModuleSubnav(moduleId, preferredPageId = null) {
                const config = moduleConfigs[moduleId];
                moduleTitle.textContent = config.title;
                moduleSubtitle.textContent = config.subtitle;
                moduleSubnav.innerHTML = '';
                config.pages.forEach((entry) => {
                    const button = document.createElement('button');
                    button.type = 'button';
                    button.textContent = entry.label;
                    button.dataset.page = entry.id;
                    if (entry.id === (preferredPageId || config.pages[0].id)) {
                        button.classList.add('active');
                    }
                    button.addEventListener('click', () => activatePage(entry.id));
                    moduleSubnav.appendChild(button);
                });
            }

            function activateModule(moduleId, preferredPageId = null) {
                const config = moduleConfigs[moduleId];
                if (!config) return;
                activeModuleId = moduleId;
                navLinks.forEach((link) => link.classList.toggle('active', link.dataset.module === moduleId));
                const nextPageId = preferredPageId || config.pages[0].id;
                renderModuleSubnav(moduleId, nextPageId);
                activatePage(nextPageId);
            }

            navLinks.forEach((link) => {
                link.addEventListener('click', (event) => {
                    event.preventDefault();
                    activateModule(link.dataset.module);
                });
            });


            // Connectors Page Logic
            const addConnectorBtn = document.getElementById('add-connector-btn');
            const addConnectorCard = document.getElementById('add-connector-card');
            const addConnectorFormContainer = document.getElementById('add-connector-form-container');
            const cancelBtn = document.getElementById('cancel-add-connector-btn');
            const connectorTypeSelect = document.getElementById('connector-type');
            const connectorFieldsDiv = document.getElementById('connector-fields');
            const saveConnectorBtn = document.getElementById('save-connector-btn');
            const connectorListDiv = document.getElementById('connector-list');

            // Default to the current origin so the backend-served frontend works on any port
            // or host. Allow an explicit override for split frontend/backend setups.
            const backendUrl = window.KAIRYX_BACKEND_URL || window.location.origin;
            const apiBaseUrl = `${backendUrl}/api/v1`;
            const HEALTH_CHECK_INTERVAL_MS = 30000;
            const HEALTH_CACHE_TTL_MS = 30000;
            const HEALTH_LIVE_TIMEOUT_MS = 3000;
            const PREDICTION_POLL_INTERVAL_MS = 1000;
            const ingestionConnectorTypes = new Set(['amplitude', 'adjust', 'appsflyer']);
            const connectorTypeLabels = {
                amplitude: 'Amplitude',
                adjust: 'Adjust',
                appsflyer: 'AppsFlyer',
                google: 'Google Gemini',
                bigquery: 'BigQuery',
                sendgrid: 'SendGrid',
                braze: 'Braze',
            };
            let backendMode = 'unknown';
            let cachedConnectors = [];
            let cachedImports = [];
            let cachedPredictionJobs = [];
            let cachedPredictionModelReadiness = null;
            let cachedExportJobs = [];
            let cachedHealthState = null;
            let cachedHealthStateFetchedAt = 0;
            let healthStateRequest = null;

            function setAuthStatus(message) {
                if (authStatusText) {
                    authStatusText.textContent = message;
                }
            }

            function persistAccessToken(token) {
                accessToken = token || '';
                try {
                    if (accessToken) {
                        localStorage.setItem(ACCESS_TOKEN_STORAGE_KEY, accessToken);
                    } else {
                        localStorage.removeItem(ACCESS_TOKEN_STORAGE_KEY);
                    }
                } catch (error) {
                    console.warn('Unable to persist access token:', error);
                }
            }

            function clearBearerSession() {
                persistAccessToken('');
                try {
                    localStorage.removeItem(OIDC_CODE_VERIFIER_STORAGE_KEY);
                } catch (error) {
                    console.warn('Unable to clear PKCE verifier:', error);
                }
                setAuthStatus('Legacy local session');
            }

            function base64UrlEncode(buffer) {
                const bytes = buffer instanceof Uint8Array ? buffer : new Uint8Array(buffer);
                let binary = '';
                for (let index = 0; index < bytes.byteLength; index += 1) {
                    binary += String.fromCharCode(bytes[index]);
                }
                return btoa(binary).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/g, '');
            }

            async function sha256Digest(value) {
                const digest = await window.crypto.subtle.digest('SHA-256', new TextEncoder().encode(value));
                return base64UrlEncode(digest);
            }

            function randomVerifier() {
                const bytes = new Uint8Array(32);
                window.crypto.getRandomValues(bytes);
                return base64UrlEncode(bytes);
            }

            function redirectUri() {
                return `${window.location.origin}${window.location.pathname}`;
            }

            async function loadOidcConfig() {
                try {
                    const response = await fetch(`${apiBaseUrl}/auth/oidc-config`);
                    oidcConfig = response.ok ? await response.json() : null;
                } catch (error) {
                    oidcConfig = null;
                }
                return oidcConfig;
            }

            async function exchangeAuthorizationCode(code, verifier) {
                if (!oidcConfig || !oidcConfig.token_url) {
                    throw new Error('OIDC token endpoint is not configured.');
                }
                const form = new URLSearchParams({
                    grant_type: 'authorization_code',
                    code,
                    client_id: oidcConfig.client_id || '',
                    code_verifier: verifier,
                    redirect_uri: redirectUri(),
                });
                if (oidcConfig.audience) {
                    form.set('audience', oidcConfig.audience);
                }
                const response = await fetch(oidcConfig.token_url, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
                    body: form.toString(),
                });
                const payload = await response.json().catch(() => ({}));
                if (!response.ok || !payload.access_token) {
                    throw new Error(payload.error_description || payload.detail || 'OIDC code exchange failed.');
                }
                persistAccessToken(payload.access_token);
            }

            async function handleOidcRedirect() {
                const params = new URLSearchParams(window.location.search);
                const code = params.get('code');
                if (!code) {
                    return;
                }
                let verifier = '';
                try {
                    verifier = localStorage.getItem(OIDC_CODE_VERIFIER_STORAGE_KEY) || '';
                } catch (error) {
                    verifier = '';
                }
                if (!verifier) {
                    setAuthStatus('OIDC callback is missing the PKCE verifier.');
                    return;
                }
                await exchangeAuthorizationCode(code, verifier);
                try {
                    localStorage.removeItem(OIDC_CODE_VERIFIER_STORAGE_KEY);
                } catch (error) {
                    console.warn('Unable to clear PKCE verifier:', error);
                }
                window.history.replaceState({}, document.title, redirectUri());
            }

            async function hydrateAuthSession() {
                if (!accessToken) {
                    setAuthStatus(oidcConfig && oidcConfig.authorize_url ? 'OIDC available. Login required.' : 'Legacy local session');
                    return;
                }
                const tenantId = (tenantIdInput.value || 'default').trim() || 'default';
                const response = await fetch(`${apiBaseUrl}/auth/me`, {
                    headers: {
                        Authorization: `Bearer ${accessToken}`,
                        'X-Kairyx-Tenant': tenantId,
                    },
                });
                const payload = await response.json().catch(() => ({}));
                if (!response.ok) {
                    clearBearerSession();
                    throw new Error(payload.detail || 'OIDC session validation failed.');
                }
                actorIdInput.value = payload.actor_id || actorIdInput.value;
                actorRoleSelect.value = payload.actor_role || actorRoleSelect.value;
                tenantIdInput.value = payload.tenant_id || tenantId;
                persistActorContext();
                setAuthStatus(`OIDC ${payload.actor_id || 'user'} @ ${tenantIdInput.value}`);
            }

            async function startOidcLogin() {
                await loadOidcConfig();
                if (!oidcConfig || !oidcConfig.authorize_url || !oidcConfig.client_id) {
                    setAuthStatus('OIDC is not configured on the backend.');
                    return;
                }
                const verifier = randomVerifier();
                const challenge = await sha256Digest(verifier);
                try {
                    localStorage.setItem(OIDC_CODE_VERIFIER_STORAGE_KEY, verifier);
                } catch (error) {
                    console.warn('Unable to persist PKCE verifier:', error);
                }
                const params = new URLSearchParams({
                    response_type: 'code',
                    client_id: oidcConfig.client_id,
                    redirect_uri: redirectUri(),
                    code_challenge: challenge,
                    code_challenge_method: 'S256',
                    scope: 'openid profile email',
                });
                if (oidcConfig.audience) {
                    params.set('audience', oidcConfig.audience);
                }
                window.location.assign(`${oidcConfig.authorize_url}?${params.toString()}`);
            }

            function buildApiHeaders(includeJsonContentType = false) {
                const tenantId = (tenantIdInput.value || 'default').trim() || 'default';
                const headers = {};
                if (accessToken) {
                    headers.Authorization = `Bearer ${accessToken}`;
                    headers['X-Kairyx-Tenant'] = tenantId;
                } else {
                    headers['x-actor-role'] = actorRoleSelect.value || 'admin';
                    headers['x-actor-id'] = (actorIdInput.value || actorRoleSelect.value || 'admin').trim();
                    headers['x-tenant-id'] = tenantId;
                }
                if (!accessToken && (apiKeyInput.value || '').trim()) {
                    headers['x-api-key'] = apiKeyInput.value.trim();
                }
                if (includeJsonContentType) {
                    headers['Content-Type'] = 'application/json';
                }
                return headers;
            }

            async function apiRequest(path, options = {}) {
                const { method = 'GET', body } = options;
                const headers = buildApiHeaders(Boolean(body));
                const response = await fetch(`${apiBaseUrl}${path}`, {
                    method,
                    headers,
                    body: body ? JSON.stringify(body) : undefined,
                });
                if (response.status === 204) {
                    return null;
                }
                const payload = await response.json().catch(() => ({}));
                if (!response.ok) {
                    if (response.status === 401 && accessToken) {
                        clearBearerSession();
                        setAuthStatus('OIDC session expired.');
                    }
                    const error = new Error(payload.detail || payload.message || `Request failed (${response.status})`);
                    error.status = response.status;
                    error.payload = payload;
                    throw error;
                }
                return payload;
            }

            oidcLoginBtn.addEventListener('click', async () => {
                try {
                    await startOidcLogin();
                } catch (error) {
                    setAuthStatus(error.message || 'OIDC login failed.');
                }
            });

            oidcLogoutBtn.addEventListener('click', () => {
                const logoutUrl = oidcConfig && oidcConfig.logout_url;
                clearBearerSession();
                if (logoutUrl) {
                    window.location.assign(logoutUrl);
                }
            });

            async function fetchHealthLiveState() {
                const controller = new AbortController();
                const timeoutId = window.setTimeout(() => controller.abort(), HEALTH_LIVE_TIMEOUT_MS);
                try {
                    const response = await fetch(`${apiBaseUrl}/health/live`, {
                        method: 'GET',
                        headers: buildApiHeaders(false),
                        signal: controller.signal,
                    });
                    if (response.status === 204) {
                        return null;
                    }
                    const payload = await response.json().catch(() => ({}));
                    if (!response.ok) {
                        throw new Error(payload.detail || payload.message || `Request failed (${response.status})`);
                    }
                    return payload;
                } catch (error) {
                    if (error && error.name === 'AbortError') {
                        throw new Error('Backend health check timed out.');
                    }
                    throw error;
                } finally {
                    window.clearTimeout(timeoutId);
                }
            }

            function parseIsoDate(value) {
                const parsed = new Date(value || Date.now());
                return Number.isNaN(parsed.getTime()) ? new Date() : parsed;
            }

            function addDaysIso(value, days) {
                const parsed = parseIsoDate(value);
                parsed.setDate(parsed.getDate() + days);
                return parsed.toISOString();
            }

            function formatConnectorLabel(type) {
                return connectorTypeLabels[type] || String(type || '').replace(/[_-]+/g, ' ').replace(/\b\w/g, (c) => c.toUpperCase());
            }

            function formatImportTimestamp(value) {
                const parsed = parseIsoDate(value);
                const pad = (part) => String(part).padStart(2, '0');
                return `${parsed.getFullYear()}${pad(parsed.getMonth() + 1)}${pad(parsed.getDate())}-${pad(parsed.getHours())}${pad(parsed.getMinutes())}${pad(parsed.getSeconds())}`;
            }

            function formatImportDisplayName(sourceName, createdAt) {
                const safeSourceName = String(sourceName || 'Import').trim() || 'Import';
                return `${safeSourceName}-${formatImportTimestamp(createdAt)}`;
            }

            function mapImportStatus(status) {
                const normalized = String(status || '').toLowerCase();
                if (normalized === 'completed') return 'Ready to Use';
                if (normalized === 'running' || normalized === 'queued') return 'Processing';
                if (normalized === 'stopping') return 'Stopping';
                if (normalized === 'stopped') return 'Stopped';
                if (normalized === 'failed') return 'Failed';
                return normalized ? normalized.replace(/\b\w/g, (c) => c.toUpperCase()) : 'Processing';
            }

            function mapPredictionStatus(status) {
                const normalized = String(status || '').toLowerCase();
                if (normalized === 'completed') return 'Ready';
                if (normalized === 'running') return 'Processing';
                if (normalized === 'queued') return 'Queued';
                if (normalized === 'stopping') return 'Stopping';
                if (normalized === 'stopped') return 'Stopped';
                if (normalized === 'failed') return 'Failed';
                return normalized ? normalized.replace(/\b\w/g, (c) => c.toUpperCase()) : 'Queued';
            }

            function nextConnectorName(type) {
                const label = formatConnectorLabel(type);
                let maxIndex = 0;
                cachedConnectors.forEach((connector) => {
                    const name = String(connector.name || '');
                    if (!name.startsWith(label)) return;
                    const suffix = name.slice(label.length).trim();
                    if (!suffix) {
                        maxIndex = Math.max(maxIndex, 1);
                        return;
                    }
                    const parsed = parseInt(suffix, 10);
                    if (!Number.isNaN(parsed)) {
                        maxIndex = Math.max(maxIndex, parsed);
                    }
                });
                return `${label} ${Math.max(1, maxIndex + 1)}`;
            }

            function normalizeConnector(connector) {
                return {
                    ...connector,
                    details: 'Configured',
                };
            }

            function normalizeImportJob(job) {
                const spec = job.spec || {};
                const progress = job.progress || {};
                const details = progress.details || {};
                const createdAt = job.created_at || job.timestamp || new Date().toISOString();
                const sourceName = spec.source_name || job.source_name || details.source || '';
                const connector = cachedConnectors.find((item) => item.name === sourceName) || null;
                const displayName = String(spec.display_name || '').trim() || formatImportDisplayName(sourceName || 'Import', createdAt);
                return {
                    id: job.id,
                    name: displayName,
                    status: mapImportStatus(job.status),
                    raw_status: job.status,
                    current_step: mapImportStatus(job.status),
                    progress_pct: Number(progress.pct || 0),
                    timestamp: createdAt,
                    created_at: createdAt,
                    updated_at: job.updated_at,
                    start_date: spec.start_date || '',
                    end_date: spec.end_date || '',
                    expiration_timestamp: addDaysIso(job.updated_at || createdAt, 7),
                    source_stats: sourceName ? [{
                        source: sourceName,
                        type: spec.connector_type || (connector && connector.type) || '',
                        ingested_events: Number(details.events_staged || progress.current || 0),
                        status: mapImportStatus(job.status),
                        error: job.error || null,
                    }] : [],
                    processing_stats: details.processing || null,
                    error: job.error || null,
                    spec,
                    progress,
                };
            }

            function getImportFailureReason(job) {
                return String(
                    job.error
                    || ((job.progress || {}).details || {}).failure_reason
                    || ((((job.source_stats || [])[0] || {}).error))
                    || ''
                ).trim();
            }

            function getImportFailureStage(job) {
                return String(
                    (((job.progress || {}).details || {}).failure_stage)
                    || (((job.progress || {}).details || {}).phase)
                    || ''
                ).trim();
            }

            function getImportFailureTooltip(job) {
                const reason = getImportFailureReason(job);
                if (!reason) {
                    return '';
                }
                const details = ((job.progress || {}).details || {});
                const checkpointState = details.checkpoint_state || {};
                const lines = [`<strong>Reason:</strong> ${escapeHtml(reason)}`];
                const stage = getImportFailureStage(job);
                if (stage) {
                    lines.push(`<strong>Phase:</strong> ${escapeHtml(stage.replace(/_/g, ' '))}`);
                }
                const processed = Number(checkpointState.processed || 0);
                const failed = Number(checkpointState.failed || 0);
                const pending = Number(checkpointState.pending || 0);
                if (processed || failed || pending) {
                    lines.push(`<strong>Checkpoints:</strong> processed ${processed}, failed ${failed}, pending ${pending}`);
                }
                const processedManifests = Number(details.processed_manifests || 0);
                const totalManifests = Number(details.total_manifests || 0);
                if (totalManifests > 0) {
                    lines.push(`<strong>Processed manifests:</strong> ${processedManifests} / ${totalManifests}`);
                }
                return lines.join('<br>');
            }

            function getImportStatusClass(job) {
                if (job.status === 'Ready to Use') return 'status-ok';
                if (job.status === 'Stopped') return 'status-neutral';
                if (job.status === 'Processing' || job.status === 'Queued' || job.status === 'Stopping') return 'status-warning';
                return 'status-error';
            }

            function renderImportStatus(job, statusClass) {
                const status = escapeHtml(job.status || 'Processing');
                const tooltip = getImportFailureTooltip(job);
                const progressText = getImportProgressText(job);
                const statusLabel = progressText
                    ? `${status} <span style="font-size: 0.8rem; color: var(--text-secondary);">${escapeHtml(progressText)}</span>`
                    : status;
                const statusText = tooltip
                    ? `<span class="status-label">${statusLabel}<span class="status-help-wrap"><button type="button" class="status-help" aria-label="Show import failure reason">?</button><span class="status-tooltip" role="tooltip">${tooltip}</span></span></span>`
                    : statusLabel;
                return `<span class="status-indicator ${statusClass}" style="display: inline-block; vertical-align: middle;"></span> ${statusText}`;
            }

            function latestByCreatedAt(items) {
                return [...items].sort((a, b) => parseIsoDate(b.created_at).getTime() - parseIsoDate(a.created_at).getTime());
            }

            async function refreshConnectorsState() {
                const connectors = await apiRequest('/connectors');
                cachedConnectors = Array.isArray(connectors) ? connectors.map(normalizeConnector) : [];
                return cachedConnectors;
            }

            async function refreshImportsState() {
                const payload = await apiRequest('/imports');
                const items = Array.isArray(payload.items) ? payload.items : [];
                cachedImports = items.map(normalizeImportJob);
                return cachedImports;
            }

            async function refreshPredictionJobsState() {
                const payload = await apiRequest('/predictions');
                cachedPredictionJobs = latestByCreatedAt(Array.isArray(payload.items) ? payload.items : []);
                return cachedPredictionJobs;
            }

            function buildDefaultPredictionModelReadiness() {
                return {
                    state: 'untrained',
                    using_model_version: 'heuristic_v1',
                    reason: 'No local model training has been run yet. Local predictions use heuristic_v1.',
                    last_trained_at: null,
                    baseline_rows: 0,
                    min_rows_required: 12,
                    class_balance: {},
                    validation_accuracy: null,
                    heuristic_accuracy: null,
                };
            }

            async function refreshPredictionModelReadinessState() {
                try {
                    const payload = await apiRequest('/predictions/models/runs');
                    cachedPredictionModelReadiness = payload.readiness || buildDefaultPredictionModelReadiness();
                } catch (error) {
                    console.warn('Unable to refresh prediction model readiness:', error);
                    cachedPredictionModelReadiness = buildDefaultPredictionModelReadiness();
                }
                return cachedPredictionModelReadiness;
            }

            async function refreshExportJobsState() {
                const payload = await apiRequest('/exports');
                cachedExportJobs = latestByCreatedAt(Array.isArray(payload.items) ? payload.items : []);
                return cachedExportJobs;
            }

            function getConfiguredSourcesFromState() {
                return cachedConnectors
                    .filter((connector) => ingestionConnectorTypes.has(String(connector.type || '').toLowerCase()))
                    .map((connector) => ({
                        id: connector.name,
                        name: connector.name,
                        type: connector.type,
                    }));
            }

            function hasConfiguredGeminiConnector() {
                return cachedConnectors.some((connector) => (
                    String(connector.type || '').toLowerCase() === 'google'
                    && Boolean((connector.config || {}).api_key)
                ));
            }

            function getPredictionModeLabel(predictionMode = 'local') {
                switch (String(predictionMode || 'local').toLowerCase()) {
                    case 'ai':
                        return 'AI';
                    case 'cloud':
                        return 'Cloud';
                    case 'parallel':
                        return 'AI + Cloud';
                    default:
                        return 'Local Model';
                }
            }

            function getPredictionModelReadiness() {
                return cachedPredictionModelReadiness || buildDefaultPredictionModelReadiness();
            }

            function getPredictionModelBadgeLabel(state = '') {
                switch (String(state || '').toLowerCase()) {
                    case 'ready':
                        return 'Ready';
                    case 'fallback':
                        return 'Fallback';
                    default:
                        return 'Learning';
                }
            }

            function getPredictionModelBadgeStyles(state = '') {
                switch (String(state || '').toLowerCase()) {
                    case 'ready':
                        return { color: 'var(--green)', borderColor: 'var(--green)' };
                    case 'fallback':
                        return { color: 'var(--yellow)', borderColor: 'var(--yellow)' };
                    default:
                        return { color: 'var(--primary-color)', borderColor: 'var(--border-color)' };
                }
            }

            function formatPredictionModelMetric(value) {
                const numeric = Number(value);
                if (!Number.isFinite(numeric)) {
                    return '';
                }
                return `${(numeric * 100).toFixed(1)}%`;
            }

            function renderPredictionModelReadiness() {
                const readiness = getPredictionModelReadiness();
                const badgeLabel = getPredictionModelBadgeLabel(readiness.state);
                const badgeStyles = getPredictionModelBadgeStyles(readiness.state);
                const usingModelVersion = String(readiness.using_model_version || 'heuristic_v1');
                const baselineRows = Number(readiness.baseline_rows || 0);
                const minRowsRequired = Number(readiness.min_rows_required || 12);
                const validationAccuracy = formatPredictionModelMetric(readiness.validation_accuracy);
                const heuristicAccuracy = formatPredictionModelMetric(readiness.heuristic_accuracy);
                const detailParts = [
                    usingModelVersion,
                    `${baselineRows}/${minRowsRequired} labeled rows`,
                ];

                if (validationAccuracy && heuristicAccuracy) {
                    detailParts.push(`val ${validationAccuracy} vs heur ${heuristicAccuracy}`);
                }
                if (readiness.last_trained_at) {
                    detailParts.push(`trained ${formatDateTime(readiness.last_trained_at)}`);
                }

                const titleLines = [
                    String(readiness.reason || '').trim(),
                    `Using model: ${usingModelVersion}`,
                    `Labeled rows: ${baselineRows}/${minRowsRequired}`,
                ];
                if (validationAccuracy && heuristicAccuracy) {
                    titleLines.push(`Validation accuracy: ${validationAccuracy}`);
                    titleLines.push(`Heuristic accuracy: ${heuristicAccuracy}`);
                }
                if (readiness.last_trained_at) {
                    titleLines.push(`Last trained: ${formatDateTime(readiness.last_trained_at)}`);
                }

                if (predictionModelReadinessBadge) {
                    predictionModelReadinessBadge.textContent = badgeLabel;
                    predictionModelReadinessBadge.style.color = badgeStyles.color;
                    predictionModelReadinessBadge.style.borderColor = badgeStyles.borderColor;
                    predictionModelReadinessBadge.title = titleLines.filter(Boolean).join('\n');
                }
                if (predictionModelReadinessDetails) {
                    predictionModelReadinessDetails.textContent = detailParts.filter(Boolean).join(' · ');
                    predictionModelReadinessDetails.title = titleLines.filter(Boolean).join('\n');
                }
                if (predictionLocalWarning) {
                    if (getSelectedPredictionMode() === 'local' && String(readiness.state || '').toLowerCase() !== 'ready') {
                        predictionLocalWarning.textContent = 'Using heuristic fallback until the local model has enough labeled data.';
                    } else {
                        predictionLocalWarning.textContent = '';
                    }
                }
            }

            function getSelectedPredictionMode() {
                return String((predictionModeSelect && predictionModeSelect.value) || 'local').toLowerCase();
            }

            function syncPredictionModeSelection(job = null) {
                if (!predictionModeSelect) {
                    return;
                }
                const jobMode = String((((job || {}).spec || {}).prediction_mode) || '').toLowerCase();
                predictionModeSelect.value = jobMode || getSelectedPredictionMode();
                renderPredictionModelReadiness();
            }

            function getPredictionStartStatusMessage(job = null, predictionMode = getSelectedPredictionMode()) {
                if (job) {
                    return formatPredictionProgressText({
                        status: mapPredictionStatus(job.status),
                        progress: job.progress,
                    });
                }
                return formatPredictionProgressText({
                    status: backendMode !== 'mock' ? 'Queued' : 'Starting',
                    progress: {
                        current: 0,
                        total: 0,
                        pct: 0,
                        details: {
                            execution_label: getPredictionModeLabel(predictionMode),
                        },
                    },
                });
            }

            async function ensureHealthState(forceRefresh = false) {
                const now = Date.now();
                if (!forceRefresh && cachedHealthState && (now - cachedHealthStateFetchedAt) < HEALTH_CACHE_TTL_MS) {
                    return cachedHealthState;
                }
                if (!forceRefresh && healthStateRequest) {
                    return healthStateRequest;
                }

                healthStateRequest = fetchHealthLiveState()
                    .then((payload) => {
                        backendMode = payload?.mode || backendMode;
                        cachedHealthState = payload;
                        cachedHealthStateFetchedAt = Date.now();
                        return payload;
                    })
                    .catch((error) => {
                        if (forceRefresh) {
                            cachedHealthState = null;
                            cachedHealthStateFetchedAt = 0;
                        }
                        throw error;
                    })
                    .finally(() => {
                        healthStateRequest = null;
                    });
                return healthStateRequest;
            }

            async function createConnectorRecord(type, config) {
                const name = nextConnectorName(type);
                const connector = await apiRequest('/connectors', {
                    method: 'POST',
                    body: {
                        name,
                        type,
                        config,
                    },
                });
                await refreshConnectorsState();
                return connector;
            }

            async function createImportRecord(sourceName, startDate, endDate) {
                await ensureHealthState().catch(() => null);
                const created = await apiRequest('/imports', {
                    method: 'POST',
                    body: {
                        source_name: sourceName,
                        start_date: startDate,
                        end_date: endDate,
                    },
                });
                if (backendMode === 'mock') {
                    setInlineStatus(importListStatus, `Created import ${created.id}. Running locally...`);
                    queueMockImportRun(created.id);
                }
                refreshImportsState().catch((error) => {
                    console.error('Unable to refresh import jobs after create:', error);
                });
                return normalizeImportJob(created);
            }

            async function queueMockImportRun(jobId) {
                try {
                    const job = await apiRequest(`/imports/${encodeURIComponent(jobId)}/run`, { method: 'POST' });
                    setInlineStatus(importListStatus, `Import ${job.name || jobId} completed.`);
                } catch (error) {
                    const failedJob = error && error.payload && error.payload.job ? normalizeImportJob(error.payload.job) : null;
                    const failureReason = (failedJob && getImportFailureReason(failedJob)) || error.message || 'Import failed.';
                    setInlineStatus(importListStatus, `Import ${jobId} failed: ${failureReason}`, true);
                    console.error(`Import job ${jobId} failed:`, error);
                } finally {
                    try {
                        await loadImportedDataList();
                    } catch (refreshError) {
                        console.error(`Unable to refresh import list for ${jobId}:`, refreshError);
                    }
                }
            }

            async function stopImportRecord(jobId) {
                const job = await apiRequest(`/imports/${encodeURIComponent(jobId)}/stop`, { method: 'POST' });
                await refreshImportsState();
                return cachedImports.find((item) => item.id === jobId) || normalizeImportJob(job);
            }

            async function deleteImportRecord(jobId) {
                await apiRequest(`/imports/${encodeURIComponent(jobId)}`, { method: 'DELETE' });
                await refreshImportsState();
            }

            function getLatestPredictionJob(importJobId, completedOnly = false) {
                return getLatestPredictionJobForMode(importJobId, completedOnly);
            }

            function getPredictionJobMode(job = {}) {
                return String((((job || {}).spec || {}).prediction_mode) || 'local').toLowerCase();
            }

            function getLatestPredictionJobForMode(importJobId, completedOnly = false, predictionMode = '') {
                const normalizedMode = String(predictionMode || '').toLowerCase();
                return cachedPredictionJobs.find((job) => {
                    const matchesImport = String((job.spec || {}).import_job_id || '') === String(importJobId);
                    if (!matchesImport) return false;
                    if (normalizedMode && getPredictionJobMode(job) !== normalizedMode) return false;
                    if (!completedOnly) return true;
                    return String(job.status || '').toLowerCase() === 'completed';
                }) || null;
            }

            function getLatestActivePredictionJob(importJobId, predictionMode = '') {
                const normalizedMode = String(predictionMode || '').toLowerCase();
                return cachedPredictionJobs.find((job) => (
                    String((job.spec || {}).import_job_id || '') === String(importJobId)
                    && (!normalizedMode || getPredictionJobMode(job) === normalizedMode)
                    && isPredictionJobActive(job)
                )) || null;
            }

            function getLatestCompletedPredictionJob(importJobId, excludeJobId = null, predictionMode = '') {
                const normalizedMode = String(predictionMode || '').toLowerCase();
                return cachedPredictionJobs.find((job) => {
                    if (excludeJobId && String(job.id || '') === String(excludeJobId)) {
                        return false;
                    }
                    return (
                        String((job.spec || {}).import_job_id || '') === String(importJobId)
                        && (!normalizedMode || getPredictionJobMode(job) === normalizedMode)
                        && String(job.status || '').toLowerCase() === 'completed'
                    );
                }) || null;
            }

            function isPredictionJobActive(job) {
                const status = String((job && job.status) || '').toLowerCase();
                return ['queued', 'running', 'stopping'].includes(status);
            }

            function clearBaselinePredictionRows() {
                baselinePredictionPlayers = [];
                baselinePredictionJobId = null;
                baselinePredictionImportJobId = null;
            }

            function mergePredictionRows(priorityRows = [], existingRows = []) {
                const combined = [];
                const seenUserIds = new Set();

                const appendUniqueRow = (row, index, prefix) => {
                    const key = String(row.user_id || row.player_id || `${prefix}-${index}`);
                    if (seenUserIds.has(key)) {
                        return;
                    }
                    seenUserIds.add(key);
                    combined.push(row);
                };

                priorityRows.forEach((row, index) => appendUniqueRow(row, index, 'priority'));
                existingRows.forEach((row, index) => appendUniqueRow(row, index, 'existing'));
                return combined;
            }

            async function fetchPredictionRows(jobId) {
                const rows = [];
                let page = 1;
                let total = null;
                do {
                    const payload = await apiRequest(`/predictions/${encodeURIComponent(jobId)}/results?page=${page}&page_size=500`);
                    const items = Array.isArray(payload.items) ? payload.items : [];
                    rows.push(...items.map((item) => ({
                        ...item,
                        ltv: Number(item.ltv || 0),
                        session_count: Number(item.session_count || 0),
                        event_count: Number(item.event_count || 0),
                    })));
                    total = Number(payload.total || 0);
                    if (rows.length >= total || items.length === 0) {
                        break;
                    }
                    page += 1;
                } while (true);
                return rows;
            }

            async function loadBaselinePredictionRows(importJobId, excludeJobId = null, predictionMode = '') {
                const normalizedImportJobId = String(importJobId || '');
                if (!normalizedImportJobId) {
                    clearBaselinePredictionRows();
                    return [];
                }

                const latestCompletedJob = getLatestCompletedPredictionJob(
                    normalizedImportJobId,
                    excludeJobId,
                    predictionMode,
                );
                if (!latestCompletedJob) {
                    clearBaselinePredictionRows();
                    baselinePredictionImportJobId = normalizedImportJobId;
                    return [];
                }

                if (
                    baselinePredictionImportJobId === normalizedImportJobId
                    && baselinePredictionJobId === String(latestCompletedJob.id || '')
                ) {
                    return baselinePredictionPlayers;
                }

                baselinePredictionPlayers = await fetchPredictionRows(latestCompletedJob.id);
                baselinePredictionImportJobId = normalizedImportJobId;
                baselinePredictionJobId = String(latestCompletedJob.id || '');
                return baselinePredictionPlayers;
            }

            async function renderCompletedPredictionJob(completedJob, importJobId) {
                const completedRows = await fetchPredictionRows(completedJob.id);
                activePredictionJobId = null;
                clearPersistedActivePredictionJob();
                predictionStopRequested = false;
                baselinePredictionPlayers = [...completedRows];
                baselinePredictionImportJobId = String(importJobId || '');
                baselinePredictionJobId = String(completedJob.id || '');
                allChurnPredictionPlayers = [...completedRows];
                renderChurnTable(completedRows.length > 0 ? undefined : 'No prediction results available.');
                renderPredictionProgress({
                    status: 'Ready',
                    progress: completedJob.progress,
                });
                setPredictionActionState('idle');
                pushAudienceBtn.disabled = completedRows.length === 0;
            }

            async function createPredictionRecord(importJobId, predictionMode = 'local') {
                await ensureHealthState().catch(() => null);
                const created = await apiRequest('/predictions', {
                    method: 'POST',
                    body: {
                        import_job_id: importJobId,
                        prediction_mode: predictionMode,
                    },
                });
                if (backendMode === 'mock') {
                    queueMockPredictionRun(created.id);
                }
                return created;
            }

            async function queueMockPredictionRun(jobId) {
                try {
                    await apiRequest(`/predictions/${encodeURIComponent(jobId)}/run`, { method: 'POST' });
                } catch (error) {
                    console.error(`Prediction job ${jobId} failed:`, error);
                } finally {
                    try {
                        await refreshPredictionJobsState();
                    } catch (refreshError) {
                        console.error(`Unable to refresh prediction jobs for ${jobId}:`, refreshError);
                    }
                }
            }

            async function stopPredictionRecord(jobId) {
                const job = await apiRequest(`/predictions/${encodeURIComponent(jobId)}/stop`, { method: 'POST' });
                await refreshPredictionJobsState();
                return cachedPredictionJobs.find((item) => item.id === jobId) || job;
            }

            async function createExportRecord(predictionJobId, payload) {
                await ensureHealthState().catch(() => null);
                const created = await apiRequest('/exports', {
                    method: 'POST',
                    body: {
                        prediction_job_id: predictionJobId,
                        provider: payload.provider,
                        channel: payload.channel,
                        include_churned: payload.include_churned,
                        include_risks: payload.include_risks,
                        audience_name: payload.audience_name,
                        webhook_url: payload.webhook_url,
                        webhook_token: payload.webhook_token,
                    },
                });
                if (backendMode === 'mock') {
                    return apiRequest(`/exports/${encodeURIComponent(created.id)}/run`, { method: 'POST' });
                }
                return created;
            }

            function buildActionHistoryItems() {
                const items = [];
                cachedConnectors.forEach((connector) => {
                    items.push({
                        timestamp: connector.created_at || connector.updated_at,
                        summary: `Configure Connector: ${connector.name}`,
                        status: 'saved',
                        details: `type=${connector.type}`,
                    });
                });
                cachedImports.forEach((job) => {
                    items.push({
                        timestamp: job.created_at || job.timestamp,
                        summary: `Start Import from ${job.source_stats?.[0]?.source || job.name}`,
                        status: 'started',
                        details: `range=${job.start_date || '-'} to ${job.end_date || '-'}`,
                    });
                    if (job.raw_status === 'completed') {
                        items.push({
                            timestamp: job.updated_at || job.timestamp,
                            summary: `Import Ready: ${job.name}`,
                            status: 'completed',
                            details: `events=${job.source_stats?.[0]?.ingested_events || 0}`,
                        });
                    }
                    if (job.raw_status === 'failed') {
                        items.push({
                            timestamp: job.updated_at || job.timestamp,
                            summary: `Import Failed: ${job.name}`,
                            status: 'failed',
                            details: job.error || 'Import failed.',
                        });
                    }
                    if (job.raw_status === 'stopped') {
                        items.push({
                            timestamp: job.updated_at || job.timestamp,
                            summary: `Import Stopped: ${job.name}`,
                            status: 'stopped',
                            details: ((job.progress || {}).details || {}).stop_reason || 'Stopped by user.',
                        });
                    }
                });
                cachedPredictionJobs.forEach((job) => {
                    items.push({
                        timestamp: job.created_at,
                        summary: `Run Prediction for ${(job.spec || {}).import_job_id || job.id}`,
                        status: 'started',
                        details: `mode=${(job.spec || {}).prediction_mode || 'local'}`,
                    });
                    if (String(job.status || '').toLowerCase() === 'completed') {
                        items.push({
                            timestamp: job.updated_at,
                            summary: `Prediction Ready for ${(job.spec || {}).import_job_id || job.id}`,
                            status: 'completed',
                            details: `rows=${(((job.progress || {}).details || {}).rows_written || (job.progress || {}).current || 0)}`,
                        });
                    }
                });
                cachedExportJobs.forEach((job) => {
                    const details = (job.progress || {}).details || {};
                    const spec = job.spec || {};
                    items.push({
                        timestamp: job.created_at,
                        summary: `Queue Audience Export to ${String(spec.provider || 'webhook').replace(/\b\w/g, (c) => c.toUpperCase())}`,
                        status: 'started',
                        details: `channel=${spec.channel || 'push_notification'}`,
                    });
                    if (String(job.status || '').toLowerCase() === 'completed') {
                        items.push({
                            timestamp: job.updated_at,
                            summary: `Push Audience to ${String(details.provider || spec.provider || 'webhook').replace(/\b\w/g, (c) => c.toUpperCase())}`,
                            status: 'completed',
                            details: `count=${details.count || 0}, channel=${spec.channel || 'push_notification'}`,
                        });
                    }
                });
                return items.sort((a, b) => parseIsoDate(b.timestamp).getTime() - parseIsoDate(a.timestamp).getTime());
            }

            function getPathValue(raw, path) {
                if (!path) return null;
                let current = raw;
                for (const part of String(path).split('.')) {
                    if (!current || typeof current !== 'object') return null;
                    current = current[part];
                }
                return current;
            }

            function pickValue(raw, keys, overridePath) {
                if (overridePath) {
                    const overrideValue = getPathValue(raw, overridePath);
                    if (overrideValue !== null && overrideValue !== undefined && overrideValue !== '') {
                        return overrideValue;
                    }
                }
                for (const key of keys) {
                    const value = raw[key];
                    if (value !== null && value !== undefined && value !== '') {
                        return value;
                    }
                }
                return null;
            }

            function toIsoTimestamp(value) {
                if (value === null || value === undefined || value === '') {
                    return new Date().toISOString();
                }
                if (typeof value === 'number') {
                    return new Date(value > 1e12 ? value : value * 1000).toISOString();
                }
                const text = String(value).endsWith('Z') ? String(value).slice(0, -1) : String(value);
                const parsed = new Date(text);
                if (Number.isNaN(parsed.getTime())) {
                    return new Date().toISOString();
                }
                return parsed.toISOString();
            }

            function previewMappedEvent(source, raw, mapping = {}) {
                const eventTime = toIsoTimestamp(pickValue(raw, ['event_time', 'timestamp', 'install_time', 'time'], mapping.event_time));
                return {
                    player_id: String(pickValue(raw, ['player_id', 'user_id', 'uid', 'PID', 'customer_user_id', 'appsflyer_id', 'idfa', 'adid'], mapping.canonical_user_id) || 'unknown_user'),
                    event_type: String(pickValue(raw, ['event_type', 'event_name', 'name'], mapping.event_name) || 'attribution_event'),
                    event_time: eventTime,
                    event_date: eventTime.slice(0, 10),
                    source,
                    source_event_id: pickValue(raw, ['event_id', 'id', 'insert_id', 'uuid'], mapping.source_event_id) || null,
                    schema_version: 'v1',
                    event_properties: {
                        campaign: pickValue(raw, ['campaign', 'campaign_name'], mapping.campaign),
                        adset: pickValue(raw, ['adset', 'adset_name', 'adgroup'], mapping.adset),
                        media_source: pickValue(raw, ['media_source', 'network', 'channel'], mapping.media_source),
                        raw,
                    },
                };
            }

            function buildMappingCoverage(mapping = {}, sampleRecords = []) {
                const fields = ['canonical_user_id', 'event_name', 'event_time', 'source_event_id', 'campaign', 'adset', 'media_source'];
                const coverage = {};
                const requiredScores = [];
                fields.forEach((field) => {
                    const path = mapping[field];
                    let hits = 0;
                    sampleRecords.forEach((record) => {
                        const value = getPathValue(record, path);
                        if (value !== null && value !== undefined && value !== '') {
                            hits += 1;
                        }
                    });
                    const total = sampleRecords.length;
                    const hitRate = total > 0 ? hits / total : 0;
                    coverage[field] = {
                        path: path || '',
                        hits,
                        total,
                        hit_rate: hitRate,
                    };
                    if (['canonical_user_id', 'event_name', 'event_time'].includes(field)) {
                        requiredScores.push(hitRate);
                    }
                });
                return {
                    coverage,
                    required_coverage_score: requiredScores.length > 0
                        ? requiredScores.reduce((sum, value) => sum + value, 0) / requiredScores.length
                        : 0,
                };
            }

            async function loadSavedConnectors() {
                connectorListDiv.innerHTML = ''; // Clear existing list
                try {
                    const connectors = await refreshConnectorsState();

                    if (connectors.length > 0) {
                        connectors.forEach(connector => {
                            const card = document.createElement('div');
                            card.className = 'card';
                            card.innerHTML = `<span><strong>${connector.name}</strong>: ${formatConnectorLabel(connector.type)}</span>`;

                            const deleteButton = document.createElement('button');
                            deleteButton.textContent = 'Delete';
                            deleteButton.style.backgroundColor = 'var(--subtle-text)';
                            // Pass the unique name to the delete handler
                            deleteButton.dataset.connectorName = connector.name;

                            deleteButton.addEventListener('click', async (e) => {
                                const nameToDelete = e.target.dataset.connectorName;
                                if (!confirm(`Are you sure you want to delete the ${connector.name} connector? This action cannot be undone.`)) {
                                    return;
                                }
                                try {
                                    await apiRequest(`/connectors/${encodeURIComponent(nameToDelete)}`, { method: 'DELETE' });
                                    loadSavedConnectors(); // Refresh the list
                                } catch (error) {
                                    alert(`Error: ${error.message}`);
                                }
                            });
                            connectorListDiv.appendChild(card);
                            card.appendChild(deleteButton);
                        });
                    }
                } catch (error) {
                    console.error('Error loading saved connectors:', error);
                    connectorListDiv.innerHTML = `<p style="color: var(--red);">${error.message}</p>`;
                }
            }

            const connectorFields = {
                amplitude: `
                    <div class="form-group">
                        <label for="amplitude_api_key">Amplitude API Key</label>
                        <input type="password" id="amplitude_api_key" placeholder="Enter your Amplitude API Key">
                    </div>
                    <div class="form-group">
                        <label for="amplitude_secret_key">Amplitude Secret Key</label>
                        <input type="password" id="amplitude_secret_key" placeholder="Enter your Amplitude Secret Key">
                    </div>`,
                adjust: `
                    <div class="form-group">
                        <label for="adjust_api_token">Adjust API Token</label>
                        <input type="password" id="adjust_api_token" placeholder="Enter your Adjust API Token">
                    </div>
                    <div class="form-group">
                        <label for="adjust_api_url">Adjust API URL (optional)</label>
                        <input type="text" id="adjust_api_url" placeholder="https://...">
                    </div>`,
                appsflyer: `
                    <div class="form-group">
                        <label for="appsflyer_api_token">AppsFlyer API Token</label>
                        <input type="password" id="appsflyer_api_token" placeholder="Enter your AppsFlyer API Token">
                    </div>
                    <div class="form-group">
                        <label for="appsflyer_app_id">AppsFlyer App ID</label>
                        <input type="text" id="appsflyer_app_id" placeholder="Enter your AppsFlyer App ID">
                    </div>
                    <div class="form-group">
                        <label for="appsflyer_pull_api_url">AppsFlyer Pull API URL (optional)</label>
                        <input type="text" id="appsflyer_pull_api_url" placeholder="https://...">
                    </div>`,
                google: `
                    <div class="form-group">
                        <label for="google_api_key">Google API Key</label>
                        <input type="password" id="google_api_key" placeholder="Enter your Google API Key">
                    </div>
                    <div class="form-group">
                        <label for="model_name">Gemini Model Version</label>
                        <select id="model_name" style="width: 100%; padding: 0.75rem; border: 1px solid var(--border-color); border-radius: 4px;">
                            <option value="gemini-flash-latest" selected>gemini-flash-latest (Default)</option>
                            <option value="gemini-pro-latest">gemini-pro-latest</option>
                        </select>
                    </div>`,
                bigquery: `
                    <div class="form-group">
                        <label for="bigquery_project_id">Google Cloud Project ID</label>
                        <input type="text" id="bigquery_project_id" placeholder="Enter your GCP Project ID">
                    </div>
                    <div class="form-group">
                        <p style="font-size: 0.8rem; color: var(--subtle-text);">Note: For authentication, ensure your backend service has Application Default Credentials (ADC) configured (e.g., by running 'gcloud auth application-default login') or that a service account is correctly set up in your server environment.</p>
                    </div>`,
                sendgrid: `
                    <div class="form-group">
                        <label for="sendgrid_api_key">SendGrid API Key</label>
                        <input type="password" id="sendgrid_api_key" placeholder="Enter your SendGrid API Key">
                    </div>`,
                braze: `
                    <div class="form-group">
                        <label for="braze_api_key">Braze API Key</label>
                        <input type="password" id="braze_api_key" placeholder="Enter your Braze API Key">
                    </div>
                    <div class="form-group">
                        <label for="braze_rest_endpoint">Braze REST Endpoint</label>
                        <input type="text" id="braze_rest_endpoint" placeholder="https://rest.iad-01.braze.com">
                    </div>`
            };

            addConnectorBtn.addEventListener('click', () => {
                addConnectorCard.style.display = 'none';
                addConnectorFormContainer.style.display = 'block';
            });

            cancelBtn.addEventListener('click', () => {
                addConnectorFormContainer.style.display = 'none';
                addConnectorCard.style.display = 'block';
                connectorTypeSelect.value = '';
                connectorFieldsDiv.innerHTML = '';
                saveConnectorBtn.style.display = 'none';
            });

            connectorTypeSelect.addEventListener('change', (e) => {
                const type = e.target.value;
                if (type && connectorFields[type]) {
                    connectorFieldsDiv.innerHTML = connectorFields[type];
                    saveConnectorBtn.style.display = 'inline-block';
                } else {
                    connectorFieldsDiv.innerHTML = '';
                    saveConnectorBtn.style.display = 'none';
                }
            });

            saveConnectorBtn.addEventListener('click', async () => {
                const type = connectorTypeSelect.value;
                let payload = {};

                if (type === 'amplitude') {
                    payload = {
                        api_key: document.getElementById('amplitude_api_key').value,
                        secret_key: document.getElementById('amplitude_secret_key').value
                    };
                } else if (type === 'adjust') {
                    payload = {
                        api_token: document.getElementById('adjust_api_token').value,
                        api_url: document.getElementById('adjust_api_url').value || undefined
                    };
                } else if (type === 'appsflyer') {
                    payload = {
                        api_token: document.getElementById('appsflyer_api_token').value,
                        app_id: document.getElementById('appsflyer_app_id').value,
                        pull_api_url: document.getElementById('appsflyer_pull_api_url').value || undefined
                    };
                } else if (type === 'google') {
                    payload = {
                        api_key: document.getElementById('google_api_key').value,
                        model_name: document.getElementById('model_name').value || null
                    };
                } else if (type === 'bigquery') {
                    payload = {
                        project_id: document.getElementById('bigquery_project_id').value
                    };
                } else if (type === 'sendgrid') {
                    payload = {
                        api_key: document.getElementById('sendgrid_api_key').value
                    };
                } else if (type === 'braze') {
                    payload = {
                        api_key: document.getElementById('braze_api_key').value,
                        rest_endpoint: document.getElementById('braze_rest_endpoint').value
                    };
                }

                if (!type) return;

                try {
                    const connector = await createConnectorRecord(type, payload);
                    alert(`${formatConnectorLabel(connector.type)} connector '${connector.name}' saved.`);

                    loadSavedConnectors(); // Refresh the list of connectors
                    cancelBtn.click(); // Reset form
                } catch (error) {
                    console.error('Error saving connector:', error);
                    alert(`Error: ${error.message}`);
                }
            });

            // Operator Hub Logic
            const datasetSelect = document.getElementById('dataset-select');
            const predictionModeSelect = document.getElementById('prediction-mode-select');
            const predictionModelReadinessBadge = document.getElementById('prediction-model-readiness-badge');
            const predictionModelReadinessDetails = document.getElementById('prediction-model-readiness-details');
            const predictionLocalWarning = document.getElementById('prediction-local-warning');
            const predictChurnBtn = document.getElementById('predict-churn-btn');
            const predictionProgressInfo = document.getElementById('prediction-progress-info');
            const campaignProviderSelect = document.getElementById('campaign-provider-select');
            const campaignChannelSelect = document.getElementById('campaign-channel-select');
            const campaignRiskFiltersInput = document.getElementById('campaign-risk-filters');
            const campaignAudienceNameInput = document.getElementById('campaign-audience-name');
            const campaignWebhookFields = document.getElementById('campaign-webhook-fields');
            const campaignWebhookUrlInput = document.getElementById('campaign-webhook-url');
            const campaignWebhookTokenInput = document.getElementById('campaign-webhook-token');
            const campaignIncludeChurnedCheckbox = document.getElementById('campaign-include-churned');
            const pushAudienceBtn = document.getElementById('push-audience-btn');
            const campaignExportStatus = document.getElementById('campaign-export-status');
            const operatorHubResults = document.getElementById('operator-hub-results');

            let allChurnPredictionPlayers = [];
            let baselinePredictionPlayers = [];
            let baselinePredictionJobId = null;
            let baselinePredictionImportJobId = null;
            let currentPage = 1;
            let itemsPerPage = 25;
            let activePredictionJobId = null;
            let predictionStopRequested = false;
            const ACTIVE_PREDICTION_STORAGE_KEY = 'kairyx.activePredictionJob';

            function getPredictionImportJobId(job) {
                return String(((job || {}).spec || {}).import_job_id || '');
            }

            function readPersistedActivePredictionJob() {
                try {
                    const raw = window.sessionStorage.getItem(ACTIVE_PREDICTION_STORAGE_KEY);
                    if (!raw) return null;
                    const parsed = JSON.parse(raw);
                    if (!parsed || !parsed.job_id) return null;
                    return parsed;
                } catch (error) {
                    return null;
                }
            }

            function persistActivePredictionJob(jobId, importJobId) {
                if (!jobId || !importJobId) return;
                try {
                    window.sessionStorage.setItem(
                        ACTIVE_PREDICTION_STORAGE_KEY,
                        JSON.stringify({
                            job_id: String(jobId),
                            import_job_id: String(importJobId),
                        }),
                    );
                } catch (error) {
                    console.warn('Unable to persist active prediction state:', error);
                }
            }

            function clearPersistedActivePredictionJob() {
                try {
                    window.sessionStorage.removeItem(ACTIVE_PREDICTION_STORAGE_KEY);
                } catch (error) {
                    console.warn('Unable to clear active prediction state:', error);
                }
            }

            async function getPersistedActivePredictionJob() {
                const persisted = readPersistedActivePredictionJob();
                if (!persisted) {
                    return null;
                }

                let job = cachedPredictionJobs.find((item) => String(item.id || '') === String(persisted.job_id)) || null;
                if (!job) {
                    try {
                        job = await apiRequest(`/predictions/${encodeURIComponent(persisted.job_id)}`);
                    } catch (error) {
                        clearPersistedActivePredictionJob();
                        return null;
                    }
                }

                if (!isPredictionJobActive(job)) {
                    clearPersistedActivePredictionJob();
                    return null;
                }
                return job;
            }

            function setPredictionActionState(state = 'idle') {
                if (state === 'starting') {
                    predictChurnBtn.textContent = 'Starting...';
                    predictChurnBtn.style.background = 'var(--primary-color)';
                    predictChurnBtn.disabled = true;
                    datasetSelect.disabled = true;
                    predictionModeSelect.disabled = true;
                    return;
                }

                if (state === 'running') {
                    predictChurnBtn.textContent = 'Stop';
                    predictChurnBtn.style.background = 'var(--red)';
                    predictChurnBtn.disabled = false;
                    datasetSelect.disabled = true;
                    predictionModeSelect.disabled = true;
                    return;
                }

                if (state === 'stopping') {
                    predictChurnBtn.textContent = 'Stopping...';
                    predictChurnBtn.style.background = 'var(--subtle-text)';
                    predictChurnBtn.disabled = true;
                    datasetSelect.disabled = true;
                    predictionModeSelect.disabled = true;
                    return;
                }

                predictChurnBtn.textContent = 'Predict Churn';
                predictChurnBtn.style.background = '';
                predictChurnBtn.disabled = false;
                datasetSelect.disabled = false;
                predictionModeSelect.disabled = false;
            }

            function renderPredictionProgress(job = {}) {
                predictionProgressInfo.textContent = formatPredictionProgressText(job);
            }

            function getPredictionExecutionLabel(details = {}) {
                const explicitLabel = String(details.execution_label || '').trim();
                if (explicitLabel) {
                    return explicitLabel;
                }
                const predictionMode = String(details.prediction_mode || details.execution_mode || 'local').toLowerCase();
                return getPredictionModeLabel(predictionMode);
            }

            function getPredictionEffectiveLocalModelLabel(details = {}, normalizedStatus = '') {
                const modelVersion = String(
                    details.effective_local_model_version
                    || details.model_version
                    || ''
                ).trim();
                if (!modelVersion) {
                    return '';
                }
                if (!['ready', 'completed'].includes(normalizedStatus)) {
                    return '';
                }
                const readinessLabel = getPredictionModelBadgeLabel(details.effective_local_model_state || '');
                return readinessLabel ? `${modelVersion} (${readinessLabel})` : modelVersion;
            }

            function isPredictionJobStale(job = {}) {
                return Boolean((((job || {}).progress || {}).details || {}).stale);
            }

            function getPredictionJobStaleReason(job = {}) {
                return String(((((job || {}).progress || {}).details || {}).stale_reason) || '').trim();
            }

            function getPredictionReusePromptMessage(completedJob, selectedPredictionMode) {
                const cachedMode = getPredictionJobMode(completedJob);
                const cachedLabel = getPredictionModeLabel(cachedMode);
                const selectedLabel = getPredictionModeLabel(selectedPredictionMode);
                if (isPredictionJobStale(completedJob)) {
                    const staleReason = getPredictionJobStaleReason(completedJob);
                    const staleSuffix = staleReason ? ` ${staleReason}` : ' Newer imports changed the merged player history.';
                    return `${cachedLabel} results for this dataset are finished but stale.${staleSuffix} Select OK to rerun with ${selectedLabel}, or Cancel to load the cached stale results.`;
                }
                if (cachedMode === String(selectedPredictionMode || '').toLowerCase()) {
                    return `${cachedLabel} results for this dataset are already finished and cached. Select OK to rerun with ${selectedLabel}, or Cancel to load the cached results.`;
                }
                return `${cachedLabel} results for this dataset are already finished and cached. Select OK to rerun with ${selectedLabel}, or Cancel to load the cached ${cachedLabel} results.`;
            }

            function formatPredictionProgressText(job = {}) {
                const progress = job.progress || {};
                const processed = job.processed_count ?? progress.current ?? 0;
                const total = job.total_count ?? progress.total ?? 0;
                const pct = job.progress_pct ?? progress.pct ?? 0;
                const rawStatus = String(job.status || 'Processing').trim() || 'Processing';
                const normalizedStatus = rawStatus.toLowerCase();
                const details = progress.details || {};
                if (normalizedStatus === 'stopped') {
                    const stopReason = String(details.stop_reason || '').trim();
                    return stopReason
                        ? `Prediction job: ${rawStatus} · ${stopReason}`
                        : `Prediction job: ${rawStatus}`;
                }
                const executionLabel = getPredictionExecutionLabel(details)
                    .replace(/\b\w/g, (c) => c.toUpperCase());
                const localModelLabel = getPredictionEffectiveLocalModelLabel(details, normalizedStatus);
                const modelSuffix = localModelLabel ? ` · Model ${localModelLabel}` : '';
                const staleSuffix = isPredictionJobStale(job) ? ' · Stale' : '';
                return `Prediction job: ${rawStatus} · ${executionLabel} · ${processed}/${total} users (${Math.round(Number(pct || 0))}%)${modelSuffix}${staleSuffix}`;
            }

            function formatCount(value) {
                return Number(value || 0).toLocaleString();
            }

            function getImportProgressText(job) {
                const rawStatus = String(job.raw_status || '').toLowerCase();
                if (!['queued', 'running', 'stopping'].includes(rawStatus)) {
                    return '';
                }

                const progress = job.progress || {};
                const details = progress.details || {};
                const phase = String(details.phase || '').toLowerCase();
                const current = Number(
                    progress.current
                    || details.normalized_events
                    || details.events_staged
                    || 0
                );
                const total = Number(progress.total || 0);
                const processedManifests = Number(details.processed_manifests || 0);
                const totalManifests = Number(details.total_manifests || details.shards_created || 0);
                const pageSize = Number(details.page_size || 0);

                if (phase === 'processing') {
                    let label = total > 0
                        ? `${formatCount(current)}/${formatCount(total)} events`
                        : `${formatCount(current)} events`;
                    if (totalManifests > 0) {
                        label += ` · ${formatCount(processedManifests)}/${formatCount(totalManifests)} shards`;
                    }
                    return label;
                }

                if (current > 0) {
                    let label = total > 0 && total !== current
                        ? `${formatCount(current)}/${formatCount(total)} events`
                        : `${formatCount(current)} events`;
                    if (total <= 0 && pageSize > 0) {
                        label = current <= pageSize
                            ? `${formatCount(current)}/${formatCount(pageSize)} events`
                            : `${formatCount(current)} events · pages of ${formatCount(pageSize)}`;
                    }
                    if (totalManifests > 0) {
                        label += ` · ${formatCount(totalManifests)} shard${totalManifests === 1 ? '' : 's'}`;
                    }
                    return `${label} staged`;
                }

                return '';
            }

            function setCampaignExportStatus(message = '', isError = false) {
                campaignExportStatus.textContent = message;
                campaignExportStatus.style.color = isError ? 'var(--red)' : 'var(--text-secondary)';
            }

            function updateCampaignProviderFields() {
                const provider = campaignProviderSelect.value || 'webhook';
                campaignWebhookFields.style.display = provider === 'webhook' ? 'grid' : 'none';
                if (provider === 'sendgrid') {
                    campaignChannelSelect.value = 'email';
                }
            }


            async function loadReadyImportsForOperatorHub() {
                datasetSelect.innerHTML = '<option>Loading...</option>';
                try {
                    await Promise.all([
                        refreshConnectorsState(),
                        refreshImportsState(),
                        refreshPredictionJobsState(),
                        refreshPredictionModelReadinessState(),
                    ]);
                    const jobs = cachedImports;
                    const readyJobs = jobs.filter(job => job.status === 'Ready to Use');
                    const previousSelection = datasetSelect.value;
                    const persistedActiveJob = await getPersistedActivePredictionJob();
                    const persistedImportJobId = getPredictionImportJobId(persistedActiveJob) || String((readPersistedActivePredictionJob() || {}).import_job_id || '');
                    datasetSelect.innerHTML = ''; // Clear loading message

                    if (readyJobs.length > 0) {
                        syncPredictionModeSelection();
                        readyJobs.forEach(job => {
                            const option = document.createElement('option');
                            option.value = job.id;
                            option.textContent = job.name;
                            datasetSelect.appendChild(option);
                        });
                        const activePredictionJob = persistedActiveJob || cachedPredictionJobs.find((job) => isPredictionJobActive(job)) || null;
                        const activeImportJobId = getPredictionImportJobId(activePredictionJob) || persistedImportJobId;
                        const selectedDataset = readyJobs.some(job => job.id === activeImportJobId)
                            ? activeImportJobId
                            : (readyJobs.some(job => job.id === previousSelection) ? previousSelection : readyJobs[0].id);
                        datasetSelect.value = selectedDataset;
                        const selectedActiveJob = [persistedActiveJob, activePredictionJob, getLatestPredictionJob(selectedDataset, false)]
                            .find((job) => job && isPredictionJobActive(job) && String((job.spec || {}).import_job_id || '') === String(selectedDataset))
                            || null;

                        if (selectedActiveJob) {
                            activePredictionJobId = selectedActiveJob.id;
                            syncPredictionModeSelection(selectedActiveJob);
                            persistActivePredictionJob(activePredictionJobId, selectedDataset);
                            predictionStopRequested = String(selectedActiveJob.status || '').toLowerCase() === 'stopping';
                            await syncPredictionRows(activePredictionJobId, selectedDataset);
                            renderPredictionProgress({
                                status: mapPredictionStatus(selectedActiveJob.status),
                                progress: selectedActiveJob.progress,
                            });
                            if (String(selectedActiveJob.status || '').toLowerCase() === 'stopping') {
                                setPredictionActionState('stopping');
                            } else {
                                setPredictionActionState('running');
                            }
                            pushAudienceBtn.disabled = true;
                            setCampaignExportStatus('');
                            return;
                        }

                        // Do not auto-run predictions on tab switch/load.
                        // User must explicitly click Predict Churn.
                        activePredictionJobId = null;
                        clearPersistedActivePredictionJob();
                        clearBaselinePredictionRows();
                        allChurnPredictionPlayers = [];
                        renderPredictionModelReadiness();
                        renderChurnTable();
                        setPredictionActionState('idle');
                        pushAudienceBtn.disabled = false;
                        predictionStopRequested = false;
                        predictionProgressInfo.textContent = '';
                        setCampaignExportStatus('');
                    } else {
                        datasetSelect.innerHTML = '<option>No processed datasets available</option>';
                        activePredictionJobId = null;
                        clearPersistedActivePredictionJob();
                        clearBaselinePredictionRows();
                        renderPredictionModelReadiness();
                        setPredictionActionState('idle');
                        predictChurnBtn.disabled = true;
                        datasetSelect.disabled = true;
                        predictionModeSelect.disabled = true;
                        pushAudienceBtn.disabled = true;
                        setCampaignExportStatus('');
                    }
                } catch (error) {
                    clearBaselinePredictionRows();
                    datasetSelect.innerHTML = `<option>Error loading datasets</option>`;
                    renderPredictionModelReadiness();
                    setPredictionActionState('idle');
                    predictChurnBtn.disabled = true;
                    datasetSelect.disabled = true;
                    predictionModeSelect.disabled = true;
                    pushAudienceBtn.disabled = true;
                    console.error('Error loading datasets for Operator Hub:', error);
                }
            }

            async function syncPredictionRows(jobId, importJobId = '') {
                if (!jobId) {
                    return;
                }
                const normalizedImportJobId = String(importJobId || datasetSelect.value || '');
                const [activeRows, existingRows] = await Promise.all([
                    fetchPredictionRows(jobId),
                    loadBaselinePredictionRows(normalizedImportJobId, jobId, getSelectedPredictionMode()),
                ]);
                allChurnPredictionPlayers = mergePredictionRows(activeRows, existingRows);
                if (activePredictionJobId === jobId) {
                    currentPage = 1;
                }
                renderChurnTable(activePredictionJobId ? 'Waiting for prediction results...' : undefined);
            }

            async function requestPredictionStop() {
                if (!activePredictionJobId || predictionStopRequested) {
                    return;
                }
                predictionStopRequested = true;
                setPredictionActionState('stopping');
                try {
                    const stoppedJob = await stopPredictionRecord(activePredictionJobId);
                    renderPredictionProgress({
                        status: mapPredictionStatus(stoppedJob.status),
                        progress: stoppedJob.progress,
                    });
                } catch (error) {
                    predictionStopRequested = false;
                    setPredictionActionState('running');
                    predictionProgressInfo.textContent = error.message || 'Unable to stop prediction job.';
                }
            }

            async function fetchAndRenderPredictions(importJobId, forceRecalculate = false) {
                if (!importJobId) {
                    clearBaselinePredictionRows();
                    allChurnPredictionPlayers = [];
                    renderChurnTable();
                    return;
                }

                predictionStopRequested = false;
                const selectedPredictionMode = getSelectedPredictionMode();
                currentPage = 1;
                let shouldForceRecalculate = Boolean(forceRecalculate);

                let predictionJob = null;
                try {
                    await ensureHealthState();
                    await Promise.all([refreshConnectorsState(), refreshPredictionJobsState(), refreshPredictionModelReadinessState()]);
                    renderPredictionModelReadiness();
                    predictionJob = !shouldForceRecalculate ? getLatestActivePredictionJob(importJobId, selectedPredictionMode) : null;
                    if (!predictionJob && !shouldForceRecalculate) {
                        const completedJob = (
                            getLatestCompletedPredictionJob(importJobId, null, selectedPredictionMode)
                            || getLatestCompletedPredictionJob(importJobId)
                        );
                        if (completedJob) {
                            const shouldRerun = window.confirm(
                                getPredictionReusePromptMessage(completedJob, selectedPredictionMode),
                            );
                            if (!shouldRerun) {
                                await renderCompletedPredictionJob(completedJob, importJobId);
                                return;
                            }
                            shouldForceRecalculate = true;
                        }
                    }

                    setPredictionActionState('starting');
                    predictionProgressInfo.textContent = getPredictionStartStatusMessage(null, selectedPredictionMode);

                    const existingRows = shouldForceRecalculate
                        ? []
                        : await loadBaselinePredictionRows(
                            importJobId,
                            predictionJob ? predictionJob.id : null,
                            selectedPredictionMode,
                        );
                    allChurnPredictionPlayers = [...existingRows];
                    renderChurnTable(existingRows.length > 0 ? undefined : 'Waiting for prediction results...');

                    if (!predictionJob) {
                        predictionJob = await createPredictionRecord(importJobId, selectedPredictionMode);
                    }
                    activePredictionJobId = predictionJob.id;
                    syncPredictionModeSelection(predictionJob);
                    persistActivePredictionJob(activePredictionJobId, importJobId);
                    predictionStopRequested = String(predictionJob.status || '').toLowerCase() === 'stopping';
                    renderPredictionProgress({
                        status: mapPredictionStatus(predictionJob.status),
                        progress: predictionJob.progress,
                    });
                    if (String(predictionJob.status || '').toLowerCase() === 'stopping') {
                        setPredictionActionState('stopping');
                    } else {
                        setPredictionActionState('running');
                    }

                    while (activePredictionJobId === predictionJob.id) {
                        predictionJob = await apiRequest(`/predictions/${encodeURIComponent(activePredictionJobId)}`);
                        if (isPredictionJobActive(predictionJob)) {
                            persistActivePredictionJob(predictionJob.id, importJobId);
                        }
                        await syncPredictionRows(activePredictionJobId, importJobId);
                        renderPredictionProgress({
                            status: mapPredictionStatus(predictionJob.status),
                            progress: predictionJob.progress,
                        });
                        const normalizedStatus = String(predictionJob.status || '').toLowerCase();
                        if (normalizedStatus === 'stopping') {
                            setPredictionActionState('stopping');
                        } else if (normalizedStatus === 'queued' || normalizedStatus === 'running') {
                            setPredictionActionState('running');
                        }
                        if (['completed', 'failed', 'stopped'].includes(normalizedStatus)) {
                            break;
                        }
                        await new Promise((resolve) => setTimeout(resolve, PREDICTION_POLL_INTERVAL_MS));
                    }

                    predictionJob = await apiRequest(`/predictions/${encodeURIComponent(activePredictionJobId)}`);
                    await syncPredictionRows(activePredictionJobId, importJobId);
                    if (predictionJob.status === 'failed') {
                        throw new Error(predictionJob.error || 'Prediction failed.');
                    }
                    if (predictionJob.status === 'stopped') {
                        renderPredictionProgress({
                            status: 'Stopped',
                            progress: predictionJob.progress,
                        });
                        if (allChurnPredictionPlayers.length === 0) {
                            renderChurnTable('Prediction stopped before any rows completed.');
                        }
                        return;
                    }
                    if (predictionJob.status !== 'completed') {
                        throw new Error('Prediction job is still queued. Start a worker or use mock mode for local execution.');
                    }

                    baselinePredictionPlayers = [...allChurnPredictionPlayers];
                    baselinePredictionImportJobId = String(importJobId);
                    baselinePredictionJobId = String(predictionJob.id || '');
                    if (allChurnPredictionPlayers.length === 0) {
                        renderChurnTable();
                    }
                    renderPredictionProgress({
                        status: 'Ready',
                        progress: predictionJob.progress,
                    });
                } catch (error) {
                    operatorHubResults.innerHTML = `<tr><td colspan="7" style="text-align: center; color: var(--red);">${error.message}</td></tr>`;
                } finally {
                    const finalStatus = String((predictionJob || {}).status || '').toLowerCase();
                    if (!predictionJob || ['completed', 'failed', 'stopped'].includes(finalStatus)) {
                        activePredictionJobId = null;
                        clearPersistedActivePredictionJob();
                        predictionStopRequested = false;
                        setPredictionActionState('idle');
                    }
                }
            }

            async function pushAudienceToCampaignProvider() {
                const jobName = datasetSelect.value;
                if (!jobName) {
                    setCampaignExportStatus('Select a ready dataset first.', true);
                    return;
                }

                const provider = campaignProviderSelect.value || 'webhook';
                const channel = campaignChannelSelect.value || 'push_notification';
                const includeRisks = (campaignRiskFiltersInput.value || '')
                    .split(',')
                    .map((risk) => risk.trim().toLowerCase())
                    .filter(Boolean);

                pushAudienceBtn.disabled = true;
                setCampaignExportStatus(`Pushing audience to ${provider}...`);
                try {
                    await refreshPredictionJobsState();
                    const predictionJob = (
                        baselinePredictionImportJobId === String(jobName)
                        && baselinePredictionJobId
                        && cachedPredictionJobs.find((job) => String(job.id || '') === String(baselinePredictionJobId))
                    ) || getLatestPredictionJobForMode(jobName, true, getSelectedPredictionMode()) || getLatestPredictionJob(jobName, true);
                    if (!predictionJob) {
                        throw new Error('Run churn prediction for this dataset before exporting an audience.');
                    }
                    const payload = {
                        include_churned: campaignIncludeChurnedCheckbox.checked,
                        include_risks: includeRisks.length > 0 ? includeRisks : ['high', 'medium'],
                        provider,
                        channel,
                        audience_name: (campaignAudienceNameInput.value || '').trim() || null,
                        webhook_url: provider === 'webhook' ? ((campaignWebhookUrlInput.value || '').trim() || null) : null,
                        webhook_token: provider === 'webhook' ? ((campaignWebhookTokenInput.value || '').trim() || null) : null,
                    };
                    const result = await createExportRecord(predictionJob.id, payload);
                    const details = (result && result.progress && result.progress.details) || {};
                    if (String(result.status || '').toLowerCase() === 'failed') {
                        throw new Error(result.error || 'Failed to push audience to provider.');
                    }
                    await refreshExportJobsState();
                    setCampaignExportStatus(`Audience pushed to ${details.provider || provider}. ${details.count || 0} row(s) exported.`);
                } catch (error) {
                    setCampaignExportStatus(error.message || 'Failed to push audience to provider.', true);
                } finally {
                    pushAudienceBtn.disabled = false;
                }
            }

            function renderChurnTable(emptyMessage = 'No players found in this dataset.') {
                operatorHubResults.innerHTML = '';
                const startIndex = (currentPage - 1) * itemsPerPage;
                const endIndex = startIndex + itemsPerPage;
                const paginatedItems = allChurnPredictionPlayers.slice(startIndex, endIndex);

                if (paginatedItems.length > 0) {
                    // Helper to apply color classes
                    const getRiskClass = (risk) => {
                        if (risk.toLowerCase() === 'high') return 'risk-high';
                        if (risk.toLowerCase() === 'medium') return 'risk-medium';
                        if (risk.toLowerCase() === 'low') return 'risk-low';
                        return '';
                    };

                    const getRiskLabel = (player) => {
                        const churnState = String(player.churn_state || '').toLowerCase();
                        if (churnState === 'churned') {
                            return 'Churned';
                        }
                        return String(player.predicted_churn_risk || '');
                    };

                    paginatedItems.forEach(p => {
                        const riskLabel = getRiskLabel(p);
                        operatorHubResults.innerHTML += `<tr><td>${escapeHtml(String(p.user_id || '-'))}</td><td>${Number(p.ltv || 0).toFixed(2)}</td><td>${Number(p.session_count || 0)}</td><td>${Number(p.event_count || 0)}</td><td class="${getRiskClass(String(p.predicted_churn_risk || ''))}">${escapeHtml(riskLabel)}</td><td>${renderExpandableText(p.churn_reason, 150)}</td><td class="action-suggested">${renderExpandableText(p.suggested_action, 90)}</td></tr>`;
                    });
                } else {
                     operatorHubResults.innerHTML = `<tr><td colspan="7" style="text-align: center;">${emptyMessage}</td></tr>`;
                }
                renderPaginationControls();
            }

            function renderPaginationControls() {
                const paginationContainer = document.getElementById('pagination-controls');
                paginationContainer.innerHTML = '';
                const totalPages = Math.ceil(allChurnPredictionPlayers.length / itemsPerPage);

                if (totalPages <= 1) return;

                const prevButton = document.createElement('button');
                prevButton.textContent = 'Previous';
                prevButton.disabled = currentPage === 1;
                prevButton.addEventListener('click', () => {
                    if (currentPage > 1) {
                        currentPage--;
                        renderChurnTable();
                    }
                });

                const pageInfo = document.createElement('span');
                pageInfo.textContent = `Page ${currentPage} of ${totalPages}`;
                pageInfo.style.margin = '0 1rem';

                const nextButton = document.createElement('button');
                nextButton.textContent = 'Next';
                nextButton.disabled = currentPage === totalPages;
                nextButton.addEventListener('click', () => {
                    if (currentPage < totalPages) {
                        currentPage++;
                        renderChurnTable();
                    }
                });

                paginationContainer.appendChild(prevButton);
                paginationContainer.appendChild(pageInfo);
                paginationContainer.appendChild(nextButton);
            }

            datasetSelect.addEventListener('change', () => {
                // Selection alone should not trigger prediction.
                clearBaselinePredictionRows();
                allChurnPredictionPlayers = [];
                renderChurnTable();
                predictionStopRequested = false;
                predictionProgressInfo.textContent = '';
                renderPredictionModelReadiness();
                setPredictionActionState('idle');
                setCampaignExportStatus('');
            });

            predictionModeSelect.addEventListener('change', () => {
                renderPredictionModelReadiness();
            });

            predictChurnBtn.addEventListener('click', async () => {
                if (activePredictionJobId) {
                    requestPredictionStop();
                    return;
                }
                const jobName = datasetSelect.value;
                fetchAndRenderPredictions(jobName, false);
            });

            campaignProviderSelect.addEventListener('change', updateCampaignProviderFields);
            pushAudienceBtn.addEventListener('click', pushAudienceToCampaignProvider);
            updateCampaignProviderFields();

            document.getElementById('items-per-page').addEventListener('change', (e) => {
                itemsPerPage = parseInt(e.target.value, 10);
                currentPage = 1; // Reset to the first page
                renderChurnTable();
            });

            // Player Cohorts Page Logic
            const importDataBtn = document.getElementById('import-data-btn');
            const importListContainer = document.getElementById('import-list-container');
            const importListStatus = document.getElementById('import-list-status');
            const importDetailSelect = document.getElementById('import-detail-select');
            const importDetailStatus = document.getElementById('import-detail-status');
            const importDetailOutput = document.getElementById('import-detail-output');
            const importManifestsList = document.getElementById('import-manifests-list');
            const importSchemaStatus = document.getElementById('import-schema-status');
            const importSchemaOutput = document.getElementById('import-schema-output');
            let importListInterval = null;
            let selectedImportJobId = null;

    
            async function loadConfiguredSources() {
                const importCard = importDataBtn.parentElement;
                const sourceSelect = document.getElementById('cohort-source-select');
                const sourceGroup = document.getElementById('cohort-source-select').parentElement;
                const startDateGroup = document.getElementById('start-date-cohort').parentElement;
                const endDateGroup = document.getElementById('end-date-cohort').parentElement;

                try {
                    await refreshConnectorsState();
                    const sources = getConfiguredSourcesFromState();

                    if (!sources || sources.length === 0) {
                        // Hide form elements and show message
                        sourceGroup.style.display = 'none';
                        startDateGroup.style.display = 'none';
                        endDateGroup.style.display = 'none';
                        importDataBtn.style.display = 'none';
                        
                        let messageEl = importCard.querySelector('.config-message');
                        if (!messageEl) {
                            messageEl = document.createElement('p');
                            messageEl.className = 'config-message';
                            messageEl.textContent = 'Please configure a data source in the Connectors section first.';
                            importCard.insertBefore(messageEl, sourceGroup);
                        }
                    } else {
                        sourceSelect.innerHTML = ''; // Clear existing options
                        sources.forEach(source => {
                            const option = document.createElement('option');
                            option.value = source.id;
                            option.textContent = source.name;
                            sourceSelect.appendChild(option);
                        });
                        // Ensure form elements are visible
                        sourceGroup.style.display = 'block';
                        startDateGroup.style.display = 'block';
                        endDateGroup.style.display = 'block';
                        importDataBtn.style.display = 'inline-block';

                        let messageEl = importCard.querySelector('.config-message');
                        if (messageEl) {
                            messageEl.remove();
                        }
                    }
                } catch (error) {
                    const importCard = importDataBtn.parentElement;
                    importCard.innerHTML = `<p style="color: var(--red);">${error.message}</p>`;
                }
            }

            let countdownInterval = null;
            function populateImportDetailSelect(imports = []) {
                if (!importDetailSelect) return;
                const previous = selectedImportJobId || importDetailSelect.value;
                importDetailSelect.innerHTML = '';
                if (!imports.length) {
                    importDetailSelect.innerHTML = '<option value="">No import jobs available</option>';
                    selectedImportJobId = null;
                    renderJsonOutput(importDetailOutput, null, 'Select an import job to inspect operations.');
                    renderSimpleTable(importManifestsList, [], [], 'No manifests available.');
                    return;
                }
                imports.forEach((job) => {
                    const option = document.createElement('option');
                    option.value = job.id;
                    option.textContent = `${job.name} (${job.status})`;
                    importDetailSelect.appendChild(option);
                });
                if (imports.some((job) => job.id === previous)) {
                    importDetailSelect.value = previous;
                    selectedImportJobId = previous;
                } else {
                    importDetailSelect.value = imports[0].id;
                    selectedImportJobId = imports[0].id;
                }
            }

            async function loadImportOperatorView(view = 'operations', silent = false) {
                const jobId = importDetailSelect?.value;
                if (!jobId) {
                    renderJsonOutput(importDetailOutput, null, 'Select an import job to inspect operations.');
                    renderSimpleTable(importManifestsList, [], [], 'No manifests available.');
                    return;
                }
                selectedImportJobId = jobId;
                try {
                    if (!silent) {
                        setInlineStatus(importDetailStatus, `Loading import ${view}...`);
                    }
                    if (view === 'manifests') {
                        const payload = await apiRequest(`/imports/${encodeURIComponent(jobId)}/manifests`);
                        renderSimpleTable(
                            importManifestsList,
                            [
                                { label: 'Shard', render: (item) => escapeHtml(String(item.shard_index ?? '-')) },
                                { label: 'Status', render: (item) => `<span class="pill">${escapeHtml(item.status || '-')}</span>` },
                                { label: 'Events', render: (item) => escapeHtml(String(item.event_count || 0)) },
                                { label: 'Schema', render: (item) => escapeHtml(item.schema_version || 'v1') },
                                { label: 'Path', render: (item) => `<span class="subtle">${escapeHtml(item.gcs_uri || item.manifest?.gcs_path || '-')}</span>` },
                            ],
                            payload.items || [],
                            'No shard manifests recorded yet.',
                        );
                        renderJsonOutput(importDetailOutput, payload, 'Manifest detail unavailable.');
                    } else if (view === 'quality') {
                        const payload = await apiRequest(`/imports/${encodeURIComponent(jobId)}/quality`);
                        renderJsonOutput(importDetailOutput, payload, 'Import quality unavailable.');
                    } else {
                        const payload = await apiRequest(`/imports/${encodeURIComponent(jobId)}/operations`);
                        renderJsonOutput(importDetailOutput, payload, 'Import operations unavailable.');
                    }
                    if (!silent) {
                        setInlineStatus(importDetailStatus, `Loaded import ${view} for ${jobId}.`);
                    }
                } catch (error) {
                    renderJsonOutput(importDetailOutput, { error: error.message }, 'Import detail unavailable.');
                    if (view === 'manifests') {
                        renderSimpleTable(importManifestsList, [], [], 'Manifest detail unavailable.');
                    }
                    setInlineStatus(importDetailStatus, error.message || 'Failed to load import detail.', true);
                }
            }

            async function loadImportSchemaContracts(listAll = false) {
                try {
                    setInlineStatus(importSchemaStatus, listAll ? 'Loading all schema contracts...' : 'Loading schema contract...');
                    const payload = listAll
                        ? await apiRequest('/imports/schema-contracts')
                        : await apiRequest(`/imports/schema-contracts/${encodeURIComponent(document.getElementById('import-schema-alias-select').value || 'standardized')}`);
                    renderJsonOutput(importSchemaOutput, payload, 'Schema contract detail unavailable.');
                    setInlineStatus(importSchemaStatus, listAll ? `Loaded ${payload.items?.length || 0} schema contract(s).` : `Loaded schema contract for ${(payload.alias || document.getElementById('import-schema-alias-select').value)}.`);
                } catch (error) {
                    renderJsonOutput(importSchemaOutput, { error: error.message }, 'Schema contract detail unavailable.');
                    setInlineStatus(importSchemaStatus, error.message || 'Failed to load schema contract.', true);
                }
            }

            async function loadImportedDataList() {
                // Show loading message only if the container is empty initially
                if (!importListContainer.innerHTML.trim()) {
                    importListContainer.innerHTML = '<p>Loading...</p>';
                }

                try {
                    const imports = await refreshImportsState();

                    if (!imports || imports.length === 0) {
                        if (countdownInterval) {
                            clearInterval(countdownInterval);
                            countdownInterval = null;
                        }
                        importListContainer.innerHTML = '<p>No data has been imported yet.</p>';
                        populateImportDetailSelect([]);
                        return;
                    }
                    const priorSelectedImport = selectedImportJobId;

                    // If table doesn't exist, create it
                    let table = importListContainer.querySelector('table');
                    if (!table) {
                        importListContainer.innerHTML = `
                            <div class="table-shell">
                                <table>
                                    <thead>
                                        <tr>
                                            <th>Import Name</th>
                                            <th>Timestamp</th>
                                            <th>Status</th>
                                            <th>Expires In</th>
                                            <th>Actions</th>
                                        </tr>
                                    </thead>
                                    <tbody></tbody>
                                </table>
                            </div>
                        `;
                        table = importListContainer.querySelector('table');
                    }
                    const tbody = table.querySelector('tbody');
                    const activeRowIds = new Set(imports.map((job) => `job-row-${job.id.replace(/[^a-zA-Z0-9]/g, "")}`));
                    Array.from(tbody.querySelectorAll('tr')).forEach((row) => {
                        if (!activeRowIds.has(row.id)) {
                            row.remove();
                        }
                    });

                    // Update rows in place
                    imports.forEach(job => {
                        const statusClass = getImportStatusClass(job);
                        const expirationId = `expires-in-${job.id.replace(/[^a-zA-Z0-9]/g, "")}`;
                        const rowId = `job-row-${job.id.replace(/[^a-zA-Z0-9]/g, "")}`;
                        const actionsCellId = `actions-cell-${job.id.replace(/[^a-zA-Z0-9]/g, "")}`;
                        let row = document.getElementById(rowId);
                        
                        if (row) {
                            // If row exists, just update the status if it changed
                            const newStatusHTML = renderImportStatus(job, statusClass);
                            const statusCell = row.cells[2];
                            if (statusCell.innerHTML !== newStatusHTML || row.cells[0].textContent !== job.name) {
                                // Update status and actions if status changed
                                row.cells[0].textContent = job.name;
                                statusCell.innerHTML = newStatusHTML;
                                const actionsCell = document.getElementById(actionsCellId);
                                actionsCell.innerHTML = getActionButtonsHTML(job);
                                addActionListeners(row, job);

                                if (job.status === 'Ready to Use' && job.expiration_timestamp) {
                                    const expirationCell = row.cells[3];
                                    expirationCell.id = expirationId;
                                    expirationCell.dataset.expiration = job.expiration_timestamp;
                                    startCountdownTimers();
                                }
                            }
                        } else {
                            // If row doesn't exist, create it
                            const newRow = tbody.insertRow();
                            newRow.id = rowId;
                            
                            const expirationCellHtml = job.status === 'Ready to Use' && job.expiration_timestamp
                                ? `<td id="${expirationId}" data-expiration="${job.expiration_timestamp}">-</td>`
                                : `<td>-</td>`;
                            
                            const actionsCellHtml = `<td id="${actionsCellId}">${getActionButtonsHTML(job)}</td>`;

                            newRow.innerHTML = `
                                <td>${job.name}</td>
                                <td>${new Date(job.timestamp).toLocaleString()}</td>
                                <td>${renderImportStatus(job, statusClass)}</td>
                                ${expirationCellHtml}
                                ${actionsCellHtml}
                            `;
                            addActionListeners(newRow, job);
                        }
                    });
                    populateImportDetailSelect(imports);
                    if (!priorSelectedImport && selectedImportJobId) {
                        await loadImportOperatorView('operations', true);
                    }

                } catch (error) {
                    importListContainer.innerHTML = `<p style="color: var(--red);">${error.message}</p>`;
                } finally {
                    startCountdownTimers(); // Ensure timers are always started/restarted after data load
                }
            }

            // Action History Page Logic
            const actionHistoryResults = document.getElementById('action-history-results');
            const actionHistoryItemsPerPageSelect = document.getElementById('action-history-items-per-page');
            const actionHistoryPaginationControls = document.getElementById('action-history-pagination-controls');
            let allActionHistoryItems = [];
            let actionHistoryCurrentPage = 1;
            let actionHistoryItemsPerPage = 25;

            function getActionHistoryStatusColor(status) {
                const normalized = String(status || '').toLowerCase();
                if (['completed', 'ok', 'saved', 'sent'].includes(normalized)) return 'var(--green)';
                if (['failed', 'error'].includes(normalized)) return 'var(--red)';
                if (['high_risk'].includes(normalized)) return 'var(--yellow)';
                if (['started', 'stopping', 'awaiting_mapping'].includes(normalized)) return 'var(--yellow)';
                if (['stopped'].includes(normalized)) return 'var(--primary-color)';
                return 'var(--text-secondary)';
            }

            function escapeHtml(value) {
                return String(value ?? '')
                    .replace(/&/g, '&amp;')
                    .replace(/</g, '&lt;')
                    .replace(/>/g, '&gt;')
                    .replace(/"/g, '&quot;')
                    .replace(/'/g, '&#39;');
            }

            function renderExpandableText(value, maxLength = 140) {
                const raw = String(value ?? '').trim();
                if (!raw) {
                    return '<span class="subtle">-</span>';
                }
                const escaped = escapeHtml(raw);
                if (raw.length <= maxLength) {
                    return escaped;
                }
                return `
                    <div class="expandable-text">
                        <div class="expandable-text__content">${escaped}</div>
                        <button type="button" class="expandable-text__toggle">Show Full Text</button>
                    </div>
                `;
            }

            function renderActionHistoryPaginationControls() {
                if (!actionHistoryPaginationControls) return;
                actionHistoryPaginationControls.innerHTML = '';

                const totalPages = Math.ceil(allActionHistoryItems.length / actionHistoryItemsPerPage);
                if (totalPages <= 1) return;

                const prevButton = document.createElement('button');
                prevButton.textContent = 'Previous';
                prevButton.disabled = actionHistoryCurrentPage === 1;
                prevButton.addEventListener('click', () => {
                    if (actionHistoryCurrentPage > 1) {
                        actionHistoryCurrentPage--;
                        renderActionHistoryTable();
                    }
                });

                const pageInfo = document.createElement('span');
                pageInfo.textContent = `Page ${actionHistoryCurrentPage} of ${totalPages}`;
                pageInfo.style.margin = '0 1rem';

                const nextButton = document.createElement('button');
                nextButton.textContent = 'Next';
                nextButton.disabled = actionHistoryCurrentPage === totalPages;
                nextButton.addEventListener('click', () => {
                    if (actionHistoryCurrentPage < totalPages) {
                        actionHistoryCurrentPage++;
                        renderActionHistoryTable();
                    }
                });

                actionHistoryPaginationControls.appendChild(prevButton);
                actionHistoryPaginationControls.appendChild(pageInfo);
                actionHistoryPaginationControls.appendChild(nextButton);
            }

            function renderActionHistoryTable() {
                if (!actionHistoryResults) return;

                if (allActionHistoryItems.length === 0) {
                    actionHistoryResults.innerHTML = '<tr><td colspan="4" style="text-align: center;">No recorded actions yet.</td></tr>';
                    if (actionHistoryPaginationControls) {
                        actionHistoryPaginationControls.innerHTML = '';
                    }
                    return;
                }

                const totalPages = Math.max(1, Math.ceil(allActionHistoryItems.length / actionHistoryItemsPerPage));
                actionHistoryCurrentPage = Math.min(actionHistoryCurrentPage, totalPages);
                const startIndex = (actionHistoryCurrentPage - 1) * actionHistoryItemsPerPage;
                const endIndex = startIndex + actionHistoryItemsPerPage;
                const paginatedItems = allActionHistoryItems.slice(startIndex, endIndex);

                actionHistoryResults.innerHTML = '';
                paginatedItems.forEach((item) => {
                    const row = document.createElement('tr');
                    const timestamp = item.timestamp ? new Date(item.timestamp).toLocaleString() : '-';
                    const summary = item.summary || '-';
                    const status = item.status || '-';
                    const details = item.details || '-';
                    const statusColor = getActionHistoryStatusColor(status);

                    row.innerHTML = `
                        <td>${escapeHtml(timestamp)}</td>
                        <td>${escapeHtml(summary)}</td>
                        <td><span style="display: inline-block; padding: 0.15rem 0.5rem; border-radius: 999px; color: ${statusColor}; border: 1px solid ${statusColor}; text-transform: capitalize;">${escapeHtml(status)}</span></td>
                        <td style="font-size: 0.85rem; color: var(--text-secondary);">${renderExpandableText(details, 220)}</td>
                    `;
                    actionHistoryResults.appendChild(row);
                });

                renderActionHistoryPaginationControls();
            }

            async function loadActionHistory() {
                if (!actionHistoryResults) return;
                actionHistoryResults.innerHTML = '<tr><td colspan="4" style="text-align: center;">Loading action history...</td></tr>';
                if (actionHistoryPaginationControls) {
                    actionHistoryPaginationControls.innerHTML = '';
                }
                try {
                    setInlineStatus(actionHistoryStatus, 'Loading audit log...');
                    const query = new URLSearchParams();
                    query.set('limit', String(Number(actionHistoryItemsPerPageSelect.value || 25)));
                    const resourceType = (document.getElementById('action-history-resource-type-filter').value || '').trim();
                    const actionType = (document.getElementById('action-history-action-type-filter').value || '').trim();
                    if (resourceType) query.set('resource_type', resourceType);
                    if (actionType) query.set('action_type', actionType);
                    if (document.getElementById('action-history-high-risk-filter').checked) {
                        query.set('high_risk_only', 'true');
                    }
                    query.set('tenant_id', (tenantIdInput.value || 'default').trim() || 'default');
                    const payload = await apiRequest(`/audit/actions?${query.toString()}`);
                    allActionHistoryItems = (payload.items || []).map((item) => ({
                        timestamp: item.created_at,
                        summary: `${item.action_type} · ${item.resource_type}${item.resource_id ? `:${item.resource_id}` : ''}`,
                        status: item.high_risk ? 'high_risk' : 'recorded',
                        details: JSON.stringify(item.payload || {}),
                    }));
                    actionHistoryCurrentPage = 1;
                    renderActionHistoryTable();
                    setInlineStatus(actionHistoryStatus, `Loaded ${payload.summary?.returned || 0} audit record(s).`);
                } catch (error) {
                    allActionHistoryItems = [];
                    actionHistoryResults.innerHTML = `<tr><td colspan="4" style="text-align: center; color: var(--red);">${error.message}</td></tr>`;
                    if (actionHistoryPaginationControls) {
                        actionHistoryPaginationControls.innerHTML = '';
                    }
                    setInlineStatus(actionHistoryStatus, error.message || 'Failed to load audit log.', true);
                }
            }

            actionHistoryItemsPerPageSelect.addEventListener('change', (event) => {
                actionHistoryItemsPerPage = Number(event.target.value) || 25;
                actionHistoryCurrentPage = 1;
                renderActionHistoryTable();
            });

            operatorHubResults.addEventListener('click', (event) => {
                const toggle = event.target.closest('.expandable-text__toggle');
                if (!toggle) return;
                const wrapper = toggle.closest('.expandable-text');
                if (!wrapper) return;
                const expanded = wrapper.classList.toggle('expanded');
                toggle.textContent = expanded ? 'Show Less' : 'Show Full Text';
            });

            actionHistoryResults.addEventListener('click', (event) => {
                const toggle = event.target.closest('.expandable-text__toggle');
                if (!toggle) return;
                const wrapper = toggle.closest('.expandable-text');
                if (!wrapper) return;
                const expanded = wrapper.classList.toggle('expanded');
                toggle.textContent = expanded ? 'Show Less' : 'Show Full Text';
            });

            function getActionButtonsHTML(job) {
                if (['completed', 'failed', 'stopped'].includes(job.raw_status)) {
                    return `<button type="button" data-import-action="delete" data-job-id="${escapeHtml(job.id)}" style="background-color: var(--subtle-text);">Delete</button>`;
                }
                if (job.raw_status === 'stopping') {
                    return `<button type="button" disabled style="background-color: var(--subtle-text); cursor: not-allowed;">Stopping...</button>`;
                }
                if (job.raw_status === 'queued' || job.raw_status === 'running') {
                    return `<button type="button" data-import-action="stop" data-job-id="${escapeHtml(job.id)}">Stop</button>`;
                }
                return '';
            }

            function addActionListeners(rowElement, job) {
                if (!rowElement || !job) return rowElement;
                const actionButton = rowElement.querySelector('[data-import-action]');
                if (!actionButton) return rowElement;

                actionButton.addEventListener('click', async () => {
                    const action = actionButton.dataset.importAction;
                    const jobId = actionButton.dataset.jobId;
                    if (!action || !jobId) return;

                    actionButton.disabled = true;
                    try {
                        if (action === 'stop') {
                            await stopImportRecord(jobId);
                        } else if (action === 'delete') {
                            if (!confirm(`Delete import '${job.name}'? This cannot be undone.`)) {
                                actionButton.disabled = false;
                                return;
                            }
                            await deleteImportRecord(jobId);
                        }
                        await loadImportedDataList();
                    } catch (error) {
                        actionButton.disabled = false;
                        alert(`Unable to ${action} import: ${error.message}`);
                    }
                });
                return rowElement;
            }

            function startCountdownTimers() {
                if (countdownInterval) {
                    clearInterval(countdownInterval);
                }

                const countdownElements = document.querySelectorAll('[data-expiration]');
                if (countdownElements.length === 0) return;

                countdownInterval = setInterval(() => {
                    countdownElements.forEach(el => {
                        const expirationTime = new Date(el.dataset.expiration).getTime();
                        const now = new Date().getTime();
                        const distance = expirationTime - now;

                        if (distance < 0) {
                            el.textContent = "Expired";
                            return;
                        }

                        const days = Math.floor(distance / (1000 * 60 * 60 * 24));
                        const hours = Math.floor((distance % (1000 * 60 * 60 * 24)) / (1000 * 60 * 60));
                        const minutes = Math.floor((distance % (1000 * 60 * 60)) / (1000 * 60));
                        const seconds = Math.floor((distance % (1000 * 60)) / 1000);

                        el.textContent = `${days}d ${hours}h ${minutes}m ${seconds}s`;
                    });
                }, 1000);
            }

            importDataBtn.addEventListener('click', async () => {
                const startDate = document.getElementById('start-date-cohort').value;
                const endDate = document.getElementById('end-date-cohort').value;
                const source = document.getElementById('cohort-source-select').value;

                if (!source) {
                    alert('No data source is available. Please configure one in the Connectors section.');
                    return;
                }

                if (!startDate || !endDate) {
                    alert('Please select a valid start and end date.');
                    return;
                }

                const payload = {
                    start_date: startDate.replace(/-/g, ''),
                    end_date: endDate.replace(/-/g, ''),
                    source: source
                };

                try {
                    const result = await createImportRecord(payload.source, payload.start_date, payload.end_date);
                    const modeSuffix = result.raw_status === 'stopped'
                        ? 'was stopped.'
                        : backendMode === 'mock'
                        ? 'started locally. Status will update below.'
                        : 'queued. A worker must run it to completion.';
                    alert(`Import job '${result.name}' ${modeSuffix}`);
                    loadImportedDataList(); // Refresh the list
                } catch (error) {
                    await loadImportedDataList();
                    alert(`Error starting import: ${error.message}`);
                }
            });
            importDetailSelect?.addEventListener('change', () => loadImportOperatorView('operations', true));
            document.getElementById('import-load-operations-btn')?.addEventListener('click', () => loadImportOperatorView('operations'));
            document.getElementById('import-load-quality-btn')?.addEventListener('click', () => loadImportOperatorView('quality'));
            document.getElementById('import-load-manifests-btn')?.addEventListener('click', () => loadImportOperatorView('manifests'));
            document.getElementById('import-load-schema-btn')?.addEventListener('click', () => loadImportSchemaContracts(false));
            document.getElementById('import-load-schema-list-btn')?.addEventListener('click', () => loadImportSchemaContracts(true));

            // Service Health Page Logic
            const serviceStatusListDiv = document.getElementById('service-status-list');

            async function loadServiceHealthStatus() {
                serviceStatusListDiv.innerHTML = '<p>Checking service status...</p>';
                try {
                    setInlineStatus(serviceHealthStatus, 'Refreshing operational state...');
                    const [health, modulesPayload, alertsPayload, schedulerPayload] = await Promise.all([
                        apiRequest('/health'),
                        apiRequest('/health/modules'),
                        apiRequest('/health/alerts?include_resolved=true'),
                        apiRequest('/health/scheduler'),
                    ]);
                    renderJsonOutput(serviceHealthOutput, health, 'No health payload available.');
                    renderSimpleTable(
                        serviceStatusListDiv,
                        [
                            { label: 'Module', render: (item) => escapeHtml(item.module || '-') },
                            { label: 'Status', render: (item) => `<span class="pill">${escapeHtml(item.status || '-')}</span>` },
                            { label: 'Metrics', render: (item) => `<span class="subtle">${escapeHtml(JSON.stringify(item.metrics || {}))}</span>` },
                        ],
                        modulesPayload.items || [],
                        'No module status records found.',
                    );
                    renderSimpleTable(
                        serviceAlertsList,
                        [
                            { label: 'Module', render: (item) => escapeHtml(item.module || '-') },
                            { label: 'Code', render: (item) => escapeHtml(item.code || '-') },
                            { label: 'Severity', render: (item) => `<span class="pill">${escapeHtml(item.severity || '-')}</span>` },
                            { label: 'Status', render: (item) => escapeHtml(item.status || '-') },
                            { label: 'Message', render: (item) => escapeHtml(item.message || '-') },
                        ],
                        alertsPayload.items || [],
                        'No persisted alerts found.',
                    );
                    renderSimpleTable(
                        serviceSchedulerList,
                        [
                            { label: 'Job', render: (item) => escapeHtml(item.name || item.job_id || '-') },
                            { label: 'Schedule', render: (item) => `<span class="subtle">${escapeHtml(JSON.stringify(item.schedule || {}))}</span>` },
                            { label: 'Last Run', render: (item) => escapeHtml(formatDateTime(item.last_run_at)) },
                            { label: 'Next Run', render: (item) => escapeHtml(formatDateTime(item.next_run_hint)) },
                            { label: 'Last Status', render: (item) => escapeHtml(item.last_status || '-') },
                        ],
                        schedulerPayload.items || [],
                        'No scheduler jobs found.',
                    );
                    setInlineStatus(serviceHealthStatus, `Loaded ${modulesPayload.items?.length || 0} module(s), ${alertsPayload.items?.length || 0} alert(s), and ${schedulerPayload.items?.length || 0} scheduler job(s).`);
                } catch (error) {
                    serviceStatusListDiv.innerHTML = `<p style="color: var(--red);">${error.message}</p>`;
                    renderJsonOutput(serviceHealthOutput, { error: error.message }, 'Health payload unavailable.');
                    renderSimpleTable(serviceAlertsList, [], [], 'Health alerts unavailable.');
                    renderSimpleTable(serviceSchedulerList, [], [], 'Scheduler state unavailable.');
                    setInlineStatus(serviceHealthStatus, error.message || 'Failed to load health state.', true);
                }
            }

            async function runServiceHealthTick() {
                try {
                    setInlineStatus(serviceHealthStatus, 'Running scheduler tick...');
                    const payload = await apiRequest('/health/scheduler/tick', {
                        method: 'POST',
                        body: { reference_time: new Date().toISOString() },
                    });
                    renderJsonOutput(serviceHealthOutput, payload, 'Scheduler tick returned no payload.');
                    setInlineStatus(serviceHealthStatus, `Executed ${payload.items?.length || 0} scheduler task(s).`);
                    await loadServiceHealthStatus();
                } catch (error) {
                    setInlineStatus(serviceHealthStatus, error.message || 'Failed to run scheduler tick.', true);
                }
            }

            async function loadScenarioTemplates() {
                if (!templatesList) return;
                templatesList.innerHTML = '<div class="list-empty">Loading templates...</div>';
                try {
                    const payload = await apiRequest('/templates');
                    const items = payload.items || [];
                    if (!items.length) {
                        templatesList.innerHTML = '<div class="list-empty">No templates available.</div>';
                        renderJsonOutput(templateDetailOutput, null, 'No template selected.');
                        return;
                    }
                    if (!selectedTemplateId || !items.some((item) => item.template_id === selectedTemplateId)) {
                        selectedTemplateId = items[0].template_id;
                    }
                    renderSimpleTable(
                        templatesList,
                        [
                            { label: 'Template', render: (item) => `<button type="button" class="template-select-btn" data-template-id="${escapeHtml(item.template_id)}">${escapeHtml(item.name || item.template_id)}</button>` },
                            { label: 'Goal', render: (item) => escapeHtml(item.goal || '-') },
                            { label: 'Scenario', render: (item) => escapeHtml(item.template_id || '-') },
                        ],
                        items,
                        'No templates found.',
                    );
                    Array.from(templatesList.querySelectorAll('.template-select-btn')).forEach((button) => {
                        button.addEventListener('click', async () => {
                            selectedTemplateId = button.dataset.templateId;
                            await loadScenarioTemplates();
                        });
                    });
                    const detail = await apiRequest(`/templates/${encodeURIComponent(selectedTemplateId)}`);
                    templatesSelectedLabel.textContent = selectedTemplateId;
                    renderJsonOutput(templateDetailOutput, detail, 'No template detail found.');
                } catch (error) {
                    templatesList.innerHTML = `<div class="list-empty" style="color: var(--red);">${escapeHtml(error.message)}</div>`;
                    renderJsonOutput(templateDetailOutput, { error: error.message }, 'Failed to load templates.');
                }
            }

            async function instantiateScenarioTemplate() {
                if (!selectedTemplateId) {
                    setInlineStatus(templateInstantiateStatus, 'Select a template first.', true);
                    return;
                }
                try {
                    setInlineStatus(templateInstantiateStatus, 'Instantiating template...');
                    const payload = await apiRequest(`/templates/${encodeURIComponent(selectedTemplateId)}/instantiate`, {
                        method: 'POST',
                        body: {
                            owner: document.getElementById('template-owner-input').value || 'frontend_operator',
                            name_prefix: (document.getElementById('template-name-prefix-input').value || '').trim() || null,
                            activate_cohort: document.getElementById('template-activate-cohort-checkbox').checked,
                            publish_workflow: document.getElementById('template-publish-workflow-checkbox').checked,
                        },
                    });
                    renderJsonOutput(templateInstanceOutput, payload, 'No instantiation payload returned.');
                    setInlineStatus(templateInstantiateStatus, `Instantiated ${selectedTemplateId}.`);
                } catch (error) {
                    setInlineStatus(templateInstantiateStatus, error.message || 'Failed to instantiate template.', true);
                    renderJsonOutput(templateInstanceOutput, { error: error.message }, 'Template instantiation failed.');
                }
            }

            // Data Sandbox Page Logic
            const dataSandboxContentDiv = document.getElementById('data-sandbox-content');
            const dataSandboxMappingStatusDiv = document.getElementById('data-sandbox-mapping-status');
            const dataSandboxMappingConnectorSelect = document.getElementById('data-sandbox-mapping-connector');
            const dataSandboxAwaitingJobSelect = document.getElementById('data-sandbox-awaiting-job');
            const dataSandboxMappingJson = document.getElementById('data-sandbox-mapping-json');
            const dataSandboxSampleJson = document.getElementById('data-sandbox-sample-json');
            const dataSandboxPreviewResult = document.getElementById('data-sandbox-preview-result');
            const dataSandboxCoverageResult = document.getElementById('data-sandbox-coverage-result');
            const dataSandboxLoadMappingBtn = document.getElementById('data-sandbox-load-mapping-btn');
            const dataSandboxSaveMappingBtn = document.getElementById('data-sandbox-save-mapping-btn');
            const dataSandboxPreviewMappingBtn = document.getElementById('data-sandbox-preview-mapping-btn');
            const dataSandboxCoverageBtn = document.getElementById('data-sandbox-coverage-btn');
            const dataSandboxProcessMappingBtn = document.getElementById('data-sandbox-process-mapping-btn');
            let eventChart = null;
            let dataSandboxAwaitingJobs = [];

            function setDataSandboxMappingStatus(message = '', isError = false) {
                if (!dataSandboxMappingStatusDiv) return;
                dataSandboxMappingStatusDiv.textContent = message;
                dataSandboxMappingStatusDiv.style.color = isError ? 'var(--red)' : 'var(--green)';
            }

            function setDataSandboxAutoSample(sampleRecord) {
                if (!dataSandboxSampleJson) return;
                const nextValue = JSON.stringify(sampleRecord || {}, null, 2);
                const currentValue = (dataSandboxSampleJson.value || '').trim();
                const priorAutoValue = dataSandboxSampleJson.dataset.autoValue || '';
                if (!currentValue || currentValue === priorAutoValue) {
                    dataSandboxSampleJson.value = nextValue;
                    dataSandboxSampleJson.dataset.autoValue = nextValue;
                }
            }

            function renderDataSandboxCoverage(result = null) {
                if (!dataSandboxCoverageResult) return;

                if (!result || !result.coverage || !Object.keys(result.coverage).length) {
                    dataSandboxCoverageResult.innerHTML = '';
                    return;
                }

                const score = Math.max(0, Math.min(100, Number(result.required_coverage_score || 0) * 100));
                const scoreColor = score >= 90 ? 'var(--green)' : (score >= 70 ? 'var(--yellow)' : 'var(--red)');
                const cardsHtml = Object.entries(result.coverage || {}).map(([field, stat]) => `
                    <div style="border: 1px solid rgba(148, 163, 184, 0.2); border-radius: 8px; padding: 0.75rem; background: rgba(15, 23, 42, 0.45);">
                        <div style="font-weight: 600; margin-bottom: 0.25rem;">${field}</div>
                        <div style="font-size: 0.8rem; color: var(--text-secondary);">path: ${stat.path || '-'}</div>
                        <div style="font-size: 0.85rem; margin-top: 0.25rem;">hit: ${(Number(stat.hit_rate || 0) * 100).toFixed(1)}% (${stat.hits || 0}/${stat.total || 0})</div>
                    </div>
                `).join('');

                dataSandboxCoverageResult.innerHTML = `
                    <div style="border: 1px solid rgba(148, 163, 184, 0.2); border-radius: 10px; padding: 1rem; background: rgba(15, 23, 42, 0.6);">
                        <div style="display: flex; justify-content: space-between; gap: 1rem; align-items: center; margin-bottom: 0.5rem;">
                            <strong>Coverage</strong>
                            <span style="font-size: 0.85rem; color: var(--text-secondary);">Required score: ${score.toFixed(1)}%</span>
                        </div>
                        <div style="width: 100%; height: 10px; background: rgba(71, 85, 105, 0.5); border-radius: 999px; overflow: hidden; margin-bottom: 1rem;">
                            <div style="width: ${score}%; height: 100%; background: ${scoreColor};"></div>
                        </div>
                        <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 0.75rem;">
                            ${cardsHtml}
                        </div>
                    </div>
                `;
            }

            function getSelectedAwaitingJob() {
                const selectedJobId = dataSandboxAwaitingJobSelect.value;
                return dataSandboxAwaitingJobs.find((job) => job.id === selectedJobId) || null;
            }

            async function loadDataSandboxMappingControls() {
                try {
                    const [connectors, imports] = await Promise.all([
                        refreshConnectorsState(),
                        refreshImportsState(),
                    ]);
                    const sources = connectors.map((source) => ({ id: source.name, name: source.name }));
                    dataSandboxAwaitingJobs = imports.filter((job) => job.status === 'Awaiting Mapping');

                    const previousConnector = dataSandboxMappingConnectorSelect.value;
                    dataSandboxMappingConnectorSelect.innerHTML = '';
                    if (sources.length > 0) {
                        sources.forEach((source) => {
                            const option = document.createElement('option');
                            option.value = source.id;
                            option.textContent = source.name;
                            dataSandboxMappingConnectorSelect.appendChild(option);
                        });
                        const suggestedConnector = dataSandboxAwaitingJobs[0]?.source_stats?.[0]?.source;
                        const selectedConnector = sources.some((source) => source.id === previousConnector)
                            ? previousConnector
                            : (sources.some((source) => source.id === suggestedConnector) ? suggestedConnector : sources[0].id);
                        dataSandboxMappingConnectorSelect.value = selectedConnector;
                    } else {
                        dataSandboxMappingConnectorSelect.innerHTML = '<option value="">No configured connectors</option>';
                    }
                    const hasConnectors = sources.length > 0;
                    dataSandboxLoadMappingBtn.disabled = !hasConnectors;
                    dataSandboxSaveMappingBtn.disabled = !hasConnectors;
                    dataSandboxPreviewMappingBtn.disabled = !hasConnectors;
                    dataSandboxCoverageBtn.disabled = !hasConnectors;

                    const previousJob = dataSandboxAwaitingJobSelect.value;
                    dataSandboxAwaitingJobSelect.innerHTML = '';
                    if (dataSandboxAwaitingJobs.length > 0) {
                        const emptyOption = document.createElement('option');
                        emptyOption.value = '';
                        emptyOption.textContent = 'Select paused import job';
                        dataSandboxAwaitingJobSelect.appendChild(emptyOption);

                        dataSandboxAwaitingJobs.forEach((job) => {
                            const option = document.createElement('option');
                            option.value = job.id;
                            option.textContent = `${job.name} (${job.current_step || job.status})`;
                            option.dataset.source = job.source_stats?.[0]?.source || '';
                            dataSandboxAwaitingJobSelect.appendChild(option);
                        });

                        if (dataSandboxAwaitingJobs.some((job) => job.id === previousJob)) {
                            dataSandboxAwaitingJobSelect.value = previousJob;
                        } else {
                            dataSandboxAwaitingJobSelect.value = dataSandboxAwaitingJobs[0].id;
                        }
                    } else {
                        dataSandboxAwaitingJobSelect.innerHTML = '<option value="">No paused import job</option>';
                    }
                    dataSandboxProcessMappingBtn.disabled = dataSandboxAwaitingJobs.length === 0;

                    const selectedJob = getSelectedAwaitingJob();
                    const selectedJobSource = selectedJob?.source_stats?.[0]?.source;
                    if (
                        selectedJobSource &&
                        Array.from(dataSandboxMappingConnectorSelect.options).some((option) => option.value === selectedJobSource)
                    ) {
                        dataSandboxMappingConnectorSelect.value = selectedJobSource;
                    }

                    if ((dataSandboxMappingJson.value || '').trim() === '{}' && dataSandboxMappingConnectorSelect.value) {
                        await loadDataSandboxFieldMapping(true);
                    }

                    if (dataSandboxAwaitingJobs.length > 0) {
                        setDataSandboxMappingStatus(`Paused import job detected: ${dataSandboxAwaitingJobs[0].name}. Review the mapping, then click "Process After Mapping".`);
                    } else {
                        setDataSandboxMappingStatus('No paused import jobs. You can still edit and preview connector mappings locally.');
                    }
                } catch (error) {
                    setDataSandboxMappingStatus(error.message || 'Failed to load field mapping controls.', true);
                }
            }

            async function loadDataSandboxFieldMapping(silent = false) {
                const connectorName = dataSandboxMappingConnectorSelect.value;
                if (!connectorName) {
                    setDataSandboxMappingStatus('Select a connector first.', true);
                    return;
                }

                try {
                    const data = await apiRequest(`/mappings/${encodeURIComponent(connectorName)}`);
                    dataSandboxMappingJson.value = JSON.stringify(data.mapping || {}, null, 2);
                    if (!silent) {
                        setDataSandboxMappingStatus(`Loaded field mapping for ${connectorName}.`);
                    }
                } catch (error) {
                    setDataSandboxMappingStatus(error.message || 'Failed to load field mapping.', true);
                }
            }

            async function saveDataSandboxFieldMapping() {
                const connectorName = dataSandboxMappingConnectorSelect.value;
                if (!connectorName) {
                    setDataSandboxMappingStatus('Select a connector first.', true);
                    return;
                }

                try {
                    const mapping = JSON.parse(dataSandboxMappingJson.value || '{}');
                    await apiRequest(`/mappings/${encodeURIComponent(connectorName)}`, {
                        method: 'PUT',
                        body: { mapping },
                    });
                    setDataSandboxMappingStatus(`Saved field mapping for ${connectorName}.`);
                } catch (error) {
                    setDataSandboxMappingStatus(error.message || 'Failed to save field mapping.', true);
                }
            }

            async function previewDataSandboxFieldMapping() {
                const connectorName = dataSandboxMappingConnectorSelect.value;
                if (!connectorName) {
                    setDataSandboxMappingStatus('Select a connector first.', true);
                    return;
                }

                try {
                    const sampleRecord = JSON.parse(dataSandboxSampleJson.value || '{}');
                    const mapping = JSON.parse(dataSandboxMappingJson.value || '{}');
                    const preview = previewMappedEvent(connectorName, sampleRecord, mapping);
                    dataSandboxPreviewResult.textContent = JSON.stringify(preview || {}, null, 2);
                    setDataSandboxMappingStatus(`Preview generated for ${connectorName}.`);
                } catch (error) {
                    dataSandboxPreviewResult.textContent = 'Preview result will appear here.';
                    setDataSandboxMappingStatus(error.message || 'Failed to preview field mapping.', true);
                }
            }

            async function loadDataSandboxMappingCoverage() {
                const connectorName = dataSandboxMappingConnectorSelect.value;
                if (!connectorName) {
                    setDataSandboxMappingStatus('Select a connector first.', true);
                    return;
                }

                try {
                    const sampleRecord = JSON.parse(dataSandboxSampleJson.value || '{}');
                    const mapping = JSON.parse(dataSandboxMappingJson.value || '{}');
                    const data = buildMappingCoverage(mapping, sampleRecord ? [sampleRecord] : []);
                    renderDataSandboxCoverage(data);
                    setDataSandboxMappingStatus(`Coverage calculated for ${connectorName}.`);
                } catch (error) {
                    renderDataSandboxCoverage(null);
                    setDataSandboxMappingStatus(error.message || 'Failed to calculate mapping coverage.', true);
                }
            }

            async function processDataSandboxAwaitingJob() {
                const selectedJob = getSelectedAwaitingJob();
                if (!selectedJob) {
                    setDataSandboxMappingStatus('Import pause/resume is not available on /api/v1.', true);
                    return;
                }

                setDataSandboxMappingStatus(`Manual mapping is saved, but processing paused jobs is not available on /api/v1 yet.`, true);
            }

            async function loadDataSandboxGlance() {
                dataSandboxContentDiv.innerHTML = '<p>Loading data glance...</p>';
                try {
                    const imports = await refreshImportsState();
                    const latestImport = imports[0];
                    if (!latestImport) {
                        dataSandboxContentDiv.innerHTML = '<p>No imported datasets available yet.</p>';
                        return;
                    }
                    const processing = latestImport.processing_stats || {};
                    const eventCounts = {
                        'Raw Normalized': Number(processing.raw_normalized_events || 0),
                        'Deduped': Number(processing.deduped_events || 0),
                        'Duplicates Removed': Number(processing.duplicates_removed || 0),
                    };
                    const data = {
                        filename: latestImport.name,
                        sample: [{
                            import_job: latestImport.name,
                            source: latestImport.source_stats?.[0]?.source || '-',
                            start_date: latestImport.start_date,
                            end_date: latestImport.end_date,
                            status: latestImport.status,
                            source_stats: latestImport.source_stats || [],
                            processing_stats: latestImport.processing_stats || {},
                        }],
                        event_counts: eventCounts,
                    };

                    // Build the HTML for the glance
                    const sampleHtml = JSON.stringify(data.sample, null, 2);
                    const contentHtml = `
                        <details>
                            <summary style="cursor: pointer; font-weight: 600;">
                                Latest Import Glance: ${data.filename}
                            </summary>
                            <pre><code style="font-size: 0.8rem; white-space: pre-wrap;">${sampleHtml}</code></pre>
                        </details>
                        <div style="margin-top: 2rem; height: 500px;">
                            <h2>Import Metrics</h2>
                            <canvas id="event-chart"></canvas>
                        </div>
                    `;
                    dataSandboxContentDiv.innerHTML = contentHtml;
                    if (data.sample && data.sample[0]) {
                        setDataSandboxAutoSample(data.sample[0]);
                    }

                    // Render the chart
                    const ctx = document.getElementById('event-chart').getContext('2d');
                    if (eventChart) {
                        eventChart.destroy();
                    }
                    eventChart = new Chart(ctx, {
                        type: 'bar',
                        data: {
                            labels: Object.keys(data.event_counts),
                            datasets: [{
                                label: 'Event Count',
                                data: Object.values(data.event_counts),
                                backgroundColor: 'rgba(74, 85, 104, 0.6)',
                                borderColor: 'rgba(74, 85, 104, 1)',
                                borderWidth: 1
                            }]
                        },
                        options: {
                            plugins: {
                                title: {
                                    display: true,
                                    text: 'Top 10 Events by Count'
                                }
                            },
                            scales: { y: { beginAtZero: true, suggestedMax: 10, ticks: { stepSize: 1 } } },
                            responsive: true,
                            maintainAspectRatio: false
                        }
                    });
                } catch (error) {
                    dataSandboxContentDiv.innerHTML = `<p style="color: var(--red);">${error.message}</p>`;
                }
            }

            dataSandboxMappingConnectorSelect.addEventListener('change', async () => {
                renderDataSandboxCoverage(null);
                dataSandboxPreviewResult.textContent = 'Preview result will appear here.';
                await loadDataSandboxFieldMapping(true);
            });

            dataSandboxAwaitingJobSelect.addEventListener('change', async () => {
                const selectedJob = getSelectedAwaitingJob();
                const selectedJobSource = selectedJob?.source_stats?.[0]?.source;
                if (
                    selectedJobSource &&
                    Array.from(dataSandboxMappingConnectorSelect.options).some((option) => option.value === selectedJobSource)
                ) {
                    dataSandboxMappingConnectorSelect.value = selectedJobSource;
                    await loadDataSandboxFieldMapping(true);
                }
                renderDataSandboxCoverage(null);
            });

            dataSandboxLoadMappingBtn.addEventListener('click', () => loadDataSandboxFieldMapping(false));
            dataSandboxSaveMappingBtn.addEventListener('click', saveDataSandboxFieldMapping);
            dataSandboxPreviewMappingBtn.addEventListener('click', previewDataSandboxFieldMapping);
            dataSandboxCoverageBtn.addEventListener('click', loadDataSandboxMappingCoverage);
            dataSandboxProcessMappingBtn.addEventListener('click', processDataSandboxAwaitingJob);

            // Audience / Workflow / Experiment / Copilot Shared Helpers
            const audienceCreateCohortBtn = document.getElementById('audience-create-cohort-btn');
            const audienceCreateStatus = document.getElementById('audience-create-status');
            const audienceCohortList = document.getElementById('audience-cohort-list');
            const audienceSelectedCohortLabel = document.getElementById('audience-selected-cohort-label');
            const audienceCohortDetail = document.getElementById('audience-cohort-detail');
            const audienceMembersList = document.getElementById('audience-members-list');
            const audienceVersionsList = document.getElementById('audience-versions-list');
            const audienceMetricsOutput = document.getElementById('audience-metrics-output');
            const audienceCompareOutput = document.getElementById('audience-compare-output');
            const sqlWorkspaceStatus = document.getElementById('sql-workspace-status');
            const sqlPreviewOutput = document.getElementById('sql-preview-output');
            const sqlSavedQueryList = document.getElementById('sql-saved-query-list');
            const workflowCreateStatus = document.getElementById('workflow-create-status');
            const workflowList = document.getElementById('workflow-list');
            const workflowSelectedLabel = document.getElementById('workflow-selected-label');
            const workflowExecutionsList = document.getElementById('workflow-executions-list');
            const workflowDeliveriesList = document.getElementById('workflow-deliveries-list');
            const workflowDeliveryDiagnosticsOutput = document.getElementById('workflow-delivery-diagnostics-output');
            const workflowPolicyOutput = document.getElementById('workflow-policy-output');
            const orchestratorRunStatus = document.getElementById('orchestrator-run-status');
            const orchestratorRunOutput = document.getElementById('orchestrator-run-output');
            const activationIngestStatus = document.getElementById('activation-ingest-status');
            const exportDiagnosticsOutput = document.getElementById('export-diagnostics-output');
            const experimentStatus = document.getElementById('experiment-status');
            const experimentSummaryOutput = document.getElementById('experiment-summary-output');
            const experimentIntegrityOutput = document.getElementById('experiment-integrity-output');
            const experimentExposuresList = document.getElementById('experiment-exposures-list');
            const experimentOutcomesList = document.getElementById('experiment-outcomes-list');
            const experimentIngestStatus = document.getElementById('experiment-ingest-status');
            const copilotResponseOutput = document.getElementById('copilot-response-output');
            const copilotQueryLogOutput = document.getElementById('copilot-query-log-output');
            const copilotAnomaliesList = document.getElementById('copilot-anomalies-list');
            const copilotReportsList = document.getElementById('copilot-reports-list');
            const actionHistoryRefreshBtn = document.getElementById('action-history-refresh-btn');
            const actionHistoryStatus = document.getElementById('action-history-status');
            const serviceHealthStatus = document.getElementById('service-health-status');
            const serviceAlertsList = document.getElementById('service-alerts-list');
            const serviceSchedulerList = document.getElementById('service-scheduler-list');
            const serviceHealthOutput = document.getElementById('service-health-output');
            const templatesList = document.getElementById('templates-list');
            const templateDetailOutput = document.getElementById('template-detail-output');
            const templateInstanceOutput = document.getElementById('template-instance-output');
            const templateInstantiateStatus = document.getElementById('template-instantiate-status');
            const templatesSelectedLabel = document.getElementById('templates-selected-label');
            let selectedAudienceCohortId = null;
            let selectedWorkflowId = null;
            let selectedTemplateId = null;
            let cachedSavedQueries = [];

            function setInlineStatus(element, message = '', isError = false) {
                if (!element) return;
                element.textContent = message;
                element.style.color = isError ? 'var(--red)' : 'var(--text-secondary)';
            }

            function renderJsonOutput(element, payload, emptyMessage = 'No data available.') {
                if (!element) return;
                if (payload === null || payload === undefined || payload === '') {
                    element.textContent = emptyMessage;
                    return;
                }
                element.textContent = typeof payload === 'string' ? payload : JSON.stringify(payload, null, 2);
            }

            function parseJsonText(value, fallback = {}) {
                const raw = String(value || '').trim();
                if (!raw) return fallback;
                return JSON.parse(raw);
            }

            function splitCsv(value) {
                return String(value || '')
                    .split(',')
                    .map((item) => item.trim())
                    .filter(Boolean);
            }

            function formatDateTime(value) {
                if (!value) return '-';
                const parsed = parseIsoDate(value);
                return Number.isNaN(parsed.getTime()) ? '-' : parsed.toLocaleString();
            }

            function renderMetricList(container, payload = {}, emptyMessage = 'No metrics available.') {
                if (!container) return;
                const entries = Object.entries(payload || {});
                if (entries.length === 0) {
                    container.innerHTML = `<div class="list-empty">${escapeHtml(emptyMessage)}</div>`;
                    return;
                }
                container.innerHTML = entries.map(([key, value]) => `
                    <div class="metric-line">
                        <span>${escapeHtml(String(key).replace(/_/g, ' '))}</span>
                        <strong>${escapeHtml(typeof value === 'object' ? JSON.stringify(value) : String(value))}</strong>
                    </div>
                `).join('');
            }

            function renderSimpleTable(container, columns, items, emptyMessage = 'No records found.') {
                if (!container) return;
                if (!Array.isArray(items) || items.length === 0) {
                    container.innerHTML = `<div class="list-empty">${escapeHtml(emptyMessage)}</div>`;
                    return;
                }
                const head = columns.map((column) => `<th>${escapeHtml(column.label)}</th>`).join('');
                const rows = items.map((item) => `
                    <tr>
                        ${columns.map((column) => `<td>${column.render ? column.render(item) : escapeHtml(String(item[column.key] ?? '-'))}</td>`).join('')}
                    </tr>
                `).join('');
                container.innerHTML = `
                    <div class="table-shell">
                        <table>
                            <thead><tr>${head}</tr></thead>
                            <tbody>${rows}</tbody>
                        </table>
                    </div>
                `;
            }

            function getCurrentExperimentId() {
                return (document.getElementById('experiment-id-input').value || 'churn_rescue_v1').trim() || 'churn_rescue_v1';
            }

            function getCurrentWorkflowReferenceTime() {
                return (document.getElementById('orchestrator-reference-time-input').value || '').trim() || null;
            }

            function populateWorkflowCohortSelect(cohorts = []) {
                const select = document.getElementById('workflow-cohort-select');
                const previousValue = select.value;
                select.innerHTML = '<option value="">Select cohort</option>';
                cohorts.forEach((cohort) => {
                    const option = document.createElement('option');
                    option.value = cohort.cohort_id;
                    option.textContent = `${cohort.name} (${cohort.status})`;
                    select.appendChild(option);
                });
                if (cohorts.some((item) => item.cohort_id === previousValue)) {
                    select.value = previousValue;
                }
            }

            function populateExportJobSelect(exportJobs = []) {
                const select = document.getElementById('export-diagnostics-select');
                const previousValue = select.value;
                select.innerHTML = '<option value="">Select export job</option>';
                exportJobs.forEach((job) => {
                    const option = document.createElement('option');
                    option.value = job.id;
                    option.textContent = `${job.id} (${job.status || 'unknown'})`;
                    select.appendChild(option);
                });
                if (exportJobs.some((item) => item.id === previousValue)) {
                    select.value = previousValue;
                }
            }

            async function loadAudienceMembers(cohortId = selectedAudienceCohortId) {
                if (!cohortId) {
                    audienceMembersList.innerHTML = '<div class="list-empty">Select a cohort first.</div>';
                    return;
                }
                const payload = await apiRequest(`/cohorts/${encodeURIComponent(cohortId)}/members?page=1&page_size=50`);
                renderSimpleTable(
                    audienceMembersList,
                    [
                        { label: 'Canonical User ID', render: (item) => escapeHtml(String(item.canonical_user_id || item.user_id || '-')) },
                        { label: 'Email', render: (item) => escapeHtml(String(item.email || '-')) },
                        { label: 'Attributes', render: (item) => `<span class="subtle">${escapeHtml(JSON.stringify(item))}</span>` },
                    ],
                    payload.items || [],
                    'No members in the latest snapshot.',
                );
            }

            async function loadAudienceVersions(cohortId = selectedAudienceCohortId) {
                if (!cohortId) {
                    audienceVersionsList.innerHTML = '<div class="list-empty">Select a cohort first.</div>';
                    return;
                }
                const payload = await apiRequest(`/cohorts/${encodeURIComponent(cohortId)}/versions`);
                renderSimpleTable(
                    audienceVersionsList,
                    [
                        { label: 'Version', render: (item) => escapeHtml(String(item.version || '-')) },
                        { label: 'Created', render: (item) => escapeHtml(formatDateTime(item.created_at)) },
                        { label: 'Status', render: (item) => `<span class="pill">${escapeHtml(String((item.payload || {}).status || '-'))}</span>` },
                    ],
                    payload.items || [],
                    'No saved versions yet.',
                );
            }

            async function loadAudienceMetrics(cohortId = selectedAudienceCohortId) {
                if (!cohortId) {
                    renderJsonOutput(audienceMetricsOutput, null, 'Select a cohort first.');
                    return;
                }
                const payload = await apiRequest(`/cohorts/${encodeURIComponent(cohortId)}/metrics`);
                renderJsonOutput(audienceMetricsOutput, payload, 'No metrics available.');
            }

            async function loadAudienceCohortDetails(cohortId) {
                selectedAudienceCohortId = cohortId;
                audienceSelectedCohortLabel.textContent = cohortId || 'No cohort selected';
                if (!cohortId) {
                    audienceCohortDetail.innerHTML = '<div class="list-empty">Select a cohort from the table.</div>';
                    audienceMembersList.innerHTML = '';
                    audienceVersionsList.innerHTML = '';
                    renderJsonOutput(audienceMetricsOutput, null, 'Cohort metrics will appear here.');
                    renderJsonOutput(audienceCompareOutput, null, 'Version comparison will appear here.');
                    return;
                }
                const cohort = await apiRequest(`/cohorts/${encodeURIComponent(cohortId)}`);
                document.getElementById('audience-base-version-input').value = cohort.version || 1;
                document.getElementById('audience-target-version-input').value = cohort.version || 1;
                audienceCohortDetail.innerHTML = `
                    <div class="stats-grid">
                        <div class="stat-card"><div class="label">Status</div><div class="value">${escapeHtml(cohort.status || '-')}</div></div>
                        <div class="stat-card"><div class="label">Members</div><div class="value">${escapeHtml(String(cohort.member_count || 0))}</div></div>
                        <div class="stat-card"><div class="label">Refresh Mode</div><div class="value">${escapeHtml(cohort.refresh_mode || '-')}</div></div>
                        <div class="stat-card"><div class="label">Version</div><div class="value">${escapeHtml(String(cohort.version || cohort.version_id || 1))}</div></div>
                    </div>
                    <div class="subtle">Last refreshed: ${escapeHtml(formatDateTime(cohort.last_refreshed_at))}</div>
                    <pre class="json-output">${escapeHtml(JSON.stringify({ definition: cohort.definition, delta: cohort.delta, activation_preflight: cohort.activation_preflight, metrics_summary: cohort.metrics_summary }, null, 2))}</pre>
                `;
                await Promise.all([loadAudienceMetrics(cohortId), loadAudienceMembers(cohortId), loadAudienceVersions(cohortId)]);
            }

            function renderSavedQueries(items = []) {
                cachedSavedQueries = Array.isArray(items) ? items : [];
                renderSimpleTable(
                    sqlSavedQueryList,
                    [
                        { label: 'Name', render: (item) => escapeHtml(item.name || '-') },
                        { label: 'Description', render: (item) => `<span class="subtle">${escapeHtml(item.description || '-')}</span>` },
                        { label: 'Updated', render: (item) => escapeHtml(formatDateTime(item.updated_at)) },
                        {
                            label: 'Actions',
                            render: (item) => `
                                <div class="table-actions">
                                    <button type="button" data-sql-action="preview" data-query-id="${escapeHtml(item.query_id)}">Preview</button>
                                    <button type="button" data-sql-action="cohort" data-query-id="${escapeHtml(item.query_id)}">To Cohort</button>
                                </div>
                            `,
                        },
                    ],
                    cachedSavedQueries,
                    'No saved SQL queries yet.',
                );
                sqlSavedQueryList.querySelectorAll('[data-sql-action]').forEach((button) => {
                    button.addEventListener('click', async () => {
                        const query = cachedSavedQueries.find((item) => item.query_id === button.dataset.queryId);
                        if (!query) return;
                        if (button.dataset.sqlAction === 'preview') {
                            document.getElementById('sql-workspace-textarea').value = query.sql || '';
                            await previewSqlWorkspace();
                            return;
                        }
                        if (button.dataset.sqlAction === 'cohort') {
                            await createCohortFromSavedQuery(query.query_id);
                        }
                    });
                });
            }

            async function previewSqlWorkspace() {
                try {
                    setInlineStatus(sqlWorkspaceStatus, 'Previewing query...');
                    const payload = await apiRequest('/sql-workspace/preview', {
                        method: 'POST',
                        body: {
                            sql: document.getElementById('sql-workspace-textarea').value,
                            limit: Number(document.getElementById('sql-preview-limit').value || 20),
                            timeout_seconds: Number(document.getElementById('sql-timeout-seconds').value || 30),
                        },
                    });
                    renderJsonOutput(sqlPreviewOutput, payload, 'Preview returned no rows.');
                    setInlineStatus(sqlWorkspaceStatus, `Preview returned ${payload.row_count || 0} row(s).`);
                } catch (error) {
                    setInlineStatus(sqlWorkspaceStatus, error.message || 'SQL preview failed.', true);
                    renderJsonOutput(sqlPreviewOutput, null, 'SQL preview failed.');
                }
            }

            async function saveSqlWorkspaceQuery() {
                try {
                    setInlineStatus(sqlWorkspaceStatus, 'Saving query...');
                    await apiRequest('/sql-workspace/queries', {
                        method: 'POST',
                        body: {
                            name: document.getElementById('sql-saved-query-name').value || 'Untitled query',
                            description: document.getElementById('sql-saved-query-description').value || '',
                            sql: document.getElementById('sql-workspace-textarea').value,
                        },
                    });
                    setInlineStatus(sqlWorkspaceStatus, 'Saved query.');
                    await loadAudienceEngine();
                } catch (error) {
                    setInlineStatus(sqlWorkspaceStatus, error.message || 'Failed to save query.', true);
                }
            }

            async function createCohortFromSavedQuery(queryId) {
                try {
                    setInlineStatus(sqlWorkspaceStatus, 'Creating cohort from saved query...');
                    const name = document.getElementById('audience-name-input').value.trim()
                        || document.getElementById('sql-saved-query-name').value.trim()
                        || `cohort_${Date.now()}`;
                    await apiRequest(`/sql-workspace/queries/${encodeURIComponent(queryId)}/cohort`, {
                        method: 'POST',
                        body: {
                            name,
                            refresh_mode: document.getElementById('audience-refresh-mode-select').value || 'manual',
                            owner: document.getElementById('audience-owner-input').value || 'frontend_operator',
                            activate: document.getElementById('audience-activate-checkbox').checked,
                        },
                    });
                    setInlineStatus(sqlWorkspaceStatus, 'Saved query converted to cohort.');
                    await loadAudienceEngine();
                } catch (error) {
                    setInlineStatus(sqlWorkspaceStatus, error.message || 'Failed to create cohort from query.', true);
                }
            }

            function renderAudienceCohorts(items = []) {
                renderSimpleTable(
                    audienceCohortList,
                    [
                        { label: 'Name', render: (item) => `<strong>${escapeHtml(item.name || '-')}</strong><div class="subtle">${escapeHtml(item.cohort_id || '-')}</div>` },
                        { label: 'Type', render: (item) => `<span class="pill">${escapeHtml(item.type || '-')}</span>` },
                        { label: 'Status', render: (item) => `<span class="pill">${escapeHtml(item.status || '-')}</span>` },
                        { label: 'Members', render: (item) => escapeHtml(String(item.member_count || 0)) },
                        { label: 'Last Refreshed', render: (item) => escapeHtml(formatDateTime(item.last_refreshed_at)) },
                        {
                            label: 'Actions',
                            render: (item) => `
                                <div class="table-actions">
                                    <button type="button" data-cohort-action="view" data-cohort-id="${escapeHtml(item.cohort_id)}">View</button>
                                    <button type="button" data-cohort-action="refresh" data-cohort-id="${escapeHtml(item.cohort_id)}">Refresh</button>
                                    <button type="button" data-cohort-action="activate" data-cohort-id="${escapeHtml(item.cohort_id)}">Activate</button>
                                    <button type="button" data-cohort-action="pause" data-cohort-id="${escapeHtml(item.cohort_id)}">Pause</button>
                                    <button type="button" data-cohort-action="archive" data-cohort-id="${escapeHtml(item.cohort_id)}">Archive</button>
                                    <button type="button" data-cohort-action="restore" data-cohort-id="${escapeHtml(item.cohort_id)}">Restore</button>
                                </div>
                            `,
                        },
                    ],
                    items,
                    'No cohorts yet.',
                );

                audienceCohortList.querySelectorAll('[data-cohort-action]').forEach((button) => {
                    button.addEventListener('click', async () => {
                        const cohortId = button.dataset.cohortId;
                        const action = button.dataset.cohortAction;
                        try {
                            if (action === 'view') {
                                await loadAudienceCohortDetails(cohortId);
                                return;
                            }
                            if (action === 'refresh') {
                                await apiRequest(`/cohorts/${encodeURIComponent(cohortId)}/refresh`, { method: 'POST' });
                            }
                            if (action === 'activate') {
                                await apiRequest(`/cohorts/${encodeURIComponent(cohortId)}/activate`, { method: 'POST' });
                            }
                            if (action === 'pause') {
                                await apiRequest(`/cohorts/${encodeURIComponent(cohortId)}/pause`, { method: 'POST' });
                            }
                            if (action === 'archive') {
                                await apiRequest(`/cohorts/${encodeURIComponent(cohortId)}/archive`, { method: 'POST' });
                            }
                            if (action === 'restore') {
                                await apiRequest(`/cohorts/${encodeURIComponent(cohortId)}/restore`, { method: 'POST' });
                            }
                            await loadAudienceEngine();
                            await loadAudienceCohortDetails(cohortId);
                        } catch (error) {
                            setInlineStatus(audienceCreateStatus, error.message || `Failed to ${action} cohort.`, true);
                        }
                    });
                });
            }

            async function loadAudienceEngine() {
                try {
                    const [cohortPayload, savedQueryPayload] = await Promise.all([
                        apiRequest('/cohorts'),
                        apiRequest('/sql-workspace/queries'),
                    ]);
                    const cohorts = Array.isArray(cohortPayload.items) ? cohortPayload.items : [];
                    renderAudienceCohorts(cohorts);
                    renderSavedQueries(savedQueryPayload.items || []);
                    populateWorkflowCohortSelect(cohorts);
                    if (selectedAudienceCohortId && cohorts.some((item) => item.cohort_id === selectedAudienceCohortId)) {
                        await loadAudienceCohortDetails(selectedAudienceCohortId);
                    } else if (!selectedAudienceCohortId && cohorts[0]) {
                        await loadAudienceCohortDetails(cohorts[0].cohort_id);
                    } else if (!cohorts.length) {
                        await loadAudienceCohortDetails(null);
                    }
                } catch (error) {
                    setInlineStatus(audienceCreateStatus, error.message || 'Failed to load audience engine.', true);
                }
            }

            async function createAudienceCohort() {
                try {
                    setInlineStatus(audienceCreateStatus, 'Creating cohort...');
                    const payload = await apiRequest('/cohorts', {
                        method: 'POST',
                        body: {
                            name: document.getElementById('audience-name-input').value || `cohort_${Date.now()}`,
                            type: document.getElementById('audience-type-select').value || 'sql',
                            refresh_mode: document.getElementById('audience-refresh-mode-select').value || 'manual',
                            owner: document.getElementById('audience-owner-input').value || 'frontend_operator',
                            description: document.getElementById('audience-description-input').value || '',
                            tags: splitCsv(document.getElementById('audience-tags-input').value),
                            definition: parseJsonText(document.getElementById('audience-definition-json').value, {}),
                            activate: document.getElementById('audience-activate-checkbox').checked,
                        },
                    });
                    selectedAudienceCohortId = payload.cohort_id;
                    setInlineStatus(audienceCreateStatus, `Created cohort ${payload.name}.`);
                    await loadAudienceEngine();
                } catch (error) {
                    setInlineStatus(audienceCreateStatus, error.message || 'Failed to create cohort.', true);
                }
            }

            async function compareAudienceVersions() {
                if (!selectedAudienceCohortId) {
                    renderJsonOutput(audienceCompareOutput, null, 'Select a cohort first.');
                    return;
                }
                try {
                    const baseVersion = Number(document.getElementById('audience-base-version-input').value || 1);
                    const targetVersion = Number(document.getElementById('audience-target-version-input').value || 1);
                    const payload = await apiRequest(`/cohorts/${encodeURIComponent(selectedAudienceCohortId)}/compare?base_version=${baseVersion}&target_version=${targetVersion}`);
                    renderJsonOutput(audienceCompareOutput, payload, 'No version comparison available.');
                } catch (error) {
                    renderJsonOutput(audienceCompareOutput, { error: error.message }, 'Comparison failed.');
                }
            }

            async function rollbackAudienceVersion() {
                if (!selectedAudienceCohortId) {
                    setInlineStatus(audienceCreateStatus, 'Select a cohort first.', true);
                    return;
                }
                try {
                    const version = Number(document.getElementById('audience-base-version-input').value || 1);
                    await apiRequest(`/cohorts/${encodeURIComponent(selectedAudienceCohortId)}/rollback?version=${version}`, { method: 'POST' });
                    setInlineStatus(audienceCreateStatus, `Rolled back cohort to version ${version}.`);
                    await loadAudienceEngine();
                    await loadAudienceCohortDetails(selectedAudienceCohortId);
                } catch (error) {
                    setInlineStatus(audienceCreateStatus, error.message || 'Failed to rollback cohort.', true);
                }
            }

            function renderWorkflowList(items = []) {
                renderSimpleTable(
                    workflowList,
                    [
                        { label: 'Name', render: (item) => `<strong>${escapeHtml(item.name || '-')}</strong><div class="subtle">${escapeHtml(item.workflow_id || '-')}</div>` },
                        { label: 'Status', render: (item) => `<span class="pill">${escapeHtml(item.status || '-')}</span>` },
                        { label: 'Trigger', render: (item) => escapeHtml((item.trigger || {}).type || '-') },
                        { label: 'Experiment', render: (item) => escapeHtml(item.experiment_id || '-') },
                        {
                            label: 'Actions',
                            render: (item) => `
                                <div class="table-actions">
                                    <button type="button" data-workflow-action="view" data-workflow-id="${escapeHtml(item.workflow_id)}">View</button>
                                    <button type="button" data-workflow-action="publish" data-workflow-id="${escapeHtml(item.workflow_id)}">Publish</button>
                                    <button type="button" data-workflow-action="pause" data-workflow-id="${escapeHtml(item.workflow_id)}">Pause</button>
                                    <button type="button" data-workflow-action="resume" data-workflow-id="${escapeHtml(item.workflow_id)}">Resume</button>
                                    <button type="button" data-workflow-action="test-run" data-workflow-id="${escapeHtml(item.workflow_id)}">Test Run</button>
                                </div>
                            `,
                        },
                    ],
                    items,
                    'No workflows yet.',
                );
                workflowList.querySelectorAll('[data-workflow-action]').forEach((button) => {
                    button.addEventListener('click', async () => {
                        const workflowId = button.dataset.workflowId;
                        const action = button.dataset.workflowAction;
                        try {
                            if (action === 'view') {
                                await loadWorkflowDetail(workflowId);
                                return;
                            }
                            if (action === 'publish') {
                                await apiRequest(`/workflows/${encodeURIComponent(workflowId)}/publish`, { method: 'POST' });
                            }
                            if (action === 'pause') {
                                await apiRequest(`/workflows/${encodeURIComponent(workflowId)}/pause`, { method: 'POST' });
                            }
                            if (action === 'resume') {
                                await apiRequest(`/workflows/${encodeURIComponent(workflowId)}/resume`, { method: 'POST' });
                            }
                            if (action === 'test-run') {
                                const payload = await apiRequest(`/workflows/${encodeURIComponent(workflowId)}/test-run`, {
                                    method: 'POST',
                                    body: { limit: 10, confirm: true, sandbox: true, reference_time: getCurrentWorkflowReferenceTime() },
                                });
                                renderJsonOutput(orchestratorRunOutput, payload, 'No workflow run output.');
                                setInlineStatus(orchestratorRunStatus, `Workflow ${workflowId} test-run completed.`);
                            }
                            await loadActionOrchestrator();
                            await loadWorkflowDetail(workflowId);
                        } catch (error) {
                            setInlineStatus(workflowCreateStatus, error.message || `Failed to ${action} workflow.`, true);
                        }
                    });
                });
            }

            async function loadWorkflowDetail(workflowId) {
                selectedWorkflowId = workflowId;
                workflowSelectedLabel.textContent = workflowId || 'No workflow selected';
                if (!workflowId) {
                    workflowExecutionsList.innerHTML = '<div class="list-empty">Select a workflow first.</div>';
                    workflowDeliveriesList.innerHTML = '<div class="list-empty">Select a workflow first.</div>';
                    renderJsonOutput(workflowPolicyOutput, null, 'Policy counters will appear here.');
                    renderJsonOutput(workflowDeliveryDiagnosticsOutput, null, 'Workflow delivery diagnostics will appear here.');
                    return;
                }
                const [executionsPayload, deliveriesPayload, policyPayload, diagnosticsPayload] = await Promise.all([
                    apiRequest(`/workflows/${encodeURIComponent(workflowId)}/executions`),
                    apiRequest(`/workflows/${encodeURIComponent(workflowId)}/deliveries`),
                    apiRequest(`/workflows/${encodeURIComponent(workflowId)}/policy-counters`),
                    apiRequest(`/workflows/${encodeURIComponent(workflowId)}/delivery-diagnostics`),
                ]);
                renderSimpleTable(
                    workflowExecutionsList,
                    [
                        { label: 'Recorded At', render: (item) => escapeHtml(formatDateTime(item.reference_time || item.executed_at || item.recorded_at)) },
                        { label: 'Triggered', render: (item) => escapeHtml(String(item.triggered || 0)) },
                        { label: 'Success', render: (item) => escapeHtml(String(item.success || 0)) },
                        { label: 'Sandbox', render: (item) => escapeHtml(String(Boolean(item.sandbox))) },
                    ],
                    executionsPayload.items || [],
                    'No workflow executions recorded yet.',
                );
                renderSimpleTable(
                    workflowDeliveriesList,
                    [
                        { label: 'Delivery', render: (item) => `<strong>${escapeHtml(item.delivery_id || '-')}</strong><div class="subtle">${escapeHtml(item.user_id || '-')}</div>` },
                        { label: 'Status', render: (item) => `<span class="pill">${escapeHtml(item.delivery_status || item.status || '-')}</span>` },
                        { label: 'Channel', render: (item) => escapeHtml(item.channel || '-') },
                        { label: 'Sandbox', render: (item) => escapeHtml(String(Boolean(item.sandbox))) },
                    ],
                    deliveriesPayload.items || [],
                    'No deliveries recorded yet.',
                );
                renderJsonOutput(workflowPolicyOutput, policyPayload, 'No policy counters recorded.');
                renderJsonOutput(workflowDeliveryDiagnosticsOutput, diagnosticsPayload, 'No workflow delivery diagnostics recorded.');
            }

            async function createWorkflow() {
                try {
                    setInlineStatus(workflowCreateStatus, 'Creating workflow...');
                    const channel = document.getElementById('workflow-channel-select').value || 'push_notification';
                    const payload = await apiRequest('/workflows', {
                        method: 'POST',
                        body: {
                            name: document.getElementById('workflow-name-input').value || `workflow_${Date.now()}`,
                            cohort_id: document.getElementById('workflow-cohort-select').value,
                            experiment_id: document.getElementById('workflow-experiment-id-input').value || null,
                            requires_confirmation: document.getElementById('workflow-requires-confirmation-checkbox').checked,
                            schedule: { type: 'daily' },
                            trigger: {
                                type: document.getElementById('workflow-trigger-type-select').value || 'daily_schedule',
                                hour: Number(document.getElementById('workflow-trigger-hour-input').value || 0),
                                minute: Number(document.getElementById('workflow-trigger-minute-input').value || 0),
                            },
                            action: {
                                channel,
                                content: document.getElementById('workflow-content-input').value || '',
                            },
                            channel_config: {
                                channel,
                                content: document.getElementById('workflow-content-input').value || '',
                            },
                            policy: {
                                global_daily_limit: Number(document.getElementById('workflow-global-limit-input').value || 5),
                                channel_daily_limit: Number(document.getElementById('workflow-channel-limit-input').value || 5),
                                cooldown_hours: Number(document.getElementById('workflow-cooldown-hours-input').value || 24),
                                blacklist_ids: splitCsv(document.getElementById('workflow-blacklist-input').value),
                                quiet_hours: {
                                    start: Number(document.getElementById('workflow-quiet-start-input').value || 22),
                                    end: Number(document.getElementById('workflow-quiet-end-input').value || 7),
                                },
                            },
                            budget_policy: {
                                daily_budget_limit: Number(document.getElementById('workflow-budget-limit-input').value || 25),
                            },
                        },
                    });
                    selectedWorkflowId = payload.workflow_id;
                    setInlineStatus(workflowCreateStatus, `Created workflow ${payload.name}.`);
                    await loadActionOrchestrator();
                    await loadWorkflowDetail(payload.workflow_id);
                } catch (error) {
                    setInlineStatus(workflowCreateStatus, error.message || 'Failed to create workflow.', true);
                }
            }

            async function loadExportDiagnostics() {
                const jobId = document.getElementById('export-diagnostics-select').value;
                if (!jobId) {
                    renderJsonOutput(exportDiagnosticsOutput, null, 'Select an export job first.');
                    return;
                }
                try {
                    const payload = await apiRequest(`/exports/${encodeURIComponent(jobId)}/diagnostics`);
                    renderJsonOutput(exportDiagnosticsOutput, payload, 'No export diagnostics found.');
                } catch (error) {
                    renderJsonOutput(exportDiagnosticsOutput, { error: error.message }, 'Failed to load export diagnostics.');
                }
            }

            async function retryExportDiagnostics() {
                const jobId = document.getElementById('export-diagnostics-select').value;
                if (!jobId) {
                    renderJsonOutput(exportDiagnosticsOutput, null, 'Select an export job first.');
                    return;
                }
                try {
                    const payload = await apiRequest(`/exports/${encodeURIComponent(jobId)}/retry`, { method: 'POST' });
                    renderJsonOutput(exportDiagnosticsOutput, payload, 'Retry returned no payload.');
                    await refreshExportJobsState();
                    populateExportJobSelect(cachedExportJobs);
                } catch (error) {
                    renderJsonOutput(exportDiagnosticsOutput, { error: error.message }, 'Failed to retry export.');
                }
            }

            async function ingestProviderCallbacks() {
                try {
                    setInlineStatus(activationIngestStatus, 'Ingesting callbacks...');
                    const provider = document.getElementById('activation-provider-select').value || 'simulator';
                    const payload = parseJsonText(document.getElementById('activation-callbacks-json').value, { callbacks: [] });
                    const response = await apiRequest(`/activation/callbacks/${encodeURIComponent(provider)}`, {
                        method: 'POST',
                        body: payload,
                    });
                    renderJsonOutput(orchestratorRunOutput, response, 'No callback ingestion response.');
                    setInlineStatus(activationIngestStatus, `Ingested ${response.ingested || 0} callback(s).`);
                    if (selectedWorkflowId) {
                        await loadWorkflowDetail(selectedWorkflowId);
                    }
                } catch (error) {
                    setInlineStatus(activationIngestStatus, error.message || 'Failed to ingest callbacks.', true);
                }
            }

            async function runDueWorkflows() {
                try {
                    setInlineStatus(orchestratorRunStatus, 'Running due workflows...');
                    const payload = await apiRequest('/orchestrator/run-due', {
                        method: 'POST',
                        body: {
                            reference_time: getCurrentWorkflowReferenceTime(),
                            limit_per_workflow: Number(document.getElementById('orchestrator-limit-input').value || 100),
                        },
                    });
                    renderJsonOutput(orchestratorRunOutput, payload, 'No due workflows were executed.');
                    setInlineStatus(orchestratorRunStatus, `Executed ${Array.isArray(payload.items) ? payload.items.length : 0} workflow run(s).`);
                    if (selectedWorkflowId) {
                        await loadWorkflowDetail(selectedWorkflowId);
                    }
                } catch (error) {
                    setInlineStatus(orchestratorRunStatus, error.message || 'Failed to run due workflows.', true);
                }
            }

            async function setKillSwitch(enabled) {
                try {
                    const path = enabled ? '/orchestrator/kill-switch/on' : '/orchestrator/kill-switch/off';
                    const payload = await apiRequest(path, { method: 'POST' });
                    renderJsonOutput(orchestratorRunOutput, payload, 'Kill switch updated.');
                    setInlineStatus(orchestratorRunStatus, `Kill switch ${enabled ? 'enabled' : 'disabled'}.`);
                } catch (error) {
                    setInlineStatus(orchestratorRunStatus, error.message || 'Failed to update kill switch.', true);
                }
            }

            async function loadActionOrchestrator() {
                try {
                    const [workflowPayload, cohortPayload] = await Promise.all([
                        apiRequest('/workflows'),
                        apiRequest('/cohorts'),
                        refreshExportJobsState(),
                    ]);
                    const workflows = Array.isArray(workflowPayload.items) ? workflowPayload.items : [];
                    renderWorkflowList(workflows);
                    populateWorkflowCohortSelect(cohortPayload.items || []);
                    populateExportJobSelect(cachedExportJobs);
                    if (selectedWorkflowId && workflows.some((item) => item.workflow_id === selectedWorkflowId)) {
                        await loadWorkflowDetail(selectedWorkflowId);
                    } else if (!selectedWorkflowId && workflows[0]) {
                        await loadWorkflowDetail(workflows[0].workflow_id);
                    } else if (!workflows.length) {
                        await loadWorkflowDetail(null);
                    }
                } catch (error) {
                    setInlineStatus(workflowCreateStatus, error.message || 'Failed to load action orchestrator.', true);
                }
            }

            function fillExperimentForm(config = {}) {
                document.getElementById('experiment-primary-metric-input').value = config.primary_metric || 'return_rate';
                document.getElementById('experiment-cohort-id-input').value = config.cohort_id || '';
                document.getElementById('experiment-guardrails-input').value = (config.guardrail_metrics || []).join(',');
                document.getElementById('experiment-min-sample-input').value = config.min_sample_size || 20;
                document.getElementById('experiment-min-runtime-input').value = config.min_runtime_hours || 24;
                document.getElementById('experiment-holdout-input').value = config.holdout_pct ?? 0.1;
                document.getElementById('experiment-b-variant-input').value = config.b_variant_pct ?? 0.5;
                document.getElementById('experiment-enabled-checkbox').checked = Boolean(config.enabled ?? true);
            }

            async function loadExperimentWorkspace() {
                const experimentId = getCurrentExperimentId();
                try {
                    const [configPayload, summaryPayload, integrityPayload, exposuresPayload, outcomesPayload] = await Promise.all([
                        apiRequest(`/experiments/config?experiment_id=${encodeURIComponent(experimentId)}`),
                        apiRequest(`/experiments/${encodeURIComponent(experimentId)}/summary`),
                        apiRequest(`/experiments/${encodeURIComponent(experimentId)}/integrity`),
                        apiRequest(`/experiments/${encodeURIComponent(experimentId)}/exposures`),
                        apiRequest(`/experiments/${encodeURIComponent(experimentId)}/outcomes`),
                    ]);
                    fillExperimentForm(configPayload.experiment || {});
                    renderJsonOutput(experimentSummaryOutput, summaryPayload, 'No experiment summary available.');
                    renderJsonOutput(experimentIntegrityOutput, integrityPayload, 'No experiment integrity payload available.');
                    renderSimpleTable(
                        experimentExposuresList,
                        [
                            { label: 'User', render: (item) => escapeHtml(item.user_id || '-') },
                            { label: 'Group', render: (item) => `<span class="pill">${escapeHtml(item.group || '-')}</span>` },
                            { label: 'Execution Status', render: (item) => escapeHtml(item.execution_status || '-') },
                            { label: 'Recorded', render: (item) => escapeHtml(formatDateTime(item.recorded_at || item.exposed_at)) },
                        ],
                        exposuresPayload.items || [],
                        'No exposures recorded yet.',
                    );
                    renderSimpleTable(
                        experimentOutcomesList,
                        [
                            { label: 'User', render: (item) => escapeHtml(item.user_id || '-') },
                            { label: 'Outcome', render: (item) => `<span class="pill">${escapeHtml(item.outcome_name || '-')}</span>` },
                            { label: 'Group', render: (item) => escapeHtml(item.group || '-') },
                            { label: 'Occurred', render: (item) => escapeHtml(formatDateTime(item.occurred_at)) },
                        ],
                        outcomesPayload.items || [],
                        'No outcomes recorded yet.',
                    );
                    setInlineStatus(experimentStatus, `Loaded experiment ${experimentId}.`);
                } catch (error) {
                    setInlineStatus(experimentStatus, error.message || 'Failed to load experiment workspace.', true);
                }
            }

            async function saveExperimentConfig() {
                const experimentId = getCurrentExperimentId();
                try {
                    setInlineStatus(experimentStatus, 'Saving experiment config...');
                    const payload = await apiRequest(`/experiments/config?experiment_id=${encodeURIComponent(experimentId)}`, {
                        method: 'POST',
                        body: {
                            enabled: document.getElementById('experiment-enabled-checkbox').checked,
                            primary_metric: document.getElementById('experiment-primary-metric-input').value || 'return_rate',
                            guardrail_metrics: splitCsv(document.getElementById('experiment-guardrails-input').value),
                            min_sample_size: Number(document.getElementById('experiment-min-sample-input').value || 20),
                            min_runtime_hours: Number(document.getElementById('experiment-min-runtime-input').value || 24),
                            cohort_id: document.getElementById('experiment-cohort-id-input').value || null,
                            holdout_pct: Number(document.getElementById('experiment-holdout-input').value || 0.1),
                            b_variant_pct: Number(document.getElementById('experiment-b-variant-input').value || 0.5),
                        },
                    });
                    fillExperimentForm(payload.experiment || {});
                    setInlineStatus(experimentStatus, `Saved experiment ${experimentId}.`);
                    await loadExperimentWorkspace();
                } catch (error) {
                    setInlineStatus(experimentStatus, error.message || 'Failed to save experiment config.', true);
                }
            }

            async function updateExperimentLifecycle(action) {
                const experimentId = getCurrentExperimentId();
                try {
                    setInlineStatus(experimentStatus, `${action === 'start' ? 'Starting' : 'Stopping'} experiment...`);
                    await apiRequest(`/experiments/${encodeURIComponent(experimentId)}/${action}`, { method: 'POST' });
                    await loadExperimentWorkspace();
                } catch (error) {
                    setInlineStatus(experimentStatus, error.message || `Failed to ${action} experiment.`, true);
                }
            }

            async function decideExperiment() {
                const experimentId = getCurrentExperimentId();
                try {
                    const payload = await apiRequest(`/experiments/${encodeURIComponent(experimentId)}/decision`, {
                        method: 'POST',
                        body: { decided_by: document.getElementById('experiment-decided-by-input').value || 'frontend_operator' },
                    });
                    renderJsonOutput(experimentSummaryOutput, payload, 'No experiment decision available.');
                    setInlineStatus(experimentStatus, `Recorded decision for ${experimentId}.`);
                    await loadExperimentWorkspace();
                } catch (error) {
                    setInlineStatus(experimentStatus, error.message || 'Failed to record decision.', true);
                }
            }

            async function ingestExperimentOutcomes() {
                const experimentId = getCurrentExperimentId();
                try {
                    setInlineStatus(experimentIngestStatus, 'Ingesting outcomes...');
                    const payload = parseJsonText(document.getElementById('experiment-outcomes-json').value, { outcomes: [] });
                    const response = await apiRequest(`/experiments/${encodeURIComponent(experimentId)}/outcomes:ingest`, {
                        method: 'POST',
                        body: payload,
                    });
                    setInlineStatus(experimentIngestStatus, `Ingested ${response.ingested || 0} outcomes.`);
                    await loadExperimentWorkspace();
                } catch (error) {
                    setInlineStatus(experimentIngestStatus, error.message || 'Failed to ingest outcomes.', true);
                }
            }

            async function loadExperimentHub() {
                await loadExperimentWorkspace();
            }

            function renderCopilotMetaList(container, items = [], label) {
                renderSimpleTable(
                    container,
                    [
                        { label: `${label} ID`, render: (item) => escapeHtml(item[`${label.toLowerCase()}_id`] || item.query_id || '-') },
                        { label: 'Created', render: (item) => escapeHtml(formatDateTime(item.created_at)) },
                        { label: 'Summary', render: (item) => `<span class="subtle">${escapeHtml(JSON.stringify(item.drivers || item.response || item.context || item.methodology || {}))}</span>` },
                    ],
                    items,
                    `No ${label.toLowerCase()} items found.`,
                );
            }

            async function runCopilotRequest(path, body) {
                try {
                    const payload = await apiRequest(path, { method: 'POST', body });
                    renderJsonOutput(copilotResponseOutput, payload, 'No copilot response.');
                    if (payload.query_id) {
                        document.getElementById('copilot-query-log-id-input').value = payload.query_id;
                    }
                    await loadCopilotMeta();
                } catch (error) {
                    renderJsonOutput(copilotResponseOutput, { error: error.message }, 'Copilot request failed.');
                }
            }

            async function loadCopilotMeta() {
                try {
                    const [anomaliesPayload, reportsPayload] = await Promise.all([
                        apiRequest('/copilot/anomalies'),
                        apiRequest('/copilot/reports'),
                    ]);
                    renderCopilotMetaList(copilotAnomaliesList, anomaliesPayload.items || [], 'Anomaly');
                    renderCopilotMetaList(copilotReportsList, reportsPayload.items || [], 'Report');
                } catch (error) {
                    renderJsonOutput(copilotQueryLogOutput, { error: error.message }, 'Failed to load copilot metadata.');
                }
            }

            async function loadCopilotQueryLog() {
                const queryId = (document.getElementById('copilot-query-log-id-input').value || '').trim();
                if (!queryId) {
                    renderJsonOutput(copilotQueryLogOutput, null, 'Enter a query log ID first.');
                    return;
                }
                try {
                    const payload = await apiRequest(`/copilot/query-logs/${encodeURIComponent(queryId)}`);
                    renderJsonOutput(copilotQueryLogOutput, payload, 'No query log available.');
                } catch (error) {
                    renderJsonOutput(copilotQueryLogOutput, { error: error.message }, 'Failed to load query log.');
                }
            }

            async function loadInsightCopilot() {
                await loadCopilotMeta();
            }

            audienceCreateCohortBtn.addEventListener('click', createAudienceCohort);
            actionHistoryRefreshBtn.addEventListener('click', loadActionHistory);
            document.getElementById('audience-refresh-list-btn').addEventListener('click', loadAudienceEngine);
            document.getElementById('audience-load-members-btn').addEventListener('click', () => loadAudienceMembers());
            document.getElementById('audience-load-versions-btn').addEventListener('click', () => loadAudienceVersions());
            document.getElementById('audience-load-metrics-btn').addEventListener('click', () => loadAudienceMetrics());
            document.getElementById('audience-compare-btn').addEventListener('click', compareAudienceVersions);
            document.getElementById('audience-rollback-btn').addEventListener('click', rollbackAudienceVersion);
            document.getElementById('sql-preview-btn').addEventListener('click', previewSqlWorkspace);
            document.getElementById('sql-save-query-btn').addEventListener('click', saveSqlWorkspaceQuery);
            document.getElementById('sql-create-cohort-btn').addEventListener('click', async () => {
                const firstQuery = cachedSavedQueries[0];
                if (!firstQuery) {
                    setInlineStatus(sqlWorkspaceStatus, 'Save a query first, or use a saved query action.', true);
                    return;
                }
                await createCohortFromSavedQuery(firstQuery.query_id);
            });
            document.getElementById('workflow-create-btn').addEventListener('click', createWorkflow);
            document.getElementById('workflow-refresh-list-btn').addEventListener('click', loadActionOrchestrator);
            document.getElementById('orchestrator-run-due-btn').addEventListener('click', runDueWorkflows);
            document.getElementById('orchestrator-kill-on-btn').addEventListener('click', () => setKillSwitch(true));
            document.getElementById('orchestrator-kill-off-btn').addEventListener('click', () => setKillSwitch(false));
            document.getElementById('activation-ingest-btn').addEventListener('click', ingestProviderCallbacks);
            document.getElementById('export-diagnostics-btn').addEventListener('click', loadExportDiagnostics);
            document.getElementById('export-retry-btn').addEventListener('click', retryExportDiagnostics);
            document.getElementById('workflow-load-delivery-diagnostics-btn').addEventListener('click', async () => {
                if (!selectedWorkflowId) {
                    renderJsonOutput(workflowDeliveryDiagnosticsOutput, null, 'Select a workflow first.');
                    return;
                }
                try {
                    const payload = await apiRequest(`/workflows/${encodeURIComponent(selectedWorkflowId)}/delivery-diagnostics`);
                    renderJsonOutput(workflowDeliveryDiagnosticsOutput, payload, 'No workflow delivery diagnostics recorded.');
                } catch (error) {
                    renderJsonOutput(workflowDeliveryDiagnosticsOutput, { error: error.message }, 'Failed to load workflow delivery diagnostics.');
                }
            });
            document.getElementById('experiment-load-btn').addEventListener('click', loadExperimentWorkspace);
            document.getElementById('experiment-save-btn').addEventListener('click', saveExperimentConfig);
            document.getElementById('experiment-start-btn').addEventListener('click', () => updateExperimentLifecycle('start'));
            document.getElementById('experiment-stop-btn').addEventListener('click', () => updateExperimentLifecycle('stop'));
            document.getElementById('experiment-refresh-btn').addEventListener('click', loadExperimentWorkspace);
            document.getElementById('experiment-load-integrity-btn').addEventListener('click', async () => {
                try {
                    const payload = await apiRequest(`/experiments/${encodeURIComponent(getCurrentExperimentId())}/integrity`);
                    renderJsonOutput(experimentIntegrityOutput, payload, 'No experiment integrity payload available.');
                } catch (error) {
                    renderJsonOutput(experimentIntegrityOutput, { error: error.message }, 'Failed to load experiment integrity.');
                }
            });
            document.getElementById('experiment-decision-btn').addEventListener('click', decideExperiment);
            document.getElementById('experiment-ingest-outcomes-btn').addEventListener('click', ingestExperimentOutcomes);
            document.getElementById('copilot-query-btn').addEventListener('click', async () => {
                await runCopilotRequest('/copilot/query', {
                    question: document.getElementById('copilot-question-input').value,
                    time_window: document.getElementById('copilot-query-window-input').value || null,
                    filters: parseJsonText(document.getElementById('copilot-query-filters-json').value, {}),
                });
            });
            document.getElementById('copilot-explain-btn').addEventListener('click', async () => {
                await runCopilotRequest('/copilot/explain', {
                    metric_id: document.getElementById('copilot-explain-metric-input').value,
                    time_window: document.getElementById('copilot-explain-window-input').value || '7d',
                    dimensions: splitCsv(document.getElementById('copilot-explain-dimensions-input').value),
                });
            });
            document.getElementById('copilot-recommend-btn').addEventListener('click', async () => {
                await runCopilotRequest('/copilot/recommend', {
                    insight: parseJsonText(document.getElementById('copilot-insight-json').value, {}),
                    metric_context: parseJsonText(document.getElementById('copilot-metric-context-json').value, {}),
                });
            });
            document.getElementById('copilot-report-btn').addEventListener('click', async () => {
                await runCopilotRequest('/copilot/report', {
                    report_type: document.getElementById('copilot-report-type-select').value || 'daily',
                    time_window: document.getElementById('copilot-report-window-input').value || '7d',
                });
            });
            document.getElementById('copilot-load-query-log-btn').addEventListener('click', loadCopilotQueryLog);
            document.getElementById('copilot-refresh-meta-btn').addEventListener('click', loadCopilotMeta);
            document.getElementById('templates-refresh-btn').addEventListener('click', loadScenarioTemplates);
            document.getElementById('template-instantiate-btn').addEventListener('click', instantiateScenarioTemplate);
            document.getElementById('service-health-refresh-btn').addEventListener('click', loadServiceHealthStatus);
            document.getElementById('service-health-tick-btn').addEventListener('click', runServiceHealthTick);

            // Health Check Logic
            const healthStatusDiv = document.getElementById('health-status');
            const statusTextSpan = document.getElementById('status-text');

            async function checkBackendStatus() {
                try {
                    await ensureHealthState(true);
                    
                    healthStatusDiv.classList.add('connected');
                    statusTextSpan.textContent = `Backend Connected (${backendMode})`;
                } catch (error) {
                    healthStatusDiv.classList.remove('connected');
                    statusTextSpan.textContent = 'Backend Disconnected';
                    console.error('Health check failed:', error);
                }
            }

            async function initializeAuthSession() {
                await loadOidcConfig();
                try {
                    await handleOidcRedirect();
                    await hydrateAuthSession();
                } catch (error) {
                    setAuthStatus(error.message || 'OIDC session initialization failed.');
                }
            }

            // Check status on page load and then every 30 seconds.
            initializeAuthSession().finally(() => {
                checkBackendStatus();
                setInterval(checkBackendStatus, HEALTH_CHECK_INTERVAL_MS);
                activateModule('data-core');
            });

            // Theme Toggle Logic
            const themeToggle = document.getElementById('theme-switch-checkbox');
            const themeLabel = document.getElementById('theme-label');
            const currentTheme = localStorage.getItem('theme');

            function setTheme(isDark) {
                document.body.classList.toggle('dark-theme', isDark);
                themeToggle.checked = isDark;
                themeLabel.textContent = isDark ? 'Light Mode' : 'Dark Mode';
                localStorage.setItem('theme', isDark ? 'dark-theme' : 'light-theme');
            }

            // Set initial theme based on localStorage
            if (currentTheme === 'dark-theme') {
                setTheme(true);
            } else {
                setTheme(false);
            }

            themeToggle.addEventListener('change', function() {
                setTheme(this.checked);
            });
        });
