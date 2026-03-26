// KairyxAI operator console runtime. Mounted by the React shell.
export function initializeOperatorConsole() {
    if (typeof window !== 'undefined' && window.__KAIRYX_OPERATOR_CONSOLE_INITIALIZED) {
        return;
    }
    if (typeof window !== 'undefined') {
        window.__KAIRYX_OPERATOR_CONSOLE_INITIALIZED = true;
    }
            const sidebarNav = document.getElementById('sidebar-nav');
            const pages = document.querySelectorAll('.page');
            const contentScroll = document.querySelector('.content-scroll');
            const moduleTitle = document.getElementById('module-title');
            const moduleSubtitle = document.getElementById('module-subtitle');
            const sidebarBackdrop = document.getElementById('sidebar-backdrop');
            const sidebarCollapseBtn = document.getElementById('sidebar-collapse-btn');
            const mobileNavOpenBtn = document.getElementById('mobile-nav-open-btn');
            const mobileNavCloseBtn = document.getElementById('mobile-nav-close-btn');
            const sidebarSessionButton = document.getElementById('sidebar-session-button');
            const sidebarSessionAvatar = document.getElementById('sidebar-session-avatar');
            const sidebarSessionAvatarImage = document.getElementById('sidebar-session-avatar-image');
            const sidebarSessionAvatarFallback = document.getElementById('sidebar-session-avatar-fallback');
            const sidebarSessionName = document.getElementById('sidebar-session-name');
            const sidebarSessionMeta = document.getElementById('sidebar-session-meta');
            const sidebarSessionMenu = document.getElementById('sidebar-session-menu');
            const sidebarSessionLogoutBtn = document.getElementById('sidebar-session-logout-btn');
            const topbarSearchForm = document.getElementById('topbar-search-form');
            const topbarSearchInput = document.getElementById('topbar-search-input');
            const topbarSearchStatus = document.getElementById('topbar-search-status');
            const settingsWorkspaceSummary = document.getElementById('settings-workspace-summary');
            const settingsSessionSummary = document.getElementById('settings-session-summary');
            const settingsAuthCopy = document.getElementById('settings-auth-copy');
            const settingsOpenSwitcherBtn = document.getElementById('settings-open-switcher-btn');
            const settingsCreateProjectBtn = document.getElementById('settings-create-project-btn');
            const authStatusText = document.getElementById('auth-status-text');
            const oidcLoginBtn = document.getElementById('oidc-login-btn');
            const oidcLogoutBtn = document.getElementById('oidc-logout-btn');
            const apiKeyInput = document.getElementById('api-key-input');
            const legacyAuthControls = document.getElementById('legacy-auth-controls');
            const legacyApiKeyGroup = document.getElementById('legacy-api-key-group');
            const oidcWorkspaceControls = document.getElementById('oidc-workspace-controls');
            const orgSpaceSelect = document.getElementById('org-space-select');
            const projectSelect = document.getElementById('project-select');
            const workspaceSummaryText = document.getElementById('workspace-summary-text');
            const workspaceRoleSummary = document.getElementById('workspace-role-summary');
            const workspaceSelectorStatus = document.getElementById('workspace-selector-status');
            const workspaceOpenSwitcherBtn = document.getElementById('workspace-open-switcher-btn');
            const workspaceCreateProjectBtn = document.getElementById('workspace-create-project-btn');
            const workspaceOverlay = document.getElementById('workspace-overlay');
            const workspaceModalTitle = document.getElementById('workspace-modal-title');
            const workspaceModalSubtitle = document.getElementById('workspace-modal-subtitle');
            const workspaceModalEyebrow = document.getElementById('workspace-modal-eyebrow');
            const workspaceStartupStatus = document.getElementById('workspace-startup-status');
            const workspaceModalCloseBtn = document.getElementById('workspace-modal-close-btn');
            const workspaceLoginPanel = document.getElementById('workspace-login-panel');
            const workspaceLoginStatus = document.getElementById('workspace-login-status');
            const workspaceGoogleLoginBtn = document.getElementById('workspace-google-login-btn');
            const googleLoginContainer = oidcLoginBtn ? document.createElement('div') : null;
            const workspaceGoogleLoginContainer = workspaceGoogleLoginBtn ? document.createElement('div') : null;
            const workspaceSelectionPanel = document.getElementById('workspace-selection-panel');
            const workspaceOnboardingPanel = document.getElementById('workspace-onboarding-panel');
            const workspaceCreateProjectPanel = document.getElementById('workspace-create-project-panel');
            const workspaceSelectionOrgStage = document.getElementById('workspace-selection-org-stage');
            const workspaceSelectionProjectStage = document.getElementById('workspace-selection-project-stage');
            const workspaceOrgChooserGroup = document.getElementById('workspace-org-chooser-group');
            const workspaceModalOrgSelect = document.getElementById('workspace-modal-org-select');
            const workspaceOrgUrlInput = document.getElementById('workspace-org-url-input');
            const workspaceOrgSuggestions = document.getElementById('workspace-org-suggestions');
            const workspaceSelectionUrlPrefix = document.getElementById('workspace-selection-url-prefix');
            const workspaceOnboardingUrlPrefix = document.getElementById('workspace-onboarding-url-prefix');
            const workspaceSelectionCurrentOrg = document.getElementById('workspace-selection-current-org');
            const workspaceModalProjectSelect = document.getElementById('workspace-modal-project-select');
            const workspaceExistingProjectGroup = document.getElementById('workspace-existing-project-group');
            const workspaceSelectionDivider = document.getElementById('workspace-selection-divider');
            const workspaceSelectionCopy = document.getElementById('workspace-selection-copy');
            const workspaceSelectionStatus = document.getElementById('workspace-selection-status');
            const workspaceSelectionBackBtn = document.getElementById('workspace-selection-back-btn');
            const workspaceSelectionResolveBtn = document.getElementById('workspace-selection-resolve-btn');
            const workspaceSelectionSwitchAccountBtn = document.getElementById('workspace-selection-switch-account-btn');
            const workspaceSelectionContinueBtn = document.getElementById('workspace-selection-continue-btn');
            const workspaceSelectionCreateProjectBtn = document.getElementById('workspace-selection-create-project-btn');
            const workspaceOnboardingStepLabel = document.getElementById('workspace-onboarding-step-label');
            const onboardingOrganizationNameInput = document.getElementById('onboarding-organization-name');
            const onboardingOrganizationIdInput = document.getElementById('onboarding-organization-id');
            const onboardingProjectNameInput = document.getElementById('onboarding-project-name');
            const onboardingProjectIdInput = document.getElementById('onboarding-project-id');
            const onboardingProjectDescriptionInput = document.getElementById('onboarding-project-description');
            const onboardingInviteEmailInput = document.getElementById('onboarding-invite-email');
            const onboardingInviteDisplayNameInput = document.getElementById('onboarding-invite-display-name');
            const onboardingInviteOrgRoleSelect = document.getElementById('onboarding-invite-org-role');
            const onboardingInviteProjectRoleSelect = document.getElementById('onboarding-invite-project-role');
            const onboardingInviteLinkInput = document.getElementById('onboarding-invite-link');
            const onboardingInviteStatus = document.getElementById('onboarding-invite-status');
            const onboardingStatus = document.getElementById('workspace-onboarding-status');
            const onboardingBackBtn = document.getElementById('onboarding-back-btn');
            const onboardingNextBtn = document.getElementById('onboarding-next-btn');
            const onboardingGenerateInviteBtn = document.getElementById('onboarding-generate-invite-btn');
            const onboardingConnectBtn = document.getElementById('onboarding-connect-btn');
            const onboardingSkipBtn = document.getElementById('onboarding-skip-btn');
            const onboardingStepPanels = document.querySelectorAll('.workspace-step-panel');
            const workspaceCreateProjectNameInput = document.getElementById('workspace-create-project-name');
            const workspaceCreateProjectIdInput = document.getElementById('workspace-create-project-id');
            const workspaceCreateProjectDescriptionInput = document.getElementById('workspace-create-project-description');
            const workspaceCreateProjectInlineNameInput = document.getElementById('workspace-create-project-inline-name');
            const workspaceCreateProjectInlineIdInput = document.getElementById('workspace-create-project-inline-id');
            const workspaceCreateProjectInlineDescriptionInput = document.getElementById('workspace-create-project-inline-description');
            const workspaceCreateProjectCurrentOrg = document.getElementById('workspace-create-project-current-org');
            const workspaceCreateProjectStatus = document.getElementById('workspace-create-project-status');
            const workspaceCreateProjectCancelBtn = document.getElementById('workspace-create-project-cancel-btn');
            const workspaceCreateProjectSubmitBtn = document.getElementById('workspace-create-project-submit-btn');
            const SIDEBAR_AUTO_COLLAPSE_MAX_WIDTH = 1200;
            const SIDEBAR_MOBILE_MAX_WIDTH = 960;
            const TENANT_ID_STORAGE_KEY = 'kairyx.tenantId';
            const PROJECT_ID_STORAGE_KEY = 'kairyx.projectId';
            const API_KEY_STORAGE_KEY = 'kairyx.apiKey';
            const ACCESS_TOKEN_STORAGE_KEY = 'kairyx.accessToken';
            const CONNECTORS_VERSION_STORAGE_KEY = 'kairyx.connectorsVersion';
            const IMPORTS_VERSION_STORAGE_KEY = 'kairyx.importsVersion';
            const OIDC_CODE_VERIFIER_STORAGE_KEY = 'kairyx.oidcCodeVerifier';
            const GOOGLE_IDENTITY_SCRIPT_SRC = 'https://accounts.google.com/gsi/client';
            const PENDING_INVITE_STORAGE_KEY = 'kairyx.pendingInvite';
            const LOCAL_DEMO_ACTOR_ID = 'local-demo';
            const LOCAL_DEMO_ACTOR_ROLE = 'admin';
            const LOCAL_DEMO_TENANT_ID = 'default';
            const LOCAL_DEMO_PROJECT_ID = 'default';
            const MOCK_STORAGE_KEY_PREFIX = 'kairyx.mockState.v1';
            const preferLocalMockState = (
                window.KAIRYX_LOCAL_MOCK_STATE === true
                || new URLSearchParams(window.location.search).get('mock_state') === 'local'
                || window.location.hostname.endsWith('vercel.app')
            );
            let activeModuleId = 'data-core';
            let activeNavItemId = 'data-core-churn-rescue';
            let activePageId = 'operator-hub';
            let expandedSidebarModuleId = 'data-core';
            let expandedSidebarSuppressedModuleId = null;
            let navItems = [];
            let navLinks = [];
            let navSubmenuLinks = [];
            let collapsedSidebarSuppressedModuleId = null;
            let oidcConfig = null;
            let accessToken = '';
            let authSessionState = null;
            let workspaceOverlayMode = null;
            let onboardingStep = 1;
            let onboardingResult = null;
            let onboardingFromWorkspaceSelection = false;
            let inviteRedemptionInFlight = false;
            let googleIdentityScriptPromise = null;
            let googleIdentityButtonClientId = '';
            let rootGatewayBootPending = !getOrganizationIdFromPathname();
            const moduleConfigs = {
                'data-core': {
                    title: 'Data Core',
                    subtitle: 'Manage connectors, imports, mappings, governance checks, and health signals that power the closed-loop lifecycle.',
                    icon: `
                        <svg viewBox="0 0 24 24" aria-hidden="true">
                            <rect x="4" y="4" width="6" height="6" rx="1.5"></rect>
                            <rect x="14" y="4" width="6" height="6" rx="1.5"></rect>
                            <rect x="4" y="14" width="6" height="6" rx="1.5"></rect>
                            <rect x="14" y="14" width="6" height="6" rx="1.5"></rect>
                        </svg>
                    `,
                    items: [
                        { id: 'data-core-churn-rescue', label: 'Churn Rescue', pageId: 'operator-hub' },
                        { id: 'data-core-imports', label: 'Imports', pageId: 'player-cohorts' },
                        { id: 'data-core-connectors', label: 'Connectors', pageId: 'connectors' },
                        { id: 'data-core-mappings', label: 'Mappings', pageId: 'data-sandbox' },
                        { id: 'data-core-audit-trail', label: 'Audit Trail', pageId: 'action-history' },
                        { id: 'data-core-templates', label: 'Templates', pageId: 'scenario-templates' },
                        { id: 'data-core-health', label: 'Health', pageId: 'service-health' },
                        { id: 'data-core-governance', label: 'Governance', pageId: 'safety-rails' },
                    ],
                },
                'audience-engine': {
                    title: 'Audience Engine',
                    subtitle: 'Build and operate cohorts with SQL workspace support, refresh controls, member previews, metrics, and version-aware rollback.',
                    icon: `
                        <svg viewBox="0 0 24 24" aria-hidden="true">
                            <path d="M7 18c0-2.21 2.24-4 5-4s5 1.79 5 4" fill="none" stroke="currentColor" stroke-linecap="round" stroke-width="1.7"></path>
                            <circle cx="12" cy="8" r="3.2" fill="none" stroke="currentColor" stroke-width="1.7"></circle>
                        </svg>
                    `,
                    items: [
                        { id: 'audience-engine-build', label: 'Create Cohort', pageId: 'audience-engine', targetId: 'audience-create-section' },
                        { id: 'audience-engine-sql', label: 'SQL Workspace', pageId: 'audience-engine', targetId: 'audience-sql-section' },
                        { id: 'audience-engine-cohorts', label: 'Cohorts', pageId: 'audience-engine', targetId: 'audience-list-section' },
                        { id: 'audience-engine-versions', label: 'Versions & Comparison', pageId: 'audience-engine', targetId: 'audience-versions-section' },
                    ],
                },
                'action-orchestrator': {
                    title: 'Action Orchestrator',
                    subtitle: 'Configure workflow runtime, publish and test journeys, inspect deliveries, and reconcile provider callbacks into durable execution logs.',
                    icon: `
                        <svg viewBox="0 0 24 24" aria-hidden="true">
                            <path d="M5 7h14M5 12h9M5 17h14" fill="none" stroke="currentColor" stroke-linecap="round" stroke-width="1.8"></path>
                            <circle cx="17" cy="12" r="2" fill="currentColor"></circle>
                        </svg>
                    `,
                    items: [
                        { id: 'action-orchestrator-create', label: 'Workflow Studio', pageId: 'action-orchestrator', targetId: 'workflow-create-section' },
                        { id: 'action-orchestrator-runtime', label: 'Runtime Controls', pageId: 'action-orchestrator', targetId: 'workflow-runtime-section' },
                        { id: 'action-orchestrator-workflows', label: 'Workflows', pageId: 'action-orchestrator', targetId: 'workflow-list-section' },
                        { id: 'action-orchestrator-deliveries', label: 'Deliveries', pageId: 'action-orchestrator', targetId: 'workflow-deliveries-section' },
                    ],
                },
                'experiment-hub': {
                    title: 'Experiment Hub',
                    subtitle: 'Operate treatment-vs-holdout experiments with explicit configuration, exposure and outcome inspection, summary gates, and decision logging.',
                    icon: `
                        <svg viewBox="0 0 24 24" aria-hidden="true">
                            <path d="M8 4h8M10 4v5l-4.5 7.5A2 2 0 0 0 7.2 20h9.6a2 2 0 0 0 1.7-3.5L14 9V4" fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="1.7"></path>
                        </svg>
                    `,
                    items: [
                        { id: 'experiment-hub-control', label: 'Experiment Control', pageId: 'experiment-hub', targetId: 'experiment-control-section' },
                        { id: 'experiment-hub-summary', label: 'Summary', pageId: 'experiment-hub', targetId: 'experiment-summary-section' },
                        { id: 'experiment-hub-results', label: 'Exposures & Outcomes', pageId: 'experiment-hub', targetId: 'experiment-results-section' },
                        { id: 'experiment-hub-ingestion', label: 'Outcome Ingestion', pageId: 'experiment-hub', targetId: 'experiment-ingest-section' },
                    ],
                },
                'insight-copilot': {
                    title: 'Insight Copilot',
                    subtitle: 'Run query, explain, recommend, and report flows against curated evidence with query logs, anomaly tracking, and archived reports.',
                    icon: `
                        <svg viewBox="0 0 24 24" aria-hidden="true">
                            <path d="M8 15h8M8 11h8M10 19h4" fill="none" stroke="currentColor" stroke-linecap="round" stroke-width="1.8"></path>
                            <path d="M7 5h10a2 2 0 0 1 2 2v7.5a2 2 0 0 1-.8 1.6l-3.5 2.6a2 2 0 0 1-1.2.4h-3a2 2 0 0 1-1.2-.4l-3.5-2.6a2 2 0 0 1-.8-1.6V7a2 2 0 0 1 2-2Z" fill="none" stroke="currentColor" stroke-width="1.7"></path>
                        </svg>
                    `,
                    items: [
                        { id: 'insight-copilot-query', label: 'Query', pageId: 'insight-copilot', targetId: 'copilot-query-section' },
                        { id: 'insight-copilot-explain', label: 'Explain', pageId: 'insight-copilot', targetId: 'copilot-explain-section' },
                        { id: 'insight-copilot-recommend', label: 'Recommend', pageId: 'insight-copilot', targetId: 'copilot-recommend-section' },
                        { id: 'insight-copilot-report', label: 'Report', pageId: 'insight-copilot', targetId: 'copilot-report-section' },
                        { id: 'insight-copilot-evidence', label: 'Evidence & Logs', pageId: 'insight-copilot', targetId: 'copilot-evidence-section' },
                    ],
                },
                'help': {
                    title: 'Help',
                    subtitle: 'Read the current v1 manual, follow the end-to-end operator path, and copy sample SQL or JSON payloads that match the live UI.',
                    icon: `
                        <svg viewBox="0 0 24 24" aria-hidden="true">
                            <circle cx="12" cy="12" r="8" fill="none" stroke="currentColor" stroke-width="1.7"></circle>
                            <path d="M9.5 9.5a2.5 2.5 0 1 1 4.28 1.77c-.76.75-1.78 1.32-1.78 2.73" fill="none" stroke="currentColor" stroke-linecap="round" stroke-width="1.7"></path>
                            <circle cx="12" cy="17" r="1" fill="currentColor"></circle>
                        </svg>
                    `,
                    items: [
                        { id: 'help-overview', label: 'Overview', pageId: 'help', targetId: 'help-overview-section' },
                        { id: 'help-quickstart', label: 'Quick Start', pageId: 'help', targetId: 'help-quickstart-section' },
                        { id: 'help-roles', label: 'Role Guide', pageId: 'help', targetId: 'help-roles-section' },
                        { id: 'help-samples', label: 'Samples', pageId: 'help', targetId: 'help-samples-section' },
                        { id: 'help-common-issues', label: 'Common Issues', pageId: 'help', targetId: 'help-issues-section' },
                    ],
                },
                'settings': {
                    title: 'Settings',
                    subtitle: 'Manage appearance, workspace tools, shell preferences, and session controls without leaving the operator console.',
                    showSubmenu: false,
                    icon: `
                        <svg viewBox="0 0 24 24" aria-hidden="true">
                            <path d="M12 8.5a3.5 3.5 0 1 0 0 7 3.5 3.5 0 0 0 0-7Z" fill="none" stroke="currentColor" stroke-width="1.7"></path>
                            <path d="M4.8 13.3a1.5 1.5 0 0 1 0-2.6l1.2-.7a6.9 6.9 0 0 1 .7-1.7L6.3 7a1.5 1.5 0 0 1 0-2.1l1.6-1.6a1.5 1.5 0 0 1 2.1 0l1 .4c.55-.25 1.12-.46 1.72-.58l.7-1.2a1.5 1.5 0 0 1 2.6 0l.7 1.2c.6.12 1.17.33 1.72.58l1-.4a1.5 1.5 0 0 1 2.1 0l1.6 1.6a1.5 1.5 0 0 1 0 2.1l-.4 1c.25.55.46 1.12.58 1.72l1.2.7a1.5 1.5 0 0 1 0 2.6l-1.2.7a6.92 6.92 0 0 1-.58 1.72l.4 1a1.5 1.5 0 0 1 0 2.1l-1.6 1.6a1.5 1.5 0 0 1-2.1 0l-1-.4c-.55.25-1.12.46-1.72.58l-.7 1.2a1.5 1.5 0 0 1-2.6 0l-.7-1.2a6.9 6.9 0 0 1-1.72-.58l-1 .4a1.5 1.5 0 0 1-2.1 0l-1.6-1.6a1.5 1.5 0 0 1 0-2.1l.4-1a6.9 6.9 0 0 1-.58-1.72Z" fill="none" stroke="currentColor" stroke-width="1.5"></path>
                        </svg>
                    `,
                    items: [
                        { id: 'settings-profile', label: 'Profile', pageId: 'settings', targetId: 'settings-tab-panel-profile' },
                        { id: 'settings-organization', label: 'Organization', pageId: 'settings', targetId: 'settings-tab-panel-organization' },
                        { id: 'settings-projects', label: 'Projects', pageId: 'settings', targetId: 'settings-tab-panel-projects' },
                        { id: 'settings-teams', label: 'Teams', pageId: 'settings', targetId: 'settings-tab-panel-teams' },
                        { id: 'settings-notifications', label: 'Notifications', pageId: 'settings', targetId: 'settings-tab-panel-notifications' },
                        { id: 'settings-billing', label: 'Billing', pageId: 'settings', targetId: 'settings-tab-panel-billing' },
                    ],
                },
            };

            function getModuleItems(moduleId) {
                return moduleConfigs[moduleId]?.items || [];
            }

            function hasSidebarSubmenu(moduleId) {
                return moduleConfigs[moduleId]?.showSubmenu !== false && getModuleItems(moduleId).length > 1;
            }

            function findModuleItem(moduleId, itemOrPageId = '') {
                if (!itemOrPageId) {
                    return getModuleItems(moduleId)[0] || null;
                }
                return getModuleItems(moduleId).find((entry) => entry.id === itemOrPageId || entry.pageId === itemOrPageId) || null;
            }

            function syncCollapsedSidebarSubmenuSuppression() {
                const suppressedModuleId = document.body.classList.contains('sidebar-is-collapsed')
                    ? collapsedSidebarSuppressedModuleId
                    : null;
                navItems.forEach((entry) => {
                    entry.classList.toggle('sidebar-popout-suppressed', entry.dataset.module === suppressedModuleId);
                });
            }

            function syncExpandedSidebarSubmenuSuppression() {
                const suppressedModuleId = !isSidebarMobileViewport() && !document.body.classList.contains('sidebar-is-collapsed')
                    ? expandedSidebarSuppressedModuleId
                    : null;
                navItems.forEach((entry) => {
                    entry.classList.toggle('sidebar-inline-submenu-suppressed', entry.dataset.module === suppressedModuleId);
                });
            }

            function suppressCollapsedSidebarSubmenu(moduleId = null) {
                collapsedSidebarSuppressedModuleId = moduleId || null;
                syncCollapsedSidebarSubmenuSuppression();
            }

            function clearCollapsedSidebarSubmenuSuppression(moduleId = null) {
                if (moduleId && collapsedSidebarSuppressedModuleId !== moduleId) {
                    return;
                }
                collapsedSidebarSuppressedModuleId = null;
                syncCollapsedSidebarSubmenuSuppression();
            }

            function suppressExpandedSidebarSubmenu(moduleId = null) {
                expandedSidebarSuppressedModuleId = moduleId || null;
                syncExpandedSidebarSubmenuSuppression();
            }

            function clearExpandedSidebarSubmenuSuppression(moduleId = null) {
                if (moduleId && expandedSidebarSuppressedModuleId !== moduleId) {
                    return;
                }
                expandedSidebarSuppressedModuleId = null;
                syncExpandedSidebarSubmenuSuppression();
            }

            function renderSidebarNav() {
                if (!sidebarNav) {
                    return;
                }
                sidebarNav.innerHTML = '';
                Object.entries(moduleConfigs).forEach(([moduleId, config]) => {
                    const moduleItems = getModuleItems(moduleId);
                    const showSubmenu = hasSidebarSubmenu(moduleId);
                    const listItem = document.createElement('li');
                    listItem.className = 'sidebar-nav-item';
                    listItem.dataset.module = moduleId;

                    const trigger = document.createElement('button');
                    trigger.type = 'button';
                    trigger.className = 'nav-link nav-link-trigger';
                    trigger.dataset.module = moduleId;
                    trigger.setAttribute('aria-haspopup', showSubmenu ? 'true' : 'false');

                    const icon = document.createElement('span');
                    icon.className = 'nav-icon';
                    icon.setAttribute('aria-hidden', 'true');
                    icon.innerHTML = config.icon.trim();

                    const copy = document.createElement('span');
                    copy.className = 'nav-copy';

                    const label = document.createElement('span');
                    label.textContent = config.title;
                    copy.appendChild(label);

                    trigger.append(icon, copy);
                    if (showSubmenu) {
                        const caret = document.createElement('span');
                        caret.className = 'nav-caret';
                        caret.setAttribute('aria-hidden', 'true');
                        caret.innerHTML = `
                            <svg viewBox="0 0 24 24">
                                <path d="M7 10l5 5 5-5" fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="1.8"></path>
                            </svg>
                        `.trim();
                        trigger.appendChild(caret);
                    }
                    listItem.appendChild(trigger);

                    if (showSubmenu) {
                        const submenu = document.createElement('div');
                        submenu.className = 'sidebar-submenu';
                        submenu.setAttribute('aria-label', `${config.title} sections`);

                        const submenuShell = document.createElement('div');
                        submenuShell.className = 'sidebar-submenu-shell';

                        const submenuHeader = document.createElement('div');
                        submenuHeader.className = 'sidebar-submenu-header';

                        const submenuEyebrow = document.createElement('span');
                        submenuEyebrow.className = 'sidebar-submenu-eyebrow';
                        submenuEyebrow.textContent = config.title;

                        const submenuTitle = document.createElement('strong');
                        submenuTitle.textContent = 'Sections';

                        submenuHeader.append(submenuEyebrow, submenuTitle);
                        submenuShell.appendChild(submenuHeader);

                        const submenuList = document.createElement('div');
                        submenuList.className = 'sidebar-submenu-list';

                        moduleItems.forEach((entry) => {
                            const itemButton = document.createElement('button');
                            itemButton.type = 'button';
                            itemButton.className = 'sidebar-submenu-link';
                            itemButton.dataset.module = moduleId;
                            itemButton.dataset.item = entry.id;
                            itemButton.textContent = entry.label;
                            submenuList.appendChild(itemButton);
                        });

                        submenuShell.appendChild(submenuList);
                        submenu.appendChild(submenuShell);
                        listItem.appendChild(submenu);
                    }
                    sidebarNav.appendChild(listItem);
                });

                navItems = Array.from(sidebarNav.querySelectorAll('.sidebar-nav-item'));
                navLinks = Array.from(sidebarNav.querySelectorAll('.nav-link-trigger'));
                navSubmenuLinks = Array.from(sidebarNav.querySelectorAll('.sidebar-submenu-link'));
            }

            const settingsTabButtons = Array.from(document.querySelectorAll('.settings-tab-button'));
            const settingsTabPanels = Array.from(document.querySelectorAll('.settings-tab-panel'));

            function syncSettingsTabState(itemId = activeNavItemId) {
                const resolvedItemId = findModuleItem('settings', itemId)?.id || getModuleItems('settings')[0]?.id;
                settingsTabButtons.forEach((button) => {
                    const isActive = button.dataset.settingsItem === resolvedItemId;
                    button.classList.toggle('active', isActive);
                    button.setAttribute('aria-selected', isActive ? 'true' : 'false');
                    button.setAttribute('tabindex', isActive ? '0' : '-1');
                });
                settingsTabPanels.forEach((panel) => {
                    const isActive = panel.dataset.settingsPanel === resolvedItemId;
                    panel.classList.toggle('active', isActive);
                    panel.classList.toggle('hidden', !isActive);
                    panel.setAttribute('aria-hidden', isActive ? 'false' : 'true');
                });
            }

            function setWorkspaceTextStatus(element, message = '', isError = false) {
                if (!element) return;
                element.textContent = message;
                element.style.color = isError ? 'var(--red)' : 'var(--text-secondary)';
            }

            const NEW_ORGANIZATION_ID_MAX_LENGTH = 16;
            const NEW_ORGANIZATION_ID_PATTERN = /^[a-z0-9]{1,16}$/;

            function slugifyIdentifier(value) {
                return String(value || '')
                    .trim()
                    .toLowerCase()
                    .replace(/[^a-z0-9]+/g, '-')
                    .replace(/^-+|-+$/g, '')
                    .slice(0, 64);
            }

            function extractOrganizationUrlValue(value) {
                let raw = String(value || '').trim();
                if (!raw) {
                    return '';
                }
                raw = raw.replace(/^https?:\/\/[^/]+\//i, '');
                raw = raw.replace(/^\/+/, '');
                return raw.split(/[/?#]/, 1)[0] || '';
            }

            function normalizeOrganizationUrl(value) {
                const raw = extractOrganizationUrlValue(value);
                if (!raw) {
                    return '';
                }
                return slugifyIdentifier(raw);
            }

            function normalizeNewOrganizationId(value) {
                return extractOrganizationUrlValue(value)
                    .toLowerCase()
                    .replace(/[^a-z0-9]/g, '')
                    .slice(0, NEW_ORGANIZATION_ID_MAX_LENGTH);
            }

            function isValidNewOrganizationId(value) {
                return NEW_ORGANIZATION_ID_PATTERN.test(String(value || '').trim());
            }

            function humanizeIdentifier(value) {
                const normalized = slugifyIdentifier(value);
                if (!normalized) {
                    return '';
                }
                return normalized
                    .split('-')
                    .filter(Boolean)
                    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
                    .join(' ');
            }

            function setWorkspaceUrlPrefixes() {
                const prefix = `${window.location.origin.replace(/\/+$/, '')}/`;
                [workspaceSelectionUrlPrefix, workspaceOnboardingUrlPrefix].forEach((element) => {
                    if (element) {
                        element.textContent = prefix;
                    }
                });
            }

            function populateWorkspaceOrgSuggestions(items) {
                if (!workspaceOrgSuggestions) return;
                workspaceOrgSuggestions.innerHTML = '';
                (items || []).forEach((item) => {
                    const option = document.createElement('option');
                    option.value = item.organization_id;
                    option.label = item.name || item.organization_id;
                    workspaceOrgSuggestions.appendChild(option);
                });
            }

            function getAccessibleTenantItems() {
                return Array.isArray(authSessionState?.accessible_organizations) ? authSessionState.accessible_organizations : [];
            }

            function setWorkspaceSelectionSwitchAccountVisible(visible = false, organizationId = '') {
                if (!workspaceSelectionSwitchAccountBtn) {
                    return;
                }
                workspaceSelectionSwitchAccountBtn.classList.toggle('hidden', !visible);
                workspaceSelectionSwitchAccountBtn.dataset.organizationId = visible ? normalizeOrganizationUrl(organizationId) : '';
            }

            function refreshWorkspaceOrganizationChooser() {
                if (!workspaceOrgChooserGroup || !workspaceModalOrgSelect) {
                    return;
                }
                const items = getAccessibleTenantItems();
                const showChooser = Boolean(accessToken && items.length > 1);
                workspaceOrgChooserGroup.classList.toggle('hidden', !showChooser);
                workspaceOrgChooserGroup.setAttribute('aria-hidden', showChooser ? 'false' : 'true');
                if (!showChooser) {
                    return;
                }
                const normalizedInputTenantId = normalizeOrganizationUrl(workspaceOrgUrlInput?.value || '');
                const normalizedSelectedTenantId = normalizeOrganizationUrl(workspaceModalOrgSelect.value || authSessionState?.organization_id || '');
                const resolvedTenantId = normalizedInputTenantId || normalizedSelectedTenantId || '';
                if (resolvedTenantId && items.some((item) => item.organization_id === resolvedTenantId)) {
                    workspaceModalOrgSelect.value = resolvedTenantId;
                }
            }

            function syncWorkspaceSelectionOrgInput(tenantId = '') {
                const normalizedTenantId = normalizeOrganizationUrl(tenantId);
                if (workspaceOrgUrlInput) {
                    workspaceOrgUrlInput.value = normalizedTenantId || '';
                }
                if (workspaceModalOrgSelect) {
                    const hasMatchingOption = Array.from(workspaceModalOrgSelect.options || []).some((option) => option.value === normalizedTenantId);
                    workspaceModalOrgSelect.value = hasMatchingOption ? normalizedTenantId : '';
                }
                refreshWorkspaceOrganizationChooser();
            }

            function syncWorkspaceSelectedOrgContext() {
                const organizationId = workspaceModalOrgSelect.value || authSessionState?.organization_id || '';
                const orgUrl = organizationId ? `${window.location.origin.replace(/\/+$/, '')}/${organizationId}` : '';
                if (workspaceSelectionCurrentOrg) {
                    workspaceSelectionCurrentOrg.textContent = orgUrl ? `Organization URL: ${orgUrl}` : '';
                    workspaceSelectionCurrentOrg.classList.toggle('hidden', !orgUrl);
                }
                if (workspaceCreateProjectCurrentOrg) {
                    workspaceCreateProjectCurrentOrg.textContent = orgUrl ? `Organization URL: ${orgUrl}` : '';
                    workspaceCreateProjectCurrentOrg.classList.toggle('hidden', !orgUrl);
                }
            }

            function findAccessibleTenant(rawValue) {
                const slug = normalizeOrganizationUrl(rawValue);
                if (!slug) {
                    return null;
                }
                const items = getAccessibleTenantItems();
                return items.find((item) => item.organization_id === slug || slugifyIdentifier(item.name) === slug) || null;
            }

            async function inspectOrganizationSpaceAccess(organizationId) {
                const normalizedOrganizationId = normalizeOrganizationUrl(organizationId);
                if (!normalizedOrganizationId || !accessToken) {
                    return null;
                }
                const response = await fetch(
                    `${getApiBaseUrl(null, { forceGlobal: true })}/auth/organization-space/${encodeURIComponent(normalizedOrganizationId)}`,
                    {
                        headers: {
                            Authorization: `Bearer ${accessToken}`,
                        },
                    },
                );
                const payload = await response.json().catch(() => ({}));
                if (!response.ok) {
                    if (response.status === 401) {
                        clearBearerSession({ openLoginGateway: true });
                    }
                    throw new Error(payload.detail || 'Failed to inspect organization access.');
                }
                return payload;
            }

            function disableGoogleAutoSelect() {
                try {
                    if (window.google && window.google.accounts && window.google.accounts.id) {
                        window.google.accounts.id.disableAutoSelect();
                    }
                } catch (error) {
                    console.warn('Unable to disable Google auto select:', error);
                }
            }

            function isGoogleProvider() {
                return Boolean(oidcConfig && oidcConfig.provider === 'google' && oidcConfig.client_id);
            }

            function isGoogleLoginConfigured() {
                return isGoogleProvider();
            }

            function ensureGoogleLoginButtonContainer(button, container, id) {
                if (!button || !container || container.parentElement) {
                    return container;
                }
                container.id = id;
                container.classList.add('hidden');
                container.style.alignItems = 'center';
                container.style.minHeight = '40px';
                button.insertAdjacentElement('afterend', container);
                return container;
            }

            function loadGoogleIdentityScript() {
                if (!isGoogleProvider()) {
                    return Promise.resolve(null);
                }
                if (window.google && window.google.accounts && window.google.accounts.id) {
                    return Promise.resolve(window.google);
                }
                if (googleIdentityScriptPromise) {
                    return googleIdentityScriptPromise;
                }
                googleIdentityScriptPromise = new Promise((resolve, reject) => {
                    const existingScript = document.querySelector('script[data-kairyx-google-identity="true"]');
                    if (existingScript) {
                        existingScript.addEventListener('load', () => resolve(window.google), { once: true });
                        existingScript.addEventListener('error', () => {
                            googleIdentityScriptPromise = null;
                            reject(new Error('Unable to load Google Sign-In.'));
                        }, { once: true });
                        return;
                    }
                    const script = document.createElement('script');
                    script.src = GOOGLE_IDENTITY_SCRIPT_SRC;
                    script.async = true;
                    script.defer = true;
                    script.setAttribute('data-kairyx-google-identity', 'true');
                    script.onload = () => resolve(window.google);
                    script.onerror = () => {
                        googleIdentityScriptPromise = null;
                        reject(new Error('Unable to load Google Sign-In.'));
                    };
                    document.head.appendChild(script);
                });
                return googleIdentityScriptPromise;
            }

            async function handleGoogleCredentialResponse(response) {
                const credential = response && typeof response.credential === 'string' ? response.credential : '';
                if (!credential) {
                    const message = 'Google login did not return an ID token.';
                    setAuthStatus(message);
                    setWorkspaceTextStatus(workspaceLoginStatus, message, true);
                    return;
                }
                persistAccessToken(credential);
                try {
                    await hydrateAuthSession();
                    if (activePageId) {
                        activatePage(activePageId);
                    }
                } catch (error) {
                    handleGoogleSessionFailure(error);
                }
            }

            async function ensureGoogleIdentityButtons() {
                if (!isGoogleProvider()) {
                    return;
                }
                const containers = [
                    ensureGoogleLoginButtonContainer(oidcLoginBtn, googleLoginContainer, 'google-login-container'),
                    ensureGoogleLoginButtonContainer(workspaceGoogleLoginBtn, workspaceGoogleLoginContainer, 'workspace-google-login-container'),
                ].filter(Boolean);
                if (containers.length === 0) {
                    return;
                }
                await loadGoogleIdentityScript();
                if (!window.google || !window.google.accounts || !window.google.accounts.id) {
                    throw new Error('Google Sign-In is unavailable in this browser.');
                }
                if (googleIdentityButtonClientId !== oidcConfig.client_id) {
                    window.google.accounts.id.initialize({
                        client_id: oidcConfig.client_id,
                        callback: (response) => {
                            handleGoogleCredentialResponse(response).catch((error) => {
                                const message = error.message || 'Google sign-in failed.';
                                setAuthStatus(message);
                                setWorkspaceTextStatus(workspaceLoginStatus, message, true);
                            });
                        },
                    });
                    googleIdentityButtonClientId = oidcConfig.client_id;
                }
                containers.forEach((container, index) => {
                    if (!container) return;
                    container.innerHTML = '';
                    window.google.accounts.id.renderButton(container, {
                        theme: 'outline',
                        size: index === 0 ? 'large' : 'large',
                        text: 'signin_with',
                        shape: 'pill',
                    });
                });
            }

            function ensureVisibleGoogleIdentityButtons() {
                if (!isGoogleLoginConfigured() || accessToken || !isGoogleProvider()) {
                    return;
                }
                [googleLoginContainer, workspaceGoogleLoginContainer].forEach((container) => {
                    if (!container) {
                        return;
                    }
                    container.classList.remove('hidden');
                });
                window.requestAnimationFrame(() => {
                    ensureGoogleIdentityButtons().catch((error) => {
                        const message = error.message || 'Google Sign-In is unavailable.';
                        setAuthStatus(message);
                        setWorkspaceTextStatus(workspaceLoginStatus, message, true);
                    });
                });
            }

            function isAuthenticatedWorkspaceReady() {
                return Boolean(
                    accessToken
                    && authSessionState
                    && authSessionState.organization_id
                    && authSessionState.project_id
                    && !authSessionState.needs_onboarding
                    && !authSessionState.needs_org_selection
                    && !authSessionState.needs_project_selection
                );
            }

            function shouldBlockProtectedAppData() {
                if (accessToken) {
                    return !isAuthenticatedWorkspaceReady();
                }
                return isGoogleLoginConfigured() && !isAuthenticatedWorkspaceReady();
            }

            function getWorkspaceResolutionMessage(payload = authSessionState) {
                const source = payload || {};
                if (source.needs_onboarding) {
                    return 'Create or join an organization before using the app.';
                }
                if (source.needs_org_selection) {
                    return 'Choose an organization before using the app.';
                }
                if (source.needs_project_selection) {
                    return 'Choose a project before using the app.';
                }
                return 'Finish workspace setup before using this part of the app.';
            }

            function canSelfServeOrganizationCreation() {
                if (!accessToken) {
                    return false;
                }
                if (backendMode === 'mock') {
                    return true;
                }
                if (authSessionState?.platform_admin) {
                    return true;
                }
                const orgCount = Array.isArray(authSessionState?.accessible_organizations)
                    ? authSessionState.accessible_organizations.length
                    : 0;
                return orgCount === 0;
            }

            function primeOnboardingForNewOrganization(organizationId) {
                const normalizedOrganizationId = normalizeNewOrganizationId(organizationId);
                onboardingFromWorkspaceSelection = true;
                setWorkspaceSelectionSwitchAccountVisible(false);
                workspaceOrgUrlInput.value = normalizedOrganizationId;
                onboardingOrganizationNameInput.value = normalizedOrganizationId;
                onboardingOrganizationIdInput.value = normalizedOrganizationId;
                if (!onboardingProjectNameInput.value.trim()) {
                    onboardingProjectNameInput.value = 'Main Project';
                    onboardingProjectIdInput.value = slugifyIdentifier(onboardingProjectNameInput.value);
                }
                onboardingResult = null;
                setOnboardingStep(1);
                openWorkspaceOverlay('onboarding');
                setWorkspaceTextStatus(
                    onboardingStatus,
                    `"${normalizedOrganizationId}" does not exist yet. Confirm this URL to create a new organization, or go back to enter an existing one.`,
                );
            }

            function isWorkspaceContextResponse(status, detail = '') {
                if (![403, 409].includes(Number(status))) {
                    return false;
                }
                const message = String(detail || '').trim().toLowerCase();
                if (!message) {
                    return false;
                }
                return message.includes('no organization space membership is active for this user')
                    || message.includes('tenant membership for')
                    || message.includes('project membership for')
                    || message.includes('project membership is missing or inactive')
                    || message.includes('organization space selection is required')
                    || message.includes('project selection is required')
                    || message.includes('organization space in the path does not match');
            }

            function buildWorkspaceContextError(payload = authSessionState, status = 409) {
                const error = new Error(getWorkspaceResolutionMessage(payload));
                error.status = status;
                error.payload = payload || {};
                error.workspaceContextRequired = true;
                return error;
            }

            function isWorkspaceContextError(error) {
                if (!error) {
                    return false;
                }
                if (error.workspaceContextRequired) {
                    return true;
                }
                return isWorkspaceContextResponse(
                    error.status,
                    error?.payload?.detail || error?.message || '',
                );
            }

            function refreshWorkspaceLoginStatus(organizationId = '') {
                const invitePending = Boolean(readPendingInvite());
                const resolvedOrganizationId = normalizeOrganizationUrl(organizationId);
                setWorkspaceTextStatus(
                    workspaceLoginStatus,
                    invitePending
                        ? 'Project invite detected. Continue with Google to redeem it.'
                        : (
                            resolvedOrganizationId
                                ? `Continue with Google to open "${resolvedOrganizationId}".`
                                : 'Continue with Google to open your organization.'
                        ),
                );
            }

            function readStoredActorContext() {
                try {
                    return {
                        tenantId: localStorage.getItem(TENANT_ID_STORAGE_KEY) || LOCAL_DEMO_TENANT_ID,
                        projectId: localStorage.getItem(PROJECT_ID_STORAGE_KEY) || LOCAL_DEMO_PROJECT_ID,
                        apiKey: localStorage.getItem(API_KEY_STORAGE_KEY) || '',
                    };
                } catch (error) {
                    return {
                        tenantId: LOCAL_DEMO_TENANT_ID,
                        projectId: LOCAL_DEMO_PROJECT_ID,
                        apiKey: '',
                    };
                }
            }

            function persistActorContext() {
                try {
                    localStorage.setItem(TENANT_ID_STORAGE_KEY, getActiveTenantId() || LOCAL_DEMO_TENANT_ID);
                    localStorage.setItem(PROJECT_ID_STORAGE_KEY, getActiveProjectId() || LOCAL_DEMO_PROJECT_ID);
                    localStorage.setItem(API_KEY_STORAGE_KEY, apiKeyInput.value || '');
                } catch (error) {
                    console.warn('Unable to persist actor context:', error);
                }
            }

            function persistWorkspaceSelection(tenantId, projectId) {
                try {
                    if (tenantId !== undefined) {
                        if (tenantId) {
                            localStorage.setItem(TENANT_ID_STORAGE_KEY, tenantId);
                        } else {
                            localStorage.removeItem(TENANT_ID_STORAGE_KEY);
                        }
                    }
                    if (projectId !== undefined) {
                        if (projectId) {
                            localStorage.setItem(PROJECT_ID_STORAGE_KEY, projectId);
                        } else {
                            localStorage.removeItem(PROJECT_ID_STORAGE_KEY);
                        }
                    }
                } catch (error) {
                    console.warn('Unable to persist workspace selection:', error);
                }
            }

            function readPendingInvite() {
                try {
                    const raw = localStorage.getItem(PENDING_INVITE_STORAGE_KEY);
                    return raw ? JSON.parse(raw) : null;
                } catch (error) {
                    return null;
                }
            }

            function persistPendingInvite(payload) {
                try {
                    if (!payload || !payload.inviteCode) {
                        localStorage.removeItem(PENDING_INVITE_STORAGE_KEY);
                        return;
                    }
                    localStorage.setItem(PENDING_INVITE_STORAGE_KEY, JSON.stringify(payload));
                } catch (error) {
                    console.warn('Unable to persist pending invite:', error);
                }
            }

            function clearPendingInvite() {
                persistPendingInvite(null);
            }

            function getStoredWorkspaceSelection() {
                try {
                    return {
                        tenantId: localStorage.getItem(TENANT_ID_STORAGE_KEY) || '',
                        projectId: localStorage.getItem(PROJECT_ID_STORAGE_KEY) || '',
                    };
                } catch (error) {
                    return {
                        tenantId: '',
                        projectId: '',
                    };
                }
            }

            function getWorkspaceOrganizationHint() {
                return normalizeOrganizationUrl(
                    getStoredWorkspaceSelection().tenantId || getOrganizationIdFromPathname()
                );
            }

            function isGatewayRootPath(pathname = window.location.pathname) {
                return !getOrganizationIdFromPathname(pathname);
            }

            function buildFallbackWorkspaceSession(errorMessage = '') {
                const organizationId = getWorkspaceOrganizationHint();
                return {
                    organization_id: organizationId || null,
                    project_id: null,
                    organization: organizationId ? { organization_id: organizationId, name: organizationId } : null,
                    project: null,
                    accessible_organizations: [],
                    accessible_projects: [],
                    needs_onboarding: !organizationId,
                    needs_org_selection: Boolean(organizationId),
                    needs_project_selection: false,
                    workspace_resolution_error: String(errorMessage || '').trim() || null,
                };
            }

            function buildPathScopedWorkspaceSession(payload = {}, requestedOrganizationId = '', errorMessage = '') {
                const organizationId = normalizeOrganizationUrl(requestedOrganizationId);
                const activeOrganizationId = normalizeOrganizationUrl(payload?.organization_id || '');
                if (!organizationId || !activeOrganizationId || activeOrganizationId === organizationId) {
                    return payload;
                }
                const accessibleOrganizations = Array.isArray(payload?.accessible_organizations) ? payload.accessible_organizations : [];
                const matchedOrganization = accessibleOrganizations.find((item) => {
                    return normalizeOrganizationUrl(item?.organization_id || '') === organizationId;
                }) || null;
                return {
                    ...payload,
                    organization_id: organizationId,
                    project_id: null,
                    organization_role: matchedOrganization?.role || null,
                    project_role: null,
                    organization: matchedOrganization
                        ? {
                            organization_id: matchedOrganization.organization_id,
                            name: matchedOrganization.name,
                            status: matchedOrganization.status,
                            role: matchedOrganization.role,
                        }
                        : { organization_id: organizationId, name: organizationId },
                    project: null,
                    accessible_projects: [],
                    needs_onboarding: false,
                    needs_org_selection: true,
                    needs_project_selection: false,
                    workspace_resolution_error: String(errorMessage || payload?.workspace_resolution_error || '').trim() || null,
                };
            }

            function getActiveTenantId() {
                const selectedTenantId = String(orgSpaceSelect.value || '').trim();
                const pathTenantId = getOrganizationIdFromPathname();
                if (accessToken) {
                    return selectedTenantId || pathTenantId || getStoredWorkspaceSelection().tenantId;
                }
                if (isGoogleLoginConfigured()) {
                    return pathTenantId || getStoredWorkspaceSelection().tenantId || '';
                }
                return LOCAL_DEMO_TENANT_ID;
            }

            function getActiveProjectId() {
                const selectedProjectId = String(projectSelect.value || '').trim();
                const pathTenantId = getOrganizationIdFromPathname();
                const storedSelection = getStoredWorkspaceSelection();
                const storedProjectId = pathTenantId
                    && normalizeOrganizationUrl(storedSelection.tenantId)
                    && normalizeOrganizationUrl(storedSelection.tenantId) !== pathTenantId
                    ? ''
                    : storedSelection.projectId || '';
                if (accessToken) {
                    return selectedProjectId || storedProjectId || '';
                }
                if (isGoogleLoginConfigured()) {
                    return storedProjectId || '';
                }
                return LOCAL_DEMO_PROJECT_ID;
            }

            function isWorkspaceSelectionRequired() {
                return Boolean(
                    accessToken
                    && authSessionState
                    && (authSessionState.needs_onboarding || authSessionState.needs_org_selection || authSessionState.needs_project_selection)
                );
            }

            function syncWorkspaceSummary(payload = authSessionState) {
                if (!workspaceSummaryText || !workspaceRoleSummary) return;
                if (!accessToken) {
                    if (isGoogleLoginConfigured()) {
                        workspaceSummaryText.textContent = 'Google login required';
                        workspaceRoleSummary.textContent = 'Sign in to access organizations and projects';
                        if (settingsWorkspaceSummary) {
                            settingsWorkspaceSummary.textContent = 'Google login required';
                        }
                        syncSidebarSessionUi(payload);
                        return;
                    }
                    const localSummary = `Local demo / ${getActiveTenantId()} / ${getActiveProjectId()}`;
                    workspaceSummaryText.textContent = localSummary;
                    workspaceRoleSummary.textContent = 'Local demo session';
                    if (settingsWorkspaceSummary) {
                        settingsWorkspaceSummary.textContent = localSummary;
                    }
                    syncSidebarSessionUi(payload);
                    return;
                }
                const tenantItems = Array.isArray(payload?.accessible_organizations) ? payload.accessible_organizations : [];
                const projectItems = Array.isArray(payload?.accessible_projects) ? payload.accessible_projects : [];
                const selectedTenantId = payload?.organization_id || getActiveTenantId();
                const selectedProjectId = payload?.project_id || getActiveProjectId();
                const tenant = tenantItems.find((item) => item.organization_id === selectedTenantId);
                const project = projectItems.find((item) => item.project_id === selectedProjectId);
                const tenantLabel = tenant?.name || selectedTenantId || 'Select organization space';
                const projectLabel = project?.name || selectedProjectId || 'Select project';
                const summaryText = `${tenantLabel} / ${projectLabel}`;
                workspaceSummaryText.textContent = summaryText;
                if (settingsWorkspaceSummary) {
                    settingsWorkspaceSummary.textContent = summaryText;
                }
                const roleBits = [];
                roleBits.push(payload?.display_name || payload?.email || 'Authenticated user');
                if (payload?.organization_role) {
                    roleBits.push(`org: ${payload.organization_role}`);
                }
                if (payload?.project_role) {
                    roleBits.push(`project: ${payload.project_role}`);
                }
                workspaceRoleSummary.textContent = roleBits.join(' | ');
                syncSidebarSessionUi(payload);
            }

            function buildSessionInitials(value) {
                const normalized = String(value || '').trim();
                if (!normalized) {
                    return 'K';
                }
                const tokens = normalized.split(/\s+/).filter(Boolean);
                if (tokens.length >= 2) {
                    return `${tokens[0].charAt(0)}${tokens[tokens.length - 1].charAt(0)}`.toUpperCase();
                }
                const compact = normalized.replace(/[^a-z0-9]/gi, '');
                return (compact.slice(0, 2) || 'K').toUpperCase();
            }

            function getSidebarSessionPresentation(payload = authSessionState) {
                const avatarUrl = String(
                    payload?.picture
                    || payload?.avatar_url
                    || payload?.image_url
                    || payload?.photo_url
                    || ''
                ).trim();
                if (accessToken) {
                    const name = String(payload?.display_name || '').trim()
                        || String(payload?.email || '').trim().split('@')[0]
                        || 'Authenticated User';
                    const meta = String(payload?.email || '').trim()
                        || String(workspaceSummaryText?.textContent || '').trim()
                        || 'Signed in';
                    return {
                        name,
                        meta,
                        avatarUrl,
                        initials: buildSessionInitials(name),
                        canLogout: true,
                    };
                }
                if (isGoogleLoginConfigured()) {
                    return {
                        name: 'Signed out',
                        meta: 'Enter organization URL to continue',
                        avatarUrl: '',
                        initials: 'K',
                        canLogout: false,
                    };
                }
                return {
                    name: 'KairyxAI Operator',
                    meta: 'Local demo session',
                    avatarUrl: '',
                    initials: 'K',
                    canLogout: false,
                };
            }

            function setSidebarSessionMenuOpen(isOpen) {
                if (!sidebarSessionButton || !sidebarSessionMenu) {
                    return;
                }
                const canOpen = Boolean(isOpen && !sidebarSessionButton.disabled);
                sidebarSessionButton.setAttribute('aria-expanded', canOpen ? 'true' : 'false');
                sidebarSessionMenu.classList.toggle('hidden', !canOpen);
                sidebarSessionMenu.setAttribute('aria-hidden', canOpen ? 'false' : 'true');
            }

            function syncSidebarSessionUi(payload = authSessionState) {
                if (!sidebarSessionButton || !sidebarSessionName || !sidebarSessionMeta || !sidebarSessionAvatarFallback) {
                    return;
                }
                const presentation = getSidebarSessionPresentation(payload);
                sidebarSessionName.textContent = presentation.name;
                sidebarSessionMeta.textContent = presentation.meta;
                sidebarSessionAvatarFallback.textContent = presentation.initials;
                sidebarSessionButton.disabled = !presentation.canLogout;
                sidebarSessionButton.classList.toggle('is-disabled', !presentation.canLogout);
                if (sidebarSessionLogoutBtn) {
                    sidebarSessionLogoutBtn.disabled = !presentation.canLogout;
                }
                if (sidebarSessionAvatarImage) {
                    if (presentation.avatarUrl) {
                        sidebarSessionAvatarImage.src = presentation.avatarUrl;
                        sidebarSessionAvatarImage.classList.remove('hidden');
                        sidebarSessionAvatarImage.alt = `${presentation.name} profile image`;
                        sidebarSessionAvatar?.classList.add('has-image');
                    } else {
                        sidebarSessionAvatarImage.removeAttribute('src');
                        sidebarSessionAvatarImage.classList.add('hidden');
                        sidebarSessionAvatarImage.alt = '';
                        sidebarSessionAvatar?.classList.remove('has-image');
                    }
                }
                if (!presentation.canLogout) {
                    setSidebarSessionMenuOpen(false);
                }
            }

            function syncAuthModeUi() {
                const usingGoogleLogin = isGoogleLoginConfigured();
                const usingOidc = Boolean(accessToken);
                if (legacyAuthControls) {
                    legacyAuthControls.classList.toggle('hidden', true);
                }
                if (legacyApiKeyGroup) {
                    legacyApiKeyGroup.classList.toggle('hidden', usingOidc || usingGoogleLogin);
                }
                oidcWorkspaceControls.classList.toggle('hidden', !usingOidc);
                oidcLoginBtn.classList.toggle('hidden', !usingGoogleLogin || usingOidc || isGoogleProvider());
                oidcLogoutBtn.classList.toggle('hidden', !usingOidc);
                workspaceOpenSwitcherBtn.disabled = !usingOidc;
                workspaceCreateProjectBtn.disabled = !usingOidc;
                if (workspaceGoogleLoginBtn) {
                    workspaceGoogleLoginBtn.classList.toggle('hidden', !usingGoogleLogin || usingOidc || isGoogleProvider());
                }
                [googleLoginContainer, workspaceGoogleLoginContainer].forEach((container) => {
                    if (!container) return;
                    container.classList.toggle('hidden', !usingGoogleLogin || usingOidc || !isGoogleProvider());
                });
                if (usingGoogleLogin && !usingOidc && isGoogleProvider()) {
                    ensureGoogleIdentityButtons().catch((error) => {
                        const message = error.message || 'Google Sign-In is unavailable.';
                        setAuthStatus(message);
                        setWorkspaceTextStatus(workspaceLoginStatus, message, true);
                    });
                }
                if (settingsOpenSwitcherBtn) {
                    settingsOpenSwitcherBtn.disabled = !usingOidc;
                }
                if (settingsCreateProjectBtn) {
                    settingsCreateProjectBtn.disabled = !usingOidc;
                }
                syncWorkspaceSummary();
            }

            function populateWorkspaceSelect(select, items, selectedValue, emptyLabel, idKey) {
                if (!select) return;
                const previousValue = selectedValue !== undefined ? selectedValue : select.value;
                select.innerHTML = '';
                const emptyOption = document.createElement('option');
                emptyOption.value = '';
                emptyOption.textContent = emptyLabel;
                select.appendChild(emptyOption);
                (items || []).forEach((item) => {
                    const option = document.createElement('option');
                    option.value = item[idKey];
                    const role = item.role ? ` (${item.role})` : '';
                    option.textContent = `${item.name || item[idKey]}${role}`;
                    select.appendChild(option);
                });
                if (previousValue && (items || []).some((item) => item[idKey] === previousValue)) {
                    select.value = previousValue;
                } else {
                    select.value = '';
                }
            }

            function setWorkspaceOverlayPanel(panel) {
                [workspaceLoginPanel, workspaceSelectionPanel, workspaceOnboardingPanel, workspaceCreateProjectPanel].forEach((entry) => {
                    entry.classList.toggle('hidden', entry !== panel);
                });
            }

            function prefillOnboardingOrganizationHint() {
                if (!onboardingOrganizationNameInput || !onboardingOrganizationIdInput) {
                    return;
                }
                if (String(onboardingOrganizationNameInput.value || '').trim()) {
                    return;
                }
                const organizationId = normalizeNewOrganizationId(getWorkspaceOrganizationHint());
                if (!organizationId) {
                    return;
                }
                onboardingOrganizationNameInput.value = organizationId;
                onboardingOrganizationIdInput.value = organizationId;
            }

            function syncWorkspaceGateClass() {
                const gated = Boolean(workspaceOverlayMode) || isWorkspaceSelectionRequired();
                syncWorkspaceBootClass();
                document.body.classList.toggle('workspace-gated', gated);
            }

            function syncWorkspaceBootClass() {
                document.documentElement.classList.toggle(
                    'workspace-gateway-boot',
                    Boolean(rootGatewayBootPending && !getOrganizationIdFromPathname()),
                );
            }

            function setWorkspaceSelectionStage(stage = 'org') {
                let normalizedStage = stage === 'project' ? 'project' : 'org';
                const pathTenantId = getOrganizationIdFromPathname();
                const selectedTenantId = workspaceModalOrgSelect.value || authSessionState?.organization_id || pathTenantId || '';
                const isStandaloneOrgPath = Boolean(pathTenantId);
                const accessibleOrgCount = getAccessibleTenantItems().length;
                const projectItems = Array.isArray(authSessionState?.accessible_projects) ? authSessionState.accessible_projects : [];
                const hasExistingProjects = Boolean(selectedTenantId && projectItems.length > 0);
                if (normalizedStage === 'project' && !selectedTenantId) {
                    normalizedStage = 'org';
                }
                if (workspaceSelectionOrgStage) {
                    workspaceSelectionOrgStage.classList.toggle('hidden', normalizedStage !== 'org');
                }
                if (workspaceSelectionProjectStage) {
                    workspaceSelectionProjectStage.classList.toggle('hidden', normalizedStage !== 'project');
                }
                workspaceSelectionBackBtn.classList.toggle('hidden', normalizedStage !== 'project');
                workspaceSelectionResolveBtn.classList.toggle('hidden', normalizedStage !== 'org');
                if (workspaceSelectionSwitchAccountBtn) {
                    workspaceSelectionSwitchAccountBtn.classList.toggle('hidden', normalizedStage !== 'org' || !workspaceSelectionSwitchAccountBtn.dataset.organizationId);
                }
                workspaceSelectionContinueBtn.classList.toggle('hidden', normalizedStage !== 'project' || !hasExistingProjects);
                workspaceSelectionCreateProjectBtn.classList.toggle('hidden', normalizedStage !== 'project');
                if (normalizedStage === 'org') {
                    const currentOrgInput = normalizeOrganizationUrl(workspaceOrgUrlInput?.value || '');
                    workspaceModalEyebrow.textContent = isStandaloneOrgPath ? 'Workspace Setup' : 'Workspace';
                    workspaceModalTitle.textContent = isStandaloneOrgPath ? 'Open this organization' : 'Log in to your organization';
                    workspaceModalSubtitle.textContent = accessibleOrgCount > 1 && accessToken
                        ? 'Choose one of your organizations below, or type another organization URL to open.'
                        : (
                            isStandaloneOrgPath
                                ? (
                                    !accessToken && isGoogleLoginConfigured()
                                        ? 'Continue with Google to open this organization path.'
                                        : 'Confirm the organization URL to continue into this organization path.'
                                )
                                : (
                                    !accessToken && isGoogleLoginConfigured()
                                        ? 'Type the organization URL you want to open, then continue with Google.'
                                        : 'Type the organization URL you want to open.'
                                )
                        );
                    workspaceSelectionResolveBtn.textContent = !accessToken && isGoogleLoginConfigured()
                        ? 'Continue with Google'
                        : 'Continue';
                    syncWorkspaceSelectionOrgInput(currentOrgInput || selectedTenantId || '');
                } else {
                    workspaceModalEyebrow.textContent = 'Workspace';
                    workspaceModalTitle.textContent = hasExistingProjects ? 'Choose a project' : 'Create your first project';
                    workspaceModalSubtitle.textContent = hasExistingProjects
                        ? 'Use an existing project or create a new one inside this organization.'
                        : 'This organization does not have a project yet. Create the first one to continue.';
                }
                syncWorkspaceSelectedOrgContext();
                refreshWorkspaceSelectionCopy();
                if (normalizedStage === 'project' && !hasExistingProjects && workspaceCreateProjectNameInput) {
                    window.requestAnimationFrame(() => workspaceCreateProjectNameInput.focus());
                }
            }

            function refreshWorkspaceSelectionCopy() {
                const selectedTenantId = workspaceModalOrgSelect.value || authSessionState?.organization_id || '';
                const projectItems = Array.isArray(authSessionState?.accessible_projects) ? authSessionState.accessible_projects : [];
                const hasExistingProjects = Boolean(selectedTenantId && projectItems.length > 0);
                refreshWorkspaceOrganizationChooser();
                syncWorkspaceSelectedOrgContext();
                if (workspaceExistingProjectGroup) {
                    workspaceExistingProjectGroup.classList.toggle('hidden', !hasExistingProjects);
                }
                if (workspaceSelectionDivider) {
                    workspaceSelectionDivider.classList.toggle('hidden', !hasExistingProjects);
                }
                if (workspaceSelectionCopy) {
                    workspaceSelectionCopy.textContent = hasExistingProjects
                        ? 'This organization already has projects. Use one below, or type a new project name to add another project.'
                        : 'This organization does not have a project yet. Type a project name to create the first one.';
                }
                workspaceSelectionContinueBtn.disabled = !hasExistingProjects || !workspaceModalProjectSelect.value;
                workspaceModalProjectSelect.disabled = !hasExistingProjects;
                workspaceSelectionCreateProjectBtn.disabled = !selectedTenantId;
                workspaceSelectionCreateProjectBtn.textContent = hasExistingProjects ? 'Add New Project' : 'Create First Project';
            }

            function resolveGatewaySelectionStage(payload = authSessionState) {
                const requestedOrganizationId = normalizeOrganizationUrl(
                    workspaceOrgUrlInput?.value
                    || getStoredWorkspaceSelection().tenantId
                    || ''
                );
                const activeOrganizationId = normalizeOrganizationUrl(payload?.organization_id || '');
                if (requestedOrganizationId && activeOrganizationId && requestedOrganizationId === activeOrganizationId) {
                    return 'project';
                }
                return 'org';
            }

            function openWorkspaceOverlay(mode, { allowClose = false, selectionStage = null } = {}) {
                workspaceOverlayMode = mode;
                workspaceOverlay.classList.remove('hidden');
                workspaceOverlay.setAttribute('aria-hidden', 'false');
                workspaceModalCloseBtn.classList.toggle('hidden', !allowClose);
                if (mode === 'login') {
                    const invitePending = Boolean(readPendingInvite());
                    workspaceModalEyebrow.textContent = 'Google Login';
                    workspaceModalTitle.textContent = invitePending ? 'Accept your invite with Google' : 'Continue with Google';
                    workspaceModalSubtitle.textContent = invitePending
                        ? 'Sign in with your Google account to redeem the invite and continue into the right organization.'
                        : 'Sign in with your Google account before opening an existing organization or creating a new one.';
                    setWorkspaceOverlayPanel(workspaceLoginPanel);
                    refreshWorkspaceLoginStatus();
                    ensureVisibleGoogleIdentityButtons();
                } else if (mode === 'onboarding') {
                    workspaceModalEyebrow.textContent = 'Workspace Setup';
                    if (onboardingStep === 1 && onboardingFromWorkspaceSelection) {
                        workspaceModalTitle.textContent = 'Create this organization?';
                        workspaceModalSubtitle.textContent = 'Confirm the organization URL below to create it, or go back to re-enter an existing organization.';
                    } else {
                        workspaceModalTitle.textContent = onboardingStep === 1 ? 'Enter your organization URL' : 'Create your first project';
                        workspaceModalSubtitle.textContent = onboardingStep === 1
                            ? 'Type the organization URL you want to use. The first project comes next.'
                            : 'Enter the project name you want to create inside this organization space.';
                    }
                    setWorkspaceOverlayPanel(workspaceOnboardingPanel);
                } else if (mode === 'create-project') {
                    workspaceModalEyebrow.textContent = 'Project';
                    workspaceModalTitle.textContent = 'Create a new project';
                    workspaceModalSubtitle.textContent = 'Create a new project inside the selected organization space.';
                    setWorkspaceOverlayPanel(workspaceCreateProjectPanel);
                    syncWorkspaceSelectedOrgContext();
                } else {
                    setWorkspaceOverlayPanel(workspaceSelectionPanel);
                    setWorkspaceSelectionStage(
                        selectionStage
                        || (
                            authSessionState?.organization_id && !authSessionState?.needs_org_selection
                                ? 'project'
                                : 'org'
                        )
                    );
                }
                syncWorkspaceGateClass();
            }

            function closeWorkspaceOverlay(force = false) {
                if (!force && isWorkspaceSelectionRequired()) {
                    return;
                }
                workspaceOverlayMode = null;
                workspaceOverlay.classList.add('hidden');
                workspaceOverlay.setAttribute('aria-hidden', 'true');
                syncWorkspaceGateClass();
            }

            function setOnboardingStep(step) {
                onboardingStep = Math.max(1, Math.min(2, Number(step) || 1));
                onboardingStepPanels.forEach((panel) => {
                    panel.classList.toggle('hidden', Number(panel.dataset.step) !== onboardingStep);
                });
                if (workspaceOnboardingStepLabel) {
                    workspaceOnboardingStepLabel.textContent = `Step ${onboardingStep} of 2`;
                }
                onboardingBackBtn.classList.toggle('hidden', onboardingStep === 1 && !onboardingFromWorkspaceSelection);
                onboardingGenerateInviteBtn.classList.add('hidden');
                onboardingConnectBtn.classList.add('hidden');
                onboardingSkipBtn.classList.add('hidden');
                onboardingNextBtn.classList.remove('hidden');
                onboardingNextBtn.textContent = onboardingStep === 1
                    ? (onboardingFromWorkspaceSelection ? 'Create Organization' : 'Continue')
                    : 'Create Project';
                if (onboardingStep === 1) {
                    prefillOnboardingOrganizationHint();
                }
                if (workspaceOverlayMode === 'onboarding') {
                    openWorkspaceOverlay('onboarding');
                }
            }

            function applyAuthSessionPayload(payload) {
                authSessionState = payload || null;
                const selectedTenantId = getOrganizationIdFromPathname() || payload?.organization_id || getStoredWorkspaceSelection().tenantId || '';
                const selectedProjectId = payload?.project_id || getStoredWorkspaceSelection().projectId || '';
                populateWorkspaceSelect(orgSpaceSelect, payload?.accessible_organizations || [], selectedTenantId, 'Select an organization space', 'organization_id');
                populateWorkspaceSelect(workspaceModalOrgSelect, payload?.accessible_organizations || [], selectedTenantId, 'Select an organization space', 'organization_id');
                populateWorkspaceSelect(projectSelect, payload?.accessible_projects || [], selectedProjectId, 'Select a project', 'project_id');
                populateWorkspaceSelect(workspaceModalProjectSelect, payload?.accessible_projects || [], selectedProjectId, 'Select an existing project', 'project_id');
                populateWorkspaceOrgSuggestions(payload?.accessible_organizations || []);
                if (payload?.organization_id || payload?.project_id) {
                    persistWorkspaceSelection(payload.organization_id || '', payload.project_id || '');
                }
                setWorkspaceSelectionSwitchAccountVisible(false);
                syncWorkspaceSelectionOrgInput(selectedTenantId);
                syncWorkspaceSelectedOrgContext();
                refreshWorkspaceSelectionCopy();
                syncAuthModeUi();
                syncWorkspaceSummary(payload);
            }

            const storedActorContext = readStoredActorContext();
            apiKeyInput.value = storedActorContext.apiKey;
            try {
                accessToken = localStorage.getItem(ACCESS_TOKEN_STORAGE_KEY) || '';
            } catch (error) {
                accessToken = '';
            }
            setWorkspaceUrlPrefixes();
            setOnboardingStep(1);
            syncWorkspaceBootClass();
            syncAuthModeUi();
            if (authStatusText) {
                authStatusText.textContent = accessToken
                    ? 'Validating Google session...'
                    : (isGoogleLoginConfigured() ? 'Google login required.' : 'Local demo session');
            }

            apiKeyInput.addEventListener('change', persistActorContext);
            window.addEventListener('storage', handleExternalDataVersionEvent);
            window.addEventListener('focus', syncExternalDataVersions);
            document.addEventListener('visibilitychange', () => {
                if (document.visibilityState === 'visible') {
                    syncExternalDataVersions();
                }
            });
            onboardingOrganizationNameInput.addEventListener('input', () => {
                onboardingOrganizationNameInput.value = normalizeNewOrganizationId(onboardingOrganizationNameInput.value);
                onboardingOrganizationIdInput.value = onboardingOrganizationNameInput.value;
            });
            onboardingOrganizationNameInput.addEventListener('blur', () => {
                onboardingOrganizationNameInput.value = normalizeNewOrganizationId(onboardingOrganizationNameInput.value);
                onboardingOrganizationIdInput.value = onboardingOrganizationNameInput.value;
            });
            onboardingProjectNameInput.addEventListener('input', () => {
                onboardingProjectIdInput.value = slugifyIdentifier(onboardingProjectNameInput.value);
            });
            workspaceOrgUrlInput.addEventListener('input', () => {
                setWorkspaceSelectionSwitchAccountVisible(false);
                if (workspaceModalOrgSelect) {
                    const normalizedTenantId = normalizeOrganizationUrl(workspaceOrgUrlInput.value);
                    const hasMatchingOption = Array.from(workspaceModalOrgSelect.options || []).some((option) => option.value === normalizedTenantId);
                    workspaceModalOrgSelect.value = hasMatchingOption ? normalizedTenantId : '';
                }
                setWorkspaceTextStatus(workspaceSelectionStatus, '');
            });
            workspaceOrgUrlInput.addEventListener('blur', () => {
                syncWorkspaceSelectionOrgInput(workspaceOrgUrlInput.value);
            });
            workspaceOrgUrlInput.addEventListener('keydown', (event) => {
                if (event.key === 'Enter') {
                    event.preventDefault();
                    workspaceSelectionResolveBtn.click();
                }
            });
            workspaceCreateProjectNameInput.addEventListener('input', () => {
                workspaceCreateProjectIdInput.value = slugifyIdentifier(workspaceCreateProjectNameInput.value);
            });
            workspaceCreateProjectInlineNameInput.addEventListener('input', () => {
                workspaceCreateProjectInlineIdInput.value = slugifyIdentifier(workspaceCreateProjectInlineNameInput.value);
            });

            function clearPageIntervals() {
                if (importListInterval) {
                    clearInterval(importListInterval);
                    importListInterval = null;
                }
            }

            function setSidebarMobileOpen(isOpen) {
                document.body.classList.toggle('sidebar-mobile-open', Boolean(isOpen));
            }

            function loadPageData(pageId) {
                if (shouldBlockProtectedAppData()) {
                    return;
                }
                if (pageId === 'operator-hub') {
                    loadReadyImportsForOperatorHub();
                }
                if (pageId === 'player-cohorts') {
                    loadConfiguredSources();
                    loadImportedDataList();
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

            function syncSidebarNavState(moduleId = activeModuleId, itemId = activeNavItemId) {
                navLinks.forEach((link) => {
                    const isActive = link.dataset.module === moduleId;
                    const isExpanded = link.dataset.module === expandedSidebarModuleId && hasSidebarSubmenu(link.dataset.module);
                    link.classList.toggle('active', isActive);
                    link.classList.toggle('expanded', isExpanded);
                    link.setAttribute('aria-expanded', isExpanded ? 'true' : 'false');
                });
                Array.from(sidebarNav?.querySelectorAll('.sidebar-nav-item') || []).forEach((entry) => {
                    const isActive = entry.dataset.module === moduleId;
                    const isExpanded = entry.dataset.module === expandedSidebarModuleId && hasSidebarSubmenu(entry.dataset.module);
                    entry.classList.toggle('active', isActive);
                    entry.classList.toggle('expanded', isExpanded);
                });
                navSubmenuLinks.forEach((button) => {
                    button.classList.toggle('active', button.dataset.item === itemId);
                });
            }

            function scrollToModuleItem(item, behavior = 'smooth') {
                const resolvedBehavior = behavior === 'instant' ? 'auto' : behavior;
                if (!item?.targetId || item?.pageId === 'settings') {
                    contentScroll?.scrollTo({ top: 0, behavior: resolvedBehavior });
                    return;
                }
                window.requestAnimationFrame(() => {
                    const target = document.getElementById(item.targetId);
                    if (target) {
                        target.scrollIntoView({ block: 'start', behavior: resolvedBehavior });
                    }
                });
            }

            function activatePage(pageId, { reload = true } = {}) {
                const pageChanged = activePageId !== pageId;
                if (pageChanged) {
                    clearPageIntervals();
                }
                activePageId = pageId;
                pages.forEach((page) => page.classList.remove('active'));
                const page = document.getElementById(pageId);
                if (page) {
                    page.classList.add('active');
                    if (reload || pageChanged) {
                        loadPageData(pageId);
                    }
                }
            }

            function renderModuleHeader(moduleId) {
                const config = moduleConfigs[moduleId];
                if (moduleTitle) {
                    moduleTitle.textContent = config.title;
                }
                if (moduleSubtitle) {
                    moduleSubtitle.textContent = config.subtitle;
                }
            }

            function activateModule(moduleId, preferredItemOrPageId = null, { scrollBehavior = 'smooth', closeSidebar = true, reloadPage = true } = {}) {
                const config = moduleConfigs[moduleId];
                if (!config) return;
                const item = findModuleItem(moduleId, preferredItemOrPageId) || getModuleItems(moduleId)[0];
                if (!item) return;
                clearExpandedSidebarSubmenuSuppression();
                activeModuleId = moduleId;
                activeNavItemId = item.id;
                expandedSidebarModuleId = hasSidebarSubmenu(moduleId) ? moduleId : null;
                renderModuleHeader(moduleId);
                syncSidebarNavState(moduleId, item.id);
                if (moduleId === 'settings') {
                    syncSettingsTabState(item.id);
                }
                activatePage(item.pageId, { reload: reloadPage });
                scrollToModuleItem(item, scrollBehavior);
                if (closeSidebar) {
                    setSidebarMobileOpen(false);
                }
            }

            renderSidebarNav();

            navItems.forEach((entry) => {
                entry.addEventListener('pointerleave', () => {
                    clearCollapsedSidebarSubmenuSuppression(entry.dataset.module);
                    clearExpandedSidebarSubmenuSuppression(entry.dataset.module);
                });
                entry.addEventListener('focusout', (event) => {
                    if (!entry.contains(event.relatedTarget) && !entry.matches(':hover')) {
                        clearCollapsedSidebarSubmenuSuppression(entry.dataset.module);
                        clearExpandedSidebarSubmenuSuppression(entry.dataset.module);
                    }
                });
            });

            navLinks.forEach((link) => {
                link.addEventListener('click', (event) => {
                    event.preventDefault();
                    const moduleId = link.dataset.module;
                    const isCollapsedDesktopNav = !isSidebarMobileViewport()
                        && document.body.classList.contains('sidebar-is-collapsed');
                    const shouldDismissCollapsedPopout = isCollapsedDesktopNav && hasSidebarSubmenu(moduleId);
                    if (
                        !isCollapsedDesktopNav
                        && hasSidebarSubmenu(moduleId)
                        && activeModuleId === moduleId
                        && expandedSidebarModuleId === moduleId
                    ) {
                        expandedSidebarModuleId = null;
                        suppressExpandedSidebarSubmenu(moduleId);
                        syncSidebarNavState(moduleId, activeNavItemId);
                        link.blur();
                        return;
                    }
                    activateModule(moduleId);
                    if (shouldDismissCollapsedPopout) {
                        suppressCollapsedSidebarSubmenu(moduleId);
                        link.blur();
                    } else {
                        clearCollapsedSidebarSubmenuSuppression(moduleId);
                    }
                });
            });

            navSubmenuLinks.forEach((button) => {
                button.addEventListener('click', (event) => {
                    event.preventDefault();
                    activateModule(
                        button.dataset.module,
                        button.dataset.item,
                        {
                            closeSidebar: true,
                            scrollBehavior: 'smooth',
                            reloadPage: activePageId !== findModuleItem(button.dataset.module, button.dataset.item)?.pageId,
                        },
                    );
                    if (!isSidebarMobileViewport() && document.body.classList.contains('sidebar-is-collapsed')) {
                        suppressCollapsedSidebarSubmenu(button.dataset.module);
                        button.blur();
                    }
                });
            });

            settingsTabButtons.forEach((button) => {
                button.addEventListener('click', (event) => {
                    event.preventDefault();
                    activateModule('settings', button.dataset.settingsItem, {
                        closeSidebar: false,
                        scrollBehavior: 'smooth',
                        reloadPage: false,
                    });
                });
            });

            function focusSearchResult(query) {
                const normalized = String(query || '').trim().toLowerCase();
                if (!normalized) {
                    if (topbarSearchStatus) {
                        topbarSearchStatus.classList.add('hidden');
                        topbarSearchStatus.textContent = '';
                    }
                    return;
                }
                for (const [moduleId, config] of Object.entries(moduleConfigs)) {
                    if (config.title.toLowerCase().includes(normalized)) {
                        activateModule(moduleId);
                        if (topbarSearchStatus) {
                            topbarSearchStatus.textContent = `Opened ${config.title}.`;
                            topbarSearchStatus.classList.remove('hidden');
                        }
                        return;
                    }
                    const itemMatch = getModuleItems(moduleId).find((entry) => entry.label.toLowerCase().includes(normalized));
                    if (itemMatch) {
                        activateModule(moduleId, itemMatch.id);
                        if (topbarSearchStatus) {
                            topbarSearchStatus.textContent = `Opened ${config.title} / ${itemMatch.label}.`;
                            topbarSearchStatus.classList.remove('hidden');
                        }
                        return;
                    }
                }
                if (topbarSearchStatus) {
                    topbarSearchStatus.textContent = `No module or section matched "${query}".`;
                    topbarSearchStatus.classList.remove('hidden');
                }
            }

            function isSidebarMobileViewport() {
                return window.innerWidth <= SIDEBAR_MOBILE_MAX_WIDTH;
            }

            function isSidebarAutoCollapseViewport() {
                return !isSidebarMobileViewport() && window.innerWidth < SIDEBAR_AUTO_COLLAPSE_MAX_WIDTH;
            }

            function syncSidebarResponsiveState() {
                const isMobileViewport = isSidebarMobileViewport();
                const isAutoCollapseViewport = isSidebarAutoCollapseViewport();

                document.body.classList.toggle('sidebar-auto-collapsed', isAutoCollapseViewport);

                if (!isAutoCollapseViewport) {
                    document.body.classList.remove('sidebar-user-expanded');
                }

                if (!isMobileViewport) {
                    document.body.classList.remove('sidebar-mobile-open');
                }

                const isCollapsed = !isMobileViewport && (
                    isAutoCollapseViewport
                        ? !document.body.classList.contains('sidebar-user-expanded')
                        : document.body.classList.contains('sidebar-collapsed')
                );

                document.body.classList.toggle('sidebar-is-collapsed', isCollapsed);
                syncCollapsedSidebarSubmenuSuppression();
                syncExpandedSidebarSubmenuSuppression();

                if (sidebarCollapseBtn) {
                    sidebarCollapseBtn.setAttribute('aria-label', isCollapsed ? 'Expand sidebar' : 'Collapse sidebar');
                    sidebarCollapseBtn.setAttribute('aria-pressed', isCollapsed ? 'true' : 'false');
                }
            }

            sidebarCollapseBtn?.addEventListener('click', () => {
                if (isSidebarAutoCollapseViewport()) {
                    document.body.classList.toggle('sidebar-user-expanded');
                } else {
                    document.body.classList.toggle('sidebar-collapsed');
                }
                syncSidebarResponsiveState();
            });
            mobileNavOpenBtn?.addEventListener('click', () => setSidebarMobileOpen(true));
            mobileNavCloseBtn?.addEventListener('click', () => setSidebarMobileOpen(false));
            sidebarBackdrop?.addEventListener('click', () => setSidebarMobileOpen(false));
            settingsOpenSwitcherBtn?.addEventListener('click', () => workspaceOpenSwitcherBtn.click());
            settingsCreateProjectBtn?.addEventListener('click', () => workspaceCreateProjectBtn.click());
            topbarSearchForm?.addEventListener('submit', (event) => {
                event.preventDefault();
                focusSearchResult(topbarSearchInput?.value || '');
            });
            topbarSearchInput?.addEventListener('keydown', (event) => {
                if (event.key === 'Escape' && topbarSearchStatus) {
                    topbarSearchStatus.classList.add('hidden');
                    topbarSearchStatus.textContent = '';
                    topbarSearchInput.value = '';
                }
            });
            window.addEventListener('resize', syncSidebarResponsiveState);
            syncSidebarResponsiveState();


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
            const defaultApiBaseUrl = `${backendUrl}/api/v1`;
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
            let cachedPredictionModelTrainingStatus = {};
            let cachedExportJobs = [];
            let cachedHealthState = null;
            let cachedHealthStateFetchedAt = 0;
            let healthStateRequest = null;
            let mockStorageEnabled = false;
            let lastSeenConnectorsVersion = '';
            let lastSeenImportsVersion = '';

            function readStoredVersion(key) {
                try {
                    return localStorage.getItem(key) || '';
                } catch (error) {
                    return '';
                }
            }

            function syncSeenDataVersions() {
                lastSeenConnectorsVersion = readStoredVersion(CONNECTORS_VERSION_STORAGE_KEY);
                lastSeenImportsVersion = readStoredVersion(IMPORTS_VERSION_STORAGE_KEY);
            }

            function publishDataVersion(key) {
                const value = `${Date.now()}:${Math.random().toString(16).slice(2)}`;
                try {
                    localStorage.setItem(key, value);
                } catch (error) {
                    console.warn('Unable to persist cross-tab data version:', error);
                }
                if (key === CONNECTORS_VERSION_STORAGE_KEY) {
                    lastSeenConnectorsVersion = value;
                } else if (key === IMPORTS_VERSION_STORAGE_KEY) {
                    lastSeenImportsVersion = value;
                }
            }

            function refreshForExternalDataChange(kind) {
                if (shouldBlockProtectedAppData()) {
                    return;
                }
                if (kind === 'connectors') {
                    cachedConnectors = [];
                    if (activePageId === 'connectors') {
                        loadSavedConnectors();
                    } else if (activePageId === 'player-cohorts') {
                        loadConfiguredSources();
                    } else if (activePageId === 'data-sandbox') {
                        loadDataSandboxGlance();
                        loadDataSandboxMappingControls();
                    } else if (activePageId === 'operator-hub') {
                        loadReadyImportsForOperatorHub();
                    } else if (activePageId === 'action-history') {
                        loadActionHistory();
                    }
                    return;
                }
                if (kind === 'imports') {
                    cachedImports = [];
                    if (activePageId === 'player-cohorts') {
                        loadImportedDataList();
                    } else if (activePageId === 'data-sandbox') {
                        loadDataSandboxGlance();
                    } else if (activePageId === 'operator-hub') {
                        loadReadyImportsForOperatorHub();
                    } else if (activePageId === 'action-history') {
                        loadActionHistory();
                    }
                }
            }

            function syncExternalDataVersions() {
                const connectorsVersion = readStoredVersion(CONNECTORS_VERSION_STORAGE_KEY);
                if (connectorsVersion && connectorsVersion !== lastSeenConnectorsVersion) {
                    lastSeenConnectorsVersion = connectorsVersion;
                    refreshForExternalDataChange('connectors');
                }
                const importsVersion = readStoredVersion(IMPORTS_VERSION_STORAGE_KEY);
                if (importsVersion && importsVersion !== lastSeenImportsVersion) {
                    lastSeenImportsVersion = importsVersion;
                    refreshForExternalDataChange('imports');
                }
            }

            function handleExternalDataVersionEvent(event) {
                if (event.storageArea !== localStorage) {
                    return;
                }
                if (event.key === CONNECTORS_VERSION_STORAGE_KEY && event.newValue && event.newValue !== lastSeenConnectorsVersion) {
                    lastSeenConnectorsVersion = event.newValue;
                    refreshForExternalDataChange('connectors');
                    return;
                }
                if (event.key === IMPORTS_VERSION_STORAGE_KEY && event.newValue && event.newValue !== lastSeenImportsVersion) {
                    lastSeenImportsVersion = event.newValue;
                    refreshForExternalDataChange('imports');
                }
            }

            syncSeenDataVersions();

            function getApiBaseUrl(tenantIdOverride = null, { forceGlobal = false } = {}) {
                if (forceGlobal) {
                    return defaultApiBaseUrl;
                }
                const resolvedTenantId = String(
                    tenantIdOverride !== null && tenantIdOverride !== undefined
                        ? tenantIdOverride
                        : getActiveTenantId()
                ).trim();
                if (accessToken && resolvedTenantId) {
                    return `${backendUrl}/${encodeURIComponent(resolvedTenantId)}/v1`;
                }
                return defaultApiBaseUrl;
            }

            function setAuthStatus(message) {
                if (authStatusText) {
                    authStatusText.textContent = message;
                }
                if (settingsSessionSummary) {
                    settingsSessionSummary.textContent = message;
                }
                if (settingsAuthCopy) {
                    settingsAuthCopy.textContent = message;
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

            function openOrganizationGateway(message = 'Enter the organization URL you want to open.') {
                openWorkspaceOverlay('selection', { selectionStage: 'org' });
                setWorkspaceSelectionSwitchAccountVisible(false);
                setWorkspaceTextStatus(workspaceSelectionStatus, message);
            }

            function clearBearerSession({ openOrgGateway = false, openLoginGateway = !openOrgGateway } = {}) {
                persistAccessToken('');
                authSessionState = null;
                try {
                    localStorage.removeItem(OIDC_CODE_VERIFIER_STORAGE_KEY);
                } catch (error) {
                    console.warn('Unable to clear PKCE verifier:', error);
                }
                setSidebarSessionMenuOpen(false);
                syncAuthModeUi();
                setWorkspaceTextStatus(workspaceSelectorStatus, '');
                setWorkspaceSelectionSwitchAccountVisible(false);
                if (isGoogleLoginConfigured()) {
                    setAuthStatus('Google login required.');
                    if (openOrgGateway) {
                        syncBrowserOrganizationPath('');
                        openOrganizationGateway();
                    } else if (openLoginGateway) {
                        syncBrowserOrganizationPath('');
                        openWorkspaceOverlay('login');
                        refreshWorkspaceLoginStatus();
                    } else {
                        syncWorkspaceOverlayFromSession();
                    }
                    return;
                }
                closeWorkspaceOverlay(true);
                setAuthStatus('Local demo session');
            }

            function handleGoogleSessionFailure(error, fallbackMessage = 'Google session validation failed.') {
                const message = error?.message || fallbackMessage;
                clearBearerSession();
                setAuthStatus(message);
                setWorkspaceTextStatus(workspaceLoginStatus, message, true);
                return message;
            }

            async function ensureValidGoogleSession() {
                if (!isGoogleLoginConfigured() || !accessToken) {
                    return true;
                }
                try {
                    await hydrateAuthSession({
                        forceGlobalScope: true,
                        syncBrowserPath: false,
                        requestedPathTenantId: getOrganizationIdFromPathname(),
                    });
                    return true;
                } catch (error) {
                    handleGoogleSessionFailure(error);
                    return false;
                }
            }

            function handleSessionLogout() {
                disableGoogleAutoSelect();
                persistWorkspaceSelection('', '');
                clearPendingInvite();
                orgSpaceSelect.value = '';
                projectSelect.value = '';
                workspaceModalOrgSelect.value = '';
                workspaceModalProjectSelect.value = '';
                syncWorkspaceSelectionOrgInput('');
                clearBearerSession({ openLoginGateway: true });
            }

            function captureWorkspaceHintsFromUrl() {
                const params = new URLSearchParams(window.location.search);
                const inviteCode = params.get('invite_code');
                const tenantId = params.get('organization_id') || params.get('tenant_id') || getOrganizationIdFromPathname();
                const projectId = params.get('project_id');
                if (tenantId || projectId) {
                    persistWorkspaceSelection(tenantId || '', projectId || '');
                }
                if (inviteCode) {
                    persistPendingInvite({
                        inviteCode,
                        tenantId: tenantId || '',
                        projectId: projectId || '',
                    });
                }
            }

            function getOrganizationIdFromPathname(pathname = window.location.pathname) {
                const normalizedPath = String(pathname || '/').trim() || '/';
                const segments = normalizedPath.split('/').filter(Boolean);
                if (segments.length !== 1) {
                    return '';
                }
                return normalizeOrganizationUrl(segments[0]);
            }

            function getCanonicalBrowserPath(tenantId = '', { preserveHintOnEmpty = false } = {}) {
                const normalizedTenantId = normalizeOrganizationUrl(tenantId)
                    || (preserveHintOnEmpty ? getWorkspaceOrganizationHint() : '');
                return normalizedTenantId ? `/${encodeURIComponent(normalizedTenantId)}` : '/';
            }

            function syncBrowserOrganizationPath(tenantId = '', options = {}) {
                const nextPath = getCanonicalBrowserPath(tenantId, options);
                const currentPath = String(window.location.pathname || '/');
                if (currentPath === nextPath && !window.location.search && !window.location.hash) {
                    return;
                }
                window.history.replaceState({}, document.title, nextPath);
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
                return `${window.location.origin}/`;
            }

            function isLikelyJwt(value) {
                const token = String(value || '').trim();
                return token.split('.').length === 3;
            }

            function selectOidcBearerToken(payload = {}) {
                if (isLikelyJwt(payload.id_token)) {
                    return String(payload.id_token).trim();
                }
                if (isLikelyJwt(payload.access_token)) {
                    return String(payload.access_token).trim();
                }
                throw new Error('Google login did not return a valid ID token for the Kairyx session.');
            }

            function delay(ms) {
                return new Promise((resolve) => {
                    window.setTimeout(resolve, ms);
                });
            }

            async function loadOidcConfig() {
                try {
                    const response = await fetch(`${getApiBaseUrl(null, { forceGlobal: true })}/auth/oidc-config`);
                    oidcConfig = response.ok ? await response.json() : null;
                } catch (error) {
                    oidcConfig = null;
                }
                return oidcConfig;
            }

            async function exchangeAuthorizationCode(code, verifier) {
                if (!oidcConfig || !oidcConfig.token_url) {
                    throw new Error('Google token endpoint is not configured.');
                }
                const form = new URLSearchParams({
                    grant_type: 'authorization_code',
                    code,
                    client_id: oidcConfig.client_id || '',
                    code_verifier: verifier,
                    redirect_uri: redirectUri(),
                });
                if (oidcConfig.audience && oidcConfig.include_audience_parameter !== false) {
                    form.set('audience', oidcConfig.audience);
                }
                const response = await fetch(oidcConfig.token_url, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
                    body: form.toString(),
                });
                const payload = await response.json().catch(() => ({}));
                if (!response.ok) {
                    throw new Error(payload.error_description || payload.detail || 'Google code exchange failed.');
                }
                persistAccessToken(selectOidcBearerToken(payload));
            }

            async function handleOidcRedirect() {
                const params = new URLSearchParams(window.location.search);
                const code = params.get('code');
                if (isGoogleProvider()) {
                    if (code) {
                        try {
                            localStorage.removeItem(OIDC_CODE_VERIFIER_STORAGE_KEY);
                        } catch (error) {
                            console.warn('Unable to clear PKCE verifier:', error);
                        }
                        window.history.replaceState({}, document.title, redirectUri());
                    }
                    return;
                }
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
                    setAuthStatus('Google callback is missing the PKCE verifier.');
                    setWorkspaceTextStatus(workspaceLoginStatus, 'Google callback is missing the PKCE verifier.', true);
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

            async function redeemPendingInviteIfNeeded() {
                const pendingInvite = readPendingInvite();
                if (!accessToken || !pendingInvite || !pendingInvite.inviteCode || inviteRedemptionInFlight) {
                    return false;
                }
                inviteRedemptionInFlight = true;
                try {
                    setAuthStatus('Redeeming project invite...');
                    const response = await fetch(`${getApiBaseUrl(pendingInvite.tenantId || '', { forceGlobal: !pendingInvite.tenantId })}/project-invites/redeem`, {
                        method: 'POST',
                        headers: {
                            Authorization: `Bearer ${accessToken}`,
                            'Content-Type': 'application/json',
                        },
                        body: JSON.stringify({ invite_code: pendingInvite.inviteCode }),
                    });
                    const payload = await response.json().catch(() => ({}));
                    if (!response.ok) {
                        throw new Error(payload.detail || 'Project invite redemption failed.');
                    }
                    persistWorkspaceSelection(
                        payload.organization_space?.organization_id || payload.organization_space?.tenant_id || pendingInvite.tenantId || '',
                        payload.project?.project_id || pendingInvite.projectId || '',
                    );
                    clearPendingInvite();
                    setWorkspaceTextStatus(onboardingInviteStatus, 'Invite redeemed.', false);
                    window.history.replaceState({}, document.title, redirectUri());
                    return true;
                } finally {
                    inviteRedemptionInFlight = false;
                }
            }

            function syncWorkspaceOverlayFromSession() {
                const sessionView = authSessionState || (accessToken ? buildFallbackWorkspaceSession() : null);
                if (isGoogleLoginConfigured() && !accessToken) {
                    if (isGatewayRootPath()) {
                        openWorkspaceOverlay('login');
                        refreshWorkspaceLoginStatus();
                    } else {
                        syncWorkspaceSelectionOrgInput(
                            normalizeOrganizationUrl(
                                workspaceOrgUrlInput?.value
                                || getStoredWorkspaceSelection().tenantId
                                || getOrganizationIdFromPathname()
                                || ''
                            )
                        );
                        openWorkspaceOverlay('selection', { selectionStage: 'org' });
                        setWorkspaceTextStatus(workspaceSelectionStatus, 'Continue with Google to open this organization.');
                    }
                    return;
                }
                if (sessionView?.needs_onboarding) {
                    onboardingFromWorkspaceSelection = false;
                    onboardingResult = null;
                    setOnboardingStep(1);
                    openWorkspaceOverlay('onboarding');
                    setWorkspaceTextStatus(onboardingStatus, 'Enter the organization URL you want to create.');
                    return;
                }
                if (isGatewayRootPath() && accessToken) {
                    if (sessionView?.organization_id) {
                        workspaceModalOrgSelect.value = sessionView.organization_id;
                    }
                    if (sessionView?.project_id) {
                        workspaceModalProjectSelect.value = sessionView.project_id;
                    }
                    syncWorkspaceSelectionOrgInput(
                        normalizeOrganizationUrl(
                            workspaceOrgUrlInput?.value
                            || getStoredWorkspaceSelection().tenantId
                            || sessionView?.organization_id
                            || ''
                        )
                    );
                    const selectionStage = resolveGatewaySelectionStage(sessionView);
                    openWorkspaceOverlay('selection', { selectionStage });
                    setWorkspaceTextStatus(
                        workspaceSelectionStatus,
                        selectionStage === 'project'
                            ? ((sessionView?.accessible_projects || []).length
                                ? 'Select an existing project to go, or create a new one.'
                                : 'Create the first project in this organization to continue.')
                            : 'Enter the organization URL you want to open.',
                    );
                    return;
                }
                if (sessionView?.needs_org_selection || sessionView?.needs_project_selection) {
                    if (!authSessionState && sessionView?.organization_id) {
                        syncWorkspaceSelectionOrgInput(sessionView.organization_id);
                    }
                    openWorkspaceOverlay('selection', {
                        selectionStage: sessionView?.needs_project_selection && sessionView?.organization_id ? 'project' : 'org',
                    });
                    setWorkspaceTextStatus(
                        workspaceSelectionStatus,
                        sessionView.needs_org_selection
                            ? 'Enter the organization URL you want to open.'
                            : ((sessionView?.accessible_projects || []).length
                                ? 'Select a project to continue.'
                                : 'Create the first project in this organization to continue.'),
                    );
                    return;
                }
                if (workspaceOverlayMode !== 'create-project') {
                    closeWorkspaceOverlay(true);
                }
                syncWorkspaceGateClass();
            }

            async function hydrateAuthSession({
                retryCount = 0,
                syncBrowserPath = true,
                forceGlobalScope = false,
                tenantIdOverride = undefined,
                projectIdOverride = undefined,
                preferredBrowserTenantId = undefined,
                requestedPathTenantId = undefined,
                workspaceResolutionError = '',
            } = {}) {
                if (!accessToken) {
                    authSessionState = null;
                    syncAuthModeUi();
                    setAuthStatus(isGoogleLoginConfigured() ? 'Google login required.' : 'Local demo session');
                    syncWorkspaceOverlayFromSession();
                    return;
                }
                const pathTenantId = normalizeOrganizationUrl(requestedPathTenantId || getOrganizationIdFromPathname());
                const tenantId = forceGlobalScope
                    ? ''
                    : String(
                        tenantIdOverride !== undefined
                            ? tenantIdOverride
                            : getActiveTenantId()
                    ).trim();
                const projectId = forceGlobalScope
                    ? ''
                    : String(
                        projectIdOverride !== undefined
                            ? projectIdOverride
                            : getActiveProjectId()
                    ).trim();
                const headers = {
                    Authorization: `Bearer ${accessToken}`,
                };
                if (projectId) {
                    headers['X-Kairyx-Project'] = projectId;
                }
                const response = await fetch(`${getApiBaseUrl(tenantId || '', { forceGlobal: forceGlobalScope || !tenantId })}/auth/me`, {
                    headers,
                });
                const payload = await response.json().catch(() => ({}));
                if (!response.ok) {
                    const errorDetail = payload.detail || 'Google session validation failed.';
                    if (response.status === 401) {
                        clearBearerSession();
                        throw new Error(errorDetail);
                    }
                    if (!forceGlobalScope && (tenantId || projectId) && (response.status === 403 || response.status === 409)) {
                        if (retryCount < 2) {
                            await delay(150 * (retryCount + 1));
                            return hydrateAuthSession({
                                retryCount: retryCount + 1,
                                syncBrowserPath,
                                forceGlobalScope: false,
                                tenantIdOverride,
                                projectIdOverride,
                                preferredBrowserTenantId,
                                requestedPathTenantId: pathTenantId,
                                workspaceResolutionError: errorDetail,
                            });
                        }
                        if (projectId) {
                            persistWorkspaceSelection(tenantId || '', '');
                            return hydrateAuthSession({
                                retryCount: retryCount + 1,
                                syncBrowserPath,
                                forceGlobalScope: false,
                                tenantIdOverride,
                                projectIdOverride: '',
                                preferredBrowserTenantId,
                                requestedPathTenantId: pathTenantId,
                                workspaceResolutionError: errorDetail,
                            });
                        }
                        persistWorkspaceSelection('', '');
                        return hydrateAuthSession({
                            retryCount: retryCount + 1,
                            syncBrowserPath,
                            forceGlobalScope: true,
                            preferredBrowserTenantId,
                            requestedPathTenantId: pathTenantId,
                            workspaceResolutionError: errorDetail,
                        });
                    }
                    throw new Error(errorDetail);
                }
                if (await redeemPendingInviteIfNeeded()) {
                    return hydrateAuthSession({
                        retryCount: retryCount + 1,
                        syncBrowserPath,
                        forceGlobalScope: false,
                        tenantIdOverride,
                        projectIdOverride,
                        preferredBrowserTenantId,
                        requestedPathTenantId: pathTenantId,
                        workspaceResolutionError,
                    });
                }
                const resolvedPayload = forceGlobalScope && pathTenantId
                    ? buildPathScopedWorkspaceSession(payload, pathTenantId, workspaceResolutionError)
                    : payload;
                applyAuthSessionPayload(resolvedPayload);
                if (syncBrowserPath || !isGatewayRootPath()) {
                    const browserTenantId = normalizeOrganizationUrl(
                        preferredBrowserTenantId !== undefined
                            ? preferredBrowserTenantId
                            : (
                                pathTenantId
                                && normalizeOrganizationUrl(resolvedPayload?.organization_id || '')
                                && normalizeOrganizationUrl(resolvedPayload?.organization_id || '') !== pathTenantId
                                    ? pathTenantId
                                    : resolvedPayload?.organization_id || ''
                            )
                    );
                    syncBrowserOrganizationPath(browserTenantId, { preserveHintOnEmpty: true });
                }
                const workspaceBits = [resolvedPayload.organization_id, resolvedPayload.project_id].filter(Boolean);
                setAuthStatus(
                    `Google ${resolvedPayload.display_name || resolvedPayload.email || 'user'}${workspaceBits.length ? ` @ ${workspaceBits.join(' / ')}` : ''}`
                );
                syncWorkspaceOverlayFromSession();
                return resolvedPayload;
            }

            async function startOidcLogin({ organizationId = '', openLoginOverlay = false } = {}) {
                captureWorkspaceHintsFromUrl();
                await loadOidcConfig();
                const resolvedOrganizationId = normalizeOrganizationUrl(
                    organizationId
                    || getStoredWorkspaceSelection().tenantId
                    || getOrganizationIdFromPathname()
                    || '',
                );
                if (resolvedOrganizationId) {
                    persistWorkspaceSelection(resolvedOrganizationId, '');
                }
                if (isGoogleProvider()) {
                    if (openLoginOverlay) {
                        openWorkspaceOverlay('login');
                        refreshWorkspaceLoginStatus(resolvedOrganizationId);
                    }
                    await ensureGoogleIdentityButtons();
                    return;
                }
                if (!oidcConfig || !oidcConfig.authorize_url || !oidcConfig.client_id) {
                    setAuthStatus('Google login is not configured on the backend.');
                    setWorkspaceTextStatus(workspaceLoginStatus, 'Google login is not configured on the backend.', true);
                    return;
                }
                setAuthStatus('Redirecting to OIDC...');
                setWorkspaceTextStatus(workspaceLoginStatus, 'Redirecting to OIDC...');
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
                if (oidcConfig.audience && oidcConfig.include_audience_parameter !== false) {
                    params.set('audience', oidcConfig.audience);
                }
                if (oidcConfig.provider === 'google' && oidcConfig.hosted_domain) {
                    params.set('hd', oidcConfig.hosted_domain);
                }
                window.location.assign(`${oidcConfig.authorize_url}?${params.toString()}`);
            }

            function buildApiHeaders(includeJsonContentType = false) {
                const tenantId = getActiveTenantId();
                const projectId = getActiveProjectId();
                const headers = {};
                if (accessToken) {
                    headers.Authorization = `Bearer ${accessToken}`;
                    if (projectId) {
                        headers['X-Kairyx-Project'] = projectId;
                    }
                } else {
                    headers['x-actor-role'] = LOCAL_DEMO_ACTOR_ROLE;
                    headers['x-actor-id'] = LOCAL_DEMO_ACTOR_ID;
                    headers['x-tenant-id'] = tenantId || LOCAL_DEMO_TENANT_ID;
                    headers['x-project-id'] = projectId || LOCAL_DEMO_PROJECT_ID;
                }
                if (!accessToken && (apiKeyInput.value || '').trim()) {
                    headers['x-api-key'] = apiKeyInput.value.trim();
                }
                if (includeJsonContentType) {
                    headers['Content-Type'] = 'application/json';
                }
                return headers;
            }

            function canUseBrowserStorage(storage) {
                try {
                    const probeKey = '__kairyx_probe__';
                    storage.setItem(probeKey, '1');
                    storage.removeItem(probeKey);
                    return true;
                } catch (error) {
                    return false;
                }
            }

            const canUseLocalMockState = canUseBrowserStorage(window.localStorage);

            function createMockRequestError(message, status = 400, payload = null) {
                const error = new Error(message);
                error.status = status;
                error.payload = payload || { detail: message };
                return error;
            }

            function getMockStorageScope() {
                return {
                    tenantId: String(getActiveTenantId() || LOCAL_DEMO_TENANT_ID).trim() || LOCAL_DEMO_TENANT_ID,
                    projectId: String(getActiveProjectId() || LOCAL_DEMO_PROJECT_ID).trim() || LOCAL_DEMO_PROJECT_ID,
                };
            }

            function getMockStorageKey(tenantId = null, projectId = null) {
                const scope = getMockStorageScope();
                const normalizedTenant = String(tenantId || scope.tenantId || LOCAL_DEMO_TENANT_ID).trim() || LOCAL_DEMO_TENANT_ID;
                const normalizedProject = String(projectId || scope.projectId || LOCAL_DEMO_PROJECT_ID).trim() || LOCAL_DEMO_PROJECT_ID;
                return `${MOCK_STORAGE_KEY_PREFIX}:${normalizedTenant}:${normalizedProject}`;
            }

            function createDefaultMockState() {
                return {
                    version: 1,
                    counters: {
                        connector: 0,
                        import: 0,
                        prediction: 0,
                        export: 0,
                    },
                    connectors: [],
                    imports: [],
                    predictions: [],
                    prediction_results: {},
                    exports: [],
                    export_diagnostics: {},
                };
            }

            function normalizeMockStateShape(candidate = {}) {
                const defaults = createDefaultMockState();
                return {
                    ...defaults,
                    ...candidate,
                    counters: {
                        ...defaults.counters,
                        ...(candidate.counters || {}),
                    },
                    connectors: Array.isArray(candidate.connectors) ? candidate.connectors : [],
                    imports: Array.isArray(candidate.imports) ? candidate.imports : [],
                    predictions: Array.isArray(candidate.predictions) ? candidate.predictions : [],
                    prediction_results: candidate.prediction_results && typeof candidate.prediction_results === 'object'
                        ? candidate.prediction_results
                        : {},
                    exports: Array.isArray(candidate.exports) ? candidate.exports : [],
                    export_diagnostics: candidate.export_diagnostics && typeof candidate.export_diagnostics === 'object'
                        ? candidate.export_diagnostics
                        : {},
                };
            }

            function readMockState(tenantId = null, projectId = null) {
                if (!canUseLocalMockState) {
                    return createDefaultMockState();
                }
                try {
                    const raw = window.localStorage.getItem(getMockStorageKey(tenantId, projectId));
                    if (!raw) {
                        return createDefaultMockState();
                    }
                    return normalizeMockStateShape(JSON.parse(raw));
                } catch (error) {
                    console.warn('Unable to read local mock state:', error);
                    return createDefaultMockState();
                }
            }

            function writeMockState(state, tenantId = null, projectId = null) {
                if (!canUseLocalMockState) {
                    return;
                }
                try {
                    window.localStorage.setItem(
                        getMockStorageKey(tenantId, projectId),
                        JSON.stringify(normalizeMockStateShape(state)),
                    );
                } catch (error) {
                    console.warn('Unable to persist local mock state:', error);
                }
            }

            function updateMockState(updater, tenantId = null, projectId = null) {
                const current = readMockState(tenantId, projectId);
                const next = normalizeMockStateShape(updater(current) || current);
                writeMockState(next, tenantId, projectId);
                return next;
            }

            function nextMockId(state, kind) {
                state.counters[kind] = Number(state.counters[kind] || 0) + 1;
                return `${kind}_${Date.now()}_${state.counters[kind]}`;
            }

            function parseApiPath(path) {
                const url = new URL(path, backendUrl);
                let pathname = url.pathname;
                if (pathname.startsWith('/api/v1')) {
                    pathname = pathname.slice('/api/v1'.length) || '/';
                } else {
                    const scopedMatch = pathname.match(/^\/[^/]+\/v1(\/.*)?$/);
                    if (scopedMatch) {
                        pathname = scopedMatch[1] || '/';
                    }
                }
                return {
                    pathname,
                    segments: pathname.split('/').filter(Boolean).map((segment) => decodeURIComponent(segment)),
                    searchParams: url.searchParams,
                };
            }

            function isMockStateResourcePath(path) {
                const { segments } = parseApiPath(path);
                return ['connectors', 'imports', 'predictions', 'exports'].includes(String(segments[0] || ''));
            }

            function shouldHandleWithLocalMockState(path) {
                return mockStorageEnabled && isMockStateResourcePath(path);
            }

            function hashText(value) {
                let hash = 0;
                const text = String(value || '');
                for (let index = 0; index < text.length; index += 1) {
                    hash = ((hash << 5) - hash) + text.charCodeAt(index);
                    hash |= 0;
                }
                return Math.abs(hash);
            }

            function parseCompactDate(value) {
                const text = String(value || '').trim();
                if (/^\d{8}$/.test(text)) {
                    return new Date(`${text.slice(0, 4)}-${text.slice(4, 6)}-${text.slice(6, 8)}T00:00:00Z`);
                }
                return parseIsoDate(text || new Date().toISOString());
            }

            function calculateMockImportVolume(job) {
                const start = parseCompactDate((job.spec || {}).start_date);
                const end = parseCompactDate((job.spec || {}).end_date);
                const msPerDay = 24 * 60 * 60 * 1000;
                const daySpan = Math.max(1, Math.round((end.getTime() - start.getTime()) / msPerDay) + 1);
                return Math.max(240, daySpan * 420);
            }

            function buildMockCompletedImportJob(job) {
                const total = calculateMockImportVolume(job);
                const shardCount = Math.max(1, Math.ceil(total / 500));
                const sourceName = String((((job || {}).spec || {}).source_name) || '').trim();
                return {
                    ...job,
                    status: 'completed',
                    updated_at: new Date().toISOString(),
                    spec: {
                        ...((job || {}).spec || {}),
                        display_name: String((((job || {}).spec || {}).display_name) || '').trim()
                            || formatImportDisplayName(sourceName || 'Import', job.created_at || new Date().toISOString()),
                    },
                    progress: {
                        current: total,
                        total,
                        pct: 100,
                        details: {
                            source: sourceName,
                            phase: 'completed',
                            events_staged: total,
                            page_size: 500,
                            processed_manifests: shardCount,
                            total_manifests: shardCount,
                            processing: {
                                normalized_records: total,
                                canonical_users: Math.max(60, Math.round(total * 0.42)),
                                rows_curated: total,
                            },
                        },
                    },
                };
            }

            function getMockExecutionLabel(connectors = [], predictionMode = 'local') {
                const normalizedMode = String(predictionMode || 'local').toLowerCase();
                if (normalizedMode === 'ai') return 'AI';
                if (normalizedMode === 'cloud') return 'Cloud';
                if (normalizedMode === 'parallel') return 'AI + Cloud';
                return connectors.some((connector) => (
                    String(connector.type || '').toLowerCase() === 'google'
                    && Boolean((connector.config || {}).api_key)
                )) ? 'AI' : 'Local Model';
            }

            function buildMockPredictionRows(predictionJob, importJob) {
                const seed = hashText(
                    `${predictionJob.id}:${importJob.id}:${(importJob.spec || {}).source_name}:${(importJob.spec || {}).start_date}:${(importJob.spec || {}).end_date}`,
                );
                const rows = [];
                const riskReasons = {
                    high: ['7-day inactivity spike', 'LTV drop after campaign exit', 'Session collapse after ad exposure'],
                    medium: ['Engagement slowing over 3 days', 'Shorter sessions vs baseline', 'Offer response cooled off'],
                    low: ['Recently active', 'Stable spend pattern', 'Frequent reward claims'],
                };
                const actions = {
                    high: 'Launch win-back push with credit offer',
                    medium: 'Send reminder with personalized bundle',
                    low: 'Hold out from intervention and monitor',
                };
                for (let index = 0; index < 36; index += 1) {
                    const basis = seed + (index * 97);
                    const userNumber = (basis % 90000) + 10000;
                    const risk = index < 12 ? 'high' : (index < 24 ? 'medium' : 'low');
                    const sessions = risk === 'high' ? (basis % 3) + 1 : risk === 'medium' ? (basis % 5) + 3 : (basis % 7) + 7;
                    const events = sessions * ((basis % 11) + 8);
                    const ltvBase = risk === 'high' ? 14 : risk === 'medium' ? 42 : 88;
                    rows.push({
                        user_id: `user_${userNumber}`,
                        ltv: Number((ltvBase + (basis % 25) + (index * 0.37)).toFixed(2)),
                        session_count: sessions,
                        event_count: events,
                        predicted_churn_risk: risk,
                        churn_reason: riskReasons[risk][basis % riskReasons[risk].length],
                        suggested_action: actions[risk],
                        effective_local_model_version: 'heuristic_v1',
                        effective_local_model_state: 'untrained',
                        completed_at: new Date().toISOString(),
                    });
                }
                return rows;
            }

            function buildMockCompletedPredictionJob(job, importJob, connectors = []) {
                const rows = buildMockPredictionRows(job, importJob);
                const predictionMode = String(((job.spec || {}).prediction_mode) || 'local').toLowerCase();
                return {
                    job: {
                        ...job,
                        status: 'completed',
                        updated_at: new Date().toISOString(),
                        progress: {
                            current: rows.length,
                            total: rows.length,
                            pct: 100,
                            details: {
                                execution_label: getMockExecutionLabel(connectors, predictionMode),
                                rows_written: rows.length,
                                prediction_mode: predictionMode,
                                effective_local_model_version: 'heuristic_v1',
                                effective_local_model_state: 'untrained',
                            },
                        },
                    },
                    rows,
                };
            }

            function filterMockExportRows(rows = [], includeRisks = []) {
                const normalizedRisks = includeRisks.map((risk) => String(risk || '').toLowerCase()).filter(Boolean);
                if (!normalizedRisks.length) {
                    return rows;
                }
                return rows.filter((row) => normalizedRisks.includes(String(row.predicted_churn_risk || '').toLowerCase()));
            }

            function buildMockExportDiagnostics(job, rows = [], priorAttempts = 0) {
                const details = (job.progress || {}).details || {};
                return {
                    export_job_id: job.id,
                    provider: details.provider || (job.spec || {}).provider || 'webhook',
                    channel: (job.spec || {}).channel || 'push_notification',
                    audience_name: (job.spec || {}).audience_name || details.audience_name || null,
                    delivered_count: rows.length,
                    preview_user_ids: rows.slice(0, 5).map((row) => row.user_id),
                    attempts: Math.max(1, priorAttempts),
                    last_status: job.status,
                    updated_at: job.updated_at,
                };
            }

            function buildMockImportOperationsPayload(job) {
                const progress = job.progress || {};
                const details = progress.details || {};
                const eventsStaged = Number(details.events_staged || progress.total || 0);
                const manifestsProcessed = Number(details.processed_manifests || details.total_manifests || 0);
                return {
                    import_job_id: job.id,
                    status: job.status,
                    current_step: mapImportStatus(job.status),
                    progress,
                    items: [
                        {
                            operation_id: `${job.id}:stage`,
                            name: 'stage_raw_events',
                            status: 'completed',
                            recorded_at: job.updated_at || job.created_at,
                            summary: {
                                events_staged: eventsStaged,
                            },
                        },
                        {
                            operation_id: `${job.id}:normalize`,
                            name: 'normalize_manifests',
                            status: 'completed',
                            recorded_at: job.updated_at || job.created_at,
                            summary: {
                                manifests_processed: manifestsProcessed,
                            },
                        },
                    ],
                };
            }

            function buildMockImportQualityPayload(job) {
                const progress = job.progress || {};
                const details = progress.details || {};
                return {
                    import_job_id: job.id,
                    mapping_coverage: 100.0,
                    checkpoint_state: {
                        processed: Number(progress.current || 0),
                        total: Number(progress.total || 0),
                    },
                    audit_id: `audit_${job.id}`,
                    quality_report: {
                        required_mapping_coverage: 100.0,
                        canonical_user_id_coverage: 100.0,
                        top20_field_coverage: {
                            fields: {
                                canonical_user_id: { coverage: 100.0 },
                                event_name: { coverage: 100.0 },
                                event_time: { coverage: 100.0 },
                            },
                        },
                    },
                    identity_summary: {
                        source_of_truth_matrix: {
                            canonical_user_id: 'mock',
                        },
                    },
                    source_of_truth: {
                        canonical_user_id: 'mock',
                    },
                    conflict_summary: {
                        count: 0,
                    },
                    processing_stats: details.processing || null,
                };
            }

            function buildMockImportManifestsPayload(job, tenantId, projectId) {
                const details = (job.progress || {}).details || {};
                const totalManifests = Math.max(1, Number(details.total_manifests || 1));
                const totalEvents = Number(details.events_staged || (job.progress || {}).total || 0);
                const manifestEvents = Math.max(1, Math.ceil(totalEvents / totalManifests));
                const basePath = `mock://raw/${tenantId}/${projectId}/${job.id}`;
                return {
                    items: Array.from({ length: totalManifests }, (_, index) => ({
                        manifest_id: `${job.id}:${index}`,
                        shard_index: index,
                        status: 'completed',
                        event_count: manifestEvents,
                        schema_version: 'v1',
                        gcs_uri: `${basePath}/part-${String(index).padStart(5, '0')}.jsonl`,
                    })),
                };
            }

            async function primeMockStorageMode(path = '') {
                if (backendMode !== 'unknown' || !preferLocalMockState || !isMockStateResourcePath(path)) {
                    return;
                }
                try {
                    await ensureHealthState();
                } catch (error) {
                    // Keep the network path if health cannot be determined.
                }
            }

            async function networkRequest(path, options = {}) {
                const { method = 'GET', body, headers = buildApiHeaders(Boolean(body)), _workspaceRetryAttempted = false } = options;
                const normalizedPath = `/${String(path || '').replace(/^\/+/, '')}`;
                const isWorkspaceBootstrapPath = normalizedPath === '/auth/me'
                    || normalizedPath === '/projects'
                    || normalizedPath === '/project-invites/redeem'
                    || normalizedPath.startsWith('/onboarding/');
                if (accessToken && !isWorkspaceBootstrapPath && shouldBlockProtectedAppData()) {
                    const sessionPayload = await hydrateAuthSession();
                    if (!isAuthenticatedWorkspaceReady()) {
                        throw buildWorkspaceContextError(sessionPayload || authSessionState || {}, 409);
                    }
                }
                const response = await fetch(`${getApiBaseUrl()}${normalizedPath}`, {
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
                        setAuthStatus('Google session expired.');
                    }
                    const errorDetail = payload.detail || payload.message || `Request failed (${response.status})`;
                    if (
                        accessToken
                        && !_workspaceRetryAttempted
                        && isWorkspaceContextResponse(response.status, errorDetail)
                    ) {
                        const sessionPayload = await hydrateAuthSession();
                        if (isAuthenticatedWorkspaceReady()) {
                            return networkRequest(normalizedPath, {
                                ...options,
                                headers: buildApiHeaders(Boolean(body)),
                                _workspaceRetryAttempted: true,
                            });
                        }
                        throw buildWorkspaceContextError(sessionPayload || authSessionState || payload, response.status);
                    }
                    const error = new Error(errorDetail);
                    error.status = response.status;
                    error.payload = payload;
                    throw error;
                }
                return payload;
            }

            async function mockStorageRequest(path, options = {}) {
                const { method = 'GET', body } = options;
                const normalizedMethod = String(method || 'GET').toUpperCase();
                const { segments, searchParams } = parseApiPath(path);
                const primary = String(segments[0] || '');
                const { tenantId, projectId } = getMockStorageScope();

                if (primary === 'connectors') {
                    if (segments.length === 1 && normalizedMethod === 'GET') {
                        return readMockState(tenantId, projectId).connectors;
                    }
                    if (segments.length === 1 && normalizedMethod === 'POST') {
                        let created = null;
                        updateMockState((state) => {
                            const connectorName = String((body || {}).name || '').trim();
                            if (!connectorName) {
                                throw createMockRequestError('Connector name is required.');
                            }
                            if (state.connectors.some((connector) => connector.name === connectorName)) {
                                throw createMockRequestError(`Connector '${connectorName}' already exists.`, 409);
                            }
                            const now = new Date().toISOString();
                            created = {
                                name: connectorName,
                                type: (body || {}).type,
                                config: (body || {}).config || {},
                                created_at: now,
                                updated_at: now,
                            };
                            state.connectors = latestByCreatedAt([created, ...state.connectors]);
                            return state;
                        }, tenantId, projectId);
                        return created;
                    }
                    if (segments.length === 2 && normalizedMethod === 'DELETE') {
                        const connectorName = segments[1];
                        let deleted = false;
                        updateMockState((state) => {
                            const nextConnectors = state.connectors.filter((connector) => connector.name !== connectorName);
                            deleted = nextConnectors.length !== state.connectors.length;
                            state.connectors = nextConnectors;
                            return state;
                        }, tenantId, projectId);
                        if (!deleted) {
                            throw createMockRequestError(`Connector '${connectorName}' not found.`, 404);
                        }
                        return null;
                    }
                }

                if (primary === 'imports') {
                    if (segments.length === 1 && normalizedMethod === 'GET') {
                        return { items: readMockState(tenantId, projectId).imports };
                    }
                    if (segments.length === 1 && normalizedMethod === 'POST') {
                        let created = null;
                        updateMockState((state) => {
                            const sourceName = String((body || {}).source_name || '').trim();
                            const connector = state.connectors.find((item) => item.name === sourceName);
                            if (!connector) {
                                throw createMockRequestError(`Connector '${sourceName}' not found.`, 404);
                            }
                            const now = new Date().toISOString();
                            const baseJob = {
                                id: nextMockId(state, 'import'),
                                status: 'queued',
                                created_at: now,
                                updated_at: now,
                                spec: {
                                    source_name: sourceName,
                                    start_date: String((body || {}).start_date || ''),
                                    end_date: String((body || {}).end_date || ''),
                                    connector_type: connector.type,
                                    display_name: formatImportDisplayName(sourceName, now),
                                },
                                progress: {
                                    current: 0,
                                    total: 0,
                                    pct: 0,
                                    details: {
                                        source: sourceName,
                                        phase: 'queued',
                                    },
                                },
                            };
                            created = buildMockCompletedImportJob(baseJob);
                            state.imports = latestByCreatedAt([created, ...state.imports]);
                            return state;
                        }, tenantId, projectId);
                        return created;
                    }
                    if (segments.length >= 2) {
                        const jobId = segments[1];
                        if (segments.length === 2 && normalizedMethod === 'DELETE') {
                            let deleted = false;
                            updateMockState((state) => {
                                const predictionIds = state.predictions
                                    .filter((job) => String(((job.spec || {}).import_job_id || '')) === String(jobId))
                                    .map((job) => job.id);
                                const exportIds = state.exports
                                    .filter((job) => predictionIds.includes(String((job.spec || {}).prediction_job_id || '')))
                                    .map((job) => job.id);
                                const nextPredictionResults = { ...state.prediction_results };
                                predictionIds.forEach((predictionId) => {
                                    delete nextPredictionResults[predictionId];
                                });
                                const nextExportDiagnostics = { ...state.export_diagnostics };
                                exportIds.forEach((exportId) => {
                                    delete nextExportDiagnostics[exportId];
                                });
                                const nextImports = state.imports.filter((job) => job.id !== jobId);
                                deleted = nextImports.length !== state.imports.length;
                                state.imports = nextImports;
                                state.predictions = state.predictions.filter((job) => !predictionIds.includes(job.id));
                                state.prediction_results = nextPredictionResults;
                                state.exports = state.exports.filter((job) => !exportIds.includes(job.id));
                                state.export_diagnostics = nextExportDiagnostics;
                                return state;
                            }, tenantId, projectId);
                            if (!deleted) {
                                throw createMockRequestError(`Import job '${jobId}' not found.`, 404);
                            }
                            return null;
                        }
                        if (segments[2] === 'run' && normalizedMethod === 'POST') {
                            const job = readMockState(tenantId, projectId).imports.find((item) => item.id === jobId);
                            if (!job) {
                                throw createMockRequestError(`Import job '${jobId}' not found.`, 404);
                            }
                            return {
                                ...normalizeImportJob(job),
                                started: searchParams.get('background') !== 'false',
                            };
                        }
                        if (segments[2] === 'stop' && normalizedMethod === 'POST') {
                            let job = null;
                            updateMockState((state) => {
                                const target = state.imports.find((item) => item.id === jobId);
                                if (!target) {
                                    throw createMockRequestError(`Import job '${jobId}' not found.`, 404);
                                }
                                job = {
                                    ...target,
                                    status: 'stopped',
                                    updated_at: new Date().toISOString(),
                                    progress: {
                                        ...(target.progress || {}),
                                        details: {
                                            ...((target.progress || {}).details || {}),
                                            stop_reason: 'Stopped by user.',
                                        },
                                    },
                                };
                                state.imports = latestByCreatedAt([job, ...state.imports.filter((item) => item.id !== jobId)]);
                                return state;
                            }, tenantId, projectId);
                            return job;
                        }
                        const job = readMockState(tenantId, projectId).imports.find((item) => item.id === jobId);
                        if (!job) {
                            throw createMockRequestError(`Import job '${jobId}' not found.`, 404);
                        }
                        if (segments[2] === 'operations' && normalizedMethod === 'GET') {
                            return buildMockImportOperationsPayload(job);
                        }
                        if (segments[2] === 'quality' && normalizedMethod === 'GET') {
                            return buildMockImportQualityPayload(job);
                        }
                        if (segments[2] === 'manifests' && normalizedMethod === 'GET') {
                            return buildMockImportManifestsPayload(job, tenantId, projectId);
                        }
                    }
                }

                if (primary === 'predictions') {
                    if (segments.length === 1 && normalizedMethod === 'GET') {
                        return { items: readMockState(tenantId, projectId).predictions };
                    }
                    if (segments.length === 1 && normalizedMethod === 'POST') {
                        let created = null;
                        updateMockState((state) => {
                            const audienceScope = String((body || {}).audience_scope || '').trim().toLowerCase()
                                || (String((body || {}).import_job_id || '').trim() ? 'import' : 'source');
                            let importJob = null;
                            let sourceName = String((body || {}).source_name || '').trim();
                            if (audienceScope === 'import') {
                                const importJobId = String((body || {}).import_job_id || '').trim();
                                importJob = state.imports.find((item) => item.id === importJobId) || null;
                                sourceName = sourceName || String((((importJob || {}).spec || {}).source_name) || '').trim();
                            } else {
                                const matchingImports = latestByCreatedAt(state.imports.filter((item) => (
                                    String((((item || {}).spec || {}).source_name) || '').trim() === sourceName
                                    && String(item.status || '').toLowerCase() === 'completed'
                                )));
                                importJob = matchingImports[0] || null;
                            }
                            if (!importJob) {
                                throw createMockRequestError('Imported data not found.', 404);
                            }
                            if (String(importJob.status || '').toLowerCase() !== 'completed') {
                                throw createMockRequestError('Imported data is not ready yet.', 409);
                            }
                            const now = new Date().toISOString();
                            const baseJob = {
                                id: nextMockId(state, 'prediction'),
                                status: 'queued',
                                created_at: now,
                                updated_at: now,
                                spec: {
                                    import_job_id: importJob.id,
                                    source_name: sourceName || String((((importJob || {}).spec || {}).source_name) || '').trim(),
                                    audience_scope: audienceScope,
                                    prediction_mode: (body || {}).prediction_mode || 'local',
                                },
                                progress: {
                                    current: 0,
                                    total: 0,
                                    pct: 0,
                                    details: {
                                        execution_label: getMockExecutionLabel(state.connectors, (body || {}).prediction_mode || 'local'),
                                        rows_written: 0,
                                    },
                                },
                            };
                            const completed = buildMockCompletedPredictionJob(baseJob, importJob, state.connectors);
                            created = completed.job;
                            state.predictions = latestByCreatedAt([created, ...state.predictions]);
                            state.prediction_results[created.id] = completed.rows;
                            return state;
                        }, tenantId, projectId);
                        return created;
                    }
                    if (segments.length >= 2) {
                        const jobId = segments[1];
                        if (segments.length === 2 && normalizedMethod === 'GET') {
                            const job = readMockState(tenantId, projectId).predictions.find((item) => item.id === jobId);
                            if (!job) {
                                throw createMockRequestError(`Prediction job '${jobId}' not found.`, 404);
                            }
                            return job;
                        }
                        if (segments[2] === 'run' && normalizedMethod === 'POST') {
                            const job = readMockState(tenantId, projectId).predictions.find((item) => item.id === jobId);
                            if (!job) {
                                throw createMockRequestError(`Prediction job '${jobId}' not found.`, 404);
                            }
                            return job;
                        }
                        if (segments[2] === 'stop' && normalizedMethod === 'POST') {
                            let job = null;
                            updateMockState((state) => {
                                const target = state.predictions.find((item) => item.id === jobId);
                                if (!target) {
                                    throw createMockRequestError(`Prediction job '${jobId}' not found.`, 404);
                                }
                                job = {
                                    ...target,
                                    status: 'stopped',
                                    updated_at: new Date().toISOString(),
                                };
                                state.predictions = latestByCreatedAt([job, ...state.predictions.filter((item) => item.id !== jobId)]);
                                return state;
                            }, tenantId, projectId);
                            return job;
                        }
                        if (segments[2] === 'results' && normalizedMethod === 'GET') {
                            const state = readMockState(tenantId, projectId);
                            const rows = Array.isArray(state.prediction_results[jobId]) ? state.prediction_results[jobId] : [];
                            const page = Math.max(1, Number(searchParams.get('page') || 1));
                            const pageSize = Math.max(1, Number(searchParams.get('page_size') || 100));
                            const startIndex = (page - 1) * pageSize;
                            return {
                                items: rows.slice(startIndex, startIndex + pageSize),
                                total: rows.length,
                                page,
                                page_size: pageSize,
                            };
                        }
                    }
                }

                if (primary === 'exports') {
                    if (segments.length === 1 && normalizedMethod === 'GET') {
                        return { items: readMockState(tenantId, projectId).exports };
                    }
                    if (segments.length === 1 && normalizedMethod === 'POST') {
                        let created = null;
                        updateMockState((state) => {
                            const predictionJobId = String((body || {}).prediction_job_id || '').trim();
                            const predictionJob = state.predictions.find((item) => item.id === predictionJobId);
                            if (!predictionJob) {
                                throw createMockRequestError(`Prediction job '${predictionJobId}' not found.`, 404);
                            }
                            const predictionRows = Array.isArray(state.prediction_results[predictionJobId])
                                ? state.prediction_results[predictionJobId]
                                : [];
                            const selectedRows = filterMockExportRows(
                                predictionRows,
                                Array.isArray((body || {}).include_risks) ? body.include_risks : [],
                            );
                            const now = new Date().toISOString();
                            const details = {
                                provider: (body || {}).provider || 'webhook',
                                channel: (body || {}).channel || 'push_notification',
                                count: selectedRows.length,
                                audience_name: (body || {}).audience_name || null,
                            };
                            created = {
                                id: nextMockId(state, 'export'),
                                status: 'completed',
                                created_at: now,
                                updated_at: now,
                                spec: {
                                    prediction_job_id: predictionJobId,
                                    provider: details.provider,
                                    channel: details.channel,
                                    include_churned: Boolean((body || {}).include_churned),
                                    include_risks: Array.isArray((body || {}).include_risks) ? body.include_risks : [],
                                    audience_name: details.audience_name,
                                    webhook_url: (body || {}).webhook_url || null,
                                    webhook_token: (body || {}).webhook_token || null,
                                },
                                progress: {
                                    current: selectedRows.length,
                                    total: selectedRows.length,
                                    pct: 100,
                                    details,
                                },
                            };
                            state.exports = latestByCreatedAt([created, ...state.exports]);
                            state.export_diagnostics[created.id] = buildMockExportDiagnostics(created, selectedRows, 1);
                            return state;
                        }, tenantId, projectId);
                        return created;
                    }
                    if (segments.length >= 2) {
                        const jobId = segments[1];
                        if (segments[2] === 'run' && normalizedMethod === 'POST') {
                            const job = readMockState(tenantId, projectId).exports.find((item) => item.id === jobId);
                            if (!job) {
                                throw createMockRequestError(`Export job '${jobId}' not found.`, 404);
                            }
                            return job;
                        }
                        if (segments[2] === 'diagnostics' && normalizedMethod === 'GET') {
                            const state = readMockState(tenantId, projectId);
                            const diagnostics = state.export_diagnostics[jobId];
                            if (!diagnostics) {
                                throw createMockRequestError(`Export job '${jobId}' diagnostics not found.`, 404);
                            }
                            return diagnostics;
                        }
                        if (segments[2] === 'retry' && normalizedMethod === 'POST') {
                            let diagnostics = null;
                            updateMockState((state) => {
                                const job = state.exports.find((item) => item.id === jobId);
                                if (!job) {
                                    throw createMockRequestError(`Export job '${jobId}' not found.`, 404);
                                }
                                const prior = state.export_diagnostics[jobId] || buildMockExportDiagnostics(job, [], 0);
                                diagnostics = {
                                    ...prior,
                                    attempts: Number(prior.attempts || 0) + 1,
                                    updated_at: new Date().toISOString(),
                                };
                                state.export_diagnostics[jobId] = diagnostics;
                                state.exports = latestByCreatedAt([
                                    {
                                        ...job,
                                        updated_at: diagnostics.updated_at,
                                    },
                                    ...state.exports.filter((item) => item.id !== jobId),
                                ]);
                                return state;
                            }, tenantId, projectId);
                            return diagnostics;
                        }
                    }
                }

                return networkRequest(path, options);
            }

            async function apiRequest(path, options = {}) {
                const normalizedPath = `/${String(path || '').replace(/^\/+/, '')}`;
                await primeMockStorageMode(normalizedPath);
                if (shouldHandleWithLocalMockState(normalizedPath)) {
                    return mockStorageRequest(normalizedPath, options);
                }
                return networkRequest(normalizedPath, options);
            }

            async function switchWorkspaceSelection(tenantId, projectId, { reloadPage = true, syncBrowserPath = true } = {}) {
                persistWorkspaceSelection(tenantId || '', projectId || '');
                const payload = await hydrateAuthSession({
                    syncBrowserPath,
                    tenantIdOverride: tenantId || '',
                    projectIdOverride: projectId || '',
                    preferredBrowserTenantId: tenantId || '',
                });
                if (reloadPage && activePageId && (!accessToken || payload?.project_id)) {
                    activateModule(activeModuleId, activeNavItemId, { closeSidebar: false, scrollBehavior: 'instant', reloadPage: true });
                }
                return payload;
            }

            function openCreateProjectOverlay() {
                if (!getActiveTenantId()) {
                    openWorkspaceOverlay('selection', { selectionStage: 'org' });
                    setWorkspaceTextStatus(workspaceSelectionStatus, 'Enter an organization URL before creating a project.', true);
                    return;
                }
                workspaceCreateProjectInlineNameInput.value = '';
                workspaceCreateProjectInlineIdInput.value = '';
                workspaceCreateProjectInlineDescriptionInput.value = '';
                setWorkspaceTextStatus(workspaceCreateProjectStatus, '');
                syncWorkspaceSelectedOrgContext();
                openWorkspaceOverlay('create-project', { allowClose: true });
            }

            oidcLoginBtn.addEventListener('click', async () => {
                try {
                    await startOidcLogin();
                } catch (error) {
                    setAuthStatus(error.message || 'Google login failed.');
                    setWorkspaceTextStatus(workspaceLoginStatus, error.message || 'Google login failed.', true);
                }
            });
            workspaceGoogleLoginBtn.addEventListener('click', async () => {
                try {
                    await startOidcLogin();
                } catch (error) {
                    setAuthStatus(error.message || 'Google login failed.');
                    setWorkspaceTextStatus(workspaceLoginStatus, error.message || 'Google login failed.', true);
                }
            });

            oidcLogoutBtn.addEventListener('click', handleSessionLogout);

            sidebarSessionButton?.addEventListener('click', (event) => {
                event.preventDefault();
                if (sidebarSessionButton.disabled) {
                    return;
                }
                const isOpen = sidebarSessionButton.getAttribute('aria-expanded') === 'true';
                setSidebarSessionMenuOpen(!isOpen);
            });
            sidebarSessionLogoutBtn?.addEventListener('click', (event) => {
                event.preventDefault();
                handleSessionLogout();
            });
            document.addEventListener('click', (event) => {
                if (
                    sidebarSessionMenu
                    && !sidebarSessionMenu.classList.contains('hidden')
                    && !sidebarSessionButton?.contains(event.target)
                    && !sidebarSessionMenu.contains(event.target)
                ) {
                    setSidebarSessionMenuOpen(false);
                }
            });
            document.addEventListener('keydown', (event) => {
                if (event.key === 'Escape') {
                    setSidebarSessionMenuOpen(false);
                }
            });

            workspaceModalCloseBtn.addEventListener('click', () => closeWorkspaceOverlay());
            workspaceOpenSwitcherBtn.addEventListener('click', () => {
                setWorkspaceSelectionSwitchAccountVisible(false);
                openWorkspaceOverlay('selection', { allowClose: true, selectionStage: 'org' });
                setWorkspaceTextStatus(workspaceSelectionStatus, '');
            });
            workspaceCreateProjectBtn.addEventListener('click', openCreateProjectOverlay);
            orgSpaceSelect.addEventListener('change', async () => {
                try {
                    setWorkspaceTextStatus(workspaceSelectorStatus, 'Switching organization space...');
                    await switchWorkspaceSelection(orgSpaceSelect.value || '', '', { reloadPage: false });
                    setWorkspaceTextStatus(
                        workspaceSelectorStatus,
                        authSessionState?.needs_project_selection ? 'Select a project to finish switching.' : 'Organization space updated.',
                    );
                } catch (error) {
                    setWorkspaceTextStatus(workspaceSelectorStatus, error.message || 'Failed to switch organization space.', true);
                }
            });
            projectSelect.addEventListener('change', async () => {
                try {
                    setWorkspaceTextStatus(workspaceSelectorStatus, 'Switching project...');
                    await switchWorkspaceSelection(orgSpaceSelect.value || '', projectSelect.value || '');
                    setWorkspaceTextStatus(workspaceSelectorStatus, 'Project updated.');
                } catch (error) {
                    setWorkspaceTextStatus(workspaceSelectorStatus, error.message || 'Failed to switch project.', true);
                }
            });
            workspaceSelectionBackBtn.addEventListener('click', () => {
                setWorkspaceSelectionSwitchAccountVisible(false);
                setWorkspaceTextStatus(workspaceSelectionStatus, '');
                setWorkspaceSelectionStage('org');
            });
            workspaceSelectionResolveBtn.addEventListener('click', async () => {
                const requestedOrganizationInput = workspaceOrgUrlInput.value || workspaceModalOrgSelect.value;
                setWorkspaceSelectionSwitchAccountVisible(false);
                if (!accessToken && isGoogleLoginConfigured()) {
                    const organizationId = normalizeOrganizationUrl(requestedOrganizationInput);
                    if (!organizationId) {
                        setWorkspaceTextStatus(workspaceSelectionStatus, 'Enter an organization URL to continue.', true);
                        return;
                    }
                    workspaceOrgUrlInput.value = organizationId;
                    persistWorkspaceSelection(organizationId, '');
                    try {
                        await startOidcLogin({ organizationId, openLoginOverlay: true });
                    } catch (error) {
                        setWorkspaceTextStatus(workspaceSelectionStatus, error.message || 'Google login failed.', true);
                    }
                    return;
                }
                if (!(await ensureValidGoogleSession())) {
                    return;
                }
                const organizationId = normalizeNewOrganizationId(requestedOrganizationInput);
                if (!organizationId) {
                    setWorkspaceTextStatus(workspaceSelectionStatus, 'Enter an organization URL to continue.', true);
                    return;
                }
                if (!isValidNewOrganizationId(organizationId)) {
                    setWorkspaceTextStatus(
                        workspaceSelectionStatus,
                        'Organization URL must use only lowercase letters and numbers and be 16 characters or fewer.',
                        true,
                    );
                    return;
                }
                let tenant = findAccessibleTenant(organizationId);
                if (!tenant) {
                    let accessState = null;
                    try {
                        setWorkspaceTextStatus(workspaceSelectionStatus, 'Checking organization access...');
                        accessState = await inspectOrganizationSpaceAccess(organizationId);
                    } catch (error) {
                        setWorkspaceTextStatus(workspaceSelectionStatus, error.message || 'Failed to inspect organization access.', true);
                        return;
                    }
                    if (accessState?.exists && !accessState?.accessible) {
                        workspaceOrgUrlInput.value = organizationId;
                        persistWorkspaceSelection(organizationId, '');
                        setWorkspaceSelectionSwitchAccountVisible(true, organizationId);
                        setWorkspaceTextStatus(
                            workspaceSelectionStatus,
                            `"${organizationId}" already exists, but it is not linked to this Google account. Sign in with a different Google account or pick one of your own organizations.`,
                            true,
                        );
                        return;
                    }
                    if (accessState?.accessible) {
                        tenant = accessState.organization || {
                            organization_id: organizationId,
                            name: accessState.organization?.name || organizationId,
                            role: accessState.role || null,
                        };
                    }
                }
                if (!tenant) {
                    if (!canSelfServeOrganizationCreation()) {
                        setWorkspaceTextStatus(
                            workspaceSelectionStatus,
                            accessState?.exists
                                ? `"${organizationId}" already exists, but it is not linked to this Google account. Enter one of your existing organization URLs, or sign in as another account.`
                                : `"${organizationId}" does not exist yet. Enter one of your existing organization URLs, or sign in as an account that can create it.`,
                            true,
                        );
                        return;
                    }
                    primeOnboardingForNewOrganization(organizationId);
                    return;
                }
                onboardingFromWorkspaceSelection = false;
                workspaceOrgUrlInput.value = tenant.organization_id;
                try {
                    setWorkspaceTextStatus(workspaceSelectionStatus, 'Loading projects...');
                    workspaceCreateProjectNameInput.value = '';
                    workspaceCreateProjectIdInput.value = '';
                    workspaceModalProjectSelect.value = '';
                    const payload = await switchWorkspaceSelection(tenant.organization_id, '', { reloadPage: false, syncBrowserPath: false });
                    if (payload?.project_id && !payload?.needs_project_selection) {
                        syncBrowserOrganizationPath(payload.organization_id || tenant.organization_id, { preserveHintOnEmpty: true });
                        closeWorkspaceOverlay(true);
                        setWorkspaceTextStatus(workspaceSelectionStatus, '');
                        if (activePageId) {
                            activateModule(activeModuleId, activeNavItemId, { closeSidebar: false, scrollBehavior: 'instant', reloadPage: true });
                        }
                        return;
                    }
                    workspaceModalOrgSelect.value = authSessionState?.organization_id || tenant.organization_id;
                    workspaceModalProjectSelect.value = authSessionState?.project_id || '';
                    setWorkspaceSelectionStage('project');
                    setWorkspaceTextStatus(
                        workspaceSelectionStatus,
                        (authSessionState?.accessible_projects || []).length
                            ? 'Use an existing project or type a new project name.'
                            : 'Create the first project in this organization to continue.',
                    );
                } catch (error) {
                    setWorkspaceTextStatus(workspaceSelectionStatus, error.message || 'Failed to load organization projects.', true);
                }
            });
            workspaceModalOrgSelect.addEventListener('change', () => {
                setWorkspaceSelectionSwitchAccountVisible(false);
                syncWorkspaceSelectionOrgInput(workspaceModalOrgSelect.value || '');
                setWorkspaceTextStatus(workspaceSelectionStatus, '');
            });
            workspaceModalProjectSelect.addEventListener('change', refreshWorkspaceSelectionCopy);
            workspaceSelectionSwitchAccountBtn.addEventListener('click', () => {
                const organizationId = normalizeOrganizationUrl(
                    workspaceSelectionSwitchAccountBtn.dataset.organizationId
                    || workspaceOrgUrlInput.value
                    || workspaceModalOrgSelect.value
                    || ''
                );
                disableGoogleAutoSelect();
                persistWorkspaceSelection(organizationId, '');
                clearPendingInvite();
                orgSpaceSelect.value = '';
                projectSelect.value = '';
                workspaceModalProjectSelect.value = '';
                clearBearerSession({ openLoginGateway: true });
                setWorkspaceTextStatus(
                    workspaceLoginStatus,
                    organizationId
                        ? `Sign in with a Google account that can access "${organizationId}".`
                        : 'Sign in with a different Google account.',
                    true,
                );
            });
            workspaceSelectionContinueBtn.addEventListener('click', async () => {
                if (!workspaceModalProjectSelect.value) {
                    setWorkspaceTextStatus(workspaceSelectionStatus, 'Select an existing project first.', true);
                    return;
                }
                try {
                    setWorkspaceTextStatus(workspaceSelectionStatus, 'Applying workspace...');
                    const payload = await switchWorkspaceSelection(
                        workspaceModalOrgSelect.value || authSessionState?.organization_id || '',
                        workspaceModalProjectSelect.value || '',
                    );
                    if (payload?.project_id) {
                        closeWorkspaceOverlay(true);
                    }
                    setWorkspaceTextStatus(workspaceSelectionStatus, '');
                } catch (error) {
                    setWorkspaceTextStatus(workspaceSelectionStatus, error.message || 'Failed to apply workspace.', true);
                }
            });
            workspaceSelectionCreateProjectBtn.addEventListener('click', async () => {
                const tenantId = workspaceModalOrgSelect.value || authSessionState?.organization_id || '';
                const projectName = workspaceCreateProjectNameInput.value.trim();
                const projectId = slugifyIdentifier(workspaceCreateProjectIdInput.value || projectName);
                if (!tenantId) {
                    setWorkspaceTextStatus(workspaceSelectionStatus, 'Enter an organization URL first.', true);
                    return;
                }
                if (!projectName || !projectId) {
                    setWorkspaceTextStatus(workspaceSelectionStatus, 'Enter a project name to add a new project.', true);
                    return;
                }
                try {
                    setWorkspaceTextStatus(workspaceSelectionStatus, 'Adding project to organization space...');
                    await switchWorkspaceSelection(tenantId, '', { reloadPage: false, syncBrowserPath: false });
                    await apiRequest('/projects', {
                        method: 'POST',
                        body: {
                            project_id: projectId,
                            name: projectName,
                            description: workspaceCreateProjectDescriptionInput.value.trim(),
                        },
                    });
                    await switchWorkspaceSelection(tenantId, projectId);
                    closeWorkspaceOverlay(true);
                    setWorkspaceTextStatus(workspaceSelectorStatus, `Added project ${projectId}.`);
                } catch (error) {
                    setWorkspaceTextStatus(workspaceSelectionStatus, error.message || 'Failed to add the project.', true);
                }
            });
            workspaceCreateProjectCancelBtn.addEventListener('click', () => {
                if (isWorkspaceSelectionRequired()) {
                    openWorkspaceOverlay('selection', { selectionStage: 'project' });
                    return;
                }
                closeWorkspaceOverlay(true);
            });
            workspaceCreateProjectSubmitBtn.addEventListener('click', async () => {
                const projectId = slugifyIdentifier(workspaceCreateProjectInlineIdInput.value || workspaceCreateProjectInlineNameInput.value);
                if (!projectId || !workspaceCreateProjectInlineNameInput.value.trim()) {
                    setWorkspaceTextStatus(workspaceCreateProjectStatus, 'Enter a project name to continue.', true);
                    return;
                }
                try {
                    setWorkspaceTextStatus(workspaceCreateProjectStatus, 'Creating project...');
                    await apiRequest('/projects', {
                        method: 'POST',
                        body: {
                            project_id: projectId,
                            name: workspaceCreateProjectInlineNameInput.value.trim(),
                            description: workspaceCreateProjectInlineDescriptionInput.value.trim(),
                        },
                    });
                    await switchWorkspaceSelection(getActiveTenantId(), projectId);
                    closeWorkspaceOverlay(true);
                    setWorkspaceTextStatus(workspaceSelectorStatus, `Created project ${projectId}.`);
                } catch (error) {
                    setWorkspaceTextStatus(workspaceCreateProjectStatus, error.message || 'Failed to create project.', true);
                }
            });
            onboardingBackBtn.addEventListener('click', () => {
                setWorkspaceTextStatus(onboardingStatus, '');
                if (onboardingStep === 1 && onboardingFromWorkspaceSelection) {
                    openWorkspaceOverlay('selection', { selectionStage: 'org' });
                    syncWorkspaceSelectionOrgInput(onboardingOrganizationIdInput.value || onboardingOrganizationNameInput.value || workspaceOrgUrlInput.value || '');
                    setWorkspaceTextStatus(
                        workspaceSelectionStatus,
                        'Enter an existing organization URL, or continue with this new one to create it.',
                    );
                    return;
                }
                if (onboardingStep > 1) {
                    setOnboardingStep(onboardingStep - 1);
                }
            });
            onboardingNextBtn.addEventListener('click', async () => {
                if (onboardingStep === 1) {
                    const organizationId = normalizeNewOrganizationId(onboardingOrganizationIdInput.value || onboardingOrganizationNameInput.value);
                    if (!organizationId) {
                        setWorkspaceTextStatus(onboardingStatus, 'Enter an organization URL to continue.', true);
                        return;
                    }
                    if (!isValidNewOrganizationId(organizationId)) {
                        setWorkspaceTextStatus(
                            onboardingStatus,
                            'Organization URL must use only lowercase letters and numbers and be 16 characters or fewer.',
                            true,
                        );
                        return;
                    }
                    onboardingOrganizationNameInput.value = organizationId;
                    onboardingOrganizationIdInput.value = organizationId;
                    setWorkspaceTextStatus(onboardingStatus, '');
                    setOnboardingStep(2);
                    return;
                }
                if (onboardingStep === 2) {
                    const organizationId = normalizeNewOrganizationId(onboardingOrganizationIdInput.value || onboardingOrganizationNameInput.value);
                    const projectId = slugifyIdentifier(onboardingProjectIdInput.value || onboardingProjectNameInput.value);
                    if (!organizationId) {
                        setWorkspaceTextStatus(
                            onboardingStatus,
                            'Organization URL must use only lowercase letters and numbers and be 16 characters or fewer.',
                            true,
                        );
                        return;
                    }
                    if (!projectId || !onboardingProjectNameInput.value.trim()) {
                        setWorkspaceTextStatus(onboardingStatus, 'Enter a project name to continue.', true);
                        return;
                    }
                    onboardingProjectIdInput.value = projectId;
                    try {
                        setWorkspaceTextStatus(onboardingStatus, 'Creating organization space and first project...');
                        onboardingResult = await apiRequest('/onboarding/organization-space', {
                            method: 'POST',
                            body: {
                                organization_id: organizationId,
                                organization_name: humanizeIdentifier(organizationId) || organizationId,
                                project_id: projectId,
                                project_name: onboardingProjectNameInput.value.trim(),
                                project_description: onboardingProjectDescriptionInput.value.trim(),
                            },
                        });
                        const createdOrganizationId = onboardingResult.organization_space?.organization_id
                            || onboardingResult.organization_space?.tenant_id
                            || organizationId;
                        const createdProjectId = onboardingResult.project?.project_id || projectId;
                        persistWorkspaceSelection(
                            createdOrganizationId,
                            createdProjectId,
                        );
                        onboardingFromWorkspaceSelection = false;
                        await switchWorkspaceSelection(createdOrganizationId, createdProjectId, { reloadPage: false });
                        if (isAuthenticatedWorkspaceReady()) {
                            activateModule(activeModuleId, activeNavItemId, { closeSidebar: false, scrollBehavior: 'instant', reloadPage: true });
                        }
                        closeWorkspaceOverlay(true);
                        setWorkspaceTextStatus(workspaceSelectorStatus, `Created ${organizationId} / ${projectId}.`);
                    } catch (error) {
                        setWorkspaceTextStatus(onboardingStatus, error.message || 'Failed to create organization space.', true);
                    }
                }
            });
            onboardingGenerateInviteBtn.addEventListener('click', async () => {
                if (!authSessionState?.project_id) {
                    setWorkspaceTextStatus(onboardingInviteStatus, 'Create the organization space first.', true);
                    return;
                }
                try {
                    setWorkspaceTextStatus(onboardingInviteStatus, 'Generating invite link...');
                    const payload = await apiRequest(`/projects/${encodeURIComponent(authSessionState.project_id)}/invites`, {
                        method: 'POST',
                        body: {
                            email: onboardingInviteEmailInput.value.trim() || null,
                            display_name: onboardingInviteDisplayNameInput.value.trim() || null,
                            org_role: onboardingInviteOrgRoleSelect.value || 'member',
                            project_role: onboardingInviteProjectRoleSelect.value || 'operator',
                        },
                    });
                    const inviteUrl = payload.invite?.invite_url ? new URL(payload.invite.invite_url, window.location.origin).toString() : '';
                    onboardingInviteLinkInput.value = inviteUrl;
                    setWorkspaceTextStatus(onboardingInviteStatus, 'Invite link generated.');
                } catch (error) {
                    setWorkspaceTextStatus(onboardingInviteStatus, error.message || 'Failed to generate invite link.', true);
                }
            });
            onboardingConnectBtn.addEventListener('click', () => {
                closeWorkspaceOverlay(true);
                activateModule('data-core', 'connectors');
            });
            onboardingSkipBtn.addEventListener('click', () => closeWorkspaceOverlay(true));
            document.querySelectorAll('.workspace-source-chip').forEach((chip) => {
                chip.addEventListener('click', () => {
                    closeWorkspaceOverlay(true);
                    activateModule('data-core', 'connectors');
                });
            });

            async function fetchHealthLiveState() {
                const controller = new AbortController();
                const timeoutId = window.setTimeout(() => controller.abort(), HEALTH_LIVE_TIMEOUT_MS);
                try {
                    const response = await fetch(`${getApiBaseUrl()}/health/live`, {
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
                if (shouldBlockProtectedAppData()) {
                    return cachedConnectors;
                }
                const connectors = await apiRequest('/connectors');
                cachedConnectors = Array.isArray(connectors) ? connectors.map(normalizeConnector) : [];
                return cachedConnectors;
            }

            async function refreshImportsState() {
                if (shouldBlockProtectedAppData()) {
                    return cachedImports;
                }
                const payload = await apiRequest('/imports');
                const items = Array.isArray(payload.items) ? payload.items : [];
                cachedImports = items.map(normalizeImportJob);
                return cachedImports;
            }

            async function refreshPredictionJobsState() {
                if (shouldBlockProtectedAppData()) {
                    return cachedPredictionJobs;
                }
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

            function buildDefaultPredictionModelTrainingStatus() {
                return {};
            }

            async function refreshPredictionModelReadinessState() {
                if (shouldBlockProtectedAppData()) {
                    cachedPredictionModelReadiness = cachedPredictionModelReadiness || buildDefaultPredictionModelReadiness();
                    cachedPredictionModelTrainingStatus = cachedPredictionModelTrainingStatus || buildDefaultPredictionModelTrainingStatus();
                    return {
                        readiness: cachedPredictionModelReadiness,
                        training_status: cachedPredictionModelTrainingStatus,
                    };
                }
                try {
                    const payload = await apiRequest('/predictions/models/runs');
                    cachedPredictionModelReadiness = payload.readiness || buildDefaultPredictionModelReadiness();
                    cachedPredictionModelTrainingStatus = payload.training_status || buildDefaultPredictionModelTrainingStatus();
                } catch (error) {
                    console.warn('Unable to refresh prediction model readiness:', error);
                    cachedPredictionModelReadiness = buildDefaultPredictionModelReadiness();
                    cachedPredictionModelTrainingStatus = buildDefaultPredictionModelTrainingStatus();
                }
                return {
                    readiness: cachedPredictionModelReadiness,
                    training_status: cachedPredictionModelTrainingStatus,
                };
            }

            async function refreshExportJobsState() {
                if (shouldBlockProtectedAppData()) {
                    return cachedExportJobs;
                }
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

            function getPredictionModelTrainingStatus() {
                return cachedPredictionModelTrainingStatus || buildDefaultPredictionModelTrainingStatus();
            }

            function isPredictionModelTrainingActive() {
                const status = String((getPredictionModelTrainingStatus().status || '')).toLowerCase();
                return status === 'running' || status === 'stopping';
            }

            function isPredictionModelTrainingStopping() {
                return String((getPredictionModelTrainingStatus().status || '')).toLowerCase() === 'stopping';
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

            function formatPredictionTrainingStatusLabel(value = '') {
                const normalized = String(value || '').trim().toLowerCase();
                if (!normalized) {
                    return '';
                }
                if (normalized === 'running') {
                    return 'Training';
                }
                return normalized.replace(/_/g, ' ').replace(/\b\w/g, (c) => c.toUpperCase());
            }

            function formatPredictionTrainingStageLabel(value = '') {
                const normalized = String(value || '').trim().toLowerCase();
                if (!normalized) {
                    return '';
                }
                return normalized.replace(/_/g, ' ');
            }

            function formatPredictionTrainingClassBalance(classBalance = {}) {
                const entries = Object.entries(classBalance || {})
                    .filter(([, value]) => Number.isFinite(Number(value)))
                    .map(([label, value]) => `${label}:${Number(value)}`);
                return entries.length ? entries.join(', ') : '';
            }

            function formatLabeledRowsSummary(rowCount = 0, minRowsRequired = 12) {
                const resolvedRowCount = Math.max(0, Number(rowCount || 0));
                const resolvedMinRows = Math.max(0, Number(minRowsRequired || 0));
                const formattedRows = formatCount(resolvedRowCount);
                const formattedMinimum = formatCount(resolvedMinRows);
                if (resolvedMinRows > 0 && resolvedRowCount < resolvedMinRows) {
                    return `${formattedRows}/${formattedMinimum} labeled rows`;
                }
                if (resolvedMinRows > 0) {
                    return `${formattedRows} labeled rows (min ${formattedMinimum})`;
                }
                return `${formattedRows} labeled rows`;
            }

            function renderPredictionModelTrainingStatus(customMessage = '') {
                const trainingStatus = getPredictionModelTrainingStatus();
                const readiness = getPredictionModelReadiness();
                const statusLabel = formatPredictionTrainingStatusLabel(trainingStatus.status);
                const stageLabel = formatPredictionTrainingStageLabel(trainingStatus.stage);
                const rowCount = Number(trainingStatus.row_count || readiness.baseline_rows || 0);
                const minRowsRequired = Number(trainingStatus.min_rows_required || readiness.min_rows_required || 12);
                const classBalanceLabel = formatPredictionTrainingClassBalance(trainingStatus.class_balance || {});
                const trainedAt = trainingStatus.trained_at || readiness.last_trained_at;
                const usersProcessed = Number(trainingStatus.users_processed || 0);
                const usersTotal = Number(trainingStatus.users_total || 0);
                const exposuresProcessed = Number(trainingStatus.exposures_processed || 0);
                const exposuresTotal = Number(trainingStatus.exposures_total || 0);
                const detailParts = [];
                const titleLines = [];

                if (customMessage) {
                    detailParts.push(String(customMessage).trim());
                } else if (statusLabel) {
                    detailParts.push(statusLabel);
                } else {
                    detailParts.push('No local model training status recorded yet.');
                }

                if (stageLabel && String(trainingStatus.status || '').toLowerCase() === 'running') {
                    detailParts.push(stageLabel);
                }
                detailParts.push(formatLabeledRowsSummary(rowCount, minRowsRequired));
                if (usersTotal > 0 && String(trainingStatus.status || '').toLowerCase() === 'running') {
                    detailParts.push(`${usersProcessed}/${usersTotal} users`);
                }
                if (exposuresTotal > 0 && String(trainingStatus.status || '').toLowerCase() === 'running') {
                    detailParts.push(`${exposuresProcessed}/${exposuresTotal} exposures`);
                }
                if (classBalanceLabel) {
                    detailParts.push(`classes ${classBalanceLabel}`);
                }
                if (trainedAt) {
                    detailParts.push(`updated ${formatDateTime(trainedAt)}`);
                }

                if (statusLabel) {
                    titleLines.push(`Status: ${statusLabel}`);
                }
                if (stageLabel) {
                    titleLines.push(`Stage: ${stageLabel}`);
                }
                titleLines.push(`Labeled rows: ${formatLabeledRowsSummary(rowCount, minRowsRequired)}`);
                if (usersTotal > 0) {
                    titleLines.push(`Users processed: ${usersProcessed}/${usersTotal}`);
                }
                if (exposuresTotal > 0) {
                    titleLines.push(`Exposures processed: ${exposuresProcessed}/${exposuresTotal}`);
                }
                if (classBalanceLabel) {
                    titleLines.push(`Class balance: ${classBalanceLabel}`);
                }
                if (trainingStatus.dataset_id) {
                    titleLines.push(`Dataset: ${trainingStatus.dataset_id}`);
                }
                if (trainingStatus.error) {
                    titleLines.push(`Error: ${trainingStatus.error}`);
                }
                if (trainedAt) {
                    titleLines.push(`Last updated: ${formatDateTime(trainedAt)}`);
                }

                if (predictionModelTrainingStatus) {
                    const normalizedTrainingStatus = String(trainingStatus.status || '').toLowerCase();
                    predictionModelTrainingStatus.style.color = normalizedTrainingStatus === 'failed'
                        ? 'var(--red)'
                        : (normalizedTrainingStatus === 'running' ? 'var(--primary-color)' : 'var(--text-secondary)');
                    predictionModelTrainingStatus.textContent = detailParts.filter(Boolean).join(' - ');
                    predictionModelTrainingStatus.title = titleLines.filter(Boolean).join('\n');
                }
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
                    formatLabeledRowsSummary(baselineRows, minRowsRequired),
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
                    `Labeled rows: ${formatLabeledRowsSummary(baselineRows, minRowsRequired)}`,
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
                    predictionModelReadinessDetails.textContent = detailParts.filter(Boolean).join(' - ');
                    predictionModelReadinessDetails.title = titleLines.filter(Boolean).join('\n');
                }
                if (predictionLocalWarning) {
                    if (getSelectedPredictionMode() === 'local' && String(readiness.state || '').toLowerCase() !== 'ready') {
                        predictionLocalWarning.textContent = 'Using heuristic fallback until the local model has enough labeled data.';
                    } else {
                        predictionLocalWarning.textContent = '';
                    }
                }
                renderPredictionModelTrainingStatus();
                setPredictionModelTrainingActionState(Boolean(predictionModelTrainingRequest));
                syncPredictionModelTrainingPolling();
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
                        mockStorageEnabled = Boolean(
                            preferLocalMockState
                            && canUseLocalMockState
                            && String(payload?.mode || '').toLowerCase() === 'mock'
                            && !Boolean(payload?.mock_state_persistent)
                        );
                        cachedHealthState = payload;
                        cachedHealthStateFetchedAt = Date.now();
                        return payload;
                    })
                    .catch((error) => {
                        mockStorageEnabled = false;
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
                publishDataVersion(CONNECTORS_VERSION_STORAGE_KEY);
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
                publishDataVersion(IMPORTS_VERSION_STORAGE_KEY);
                return normalizeImportJob(created);
            }

            async function queueMockImportRun(jobId) {
                try {
                    const job = await apiRequest(`/imports/${encodeURIComponent(jobId)}/run?background=true`, { method: 'POST' });
                    setInlineStatus(
                        importListStatus,
                        job.started === false
                            ? `Import ${job.name || jobId} is already running. Status will update below.`
                            : `Import ${job.name || jobId} started in the background. Status will update below.`,
                    );
                    loadImportedDataList().catch((error) => {
                        console.error('Unable to refresh import jobs after starting background import:', error);
                    });
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
                publishDataVersion(IMPORTS_VERSION_STORAGE_KEY);
                return cachedImports.find((item) => item.id === jobId) || normalizeImportJob(job);
            }

            async function deleteImportRecord(jobId) {
                await apiRequest(`/imports/${encodeURIComponent(jobId)}`, { method: 'DELETE' });
                await refreshImportsState();
                publishDataVersion(IMPORTS_VERSION_STORAGE_KEY);
            }

            function getLatestPredictionJob(audienceKey, completedOnly = false, audienceScope = getSelectedPredictionAudienceScope()) {
                return getLatestPredictionJobForMode(audienceKey, completedOnly, '', audienceScope);
            }

            function getPredictionJobMode(job = {}) {
                return String((((job || {}).spec || {}).prediction_mode) || 'local').toLowerCase();
            }

            function getLatestPredictionJobForMode(audienceKey, completedOnly = false, predictionMode = '', audienceScope = getSelectedPredictionAudienceScope()) {
                return cachedPredictionJobs.find((job) => {
                    if (!predictionJobMatchesSelection(job, audienceKey, audienceScope, predictionMode)) return false;
                    if (!completedOnly) return true;
                    return String(job.status || '').toLowerCase() === 'completed';
                }) || null;
            }

            function getLatestActivePredictionJob(audienceKey, predictionMode = '', audienceScope = getSelectedPredictionAudienceScope()) {
                return cachedPredictionJobs.find((job) => (
                    predictionJobMatchesSelection(job, audienceKey, audienceScope, predictionMode)
                    && isPredictionJobActive(job)
                )) || null;
            }

            function getLatestCompletedPredictionJob(audienceKey, excludeJobId = null, predictionMode = '', audienceScope = getSelectedPredictionAudienceScope()) {
                return cachedPredictionJobs.find((job) => {
                    if (excludeJobId && String(job.id || '') === String(excludeJobId)) {
                        return false;
                    }
                    return (
                        predictionJobMatchesSelection(job, audienceKey, audienceScope, predictionMode)
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
                baselinePredictionAudienceKey = null;
                baselinePredictionAudienceScope = null;
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

            async function loadBaselinePredictionRows(audienceKey, excludeJobId = null, predictionMode = '', audienceScope = getSelectedPredictionAudienceScope()) {
                const normalizedAudienceKey = String(audienceKey || '').trim();
                const normalizedAudienceScope = String(audienceScope || '').trim().toLowerCase() || 'source';
                if (!normalizedAudienceKey) {
                    clearBaselinePredictionRows();
                    return [];
                }

                const latestCompletedJob = getLatestCompletedPredictionJob(
                    normalizedAudienceKey,
                    excludeJobId,
                    predictionMode,
                    normalizedAudienceScope,
                );
                if (!latestCompletedJob) {
                    clearBaselinePredictionRows();
                    baselinePredictionAudienceKey = normalizedAudienceKey;
                    baselinePredictionAudienceScope = normalizedAudienceScope;
                    return [];
                }

                if (
                    baselinePredictionAudienceScope === normalizedAudienceScope
                    && baselinePredictionAudienceKey === normalizedAudienceKey
                    && baselinePredictionJobId === String(latestCompletedJob.id || '')
                ) {
                    return baselinePredictionPlayers;
                }

                baselinePredictionPlayers = await fetchPredictionRows(latestCompletedJob.id);
                baselinePredictionAudienceKey = normalizedAudienceKey;
                baselinePredictionAudienceScope = normalizedAudienceScope;
                baselinePredictionJobId = String(latestCompletedJob.id || '');
                return baselinePredictionPlayers;
            }

            async function renderCompletedPredictionJob(completedJob, audienceKey, audienceScope = getSelectedPredictionAudienceScope()) {
                const completedRows = await fetchPredictionRows(completedJob.id);
                activePredictionJobId = null;
                clearPersistedActivePredictionJob();
                predictionStopRequested = false;
                baselinePredictionPlayers = [...completedRows];
                baselinePredictionAudienceKey = String(audienceKey || '');
                baselinePredictionAudienceScope = String(audienceScope || '').toLowerCase() || 'source';
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

            async function createPredictionRecord(audienceKey, predictionMode = 'local', audienceScope = getSelectedPredictionAudienceScope()) {
                await ensureHealthState().catch(() => null);
                const requestBody = {
                    prediction_mode: predictionMode,
                    audience_scope: audienceScope,
                };
                if (audienceScope === 'source') {
                    requestBody.source_name = audienceKey;
                } else {
                    requestBody.import_job_id = audienceKey;
                }
                const created = await apiRequest('/predictions', {
                    method: 'POST',
                    body: requestBody,
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
                                    publishDataVersion(CONNECTORS_VERSION_STORAGE_KEY);
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
                    if (isWorkspaceContextError(error)) {
                        connectorListDiv.innerHTML = '<p>Finish workspace setup to load connectors.</p>';
                        return;
                    }
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
            const datasetSelectLabel = document.getElementById('dataset-select-label');
            const datasetSelectHelp = document.getElementById('dataset-select-help');
            const predictionAudienceScopeSelect = document.getElementById('prediction-audience-scope-select');
            const predictionModeSelect = document.getElementById('prediction-mode-select');
            const predictionModelReadinessBadge = document.getElementById('prediction-model-readiness-badge');
            const predictionModelReadinessDetails = document.getElementById('prediction-model-readiness-details');
            const trainLocalModelBtn = document.getElementById('train-local-model-btn');
            const refreshLocalModelStatusBtn = document.getElementById('refresh-local-model-status-btn');
            const predictionModelTrainingStatus = document.getElementById('prediction-model-training-status');
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
            let baselinePredictionAudienceKey = null;
            let baselinePredictionAudienceScope = null;
            let currentPage = 1;
            let itemsPerPage = 25;
            let activePredictionJobId = null;
            let predictionStopRequested = false;
            let predictionModelTrainingRequest = null;
            let predictionModelTrainingPollInterval = null;
            const ACTIVE_PREDICTION_STORAGE_KEY = 'kairyx.activePredictionJob';

            function getPredictionImportJobId(job) {
                return String(((job || {}).spec || {}).import_job_id || '');
            }

            function getPredictionAudienceScope(job = {}) {
                const spec = (job || {}).spec || {};
                const explicitScope = String(spec.audience_scope || '').trim().toLowerCase();
                if (explicitScope) {
                    return explicitScope;
                }
                return String(spec.source_name || '').trim() ? 'source' : 'import';
            }

            function getPredictionJobSourceName(job = {}) {
                const spec = (job || {}).spec || {};
                const details = (((job || {}).progress || {}).details || {});
                return String(spec.source_name || details.source_name || '').trim();
            }

            function getPredictionAudienceKey(job = {}) {
                return getPredictionAudienceScope(job) === 'source'
                    ? getPredictionJobSourceName(job)
                    : getPredictionImportJobId(job);
            }

            function getSelectedPredictionAudienceScope() {
                return String((predictionAudienceScopeSelect && predictionAudienceScopeSelect.value) || 'source').toLowerCase();
            }

            function getSelectedPredictionAudienceKey() {
                return String((datasetSelect && datasetSelect.value) || '').trim();
            }

            function getPredictionAudienceOptions(audienceScope = getSelectedPredictionAudienceScope()) {
                const readyJobs = cachedImports.filter((job) => job.status === 'Ready to Use');
                if (audienceScope === 'source') {
                    const latestImportBySource = new Map();
                    latestByCreatedAt(readyJobs).forEach((job) => {
                        const sourceName = String(((job.spec || {}).source_name) || '').trim();
                        if (!sourceName || latestImportBySource.has(sourceName)) {
                            return;
                        }
                        latestImportBySource.set(sourceName, job);
                    });
                    return Array.from(latestImportBySource.entries())
                        .sort((a, b) => a[0].localeCompare(b[0]))
                        .map(([sourceName, latestJob]) => ({
                            value: sourceName,
                            label: `${sourceName} (latest: ${latestJob.name})`,
                        }));
                }
                return readyJobs.map((job) => ({
                    value: String(job.id || ''),
                    label: job.name,
                }));
            }

            function renderPredictionAudienceSelectorMeta() {
                const audienceScope = getSelectedPredictionAudienceScope();
                if (datasetSelectLabel) {
                    datasetSelectLabel.textContent = audienceScope === 'source' ? 'Select Source' : 'Select Import';
                }
                if (datasetSelectHelp) {
                    datasetSelectHelp.textContent = audienceScope === 'source'
                        ? 'Source mode resolves to the latest completed import when prediction starts.'
                        : 'Import mode scores only the players contained in the selected completed import.';
                }
            }

            function predictionJobMatchesSelection(job, audienceKey, audienceScope = getSelectedPredictionAudienceScope(), predictionMode = '') {
                const normalizedAudienceKey = String(audienceKey || '').trim();
                const normalizedAudienceScope = String(audienceScope || '').trim().toLowerCase() || 'source';
                const normalizedMode = String(predictionMode || '').toLowerCase();
                if (!normalizedAudienceKey) {
                    return false;
                }
                if (getPredictionAudienceScope(job) !== normalizedAudienceScope) {
                    return false;
                }
                if (getPredictionAudienceKey(job) !== normalizedAudienceKey) {
                    return false;
                }
                if (normalizedMode && getPredictionJobMode(job) !== normalizedMode) {
                    return false;
                }
                return true;
            }

            function readPersistedActivePredictionJob() {
                try {
                    const raw = window.sessionStorage.getItem(ACTIVE_PREDICTION_STORAGE_KEY);
                    if (!raw) return null;
                    const parsed = JSON.parse(raw);
                    if (!parsed || !parsed.job_id) return null;
                    if (!parsed.audience_scope && parsed.import_job_id) {
                        parsed.audience_scope = 'import';
                        parsed.audience_key = String(parsed.import_job_id);
                    }
                    return parsed;
                } catch (error) {
                    return null;
                }
            }

            function persistActivePredictionJob(jobId, audienceKey, audienceScope) {
                if (!jobId || !audienceKey) return;
                try {
                    window.sessionStorage.setItem(
                        ACTIVE_PREDICTION_STORAGE_KEY,
                        JSON.stringify({
                            job_id: String(jobId),
                            audience_scope: String(audienceScope || 'source'),
                            audience_key: String(audienceKey),
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

            function applyPredictionModelTrainingPayload(payload = {}) {
                if (payload && payload.training_status) {
                    cachedPredictionModelTrainingStatus = payload.training_status;
                }
                if (payload && payload.readiness) {
                    cachedPredictionModelReadiness = payload.readiness;
                }
            }

            function syncPredictionModelTrainingPolling() {
                if (isPredictionModelTrainingActive()) {
                    if (predictionModelTrainingPollInterval) {
                        return;
                    }
                    predictionModelTrainingPollInterval = window.setInterval(async () => {
                        try {
                            await refreshPredictionModelReadinessState();
                            renderPredictionModelReadiness();
                        } catch (error) {
                            console.warn('Unable to refresh local model training status while running:', error);
                        }
                    }, 1000);
                    return;
                }
                if (predictionModelTrainingPollInterval) {
                    window.clearInterval(predictionModelTrainingPollInterval);
                    predictionModelTrainingPollInterval = null;
                }
            }

            function setPredictionModelTrainingActionState(isRunning = false) {
                const trainingActive = isPredictionModelTrainingActive();
                const stopping = isPredictionModelTrainingStopping();
                if (trainLocalModelBtn) {
                    trainLocalModelBtn.disabled = isRunning || (Boolean(activePredictionJobId) && !trainingActive) || stopping;
                    trainLocalModelBtn.textContent = stopping ? 'Stopping...' : (trainingActive ? 'Stop' : 'Train Local Model');
                    trainLocalModelBtn.style.background = trainingActive ? 'var(--red)' : '#0f766e';
                }
                if (refreshLocalModelStatusBtn) {
                    refreshLocalModelStatusBtn.disabled = isRunning;
                }
            }

            function setPredictionActionState(state = 'idle') {
                if (state === 'starting') {
                    predictChurnBtn.textContent = 'Starting...';
                    predictChurnBtn.style.background = 'var(--primary-color)';
                    predictChurnBtn.disabled = true;
                    datasetSelect.disabled = true;
                    predictionAudienceScopeSelect.disabled = true;
                    predictionModeSelect.disabled = true;
                    setPredictionModelTrainingActionState(Boolean(predictionModelTrainingRequest));
                    return;
                }

                if (state === 'running') {
                    predictChurnBtn.textContent = 'Stop';
                    predictChurnBtn.style.background = 'var(--red)';
                    predictChurnBtn.disabled = false;
                    datasetSelect.disabled = true;
                    predictionAudienceScopeSelect.disabled = true;
                    predictionModeSelect.disabled = true;
                    setPredictionModelTrainingActionState(Boolean(predictionModelTrainingRequest));
                    return;
                }

                if (state === 'stopping') {
                    predictChurnBtn.textContent = 'Stopping...';
                    predictChurnBtn.style.background = 'var(--subtle-text)';
                    predictChurnBtn.disabled = true;
                    datasetSelect.disabled = true;
                    predictionAudienceScopeSelect.disabled = true;
                    predictionModeSelect.disabled = true;
                    setPredictionModelTrainingActionState(Boolean(predictionModelTrainingRequest));
                    return;
                }

                predictChurnBtn.textContent = 'Predict Churn';
                predictChurnBtn.style.background = '';
                predictChurnBtn.disabled = false;
                datasetSelect.disabled = false;
                predictionAudienceScopeSelect.disabled = false;
                predictionModeSelect.disabled = false;
                setPredictionModelTrainingActionState(Boolean(predictionModelTrainingRequest));
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

            function getPredictionAudienceProgressLabel(details = {}) {
                const audienceScope = String(details.audience_scope || '').trim().toLowerCase();
                const sourceName = String(details.source_name || '').trim();
                if (audienceScope === 'source' && sourceName) {
                    return ` - Source ${sourceName}`;
                }
                return '';
            }

            function getPredictionReusePromptMessage(completedJob, selectedPredictionMode) {
                const cachedMode = getPredictionJobMode(completedJob);
                const cachedLabel = getPredictionModeLabel(cachedMode);
                const selectedLabel = getPredictionModeLabel(selectedPredictionMode);
                if (isPredictionJobStale(completedJob)) {
                    const staleReason = getPredictionJobStaleReason(completedJob);
                    const staleSuffix = staleReason ? ` ${staleReason}` : ' Newer imports changed the merged player history.';
                    return `${cachedLabel} results for this selection are finished but stale.${staleSuffix} Select OK to rerun with ${selectedLabel}, or Cancel to load the cached stale results.`;
                }
                if (cachedMode === String(selectedPredictionMode || '').toLowerCase()) {
                    return `${cachedLabel} results for this selection are already finished and cached. Select OK to rerun with ${selectedLabel}, or Cancel to load the cached results.`;
                }
                return `${cachedLabel} results for this selection are already finished and cached. Select OK to rerun with ${selectedLabel}, or Cancel to load the cached ${cachedLabel} results.`;
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
                        ? `Prediction job: ${rawStatus} - ${stopReason}`
                        : `Prediction job: ${rawStatus}`;
                }
                const executionLabel = getPredictionExecutionLabel(details)
                    .replace(/\b\w/g, (c) => c.toUpperCase());
                const localModelLabel = getPredictionEffectiveLocalModelLabel(details, normalizedStatus);
                const modelSuffix = localModelLabel ? ` - Model ${localModelLabel}` : '';
                const audienceSuffix = getPredictionAudienceProgressLabel(details);
                const staleSuffix = isPredictionJobStale(job) ? ' - Stale' : '';
                return `Prediction job: ${rawStatus} - ${executionLabel}${audienceSuffix} - ${processed}/${total} users (${Math.round(Number(pct || 0))}%)${modelSuffix}${staleSuffix}`;
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
                        label += ` - ${formatCount(processedManifests)}/${formatCount(totalManifests)} shards`;
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
                            : `${formatCount(current)} events - pages of ${formatCount(pageSize)}`;
                    }
                    if (totalManifests > 0) {
                        label += ` - ${formatCount(totalManifests)} shard${totalManifests === 1 ? '' : 's'}`;
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
                    const previousSelection = datasetSelect.value;
                    const previousAudienceScope = getSelectedPredictionAudienceScope();
                    const persistedActiveJob = await getPersistedActivePredictionJob();
                    const persistedSelection = readPersistedActivePredictionJob() || {};
                    datasetSelect.innerHTML = ''; // Clear loading message
                    const activePredictionJob = persistedActiveJob || cachedPredictionJobs.find((job) => isPredictionJobActive(job)) || null;
                    const selectedAudienceScope = String(
                        (activePredictionJob && getPredictionAudienceScope(activePredictionJob))
                        || persistedSelection.audience_scope
                        || previousAudienceScope
                        || 'source'
                    ).toLowerCase();
                    if (predictionAudienceScopeSelect) {
                        predictionAudienceScopeSelect.value = selectedAudienceScope;
                    }
                    renderPredictionAudienceSelectorMeta();
                    const audienceOptions = getPredictionAudienceOptions(selectedAudienceScope);

                    if (audienceOptions.length > 0) {
                        syncPredictionModeSelection();
                        audienceOptions.forEach((optionItem) => {
                            const option = document.createElement('option');
                            option.value = optionItem.value;
                            option.textContent = optionItem.label;
                            datasetSelect.appendChild(option);
                        });
                        const activeAudienceKey = activePredictionJob ? getPredictionAudienceKey(activePredictionJob) : String(persistedSelection.audience_key || '');
                        const selectedAudienceKey = audienceOptions.some((optionItem) => optionItem.value === activeAudienceKey)
                            ? activeAudienceKey
                            : (audienceOptions.some((optionItem) => optionItem.value === previousSelection) ? previousSelection : audienceOptions[0].value);
                        datasetSelect.value = selectedAudienceKey;
                        const selectedActiveJob = [
                            persistedActiveJob,
                            activePredictionJob,
                            getLatestPredictionJob(selectedAudienceKey, false, selectedAudienceScope),
                        ]
                            .find((job) => job && isPredictionJobActive(job) && predictionJobMatchesSelection(job, selectedAudienceKey, selectedAudienceScope))
                            || null;

                        if (selectedActiveJob) {
                            activePredictionJobId = selectedActiveJob.id;
                            syncPredictionModeSelection(selectedActiveJob);
                            persistActivePredictionJob(activePredictionJobId, selectedAudienceKey, selectedAudienceScope);
                            predictionStopRequested = String(selectedActiveJob.status || '').toLowerCase() === 'stopping';
                            await syncPredictionRows(activePredictionJobId, selectedAudienceKey, selectedAudienceScope);
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
                        datasetSelect.innerHTML = `<option>No ready ${selectedAudienceScope === 'source' ? 'sources' : 'imports'} available</option>`;
                        activePredictionJobId = null;
                        clearPersistedActivePredictionJob();
                        clearBaselinePredictionRows();
                        renderPredictionModelReadiness();
                        setPredictionActionState('idle');
                        predictChurnBtn.disabled = true;
                        datasetSelect.disabled = true;
                        predictionAudienceScopeSelect.disabled = false;
                        predictionModeSelect.disabled = true;
                        pushAudienceBtn.disabled = true;
                        setCampaignExportStatus('');
                    }
                } catch (error) {
                    if (isWorkspaceContextError(error)) {
                        clearBaselinePredictionRows();
                        datasetSelect.innerHTML = '<option>Finish workspace setup first</option>';
                        renderPredictionModelReadiness();
                        renderChurnTable('Finish workspace setup to load prediction audiences.');
                        renderPredictionProgress({});
                        setPredictionActionState('idle');
                        predictChurnBtn.disabled = true;
                        datasetSelect.disabled = true;
                        predictionAudienceScopeSelect.disabled = false;
                        predictionModeSelect.disabled = true;
                        pushAudienceBtn.disabled = true;
                        setCampaignExportStatus(getWorkspaceResolutionMessage(error.payload || authSessionState));
                        return;
                    }
                    clearBaselinePredictionRows();
                    datasetSelect.innerHTML = `<option>Error loading prediction audiences</option>`;
                    renderPredictionModelReadiness();
                    setPredictionActionState('idle');
                    predictChurnBtn.disabled = true;
                    datasetSelect.disabled = true;
                    predictionAudienceScopeSelect.disabled = false;
                    predictionModeSelect.disabled = true;
                    pushAudienceBtn.disabled = true;
                    console.error('Error loading prediction audiences for Operator Hub:', error);
                }
            }

            async function refreshLocalModelTrainingStatus() {
                try {
                    await refreshPredictionModelReadinessState();
                    renderPredictionModelReadiness();
                    setInlineStatus(
                        predictionModelTrainingStatus,
                        'Refreshed local model status.',
                    );
                } catch (error) {
                    setInlineStatus(
                        predictionModelTrainingStatus,
                        error.message || 'Failed to refresh local model status.',
                        true,
                    );
                }
            }

            async function startLocalPredictionModelTraining() {
                if (predictionModelTrainingRequest) {
                    return predictionModelTrainingRequest;
                }

                const minRows = Number(getPredictionModelReadiness().min_rows_required || 12);
                const referenceTime = new Date().toISOString();
                setInlineStatus(
                    predictionModelTrainingStatus,
                    `Training local model with minimum ${minRows} labeled rows...`,
                );
                cachedPredictionModelTrainingStatus = {
                    ...getPredictionModelTrainingStatus(),
                    status: 'running',
                    stage: 'building_dataset',
                    reference_time: referenceTime,
                    started_at: referenceTime,
                    min_rows_required: minRows,
                    stop_requested: false,
                };
                renderPredictionModelReadiness();
                setPredictionModelTrainingActionState(true);
                syncPredictionModelTrainingPolling();

                predictionModelTrainingRequest = apiRequest('/predictions/models/train/start', {
                    method: 'POST',
                    body: {
                        reference_time: referenceTime,
                        min_rows: minRows,
                    },
                });

                try {
                    const payload = await predictionModelTrainingRequest;
                    applyPredictionModelTrainingPayload(payload);
                    await refreshPredictionModelReadinessState().catch(() => null);
                    renderPredictionModelReadiness();
                    setInlineStatus(
                        predictionModelTrainingStatus,
                        'Local model training started.',
                    );
                    return payload;
                } catch (error) {
                    await refreshPredictionModelReadinessState().catch(() => null);
                    renderPredictionModelReadiness();
                    setInlineStatus(
                        predictionModelTrainingStatus,
                        error.message || 'Local model training failed.',
                        true,
                    );
                    throw error;
                } finally {
                    predictionModelTrainingRequest = null;
                    setPredictionModelTrainingActionState(false);
                    syncPredictionModelTrainingPolling();
                }
            }

            async function stopLocalPredictionModelTraining() {
                if (predictionModelTrainingRequest) {
                    return predictionModelTrainingRequest;
                }

                setInlineStatus(
                    predictionModelTrainingStatus,
                    'Stopping local model training...',
                );
                cachedPredictionModelTrainingStatus = {
                    ...getPredictionModelTrainingStatus(),
                    status: 'stopping',
                    stop_requested: true,
                    stop_requested_at: new Date().toISOString(),
                };
                renderPredictionModelReadiness();
                setPredictionModelTrainingActionState(true);
                syncPredictionModelTrainingPolling();

                predictionModelTrainingRequest = apiRequest('/predictions/models/train/stop', {
                    method: 'POST',
                });

                try {
                    const payload = await predictionModelTrainingRequest;
                    applyPredictionModelTrainingPayload(payload);
                    await refreshPredictionModelReadinessState().catch(() => null);
                    renderPredictionModelReadiness();
                    setInlineStatus(
                        predictionModelTrainingStatus,
                        'Stop requested for local model training.',
                    );
                    return payload;
                } catch (error) {
                    await refreshPredictionModelReadinessState().catch(() => null);
                    renderPredictionModelReadiness();
                    setInlineStatus(
                        predictionModelTrainingStatus,
                        error.message || 'Failed to stop local model training.',
                        true,
                    );
                    throw error;
                } finally {
                    predictionModelTrainingRequest = null;
                    setPredictionModelTrainingActionState(false);
                    syncPredictionModelTrainingPolling();
                }
            }

            async function trainLocalPredictionModel() {
                if (isPredictionModelTrainingActive()) {
                    if (isPredictionModelTrainingStopping()) {
                        return null;
                    }
                    return stopLocalPredictionModelTraining();
                }
                return startLocalPredictionModelTraining();
            }

            async function syncPredictionRows(jobId, audienceKey = '', audienceScope = getSelectedPredictionAudienceScope()) {
                if (!jobId) {
                    return;
                }
                const normalizedAudienceKey = String(audienceKey || getSelectedPredictionAudienceKey() || '').trim();
                const normalizedAudienceScope = String(audienceScope || '').toLowerCase() || 'source';
                const [activeRows, existingRows] = await Promise.all([
                    fetchPredictionRows(jobId),
                    loadBaselinePredictionRows(normalizedAudienceKey, jobId, getSelectedPredictionMode(), normalizedAudienceScope),
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

            async function fetchAndRenderPredictions(audienceKey, forceRecalculate = false, audienceScope = getSelectedPredictionAudienceScope()) {
                if (!audienceKey) {
                    clearBaselinePredictionRows();
                    allChurnPredictionPlayers = [];
                    renderChurnTable();
                    return;
                }

                predictionStopRequested = false;
                const selectedPredictionMode = getSelectedPredictionMode();
                const normalizedAudienceScope = String(audienceScope || '').toLowerCase() || 'source';
                currentPage = 1;
                let shouldForceRecalculate = Boolean(forceRecalculate);

                let predictionJob = null;
                try {
                    await ensureHealthState();
                    await Promise.all([refreshConnectorsState(), refreshPredictionJobsState(), refreshPredictionModelReadinessState()]);
                    renderPredictionModelReadiness();
                    predictionJob = !shouldForceRecalculate ? getLatestActivePredictionJob(audienceKey, selectedPredictionMode, normalizedAudienceScope) : null;
                    if (!predictionJob && !shouldForceRecalculate) {
                        const completedJob = (
                            getLatestCompletedPredictionJob(audienceKey, null, selectedPredictionMode, normalizedAudienceScope)
                            || getLatestCompletedPredictionJob(audienceKey, null, '', normalizedAudienceScope)
                        );
                        if (completedJob) {
                            const shouldRerun = window.confirm(
                                getPredictionReusePromptMessage(completedJob, selectedPredictionMode),
                            );
                            if (!shouldRerun) {
                                await renderCompletedPredictionJob(completedJob, audienceKey, normalizedAudienceScope);
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
                            audienceKey,
                            predictionJob ? predictionJob.id : null,
                            selectedPredictionMode,
                            normalizedAudienceScope,
                        );
                    allChurnPredictionPlayers = [...existingRows];
                    renderChurnTable(existingRows.length > 0 ? undefined : 'Waiting for prediction results...');

                    if (!predictionJob) {
                        predictionJob = await createPredictionRecord(audienceKey, selectedPredictionMode, normalizedAudienceScope);
                    }
                    activePredictionJobId = predictionJob.id;
                    syncPredictionModeSelection(predictionJob);
                    persistActivePredictionJob(activePredictionJobId, audienceKey, normalizedAudienceScope);
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
                            persistActivePredictionJob(predictionJob.id, audienceKey, normalizedAudienceScope);
                        }
                        await syncPredictionRows(activePredictionJobId, audienceKey, normalizedAudienceScope);
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
                    await syncPredictionRows(activePredictionJobId, audienceKey, normalizedAudienceScope);
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
                    baselinePredictionAudienceKey = String(audienceKey);
                    baselinePredictionAudienceScope = normalizedAudienceScope;
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
                const audienceKey = getSelectedPredictionAudienceKey();
                const audienceScope = getSelectedPredictionAudienceScope();
                if (!audienceKey) {
                    setCampaignExportStatus(`Select a ready ${audienceScope === 'source' ? 'source' : 'import'} first.`, true);
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
                        baselinePredictionAudienceScope === audienceScope
                        && baselinePredictionAudienceKey === String(audienceKey)
                        && baselinePredictionJobId
                        && cachedPredictionJobs.find((job) => String(job.id || '') === String(baselinePredictionJobId))
                    ) || getLatestPredictionJobForMode(audienceKey, true, getSelectedPredictionMode(), audienceScope)
                        || getLatestPredictionJob(audienceKey, true, audienceScope);
                    if (!predictionJob) {
                        throw new Error('Run churn prediction for this selection before exporting an audience.');
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

            function renderChurnTable(emptyMessage = 'No players found in this selection.') {
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

            predictionAudienceScopeSelect.addEventListener('change', async () => {
                clearBaselinePredictionRows();
                allChurnPredictionPlayers = [];
                renderPredictionAudienceSelectorMeta();
                renderChurnTable();
                predictionStopRequested = false;
                predictionProgressInfo.textContent = '';
                renderPredictionModelReadiness();
                setPredictionActionState('idle');
                setCampaignExportStatus('');
                await loadReadyImportsForOperatorHub();
            });

            predictionModeSelect.addEventListener('change', () => {
                renderPredictionModelReadiness();
            });

            trainLocalModelBtn?.addEventListener('click', () => {
                trainLocalPredictionModel().catch(() => null);
            });

            refreshLocalModelStatusBtn?.addEventListener('click', () => {
                refreshLocalModelTrainingStatus().catch(() => null);
            });

            predictChurnBtn.addEventListener('click', async () => {
                if (activePredictionJobId) {
                    requestPredictionStop();
                    return;
                }
                fetchAndRenderPredictions(getSelectedPredictionAudienceKey(), false, getSelectedPredictionAudienceScope());
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
            const importSourceStatus = document.getElementById('import-source-status');
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
                const setImportSourceFormVisible = (visible) => {
                    sourceGroup.style.display = visible ? 'block' : 'none';
                    startDateGroup.style.display = visible ? 'block' : 'none';
                    endDateGroup.style.display = visible ? 'block' : 'none';
                    importDataBtn.style.display = visible ? 'inline-block' : 'none';
                };
                const ensureConfigMessage = (message = '') => {
                    let messageEl = importCard.querySelector('.config-message');
                    if (!message) {
                        messageEl?.remove();
                        return;
                    }
                    if (!messageEl) {
                        messageEl = document.createElement('p');
                        messageEl.className = 'config-message';
                        importCard.insertBefore(messageEl, sourceGroup);
                    }
                    messageEl.textContent = message;
                };

                if (shouldBlockProtectedAppData()) {
                    setImportSourceFormVisible(false);
                    ensureConfigMessage('');
                    setInlineStatus(
                        importSourceStatus,
                        accessToken ? 'Finish workspace setup to load import sources.' : '',
                        false,
                    );
                    return;
                }

                try {
                    await refreshConnectorsState();
                    const sources = getConfiguredSourcesFromState();
                    const previousSelection = sourceSelect.value;

                    if (!sources || sources.length === 0) {
                        setImportSourceFormVisible(false);
                        ensureConfigMessage('Please configure a data source in the Connectors section first.');
                        setInlineStatus(importSourceStatus, '');
                    } else {
                        sourceSelect.innerHTML = ''; // Clear existing options
                        sources.forEach(source => {
                            const option = document.createElement('option');
                            option.value = source.id;
                            option.textContent = source.name;
                            sourceSelect.appendChild(option);
                        });
                        if (sources.some((source) => source.id === previousSelection)) {
                            sourceSelect.value = previousSelection;
                        }
                        setImportSourceFormVisible(true);
                        ensureConfigMessage('');
                        setInlineStatus(importSourceStatus, '');
                    }
                } catch (error) {
                    setImportSourceFormVisible(false);
                    ensureConfigMessage('');
                    if (isWorkspaceContextError(error)) {
                        setInlineStatus(importSourceStatus, getWorkspaceResolutionMessage(error.payload || authSessionState));
                        return;
                    }
                    setInlineStatus(importSourceStatus, error.message || 'Failed to load import sources.', true);
                }
            }

            let countdownInterval = null;
            function shouldPollImportJobs(imports = []) {
                return imports.some((job) => ['queued', 'running', 'stopping'].includes(String(job.raw_status || '').toLowerCase()));
            }

            function syncImportListPolling(imports = []) {
                const shouldPoll = shouldPollImportJobs(imports);
                if (shouldPoll && !importListInterval) {
                    importListInterval = setInterval(loadImportedDataList, 3000);
                    return;
                }
                if (!shouldPoll && importListInterval) {
                    clearInterval(importListInterval);
                    importListInterval = null;
                }
            }

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

            async function retryBusyRequest(callback, attempts = 2, waitMs = 750) {
                let lastError = null;
                for (let attempt = 0; attempt < attempts; attempt += 1) {
                    try {
                        return await callback();
                    } catch (error) {
                        lastError = error;
                        if (Number(error.status || 0) !== 423 || attempt === attempts - 1) {
                            throw error;
                        }
                        await new Promise((resolve) => setTimeout(resolve, waitMs));
                    }
                }
                throw lastError || new Error('Request failed.');
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
                        const payload = await retryBusyRequest(() => apiRequest(`/imports/${encodeURIComponent(jobId)}/manifests`));
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
                        const payload = await retryBusyRequest(() => apiRequest(`/imports/${encodeURIComponent(jobId)}/quality`));
                        renderJsonOutput(importDetailOutput, payload, 'Import quality unavailable.');
                    } else {
                        const payload = await retryBusyRequest(() => apiRequest(`/imports/${encodeURIComponent(jobId)}/operations`));
                        renderJsonOutput(importDetailOutput, payload, 'Import operations unavailable.');
                    }
                    if (!silent) {
                        setInlineStatus(importDetailStatus, `Loaded import ${view} for ${jobId}.`);
                    }
                } catch (error) {
                    const detailMessage = Number(error.status || 0) === 423
                        ? 'The control plane is busy after restart. Retry in a moment.'
                        : (error.message || 'Failed to load import detail.');
                    renderJsonOutput(importDetailOutput, { error: error.message }, 'Import detail unavailable.');
                    if (view === 'manifests') {
                        renderSimpleTable(importManifestsList, [], [], 'Manifest detail unavailable.');
                    }
                    setInlineStatus(importDetailStatus, detailMessage, true);
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
                if (shouldBlockProtectedAppData()) {
                    syncImportListPolling([]);
                    if (!importListContainer.innerHTML.trim()) {
                        importListContainer.innerHTML = '<p>Finish workspace setup to load imports.</p>';
                    }
                    setInlineStatus(importListStatus, accessToken ? 'Finish workspace setup to load imports.' : '');
                    return;
                }
                // Show loading message only if the container is empty initially
                if (!importListContainer.innerHTML.trim()) {
                    importListContainer.innerHTML = '<p>Loading...</p>';
                }

                try {
                    const imports = await refreshImportsState();

                    if (!imports || imports.length === 0) {
                        syncImportListPolling([]);
                        if (countdownInterval) {
                            clearInterval(countdownInterval);
                            countdownInterval = null;
                        }
                        importListContainer.innerHTML = '<p>No data has been imported yet.</p>';
                        populateImportDetailSelect([]);
                        setInlineStatus(importListStatus, 'No imports found.');
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
                        renderJsonOutput(importDetailOutput, null, 'Select a view to load import diagnostics on demand.');
                        renderSimpleTable(importManifestsList, [], [], 'Manifest detail loads on demand.');
                        setInlineStatus(importDetailStatus, 'Import diagnostics now load on demand to keep page startup fast.');
                    }
                    syncImportListPolling(imports);
                    setInlineStatus(
                        importListStatus,
                        shouldPollImportJobs(imports)
                            ? `Loaded ${imports.length} import(s). Active imports will refresh automatically.`
                            : `Loaded ${imports.length} import(s). Select a job and click Load Operations, Load Quality, or Load Manifests when needed.`,
                    );

                } catch (error) {
                    if (isWorkspaceContextError(error)) {
                        syncImportListPolling([]);
                        importListContainer.innerHTML = '<p>Finish workspace setup to load imports.</p>';
                        setInlineStatus(importListStatus, getWorkspaceResolutionMessage(error.payload || authSessionState));
                        return;
                    }
                    syncImportListPolling([]);
                    importListContainer.innerHTML = `<p style="color: var(--red);">${error.message}</p>`;
                    setInlineStatus(importListStatus, error.message || 'Failed to load imports.', true);
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
                    if (mockStorageEnabled && backendMode === 'mock') {
                        await Promise.all([
                            refreshConnectorsState(),
                            refreshImportsState(),
                            refreshPredictionJobsState(),
                            refreshExportJobsState(),
                        ]);
                        allActionHistoryItems = buildActionHistoryItems();
                        actionHistoryCurrentPage = 1;
                        renderActionHistoryTable();
                        setInlineStatus(
                            actionHistoryStatus,
                            allActionHistoryItems.length
                                ? `Loaded ${allActionHistoryItems.length} local mock action(s).`
                                : 'No recorded actions yet.',
                        );
                        return;
                    }
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
                    if (getActiveTenantId()) query.set('tenant_id', getActiveTenantId());
                    if (getActiveProjectId()) query.set('project_id', getActiveProjectId());
                    const payload = await apiRequest(`/audit/actions?${query.toString()}`);
                    allActionHistoryItems = (payload.items || []).map((item) => ({
                        timestamp: item.created_at,
                        summary: `${item.action_type} - ${item.resource_type}${item.resource_id ? `:${item.resource_id}` : ''}`,
                        status: item.high_risk ? 'high_risk' : 'recorded',
                        details: JSON.stringify(item.payload || {}),
                    }));
                    actionHistoryCurrentPage = 1;
                    renderActionHistoryTable();
                    setInlineStatus(actionHistoryStatus, `Loaded ${payload.summary?.returned || 0} audit record(s).`);
                } catch (error) {
                    allActionHistoryItems = [];
                    if (isWorkspaceContextError(error)) {
                        actionHistoryResults.innerHTML = '<tr><td colspan="4" style="text-align: center;">Finish workspace setup to load audit history.</td></tr>';
                        if (actionHistoryPaginationControls) {
                            actionHistoryPaginationControls.innerHTML = '';
                        }
                        setInlineStatus(actionHistoryStatus, getWorkspaceResolutionMessage(error.payload || authSessionState));
                        return;
                    }
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
                    if (isWorkspaceContextError(error)) {
                        serviceStatusListDiv.innerHTML = '<p>Finish workspace setup to load health state.</p>';
                        renderJsonOutput(serviceHealthOutput, null, 'Finish workspace setup to load health payload.');
                        renderSimpleTable(serviceAlertsList, [], [], 'Finish workspace setup to load health alerts.');
                        renderSimpleTable(serviceSchedulerList, [], [], 'Finish workspace setup to load scheduler state.');
                        setInlineStatus(serviceHealthStatus, getWorkspaceResolutionMessage(error.payload || authSessionState));
                        return;
                    }
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
                    if (isWorkspaceContextError(error)) {
                        templatesList.innerHTML = '<div class="list-empty">Finish workspace setup to load templates.</div>';
                        templatesSelectedLabel.textContent = 'No template selected';
                        renderJsonOutput(templateDetailOutput, null, 'Finish workspace setup to load template detail.');
                        return;
                    }
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
                if (shouldBlockProtectedAppData()) {
                    dataSandboxMappingConnectorSelect.innerHTML = '<option value="">Finish workspace setup first</option>';
                    dataSandboxAwaitingJobSelect.innerHTML = '<option value="">Finish workspace setup first</option>';
                    dataSandboxLoadMappingBtn.disabled = true;
                    dataSandboxSaveMappingBtn.disabled = true;
                    dataSandboxPreviewMappingBtn.disabled = true;
                    dataSandboxCoverageBtn.disabled = true;
                    dataSandboxProcessMappingBtn.disabled = true;
                    setDataSandboxMappingStatus(accessToken ? 'Finish workspace setup to load field mapping controls.' : '');
                    return;
                }
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
                    if (isWorkspaceContextError(error)) {
                        dataSandboxMappingConnectorSelect.innerHTML = '<option value="">Finish workspace setup first</option>';
                        dataSandboxAwaitingJobSelect.innerHTML = '<option value="">Finish workspace setup first</option>';
                        dataSandboxLoadMappingBtn.disabled = true;
                        dataSandboxSaveMappingBtn.disabled = true;
                        dataSandboxPreviewMappingBtn.disabled = true;
                        dataSandboxCoverageBtn.disabled = true;
                        dataSandboxProcessMappingBtn.disabled = true;
                        setDataSandboxMappingStatus(getWorkspaceResolutionMessage(error.payload || authSessionState));
                        return;
                    }
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
                    if (isWorkspaceContextError(error)) {
                        dataSandboxContentDiv.innerHTML = '<p>Finish workspace setup to load the data glance.</p>';
                        return;
                    }
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
                    if (isWorkspaceContextError(error)) {
                        setInlineStatus(audienceCreateStatus, getWorkspaceResolutionMessage(error.payload || authSessionState));
                        renderAudienceCohorts([]);
                        renderSavedQueries([]);
                        await loadAudienceCohortDetails(null);
                        return;
                    }
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
                    if (isWorkspaceContextError(error)) {
                        setInlineStatus(workflowCreateStatus, getWorkspaceResolutionMessage(error.payload || authSessionState));
                        renderWorkflowList([]);
                        populateWorkflowCohortSelect([]);
                        populateExportJobSelect([]);
                        await loadWorkflowDetail(null);
                        return;
                    }
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
                    if (isWorkspaceContextError(error)) {
                        fillExperimentForm({});
                        renderJsonOutput(experimentSummaryOutput, null, 'Finish workspace setup to load experiment summary.');
                        renderJsonOutput(experimentIntegrityOutput, null, 'Finish workspace setup to load experiment integrity.');
                        renderSimpleTable(experimentExposuresList, [], [], 'Finish workspace setup to load experiment exposures.');
                        renderSimpleTable(experimentOutcomesList, [], [], 'Finish workspace setup to load experiment outcomes.');
                        setInlineStatus(experimentStatus, getWorkspaceResolutionMessage(error.payload || authSessionState));
                        return;
                    }
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
                    if (isWorkspaceContextError(error)) {
                        renderCopilotMetaList(copilotAnomaliesList, [], 'Anomaly');
                        renderCopilotMetaList(copilotReportsList, [], 'Report');
                        renderJsonOutput(copilotQueryLogOutput, null, 'Finish workspace setup to load Copilot metadata.');
                        return;
                    }
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

            function setApplicationStartupStatus(message, state = 'neutral') {
                if (statusTextSpan) {
                    statusTextSpan.textContent = message;
                }
                if (workspaceStartupStatus) {
                    workspaceStartupStatus.textContent = message;
                    workspaceStartupStatus.classList.toggle('is-ok', state === 'ok');
                    workspaceStartupStatus.classList.toggle('is-error', state === 'error');
                }
            }

            async function checkBackendStatus() {
                try {
                    await ensureHealthState(true);
                    
                    healthStatusDiv.classList.add('connected');
                    setApplicationStartupStatus(`Application start completed (${backendMode})`, 'ok');
                } catch (error) {
                    healthStatusDiv.classList.remove('connected');
                    setApplicationStartupStatus('Application start failed', 'error');
                    console.error('Health check failed:', error);
                }
            }

            async function initializeAuthSession() {
                captureWorkspaceHintsFromUrl();
                await loadOidcConfig();
                syncAuthModeUi();
                try {
                    await handleOidcRedirect();
                    await hydrateAuthSession({ syncBrowserPath: !isGatewayRootPath() });
                } catch (error) {
                    const errorMessage = handleGoogleSessionFailure(error, 'Google session initialization failed.');
                    setWorkspaceTextStatus(workspaceLoginStatus, errorMessage, true);
                }
                syncWorkspaceOverlayFromSession();
                rootGatewayBootPending = false;
                syncWorkspaceBootClass();
            }

            // Check status on page load and then every 30 seconds.
            initializeAuthSession().finally(() => {
                checkBackendStatus();
                setInterval(checkBackendStatus, HEALTH_CHECK_INTERVAL_MS);
                activateModule('data-core');
            });

            // Theme mode logic
            const themeModeButtons = Array.from(document.querySelectorAll('.theme-mode-button'));
            const systemThemeQuery = window.matchMedia ? window.matchMedia('(prefers-color-scheme: dark)') : null;
            const storedThemeMode = localStorage.getItem('theme');

            function normalizeThemeMode(themeMode) {
                if (themeMode === 'dark-theme') {
                    return 'dark';
                }
                if (themeMode === 'light-theme') {
                    return 'light';
                }
                if (themeMode === 'light' || themeMode === 'dark' || themeMode === 'system') {
                    return themeMode;
                }
                return 'system';
            }

            function updateThemeModeButtons(themeMode) {
                themeModeButtons.forEach((button) => {
                    const isActive = button.dataset.themeMode === themeMode;
                    button.classList.toggle('active', isActive);
                    button.setAttribute('aria-pressed', isActive ? 'true' : 'false');
                });
            }

            function resolveEffectiveDarkMode(themeMode) {
                if (themeMode === 'dark') {
                    return true;
                }
                if (themeMode === 'light') {
                    return false;
                }
                return Boolean(systemThemeQuery && systemThemeQuery.matches);
            }

            function applyThemeMode(themeMode) {
                const normalizedThemeMode = normalizeThemeMode(themeMode);
                document.body.classList.toggle('dark-theme', resolveEffectiveDarkMode(normalizedThemeMode));
                document.body.dataset.themeMode = normalizedThemeMode;
                updateThemeModeButtons(normalizedThemeMode);
                localStorage.setItem('theme', normalizedThemeMode);
            }

            applyThemeMode(storedThemeMode);

            themeModeButtons.forEach((button) => {
                button.addEventListener('click', () => {
                    applyThemeMode(button.dataset.themeMode);
                });
            });

            if (systemThemeQuery) {
                const handleSystemThemeChange = () => {
                    if (normalizeThemeMode(localStorage.getItem('theme')) === 'system') {
                        applyThemeMode('system');
                    }
                };

                if (typeof systemThemeQuery.addEventListener === 'function') {
                    systemThemeQuery.addEventListener('change', handleSystemThemeChange);
                } else if (typeof systemThemeQuery.addListener === 'function') {
                    systemThemeQuery.addListener(handleSystemThemeChange);
                }
            }
}

if (typeof document !== 'undefined' && document.getElementById('sidebar-nav')) {
    initializeOperatorConsole();
}
