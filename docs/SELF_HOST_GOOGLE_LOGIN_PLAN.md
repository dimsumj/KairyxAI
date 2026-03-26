# Self-Hosted Google Login Plan

## Purpose

This document defines how KairyxAI should support Google login for self-hosted deployments, how internal staging and test environments should validate that flow, and what repository changes are required to move the Google OAuth code exchange to the backend.

The intent is to keep the current user-facing product flow:

- user reaches the full-screen Google login gate
- user signs in with Google
- KairyxAI resolves organization-space and project access
- first-time users go into onboarding
- returning users land in their existing workspace

while making the auth model safe for customer-managed domains and production Google OAuth rules.

The intended browser result after login remains:

- the Google callback uses one fixed application callback URL
- the bare base URL `https://<host>/` is only the gateway page
- once Kairyx resolves the active organization, the browser URL becomes `https://<host>/<organization_id>`
- Kairyx API traffic continues on `https://<host>/<organization_id>/v1/...`

## Decision Summary

KairyxAI should support self-hosted Google login with a customer-managed OAuth client per deployment.

The recommended model is:

- one Google Cloud project and OAuth client owned by the customer
- one client per environment
  - production
  - staging
  - optional local or preview
- KairyxAI remains the authorization system
  - Google proves identity
  - KairyxAI stores organization-space membership, project membership, org role, and project role
- the Vercel adapter remains a separate demo surface only
  - `api/index.py` sets `KAIRYX_PLATFORM_SURFACE=vercel_demo`
  - runtime SQLite fallback and database-backed mock state are fenced to that demo adapter
  - self-hosted production should not depend on those demo-only fallbacks

KairyxAI should not rely on a single shared Google OAuth client across customer-hosted domains.

## Why A Shared Google Client Is Not Enough

Google requires the `redirect_uri` in the authorization request to exactly match a redirect URI registered on the OAuth client. Wildcards are not supported. Google also expects production OAuth clients to be tied to real owned domains and production-approved app configuration.

That creates two practical constraints:

- a shared Kairyx-owned OAuth client does not scale across arbitrary customer domains such as `https://analytics.customer-a.com` and `https://engagement.customer-b.net`
- production and staging should not share the same OAuth client because the allowed origins and redirect URIs are environment-specific

For self-hosting, the clean pattern is customer-managed Google OAuth settings injected into Kairyx at deploy time.

## Current Repository Gap

The current repository is Google-first in UI copy and env templates, but it is not yet self-host-ready from an auth architecture standpoint.

Current behavior:

- the frontend starts Google login from [frontend/assets/operator-console.js](frontend/assets/operator-console.js)
- the browser exchanges the authorization code directly against `OIDC_TOKEN_URL`
- the frontend stores the returned Google `id_token` as the API bearer token
- the backend validator in [backend/services/app/core/auth.py](backend/services/app/core/auth.py) expects a JWT-like bearer token with `iss`, `aud`, and `sub`
- Google-friendly env aliases are also accepted in the repo today:
  - `GOOGLE_OIDC_CLIENT_ID`
  - `GOOGLE_OIDC_HOSTED_DOMAIN`

That is not a safe or reliable long-term Google production model for self-hosting:

- Google access tokens are for Google APIs, not for Kairyx API auth
- the token exchange should not stay fully client-side for the self-host production path
- Kairyx should not treat a Google access token as its own application session token

## Target Auth Architecture

### Core Principle

Google authenticates the user.
KairyxAI issues or manages the application session used against Kairyx APIs.

### Target Flow

1. User opens Kairyx.
2. If no active Kairyx session exists, the full-screen Google login gate is shown.
3. User clicks `Continue with Google`.
4. Kairyx backend starts the Google auth flow and stores temporary login state.
5. Google redirects back to a Kairyx backend callback endpoint.
6. Kairyx backend exchanges the authorization code with Google.
7. Kairyx backend verifies the returned Google `id_token`.
8. Kairyx backend creates a Kairyx session token.
9. Frontend receives only the Kairyx session token or a short-lived bootstrap code that can be redeemed for it.
10. Frontend calls `GET /api/v1/auth/me`.
11. Kairyx routes the user to:
    - onboarding if no memberships exist
    - direct workspace entry if exactly one org and project are active
    - workspace selection if more than one org or project is available
12. After the organization is resolved, the frontend rewrites the browser URL to `https://<host>/<organization_id>` while keeping the Google callback URL fixed.

### Session Ownership

Recommended v1.1 approach:

- Google token exchange happens only on the backend
- Google `id_token` is verified only on the backend
- Kairyx mints its own short-lived session JWT for API traffic
- frontend stores the Kairyx session JWT instead of a Google token

This keeps the current `Authorization: Bearer ...` API shape without forcing the API layer to trust raw Google OAuth access tokens.

## Recommended Backend Changes

### 1. Add Server-Side Google OAuth Settings

Extend [backend/services/app/core/settings.py](backend/services/app/core/settings.py) and [backend/services/.env.example](backend/services/.env.example) with:

- `PUBLIC_BASE_URL`
- `OIDC_CLIENT_SECRET`
- `OIDC_CLIENT_SECRET_REF`
- `AUTH_SESSION_SIGNING_SECRET`
- `AUTH_SESSION_ISSUER`
- `AUTH_SESSION_AUDIENCE`
- `AUTH_SESSION_TTL_SECONDS`

Keep the existing Google-friendly aliases supported too:

- `GOOGLE_OIDC_CLIENT_ID`
- `GOOGLE_OIDC_HOSTED_DOMAIN`

Purpose:

- `PUBLIC_BASE_URL` builds the canonical Google callback URL at the base app hostname, not at an org-scoped browser path
- `OIDC_CLIENT_SECRET` is required for the backend code exchange
- `AUTH_SESSION_*` separates Kairyx session tokens from Google provider tokens

### 2. Add Auth Start Endpoint

Add an endpoint such as:

- `GET /api/v1/auth/google/start`

Behavior:

- generate `state`
- generate `nonce`
- generate PKCE `code_verifier` and `code_challenge`
- persist temporary login state server-side or in a signed short-lived cookie
- return or redirect to the Google authorize URL

Preferred behavior for the current app:

- frontend calls the start endpoint
- backend returns `{ authorize_url }`
- frontend redirects the browser

### 3. Add Backend Callback Endpoint

Add an endpoint such as:

- `GET /api/v1/auth/google/callback`

Behavior:

- validate `state`
- exchange the authorization `code` with Google at `https://oauth2.googleapis.com/token`
- verify the returned `id_token`
- extract `sub`, `email`, `email_verified`, `name`, and profile claims
- create or update the `platform_user`
- create a one-time bootstrap code or directly mint a Kairyx session token
- redirect the browser back to the frontend root

Recommended redirect target:

- `https://<host>/?auth_bootstrap_code=<one_time_code>`

The bootstrap code should be one-time and short-lived. It should not be the final API bearer.
After the frontend redeems that bootstrap code and resolves workspace access, it should rewrite the browser URL to `https://<host>/<organization_id>`.

### 4. Add Session Redeem Endpoint

Add an endpoint such as:

- `POST /api/v1/auth/session/redeem`

Behavior:

- accept `auth_bootstrap_code`
- validate that it was created by the callback flow and has not expired
- issue a Kairyx session JWT
- return the Kairyx session JWT to the frontend

This keeps Google artifacts off the frontend API session path.

### 5. Add Kairyx Session Validation

Extend [backend/services/app/core/auth.py](backend/services/app/core/auth.py) so it can validate:

- Kairyx-issued session JWTs for normal app API traffic
- optionally Google ID tokens during the transition period, if you want a staged migration

Recommended end state:

- Kairyx API trusts Kairyx session JWTs
- Google tokens are used only during login bootstrap

### 6. Keep `/api/v1/auth/me` As The Workspace Router

Keep [backend/services/app/api/routers/auth.py](backend/services/app/api/routers/auth.py) as the post-login router for:

- `needs_onboarding`
- `needs_org_selection`
- `needs_project_selection`
- accessible organizations and projects

That preserves the current product behavior after login.

### 7. Update Frontend Login Flow

Update [frontend/assets/operator-console.js](frontend/assets/operator-console.js):

- stop exchanging the Google authorization code in the browser
- start login through `GET /api/v1/auth/google/start`
- after redirect back, detect `auth_bootstrap_code`
- call `POST /api/v1/auth/session/redeem`
- store the returned Kairyx session token
- continue into `GET /api/v1/auth/me`

### 8. Preserve Local Demo Compatibility

Keep local/demo support:

- when Google settings are not configured, the app may still use legacy local/demo auth
- when Google settings are configured, Google login gate becomes the required path

This preserves current development workflows without forcing every local session through live Google OAuth.

## Self-Hosted Packaging Requirements

For self-hosted customers, ship Google login as a documented install-time option.

Customer operator checklist:

1. Create a Google Cloud project owned by the customer.
2. Configure the Google Auth platform.
3. Create a `Web application` OAuth client.
4. Register exact authorized origins and redirect URIs for the customer deployment hostname.
5. Put those values into Kairyx secrets or env variables.
6. Restart Kairyx.
7. Verify `Continue with Google` appears and login succeeds.

Kairyx installation docs should explicitly tell customers:

- do not reuse Kairyx-managed production OAuth credentials
- create a dedicated client per environment
- use the exact hostname and callback path with no wildcard assumptions

## Internal Test Environment Strategy

Use a separate Google Cloud project and separate OAuth client for the internal test environment.

### Recommended Internal Test Setup

- test hostname example: `https://staging.kairyx.ai`
- Google project: `kairyx-staging-auth`
- OAuth client type: `Web application`
- authorized JavaScript origin:
  - `https://staging.kairyx.ai`
- authorized redirect URI:
  - `https://staging.kairyx.ai/api/v1/auth/google/callback`

Browser behavior after successful login:

- Kairyx callback lands on the fixed backend callback URL above
- after session bootstrap and workspace resolution, the browser URL should become `https://staging.kairyx.ai/<organization_id>`

If only company Google Workspace accounts need access:

- use `Internal` audience

If testers include external Gmail or contractor accounts:

- use `External`
- keep the app in `Testing` until ready
- add every tester as a test user

### Internal Test Env Variables

```env
APP_ENV=prod
LEGACY_HEADER_AUTH_ENABLED=false
CORS_ALLOWED_ORIGINS=https://staging.kairyx.ai
PUBLIC_BASE_URL=https://staging.kairyx.ai

OIDC_ISSUER=https://accounts.google.com
OIDC_AUDIENCE=staging-client-id.apps.googleusercontent.com
OIDC_JWKS_URL=https://www.googleapis.com/oauth2/v3/certs
OIDC_CLIENT_ID=staging-client-id.apps.googleusercontent.com
OIDC_CLIENT_SECRET=staging-client-secret
OIDC_AUTHORIZE_URL=https://accounts.google.com/o/oauth2/v2/auth
OIDC_TOKEN_URL=https://oauth2.googleapis.com/token
OIDC_LOGOUT_URL=

AUTH_SESSION_ISSUER=https://staging.kairyx.ai
AUTH_SESSION_AUDIENCE=kairyx-operator
AUTH_SESSION_SIGNING_SECRET=replace-with-strong-random-secret
AUTH_SESSION_TTL_SECONDS=3600
```

### Internal Test Matrix

Run all of these before production rollout:

1. Logged-out user sees only the Google login gate.
2. First-time Google user with no memberships reaches onboarding.
3. Existing user with one org and one project enters directly.
4. Existing user with one org and multiple projects reaches project selection.
5. Existing user with multiple orgs reaches organization selection first.
6. Invite link redemption works through Google login.
7. Logout clears the Kairyx session and returns to the login gate.
8. Wrong redirect URI or wrong client ID fails with visible operator-facing error text.

## Production Self-Host Deployment Plan

Use a separate Google Cloud project and separate OAuth client for every customer production deployment.

### Recommended Production Setup

- production hostname example: `https://analytics.customer-a.com`
- Google project owner: the customer
- OAuth client type: `Web application`
- authorized JavaScript origin:
  - `https://analytics.customer-a.com`
- authorized redirect URI:
  - `https://analytics.customer-a.com/api/v1/auth/google/callback`

Browser behavior after successful login:

- Kairyx callback lands on the fixed backend callback URL above
- after session bootstrap and workspace resolution, the browser URL should become `https://analytics.customer-a.com/<organization_id>`

Google audience choice:

- `Internal` only when the customer wants login restricted to users inside their Google Workspace
- `External` when the customer needs broader Google-account access

For production, the customer should complete the normal Google consent-screen, branding, and domain-verification steps for their owned domain before go-live.

### Production Environment Variables

```env
APP_ENV=prod
LEGACY_HEADER_AUTH_ENABLED=false
CORS_ALLOWED_ORIGINS=https://analytics.customer-a.com
PUBLIC_BASE_URL=https://analytics.customer-a.com

OIDC_ISSUER=https://accounts.google.com
OIDC_AUDIENCE=customer-prod-client-id.apps.googleusercontent.com
OIDC_JWKS_URL=https://www.googleapis.com/oauth2/v3/certs
OIDC_CLIENT_ID=customer-prod-client-id.apps.googleusercontent.com
OIDC_CLIENT_SECRET=customer-prod-client-secret
OIDC_AUTHORIZE_URL=https://accounts.google.com/o/oauth2/v2/auth
OIDC_TOKEN_URL=https://oauth2.googleapis.com/token
OIDC_LOGOUT_URL=

AUTH_SESSION_ISSUER=https://analytics.customer-a.com
AUTH_SESSION_AUDIENCE=kairyx-operator
AUTH_SESSION_SIGNING_SECRET=replace-with-strong-random-secret
AUTH_SESSION_TTL_SECONDS=3600
```

Secret-handling expectations:

- store `OIDC_CLIENT_SECRET` and `AUTH_SESSION_SIGNING_SECRET` in a secret manager, not plain env files checked into source control
- rotate `AUTH_SESSION_SIGNING_SECRET` with a controlled maintenance plan because it invalidates active Kairyx sessions
- rotate the Google client secret according to the customer's internal security policy

### Production Customer Setup Checklist

1. Customer creates a dedicated Google Cloud project for Kairyx production auth.
2. Customer configures the Google Auth platform branding for the production app name, support email, privacy policy, and terms links.
3. Customer verifies the production domain used by the Kairyx deployment.
4. Customer creates a `Web application` OAuth client for the exact production hostname.
5. Customer registers the exact authorized JavaScript origin and backend callback URI.
6. Customer places the Google OAuth client values and Kairyx session-signing secret into deployment secrets.
7. Customer deploys or restarts Kairyx with those production values.
8. Customer verifies login, onboarding, workspace routing, and logout behavior with production-safe test accounts.

### Production Go-Live Validation

Run all of these before declaring production ready:

1. Logged-out user sees only the Google login gate.
2. Google login redirects to the exact customer-owned production domain with no redirect mismatch.
3. First-time user reaches organization-space onboarding.
4. Existing user lands directly in their expected organization and project or sees the workspace selector when appropriate.
5. Invite redemption works end to end through Google login.
6. Logout clears the Kairyx session and returns the browser to the login gate.
7. Backend logs contain login success and login failure events without leaking provider tokens or client secrets.
8. Customer can rotate the Google client secret and redeploy without schema or data changes.

### Production Rollout Pattern

Recommended rollout sequence:

1. Enable Google login in a production-like pre-prod or pilot environment first.
2. Validate a small allowlist of real customer operator accounts.
3. Keep a break-glass admin path available during rollout.
   - this may be a temporary local-only compatibility environment or a short-lived emergency admin session path
   - it should not remain the default production access path
4. After Google login is stable, disable any fallback user access paths for normal operations.
5. Record the final production callback URI, client id, and operational owner in the customer runbook.

### Production Ownership Split

Customer-owned:

- Google Cloud project
- OAuth consent-screen branding
- authorized origins and redirect URIs
- domain verification
- Google client secret lifecycle

Kairyx-owned:

- backend callback implementation
- Kairyx session issuance
- organization-space and project authorization logic
- onboarding and workspace routing
- documentation and upgrade path

## Production Self-Host Rules

For customer-hosted production environments:

- customer owns the Google OAuth project
- customer owns the OAuth consent-screen branding and domain verification
- customer owns the exact redirect URIs
- Kairyx ships documentation and env hooks, not shared production credentials

This avoids:

- `redirect_uri_mismatch`
- broken auth when customers use custom domains
- policy drift between Kairyx-managed and customer-managed branding

## Suggested Implementation Order

### Phase 1: Stabilize the Auth Model

1. Add backend settings for client secret, session signing, and public base URL.
2. Add backend Google code exchange and callback endpoints.
3. Add Kairyx session-token minting.
4. Update frontend to redeem a bootstrap code instead of calling Google token endpoint directly.

### Phase 2: Make It Self-Host Ready

1. Add self-host installation documentation.
2. Add startup validation for missing `PUBLIC_BASE_URL`, `OIDC_CLIENT_SECRET`, or session-signing settings when Google mode is enabled.
3. Add staging and self-host example env templates.
4. Add automated auth smoke coverage for:
   - first login
   - existing workspace login
   - invite redemption

### Phase 3: Harden Customer Adoption

1. Document customer-managed Google setup step by step.
2. Add explicit error copy for:
   - invalid client
   - redirect mismatch
   - unauthorized test user
3. Add audit logging for login success, login failure, callback failure, and logout.

## Acceptance Criteria

This plan is complete when:

- Kairyx no longer uses a raw Google access token as the Kairyx API bearer
- Google code exchange is backend-only
- self-hosted deployments can configure customer-owned Google OAuth credentials without code changes
- internal staging can validate first-login onboarding and returning-user routing
- the frontend always shows login first, then onboarding or workspace routing

## External References

- Google OAuth for web-server applications:
  - https://developers.google.com/identity/protocols/oauth2/web-server
- Google sign-in server-side flow:
  - https://developers.google.com/identity/sign-in/web/server-side-flow
- Google OAuth consent configuration:
  - https://developers.google.com/workspace/guides/configure-oauth-consent
- Google OAuth production policy compliance:
  - https://developers.google.com/identity/protocols/oauth2/production-readiness/policy-compliance
