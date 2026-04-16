from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from typing import Any

import jwt
from jwt import PyJWKClient
from jwt.exceptions import PyJWKClientConnectionError

from app.core.settings import Settings, get_settings


@dataclass(frozen=True)
class AuthenticatedPrincipal:
    subject: str
    email: str | None
    display_name: str | None
    platform_admin: bool
    claims: dict[str, Any]


class OIDCAuthenticator:
    def __init__(self, settings: Settings):
        self.settings = settings
        self._jwks_client = (
            PyJWKClient(
                settings.oidc_jwks_url,
                timeout=settings.oidc_jwks_timeout_seconds,
            )
            if settings.oidc_jwks_url
            else None
        )

    def authenticate_token(self, token: str) -> AuthenticatedPrincipal:
        payload = self._decode_token(token)
        subject = str(payload.get("sub") or "").strip()
        if not subject:
            raise ValueError("JWT is missing 'sub'.")
        email = str(payload.get("email") or "").strip() or None
        display_name = str(
            payload.get("name")
            or payload.get("preferred_username")
            or payload.get("email")
            or subject
        ).strip() or subject
        platform_admin = self._is_platform_admin(payload)
        return AuthenticatedPrincipal(
            subject=subject,
            email=email,
            display_name=display_name,
            platform_admin=platform_admin,
            claims=dict(payload),
        )

    def _decode_token(self, token: str) -> dict[str, Any]:
        expected_issuers = self._expected_issuers()
        primary_issuer = self.settings.oidc_issuer or None
        audience = self.settings.oidc_audience or None
        options = {
            "verify_signature": True,
            "verify_exp": True,
            "verify_aud": bool(audience),
            "verify_iss": False,
        }
        algorithms = ["HS256"] if self.settings.oidc_jwt_signing_secret else None
        try:
            if self.settings.oidc_jwt_signing_secret:
                payload = dict(
                    jwt.decode(
                        token,
                        self.settings.oidc_jwt_signing_secret,
                        algorithms=algorithms,
                        audience=audience,
                        issuer=primary_issuer,
                        options=options,
                    )
                )
            else:
                if self._jwks_client is None:
                    raise ValueError("OIDC JWKS is not configured.")
                signing_key = self._jwks_client.get_signing_key_from_jwt(token)
                payload = dict(
                    jwt.decode(
                        token,
                        signing_key.key,
                        algorithms=["RS256", "RS384", "RS512", "ES256", "ES384", "ES512"],
                        audience=audience,
                        issuer=primary_issuer,
                        options=options,
                    )
                )
        except PyJWKClientConnectionError as exc:
            raise ValueError("OIDC JWKS lookup failed. Verify outbound access to the identity provider and OIDC_JWKS_URL.") from exc
        except jwt.PyJWTError as exc:
            raise ValueError("Invalid bearer token.") from exc

        self._validate_issuer(payload, expected_issuers)
        self._validate_google_hosted_domain(payload)
        return payload

    def _expected_issuers(self) -> set[str]:
        configured = str(self.settings.oidc_issuer or "").strip()
        issuers = {configured} if configured else set()
        if self.settings.oidc_provider == "google":
            issuers.update({"https://accounts.google.com", "accounts.google.com"})
        return {issuer for issuer in issuers if issuer}

    @staticmethod
    def _validate_issuer(payload: dict[str, Any], expected_issuers: set[str]) -> None:
        if not expected_issuers:
            return
        actual = str(payload.get("iss") or "").strip()
        if actual not in expected_issuers:
            raise ValueError("JWT issuer is invalid.")

    def _validate_google_hosted_domain(self, payload: dict[str, Any]) -> None:
        hosted_domain = str(self.settings.oidc_google_hosted_domain or "").strip().lower()
        if not hosted_domain:
            return
        actual = str(payload.get("hd") or "").strip().lower()
        if actual != hosted_domain:
            raise ValueError(f"Google account must belong to hosted domain '{hosted_domain}'.")

    @staticmethod
    def _is_platform_admin(payload: dict[str, Any]) -> bool:
        if bool(payload.get("kairyx_platform_admin")):
            return True
        for field in ("roles", "role", "kairyx_roles"):
            raw_value = payload.get(field)
            if isinstance(raw_value, list) and any(str(item).lower() == "platform_admin" for item in raw_value):
                return True
            if isinstance(raw_value, str) and "platform_admin" in {part.strip().lower() for part in raw_value.split(",")}:
                return True
        return False


@lru_cache(maxsize=4)
def _get_authenticator(cache_key: tuple[str, ...]) -> OIDCAuthenticator:
    return OIDCAuthenticator(get_settings())


def get_authenticator() -> OIDCAuthenticator:
    settings = get_settings()
    cache_key = (
        settings.oidc_issuer,
        settings.oidc_audience,
        settings.oidc_jwks_url,
        settings.oidc_jwt_signing_secret,
        settings.oidc_provider,
        settings.oidc_google_hosted_domain,
    )
    return _get_authenticator(cache_key)
