from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from typing import Any

import jwt
from jwt import PyJWKClient

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
        self._jwks_client = PyJWKClient(settings.oidc_jwks_url) if settings.oidc_jwks_url else None

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
        issuer = self.settings.oidc_issuer or None
        audience = self.settings.oidc_audience or None
        options = {
            "verify_signature": True,
            "verify_exp": True,
            "verify_aud": bool(audience),
            "verify_iss": bool(issuer),
        }
        algorithms = ["HS256"] if self.settings.oidc_jwt_signing_secret else None
        if self.settings.oidc_jwt_signing_secret:
            return dict(
                jwt.decode(
                    token,
                    self.settings.oidc_jwt_signing_secret,
                    algorithms=algorithms,
                    audience=audience,
                    issuer=issuer,
                    options=options,
                )
            )
        if self._jwks_client is None:
            raise ValueError("OIDC JWKS is not configured.")
        signing_key = self._jwks_client.get_signing_key_from_jwt(token)
        return dict(
            jwt.decode(
                token,
                signing_key.key,
                algorithms=["RS256", "RS384", "RS512", "ES256", "ES384", "ES512"],
                audience=audience,
                issuer=issuer,
                options=options,
            )
        )

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
    )
    return _get_authenticator(cache_key)
