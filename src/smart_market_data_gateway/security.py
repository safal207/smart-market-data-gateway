import asyncio
from dataclasses import dataclass
import logging
import time
from typing import Any

import httpx
import jwt
from jwt import PyJWKClient

from smart_market_data_gateway.config import Settings
from smart_market_data_gateway.domain import ClientIdentity, ServiceTier
from smart_market_data_gateway.metrics import GatewayMetrics
from smart_market_data_gateway.storage import RedisStore

logger = logging.getLogger(__name__)


class AuthenticationError(ValueError):
    pass


class AuthorizationError(ValueError):
    pass


@dataclass(slots=True)
class _CachedIdentity:
    identity: ClientIdentity
    expires_at: float


class AuthService:
    def __init__(self, settings: Settings, metrics: GatewayMetrics) -> None:
        self.settings = settings
        self.metrics = metrics
        self._jwks_client = (
            PyJWKClient(str(settings.jwt_jwks_url)) if settings.jwt_jwks_url is not None else None
        )

    async def authenticate(self, token: str | None) -> ClientIdentity:
        if not token:
            if self.settings.allow_anonymous_dev:
                return ClientIdentity(client_id="anonymous-dev", tier=ServiceTier.BASIC)
            self.metrics.auth_failures.labels("missing_token").inc()
            raise AuthenticationError("authentication required")

        if self.settings.allow_dev_tokens and token.startswith("dev-"):
            tier_text = token.removeprefix("dev-").split(":", 1)[0]
            try:
                tier = ServiceTier(tier_text)
            except ValueError as exc:
                self.metrics.auth_failures.labels("invalid_dev_tier").inc()
                raise AuthenticationError("invalid development token") from exc
            client_id = token.split(":", 1)[1] if ":" in token else f"dev-{tier.value}"
            return ClientIdentity(client_id=client_id, tier=tier)

        try:
            key: Any = self.settings.jwt_secret
            if self._jwks_client is not None:
                signing_key = await asyncio.to_thread(
                    self._jwks_client.get_signing_key_from_jwt,
                    token,
                )
                key = signing_key.key
            claims = jwt.decode(
                token,
                key=key,
                algorithms=[self.settings.jwt_algorithm],
                issuer=self.settings.jwt_issuer,
                audience=self.settings.jwt_audience,
                options={"require": ["exp", "sub", "iss", "aud"]},
            )
        except jwt.ExpiredSignatureError as exc:
            self.metrics.auth_failures.labels("expired").inc()
            raise AuthenticationError("token expired") from exc
        except jwt.PyJWTError as exc:
            self.metrics.auth_failures.labels("invalid_token").inc()
            raise AuthenticationError("invalid token") from exc

        try:
            tier = ServiceTier(str(claims.get("tier", "basic")))
            return ClientIdentity(
                client_id=str(claims["sub"]),
                tier=tier,
                allowed_symbols=claims.get("allowed_symbols"),
                allowed_channels=set(claims.get("allowed_channels", ["quote"])),
                organization_id=claims.get("organization_id"),
                claims=claims,
            )
        except (KeyError, TypeError, ValueError) as exc:
            self.metrics.auth_failures.labels("invalid_claims").inc()
            raise AuthenticationError("invalid token claims") from exc

    @staticmethod
    def bearer_token(authorization: str | None) -> str | None:
        if not authorization:
            return None
        scheme, _, value = authorization.partition(" ")
        if scheme.lower() != "bearer" or not value:
            return None
        return value.strip()


class ClientProfileClient:
    """Optional entitlement resolver with timeout, retry, fallback, and short-lived cache."""

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self._cache: dict[str, _CachedIdentity] = {}
        self._client = httpx.AsyncClient(timeout=httpx.Timeout(2.0))

    async def resolve(self, token_identity: ClientIdentity) -> ClientIdentity:
        if self.settings.client_profile_url is None:
            return token_identity
        cached = self._cache.get(token_identity.client_id)
        if cached is not None and cached.expires_at > time.monotonic():
            return cached.identity

        url = f"{str(self.settings.client_profile_url).rstrip('/')}/v1/clients/{token_identity.client_id}/entitlements"
        headers: dict[str, str] = {}
        if self.settings.client_profile_service_token:
            headers["Authorization"] = f"Bearer {self.settings.client_profile_service_token}"

        last_error: Exception | None = None
        for attempt in range(2):
            try:
                response = await self._client.get(url, headers=headers)
                response.raise_for_status()
                payload = response.json()
                resolved = ClientIdentity(
                    client_id=token_identity.client_id,
                    tier=ServiceTier(str(payload.get("tier", token_identity.tier.value))),
                    allowed_symbols=payload.get(
                        "allowed_symbols",
                        token_identity.allowed_symbols,
                    ),
                    allowed_channels=set(
                        payload.get("allowed_channels", token_identity.allowed_channels)
                    ),
                    organization_id=payload.get(
                        "organization_id",
                        token_identity.organization_id,
                    ),
                    claims=token_identity.claims,
                )
                self._cache[token_identity.client_id] = _CachedIdentity(
                    identity=resolved,
                    expires_at=time.monotonic()
                    + self.settings.entitlement_cache_ttl_seconds,
                )
                return resolved
            except (httpx.HTTPError, ValueError) as exc:
                last_error = exc
                if attempt == 0:
                    await asyncio.sleep(0.1)

        logger.warning(
            "client profile fallback",
            extra={
                "event": "client_profile_fallback",
                "client_id": token_identity.client_id,
            },
        )
        if last_error is not None:
            logger.debug("client profile error: %s", last_error)
        return token_identity

    async def close(self) -> None:
        await self._client.aclose()


class AuthorizationService:
    @staticmethod
    def check_symbols(identity: ClientIdentity, symbols: set[str]) -> None:
        if identity.allowed_symbols is None:
            return
        denied = symbols - identity.allowed_symbols
        if denied:
            raise AuthorizationError(f"symbols not entitled: {','.join(sorted(denied))}")

    @staticmethod
    def check_channels(identity: ClientIdentity, channels: set[str]) -> None:
        denied = channels - identity.allowed_channels
        if denied:
            raise AuthorizationError(f"channels not entitled: {','.join(sorted(denied))}")


class DistributedRateLimiter:
    def __init__(self, store: RedisStore) -> None:
        self.store = store

    async def allow(self, scope: str, identity: ClientIdentity, limit: int) -> bool:
        return await self.store.rate_limit(f"{scope}:{identity.client_id}", limit)
