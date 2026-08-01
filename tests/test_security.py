from datetime import UTC, datetime, timedelta

import jwt
import pytest

from smart_market_data_gateway.domain import ClientIdentity, ServiceTier
from smart_market_data_gateway.metrics import GatewayMetrics
from smart_market_data_gateway.security import (
    AuthenticationError,
    AuthService,
    AuthorizationError,
    AuthorizationService,
)


async def test_dev_token_and_anonymous_identity(test_settings) -> None:
    auth = AuthService(test_settings, GatewayMetrics())
    anonymous = await auth.authenticate(None)
    assert anonymous.client_id == "anonymous-dev"
    identity = await auth.authenticate("dev-premium:client-42")
    assert identity.client_id == "client-42"
    assert identity.tier is ServiceTier.PREMIUM


async def test_signed_jwt_validation(test_settings) -> None:
    auth = AuthService(test_settings, GatewayMetrics())
    now = datetime.now(UTC)
    token = jwt.encode(
        {
            "sub": "client-1",
            "tier": "pro",
            "iss": test_settings.jwt_issuer,
            "aud": test_settings.jwt_audience,
            "iat": now,
            "exp": now + timedelta(minutes=5),
            "allowed_symbols": ["AAPL"],
        },
        test_settings.jwt_secret,
        algorithm=test_settings.jwt_algorithm,
    )
    identity = await auth.authenticate(token)
    assert identity.tier is ServiceTier.PRO
    assert identity.allowed_symbols == {"AAPL"}

    with pytest.raises(AuthenticationError):
        await auth.authenticate(token + "broken")


def test_authorization_checks_symbols_and_channels() -> None:
    identity = ClientIdentity(
        client_id="client-1",
        allowed_symbols={"AAPL"},
        allowed_channels={"quote"},
    )
    AuthorizationService.check_symbols(identity, {"AAPL"})
    AuthorizationService.check_channels(identity, {"quote"})
    with pytest.raises(AuthorizationError):
        AuthorizationService.check_symbols(identity, {"TSLA"})
    with pytest.raises(AuthorizationError):
        AuthorizationService.check_channels(identity, {"depth"})
