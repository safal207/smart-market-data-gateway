from functools import cached_property
import json

from pydantic import BaseModel, Field, HttpUrl
from pydantic_settings import BaseSettings, SettingsConfigDict


class TierPolicyConfig(BaseModel):
    max_symbols: int
    max_connections: int
    updates_per_second: float
    rest_requests_per_minute: int
    subscription_ops_per_minute: int
    market_depth: str = "top"
    historical_data: bool = False


DEFAULT_TIER_POLICIES = {
    "basic": {
        "max_symbols": 20,
        "max_connections": 2,
        "updates_per_second": 1.0,
        "rest_requests_per_minute": 120,
        "subscription_ops_per_minute": 60,
        "market_depth": "top",
        "historical_data": False,
    },
    "pro": {
        "max_symbols": 100,
        "max_connections": 5,
        "updates_per_second": 5.0,
        "rest_requests_per_minute": 600,
        "subscription_ops_per_minute": 300,
        "market_depth": "limited",
        "historical_data": True,
    },
    "premium": {
        "max_symbols": 500,
        "max_connections": 20,
        "updates_per_second": 10.0,
        "rest_requests_per_minute": 3000,
        "subscription_ops_per_minute": 1200,
        "market_depth": "extended",
        "historical_data": True,
    },
}


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="SMDG_",
        env_file=".env",
        extra="ignore",
    )

    app_name: str = "Smart Market Data Gateway"
    environment: str = "development"
    log_level: str = "INFO"

    redis_url: str = Field(default="redis://localhost:6379/0")
    database_url: str | None = Field(default=None)
    client_profile_url: HttpUrl | None = None
    client_profile_service_token: str | None = None

    jwt_secret: str = "change-me-in-production"
    jwt_algorithm: str = "HS256"
    jwt_issuer: str = "smart-market-data-gateway"
    jwt_audience: str = "market-data-clients"
    jwt_jwks_url: HttpUrl | None = None
    allow_dev_tokens: bool = True
    allow_anonymous_dev: bool = True

    quote_stream: str = "smdg:quotes:v1"
    quote_pubsub_channel: str = "smdg:quotes:fanout:v1"
    control_stream: str = "smdg:subscriptions:control:v1"
    usage_stream: str = "smdg:usage:v1"
    dead_letter_stream: str = "smdg:dead-letter:v1"
    stream_group: str = "smdg:processors:v1"
    control_group: str = "smdg:collectors:v1"
    stream_maxlen: int = 100_000
    retry_limit: int = 3
    dedupe_ttl_seconds: int = 3600

    quote_freshness_seconds: float = 5.0
    subscription_grace_seconds: float = 3.0
    subscription_ttl_seconds: int = 60
    heartbeat_seconds: float = 15.0
    websocket_queue_size: int = 512
    entitlement_cache_ttl_seconds: int = 30
    shutdown_timeout_seconds: float = 10.0

    mock_symbols: str = "AAPL,TSLA,NVDA,MSFT,GOOG"
    mock_interval_seconds: float = 0.1
    mock_duplicate_every: int | None = None
    mock_fail_after_events: int | None = None

    tier_policies_json: str = json.dumps(DEFAULT_TIER_POLICIES)

    @cached_property
    def tier_policies(self) -> dict[str, TierPolicyConfig]:
        raw = json.loads(self.tier_policies_json)
        return {name: TierPolicyConfig.model_validate(value) for name, value in raw.items()}

    @property
    def mock_symbol_list(self) -> list[str]:
        return [symbol.strip().upper() for symbol in self.mock_symbols.split(",") if symbol.strip()]


settings = Settings()
