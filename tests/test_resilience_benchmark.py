from datetime import UTC, datetime

import pytest

from smart_market_data_gateway.resilience_evidence import (
    parse_utc_timestamp,
    quote_was_received_after,
)


def test_quote_recovery_uses_gateway_receive_time_not_provider_time() -> None:
    cutoff = datetime(2026, 8, 4, 0, 0, 10, tzinfo=UTC)
    buffered_quote = {
        "provider_timestamp": "2026-08-04T00:01:00+00:00",
        "received_at": "2026-08-04T00:00:09+00:00",
    }
    fresh_quote = {
        "provider_timestamp": "2026-08-03T23:59:00+00:00",
        "received_at": "2026-08-04T00:00:11+00:00",
    }

    assert quote_was_received_after(buffered_quote, cutoff) is False
    assert quote_was_received_after(fresh_quote, cutoff) is True


def test_quote_without_received_at_fails_closed() -> None:
    cutoff = datetime(2026, 8, 4, 0, 0, 10, tzinfo=UTC)
    quote = {"provider_timestamp": "2099-01-01T00:00:00Z"}

    assert quote_was_received_after(quote, cutoff) is False


def test_resilience_timestamp_must_be_timezone_aware() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        parse_utc_timestamp("2026-08-04T00:00:00")
