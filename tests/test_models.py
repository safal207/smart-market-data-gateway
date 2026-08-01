from datetime import UTC, datetime
from decimal import Decimal

import pytest
from pydantic import ValidationError

from smart_market_data_gateway.domain import QuoteEvent


def test_quote_event_normalizes_symbol() -> None:
    event = QuoteEvent(
        symbol=" aapl ",
        price=Decimal("215.42"),
        bid=Decimal("215.40"),
        ask=Decimal("215.44"),
        provider_timestamp=datetime.now(UTC),
        provider="mock-provider",
    )

    assert event.symbol == "AAPL"
    assert event.schema_version == "1.0"


def test_quote_event_rejects_crossed_market() -> None:
    with pytest.raises(ValidationError, match="bid must be less than or equal to ask"):
        QuoteEvent(
            symbol="AAPL",
            price=Decimal("215.42"),
            bid=Decimal("215.45"),
            ask=Decimal("215.40"),
            provider_timestamp=datetime.now(UTC),
            provider="mock-provider",
        )


def test_quote_event_requires_timezone() -> None:
    with pytest.raises(ValidationError, match="timestamps must include timezone"):
        QuoteEvent(
            symbol="AAPL",
            price=Decimal("215.42"),
            provider_timestamp=datetime.now(),
            provider="mock-provider",
        )
