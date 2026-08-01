from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import UUID

from smart_market_data_gateway.domain import AcceptedQuoteEvent, DataQualityMetadata, QuoteEvent
from smart_market_data_gateway.replay import ReplayService, parse_speed


def accepted_event(event_id: int, timestamp: datetime) -> AcceptedQuoteEvent:
    event = QuoteEvent(
        event_id=UUID(int=event_id),
        symbol="MSFT",
        price=Decimal("200") + Decimal(event_id),
        provider_timestamp=timestamp,
        received_at=timestamp + timedelta(milliseconds=5),
        sequence=event_id,
        provider="test-provider",
    )
    return AcceptedQuoteEvent(
        event=event,
        quality=DataQualityMetadata(
            score=1.0,
            source_provider=event.provider,
            accepted_at=event.received_at,
        ),
        data_cutoff=event.provider_timestamp,
    )


async def event_source(events: list[AcceptedQuoteEvent]) -> AsyncIterator[AcceptedQuoteEvent]:
    for event in events:
        yield event


async def test_replay_preserves_order_and_scales_delays() -> None:
    start = datetime(2026, 8, 1, 12, 0, tzinfo=UTC)
    events = [
        accepted_event(1, start),
        accepted_event(2, start + timedelta(seconds=2)),
        accepted_event(3, start + timedelta(seconds=5)),
    ]
    emitted: list[int] = []
    delays: list[float] = []

    async def emit(event: AcceptedQuoteEvent) -> None:
        emitted.append(event.event.sequence or 0)

    async def sleep(delay: float) -> None:
        delays.append(delay)

    count = await ReplayService(event_source(events), emit, sleep=sleep).run(speed=10)

    assert count == 3
    assert emitted == [1, 2, 3]
    assert delays == [0.2, 0.3]


async def test_replay_max_speed_does_not_sleep() -> None:
    start = datetime(2026, 8, 1, 12, 0, tzinfo=UTC)
    events = [accepted_event(1, start), accepted_event(2, start + timedelta(seconds=10))]
    delays: list[float] = []

    async def emit(_event: AcceptedQuoteEvent) -> None:
        return None

    async def sleep(delay: float) -> None:
        delays.append(delay)

    count = await ReplayService(event_source(events), emit, sleep=sleep).run(speed=None)

    assert count == 2
    assert delays == []


def test_parse_speed_supports_named_and_numeric_modes() -> None:
    assert parse_speed("max") is None
    assert parse_speed("1x") == 1
    assert parse_speed("10") == 10
