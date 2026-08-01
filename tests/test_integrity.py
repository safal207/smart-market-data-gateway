from datetime import UTC, datetime, timedelta
from decimal import Decimal
import os
from uuid import UUID

import asyncpg
import pytest

from smart_market_data_gateway.domain import AcceptedQuoteEvent, DataQualityMetadata, QuoteEvent
from smart_market_data_gateway.durability import (
    HistoryCrash,
    HistoryFailpoint,
    crash_if,
    parse_history_failpoint,
)
from smart_market_data_gateway.history import HistorySink
from smart_market_data_gateway.integrity import (
    IntegrityChainError,
    build_integrity_record,
    verify_accepted_event_chain,
)


def accepted_event(event_id: int, timestamp: datetime, price: str) -> AcceptedQuoteEvent:
    event = QuoteEvent(
        event_id=UUID(int=event_id),
        symbol="AAPL",
        price=Decimal(price),
        provider_timestamp=timestamp,
        received_at=timestamp + timedelta(milliseconds=10),
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
        source_stream_id=f"{event_id}-0",
    )


def history_database_url() -> str:
    database_url = os.getenv("TEST_DATABASE_URL")
    if not database_url:
        pytest.skip("TEST_DATABASE_URL is required for the integrity integration test")
    return database_url


async def reset_history_tables(connection: asyncpg.Connection) -> None:
    await connection.execute(
        "TRUNCATE TABLE accepted_event_integrity, late_quote_events, candles, quote_events"
    )
    await connection.execute(
        """
        UPDATE integrity_chain_heads
        SET chain_sequence = 0, record_hash = NULL, updated_at = NOW()
        WHERE chain_name = 'accepted_quotes'
        """
    )


class FailingIntegrityHistorySink(HistorySink):
    async def _persist_integrity_record(
        self,
        connection: asyncpg.Connection,
        accepted: AcceptedQuoteEvent,
    ) -> None:
        raise RuntimeError("injected integrity failure")


def test_integrity_record_is_deterministic_and_chained() -> None:
    event = accepted_event(
        1,
        datetime(2026, 8, 1, 12, 0, tzinfo=UTC),
        "100.25",
    )
    first = build_integrity_record(1, event, None)
    repeated = build_integrity_record(1, event, None)
    second = build_integrity_record(2, event, first.record_hash)

    assert first == repeated
    assert first.payload_digest.startswith("sha256:")
    assert len(first.payload_digest) == 71
    assert second.previous_record_hash == first.record_hash
    assert second.record_hash != first.record_hash


def test_history_failpoints_are_stable_and_explicit() -> None:
    point = HistoryFailpoint.AFTER_LEDGER_APPEND_BEFORE_COMMIT
    assert parse_history_failpoint(None) is None
    assert parse_history_failpoint("  ") is None
    assert parse_history_failpoint(f" {point.value} ") == point
    crash_if(None, point)
    with pytest.raises(HistoryCrash) as raised:
        crash_if(point, point)
    assert raised.value.point == point


async def test_integrity_chain_verifies_and_detects_payload_tampering(
    redis_client,
    test_settings,
) -> None:
    config = test_settings.model_copy(
        update={
            "database_url": history_database_url(),
            "enable_history_retention": False,
        }
    )
    sink = HistorySink(redis_client, config)
    await sink.start()
    assert sink.pool is not None

    first = accepted_event(1, datetime(2026, 8, 1, 12, 0, tzinfo=UTC), "100")
    second = accepted_event(2, datetime(2026, 8, 1, 12, 0, 1, tzinfo=UTC), "101")
    async with sink.pool.acquire() as connection:
        await reset_history_tables(connection)
        async with connection.transaction():
            await sink._insert_event(connection, first)
            await sink._insert_event(connection, second)

        verified = await verify_accepted_event_chain(connection)
        assert verified.event_count == 2
        assert verified.head_record_hash is not None

        await connection.execute(
            """
            UPDATE quote_events
            SET payload = jsonb_set(payload, '{event,price}', '"999"'::jsonb)
            WHERE event_id = $1 AND provider_timestamp = $2
            """,
            first.event.event_id,
            first.event.provider_timestamp,
        )
        with pytest.raises(IntegrityChainError, match="payload digest mismatch"):
            await verify_accepted_event_chain(connection)

    await sink.close()


async def test_integrity_chain_rejects_quote_without_integrity_record(
    redis_client,
    test_settings,
) -> None:
    config = test_settings.model_copy(
        update={
            "database_url": history_database_url(),
            "enable_history_retention": False,
        }
    )
    sink = HistorySink(redis_client, config)
    await sink.start()
    assert sink.pool is not None

    chained = accepted_event(3, datetime(2026, 8, 1, 12, 0, tzinfo=UTC), "100")
    unchained = accepted_event(4, datetime(2026, 8, 1, 12, 0, 1, tzinfo=UTC), "101")
    async with sink.pool.acquire() as connection:
        await reset_history_tables(connection)
        async with connection.transaction():
            await sink._insert_event(connection, chained)
        event = unchained.event
        await connection.execute(
            """
            INSERT INTO quote_events (
                event_id, provider_timestamp, received_at, accepted_at, data_cutoff,
                symbol, provider, sequence, price, bid, ask, quality_score,
                gap_detected, normalization_version, source_stream_id, payload
            )
            VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12,
                $13, $14, $15, $16::jsonb
            )
            """,
            event.event_id,
            event.provider_timestamp,
            event.received_at,
            unchained.quality.accepted_at,
            unchained.data_cutoff,
            event.symbol,
            event.provider,
            event.sequence,
            event.price,
            event.bid,
            event.ask,
            unchained.quality.score,
            unchained.quality.gap_detected,
            unchained.quality.normalization_version,
            unchained.source_stream_id,
            unchained.model_dump_json(),
        )
        with pytest.raises(
            IntegrityChainError,
            match="quote history count does not match the integrity chain",
        ):
            await verify_accepted_event_chain(connection)

    await sink.close()


async def test_duplicate_event_does_not_advance_integrity_head(
    redis_client,
    test_settings,
) -> None:
    config = test_settings.model_copy(
        update={
            "database_url": history_database_url(),
            "enable_history_retention": False,
        }
    )
    sink = HistorySink(redis_client, config)
    await sink.start()
    assert sink.pool is not None

    event = accepted_event(7, datetime(2026, 8, 1, 12, 0, tzinfo=UTC), "100")
    async with sink.pool.acquire() as connection:
        await reset_history_tables(connection)
        async with connection.transaction():
            first = await sink._insert_event(connection, event)
            duplicate = await sink._insert_event(connection, event)
        verified = await verify_accepted_event_chain(connection)

    assert first == event.event.event_id
    assert duplicate is None
    assert verified.event_count == 1
    await sink.close()


async def test_integrity_failure_rolls_back_quote_insert(
    redis_client,
    test_settings,
) -> None:
    config = test_settings.model_copy(
        update={
            "database_url": history_database_url(),
            "enable_history_retention": False,
        }
    )
    sink = FailingIntegrityHistorySink(redis_client, config)
    await sink.start()
    assert sink.pool is not None

    event = accepted_event(9, datetime(2026, 8, 1, 12, 0, tzinfo=UTC), "100")
    async with sink.pool.acquire() as connection:
        await reset_history_tables(connection)
        with pytest.raises(RuntimeError, match="injected integrity failure"):
            async with connection.transaction():
                await sink._insert_event(connection, event)
        quote_count = await connection.fetchval("SELECT COUNT(*) FROM quote_events")
        integrity_count = await connection.fetchval(
            "SELECT COUNT(*) FROM accepted_event_integrity"
        )
        head_sequence = await connection.fetchval(
            """
            SELECT chain_sequence
            FROM integrity_chain_heads
            WHERE chain_name = 'accepted_quotes'
            """
        )

    assert quote_count == 0
    assert integrity_count == 0
    assert head_sequence == 0
    await sink.close()
