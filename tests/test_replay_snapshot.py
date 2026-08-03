from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from uuid import UUID

import pytest

from smart_market_data_gateway.domain import AcceptedQuoteEvent, DataQualityMetadata, QuoteEvent
from smart_market_data_gateway.integrity import IntegrityVerification
import smart_market_data_gateway.replay as replay


def accepted_event() -> AcceptedQuoteEvent:
    timestamp = datetime(2026, 8, 1, 12, 0, tzinfo=UTC)
    event = QuoteEvent(
        event_id=UUID(int=1),
        symbol="AAPL",
        price=Decimal("100"),
        provider_timestamp=timestamp,
        received_at=timestamp + timedelta(milliseconds=5),
        sequence=1,
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
        source_stream_id="1-0",
    )


class RecordingTransaction:
    def __init__(self, connection: "RecordingConnection") -> None:
        self.connection = connection

    async def __aenter__(self) -> None:
        self.connection.in_transaction = True

    async def __aexit__(self, *_exc: object) -> None:
        self.connection.in_transaction = False


class RowsCursor:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self.rows = iter(rows)

    def __aiter__(self) -> AsyncIterator[dict[str, Any]]:
        return self

    async def __anext__(self) -> dict[str, Any]:
        try:
            return next(self.rows)
        except StopIteration as exc:
            raise StopAsyncIteration from exc


class RecordingConnection:
    def __init__(self, payload: str) -> None:
        self.payload = payload
        self.in_transaction = False
        self.closed = False
        self.transaction_options: tuple[str | None, bool | None] | None = None
        self.cursor_used_while_transaction_active = False

    def transaction(
        self,
        *,
        isolation: str | None = None,
        readonly: bool | None = None,
    ) -> RecordingTransaction:
        self.transaction_options = (isolation, readonly)
        return RecordingTransaction(self)

    def is_in_transaction(self) -> bool:
        return self.in_transaction

    def cursor(self, *_args: object, **_kwargs: object) -> RowsCursor:
        self.cursor_used_while_transaction_active = self.in_transaction
        return RowsCursor([{"payload": self.payload}])

    async def close(self) -> None:
        self.closed = True


async def test_replay_verifies_and_reads_from_same_snapshot(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    accepted = accepted_event()
    connection = RecordingConnection(accepted.model_dump_json())
    verified_connections: list[RecordingConnection] = []

    async def connect(**_kwargs: object) -> RecordingConnection:
        return connection

    async def verify(
        candidate: RecordingConnection,
        *,
        use_existing_snapshot: bool = False,
    ) -> IntegrityVerification:
        assert use_existing_snapshot is True
        assert candidate.in_transaction is True
        verified_connections.append(candidate)
        return IntegrityVerification(
            chain_name="accepted_quotes",
            event_count=1,
            head_record_hash="sha256:test",
        )

    monkeypatch.setattr(replay.asyncpg, "connect", connect)
    monkeypatch.setattr(replay, "verify_accepted_event_chain", verify)
    monkeypatch.setattr(
        replay,
        "settings",
        SimpleNamespace(
            database_url="postgresql://test",
            history_command_timeout_seconds=5,
            redis_url="redis://unused",
            accepted_stream_maxlen=100,
        ),
    )

    output = tmp_path / "replay.jsonl"
    args = replay.build_parser().parse_args(["--output", str(output)])
    count = await replay._run(args)

    assert count == 1
    assert verified_connections == [connection]
    assert connection.transaction_options == ("repeatable_read", True)
    assert connection.cursor_used_while_transaction_active is True
    assert connection.closed is True
    assert output.read_text(encoding="utf-8").strip() == accepted.model_dump_json()
