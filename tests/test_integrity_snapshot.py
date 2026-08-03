from collections.abc import AsyncIterator
from typing import Any

import pytest

from smart_market_data_gateway.integrity import verify_accepted_event_chain


class RecordingTransaction:
    def __init__(self, connection: "RecordingConnection") -> None:
        self.connection = connection

    async def __aenter__(self) -> None:
        self.connection.in_transaction = True

    async def __aexit__(self, *_exc: object) -> None:
        self.connection.in_transaction = False


class EmptyCursor:
    def __aiter__(self) -> AsyncIterator[dict[str, Any]]:
        return self

    async def __anext__(self) -> dict[str, Any]:
        raise StopAsyncIteration


class RecordingConnection:
    def __init__(self) -> None:
        self.in_transaction = False
        self.transaction_options: tuple[str | None, bool | None] | None = None
        self.cursor_used = False

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

    def cursor(self, *_args: object, **_kwargs: object) -> EmptyCursor:
        assert self.in_transaction
        self.cursor_used = True
        return EmptyCursor()

    async def fetchval(self, _query: str) -> int:
        assert self.in_transaction
        return 0

    async def fetchrow(self, _query: str, _chain_name: str) -> dict[str, Any]:
        assert self.in_transaction
        return {"chain_sequence": 0, "record_hash": None}


async def test_integrity_verification_owns_repeatable_read_snapshot() -> None:
    connection = RecordingConnection()

    result = await verify_accepted_event_chain(connection)  # type: ignore[arg-type]

    assert result.event_count == 0
    assert connection.transaction_options == ("repeatable_read", True)
    assert connection.cursor_used is True
    assert connection.in_transaction is False


async def test_existing_snapshot_mode_requires_active_transaction() -> None:
    connection = RecordingConnection()

    with pytest.raises(ValueError, match="active transaction"):
        await verify_accepted_event_chain(  # type: ignore[arg-type]
            connection,
            use_existing_snapshot=True,
        )
