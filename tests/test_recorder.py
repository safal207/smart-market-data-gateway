from __future__ import annotations

from collections.abc import AsyncIterator
import json
import os
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest

from smart_market_data_gateway.recorder import (
    AtomicJsonlWriter,
    QuoteMessageRecorder,
    RecorderCounters,
    consume_messages,
)


def quote_message(symbol: str, sequence: int, price: str = "100.00") -> str:
    return json.dumps(
        {
            "type": "quote",
            "data": {
                "schema_version": "1.0",
                "event_id": str(uuid4()),
                "symbol": symbol,
                "price": price,
                "bid": "99.90",
                "ask": "100.10",
                "provider_timestamp": "2026-08-04T07:00:00Z",
                "received_at": "2026-08-04T07:00:00.010000Z",
                "sequence": sequence,
                "provider": "mock",
            },
        }
    )


def read_rows(path: Path) -> list[dict[str, Any]]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]


def test_writes_flat_tmi_compatible_quote_rows(tmp_path: Path) -> None:
    output = tmp_path / "quotes.jsonl"
    counters = RecorderCounters()

    with AtomicJsonlWriter(output) as writer:
        recorder = QuoteMessageRecorder(writer, counters)
        assert recorder.handle(quote_message("AAPL", 1))
        assert recorder.handle(quote_message("TSLA", 1, "250.00"))

    rows = read_rows(output)
    assert [row["symbol"] for row in rows] == ["AAPL", "TSLA"]
    assert rows[0]["event_type"] == "quote"
    assert rows[0]["channel"] == "quote"
    assert rows[0]["provider"] == "mock"
    assert rows[0]["sequence_gap"] is False
    assert counters.as_dict() == {
        "received": 2,
        "written": 2,
        "malformed": 0,
        "duplicates": 0,
        "gaps": 0,
        "reconnects": 0,
        "dropped": 0,
    }


def test_skips_duplicates_and_marks_sequence_gaps(tmp_path: Path) -> None:
    output = tmp_path / "quotes.jsonl"

    with AtomicJsonlWriter(output) as writer:
        recorder = QuoteMessageRecorder(writer)
        assert recorder.handle(quote_message("AAPL", 10))
        assert not recorder.handle(quote_message("AAPL", 10))
        assert recorder.handle(quote_message("AAPL", 13))

    rows = read_rows(output)
    assert len(rows) == 2
    assert rows[1]["degraded_stream"] is True
    assert rows[1]["sequence_gap"] is True
    assert rows[1]["gap_size"] == 2
    assert recorder.counters.duplicates == 1
    assert recorder.counters.gaps == 1


def test_counts_malformed_without_writing_payload(tmp_path: Path) -> None:
    output = tmp_path / "quotes.jsonl"

    with AtomicJsonlWriter(output) as writer:
        recorder = QuoteMessageRecorder(writer)
        assert not recorder.handle("not-json")
        assert not recorder.handle(json.dumps({"type": "heartbeat"}))

    assert output.read_bytes() == b""
    assert recorder.counters.received == 2
    assert recorder.counters.malformed == 1
    assert recorder.counters.written == 0


def test_rolls_back_partial_append(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    output = tmp_path / "quotes.jsonl"
    real_write = os.write
    calls = 0

    def flaky_write(fd: int, payload: bytes | memoryview) -> int:
        nonlocal calls
        calls += 1
        if calls == 1:
            partial = bytes(payload[:8])
            return real_write(fd, partial)
        raise OSError("simulated interrupted append")

    monkeypatch.setattr("smart_market_data_gateway.recorder.os.write", flaky_write)

    with AtomicJsonlWriter(output, fsync=False) as writer:
        with pytest.raises(OSError, match="interrupted append"):
            writer.write({"symbol": "AAPL", "price": "100.00"})

    assert output.read_bytes() == b""


@pytest.mark.asyncio
async def test_consumes_until_requested_record_count(tmp_path: Path) -> None:
    output = tmp_path / "quotes.jsonl"

    async def messages() -> AsyncIterator[str]:
        yield json.dumps({"type": "connected"})
        yield quote_message("AAPL", 1)
        yield quote_message("TSLA", 1)
        yield quote_message("NVDA", 1)

    with AtomicJsonlWriter(output) as writer:
        recorder = QuoteMessageRecorder(writer)
        await consume_messages(messages(), recorder, max_records=2)

    assert [row["symbol"] for row in read_rows(output)] == ["AAPL", "TSLA"]
