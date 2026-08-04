from __future__ import annotations

from collections.abc import AsyncIterator
import json
import os
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest

from smart_market_data_gateway.recorder import (
    GENESIS_HASH,
    AtomicJsonlWriter,
    LedgerIntegrityError,
    QuoteMessageRecorder,
    RecorderCounters,
    compute_record_hash,
    consume_messages,
    main,
    verify_jsonl_ledger,
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


def test_writes_flat_tmi_compatible_hash_chained_quote_rows(tmp_path: Path) -> None:
    output = tmp_path / "quotes.jsonl"
    counters = RecorderCounters()

    with AtomicJsonlWriter(output, session_id="session-a") as writer:
        recorder = QuoteMessageRecorder(writer, counters)
        assert recorder.handle(quote_message("AAPL", 1))
        assert recorder.handle(quote_message("TSLA", 1, "250.00"))

    rows = read_rows(output)
    assert [row["symbol"] for row in rows] == ["AAPL", "TSLA"]
    assert rows[0]["event_type"] == "quote"
    assert rows[0]["channel"] == "quote"
    assert rows[0]["provider"] == "mock"
    assert rows[0]["sequence_gap"] is False
    assert rows[0]["ledger_index"] == 0
    assert rows[0]["previous_record_hash"] == GENESIS_HASH
    assert rows[0]["recorder_session_id"] == "session-a"
    assert rows[0]["record_hash"] == compute_record_hash(rows[0])
    assert rows[1]["ledger_index"] == 1
    assert rows[1]["previous_record_hash"] == rows[0]["record_hash"]
    assert rows[1]["record_hash"] == compute_record_hash(rows[1])
    assert verify_jsonl_ledger(output).as_dict() == {
        "records": 2,
        "head_hash": rows[1]["record_hash"],
        "session_ids": ["session-a"],
        "verified": True,
    }
    assert counters.as_dict() == {
        "received": 2,
        "written": 2,
        "malformed": 0,
        "duplicates": 0,
        "gaps": 0,
        "reconnects": 0,
        "dropped": 0,
    }


def test_continues_chain_across_process_sessions(tmp_path: Path) -> None:
    output = tmp_path / "quotes.jsonl"

    with AtomicJsonlWriter(output, session_id="session-a") as writer:
        QuoteMessageRecorder(writer).handle(quote_message("AAPL", 1))

    with AtomicJsonlWriter(output, session_id="session-b") as writer:
        QuoteMessageRecorder(writer).handle(quote_message("AAPL", 2))

    rows = read_rows(output)
    assert rows[1]["ledger_index"] == 1
    assert rows[1]["previous_record_hash"] == rows[0]["record_hash"]
    assert rows[1]["recorder_session_id"] == "session-b"
    verification = verify_jsonl_ledger(output)
    assert verification.records == 2
    assert verification.session_ids == ("session-a", "session-b")


def test_rejects_tampered_record_and_refuses_append(tmp_path: Path) -> None:
    output = tmp_path / "quotes.jsonl"

    with AtomicJsonlWriter(output, session_id="session-a") as writer:
        QuoteMessageRecorder(writer).handle(quote_message("AAPL", 1))
        QuoteMessageRecorder(writer).handle(quote_message("TSLA", 1))

    rows = read_rows(output)
    rows[0]["price"] = "999.00"
    output.write_text(
        "".join(json.dumps(row, sort_keys=True, separators=(",", ":")) + "\n" for row in rows),
        encoding="utf-8",
    )

    with pytest.raises(LedgerIntegrityError, match="record_hash mismatch"):
        verify_jsonl_ledger(output)
    with pytest.raises(LedgerIntegrityError, match="record_hash mismatch"):
        with AtomicJsonlWriter(output):
            pass


def test_rejects_truncated_last_record(tmp_path: Path) -> None:
    output = tmp_path / "quotes.jsonl"

    with AtomicJsonlWriter(output) as writer:
        QuoteMessageRecorder(writer).handle(quote_message("AAPL", 1))

    output.write_bytes(output.read_bytes().rstrip(b"\n"))
    with pytest.raises(LedgerIntegrityError, match="newline-terminated"):
        verify_jsonl_ledger(output)


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
    assert verify_jsonl_ledger(output).records == 2


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


def test_rolls_back_partial_append_without_advancing_chain(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    output = tmp_path / "quotes.jsonl"
    real_write = os.write
    calls = 0

    def flaky_write(fd: int, payload: bytes | memoryview) -> int:
        nonlocal calls
        calls += 1
        if calls == 1:
            partial = bytes(payload[:8])
            return real_write(fd, partial)
        if calls == 2:
            raise OSError("simulated interrupted append")
        return real_write(fd, payload)

    monkeypatch.setattr("smart_market_data_gateway.recorder.os.write", flaky_write)

    with AtomicJsonlWriter(output, fsync=False, session_id="session-a") as writer:
        with pytest.raises(OSError, match="interrupted append"):
            writer.write({"symbol": "AAPL", "price": "100.00"})
        persisted = writer.write({"symbol": "AAPL", "price": "101.00"})

    assert persisted["ledger_index"] == 0
    assert persisted["previous_record_hash"] == GENESIS_HASH
    assert len(read_rows(output)) == 1
    assert verify_jsonl_ledger(output).records == 1


def test_cli_verifies_ledger_without_token(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    output = tmp_path / "quotes.jsonl"
    with AtomicJsonlWriter(output, session_id="session-a") as writer:
        QuoteMessageRecorder(writer).handle(quote_message("AAPL", 1))

    assert main(["--verify-ledger", str(output)]) == 0
    result = json.loads(capsys.readouterr().out)
    assert result["verified"] is True
    assert result["records"] == 1
    assert result["session_ids"] == ["session-a"]


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
    assert verify_jsonl_ledger(output).records == 2
