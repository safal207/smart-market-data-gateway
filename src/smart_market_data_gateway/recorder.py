from __future__ import annotations

import argparse
import asyncio
from collections.abc import AsyncIterator, Mapping, Sequence
from dataclasses import asdict, dataclass
import hashlib
import hmac
import json
import os
from pathlib import Path
import sys
from typing import Any, cast
from uuid import uuid4

from pydantic import ValidationError
from websockets.asyncio.client import connect

from smart_market_data_gateway.domain import QuoteEvent, StreamMessage

LEDGER_VERSION = "1.0"
LEDGER_ALGORITHM = "sha256"
GENESIS_HASH = "0" * 64
PROVENANCE_SYSTEM = "smart-market-data-gateway"
PROVENANCE_COMPONENT = "websocket-jsonl-recorder"
PROVENANCE_TRANSPORT = "websocket"
RESERVED_LEDGER_FIELDS = frozenset(
    {
        "ledger_version",
        "ledger_algorithm",
        "ledger_index",
        "previous_record_hash",
        "record_hash",
        "recorder_session_id",
        "provenance_system",
        "provenance_component",
        "provenance_transport",
    }
)


class LedgerIntegrityError(ValueError):
    """Raised when an existing recording cannot be trusted as a ledger."""


@dataclass(frozen=True, slots=True)
class LedgerVerification:
    """Deterministic verification result for one complete JSONL ledger."""

    records: int
    head_hash: str
    session_ids: tuple[str, ...]

    def as_dict(self) -> dict[str, object]:
        return {
            "records": self.records,
            "head_hash": self.head_hash,
            "session_ids": list(self.session_ids),
            "verified": True,
        }


@dataclass(slots=True)
class RecorderCounters:
    """Operational counters emitted without exposing credentials or payloads."""

    received: int = 0
    written: int = 0
    malformed: int = 0
    duplicates: int = 0
    gaps: int = 0
    reconnects: int = 0
    dropped: int = 0

    def as_dict(self) -> dict[str, int]:
        return asdict(self)


def canonical_record_bytes(record: Mapping[str, Any]) -> bytes:
    """Encode one ledger record in the canonical form used by the hash chain."""

    payload = dict(record)
    payload.pop("record_hash", None)
    return json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")


def compute_record_hash(record: Mapping[str, Any]) -> str:
    """Return the lowercase SHA-256 digest for a canonical ledger record."""

    return hashlib.sha256(canonical_record_bytes(record)).hexdigest()


def _require_hash(value: object, *, field: str, line_number: int) -> str:
    if (
        not isinstance(value, str)
        or len(value) != 64
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise LedgerIntegrityError(
            f"line {line_number}: {field} must be a lowercase 64-character SHA-256 digest"
        )
    return value


def verify_jsonl_ledger(path: Path, *, allow_missing: bool = False) -> LedgerVerification:
    """Verify framing, canonical hashes, indexes, provenance, and every chain link."""

    if not path.exists():
        if allow_missing:
            return LedgerVerification(records=0, head_hash=GENESIS_HASH, session_ids=())
        raise LedgerIntegrityError(f"ledger does not exist: {path}")

    size = path.stat().st_size
    if size == 0:
        return LedgerVerification(records=0, head_hash=GENESIS_HASH, session_ids=())

    with path.open("rb") as handle:
        handle.seek(-1, os.SEEK_END)
        if handle.read(1) != b"\n":
            raise LedgerIntegrityError("ledger must end with a complete newline-terminated record")
        handle.seek(0)

        expected_previous_hash = GENESIS_HASH
        expected_index = 0
        session_ids: set[str] = set()

        for line_number, raw_line in enumerate(handle, start=1):
            try:
                payload = json.loads(raw_line)
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise LedgerIntegrityError(
                    f"line {line_number}: invalid UTF-8 JSON record"
                ) from exc
            if not isinstance(payload, dict):
                raise LedgerIntegrityError(f"line {line_number}: ledger record must be an object")

            if payload.get("ledger_version") != LEDGER_VERSION:
                raise LedgerIntegrityError(
                    f"line {line_number}: unsupported or missing ledger_version"
                )
            if payload.get("ledger_algorithm") != LEDGER_ALGORITHM:
                raise LedgerIntegrityError(
                    f"line {line_number}: unsupported or missing ledger_algorithm"
                )
            if payload.get("ledger_index") != expected_index:
                raise LedgerIntegrityError(
                    f"line {line_number}: ledger_index must equal {expected_index}"
                )
            if payload.get("provenance_system") != PROVENANCE_SYSTEM:
                raise LedgerIntegrityError(f"line {line_number}: invalid provenance_system")
            if payload.get("provenance_component") != PROVENANCE_COMPONENT:
                raise LedgerIntegrityError(f"line {line_number}: invalid provenance_component")
            if payload.get("provenance_transport") != PROVENANCE_TRANSPORT:
                raise LedgerIntegrityError(f"line {line_number}: invalid provenance_transport")

            session_id = payload.get("recorder_session_id")
            if not isinstance(session_id, str) or not session_id.strip():
                raise LedgerIntegrityError(f"line {line_number}: recorder_session_id is required")
            session_ids.add(session_id)

            previous_hash = _require_hash(
                payload.get("previous_record_hash"),
                field="previous_record_hash",
                line_number=line_number,
            )
            if not hmac.compare_digest(previous_hash, expected_previous_hash):
                raise LedgerIntegrityError(
                    f"line {line_number}: previous_record_hash does not match the prior record"
                )

            stored_hash = _require_hash(
                payload.get("record_hash"),
                field="record_hash",
                line_number=line_number,
            )
            computed_hash = compute_record_hash(payload)
            if not hmac.compare_digest(stored_hash, computed_hash):
                raise LedgerIntegrityError(f"line {line_number}: record_hash mismatch")

            expected_previous_hash = stored_hash
            expected_index += 1

    return LedgerVerification(
        records=expected_index,
        head_hash=expected_previous_hash,
        session_ids=tuple(sorted(session_ids)),
    )


class AtomicJsonlWriter:
    """Append complete hash-chained JSONL records and roll back partial writes."""

    def __init__(
        self,
        path: Path,
        *,
        fsync: bool = True,
        session_id: str | None = None,
    ) -> None:
        self.path = path
        self.fsync = fsync
        self.session_id = session_id or str(uuid4())
        if not self.session_id.strip():
            raise ValueError("session_id must not be empty")
        self._fd: int | None = None
        self._next_ledger_index = 0
        self._previous_record_hash = GENESIS_HASH

    def __enter__(self) -> AtomicJsonlWriter:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        flags = os.O_APPEND | os.O_CREAT | os.O_WRONLY
        if hasattr(os, "O_CLOEXEC"):
            flags |= os.O_CLOEXEC
        self._fd = os.open(self.path, flags, 0o600)
        try:
            verification = verify_jsonl_ledger(self.path)
        except BaseException:
            os.close(self._fd)
            self._fd = None
            raise
        self._next_ledger_index = verification.records
        self._previous_record_hash = verification.head_hash
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        self.close()

    def write(self, record: Mapping[str, Any]) -> dict[str, Any]:
        if self._fd is None:
            raise RuntimeError("JSONL writer is not open")

        conflicting_fields = RESERVED_LEDGER_FIELDS.intersection(record)
        if conflicting_fields:
            names = ", ".join(sorted(conflicting_fields))
            raise ValueError(f"record contains reserved evidence-ledger fields: {names}")

        ledger_record = dict(record)
        ledger_record.update(
            {
                "ledger_version": LEDGER_VERSION,
                "ledger_algorithm": LEDGER_ALGORITHM,
                "ledger_index": self._next_ledger_index,
                "previous_record_hash": self._previous_record_hash,
                "recorder_session_id": self.session_id,
                "provenance_system": PROVENANCE_SYSTEM,
                "provenance_component": PROVENANCE_COMPONENT,
                "provenance_transport": PROVENANCE_TRANSPORT,
            }
        )
        ledger_record["record_hash"] = compute_record_hash(ledger_record)
        payload = canonical_record_bytes(ledger_record)
        payload = payload[:-1] if payload.endswith(b"\n") else payload
        payload = (
            json.dumps(
                ledger_record,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=False,
            )
            + "\n"
        ).encode("utf-8")

        original_size = os.fstat(self._fd).st_size
        remaining = memoryview(payload)
        try:
            while remaining:
                written = os.write(self._fd, remaining)
                if written <= 0:
                    raise OSError("JSONL append made no progress")
                remaining = remaining[written:]
            if self.fsync:
                os.fsync(self._fd)
        except BaseException:
            os.ftruncate(self._fd, original_size)
            if self.fsync:
                os.fsync(self._fd)
            raise

        self._previous_record_hash = ledger_record["record_hash"]
        self._next_ledger_index += 1
        return ledger_record

    def close(self) -> None:
        if self._fd is None:
            return
        os.close(self._fd)
        self._fd = None


class QuoteMessageRecorder:
    """Validate gateway stream messages and persist TMI-compatible quote rows."""

    def __init__(self, writer: AtomicJsonlWriter, counters: RecorderCounters | None = None) -> None:
        self.writer = writer
        self.counters = counters or RecorderCounters()
        self._last_sequence: dict[str, int] = {}

    def handle(self, raw_message: str | bytes) -> bool:
        self.counters.received += 1
        try:
            payload = json.loads(raw_message)
            if not isinstance(payload, dict):
                raise ValueError("stream message must be a JSON object")
            message = StreamMessage.model_validate(payload)
            quote, metadata = self._quote_from_message(message)
        except (UnicodeDecodeError, json.JSONDecodeError, TypeError, ValueError, ValidationError):
            self.counters.malformed += 1
            return False

        if quote is None:
            return False

        sequence_metadata = self._sequence_metadata(quote)
        if sequence_metadata is None:
            return False

        record = quote.model_dump(mode="json")
        record.update(metadata)
        record.update(sequence_metadata)
        record["event_type"] = "quote"
        record["channel"] = "quote"

        try:
            self.writer.write(record)
        except OSError:
            self.counters.dropped += 1
            raise
        self.counters.written += 1
        return True

    @staticmethod
    def _quote_from_message(
        message: StreamMessage,
    ) -> tuple[QuoteEvent | None, dict[str, Any]]:
        if message.type == "quote":
            quote_payload: object = message.data
            metadata = {"source_message_type": "quote", "stale": False}
        elif message.type == "snapshot":
            quote_payload = message.data.get("quote", message.data)
            metadata = {
                "source_message_type": "snapshot",
                "stale": bool(message.data.get("stale", False)),
            }
        else:
            return None, {}

        if not isinstance(quote_payload, Mapping):
            raise ValueError("quote payload must be an object")
        return QuoteEvent.model_validate(quote_payload), metadata

    def _sequence_metadata(self, quote: QuoteEvent) -> dict[str, Any] | None:
        if quote.sequence is None:
            return {"degraded_stream": False, "sequence_gap": False, "gap_size": 0}

        previous = self._last_sequence.get(quote.symbol)
        if previous is not None and quote.sequence <= previous:
            self.counters.duplicates += 1
            return None

        gap_size = 0 if previous is None else max(0, quote.sequence - previous - 1)
        if gap_size:
            self.counters.gaps += 1
        self._last_sequence[quote.symbol] = quote.sequence
        return {
            "degraded_stream": gap_size > 0,
            "sequence_gap": gap_size > 0,
            "gap_size": gap_size,
        }


async def consume_messages(
    messages: AsyncIterator[str | bytes],
    recorder: QuoteMessageRecorder,
    *,
    max_records: int = 0,
) -> None:
    """Consume an arbitrary async stream; useful for live sockets and deterministic tests."""

    async for message in messages:
        recorder.handle(message)
        if max_records > 0 and recorder.counters.written >= max_records:
            return


async def record_websocket(
    *,
    url: str,
    token: str,
    symbols: Sequence[str],
    output: Path,
    max_records: int = 0,
    max_reconnects: int = 10,
    fsync: bool = True,
) -> RecorderCounters:
    """Record authenticated gateway quotes, reconnecting without truncating prior rows."""

    normalized_symbols = sorted({symbol.strip().upper() for symbol in symbols if symbol.strip()})
    if not normalized_symbols:
        raise ValueError("at least one symbol is required")
    if not token:
        raise ValueError("a bearer token is required")

    counters = RecorderCounters()
    attempt = 0
    with AtomicJsonlWriter(output, fsync=fsync) as writer:
        recorder = QuoteMessageRecorder(writer, counters)
        while max_records <= 0 or counters.written < max_records:
            try:
                async with connect(
                    url,
                    additional_headers={"Authorization": f"Bearer {token}"},
                    open_timeout=10,
                    close_timeout=5,
                    ping_interval=20,
                    ping_timeout=20,
                    max_size=1_048_576,
                ) as websocket:
                    attempt = 0
                    await websocket.send(
                        json.dumps(
                            {
                                "action": "subscribe",
                                "symbols": normalized_symbols,
                                "channels": ["quote"],
                                "request_id": "tmi-recorder",
                            }
                        )
                    )
                    await consume_messages(
                        cast(AsyncIterator[str | bytes], websocket),
                        recorder,
                        max_records=max_records,
                    )
                    if max_records > 0 and counters.written >= max_records:
                        return counters
                    raise ConnectionError("gateway WebSocket stream ended")
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                attempt += 1
                counters.reconnects += 1
                if attempt > max_reconnects:
                    raise RuntimeError("gateway recorder exceeded reconnect limit") from exc
                await asyncio.sleep(min(30.0, 0.5 * (2 ** (attempt - 1))))
    return counters


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="smdg-recorder",
        description="Record or verify tamper-evident TMI-compatible market-data JSONL.",
    )
    parser.add_argument(
        "--url",
        default=os.getenv("SMDG_RECORDER_URL", "ws://localhost:8000/v1/stream"),
    )
    parser.add_argument("--token", default=os.getenv("SMDG_RECORDER_TOKEN"))
    parser.add_argument("--symbol", action="append", dest="symbols")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--verify-ledger", type=Path)
    parser.add_argument("--max-records", type=int, default=0)
    parser.add_argument("--max-reconnects", type=int, default=10)
    parser.add_argument("--no-fsync", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.verify_ledger is not None:
        try:
            verification = verify_jsonl_ledger(args.verify_ledger)
        except (OSError, LedgerIntegrityError) as exc:
            raise SystemExit(f"smdg-recorder: {exc}") from exc
        json.dump(verification.as_dict(), sys.stdout, sort_keys=True)
        sys.stdout.write("\n")
        return 0

    if not args.token:
        raise SystemExit("smdg-recorder: set --token or SMDG_RECORDER_TOKEN")
    if not args.symbols:
        raise SystemExit("smdg-recorder: provide at least one --symbol")
    if args.output is None:
        raise SystemExit("smdg-recorder: --output is required when recording")

    try:
        counters = asyncio.run(
            record_websocket(
                url=args.url,
                token=args.token,
                symbols=args.symbols,
                output=args.output,
                max_records=args.max_records,
                max_reconnects=args.max_reconnects,
                fsync=not args.no_fsync,
            )
        )
    except (OSError, RuntimeError, ValueError) as exc:
        raise SystemExit(f"smdg-recorder: {exc}") from exc
    json.dump(counters.as_dict(), sys.stdout, sort_keys=True)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
