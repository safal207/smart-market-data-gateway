from __future__ import annotations

import argparse
import asyncio
from collections.abc import AsyncIterator, Mapping, Sequence
from dataclasses import asdict, dataclass
import json
import os
from pathlib import Path
import sys
from typing import Any, cast

from pydantic import ValidationError
from websockets.asyncio.client import connect

from smart_market_data_gateway.domain import QuoteEvent, StreamMessage


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


class AtomicJsonlWriter:
    """Append complete JSONL records and roll back a failed partial append."""

    def __init__(self, path: Path, *, fsync: bool = True) -> None:
        self.path = path
        self.fsync = fsync
        self._fd: int | None = None

    def __enter__(self) -> AtomicJsonlWriter:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        flags = os.O_APPEND | os.O_CREAT | os.O_WRONLY
        if hasattr(os, "O_CLOEXEC"):
            flags |= os.O_CLOEXEC
        self._fd = os.open(self.path, flags, 0o600)
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        self.close()

    def write(self, record: Mapping[str, Any]) -> None:
        if self._fd is None:
            raise RuntimeError("JSONL writer is not open")

        payload = (
            json.dumps(record, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
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
        description="Record authenticated quote messages as append-only TMI-compatible JSONL.",
    )
    parser.add_argument(
        "--url",
        default=os.getenv("SMDG_RECORDER_URL", "ws://localhost:8000/v1/stream"),
    )
    parser.add_argument("--token", default=os.getenv("SMDG_RECORDER_TOKEN"))
    parser.add_argument("--symbol", action="append", dest="symbols", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--max-records", type=int, default=0)
    parser.add_argument("--max-reconnects", type=int, default=10)
    parser.add_argument("--no-fsync", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if not args.token:
        raise SystemExit("smdg-recorder: set --token or SMDG_RECORDER_TOKEN")
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
