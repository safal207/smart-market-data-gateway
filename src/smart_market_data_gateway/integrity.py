import argparse
import asyncio
from dataclasses import dataclass
from datetime import datetime
import hashlib
import json
import sys
from typing import Any
from uuid import UUID

import asyncpg

from smart_market_data_gateway.config import settings
from smart_market_data_gateway.domain import AcceptedQuoteEvent

ACCEPTED_EVENT_CHAIN_NAME = "accepted_quotes"
INTEGRITY_PROFILE = "org.smdg.accepted-event-integrity.v1"


class IntegrityChainError(RuntimeError):
    """Raised when accepted-event history no longer matches its integrity chain."""


@dataclass(frozen=True, slots=True)
class IntegrityRecord:
    chain_name: str
    profile: str
    sequence: int
    event_id: UUID
    provider_timestamp: datetime
    source_stream_id: str | None
    payload_digest: str
    previous_record_hash: str | None
    record_hash: str


@dataclass(frozen=True, slots=True)
class IntegrityVerification:
    chain_name: str
    event_count: int
    head_record_hash: str | None


def canonical_json(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def sha256_ref(value: bytes) -> str:
    return f"sha256:{hashlib.sha256(value).hexdigest()}"


def accepted_event_payload_digest(accepted: AcceptedQuoteEvent) -> str:
    return sha256_ref(canonical_json(accepted.model_dump(mode="json")))


def build_integrity_record(
    sequence: int,
    accepted: AcceptedQuoteEvent,
    previous_record_hash: str | None,
    *,
    chain_name: str = ACCEPTED_EVENT_CHAIN_NAME,
) -> IntegrityRecord:
    if sequence <= 0:
        raise ValueError("integrity sequence must be positive")
    payload_digest = accepted_event_payload_digest(accepted)
    body = {
        "chain_name": chain_name,
        "profile": INTEGRITY_PROFILE,
        "sequence": sequence,
        "event_id": str(accepted.event.event_id),
        "provider_timestamp": accepted.event.provider_timestamp.isoformat(),
        "source_stream_id": accepted.source_stream_id,
        "payload_digest": payload_digest,
        "previous_record_hash": previous_record_hash,
    }
    return IntegrityRecord(
        chain_name=chain_name,
        profile=INTEGRITY_PROFILE,
        sequence=sequence,
        event_id=accepted.event.event_id,
        provider_timestamp=accepted.event.provider_timestamp,
        source_stream_id=accepted.source_stream_id,
        payload_digest=payload_digest,
        previous_record_hash=previous_record_hash,
        record_hash=sha256_ref(canonical_json(body)),
    )


async def verify_accepted_event_chain(
    connection: asyncpg.Connection,
    *,
    chain_name: str = ACCEPTED_EVENT_CHAIN_NAME,
) -> IntegrityVerification:
    rows = await connection.fetch(
        """
        SELECT
            integrity.chain_sequence,
            integrity.profile,
            integrity.event_id,
            integrity.provider_timestamp,
            integrity.source_stream_id,
            integrity.payload_digest,
            integrity.previous_record_hash,
            integrity.record_hash,
            quote_events.payload::text AS payload_text
        FROM accepted_event_integrity AS integrity
        LEFT JOIN quote_events
          ON quote_events.event_id = integrity.event_id
         AND quote_events.provider_timestamp = integrity.provider_timestamp
        WHERE integrity.chain_name = $1
        ORDER BY integrity.chain_sequence
        """,
        chain_name,
    )

    expected_sequence = 1
    previous_record_hash: str | None = None
    for row in rows:
        sequence = int(row["chain_sequence"])
        if sequence != expected_sequence:
            raise IntegrityChainError(
                f"integrity sequence mismatch: expected {expected_sequence}, got {sequence}"
            )
        payload_text = row["payload_text"]
        if not isinstance(payload_text, str):
            raise IntegrityChainError(f"missing quote payload at sequence {sequence}")
        accepted = AcceptedQuoteEvent.model_validate_json(payload_text)
        if row["event_id"] != accepted.event.event_id:
            raise IntegrityChainError(f"event id mismatch at sequence {sequence}")
        if row["provider_timestamp"] != accepted.event.provider_timestamp:
            raise IntegrityChainError(f"provider timestamp mismatch at sequence {sequence}")
        if row["source_stream_id"] != accepted.source_stream_id:
            raise IntegrityChainError(f"source stream mismatch at sequence {sequence}")
        if row["profile"] != INTEGRITY_PROFILE:
            raise IntegrityChainError(f"integrity profile mismatch at sequence {sequence}")

        expected = build_integrity_record(
            sequence,
            accepted,
            previous_record_hash,
            chain_name=chain_name,
        )
        if row["payload_digest"] != expected.payload_digest:
            raise IntegrityChainError(f"payload digest mismatch at sequence {sequence}")
        if row["previous_record_hash"] != previous_record_hash:
            raise IntegrityChainError(f"previous record hash mismatch at sequence {sequence}")
        if row["record_hash"] != expected.record_hash:
            raise IntegrityChainError(f"record hash mismatch at sequence {sequence}")

        previous_record_hash = expected.record_hash
        expected_sequence += 1

    event_count = expected_sequence - 1
    head = await connection.fetchrow(
        """
        SELECT chain_sequence, record_hash
        FROM integrity_chain_heads
        WHERE chain_name = $1
        """,
        chain_name,
    )
    if head is None:
        raise IntegrityChainError(f"missing integrity head for chain {chain_name}")
    if int(head["chain_sequence"]) != event_count:
        raise IntegrityChainError(
            "integrity head sequence does not match the verified event count"
        )
    if head["record_hash"] != previous_record_hash:
        raise IntegrityChainError("integrity head hash does not match the verified chain")

    return IntegrityVerification(
        chain_name=chain_name,
        event_count=event_count,
        head_record_hash=previous_record_hash,
    )


async def _run(database_url: str | None) -> int:
    dsn = database_url or settings.database_url
    if not dsn:
        raise ValueError("SMDG_DATABASE_URL or --database-url is required")
    connection = await asyncpg.connect(
        dsn=dsn,
        command_timeout=settings.history_command_timeout_seconds,
    )
    try:
        result = await verify_accepted_event_chain(connection)
    finally:
        await connection.close()
    print(
        json.dumps(
            {
                "chain_name": result.chain_name,
                "event_count": result.event_count,
                "head_record_hash": result.head_record_hash,
                "verified": True,
            },
            sort_keys=True,
        )
    )
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Verify the tamper-evident accepted-event history chain"
    )
    parser.add_argument(
        "--database-url",
        help="PostgreSQL URL; defaults to SMDG_DATABASE_URL",
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()
    try:
        raise SystemExit(asyncio.run(_run(args.database_url)))
    except IntegrityChainError as exc:
        print(f"integrity_verification_failed={exc}", file=sys.stderr)
        raise SystemExit(1) from exc


if __name__ == "__main__":
    main()
