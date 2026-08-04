import hashlib
import json
from collections.abc import Iterable
from datetime import datetime
from enum import StrEnum
from uuid import UUID

from smart_market_data_gateway.intelligence.models import (
    EvidenceRef,
    Hypothesis,
    HypothesisSnapshot,
    HypothesisState,
    LedgerEntry,
)

GENESIS_HASH = "0" * 64
_ALLOWED_TRANSITIONS: dict[HypothesisState, frozenset[HypothesisState]] = {
    HypothesisState.NO_SIGNAL: frozenset({HypothesisState.WATCH}),
    HypothesisState.WATCH: frozenset(
        {
            HypothesisState.CONFIRMED,
            HypothesisState.INVALIDATED,
            HypothesisState.EXPIRED,
        }
    ),
    HypothesisState.CONFIRMED: frozenset(),
    HypothesisState.INVALIDATED: frozenset(),
    HypothesisState.EXPIRED: frozenset(),
}


class PredictionLedger:
    """Append-only, hash-linked state machine for falsifiable hypotheses."""

    def __init__(self) -> None:
        self._hypotheses: dict[UUID, Hypothesis] = {}
        self._states: dict[UUID, HypothesisState] = {}
        self._latest: dict[UUID, LedgerEntry] = {}
        self._entries: list[LedgerEntry] = []

    @property
    def entries(self) -> tuple[LedgerEntry, ...]:
        return tuple(self._entries)

    def register(
        self,
        hypothesis: Hypothesis,
        *,
        reason: str = "hypothesis_registered",
    ) -> LedgerEntry:
        """Register a hypothesis and move it from `NO_SIGNAL` to `WATCH`."""

        if hypothesis.hypothesis_id in self._hypotheses:
            raise ValueError("hypothesis_id already exists")
        entry = self._append_entry(
            hypothesis_id=hypothesis.hypothesis_id,
            from_state=HypothesisState.NO_SIGNAL,
            to_state=HypothesisState.WATCH,
            occurred_at=hypothesis.created_at,
            reason=reason,
            evidence=hypothesis.supporting_evidence,
        )
        self._hypotheses[hypothesis.hypothesis_id] = hypothesis
        self._states[hypothesis.hypothesis_id] = HypothesisState.WATCH
        self._latest[hypothesis.hypothesis_id] = entry
        return entry

    def transition(
        self,
        hypothesis_id: UUID,
        *,
        to_state: HypothesisState,
        occurred_at: datetime,
        reason: str,
        evidence: Iterable[EvidenceRef] = (),
    ) -> LedgerEntry:
        """Apply one legal terminal transition with explicit evidence."""

        self._require_timezone(occurred_at)
        if hypothesis_id not in self._hypotheses:
            raise KeyError(f"unknown hypothesis_id: {hypothesis_id}")
        from_state = self._states[hypothesis_id]
        if to_state not in _ALLOWED_TRANSITIONS[from_state]:
            raise ValueError(f"illegal hypothesis transition: {from_state} -> {to_state}")
        latest = self._latest[hypothesis_id]
        if occurred_at < latest.occurred_at:
            raise ValueError("transition occurred_at must not precede the prior transition")

        evidence_tuple = tuple(evidence)
        if to_state in {HypothesisState.CONFIRMED, HypothesisState.INVALIDATED} and not evidence_tuple:
            raise ValueError(f"{to_state} transition requires evidence")

        entry = self._append_entry(
            hypothesis_id=hypothesis_id,
            from_state=from_state,
            to_state=to_state,
            occurred_at=occurred_at,
            reason=reason,
            evidence=evidence_tuple,
        )
        self._states[hypothesis_id] = to_state
        self._latest[hypothesis_id] = entry
        return entry

    def expire_due(self, now: datetime) -> tuple[LedgerEntry, ...]:
        """Expire every watched hypothesis whose explicit deadline elapsed."""

        self._require_timezone(now)
        expired: list[LedgerEntry] = []
        for hypothesis_id in sorted(self._hypotheses, key=str):
            hypothesis = self._hypotheses[hypothesis_id]
            if self._states[hypothesis_id] is HypothesisState.WATCH and hypothesis.deadline <= now:
                expired.append(
                    self.transition(
                        hypothesis_id,
                        to_state=HypothesisState.EXPIRED,
                        occurred_at=now,
                        reason="deadline_elapsed_without_confirmation",
                    )
                )
        return tuple(expired)

    def snapshot(self, hypothesis_id: UUID) -> HypothesisSnapshot:
        if hypothesis_id not in self._hypotheses:
            raise KeyError(f"unknown hypothesis_id: {hypothesis_id}")
        return HypothesisSnapshot(
            hypothesis=self._hypotheses[hypothesis_id],
            state=self._states[hypothesis_id],
            latest_transition=self._latest[hypothesis_id],
        )

    def verify(self) -> None:
        """Verify the complete chain and all state transitions or raise ValueError."""

        verify_ledger_entries(self._entries)

    def _append_entry(
        self,
        *,
        hypothesis_id: UUID,
        from_state: HypothesisState,
        to_state: HypothesisState,
        occurred_at: datetime,
        reason: str,
        evidence: tuple[EvidenceRef, ...],
    ) -> LedgerEntry:
        self._require_timezone(occurred_at)
        if self._entries and occurred_at < self._entries[-1].occurred_at:
            raise ValueError("ledger append time must be monotonic")
        if to_state not in _ALLOWED_TRANSITIONS[from_state]:
            raise ValueError(f"illegal hypothesis transition: {from_state} -> {to_state}")

        ledger_index = len(self._entries)
        previous_record_hash = self._entries[-1].record_hash if self._entries else GENESIS_HASH
        payload = _entry_payload(
            ledger_index=ledger_index,
            hypothesis_id=hypothesis_id,
            from_state=from_state,
            to_state=to_state,
            occurred_at=occurred_at,
            reason=reason,
            evidence=evidence,
            previous_record_hash=previous_record_hash,
        )
        entry = LedgerEntry(
            **payload,
            record_hash=_hash_payload(payload),
        )
        self._entries.append(entry)
        return entry

    @staticmethod
    def _require_timezone(value: datetime) -> None:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("occurred_at must include timezone information")


def verify_ledger_entries(entries: Iterable[LedgerEntry]) -> None:
    """Verify an arbitrary prediction-ledger sequence without mutating it."""

    previous_record_hash = GENESIS_HASH
    states: dict[UUID, HypothesisState] = {}
    previous_occurred_at: datetime | None = None

    for expected_index, entry in enumerate(entries):
        if entry.ledger_index != expected_index:
            raise ValueError(f"ledger index mismatch at position {expected_index}")
        if entry.previous_record_hash != previous_record_hash:
            raise ValueError(f"ledger chain mismatch at index {expected_index}")
        if previous_occurred_at is not None and entry.occurred_at < previous_occurred_at:
            raise ValueError(f"ledger time regression at index {expected_index}")

        expected_from_state = states.get(entry.hypothesis_id, HypothesisState.NO_SIGNAL)
        if entry.from_state is not expected_from_state:
            raise ValueError(f"state-chain mismatch at index {expected_index}")
        if entry.to_state not in _ALLOWED_TRANSITIONS[entry.from_state]:
            raise ValueError(f"illegal transition at index {expected_index}")
        if (
            entry.to_state in {HypothesisState.CONFIRMED, HypothesisState.INVALIDATED}
            and not entry.evidence
        ):
            raise ValueError(f"resolved transition lacks evidence at index {expected_index}")

        payload = _entry_payload(
            ledger_index=entry.ledger_index,
            hypothesis_id=entry.hypothesis_id,
            from_state=entry.from_state,
            to_state=entry.to_state,
            occurred_at=entry.occurred_at,
            reason=entry.reason,
            evidence=entry.evidence,
            previous_record_hash=entry.previous_record_hash,
        )
        if entry.record_hash != _hash_payload(payload):
            raise ValueError(f"record hash mismatch at index {expected_index}")

        states[entry.hypothesis_id] = entry.to_state
        previous_record_hash = entry.record_hash
        previous_occurred_at = entry.occurred_at


def _entry_payload(
    *,
    ledger_index: int,
    hypothesis_id: UUID,
    from_state: HypothesisState,
    to_state: HypothesisState,
    occurred_at: datetime,
    reason: str,
    evidence: tuple[EvidenceRef, ...],
    previous_record_hash: str,
) -> dict[str, object]:
    return {
        "ledger_index": ledger_index,
        "hypothesis_id": hypothesis_id,
        "from_state": from_state,
        "to_state": to_state,
        "occurred_at": occurred_at,
        "reason": reason,
        "evidence": evidence,
        "previous_record_hash": previous_record_hash,
    }


def _hash_payload(payload: dict[str, object]) -> str:
    canonical = json.dumps(
        _jsonable(payload),
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()


def _jsonable(value: object) -> object:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, StrEnum):
        return value.value
    if isinstance(value, EvidenceRef):
        return value.model_dump(mode="json")
    if isinstance(value, tuple):
        return [_jsonable(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _jsonable(item) for key, item in value.items()}
    return value
