from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import uuid4

import pytest
from pydantic import ValidationError

from smart_market_data_gateway.domain.models import (
    DepthSemantics,
    EvidenceOrigin,
    MarketEvidenceCapability,
    QuantityUnit,
    QuoteEvent,
    VolumeKind,
    VolumeSemantics,
)
from smart_market_data_gateway.intelligence import (
    EvidenceRef,
    Hypothesis,
    HypothesisState,
    MarketObservation,
    ObservationKind,
    PredictionLedger,
    TemporalMarketMemory,
    quote_event_to_observations,
    verify_ledger_entries,
)

BASE_TIME = datetime(2026, 8, 4, 12, 0, tzinfo=UTC)
RECORD_HASH = "a" * 64


def evidence(
    *,
    minute: int = 0,
    received_minute: int | None = None,
    suffix: str = "quote",
) -> EvidenceRef:
    return EvidenceRef(
        provenance_system="smart-market-data-gateway",
        provenance_component="test-provider",
        locator=f"test:{suffix}",
        observed_at=BASE_TIME + timedelta(minutes=minute),
        received_at=BASE_TIME + timedelta(
            minutes=minute if received_minute is None else received_minute
        ),
        record_hash=RECORD_HASH,
        ledger_index=minute,
    )


def observation(
    *,
    minute_observed: int,
    minute_received: int,
    expires_minute: int | None = None,
) -> MarketObservation:
    return MarketObservation(
        symbol="BTC-USD",
        kind=ObservationKind.EVENT,
        observed_at=BASE_TIME + timedelta(minutes=minute_observed),
        received_at=BASE_TIME + timedelta(minutes=minute_received),
        source="test-wire",
        fact="material event observed",
        metrics={"score": Decimal("0.8")},
        confidence=Decimal("0.8"),
        evidence=(
            evidence(
                minute=minute_observed,
                received_minute=minute_received,
                suffix=str(uuid4()),
            ),
        ),
        expires_at=(
            BASE_TIME + timedelta(minutes=expires_minute)
            if expires_minute is not None
            else None
        ),
    )


def hypothesis(*, created_minute: int = 0, deadline_minute: int = 10) -> Hypothesis:
    return Hypothesis(
        symbol="BTC-USD",
        statement="buy pressure may precede a bounded breakout",
        expected_event="price trades above the declared resistance before deadline",
        created_at=BASE_TIME + timedelta(minutes=created_minute),
        deadline=BASE_TIME + timedelta(minutes=deadline_minute),
        confidence=Decimal("0.65"),
        supporting_evidence=(
            evidence(
                minute=created_minute,
                received_minute=created_minute,
            ),
        ),
        counter_evidence=(),
    )


def test_quote_event_adapter_emits_only_available_typed_evidence() -> None:
    received_at = BASE_TIME + timedelta(milliseconds=25)
    event = QuoteEvent(
        schema_version="1.1",
        symbol="btc-usd",
        price=Decimal("65000.5"),
        bid=Decimal("65000.0"),
        ask=Decimal("65001.0"),
        provider_timestamp=BASE_TIME,
        received_at=received_at,
        sequence=7,
        provider="coinbase-research",
        capabilities=frozenset(
            {
                MarketEvidenceCapability.LEVEL1_QUOTE,
                MarketEvidenceCapability.VOLUME,
                MarketEvidenceCapability.AGGRESSOR_FLOW,
                MarketEvidenceCapability.TRADE_COUNT,
                MarketEvidenceCapability.TOP_OF_BOOK_DEPTH,
            }
        ),
        volume=Decimal("12"),
        buy_volume=Decimal("8"),
        sell_volume=Decimal("3"),
        trade_count=4,
        bid_depth=Decimal("5"),
        ask_depth=Decimal("2"),
        volume_semantics=VolumeSemantics(
            kind=VolumeKind.INTERVAL,
            unit=QuantityUnit.BASE_ASSET,
            aggregation_window_ms=1_000,
            origin=EvidenceOrigin.NATIVE,
        ),
        depth_semantics=DepthSemantics(
            unit=QuantityUnit.BASE_ASSET,
            levels=1,
            origin=EvidenceOrigin.NATIVE,
        ),
    )

    observations = quote_event_to_observations(
        event,
        record_hash=RECORD_HASH,
        ledger_index=42,
        generation=3,
    )

    assert [item.kind for item in observations] == [
        ObservationKind.QUOTE,
        ObservationKind.VOLUME,
        ObservationKind.AGGRESSOR_FLOW,
        ObservationKind.TOP_OF_BOOK_DEPTH,
    ]
    assert all(item.symbol == "BTC-USD" for item in observations)
    assert all(item.evidence[0].record_hash == RECORD_HASH for item in observations)
    assert all(item.evidence[0].ledger_index == 42 for item in observations)
    assert all(item.evidence[0].received_at == received_at for item in observations)
    assert all(
        item.evidence[0].provenance_component == "websocket-jsonl-recorder"
        for item in observations
    )
    assert observations[0].metrics["provider"] == "coinbase-research"
    assert observations[2].metrics["net_aggressor_flow"] == Decimal("5")
    assert observations[3].metrics["depth_imbalance"] == Decimal("3")


def test_temporal_memory_blocks_late_evidence_and_filters_expired_facts() -> None:
    memory = TemporalMarketMemory()
    on_time = observation(minute_observed=0, minute_received=0, expires_minute=4)
    late = observation(minute_observed=1, minute_received=10)
    memory.append_many((late, on_time))

    assert memory.as_of(
        "btc-usd",
        knowledge_time=BASE_TIME + timedelta(minutes=5),
    ) == ()

    historical_view = memory.as_of(
        "BTC-USD",
        knowledge_time=BASE_TIME + timedelta(minutes=11),
        valid_at=BASE_TIME + timedelta(minutes=3),
    )
    assert historical_view == (on_time, late)


def test_temporal_memory_is_idempotent_but_rejects_id_reuse() -> None:
    memory = TemporalMarketMemory()
    item = observation(minute_observed=0, minute_received=0)

    assert memory.append(item) is True
    assert memory.append(item) is False
    with pytest.raises(ValueError, match="different content"):
        memory.append(item.model_copy(update={"fact": "rewritten fact"}))


def test_observation_rejects_evidence_learned_after_observation_receipt() -> None:
    with pytest.raises(ValidationError, match="learned after received_at"):
        MarketObservation(
            symbol="BTC-USD",
            kind=ObservationKind.EVENT,
            observed_at=BASE_TIME,
            received_at=BASE_TIME,
            source="test-wire",
            fact="impossible future evidence",
            evidence=(evidence(minute=0, received_minute=1),),
        )


def test_hypothesis_rejects_evidence_learned_after_creation() -> None:
    with pytest.raises(ValidationError, match="not known at created_at"):
        Hypothesis(
            symbol="BTC-USD",
            statement="future-assisted claim",
            expected_event="a bounded move",
            created_at=BASE_TIME,
            deadline=BASE_TIME + timedelta(minutes=10),
            confidence=Decimal("0.5"),
            supporting_evidence=(evidence(minute=0, received_minute=1),),
        )


def test_prediction_ledger_hash_links_the_full_hypothesis_and_confirmation() -> None:
    ledger = PredictionLedger()
    claim = hypothesis()

    opened = ledger.register(claim)
    confirmed = ledger.transition(
        claim.hypothesis_id,
        to_state=HypothesisState.CONFIRMED,
        occurred_at=BASE_TIME + timedelta(minutes=5),
        reason="price and aggressive flow crossed the declared thresholds",
        evidence=(evidence(minute=5, suffix="confirmation"),),
    )

    ledger.verify()
    assert opened.previous_record_hash == "0" * 64
    assert opened.hypothesis == claim
    assert confirmed.hypothesis is None
    assert confirmed.previous_record_hash == opened.record_hash
    assert ledger.snapshot(claim.hypothesis_id).state is HypothesisState.CONFIRMED


def test_prediction_ledger_rejects_resolution_without_evidence() -> None:
    ledger = PredictionLedger()
    claim = hypothesis()
    ledger.register(claim)

    with pytest.raises(ValueError, match="requires evidence"):
        ledger.transition(
            claim.hypothesis_id,
            to_state=HypothesisState.INVALIDATED,
            occurred_at=BASE_TIME + timedelta(minutes=2),
            reason="counterfactual observed",
        )


def test_prediction_ledger_rejects_evidence_not_known_at_transition_time() -> None:
    ledger = PredictionLedger()
    claim = hypothesis()
    ledger.register(claim)

    with pytest.raises(ValueError, match="not known at occurred_at"):
        ledger.transition(
            claim.hypothesis_id,
            to_state=HypothesisState.CONFIRMED,
            occurred_at=BASE_TIME + timedelta(minutes=2),
            reason="future-assisted confirmation",
            evidence=(evidence(minute=1, received_minute=3),),
        )


def test_prediction_ledger_rejects_resolution_at_or_after_deadline() -> None:
    ledger = PredictionLedger()
    claim = hypothesis(deadline_minute=3)
    ledger.register(claim)

    with pytest.raises(ValueError, match="deadline elapsed"):
        ledger.transition(
            claim.hypothesis_id,
            to_state=HypothesisState.CONFIRMED,
            occurred_at=BASE_TIME + timedelta(minutes=3),
            reason="late confirmation",
            evidence=(evidence(minute=3),),
        )


def test_prediction_ledger_rejects_transition_after_terminal_state() -> None:
    ledger = PredictionLedger()
    claim = hypothesis()
    ledger.register(claim)
    ledger.transition(
        claim.hypothesis_id,
        to_state=HypothesisState.CONFIRMED,
        occurred_at=BASE_TIME + timedelta(minutes=2),
        reason="threshold reached",
        evidence=(evidence(minute=2),),
    )

    with pytest.raises(ValueError, match="illegal hypothesis transition"):
        ledger.transition(
            claim.hypothesis_id,
            to_state=HypothesisState.INVALIDATED,
            occurred_at=BASE_TIME + timedelta(minutes=3),
            reason="late reversal",
            evidence=(evidence(minute=3),),
        )


def test_prediction_ledger_expires_unresolved_hypotheses() -> None:
    ledger = PredictionLedger()
    claim = hypothesis(deadline_minute=3)
    ledger.register(claim)

    assert ledger.expire_due(BASE_TIME + timedelta(minutes=2)) == ()
    expired = ledger.expire_due(BASE_TIME + timedelta(minutes=3))

    assert len(expired) == 1
    assert expired[0].to_state is HypothesisState.EXPIRED
    assert ledger.snapshot(claim.hypothesis_id).state is HypothesisState.EXPIRED
    ledger.verify()


def test_prediction_ledger_allows_independent_hypotheses_to_interleave() -> None:
    ledger = PredictionLedger()
    later_claim = hypothesis(created_minute=2, deadline_minute=12)
    earlier_claim = hypothesis(created_minute=1, deadline_minute=11)

    ledger.register(later_claim)
    ledger.register(earlier_claim)
    ledger.transition(
        earlier_claim.hypothesis_id,
        to_state=HypothesisState.INVALIDATED,
        occurred_at=BASE_TIME + timedelta(minutes=3),
        reason="counter-evidence arrived",
        evidence=(evidence(minute=3),),
    )

    ledger.verify()


def test_prediction_ledger_detects_post_hoc_transition_rewriting() -> None:
    ledger = PredictionLedger()
    claim = hypothesis()
    ledger.register(claim)
    ledger.transition(
        claim.hypothesis_id,
        to_state=HypothesisState.INVALIDATED,
        occurred_at=BASE_TIME + timedelta(minutes=2),
        reason="expected breakout did not occur",
        evidence=(evidence(minute=2),),
    )

    tampered = list(ledger.entries)
    tampered[1] = tampered[1].model_copy(update={"reason": "rewritten after the outcome"})

    with pytest.raises(ValueError, match="record hash mismatch"):
        verify_ledger_entries(tampered)


def test_prediction_ledger_detects_post_hoc_hypothesis_rewriting() -> None:
    ledger = PredictionLedger()
    claim = hypothesis()
    ledger.register(claim)

    tampered = list(ledger.entries)
    assert tampered[0].hypothesis is not None
    rewritten = tampered[0].hypothesis.model_copy(
        update={"statement": "rewritten after observing the outcome"}
    )
    tampered[0] = tampered[0].model_copy(update={"hypothesis": rewritten})

    with pytest.raises(ValueError, match="record hash mismatch"):
        verify_ledger_entries(tampered)
