from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest
from pydantic import ValidationError

from smart_market_data_gateway.domain import (
    DepthSemantics,
    EvidenceOrigin,
    MarketEvidenceCapability,
    QuantityUnit,
    QuoteEvent,
    VolumeKind,
    VolumeSemantics,
)
from smart_market_data_gateway.recorder import (
    AtomicJsonlWriter,
    LedgerIntegrityError,
    verify_jsonl_ledger,
)


def base_event(**overrides: object) -> dict[str, object]:
    event: dict[str, object] = {
        "event_id": uuid4(),
        "symbol": "AAPL",
        "price": Decimal("100.00"),
        "bid": Decimal("99.99"),
        "ask": Decimal("100.01"),
        "provider_timestamp": datetime(2026, 8, 4, 10, 0, tzinfo=UTC),
        "received_at": datetime(2026, 8, 4, 10, 0, 0, 10_000, tzinfo=UTC),
        "sequence": 1,
        "provider": "test-provider",
    }
    event.update(overrides)
    return event


def rich_event(**overrides: object) -> QuoteEvent:
    capabilities = frozenset(
        {
            MarketEvidenceCapability.LEVEL1_QUOTE,
            MarketEvidenceCapability.VOLUME,
            MarketEvidenceCapability.AGGRESSOR_FLOW,
            MarketEvidenceCapability.TRADE_COUNT,
            MarketEvidenceCapability.TOP_OF_BOOK_DEPTH,
        }
    )
    event = base_event(
        schema_version="1.1",
        capabilities=capabilities,
        volume=Decimal("1000"),
        buy_volume=Decimal("550"),
        sell_volume=Decimal("450"),
        trade_count=42,
        bid_depth=Decimal("600"),
        ask_depth=Decimal("500"),
        volume_semantics=VolumeSemantics(
            kind=VolumeKind.INTERVAL,
            unit=QuantityUnit.BASE_ASSET,
            aggregation_window_ms=1000,
            origin=EvidenceOrigin.NATIVE,
        ),
        depth_semantics=DepthSemantics(
            unit=QuantityUnit.BASE_ASSET,
            levels=1,
            origin=EvidenceOrigin.NATIVE,
        ),
    )
    event.update(overrides)
    return QuoteEvent.model_validate(event)


def test_quote_event_v1_remains_level1_compatible() -> None:
    event = QuoteEvent.model_validate(base_event())

    assert event.schema_version == "1.0"
    assert event.capabilities == frozenset({MarketEvidenceCapability.LEVEL1_QUOTE})
    assert event.volume is None
    assert event.bid_depth is None


def test_level1_serialization_omits_unavailable_evidence() -> None:
    event = QuoteEvent.model_validate(base_event())

    payload = event.model_dump(mode="json")
    assert payload["capabilities"] == ["level1_quote"]
    for field in (
        "volume",
        "buy_volume",
        "sell_volume",
        "trade_count",
        "bid_depth",
        "ask_depth",
        "volume_semantics",
        "depth_semantics",
    ):
        assert field not in payload


def test_rich_v11_event_preserves_units_capabilities_and_observed_zero() -> None:
    event = rich_event(volume=Decimal("0"), buy_volume=Decimal("0"), sell_volume=Decimal("0"))

    assert event.volume == Decimal("0")
    assert event.volume_semantics is not None
    assert event.volume_semantics.aggregation_window_ms == 1000
    assert MarketEvidenceCapability.AGGRESSOR_FLOW in event.capabilities
    assert event.depth_semantics is not None
    assert event.depth_semantics.levels == 1
    payload = event.model_dump(mode="json")
    assert payload["volume"] == "0"
    assert payload["capabilities"] == sorted(payload["capabilities"])


def test_rich_fields_require_schema_v11() -> None:
    with pytest.raises(ValidationError, match="schema_version 1.1"):
        QuoteEvent.model_validate(
            base_event(
                volume=Decimal("10"),
                capabilities=[
                    MarketEvidenceCapability.LEVEL1_QUOTE,
                    MarketEvidenceCapability.VOLUME,
                ],
                volume_semantics={
                    "kind": "interval",
                    "unit": "base_asset",
                    "aggregation_window_ms": 1000,
                    "origin": "native",
                },
            )
        )


def test_interval_volume_requires_explicit_window() -> None:
    with pytest.raises(ValidationError, match="aggregation_window_ms"):
        VolumeSemantics(
            kind=VolumeKind.INTERVAL,
            unit=QuantityUnit.BASE_ASSET,
            origin=EvidenceOrigin.NATIVE,
        )


def test_quote_notional_requires_currency_and_base_unit_rejects_currency() -> None:
    with pytest.raises(ValidationError, match="requires currency"):
        VolumeSemantics(
            kind=VolumeKind.INTERVAL,
            unit=QuantityUnit.QUOTE_NOTIONAL,
            aggregation_window_ms=1000,
            origin=EvidenceOrigin.NATIVE,
        )

    with pytest.raises(ValidationError, match="only valid"):
        DepthSemantics(
            unit=QuantityUnit.BASE_ASSET,
            currency="USD",
            origin=EvidenceOrigin.NATIVE,
        )


def test_evidence_fields_require_declared_capabilities() -> None:
    with pytest.raises(ValidationError, match="volume capability"):
        rich_event(
            capabilities=frozenset(
                {
                    MarketEvidenceCapability.LEVEL1_QUOTE,
                    MarketEvidenceCapability.AGGRESSOR_FLOW,
                    MarketEvidenceCapability.TRADE_COUNT,
                    MarketEvidenceCapability.TOP_OF_BOOK_DEPTH,
                }
            )
        )


def test_aggressor_flow_requires_both_sides_and_cannot_exceed_total() -> None:
    with pytest.raises(ValidationError, match="provided together"):
        rich_event(sell_volume=None)

    with pytest.raises(ValidationError, match="must not exceed"):
        rich_event(
            volume=Decimal("100"),
            buy_volume=Decimal("80"),
            sell_volume=Decimal("30"),
        )


def test_depth_requires_both_sides_top_level_semantics_and_capability() -> None:
    with pytest.raises(ValidationError, match="provided together"):
        rich_event(ask_depth=None)

    with pytest.raises(ValidationError, match="exactly one"):
        rich_event(
            depth_semantics=DepthSemantics(
                unit=QuantityUnit.BASE_ASSET,
                levels=2,
                origin=EvidenceOrigin.NATIVE,
            )
        )

    with pytest.raises(ValidationError, match="top_of_book_depth capability"):
        rich_event(
            capabilities=frozenset(
                {
                    MarketEvidenceCapability.LEVEL1_QUOTE,
                    MarketEvidenceCapability.VOLUME,
                    MarketEvidenceCapability.AGGRESSOR_FLOW,
                    MarketEvidenceCapability.TRADE_COUNT,
                }
            )
        )


def test_negative_evidence_is_rejected() -> None:
    with pytest.raises(ValidationError):
        rich_event(volume=Decimal("-1"))
    with pytest.raises(ValidationError):
        rich_event(bid_depth=Decimal("-1"))


def test_ledger_hash_covers_rich_evidence_fields(tmp_path: Path) -> None:
    output = tmp_path / "rich-ledger.jsonl"
    event = rich_event()

    with AtomicJsonlWriter(output, session_id="rich-session") as writer:
        writer.write(event.model_dump(mode="json"))

    assert verify_jsonl_ledger(output).records == 1
    row: dict[str, Any] = json.loads(output.read_text(encoding="utf-8"))
    original_hash = row["record_hash"]
    canonical = dict(row)
    canonical.pop("record_hash")
    assert original_hash == hashlib.sha256(
        json.dumps(
            canonical,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        ).encode("utf-8")
    ).hexdigest()

    row["volume"] = "999999"
    output.write_text(
        json.dumps(row, sort_keys=True, separators=(",", ":")) + "\n",
        encoding="utf-8",
    )

    with pytest.raises(LedgerIntegrityError, match="record_hash mismatch"):
        verify_jsonl_ledger(output)
