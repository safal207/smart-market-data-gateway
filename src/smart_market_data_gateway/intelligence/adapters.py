from datetime import datetime
from decimal import Decimal

from smart_market_data_gateway.domain.models import QuoteEvent
from smart_market_data_gateway.intelligence.models import (
    EvidenceRef,
    MarketObservation,
    MetricValue,
    ObservationKind,
)


def quote_event_to_observations(
    event: QuoteEvent,
    *,
    record_hash: str | None = None,
    ledger_index: int | None = None,
    stale: bool = False,
    age_ms: int = 0,
    expires_at: datetime | None = None,
    generation: int | None = None,
) -> tuple[MarketObservation, ...]:
    """Translate one normalized QuoteEvent into typed TMI observations.

    The adapter preserves provider semantics and never fabricates unsupported
    evidence. Rich observations are emitted only when the corresponding values
    exist on the validated QuoteEvent.
    """

    if age_ms < 0:
        raise ValueError("age_ms must be non-negative")
    if ledger_index is not None and record_hash is None:
        raise ValueError("ledger_index requires record_hash")

    evidence_ref = EvidenceRef(
        provenance_system="smart-market-data-gateway",
        provenance_component=event.provider,
        locator=f"quote-event:{event.event_id}",
        observed_at=event.provider_timestamp,
        record_hash=record_hash,
        ledger_index=ledger_index,
    )
    common = {
        "symbol": event.symbol,
        "observed_at": event.provider_timestamp,
        "received_at": event.received_at,
        "source": event.provider,
        "confidence": Decimal("1"),
        "evidence": (evidence_ref,),
        "expires_at": expires_at,
        "generation": generation,
    }

    quote_metrics: dict[str, MetricValue] = {
        "schema_version": event.schema_version,
        "price": event.price,
        "stale": stale,
        "age_ms": age_ms,
    }
    _put_if_present(quote_metrics, "bid", event.bid)
    _put_if_present(quote_metrics, "ask", event.ask)
    _put_if_present(quote_metrics, "sequence", event.sequence)
    observations: list[MarketObservation] = [
        MarketObservation(
            **common,
            kind=ObservationKind.QUOTE,
            fact=f"{event.symbol} quote observed at {event.price}",
            metrics=quote_metrics,
        )
    ]

    if event.volume is not None or event.trade_count is not None:
        volume_metrics: dict[str, MetricValue] = {}
        _put_if_present(volume_metrics, "volume", event.volume)
        _put_if_present(volume_metrics, "trade_count", event.trade_count)
        _add_volume_semantics(volume_metrics, event)
        observations.append(
            MarketObservation(
                **common,
                kind=ObservationKind.VOLUME,
                fact=f"{event.symbol} volume evidence observed",
                metrics=volume_metrics,
            )
        )

    if event.buy_volume is not None and event.sell_volume is not None:
        flow_metrics: dict[str, MetricValue] = {
            "buy_volume": event.buy_volume,
            "sell_volume": event.sell_volume,
            "net_aggressor_flow": event.buy_volume - event.sell_volume,
        }
        _add_volume_semantics(flow_metrics, event)
        observations.append(
            MarketObservation(
                **common,
                kind=ObservationKind.AGGRESSOR_FLOW,
                fact=f"{event.symbol} aggressor-flow evidence observed",
                metrics=flow_metrics,
            )
        )

    if event.bid_depth is not None and event.ask_depth is not None:
        depth_metrics: dict[str, MetricValue] = {
            "bid_depth": event.bid_depth,
            "ask_depth": event.ask_depth,
            "depth_imbalance": event.bid_depth - event.ask_depth,
        }
        if event.depth_semantics is not None:
            depth_metrics.update(
                {
                    "depth_unit": event.depth_semantics.unit.value,
                    "depth_levels": event.depth_semantics.levels,
                    "depth_origin": event.depth_semantics.origin.value,
                }
            )
            _put_if_present(depth_metrics, "depth_currency", event.depth_semantics.currency)
        observations.append(
            MarketObservation(
                **common,
                kind=ObservationKind.TOP_OF_BOOK_DEPTH,
                fact=f"{event.symbol} top-of-book depth observed",
                metrics=depth_metrics,
            )
        )

    return tuple(observations)


def _add_volume_semantics(metrics: dict[str, MetricValue], event: QuoteEvent) -> None:
    semantics = event.volume_semantics
    if semantics is None:
        return
    metrics.update(
        {
            "volume_kind": semantics.kind.value,
            "volume_unit": semantics.unit.value,
            "volume_origin": semantics.origin.value,
        }
    )
    _put_if_present(metrics, "aggregation_window_ms", semantics.aggregation_window_ms)
    _put_if_present(metrics, "volume_currency", semantics.currency)


def _put_if_present(
    target: dict[str, MetricValue],
    key: str,
    value: MetricValue | None,
) -> None:
    if value is not None:
        target[key] = value
