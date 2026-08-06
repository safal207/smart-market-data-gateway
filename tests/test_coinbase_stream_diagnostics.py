from __future__ import annotations

from smart_market_data_gateway.providers.coinbase import (
    CoinbaseProtocolError,
    CoinbaseResearchConfig,
    CoinbaseResearchMarketDataProvider,
    CoinbaseStreamDiagnostics,
)


def _provider() -> CoinbaseResearchMarketDataProvider:
    return CoinbaseResearchMarketDataProvider(
        CoinbaseResearchConfig(
            url="ws://127.0.0.1:1",
            use_mode="personal_research",
            market_data_terms_accepted=True,
            environment="research",
        )
    )


def test_stream_diagnostics_records_counters_never_payloads() -> None:
    diagnostics = CoinbaseStreamDiagnostics()
    diagnostics.record_raw_message("ticker")
    diagnostics.record_raw_message("heartbeats")
    diagnostics.record_raw_message("ticker")
    diagnostics.record_projected_event()
    diagnostics.record_projected_event()
    diagnostics.record_rejected(CoinbaseProtocolError("malformed"))
    diagnostics.record_queue_depth(3)
    diagnostics.record_subscription_acknowledgement("ticker")
    diagnostics.finalize_reader("ended")

    payload = diagnostics.as_dict()

    assert payload["raw_messages_received_total"] == 3
    assert payload["raw_messages_by_channel"] == {"heartbeats": 1, "ticker": 2}
    assert payload["projected_quote_events_total"] == 2
    assert payload["rejected_messages_total"] == 1
    assert payload["rejected_messages_by_exception_type"] == {
        "CoinbaseProtocolError": 1
    }
    assert payload["queue_high_water_mark"] == 3
    assert payload["subscription_acknowledgements"] == {"ticker": 1}
    assert payload["reader_final_state"] == "ended"
    assert payload["first_message_at"] is not None
    assert payload["last_message_at"] is not None
    assert payload["maximum_raw_message_gap_seconds"] >= 0
    assert payload["maximum_projected_event_gap_seconds"] >= 0
    assert payload["provider_state"] == "disconnected"
    assert payload["provider_message"] is None


def test_stream_diagnostics_reset_clears_session() -> None:
    diagnostics = CoinbaseStreamDiagnostics()
    diagnostics.record_raw_message("ticker")
    diagnostics.record_projected_event()
    diagnostics.finalize_reader("failed", "ConnectionResetError")

    diagnostics.reset()

    payload = diagnostics.as_dict()
    assert payload["raw_messages_received_total"] == 0
    assert payload["raw_messages_by_channel"] == {}
    assert payload["projected_quote_events_total"] == 0
    assert payload["rejected_messages_total"] == 0
    assert payload["queue_high_water_mark"] == 0
    assert payload["first_message_at"] is None
    assert payload["last_message_at"] is None
    assert payload["reader_final_state"] == "never_started"
    assert payload["reader_final_error_type"] is None
    assert payload["provider_state"] == "disconnected"


def test_stream_diagnostics_tracks_unknown_and_unparsed_channels() -> None:
    diagnostics = CoinbaseStreamDiagnostics()
    diagnostics.record_raw_message("unknown")
    diagnostics.record_raw_message("unparsed")
    diagnostics.record_raw_message("unparsed")

    payload = diagnostics.as_dict()

    assert payload["raw_messages_received_total"] == 3
    assert payload["raw_messages_by_channel"] == {"unknown": 1, "unparsed": 2}


def test_provider_parses_subscription_ack_with_channels_field() -> None:
    provider = _provider()

    provider._record_subscription_acknowledgements(
        {
            "channel": "subscriptions",
            "events": [
                {
                    "type": "subscriptions",
                    "channels": [{"name": "ticker"}, {"name": "heartbeats"}],
                }
            ],
        }
    )

    assert provider.diagnostics["subscription_acknowledgements"] == {
        "ticker": 1,
        "heartbeats": 1,
    }
    assert provider.diagnostics["rejected_messages_total"] == 0


def test_provider_parses_subscription_ack_with_channel_map() -> None:
    provider = _provider()

    provider._record_subscription_acknowledgements(
        {
            "channel": "subscriptions",
            "events": [
                {
                    "type": "subscriptions",
                    "subscriptions": {
                        "heartbeats": ["BTC-USD"],
                        "market_trades": ["BTC-USD"],
                    },
                }
            ],
        }
    )

    assert provider.diagnostics["subscription_acknowledgements"] == {
        "heartbeats": 1,
        "market_trades": 1,
    }
    assert provider.diagnostics["rejected_messages_total"] == 0


def test_provider_parses_subscription_ack_with_top_level_channels() -> None:
    provider = _provider()

    provider._record_subscription_acknowledgements(
        {"channel": "subscriptions", "channels": [{"name": "market_trades"}]}
    )

    assert provider.diagnostics["subscription_acknowledgements"] == {
        "market_trades": 1
    }
    assert provider.diagnostics["rejected_messages_total"] == 0


def test_provider_ignores_unknown_subscription_ack_shape_without_rejection() -> None:
    provider = _provider()

    provider._record_subscription_acknowledgements(
        {"channel": "subscriptions", "events": [{"type": "subscriptions"}]}
    )

    assert provider.diagnostics["subscription_acknowledgements"] == {}
    assert provider.diagnostics["rejected_messages_total"] == 0
