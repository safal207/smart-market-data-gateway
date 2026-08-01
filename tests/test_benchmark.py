from smart_market_data_gateway.benchmark import BenchmarkScenario, run_benchmark


def test_smart_benchmark_reduces_subscriptions_and_events() -> None:
    result = run_benchmark(
        BenchmarkScenario(
            clients=100,
            symbols_per_client=5,
            symbol_universe=20,
            events_per_symbol=20,
            provider_events_per_second=10,
            seed=207,
        )
    )
    assert result["smart"]["provider_subscriptions"] < result["baseline"][
        "provider_subscriptions"
    ]
    assert result["smart"]["delivered_events"] < result["baseline"][
        "delivered_events"
    ]
    assert result["comparison"]["provider_subscription_reduction_percent"] > 0
    assert result["comparison"]["delivered_event_reduction_percent"] > 0
