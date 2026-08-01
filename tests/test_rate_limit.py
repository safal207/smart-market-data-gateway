from smart_market_data_gateway.rate_limit import RedisTokenBucket


async def test_token_bucket_enforces_burst_and_refills(redis_client) -> None:
    bucket = RedisTokenBucket(redis_client)

    first, remaining = await bucket.allow(
        "client-1",
        requests_per_minute=60,
        burst_capacity=2,
    )
    second, _ = await bucket.allow(
        "client-1",
        requests_per_minute=60,
        burst_capacity=2,
    )
    third, _ = await bucket.allow(
        "client-1",
        requests_per_minute=60,
        burst_capacity=2,
    )

    assert first is True
    assert remaining == 1
    assert second is True
    assert third is False
