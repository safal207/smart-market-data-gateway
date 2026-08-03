import os

import pytest

from smart_market_data_gateway.history import HistorySink


def history_database_url() -> str:
    database_url = os.getenv("TEST_DATABASE_URL")
    if not database_url:
        pytest.skip("TEST_DATABASE_URL is required for history safety tests")
    return database_url


def history_config(test_settings, **updates):
    return test_settings.model_copy(
        update={
            "database_url": history_database_url(),
            "enable_history_retention": False,
            **updates,
        }
    )


async def test_second_history_writer_is_rejected_until_owner_closes(
    redis_client,
    test_settings,
) -> None:
    config = history_config(test_settings)
    owner = HistorySink(redis_client, config)
    contender = HistorySink(redis_client, config)

    await owner.start()
    try:
        with pytest.raises(RuntimeError, match="another history writer is already active"):
            await contender.start()
        assert contender.pool is None
        assert contender._lock_connection is None
    finally:
        await contender.close()
        await owner.close()

    replacement = HistorySink(redis_client, config)
    await replacement.start()
    await replacement.close()


async def test_retention_fails_closed_without_integrity_checkpoints(
    redis_client,
    test_settings,
) -> None:
    unsafe = HistorySink(
        redis_client,
        history_config(test_settings, enable_history_retention=True),
    )

    with pytest.raises(RuntimeError, match="integrity-preserving checkpoints"):
        await unsafe.start()

    assert unsafe.pool is None
    assert unsafe._lock_connection is None

    safe = HistorySink(redis_client, history_config(test_settings))
    await safe.start()
    await safe.close()
