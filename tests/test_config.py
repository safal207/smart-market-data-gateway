import pytest
from pydantic import ValidationError

from smart_market_data_gateway.config import Settings


def test_candle_retention_must_be_positive() -> None:
    with pytest.raises(ValidationError):
        Settings(candle_history_retention_seconds=0)


def test_candle_retry_limit_must_be_positive() -> None:
    with pytest.raises(ValidationError):
        Settings(candle_update_retry_limit=0)


def test_candle_archive_retention_must_be_positive() -> None:
    with pytest.raises(ValidationError):
        Settings(candle_archive_retention_seconds=0)


def test_candle_archive_pool_size_is_bounded() -> None:
    with pytest.raises(ValidationError):
        Settings(candle_archive_pool_max_size=0)
    with pytest.raises(ValidationError):
        Settings(candle_archive_pool_max_size=33)
