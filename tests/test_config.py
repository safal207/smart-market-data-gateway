import pytest
from pydantic import ValidationError

from smart_market_data_gateway.config import Settings


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("candle_history_retention_seconds", 0),
        ("candle_update_retry_limit", 0),
    ],
)
def test_candle_settings_must_be_positive(field: str, value: int) -> None:
    with pytest.raises(ValidationError):
        Settings(**{field: value})
