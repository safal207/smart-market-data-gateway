from datetime import UTC, datetime
from typing import Any


def parse_utc_timestamp(value: Any) -> datetime:
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("quote timestamp must be timezone-aware")
    return parsed.astimezone(UTC)


def quote_was_received_after(quote: dict[str, Any], cutoff: datetime) -> bool:
    received_at = quote.get("received_at")
    if received_at is None:
        return False
    return parse_utc_timestamp(received_at) > cutoff
