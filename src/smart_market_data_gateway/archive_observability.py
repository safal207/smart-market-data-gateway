from __future__ import annotations

import asyncio
from dataclasses import asdict, dataclass
import logging
import time
from typing import Any, Mapping

from prometheus_client import CollectorRegistry, Counter, Gauge
from redis.asyncio import Redis
from redis.exceptions import RedisError, ResponseError

from smart_market_data_gateway.config import Settings

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class CandleArchiveConsumerHealth:
    stream_length_entries: int
    group_present: bool
    consumer_count: int
    pending_entries: int
    undelivered_entries: int
    backlog_entries: int
    backlog_ratio: float
    trim_headroom_entries: int
    oldest_backlog_age_seconds: float

    def model_dump(self) -> dict[str, Any]:
        return asdict(self)


class CandleArchiveMetrics:
    """Dedicated Prometheus registry for the archive worker process."""

    def __init__(self, config: Settings) -> None:
        self.registry = CollectorRegistry(auto_describe=True)
        self.stream_maxlen_entries = Gauge(
            "smdg_candle_archive_stream_maxlen_entries",
            "Configured approximate maximum retained quote stream entries",
            registry=self.registry,
        )
        self.monitor_up = Gauge(
            "smdg_candle_archive_monitor_up",
            "Whether the most recent archive consumer health sample succeeded",
            registry=self.registry,
        )
        self.consumer_group_present = Gauge(
            "smdg_candle_archive_consumer_group_present",
            "Whether the configured archive Redis consumer group exists",
            registry=self.registry,
        )
        self.consumers = Gauge(
            "smdg_candle_archive_consumers",
            "Consumers registered in the candle archive Redis group",
            registry=self.registry,
        )
        self.stream_length_entries = Gauge(
            "smdg_candle_archive_stream_length_entries",
            "Entries currently retained in the shared quote stream",
            registry=self.registry,
        )
        self.pending_entries = Gauge(
            "smdg_candle_archive_pending_entries",
            "Quote entries delivered to archive consumers but not acknowledged",
            registry=self.registry,
        )
        self.undelivered_entries = Gauge(
            "smdg_candle_archive_undelivered_entries",
            "Quote entries not yet delivered to the candle archive group",
            registry=self.registry,
        )
        self.backlog_entries = Gauge(
            "smdg_candle_archive_backlog_entries",
            "Pending plus undelivered quote entries for the candle archive",
            registry=self.registry,
        )
        self.backlog_ratio = Gauge(
            "smdg_candle_archive_backlog_ratio",
            "Archive backlog divided by configured quote stream max length",
            registry=self.registry,
        )
        self.trim_headroom_entries = Gauge(
            "smdg_candle_archive_trim_headroom_entries",
            "Remaining entries before archive backlog reaches configured stream max length",
            registry=self.registry,
        )
        self.oldest_backlog_age_seconds = Gauge(
            "smdg_candle_archive_oldest_backlog_age_seconds",
            "Age of the oldest pending or undelivered archive stream entry",
            registry=self.registry,
        )
        self.monitor_errors = Counter(
            "smdg_candle_archive_monitor_errors_total",
            "Archive consumer health sampling failures",
            registry=self.registry,
        )
        self.stream_maxlen_entries.set(config.stream_maxlen)


def _mapping_value(mapping: Mapping[Any, Any], key: str, default: Any = None) -> Any:
    if key in mapping:
        return mapping[key]
    encoded = key.encode()
    if encoded in mapping:
        return mapping[encoded]
    return default


def _text(value: Any) -> str:
    if isinstance(value, bytes):
        return value.decode()
    return str(value)


def _stream_id_age_seconds(stream_id: str, now_ms: int) -> float:
    try:
        timestamp_ms = int(stream_id.split("-", 1)[0])
    except (AttributeError, TypeError, ValueError):
        return 0.0
    return max(0.0, (now_ms - timestamp_ms) / 1000.0)


async def _first_stream_entry_age_seconds(
    redis: Redis,
    stream: str,
    minimum: str,
    now_ms: int,
) -> float:
    rows = await redis.xrange(stream, min=minimum, max="+", count=1)
    if not rows:
        return 0.0
    return _stream_id_age_seconds(_text(rows[0][0]), now_ms)


async def collect_candle_archive_consumer_health(
    redis: Redis,
    config: Settings,
) -> CandleArchiveConsumerHealth:
    """Read Redis consumer-group state without changing delivery ownership."""

    stream = config.quote_stream
    group = config.candle_archive_group
    maxlen = max(1, config.stream_maxlen)
    now_ms = int(time.time() * 1000)
    stream_length = int(await redis.xlen(stream))

    try:
        groups = await redis.xinfo_groups(stream)
    except ResponseError as exc:
        if "no such key" not in str(exc).lower():
            raise
        groups = []

    selected: Mapping[Any, Any] | None = None
    for candidate in groups:
        if _text(_mapping_value(candidate, "name", "")) == group:
            selected = candidate
            break

    if selected is None:
        backlog = stream_length
        oldest_age = (
            await _first_stream_entry_age_seconds(redis, stream, "-", now_ms)
            if stream_length
            else 0.0
        )
        return CandleArchiveConsumerHealth(
            stream_length_entries=stream_length,
            group_present=False,
            consumer_count=0,
            pending_entries=0,
            undelivered_entries=stream_length,
            backlog_entries=backlog,
            backlog_ratio=backlog / maxlen,
            trim_headroom_entries=max(0, maxlen - backlog),
            oldest_backlog_age_seconds=oldest_age,
        )

    pending = int(_mapping_value(selected, "pending", 0) or 0)
    raw_lag = _mapping_value(selected, "lag")
    if raw_lag is None:
        raise ValueError("Redis did not report archive consumer-group lag")
    undelivered = max(0, int(raw_lag))
    consumers = int(_mapping_value(selected, "consumers", 0) or 0)
    last_delivered_id = _text(
        _mapping_value(selected, "last-delivered-id", "0-0")
    )

    ages: list[float] = []
    if pending:
        pending_rows = await redis.xpending_range(
            stream,
            group,
            min="-",
            max="+",
            count=1,
        )
        if pending_rows:
            message_id = _mapping_value(pending_rows[0], "message_id", "0-0")
            ages.append(_stream_id_age_seconds(_text(message_id), now_ms))

    if undelivered:
        minimum = "-" if last_delivered_id == "0-0" else f"({last_delivered_id}"
        ages.append(await _first_stream_entry_age_seconds(redis, stream, minimum, now_ms))

    backlog = pending + undelivered
    return CandleArchiveConsumerHealth(
        stream_length_entries=stream_length,
        group_present=True,
        consumer_count=consumers,
        pending_entries=pending,
        undelivered_entries=undelivered,
        backlog_entries=backlog,
        backlog_ratio=backlog / maxlen,
        trim_headroom_entries=max(0, maxlen - backlog),
        oldest_backlog_age_seconds=max(ages, default=0.0),
    )


class CandleArchiveMonitor:
    """Periodically exports archive consumer lag and trim-safety metrics."""

    def __init__(self, redis: Redis, config: Settings, metrics: CandleArchiveMetrics) -> None:
        self.redis = redis
        self.config = config
        self.metrics = metrics
        self.last_snapshot: CandleArchiveConsumerHealth | None = None
        self._closed = asyncio.Event()

    async def sample(self) -> CandleArchiveConsumerHealth | None:
        try:
            snapshot = await collect_candle_archive_consumer_health(self.redis, self.config)
        except (RedisError, TypeError, ValueError):
            self.metrics.monitor_up.set(0)
            self.metrics.monitor_errors.inc()
            logger.warning(
                "Candle archive consumer health sampling failed",
                extra={"event": "candle_archive_monitor_failed"},
                exc_info=True,
            )
            return None

        self.last_snapshot = snapshot
        self.metrics.monitor_up.set(1)
        self.metrics.consumer_group_present.set(1 if snapshot.group_present else 0)
        self.metrics.consumers.set(snapshot.consumer_count)
        self.metrics.stream_length_entries.set(snapshot.stream_length_entries)
        self.metrics.pending_entries.set(snapshot.pending_entries)
        self.metrics.undelivered_entries.set(snapshot.undelivered_entries)
        self.metrics.backlog_entries.set(snapshot.backlog_entries)
        self.metrics.backlog_ratio.set(snapshot.backlog_ratio)
        self.metrics.trim_headroom_entries.set(snapshot.trim_headroom_entries)
        self.metrics.oldest_backlog_age_seconds.set(
            snapshot.oldest_backlog_age_seconds
        )
        return snapshot

    async def run(self) -> None:
        while not self._closed.is_set():
            await self.sample()
            try:
                await asyncio.wait_for(
                    self._closed.wait(),
                    timeout=self.config.candle_archive_metrics_interval_seconds,
                )
            except TimeoutError:
                pass

    async def close(self) -> None:
        self._closed.set()
