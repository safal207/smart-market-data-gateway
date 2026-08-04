from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram


class GatewayMetrics:
    def __init__(self) -> None:
        self.registry = CollectorRegistry(auto_describe=True)
        self.active_connections = Gauge(
            "smdg_active_websocket_connections",
            "Active WebSocket connections",
            registry=self.registry,
        )
        self.client_subscriptions = Gauge(
            "smdg_client_subscriptions",
            "Active client subscriptions",
            registry=self.registry,
        )
        self.unique_upstream_subscriptions = Gauge(
            "smdg_unique_upstream_subscriptions",
            "Unique logical upstream subscriptions",
            registry=self.registry,
        )
        self.aggregation_ratio = Gauge(
            "smdg_subscription_aggregation_ratio",
            "Client subscriptions divided by unique upstream subscriptions",
            registry=self.registry,
        )
        self.provider_events = Counter(
            "smdg_provider_events_total",
            "Provider events ingested",
            ["provider"],
            registry=self.registry,
        )
        self.delivered_events = Counter(
            "smdg_delivered_events_total",
            "Events delivered to clients",
            ["tier"],
            registry=self.registry,
        )
        self.deduplicated_events = Counter(
            "smdg_deduplicated_events_total",
            "Duplicate events dropped",
            registry=self.registry,
        )
        self.gap_events = Counter(
            "smdg_sequence_gap_events_total",
            "Sequence gaps and out-of-order events detected",
            ["kind"],
            registry=self.registry,
        )
        self.queue_depth = Gauge(
            "smdg_delivery_queue_depth",
            "Current latest-value queue depth",
            ["connection_id"],
            registry=self.registry,
        )
        self.coalesced_events = Counter(
            "smdg_coalesced_quote_events_total",
            "Older unsent quote events replaced by newer values",
            ["tier"],
            registry=self.registry,
        )
        self.delivery_latency = Histogram(
            "smdg_delivery_latency_seconds",
            "Provider timestamp to client delivery latency",
            ["tier"],
            buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5),
            registry=self.registry,
        )
        self.provider_reconnects = Counter(
            "smdg_provider_reconnects_total",
            "Provider reconnect attempts",
            ["provider"],
            registry=self.registry,
        )
        self.provider_connected = Gauge(
            "smdg_provider_connected",
            "Whether a provider adapter is currently connected",
            ["provider"],
            registry=self.registry,
        )
        self.provider_outage_duration = Histogram(
            "smdg_provider_outage_duration_seconds",
            "Provider outage duration observed before reconnect",
            ["provider"],
            buckets=(0.1, 0.5, 1, 2, 5, 10, 30, 60, 300),
            registry=self.registry,
        )
        self.redis_pending_entries = Gauge(
            "smdg_redis_pending_entries",
            "Pending entries for the processing consumer group",
            registry=self.registry,
        )
        self.candle_archive_enabled = Gauge(
            "smdg_candle_archive_enabled",
            "Whether durable candle archiving is enabled",
            registry=self.registry,
        )
        self.candle_archive_monitor_up = Gauge(
            "smdg_candle_archive_monitor_up",
            "Whether the most recent archive consumer health sample succeeded",
            registry=self.registry,
        )
        self.candle_archive_consumer_group_present = Gauge(
            "smdg_candle_archive_consumer_group_present",
            "Whether the configured archive Redis consumer group exists",
            registry=self.registry,
        )
        self.candle_archive_consumers = Gauge(
            "smdg_candle_archive_consumers",
            "Consumers registered in the candle archive Redis group",
            registry=self.registry,
        )
        self.candle_archive_stream_length_entries = Gauge(
            "smdg_candle_archive_stream_length_entries",
            "Entries currently retained in the shared quote stream",
            registry=self.registry,
        )
        self.candle_archive_pending_entries = Gauge(
            "smdg_candle_archive_pending_entries",
            "Quote entries delivered to archive consumers but not acknowledged",
            registry=self.registry,
        )
        self.candle_archive_undelivered_entries = Gauge(
            "smdg_candle_archive_undelivered_entries",
            "Quote entries not yet delivered to the candle archive group",
            registry=self.registry,
        )
        self.candle_archive_backlog_entries = Gauge(
            "smdg_candle_archive_backlog_entries",
            "Pending plus undelivered quote entries for the candle archive",
            registry=self.registry,
        )
        self.candle_archive_backlog_ratio = Gauge(
            "smdg_candle_archive_backlog_ratio",
            "Archive backlog divided by configured quote stream max length",
            registry=self.registry,
        )
        self.candle_archive_trim_headroom_entries = Gauge(
            "smdg_candle_archive_trim_headroom_entries",
            "Remaining stream entries before archive backlog reaches configured max length",
            registry=self.registry,
        )
        self.candle_archive_oldest_backlog_age_seconds = Gauge(
            "smdg_candle_archive_oldest_backlog_age_seconds",
            "Age of the oldest pending or undelivered archive stream entry",
            registry=self.registry,
        )
        self.candle_archive_monitor_errors = Counter(
            "smdg_candle_archive_monitor_errors_total",
            "Archive consumer health sampling failures",
            registry=self.registry,
        )
        self.auth_failures = Counter(
            "smdg_auth_failures_total",
            "Authentication or authorization failures",
            ["reason"],
            registry=self.registry,
        )
        self.stale_quotes = Counter(
            "smdg_stale_quote_reads_total",
            "REST or snapshot reads that returned stale data",
            registry=self.registry,
        )
        self.usage_queue_dropped = Counter(
            "smdg_usage_queue_dropped_total",
            "Usage events dropped because the asynchronous queue was full",
            registry=self.registry,
        )

    def update_subscription_metrics(self, clients: int, unique_symbols: int) -> None:
        self.client_subscriptions.set(clients)
        self.unique_upstream_subscriptions.set(unique_symbols)
        self.aggregation_ratio.set(clients / unique_symbols if unique_symbols else 0)
