CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS quote_events (
    event_id UUID NOT NULL,
    provider_timestamp TIMESTAMPTZ NOT NULL,
    received_at TIMESTAMPTZ NOT NULL,
    accepted_at TIMESTAMPTZ NOT NULL,
    data_cutoff TIMESTAMPTZ NOT NULL,
    symbol TEXT NOT NULL,
    provider TEXT NOT NULL,
    sequence BIGINT,
    price NUMERIC NOT NULL CHECK (price > 0),
    bid NUMERIC CHECK (bid > 0),
    ask NUMERIC CHECK (ask > 0),
    quality_score DOUBLE PRECISION NOT NULL CHECK (quality_score BETWEEN 0 AND 1),
    gap_detected BOOLEAN NOT NULL,
    normalization_version TEXT NOT NULL,
    source_stream_id TEXT,
    payload JSONB NOT NULL,
    persisted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (event_id, provider_timestamp)
);
SELECT create_hypertable(
    'quote_events',
    'provider_timestamp',
    if_not_exists => TRUE,
    migrate_data => TRUE
);
CREATE INDEX IF NOT EXISTS quote_events_symbol_time_idx
    ON quote_events (symbol, provider_timestamp DESC);
CREATE INDEX IF NOT EXISTS quote_events_provider_time_idx
    ON quote_events (provider, provider_timestamp DESC);

CREATE TABLE IF NOT EXISTS candles (
    symbol TEXT NOT NULL,
    interval_seconds INTEGER NOT NULL CHECK (interval_seconds > 0),
    bucket_start TIMESTAMPTZ NOT NULL,
    open NUMERIC NOT NULL CHECK (open > 0),
    high NUMERIC NOT NULL CHECK (high > 0),
    low NUMERIC NOT NULL CHECK (low > 0),
    close NUMERIC NOT NULL CHECK (close > 0),
    event_count BIGINT NOT NULL CHECK (event_count > 0),
    first_event_time TIMESTAMPTZ NOT NULL,
    last_event_time TIMESTAMPTZ NOT NULL,
    quality_score DOUBLE PRECISION NOT NULL CHECK (quality_score BETWEEN 0 AND 1),
    finalized BOOLEAN NOT NULL DEFAULT TRUE,
    schema_version TEXT NOT NULL,
    persisted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (symbol, interval_seconds, bucket_start)
);
SELECT create_hypertable(
    'candles',
    'bucket_start',
    if_not_exists => TRUE,
    migrate_data => TRUE
);
CREATE INDEX IF NOT EXISTS candles_symbol_interval_time_idx
    ON candles (symbol, interval_seconds, bucket_start DESC);

CREATE TABLE IF NOT EXISTS late_quote_events (
    event_id UUID NOT NULL,
    provider_timestamp TIMESTAMPTZ NOT NULL,
    symbol TEXT NOT NULL,
    interval_seconds INTEGER NOT NULL,
    payload JSONB NOT NULL,
    observed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (event_id, provider_timestamp, interval_seconds)
);
SELECT create_hypertable(
    'late_quote_events',
    'provider_timestamp',
    if_not_exists => TRUE,
    migrate_data => TRUE
);

CREATE TABLE IF NOT EXISTS data_quality_intervals (
    provider TEXT NOT NULL,
    symbol TEXT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL,
    ended_at TIMESTAMPTZ,
    status TEXT NOT NULL,
    reason TEXT NOT NULL,
    score DOUBLE PRECISION NOT NULL CHECK (score BETWEEN 0 AND 1),
    details JSONB NOT NULL DEFAULT '{}'::jsonb,
    PRIMARY KEY (provider, symbol, started_at)
);

CREATE TABLE IF NOT EXISTS market_sessions (
    market TEXT NOT NULL,
    session_date DATE NOT NULL,
    opens_at TIMESTAMPTZ NOT NULL,
    closes_at TIMESTAMPTZ NOT NULL,
    timezone TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'scheduled',
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    PRIMARY KEY (market, session_date),
    CHECK (closes_at > opens_at)
);

-- Retention is intentionally opt-in. Enable SMDG_ENABLE_HISTORY_RETENTION only
-- after licensing, replay, backup, and audit requirements are agreed.
