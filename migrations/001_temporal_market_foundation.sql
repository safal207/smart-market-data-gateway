-- Authoritative schema for the trusted temporal market-data foundation.
-- Applied in order by `smdg-migrate`; runtime workers only verify this schema.

DO $$
BEGIN
    CREATE EXTENSION IF NOT EXISTS timescaledb;
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'timescaledb unavailable; using plain PostgreSQL tables';
END
$$;

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
    source_stream_ms BIGINT,
    source_stream_sequence BIGINT,
    payload JSONB NOT NULL,
    persisted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (event_id, provider_timestamp),
    CHECK (
        (source_stream_ms IS NULL AND source_stream_sequence IS NULL)
        OR (source_stream_ms >= 0 AND source_stream_sequence >= 0)
    )
);
CREATE INDEX IF NOT EXISTS quote_events_symbol_time_idx
    ON quote_events (symbol, provider_timestamp DESC);
CREATE INDEX IF NOT EXISTS quote_events_provider_time_idx
    ON quote_events (provider, provider_timestamp DESC);
CREATE INDEX IF NOT EXISTS quote_events_replay_order_idx
    ON quote_events (
        source_stream_ms NULLS LAST,
        source_stream_sequence NULLS LAST,
        accepted_at,
        event_id
    );

CREATE TABLE IF NOT EXISTS accepted_event_integrity (
    chain_name TEXT NOT NULL,
    chain_sequence BIGINT NOT NULL CHECK (chain_sequence > 0),
    profile TEXT NOT NULL,
    event_id UUID NOT NULL,
    provider_timestamp TIMESTAMPTZ NOT NULL,
    source_stream_id TEXT,
    payload_digest TEXT NOT NULL CHECK (payload_digest ~ '^sha256:[0-9a-f]{64}$'),
    previous_record_hash TEXT CHECK (
        previous_record_hash IS NULL OR previous_record_hash ~ '^sha256:[0-9a-f]{64}$'
    ),
    record_hash TEXT NOT NULL CHECK (record_hash ~ '^sha256:[0-9a-f]{64}$'),
    appended_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (chain_name, chain_sequence),
    UNIQUE (chain_name, event_id, provider_timestamp),
    UNIQUE (chain_name, record_hash)
);
CREATE INDEX IF NOT EXISTS accepted_event_integrity_event_idx
    ON accepted_event_integrity (event_id, provider_timestamp);

CREATE OR REPLACE FUNCTION reject_accepted_event_integrity_mutation()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'accepted_event_integrity is append-only';
END;
$$;
DROP TRIGGER IF EXISTS accepted_event_integrity_append_only
    ON accepted_event_integrity;
CREATE TRIGGER accepted_event_integrity_append_only
BEFORE UPDATE OR DELETE ON accepted_event_integrity
FOR EACH ROW EXECUTE FUNCTION reject_accepted_event_integrity_mutation();

CREATE TABLE IF NOT EXISTS integrity_chain_heads (
    chain_name TEXT PRIMARY KEY,
    chain_sequence BIGINT NOT NULL DEFAULT 0 CHECK (chain_sequence >= 0),
    record_hash TEXT CHECK (
        record_hash IS NULL OR record_hash ~ '^sha256:[0-9a-f]{64}$'
    ),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK (
        (chain_sequence = 0 AND record_hash IS NULL)
        OR (chain_sequence > 0 AND record_hash IS NOT NULL)
    )
);
INSERT INTO integrity_chain_heads (chain_name)
VALUES ('accepted_quotes')
ON CONFLICT (chain_name) DO NOTHING;

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

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'timescaledb') THEN
        EXECUTE 'SELECT create_hypertable(''quote_events'', ''provider_timestamp'', if_not_exists => TRUE, migrate_data => TRUE)';
        EXECUTE 'SELECT create_hypertable(''candles'', ''bucket_start'', if_not_exists => TRUE, migrate_data => TRUE)';
        EXECUTE 'SELECT create_hypertable(''late_quote_events'', ''provider_timestamp'', if_not_exists => TRUE, migrate_data => TRUE)';
    END IF;
END
$$;

-- Retention is intentionally opt-in and managed reversibly by the history worker.
-- Integrity records are never covered by market-data retention policies.
