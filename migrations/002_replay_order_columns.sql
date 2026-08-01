-- Upgrade prototype databases that created quote_events before the migration runner.

ALTER TABLE quote_events
    ADD COLUMN IF NOT EXISTS source_stream_ms BIGINT;
ALTER TABLE quote_events
    ADD COLUMN IF NOT EXISTS source_stream_sequence BIGINT;

CREATE INDEX IF NOT EXISTS quote_events_replay_order_idx
    ON quote_events (
        source_stream_ms NULLS LAST,
        source_stream_sequence NULLS LAST,
        accepted_at,
        event_id
    );
