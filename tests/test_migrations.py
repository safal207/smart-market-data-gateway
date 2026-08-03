from pathlib import Path
import os
from uuid import uuid4

import asyncpg
import pytest

from smart_market_data_gateway.migrations import (
    MigrationError,
    apply_migrations,
    discover_migrations,
    migration_checksum,
)

MIGRATIONS_DIR = Path(__file__).resolve().parents[1] / "migrations"


def database_url() -> str:
    value = os.getenv("TEST_DATABASE_URL")
    if not value:
        pytest.skip("TEST_DATABASE_URL is required for migration integration tests")
    return value


def test_discover_migrations_is_ordered(tmp_path: Path) -> None:
    (tmp_path / "010_last.sql").write_text("SELECT 10;", encoding="utf-8")
    (tmp_path / "001_first.sql").write_text("SELECT 1;", encoding="utf-8")

    assert [path.name for path in discover_migrations(tmp_path)] == [
        "001_first.sql",
        "010_last.sql",
    ]


def test_discover_migrations_rejects_empty_directory(tmp_path: Path) -> None:
    with pytest.raises(MigrationError, match="no SQL migrations"):
        discover_migrations(tmp_path)


def test_migration_checksum_is_stable() -> None:
    assert migration_checksum("SELECT 1;\n") == migration_checksum("SELECT 1;\n")
    assert migration_checksum("SELECT 1;\n") != migration_checksum("SELECT 2;\n")


def test_replay_index_waits_for_prototype_column_upgrade() -> None:
    foundation = (MIGRATIONS_DIR / "001_temporal_market_foundation.sql").read_text(
        encoding="utf-8"
    )
    upgrade = (MIGRATIONS_DIR / "002_replay_order_columns.sql").read_text(
        encoding="utf-8"
    )

    assert "CREATE INDEX IF NOT EXISTS quote_events_replay_order_idx" not in foundation
    index_offset = upgrade.index("CREATE INDEX IF NOT EXISTS quote_events_replay_order_idx")
    assert upgrade.index("ADD COLUMN IF NOT EXISTS source_stream_ms") < index_offset
    assert upgrade.index("ADD COLUMN IF NOT EXISTS source_stream_sequence") < index_offset


async def test_authoritative_migration_is_idempotent() -> None:
    connection = await asyncpg.connect(database_url())
    try:
        first = await apply_migrations(connection, MIGRATIONS_DIR)
        second = await apply_migrations(connection, MIGRATIONS_DIR)
        assert any(item.version == "001_temporal_market_foundation.sql" for item in first)
        assert all(not item.applied for item in second)
        assert await connection.fetchval(
            """
            SELECT EXISTS (
                SELECT 1 FROM schema_migrations
                WHERE version = '001_temporal_market_foundation.sql'
            )
            """
        )
        assert await connection.fetchval(
            "SELECT to_regclass('public.accepted_event_integrity') IS NOT NULL"
        )
    finally:
        await connection.close()


async def test_migration_rejects_legacy_quotes_without_integrity_backfill() -> None:
    connection = await asyncpg.connect(database_url())
    schema = f"migration_legacy_{uuid4().hex}"
    try:
        await connection.execute(f'CREATE SCHEMA "{schema}"')
        await connection.execute(f'SET search_path TO "{schema}", public')
        await connection.execute(
            """
            CREATE TABLE quote_events (
                event_id UUID NOT NULL,
                provider_timestamp TIMESTAMPTZ NOT NULL
            )
            """
        )
        await connection.execute(
            "INSERT INTO quote_events (event_id, provider_timestamp) VALUES ($1, NOW())",
            uuid4(),
        )

        with pytest.raises(
            asyncpg.PostgresError,
            match="explicit accepted-event integrity backfill",
        ):
            await apply_migrations(connection, MIGRATIONS_DIR)

        assert await connection.fetchval(
            """
            SELECT COUNT(*)
            FROM schema_migrations
            WHERE version = '001_temporal_market_foundation.sql'
            """
        ) == 0
    finally:
        await connection.execute("RESET search_path")
        await connection.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await connection.close()
