from pathlib import Path
import os

import asyncpg
import pytest

from smart_market_data_gateway.migrations import (
    MigrationError,
    apply_migrations,
    discover_migrations,
    migration_checksum,
)


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


async def test_authoritative_migration_is_idempotent() -> None:
    connection = await asyncpg.connect(database_url())
    try:
        first = await apply_migrations(connection, Path("migrations"))
        second = await apply_migrations(connection, Path("migrations"))
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
