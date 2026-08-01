import argparse
import asyncio
from dataclasses import dataclass
import hashlib
from pathlib import Path

import asyncpg

from smart_market_data_gateway.config import settings

MIGRATION_LOCK_ID = 742_903_119


class MigrationError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class AppliedMigration:
    version: str
    checksum: str
    applied: bool


def migration_checksum(sql: str) -> str:
    return hashlib.sha256(sql.encode("utf-8")).hexdigest()


def discover_migrations(directory: Path) -> tuple[Path, ...]:
    if not directory.is_dir():
        raise MigrationError(f"migration directory does not exist: {directory}")
    migrations = tuple(sorted(directory.glob("*.sql")))
    if not migrations:
        raise MigrationError(f"no SQL migrations found in {directory}")
    return migrations


async def apply_migrations(
    connection: asyncpg.Connection,
    directory: Path,
) -> tuple[AppliedMigration, ...]:
    await connection.execute(
        """
        CREATE TABLE IF NOT EXISTS schema_migrations (
            version TEXT PRIMARY KEY,
            checksum TEXT NOT NULL CHECK (checksum ~ '^[0-9a-f]{64}$'),
            applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """
    )
    await connection.execute("SELECT pg_advisory_lock($1)", MIGRATION_LOCK_ID)
    results: list[AppliedMigration] = []
    try:
        for path in discover_migrations(directory):
            sql = path.read_text(encoding="utf-8")
            checksum = migration_checksum(sql)
            existing = await connection.fetchval(
                "SELECT checksum FROM schema_migrations WHERE version = $1",
                path.name,
            )
            if existing is not None:
                if str(existing) != checksum:
                    raise MigrationError(
                        f"applied migration checksum changed: {path.name}"
                    )
                results.append(
                    AppliedMigration(path.name, checksum, applied=False)
                )
                continue

            async with connection.transaction():
                await connection.execute(sql)
                await connection.execute(
                    """
                    INSERT INTO schema_migrations (version, checksum)
                    VALUES ($1, $2)
                    """,
                    path.name,
                    checksum,
                )
            results.append(AppliedMigration(path.name, checksum, applied=True))
    finally:
        await connection.execute("SELECT pg_advisory_unlock($1)", MIGRATION_LOCK_ID)
    return tuple(results)


async def migrate(database_url: str, directory: Path) -> tuple[AppliedMigration, ...]:
    connection = await asyncpg.connect(
        dsn=database_url,
        command_timeout=settings.history_command_timeout_seconds,
    )
    try:
        return await apply_migrations(connection, directory)
    finally:
        await connection.close()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Apply ordered SMDG SQL migrations")
    parser.add_argument(
        "--database-url",
        default=settings.database_url,
        help="PostgreSQL URL; defaults to SMDG_DATABASE_URL",
    )
    parser.add_argument(
        "--migrations-dir",
        type=Path,
        default=Path("migrations"),
        help="directory containing ordered *.sql files",
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()
    if not args.database_url:
        raise SystemExit("SMDG_DATABASE_URL or --database-url is required")
    results = asyncio.run(migrate(args.database_url, args.migrations_dir))
    for result in results:
        state = "applied" if result.applied else "already_applied"
        print(f"migration={result.version} state={state} checksum={result.checksum}")


if __name__ == "__main__":
    main()
