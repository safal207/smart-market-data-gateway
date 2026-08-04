from __future__ import annotations

import asyncio
import logging

from redis.asyncio import Redis

from smart_market_data_gateway.candle_archive import CandleArchiveSink
from smart_market_data_gateway.config import settings
from smart_market_data_gateway.logging import configure_logging

logger = logging.getLogger(__name__)


class RecoveringCandleArchiveSink(CandleArchiveSink):
    """Archive worker that reclaims deliveries abandoned by crashed consumers."""

    async def run(self) -> None:
        if not self.archive.available:
            await self.start()
        while not self._closed:
            try:
                messages = await self.store.read_group(
                    self.config.quote_stream,
                    self.config.candle_archive_group,
                    self.consumer_name,
                    count=200,
                    block_ms=1000,
                )
                if not messages:
                    messages = await self._claim_stale()
                for stream_id, fields in messages:
                    await self._persist(stream_id, fields)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception(
                    "candle archive worker loop failed",
                    extra={"event": "candle_archive_worker_failed"},
                )
                await asyncio.sleep(1)

    async def _claim_stale(self) -> list[tuple[str, dict[str, str]]]:
        claimed = await self.redis.xautoclaim(
            self.config.quote_stream,
            self.config.candle_archive_group,
            self.consumer_name,
            min_idle_time=self.config.candle_archive_claim_idle_ms,
            start_id="0-0",
            count=200,
        )
        entries = claimed[1] if isinstance(claimed, (list, tuple)) and len(claimed) > 1 else []
        return [
            (
                str(stream_id),
                {str(key): str(value) for key, value in fields.items()},
            )
            for stream_id, fields in entries
        ]


async def _run() -> None:
    configure_logging(settings.log_level)
    redis = Redis.from_url(settings.redis_url, decode_responses=True)
    sink = RecoveringCandleArchiveSink(redis, settings)
    try:
        await sink.run()
    finally:
        await sink.close()
        await redis.aclose()


def main() -> None:
    asyncio.run(_run())


if __name__ == "__main__":
    main()
