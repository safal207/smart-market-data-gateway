from types import SimpleNamespace

from smart_market_data_gateway import candle_api
from smart_market_data_gateway.config import Settings


async def test_archive_reader_reconnects_and_replaces_hot_only_history(monkeypatch) -> None:
    config = Settings(
        candle_archive_enabled=True,
        database_url="postgresql://archive.test/smdg",
    )
    archive = FakeArchive()
    state = SimpleNamespace(
        store=SimpleNamespace(settings=config),
        candle_archive=None,
        candle_history="hot-only",
    )
    request = SimpleNamespace(app=SimpleNamespace(state=state))
    monkeypatch.setattr(candle_api, "PostgresCandleArchive", lambda _config: archive)
    monkeypatch.setattr(
        candle_api,
        "HybridCandleHistoryStore",
        lambda store, connected, archive_config: (store, connected, archive_config),
    )

    await candle_api._ensure_archive_reader(request)

    assert archive.started == 1
    assert state.candle_archive is archive
    assert state.candle_history == (state.store, archive, config)
    assert state.candle_archive_retry_at == 0.0


async def test_archive_reader_applies_backoff_after_failed_reconnect(monkeypatch) -> None:
    config = Settings(
        candle_archive_enabled=True,
        database_url="postgresql://archive.test/smdg",
    )
    archives: list[FakeArchive] = []

    def archive_factory(_config):
        archive = FakeArchive(fail=True)
        archives.append(archive)
        return archive

    state = SimpleNamespace(
        store=SimpleNamespace(settings=config),
        candle_archive=None,
        candle_history="hot-only",
    )
    request = SimpleNamespace(app=SimpleNamespace(state=state))
    monkeypatch.setattr(candle_api, "PostgresCandleArchive", archive_factory)
    monkeypatch.setattr(candle_api.time, "monotonic", lambda: 100.0)

    await candle_api._ensure_archive_reader(request)
    await candle_api._ensure_archive_reader(request)

    assert len(archives) == 1
    assert archives[0].started == 1
    assert archives[0].closed == 1
    assert state.candle_archive is None
    assert state.candle_history == "hot-only"
    assert state.candle_archive_retry_at == 130.0


class FakeArchive:
    def __init__(self, *, fail: bool = False) -> None:
        self.fail = fail
        self.available = False
        self.started = 0
        self.closed = 0

    async def start(self) -> None:
        self.started += 1
        if self.fail:
            raise RuntimeError("database unavailable")
        self.available = True

    async def close(self) -> None:
        self.closed += 1
        self.available = False
