from enum import StrEnum


class HistoryFailpoint(StrEnum):
    """Stable crash boundaries for history-worker durability tests."""

    AFTER_LEDGER_APPEND_BEFORE_COMMIT = "after_ledger_append_before_commit"
    AFTER_DB_COMMIT_BEFORE_ACK = "after_db_commit_before_ack"


class HistoryCrash(BaseException):
    """Deliberate process-crash simulation that bypasses normal retry handling."""

    def __init__(self, point: HistoryFailpoint) -> None:
        super().__init__(f"history failpoint triggered: {point.value}")
        self.point = point


def parse_history_failpoint(value: str | None) -> HistoryFailpoint | None:
    if value is None or not value.strip():
        return None
    return HistoryFailpoint(value.strip())


def crash_if(selected: HistoryFailpoint | None, point: HistoryFailpoint) -> None:
    if selected == point:
        raise HistoryCrash(point)
