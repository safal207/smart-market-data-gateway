import json
from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[1]


def test_prometheus_scrapes_archive_worker_and_loads_archive_alerts() -> None:
    prometheus = yaml.safe_load(
        (ROOT / "observability/prometheus.yml").read_text(encoding="utf-8")
    )
    jobs = {item["job_name"]: item for item in prometheus["scrape_configs"]}
    archive_job = jobs["smart-market-data-candle-archive"]
    assert archive_job["static_configs"][0]["targets"] == ["candle-archive:9102"]

    alerts = yaml.safe_load(
        (ROOT / "observability/alerts.yml").read_text(encoding="utf-8")
    )
    names = {
        rule["alert"]
        for group in alerts["groups"]
        for rule in group["rules"]
        if "alert" in rule
    }
    assert {
        "CandleArchiveExporterDown",
        "CandleArchiveMonitorUnavailable",
        "CandleArchiveConsumerGroupMissing",
        "CandleArchiveNoConsumers",
        "CandleArchiveBacklogStale",
        "CandleArchiveTrimRisk",
        "CandleArchiveTrimImminent",
    } <= names


def test_grafana_dashboard_contains_archive_trim_panels() -> None:
    dashboard = json.loads(
        (ROOT / "observability/grafana/dashboards/gateway.json").read_text(
            encoding="utf-8"
        )
    )
    titles = {panel["title"] for panel in dashboard["panels"]}
    assert {
        "Archive backlog",
        "Archive trim risk",
        "Oldest archive backlog",
        "Archive trim headroom",
        "Candle archive backlog composition",
        "Candle archive worker health",
    } <= titles
