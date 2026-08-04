from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import yaml

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
WORKFLOW_PATH = REPOSITORY_ROOT / ".github" / "workflows" / "chart-reference.yml"
CHART_ROOT = REPOSITORY_ROOT / "apps" / "chart-reference"
PINNED_ACTION = re.compile(r"^[^@\s]+@[0-9a-f]{40}$")


def _load_workflow() -> dict[str, Any]:
    payload = yaml.safe_load(WORKFLOW_PATH.read_text(encoding="utf-8"))
    assert isinstance(payload, dict)
    return payload


def test_chart_reference_workflow_is_pinned_and_least_privilege() -> None:
    workflow = _load_workflow()
    assert workflow["permissions"] == {"contents": "read"}

    triggers = workflow.get("on", workflow.get(True))
    assert isinstance(triggers, dict)
    assert set(triggers) == {"push", "pull_request", "workflow_dispatch"}

    job = workflow["jobs"]["chart-reference"]
    assert job["timeout-minutes"] == 30
    assert job["permissions"] == {"contents": "read"}

    steps = job["steps"]
    action_steps = [step for step in steps if "uses" in step]
    assert action_steps
    assert all(PINNED_ACTION.fullmatch(step["uses"]) for step in action_steps)

    checkout = next(step for step in action_steps if step["uses"].startswith("actions/checkout@"))
    assert checkout["with"]["persist-credentials"] is False

    commands = "\n".join(str(step.get("run", "")) for step in steps)
    for required in (
        "pnpm install --frozen-lockfile",
        "pnpm audit --audit-level high",
        "pnpm lint",
        "pnpm typecheck",
        "pnpm test:coverage",
        "pnpm build",
        "pnpm exec playwright install --with-deps chromium",
        "pnpm test:e2e",
    ):
        assert required in commands


def test_chart_runtime_never_claims_live_before_current_generation_data() -> None:
    workspace = (CHART_ROOT / "src" / "features" / "workspace" / "ChartWorkspace.tsx").read_text(
        encoding="utf-8"
    )
    store = (CHART_ROOT / "src" / "engine" / "market-data-store.ts").read_text(
        encoding="utf-8"
    )
    store_test = (CHART_ROOT / "src" / "engine" / "market-data-store.test.ts").read_text(
        encoding="utf-8"
    )
    app_test = (CHART_ROOT / "src" / "App.test.tsx").read_text(encoding="utf-8")
    browser_test = (CHART_ROOT / "tests" / "e2e" / "chart-reference.spec.ts").read_text(
        encoding="utf-8"
    )

    assert "FIRST_MARKET_DATA_TIMEOUT_MS = 5_000" in workspace
    assert 'model.connectionState === "live"' in workspace
    assert "model.quote == null" in workspace
    assert '? "no_data"' in workspace
    assert "generationChanged" in store
    assert "this.latestBySymbol.clear()" in store
    assert "requires current-generation data after reconnect" in store_test
    assert "requires a quote from the new generation after reconnect" in app_test
    assert "Waiting for the first market-data update before marking the stream Live." in workspace
    assert "an open gateway socket without quotes never claims Live" in browser_test
    assert 'not.toContainText("Live")' in browser_test
    assert 'toContainText("No data"' in browser_test


def test_workspace_storage_has_an_exact_preference_allowlist() -> None:
    persistence = (CHART_ROOT / "src" / "engine" / "workspace-persistence.ts").read_text(
        encoding="utf-8"
    )
    persistence_test = (
        CHART_ROOT / "src" / "engine" / "workspace-persistence.test.ts"
    ).read_text(encoding="utf-8")

    save_body = persistence.split("export function saveWorkspace", maxsplit=1)[1].split(
        "export function clearWorkspace", maxsplit=1
    )[0]
    assert "selectedSymbol: input.selectedSymbol" in save_body
    assert "timeframe: input.timeframe" in save_body
    assert "theme: input.theme" in save_body
    assert "showVolume: input.showVolume" not in save_body
    assert "autoReconnect: input.autoReconnect" not in save_body
    assert "persists the exact declared allowlist" in persistence_test
    assert '"showVolume"' in persistence_test
    assert '"autoReconnect"' in persistence_test
