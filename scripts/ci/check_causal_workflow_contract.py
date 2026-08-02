#!/usr/bin/env python3
"""Validate the self-protection contract of the permanent Causal PR Gate workflow."""

from __future__ import annotations

import argparse
from pathlib import Path
import re
from typing import Any

import yaml

PINNED_ACTION_RE = re.compile(
    r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+(?:/[A-Za-z0-9_./-]+)?@[0-9a-f]{40}$"
)
POSITIVE_TIMEOUT_RE = re.compile(r"^[1-9][0-9]*$")
REQUIRED_EVENTS = {"opened", "synchronize", "reopened", "ready_for_review", "edited"}
EXACT_CHECK_NAME = "Causal PR Gate"
BOOTSTRAP_BASE_SHA = "59ff16e72fe856cf9b284427f357a5b4bc409ffb"


class ContractError(RuntimeError):
    pass


class UniqueKeyLoader(yaml.BaseLoader):
    """Reject duplicate keys while preserving scalar strings such as the workflow `on` key.

    BaseLoader constructs no arbitrary Python objects. Replacing it with the YAML 1.1
    safe loader would coerce `on` to a boolean and silently break trigger validation.
    """

    pass


def construct_mapping(
    loader: UniqueKeyLoader,
    node: yaml.MappingNode,
    deep: bool = False,
) -> dict[str, Any]:
    mapping: dict[str, Any] = {}
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node, deep=deep)
        if key in mapping:
            raise ContractError(f"duplicate YAML key: {key}")
        mapping[str(key)] = loader.construct_object(value_node, deep=deep)
    return mapping


UniqueKeyLoader.add_constructor(
    yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG,
    construct_mapping,
)


def as_mapping(value: Any, label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ContractError(f"{label} must be a mapping")
    return value


def as_list(value: Any, label: str) -> list[Any]:
    if not isinstance(value, list):
        raise ContractError(f"{label} must be a list")
    return value


def positive_timeout(value: Any) -> bool:
    return bool(POSITIVE_TIMEOUT_RE.fullmatch(str(value)))


def load_unique_yaml(text: str) -> dict[str, Any]:
    try:
        # nosemgrep: python-yaml-unsafe-load - UniqueKeyLoader derives from BaseLoader.
        loaded = yaml.load(text, Loader=UniqueKeyLoader)
    except yaml.YAMLError as exc:
        raise ContractError(f"invalid YAML: {exc}") from exc
    return as_mapping(loaded, "workflow")


def all_steps(workflow: dict[str, Any]) -> list[dict[str, Any]]:
    jobs = as_mapping(workflow.get("jobs"), "jobs")
    steps: list[dict[str, Any]] = []
    for job_name, raw_job in jobs.items():
        job = as_mapping(raw_job, f"job {job_name}")
        for raw_step in as_list(job.get("steps", []), f"job {job_name} steps"):
            steps.append(as_mapping(raw_step, f"job {job_name} step"))
    return steps


def require_base_bound_execution(
    command: str,
    *,
    label: str,
    source_path: str,
    extracted_name: str,
) -> None:
    extraction = f'git show "${{BASE_SHA}}:{source_path}"'
    destination = f'> "${{RUNNER_TEMP}}/{extracted_name}"'
    base_execution = f'python "${{RUNNER_TEMP}}/{extracted_name}"'
    bootstrap_guard = 'elif [[ "${BASE_SHA}" == "${BOOTSTRAP_BASE_SHA}" ]]; then'
    bootstrap_execution = f"python {source_path}"

    required_once = {
        "base extraction": extraction,
        "fixed extraction destination": destination,
        "fixed base-side execution": base_execution,
        "exact bootstrap guard": bootstrap_guard,
        "bootstrap head-side execution": bootstrap_execution,
    }
    for description, fragment in required_once.items():
        if command.count(fragment) != 1:
            raise ContractError(f"{label} must contain exactly one {description}")

    extraction_index = command.index(extraction)
    destination_index = command.index(destination)
    base_execution_index = command.index(base_execution)
    guard_index = command.index(bootstrap_guard)
    bootstrap_execution_index = command.index(bootstrap_execution)

    if not (
        extraction_index
        < destination_index
        < base_execution_index
        < guard_index
        < bootstrap_execution_index
    ):
        raise ContractError(
            f"{label} must execute the extracted exact base-SHA tool before the bootstrap branch"
        )
    if bootstrap_execution in command[:guard_index]:
        raise ContractError(f"{label} must not execute the head-side tool in the established branch")
    if "exit 1" not in command[bootstrap_execution_index:]:
        raise ContractError(f"{label} must fail closed outside the exact bootstrap base")


def validate_workflow_text(text: str, *, default_branch: str = "main") -> None:
    if "pull_request_target" in text:
        raise ContractError("pull_request_target is forbidden")
    workflow = load_unique_yaml(text)
    if workflow.get("name") != EXACT_CHECK_NAME:
        raise ContractError(f"workflow name must remain '{EXACT_CHECK_NAME}'")
    if workflow.get("permissions") != {}:
        raise ContractError("workflow-level permissions must be an empty mapping")

    triggers = as_mapping(workflow.get("on"), "on")
    if "pull_request_target" in triggers:
        raise ContractError("pull_request_target is forbidden")
    pull_request = as_mapping(triggers.get("pull_request"), "on.pull_request")
    event_types = set(as_list(pull_request.get("types"), "on.pull_request.types"))
    if not REQUIRED_EVENTS.issubset(event_types):
        missing = sorted(REQUIRED_EVENTS - event_types)
        raise ContractError(f"pull_request event types missing: {', '.join(missing)}")
    branches = set(as_list(pull_request.get("branches"), "on.pull_request.branches"))
    if default_branch not in branches:
        raise ContractError(f"workflow must target default branch {default_branch}")

    concurrency = as_mapping(workflow.get("concurrency"), "concurrency")
    group = str(concurrency.get("group", ""))
    if "github.workflow" not in group or "github.event.pull_request.number" not in group:
        raise ContractError("concurrency group must bind workflow and PR number")
    if str(concurrency.get("cancel-in-progress", "")).lower() != "true":
        raise ContractError("concurrency must cancel stale runs")

    jobs = as_mapping(workflow.get("jobs"), "jobs")
    gate_jobs = [
        as_mapping(job, "gate job")
        for job in jobs.values()
        if isinstance(job, dict) and job.get("name") == EXACT_CHECK_NAME
    ]
    if len(gate_jobs) != 1:
        raise ContractError(f"exactly one job must expose stable check name '{EXACT_CHECK_NAME}'")
    gate = gate_jobs[0]
    if "if" in gate:
        raise ContractError("Causal PR Gate job must not define an if condition")
    if not positive_timeout(gate.get("timeout-minutes", "")):
        raise ContractError("Causal PR Gate job must have a positive ASCII timeout-minutes")
    gate_permissions = as_mapping(gate.get("permissions"), "Causal PR Gate permissions")
    if gate_permissions != {"contents": "read"}:
        raise ContractError("Causal PR Gate permissions must be exactly contents: read")
    gate_env = as_mapping(gate.get("env"), "Causal PR Gate env")
    if gate_env.get("BOOTSTRAP_BASE_SHA") != BOOTSTRAP_BASE_SHA:
        raise ContractError("Causal PR Gate must bind the one authorized bootstrap base SHA")

    steps = all_steps(workflow)
    for step in steps:
        if "continue-on-error" in step:
            raise ContractError("workflow steps must not define continue-on-error")

    uses_steps = [step for step in steps if "uses" in step]
    for step in uses_steps:
        uses = str(step["uses"])
        if not PINNED_ACTION_RE.fullmatch(uses):
            raise ContractError(f"external action is not pinned to a full commit SHA: {uses}")

    gate_steps = [
        as_mapping(step, "gate step")
        for step in as_list(gate.get("steps"), "gate steps")
    ]
    for step in gate_steps:
        is_upload = str(step.get("uses", "")).startswith("actions/upload-artifact@")
        if not is_upload and "if" in step:
            raise ContractError("Causal PR Gate steps must not define if conditions")

    checkout_steps = [
        step
        for step in gate_steps
        if str(step.get("uses", "")).startswith("actions/checkout@")
    ]
    if len(checkout_steps) != 1:
        raise ContractError("Causal PR Gate must contain exactly one actions/checkout step")
    checkout_with = as_mapping(checkout_steps[0].get("with"), "checkout with")
    if checkout_with.get("ref") != "${{ github.event.pull_request.head.sha }}":
        raise ContractError("checkout ref must be exact pull_request.head.sha")
    if checkout_with.get("repository") != "${{ github.event.pull_request.head.repo.full_name }}":
        raise ContractError("checkout repository must be pull_request.head.repo.full_name")
    if str(checkout_with.get("persist-credentials", "")).lower() != "false":
        raise ContractError("checkout must set persist-credentials: false")
    if str(checkout_with.get("fetch-depth", "")) != "0":
        raise ContractError("checkout must use fetch-depth: 0")

    analyzer_steps = [
        step
        for step in gate_steps
        if "build_causal_pr_report.py" in str(step.get("run", ""))
    ]
    if len(analyzer_steps) != 1:
        raise ContractError("workflow must run build_causal_pr_report.py exactly once")
    analyzer_command = str(analyzer_steps[0].get("run", ""))
    require_base_bound_execution(
        analyzer_command,
        label="causal analyzer selection",
        source_path="scripts/ci/build_causal_pr_report.py",
        extracted_name="build_causal_pr_report.base.py",
    )
    required_fragments = (
        '--base-sha "${{ github.event.pull_request.base.sha }}"',
        '--head-sha "${{ github.event.pull_request.head.sha }}"',
        '--event-path "${{ github.event_path }}"',
        '--run-id "${{ github.run_id }}"',
        '--run-attempt "${{ github.run_attempt }}"',
    )
    for fragment in required_fragments:
        if analyzer_command.count(fragment) != 2:
            raise ContractError(
                f"analyzer command must pass provenance in established and bootstrap branches: {fragment}"
            )

    trust_steps = [
        step
        for step in gate_steps
        if "check_causal_trust_root.py" in str(step.get("run", ""))
    ]
    if len(trust_steps) != 1:
        raise ContractError("workflow must run check_causal_trust_root.py exactly once")
    trust_command = str(trust_steps[0].get("run", ""))
    require_base_bound_execution(
        trust_command,
        label="trust-root checker selection",
        source_path="scripts/ci/check_causal_trust_root.py",
        extracted_name="check_causal_trust_root.base.py",
    )
    for fragment in (
        '--base-sha "${{ github.event.pull_request.base.sha }}"',
        '--head-sha "${{ github.event.pull_request.head.sha }}"',
        "--manifest-path .github/causal-trust-root.json",
        "--output artifacts/causal/trust-root-verification.json",
    ):
        if trust_command.count(fragment) != 2:
            raise ContractError(
                f"trust-root command must pass required arguments in both branches: {fragment}"
            )

    validator_steps = [
        step
        for step in gate_steps
        if "check_causal_workflow_contract.py" in str(step.get("run", ""))
    ]
    if len(validator_steps) != 1:
        raise ContractError("workflow must run the workflow contract validator exactly once")
    validator_command = str(validator_steps[0].get("run", ""))
    require_base_bound_execution(
        validator_command,
        label="workflow validator selection",
        source_path="scripts/ci/check_causal_workflow_contract.py",
        extracted_name="check_causal_workflow_contract.base.py",
    )
    if validator_command.count(".github/workflows/causal-pr-gate.yml") != 2:
        raise ContractError("workflow validator must inspect the candidate workflow in both branches")

    test_steps = [
        step
        for step in gate_steps
        if "pytest -o addopts=" in str(step.get("run", ""))
    ]
    if len(test_steps) != 1:
        raise ContractError("Causal PR Gate must run one targeted pytest step")
    test_command = str(test_steps[0].get("run", ""))
    for test_path in (
        "tests/test_causal_pr_contract.py",
        "tests/test_causal_trust_root.py",
        "tests/test_causal_workflow_contract.py",
    ):
        if test_path not in test_command:
            raise ContractError(f"targeted causal tests missing protected test path: {test_path}")

    upload_steps = [
        step
        for step in gate_steps
        if str(step.get("uses", "")).startswith("actions/upload-artifact@")
    ]
    if len(upload_steps) != 1:
        raise ContractError("Causal PR Gate must upload exactly one evidence artifact")
    upload = upload_steps[0]
    if str(upload.get("if", "")).replace(" ", "") not in {"${{always()}}", "always()"}:
        raise ContractError("evidence upload must run with always()")
    upload_with = as_mapping(upload.get("with"), "artifact upload with")
    if upload_with.get("if-no-files-found") != "error":
        raise ContractError("evidence upload must fail closed with if-no-files-found: error")
    artifact_name = str(upload_with.get("name", ""))
    for marker in (
        "github.event.pull_request.head.sha",
        "github.run_id",
        "github.run_attempt",
    ):
        if marker not in artifact_name:
            raise ContractError(f"artifact name missing provenance marker {marker}")
    if upload_with.get("path") != "artifacts/causal/":
        raise ContractError("artifact upload path must be artifacts/causal/")

    for job_name, raw_job in jobs.items():
        job = as_mapping(raw_job, f"job {job_name}")
        if not positive_timeout(job.get("timeout-minutes", "")):
            raise ContractError(f"job {job_name} must have a positive ASCII timeout-minutes")
        if "permissions" not in job:
            raise ContractError(f"job {job_name} must define explicit permissions")

    codeql_jobs = [
        as_mapping(job, "CodeQL job")
        for job in jobs.values()
        if isinstance(job, dict) and job.get("name") == "CodeQL"
    ]
    if len(codeql_jobs) != 1:
        raise ContractError("workflow must contain one CodeQL job")
    codeql = codeql_jobs[0]
    if codeql.get("if") != "${{ github.event.pull_request.head.repo.fork == false }}":
        raise ContractError("CodeQL must explicitly skip fork PRs that cannot receive security-events write")
    codeql_permissions = as_mapping(codeql.get("permissions"), "CodeQL permissions")
    if codeql_permissions != {
        "contents": "read",
        "packages": "read",
        "security-events": "write",
    }:
        raise ContractError("CodeQL job permissions are not the explicit minimum contract")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("workflow", type=Path)
    parser.add_argument("--default-branch", default="main")
    args = parser.parse_args()
    validate_workflow_text(
        args.workflow.read_text(encoding="utf-8"),
        default_branch=args.default_branch,
    )
    print("Causal PR Gate workflow contract is satisfied")


if __name__ == "__main__":
    main()
