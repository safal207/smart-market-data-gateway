from __future__ import annotations

import importlib.util
from pathlib import Path
import sys
from types import ModuleType

import pytest


def load_module() -> ModuleType:
    path = Path(__file__).parents[1] / "scripts/ci/check_causal_workflow_contract.py"
    spec = importlib.util.spec_from_file_location("causal_workflow_contract", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


contract = load_module()
WORKFLOW_PATH = Path(__file__).parents[1] / ".github/workflows/causal-pr-gate.yml"


def workflow_text() -> str:
    return WORKFLOW_PATH.read_text(encoding="utf-8")


def assert_rejected(text: str, message: str) -> None:
    with pytest.raises(contract.ContractError, match=message):
        contract.validate_workflow_text(text)


def test_current_workflow_contract_passes() -> None:
    contract.validate_workflow_text(workflow_text())


def test_mutable_action_tag_is_rejected() -> None:
    mutated = workflow_text().replace(
        "actions/checkout@11d5960a326750d5838078e36cf38b85af677262",
        "actions/checkout@v4",
        1,
    )
    assert_rejected(mutated, "not pinned")


def test_wrong_checkout_ref_is_rejected() -> None:
    mutated = workflow_text().replace(
        "ref: ${{ github.event.pull_request.head.sha }}",
        "ref: ${{ github.sha }}",
        1,
    )
    assert_rejected(mutated, "checkout ref")


def test_persisted_credentials_are_rejected() -> None:
    mutated = workflow_text().replace(
        "persist-credentials: false",
        "persist-credentials: true",
        1,
    )
    assert_rejected(mutated, "persist-credentials")


def test_pull_request_target_is_rejected() -> None:
    mutated = workflow_text().replace("pull_request:", "pull_request_target:", 1)
    assert_rejected(mutated, "pull_request_target")


def test_fail_open_artifact_upload_is_rejected() -> None:
    mutated = workflow_text().replace(
        "if-no-files-found: error",
        "if-no-files-found: warn",
        1,
    )
    assert_rejected(mutated, "if-no-files-found")


def test_duplicate_yaml_keys_are_rejected() -> None:
    mutated = workflow_text().replace(
        "permissions: {}",
        "permissions: {}\npermissions: {}",
        1,
    )
    assert_rejected(mutated, "duplicate YAML key")


def test_stable_check_name_change_is_rejected() -> None:
    mutated = workflow_text().replace("name: Causal PR Gate", "name: Causal Gate", 1)
    assert_rejected(mutated, "workflow name")


def test_removing_edited_event_is_rejected() -> None:
    mutated = workflow_text().replace("      - edited\n", "", 1)
    assert_rejected(mutated, "event types missing")


def test_weakened_permissions_are_rejected() -> None:
    mutated = workflow_text().replace(
        "    permissions:\n      contents: read\n",
        "    permissions:\n      contents: write\n",
        1,
    )
    assert_rejected(mutated, "permissions must be exactly")


def test_removing_base_bound_trust_checker_is_rejected() -> None:
    mutated = workflow_text().replace(
        'git show "${BASE_SHA}:scripts/ci/check_causal_trust_root.py"',
        'echo "trust checker bypassed"',
        1,
    )
    assert_rejected(mutated, "trust-root validation must prefer")


def test_removing_exact_tree_test_is_rejected() -> None:
    mutated = workflow_text().replace(
        "tests/test_causal_trust_root.py",
        "tests/test_removed_trust_root.py",
        1,
    )
    assert_rejected(mutated, "targeted causal tests missing")
