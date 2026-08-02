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


def test_conditional_gate_job_is_rejected() -> None:
    mutated = workflow_text().replace(
        "    runs-on: ubuntu-24.04\n",
        "    if: ${{ false }}\n    runs-on: ubuntu-24.04\n",
        1,
    )
    assert_rejected(mutated, "must not define an if condition")


def test_conditional_trust_root_step_is_rejected() -> None:
    mutated = workflow_text().replace(
        "      - name: Verify exact-tree trust root with base checker\n        shell: bash\n",
        "      - name: Verify exact-tree trust root with base checker\n"
        "        if: ${{ false }}\n"
        "        shell: bash\n",
        1,
    )
    assert_rejected(mutated, "steps must not define if conditions")


def test_continue_on_error_on_trust_root_step_is_rejected() -> None:
    mutated = workflow_text().replace(
        "      - name: Verify exact-tree trust root with base checker\n        shell: bash\n",
        "      - name: Verify exact-tree trust root with base checker\n"
        "        continue-on-error: true\n"
        "        shell: bash\n",
        1,
    )
    assert_rejected(mutated, "must not define continue-on-error")


def test_continue_on_error_on_artifact_upload_is_rejected() -> None:
    mutated = workflow_text().replace(
        "      - name: Upload exact-SHA causal evidence\n        if: ${{ always() }}\n",
        "      - name: Upload exact-SHA causal evidence\n"
        "        continue-on-error: true\n"
        "        if: ${{ always() }}\n",
        1,
    )
    assert_rejected(mutated, "must not define continue-on-error")


def test_removing_edited_event_is_rejected() -> None:
    mutated = workflow_text().replace("      - edited\n", "", 1)
    assert_rejected(mutated, "event types missing")


def test_weakened_gate_permissions_are_rejected() -> None:
    mutated = workflow_text().replace(
        "    permissions:\n      contents: read\n",
        "    permissions:\n      contents: write\n",
        1,
    )
    assert_rejected(mutated, "permissions must be exactly")


def test_zero_timeout_is_rejected() -> None:
    mutated = workflow_text().replace("    timeout-minutes: 20\n", "    timeout-minutes: 0\n", 1)
    assert_rejected(mutated, "positive ASCII timeout")


def test_non_ascii_timeout_is_rejected() -> None:
    mutated = workflow_text().replace("    timeout-minutes: 20\n", "    timeout-minutes: ٢٠\n", 1)
    assert_rejected(mutated, "positive ASCII timeout")


def test_concurrency_binding_removal_is_rejected() -> None:
    mutated = workflow_text().replace(
        "group: ${{ github.workflow }}-${{ github.event.pull_request.number }}",
        "group: causal-pr-gate",
        1,
    )
    assert_rejected(mutated, "concurrency group")


def test_bootstrap_base_change_is_rejected() -> None:
    mutated = workflow_text().replace(
        contract.BOOTSTRAP_BASE_SHA,
        "1" * 40,
        1,
    )
    assert_rejected(mutated, "authorized bootstrap base SHA")


def test_broadening_bootstrap_fallback_is_rejected() -> None:
    mutated = workflow_text().replace(
        'elif [[ "${BASE_SHA}" == "${BOOTSTRAP_BASE_SHA}" ]]; then',
        "else",
        1,
    )
    assert_rejected(mutated, "fail closed outside the exact bootstrap base")


def test_removing_base_bound_analyzer_is_rejected() -> None:
    mutated = workflow_text().replace(
        'git show "${BASE_SHA}:scripts/ci/build_causal_pr_report.py"',
        'echo "analyzer bypassed"',
        1,
    )
    assert_rejected(mutated, "causal analysis must prefer")


def test_removing_base_bound_trust_checker_is_rejected() -> None:
    mutated = workflow_text().replace(
        'git show "${BASE_SHA}:scripts/ci/check_causal_trust_root.py"',
        'echo "trust checker bypassed"',
        1,
    )
    assert_rejected(mutated, "trust-root validation must prefer")


def test_removing_base_bound_workflow_validator_is_rejected() -> None:
    mutated = workflow_text().replace(
        'git show "${BASE_SHA}:scripts/ci/check_causal_workflow_contract.py"',
        'echo "workflow validator bypassed"',
        1,
    )
    assert_rejected(mutated, "workflow validation must prefer")


def test_removing_run_attempt_provenance_is_rejected() -> None:
    mutated = workflow_text().replace(
        '            --run-attempt "${{ github.run_attempt }}" \\\n',
        "",
        1,
    )
    assert_rejected(mutated, "missing exact provenance argument")


def test_artifact_always_condition_is_rejected_when_weakened() -> None:
    mutated = workflow_text().replace("if: ${{ always() }}", "if: ${{ success() }}", 1)
    assert_rejected(mutated, "always")


def test_artifact_attempt_marker_is_required() -> None:
    mutated = workflow_text().replace(
        "name: causal-pr-${{ github.event.pull_request.head.sha }}-${{ github.run_id }}-${{ github.run_attempt }}",
        "name: causal-pr-${{ github.event.pull_request.head.sha }}-${{ github.run_id }}",
        1,
    )
    assert_rejected(mutated, "artifact name missing provenance marker")


def test_codeql_fork_condition_change_is_rejected() -> None:
    mutated = workflow_text().replace(
        "if: ${{ github.event.pull_request.head.repo.fork == false }}",
        "if: ${{ true }}",
        1,
    )
    assert_rejected(mutated, "CodeQL must explicitly skip fork PRs")


def test_codeql_permissions_change_is_rejected() -> None:
    mutated = workflow_text().replace("      security-events: write\n", "      security-events: read\n", 1)
    assert_rejected(mutated, "CodeQL job permissions")


def test_removing_exact_tree_test_is_rejected() -> None:
    mutated = workflow_text().replace(
        "tests/test_causal_trust_root.py",
        "tests/test_removed_trust_root.py",
        1,
    )
    assert_rejected(mutated, "targeted causal tests missing")
