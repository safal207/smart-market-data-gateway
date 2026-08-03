from pathlib import Path


WORKFLOW = Path(".github/workflows/tradernet-integration.yml")


def workflow_text() -> str:
    return WORKFLOW.read_text(encoding="utf-8")


def test_tradernet_workflow_is_manual_and_read_only() -> None:
    text = workflow_text()

    assert "workflow_dispatch:" in text
    assert "permissions:\n  contents: read" in text
    assert "pull_request:" not in text
    assert "push:" not in text


def test_tradernet_workflow_does_not_persist_checkout_credentials() -> None:
    text = workflow_text()

    assert "uses: actions/checkout@v4" in text
    assert "persist-credentials: false" in text


def test_tradernet_secrets_are_scoped_to_steps_that_need_them() -> None:
    text = workflow_text()
    job_prefix, steps = text.split("    steps:\n", maxsplit=1)

    assert "secrets.TRADERNET_SID" not in job_prefix
    assert "secrets.TRADERNET_USER_ID" not in job_prefix
    assert "SMDG_TRADERNET_SID: ${{ secrets.TRADERNET_SID }}" in steps
    assert "SMDG_TRADERNET_USER_ID: ${{ secrets.TRADERNET_USER_ID }}" in steps


def test_dispatch_inputs_reach_shell_only_through_environment() -> None:
    text = workflow_text()
    run_blocks = "\n".join(
        line for line in text.splitlines() if line.lstrip().startswith("run:")
    )

    assert "INPUT_SYMBOLS: ${{ inputs.symbols }}" in text
    assert "INPUT_EVENTS: ${{ inputs.events }}" in text
    assert "INPUT_TIMEOUT: ${{ inputs.timeout_seconds }}" in text
    assert "${{ inputs." not in run_blocks
    assert '--symbols "$INPUT_SYMBOLS"' in text
    assert '--events "$INPUT_EVENTS"' in text
    assert '--timeout "$INPUT_TIMEOUT"' in text


def test_authenticated_mode_fails_closed_without_sid_secret() -> None:
    text = workflow_text()

    assert "if: ${{ inputs.mode == 'sid_session' }}" in text
    assert 'if [ -z "$SMDG_TRADERNET_SID" ]; then' in text
    assert "exit 1" in text
