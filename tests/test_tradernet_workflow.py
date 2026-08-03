from pathlib import Path


WORKFLOW = Path(".github/workflows/tradernet-integration.yml")


def workflow_text() -> str:
    return WORKFLOW.read_text(encoding="utf-8")


def step_block(text: str, name: str) -> str:
    marker = f"      - name: {name}\n"
    start = text.index(marker)
    end = text.find("\n      - name: ", start + len(marker))
    return text[start:] if end == -1 else text[start:end]


def all_step_blocks(text: str) -> dict[str, str]:
    blocks: dict[str, str] = {}
    for chunk in text.split("\n      - name: ")[1:]:
        name, _, remainder = chunk.partition("\n")
        blocks[name] = f"      - name: {name}\n{remainder}"
    return blocks


def test_tradernet_workflow_is_manual_and_read_only() -> None:
    text = workflow_text()

    assert "workflow_dispatch:" in text
    assert "permissions:\n  contents: read" in text
    assert "pull_request:" not in text
    assert "push:" not in text


def test_tradernet_workflow_does_not_persist_checkout_credentials() -> None:
    checkout = step_block(workflow_text(), "Checkout")

    assert "uses: actions/checkout@v4" in checkout
    assert "persist-credentials: false" in checkout
    assert "secrets." not in checkout


def test_public_demo_step_receives_no_sid_credentials() -> None:
    public_demo = step_block(
        workflow_text(), "Run public-demo quote and reconnect checks"
    )

    assert "if: ${{ inputs.mode == 'public_demo' }}" in public_demo
    assert "secrets." not in public_demo
    assert "SMDG_TRADERNET_SID" not in public_demo
    assert "SMDG_TRADERNET_USER_ID" not in public_demo


def test_sid_credentials_are_limited_to_authenticated_steps() -> None:
    text = workflow_text()
    blocks = all_step_blocks(text)
    allowed = {
        "Validate SID secret for authenticated mode",
        "Run authenticated quote and reconnect checks",
    }

    for name, block in blocks.items():
        if "secrets.TRADERNET" in block:
            assert name in allowed

    validation = blocks["Validate SID secret for authenticated mode"]
    authenticated = blocks["Run authenticated quote and reconnect checks"]
    assert "if: ${{ inputs.mode == 'sid_session' }}" in validation
    assert "SMDG_TRADERNET_SID: ${{ secrets.TRADERNET_SID }}" in validation
    assert "secrets.TRADERNET_USER_ID" not in validation
    assert "if: ${{ inputs.mode == 'sid_session' }}" in authenticated
    assert "SMDG_TRADERNET_SID: ${{ secrets.TRADERNET_SID }}" in authenticated
    assert (
        "SMDG_TRADERNET_USER_ID: ${{ secrets.TRADERNET_USER_ID }}"
        in authenticated
    )


def test_dispatch_inputs_reach_shell_only_through_environment() -> None:
    text = workflow_text()
    run_lines = "\n".join(
        line for line in text.splitlines() if line.lstrip().startswith("run:")
    )

    assert text.count("INPUT_SYMBOLS: ${{ inputs.symbols }}") == 2
    assert text.count("INPUT_EVENTS: ${{ inputs.events }}") == 2
    assert text.count("INPUT_TIMEOUT: ${{ inputs.timeout_seconds }}") == 2
    assert "${{ inputs." not in run_lines
    assert text.count('--symbols "$INPUT_SYMBOLS"') == 2
    assert text.count('--events "$INPUT_EVENTS"') == 2
    assert text.count('--timeout "$INPUT_TIMEOUT"') == 2


def test_authenticated_mode_fails_closed_without_sid_secret() -> None:
    validation = step_block(
        workflow_text(), "Validate SID secret for authenticated mode"
    )

    assert "if: ${{ inputs.mode == 'sid_session' }}" in validation
    assert 'if [ -z "$SMDG_TRADERNET_SID" ]; then' in validation
    assert "exit 1" in validation
