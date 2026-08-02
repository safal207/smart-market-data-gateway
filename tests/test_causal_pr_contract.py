from __future__ import annotations

import argparse
import importlib.util
import json
from pathlib import Path
import subprocess
import sys
from types import ModuleType


def load_module() -> ModuleType:
    path = Path(__file__).parents[1] / "scripts/ci/build_causal_pr_report.py"
    spec = importlib.util.spec_from_file_location("causal_report", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


causal = load_module()


def git(repo: Path, *args: str) -> str:
    result = subprocess.run(
        ["git", "-C", str(repo), *args],
        text=True,
        capture_output=True,
        check=True,
    )
    return result.stdout.strip()


def write(repo: Path, path: str, content: str) -> None:
    target = repo / path
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(content, encoding="utf-8")


def init_repo(tmp_path: Path) -> tuple[Path, str]:
    repo = tmp_path / "repo"
    repo.mkdir()
    git(repo, "init", "-b", "main")
    git(repo, "config", "user.name", "Causal Test")
    git(repo, "config", "user.email", "causal@example.invalid")
    write(repo, "src/app.py", "def value() -> int:\n    return 1\n")
    write(repo, "tests/test_app.py", "def test_value() -> None:\n    assert 1 == 1\n")
    write(repo, "docs/guide.md", "# Guide\n")
    git(repo, "add", ".")
    git(repo, "commit", "-m", "base")
    return repo, git(repo, "rev-parse", "HEAD")


def commit(repo: Path, message: str = "head") -> str:
    git(repo, "add", ".")
    git(repo, "commit", "-m", message)
    return git(repo, "rev-parse", "HEAD")


def full_body(regression: str = "Changed tests/test_app.py proves the invariant.") -> str:
    return f"""### Failure path
A stale or unsafe transition could pass without causal evidence.

### Invariant after change
Every executable transition is bound to exact Git objects and regression evidence.

### Regression evidence
{regression}

### Residual risk
External GitHub availability and owner branch settings remain outside this patch.
"""


def run_report(
    tmp_path: Path,
    repo: Path,
    base_sha: str,
    head_sha: str,
    body: str,
    *,
    event_base: str | None = None,
    event_head: str | None = None,
):
    event = tmp_path / "event.json"
    event.write_text(
        json.dumps(
            {
                "pull_request": {
                    "base": {"sha": event_base or base_sha},
                    "head": {"sha": event_head or head_sha},
                    "body": body,
                }
            }
        ),
        encoding="utf-8",
    )
    output = tmp_path / "out"
    args = argparse.Namespace(
        repository_path=str(repo),
        repository="owner/repo",
        base_sha=base_sha,
        head_sha=head_sha,
        event_path=str(event),
        run_id="123",
        run_attempt="2",
        output_dir=str(output),
    )
    return causal.build_report(args), output


def test_strict_change_with_changed_test_passes(tmp_path: Path) -> None:
    repo, base = init_repo(tmp_path)
    write(repo, "src/app.py", "def value() -> int:\n    return 2\n")
    write(repo, "tests/test_app.py", "def test_value() -> None:\n    assert 2 == 2\n")
    head = commit(repo)

    report, _ = run_report(tmp_path, repo, base, head, full_body())

    assert report.passed
    assert report.mode == "STRICT"
    assert "tests/test_app.py" in report.regression_test_paths


def test_missing_causal_sections_block_pr(tmp_path: Path) -> None:
    repo, base = init_repo(tmp_path)
    write(repo, "src/app.py", "def value() -> int:\n    return 2\n")
    head = commit(repo)

    report, _ = run_report(tmp_path, repo, base, head, "### Failure path\nA real failure path.\n")

    assert not report.passed
    assert any("Invariant after change" in error for error in report.errors)


def test_empty_section_does_not_inherit_following_heading_content(tmp_path: Path) -> None:
    repo, base = init_repo(tmp_path)
    write(repo, "src/app.py", "def value() -> int:\n    return 2\n")
    write(repo, "tests/test_app.py", "def test_value() -> None:\n    assert 2 == 2\n")
    head = commit(repo)
    body = """### Failure path
A concrete failure path.

### Invariant after change
A concrete invariant.

### Regression evidence
Changed tests/test_app.py proves it.

### Residual risk
<!-- intentionally empty -->

## Operational notes
- [ ] This checklist must not fill Residual risk.
"""

    report, _ = run_report(tmp_path, repo, base, head, body)

    assert not report.passed
    assert any("Residual risk" in error for error in report.errors)


def test_empty_atx_heading_terminates_current_section(tmp_path: Path) -> None:
    repo, base = init_repo(tmp_path)
    write(repo, "src/app.py", "def value() -> int:\n    return 2\n")
    write(repo, "tests/test_app.py", "def test_value() -> None:\n    assert 2 == 2\n")
    head = commit(repo)
    body = """### Failure path
A concrete failure path.

### Invariant after change
A concrete invariant.

### Regression evidence
Changed tests/test_app.py proves it.

### Residual risk
<!-- intentionally empty -->

##
This text belongs after an empty heading and must not fill Residual risk.
"""

    report, _ = run_report(tmp_path, repo, base, head, body)

    assert not report.passed
    assert any("Residual risk" in error for error in report.errors)


def test_fenced_code_headings_do_not_satisfy_sections(tmp_path: Path) -> None:
    repo, base = init_repo(tmp_path)
    write(repo, "src/app.py", "def value() -> int:\n    return 2\n")
    head = commit(repo)
    body = """```markdown
### Failure path
A code example is not review evidence.
### Invariant after change
Still only code.
### Regression evidence
tests/test_app.py
### Residual risk
Still only code.
```
"""

    report, _ = run_report(tmp_path, repo, base, head, body)

    assert not report.passed
    assert all(not present for present in report.causal_sections_present.values())


def test_indented_code_headings_do_not_satisfy_sections(tmp_path: Path) -> None:
    repo, base = init_repo(tmp_path)
    write(repo, "src/app.py", "def value() -> int:\n    return 2\n")
    head = commit(repo)
    body = """    ### Failure path
    A code example is not review evidence.
    ### Invariant after change
    Still only code.
    ### Regression evidence
    tests/test_app.py
    ### Residual risk
    Still only code.
"""

    report, _ = run_report(tmp_path, repo, base, head, body)

    assert not report.passed
    assert all(not present for present in report.causal_sections_present.values())


def test_placeholders_block_pr(tmp_path: Path) -> None:
    repo, base = init_repo(tmp_path)
    write(repo, "src/app.py", "def value() -> int:\n    return 2\n")
    write(repo, "tests/test_app.py", "def test_value() -> None:\n    assert 2 == 2\n")
    head = commit(repo)
    body = full_body().replace(
        "A stale or unsafe transition could pass without causal evidence.",
        "TODO",
    )

    report, _ = run_report(tmp_path, repo, base, head, body)

    assert not report.passed
    assert any("Failure path" in error for error in report.errors)


def test_multiline_placeholder_only_section_blocks_pr(tmp_path: Path) -> None:
    repo, base = init_repo(tmp_path)
    write(repo, "src/app.py", "def value() -> int:\n    return 2\n")
    write(repo, "tests/test_app.py", "def test_value() -> None:\n    assert 2 == 2\n")
    head = commit(repo)
    body = full_body().replace(
        "A stale or unsafe transition could pass without causal evidence.",
        "TODO\nTBD",
    )

    report, _ = run_report(tmp_path, repo, base, head, body)

    assert not report.passed
    assert any("Failure path" in error for error in report.errors)


def test_non_placeholder_phrase_is_not_rejected(tmp_path: Path) -> None:
    repo, base = init_repo(tmp_path)
    write(repo, "src/app.py", "def value() -> int:\n    return 2\n")
    write(repo, "tests/test_app.py", "def test_value() -> None:\n    assert 2 == 2\n")
    head = commit(repo)
    body = full_body().replace(
        "A stale or unsafe transition could pass without causal evidence.",
        "The parser accepts non-placeholder sections that contain complete causal reasoning.",
    )

    report, _ = run_report(tmp_path, repo, base, head, body)

    assert report.passed


def test_existing_test_path_is_accepted(tmp_path: Path) -> None:
    repo, base = init_repo(tmp_path)
    write(repo, "src/app.py", "def value() -> int:\n    return 2\n")
    head = commit(repo)

    report, _ = run_report(
        tmp_path,
        repo,
        base,
        head,
        full_body("Existing tests/test_app.py proves the unchanged public contract."),
    )

    assert report.passed
    assert report.regression_test_paths == ("tests/test_app.py",)


def test_executable_change_without_regression_evidence_blocks(tmp_path: Path) -> None:
    repo, base = init_repo(tmp_path)
    write(repo, "src/app.py", "def value() -> int:\n    return 2\n")
    head = commit(repo)

    report, _ = run_report(
        tmp_path,
        repo,
        base,
        head,
        full_body("The behavior was inspected manually."),
    )

    assert not report.passed
    assert any("executable change requires" in error for error in report.errors)


def test_deleted_test_does_not_count_as_regression_evidence(tmp_path: Path) -> None:
    repo, base = init_repo(tmp_path)
    write(repo, "src/app.py", "def value() -> int:\n    return 2\n")
    (repo / "tests/test_app.py").unlink()
    head = commit(repo)

    report, _ = run_report(
        tmp_path,
        repo,
        base,
        head,
        full_body("Deleted tests/test_app.py used to cover this behavior."),
    )

    assert not report.passed
    assert any("executable change requires" in error for error in report.errors)


def test_workflow_change_without_contract_test_blocks(tmp_path: Path) -> None:
    repo, base = init_repo(tmp_path)
    write(repo, ".github/workflows/ci.yml", "name: CI\n")
    head = commit(repo)

    report, _ = run_report(
        tmp_path,
        repo,
        base,
        head,
        full_body("Existing tests/test_app.py exercises application behavior."),
    )

    assert not report.passed
    assert any("workflow mutation/regression test" in error for error in report.errors)


def test_validator_change_without_changed_mutation_test_blocks(tmp_path: Path) -> None:
    repo, base = init_repo(tmp_path)
    write(
        repo,
        "scripts/ci/check_causal_workflow_contract.py",
        "def validate() -> bool:\n    return True\n",
    )
    head = commit(repo)

    report, _ = run_report(
        tmp_path,
        repo,
        base,
        head,
        full_body("Existing tests/test_app.py covers only application behavior."),
    )

    assert not report.passed
    assert any("workflow mutation/regression test" in error for error in report.errors)


def test_documentation_only_change_passes_lightweight_mode(tmp_path: Path) -> None:
    repo, base = init_repo(tmp_path)
    write(repo, "docs/guide.md", "# Updated guide\n")
    head = commit(repo)

    report, _ = run_report(tmp_path, repo, base, head, "")

    assert report.passed
    assert report.mode == "LIGHTWEIGHT"


def test_exact_tree_diff_includes_changes_present_only_on_current_base(tmp_path: Path) -> None:
    repo, _ = init_repo(tmp_path)
    git(repo, "checkout", "-b", "feature")
    write(repo, "docs/guide.md", "# Feature guide\n")
    head = commit(repo, "feature head")

    git(repo, "checkout", "main")
    write(repo, "src/app.py", "def value() -> int:\n    return 9\n")
    advanced_base = commit(repo, "advance base")
    git(repo, "checkout", "feature")

    report, _ = run_report(
        tmp_path,
        repo,
        advanced_base,
        head,
        full_body("Existing tests/test_app.py covers the exact tree transition."),
    )

    assert report.passed
    assert report.mode == "STRICT"
    assert any(item.path == "src/app.py" for item in report.changed_files)


def test_documentation_rename_uses_source_and_destination_categories(tmp_path: Path) -> None:
    repo, base = init_repo(tmp_path)
    git(repo, "mv", "docs/guide.md", "docs/renamed-guide.md")
    head = commit(repo)

    report, _ = run_report(tmp_path, repo, base, head, "")

    renamed = next(item for item in report.changed_files if item.path == "docs/renamed-guide.md")
    assert renamed.old_path == "docs/guide.md"
    assert renamed.old_category == "documentation"
    assert renamed.category == "documentation"
    assert report.mode == "LIGHTWEIGHT"


def test_executable_to_documentation_rename_remains_strict(tmp_path: Path) -> None:
    repo, base = init_repo(tmp_path)
    git(repo, "mv", "src/app.py", "docs/app.md")
    head = commit(repo)

    report, _ = run_report(
        tmp_path,
        repo,
        base,
        head,
        full_body("Existing tests/test_app.py records the removed executable surface."),
    )

    renamed = next(item for item in report.changed_files if item.path == "docs/app.md")
    assert renamed.old_path == "src/app.py"
    assert renamed.old_category == "implementation"
    assert renamed.category == "documentation"
    assert report.mode == "STRICT"
    assert report.passed


def test_urls_and_secret_like_values_do_not_reach_markdown(tmp_path: Path) -> None:
    repo, base = init_repo(tmp_path)
    write(repo, "src/app.py", "def value() -> int:\n    return 2\n")
    write(repo, "tests/test_app.py", "def test_value() -> None:\n    assert 2 == 2\n")
    head = commit(repo)
    fake_token = "gh" + "p_" + "A" * 36
    body = full_body().replace(
        "Every executable transition is bound to exact Git objects and regression evidence.",
        f"See https://private.example.invalid/path and token={fake_token} for context.",
    )

    report, output = run_report(tmp_path, repo, base, head, body)
    markdown = (output / "causal-pr-report.md").read_text(encoding="utf-8")

    assert report.passed
    assert "private.example.invalid" not in markdown
    assert fake_token not in markdown
    assert "[REDACTED_URL]" in markdown
    assert "[REDACTED_SECRET]" in markdown


def test_invalid_base_sha_blocks(tmp_path: Path) -> None:
    repo, base = init_repo(tmp_path)
    write(repo, "docs/guide.md", "# Updated guide\n")
    head = commit(repo)

    report, _ = run_report(tmp_path, repo, "not-a-sha", head, "", event_base="not-a-sha")

    assert not report.passed
    assert any("exact lowercase 40-character" in error for error in report.errors)


def test_dirty_worktree_blocks(tmp_path: Path) -> None:
    repo, base = init_repo(tmp_path)
    write(repo, "docs/guide.md", "# Updated guide\n")
    head = commit(repo)
    write(repo, "untracked.txt", "dirty\n")

    report, _ = run_report(tmp_path, repo, base, head, "")

    assert not report.passed
    assert any("worktree is dirty" in error for error in report.errors)


def test_stale_head_blocks(tmp_path: Path) -> None:
    repo, base = init_repo(tmp_path)
    write(repo, "docs/guide.md", "# Updated guide\n")
    head = commit(repo)
    stale = "1" * 40

    report, _ = run_report(tmp_path, repo, base, head, "", event_head=stale)

    assert not report.passed
    assert any("stale head" in error for error in report.errors)
