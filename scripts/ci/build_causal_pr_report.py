#!/usr/bin/env python3
"""Build a fail-closed causal report for one exact pull-request transition."""

from __future__ import annotations

import argparse
from collections.abc import Iterable
from dataclasses import asdict, dataclass
import json
from pathlib import Path
import re
import subprocess

SHA_RE = re.compile(r"^[0-9a-f]{40}$")
SECTION_TITLES = (
    "Failure path",
    "Invariant after change",
    "Regression evidence",
    "Residual risk",
)
PLACEHOLDER_RE = re.compile(
    r"^(?:[-*]\s*)?(?:todo|tbd|placeholder|describe|describe here|fill this in|n/?a|none)"
    r"(?:[\s:;,.!\-]*)$",
    re.IGNORECASE,
)
URL_RE = re.compile(r"https?://[^\s)>\]}]+", re.IGNORECASE)
SECRET_PATTERNS = (
    re.compile(r"\bAKIA[0-9A-Z]{16}\b"),
    re.compile(r"\bgh[oprsu]_[A-Za-z0-9_]{20,}\b"),
    re.compile(r"\bsk-[A-Za-z0-9]{20,}\b"),
    re.compile(r"-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----"),
    re.compile(
        r"(?i)\b(?:api[_-]?key|token|secret|password|passwd|authorization)\b\s*[:=]\s*[^\s,;]+"
    ),
)
TEST_PATH_RE = re.compile(r"(?<![A-Za-z0-9_.-])((?:tests?|test)/[A-Za-z0-9_./-]+\.py)\b")
PROTECTED_VALIDATOR_PATHS = {
    ".github/causal-trust-root.json",
    "scripts/ci/build_causal_pr_report.py",
    "scripts/ci/check_causal_trust_root.py",
    "scripts/ci/check_causal_workflow_contract.py",
}


@dataclass(frozen=True)
class ChangedFile:
    status: str
    path: str
    category: str
    old_path: str | None = None


@dataclass(frozen=True)
class Report:
    schema_version: str
    repository: str
    base_sha: str
    head_sha: str
    event_head_sha: str
    run_id: str
    run_attempt: str
    mode: str
    passed: bool
    errors: tuple[str, ...]
    changed_files: tuple[ChangedFile, ...]
    category_counts: dict[str, int]
    causal_sections_present: dict[str, bool]
    regression_test_paths: tuple[str, ...]
    worktree_clean: bool
    checkout_matches_head: bool


class GateFailure(RuntimeError):
    """Raised for one causal-contract validation error."""


def run_git(repo: Path, *args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(
        ["git", "-C", str(repo), *args],
        text=True,
        capture_output=True,
        check=False,
    )
    if check and result.returncode != 0:
        detail = sanitize(result.stderr.strip() or result.stdout.strip())
        raise GateFailure(f"git {' '.join(args)} failed: {detail}")
    return result


def sanitize(value: str) -> str:
    sanitized = URL_RE.sub("[REDACTED_URL]", value)
    for pattern in SECRET_PATTERNS:
        sanitized = pattern.sub("[REDACTED_SECRET]", sanitized)
    return sanitized


def validate_sha(name: str, value: str) -> None:
    if not SHA_RE.fullmatch(value):
        raise GateFailure(f"{name} must be an exact lowercase 40-character Git SHA")


def parse_event(path: Path) -> dict[str, object]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise GateFailure(f"cannot read GitHub event payload: {sanitize(str(exc))}") from exc
    if not isinstance(payload, dict):
        raise GateFailure("GitHub event payload must be a JSON object")
    return payload


def nested_string(payload: dict[str, object], *keys: str) -> str:
    current: object = payload
    for key in keys:
        if not isinstance(current, dict) or key not in current:
            raise GateFailure(f"GitHub event payload is missing {'.'.join(keys)}")
        current = current[key]
    if not isinstance(current, str):
        raise GateFailure(f"GitHub event field {'.'.join(keys)} must be a string")
    return current


def strip_html_comments(value: str) -> str:
    return re.sub(r"<!--.*?-->", "", value, flags=re.DOTALL).strip()


def parse_causal_sections(body: str) -> dict[str, str]:
    headings = {title.casefold(): title for title in SECTION_TITLES}
    sections: dict[str, list[str]] = {title: [] for title in SECTION_TITLES}
    active: str | None = None
    for line in body.splitlines():
        heading = re.match(r"^\s*###\s+(.+?)\s*$", line)
        if heading:
            active = headings.get(heading.group(1).strip().casefold())
            continue
        if active is not None:
            sections[active].append(line)
    return {title: strip_html_comments("\n".join(lines)) for title, lines in sections.items()}


def is_placeholder(value: str) -> bool:
    normalized = strip_html_comments(value).strip()
    if not normalized:
        return True
    return bool(PLACEHOLDER_RE.fullmatch(normalized))


def classify_path(path: str) -> str:
    lowered = path.lower()
    name = Path(lowered).name
    if lowered.startswith(".github/workflows/") or lowered.startswith(".github/actions/"):
        return "workflows"
    if lowered.startswith("tests/") or lowered.startswith("test/") or name.startswith("test_"):
        return "tests"
    if (
        lowered.startswith("docs/")
        or lowered.endswith(".md")
        or name in {"license", "license.txt", "code_of_conduct.md", "contributing.md"}
    ):
        return "documentation"
    if (
        lowered.startswith(("src/", "scripts/", "migrations/", "contracts/", "security/"))
        or lowered.startswith(".github/codeowners")
        or name
        in {
            "pyproject.toml",
            "setup.py",
            "setup.cfg",
            "tox.ini",
            "dockerfile",
            "docker-compose.yml",
            "docker-compose.yaml",
            "requirements.txt",
            "requirements-dev.txt",
            "poetry.lock",
            "pipfile",
            "pipfile.lock",
        }
        or lowered.endswith((".py", ".sh", ".sql", ".toml", ".ini", ".cfg"))
    ):
        return "implementation"
    return "other"


def parse_diff(repo: Path, base_sha: str, head_sha: str) -> tuple[ChangedFile, ...]:
    result = run_git(repo, "diff", "--name-status", "-M", f"{base_sha}...{head_sha}")
    changed: list[ChangedFile] = []
    for raw in result.stdout.splitlines():
        if not raw.strip():
            continue
        parts = raw.split("\t")
        status = parts[0]
        if status.startswith("R") or status.startswith("C"):
            if len(parts) != 3:
                raise GateFailure(f"unexpected rename/copy diff record: {sanitize(raw)}")
            old_path, destination = parts[1], parts[2]
            changed.append(
                ChangedFile(
                    status=status,
                    path=destination,
                    old_path=old_path,
                    category=classify_path(destination),
                )
            )
        else:
            if len(parts) != 2:
                raise GateFailure(f"unexpected diff record: {sanitize(raw)}")
            destination = parts[1]
            changed.append(ChangedFile(status=status, path=destination, category=classify_path(destination)))
    return tuple(changed)


def determine_mode(changed: Iterable[ChangedFile]) -> str:
    files = tuple(changed)
    if files and all(item.category == "documentation" for item in files):
        return "LIGHTWEIGHT"
    return "STRICT"


def referenced_test_paths(repo: Path, regression_text: str) -> tuple[str, ...]:
    paths: list[str] = []
    for match in TEST_PATH_RE.finditer(regression_text):
        candidate = match.group(1)
        if candidate in paths:
            continue
        resolved = (repo / candidate).resolve()
        try:
            resolved.relative_to(repo.resolve())
        except ValueError:
            continue
        if resolved.is_file():
            paths.append(candidate)
    return tuple(paths)


def validate_repository_state(
    repo: Path,
    base_sha: str,
    head_sha: str,
    event_base_sha: str,
    event_head_sha: str,
) -> tuple[bool, bool]:
    validate_sha("base SHA", base_sha)
    validate_sha("head SHA", head_sha)
    validate_sha("event base SHA", event_base_sha)
    validate_sha("event head SHA", event_head_sha)
    if base_sha != event_base_sha:
        raise GateFailure("base SHA does not match github.event.pull_request.base.sha")
    if head_sha != event_head_sha:
        raise GateFailure("stale head: requested head SHA does not match github.event.pull_request.head.sha")

    run_git(repo, "cat-file", "-e", f"{base_sha}^{{commit}}")
    run_git(repo, "cat-file", "-e", f"{head_sha}^{{commit}}")
    checkout = run_git(repo, "rev-parse", "HEAD").stdout.strip()
    checkout_matches = checkout == head_sha
    if not checkout_matches:
        raise GateFailure(f"checkout mismatch: HEAD is {checkout}, expected exact head SHA {head_sha}")
    dirty = run_git(repo, "status", "--porcelain=v1", "--untracked-files=all").stdout.strip()
    clean = not dirty
    if not clean:
        raise GateFailure("worktree is dirty before causal analysis")
    return clean, checkout_matches


def validate_contract(
    repo: Path,
    changed: tuple[ChangedFile, ...],
    mode: str,
    sections: dict[str, str],
) -> tuple[list[str], tuple[str, ...]]:
    errors: list[str] = []
    if mode == "STRICT":
        for title in SECTION_TITLES:
            value = sections.get(title, "")
            if is_placeholder(value):
                errors.append(f"section '{title}' is missing, empty, or an explicit placeholder")

    regression_text = sections.get("Regression evidence", "")
    existing_paths = referenced_test_paths(repo, regression_text)
    changed_test_paths = tuple(
        item.path
        for item in changed
        if item.category == "tests" and not item.status.startswith("D")
    )
    executable_change = any(item.category in {"implementation", "workflows", "other"} for item in changed)
    validator_or_workflow_change = any(
        item.category == "workflows" or item.path in PROTECTED_VALIDATOR_PATHS
        for item in changed
    )

    if mode == "STRICT" and executable_change and not changed_test_paths and not existing_paths:
        errors.append(
            "executable change requires a changed test or an exact existing repository test path in Regression evidence"
        )
    if validator_or_workflow_change:
        mutation_candidates = tuple(
            path
            for path in changed_test_paths
            if "workflow" in path.lower()
            or "causal" in path.lower()
            or "contract" in path.lower()
            or "trust_root" in path.lower()
        )
        if not mutation_candidates:
            errors.append("workflow or CI-validator change requires a changed workflow mutation/regression test")
    return errors, tuple(dict.fromkeys((*changed_test_paths, *existing_paths)))


def mermaid(report: Report, invariant: str, evidence: str) -> str:
    invariant_label = sanitize(" ".join(invariant.split()))[:160] or "not required in lightweight mode"
    evidence_label = sanitize(" ".join(evidence.split()))[:160] or "documentation-only evidence"
    return "\n".join(
        (
            "```mermaid",
            "flowchart LR",
            f'  A["exact base {report.base_sha}"] --> B["classified diff: {report.mode}"]',
            f'  B --> C["target invariant: {invariant_label}"]',
            f'  C --> D["regression evidence: {evidence_label}"]',
            f'  D --> E["exact head {report.head_sha}"]',
            "```",
        )
    )


def write_reports(output_dir: Path, report: Report, sections: dict[str, str]) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / "causal-pr-report.json"
    md_path = output_dir / "causal-pr-report.md"
    json_path.write_text(
        json.dumps(asdict(report), indent=2, sort_keys=True, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    counts = ", ".join(f"{key}={value}" for key, value in sorted(report.category_counts.items()))
    errors = "\n".join(f"- {sanitize(error)}" for error in report.errors) or "- none"
    changed = "\n".join(
        f"- `{item.status}` `{sanitize(item.path)}` → **{item.category}**" for item in report.changed_files
    ) or "- no changed files"
    markdown = f"""# Causal PR Report

- **Result:** {'PASS' if report.passed else 'FAIL'}
- **Mode:** `{report.mode}`
- **Repository:** `{sanitize(report.repository)}`
- **Base SHA:** `{report.base_sha}`
- **Head SHA:** `{report.head_sha}`
- **Run:** `{sanitize(report.run_id)}` attempt `{sanitize(report.run_attempt)}`
- **Categories:** {counts or 'none'}

## Causal transition

{mermaid(report, sections.get('Invariant after change', ''), sections.get('Regression evidence', ''))}

## Changed files

{changed}

## Validation failures

{errors}

## Provenance

This report is bound to exact Git objects, the GitHub run ID, and the run attempt.
PR body values are not copied verbatim; potentially sensitive URLs and secret-like values are redacted.
"""
    md_path.write_text(markdown, encoding="utf-8")


def build_report(args: argparse.Namespace) -> Report:
    repo = Path(args.repository_path).resolve()
    errors: list[str] = []
    changed: tuple[ChangedFile, ...] = ()
    sections: dict[str, str] = {title: "" for title in SECTION_TITLES}
    mode = "STRICT"
    event_head = ""
    worktree_clean = False
    checkout_matches = False

    try:
        payload = parse_event(Path(args.event_path))
        event_base = nested_string(payload, "pull_request", "base", "sha")
        event_head = nested_string(payload, "pull_request", "head", "sha")
        body_value = payload.get("pull_request", {})
        body = body_value.get("body", "") if isinstance(body_value, dict) else ""
        if body is None:
            body = ""
        if not isinstance(body, str):
            raise GateFailure("pull_request.body must be a string or null")
        sections = parse_causal_sections(body)
        worktree_clean, checkout_matches = validate_repository_state(
            repo, args.base_sha, args.head_sha, event_base, event_head
        )
        changed = parse_diff(repo, args.base_sha, args.head_sha)
        mode = determine_mode(changed)
        contract_errors, regression_paths = validate_contract(repo, changed, mode, sections)
        errors.extend(contract_errors)
    except GateFailure as exc:
        regression_paths = ()
        errors.append(str(exc))

    counts = {category: 0 for category in ("implementation", "workflows", "tests", "documentation", "other")}
    for item in changed:
        counts[item.category] += 1
    report = Report(
        schema_version="1.0",
        repository=sanitize(args.repository),
        base_sha=args.base_sha,
        head_sha=args.head_sha,
        event_head_sha=event_head,
        run_id=str(args.run_id),
        run_attempt=str(args.run_attempt),
        mode=mode,
        passed=not errors,
        errors=tuple(sanitize(error) for error in errors),
        changed_files=changed,
        category_counts=counts,
        causal_sections_present={title: not is_placeholder(sections.get(title, "")) for title in SECTION_TITLES},
        regression_test_paths=regression_paths,
        worktree_clean=worktree_clean,
        checkout_matches_head=checkout_matches,
    )
    write_reports(Path(args.output_dir), report, sections)
    return report


def parser() -> argparse.ArgumentParser:
    value = argparse.ArgumentParser(description=__doc__)
    value.add_argument("--repository-path", default=".")
    value.add_argument("--repository", required=True)
    value.add_argument("--base-sha", required=True)
    value.add_argument("--head-sha", required=True)
    value.add_argument("--event-path", required=True)
    value.add_argument("--run-id", required=True)
    value.add_argument("--run-attempt", required=True)
    value.add_argument("--output-dir", default="artifacts/causal")
    return value


def main() -> None:
    report = build_report(parser().parse_args())
    print(json.dumps({"passed": report.passed, "mode": report.mode, "errors": report.errors}))
    raise SystemExit(0 if report.passed else 1)


if __name__ == "__main__":
    main()
