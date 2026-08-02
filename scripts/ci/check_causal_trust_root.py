#!/usr/bin/env python3
"""Verify protected causal-CI blobs against an immutable base-bound manifest."""

from __future__ import annotations

import argparse
from dataclasses import asdict, dataclass
import json
from pathlib import Path
import re
import subprocess

SHA_RE = re.compile(r"^[0-9a-f]{40}$")
BLOB_RE = re.compile(r"^[0-9a-f]{40}$")
REQUIRED_PROTECTED_PATHS = {
    ".github/CODEOWNERS",
    ".github/pull_request_template.md",
    ".github/workflows/causal-pr-gate.yml",
    ".github/workflows/ci.yml",
    "AGENTS.md",
    "docs/ci/CAUSAL_PR_GATE.md",
    "pyproject.toml",
    "scripts/ci/build_causal_pr_report.py",
    "scripts/ci/check_causal_workflow_contract.py",
    "scripts/ci/check_causal_trust_root.py",
    "tests/test_causal_pr_contract.py",
    "tests/test_causal_workflow_contract.py",
    "tests/test_causal_trust_root.py",
}


class TrustRootError(RuntimeError):
    """Raised when the exact-tree trust root does not match the requested transition."""


@dataclass(frozen=True)
class TrustRootVerification:
    schema_version: str
    mode: str
    base_sha: str
    head_sha: str
    manifest_path: str
    manifest_blob: str
    protected_file_count: int
    verified: bool


def run_git(repo: Path, *args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(
        ["git", "-C", str(repo), *args],
        text=True,
        capture_output=True,
        check=False,
    )
    if check and result.returncode != 0:
        detail = result.stderr.strip() or result.stdout.strip()
        raise TrustRootError(f"git {' '.join(args)} failed: {detail}")
    return result


def validate_sha(label: str, value: str) -> None:
    if not SHA_RE.fullmatch(value):
        raise TrustRootError(f"{label} must be an exact lowercase 40-character Git SHA")


def object_exists(repo: Path, spec: str) -> bool:
    return run_git(repo, "cat-file", "-e", spec, check=False).returncode == 0


def git_blob(repo: Path, commit_sha: str, path: str) -> str:
    result = run_git(repo, "rev-parse", f"{commit_sha}:{path}")
    blob = result.stdout.strip()
    if not BLOB_RE.fullmatch(blob):
        raise TrustRootError(f"unexpected blob identity for {path}: {blob}")
    return blob


def read_git_file(repo: Path, commit_sha: str, path: str) -> str:
    return run_git(repo, "show", f"{commit_sha}:{path}").stdout


def parse_manifest(text: str) -> dict[str, str]:
    try:
        payload = json.loads(text)
    except json.JSONDecodeError as exc:
        raise TrustRootError(f"trust-root manifest is invalid JSON: {exc}") from exc
    if not isinstance(payload, dict) or payload.get("schema_version") != 1:
        raise TrustRootError("trust-root manifest must have schema_version 1")
    protected = payload.get("protected_files")
    if not isinstance(protected, dict):
        raise TrustRootError("trust-root manifest protected_files must be an object")
    normalized: dict[str, str] = {}
    for raw_path, raw_blob in protected.items():
        if not isinstance(raw_path, str) or not isinstance(raw_blob, str):
            raise TrustRootError("trust-root manifest paths and blob IDs must be strings")
        path = raw_path.strip()
        if not path or path.startswith("/") or ".." in Path(path).parts:
            raise TrustRootError(f"invalid protected path: {raw_path}")
        if not BLOB_RE.fullmatch(raw_blob):
            raise TrustRootError(f"invalid Git blob ID for {path}")
        normalized[path] = raw_blob
    missing = sorted(REQUIRED_PROTECTED_PATHS - normalized.keys())
    if missing:
        raise TrustRootError(f"trust-root manifest is missing required paths: {', '.join(missing)}")
    return normalized


def verify_transition(
    repo: Path,
    base_sha: str,
    head_sha: str,
    manifest_path: str,
) -> TrustRootVerification:
    validate_sha("base SHA", base_sha)
    validate_sha("head SHA", head_sha)
    run_git(repo, "cat-file", "-e", f"{base_sha}^{{commit}}")
    run_git(repo, "cat-file", "-e", f"{head_sha}^{{commit}}")

    base_manifest_spec = f"{base_sha}:{manifest_path}"
    if object_exists(repo, base_manifest_spec):
        mode = "ESTABLISHED"
        manifest_text = read_git_file(repo, base_sha, manifest_path)
        base_manifest_blob = git_blob(repo, base_sha, manifest_path)
        head_manifest_blob = git_blob(repo, head_sha, manifest_path)
        if head_manifest_blob != base_manifest_blob:
            raise TrustRootError(
                "trust-root manifest changed in an ordinary PR; create an independently reviewed bootstrap PR"
            )
        manifest_blob = base_manifest_blob
    else:
        mode = "BOOTSTRAP"
        if not object_exists(repo, f"{head_sha}:{manifest_path}"):
            raise TrustRootError("bootstrap head does not contain the trust-root manifest")
        manifest_text = read_git_file(repo, head_sha, manifest_path)
        manifest_blob = git_blob(repo, head_sha, manifest_path)

    protected = parse_manifest(manifest_text)
    for path, expected_blob in sorted(protected.items()):
        if not object_exists(repo, f"{head_sha}:{path}"):
            raise TrustRootError(f"protected file is missing from exact head: {path}")
        actual_blob = git_blob(repo, head_sha, path)
        if actual_blob != expected_blob:
            raise TrustRootError(
                f"protected file blob mismatch for {path}: expected {expected_blob}, got {actual_blob}"
            )

    return TrustRootVerification(
        schema_version="1.0",
        mode=mode,
        base_sha=base_sha,
        head_sha=head_sha,
        manifest_path=manifest_path,
        manifest_blob=manifest_blob,
        protected_file_count=len(protected),
        verified=True,
    )


def parser() -> argparse.ArgumentParser:
    value = argparse.ArgumentParser(description=__doc__)
    value.add_argument("--repository-path", default=".")
    value.add_argument("--base-sha", required=True)
    value.add_argument("--head-sha", required=True)
    value.add_argument("--manifest-path", default=".github/causal-trust-root.json")
    value.add_argument("--output")
    return value


def main() -> None:
    args = parser().parse_args()
    verification = verify_transition(
        Path(args.repository_path).resolve(),
        args.base_sha,
        args.head_sha,
        args.manifest_path,
    )
    serialized = json.dumps(asdict(verification), indent=2, sort_keys=True) + "\n"
    if args.output:
        output = Path(args.output)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(serialized, encoding="utf-8")
    print(serialized, end="")


if __name__ == "__main__":
    main()
