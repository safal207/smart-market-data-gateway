from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import subprocess
import sys
from types import ModuleType

import pytest


def load_module() -> ModuleType:
    path = Path(__file__).parents[1] / "scripts/ci/check_causal_trust_root.py"
    spec = importlib.util.spec_from_file_location("causal_trust_root", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


trust_root = load_module()
REPOSITORY_ROOT = Path(__file__).parents[1]
MANIFEST_PATH = ".github/causal-trust-root.json"


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


def commit(repo: Path, message: str) -> str:
    git(repo, "add", ".")
    git(repo, "commit", "-m", message)
    return git(repo, "rev-parse", "HEAD")


def manifest_for(repo: Path, paths: list[str]) -> str:
    protected = {path: git(repo, "hash-object", path) for path in paths}
    return json.dumps(
        {"schema_version": 1, "protected_files": protected},
        indent=2,
        sort_keys=True,
    ) + "\n"


def init_repository(tmp_path: Path) -> tuple[Path, str, list[str]]:
    repo = tmp_path / "repo"
    repo.mkdir()
    git(repo, "init", "-b", "main")
    git(repo, "config", "user.name", "Trust Root Test")
    git(repo, "config", "user.email", "trust-root@example.invalid")
    paths = sorted(trust_root.REQUIRED_PROTECTED_PATHS)
    for index, path in enumerate(paths):
        write(repo, path, f"protected-{index}\n")
    base = commit(repo, "base without manifest")
    return repo, base, paths


def test_bootstrap_manifest_matches_exact_head(tmp_path: Path) -> None:
    repo, base, paths = init_repository(tmp_path)
    write(repo, MANIFEST_PATH, manifest_for(repo, paths))
    head = commit(repo, "bootstrap manifest")

    verification = trust_root.verify_transition(repo, base, head, MANIFEST_PATH)

    assert verification.mode == "BOOTSTRAP"
    assert verification.verified
    assert verification.protected_file_count == len(paths)


def test_established_manifest_accepts_unchanged_protected_tree(tmp_path: Path) -> None:
    repo, first, paths = init_repository(tmp_path)
    write(repo, MANIFEST_PATH, manifest_for(repo, paths))
    base = commit(repo, "establish manifest")
    write(repo, "docs/note.md", "safe documentation change\n")
    head = commit(repo, "ordinary change")

    verification = trust_root.verify_transition(repo, base, head, MANIFEST_PATH)

    assert first != base
    assert verification.mode == "ESTABLISHED"
    assert verification.verified


def test_established_manifest_blocks_protected_file_change(tmp_path: Path) -> None:
    repo, _, paths = init_repository(tmp_path)
    write(repo, MANIFEST_PATH, manifest_for(repo, paths))
    base = commit(repo, "establish manifest")
    write(repo, paths[0], "tampered\n")
    head = commit(repo, "change protected file")

    with pytest.raises(trust_root.TrustRootError, match="blob mismatch"):
        trust_root.verify_transition(repo, base, head, MANIFEST_PATH)


def test_established_manifest_blocks_manifest_replacement(tmp_path: Path) -> None:
    repo, _, paths = init_repository(tmp_path)
    write(repo, MANIFEST_PATH, manifest_for(repo, paths))
    base = commit(repo, "establish manifest")
    write(repo, paths[0], "replacement\n")
    write(repo, MANIFEST_PATH, manifest_for(repo, paths))
    head = commit(repo, "replace root and protected file")

    with pytest.raises(trust_root.TrustRootError, match="manifest changed"):
        trust_root.verify_transition(repo, base, head, MANIFEST_PATH)


def test_repository_manifest_matches_current_tree() -> None:
    payload = json.loads((REPOSITORY_ROOT / MANIFEST_PATH).read_text(encoding="utf-8"))
    protected = payload["protected_files"]
    assert trust_root.REQUIRED_PROTECTED_PATHS.issubset(protected)
    for path, expected_blob in protected.items():
        assert git(REPOSITORY_ROOT, "hash-object", path) == expected_blob
