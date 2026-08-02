# Repository reviewer instructions

Every pull request must be reviewed as an exact causal transition from `pull_request.base.sha` to `pull_request.head.sha`, not merely as a set of green checks.

## Required review questions

1. Does the stated failure path exist and does the diff actually block it?
2. Is the post-change invariant precise, executable, and narrower than a product claim?
3. Does regression evidence fail when the protection or behavior is removed?
4. Are residual risks honest, including external services, branch settings, licensing, timing, and hardware assumptions?
5. Are the checked-out repository, base SHA, head SHA, evidence artifact, run ID, and run attempt mutually consistent?

## Trust-root changes

Treat these paths as protected review surfaces:

- `.github/workflows/causal-pr-gate.yml`
- `.github/workflows/ci.yml`
- `scripts/ci/build_causal_pr_report.py`
- `scripts/ci/check_causal_workflow_contract.py`
- `tests/test_causal_pr_contract.py`
- `tests/test_causal_workflow_contract.py`
- `.github/CODEOWNERS`
- `.github/pull_request_template.md`
- `AGENTS.md`
- `docs/ci/CAUSAL_PR_GATE.md`

A change to a workflow or validator must include a mutation/regression test that fails when the protection is removed. Review evidence never grants merge authority. Trust-root bootstrap and replacement require independent owner review; bots may advise but may not merge.

## Modes

- `STRICT`: runtime, API, CLI, schema, packaging, scripts, security, workflow, or unclassified changes. All four causal sections and regression evidence are mandatory.
- `LIGHTWEIGHT`: documentation-only changes. Exact SHA and clean-worktree checks still apply, while the four causal sections are optional.
