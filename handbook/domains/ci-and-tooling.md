# CI and Tooling Domain

## Purpose

Map the local scripts and GitHub workflows that enforce structural rules, run validation, and expose repo-native guardrails to contributors and agents.

## Entrypoints

- [`build-support/README.md`](../../build-support/README.md)
- [`.github/workflows/ci-pipeline.yml`](../../.github/workflows/ci-pipeline.yml)
- [`.github/workflows/ci-pipeline-branch.yml`](../../.github/workflows/ci-pipeline-branch.yml)

## Commands

- `python3 -m unittest build-support/test_check_repo_handbook.py`
- `python3 build-support/check_repo_handbook.py`
- `python3 build-support/check_be_module_boundaries.py --mode full`

## Guardrails

- Structural rules should be enforced by small repo-local scripts with actionable error messages.
- CI path filters must stay aligned with the files each checker owns.
- Generated handbook or AGENTS content should be validated mechanically when a generator exists.

## Test and Validation

- Keep checker unit tests next to the checker implementation in `build-support/`.
- Add CI coverage when a new checker becomes part of the contributor contract.
- Prefer changed-files filters for expensive jobs and structural checks for fast feedback.

## Open Gaps

- There is no repo-native eval registry or environment descriptor layer yet.
- Many CI decisions still live only in workflow YAML and external `ci-tool` scripts.
- Observability, performance budgets, and evidence collection are not standardized for agent loops.
