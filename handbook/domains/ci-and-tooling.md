# CI and Tooling Domain

## Purpose

Map the local scripts and GitHub workflows that enforce structural rules, run validation, and expose repo-native guardrails to contributors and agents.

## Entrypoints

- [`build-support/README.md`](../../build-support/README.md)
- [`build-support/check_gensrc_schema_compatibility.py`](../../build-support/check_gensrc_schema_compatibility.py)
- [`.github/workflows/ci-pipeline.yml`](../../.github/workflows/ci-pipeline.yml)
- [`.github/workflows/ci-pipeline-branch.yml`](../../.github/workflows/ci-pipeline-branch.yml)

## Commands

- `python3 -m unittest build-support/test_check_repo_handbook.py`
- `python3 -m unittest build-support/test_check_gensrc_schema_compatibility.py`
- `python3 build-support/check_repo_handbook.py`
- `python3 build-support/check_gensrc_schema_compatibility.py --mode changed --base origin/main`
- `python3 build-support/check_be_module_boundaries.py --mode full`

## Guardrails

- Structural rules should be enforced by small repo-local scripts with actionable error messages.
- CI path filters must stay aligned with the files each checker owns.
- Generated handbook or AGENTS content should be validated mechanically when a generator exists.
- Self-hosted jobs must never `rm -rf` the runner workspace: all jobs of a repo share one
  workspace per runner, so a nuke forces the next `actions/checkout` into a full re-clone.
  Clean with `git -C ${{ github.workspace }} clean -ffdxq` (falling back to `rm -rf` when no
  repo exists) so the clone is reused and fetches stay incremental. Workspace-root checkouts
  use `fetch-depth: 0`; shallow or sparse checkouts go to a dedicated `path:` so they never
  flip the shared clone between shallow/full or sparse/full states.
- Because that clone is reused, no `run:` step may assume a pristine repo. Resolve branches
  explicitly (`git checkout -B "$BRANCH" "refs/remotes/origin/$BRANCH"`, not `git checkout
  "$BRANCH"`, which fails with "matched multiple remote tracking branches" when the clone
  carries a second remote) and force-update anything a previous run may have left behind
  (`git fetch --force`, `git checkout -B merge_pr`).

## Test and Validation

- Keep checker unit tests next to the checker implementation in `build-support/`.
- Add CI coverage when a new checker becomes part of the contributor contract.
- Prefer changed-files filters for expensive jobs and structural checks for fast feedback.

## Open Gaps

- There is no repo-native eval registry or environment descriptor layer yet.
- Many CI decisions still live only in workflow YAML and external `ci-tool` scripts.
- Observability, performance budgets, and evidence collection are not standardized for agent loops.
