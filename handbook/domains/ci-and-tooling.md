# CI and Tooling Domain

## Purpose

Map the local scripts and GitHub workflows that enforce structural rules, run validation, and expose repo-native guardrails to contributors and agents.

## Entrypoints

- [`build-support/README.md`](../../build-support/README.md)
- [`build-support/agent_pool.py`](../../build-support/agent_pool.py)
- [`build-support/check_gensrc_schema_compatibility.py`](../../build-support/check_gensrc_schema_compatibility.py)
- [`handbook/policies/agent-pool-workflow.md`](../policies/agent-pool-workflow.md)
- [`.github/workflows/ci-pipeline.yml`](../../.github/workflows/ci-pipeline.yml)
- [`.github/workflows/ci-pipeline-branch.yml`](../../.github/workflows/ci-pipeline-branch.yml)

## Commands

- `python3 -m unittest build-support/test_check_repo_handbook.py`
- `python3 -m unittest build-support/test_check_gensrc_schema_compatibility.py`
- `python3 -m unittest build-support/test_agent_pool.py`
- `python3 build-support/agent_pool.py status --json`
- `python3 build-support/agent_pool.py acquire --base HEAD --json`
- `python3 build-support/check_repo_handbook.py`
- `python3 build-support/check_gensrc_schema_compatibility.py --mode changed --base origin/main`
- `python3 build-support/check_be_module_boundaries.py --mode full`

## Guardrails

- Structural rules should be enforced by small repo-local scripts with actionable error messages.
- Parallel backend compile loops should use the repo-local agent-pool workflow instead of creating one-off cold worktrees.
- Agent-pool build loops must keep the shared repo `thirdparty/` tree and the slot `CMAKE_BUILD_PREFIX` from the acquired env attached to every BE build or UT command; prefer `build-support/agent-pool.sh run -- ...` over manually reassembling that environment.
- CI path filters must stay aligned with the files each checker owns.
- Generated handbook or AGENTS content should be validated mechanically when a generator exists.

## Test and Validation

- Keep checker unit tests next to the checker implementation in `build-support/`.
- Keep agent-pool unit tests next to the implementation and validate the CLI before changing the repo rules or policy text.
- Add CI coverage when a new checker becomes part of the contributor contract.
- Prefer changed-files filters for expensive jobs and structural checks for fast feedback.

## Open Gaps

- There is no repo-native eval registry or environment descriptor layer yet.
- Many CI decisions still live only in workflow YAML and external `ci-tool` scripts.
- Observability, performance budgets, and evidence collection are not standardized for agent loops.
- Agent-pool adoption relies on repo entrypoint rules today; there is no mechanical enforcement that external agent runtimes honor them.
