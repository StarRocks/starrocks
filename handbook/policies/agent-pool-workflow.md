# Agent Pool Workflow

## Intent

Keep parallel StarRocks backend compile loops fast and isolated by reusing a fixed worktree pool and a shared ccache instead of creating cold ad hoc worktrees for every agent run.

## Applies To

- `build-support/agent-pool.sh`
- `build-support/agent_pool.py`
- `AGENTS.md`
- `CLAUDE.md`
- Backend build and backend unit-test workflows that may compile code in parallel agent-owned worktrees

## Enforcement

- Agents that may run `./build.sh --be` or `./run-be-ut.sh` in parallel should acquire a slot through `build-support/agent-pool.sh` or `python3 build-support/agent_pool.py acquire --json` before starting build work.
- The default repo-local pool roots are `.worktrees/agent-pool/` for worktrees and `.agent-pool/` for persistent slot state.
- Keep one shared ccache. The pool workflow should export `CCACHE_BASEDIR=<repo-root>` and `CCACHE_NOHASHDIR=1`; do not split ccache by slot unless the user explicitly overrides that policy.
- Use the acquired slot's `worktree_path` as the working directory and the returned `env` map for subsequent backend build or backend unit-test commands.
- Release slots only when clean by default. Dirty slots stay owned until the user explicitly releases them with `--force` or continues work in that slot.
- Agents should rely on the repo rules in `AGENTS.md` or the symlinked `CLAUDE.md` plus the shared wrapper instead of tool-specific agent-pool skills.
- Keep the operational entrypoints documented in [`build-support/README.md`](../../build-support/README.md) so humans and agents share one workflow.

## Exceptions

- Serial work or tasks that do not compile backend code may stay outside the agent pool.
- Users may override the worktree root, state root, base revision, or `BUILD_TYPE` when debugging a special case.
- Emergency cleanup may recycle slot state or force-release a stale lock after confirming the prior owner is gone.
