# Agent Change Flow

## Intent

Define the expected repo-local loop for agent-assisted changes while preserving human merge authority.

## Applies To

- Core maintainer workflows
- Repo-local plans and handbook navigation
- Local and CI verification for structural checks

## Enforcement

- Start at `AGENTS.md`, then follow `handbook/` and the nearest nested guide.
- Prefer repo-local plans and checkers over external context or ad hoc conventions.
- After reading `handbook/plans/index.md`, also read `handbook/plans/local/index.md` when it exists.
- A local plan with `- Overrides: handbook/plans/active/<plan>.md` replaces that tracked plan for the current checkout only.
- Local plans are machine-local working state: create them through `python3 build-support/handbook_plan.py`, keep them gitignored, and delete them when finished instead of archiving them in tracked completed plans. Directory-root local plan trees under `handbook/plans/local/active/<slug>/README.md` follow the same rule.
- Changes should carry fresh verification evidence before being described as complete.
- Agents can prepare and iterate on changes, but humans remain the final merge authority.

## Exceptions

- Manual human intervention is allowed when automation is missing or a judgment call is required.
- Future milestones may grant narrow autonomous flows for low-risk cleanup changes only after explicit policy updates.
