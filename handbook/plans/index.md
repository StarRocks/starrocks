# Handbook Plans

Use this directory for repo-local execution plans that agents can read, update, and hand off without external context.

## Active Plans

- [Harness Engineering Roadmap](active/2026-03-27-harness-engineering-roadmap.md)

## Templates

- [Execution Plan Template](templates/execution-plan.md)

## Local Plans

Use `python3 build-support/handbook_plan.py create --local --title "<title>" --owner "<owner>"` to create a gitignored local plan under `handbook/plans/local/active/`.

If `handbook/plans/local/index.md` exists, agents should read it after this page. Local plans may add `- Overrides: handbook/plans/active/<plan>.md` to replace a tracked plan for the current checkout only.

Use `python3 build-support/handbook_plan.py list` to print the effective active plan set and `python3 build-support/handbook_plan.py complete --local --plan <path-or-slug>` to delete a finished local plan.

## Completed Plans

- [Completed Plans README](completed/README.md)
