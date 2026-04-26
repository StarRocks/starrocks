# Reliability First

## Intent

Prefer trustworthy mechanical checks, reproducible evidence, and explicit gaps over maximizing authoring speed.

## Applies To

- Repo-local handbook structure
- Custom structural checkers
- Validation loops for FE, BE, SQL, docs, and CI/tooling changes

## Enforcement

- New contributor-facing guidance should be discoverable from `AGENTS.md` and `handbook/`.
- Structural rules should become local checks or CI jobs once they are stable enough to enforce mechanically.
- When a rule cannot be automated yet, document it as a gap instead of pretending it is covered.

## Exceptions

- Emergency fixes can use the smallest viable validation loop when full coverage is unavailable.
- Temporary gaps are acceptable when documented in the relevant domain page or active plan.
