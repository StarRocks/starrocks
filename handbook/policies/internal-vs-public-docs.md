# Internal vs Public Docs

## Intent

Keep internal engineering guidance separate from user-facing product documentation so both remain legible and maintainable.

## Applies To

- `handbook/`
- `docs/`
- `AGENTS.md` entrypoints that route contributors into the correct source of truth

## Enforcement

- `handbook/` owns repo topology, architecture notes, policies, quality scorecards, and execution plans.
- `docs/` owns user/admin/deployment/reference content and translation-facing documentation.
- Handbook pages may link to public docs for product behavior, but should not duplicate long product explanations.

## Exceptions

- Short cross-references are allowed when they keep contributors from looking in the wrong tree.
- Migration notes may temporarily exist in both places while a workflow moves from convention to enforcement.
