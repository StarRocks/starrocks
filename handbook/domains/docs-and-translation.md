# Docs and Translation Domain

## Purpose

Map the boundary between internal engineering knowledge in `handbook/` and public product documentation in `docs/`.

## Entrypoints

- [`docs/AGENTS.md`](../../docs/AGENTS.md)
- [`docs/README.md`](../../docs/README.md)
- [`docs/docusaurus/README.md`](../../docs/docusaurus/README.md)

## Commands

- `cd docs/docusaurus && npm install`
- `cd docs/docusaurus && npm start`
- `cd docs/docusaurus && npm run build`

## Guardrails

- Product behavior, deployment, SQL semantics, and config docs belong in `docs/`, not `handbook/`.
- Repo operations, architecture notes, policies, and execution plans belong in `handbook/`, not `docs/`.
- User-facing config or metric changes should update both English and Chinese docs when applicable.

## Test and Validation

- Add new public pages to `docs/docusaurus/sidebars.js`.
- Keep cross-links relative and keep assets in `docs/_assets/`.
- When docs describe code behavior, validate the code path before updating prose.

## Open Gaps

- There is no mechanical checker for handbook-to-public-doc obligations yet.
- Translation freshness is not encoded as repo-local policy data.
- The boundary between doc generation workflows and public docs remains mostly convention-driven.
