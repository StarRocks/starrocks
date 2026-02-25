# StarRocks Testing Utilities and Mocks

This directory hosts shared test dependencies used across the project: common test code, utilities, fixtures, and mock/stub implementations. The goal is to maximize reuse, keep tests consistent, and reduce duplication.

## What lives here
- Utilities: helpers for assertions, data generation, temporary files/dirs, config parsing, JSON/CSV helpers.
- Mocks/Stubs/Fakes: lightweight implementations of SPI/core-facing interfaces for isolated tests (e.g., mock connectors, auth providers, catalog services).
- Fixtures: reusable datasets, schemas, DDLs, and golden files for deterministic tests.
- Test-only common code: shared abstractions and base classes to simplify test setup/teardown.

## Usage
- Import and reuse helpers instead of duplicating logic in individual test modules.
- Prefer fakes/stubs for fast unit tests; reserve heavy integrations for dedicated test suites.
- Keep fixtures small and deterministic; document assumptions in comments.

## Guidelines
- Deterministic: avoid time, randomness, and network unless explicitly controlled.
- Thread-safe: mocks/utilities should be safe for concurrent tests or document limitations.
- Minimal coupling: do not reference FE/BE internal classes unless strictly necessary; prefer public APIs/SPI.
- Clear ownership: place helpers close to their domain but keep them generic and reusable.
- No secrets: never include real credentials or PII in fixtures or logs.

## Non-goals
- Not a place for production utilities or runtime dependencies.
- Avoid embedding heavy external services; use in-memory fakes or containers only in dedicated integration tests.

## References
- Test suites: `fe/fe-core/src/test/`
- Project docs: https://docs.starrocks.io/