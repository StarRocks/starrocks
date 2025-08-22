# StarRocks FE Utilities and Base Libraries

The `fe/utils` module provides foundational, business-agnostic utilities for the Frontend. It offers reusable helpers and lightweight base libraries that other FE modules can depend on. No business logic lives here.

## Responsibilities
- Common helpers: collections, concurrency, I/O, serialization, string/bytes, hashing/encoding.
- Runtime utilities: clocks/timers, retry/backoff, rate limiting, validation/preconditions.
- Error handling: shared exceptions and error codes intended for cross-module use.
- Small, focused abstractions that simplify FE development.

## Design principles
- No business logic or domain-specific behavior.
- Dependency-light: avoid coupling to FE core internals; no cycles.
- Thread-safe or clearly documented threading semantics.
- Stable, minimal APIs; prefer composition over inheritance.
- Deterministic and side-effect-free where possible.

## What lives here (examples)
- Utility classes (e.g., StringUtils, CollectionUtils, Json/Yaml helpers).
- Concurrency helpers (executor wrappers, retry/backoff, locks).
- Validation and precondition checks.
- Lightweight data holders and result types shared across modules.

What does NOT live here:
- SQL/Planner/Optimizer/Analyzer code
- Connector/Plugin implementations
- RPC clients, storage, or environment-specific integrations

## Usage guidelines
- Other FE modules may depend on `fe/utils`; `fe/utils` must not depend on FE business modules.
- Keep helpers generic; avoid leaking domain concepts.
- Prefer small, single-purpose classes and clear method contracts.
- Cover utilities with unit tests and document edge cases.

## Testing
- Provide unit tests for all utilities, including negative and concurrency scenarios.
- Ensure determinism; avoid non-deterministic time or randomness unless injected.

## Versioning and compatibility
- Evolve APIs conservatively and keep backward compatibility.
- Deprecate before removal; document migration paths.

## References
- FE core: `fe/fe-core/src/main/java/com/starrocks/`
- Testing: `fe/fe-core/src/test/`
- Project docs: https://docs.starrocks.io/

