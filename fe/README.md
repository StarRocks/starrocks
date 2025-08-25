# StarRocks Frontend (FE) Overview

The Frontend (FE) is the Java layer of StarRocks that provides the SQL interface and cluster coordination. It parses and validates SQL, builds optimized query plans, manages metadata, and coordinates execution with the Backend (BE).

## Key responsibilities
- SQL parsing and AST construction
- Semantic analysis and validation
- Cost-based optimization and plan generation
- Metadata/catalog management and statistics
- Session management, authentication, and authorization
- Scheduling and coordination of query execution with BEs

## Directory layout (high level)
- `fe-core/`
  - `src/main/java/com/starrocks/`
    - `sql/` — Analyzer and optimizer
    - `planner/` — Plan fragments and physical planning
    - `catalog/` — Metadata (databases, tables, partitions, statistics)
    - `qe/` — Query execution coordination and session management
    - `privilege/` — Authentication and authorization
    - `scheduler/`, `load/`, `backup/` — Scheduling, data loading, backup/restore
- `fe/fe-grammar/` — ANTLR grammars (.g4) for StarRocks SQL
- `fe/fe-parser/` — StarRocks SQL parser and full AST definitions
- `fe/fe-spi/` — FE Service Provider Interfaces (contracts for connectors/plugins)
- `fe/connector/` — Data source connector implementations and FE-side integration
- `fe/plugin/` — Plugin implementations extending FE core capabilities (non-data-source)
- `fe/fe-utils/` — FE utilities and base libraries (no business logic)
- `fe/fe-testing/` — Shared test utilities, fixtures, and mock implementations

Related:
- `be/` — C++ Backend engine (execution and storage)

## Extensibility overview
- Syntax/AST:
  - Extend SQL grammar in `fe/fe-grammar/` (ANTLR .g4 files).
  - Parser and AST live in `fe/fe-parser/` (package `com.starrocks.sql.parser` and `com.starrocks.sql.ast`).
  - Follow AST conventions: include source positions; keep nodes immutable; push semantics to analyzer.
  - Update analyzer/optimizer and add tests.
- Connectors (data sources):
  - Implement FE SPI in a dedicated module; provide discovery metadata (e.g., ServiceLoader or plugin manifest).
  - Register catalogs and handle metadata/scan capabilities.
- Plugins (core extensions):
  - Implement FE SPI to extend authentication, authorization, auditing, observability, governance, etc.
  - Examples: Ranger-based authorization, LDAP/Kerberos authentication.

## Development guidelines
- AST immutability:
  - Do not add setter methods to AST nodes; do not use AST as temporary storage between Parser and Analyzer; keep the AST immutable.
- Utilities:
  - `fe/fe-utils/` contains general-purpose helpers only; no business logic or FE internals.
- SPI boundaries:
  - Keep connectors/plugins decoupled from FE internals; depend only on `fe/fe-spi/`.
- Performance and correctness:
  - Prefer unambiguous grammar; avoid heavy work in the parser; push semantic checks to analyzer.
- Security:
  - Validate inputs; avoid leaking secrets/PII in logs; honor FE authN/authZ policies.

## Testing
- Unit tests for FE modules: `fe/fe-'xxx'/src/test/`
- Shared testing helpers and mocks: `fe/fe-testing/`
- Add positive/negative/edge cases for new syntax, SPI implementations, and security paths.

## Critical notes
- Build system is heavy. Avoid running full builds or UT unless necessary.
- Prefer targeted changes and module-level verification.

## References
- Grammar: `fe/fe-grammar/`
- Parser and AST: `fe/fe-parser/`
- SPI contracts: `fe/fe-spi/`
- Connectors: `fe/connector/`
- Plugins: `fe/plugin/`
- Utilities: `fe/fe-utils/`
- Testing: `fe/fe-testing/`
- Docs: https://docs.starrocks.io/
