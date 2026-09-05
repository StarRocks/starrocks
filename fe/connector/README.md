# StarRocks Data Source Connectors

This directory is the home for data source connector implementations and FE-side integration. Each connector is an independent module that implements the contracts defined in `fe/spi`, enabling StarRocks to query heterogeneous external data sources.

## Responsibilities
- Provide concrete implementations of FE SPI for external catalogs and tables.
- Bridge StarRocks FE planning/execution with external metadata and I/O.
- Offer minimal integration and shared FE-side utilities for connectors.

## Relationship to FE SPI
- FE core programs against interfaces defined in `fe/spi`.
- Connectors implement those interfaces and are discovered/loaded by FE.
- Keep implementations decoupled from FE internals beyond the SPI boundary.

## Where things live
- This package: FE-side integration classes and built-in connector.
- Independent connectors: separate modules that depend on `fe/spi` and optionally reuse helpers from here.
- Do not place FE private types into connector-facing APIs.

## Implementing a new connector
1. Identify the relevant SPI interfaces in `fe/spi` (capabilities, metadata, scanning).
2. Create a new module for your connector; depend on `fe/spi`.
3. Implement the SPI contracts and provide discovery metadata (e.g., ServiceLoader entries or plugin manifest supported by StarRocks).
4. Add configuration parsing and validation; prefer explicit, typed configs.
5. Integrate with FE by registering the connector and exposing catalog(s).
6. Write tests (unit for SPI conformance; integration for end-to-end queries).

Guidelines:
- Favor stateless or explicitly thread-safe implementations.
- Fail fast on misconfiguration; return well-typed errors.
- Avoid blocking FE critical paths; use timeouts and backoff for I/O.
- Log actionable messages; never log secrets or PII.

## Lifecycle
- Initialize: receive context/config, validate connectivity and schema access.
- Runtime: serve concurrent metadata and split/scan requests.
- Shutdown: release resources (clients, threads) idempotently.

## Configuration
- Use namespaced keys for connector-specific options.
- Support secure credentials handling (env/keystore/injected secrets).
- Provide sensible defaults and minimal required options.

## Compatibility and versioning
- Track the `fe/spi` version your connector supports.
- Prefer backward-compatible changes; deprecate before breaking.
- Document supported external system versions and features.

## Testing
- Unit tests against SPI interfaces with mocks/fakes.
- Integration tests that query real or embedded backends where feasible.
- Include negative tests for permissions, schema drift, and network failures.

## Security considerations
- Enforce authorization checks exposed via SPI contexts.
- Sanitize external inputs; validate identifiers and filter expressions.
- Obfuscate or omit sensitive fields from logs and error messages.

## References
- FE SPI (contracts): `fe/spi/`
- FE connector integration: this package (`com.starrocks.connector`)
- Project docs: https://docs.starrocks.io/

