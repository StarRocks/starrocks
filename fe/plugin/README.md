# StarRocks FE Plugins

The `fe/plugin` directory hosts plugin implementations and integration glue for the Frontend. Plugins extend the FE core with capabilities beyond the built-in features by implementing interfaces from `fe/spi` and interacting with FE core through stable contracts.

Unlike connectors:
- Connectors focus on integrating external data sources and catalogs.
- Plugins focus on extending core capabilities (authorization, authentication, governance, observability, etc.) and do not act as data source bridges.

## Responsibilities
- Implement FE SPI contracts to augment core behavior.
- Provide optional features that can be enabled/disabled without altering FE internals.
- Remain decoupled from FE private classes; depend only on `fe/spi` and public abstractions.

## Typical plugin capabilities
- Authorization providers (e.g., Apache Ranger integration)
- Authentication providers (e.g., LDAP, Kerberos)
- Auditing and logging sinks
- Observability hooks (metrics/tracing exporters)
- Resource governance and admission control hooks
- Policy, masking, and row-level access integrations

Note: Exact plugin types depend on the available SPI extension points.

## How it interacts with FE core
- FE core programs against `fe/spi` interfaces.
- Plugins implement those interfaces and are discovered/loaded at runtime.
- FE core invokes plugin hooks during authentication, authorization, session, or administrative flows as defined by the SPI.

## Implementing a plugin
1. Identify relevant interfaces in `fe/spi` (e.g., authN/authZ, auditing, hooks).
2. Create a new module or package for your plugin and depend on `fe/spi`.
3. Implement the SPI contracts and provide discovery metadata.
4. Add configuration parsing/validation and document required settings.
5. Ensure thread-safety; plugins may be invoked concurrently.
6. Write tests (unit for SPI behavior; integration for end-to-end flows).

Guidelines:
- Keep implementations stateless where possible or guard state with proper synchronization.
- Fail fast on misconfiguration; surface actionable error messages.
- Do not block FE critical paths; use timeouts/backoff for I/O.
- Never log secrets or PII; honor FE security and redaction policies.

## Lifecycle
- Initialize: receive configuration/context and validate dependencies.
- Runtime: serve concurrent requests through defined SPI hooks.
- Shutdown: release external resources (clients, threads) idempotently.

## Configuration
- Use namespaced keys for plugin options.
- Support secure secret handling (env, keystores, injected credentials).
- Provide safe defaults and minimal required parameters.

## Versioning and compatibility
- Track and declare the `fe/spi` version supported by the plugin.
- Prefer backward-compatible changes; deprecate before breaking compatibility.
- Document compatibility with external systems (e.g., Ranger/LDAP/Kerberos versions).

## Security considerations
- Enforce authorization and authentication semantics defined by FE.
- Validate and sanitize all external inputs.
- Avoid leaking sensitive data in logs, errors, or metrics.

## Non-goals
- Plugins are not data source connectors and should not expose catalog/table access.
- Plugins must not depend on FE private/internal classes beyond SPI boundaries.

## References
- SPI contracts: `fe/spi/`
- FE core entry points: `fe/fe-core/src/main/java/com/starrocks/`
- Project docs: https://docs.starrocks.io/

