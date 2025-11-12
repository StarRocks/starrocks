# StarRocks FE SPI (Service Provider Interface)

The `fe/spi` module defines the public Service Provider Interfaces (SPIs) for the StarRocks Frontend. It is the foundation of the pluggable architecture: connectors and plugins must implement these contracts, while the FE core programs against the interfaces.

## Responsibilities
- Provide stable, minimal contracts for connectors and plugins.
- Decouple FE core from concrete implementations.
- Enable safe evolution via versioning and deprecation policies.

## Design goals
- Stability first: interfaces evolve conservatively and compatibly.
- Clear separation: SPI must not depend on FE core internal types.
- Minimal surface: small, orthogonal interfaces; no hidden coupling.
- Testability: interfaces are mock-friendly and deterministic.

## What lives here
- Public Java interfaces and data contracts for plugin authors.
- Common exceptions, result/status types intended for SPI boundaries.
- Documentation and guidance for implementers.

## Typical extension points
- Connectors (external catalogs, metadata/external table access, etc.)
- Resource and system integrations (e.g., external services)
- Administrative or management hooks exposed via well-defined SPI

## How FE core uses SPI
- FE core depends only on SPI interfaces.
- Concrete plugins/connectors are discovered and wired at runtime.
- Implementations must be prepared for concurrent use by FE subsystems.

## Testing guidance
- Unit test against SPI interfaces with mocks/fakes.
- Add integration tests that exercise real interactions where applicable.
- Include negative tests for error paths and boundary conditions.

## References
- Architecture overview: see project root README and FE module docs
- Testing structure: `test/sql/`, `fe/fe-core/src/test/`
- StarRocks documentation: https://docs.starrocks.io/

