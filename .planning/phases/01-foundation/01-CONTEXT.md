# Phase 1: Foundation - Context

**Gathered:** 2026-03-04
**Status:** Ready for planning

<domain>
## Phase Boundary

Build dependencies (FE Maven + BE CMake), catalog DDL (CREATE/DROP/ALTER CATALOG with type='adbc'), metadata browsing (SHOW DATABASES, SHOW TABLES, DESCRIBE), and Arrow→StarRocks type mapping. No data scanning — that's Phase 2.

</domain>

<decisions>
## Implementation Decisions

### Catalog Properties
- Mirror JDBC catalog property pattern but with `adbc.` prefix for namespace clarity
- Required properties: `type` (='adbc'), `adbc.driver` (='flight_sql'), `adbc.url` (gRPC endpoint)
- Optional properties: `adbc.username`, `adbc.password`, `adbc.token`, plus driver-specific properties passed through as-is
- Connection properties prefixed with driver name get forwarded: e.g., `flight_sql.timeout` passes through to the FlightSQL driver
- Metadata cache properties follow existing pattern: `adbc_meta_cache_enable`, `adbc_meta_cache_expire_sec`

### Connector Code Structure
- FE package: `com.starrocks.connector.adbc` — mirrors JDBC's `connector.jdbc` package
- Core classes follow JDBC naming pattern:
  - `ADBCConnector.java` (implements Connector interface)
  - `ADBCMetadata.java` (implements ConnectorMetadata)
  - `ADBCMetaCache.java` (Caffeine-based cache, same pattern as JDBCMetaCache)
  - `ADBCTable.java` (extends Table, similar to JDBCTable)
  - `ADBCSchemaResolver.java` (base class for driver-specific type mapping)
  - `FlightSQLSchemaResolver.java` (first concrete resolver)
- Schema resolver abstraction: base `ADBCSchemaResolver` with per-driver subclasses, same as JDBC's pattern (`MysqlSchemaResolver`, `PostgresSchemaResolver`, etc.)

### Type Mapping Rules
- Arrow types map to StarRocks types following natural correspondences:
  - int8/16/32/64 → TINYINT/SMALLINT/INT/BIGINT
  - uint8/16/32/64 → SMALLINT/INT/BIGINT/LARGEINT (promote to avoid overflow, same as JDBC unsigned handling)
  - float/double → FLOAT/DOUBLE
  - decimal128 → DECIMAL(p,s)
  - utf8/large_utf8 → VARCHAR
  - binary/large_binary → VARBINARY
  - boolean → BOOLEAN
  - date32 → DATE
  - timestamp (any unit, with/without tz) → DATETIME
  - null → NULL type
- Unsupported types (list, struct, map, union): log warning, exclude column from schema
- Type mapping lives in schema resolver so drivers can override (e.g., FlightSQL may handle timestamps differently)

### Build Integration
- FE: Add Maven dependencies to `fe/fe-core/pom.xml` — `org.apache.arrow.adbc:adbc-core` and `adbc-driver-flight-sql`
- BE: Add C++ ADBC as thirdparty dependency — follow existing pattern in `thirdparty/` (download script + CMake integration)
- Pin specific versions for reproducible builds
- BE C++ ADBC only needed for Phase 2 scanning — but add the build dependency now so it's validated early

### Claude's Discretion
- Exact Maven dependency versions (use latest stable compatible with existing Arrow/gRPC versions)
- Internal caching strategy details (Caffeine config, eviction policies)
- Error message wording for invalid properties
- Whether to use a factory pattern or direct instantiation for schema resolvers

</decisions>

<specifics>
## Specific Ideas

- "Like JDBC" — the user wants this to feel like a natural sibling of the JDBC catalog, not a foreign addition
- Driver extensibility from day one: `adbc.driver` property determines which schema resolver and connection factory to use
- Follow the existing connector registration pattern — ADBC should appear alongside jdbc, hive, iceberg in the catalog type registry

</specifics>

<code_context>
## Existing Code Insights

### Reusable Assets
- `JDBCConnector.java`: Reference implementation — ADBC connector mirrors this structure
- `JDBCMetadata.java`: Metadata operations pattern — database listing, table discovery, column metadata
- `JDBCMetaCache.java`: Caffeine cache pattern — reuse same cache strategy
- `JDBCTable.java`: Table representation with identifier quoting and partition tracking
- `JDBCSchemaResolver.java`: Base resolver pattern — ADBC creates parallel hierarchy
- `JDBCResource.java`: Resource/property validation pattern

### Established Patterns
- Connector SPI: `fe/fe-spi/src/main/java/com/starrocks/connector/` defines interfaces all connectors implement
- Catalog type registration: `ConnectorFactory` pattern registers new catalog types
- Metadata caching: Caffeine-based with configurable TTL, separate caches for different entity types
- Property validation: Required properties checked in connector constructor, descriptive error on missing

### Integration Points
- `ConnectorFactory`: Register "adbc" as new catalog type
- `CatalogType` enum: Add ADBC entry
- `fe/fe-core/pom.xml`: Add ADBC Maven dependencies
- `thirdparty/build-thirdparty.sh` + `thirdparty/vars-*.sh`: Add C++ ADBC library
- `be/CMakeLists.txt`: Link C++ ADBC library (needed for Phase 2 but validated in Phase 1 build)

</code_context>

<deferred>
## Deferred Ideas

None — discussion stayed within phase scope

</deferred>

---

*Phase: 01-foundation*
*Context gathered: 2026-03-04*
