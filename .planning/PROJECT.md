# ADBC External Catalog for StarRocks

## What This Is

An external catalog connector for StarRocks that uses the Apache Arrow ADBC (Arrow Database Connectivity) API to query remote databases. Modeled after the existing JDBC catalog with full feature parity — predicate pushdown, limit pushdown, partitioning, materialized view support, and statistics. The initial driver is Arrow Flight SQL, but the architecture supports pluggable drivers (PostgreSQL, etc.) via an `adbc.driver` catalog property.

## Core Value

Users can `CREATE EXTERNAL CATALOG` with ADBC and query remote Arrow Flight SQL (and future ADBC-compatible) databases with the same ease, performance, and feature set as JDBC catalogs — including materialized views and partition-aware queries.

## Requirements

### Validated

- ✓ JDBC external catalog with full connector infrastructure — existing
- ✓ Predicate/limit pushdown framework for external catalogs — existing
- ✓ Materialized view support for external catalogs — existing
- ✓ External catalog DDL (CREATE/DROP/ALTER CATALOG) — existing
- ✓ Connector SPI and metadata caching — existing
- ✓ JNI bridge pattern for BE data reading (java-extensions/) — existing
- ✓ Pipeline-based vectorized execution with Chunk/Column format — existing

### Active

- [ ] ADBC catalog connector in FE (Java ADBC API for metadata)
- [ ] ADBC data scanner in BE (native C++ ADBC for data reading)
- [ ] Arrow RecordBatch → StarRocks Chunk direct conversion in C++
- [ ] Pluggable driver model (`adbc.driver` property: flight_sql, future: postgresql, etc.)
- [ ] SQL string predicate pushdown (same mechanism as JDBC)
- [ ] Limit pushdown
- [ ] Partition discovery and pruning
- [ ] Materialized view support (refresh, rewrite)
- [ ] Column statistics collection via ADBC
- [ ] Type mapping (Arrow types ↔ StarRocks types)
- [ ] Connection pooling and lifecycle management
- [ ] FlightSQL driver implementation as first concrete driver

### Out of Scope

- Substrait-based pushdown — using SQL string pushdown like JDBC, not Substrait plans
- Native ADBC C driver wrappers — using the official apache/arrow-adbc Java and C++ APIs
- Write path (INSERT INTO remote tables via ADBC) — read-only for now
- Raw Arrow Flight protocol (non-ADBC) — ADBC abstracts this

## Context

**Existing JDBC catalog** serves as the primary reference implementation:
- FE connector: `fe/fe-core/src/main/java/com/starrocks/connector/jdbc/`
- BE connector: `be/src/connector/jdbc_connector.cpp`
- JNI bridge: `java-extensions/jdbc-bridge/`
- Schema resolvers per database type

**Key difference from JDBC:** BE uses native C++ ADBC instead of JNI bridge. FE uses Java ADBC for metadata. Arrow data flows directly to StarRocks Chunk format in C++ (zero-copy where possible).

**Arrow ADBC libraries:**
- Java: `org.apache.arrow.adbc:adbc-core`, `adbc-driver-flight-sql`
- C++: `adbc` C++ library (thirdparty dependency for BE)

**Branch:** `feature/adbc-catalog-2`

## Constraints

- **Architecture**: FE Java ADBC for metadata, BE native C++ ADBC for data scanning — no JNI for data path
- **Driver extensibility**: Must not hardcode FlightSQL assumptions; use `adbc.driver` property
- **JDBC parity**: Full feature parity with JDBC catalog (pushdown, partitioning, MV, statistics)
- **Arrow data path**: Arrow RecordBatch → StarRocks Chunk direct conversion in C++ (zero-copy where possible)
- **Compatibility**: Must work with existing catalog infrastructure (DDL, metadata cache, optimizer rules)

## Key Decisions

| Decision | Rationale | Outcome |
|----------|-----------|---------|
| Arrow Flight SQL only initially | Most common ADBC use case, clean starting point | — Pending |
| FE Java + BE native C++ (no JNI for data) | Avoids JNI overhead for data path, leverages Arrow's native C++ performance | — Pending |
| SQL string pushdown (not Substrait) | Matches JDBC pattern, simpler initial implementation | — Pending |
| Pluggable driver property from day one | Prevents refactoring when adding PostgreSQL driver later | — Pending |
| Arrow → Chunk direct conversion | Zero-copy where possible, avoids serialization overhead | — Pending |

---
*Last updated: 2026-03-04 after initialization*
