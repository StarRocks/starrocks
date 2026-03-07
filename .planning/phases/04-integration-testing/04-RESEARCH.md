# Phase 4: Integration Testing - Research

**Researched:** 2026-03-07
**Domain:** FE plan tests (MockedADBCMetadata) + SQL integration tests (T/R files) + E2E environment (DuckDB Flight SQL)
**Confidence:** HIGH

## Summary

Phase 4 adds test coverage for the ADBC external catalog feature across two levels: (1) FE plan tests using a MockedADBCMetadata class registered in ConnectorPlanTestBase, and (2) SQL integration tests using the standard T/R file format in `test/sql/test_adbc_catalog/`. The plan test infrastructure requires creating a new MockedADBCMetadata class that mirrors MockedJDBCMetadata (implements ConnectorMetadata, returns fake ADBCTable instances), registering it in ConnectorPlanTestBase alongside the existing JDBC/Hive/Iceberg/Paimon/DeltaLake/Kudu mocks, and writing test classes that verify EXPLAIN output shows ADBC_SCAN_NODE with correct pushdowns.

The SQL integration tests follow the established StarRocks T/R pattern: T files contain SQL statements with `${variable}` substitutions from sr.conf, R files contain expected results. Tests are gated on sr.conf `[.flightsql]` variables -- when empty, tests skip gracefully. For local E2E verification, sqlflite (voltrondata/sqlflite) provides a ready-to-use Flight SQL server backed by DuckDB, available as both a Docker image and standalone binary.

**Primary recommendation:** Mirror the JDBC mocked metadata and plan test patterns exactly. The codebase has well-established patterns for every artifact needed; this is a pattern-replication phase, not a design phase.

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions
- **SQL Integration Test Scenarios:** DDL tests (CREATE/SHOW/DESCRIBE/ALTER/DROP CATALOG), Query tests (SELECT *, column pruning, WHERE pushdown, LIMIT pushdown, combined pushdown, cross-catalog joins, subqueries, EXPLAIN showing pushed SQL), MV tests (CREATE MV, REFRESH, SELECT, query rewrite via EXPLAIN, DROP MV)
- **File organization:** Multiple T/R files -- test_adbc_ddl, test_adbc_query, test_adbc_mv (separate files per feature area)
- **E2E Test Environment:** DuckDB Flight SQL for local verification; sr.conf gating with `[.flightsql]` section; tests skip gracefully when variables empty
- **sr.conf variables:** `external_flightsql_ip` and `external_flightsql_port` only
- **CI Integration:** Skip CI automation for now; document Docker sidecar approach but don't block on CI infra provisioning
- **Seed data:** Include a seed data setup script that creates test tables in DuckDB with known data for deterministic results

### Claude's Discretion
- MockedADBCMetadata implementation -- mirror MockedJDBCMetadata pattern exactly, register in ConnectorPlanTestBase, test EXPLAIN for ADBC_SCAN_NODE with pushdowns
- Plan test coverage scope and number of test methods
- Exact seed data schema and row counts
- Test cleanup approach and isolation strategy

### Deferred Ideas (OUT OF SCOPE)
- CI pipeline provisioning of DuckDB Flight SQL Docker container
- Performance/load testing against Flight SQL endpoints
- Write path tests (INSERT INTO remote)
</user_constraints>

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|-----------------|
| TEST-01 | MockedADBCMetadata registered in ConnectorPlanTestBase; ADBC plan tests pass (EXPLAIN shows ADBC_SCAN_NODE with pushdowns) | MockedJDBCMetadata pattern fully documented; ConnectorPlanTestBase registration pattern mapped (mockJDBCCatalogImpl -> mockADBCCatalogImpl); ADBCTable constructor and properties understood |
| TEST-02 | SQL integration test files exist in `test/sql/test_adbc_catalog/` with T/R pairs covering DDL, queries, and MVs | JDBC catalog T/R file patterns documented; sr.conf variable substitution pattern understood; uuid0 cleanup pattern mapped |
| TEST-03 | `test/conf/sr.conf` has `[.flightsql]` section with `external_flightsql_ip`, `external_flightsql_port` variables | sr.conf structure fully mapped; existing `[.mysql]` section serves as exact template |
| TEST-04 | SQL integration tests pass when run against a manually provisioned Flight SQL server | sqlflite Docker image and CLI documented; seed data script approach defined |
</phase_requirements>

## Standard Stack

### Core
| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| JUnit 5 (Jupiter) | 5.x | FE unit/plan test framework | Standard StarRocks FE test framework |
| StarRocks SQL-tester | n/a | T/R SQL integration tests | Standard StarRocks integration test framework in `test/` |
| sqlflite (voltrondata) | latest | DuckDB Flight SQL server for E2E | Ready-to-use Docker image, DuckDB backend, supports auth |

### Supporting
| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| Mockito | existing | Mocking in FE tests | Not needed here -- MockedADBCMetadata is a concrete implementation, not a mock |
| Docker | any | Running sqlflite container | Local E2E testing |

### Alternatives Considered
| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| sqlflite Docker | Native DuckDB `--flight-sql` flag | DuckDB CLI Flight SQL support is experimental and may not be available in all builds; sqlflite is purpose-built |
| sqlflite | porter (TFMV/porter) | Porter is newer and less proven; sqlflite has more community usage |

## Architecture Patterns

### Recommended Project Structure
```
fe/fe-core/src/test/java/com/starrocks/
├── connector/adbc/
│   └── MockedADBCMetadata.java          # New: mocked ConnectorMetadata for ADBC
├── sql/plan/
│   └── ADBCScanPlanTest.java            # New: plan tests for ADBC scan node
└── sql/optimizer/rule/transformation/materialization/
    └── MvRewriteADBCTest.java           # New: MV rewrite tests for ADBC (optional)

test/
├── conf/
│   └── sr.conf                          # Modified: add [.flightsql] section
├── sql/test_adbc_catalog/
│   ├── T/
│   │   ├── test_adbc_ddl               # New: DDL lifecycle tests
│   │   ├── test_adbc_query             # New: query pushdown tests
│   │   └── test_adbc_mv               # New: MV lifecycle tests
│   └── R/
│       ├── test_adbc_ddl               # New: expected DDL results
│       ├── test_adbc_query             # New: expected query results
│       └── test_adbc_mv               # New: expected MV results
└── lib/
    └── setup_flightsql_data.sql        # New: seed data script for DuckDB
```

### Pattern 1: MockedADBCMetadata (mirrors MockedJDBCMetadata exactly)
**What:** A concrete ConnectorMetadata implementation that returns fake ADBCTable instances with predefined schemas, without requiring a real ADBC connection
**When to use:** FE plan tests that need to verify EXPLAIN output for ADBC queries

**Key implementation details from MockedJDBCMetadata (source: `fe/fe-core/src/test/java/com/starrocks/connector/jdbc/MockedJDBCMetadata.java`):**
```java
// MockedADBCMetadata must:
// 1. Implement ConnectorMetadata
// 2. Return Table.TableType.ADBC from getTableType()
// 3. Return ADBCTable instances from getTable() (not JDBCTable)
// 4. Provide listDbNames(), listTableNames(), getDb()
// 5. Provide listPartitionNames(), getPartitions() for MV tests
// 6. Use ReentrantReadWriteLock for thread safety (same pattern as JDBC)
// 7. Define MOCKED_ADBC_CATALOG_NAME constant (e.g., "adbc0")

// ADBCTable constructor:
// new ADBCTable(id, name, fullSchema, dbName, catalogName, properties)
// Properties must include "adbc.driver" = "flight_sql", "adbc.url" = "grpc://127.0.0.1:31337"
```

### Pattern 2: ConnectorPlanTestBase Registration
**What:** Register the mocked ADBC catalog in `mockAllCatalogs()` and the `mockCatalog()` switch
**When to use:** Always -- this is required for any plan test to use the ADBC catalog

**Source: `ConnectorPlanTestBase.java` lines 97-107 and 117-139:**
```java
// In mockAllCatalogs():
mockADBCCatalogImpl(metadataMgr);

// In mockCatalog() switch:
case MockedADBCMetadata.MOCKED_ADBC_CATALOG_NAME:
    mockADBCCatalogImpl(metadataMgr);
    break;

// New method:
private static void mockADBCCatalogImpl(MockedMetadataMgr metadataMgr) throws DdlException {
    Map<String, String> properties = Maps.newHashMap();
    properties.put("type", "adbc");
    properties.put("adbc.driver", "flight_sql");
    properties.put("adbc.url", "grpc://127.0.0.1:31337");
    GlobalStateMgr.getCurrentState().getCatalogMgr()
        .createCatalog("adbc", MockedADBCMetadata.MOCKED_ADBC_CATALOG_NAME, "", properties);
    MockedADBCMetadata mockedADBCMetadata = new MockedADBCMetadata(properties);
    metadataMgr.registerMockedMetadata(
        MockedADBCMetadata.MOCKED_ADBC_CATALOG_NAME, mockedADBCMetadata);
}
```

### Pattern 3: Plan Test Assertions for ADBC
**What:** Test class that extends ConnectorPlanTestBase, calls getFragmentPlan(), and asserts EXPLAIN output
**When to use:** Verifying ADBC_SCAN_NODE appears with correct columns, filters, and LIMIT

**Source: `JDBCIdentifierQuoteTest.java` for pattern:**
```java
public class ADBCScanPlanTest extends ConnectorPlanTestBase {
    @BeforeAll
    public static void beforeClass() throws Exception {
        ConnectorPlanTestBase.beforeClass();
    }

    @Test
    public void testADBCSelectAll() throws Exception {
        String sql = "select a, b from adbc0.partitioned_db0.tbl0";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "SCAN ADBC");
        assertContains(plan, "QUERY: SELECT");
    }
}
```

### Pattern 4: SQL Integration T/R Files
**What:** T files with SQL statements using sr.conf variable substitution; R files with expected results
**When to use:** E2E testing against a real Flight SQL server

**Source: `test/sql/test_jdbc_catalog/T/test_binary_type`:**
```sql
-- name: test_adbc_ddl_lifecycle
-- create catalog
set catalog default_catalog;
create external catalog adbc_catalog_${uuid0}
properties
(
    "type"="adbc",
    "adbc.driver"="flight_sql",
    "adbc.url"="grpc://${external_flightsql_ip}:${external_flightsql_port}"
);
show databases from adbc_catalog_${uuid0};
-- cleanup
set catalog default_catalog;
drop catalog adbc_catalog_${uuid0};
```

### Pattern 5: sr.conf Section Addition
**What:** Add `[.flightsql]` under `[env]` section with Flight SQL connection variables
**When to use:** Always -- required for SQL integration test variable substitution

**Source: sr.conf existing pattern (`[.mysql]` section):**
```ini
  [.flightsql]
    external_flightsql_ip =
    external_flightsql_port =
```

### Anti-Patterns to Avoid
- **Creating a real AdbcConnection in plan tests:** Plan tests must use MockedADBCMetadata, never real connections. The point is testing FE query planning logic, not ADBC connectivity.
- **Hardcoded host/port in T files:** Always use `${external_flightsql_ip}` and `${external_flightsql_port}` from sr.conf.
- **Missing cleanup in T files:** Always DROP CATALOG and any test objects at end of test case. Use `${uuid0}` for unique names.
- **Testing MV refresh in plan tests:** MV refresh requires a running BE cluster. Plan tests can only verify MV creation SQL parsing and query rewrite in EXPLAIN, not actual data refresh.

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Flight SQL test server | Custom Arrow Flight server | sqlflite Docker image (`voltrondata/sqlflite:latest`) | Production-quality, supports auth, TLS, DuckDB backend |
| Mocked metadata framework | Custom mock framework | ConnectorMetadata interface + MockedMetadataMgr | Established pattern used by 6+ other connector types |
| Test result validation | Custom diff tool | StarRocks SQL-tester (`python3 run.py -v`) | Standard framework handles uuid substitution, result matching |
| Test data generation | Random data generators | Static seed SQL script | Deterministic results needed for T/R comparison |

## Common Pitfalls

### Pitfall 1: ADBCTable vs JDBCTable in MockedMetadata
**What goes wrong:** Using JDBCTable instead of ADBCTable in the mocked metadata, causing TableType mismatch
**Why it happens:** Copy-paste from MockedJDBCMetadata without updating the table construction
**How to avoid:** MockedADBCMetadata.getTableType() must return ADBC. Table instances must be `new ADBCTable(...)` not `new JDBCTable(...)`.
**Warning signs:** Plan tests show JDBC_SCAN_NODE instead of ADBC_SCAN_NODE in EXPLAIN output

### Pitfall 2: Missing CatalogMgr.createCatalog() Before registerMockedMetadata()
**What goes wrong:** Tests fail with "catalog not found" even though metadata is registered
**Why it happens:** MockedMetadataMgr.getOptionalMetadata() requires the catalog to exist in CatalogMgr first
**How to avoid:** Always call `GlobalStateMgr.getCurrentState().getCatalogMgr().createCatalog(...)` before `metadataMgr.registerMockedMetadata(...)`
**Warning signs:** SemanticException: "Unknown catalog" in plan tests

### Pitfall 3: EXPLAIN Output String Matching for ADBC
**What goes wrong:** Plan tests fail because EXPLAIN output format for ADBC differs from JDBC
**Why it happens:** ADBCScanNode uses `"SCAN ADBC"` as display name (line 49 of ADBCScanNode.java), uses double-quote identifiers by default
**How to avoid:** Assert `"SCAN ADBC"` not `"SCAN JDBC"`. Assert double-quoted identifiers (`"col"`) not backticks.
**Warning signs:** assertContains failures in plan test output

### Pitfall 4: sr.conf Variable Scoping
**What goes wrong:** Variables from `[.flightsql]` section not available in T file substitutions
**Why it happens:** The `[env]` section nesting must be correct; `[.flightsql]` must be a subsection of `[env]`
**How to avoid:** Place `[.flightsql]` at the same indentation level as `[.mysql]`, `[.hive]`, etc. under `[env]`
**Warning signs:** `${external_flightsql_ip}` appears literally in executed SQL instead of being substituted

### Pitfall 5: ADBCTable Properties Must Include adbc.driver
**What goes wrong:** ADBCScanNode.getADBCDriverName() returns empty string, BE cannot load driver
**Why it happens:** MockedADBCMetadata properties map missing `adbc.driver` key
**How to avoid:** Properties passed to MockedADBCMetadata constructor must include `"adbc.driver" = "flight_sql"` and `"adbc.url" = "grpc://..."` at minimum
**Warning signs:** EXPLAIN output shows empty DRIVER field

### Pitfall 6: Cross-Catalog Join Tests Need Native Table Setup
**What goes wrong:** Cross-catalog join tests fail because there is no native StarRocks table to join against
**Why it happens:** Plan tests extend ConnectorPlanTestBase which sets up mocked external catalogs but also needs PlanTestBase's native tables
**How to avoid:** PlanTestBase.beforeClass() (called by ConnectorPlanTestBase.beforeClass()) creates test tables t0, t1, t2 in the default catalog. Use these for cross-catalog joins.
**Warning signs:** "Unknown table" errors when referencing `test.t0` in join queries

## Code Examples

### MockedADBCMetadata Core Structure
```java
// Source: Derived from MockedJDBCMetadata.java pattern
package com.starrocks.connector.adbc;

import com.starrocks.catalog.ADBCTable;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.connector.ConnectorMetadatRequestContext;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.PartitionInfo;
import com.starrocks.qe.ConnectContext;
import com.starrocks.type.IntegerType;
import com.starrocks.type.VarcharType;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static com.starrocks.catalog.Table.TableType.ADBC;

public class MockedADBCMetadata implements ConnectorMetadata {
    private final AtomicLong idGen = new AtomicLong(0L);
    public static final String MOCKED_ADBC_CATALOG_NAME = "adbc0";
    public static final String MOCKED_DB_NAME = "test_db0";
    public static final String MOCKED_TABLE_NAME0 = "tbl0";

    private Map<String, String> properties;
    private Map<String, ADBCTable> tables = new HashMap<>();

    public MockedADBCMetadata(Map<String, String> properties) {
        this.properties = properties;
    }

    @Override
    public Table.TableType getTableType() {
        return ADBC;
    }

    @Override
    public Table getTable(ConnectContext context, String dbName, String tblName) {
        return tables.computeIfAbsent(tblName, k -> {
            List<Column> schema = Arrays.asList(
                new Column("a", VarcharType.VARCHAR),
                new Column("b", VarcharType.VARCHAR),
                new Column("c", IntegerType.INT),
                new Column("d", VarcharType.VARCHAR)
            );
            return new ADBCTable(100000 + idGen.getAndIncrement(),
                tblName, schema, dbName, MOCKED_ADBC_CATALOG_NAME, properties);
        });
    }

    @Override
    public Database getDb(ConnectContext context, String dbName) {
        return new Database(idGen.getAndIncrement(), dbName);
    }

    @Override
    public List<String> listDbNames(ConnectContext context) {
        return Arrays.asList(MOCKED_DB_NAME);
    }

    @Override
    public List<String> listTableNames(ConnectContext context, String dbName) {
        return Arrays.asList(MOCKED_TABLE_NAME0);
    }
}
```

### ADBCScanPlanTest Assertions
```java
// Source: Derived from JDBCIdentifierQuoteTest.java pattern
@Test
public void testADBCSelectWithColumnPruning() throws Exception {
    String sql = "select a, c from adbc0.test_db0.tbl0";
    String plan = getFragmentPlan(sql);
    assertContains(plan, "SCAN ADBC");
    assertContains(plan, "QUERY: SELECT \"a\", \"c\" FROM \"test_db0\".\"tbl0\"");
}

@Test
public void testADBCPredicatePushdown() throws Exception {
    String sql = "select a from adbc0.test_db0.tbl0 where c > 10";
    String plan = getFragmentPlan(sql);
    assertContains(plan, "SCAN ADBC");
    assertContains(plan, "WHERE");
}

@Test
public void testADBCLimitPushdown() throws Exception {
    String sql = "select a from adbc0.test_db0.tbl0 limit 5";
    String plan = getFragmentPlan(sql);
    assertContains(plan, "LIMIT 5");
}
```

### SQL Integration Test T File (DDL)
```sql
-- name: test_adbc_ddl_create_and_drop
set catalog default_catalog;
create external catalog adbc_test_${uuid0}
properties
(
    "type"="adbc",
    "adbc.driver"="flight_sql",
    "adbc.url"="grpc://${external_flightsql_ip}:${external_flightsql_port}"
);
show catalogs;
set catalog adbc_test_${uuid0};
show databases;
set catalog default_catalog;
drop catalog adbc_test_${uuid0};
```

### Seed Data Script for DuckDB
```sql
-- setup_flightsql_data.sql: Run against DuckDB before E2E tests
CREATE SCHEMA IF NOT EXISTS test_db;
CREATE TABLE test_db.test_types (
    id INTEGER PRIMARY KEY,
    name VARCHAR,
    value DOUBLE,
    created_date DATE,
    flag BOOLEAN
);
INSERT INTO test_db.test_types VALUES
    (1, 'alpha', 1.5, '2024-01-01', true),
    (2, 'beta', 2.5, '2024-01-02', false),
    (3, 'gamma', 3.5, '2024-01-03', true);
```

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| Raw Flight SQL client for testing | sqlflite Docker image | 2024 | Easy Docker-based E2E test environment |
| Single monolithic test file | Multiple T/R files per feature area | Project convention | Better test isolation and debugging |

## Open Questions

1. **DuckDB schema semantics vs Flight SQL catalog/schema**
   - What we know: ADBCMetadata uses `null` for catalog parameter, dbName as schema in `getTableSchema(null, dbName, tblName)`
   - What's unclear: How DuckDB Flight SQL exposes its schemas via getObjects -- does it use DuckDB schema names or database file names?
   - Recommendation: Test empirically with sqlflite when writing seed data script; may need to adjust schema references in T files

2. **MV refresh in SQL integration tests**
   - What we know: MV tests require running BE for data refresh. T/R tests run against a cluster.
   - What's unclear: Whether REFRESH MATERIALIZED VIEW works correctly with ADBC tables in a test cluster (never been tested E2E)
   - Recommendation: Include MV tests in T files but accept they may need iteration during local testing

## Validation Architecture

### Test Framework
| Property | Value |
|----------|-------|
| Framework | JUnit 5 (FE plan tests) + StarRocks SQL-tester (T/R integration tests) |
| Config file | `fe/pom.xml` (JUnit), `test/conf/sr.conf` (SQL-tester) |
| Quick run command | `cd fe && mvn test -Dtest=ADBCScanPlanTest -pl fe-core` |
| Full suite command | `cd test && python3 run.py -d sql/test_adbc_catalog -v` |

### Phase Requirements -> Test Map
| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| TEST-01 | MockedADBCMetadata + EXPLAIN shows ADBC_SCAN_NODE | unit (plan test) | `cd fe && mvn test -Dtest=ADBCScanPlanTest -pl fe-core` | No -- Wave 0 |
| TEST-02 | T/R files cover DDL, queries, MVs | integration | `cd test && python3 run.py -d sql/test_adbc_catalog -v` | No -- Wave 0 |
| TEST-03 | sr.conf has [.flightsql] section | config | `grep -q flightsql test/conf/sr.conf` | No -- Wave 0 |
| TEST-04 | Tests pass against Flight SQL server | integration (manual) | Manual: start sqlflite, configure sr.conf, run tests | No -- Wave 0 |

### Sampling Rate
- **Per task commit:** `cd fe && mvn test -Dtest=ADBCScanPlanTest -pl fe-core`
- **Per wave merge:** Full plan test suite + manual E2E verification
- **Phase gate:** All plan tests green; E2E tests verified locally against sqlflite

### Wave 0 Gaps
- [ ] `fe/fe-core/src/test/java/com/starrocks/connector/adbc/MockedADBCMetadata.java` -- covers TEST-01
- [ ] `fe/fe-core/src/test/java/com/starrocks/sql/plan/ADBCScanPlanTest.java` -- covers TEST-01
- [ ] `test/sql/test_adbc_catalog/T/test_adbc_ddl` -- covers TEST-02
- [ ] `test/sql/test_adbc_catalog/T/test_adbc_query` -- covers TEST-02
- [ ] `test/sql/test_adbc_catalog/T/test_adbc_mv` -- covers TEST-02
- [ ] `test/conf/sr.conf` update with `[.flightsql]` section -- covers TEST-03

## Sources

### Primary (HIGH confidence)
- `fe/fe-core/src/test/java/com/starrocks/connector/jdbc/MockedJDBCMetadata.java` -- exact template for MockedADBCMetadata
- `fe/fe-core/src/test/java/com/starrocks/sql/plan/ConnectorPlanTestBase.java` -- registration pattern for mocked catalogs
- `fe/fe-core/src/test/java/com/starrocks/sql/plan/JDBCIdentifierQuoteTest.java` -- plan test assertion pattern
- `fe/fe-core/src/test/java/com/starrocks/sql/optimizer/rule/transformation/materialization/MvRewriteJDBCTest.java` -- MV rewrite test pattern
- `test/sql/test_jdbc_catalog/T/test_binary_type` -- T file format and variable substitution pattern
- `test/conf/sr.conf` -- configuration file structure
- `fe/fe-core/src/main/java/com/starrocks/catalog/ADBCTable.java` -- ADBCTable constructor and properties
- `fe/fe-core/src/main/java/com/starrocks/planner/ADBCScanNode.java` -- EXPLAIN output format ("SCAN ADBC", double-quote identifiers)
- `fe/fe-core/src/main/java/com/starrocks/connector/adbc/ADBCMetadata.java` -- real metadata implementation (reference for mock)

### Secondary (MEDIUM confidence)
- [voltrondata/sqlflite](https://github.com/voltrondata/sqlflite) -- Flight SQL server Docker image for local E2E testing

### Tertiary (LOW confidence)
- DuckDB Flight SQL schema exposure semantics -- needs empirical verification with sqlflite

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH -- all patterns directly observable in codebase
- Architecture: HIGH -- exact templates exist for every artifact (JDBC -> ADBC pattern)
- Pitfalls: HIGH -- identified from codebase analysis of real registration and scan node code

**Research date:** 2026-03-07
**Valid until:** 2026-04-07 (stable patterns, unlikely to change)
