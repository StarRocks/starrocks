# Roadmap: ADBC External Catalog

## Overview

Four phases deliver a fully JDBC-parity ADBC external catalog. Phase 1 wires up the catalog infrastructure end-to-end — build dependencies, DDL, metadata browsing, and the type system. Phase 2 delivers the actual data path — BE native C++ ADBC scanning, Arrow-to-Chunk conversion, pushdowns, and optimizer integration. Phase 3 completes JDBC parity with materialized view support and statistics collection. Phase 4 adds MockedADBCMetadata plan tests and SQL integration test files (E2E requires CI infra team to provision Flight SQL server).

## Phases

**Phase Numbering:**
- Integer phases (1, 2, 3): Planned milestone work
- Decimal phases (2.1, 2.2): Urgent insertions (marked with INSERTED)

Decimal phases appear between their surrounding integers in numeric order.

- [x] **Phase 1: Foundation** - Build dependencies compile, catalog DDL works, metadata browsable, Arrow types map correctly
- [x] **Phase 2: Scanning** - Full data path from FE query to BE Arrow scan to StarRocks Chunk, with column/predicate/limit pushdown and optimizer integration
- [ ] **Phase 3: JDBC Parity** - Materialized views refresh against ADBC tables, column statistics drive optimizer costs, partition discovery and pruning
- [ ] **Phase 4: Integration Testing** - MockedADBCMetadata + ConnectorPlanTestBase plan tests, SQL integration test T/R files for test_adbc_catalog

## Phase Details

### Phase 1: Foundation
**Goal**: Users can create, browse, and drop ADBC catalogs; the build compiles with ADBC dependencies; Arrow types resolve to correct StarRocks types
**Depends on**: Nothing (first phase)
**Requirements**: BUILD-01, BUILD-02, BUILD-03, CAT-01, CAT-02, CAT-03, CAT-04, CAT-05, CAT-06, CAT-07, CAT-08, CAT-09, TYPE-01, TYPE-02, TYPE-03, TYPE-04
**Success Criteria** (what must be TRUE):
  1. User can run `CREATE CATALOG my_adbc TYPE='adbc' PROPERTIES('adbc.driver'='flight_sql', 'uri'='...')` without error
  2. User can run `SHOW DATABASES IN my_adbc` and see remote schemas returned
  3. User can run `DESCRIBE my_adbc.mydb.mytable` and see columns with correct StarRocks types (int, varchar, decimal, date, etc.)
  4. User can run `DROP CATALOG my_adbc` and the catalog is removed
  5. FE and BE build cleanly (`./build.sh --fe --be`) with Arrow ADBC Maven and CMake dependencies included
**Plans**: 5 plans

Plans:
- [x] 01-01-PLAN.md — FE Maven + BE thirdparty ADBC build dependencies (BUILD-01, BUILD-02)
- [x] 01-02-PLAN.md — Connector skeleton: ConnectorType.ADBC, ADBCConnector, ADBCTable, ADBCSchemaResolver base, ADBCTableName (CAT-01, CAT-02, CAT-04, CAT-05, TYPE-02)
- [x] 01-03-PLAN.md — Arrow type mapping: FlightSQLSchemaResolver TDD (TYPE-01, TYPE-02, TYPE-03, TYPE-04)
- [x] 01-04-PLAN.md — Metadata operations: ADBCMetadata + ADBCMetaCache with ADBC Java API (CAT-03, CAT-04, CAT-05, CAT-06, CAT-07, CAT-08, CAT-09)
- [x] 01-05-PLAN.md — SQL analysis guards + full build verification (BUILD-03, CAT-01, CAT-02, CAT-03, CAT-06, CAT-07, CAT-08)

### Phase 2: Scanning
**Goal**: Users can SELECT from ADBC tables with full pushdown; Arrow data flows from BE native C++ ADBC to StarRocks Chunk; optimizer integrates ADBC scan operators
**Depends on**: Phase 1
**Requirements**: SCAN-01, SCAN-02, SCAN-03, SCAN-04, SCAN-05, SCAN-06, SCAN-07, OPT-01, OPT-02, OPT-03, OPT-04
**Success Criteria** (what must be TRUE):
  1. User can `SELECT col1, col2 FROM my_adbc.mydb.mytable WHERE col1 = 'value' LIMIT 100` and receive correct results
  2. `EXPLAIN SELECT ...` shows the pushed-down SQL query string sent to the remote source (predicate + limit visible)
  3. Column pruning is active: only selected columns appear in the remote query, not SELECT *
  4. Connection-per-scan lifecycle: each query opens and closes ADBC connections cleanly
**Plans**: 4 plans

Plans:
- [x] 02-01-PLAN.md — Thrift IDL additions + FE optimizer scaffolding (SCAN-01, OPT-01, OPT-02)
- [x] 02-02-PLAN.md — BE native C++ ADBC connector and scanner (SCAN-02, SCAN-03, SCAN-07)
- [x] 02-03-PLAN.md — ADBCScanNode SQL generation + PlanFragmentBuilder + RelationTransformer wiring (OPT-03, OPT-04, SCAN-04, SCAN-05, SCAN-06)
- [x] 02-04-PLAN.md — BE wiring (exec_factory, connector registration) + full build verification (SCAN-02, SCAN-03, SCAN-04, SCAN-05, SCAN-06, SCAN-07)

### Phase 3: JDBC Parity
**Goal**: ADBC connector has partition traits registered for MV framework, optimizer uses row count statistics, materialized views work over ADBC tables with full refresh
**Depends on**: Phase 2
**Requirements**: MV-01, MV-02, MV-03, MV-04, STAT-01, STAT-02, PART-01, PART-02, PART-03
**Success Criteria** (what must be TRUE):
  1. User can `CREATE MATERIALIZED VIEW mv AS SELECT ... FROM my_adbc.mydb.mytable` and the MV is created without error
  2. `REFRESH MATERIALIZED VIEW mv` successfully fetches data from the ADBC table and populates the MV
  3. PCT refresh is explicitly unsupported (isSupportPCTRefresh=false) — full refresh only
  4. The query optimizer rewrites eligible queries to use the MV instead of scanning the ADBC table
  5. getTableStatistics() returns row count from remote COUNT(*) for optimizer cost estimation
  6. Partition discovery returns empty list (minimal support); no partition pruning logic
**Plans**: 3 plans

Plans:
- [ ] 03-01-PLAN.md — ADBCPartitionTraits + getCatalogTableName + TRAITS_TABLE registration (PART-01, PART-02, PART-03, MV-01, MV-03)
- [ ] 03-02-PLAN.md — getTableStatistics() with remote row count via ADBC (STAT-01, STAT-02)
- [ ] 03-03-PLAN.md — MV support verification tests + full build check (MV-01, MV-02, MV-04)

### Phase 4: Integration Testing
**Goal**: Add MockedADBCMetadata for FE plan tests via ConnectorPlanTestBase, write SQL integration test T/R files for test_adbc_catalog, and add sr.conf variables for Flight SQL endpoints
**Depends on**: Phase 3
**Requirements**: TEST-01, TEST-02, TEST-03, TEST-04
**Success Criteria** (what must be TRUE):
  1. MockedADBCMetadata registered in ConnectorPlanTestBase; ADBC plan tests pass (EXPLAIN shows ADBC_SCAN_NODE with pushdowns)
  2. SQL integration test files exist in `test/sql/test_adbc_catalog/` with T/R pairs covering: CREATE CATALOG, SHOW DATABASES, DESCRIBE, SELECT with pushdowns, DROP CATALOG
  3. `test/conf/sr.conf` has `[.flightsql]` section with `external_flightsql_ip`, `external_flightsql_port` variables
  4. SQL integration tests pass when run against a manually provisioned Flight SQL server (e.g., DuckDB Flight SQL)
**Note**: E2E tests in CI require infra team to add Flight SQL server provisioning to ci-tool. Local verification uses manually started DuckDB Flight SQL.
**Plans**: TBD

## Progress

**Execution Order:**
Phases execute in numeric order: 1 → 2 → 3

| Phase | Plans Complete | Status | Completed |
|-------|----------------|--------|-----------|
| 1. Foundation | 5/5 | Complete | 2026-03-05 |
| 2. Scanning | 4/4 | Complete | 2026-03-06 |
| 3. JDBC Parity | 0/3 | In progress | - |
| 4. Integration Testing | 0/TBD | Not started | - |
