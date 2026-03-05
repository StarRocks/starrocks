# Roadmap: ADBC External Catalog

## Overview

Three phases deliver a fully JDBC-parity ADBC external catalog. Phase 1 wires up the catalog infrastructure end-to-end — build dependencies, DDL, metadata browsing, and the type system. Phase 2 delivers the actual data path — BE native C++ ADBC scanning, Arrow-to-Chunk conversion, pushdowns, and optimizer integration. Phase 3 completes JDBC parity with materialized view support and statistics collection.

## Phases

**Phase Numbering:**
- Integer phases (1, 2, 3): Planned milestone work
- Decimal phases (2.1, 2.2): Urgent insertions (marked with INSERTED)

Decimal phases appear between their surrounding integers in numeric order.

- [x] **Phase 1: Foundation** - Build dependencies compile, catalog DDL works, metadata browsable, Arrow types map correctly
- [ ] **Phase 2: Scanning** - Full data path from FE query to BE Arrow scan to StarRocks Chunk, with column/predicate/limit pushdown and optimizer integration
- [ ] **Phase 3: JDBC Parity** - Materialized views refresh against ADBC tables, column statistics drive optimizer costs, partition discovery and pruning

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
- [ ] 02-04-PLAN.md — BE wiring (exec_factory, connector registration) + full build verification (SCAN-02, SCAN-03, SCAN-04, SCAN-05, SCAN-06, SCAN-07)

### Phase 3: JDBC Parity
**Goal**: Users can create materialized views over ADBC tables, refresh them incrementally, and the optimizer uses column statistics for cost-based decisions; partition discovery and pruning
**Depends on**: Phase 2
**Requirements**: MV-01, MV-02, MV-03, MV-04, STAT-01, STAT-02, PART-01, PART-02, PART-03
**Success Criteria** (what must be TRUE):
  1. User can `CREATE MATERIALIZED VIEW mv AS SELECT ... FROM my_adbc.mydb.mytable` and the MV is created without error
  2. `REFRESH MATERIALIZED VIEW mv` successfully fetches data from the ADBC table and populates the MV
  3. Incremental MV refresh (PCT) detects partition changes and refreshes only affected partitions
  4. The query optimizer rewrites eligible queries to use the MV instead of scanning the ADBC table
  5. `ANALYZE TABLE my_adbc.mydb.mytable` collects column statistics (min, max, ndv, null count) when the remote source provides them
  6. Partition pruning filters irrelevant partitions during query planning (visible in EXPLAIN)
**Plans**: TBD

## Progress

**Execution Order:**
Phases execute in numeric order: 1 → 2 → 3

| Phase | Plans Complete | Status | Completed |
|-------|----------------|--------|-----------|
| 1. Foundation | 5/5 | Complete | 2026-03-05 |
| 2. Scanning | 3/4 | In progress | - |
| 3. JDBC Parity | 0/TBD | Not started | - |
