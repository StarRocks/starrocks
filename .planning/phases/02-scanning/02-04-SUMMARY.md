---
phase: 02-scanning
plan: 04
subsystem: connector
tags: [adbc, connector-registration, exec-factory, scan-node, be-wiring]

# Dependency graph
requires:
  - phase: 02-scanning/02-02
    provides: "ADBCConnector, ADBCDataSourceProvider, ADBCDataSource, ADBCScanner classes"
  - phase: 02-scanning/02-03
    provides: "ADBCScanNode, LogicalADBCScanOperator, PhysicalADBCScanOperator, RelationTransformer wiring"
provides:
  - "ConnectorType::ADBC enum and Connector::ADBC string constant in BE"
  - "ADBCConnector registered in ConnectorManager for 'adbc' connector name resolution"
  - "ADBC_SCAN_NODE case in exec_factory creating ConnectorScanNode with adbc connector"
  - "ADBC_SCAN_NODE in exec_node collect_scan_nodes for plan traversal"
  - "Complete FE->BE data path: SQL -> optimizer -> ADBCScanNode -> Thrift -> exec_factory -> ConnectorScanNode -> ADBCConnector"
affects: [03-integration]

# Tech tracking
tech-stack:
  added: []
  patterns: [adbc-scan-node-mirrors-jdbc-in-exec-factory, connector-manager-registration]

key-files:
  created: []
  modified:
    - be/src/connector/connector.h
    - be/src/connector/connector.cpp
    - be/src/exec/exec_factory.cpp
    - be/src/exec/exec_node.cpp
    - fe/fe-core/src/test/java/com/starrocks/planner/ADBCScanNodeTest.java
    - fe/fe-core/src/test/java/com/starrocks/sql/optimizer/rule/ADBCScanImplementationRuleTest.java

key-decisions:
  - "ADBC ConnectorType enum value = 9, after BENCHMARK = 8"
  - "ADBCConnector registered after Iceberg in ConnectorManagerInit (outside __APPLE__ guard)"

patterns-established:
  - "ADBC_SCAN_NODE handling mirrors JDBC_SCAN_NODE exactly in exec_factory and exec_node"

requirements-completed: [SCAN-02, SCAN-03, SCAN-04, SCAN-05, SCAN-06, SCAN-07]

# Metrics
duration: 15min
completed: 2026-03-06
---

# Phase 2 Plan 04: BE Wiring Summary

**BE connector registration (ConnectorType::ADBC, ConnectorManager) and exec_factory/exec_node ADBC_SCAN_NODE routing completing the full FE-to-BE data path**

## Performance

- **Duration:** 15 min
- **Started:** 2026-03-05T22:36:16Z
- **Completed:** 2026-03-05T22:50:54Z
- **Tasks:** 2
- **Files modified:** 6

## Accomplishments
- ConnectorType::ADBC enum and Connector::ADBC = "adbc" string constant added to connector.h/cpp
- ADBCConnector registered in ConnectorManager initialization alongside other connectors
- ADBC_SCAN_NODE case in exec_factory.cpp creates ConnectorScanNode with "adbc" connector name
- ADBC_SCAN_NODE added to collect_scan_nodes in exec_node.cpp for plan traversal
- FE compiles cleanly with all Phase 2 changes
- All Phase 2 ADBC tests pass (test compilation errors fixed)
- All 10 production files and 4 test files verified present
- Complete data path verified: SQL -> RelationTransformer -> LogicalADBCScanOperator -> ADBCScanImplementationRule -> PhysicalADBCScanOperator -> PlanFragmentBuilder -> ADBCScanNode -> TADBCScanNode (Thrift) -> exec_factory -> ConnectorScanNode -> ADBCConnector -> ADBCDataSource -> ADBCScanner

## Task Commits

Each task was committed atomically:

1. **Task 1: Register ADBC connector type and add exec_factory/exec_node cases** - `746963c11b` (feat)
2. **Task 2: Full Phase 2 build verification and test fixes** - `ba7890ca0c` (fix)

## Files Created/Modified
- `be/src/connector/connector.h` - Added ADBC to ConnectorType enum and Connector string constants
- `be/src/connector/connector.cpp` - Added Connector::ADBC = "adbc", registered ADBCConnector in ConnectorManager
- `be/src/exec/exec_factory.cpp` - Added ADBC_SCAN_NODE case creating ConnectorScanNode
- `be/src/exec/exec_node.cpp` - Added ADBC_SCAN_NODE to collect_scan_nodes
- `fe/fe-core/src/test/java/com/starrocks/planner/ADBCScanNodeTest.java` - Fixed Type import, TTupleId, ArrayList type
- `fe/fe-core/src/test/java/com/starrocks/sql/optimizer/rule/ADBCScanImplementationRuleTest.java` - Fixed Pattern.getOpType() to Pattern.is()

## Decisions Made
- ADBC ConnectorType enum value = 9, placed after BENCHMARK = 8 to maintain ordinal sequence
- ADBCConnector registered outside the __APPLE__ guard (ADBC has no macOS restrictions)

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Fixed com.starrocks.catalog.Type import in ADBCScanNodeTest**
- **Found during:** Task 2 (build verification)
- **Issue:** Test imported com.starrocks.catalog.Type which doesn't exist; correct class is com.starrocks.type.IntegerType
- **Fix:** Changed import to com.starrocks.type.IntegerType and usage to IntegerType.INT
- **Files modified:** ADBCScanNodeTest.java
- **Verification:** Test compiles and passes
- **Committed in:** ba7890ca0c (Task 2 commit)

**2. [Rule 1 - Bug] Fixed TTupleId reference in ADBCScanNodeTest**
- **Found during:** Task 2 (build verification)
- **Issue:** Test used com.starrocks.thrift.TTupleId which doesn't exist; correct class is com.starrocks.planner.TupleId
- **Fix:** Changed to new TupleId(0) (already in same package)
- **Files modified:** ADBCScanNodeTest.java
- **Verification:** Test compiles and passes
- **Committed in:** ba7890ca0c (Task 2 commit)

**3. [Rule 1 - Bug] Fixed ArrayList type mismatch in ADBCScanNodeTest**
- **Found during:** Task 2 (build verification)
- **Issue:** TupleDescriptor.getSlots() returns ArrayList<SlotDescriptor>, but mock used List<SlotDescriptor>
- **Fix:** Changed variable type to ArrayList<SlotDescriptor>
- **Files modified:** ADBCScanNodeTest.java
- **Verification:** Test compiles and passes
- **Committed in:** ba7890ca0c (Task 2 commit)

**4. [Rule 1 - Bug] Fixed Pattern.getOpType() in ADBCScanImplementationRuleTest**
- **Found during:** Task 2 (build verification)
- **Issue:** Pattern class has no getOpType() method; uses is() for type checking
- **Fix:** Changed to assertTrue(rule.getPattern().is(OperatorType.LOGICAL_ADBC_SCAN))
- **Files modified:** ADBCScanImplementationRuleTest.java
- **Verification:** Test compiles and passes
- **Committed in:** ba7890ca0c (Task 2 commit)

---

**Total deviations:** 4 auto-fixed (4 bugs in test files from prior plans)
**Impact on plan:** All fixes in test code only. No production code scope creep.

## Issues Encountered
None

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- Phase 2 complete: all FE and BE scanning infrastructure is in place
- Complete data path from SQL query to BE scanner execution is structurally wired
- Ready for Phase 3 (integration testing with live Flight SQL server)
- BE full compilation requires thirdparty ADBC C++ libraries (structural verification only in this phase)

---
## Self-Check: PASSED

All 6 modified files verified present. Both task commits (746963c11b, ba7890ca0c) verified in git log.

---
*Phase: 02-scanning*
*Completed: 2026-03-06*
