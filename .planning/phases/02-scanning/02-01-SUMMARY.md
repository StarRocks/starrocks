---
phase: 02-scanning
plan: 01
subsystem: optimizer
tags: [thrift, scan-operator, optimizer-framework, adbc, arrow-flight-sql]

requires:
  - phase: 01-foundation
    provides: ADBCTable catalog class, ADBCMetadata, type mapping
provides:
  - TADBCScanNode Thrift struct for FE-BE wire format
  - TADBCTable Thrift struct for table descriptor
  - LogicalADBCScanOperator and PhysicalADBCScanOperator
  - ADBCScanImplementationRule registered in optimizer RuleSet
  - All visitor/transformation rule registrations for ADBC scan
affects: [02-scanning plan 02 (ADBCScanNode), 02-scanning plan 03 (PlanFragmentBuilder)]

tech-stack:
  added: []
  patterns: [mirror-jdbc-scan-pattern for ADBC operators]

key-files:
  created:
    - fe/fe-core/src/main/java/com/starrocks/sql/optimizer/operator/logical/LogicalADBCScanOperator.java
    - fe/fe-core/src/main/java/com/starrocks/sql/optimizer/operator/physical/PhysicalADBCScanOperator.java
    - fe/fe-core/src/main/java/com/starrocks/sql/optimizer/rule/implementation/ADBCScanImplementationRule.java
    - fe/fe-core/src/test/java/com/starrocks/sql/optimizer/operator/ADBCScanOperatorTest.java
    - fe/fe-core/src/test/java/com/starrocks/sql/optimizer/rule/ADBCScanImplementationRuleTest.java
  modified:
    - gensrc/thrift/PlanNodes.thrift
    - gensrc/thrift/Types.thrift
    - gensrc/thrift/Descriptors.thrift
    - fe/fe-core/src/main/java/com/starrocks/sql/optimizer/operator/OperatorType.java
    - fe/fe-core/src/main/java/com/starrocks/sql/optimizer/rule/RuleType.java
    - fe/fe-core/src/main/java/com/starrocks/sql/optimizer/operator/OperatorVisitor.java
    - fe/fe-core/src/main/java/com/starrocks/sql/optimizer/OptExpressionVisitor.java
    - fe/fe-core/src/main/java/com/starrocks/sql/optimizer/statistics/StatisticsCalculator.java
    - fe/fe-core/src/main/java/com/starrocks/sql/optimizer/LogicalPlanPrinter.java
    - fe/fe-core/src/main/java/com/starrocks/sql/optimizer/OutputPropertyDeriver.java
    - fe/fe-core/src/main/java/com/starrocks/sql/optimizer/rule/RuleSet.java
    - fe/fe-core/src/main/java/com/starrocks/sql/optimizer/operator/pattern/MultiOpPattern.java

key-decisions:
  - "TADBCScanNode field number 85 in TPlanNode (after reserved stream range 70-80)"
  - "TADBCTable field number 37 in TTableDescriptor (next after paimonTable at 36)"
  - "TADBCScanNode has 10 fields including adbc_driver, adbc_uri, adbc_username, adbc_password, adbc_token for connection"

patterns-established:
  - "ADBC scan operators mirror JDBC scan pattern exactly for all optimizer registrations"

requirements-completed: [SCAN-01, OPT-01, OPT-02]

duration: 8min
completed: 2026-03-06
---

# Phase 2 Plan 1: Thrift IDL + FE Optimizer Scaffolding Summary

**TADBCScanNode Thrift wire format and full FE optimizer framework registrations for ADBC scan operators mirroring JDBC pattern**

## Performance

- **Duration:** 8 min
- **Started:** 2026-03-05T22:11:36Z
- **Completed:** 2026-03-05T22:19:36Z
- **Tasks:** 3
- **Files modified:** 17

## Accomplishments
- TADBCScanNode (10 fields) and TADBCTable (6 fields) Thrift structs defined with ADBC_SCAN_NODE and ADBC_TABLE enum entries
- LogicalADBCScanOperator and PhysicalADBCScanOperator created with full visitor dispatch chain
- ADBCScanImplementationRule registered in RuleSet; all transformation rules (predicate pushdown, column prune, limit merge) include ADBC operator type
- FE compiles cleanly with all ADBC scan registrations

## Task Commits

Each task was committed atomically:

1. **Task 1: Add Thrift IDL definitions for ADBC scan** - `1432cef6b7` (feat)
2. **Task 2: Create FE operator classes and add enum entries** - `1fbc1a3de4` (feat)
3. **Task 3: Register ADBC scan in visitor, rule, and pattern files** - `ffef7d592b` (feat)

## Files Created/Modified
- `gensrc/thrift/PlanNodes.thrift` - ADBC_SCAN_NODE enum, TADBCScanNode struct, adbc_scan_node in TPlanNode
- `gensrc/thrift/Types.thrift` - ADBC_TABLE enum entry
- `gensrc/thrift/Descriptors.thrift` - TADBCTable struct, adbcTable in TTableDescriptor
- `fe/.../operator/logical/LogicalADBCScanOperator.java` - Logical ADBC scan operator (mirrors JDBC)
- `fe/.../operator/physical/PhysicalADBCScanOperator.java` - Physical ADBC scan operator (mirrors JDBC)
- `fe/.../rule/implementation/ADBCScanImplementationRule.java` - Logical-to-physical transform rule
- `fe/.../operator/OperatorType.java` - LOGICAL_ADBC_SCAN, PHYSICAL_ADBC_SCAN enum entries
- `fe/.../rule/RuleType.java` - IMP_ADBC_LSCAN_TO_PSCAN enum entry
- `fe/.../operator/OperatorVisitor.java` - visitLogicalADBCScan, visitPhysicalADBCScan methods
- `fe/.../OptExpressionVisitor.java` - visitPhysicalADBCScan method
- `fe/.../statistics/StatisticsCalculator.java` - ADBC scan statistics delegation
- `fe/.../LogicalPlanPrinter.java` - "ADBC SCAN" plan output
- `fe/.../OutputPropertyDeriver.java` - UNPARTITIONED output property for ADBC scan
- `fe/.../rule/RuleSet.java` - ADBCScanImplementationRule registration
- `fe/.../operator/pattern/MultiOpPattern.java` - LOGICAL_ADBC_SCAN in scan patterns
- `fe/.../rule/transformation/PushDownPredicateToExternalTableScanRule.java` - ADBC in predicate pushdown
- `fe/.../rule/transformation/PruneScanColumnRule.java` - ADBC in column pruning
- `fe/.../rule/transformation/MergeLimitDirectRule.java` - ADBC in limit merge
- `fe/.../test/.../ADBCScanOperatorTest.java` - Operator type, accept, and table rejection tests
- `fe/.../test/.../ADBCScanImplementationRuleTest.java` - Rule type, pattern, and transform tests

## Decisions Made
- TADBCScanNode uses field number 85 in TPlanNode (placed after reserved stream operator range 70-80, before no next used range)
- TADBCTable uses field number 37 in TTableDescriptor (next available after paimonTable at 36)
- TADBCScanNode includes connection fields (adbc_driver, adbc_uri, adbc_username, adbc_password, adbc_token) beyond base JDBC pattern

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered
None

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- Thrift IDL and optimizer scaffolding complete; ready for ADBCScanNode planner (Plan 02) and PlanFragmentBuilder integration (Plan 03)
- All visitor dispatch methods in place for plan fragment building

---
*Phase: 02-scanning*
*Completed: 2026-03-06*
