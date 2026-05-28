# Disable Sync Rollup (and related Alter Paths) on Range-Distribution Tables

**Date:** 2026-05-28
**Status:** Approved design, pending implementation
**Author:** xiangguangyxg

## Problem

StarRocks shared-data mode introduces **range distribution** tables, where tablets
are split by ranges of the sort key, and tablets can be SPLIT or MERGED at
runtime by `SplitTabletJob` / `MergeTabletJobFactory`.

Several FE alter-job code paths assume a **1-to-1 tablet pairing** between the
base index and a newly created index, with row-by-row in-place rewrites that
do not re-distribute by any key. This pattern is built into the legacy
"rollup / schema change" job framework via:

```java
mvJob.addTabletIdMap(physicalPartitionId, newTabletId, baseTabletId);
```

For **hash distribution**, this assumption holds: distribution columns determine
both the base and rollup tablet placement, so row coverage transfers 1-to-1.

For **range distribution**, the assumption breaks in several ways:

1. **Range metadata becomes meaningless.** `LakeTableRollupBuilder` copies
   `shadowTablet.setRange(originTablet.getRange())` from base to rollup, but the
   rollup data is physically sorted by the rollup's own column order, not the
   base sort key.
2. **No sort key on rollup tablets.** `RollupJobV2` explicitly calls
   `setSortKeyIndexes(null)` on the new tablet schema, leaving rollup tablets
   with no key by which a future reshard could decide split points.
3. **Post-rollup reshard is ill-defined.** When the base tablet splits along
   its sort-key range, the paired rollup tablet must also split, but its
   physical ordering is keyed differently.

This is a pre-release compatibility concern: range distribution + these alter
paths have not been validated together, and the structural mismatch above
indicates they cannot work correctly without significant new design.

## Goal

Before range distribution ships, **block** every FE alter path whose 1-to-1
tablet-pairing assumption is structurally incompatible with range distribution.
Leave full support as a follow-up.

## Non-Goals

- Designing a correct rollup-on-range protocol. That requires either rewriting
  rollup tablets keyed by the rollup's own sort key (with corresponding
  reshard support) or re-architecting how rollup tablets pair with base
  tablets after split/merge. Out of scope.
- Supporting `ALTER TABLE ... ORDER BY` / `ALTER TABLE ... OPTIMIZE` on
  range-distribution tables. These also break range semantics and are blocked
  for the same reason.
- Touching async materialized views (`CREATE MATERIALIZED VIEW`), which are
  independent tables with their own distribution.
- Touching BE-side code.
- Modifying `ALTER TABLE ... ADD/DROP/MODIFY COLUMN` (without sort-key change),
  which does not change physical row order and is safe on range-distribution
  tables.

## Audit Result

| # | DDL operation | Implementation | Range-dist verdict | Action |
|---|---|---|---|---|
| 1 | `ALTER TABLE ... ADD ROLLUP` | `MaterializedViewHandler.processBatchAddRollup` → `RollupJobV2` / `LakeTableRollupBuilder` | Broken: rollup data sorted by rollup columns; tablet range metadata invalid | Guard |
| 2 | `ALTER TABLE ... ORDER BY (new_keys)` | `SchemaChangeHandler.processModifySortKeyColumn` | Broken: physical row order in each tablet changes; range boundaries no longer reflect data | Guard |
| 3 | `ALTER TABLE ... OPTIMIZE` | `AlterTableClauseAnalyzer.visitOptimizeClause` → `OptimizeJobV2` / `OnlineOptimizeJobV2` | Broken: entire partition is redistributed/rebucketed, range semantics lost | Guard |
| 4 | `ALTER TABLE ... ADD/DROP/MODIFY COLUMN` (no sort-key change) | `SchemaChangeJobV2` / `LakeTableSchemaChangeJob` | Safe: physical row order preserved; tablet ranges remain valid | No change |

All three "Guard" paths are currently **unguarded** as of the audit. There is
no existing `isRangeDistribution()` check on any of them.

## Design

### Guard #1: Sync rollup

**Location:** `fe/fe-core/src/main/java/com/starrocks/alter/MaterializedViewHandler.java`,
in `processBatchAddRollup`, immediately after the `OlapTable` is obtained and
before `createMaterializedViewJob` is called.

**Check:**
```java
if (olapTable.isRangeDistribution()) {
    throw new DdlException(
        "Synchronous materialized view (ADD ROLLUP) is not supported on " +
        "tables with range distribution. Use asynchronous materialized view " +
        "(CREATE MATERIALIZED VIEW ...) instead.");
}
```

**Rationale for entry-point placement:** Fail-fast before any metadata-prep
work runs. A second defensive check in `LakeTableRollupBuilder` is intentionally
omitted: duplicating the guard suggests the entry point is unreliable, which
would confuse future readers.

### Guard #2: Sort-key modification

**Location:** `fe/fe-core/src/main/java/com/starrocks/alter/SchemaChangeHandler.java`,
in `processModifySortKeyColumn`, before the existing keysType-based validation
loop.

**Check:**
```java
if (olapTable.isRangeDistribution()) {
    throw new DdlException(
        "Modifying sort key (ALTER TABLE ... ORDER BY) is not supported on " +
        "tables with range distribution, because the sort key defines tablet " +
        "boundaries.");
}
```

### Guard #3: OPTIMIZE

**Location:** `fe/fe-core/src/main/java/com/starrocks/sql/analyzer/AlterTableClauseAnalyzer.java`,
in `visitOptimizeClause`, immediately after the existing
colocate / materialized-view / automatic-bucket-size checks.

**Check:**
```java
if (olapTable.isRangeDistribution()) {
    ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR,
        "OPTIMIZE is not supported on tables with range distribution, " +
        "because it redistributes data and would violate range tablet " +
        "boundaries.");
}
```

Placement at the analyzer (rather than the handler) follows the existing
pattern in `visitOptimizeClause` and produces an earlier, clearer error.

### What is intentionally not changed

- **No `LakeTableRollupBuilder` / `OlapTableRollupJobBuilder` change.** Entry
  point check is sufficient.
- **No `RollupJobV2` change.** Same reason.
- **No BE-side change.** BE does not need to know about this restriction.
- **No `SchemaChangeJobV2` change for plain column add/drop/modify.** Safe on
  range distribution.
- **No change to async MV (`CREATE MATERIALIZED VIEW`) handling.** Different
  code path, different model, no 1-to-1 assumption.

## Tests

One FE unit test per guard. Each test creates a range-distribution table,
attempts the disallowed DDL, and asserts the corresponding exception with a
message containing "range distribution".

- `MaterializedViewHandlerTest` (or equivalent) — `ADD ROLLUP` test
- `SchemaChangeHandlerTest` (or equivalent) — `ORDER BY` (sort-key modify) test
- `AlterTableClauseAnalyzerTest` (or equivalent) — `OPTIMIZE` test

Regression sweep: grep existing FE tests for combinations of range-distribution
table creation + any of the three blocked DDLs. If a happy-path test exists,
convert it to a negative test (assert exception) or delete it.

## Documentation

- `docs/en/table_design/data_distribution/` — add a "Limitations" section to
  the range-distribution doc enumerating these three unsupported operations
  and pointing to async MV as the alternative for rollup-like use cases.
- `docs/zh/table_design/data_distribution/` — mirror the English doc.
- (Optional) Cross-reference from the ROLLUP / ORDER BY / OPTIMIZE DDL
  reference pages back to the range-distribution limitation note.

## Risks

1. **Unknown happy-path tests** may be relying on these combinations. The
   regression sweep above must run before the patch lands.
2. **A user currently relying on `ADD ROLLUP` for a range-distribution
   workload** — unlikely given the feature has not shipped, but possible if
   internal users have early access. Mitigation: the error message directs
   them to async MV.
3. **Range distribution may be unintentionally creatable outside shared-data
   mode.** The audit found `CreateTableAnalyzer.java:697-712` gates range
   distribution on `Config.enable_range_distribution` only, not on
   `isCloudNativeTable()`. This is out of scope for this design but worth
   flagging as a separate follow-up: the design intent appears to be
   shared-data only.

## Follow-up (out of scope)

- Decide whether range distribution should be hard-restricted to shared-data
  mode at create-table time.
- Investigate a future protocol for rollup-on-range that does not depend on
  1-to-1 tablet pairing (e.g., rollup-tablet boundaries derived from rollup's
  own key, with corresponding reshard semantics).
