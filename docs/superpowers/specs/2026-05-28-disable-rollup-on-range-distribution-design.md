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
   base sort key. (Note: this range-propagation logic itself stays — it is also
   used by safe schema-change paths where row order does not change. The
   problem is the operation as a whole, not the copy.)
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
- Touching async materialized views — i.e. `CREATE MATERIALIZED VIEW ... REFRESH
  ASYNC|MANUAL ... [DISTRIBUTED BY ...] AS ...`, which are independent tables
  with their own distribution and refresh model.
- Touching BE-side code.
- `ALTER TABLE ... ADD/DROP/MODIFY COLUMN` **when the touched column is not
  a key column, not in `MetaUtils.getRangeDistributionColumns()`, and (for
  MODIFY) the operation does not change keyness** (row #4a). This does not
  change physical row order *and* does not change the range sort-key column
  set or its type/semantics, so it is safe on range-distribution tables.
  The existing `setRange` propagation in shared schema-change builders is
  intentionally preserved for this safe path. (Rows #4b/#4c/#4d/#4e — ADD
  key, DROP sort-key, MODIFY sort-key, MODIFY keyness-change — ARE in
  scope; see Guard #4.)

## Audit Result

| # | DDL operation | Implementation | Range-dist verdict | Action |
|---|---|---|---|---|
| 1a | `ALTER TABLE ... ADD ROLLUP` | `MaterializedViewHandler.processBatchAddRollup` → `createMaterializedViewJob` → `RollupJobV2` / `LakeTableRollupBuilder` | Broken: rollup data sorted by rollup columns; tablet range metadata invalid | Guard (central) |
| 1b | `CREATE MATERIALIZED VIEW ... AS ...` (sync form — no `REFRESH` / no `DISTRIBUTED BY`, parsed as `CreateSyncMVStmt`) | `LocalMetastore.createMaterializedView(CreateSyncMVStmt)` → `MaterializedViewHandler.processCreateMaterializedView` → `createMaterializedViewJob` → same rollup job path | Same as 1a — same job path, same broken assumptions | Guard (central) |
| 2 | `ALTER TABLE ... ORDER BY (new_keys)` | `SchemaChangeHandler.processModifySortKeyColumn` | Broken: physical row order in each tablet changes; range boundaries no longer reflect data | Guard |
| 3 | `ALTER TABLE ... OPTIMIZE` | `AlterTableClauseAnalyzer.visitOptimizeClause` → `SchemaChangeHandler.createOptimizeTableJob` → `OptimizeJobV2` / `OnlineOptimizeJobV2` | Broken: entire partition is redistributed/rebucketed, range semantics lost | Guard (analyzer + handler, defense in depth) |
| 4a | `ALTER TABLE ... ADD/DROP/MODIFY COLUMN` where (a) the column is **NOT** a key column, (b) the column is **NOT** in `MetaUtils.getRangeDistributionColumns()`, AND (c) for MODIFY, the operation does **NOT** change keyness (`oriColumn.isKey() == modColumn.isKey()`) | `SchemaChangeJobV2` / `LakeTableSchemaChangeJob` | Safe: physical row order preserved AND the range sort-key column set / type-semantics are unchanged; stored 1:1-copied range boundary values remain valid | No change |
| 4b | `ALTER TABLE ... ADD COLUMN` of a key column on a range-distribution table | `SchemaChangeHandler.processAddColumn(s)` | Broken: new key columns are appended to range sort key on AGG/UNIQUE, or shift the key-column-derived sort key on tables without explicit `ORDER BY` (see `SchemaChangeHandler.java:1837`, `MetaUtils.java:301`, `OlapTableFactory.java:683`) | Guard |
| 4c | `ALTER TABLE ... DROP COLUMN` where the column is in `MetaUtils.getRangeDistributionColumns()` | `SchemaChangeHandler.processDropColumn` | Broken: existing logic only rejects sort-key drops for PK; DUP/AGG paths can allow it, removing a column whose stored boundary values are still referenced | Guard |
| 4d | `ALTER TABLE ... MODIFY COLUMN` where the column is in `MetaUtils.getRangeDistributionColumns()` | `SchemaChangeHandler.processModifyColumn` | Broken: even with row order unchanged, changing the column's type/semantics changes how the 1:1-copied range boundary values are interpreted; existing PK-sort-column type guard at `SchemaChangeHandler.java:684` is insufficient for DUP/AGG and for non-PK keys-types with non-key sort columns | Guard |
| 4e | `ALTER TABLE ... MODIFY COLUMN` that changes keyness (`oriColumn.isKey() != modColumn.isKey()`), including AGG-inferred-KEY when no aggregation specified | `SchemaChangeHandler.processModifyColumn` | Broken: a non-key→key promotion adds the column to the (key-derived) range sort key on AGG/UNIQUE / no-explicit-ORDER-BY tables; the existing `keyOrderChanged` check only fires when both sides are keys (`SchemaChangeHandler.java:950`), so this transition is uncovered | Guard |

At the time of audit, none of the target paths have any **rejecting** guard
against range distribution. Existing references to `isRangeDistribution()` in
the codebase (e.g. `LakeTableRollupBuilder` copying base range to shadow
tablet) are for range *propagation*, not rejection, and must be left alone —
they remain correct for the safe schema-change paths in row #4a.

## Design

### Guard #1: Sync materialized view / rollup (centralized)

**Location:** `fe/fe-core/src/main/java/com/starrocks/alter/MaterializedViewHandler.java`,
in `createMaterializedViewJob`, immediately on entry, before any tablet ID
allocation or replica setup.

**Why central, not entry-specific:** Two distinct DDL entry points feed this
same job-creation path:

- `processBatchAddRollup` ← `ALTER TABLE ... ADD ROLLUP`
- `processCreateMaterializedView` ← `CREATE MATERIALIZED VIEW ... AS ...` (sync form, `CreateSyncMVStmt`)

Guarding only the first leaves the second corrupting the metadata silently.
A single check in `createMaterializedViewJob` covers both and avoids the
"guard skew" risk where one entry is forgotten in a future refactor.

**Check:**
```java
if (olapTable.isRangeDistribution()) {
    throw new DdlException(
        "Synchronous materialized view / rollup is not supported on " +
        "tables with range distribution. Use an asynchronous materialized " +
        "view instead — declare it with an explicit REFRESH clause " +
        "(REFRESH ASYNC or REFRESH MANUAL) or a DISTRIBUTED BY clause, " +
        "e.g. CREATE MATERIALIZED VIEW ... DISTRIBUTED BY HASH(...) " +
        "REFRESH ASYNC AS SELECT ...");
}
```

The wording is explicit because plain `CREATE MATERIALIZED VIEW ... AS ...`
without `REFRESH` / `DISTRIBUTED BY` is exactly the sync syntax being
rejected; recommending it again would be circular.

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

### Guard #3: OPTIMIZE (analyzer + handler)

**Primary location:** `fe/fe-core/src/main/java/com/starrocks/sql/analyzer/AlterTableClauseAnalyzer.java`,
in `visitOptimizeClause`, immediately after the existing
colocate / materialized-view / automatic-bucket-size checks.

```java
if (olapTable.isRangeDistribution()) {
    ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR,
        "OPTIMIZE is not supported on tables with range distribution, " +
        "because it redistributes data and would violate range tablet " +
        "boundaries.");
}
```

**Secondary (defensive) location:** `fe/fe-core/src/main/java/com/starrocks/alter/SchemaChangeHandler.java`,
in `createOptimizeTableJob`, immediately on entry (before related-MV
invalidation and any other irreversible step).

```java
if (olapTable.isRangeDistribution()) {
    throw new DdlException(
        "OPTIMIZE is not supported on tables with range distribution.");
}
```

**Why two layers:** `createOptimizeTableJob` is also reachable from
non-`visitOptimizeClause` callers (e.g. internal SchemaChangeHandler paths
that synthesize the job). The handler-side check is defense in depth, not a
substitute — the analyzer check still produces the clearest user error for
the SQL path. The two checks have intentionally different exception types
(`SemanticException` for the analyzer's SQL-validation surface,
`DdlException` for the handler) to match each layer's convention.

### Guard #4: ADD / DROP / MODIFY COLUMN that touches a range sort-key column

The 1:1 base→shadow tablet copy preserves the stored *range boundary values*
verbatim. Any schema change that adds to, removes from, or changes the
type/semantics of the column set used as the range sort key invalidates the
interpretation of those stored boundary values — even if no row physically
moves. Three entry points need a guard.

**Shared helper.** Introduce a private helper in `SchemaChangeHandler`:

```java
private static void rejectIfTouchesRangeSortKey(
        OlapTable olapTable, long indexMetaId,
        String operation, String colName) throws DdlException {
    if (!olapTable.isRangeDistribution()) {
        return;
    }
    List<Column> rangeSortCols =
            MetaUtils.getRangeDistributionColumns(olapTable, indexMetaId);
    boolean hit = rangeSortCols.stream()
            .anyMatch(c -> c.getName().equalsIgnoreCase(colName));
    if (hit) {
        throw new DdlException(operation +
            " on a range-distribution sort-key column is not supported, " +
            "because it would change the column set or type/semantics that " +
            "the existing range tablet boundary values were recorded under. " +
            "Column: " + colName);
    }
}
```

`MetaUtils.getRangeDistributionColumns` already falls back to key columns when
there is no explicit `ORDER BY`, so this helper correctly catches AGG/UNIQUE
range tables whose sort key derives from key columns.

#### #4-ADD: `ALTER TABLE ... ADD COLUMN <col> ...`

**Location:** `fe/fe-core/src/main/java/com/starrocks/alter/SchemaChangeHandler.java`,
in `processAddColumn` (and the multi-column `processAddColumns`), at the point
where the column-is-key bit and the target index meta id are both known but
*before* the column is appended to the index metadata.

**What to reject:** Range distribution table + (the new column is a key column
**OR** the table has no explicit `ORDER BY` so range columns derive from key
columns and the new column would shift that derivation).

Conservative implementation: on a range-distribution table, reject any
`ADD COLUMN` whose column is marked as key (`columnDef.isKey()` /
`column.isKey()`). This is broader than strictly necessary (it also rejects
adding key columns on DUP range tables, where there is no AGG/UNIQUE
auto-append behavior), but the inverse is rare and the broader cut is safer.

**Error:** `"ADD COLUMN of a key column on a range-distribution table is not
supported, because new key columns are appended to the range sort key
(AGG/UNIQUE) or derived sort key (no explicit ORDER BY), invalidating stored
range tablet boundary values. Column: <name>"`

#### #4-DROP: `ALTER TABLE ... DROP COLUMN <col>`

**Location:** `processDropColumn` in `SchemaChangeHandler.java` (around lines
344 / 366 / 390 — the existing per-keys-type branches). The existing code
*only* rejects sort-key drops for PK tables; DUP/AGG paths can allow key /
sort-key column drops depending on shape.

**What to reject:** Range distribution table + the dropped column is in
`MetaUtils.getRangeDistributionColumns(olapTable, indexMetaId)`.

Call `rejectIfTouchesRangeSortKey(olapTable, indexMetaId, "DROP COLUMN",
columnName)` at the top of each per-keys-type branch (or once before the
keys-type dispatch, if a single index meta id is available there).

#### #4-MODIFY: `ALTER TABLE ... MODIFY COLUMN <col> ...`

**Location:** `processModifyColumn` in `SchemaChangeHandler.java`. Placement
correction from earlier draft: the index meta id used to look up the column
(`indexMetaIdForFindingColumn`) and the `found` flag are computed around line
755, after the PK-sort-column type-protection block at line ~684. Both
checks below must run **after** `indexMetaIdForFindingColumn` is established
and the column is confirmed found, and **before** `schemaForFinding` is
mutated.

**Two checks:**

1. **Existing sort-key column check (row #4d):**
   ```java
   rejectIfTouchesRangeSortKey(olapTable, indexMetaIdForFindingColumn,
                                "MODIFY COLUMN", modifiedColumnName);
   ```

2. **Keyness-change check (row #4e):** Even if the column is not currently
   in the range sort key, promoting a non-key column to a key column adds it
   to the key-derived range sort key on AGG/UNIQUE tables or tables without
   explicit `ORDER BY`. The reverse (demoting a key to a value column) is
   equally disruptive on those table shapes.

   ```java
   if (olapTable.isRangeDistribution() &&
       oriColumn.isKey() != modColumn.isKey()) {
       throw new DdlException(
           "MODIFY COLUMN that changes keyness on a range-distribution " +
           "table is not supported, because adding to or removing from the " +
           "key column set shifts the range sort key on AGG/UNIQUE tables " +
           "and on tables without explicit ORDER BY. Column: " +
           modifiedColumnName);
   }
   ```

   This check runs unconditionally on range-distribution tables, even on
   columns not currently in `getRangeDistributionColumns()`, because the
   keyness flip is precisely what brings (or removes) the column from the
   derived sort key.

Both checks share the same overall location; they are split into two
conditional blocks for clarity in the resulting code.

**Conservative scope:** All three sub-guards reject the entire class of
operations rather than enumerating provably-safe sub-cases (e.g. widening
VARCHAR). Narrowing to a safe allow-list is a follow-up; for the pre-release
window, the conservative block is the right call.

### What is intentionally not changed

- **No `LakeTableRollupBuilder` / `OlapTableRollupJobBuilder` change.** Job
  creation now bails out earlier, so the builders are never reached.
- **No `RollupJobV2` change.** Same reason.
- **No removal of existing `isRangeDistribution()` checks** that copy
  base-tablet range metadata into shadow tablets. Those still apply to row
  #4a (safe schema change that does not touch a range sort-key or key
  column).
- **No BE-side change.** BE does not need to know about this restriction.
- **No change to async MV (`CREATE MATERIALIZED VIEW ... REFRESH ... `).**
  Different code path, different model, no 1-to-1 assumption.

## Tests

FE unit tests, one per guarded entry — **eight** total. Each negative test
creates a range-distribution table, attempts the disallowed DDL, and asserts
the corresponding exception with a message containing "range distribution"
(or, for the Guard #4 family, including the offending column name).

- `MaterializedViewHandlerTest` (or equivalent) — `ALTER TABLE ... ADD ROLLUP`
- `MaterializedViewHandlerTest` (or equivalent) — `CREATE MATERIALIZED VIEW ... AS ...`
  in sync form (no `REFRESH`, no `DISTRIBUTED BY`); confirms the central
  `createMaterializedViewJob` guard fires from the `CreateSyncMVStmt` path
- `SchemaChangeHandlerTest` (or equivalent) — `ALTER TABLE ... ORDER BY (...)`
  with a **column list shorter than the base schema**, so it exercises
  `processModifySortKeyColumn` rather than the schema-reorder overload
- `AlterTableClauseAnalyzerTest` (or equivalent) — `ALTER TABLE ... OPTIMIZE`
  (covers the analyzer guard; the handler guard is best-effort defense and
  may be left to a single integration smoke test if no direct unit harness
  exists)
- `SchemaChangeHandlerTest` — Guard #4-ADD: `ALTER TABLE ... ADD COLUMN <k> KEY`
  on an **AGG/UNIQUE range-distribution table** (or a DUP range table with
  no explicit `ORDER BY`); asserts `DdlException`
- `SchemaChangeHandlerTest` — Guard #4-DROP: `ALTER TABLE ... DROP COLUMN`
  on a **range sort-key column** of a DUP/AGG range table (one with the
  drop allowed by the existing per-keys-type logic); asserts `DdlException`
- `SchemaChangeHandlerTest` — Guard #4-MODIFY (row #4d): `ALTER TABLE ... MODIFY COLUMN`
  on a **DUP range-distribution table with a non-key sort column**,
  attempting to change that sort column's type (e.g. INT→BIGINT or
  VARCHAR(10)→VARCHAR(20)); asserts `DdlException` and that the message
  names the offending column
- `SchemaChangeHandlerTest` — Guard #4-MODIFY-keyness (row #4e): `ALTER TABLE
  ... MODIFY COLUMN <value_col> ... KEY` on a range-distribution table where
  the column was previously a value column (not in
  `MetaUtils.getRangeDistributionColumns()`); asserts `DdlException`. Use
  AGG or UNIQUE keys-type so the keyness flip is meaningful

**One positive test** in `SchemaChangeHandlerTest`: on the same DUP
range-distribution table, run a `MODIFY COLUMN` on a **non-sort-key /
non-key column** (with no keyness change) and assert it still succeeds.
This proves the Guard #4 family is narrow (row #4a behaviour unchanged)
rather than blanket-rejecting all schema changes. One positive test is
enough — repeating it for ADD and DROP doesn't add coverage.

Regression sweep: grep existing FE tests for combinations of range-distribution
table creation + any of the eight blocked DDLs. If a happy-path test exists,
convert it to a negative test (assert exception) or delete it.

## Documentation

Update the range-distribution doc with a "Limitations" section in **all three
language trees** (per `docs/CLAUDE.md`, Japanese updates are required when a
corresponding `docs/ja/` page exists):

- `docs/en/table_design/data_distribution/Data_distribution.md`
- `docs/zh/table_design/data_distribution/Data_distribution.md`
- `docs/ja/table_design/data_distribution/Data_distribution.md`

Content: enumerate the blocked DDLs — `ADD ROLLUP`, sync `CREATE MATERIALIZED
VIEW`, `ORDER BY` sort-key change, `OPTIMIZE`, and the Guard #4 family
(`ADD COLUMN` of a key column, `DROP COLUMN` of a sort-key column, `MODIFY
COLUMN` of a sort-key column, and `MODIFY COLUMN` that changes keyness) —
and direct rollup-like use cases to async MV with an explicit `REFRESH` or
`DISTRIBUTED BY` clause.

(Optional) Cross-reference from the ROLLUP / `CREATE MATERIALIZED VIEW` /
ORDER BY / OPTIMIZE DDL reference pages back to the range-distribution
limitation note.

## Risks

1. **Unknown happy-path tests** may be relying on these combinations. The
   regression sweep above must run before the patch lands.
2. **A user currently relying on `ADD ROLLUP` for a range-distribution
   workload** — unlikely given the feature has not shipped, but possible if
   internal users have early access. Mitigation: the error message gives
   explicit async MV syntax.
3. **Range distribution may be unintentionally creatable outside shared-data
   mode.** The audit found `CreateTableAnalyzer.java:697-712` gates range
   distribution on `Config.enable_range_distribution` only, not on
   `isCloudNativeTable()`. This is out of scope for this design but worth
   flagging as a separate follow-up: the design intent appears to be
   shared-data only.
4. **Silent metadata/data corruption from any unblocked entry point** is the
   primary failure mode this design protects against. The central
   `createMaterializedViewJob` guard plus the handler-side OPTIMIZE check are
   chosen specifically to close obvious bypasses; future code that adds a new
   DDL surface (e.g. another way to spawn a rollup job) must be reviewed for
   this hazard.

## Follow-up (out of scope)

- Decide whether range distribution should be hard-restricted to shared-data
  mode at create-table time.
- Investigate a future protocol for rollup-on-range that does not depend on
  1-to-1 tablet pairing (e.g., rollup-tablet boundaries derived from rollup's
  own key, with corresponding reshard semantics).
