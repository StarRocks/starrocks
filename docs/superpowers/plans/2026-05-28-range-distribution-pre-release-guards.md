# Range-Distribution Pre-Release Guards Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Block four families of FE alter-table operations that silently corrupt range-distribution tables: sync rollup / sync CREATE MATERIALIZED VIEW, `ALTER TABLE ... ORDER BY`, `ALTER TABLE ... OPTIMIZE`, and the schema-change cases that change the range sort-key column set (ADD key, DROP sort-key, MODIFY sort-key, MODIFY keyness flip).

**Architecture:** FE-only. Add fail-fast `DdlException` / `SemanticException` guards at the entry-point methods of each alter-job code path; introduce a single shared helper in `SchemaChangeHandler` to avoid duplicating the `MetaUtils.getRangeDistributionColumns(...)` lookup. Each guard is independently tested with one negative FE unit test (plus one positive narrowness test for the Guard #4 family). Docs updated in `docs/en/`, `docs/zh/`, `docs/ja/`.

**Tech Stack:** Java (FE), JUnit, `starRocksAssert` test harness, Markdown docs.

**Reference spec:** [`docs/superpowers/specs/2026-05-28-disable-rollup-on-range-distribution-design.md`](../specs/2026-05-28-disable-rollup-on-range-distribution-design.md)

---

## File Map

| File | Change |
|---|---|
| `fe/fe-core/src/main/java/com/starrocks/alter/MaterializedViewHandler.java` | Modify: add range-dist guard in `createMaterializedViewJob` |
| `fe/fe-core/src/main/java/com/starrocks/alter/SchemaChangeHandler.java` | Modify: add range-dist guards in `processModifySortKeyColumn`, `createOptimizeTableJob`, `processAddColumn`, `processAddColumns`, `processDropColumn`, `processModifyColumn`; add private helper `rejectIfTouchesRangeSortKey` |
| `fe/fe-core/src/main/java/com/starrocks/sql/analyzer/AlterTableClauseAnalyzer.java` | Modify: add range-dist guard in `visitOptimizeClause` |
| `fe/fe-core/src/test/java/com/starrocks/alter/RangeDistributionGuardTest.java` | Create: new test class containing all 8 negative tests + 1 positive narrowness test |
| `docs/en/table_design/data_distribution/Data_distribution.md` | Modify: add "Limitations" section |
| `docs/zh/table_design/data_distribution/Data_distribution.md` | Modify: mirror EN |
| `docs/ja/table_design/data_distribution/Data_distribution.md` | Modify: mirror EN |

A single test class consolidates all 8 negative + 1 positive cases. This keeps the range-distribution-specific test setup (`Config.enable_range_distribution = true` plus the shared range-dist table fixtures) in one place rather than scattering related cases across `MaterializedViewHandlerTest`, `SchemaChangeHandlerTest`, `AlterTableClauseAnalyzerTest`. The spec named those existing test files as fallbacks "or equivalent" — a dedicated focused test class is the clearer choice.

---

## Task 0: Branch setup

**Files:** none (git only)

- [ ] **Step 1: Confirm a clean working tree on `main`**

Run:
```bash
cd /home/disk1/yanxiangguang/workspace/starrocks1/starrocks && git status
```

Expected: `nothing to commit, working tree clean` and `On branch main`.

- [ ] **Step 2: Create a feature branch**

Run:
```bash
git checkout -b range-distribution-pre-release-guards
```

Expected: `Switched to a new branch 'range-distribution-pre-release-guards'`.

---

## Task 1: Guard #1 — sync rollup / sync CREATE MV in `createMaterializedViewJob`

Centralized guard that covers both `ALTER TABLE ... ADD ROLLUP` (via `processBatchAddRollup`) and `CREATE MATERIALIZED VIEW ... AS ...` sync form (via `processCreateMaterializedView` → `CreateSyncMVStmt`).

**Files:**
- Modify: `fe/fe-core/src/main/java/com/starrocks/alter/MaterializedViewHandler.java` (insertion point: top of `createMaterializedViewJob` body, currently line 332)
- Modify: `fe/fe-core/src/test/java/com/starrocks/alter/RangeDistributionGuardTest.java` (create file in Task 1; later tasks just append tests)

- [ ] **Step 1: Read the existing method to confirm the insertion point and `olapTable` is in scope**

Run:
```bash
sed -n '320,345p' fe/fe-core/src/main/java/com/starrocks/alter/MaterializedViewHandler.java
```

Expected: see signature
```
private AlterJobV2 createMaterializedViewJob(String mvName, ..., OlapTable olapTable, Database db, ...)
        throws DdlException, AnalysisException {
    if (mvKeysType == null) {
        ...
```

Confirm `olapTable` is a parameter at the method signature.

- [ ] **Step 2: Create the new test class with the first two negative tests (ADD ROLLUP + sync CREATE MV)**

Create file `fe/fe-core/src/test/java/com/starrocks/alter/RangeDistributionGuardTest.java`:

```java
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.alter;

import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RangeDistributionGuardTest {
    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;
    private static boolean savedEnableRangeDistribution;

    @BeforeAll
    public static void setUp() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);
        starRocksAssert.withDatabase("test").useDatabase("test");
        savedEnableRangeDistribution = Config.enable_range_distribution;
        Config.enable_range_distribution = true;
    }

    @AfterAll
    public static void tearDown() {
        Config.enable_range_distribution = savedEnableRangeDistribution;
    }

    private static String rangeTableDdl(String name) {
        return "create table " + name + " (k1 int, k2 int, v1 int)\n" +
                "order by(k1, k2)\n" +
                "properties('replication_num' = '1');";
    }

    @Test
    public void testAddRollupRejectedOnRangeDistribution() throws Exception {
        starRocksAssert.withTable(rangeTableDdl("t_guard_rollup"));
        DdlException ex = assertThrows(DdlException.class, () ->
                starRocksAssert.alterTable(
                        "alter table t_guard_rollup add rollup r1(k1, v1)"));
        assertTrue(ex.getMessage().toLowerCase().contains("range distribution"),
                "Expected 'range distribution' in: " + ex.getMessage());
    }

    @Test
    public void testSyncCreateMaterializedViewRejectedOnRangeDistribution() throws Exception {
        starRocksAssert.withTable(rangeTableDdl("t_guard_syncmv"));
        DdlException ex = assertThrows(DdlException.class, () ->
                starRocksAssert.withMaterializedView(
                        "create materialized view mv_guard_sync as " +
                        "select k1, v1 from t_guard_syncmv"));
        assertTrue(ex.getMessage().toLowerCase().contains("range distribution"),
                "Expected 'range distribution' in: " + ex.getMessage());
    }
}
```

**Note on test harness:** Confirm the exact `StarRocksAssert` / `UtFrameUtils` API by looking at an existing test in `fe/fe-core/src/test/java/com/starrocks/alter/`. If `starRocksAssert.alterTable(String)` does not exist, use the pattern from `AlterTableTest.java:74` (the `starRocksAssert.useDatabase("test").withTable(...)` block); if needed, wrap the ALTER in a `UtFrameUtils.parseStmtWithNewParser` + `DDLStmtExecutor.execute` call. The exception type and the message-contains-"range distribution" assertion are the load-bearing parts.

- [ ] **Step 3: Run the new tests to verify both FAIL (no guard yet)**

Run:
```bash
cd fe && mvn test -pl fe-core \
    -Dtest=RangeDistributionGuardTest \
    -DfailIfNoTests=false 2>&1 | tail -60
```

Expected: both `testAddRollupRejectedOnRangeDistribution` and `testSyncCreateMaterializedViewRejectedOnRangeDistribution` FAIL — either no exception thrown (test fails on `assertThrows`) or wrong exception thrown.

If the tests pass at this stage, the test harness is bypassing the relevant code path and the test needs to be fixed before the guard can be meaningfully tested. Stop and fix the test first.

- [ ] **Step 4: Add the guard in `createMaterializedViewJob`**

In `fe/fe-core/src/main/java/com/starrocks/alter/MaterializedViewHandler.java`, immediately after the method signature (current line 331, before line 332 `if (mvKeysType == null)`):

```java
        if (olapTable.isRangeDistribution()) {
            throw new DdlException(
                "Synchronous materialized view / rollup is not supported on " +
                "tables with range distribution. Use an asynchronous " +
                "materialized view instead — declare it with an explicit " +
                "REFRESH clause (REFRESH ASYNC or REFRESH MANUAL) or a " +
                "DISTRIBUTED BY clause, e.g. CREATE MATERIALIZED VIEW ... " +
                "DISTRIBUTED BY HASH(...) REFRESH ASYNC AS SELECT ...");
        }
```

- [ ] **Step 5: Run the tests to verify both now PASS**

Run:
```bash
cd fe && mvn test -pl fe-core \
    -Dtest=RangeDistributionGuardTest \
    -DfailIfNoTests=false 2>&1 | tail -40
```

Expected: both `testAddRollupRejectedOnRangeDistribution` and `testSyncCreateMaterializedViewRejectedOnRangeDistribution` PASS.

- [ ] **Step 6: Commit**

```bash
cd /home/disk1/yanxiangguang/workspace/starrocks1/starrocks
git add fe/fe-core/src/main/java/com/starrocks/alter/MaterializedViewHandler.java \
        fe/fe-core/src/test/java/com/starrocks/alter/RangeDistributionGuardTest.java
git commit -m "[BugFix] Reject sync MV / ADD ROLLUP on range-distribution tables

The legacy sync MV / rollup job path assumes 1-to-1 tablet pairing between
the base index and the new index, with row-by-row in-place rewrites. On
range-distribution tables the resulting rollup tablet's range metadata is
copied from the base but the rollup data is physically sorted differently,
so the assumption breaks. Block at the central createMaterializedViewJob
entry, which is hit by both ALTER TABLE ... ADD ROLLUP and the
CreateSyncMVStmt form of CREATE MATERIALIZED VIEW.
"
```

---

## Task 2: Guard #2 — `ALTER TABLE ... ORDER BY` (modify sort key)

**Files:**
- Modify: `fe/fe-core/src/main/java/com/starrocks/alter/SchemaChangeHandler.java` (insertion point: top of `processModifySortKeyColumn` body, currently line 1110)
- Modify: `fe/fe-core/src/test/java/com/starrocks/alter/RangeDistributionGuardTest.java` (append test)

- [ ] **Step 1: Read the existing method to confirm the insertion point**

Run:
```bash
sed -n '1105,1115p' fe/fe-core/src/main/java/com/starrocks/alter/SchemaChangeHandler.java
```

Expected: signature
```
private void processModifySortKeyColumn(ReorderColumnsClause alterClause, OlapTable olapTable, ...)
        throws DdlException {
    LinkedList<Column> targetIndexSchema = ...
```

- [ ] **Step 2: Append the failing test to `RangeDistributionGuardTest.java`**

Add inside the existing class (before the closing `}`):

```java
    @Test
    public void testModifySortKeyRejectedOnRangeDistribution() throws Exception {
        starRocksAssert.withTable(rangeTableDdl("t_guard_orderby"));
        // Use a column list SHORTER than base schema so this routes to
        // processModifySortKeyColumn (not the schema-reorder overload).
        DdlException ex = assertThrows(DdlException.class, () ->
                starRocksAssert.alterTable(
                        "alter table t_guard_orderby order by (k1)"));
        assertTrue(ex.getMessage().toLowerCase().contains("range distribution"),
                "Expected 'range distribution' in: " + ex.getMessage());
    }
```

- [ ] **Step 3: Run the new test to verify it FAILS**

Run:
```bash
cd fe && mvn test -pl fe-core \
    -Dtest=RangeDistributionGuardTest#testModifySortKeyRejectedOnRangeDistribution \
    -DfailIfNoTests=false 2>&1 | tail -40
```

Expected: FAIL (no exception or wrong exception).

- [ ] **Step 4: Add the guard**

In `processModifySortKeyColumn`, immediately after the method signature (current line 1109, before line 1110 `LinkedList<Column> targetIndexSchema = ...`):

```java
        if (olapTable.isRangeDistribution()) {
            throw new DdlException(
                "Modifying sort key (ALTER TABLE ... ORDER BY) is not " +
                "supported on tables with range distribution, because the " +
                "sort key defines tablet boundaries.");
        }
```

- [ ] **Step 5: Run the test to verify it PASSES**

Run:
```bash
cd fe && mvn test -pl fe-core \
    -Dtest=RangeDistributionGuardTest#testModifySortKeyRejectedOnRangeDistribution \
    -DfailIfNoTests=false 2>&1 | tail -20
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add fe/fe-core/src/main/java/com/starrocks/alter/SchemaChangeHandler.java \
        fe/fe-core/src/test/java/com/starrocks/alter/RangeDistributionGuardTest.java
git commit -m "[BugFix] Reject ALTER TABLE ... ORDER BY on range-distribution tables

Modifying the sort key changes the physical row order within each tablet.
On range-distribution tables that ordering IS the tablet boundary
definition, so the existing 1:1-copied range metadata becomes inconsistent
with the data. Block at processModifySortKeyColumn entry.
"
```

---

## Task 3: Guard #3a — OPTIMIZE analyzer guard

**Files:**
- Modify: `fe/fe-core/src/main/java/com/starrocks/sql/analyzer/AlterTableClauseAnalyzer.java` (insertion point: in `visitOptimizeClause`, after the existing colocate/MV/automatic-bucket-size checks at line 552)
- Modify: `RangeDistributionGuardTest.java` (append)

- [ ] **Step 1: Read the existing visitOptimizeClause**

Run:
```bash
sed -n '536,560p' fe/fe-core/src/main/java/com/starrocks/sql/analyzer/AlterTableClauseAnalyzer.java
```

Expected: see the existing three `ErrorReport.reportSemanticException(...)` calls ending around line 552.

- [ ] **Step 2: Append failing test**

```java
    @Test
    public void testOptimizeRejectedOnRangeDistribution() throws Exception {
        starRocksAssert.withTable(rangeTableDdl("t_guard_optimize"));
        SemanticException ex = assertThrows(SemanticException.class, () ->
                starRocksAssert.alterTable(
                        "alter table t_guard_optimize optimize"));
        assertTrue(ex.getMessage().toLowerCase().contains("range distribution"),
                "Expected 'range distribution' in: " + ex.getMessage());
    }
```

- [ ] **Step 3: Run test → FAIL**

```bash
cd fe && mvn test -pl fe-core \
    -Dtest=RangeDistributionGuardTest#testOptimizeRejectedOnRangeDistribution \
    -DfailIfNoTests=false 2>&1 | tail -30
```

Expected: FAIL.

- [ ] **Step 4: Add the analyzer guard**

In `AlterTableClauseAnalyzer.visitOptimizeClause`, immediately after line 552 (after the `automaticBucketSize` check, before whatever follows):

```java
        if (olapTable.isRangeDistribution()) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR,
                "OPTIMIZE is not supported on tables with range " +
                "distribution, because it redistributes data and would " +
                "violate range tablet boundaries.");
        }
```

- [ ] **Step 5: Run test → PASS**

```bash
cd fe && mvn test -pl fe-core \
    -Dtest=RangeDistributionGuardTest#testOptimizeRejectedOnRangeDistribution \
    -DfailIfNoTests=false 2>&1 | tail -20
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add fe/fe-core/src/main/java/com/starrocks/sql/analyzer/AlterTableClauseAnalyzer.java \
        fe/fe-core/src/test/java/com/starrocks/alter/RangeDistributionGuardTest.java
git commit -m "[BugFix] Reject ALTER TABLE ... OPTIMIZE on range-distribution tables (analyzer)

OPTIMIZE redistributes / rebuckets an entire partition, which is
fundamentally incompatible with range-distribution tablet boundaries.
Guard at the analyzer for clearest user-facing SQL error.
"
```

---

## Task 4: Guard #3b — OPTIMIZE handler defense-in-depth

`createOptimizeTableJob` is also reachable from non-`visitOptimizeClause` callers. A second guard prevents corruption from any future caller that bypasses the analyzer.

**Files:**
- Modify: `fe/fe-core/src/main/java/com/starrocks/alter/SchemaChangeHandler.java` (insertion point: top of `createOptimizeTableJob`, currently line 177)

No new test — the analyzer guard already covers the SQL path, and the handler check is best-effort defense per the spec. The existing OPTIMIZE test exercises the analyzer path and would not reach this handler check anyway.

- [ ] **Step 1: Read the existing createOptimizeTableJob**

Run:
```bash
sed -n '174,184p' fe/fe-core/src/main/java/com/starrocks/alter/SchemaChangeHandler.java
```

Expected:
```
private AlterJobV2 createOptimizeTableJob(
        OptimizeClause optimizeClause, Database db, OlapTable olapTable, Map<String, String> propertyMap)
        throws StarRocksException {
    if (olapTable.getState() != OlapTableState.NORMAL) { ...
```

- [ ] **Step 2: Add the defensive guard**

In `createOptimizeTableJob`, immediately after the method signature opens (before line 177, the `getState() != NORMAL` check):

```java
        if (olapTable.isRangeDistribution()) {
            throw new DdlException(
                "OPTIMIZE is not supported on tables with range distribution.");
        }
```

- [ ] **Step 3: Build to confirm the file still compiles**

Run:
```bash
cd fe && mvn compile -pl fe-core 2>&1 | tail -20
```

Expected: BUILD SUCCESS. (No new test for this layer per spec; the previous OPTIMIZE test still PASSes.)

- [ ] **Step 4: Re-run the existing OPTIMIZE test to confirm no regression**

```bash
cd fe && mvn test -pl fe-core \
    -Dtest=RangeDistributionGuardTest#testOptimizeRejectedOnRangeDistribution \
    -DfailIfNoTests=false 2>&1 | tail -20
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add fe/fe-core/src/main/java/com/starrocks/alter/SchemaChangeHandler.java
git commit -m "[BugFix] Reject OPTIMIZE on range-distribution tables (handler defense)

createOptimizeTableJob is reachable from callers that bypass the analyzer.
Add a defensive guard so any future bypass also fails loudly.
"
```

---

## Task 5: Guard #4 shared helper — `rejectIfTouchesRangeSortKey`

Single private helper used by the DROP and MODIFY (existing sort-key column) sub-guards. ADD uses a simpler `column.isKey()` check and does not call this helper.

**Files:**
- Modify: `fe/fe-core/src/main/java/com/starrocks/alter/SchemaChangeHandler.java` (add private static method, near the existing helper methods in the class — locate any private static helper at the bottom of the class)

- [ ] **Step 1: Locate a sensible insertion point for the helper**

Run:
```bash
grep -n "private static" fe/fe-core/src/main/java/com/starrocks/alter/SchemaChangeHandler.java | tail -10
```

Pick the file's existing convention: if helpers live at the bottom of the class, append near the closing brace; if mid-file, follow that pattern. Avoid declaring imports the file does not already have without checking — `MetaUtils`, `Column`, `OlapTable`, `DdlException`, and `List` must already be imported or added.

- [ ] **Step 2: Confirm needed imports**

Run:
```bash
grep -n "^import " fe/fe-core/src/main/java/com/starrocks/alter/SchemaChangeHandler.java \
    | grep -E "(MetaUtils|Column|OlapTable|DdlException|java.util.List)" || true
```

If any of the following imports are missing, add them in alphabetical order with the existing import block:
- `import com.starrocks.catalog.Column;`
- `import com.starrocks.catalog.OlapTable;`
- `import com.starrocks.common.DdlException;`
- `import com.starrocks.sql.common.MetaUtils;`
- `import java.util.List;`

- [ ] **Step 3: Add the helper method**

Append to `SchemaChangeHandler` (replace `<INSERTION_POINT>` with the chosen location):

```java
    /**
     * Reject the operation if the named column belongs to the range-
     * distribution sort-key column set of the given index.
     *
     * Range-distribution tablet boundaries are stored as 1:1 copies of the
     * base values, and any ADD/DROP/MODIFY that changes the column set or
     * type/semantics of those columns invalidates the stored boundary
     * interpretation — even if no row physically moves.
     */
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
                "because it would change the column set or type/semantics " +
                "that the existing range tablet boundary values were " +
                "recorded under. Column: " + colName);
        }
    }
```

- [ ] **Step 4: Build**

```bash
cd fe && mvn compile -pl fe-core 2>&1 | tail -20
```

Expected: BUILD SUCCESS.

- [ ] **Step 5: Commit**

```bash
git add fe/fe-core/src/main/java/com/starrocks/alter/SchemaChangeHandler.java
git commit -m "[Refactor] Add rejectIfTouchesRangeSortKey helper in SchemaChangeHandler

Shared by the upcoming DROP / MODIFY range-distribution sort-key column
guards. Centralizes the MetaUtils.getRangeDistributionColumns lookup and
the error message wording.
"
```

---

## Task 6: Guard #4-ADD — block ADD of a key column on range-dist tables

**Files:**
- Modify: `fe/fe-core/src/main/java/com/starrocks/alter/SchemaChangeHandler.java` (`processAddColumn` and `processAddColumns`)
- Modify: `RangeDistributionGuardTest.java` (append)

- [ ] **Step 1: Read processAddColumn to locate the insertion point**

Run:
```bash
sed -n '234,260p' fe/fe-core/src/main/java/com/starrocks/alter/SchemaChangeHandler.java
```

Confirm:
- Signature is `private boolean processAddColumn(AddColumnClause alterClause, OlapTable olapTable, ...)`.
- `Column column = alterClause.getColumn();` (or equivalent — find the line where the single `column` is obtained).
- The new guard goes immediately after `column` is available and before `addColumnInternal` is called.

Also read `processAddColumns` (multi-column variant) at lines 270+:
```bash
sed -n '270,300p' fe/fe-core/src/main/java/com/starrocks/alter/SchemaChangeHandler.java
```

Locate the loop / list `List<Column> columns = alterClause.getColumns();` (or equivalent).

- [ ] **Step 2: Append failing test**

```java
    @Test
    public void testAddKeyColumnRejectedOnRangeDistribution() throws Exception {
        // AGG keys-type so that ADD of a key column is the dangerous variant
        // (new key columns auto-append to the key-derived sort key).
        starRocksAssert.withTable(
                "create table t_guard_addkey (k1 int sum, k2 int sum)\n" +
                "AGGREGATE KEY()\n" +
                "order by(k1)\n" +
                "properties('replication_num' = '1');");
        DdlException ex = assertThrows(DdlException.class, () ->
                starRocksAssert.alterTable(
                        "alter table t_guard_addkey add column k_new int key default '0'"));
        assertTrue(ex.getMessage().toLowerCase().contains("range distribution"),
                "Expected 'range distribution' in: " + ex.getMessage());
    }
```

**Note:** If the AGG range table syntax above is rejected by the analyzer (it may be — verify by testing the table-creation in isolation first), adjust to a DUP range table with no explicit ORDER BY:
```sql
create table t_guard_addkey (k1 int, k2 int) properties('replication_num' = '1');
```
The key requirement is: the test creates a range-distribution table on which `ADD COLUMN ... KEY` would normally succeed.

- [ ] **Step 3: Run the test → FAIL**

```bash
cd fe && mvn test -pl fe-core \
    -Dtest=RangeDistributionGuardTest#testAddKeyColumnRejectedOnRangeDistribution \
    -DfailIfNoTests=false 2>&1 | tail -30
```

Expected: FAIL.

- [ ] **Step 4: Add the guard in processAddColumn**

In `processAddColumn`, immediately after `column` is obtained from `alterClause` and before `addColumnInternal` is called (current location ~line 256):

```java
        if (olapTable.isRangeDistribution() && column.isKey()) {
            throw new DdlException(
                "ADD COLUMN of a key column on a range-distribution table " +
                "is not supported, because new key columns are appended to " +
                "the range sort key (AGG/UNIQUE) or derived sort key (no " +
                "explicit ORDER BY), invalidating stored range tablet " +
                "boundary values. Column: " + column.getName());
        }
```

- [ ] **Step 5: Add the same guard in processAddColumns (multi-column variant)**

In `processAddColumns`, after the list of columns is obtained (e.g. `List<Column> columns = alterClause.getColumns();`), iterate and reject any key column:

```java
        if (olapTable.isRangeDistribution()) {
            for (Column c : columns) {
                if (c.isKey()) {
                    throw new DdlException(
                        "ADD COLUMN of a key column on a range-distribution " +
                        "table is not supported, because new key columns are " +
                        "appended to the range sort key (AGG/UNIQUE) or " +
                        "derived sort key (no explicit ORDER BY), " +
                        "invalidating stored range tablet boundary values. " +
                        "Column: " + c.getName());
                }
            }
        }
```

(Replace `columns` with the actual local variable name in that scope after reading the code.)

- [ ] **Step 6: Run test → PASS**

```bash
cd fe && mvn test -pl fe-core \
    -Dtest=RangeDistributionGuardTest#testAddKeyColumnRejectedOnRangeDistribution \
    -DfailIfNoTests=false 2>&1 | tail -20
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add fe/fe-core/src/main/java/com/starrocks/alter/SchemaChangeHandler.java \
        fe/fe-core/src/test/java/com/starrocks/alter/RangeDistributionGuardTest.java
git commit -m "[BugFix] Reject ADD COLUMN of key column on range-distribution tables

On AGG/UNIQUE tables new key columns auto-append to the range sort key,
and on tables without explicit ORDER BY the range sort key is derived from
key columns. Adding a key column thus shifts the stored range tablet
boundary interpretation. Block at both processAddColumn (single) and
processAddColumns (multi-column).
"
```

---

## Task 7: Guard #4-DROP — block DROP of a range sort-key column

**Files:**
- Modify: `fe/fe-core/src/main/java/com/starrocks/alter/SchemaChangeHandler.java` (`processDropColumn`)
- Modify: `RangeDistributionGuardTest.java` (append)

- [ ] **Step 1: Read processDropColumn to find a single insertion point**

Run:
```bash
sed -n '324,395p' fe/fe-core/src/main/java/com/starrocks/alter/SchemaChangeHandler.java
```

Confirm:
- Signature is `private boolean processDropColumn(DropColumnClause alterClause, OlapTable olapTable, ...)`.
- Locate the line where the to-be-dropped column NAME is obtained from `alterClause` (e.g. `String dropColName = alterClause.getColName();`).
- Locate where `targetIndexMetaId` / `baseIndexMetaId` is determined (likely similar to `processAddColumn`'s pattern around line 245). The guard must run after the index meta id is known and before any per-keys-type branch mutates state.

If a single insertion point above all four per-keys-type branches (PK/UNIQUE/AGG/DUP at lines 344/359/366/390) is available, use it. Otherwise add the same call at the top of each branch.

- [ ] **Step 2: Append failing test**

```java
    @Test
    public void testDropSortKeyColumnRejectedOnRangeDistribution() throws Exception {
        // DUP range table: k1, k2 are both sort/key columns.
        starRocksAssert.withTable(rangeTableDdl("t_guard_dropsk"));
        DdlException ex = assertThrows(DdlException.class, () ->
                starRocksAssert.alterTable(
                        "alter table t_guard_dropsk drop column k2"));
        assertTrue(ex.getMessage().toLowerCase().contains("range distribution"),
                "Expected 'range distribution' in: " + ex.getMessage());
        assertTrue(ex.getMessage().toLowerCase().contains("k2"),
                "Expected 'k2' (offending column) in: " + ex.getMessage());
    }
```

- [ ] **Step 3: Run test → FAIL**

```bash
cd fe && mvn test -pl fe-core \
    -Dtest=RangeDistributionGuardTest#testDropSortKeyColumnRejectedOnRangeDistribution \
    -DfailIfNoTests=false 2>&1 | tail -30
```

Expected: FAIL. (Existing PK-only sort-key drop rejection does not apply to DUP, so without the new guard the DROP either succeeds or fails with a non-range-distribution-specific message.)

- [ ] **Step 4: Add the guard**

In `processDropColumn`, immediately after the dropped column name and the relevant index meta id are determined (and before the per-keys-type dispatch at line ~344):

```java
        rejectIfTouchesRangeSortKey(olapTable, olapTable.getBaseIndexMetaId(),
                "DROP COLUMN", dropColName);
```

Use the actual local variable name for the dropped column name (e.g. `alterClause.getColName()` may be assigned to a different variable name in this method).

- [ ] **Step 5: Run test → PASS**

```bash
cd fe && mvn test -pl fe-core \
    -Dtest=RangeDistributionGuardTest#testDropSortKeyColumnRejectedOnRangeDistribution \
    -DfailIfNoTests=false 2>&1 | tail -20
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add fe/fe-core/src/main/java/com/starrocks/alter/SchemaChangeHandler.java \
        fe/fe-core/src/test/java/com/starrocks/alter/RangeDistributionGuardTest.java
git commit -m "[BugFix] Reject DROP COLUMN on range-distribution sort-key column

Existing processDropColumn rejects sort-key drops only for PRIMARY_KEYS.
DUP and AGG paths may allow it, removing a column whose stored 1:1-copied
range tablet boundary values are still referenced. Block via the new
rejectIfTouchesRangeSortKey helper at the top of processDropColumn.
"
```

---

## Task 8: Guard #4-MODIFY — block MODIFY of a range sort-key column AND keyness flips

Two checks at the same overall location: (1) the column is already in the range sort key; (2) the operation changes keyness regardless of current sort-key membership.

**Files:**
- Modify: `fe/fe-core/src/main/java/com/starrocks/alter/SchemaChangeHandler.java` (`processModifyColumn`)
- Modify: `RangeDistributionGuardTest.java` (append two negative tests + one positive narrowness test)

- [ ] **Step 1: Read processModifyColumn around the placement target**

Run:
```bash
sed -n '676,790p' fe/fe-core/src/main/java/com/starrocks/alter/SchemaChangeHandler.java
```

Confirm:
- `modColumn` is built around line 683.
- `indexMetaIdForFindingColumn` is computed around line 771.
- The `found` flag is set in the for-loop around line 781–785.
- `oriColumn` is the column at `schemaForFinding.get(modColIndex)` once `found == true`.

The guard must run AFTER `found == true` is established for the current iteration (so that `oriColumn` is meaningful) and BEFORE `schemaForFinding` is mutated.

- [ ] **Step 2: Append the two negative tests + one positive narrowness test**

```java
    @Test
    public void testModifySortKeyColumnTypeRejectedOnRangeDistribution() throws Exception {
        starRocksAssert.withTable(rangeTableDdl("t_guard_modsk"));
        DdlException ex = assertThrows(DdlException.class, () ->
                starRocksAssert.alterTable(
                        "alter table t_guard_modsk modify column k1 bigint"));
        assertTrue(ex.getMessage().toLowerCase().contains("range distribution"),
                "Expected 'range distribution' in: " + ex.getMessage());
        assertTrue(ex.getMessage().toLowerCase().contains("k1"),
                "Expected 'k1' (offending column) in: " + ex.getMessage());
    }

    @Test
    public void testModifyKeynessFlipRejectedOnRangeDistribution() throws Exception {
        // AGG table so that a value column can be promoted via MODIFY ... KEY.
        starRocksAssert.withTable(
                "create table t_guard_keyflip (k1 int, v1 int sum)\n" +
                "AGGREGATE KEY(k1)\n" +
                "order by(k1)\n" +
                "properties('replication_num' = '1');");
        DdlException ex = assertThrows(DdlException.class, () ->
                starRocksAssert.alterTable(
                        "alter table t_guard_keyflip modify column v1 int key"));
        assertTrue(ex.getMessage().toLowerCase().contains("range distribution"),
                "Expected 'range distribution' in: " + ex.getMessage());
        assertTrue(ex.getMessage().toLowerCase().contains("keyness")
                        || ex.getMessage().toLowerCase().contains("key column"),
                "Expected keyness/key-column language in: " + ex.getMessage());
    }

    @Test
    public void testModifyNonSortKeyColumnAllowedOnRangeDistribution() throws Exception {
        // Positive narrowness test: MODIFY of a non-sort-key, non-key column,
        // with no keyness change, must still succeed on a range-distribution table.
        starRocksAssert.withTable(rangeTableDdl("t_guard_modok"));
        // v1 is a value column (not in (k1, k2) sort key) — widening should pass.
        starRocksAssert.alterTable(
                "alter table t_guard_modok modify column v1 bigint");
        // If it returned without exception, the narrow guard is correct.
    }
```

**Note on the AGG fixture:** If the AGG syntax above doesn't construct a range table cleanly, an alternative is a UNIQUE keys-type table or any range table where keyness-flip is meaningful — the goal is `oriColumn.isKey() != modColumn.isKey()` and the column was NOT previously in `MetaUtils.getRangeDistributionColumns()`.

- [ ] **Step 3: Run the three new tests → first two FAIL, third PASSes**

```bash
cd fe && mvn test -pl fe-core \
    -Dtest='RangeDistributionGuardTest#testModifySortKeyColumnTypeRejectedOnRangeDistribution+RangeDistributionGuardTest#testModifyKeynessFlipRejectedOnRangeDistribution+RangeDistributionGuardTest#testModifyNonSortKeyColumnAllowedOnRangeDistribution' \
    -DfailIfNoTests=false 2>&1 | tail -50
```

Expected:
- `testModifySortKeyColumnTypeRejectedOnRangeDistribution` FAIL
- `testModifyKeynessFlipRejectedOnRangeDistribution` FAIL
- `testModifyNonSortKeyColumnAllowedOnRangeDistribution` PASS already (no guard preventing it)

If the positive test fails at this stage (something else is blocking the legal MODIFY), stop and resolve before adding the negative guards — implementing the guards on top of a broken positive baseline will hide whichever pre-existing issue blocks plain MODIFY.

- [ ] **Step 4: Add the two checks at the established placement**

Inside the for-loop in `processModifyColumn`, after `found = true; modColIndex = i; modColumn = ...;` is established and before `schemaForFinding` is mutated:

```java
                    // Range-distribution sort-key column protection (row #4d).
                    rejectIfTouchesRangeSortKey(olapTable, indexMetaIdForFindingColumn,
                            "MODIFY COLUMN", newColName);
                    // Range-distribution keyness-flip protection (row #4e).
                    Column oriColumn = schemaForFinding.get(modColIndex);
                    if (olapTable.isRangeDistribution()
                            && oriColumn.isKey() != modColumn.isKey()) {
                        throw new DdlException(
                            "MODIFY COLUMN that changes keyness on a " +
                            "range-distribution table is not supported, " +
                            "because adding to or removing from the key " +
                            "column set shifts the range sort key on " +
                            "AGG/UNIQUE tables and on tables without " +
                            "explicit ORDER BY. Column: " + newColName);
                    }
```

(Use the actual local variable names already in the method: `newColName`, `modColumn`, `schemaForFinding`, `modColIndex`, `indexMetaIdForFindingColumn`. Confirm by reading lines 771–790 first.)

- [ ] **Step 5: Run the three new tests → all PASS**

```bash
cd fe && mvn test -pl fe-core \
    -Dtest='RangeDistributionGuardTest#testModifySortKeyColumnTypeRejectedOnRangeDistribution+RangeDistributionGuardTest#testModifyKeynessFlipRejectedOnRangeDistribution+RangeDistributionGuardTest#testModifyNonSortKeyColumnAllowedOnRangeDistribution' \
    -DfailIfNoTests=false 2>&1 | tail -30
```

Expected: all three PASS.

- [ ] **Step 6: Run the FULL `RangeDistributionGuardTest` class to verify nothing earlier broke**

```bash
cd fe && mvn test -pl fe-core \
    -Dtest=RangeDistributionGuardTest \
    -DfailIfNoTests=false 2>&1 | tail -40
```

Expected: all 8 negative tests + 1 positive narrowness test PASS.

- [ ] **Step 7: Commit**

```bash
git add fe/fe-core/src/main/java/com/starrocks/alter/SchemaChangeHandler.java \
        fe/fe-core/src/test/java/com/starrocks/alter/RangeDistributionGuardTest.java
git commit -m "[BugFix] Reject MODIFY COLUMN on range-distribution sort-key column or keyness flip

Two checks at the same processModifyColumn placement (after the column is
found, before schemaForFinding is mutated):
(1) reject if the column is already in MetaUtils.getRangeDistributionColumns()
(2) reject if MODIFY changes keyness, even for columns not currently in the
    sort key — a value->KEY promotion adds the column to the key-derived
    range sort key on AGG/UNIQUE / no-explicit-ORDER-BY tables.

Positive narrowness test confirms that MODIFY of a non-sort-key,
non-keyness-changing column still succeeds.
"
```

---

## Task 9: Regression sweep

Find and fix any existing happy-path tests that combined range distribution with one of the now-blocked DDLs.

**Files:** any FE test files containing both `enable_range_distribution` setup and one of the blocked DDLs.

- [ ] **Step 1: Grep for existing happy-path combinations**

Run:
```bash
cd /home/disk1/yanxiangguang/workspace/starrocks1/starrocks
grep -rlE "(enable_range_distribution|order by\\(|create table.*order by)" \
    fe/fe-core/src/test/java/ 2>/dev/null \
  | xargs grep -lE "(add rollup|create materialized view|order by \\(|optimize|add column.*key|drop column|modify column)" 2>/dev/null \
  | sort -u
```

Expected: a list of test files. Open each and inspect whether it actually combines a range-distribution table with a now-blocked DDL.

- [ ] **Step 2: For each match, decide: convert to negative test, delete the case, or no-op (false positive)**

For each affected test:
- If it asserts the DDL succeeds → convert to a negative test (assert `DdlException` / `SemanticException` with "range distribution" in the message), OR move the table fixture to hash distribution if that was the test's actual intent.
- If it incidentally creates a range table but the DDL targets a different table → no change.
- Document each decision inline below this step:

```
<file>:<line> — <decision: convert / move-to-hash / no-op / delete> — <reason>
```

- [ ] **Step 3: Run FE module tests for any files touched in Step 2**

For each modified test file `X.java`:
```bash
cd fe && mvn test -pl fe-core -Dtest=X -DfailIfNoTests=false 2>&1 | tail -30
```

Expected: all tests PASS.

- [ ] **Step 4: Commit**

If any files were modified in Step 2:
```bash
git add <files-modified-in-step-2>
git commit -m "[BugFix] Regression sweep: convert range-distribution + blocked-DDL happy-path tests

Range-distribution + sync rollup / ORDER BY / OPTIMIZE / ADD-key /
DROP-sort-key / MODIFY-sort-key are now blocked. Existing tests that
relied on these combinations are converted to negative assertions (or
moved to hash distribution where the DDL was the load-bearing part).
"
```

If no files were modified, skip this commit and just record in the PR description that the regression sweep produced no hits.

---

## Task 10: Documentation (en/zh/ja)

**Files:**
- Modify: `docs/en/table_design/data_distribution/Data_distribution.md`
- Modify: `docs/zh/table_design/data_distribution/Data_distribution.md`
- Modify: `docs/ja/table_design/data_distribution/Data_distribution.md`

- [ ] **Step 1: Locate the range-distribution section in the English doc**

Run:
```bash
grep -n -iE "range[ -]distribution" docs/en/table_design/data_distribution/Data_distribution.md
```

Pick an appropriate insertion point — preferably immediately after the section that introduces range distribution, or under a dedicated `## Limitations` subsection.

- [ ] **Step 2: Add the limitations section to the English doc**

Append (or insert in the range-distribution section):

```markdown
### Limitations

The following operations are not supported on tables with range distribution:

| DDL | Reason |
|---|---|
| `ALTER TABLE ... ADD ROLLUP ...` | Synchronous rollup assumes 1-to-1 base/rollup tablet pairing with same row order, which range distribution does not provide. |
| `CREATE MATERIALIZED VIEW ... AS ...` (synchronous form, no `REFRESH` and no `DISTRIBUTED BY` clause) | Same code path as `ADD ROLLUP`. |
| `ALTER TABLE ... ORDER BY (...)` (modify sort key) | The sort key defines tablet boundaries, so modifying it would invalidate existing range tablets. |
| `ALTER TABLE ... OPTIMIZE` | OPTIMIZE redistributes / rebuckets a partition, which is incompatible with range tablet boundaries. |
| `ALTER TABLE ... ADD COLUMN <col> KEY ...` | New key columns auto-append to the (derived) range sort key on AGG/UNIQUE tables or tables without explicit `ORDER BY`. |
| `ALTER TABLE ... DROP COLUMN <col>` where `<col>` is in the range sort key | Removing a sort-key column invalidates the stored 1:1-copied range tablet boundary values. |
| `ALTER TABLE ... MODIFY COLUMN <col> ...` where `<col>` is in the range sort key | Changes the type/semantics under which the stored range tablet boundary values were recorded. |
| `ALTER TABLE ... MODIFY COLUMN <col> ... KEY` (or value-column promotion in any keys-type) that flips keyness | A keyness flip shifts the key-derived range sort key on AGG/UNIQUE / no-explicit-ORDER-BY tables. |

For rollup-like aggregation use cases, use an **asynchronous materialized view** with an explicit `REFRESH` clause or a `DISTRIBUTED BY` clause, e.g.:

```sql
CREATE MATERIALIZED VIEW mv
DISTRIBUTED BY HASH(col)
REFRESH ASYNC
AS SELECT ... FROM range_table;
```
```

- [ ] **Step 3: Mirror to the Chinese doc**

Add the equivalent section to `docs/zh/table_design/data_distribution/Data_distribution.md`. Translation guidance:

| EN | ZH |
|---|---|
| Limitations | 限制 |
| not supported | 不支持 |
| sort key defines tablet boundaries | sort key 决定了 tablet 的边界 |
| redistributes / rebuckets | 重新分布数据 |
| asynchronous materialized view | 异步物化视图 |

Use the EN section as a template; preserve the table structure and SQL example verbatim.

- [ ] **Step 4: Mirror to the Japanese doc**

Per `docs/CLAUDE.md`, Japanese updates are required when a corresponding `docs/ja/` page exists — and `docs/ja/table_design/data_distribution/Data_distribution.md` does exist.

Add the equivalent section to the Japanese doc. Translation guidance:

| EN | JA |
|---|---|
| Limitations | 制限事項 |
| not supported | サポートされていません |
| sort key defines tablet boundaries | ソートキーが tablet の境界を決定します |
| asynchronous materialized view | 非同期マテリアライズドビュー |

- [ ] **Step 5: Spot-check rendering**

Run:
```bash
ls -la docs/en/table_design/data_distribution/Data_distribution.md \
       docs/zh/table_design/data_distribution/Data_distribution.md \
       docs/ja/table_design/data_distribution/Data_distribution.md
```

Confirm all three were modified. (No Docusaurus build required for a small content-only change, but if uncertainty exists, run `cd docs/docusaurus && npm run build` per `docs/CLAUDE.md`.)

- [ ] **Step 6: Commit**

```bash
git add docs/en/table_design/data_distribution/Data_distribution.md \
        docs/zh/table_design/data_distribution/Data_distribution.md \
        docs/ja/table_design/data_distribution/Data_distribution.md
git commit -m "[Doc] Document range-distribution limitations for sync MV / OPTIMIZE / schema change

Enumerate the eight DDL operations that are now blocked on
range-distribution tables, and point rollup-like use cases to async
materialized views with explicit REFRESH or DISTRIBUTED BY.
"
```

---

## Task 11: Full FE module test + checkstyle gate

Catch any cross-test regression and any style violations introduced by the new code.

- [ ] **Step 1: Run the full `RangeDistributionGuardTest` once more (sanity)**

```bash
cd fe && mvn test -pl fe-core \
    -Dtest=RangeDistributionGuardTest \
    -DfailIfNoTests=false 2>&1 | tail -30
```

Expected: 9 tests run, 9 pass.

- [ ] **Step 2: Run the alter / schema-change adjacent test classes to catch any cross-test interference**

```bash
cd fe && mvn test -pl fe-core \
    -Dtest='AlterTableTest,SchemaChangeHandlerTest,MaterializedViewHandlerTest,AlterTableClauseAnalyzerTest,RangeDistributionGuardTest,RangeColocateTableTest' \
    -DfailIfNoTests=false 2>&1 | tail -40
```

Expected: all PASS. If any of these test class names do not exist in the repo, the harness will skip them (`-DfailIfNoTests=false`) — that is fine; the goal is to exercise the neighbours that actually exist.

- [ ] **Step 3: Run checkstyle on the modified files**

```bash
cd fe && mvn checkstyle:check -pl fe-core 2>&1 | tail -30
```

Expected: BUILD SUCCESS. Fix any violations (most commonly: line length, import order, missing javadoc on public API).

- [ ] **Step 4: Final verification — branch is clean and ahead of main**

```bash
cd /home/disk1/yanxiangguang/workspace/starrocks1/starrocks
git status
git log --oneline main..HEAD
```

Expected: clean working tree, ~10 commits ahead of `main` (one per Task 1–10, minus the no-op-allowed Task 9).

---

## Done

The branch `range-distribution-pre-release-guards` is ready for code review. Open a PR with the title prefix `[BugFix]` per the StarRocks PR contract, fill the PR template (behavior-change classification: bug fix / pre-release safety), and check the test + docs boxes.
