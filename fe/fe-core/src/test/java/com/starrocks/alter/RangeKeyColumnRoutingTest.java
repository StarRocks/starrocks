// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.alter;

import com.google.common.collect.Maps;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.OlapTable;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.sql.ast.AlterClause;
import com.starrocks.sql.ast.DropColumnClause;
import com.starrocks.sql.ast.KeysType;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Routing tests for DROP COLUMN of a range sort-key column on a shared-data (lake) range-distribution
 * table: DUP and AGG-without-REPLACE drops are routed to {@link LakeRangeRewriteSchemaChangeJob}, while
 * PK / UNIQUE / AGG-REPLACE key drops, partition-column drops, and last-key drops stay rejected.
 */
public class RangeKeyColumnRoutingTest {
    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;
    private static boolean savedEnableRangeDistribution;
    private static final AtomicInteger TABLE_SEQ = new AtomicInteger();

    @BeforeAll
    public static void setUp() throws Exception {
        UtFrameUtils.createMinStarRocksCluster(RunMode.SHARED_DATA);
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

    /**
     * Creates a shared-data range-distribution table with the given keysType and sort key (k1, k2, ...);
     * every sort-key column is an INT and the sole value column is {@code v1 int}, aggregated with SUM
     * for AGG_KEYS. Each call gets a fresh table name so tests do not collide.
     */
    private static OlapTable buildLakeRangeTable(KeysType keysType, List<String> sortKey) throws Exception {
        return buildLakeRangeTable(keysType, sortKey, keysType == KeysType.AGG_KEYS ? "SUM" : null);
    }

    /** Like {@link #buildLakeRangeTable(KeysType, List)} but with an explicit value-column aggregation. */
    private static OlapTable buildLakeRangeTable(KeysType keysType, List<String> sortKey, String valueAgg)
            throws Exception {
        String tableName = "t_rangekey_" + TABLE_SEQ.incrementAndGet();
        String keyList = String.join(", ", sortKey);
        String columnDefs = sortKey.stream().map(c -> c + " int NOT NULL").collect(Collectors.joining(", "));
        String valueColumnDef = valueAgg == null ? "v1 int" : "v1 int " + valueAgg;
        String ddl = "create table " + tableName + " (" + columnDefs + ", " + valueColumnDef + ")\n"
                + keysType.toSql() + "(" + keyList + ")\n"
                + "order by(" + keyList + ")\n"
                + "properties('replication_num' = '1');";
        starRocksAssert.withTable(ddl);
        return (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable("test", tableName);
    }

    /**
     * Creates a shared-data range-distribution table like {@link #buildLakeRangeTable} but additionally
     * RANGE-partitioned on {@code partitionCol}, so the partition-column-drop guard applies.
     */
    private static OlapTable buildLakeRangePartitionedTable(KeysType keysType, List<String> sortKey,
                                                             String partitionCol) throws Exception {
        String tableName = "t_rangekey_part_" + TABLE_SEQ.incrementAndGet();
        String keyList = String.join(", ", sortKey);
        String columnDefs = sortKey.stream().map(c -> c + " int NOT NULL").collect(Collectors.joining(", "));
        String ddl = "create table " + tableName + " (" + columnDefs + ", v1 int)\n"
                + keysType.toSql() + "(" + keyList + ")\n"
                + "PARTITION BY RANGE(" + partitionCol + ") (\n"
                + "    PARTITION p1 VALUES LESS THAN (\"100\"),\n"
                + "    PARTITION p2 VALUES LESS THAN MAXVALUE\n"
                + ")\n"
                + "order by(" + keyList + ")\n"
                + "properties('replication_num' = '1');";
        starRocksAssert.withTable(ddl);
        return (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable("test", tableName);
    }

    private static void alter(OlapTable table, String clauseSql) throws Exception {
        starRocksAssert.alterTable("alter table " + table.getName() + " " + clauseSql);
    }

    /**
     * The alter-table DDL path wraps the originating {@link DdlException} in an
     * {@link AlterJobException} that carries only its message (no cause), so assert directly against
     * the thrown exception's message rather than unwrapping a cause chain.
     */
    private static void assertThrowsDdl(String expectedSubstring, Executable executable) {
        Throwable thrown = Assertions.assertThrows(Throwable.class, executable);
        Assertions.assertTrue(thrown.getMessage().contains(expectedSubstring),
                "Expected '" + expectedSubstring + "' in: " + thrown.getMessage());
    }

    /** Parses a single alter-clause SQL string against {@code table} and returns the built AlterClause. */
    private static AlterClause parseAlterClause(OlapTable table, String clauseSql) throws Exception {
        com.starrocks.sql.ast.AlterTableStmt stmt = (com.starrocks.sql.ast.AlterTableStmt)
                UtFrameUtils.parseStmtWithNewParser("alter table " + table.getName() + " " + clauseSql, connectContext);
        return stmt.getAlterClauseList().get(0);
    }

    @Test
    public void testAddKeyColumnRoutesForDupAggUnique() throws Exception {
        // A key column added to a table whose range sort key is key-derived (no divergent explicit
        // ORDER BY) routes to the range rewrite job for DUP/AGG/UNIQUE.
        for (KeysType kt : List.of(KeysType.DUP_KEYS, KeysType.AGG_KEYS, KeysType.UNIQUE_KEYS)) {
            OlapTable t = buildLakeRangeTable(kt, List.of("k1"));
            AlterClause add = parseAlterClause(t, "ADD COLUMN k2 INT KEY DEFAULT '0'");
            Assertions.assertTrue(SchemaChangeHandler.needsRangeRewriteSchemaChange(t, add), kt.toString());
        }
    }

    @Test
    public void testAddKeyColumnRejectedForPk() throws Exception {
        // PK add-key stays rejected regardless of routing. In practice the rejection surfaces earlier, at
        // analysis time: AlterTableClauseAnalyzer.visitAddColumnClause stamps REPLACE onto every added
        // column of a PRIMARY_KEYS table, which conflicts with the explicit KEY attribute before
        // addColumnInternal's own (unconditional) PK guard is ever reached.
        OlapTable pk = buildLakeRangeTable(KeysType.PRIMARY_KEYS, List.of("k1"));
        assertThrowsDdl("Cannot specify aggregate function 'REPLACE' for key column",
                () -> alter(pk, "ADD COLUMN k2 INT KEY DEFAULT \"0\""));
    }

    @Test
    public void testAddNonConstantDefaultRejected() throws Exception {
        // A non-constant (VARY) default on an added column is rejected by the pre-existing
        // addColumnInternal CONST/NULL guard, regardless of whether the add is routed to the range
        // rewrite job.
        OlapTable dup = buildLakeRangeTable(KeysType.DUP_KEYS, List.of("k1"));
        assertThrowsDdl("unsupported default expr", () -> alter(dup, "ADD COLUMN k2 VARCHAR(40) KEY DEFAULT (uuid())"));
    }

    @Test
    public void testAddKeyColumnAfterValueColumnRejected() throws Exception {
        // The added column is still routed (it is KEY and the sort key is key-derived), but placing it
        // AFTER the value column v1 would break the key prefix (schema becomes [k1, v1, k2]). The routed
        // add now flows through finalAnalyze, which rejects the invalid column order before any job is
        // built.
        OlapTable dup = buildLakeRangeTable(KeysType.DUP_KEYS, List.of("k1"));
        assertThrowsDdl("Invalid column order. value should be after key",
                () -> alter(dup, "ADD COLUMN k2 INT KEY DEFAULT '0' AFTER v1"));
    }

    @Test
    public void testDropRangeSortKeyColumnRoutesForDupAndAgg() throws Exception {
        // DUP range table with sort key (k1, k2), value v1.
        OlapTable dup = buildLakeRangeTable(KeysType.DUP_KEYS, List.of("k1", "k2"));
        DropColumnClause drop = new DropColumnClause("k2", null, Maps.newHashMap());
        Assertions.assertTrue(SchemaChangeHandler.needsRangeRewriteSchemaChange(dup, drop));

        OlapTable agg = buildLakeRangeTable(KeysType.AGG_KEYS, List.of("k1", "k2")); // v1 SUM (no REPLACE)
        Assertions.assertTrue(SchemaChangeHandler.needsRangeRewriteSchemaChange(agg, drop));

        // Dropping a non-sort-key column is NOT routed (goes the normal path).
        DropColumnClause dropValue = new DropColumnClause("v1", null, Maps.newHashMap());
        Assertions.assertFalse(SchemaChangeHandler.needsRangeRewriteSchemaChange(dup, dropValue));
    }

    @Test
    public void testDropRangeSortKeyColumnRoutedToRewriteJobForDupAndAgg() throws Exception {
        // The routing decision is exercised end to end: analyzeAndCreateJob constructs (but does not
        // run) a LakeRangeRewriteSchemaChangeJob for a DUP or AGG-without-REPLACE drop of a range
        // sort-key column, instead of rejecting it.
        OlapTable dup = buildLakeRangeTable(KeysType.DUP_KEYS, List.of("k1", "k2"));
        AlterJobV2 dupJob = alterAndReturnJob(dup, "DROP COLUMN k2");
        Assertions.assertTrue(dupJob instanceof LakeRangeRewriteSchemaChangeJob,
                "Expected a LakeRangeRewriteSchemaChangeJob, got: " + dupJob);

        OlapTable agg = buildLakeRangeTable(KeysType.AGG_KEYS, List.of("k1", "k2")); // v1 SUM (no REPLACE)
        AlterJobV2 aggJob = alterAndReturnJob(agg, "DROP COLUMN k2");
        Assertions.assertTrue(aggJob instanceof LakeRangeRewriteSchemaChangeJob,
                "Expected a LakeRangeRewriteSchemaChangeJob, got: " + aggJob);
    }

    @Test
    public void testDropRangeKeyColumnStillRejectedForPkUniqueAggReplace() throws Exception {
        OlapTable pk = buildLakeRangeTable(KeysType.PRIMARY_KEYS, List.of("k1", "k2"));
        assertThrowsDdl("Can not drop key column in primary data model table",
                () -> alter(pk, "DROP COLUMN k2"));
        OlapTable uniq = buildLakeRangeTable(KeysType.UNIQUE_KEYS, List.of("k1", "k2"));
        assertThrowsDdl("Can not drop key column in Unique data model table",
                () -> alter(uniq, "DROP COLUMN k2"));
        OlapTable aggReplace = buildLakeRangeTable(KeysType.AGG_KEYS, List.of("k1", "k2"), "REPLACE");
        assertThrowsDdl("Can not drop key column when table has value column with REPLACE",
                () -> alter(aggReplace, "DROP COLUMN k2"));
    }

    @Test
    public void testDropInvalidOrProtectedColumnsStillRejected() throws Exception {
        // Dropping the sole/last key column is rejected precisely (not routed into an unsafe normal job).
        OlapTable dup1 = buildLakeRangeTable(KeysType.DUP_KEYS, List.of("k1")); // single key
        assertThrowsDdl("would leave no key column", () -> alter(dup1, "DROP COLUMN k1"));
        // Dropping a PARTITION column stays rejected by the pre-existing (non-range) guard.
        OlapTable partitioned = buildLakeRangePartitionedTable(KeysType.DUP_KEYS,
                /*sortKey*/ List.of("k1", "k2"), /*partitionCol*/ "k1");
        assertThrowsDdl("Partition column", () -> alter(partitioned, "DROP COLUMN k1"));
    }

    @Test
    public void testDropPreservesExplicitOrderBySortKey() throws Exception {
        // DUP range table whose explicit ORDER BY is a strict superset of KEY(): only k1 is a key
        // column (isKey() == true), but the range sort key is [k1, k2, k3]. createRangeRewriteJob's
        // 3-arg overload derives sortKeyIdxes from isKey() alone, which would collapse the post-drop
        // sort key down to [k1] -- silently dropping k3 from the range sort key. The routed dispatch
        // instead computes the post-drop sort key explicitly from the current range sort key (minus
        // the dropped column) and calls buildRangeRewriteJob directly, preserving [k1, k3].
        String tableName = "t_rangekey_" + TABLE_SEQ.incrementAndGet();
        starRocksAssert.withTable("create table " + tableName
                + " (k1 int NOT NULL, k2 int NOT NULL, k3 int NOT NULL, v1 int)\n"
                + "duplicate key(k1)\n"
                + "order by(k1, k2, k3)\n"
                + "properties('replication_num' = '1');");
        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable("test", tableName);

        AlterJobV2 job = alterAndReturnJob(table, "DROP COLUMN k2");
        Assertions.assertTrue(job instanceof LakeRangeRewriteSchemaChangeJob,
                "Expected a LakeRangeRewriteSchemaChangeJob, got: " + job);
        List<String> newSortKeyNames = ((LakeRangeRewriteSchemaChangeJob) job).getNewSortKeyColumns()
                .stream().map(Column::getName).collect(Collectors.toList());
        Assertions.assertEquals(List.of("k1", "k3"), newSortKeyNames);
    }

    @Test
    public void testDropWholeExplicitSortKeyFallsBackToKeyDerivedSortKey() throws Exception {
        // DUP range table whose explicit ORDER BY is a strict SUBSET of KEY(): KEY(k1, k2) but the
        // range sort key is only [k1]. Dropping k1 removes the entire explicit sort key, even though
        // k2 remains a valid key column. The routed dispatch must fall back to the key-derived sort
        // key (the remaining key columns) instead of building a job with an empty sort key.
        String tableName = "t_rangekey_" + TABLE_SEQ.incrementAndGet();
        starRocksAssert.withTable("create table " + tableName
                + " (k1 int NOT NULL, k2 int NOT NULL, v1 int)\n"
                + "duplicate key(k1, k2)\n"
                + "order by(k1)\n"
                + "properties('replication_num' = '1');");
        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable("test", tableName);

        AlterJobV2 job = alterAndReturnJob(table, "DROP COLUMN k1");
        Assertions.assertTrue(job instanceof LakeRangeRewriteSchemaChangeJob,
                "Expected a LakeRangeRewriteSchemaChangeJob, got: " + job);
        List<String> newSortKeyNames = ((LakeRangeRewriteSchemaChangeJob) job).getNewSortKeyColumns()
                .stream().map(Column::getName).collect(Collectors.toList());
        Assertions.assertEquals(List.of("k2"), newSortKeyNames);
    }

    @Test
    public void testDropRangeSortKeyColumnWithSecondaryIndexRejected() throws Exception {
        // A BITMAP index on the range sort-key column being dropped lives in OlapTable.indexes, not as
        // a MaterializedIndexMeta, so it does not trip the getIndexMetaIdToMeta().size() > 1 single-index
        // routing gate. The routed job never carries index removal (finalAnalyze, which commits
        // processDropColumn's newIndexes back onto the table, is bypassed by the early return), so
        // dropping k2 here must be rejected instead of silently orphaning idx_k2.
        String tableName = "t_rangekey_" + TABLE_SEQ.incrementAndGet();
        starRocksAssert.withTable("create table " + tableName
                + " (k1 int NOT NULL, k2 int NOT NULL, v1 int, INDEX idx_k2 (k2) USING BITMAP)\n"
                + "duplicate key(k1, k2)\n"
                + "order by(k1, k2)\n"
                + "properties('replication_num' = '1');");
        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable("test", tableName);

        assertThrowsDdl("DROP COLUMN of a range sort-key column that has a secondary index",
                () -> alter(table, "DROP COLUMN k2"));
    }

    @Test
    public void testDropRangeSortKeyColumnWithBloomFilterRejected() throws Exception {
        // A plain bloom filter on the dropped range sort-key column is tracked on OlapTable.bfColumns,
        // NOT in getIndexes(), so it does not trip the secondary-index reject above. The routed early
        // return never rewrites the bloom-filter set, so dropping k2 would leave a dangling bloom-filter
        // ColumnId after the flip. It must be rejected.
        String tableName = "t_rangekey_" + TABLE_SEQ.incrementAndGet();
        starRocksAssert.withTable("create table " + tableName
                + " (k1 int NOT NULL, k2 int NOT NULL, v1 int)\n"
                + "duplicate key(k1, k2)\n"
                + "order by(k1, k2)\n"
                + "properties('replication_num' = '1', 'bloom_filter_columns' = 'k2');");
        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable("test", tableName);

        assertThrowsDdl("DROP COLUMN of a range sort-key column that has a bloom filter",
                () -> alter(table, "DROP COLUMN k2"));
    }

    @Test
    public void testAddKeyColumnWithCurrentTimestampDefault() throws Exception {
        // A key column added with DEFAULT CURRENT_TIMESTAMP. The sampling projection (getDefaultValue) and
        // the rewrite materialization (calculatedDefaultValue) resolve a time-function default differently,
        // but the added key column is constant across all historical rows (all get the ALTER-time value),
        // so it adds no distinction to the sort key and K collapses -- no boundary/correctness impact. This
        // pins the behavior (routed + built, not an internal error) so a future regression is caught.
        OlapTable dup = buildLakeRangeTable(KeysType.DUP_KEYS, List.of("k1"));
        AlterJobV2 job = alterAndReturnJob(dup, "ADD COLUMN k_ts DATETIME KEY DEFAULT CURRENT_TIMESTAMP");
        Assertions.assertTrue(job instanceof LakeRangeRewriteSchemaChangeJob,
                "Expected a LakeRangeRewriteSchemaChangeJob, got: " + job);
    }

    @Test
    public void testDropKeyColumnWithoutUniqueIdsClearsSortKeyUniqueIds() throws Exception {
        // A range table whose sort-key columns carry no assigned unique id (uniqueId == INIT_VALUE, e.g.
        // created before fast-schema-evolution unique ids) must not produce a bogus sentinel [-1, ...]
        // sort-key-unique-id list on the flipped index. The routed DROP must clear the list (mirroring the
        // reorder path) and fall back to positional sort-key idxes.
        String tableName = "t_rangekey_" + TABLE_SEQ.incrementAndGet();
        starRocksAssert.withTable("create table " + tableName
                + " (k1 int NOT NULL, k2 int NOT NULL, k3 int NOT NULL, v1 int)\n"
                + "duplicate key(k1, k2, k3)\n"
                + "order by(k1, k2, k3)\n"
                + "properties('replication_num' = '1');");
        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable("test", tableName);
        // Force the pre-unique-id state: strip unique ids from every column.
        for (Column c : table.getBaseSchema()) {
            c.setUniqueId(Column.COLUMN_UNIQUE_ID_INIT_VALUE);
        }

        AlterJobV2 job = alterAndReturnJob(table, "DROP COLUMN k2");
        Assertions.assertTrue(job instanceof LakeRangeRewriteSchemaChangeJob,
                "Expected a LakeRangeRewriteSchemaChangeJob, got: " + job);
        List<Integer> uids = ((LakeRangeRewriteSchemaChangeJob) job).getNewSortKeyUniqueIds();
        Assertions.assertTrue(uids == null || uids.isEmpty(),
                "sort-key unique ids must be cleared when columns have no assigned unique id, got: " + uids);
    }

    @Test
    public void testMultiColumnAddKeyDoesNotRejectNonKeyComplexDefault() throws Exception {
        // A multi-column ADD that bundles a range KEY column (which routes the whole clause to the range
        // rewrite job) with a non-key value column carrying a complex CONST (expr-object) default must
        // NOT reject the value column with the key-column "scalar constant default" error: that guard is
        // scoped to the KEY column. Before the isKey() gate this wrongly rejected v2.
        String tableName = "t_rangekey_" + TABLE_SEQ.incrementAndGet();
        starRocksAssert.withTable("create table " + tableName
                + " (k1 int NOT NULL, v1 int)\n"
                + "duplicate key(k1)\n"
                + "order by(k1)\n"
                + "properties('replication_num' = '1');");
        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable("test", tableName);

        AlterJobV2 job = alterAndReturnJob(table,
                "ADD COLUMN (k2 INT KEY DEFAULT '0', v2 ARRAY<INT> DEFAULT [1, 2, 3])");
        Assertions.assertTrue(job instanceof LakeRangeRewriteSchemaChangeJob,
                "Expected a LakeRangeRewriteSchemaChangeJob, got: " + job);
    }

    @Test
    public void testReorderAndKeynessFlipRoutingUnchanged() throws Exception {
        // Regression guard on the shared predicate: additive ADD/DROP-key routing must not disturb the
        // pre-existing reorder / keyness-flip routing already exercised by RangeRewriteRoutingTest.
        OlapTable dup = buildLakeRangeTable(KeysType.DUP_KEYS, List.of("k1", "k2"));

        // A pure sort-key reorder (fewer columns than the base schema) still routes exactly as before.
        AlterClause reorder = parseAlterClause(dup, "ORDER BY (k2, k1)");
        Assertions.assertTrue(SchemaChangeHandler.needsRangeRewriteSchemaChange(dup, reorder));

        // A MODIFY that is neither add/drop/reorder/flip is still not routed.
        AlterClause unrelatedModify = parseAlterClause(dup, "MODIFY COLUMN v1 BIGINT");
        Assertions.assertFalse(SchemaChangeHandler.needsRangeRewriteSchemaChange(dup, unrelatedModify));
    }

    /** Runs the ALTER via analyzeAndCreateJob and returns the constructed (not run) job. */
    private static AlterJobV2 alterAndReturnJob(OlapTable table, String clauseSql) throws Exception {
        com.starrocks.sql.ast.AlterTableStmt stmt = (com.starrocks.sql.ast.AlterTableStmt)
                UtFrameUtils.parseStmtWithNewParser("alter table " + table.getName() + " " + clauseSql, connectContext);
        return GlobalStateMgr.getCurrentState().getSchemaChangeHandler()
                .analyzeAndCreateJob(stmt.getAlterClauseList(),
                        GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test"), table);
    }
}
