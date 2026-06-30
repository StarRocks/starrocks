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

import com.starrocks.catalog.ColocateTableIndex;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.MaterializedIndexMeta;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.sql.ast.KeysType;
import com.starrocks.thrift.TStorageType;
import com.starrocks.type.PrimitiveType;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import java.util.List;
import java.util.Locale;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class RangeDistributionGuardTest {
    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;
    private static boolean savedEnableRangeDistribution;

    @BeforeAll
    public static void setUp() throws Exception {
        // Keep the background rollup loop from running the LakeRangeRollupJob that the "allowed"
        // tests construct (it would attempt a real sample/rewrite that needs a backend). Tests
        // inspect the just-constructed job and then clear the handler's job map.
        new MockUp<MaterializedViewHandler>() {
            @Mock
            protected void runAfterLeaseValid() {
            }
        };
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

    private static String rangeTableDdl(String name) {
        return "create table " + name + " (k1 int, k2 int, v1 int)\n" +
                "order by(k1, k2)\n" +
                "properties('replication_num' = '1');";
    }

    // Range table whose leading sort/key column k1 is a VARCHAR, used by the
    // VARCHAR-widen relaxation tests below.
    private static String varcharSortKeyRangeTableDdl(String name) {
        return "create table " + name + " (k1 varchar(20) NOT NULL, k2 int NOT NULL, v1 int)\n" +
                "DUPLICATE KEY(k1, k2)\n" +
                "order by(k1, k2)\n" +
                "properties('replication_num' = '1');";
    }

    // Asserts the ALTER is rejected by the range-distribution sort-key guard
    // (whose DdlException message always contains "range distribution").
    private static void assertAlterRejectedWithRangeDistribution(String alterSql) {
        Throwable e = assertThrows(Throwable.class, () -> starRocksAssert.alterTable(alterSql));
        assertTrue(e.getMessage().toLowerCase(Locale.ROOT).contains("range distribution"),
                "Expected 'range distribution' in: " + e.getMessage());
    }

    private static Column baseColumn(String tableName, String columnName) {
        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState()
                .getLocalMetastore().getDb("test").getTable(tableName);
        return table.getBaseSchema().stream()
                .filter(c -> c.getName().equalsIgnoreCase(columnName))
                .findFirst().orElseThrow();
    }

    /**
     * The alter-table DDL path wraps {@link DdlException} in a {@link RuntimeException}
     * via {@code ErrorReport.wrapWithRuntimeException}, while other paths throw the
     * {@link DdlException} directly. This helper extracts the underlying
     * {@link DdlException} regardless of which path is taken.
     */
    private static DdlException assertThrowsDdlException(Executable executable) {
        Throwable thrown = assertThrows(Throwable.class, executable);
        for (Throwable current = thrown; current != null; current = current.getCause()) {
            if (current instanceof DdlException) {
                return (DdlException) current;
            }
        }
        fail("Expected DdlException in cause chain of " + thrown);
        return null; // unreachable
    }

    /**
     * Fetch the single unfinished rollup job the just-run ALTER constructed, then clear the handler's
     * job map so it is not picked up by the (mocked-out) background loop and does not leak into other
     * tests. Mirrors {@code LakeRollupJobTest.createJob}.
     */
    private static AlterJobV2 fetchAndClearRollupJob() {
        MaterializedViewHandler handler = GlobalStateMgr.getCurrentState().getRollupHandler();
        List<AlterJobV2> jobs = new java.util.ArrayList<>(handler.getAlterJobsV2().values());
        handler.clearJobs();
        assertEquals(1, jobs.size(), "expected exactly one rollup job to be constructed");
        return jobs.get(0);
    }

    /**
     * On a shared-data (lake) range DUP table, {@code ADD ROLLUP ... ORDER BY (...)} is now allowed:
     * the handler constructs a {@link LakeRangeRollupJob} (the additive range-rollup path) instead of
     * rejecting it. The job is constructed (not run); we inspect its routing then clear it.
     */
    @Test
    public void testLakeRangeRollupAllowed() throws Exception {
        starRocksAssert.withTable(rangeTableDdl("t_guard_rollup"));
        starRocksAssert.alterTable(
                "alter table t_guard_rollup add rollup r2(k1, k2, v1) order by (k2, k1)");
        AlterJobV2 job = fetchAndClearRollupJob();
        assertTrue(job instanceof LakeRangeRollupJob,
                "Expected a LakeRangeRollupJob, got: " + job);
        assertEquals("r2", ((LakeRangeRollupJob) job).getRollupIndexName());
    }

    /**
     * The constructed rollup's schema is stored in SORT-KEY order ({@code [k2, k1, v1]} for ORDER BY
     * (k2, k1) over selected (k1, k2, v1)) with the sort-key columns as the leading prefix
     * ({@code sortKeyIdxes == [0, 1]}). This makes the optimizer's prefix-index scorer (which scores by
     * schema column order) select the rollup for {@code WHERE k2 = ...}.
     */
    @Test
    public void testLakeRangeRollupSchemaStoredInSortKeyOrder() throws Exception {
        starRocksAssert.withTable(rangeTableDdl("t_guard_rollup_order"));
        starRocksAssert.alterTable(
                "alter table t_guard_rollup_order add rollup r_sk(k1, k2, v1) order by (k2, k1)");
        LakeRangeRollupJob job = (LakeRangeRollupJob) fetchAndClearRollupJob();

        List<Column> rollupSchema = Deencapsulation.getField(job, "rollupSchema");
        List<String> schemaNames = rollupSchema.stream().map(Column::getName).collect(
                java.util.stream.Collectors.toList());
        assertEquals(List.of("k2", "k1", "v1"), schemaNames, "rollup schema must be in sort-key order");
        // The sort-key columns are the leading prefix and are keys; the rest are values.
        assertTrue(rollupSchema.get(0).isKey() && rollupSchema.get(1).isKey());
        assertTrue(!rollupSchema.get(2).isKey());

        List<Integer> sortKeyIdxes = Deencapsulation.getField(job, "rollupSortKeyIdxes");
        assertEquals(List.of(0, 1), sortKeyIdxes, "sort key must be the leading schema prefix");
    }

    /**
     * A shared-nothing range table rollup stays rejected: the additive range-rollup path is
     * shared-data only (it builds a K-tablet shadow via the lake online-rewrite machinery), so the
     * routing predicate requires {@code isCloudNativeTable()}. This is asserted on the predicate
     * directly because a SHARED_DATA test cluster cannot host a genuine shared-nothing OLAP table.
     */
    @Test
    public void testSharedNothingRangeRollupRejected() throws Exception {
        starRocksAssert.withTable(rangeTableDdl("t_guard_rollup_sn"));
        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState()
                .getLocalMetastore().getTable("test", "t_guard_rollup_sn");
        // Sanity: the real (cloud-native) range table IS routable.
        assertTrue(MaterializedViewHandler.isRangeRollupRoutable(table));
        // A shared-nothing table (isCloudNativeTable() == false) is NOT routable -> stays rejected.
        new MockUp<OlapTable>() {
            @Mock
            public boolean isCloudNativeTable() {
                return false;
            }
        };
        assertTrue(!MaterializedViewHandler.isRangeRollupRoutable(table),
                "shared-nothing range table must not be routed to LakeRangeRollupJob");
    }

    /**
     * A colocate range table rollup stays rejected (parallel to the sort-key-reorder colocate
     * rejection): the rollup would build a fresh K-tablet layout independent of the colocate range
     * manager's expected ranges.
     */
    @Test
    public void testRangeRollupOnColocateRejected() throws Exception {
        starRocksAssert.withTable(rangeTableDdl("t_guard_rollup_colocate"));
        new MockUp<ColocateTableIndex>() {
            @Mock
            public boolean isColocateTable(long tableId) {
                return true;
            }
        };
        assertAlterRejectedWithRangeDistribution(
                "alter table t_guard_rollup_colocate add rollup r1(k1, k2, v1) order by (k2, k1)");
    }

    /**
     * A range table carrying an AUTO_INCREMENT column stays rejected (out of scope for the rollup
     * re-route, mirroring the sort-key-reorder auto-increment rejection).
     */
    @Test
    public void testRangeRollupWithAutoIncrementRejected() throws Exception {
        starRocksAssert.withTable("create table t_guard_rollup_autoinc "
                + "(k1 int not null, k2 int not null, id bigint not null auto_increment)\n"
                + "duplicate key(k1, k2)\n"
                + "order by(k1, k2)\n"
                + "properties('replication_num' = '1');");
        assertAlterRejectedWithRangeDistribution(
                "alter table t_guard_rollup_autoinc add rollup r1(k1, k2) order by (k2, k1)");
    }

    /**
     * A PRIMARY KEY range table rollup stays rejected. ADD ROLLUP on a PK table is rejected up front
     * by {@code process} ("Do not support add rollup on primary key table"); the range-rollup guard
     * additionally excludes PRIMARY_KEYS. Either way the ALTER must throw.
     */
    @Test
    public void testRangeRollupOnPrimaryKeyRejected() throws Exception {
        starRocksAssert.withTable("create table t_guard_rollup_pk "
                + "(k1 int not null, k2 int not null, v1 int)\n"
                + "primary key(k1, k2)\n"
                + "order by(k1, k2)\n"
                + "properties('replication_num' = '1');");
        assertThrows(Throwable.class, () ->
                starRocksAssert.alterTable(
                        "alter table t_guard_rollup_pk add rollup r1(k1, k2) order by (k2, k1)"));
    }

    /**
     * Force-drop / CANCEL must recognize an in-flight {@link LakeRangeRollupJob} by its rollup index
     * name (parallel to {@code RollupJobV2}/{@code LakeRollupJob}). The job is constructed by the
     * ALTER, then {@code cancelRollupJobsForForceDrop} finds and cancels it.
     */
    @Test
    public void testCancelRecognizesLakeRangeRollupJob() throws Exception {
        starRocksAssert.withTable(rangeTableDdl("t_guard_rollup_cancel"));
        MaterializedViewHandler handler = GlobalStateMgr.getCurrentState().getRollupHandler();
        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState()
                .getLocalMetastore().getTable("test", "t_guard_rollup_cancel");
        try {
            starRocksAssert.alterTable(
                    "alter table t_guard_rollup_cancel add rollup r_cancel(k1, k2, v1) order by (k2, k1)");
            // The job is registered (unfinished) under the table id; cancel must match it by name.
            boolean cancelled = handler.cancelRollupJobsForForceDrop(table.getId(), "r_cancel", "test");
            assertTrue(cancelled, "cancelRollupJobsForForceDrop must recognize the LakeRangeRollupJob");
            // A non-matching name must NOT be recognized.
            assertTrue(!handler.cancelRollupJobsForForceDrop(table.getId(), "no_such_rollup", "test"));
        } finally {
            handler.clearJobs();
        }
    }

    @Test
    public void testSyncCreateMaterializedViewRejectedOnRangeDistribution() throws Exception {
        starRocksAssert.withTable(rangeTableDdl("t_guard_syncmv"));
        DdlException exception = assertThrowsDdlException(() ->
                starRocksAssert.withMaterializedView(
                        "create materialized view mv_guard_sync as " +
                        "select k1, v1 from t_guard_syncmv"));
        assertTrue(exception.getMessage().toLowerCase(Locale.ROOT).contains("range distribution"),
                "Expected 'range distribution' in: " + exception.getMessage());
    }

    /**
     * On a shared-data (lake) range table a sort-key reorder shifts the range sort key, so it is now
     * routed to {@link LakeRangeRewriteSchemaChangeJob} instead of being rejected. The job is built
     * (not run) via {@code analyzeAndCreateJob} to assert the routing without needing a backend.
     */
    @Test
    public void testModifySortKeyRoutedToRangeRewriteOnRangeDistribution() throws Exception {
        starRocksAssert.withTable(rangeTableDdl("t_guard_orderby"));
        // Column list SHORTER than base schema routes to processModifySortKeyColumn (sort-key modify).
        com.starrocks.sql.ast.AlterTableStmt stmt = (com.starrocks.sql.ast.AlterTableStmt)
                UtFrameUtils.parseStmtWithNewParser("alter table t_guard_orderby order by (k1)", connectContext);
        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState()
                .getLocalMetastore().getTable("test", "t_guard_orderby");
        AlterJobV2 job = GlobalStateMgr.getCurrentState().getSchemaChangeHandler()
                .analyzeAndCreateJob(stmt.getAlterClauseList(),
                        GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test"), table);
        assertTrue(job instanceof LakeRangeRewriteSchemaChangeJob,
                "Expected a LakeRangeRewriteSchemaChangeJob, got: " + job);
    }

    @Test
    public void testOptimizeRejectedOnRangeDistribution() throws Exception {
        starRocksAssert.withTable(rangeTableDdl("t_guard_optimize"));
        // OPTIMIZE is triggered by `alter table ... distributed by ...` —
        // there is no standalone OPTIMIZE keyword in the grammar. The
        // analyzer raises SemanticException, but
        // UtFrameUtils.parseStmtWithNewParser wraps it as AnalysisException,
        // so we match on Throwable + message rather than the concrete type.
        Throwable exception = assertThrows(Throwable.class, () ->
                starRocksAssert.alterTable(
                        "alter table t_guard_optimize distributed by hash(k1)"));
        assertTrue(exception.getMessage().toLowerCase(Locale.ROOT).contains("range distribution"),
                "Expected 'range distribution' in: " + exception.getMessage());
    }

    @Test
    public void testAddKeyColumnRejectedOnRangeDistribution() throws Exception {
        // Range table (order by(k1, k2)). Adding a key column would otherwise
        // append to the sort key, invalidating stored range tablet boundaries.
        starRocksAssert.withTable(rangeTableDdl("t_guard_addkey"));
        Throwable exception = assertThrows(Throwable.class, () ->
                starRocksAssert.alterTable(
                        "alter table t_guard_addkey add column k_new int key default '0'"));
        assertTrue(exception.getMessage().toLowerCase(Locale.ROOT).contains("range distribution"),
                "Expected 'range distribution' in: " + exception.getMessage());
    }

    /**
     * A range-COLOCATE table stays rejected: the rewrite would resample a fresh K-tablet layout
     * independently of the colocate range manager's expected ranges, desyncing colocate routing after
     * the flip. The routing predicate must therefore exclude colocate tables, keeping the existing
     * range-distribution rejection. (Colocate membership is simulated; create-time colocate+range
     * wiring is orthogonal to the routing decision under test.)
     */
    @Test
    public void testColocateRangeTableRejectsSortKeyReorder() throws Exception {
        starRocksAssert.withTable(rangeTableDdl("t_guard_colocate"));
        new MockUp<ColocateTableIndex>() {
            @Mock
            public boolean isColocateTable(long tableId) {
                return true;
            }
        };
        assertAlterRejectedWithRangeDistribution("alter table t_guard_colocate order by (k2, k1)");
    }

    /**
     * A range table carrying an AUTO_INCREMENT column stays rejected (out of scope for the re-route).
     */
    @Test
    public void testAutoIncrementRangeTableRejectsSortKeyReorder() throws Exception {
        starRocksAssert.withTable("create table t_guard_autoinc "
                + "(k1 int not null, k2 int not null, id bigint not null auto_increment)\n"
                + "duplicate key(k1, k2)\n"
                + "order by(k1, k2)\n"
                + "properties('replication_num' = '1');");
        assertAlterRejectedWithRangeDistribution("alter table t_guard_autoinc order by (k2, k1)");
    }

    /**
     * On an AGG range-distribution table, adding a column with no aggregate
     * function is auto-promoted to a key column by the schema-change handler
     * (no explicit KEY keyword needed). The guard must catch this path too;
     * an earlier draft checked isKey() before promotion and let this through.
     */
    @Test
    public void testAddNoAggregateColumnOnAggRangeTableRejected() throws Exception {
        starRocksAssert.withTable(
                "create table t_guard_addagg (k1 int, v1 int sum)\n" +
                "AGGREGATE KEY(k1)\n" +
                "order by(k1)\n" +
                "properties('replication_num' = '1');");
        // No KEY keyword and no aggregate -> AGG promotion turns this into a key.
        Throwable exception = assertThrows(Throwable.class, () ->
                starRocksAssert.alterTable(
                        "alter table t_guard_addagg add column c_promoted int default '0'"));
        assertTrue(exception.getMessage().toLowerCase(Locale.ROOT).contains("range distribution"),
                "Expected 'range distribution' in: " + exception.getMessage());
    }

    @Test
    public void testDropSortKeyColumnRejectedOnRangeDistribution() throws Exception {
        // DUP range table: k1, k2 are both sort/key columns.
        starRocksAssert.withTable(rangeTableDdl("t_guard_dropsk"));
        Throwable exception = assertThrows(Throwable.class, () ->
                starRocksAssert.alterTable(
                        "alter table t_guard_dropsk drop column k2"));
        assertTrue(exception.getMessage().toLowerCase(Locale.ROOT).contains("range distribution"),
                "Expected 'range distribution' in: " + exception.getMessage());
        assertTrue(exception.getMessage().toLowerCase(Locale.ROOT).contains("k2"),
                "Expected 'k2' (offending column) in: " + exception.getMessage());
    }

    @Test
    public void testModifySortKeyColumnTypeRejectedOnRangeDistribution() throws Exception {
        starRocksAssert.withTable(rangeTableDdl("t_guard_modsk"));
        Throwable exception = assertThrows(Throwable.class, () ->
                starRocksAssert.alterTable(
                        "alter table t_guard_modsk modify column k1 bigint"));
        assertTrue(exception.getMessage().toLowerCase(Locale.ROOT).contains("range distribution"),
                "Expected 'range distribution' in: " + exception.getMessage());
        assertTrue(exception.getMessage().toLowerCase(Locale.ROOT).contains("k1"),
                "Expected 'k1' (offending column) in: " + exception.getMessage());
    }

    @Test
    public void testModifyKeynessFlipRejectedOnRangeDistribution() throws Exception {
        // AGG table so that a value column can be promoted via MODIFY ... KEY.
        starRocksAssert.withTable(
                "create table t_guard_keyflip (k1 int, v1 int sum)\n" +
                "AGGREGATE KEY(k1)\n" +
                "order by(k1)\n" +
                "properties('replication_num' = '1');");
        Throwable exception = assertThrows(Throwable.class, () ->
                starRocksAssert.alterTable(
                        "alter table t_guard_keyflip modify column v1 int key"));
        assertTrue(exception.getMessage().toLowerCase(Locale.ROOT).contains("range distribution"),
                "Expected 'range distribution' in: " + exception.getMessage());
        assertTrue(exception.getMessage().toLowerCase(Locale.ROOT).contains("keyness")
                        || exception.getMessage().toLowerCase(Locale.ROOT).contains("key column"),
                "Expected keyness/key-column language in: " + exception.getMessage());
    }

    /**
     * Positive narrowness test: MODIFY of a non-sort-key, non-key column with
     * no keyness change must still succeed on a range-distribution table.
     *
     * The explicit {@code DUPLICATE KEY(k1, k2)} keeps v1 unambiguously a
     * value column; without it, short-key derivation in
     * {@code CreateTableAnalyzer.analyzeKeysDesc} would pick all three int
     * columns as keys (they fit under {@code SHORTKEY_MAXSIZE_BYTES}) and
     * MODIFY would become a keyness flip.
     */
    @Test
    public void testModifyNonSortKeyColumnAllowedOnRangeDistribution() throws Exception {
        starRocksAssert.withTable(
                "create table t_guard_modok (k1 int, k2 int, v1 int)\n" +
                "DUPLICATE KEY(k1, k2)\n" +
                "order by(k1, k2)\n" +
                "properties('replication_num' = '1');");
        starRocksAssert.alterTable(
                "alter table t_guard_modok modify column v1 bigint");
    }

    @Test
    public void testWidenVarcharSortKeyColumnAllowedOnRangeDistribution() throws Exception {
        starRocksAssert.withTable(varcharSortKeyRangeTableDdl("t_guard_widen"));

        // VARCHAR length increase on a sort-key column: allowed (must not throw).
        starRocksAssert.alterTable(
                "alter table t_guard_widen modify column k1 varchar(40) KEY NOT NULL");

        // The synchronous FSE path applies the new length in place; verify it.
        Column k1 = baseColumn("t_guard_widen", "k1");
        assertEquals(PrimitiveType.VARCHAR, k1.getPrimitiveType());
        assertEquals(40, k1.getStrLen());
    }

    @Test
    public void testShrinkVarcharSortKeyColumnRejectedOnRangeDistribution() throws Exception {
        starRocksAssert.withTable(varcharSortKeyRangeTableDdl("t_guard_shrink"));
        assertAlterRejectedWithRangeDistribution(
                "alter table t_guard_shrink modify column k1 varchar(5) KEY NOT NULL");
    }

    @Test
    public void testNonVarcharTypeChangeSortKeyColumnRejectedOnRangeDistribution() throws Exception {
        starRocksAssert.withTable(
                "create table t_guard_int (k1 int NOT NULL, k2 int NOT NULL, v1 int)\n" +
                "DUPLICATE KEY(k1, k2)\n" +
                "order by(k1, k2)\n" +
                "properties('replication_num' = '1');");
        assertAlterRejectedWithRangeDistribution(
                "alter table t_guard_int modify column k1 bigint KEY NOT NULL");
    }

    /**
     * With FSE v2 off the widen no longer takes the synchronous in-place path; it is
     * routed to the asynchronous fast-schema-change job instead. The widen keeps the
     * boundary tuples byte-identical, so it is range-correct on that path too and must
     * be allowed — it behaves exactly like a VARCHAR widen on a non-range table. The
     * call succeeding (no DdlException) is the assertion; the async job applies the new
     * length later, so the length is not checked here.
     */
    @Test
    public void testVarcharWidenAllowedWhenFastSchemaEvolutionV2Disabled() throws Exception {
        starRocksAssert.withTable(
                "create table t_guard_nov2 (k1 varchar(20) NOT NULL, k2 int NOT NULL, v1 int)\n" +
                "DUPLICATE KEY(k1, k2)\n" +
                "order by(k1, k2)\n" +
                "properties('replication_num' = '1',\n" +
                "           'cloud_native_fast_schema_evolution_v2' = 'false');");
        starRocksAssert.alterTable(
                "alter table t_guard_nov2 modify column k1 varchar(40) KEY NOT NULL");
    }

    @Test
    public void testVarcharWidenOfNonSortKeyValueColumnUnaffectedOnRangeDistribution() throws Exception {
        starRocksAssert.withTable(
                "create table t_guard_val (k1 int NOT NULL, k2 int NOT NULL, v1 varchar(20))\n" +
                "DUPLICATE KEY(k1, k2)\n" +
                "order by(k1, k2)\n" +
                "properties('replication_num' = '1');");
        // v1 is NOT in the sort key, so the guard never applied; widening it stays allowed.
        starRocksAssert.alterTable(
                "alter table t_guard_val modify column v1 varchar(40)");
        assertEquals(40, baseColumn("t_guard_val", "v1").getStrLen());
    }

    @Test
    public void testVarcharWidenWithColumnRepositionRejectedOnRangeDistribution() throws Exception {
        starRocksAssert.withTable(varcharSortKeyRangeTableDdl("t_guard_after"));
        // A widen that ALSO repositions the sort-key column (AFTER) reorders the
        // sort key and would invalidate boundaries -> must stay rejected.
        assertAlterRejectedWithRangeDistribution(
                "alter table t_guard_after modify column k1 varchar(40) KEY NOT NULL AFTER k2");
    }

    @Test
    public void testVarcharWidenRejectedWhenGlobalShareDataFseDisabled() throws Exception {
        boolean saved = Config.enable_fast_schema_evolution_in_share_data_mode;
        Config.enable_fast_schema_evolution_in_share_data_mode = false;
        try {
            starRocksAssert.withTable(varcharSortKeyRangeTableDdl("t_guard_nogfse"));
            assertAlterRejectedWithRangeDistribution(
                    "alter table t_guard_nogfse modify column k1 varchar(40) KEY NOT NULL");
        } finally {
            Config.enable_fast_schema_evolution_in_share_data_mode = saved;
        }
    }

    @Test
    public void testVarcharWidenThatAlsoFlipsKeynessRejectedOnRangeDistribution() throws Exception {
        starRocksAssert.withTable(varcharSortKeyRangeTableDdl("t_guard_flip"));
        // Widen k1 but DROP the KEY attribute: this is a keyness flip, not a pure
        // widen, so the relaxation must not apply and it must stay rejected.
        assertAlterRejectedWithRangeDistribution(
                "alter table t_guard_flip modify column k1 varchar(40)");
    }

    /**
     * Inject a FINISHED range-rollup index directly into the table (simulating the post-FINISHED catalog
     * state), then {@code ALTER TABLE ... DROP ROLLUP} and assert the rollup meta + index are removed
     * while the base index is entirely untouched.
     *
     * <p>This is the regression for the generic meta-id deletion path
     * ({@code MaterializedViewHandler.java#processBatchDropRollup}): a {@link LakeRangeRollupJob} that
     * finished and promoted its shadow to NORMAL must be removable by the standard DROP ROLLUP DDL.
     */
    @Test
    public void testFinishedRangeRollupCanBeDropped() throws Exception {
        starRocksAssert.withTable(rangeTableDdl("t_guard_drop_finished"));
        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState()
                .getLocalMetastore().getTable("test", "t_guard_drop_finished");
        long baseMetaId = table.getBaseIndexMetaId();

        // Inject a FINISHED rollup: register its meta then add a NORMAL MaterializedIndex
        // to every physical partition (mirrors what visualiseShadowIndex does after FINISHED).
        long rollupMetaId = GlobalStateMgr.getCurrentState().getNextId();
        List<Column> rollupSchema = table.getSchemaByIndexMetaId(baseMetaId).subList(0, 2);
        table.setIndexMeta(rollupMetaId, "r_finished", rollupSchema, 0, 0, (short) 1,
                TStorageType.COLUMN, KeysType.DUP_KEYS, null, List.of(0, 1));
        for (PhysicalPartition pp : table.getPhysicalPartitions()) {
            long physId = GlobalStateMgr.getCurrentState().getNextId();
            MaterializedIndex rollupIdx = new MaterializedIndex(physId, rollupMetaId,
                    MaterializedIndex.IndexState.NORMAL,
                    com.starrocks.catalog.PhysicalPartition.INVALID_SHARD_GROUP_ID);
            pp.createRollupIndex(rollupIdx);
        }
        assertTrue(table.hasMaterializedIndex("r_finished"),
                "rollup must be present after injection");
        assertNotNull(table.getIndexMetaByMetaId(rollupMetaId),
                "rollup meta must be registered");

        // DROP ROLLUP via the standard DDL path.
        starRocksAssert.alterTable("alter table t_guard_drop_finished drop rollup r_finished");

        // Rollup index meta and physical indexes must be gone.
        assertFalse(table.hasMaterializedIndex("r_finished"),
                "rollup must be removed after DROP ROLLUP");
        // The base index meta id and meta must be untouched.
        assertEquals(baseMetaId, table.getBaseIndexMetaId(),
                "base meta id must not change after DROP ROLLUP");
        assertNotNull(table.getIndexMetaByMetaId(baseMetaId),
                "base meta must remain after DROP ROLLUP");
        for (PhysicalPartition pp : table.getPhysicalPartitions()) {
            MaterializedIndex base = pp.getLatestIndex(baseMetaId);
            assertNotNull(base, "base physical index must remain in every partition");
        }
    }

    /**
     * An in-flight {@link LakeRangeRollupJob} (shadow index in SHADOW state) is cancelled via
     * {@code CANCEL ALTER TABLE MATERIALIZED VIEW <name>}: the job transitions to CANCELLED and the
     * base index is untouched throughout.
     *
     * <p>The existing {@link #testCancelRecognizesLakeRangeRollupJob()} checks that
     * {@link MaterializedViewHandler#cancelRollupJobsForForceDrop} matches the job by rollup name.
     * This test additionally verifies that after cancel the job is in CANCELLED state and the base
     * index remains fully intact, covering the regression that cancel must not damage the base.
     */
    @Test
    public void testInFlightRangeRollupCancelLeavesBaseIntact() throws Exception {
        starRocksAssert.withTable(rangeTableDdl("t_guard_cancel_base"));
        MaterializedViewHandler handler = GlobalStateMgr.getCurrentState().getRollupHandler();
        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState()
                .getLocalMetastore().getTable("test", "t_guard_cancel_base");
        long baseMetaId = table.getBaseIndexMetaId();
        MaterializedIndexMeta baseMeta = table.getIndexMetaByMetaId(baseMetaId);
        assertNotNull(baseMeta, "base meta must be present before cancel");

        try {
            starRocksAssert.alterTable(
                    "alter table t_guard_cancel_base add rollup r_inflight(k1, k2, v1) order by (k2, k1)");
            AlterJobV2 job = handler.getAlterJobsV2().values().iterator().next();
            assertTrue(job instanceof LakeRangeRollupJob,
                    "constructed job must be a LakeRangeRollupJob");
            // Cancel via the MATERIALIZED VIEW cancel path (same as cancelMV, routes by rollup name).
            // cancelRollupJobsForForceDrop drives the cancel path the production force-drop uses.
            boolean cancelled = handler.cancelRollupJobsForForceDrop(
                    table.getId(), "r_inflight", "test cancel");
            assertTrue(cancelled, "cancel must succeed for an in-flight LakeRangeRollupJob");
            assertEquals(AlterJobV2.JobState.CANCELLED, job.getJobState(),
                    "job must be in CANCELLED state after cancel");
            // Base index must be untouched.
            assertEquals(baseMetaId, table.getBaseIndexMetaId(),
                    "base meta id must not change after cancel");
            assertNotNull(table.getIndexMetaByMetaId(baseMetaId),
                    "base meta must remain after cancel");
            for (PhysicalPartition pp : table.getPhysicalPartitions()) {
                assertNotNull(pp.getLatestIndex(baseMetaId),
                        "base physical index must remain in every partition after cancel");
            }
            // Table state must return to NORMAL after cancel.
            assertEquals(OlapTable.OlapTableState.NORMAL, table.getState(),
                    "table state must be NORMAL after cancel");
        } finally {
            handler.clearJobs();
        }
    }
}
