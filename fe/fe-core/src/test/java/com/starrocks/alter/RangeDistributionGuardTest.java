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
import com.starrocks.catalog.Database;
import com.starrocks.catalog.DefaultExpr;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.MaterializedIndexMeta;
import com.starrocks.catalog.NullVariant;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.SchemaInfo;
import com.starrocks.catalog.Tablet;
import com.starrocks.catalog.TabletRange;
import com.starrocks.catalog.Tuple;
import com.starrocks.catalog.Variant;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.Range;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.sql.ast.KeysType;
import com.starrocks.thrift.TStorageType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.PrimitiveType;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import java.util.ArrayList;
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
     * (no explicit KEY keyword needed). The added column joins the (key-derived)
     * range sort key -- a trailing sort-key add -- so it now runs the metadata-only
     * async schema-evolution job (the routing predicate's auto-promote handling must
     * catch this path too; an earlier draft checked isKey() before promotion and
     * would have missed it).
     */
    @Test
    public void testAddNoAggregateColumnOnAggRangeTableRunsMetadataOnly() throws Exception {
        starRocksAssert.withTable(
                "create table t_guard_addagg (k1 int, v1 int sum)\n" +
                "AGGREGATE KEY(k1)\n" +
                "order by(k1)\n" +
                "properties('replication_num' = '1');");
        // No KEY keyword and no aggregate -> AGG promotion turns this into a key.
        com.starrocks.sql.ast.AlterTableStmt stmt = (com.starrocks.sql.ast.AlterTableStmt)
                UtFrameUtils.parseStmtWithNewParser(
                        "alter table t_guard_addagg add column c_promoted int default '0'", connectContext);
        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState()
                .getLocalMetastore().getTable("test", "t_guard_addagg");
        AlterJobV2 job = GlobalStateMgr.getCurrentState().getSchemaChangeHandler()
                .analyzeAndCreateJob(stmt.getAlterClauseList(),
                        GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test"), table);
        assertMetadataOnlyJob(job);
    }

    /**
     * On a shared-data (lake) range DUP table, DROP of a range sort-key column is now allowed: the
     * handler routes it to {@link LakeRangeRewriteSchemaChangeJob} instead of rejecting it (see
     * {@code RangeKeyColumnRoutingTest} for the full DUP/AGG/PK/UNIQUE routing matrix). The job is built
     * (not run) via {@code analyzeAndCreateJob} to assert routing without needing a backend.
     */
    @Test
    public void testDropSortKeyColumnRoutedToRangeRewriteOnRangeDistribution() throws Exception {
        // DUP range table: k1, k2 are both sort/key columns.
        starRocksAssert.withTable(rangeTableDdl("t_guard_dropsk"));
        com.starrocks.sql.ast.AlterTableStmt stmt = (com.starrocks.sql.ast.AlterTableStmt)
                UtFrameUtils.parseStmtWithNewParser("alter table t_guard_dropsk drop column k2", connectContext);
        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState()
                .getLocalMetastore().getTable("test", "t_guard_dropsk");
        AlterJobV2 job = GlobalStateMgr.getCurrentState().getSchemaChangeHandler()
                .analyzeAndCreateJob(stmt.getAlterClauseList(),
                        GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test"), table);
        assertTrue(job instanceof LakeRangeRewriteSchemaChangeJob,
                "Expected a LakeRangeRewriteSchemaChangeJob, got: " + job);
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

    /**
     * A range DUP rollup may declare a DUPLICATE KEY that is a strict prefix of its ORDER BY (sort key),
     * mirroring CreateTableAnalyzer's exemption for DUP base tables. The existing
     * {@code checkAndPrepareMaterializedView} requires the DUPLICATE KEY to be a prefix of the rollup
     * column list, so the column list leads with {@code k2}. For
     * {@code ADD ROLLUP r(k2, k1, v1) DUPLICATE KEY(k2) ORDER BY(k2, k1)} the built schema is stored in
     * sort-key order {@code [k2, k1, v1]} but only {@code k2} (the declared DUPLICATE KEY) is a key; the
     * trailing sort-key column {@code k1} is a value (AggregateType.NONE). Without this fix the rollup
     * path would have widened the key set to every ORDER BY column. The sort key still spans the full
     * ORDER BY {@code (k2, k1)} so storage order is unchanged.
     */
    @Test
    public void testRangeDupRollupHonorsNarrowerDuplicateKey() throws Exception {
        starRocksAssert.withTable(rangeTableDdl("t_guard_dupkey"));
        starRocksAssert.alterTable(
                "alter table t_guard_dupkey add rollup r_dup(k2, k1, v1) duplicate key(k2) order by (k2, k1)");
        LakeRangeRollupJob job = (LakeRangeRollupJob) fetchAndClearRollupJob();

        List<Column> rollupSchema = Deencapsulation.getField(job, "rollupSchema");
        List<String> schemaNames = rollupSchema.stream().map(Column::getName).collect(
                java.util.stream.Collectors.toList());
        assertEquals(List.of("k2", "k1", "v1"), schemaNames, "rollup schema must be in sort-key order");
        // Only the declared DUPLICATE KEY (k2) is a key; k1 (trailing sort-key col) and v1 are values.
        assertTrue(rollupSchema.get(0).isKey(), "k2 (the DUPLICATE KEY) must be a key");
        assertFalse(rollupSchema.get(1).isKey(), "k1 must NOT be a key (DUPLICATE KEY is a strict prefix)");
        assertFalse(rollupSchema.get(2).isKey(), "v1 must NOT be a key");
        assertEquals(com.starrocks.sql.ast.AggregateType.NONE, rollupSchema.get(1).getAggregationType(),
                "non-key k1 must be a value column (AggregateType.NONE)");

        // The sort key still spans the FULL ORDER BY (k2, k1): storage order is unchanged.
        List<Integer> sortKeyIdxes = Deencapsulation.getField(job, "rollupSortKeyIdxes");
        assertEquals(List.of(0, 1), sortKeyIdxes, "sort key must span both ORDER BY columns (k2, k1)");
    }

    /**
     * A range-distribution table supports at most one routable rollup per ALTER TABLE statement (the
     * additive path promotes its shadow to NORMAL directly and cannot share a multi-job ROLLUP-state
     * batch). A batch with two ADD ROLLUP clauses must be rejected up front.
     */
    @Test
    public void testRangeRollupBatchWithMultipleClausesRejected() throws Exception {
        starRocksAssert.withTable(rangeTableDdl("t_guard_rollup_batch"));
        // A single ADD ROLLUP may list multiple rollupItems (comma-separated), each becoming its own
        // AddRollupClause: "add rollup r1(...) ..., r2(...) ...".
        DdlException exception = assertThrowsDdlException(() ->
                starRocksAssert.alterTable(
                        "alter table t_guard_rollup_batch "
                                + "add rollup r1(k1, k2, v1) order by (k2, k1), "
                                + "r2(k1, k2, v1) order by (k1, k2)"));
        assertTrue(exception.getMessage().toLowerCase(Locale.ROOT).contains("at most one rollup"),
                "Expected 'at most one rollup' in: " + exception.getMessage());
        // No job should have been constructed.
        GlobalStateMgr.getCurrentState().getRollupHandler().clearJobs();
    }

    /**
     * A range table that already carries a secondary index/rollup ({@code getIndexMetaIdToMeta().size() > 1})
     * is no longer routable to {@link LakeRangeRollupJob}: it falls through to the existing range rejection
     * in {@code createMaterializedViewJob}. Asserted on the routing predicate directly (and via a real
     * ALTER) since the additive path's shadow promotion bypasses the multi-job ROLLUP coordination.
     */
    @Test
    public void testRangeRollupWithExistingRollupNotRouted() throws Exception {
        starRocksAssert.withTable(rangeTableDdl("t_guard_rollup_existing"));
        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState()
                .getLocalMetastore().getTable("test", "t_guard_rollup_existing");
        // Sanity: a fresh range table (only the base index) IS routable.
        assertTrue(MaterializedViewHandler.isRangeRollupRoutable(table));

        // Inject a second index meta so the table now has two indexes.
        long baseMetaId = table.getBaseIndexMetaId();
        long extraMetaId = GlobalStateMgr.getCurrentState().getNextId();
        List<Column> rollupSchema = table.getSchemaByIndexMetaId(baseMetaId).subList(0, 2);
        table.setIndexMeta(extraMetaId, "r_existing", rollupSchema, 0, 0, (short) 1,
                TStorageType.COLUMN, KeysType.DUP_KEYS, null, List.of(0, 1));

        // With a second index present the table is no longer routable.
        assertFalse(MaterializedViewHandler.isRangeRollupRoutable(table),
                "a range table with an existing rollup must not be routed to LakeRangeRollupJob");
        // A real ADD ROLLUP now falls through to the existing range rejection.
        assertAlterRejectedWithRangeDistribution(
                "alter table t_guard_rollup_existing add rollup r_new(k1, k2, v1) order by (k2, k1)");
    }

    // ------------------------------------------------------------------------------------------
    // SchemaChangeHandler.isMetadataOnlyTrailingKeyAdd / resolveEffectiveSortKeyColumns
    //
    // These are additive, self-contained pieces (not wired into the ALTER dispatch yet), so they are
    // exercised as pure functions over a hand-built resolved SchemaChangeData rather than by driving a
    // real ADD COLUMN KEY through analyzeAndCreateJob (which today still routes to the existing
    // needsRangeRewriteSchemaChange path before finalAnalyze ever runs).
    // ------------------------------------------------------------------------------------------

    private static String dupRangeTableWithValueDdl(String name) {
        return "create table " + name + " (k1 int, k2 int, v1 int)\n" +
                "DUPLICATE KEY(k1, k2)\n" +
                "order by(k1, k2)\n" +
                "properties('replication_num' = '1');";
    }

    private static String aggRangeTableWithValueDdl(String name) {
        return "create table " + name + " (k1 int, k2 int, v1 int sum)\n" +
                "AGGREGATE KEY(k1, k2)\n" +
                "order by(k1, k2)\n" +
                "properties('replication_num' = '1');";
    }

    private static String uniqueRangeTableWithValueDdl(String name) {
        return "create table " + name + " (k1 int, k2 int, v1 int)\n" +
                "UNIQUE KEY(k1, k2)\n" +
                "order by(k1, k2)\n" +
                "properties('replication_num' = '1');";
    }

    private static String primaryKeyRangeTableWithValueDdl(String name) {
        return "create table " + name + " (k1 int not null, k2 int not null, v1 int)\n" +
                "PRIMARY KEY(k1, k2)\n" +
                "order by(k1, k2)\n" +
                "properties('replication_num' = '1');";
    }

    private static OlapTable getTable(String name) {
        return (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable("test", name);
    }

    private static Column constKeyColumn(OlapTable table, String name) {
        Column column = new Column(name, IntegerType.INT);
        column.setIsKey(true);
        column.setUniqueId(table.getMaxColUniqueId() + 1000);
        column.setDefaultValue("0");
        return column;
    }

    /**
     * Build a resolved {@link SchemaChangeData} simulating {@code ADD COLUMN <newKeyColumn> KEY} on
     * {@code table}: the base index schema with {@code newKeyColumn} inserted as a brand-new trailing
     * key column (after the existing key columns, keeping the key set a contiguous prefix), and a
     * candidate sort key that is the table's current effective sort key plus that new column,
     * expressed via {@code sortKeyUniqueIds} -- the resolver's highest-precedence tier, so this is
     * valid regardless of which tier the live table itself uses.
     */
    private static SchemaChangeData buildTrailingKeyAddCandidate(OlapTable table, Column newKeyColumn) {
        long baseIndexMetaId = table.getBaseIndexMetaId();
        List<Column> oldSchema = table.getSchemaByIndexMetaId(baseIndexMetaId);
        MaterializedIndexMeta oldIndexMeta = table.getIndexMetaByMetaId(baseIndexMetaId);
        List<Column> oldSortKey = SchemaChangeHandler.resolveEffectiveSortKeyColumns(
                oldSchema, oldIndexMeta.getSortKeyUniqueIds(), oldIndexMeta.getSortKeyIdxes());

        List<Column> newSchema = new ArrayList<>();
        for (Column column : oldSchema) {
            if (column.isKey()) {
                newSchema.add(column);
            }
        }
        newSchema.add(newKeyColumn);
        for (Column column : oldSchema) {
            if (!column.isKey()) {
                newSchema.add(column);
            }
        }

        List<Integer> candidateSortKeyUniqueIds = new ArrayList<>();
        for (Column column : oldSortKey) {
            candidateSortKeyUniqueIds.add(column.getUniqueId());
        }
        candidateSortKeyUniqueIds.add(newKeyColumn.getUniqueId());

        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        return SchemaChangeData.newBuilder()
                .withDatabase(db)
                .withTable(table)
                .withNewIndexMetaIdToSchema(baseIndexMetaId, newSchema)
                .withSortKeyUniqueIds(candidateSortKeyUniqueIds)
                .build();
    }

    @Test
    public void testDupTrailingKeyAddIsMetadataOnly() throws Exception {
        starRocksAssert.withTable(dupRangeTableWithValueDdl("t_meta_dup"));
        OlapTable table = getTable("t_meta_dup");
        SchemaChangeData resolved = buildTrailingKeyAddCandidate(table, constKeyColumn(table, "c_new"));
        assertTrue(SchemaChangeHandler.isMetadataOnlyTrailingKeyAdd(resolved));
    }

    @Test
    public void testAggTrailingKeyAddIsMetadataOnly() throws Exception {
        starRocksAssert.withTable(aggRangeTableWithValueDdl("t_meta_agg"));
        OlapTable table = getTable("t_meta_agg");
        SchemaChangeData resolved = buildTrailingKeyAddCandidate(table, constKeyColumn(table, "c_new"));
        assertTrue(SchemaChangeHandler.isMetadataOnlyTrailingKeyAdd(resolved));
    }

    @Test
    public void testUniqueTrailingKeyAddIsMetadataOnly() throws Exception {
        starRocksAssert.withTable(uniqueRangeTableWithValueDdl("t_meta_unique"));
        OlapTable table = getTable("t_meta_unique");
        SchemaChangeData resolved = buildTrailingKeyAddCandidate(table, constKeyColumn(table, "c_new"));
        assertTrue(SchemaChangeHandler.isMetadataOnlyTrailingKeyAdd(resolved));
    }

    /**
     * Same AGG scenario as {@link #testAggTrailingKeyAddIsMetadataOnly}, but the candidate sort key is
     * expressed via {@code sortKeyIdxes} (schema position) instead of {@code sortKeyUniqueIds},
     * exercising the resolver's second precedence tier at the classifier level, over a resolved schema
     * whose key columns (k1, k2, c_new) form a contiguous leading prefix matching those idxes.
     */
    @Test
    public void testAggIdxBasedTrailingKeyAddIsMetadataOnly() throws Exception {
        starRocksAssert.withTable(aggRangeTableWithValueDdl("t_meta_agg_idx"));
        OlapTable table = getTable("t_meta_agg_idx");
        long baseIndexMetaId = table.getBaseIndexMetaId();
        List<Column> oldSchema = table.getSchemaByIndexMetaId(baseIndexMetaId);

        Column newKeyColumn = constKeyColumn(table, "c_new");
        List<Column> newSchema = new ArrayList<>();
        for (Column column : oldSchema) {
            if (column.isKey()) {
                newSchema.add(column);
            }
        }
        newSchema.add(newKeyColumn);
        for (Column column : oldSchema) {
            if (!column.isKey()) {
                newSchema.add(column);
            }
        }
        // k1, k2, c_new form the leading contiguous key prefix -> idxes [0, 1, 2].
        assertTrue(newSchema.get(0).isKey() && newSchema.get(1).isKey() && newSchema.get(2).isKey());
        assertFalse(newSchema.get(3).isKey());

        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        SchemaChangeData resolved = SchemaChangeData.newBuilder()
                .withDatabase(db)
                .withTable(table)
                .withNewIndexMetaIdToSchema(baseIndexMetaId, newSchema)
                .withSortKeyIdxes(List.of(0, 1, 2))
                .build();
        assertTrue(SchemaChangeHandler.isMetadataOnlyTrailingKeyAdd(resolved));
    }

    @Test
    public void testPrimaryKeyTableNotMetadataOnly() throws Exception {
        starRocksAssert.withTable(primaryKeyRangeTableWithValueDdl("t_meta_pk"));
        OlapTable table = getTable("t_meta_pk");
        SchemaChangeData resolved = buildTrailingKeyAddCandidate(table, constKeyColumn(table, "c_new"));
        assertFalse(SchemaChangeHandler.isMetadataOnlyTrailingKeyAdd(resolved));
    }

    /**
     * Promoting an EXISTING value column to key (a value-to-key flip) is not an ADD of a brand-new
     * column: the classifier must reject it even though the resulting sort key also grows by one.
     */
    @Test
    public void testPromotingExistingColumnNotMetadataOnly() throws Exception {
        starRocksAssert.withTable(dupRangeTableWithValueDdl("t_meta_promote"));
        OlapTable table = getTable("t_meta_promote");
        long baseIndexMetaId = table.getBaseIndexMetaId();
        List<Column> oldSchema = table.getSchemaByIndexMetaId(baseIndexMetaId);
        MaterializedIndexMeta oldIndexMeta = table.getIndexMetaByMetaId(baseIndexMetaId);
        List<Column> oldSortKey = SchemaChangeHandler.resolveEffectiveSortKeyColumns(
                oldSchema, oldIndexMeta.getSortKeyUniqueIds(), oldIndexMeta.getSortKeyIdxes());

        List<Column> newSchema = new ArrayList<>();
        Column promotedV1 = null;
        for (Column column : oldSchema) {
            if (!column.isKey()) {
                Column copy = column.deepCopy();
                copy.setIsKey(true);
                promotedV1 = copy;
                newSchema.add(copy);
            } else {
                newSchema.add(column);
            }
        }

        List<Integer> candidateSortKeyUniqueIds = new ArrayList<>();
        for (Column column : oldSortKey) {
            candidateSortKeyUniqueIds.add(column.getUniqueId());
        }
        candidateSortKeyUniqueIds.add(promotedV1.getUniqueId());

        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        SchemaChangeData resolved = SchemaChangeData.newBuilder()
                .withDatabase(db)
                .withTable(table)
                .withNewIndexMetaIdToSchema(baseIndexMetaId, newSchema)
                .withSortKeyUniqueIds(candidateSortKeyUniqueIds)
                .build();
        assertFalse(SchemaChangeHandler.isMetadataOnlyTrailingKeyAdd(resolved));
    }

    @Test
    public void testColocateTableNotMetadataOnly() throws Exception {
        starRocksAssert.withTable(dupRangeTableWithValueDdl("t_meta_colocate"));
        OlapTable table = getTable("t_meta_colocate");
        SchemaChangeData resolved = buildTrailingKeyAddCandidate(table, constKeyColumn(table, "c_new"));
        new MockUp<ColocateTableIndex>() {
            @Mock
            public boolean isColocateTable(long tableId) {
                return true;
            }
        };
        assertFalse(SchemaChangeHandler.isMetadataOnlyTrailingKeyAdd(resolved));
    }

    @Test
    public void testAutoIncrementTableNotMetadataOnly() throws Exception {
        starRocksAssert.withTable("create table t_meta_autoinc "
                + "(k1 int not null, k2 int not null, id bigint not null auto_increment)\n"
                + "DUPLICATE KEY(k1, k2)\n"
                + "order by(k1, k2)\n"
                + "properties('replication_num' = '1');");
        OlapTable table = getTable("t_meta_autoinc");
        SchemaChangeData resolved = buildTrailingKeyAddCandidate(table, constKeyColumn(table, "c_new"));
        assertFalse(SchemaChangeHandler.isMetadataOnlyTrailingKeyAdd(resolved));
    }

    @Test
    public void testMultiIndexTableNotMetadataOnly() throws Exception {
        starRocksAssert.withTable(dupRangeTableWithValueDdl("t_meta_multi"));
        OlapTable table = getTable("t_meta_multi");
        SchemaChangeData resolved = buildTrailingKeyAddCandidate(table, constKeyColumn(table, "c_new"));

        long extraMetaId = GlobalStateMgr.getCurrentState().getNextId();
        List<Column> rollupSchema = table.getSchemaByIndexMetaId(table.getBaseIndexMetaId()).subList(0, 2);
        table.setIndexMeta(extraMetaId, "r_meta_multi", rollupSchema, 0, 0, (short) 1,
                TStorageType.COLUMN, KeysType.DUP_KEYS, null, List.of(0, 1));

        assertFalse(SchemaChangeHandler.isMetadataOnlyTrailingKeyAdd(resolved));
    }

    @Test
    public void testTempPartitionsTableNotMetadataOnly() throws Exception {
        starRocksAssert.withTable(dupRangeTableWithValueDdl("t_meta_temp"));
        OlapTable table = getTable("t_meta_temp");
        SchemaChangeData resolved = buildTrailingKeyAddCandidate(table, constKeyColumn(table, "c_new"));
        new MockUp<OlapTable>() {
            @Mock
            public boolean existTempPartitions() {
                return true;
            }
        };
        assertFalse(SchemaChangeHandler.isMetadataOnlyTrailingKeyAdd(resolved));
    }

    @Test
    public void testNonConstDefaultNotMetadataOnly() throws Exception {
        starRocksAssert.withTable(dupRangeTableWithValueDdl("t_meta_nonconst"));
        OlapTable table = getTable("t_meta_nonconst");
        Column newKeyColumn = constKeyColumn(table, "c_new");
        // Force a variable (non-constant) default, as if DEFAULT uuid() had been specified.
        Deencapsulation.setField(newKeyColumn, "defaultExpr", new DefaultExpr("uuid()", false));
        SchemaChangeData resolved = buildTrailingKeyAddCandidate(table, newKeyColumn);
        assertFalse(SchemaChangeHandler.isMetadataOnlyTrailingKeyAdd(resolved));
    }

    // -- resolveEffectiveSortKeyColumns: BE-compatible precedence (tablet_schema.cpp) --

    private static List<Column> threeColumnSchema() {
        Column a = new Column("a", IntegerType.INT);
        a.setIsKey(true);
        a.setUniqueId(10);
        Column b = new Column("b", IntegerType.INT);
        b.setIsKey(true);
        b.setUniqueId(11);
        Column c = new Column("c", IntegerType.INT);
        c.setUniqueId(12);
        return new ArrayList<>(List.of(a, b, c));
    }

    @Test
    public void testResolverPrefersSortKeyUniqueIds() {
        List<Column> schema = threeColumnSchema();
        // Reordered relative to schema position, and ignores sortKeyIdxes entirely.
        List<Column> resolved = SchemaChangeHandler.resolveEffectiveSortKeyColumns(
                schema, List.of(11, 10), List.of(2, 1, 0));
        assertEquals(List.of("b", "a"),
                resolved.stream().map(Column::getName).collect(java.util.stream.Collectors.toList()));
    }

    @Test
    public void testResolverFallsBackToSortKeyIdxesWhenUniqueIdsNull() {
        List<Column> schema = threeColumnSchema();
        List<Column> resolved = SchemaChangeHandler.resolveEffectiveSortKeyColumns(schema, null, List.of(2, 0));
        assertEquals(List.of("c", "a"),
                resolved.stream().map(Column::getName).collect(java.util.stream.Collectors.toList()));
    }

    @Test
    public void testResolverFallsBackToSortKeyIdxesWhenUniqueIdsEmpty() {
        List<Column> schema = threeColumnSchema();
        List<Column> resolved = SchemaChangeHandler.resolveEffectiveSortKeyColumns(
                schema, List.of(), List.of(2, 0));
        assertEquals(List.of("c", "a"),
                resolved.stream().map(Column::getName).collect(java.util.stream.Collectors.toList()));
    }

    @Test
    public void testResolverFallsBackToKeyColumnsWhenBothNull() {
        List<Column> schema = threeColumnSchema();
        List<Column> resolved = SchemaChangeHandler.resolveEffectiveSortKeyColumns(schema, null, null);
        assertEquals(List.of("a", "b"),
                resolved.stream().map(Column::getName).collect(java.util.stream.Collectors.toList()));
    }

    @Test
    public void testResolverFallsBackToKeyColumnsWhenBothEmpty() {
        List<Column> schema = threeColumnSchema();
        List<Column> resolved = SchemaChangeHandler.resolveEffectiveSortKeyColumns(
                schema, List.of(), List.of());
        assertEquals(List.of("a", "b"),
                resolved.stream().map(Column::getName).collect(java.util.stream.Collectors.toList()));
    }

    // ------------------------------------------------------------------------------------------
    // End-to-end ADD COLUMN dispatch: a qualifying trailing key-column add on a shared-data range
    // table is routed to the async metadata-only schema-evolution job (in-place range reprojection),
    // NOT the K-tablet data-rewrite job. Jobs are built (not run) via analyzeAndCreateJob.
    // ------------------------------------------------------------------------------------------

    // DUP range table with a key-derived sort key (no explicit ORDER BY): the new key column joins
    // the derived sort key, so the add is metadata-only.
    private static String dupRangeKeyDerivedDdl(String name) {
        return "create table " + name + " (k1 int, k2 int, v1 int)\n" +
                "DUPLICATE KEY(k1, k2)\n" +
                "properties('replication_num' = '1');";
    }

    private static AlterJobV2 buildAlterJob(String tableName, String alterSql) throws Exception {
        com.starrocks.sql.ast.AlterTableStmt stmt = (com.starrocks.sql.ast.AlterTableStmt)
                UtFrameUtils.parseStmtWithNewParser(alterSql, connectContext);
        OlapTable table = getTable(tableName);
        return GlobalStateMgr.getCurrentState().getSchemaChangeHandler()
                .analyzeAndCreateJob(stmt.getAlterClauseList(),
                        GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test"), table);
    }

    private static LakeTableAsyncFastSchemaChangeJob assertMetadataOnlyJob(AlterJobV2 job) {
        assertNotNull(job, "expected a job to be built");
        assertFalse(job instanceof LakeRangeRewriteSchemaChangeJob,
                "must NOT be the K-tablet data-rewrite job, got: " + job);
        assertTrue(job instanceof LakeTableAsyncFastSchemaChangeJob,
                "expected the async metadata-only job, got: " + job);
        LakeTableAsyncFastSchemaChangeJob aj = (LakeTableAsyncFastSchemaChangeJob) job;
        assertNotNull(aj.getTargetRanges(), "targetRanges must be set");
        assertFalse(aj.getTargetRanges().isEmpty(), "targetRanges must be non-empty");
        return aj;
    }

    private static void assertContiguousKeyPrefix(List<Column> columns) {
        boolean sawValue = false;
        for (Column column : columns) {
            if (column.isKey()) {
                assertFalse(sawValue, "key column after a value column breaks the prefix: " + column.getName());
            } else {
                sawValue = true;
            }
        }
    }

    private static int countBaseTablets(OlapTable table) {
        long baseId = table.getBaseIndexMetaId();
        int count = 0;
        for (PhysicalPartition pp : table.getPhysicalPartitions()) {
            MaterializedIndex idx = pp.getLatestIndex(baseId);
            if (idx != null) {
                count += idx.getTablets().size();
            }
        }
        return count;
    }

    @Test
    public void testAddTrailingKeyColRunsMetadataOnlyDup() throws Exception {
        starRocksAssert.withTable(dupRangeKeyDerivedDdl("t_add_meta_dup"));
        OlapTable table = getTable("t_add_meta_dup");
        long baseId = table.getBaseIndexMetaId();
        short oldShortKey = table.getIndexMetaByMetaId(baseId).getShortKeyColumnCount();
        long oldKeyCount = table.getSchemaByIndexMetaId(baseId).stream().filter(Column::isKey).count();
        int tabletCount = countBaseTablets(table);

        AlterJobV2 job = buildAlterJob("t_add_meta_dup",
                "alter table t_add_meta_dup add column c int key default '0'");
        LakeTableAsyncFastSchemaChangeJob aj = assertMetadataOnlyJob(job);
        assertEquals(tabletCount, aj.getTargetRanges().size(), "one target range per base tablet");

        SchemaInfo si = aj.getSchemaInfoList().get(0);
        Column added = si.getColumns().stream().filter(c -> c.getName().equalsIgnoreCase("c"))
                .findFirst().orElseThrow();
        assertTrue(added.isKey(), "added column c must be a key");
        // DUP key-derived sort key == key columns, so the sort-key arity grows with the key count.
        long newKeyCount = si.getColumns().stream().filter(Column::isKey).count();
        assertEquals(oldKeyCount + 1, newKeyCount, "sort/key arity grew by one");
        assertTrue(si.getShortKeyColumnCount() > oldShortKey, "short key count grew");
        assertContiguousKeyPrefix(si.getColumns());
    }

    @Test
    public void testAddTrailingKeyColRunsMetadataOnlyAgg() throws Exception {
        starRocksAssert.withTable(aggRangeTableWithValueDdl("t_add_meta_agg"));
        assertOrderByRangeTrailingKeyAddMetadataOnly("t_add_meta_agg");
    }

    @Test
    public void testAddTrailingKeyColRunsMetadataOnlyUnique() throws Exception {
        starRocksAssert.withTable(uniqueRangeTableWithValueDdl("t_add_meta_uniq"));
        assertOrderByRangeTrailingKeyAddMetadataOnly("t_add_meta_uniq");
    }

    // AGG/UNIQUE range tables carry an explicit sort key (sortKeyIdxes); the new key column is appended
    // to it in lockstep, so both the sort-key arity and the short-key count grow.
    private void assertOrderByRangeTrailingKeyAddMetadataOnly(String tableName) throws Exception {
        OlapTable table = getTable(tableName);
        long baseId = table.getBaseIndexMetaId();
        int oldSortKeyArity = table.getIndexMetaByMetaId(baseId).getSortKeyIdxes().size();
        short oldShortKey = table.getIndexMetaByMetaId(baseId).getShortKeyColumnCount();

        AlterJobV2 job = buildAlterJob(tableName,
                "alter table " + tableName + " add column c int key default '0'");
        LakeTableAsyncFastSchemaChangeJob aj = assertMetadataOnlyJob(job);

        SchemaInfo si = aj.getSchemaInfoList().get(0);
        assertEquals(oldSortKeyArity + 1, si.getSortKeyIndexes().size(), "sort key arity grew by one");
        int lastIdx = si.getSortKeyIndexes().get(si.getSortKeyIndexes().size() - 1);
        assertEquals("c", si.getColumns().get(lastIdx).getName(), "new column is the trailing sort key");
        assertTrue(si.getColumns().get(lastIdx).isKey());
        assertTrue(si.getShortKeyColumnCount() > oldShortKey, "short key count grew");
        assertContiguousKeyPrefix(si.getColumns());
    }

    /**
     * The resolved schema of a metadata-only AGG/UNIQUE trailing key add keeps the key columns a
     * contiguous leading prefix and points {@code sort_key_idxes} exactly at those prefix positions --
     * the FE obligation the BE merge/aggregate path relies on (design's AGG/UNIQUE lockstep rule).
     */
    @Test
    public void testAggUniqueAddContiguousKeyPrefixAndSortKeyIdxes() throws Exception {
        starRocksAssert.withTable(aggRangeTableWithValueDdl("t_add_meta_ck_agg"));
        starRocksAssert.withTable(uniqueRangeTableWithValueDdl("t_add_meta_ck_uniq"));
        for (String tableName : new String[] {"t_add_meta_ck_agg", "t_add_meta_ck_uniq"}) {
            AlterJobV2 job = buildAlterJob(tableName,
                    "alter table " + tableName + " add column c int key default '0'");
            LakeTableAsyncFastSchemaChangeJob aj = assertMetadataOnlyJob(job);
            SchemaInfo si = aj.getSchemaInfoList().get(0);
            assertContiguousKeyPrefix(si.getColumns());
            List<Integer> sortKeyIdxes = si.getSortKeyIndexes();
            long keyCount = si.getColumns().stream().filter(Column::isKey).count();
            assertEquals(keyCount, sortKeyIdxes.size(), "sort key covers exactly the key columns");
            for (int i = 0; i < sortKeyIdxes.size(); i++) {
                assertEquals(i, sortKeyIdxes.get(i).intValue(), "sort key idx matches key prefix position");
                assertTrue(si.getColumns().get(sortKeyIdxes.get(i)).isKey());
            }
        }
    }

    /**
     * A tablet with a bounded (non-all) range is reprojected in place: each existing boundary tuple
     * gains one trailing NULL sentinel for the new column, its prefix values preserved.
     */
    @Test
    public void testAddTrailingKeyColReprojectsBoundedRangeWithNullSentinel() throws Exception {
        starRocksAssert.withTable(dupRangeKeyDerivedDdl("t_add_meta_bounded"));
        OlapTable table = getTable("t_add_meta_bounded");
        long baseId = table.getBaseIndexMetaId();
        long tabletId = -1;
        for (PhysicalPartition pp : table.getPhysicalPartitions()) {
            for (Tablet t : pp.getLatestIndex(baseId).getTablets()) {
                t.setRange(new TabletRange(Range.of(
                        new Tuple(List.of(Variant.of(IntegerType.INT, "10"), Variant.of(IntegerType.INT, "20"))),
                        new Tuple(List.of(Variant.of(IntegerType.INT, "30"), Variant.of(IntegerType.INT, "40"))),
                        true, false)));
                tabletId = t.getId();
            }
        }

        AlterJobV2 job = buildAlterJob("t_add_meta_bounded",
                "alter table t_add_meta_bounded add column c int key default '0'");
        LakeTableAsyncFastSchemaChangeJob aj = assertMetadataOnlyJob(job);

        Range<Tuple> reprojected = aj.getTargetRanges().get(tabletId).getRange();
        assertEquals(3, reprojected.getLowerBound().getValues().size(), "lower bound arity grew to 3");
        assertEquals(3, reprojected.getUpperBound().getValues().size(), "upper bound arity grew to 3");
        assertTrue(reprojected.getLowerBound().getValues().get(2) instanceof NullVariant,
                "trailing lower sentinel is NULL");
        assertTrue(reprojected.getUpperBound().getValues().get(2) instanceof NullVariant,
                "trailing upper sentinel is NULL");
        assertEquals("10", reprojected.getLowerBound().getValues().get(0).getStringValue(),
                "existing prefix values preserved");
        assertEquals("40", reprojected.getUpperBound().getValues().get(1).getStringValue(),
                "existing prefix values preserved");
    }

    /**
     * A PRIMARY KEY range table's ADD COLUMN KEY is rejected during statement analysis (a PK key column
     * cannot carry the aggregate the analyzer assigns), so it never reaches the metadata-only
     * classifier -- existing behavior, not the async job.
     */
    @Test
    public void testPkRangeAddKeyColNotMetadataOnly() throws Exception {
        starRocksAssert.withTable(primaryKeyRangeTableWithValueDdl("t_add_meta_pk"));
        assertThrows(Throwable.class, () ->
                buildAlterJob("t_add_meta_pk", "alter table t_add_meta_pk add column c int key default '0'"));
    }

    /**
     * Promoting an existing value column to a key is a MODIFY (keyness flip), not an ADD, so it is
     * never diverted to the metadata-only ADD job; it stays on the MODIFY path and is rejected on
     * range tables.
     */
    @Test
    public void testPromoteExistingColumnToKeyNotMetadataOnly() throws Exception {
        starRocksAssert.withTable(dupRangeTableWithValueDdl("t_add_meta_promote"));
        DdlException e = assertThrowsDdlException(() ->
                buildAlterJob("t_add_meta_promote", "alter table t_add_meta_promote modify column v1 int key"));
        assertTrue(e.getMessage().toLowerCase(Locale.ROOT).contains("range distribution"),
                "Expected 'range distribution' in: " + e.getMessage());
    }

    /**
     * Colocate range tables are excluded from range-rewrite routing, so an ADD COLUMN KEY is not
     * diverted and the existing range-distribution rejection applies (unaffected by the diversion).
     */
    @Test
    public void testColocateRangeAddKeyColUnaffected() throws Exception {
        starRocksAssert.withTable(dupRangeKeyDerivedDdl("t_add_meta_colo"));
        new MockUp<ColocateTableIndex>() {
            @Mock
            public boolean isColocateTable(long tableId) {
                return true;
            }
        };
        DdlException e = assertThrowsDdlException(() ->
                buildAlterJob("t_add_meta_colo", "alter table t_add_meta_colo add column c int key default '0'"));
        assertTrue(e.getMessage().toLowerCase(Locale.ROOT).contains("range distribution"),
                "Expected 'range distribution' in: " + e.getMessage());
    }

    /**
     * AUTO_INCREMENT range tables are excluded from range-rewrite routing, so an ADD COLUMN KEY stays
     * on the existing range-distribution rejection (unaffected by the diversion).
     */
    @Test
    public void testAutoIncrementRangeAddKeyColUnaffected() throws Exception {
        starRocksAssert.withTable("create table t_add_meta_autoinc "
                + "(k1 int not null, k2 int not null, id bigint not null auto_increment)\n"
                + "duplicate key(k1, k2)\n"
                + "order by(k1, k2)\n"
                + "properties('replication_num' = '1');");
        DdlException e = assertThrowsDdlException(() ->
                buildAlterJob("t_add_meta_autoinc",
                        "alter table t_add_meta_autoinc add column c int key default '0'"));
        assertTrue(e.getMessage().toLowerCase(Locale.ROOT).contains("range distribution"),
                "Expected 'range distribution' in: " + e.getMessage());
    }

    /**
     * With temp partitions present, the schema-change loop rejects early (unchanged), so an ADD COLUMN
     * KEY never reaches the metadata-only diversion.
     */
    @Test
    public void testTempPartitionRangeAddKeyColUnaffected() throws Exception {
        starRocksAssert.withTable(dupRangeKeyDerivedDdl("t_add_meta_temp"));
        new MockUp<OlapTable>() {
            @Mock
            public boolean existTempPartitions() {
                return true;
            }
        };
        DdlException e = assertThrowsDdlException(() ->
                buildAlterJob("t_add_meta_temp", "alter table t_add_meta_temp add column c int key default '0'"));
        assertTrue(e.getMessage().toLowerCase(Locale.ROOT).contains("temp partition"),
                "Expected 'temp partition' in: " + e.getMessage());
    }

    /**
     * Regression: adding a VALUE column (not a key) to a range DUP table is unaffected by the
     * diversion -- it is applied as an ordinary fast-schema-evolution change, never the metadata-only
     * trailing-key job.
     */
    @Test
    public void testDupValueColumnAddUnaffected() throws Exception {
        starRocksAssert.withTable(dupRangeKeyDerivedDdl("t_add_meta_val"));
        AlterJobV2 job = buildAlterJob("t_add_meta_val",
                "alter table t_add_meta_val add column c int default '0'");
        assertFalse(job instanceof LakeTableAsyncFastSchemaChangeJob,
                "value-column add must not be the metadata-only job, got: " + job);
    }

    /**
     * Regression: an ordinary (non-range) table's ADD COLUMN KEY is unaffected -- it stays on the
     * standard schema-change path, never the metadata-only trailing-key job.
     */
    @Test
    public void testNonRangeAddKeyColUnaffected() throws Exception {
        boolean saved = Config.enable_range_distribution;
        Config.enable_range_distribution = false;
        try {
            starRocksAssert.withTable("create table t_add_meta_hash (k1 int, k2 int, v1 int)\n"
                    + "DUPLICATE KEY(k1, k2) DISTRIBUTED BY HASH(k1) BUCKETS 2\n"
                    + "properties('replication_num' = '1');");
            AlterJobV2 job = buildAlterJob("t_add_meta_hash",
                    "alter table t_add_meta_hash add column c int key default '0'");
            assertFalse(job instanceof LakeTableAsyncFastSchemaChangeJob,
                    "non-range add key must not be metadata-only, got: " + job);
        } finally {
            Config.enable_range_distribution = saved;
        }
    }

    /**
     * A batch combining the trailing key-column ADD with another SCHEMA_CHANGE clause (ADD and MODIFY
     * both map to AlterOpType.SCHEMA_CHANGE, so conflict analysis allows the batch) must NOT be routed
     * to the metadata-only async job: that job would carry the MODIFY'd column's new type in the
     * schema with no data rewrite, while old segments still hold the pre-MODIFY encoding. The batch
     * must be rejected precisely, mirroring {@code buildRoutedAddKeyColumnJob}'s single-clause check.
     */
    @Test
    public void testAddTrailingKeyColCombinedWithModifyColumnRejected() throws Exception {
        starRocksAssert.withTable(dupRangeKeyDerivedDdl("t_add_meta_combined"));
        DdlException e = assertThrowsDdlException(() ->
                buildAlterJob("t_add_meta_combined",
                        "alter table t_add_meta_combined add column c int key default '0', "
                                + "modify column v1 bigint"));
        assertTrue(e.getMessage().contains("can not be combined with other alter operations"),
                "Expected combined-op rejection in: " + e.getMessage());
    }
}
