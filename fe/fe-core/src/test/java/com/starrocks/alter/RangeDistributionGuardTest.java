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
import com.starrocks.catalog.OlapTable;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.type.PrimitiveType;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import java.util.Locale;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class RangeDistributionGuardTest {
    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;
    private static boolean savedEnableRangeDistribution;

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

    @Test
    public void testAddRollupRejectedOnRangeDistribution() throws Exception {
        starRocksAssert.withTable(rangeTableDdl("t_guard_rollup"));
        DdlException exception = assertThrowsDdlException(() ->
                starRocksAssert.alterTable(
                        "alter table t_guard_rollup add rollup r1(k1, v1)"));
        assertTrue(exception.getMessage().toLowerCase(Locale.ROOT).contains("range distribution"),
                "Expected 'range distribution' in: " + exception.getMessage());
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
}
