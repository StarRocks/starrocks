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

import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.RunMode;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

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

    /**
     * The alter-table DDL path wraps {@link DdlException} in a {@link RuntimeException}
     * via {@code ErrorReport.wrapWithRuntimeException}, while other paths throw the
     * {@link DdlException} directly. This helper extracts the underlying
     * {@link DdlException} regardless of which path is taken.
     */
    private static DdlException assertThrowsDdlException(Executable executable) {
        Throwable thrown = assertThrows(Throwable.class, executable);
        Throwable cur = thrown;
        while (cur != null) {
            if (cur instanceof DdlException) {
                return (DdlException) cur;
            }
            cur = cur.getCause();
        }
        fail("Expected DdlException in cause chain of " + thrown);
        return null; // unreachable
    }

    @Test
    public void testAddRollupRejectedOnRangeDistribution() throws Exception {
        starRocksAssert.withTable(rangeTableDdl("t_guard_rollup"));
        DdlException ex = assertThrowsDdlException(() ->
                starRocksAssert.alterTable(
                        "alter table t_guard_rollup add rollup r1(k1, v1)"));
        assertTrue(ex.getMessage().toLowerCase().contains("range distribution"),
                "Expected 'range distribution' in: " + ex.getMessage());
    }

    @Test
    public void testSyncCreateMaterializedViewRejectedOnRangeDistribution() throws Exception {
        starRocksAssert.withTable(rangeTableDdl("t_guard_syncmv"));
        DdlException ex = assertThrowsDdlException(() ->
                starRocksAssert.withMaterializedView(
                        "create materialized view mv_guard_sync as " +
                        "select k1, v1 from t_guard_syncmv"));
        assertTrue(ex.getMessage().toLowerCase().contains("range distribution"),
                "Expected 'range distribution' in: " + ex.getMessage());
    }

    @Test
    public void testModifySortKeyRejectedOnRangeDistribution() throws Exception {
        starRocksAssert.withTable(rangeTableDdl("t_guard_orderby"));
        // Use a column list SHORTER than base schema so this routes to
        // processModifySortKeyColumn (not the schema-reorder overload).
        // Unlike the ADD ROLLUP / sync MV paths (which use
        // ErrorReport.wrapWithRuntimeException and preserve DdlException as the
        // cause), the SCHEMA_CHANGE path in AlterJobExecutor catches
        // StarRocksException and re-throws as AlterJobException with only the
        // message — no cause — so we assert on the message directly rather
        // than via assertThrowsDdlException's cause-chain walk.
        Throwable ex = assertThrows(Throwable.class, () ->
                starRocksAssert.alterTable(
                        "alter table t_guard_orderby order by (k1)"));
        assertTrue(ex.getMessage().toLowerCase().contains("range distribution"),
                "Expected 'range distribution' in: " + ex.getMessage());
    }

    @Test
    public void testOptimizeRejectedOnRangeDistribution() throws Exception {
        starRocksAssert.withTable(rangeTableDdl("t_guard_optimize"));
        // OPTIMIZE clause is triggered by `alter table ... distributed by ...`
        // — there is no standalone OPTIMIZE keyword in the grammar.
        // The analyzer raises SemanticException, but
        // UtFrameUtils.parseStmtWithNewParser wraps it as AnalysisException, so
        // we match on Throwable + message rather than the concrete type. The
        // load-bearing check is the "range distribution" substring.
        Throwable ex = assertThrows(Throwable.class, () ->
                starRocksAssert.alterTable(
                        "alter table t_guard_optimize distributed by hash(k1)"));
        assertTrue(ex.getMessage().toLowerCase().contains("range distribution"),
                "Expected 'range distribution' in: " + ex.getMessage());
    }

    @Test
    public void testAddKeyColumnRejectedOnRangeDistribution() throws Exception {
        // Range table (order by(k1, k2)). Adding a key column would otherwise
        // append to the sort key, invalidating stored range tablet boundaries.
        starRocksAssert.withTable(rangeTableDdl("t_guard_addkey"));
        // Schema-change DDL path — exception is AlterJobException (no DdlException cause),
        // so assert on Throwable + message, consistent with the ORDER BY test pattern.
        Throwable ex = assertThrows(Throwable.class, () ->
                starRocksAssert.alterTable(
                        "alter table t_guard_addkey add column k_new int key default '0'"));
        assertTrue(ex.getMessage().toLowerCase().contains("range distribution"),
                "Expected 'range distribution' in: " + ex.getMessage());
    }

    @Test
    public void testDropSortKeyColumnRejectedOnRangeDistribution() throws Exception {
        // DUP range table: k1, k2 are both sort/key columns.
        starRocksAssert.withTable(rangeTableDdl("t_guard_dropsk"));
        // Schema-change DDL path — Throwable + message substring, consistent with Task 2.
        Throwable ex = assertThrows(Throwable.class, () ->
                starRocksAssert.alterTable(
                        "alter table t_guard_dropsk drop column k2"));
        assertTrue(ex.getMessage().toLowerCase().contains("range distribution"),
                "Expected 'range distribution' in: " + ex.getMessage());
        assertTrue(ex.getMessage().toLowerCase().contains("k2"),
                "Expected 'k2' (offending column) in: " + ex.getMessage());
    }
}
