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

import java.util.Locale;

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
     * The SCHEMA_CHANGE alter path in {@code AlterJobExecutor} catches
     * {@code StarRocksException} and re-throws as {@code AlterJobException}
     * with only the message (no cause), so tests on that path assert
     * directly on the top exception's message rather than going through
     * {@link #assertThrowsDdlException} which walks the cause chain.
     */
    @Test
    public void testModifySortKeyRejectedOnRangeDistribution() throws Exception {
        starRocksAssert.withTable(rangeTableDdl("t_guard_orderby"));
        // Column list SHORTER than base schema routes to
        // processModifySortKeyColumn (not the schema-reorder overload).
        Throwable exception = assertThrows(Throwable.class, () ->
                starRocksAssert.alterTable(
                        "alter table t_guard_orderby order by (k1)"));
        assertTrue(exception.getMessage().toLowerCase(Locale.ROOT).contains("range distribution"),
                "Expected 'range distribution' in: " + exception.getMessage());
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
}
