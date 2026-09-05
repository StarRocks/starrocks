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

package com.starrocks.planner;

import com.starrocks.common.FeConstants;
import com.starrocks.qe.ConnectContext;
import com.starrocks.statistic.StatsConstants;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import static com.starrocks.sql.optimizer.statistics.CachedStatisticStorageTest.DEFAULT_CREATE_TABLE_TEMPLATE;

/**
 * Regression test for a CBO table-prune Memo self-reference crash.
 *
 * <p>A query over a view that LEFT OUTER JOINs a fact table to several dimension tables carrying
 * unique / foreign-key constraints, combined with {@code enable_cbo_table_prune=true} and an
 * {@code IN (subquery)} predicate, could fail during planning with an unmessaged
 * {@code IllegalStateException at Memo.copyIn} — the {@code checkState(group != targetGroup)}.
 *
 * <p>Mechanism: when only the fact columns are referenced, CboTablePruneRule collapses the prunable
 * {@code Join(Scan, Scan)} into a single Scan. If that scan hash-collides with an existing group
 * expression, the Memo merges the two groups and a child group can collapse into its own target
 * group; a surviving JoinCommutativityRule task then copies in an expression whose input group is
 * the target group itself, forming a self-reference cycle.
 *
 * <p>All four cases below must PLAN SUCCESSFULLY. The two view cases are the ones that regressed;
 * the base-table and prune-disabled cases are controls.
 */
public class CboTablePruneMemoSelfRefTest {
    private static ConnectContext ctx;
    private static StarRocksAssert starRocksAssert;

    private static String readDdl(String name) throws Exception {
        try (InputStream in = CboTablePruneMemoSelfRefTest.class.getClassLoader()
                .getResourceAsStream("sql/memo_self_ref/" + name + ".sql")) {
            return IOUtils.toString(in, StandardCharsets.UTF_8);
        }
    }

    @BeforeAll
    public static void setUp() throws Exception {
        FeConstants.runningUnitTest = true;
        UtFrameUtils.createMinStarRocksCluster();
        ctx = UtFrameUtils.createDefaultCtx();
        ctx.getSessionVariable().setOptimizerExecuteTimeout(30000);
        starRocksAssert = new StarRocksAssert(ctx);
        starRocksAssert.withDatabase(StatsConstants.STATISTICS_DB_NAME)
                .useDatabase(StatsConstants.STATISTICS_DB_NAME)
                .withTable(DEFAULT_CREATE_TABLE_TEMPLATE);
        starRocksAssert.withDatabase("test_prune").useDatabase("test_prune");
        starRocksAssert.withTable(readDdl("t_fact"));
        starRocksAssert.withTable(readDdl("t_dim_a"));
        starRocksAssert.withTable(readDdl("t_dim_b"));
        starRocksAssert.withTable(readDdl("t_dim_c"));
        starRocksAssert.withTable(readDdl("t_drive"));
        starRocksAssert.withView(readDdl("v_fact"));

        ctx.getSessionVariable().setEnableCboTablePrune(true);
        ctx.getSessionVariable().setEnableUKFKOpt(true);
    }

    private static final String SELECT_FROM_JOIN_FMT = "SELECT\n" +
            "  `b`.`name` AS `name`,\n" +
            "  `a`.`fk_id` AS `fk_id`\n" +
            "FROM\n" +
            "  `default_catalog`.`test_prune`.`t_drive` AS `a`\n" +
            "  INNER JOIN `default_catalog`.`test_prune`.`%s` AS `b` ON `a`.`fk_id` = `b`.`id`\n" +
            "  AND `a`.`part_key` = `b`.`part_key`\n" +
            "WHERE\n" +
            "  `b`.`part_key` = CAST(100 AS BIGINT)\n" +
            "  AND `a`.`part_key` = CAST(100 AS BIGINT)\n" +
            "  AND `b`.`name` IN %s";

    /** With the fix, planning succeeds (non-null plan). Without it, getFragmentPlan throws. */
    private void assertPlans(String sql) throws Exception {
        String plan = UtFrameUtils.getFragmentPlan(ctx, sql);
        Assertions.assertNotNull(plan);
    }

    /** view + IN (inline subselect) + cbo_table_prune — the regressed case; must plan. */
    @Test
    public void testViewWithInlineInSubqueryPlans() throws Exception {
        assertPlans(String.format(SELECT_FROM_JOIN_FMT, "v_fact", "(SELECT 'x' AS name)"));
    }

    /** view + IN (CTE subselect) + cbo_table_prune — the regressed case; must plan. */
    @Test
    public void testViewWithCteInSubqueryPlans() throws Exception {
        String sql = "WITH `cte` AS (SELECT 'x' AS name)\n" +
                String.format(SELECT_FROM_JOIN_FMT, "v_fact",
                        "(SELECT `cte`.`name` FROM `cte` AS `cte`)");
        assertPlans(sql);
    }

    /** base fact table (no prunable joins) — control. */
    @Test
    public void testBaseTableWithInlineInSubqueryPlans() throws Exception {
        assertPlans(String.format(SELECT_FROM_JOIN_FMT, "t_fact", "(SELECT 'x' AS name)"));
    }

    /** view with cbo_table_prune OFF — control; planned fine before and after the fix. */
    @Test
    public void testViewWithCboTablePruneDisabledPlans() throws Exception {
        ctx.getSessionVariable().setEnableCboTablePrune(false);
        try {
            assertPlans(String.format(SELECT_FROM_JOIN_FMT, "v_fact", "(SELECT 'x' AS name)"));
        } finally {
            ctx.getSessionVariable().setEnableCboTablePrune(true);
        }
    }
}
