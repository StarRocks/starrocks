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

package com.starrocks.sql.plan;

import com.starrocks.catalog.OlapTable;
import com.starrocks.common.FeConstants;
import com.starrocks.sql.optimizer.statistics.CachedStatisticStorage;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.Histogram;
import com.starrocks.statistic.StatisticsMetaManager;
import com.starrocks.thrift.TExplainLevel;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertThrows;

class WindowSkewTest extends PlanTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        FeConstants.runningUnitTest = true;
        connectContext.getGlobalStateMgr().setStatisticStorage(new CachedStatisticStorage());
        starRocksAssert.withTable(
                """
                        CREATE TABLE `window_skew_table` (
                          `p` int NULL,
                          `s` int NULL,
                          `x` int NULL
                        ) ENGINE=OLAP
                        DUPLICATE KEY(`p`, `s`, `x`)
                        DISTRIBUTED BY HASH(`p`) BUCKETS 3
                        PROPERTIES (
                          "replication_num" = "1",
                          "in_memory" = "false"
                        );
                        """
        );
        starRocksAssert.withTable(
                """
                        CREATE TABLE `window_skew_table_join` (
                          `id` int NULL,
                          `val` int NULL
                        ) ENGINE=OLAP
                        DUPLICATE KEY(`id`, `val`)
                        DISTRIBUTED BY HASH(`id`) BUCKETS 3
                        PROPERTIES (
                          "replication_num" = "1",
                          "in_memory" = "false"
                        );
                        """
        );

        if (!starRocksAssert.databaseExist("_statistics_")) {
            StatisticsMetaManager m = new StatisticsMetaManager();
            m.createStatisticsTablesForTest();
        }
    }

    @BeforeEach
    @Override
    public void setUp() {
        super.setUp();
        connectContext.getSessionVariable().setEnableSplitWindowSkewToUnion(true);

        final var table = getOlapTable("window_skew_table");
        setTableStatistics(table, 1000);
    }

    @Test
    void testWindowWithSkew() throws Exception {

        final var table = getOlapTable("window_skew_table");

        final var statisticStorage = connectContext.getGlobalStateMgr().getStatisticStorage();
        final var skewedColumnStat = ColumnStatistic.builder().setNullsFraction(0.3).build();

        statisticStorage.refreshColumnStatistics(table, List.of("p", "s", "x"), true);
        statisticStorage.addColumnStatistic(table, "p", skewedColumnStat);
        statisticStorage.getColumnStatistics(table, List.of("p", "s", "x"));

        String sql = "select p, s, sum(x) over (partition by p order by s) from window_skew_table";

        String plan = getFragmentPlan(sql, TExplainLevel.COSTS, "");

        assertContains(plan, "Output Exprs:1: p | 2: s | 4: sum(3: x)");
        assertContains(plan, "UNION");
        // Validate that data is split into NULL and NOT NULL paths
        assertContains(plan, "Predicates: [5: p, INT, true] IS NOT NULL");
        assertContains(plan, "Predicates: [1: p, INT, true] IS NULL");
        // Validate Union child expressions match the expected columns from both branches
        assertContains(plan,
                "ANALYTIC\n" +
                        "  |  functions: [, sum[([7: x, INT, true]); args: INT; result: BIGINT; args nullable: true;" +
                        " result nullable: true], ]\n" +
                        "  |  partition by: [5: p, INT, true]\n" +
                        "  |  order by: [6: s, INT, true] ASC\n" +
                        "  |  window: RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW\n" +
                        "  |  cardinality: 700\n" +
                        "  |  column statistics: \n" +
                        "  |  * p-->[-Infinity, Infinity, 0.0, NaN, NaN] ESTIMATE\n" +
                        "  |  * s-->[-Infinity, Infinity, 0.0, 1.0, 1.0] UNKNOWN\n" +
                        "  |  * x-->[-Infinity, Infinity, 0.0, 1.0, 1.0] UNKNOWN\n" +
                        "  |  * sum(7: x)-->[-Infinity, Infinity, 0.0, 1.0, 1.0] UNKNOWN");

        assertContains(plan,
                "ANALYTIC\n" +
                        "  |  functions: [, sum[([3: x, INT, true]); args: INT; result: BIGINT; args nullable: true; " +
                        "result nullable: true], ]\n" +
                        "  |  order by: [2: s, INT, true] ASC\n" +
                        "  |  window: RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW\n" +
                        "  |  cardinality: 300\n" +
                        "  |  column statistics: \n" +
                        "  |  * p-->[-Infinity, Infinity, 1.0, NaN, NaN] ESTIMATE\n" +
                        "  |  * s-->[-Infinity, Infinity, 0.0, 1.0, 1.0] UNKNOWN\n" +
                        "  |  * x-->[-Infinity, Infinity, 0.0, 1.0, 1.0] UNKNOWN\n" +
                        "  |  * sum(3: x)-->[-Infinity, Infinity, 0.0, 1.0, 1.0] UNKNOWN");
    }

    @Test
    void testWindowWithoutSkew() throws Exception {
        String sql = "select p, s, sum(x) over (partition by p order by s) from window_skew_table";

        final var nonSkewedColumnStat = ColumnStatistic.builder().setNullsFraction(0.1).build();

        final var table = getOlapTable("window_skew_table");
        final var statisticStorage = connectContext.getGlobalStateMgr().getStatisticStorage();
        statisticStorage.addColumnStatistic(table, "p", nonSkewedColumnStat);
        statisticStorage.getColumnStatistics(table, List.of("p", "s", "x"));

        String plan = getFragmentPlan(sql);

        assertNotContains(plan, "UNION");
        assertContains(plan, "ANALYTIC");
    }

    @Test
    void testWindowWithMultipleSkewedColumns() throws Exception {
        OlapTable table = getOlapTable("window_skew_table");
        final var statisticStorage = connectContext.getGlobalStateMgr().getStatisticStorage();

        final var skewedP = ColumnStatistic.builder().setNullsFraction(0.4).build();
        final var skewedS = ColumnStatistic.builder().setNullsFraction(0.4).build();

        statisticStorage.refreshColumnStatistics(table, List.of("p", "s", "x"), true);
        statisticStorage.addColumnStatistic(table, "p", skewedP);
        statisticStorage.addColumnStatistic(table, "s", skewedS);
        statisticStorage.getColumnStatistics(table, List.of("p", "s", "x"));

        // Partition by multiple columns
        String sql = "select p, s, sum(x) over (partition by p, s order by x) from window_skew_table";

        String plan = getFragmentPlan(sql, TExplainLevel.COSTS, "");

        assertNotContains(plan, "UNION");
        assertContains(plan, "ANALYTIC");
    }

    @Test
    void testWindowWithMCVSkew() throws Exception {
        OlapTable table = getOlapTable("window_skew_table");
        final var statisticStorage = connectContext.getGlobalStateMgr().getStatisticStorage();

        Histogram histogram = new Histogram(
                /* buckets */ List.of(),
                /* mcv */ Map.of("1", 300L));

        final var skewedMCV = ColumnStatistic.builder().setNullsFraction(0.0).setHistogram(histogram).build();

        statisticStorage.refreshColumnStatistics(table, List.of("p", "s", "x"), true);
        statisticStorage.addColumnStatistic(table, "p", skewedMCV);
        statisticStorage.getColumnStatistics(table, List.of("p", "s", "x"));

        String sql = "select p, s, sum(x) over (partition by p order by s) from window_skew_table";
        String plan = getFragmentPlan(sql, TExplainLevel.COSTS, "");

        assertContains(plan, "UNION");
        assertContains(plan, "Predicates: [1: p, INT, true] = 1");
        // Ensure that unskewed partition preserves NULLs
        assertContains(plan, "Predicates: (cast([5: p, INT, true] as VARCHAR(1048576)) != '1') " +
                "OR ([5: p, INT, true] IS NULL)");

        assertContains(plan,
                "ANALYTIC\n" +
                        "  |  functions: [, sum[([7: x, INT, true]); args: INT; result: BIGINT; args nullable: true;" +
                        " result nullable: true], ]\n" +
                        "  |  partition by: [5: p, INT, true]\n" +
                        "  |  order by: [6: s, INT, true] ASC\n" +
                        "  |  window: RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW\n" +
                        "  |  cardinality: 730\n" +
                        "  |  column statistics: \n" +
                        "  |  * p-->[-Infinity, Infinity, 0.0, NaN, NaN] MCV: [[1:300]] ESTIMATE\n" +
                        "  |  * s-->[-Infinity, Infinity, 0.0, 1.0, 1.0] UNKNOWN\n" +
                        "  |  * x-->[-Infinity, Infinity, 0.0, 1.0, 1.0] UNKNOWN\n" +
                        "  |  * sum(7: x)-->[-Infinity, Infinity, 0.0, 1.0, 1.0] UNKNOWN");
        assertContains(plan,
                "ANALYTIC\n" +
                        "  |  functions: [, sum[([3: x, INT, true]); args: INT; result: BIGINT; args nullable: true;" +
                        " result nullable: true], ]\n" +
                        "  |  order by: [2: s, INT, true] ASC\n" +
                        "  |  window: RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW\n" +
                        "  |  cardinality: 300\n" +
                        "  |  column statistics: \n" +
                        "  |  * p-->[1.0, 1.0, 0.0, NaN, NaN] MCV: [[1:300]] ESTIMATE\n" +
                        "  |  * s-->[-Infinity, Infinity, 0.0, 1.0, 1.0] UNKNOWN\n" +
                        "  |  * x-->[-Infinity, Infinity, 0.0, 1.0, 1.0] UNKNOWN\n" +
                        "  |  * sum(3: x)-->[-Infinity, Infinity, 0.0, 1.0, 1.0] UNKNOWN");
    }

    @Test
    void testOtherAnalyticalFunctionsWithSkew() throws Exception {
        OlapTable table = getOlapTable("window_skew_table");
        final var statisticStorage = connectContext.getGlobalStateMgr().getStatisticStorage();
        final var skewedColumnStat = ColumnStatistic.builder().setNullsFraction(0.5).build();

        statisticStorage.addColumnStatistic(table, "p", skewedColumnStat);

        String sql = "select p, s, avg(x) over (partition by p order by s), " +
                "rank() over (partition by p order by s) from window_skew_table";

        String plan = getFragmentPlan(sql, TExplainLevel.COSTS, "");

        assertContains(plan, "Output Exprs:1: p | 2: s | 4: avg(3: x) | 5: rank()");
        assertContains(plan, "UNION");
        assertContains(plan, "Predicates: [6: p, INT, true] IS NOT NULL");
        assertContains(plan, "Predicates: [1: p, INT, true] IS NULL");
    }

    @Test
    void testWindowSkewOptimizationDisabled() throws Exception {
        connectContext.getSessionVariable().setEnableSplitWindowSkewToUnion(false);
        OlapTable table = getOlapTable("window_skew_table");
        final var statisticStorage = connectContext.getGlobalStateMgr().getStatisticStorage();
        final var skewedColumnStat = ColumnStatistic.builder().setNullsFraction(0.3).build();

        statisticStorage.addColumnStatistic(table, "p", skewedColumnStat);
        statisticStorage.getColumnStatistics(table, List.of("p", "s", "x"));

        String sql = "select p, s, sum(x) over (partition by p order by s) from window_skew_table";

        String plan = getFragmentPlan(sql);

        assertNotContains(plan, "UNION");
        assertContains(plan, "ANALYTIC");

    }

    @Test
    void testWindowWithoutOrderBy() throws Exception {
        OlapTable table = getOlapTable("window_skew_table");
        final var statisticStorage = connectContext.getGlobalStateMgr().getStatisticStorage();
        final var skewedColumnStat = ColumnStatistic.builder().setNullsFraction(0.3).build();

        statisticStorage.addColumnStatistic(table, "p", skewedColumnStat);
        statisticStorage.getColumnStatistics(table, List.of("p", "s", "x"));

        String sql = "select p, s, sum(x) over (partition by p) from window_skew_table";

        String plan = getFragmentPlan(sql);

        assertNotContains(plan, "UNION");
        assertContains(plan, "ANALYTIC");
    }

    @Test
    void testWindowWithComplexPartition() throws Exception {
        OlapTable table = getOlapTable("window_skew_table");
        final var statisticStorage = connectContext.getGlobalStateMgr().getStatisticStorage();
        final var skewedColumnStat = ColumnStatistic.builder().setNullsFraction(0.3).build();

        statisticStorage.addColumnStatistic(table, "p", skewedColumnStat);
        statisticStorage.getColumnStatistics(table, List.of("p", "s", "x"));

        // Partition by expression (case when) instead of direct column
        String sql = "select p, s, sum(x) " +
                "over (partition by case when p is null then -1 else p end order by s) from window_skew_table";

        String plan = getFragmentPlan(sql);

        assertNotContains(plan, "UNION");
        assertContains(plan, "ANALYTIC");
    }

    @Test
    void testWindowSkewHintWithJoinBeforeWindow() throws Exception {
        // Test that the skew hint works correctly when there is a join before the window function
        final var table = getOlapTable("window_skew_table");
        final var joinTable = getOlapTable("window_skew_table_join");

        final var statisticStorage = connectContext.getGlobalStateMgr().getStatisticStorage();
        final var skewedColumnStat = ColumnStatistic.builder().setNullsFraction(0.3).build();
        setTableStatistics(table, 1000);
        setTableStatistics(joinTable, 500);
        statisticStorage.addColumnStatistic(table, "p", skewedColumnStat);
        statisticStorage.getColumnStatistics(table, List.of("p", "s", "x"));

        String sql = "select a.p, a.s, b.val, sum(a.x) over (partition by a.p order by a.s) " +
                "from window_skew_table a " +
                "join [skew|b.id(NULL)] window_skew_table_join b on a.p = b.id";
        String plan = getFragmentPlan(sql, TExplainLevel.COSTS, "");

        // Verify UNION rewrite is triggered
        assertContains(plan, "UNION");
        // Verify JOIN is present in the plan
        assertContains(plan, "JOIN");
        // Verify the NULL/NOT NULL predicates for skew handling
        assertContains(plan, "IS NULL");
        assertContains(plan, "IS NOT NULL");
        // Verify ANALYTIC window function is present
        assertContains(plan, "ANALYTIC");
    }

    @Test
    void testWindowSkewHintWithExplicitNullValue() throws Exception {
        // Test that the skew hint syntax with explicit column and NULL value triggers the UNION rewrite
        // The hint [skew|p(NULL)] should be parsed and used by SplitWindowSkewToUnionRule
        String sql = "select p, s, sum(x) over ([skew|p(NULL)] partition by p order by s) from window_skew_table";
        String plan = getFragmentPlan(sql, TExplainLevel.COSTS, "");

        // Verify UNION rewrite is triggered
        assertContains(plan, "UNION");

        assertContains(plan, "Predicates: [1: p, INT, true] IS NULL");
        assertContains(plan, "Predicates: [5: p, INT, true] IS NOT NULL");

        assertContains(plan,
                "ANALYTIC\n" +
                        "  |  functions: [, sum[([3: x, INT, true]); args: INT; result: BIGINT; args nullable: true; " +
                        "result nullable: true], ]\n" +
                        "  |  order by: [2: s, INT, true] ASC");

        assertContains(plan,
                "ANALYTIC\n" +
                        "  |  functions: [, sum[([7: x, INT, true]); args: INT; result: BIGINT; args nullable: true;" +
                        " result nullable: true], ]\n" +
                        "  |  partition by: [5: p, INT, true]");
    }

    @Test
    void testWindowSkewHintWithNonNullValue() throws Exception {
        // Test skew hint with a non-null integer value triggers the UNION rewrite
        String sql = "select p, s, sum(x) over ([skew|p(1)] partition by p order by s) from window_skew_table";
        String plan = getFragmentPlan(sql, TExplainLevel.COSTS, "");

        // Verify UNION rewrite is triggered
        assertContains(plan, "UNION");

        assertContains(plan, "Predicates: [1: p, INT, true] = 1");
        assertContains(plan, "Predicates: ([5: p, INT, true] != 1) OR ([5: p, INT, true] IS NULL)");

        assertContains(plan,
                "ANALYTIC\n" +
                        "  |  functions: [, sum[([3: x, INT, true]); args: INT; result: BIGINT; args nullable: true; " +
                        "result nullable: true], ]\n" +
                        "  |  order by: [2: s, INT, true] ASC");

        assertContains(plan,
                "ANALYTIC\n" +
                        "  |  functions: [, sum[([7: x, INT, true]); args: INT; result: BIGINT; args nullable: true;" +
                        " result nullable: true], ]\n" +
                        "  |  partition by: [5: p, INT, true]");
    }

    @Test
    void testWindowSkewHintWithStringValue() throws Exception {
        // Test skew hint with a string value triggers the UNION rewrite
        starRocksAssert.withTable(
                """
                        CREATE TABLE IF NOT EXISTS `window_skew_table_str` (
                          `p` varchar(100) NULL,
                          `s` int NULL,
                          `x` int NULL
                        ) ENGINE=OLAP
                        DUPLICATE KEY(`p`, `s`, `x`)
                        DISTRIBUTED BY HASH(`p`) BUCKETS 3
                        PROPERTIES (
                          "replication_num" = "1",
                          "in_memory" = "false"
                        );
                        """
        );
        final var table = getOlapTable("window_skew_table_str");
        setTableStatistics(table, 1000);
        String sql = "select p, s, sum(x) over ([skew|p('abc')] partition by p order by s) from window_skew_table_str";
        String plan = getFragmentPlan(sql, TExplainLevel.COSTS, "");

        assertContains(plan, "UNION");
        assertContains(plan, "Predicates: [1: p, VARCHAR, true] = 'abc'");
        assertContains(plan, "Predicates: ([5: p, VARCHAR, true] != 'abc') OR ([5: p, VARCHAR, true] IS NULL)");
    }

    @Test
    void testWindowSkewHintIgnoredWhenFeatureDisabled() throws Exception {
        // Disable the skew optimization feature
        connectContext.getSessionVariable().setEnableSplitWindowSkewToUnion(false);

        // Even with explicit skew hint, the optimization should NOT be applied
        String sql = "select p, s, sum(x) over ([skew|p(NULL)] partition by p order by s) from window_skew_table";
        String plan = getFragmentPlan(sql);

        // Verify UNION rewrite is NOT triggered
        assertNotContains(plan, "UNION");
        // But the query should still work with normal ANALYTIC
        assertContains(plan, "ANALYTIC");
        assertContains(plan, "partition by: 1: p");
    }

    @Test
    void testWindowSkewHintWithWrongColumn() throws Exception {
        final var skewedColumnStat = ColumnStatistic.builder().setNullsFraction(0.0).build();

        OlapTable table = getOlapTable("window_skew_table");
        final var statisticStorage = connectContext.getGlobalStateMgr().getStatisticStorage();
        statisticStorage.addColumnStatistic(table, "p", skewedColumnStat);
        statisticStorage.getColumnStatistics(table, List.of("p", "s", "x"));

        // Test providing a skew hint for a column that is not in the partition clause
        // Skew hint on 's' but partitioned by 'p'
        String sql = "select p, s, sum(x) over ([skew|s(1)] partition by p order by s) from window_skew_table";

        assertThrows(Exception.class, () ->
                getFragmentPlan(sql)
        );

    }

    @Test
    void testWindowSkewHintWithNonConstantValue() {
        String sql = "select p, s, sum(x) over ([skew|p(s)] partition by p order by s) from window_skew_table";

        assertThrows(Exception.class, () ->
                getFragmentPlan(sql)
        );
    }

    @Test
    void testTwoWindowsOneWithSkewHint() throws Exception {
        // Case 1: Skew hint on the first window function
        String sql1 = "select p, s, " +
                "sum(x) over ([skew|p(NULL)] partition by p order by s), " +
                "avg(x) over (partition by p order by s) " +
                "from window_skew_table";

        String plan1 = getFragmentPlan(sql1, TExplainLevel.COSTS, "");
        assertContains(plan1, "UNION");
        assertContains(plan1, "Predicates: [1: p, INT, true] IS NULL");
        assertContains(plan1, "Predicates: [6: p, INT, true] IS NOT NULL");

        // Case 2: Skew hint on the second window function
        String sql2 = "select p, s, " +
                "sum(x) over (partition by p order by s), " +
                "avg(x) over ([skew|p(NULL)] partition by p order by s) " +
                "from window_skew_table";

        String plan2 = getFragmentPlan(sql2, TExplainLevel.COSTS, "");

        assertContains(plan2, "UNION");
        assertContains(plan2, "Predicates: [1: p, INT, true] IS NULL");
        assertContains(plan2, "Predicates: [6: p, INT, true] IS NOT NULL");
    }

    @Test
    void testMixedWindowPartitionsWithSkewHint() throws Exception {
        final var skewedColumnStat = ColumnStatistic.builder().setNullsFraction(0.0).build();

        OlapTable table = getOlapTable("window_skew_table");
        final var statisticStorage = connectContext.getGlobalStateMgr().getStatisticStorage();
        statisticStorage.addColumnStatistic(table, "p", skewedColumnStat);
        statisticStorage.getColumnStatistics(table, List.of("p", "s", "x"));
        // Three analytical windows:
        // 1. partition by p (with skew hint)
        // 2. partition by p (no skew hint)
        // 3. partition by s (different partition)
        String sql = "select p, s, " +
                "sum(x) over ([skew|p(NULL)] partition by p order by s), " +
                "avg(x) over (partition by p order by s), " +
                "count(x) over (partition by s order by p) " +
                "from window_skew_table";

        String plan = getFragmentPlan(sql, TExplainLevel.COSTS, "");
        assertContains(plan, "UNION");
        // Verify that the skew hint on 'p' triggered the split
        assertContains(plan, "Predicates: [1: p, INT, true] IS NULL");
        assertContains(plan, "Predicates: [7: p, INT, true] IS NOT NULL");
    }

    @Test
    void testWindowSkewHintRejectsMultipleValues() {
        // Window skew hint currently only supports a single value
        String sql = "select p, s, sum(x) over ([skew|p(1, 2)] partition by p order by s) from window_skew_table";
        Exception e = assertThrows(Exception.class, () -> getFragmentPlan(sql));
        assertContains(e.getMessage(), "Window skew hint currently supports only a single value, but got 2 values");

        String sql2 = "select p, s, sum(x) over ([skew|p(NULL, 2)] partition by p order by s) from window_skew_table";
        Exception e2 = assertThrows(Exception.class, () -> getFragmentPlan(sql2));
        assertContains(e2.getMessage(), "Window skew hint currently supports only a single value, but got 2 values");
    }

    @Test
    void testWindowSkewHintWithWrongValueType() {
        // Column 'p' is INT, but the skew hint provides a string value that cannot be cast to INT
        String sql = "select p, s, sum(x) over ([skew|p('abc')] partition by p order by s) from window_skew_table";

        Exception e = assertThrows(Exception.class, () -> getFragmentPlan(sql));
        assertContains(e.getMessage(), "Window skew hint value type mismatch");
    }

    @Test
    void testWindowSkewHintWithWrongValueTypeDate() throws Exception {
        // Create a table with a DATE partition column
        starRocksAssert.withTable(
                """
                        CREATE TABLE IF NOT EXISTS `window_skew_table_date` (
                          `p` date NULL,
                          `s` int NULL,
                          `x` int NULL
                        ) ENGINE=OLAP
                        DUPLICATE KEY(`p`, `s`, `x`)
                        DISTRIBUTED BY HASH(`p`) BUCKETS 3
                        PROPERTIES (
                          "replication_num" = "1",
                          "in_memory" = "false"
                        );
                        """
        );
        final var table = getOlapTable("window_skew_table_date");
        setTableStatistics(table, 1000);

        // Column 'p' is DATE, but the skew hint provides a non-date string value
        String sql = "select p, s, sum(x) over ([skew|p('not_a_date')] partition by p order by s) " +
                "from window_skew_table_date";

        Exception e = assertThrows(Exception.class, () -> getFragmentPlan(sql));
        assertContains(e.getMessage(), "Window skew hint value type mismatch");
    }
}
