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
import com.starrocks.sql.optimizer.statistics.StatisticStorage;
import com.starrocks.statistic.StatisticsMetaManager;
import com.starrocks.thrift.TExplainLevel;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class WindowSkewTest extends PlanTestBase {
    private static final String TABLE_NAME = "window_skew_table";
    private static final List<String> ALL_COLUMNS = List.of("p", "s", "x");
    private static final String BASIC_WINDOW_SQL =
            "select p, s, sum(x) over (partition by p order by s) from " + TABLE_NAME;

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

        final var table = table();
        setTableStatistics(table, 1000);
    }

    private OlapTable table() {
        return getOlapTable(TABLE_NAME);
    }

    private StatisticStorage storage() {
        return connectContext.getGlobalStateMgr().getStatisticStorage();
    }

    private void setColumnStatForP(double nullsFraction) {
        final var stat = ColumnStatistic.builder().setNullsFraction(nullsFraction).build();
        storage().addColumnStatistic(table(), "p", stat);
        storage().getColumnStatistics(table(), ALL_COLUMNS);
    }

    private void refreshAndSetColumnStatForP(ColumnStatistic stat) {
        storage().refreshColumnStatistics(table(), ALL_COLUMNS, true);
        storage().addColumnStatistic(table(), "p", stat);
        storage().getColumnStatistics(table(), ALL_COLUMNS);
    }

    private String getCostPlan(String sql) throws Exception {
        return getFragmentPlan(sql, TExplainLevel.COSTS, "");
    }

    private void assertPlanHasUnionAndAnalytic(String plan, int expectedAnalyticCount) {
        assertContains(plan, "UNION");
        long analyticCount = plan.lines().filter(l -> l.contains("ANALYTIC")).count();
        assertEquals(expectedAnalyticCount, analyticCount,
                "Expected exactly 2 ANALYTIC operators after UNION split, but found " + analyticCount);
    }

    private void assertPlanHasUnionAndAnalytic(String plan) {
        assertPlanHasUnionAndAnalytic(plan, 2);
    }

    private void assertPlanHasNoUnionButAnalytic(String plan) {
        assertNotContains(plan, "UNION");
        assertContains(plan, "ANALYTIC");
    }

    private void assertNullSplit(String plan) {
        assertContains(plan, "IS NOT NULL");
        assertContains(plan, "IS NULL");
    }

    @Test
    void testWindowWithSkew() throws Exception {
        refreshAndSetColumnStatForP(
                ColumnStatistic.builder().setNullsFraction(0.3).build());

        String plan = getCostPlan(BASIC_WINDOW_SQL);

        assertContains(plan, "Output Exprs:1: p | 2: s | 4: sum(3: x)");
        assertPlanHasUnionAndAnalytic(plan);
        assertNullSplit(plan);
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
        setColumnStatForP(0.1);

        String plan = getFragmentPlan(BASIC_WINDOW_SQL);

        assertPlanHasNoUnionButAnalytic(plan);
    }

    @Test
    void testWindowWithMultipleSkewedColumns() throws Exception {
        final var skewedP = ColumnStatistic.builder().setNullsFraction(0.4).build();
        final var skewedS = ColumnStatistic.builder().setNullsFraction(0.4).build();

        storage().refreshColumnStatistics(table(), ALL_COLUMNS, true);
        storage().addColumnStatistic(table(), "p", skewedP);
        storage().addColumnStatistic(table(), "s", skewedS);
        storage().getColumnStatistics(table(), ALL_COLUMNS);

        // Partition by multiple columns
        String sql = "select p, s, sum(x) over (partition by p, s order by x) from " + TABLE_NAME;

        String plan = getCostPlan(sql);

        assertPlanHasNoUnionButAnalytic(plan);
    }

    @Test
    void testWindowWithMCVSkew() throws Exception {
        Histogram histogram = new Histogram(
                /* buckets */ List.of(),
                /* mcv */ Map.of("1", 300L));

        refreshAndSetColumnStatForP(
                ColumnStatistic.builder().setNullsFraction(0.0).setHistogram(histogram).build());

        String plan = getCostPlan(BASIC_WINDOW_SQL);

        assertContains(plan, "UNION");
        assertContains(plan, "Predicates: [1: p, INT, true] = 1");
        // Ensure that unskewed partition preserves NULLs
        assertContains(plan, "Predicates: ([5: p, INT, true] != 1) OR ([5: p, INT, true] IS NULL)");

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
        setColumnStatForP(0.5);

        String sql = "select p, s, avg(x) over (partition by p order by s), " +
                "rank() over (partition by p order by s) from " + TABLE_NAME;

        String plan = getCostPlan(sql);

        assertContains(plan, "Output Exprs:1: p | 2: s | 4: avg(3: x) | 5: rank()");
        assertContains(plan, "UNION");
        assertContains(plan, "Predicates: [6: p, INT, true] IS NOT NULL");
        assertContains(plan, "Predicates: [1: p, INT, true] IS NULL");
    }

    @Test
    void testWindowSkewOptimizationDisabled() throws Exception {
        connectContext.getSessionVariable().setEnableSplitWindowSkewToUnion(false);
        setColumnStatForP(0.3);

        String plan = getFragmentPlan(BASIC_WINDOW_SQL);

        assertPlanHasNoUnionButAnalytic(plan);
    }

    @Test
    void testWindowWithoutOrderBy() throws Exception {
        setColumnStatForP(0.3);

        String sql = "select p, s, sum(x) over (partition by p) from " + TABLE_NAME;

        String plan = getFragmentPlan(sql);

        assertPlanHasNoUnionButAnalytic(plan);
    }

    @Test
    void testWindowWithComplexPartition() throws Exception {
        // Test that the rewrite is not triggered if the complex expr stats return a lower
        // null fraction. In our case null fractions from the CASE WHEN should be (1 - 0.2) * 0.2 = 0.16 --> below threshold
        setColumnStatForP(0.2);

        // Partition by expression (case when) instead of direct column
        String sql = "select p, s, sum(x) " +
                "over (partition by case when p is null then -1 else p end order by s) from " + TABLE_NAME;

        String plan = getFragmentPlan(sql);

        assertPlanHasNoUnionButAnalytic(plan);
    }

    @Test
    void testWindowSkewHintWithJoinBeforeWindow() throws Exception {
        // Test that the skew hint works correctly when there is a join before the window function
        final var joinTable = getOlapTable("window_skew_table_join");

        setTableStatistics(table(), 1000);
        setTableStatistics(joinTable, 500);
        setColumnStatForP(0.3);

        String sql = "select a.p, a.s, b.val, sum(a.x) over (partition by a.p order by a.s) " +
                "from " + TABLE_NAME + " a " +
                "join [skew|b.id(NULL)] window_skew_table_join b on a.p = b.id";
        String plan = getCostPlan(sql);

        // Verify UNION rewrite is triggered
        assertContains(plan, "UNION");
        // Verify JOIN is present in the plan
        assertContains(plan, "JOIN");
        // Verify the NULL/NOT NULL predicates for skew handling
        assertNullSplit(plan);
        // Verify ANALYTIC window function is present
        assertContains(plan, "ANALYTIC");
    }

    @Test
    void testWindowSkewHintWithExplicitNullValue() throws Exception {
        // Test that the skew hint syntax with explicit column and NULL value triggers the UNION rewrite
        // The hint [skew|p(NULL)] should be parsed and used by SplitWindowSkewToUnionRule
        String sql = "select p, s, sum(x) over ([skew|p(NULL)] partition by p order by s) from " + TABLE_NAME;
        String plan = getCostPlan(sql);

        // Verify UNION rewrite is triggered
        assertContains(plan, "UNION");
        assertNullSplit(plan);

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
        String sql = "select p, s, sum(x) over ([skew|p(1)] partition by p order by s) from " + TABLE_NAME;
        String plan = getCostPlan(sql);

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
        String plan = getCostPlan(sql);

        assertContains(plan, "UNION");
        assertContains(plan, "Predicates: [1: p, VARCHAR, true] = 'abc'");
        assertContains(plan, "Predicates: ([5: p, VARCHAR, true] != 'abc') OR ([5: p, VARCHAR, true] IS NULL)");
    }

    @Test
    void testWindowSkewHintIgnoredWhenFeatureDisabled() throws Exception {
        // Disable the skew optimization feature
        connectContext.getSessionVariable().setEnableSplitWindowSkewToUnion(false);

        // Even with explicit skew hint, the optimization should NOT be applied
        String sql = "select p, s, sum(x) over ([skew|p(NULL)] partition by p order by s) from " + TABLE_NAME;
        String plan = getFragmentPlan(sql);

        // Verify UNION rewrite is NOT triggered
        assertPlanHasNoUnionButAnalytic(plan);
        assertContains(plan, "partition by: 1: p");
    }

    @Test
    void testWindowSkewHintWithWrongColumn() throws Exception {
        setColumnStatForP(0.0);

        // Test providing a skew hint for a column that is not in the partition clause
        // Skew hint on 's' but partitioned by 'p'
        String sql = "select p, s, sum(x) over ([skew|s(1)] partition by p order by s) from " + TABLE_NAME;

        assertThrows(Exception.class, () ->
                getFragmentPlan(sql)
        );

    }

    @Test
    void testWindowSkewHintWithNonConstantValue() {
        String sql = "select p, s, sum(x) over ([skew|p(s)] partition by p order by s) from " + TABLE_NAME;

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
                "from " + TABLE_NAME;

        String plan1 = getCostPlan(sql1);
        assertContains(plan1, "UNION");
        assertNullSplit(plan1);

        // Case 2: Skew hint on the second window function
        String sql2 = "select p, s, " +
                "sum(x) over (partition by p order by s), " +
                "avg(x) over ([skew|p(NULL)] partition by p order by s) " +
                "from " + TABLE_NAME;

        String plan2 = getCostPlan(sql2);

        assertContains(plan2, "UNION");
        assertNullSplit(plan2);
    }

    @Test
    void testMixedWindowPartitionsWithSkewHint() throws Exception {
        setColumnStatForP(0.0);

        // Three analytical windows:
        // 1. partition by p (with skew hint)
        // 2. partition by p (no skew hint)
        // 3. partition by s (different partition)
        String sql = "select p, s, " +
                "sum(x) over ([skew|p(NULL)] partition by p order by s), " +
                "avg(x) over (partition by p order by s), " +
                "count(x) over (partition by s order by p) " +
                "from " + TABLE_NAME;

        String plan = getCostPlan(sql);
        assertContains(plan, "UNION");
        // Verify that the skew hint on 'p' triggered the split
        assertNullSplit(plan);
    }

    @Test
    void testWindowSkewHintRejectsMultipleValues() {
        // Window skew hint currently only supports a single value
        String sql = "select p, s, sum(x) over ([skew|p(1, 2)] partition by p order by s) from " + TABLE_NAME;
        Exception e = assertThrows(Exception.class, () -> getFragmentPlan(sql));
        assertContains(e.getMessage(), "Window skew hint currently supports only a single value, but got 2 values");

        String sql2 = "select p, s, sum(x) over ([skew|p(NULL, 2)] partition by p order by s) from " + TABLE_NAME;
        Exception e2 = assertThrows(Exception.class, () -> getFragmentPlan(sql2));
        assertContains(e2.getMessage(), "Window skew hint currently supports only a single value, but got 2 values");
    }

    @Test
    void testWindowSkewHintWithWrongValueType() {
        // Column 'p' is INT, but the skew hint provides a string value that cannot be cast to INT
        String sql = "select p, s, sum(x) over ([skew|p('abc')] partition by p order by s) from " + TABLE_NAME;

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

    @Test
    void testWindowWithAggBug() throws Exception {
        String sql = "with cte_0 as (\n" +
                "    select  p, s as o,x+1 as s from window_skew_table\n" +
                "),\n" +
                "cte_1 as (\n" +
                "    select p, s as o, sum(s) over ([skew|p(NULL)]partition by p order by s) as s from cte_0\n" +
                "),\n" +
                "cte_2 as (\n" +
                "    select p, o, s from cte_1\n" +
                "    union all\n" +
                "    select p, o, s from cte_0\n" +
                ")\n" +
                "select count(distinct s), count(distinct o) from cte_2;";

        setColumnStatForP(0.1);

        String plan = getCostPlan(sql);
        assertPlanHasUnionAndAnalytic(plan);
    }

    @Test
    void testWindowSkewStatisticsBasedWithCTEAndMultiDistinct() throws Exception {
        Histogram histogram = new Histogram(List.of(), Map.of("1", 300L));
        refreshAndSetColumnStatForP(
                ColumnStatistic.builder().setNullsFraction(0.0).setHistogram(histogram).build());

        String sql = "with cte_0 as (\n" +
                "    select p, s, x from window_skew_table\n" +
                "),\n" +
                "cte_1 as (\n" +
                "    select p, s, sum(x) over (partition by p order by s) as wx from cte_0\n" +
                "),\n" +
                "cte_2 as (\n" +
                "    select p, s, wx from cte_1\n" +
                "    union all\n" +
                "    select p, s, x from cte_0\n" +
                ")\n" +
                "select count(distinct wx), count(distinct s) from cte_2;";

        String plan = getCostPlan(sql);
        assertPlanHasUnionAndAnalytic(plan);
    }

    @Test
    void testWindowSkewWithJoinedCTEConsumersAndMultiDistinct() throws Exception {
        String sql = "with base as (\n" +
                "    select p, s, x from window_skew_table\n" +
                "),\n" +
                "joined as (\n" +
                "    select a.p, a.s, a.x + b.x as combined_x\n" +
                "    from base a join base b on a.p = b.p\n" +
                "),\n" +
                "windowed as (\n" +
                "    select p, s, sum(combined_x) over ([skew|p(NULL)] partition by p order by s) as wx from joined\n" +
                ")\n" +
                "select count(distinct wx), count(distinct s) from windowed;";

        setColumnStatForP(0.1);

        String plan = getCostPlan(sql);
        assertContains(plan, "ANALYTIC");
    }

    @Test
    void testWindowSkewWithCTEUsedInSubqueryAndMultiDistinct() throws Exception {
        String sql = "with base as (\n" +
                "    select p, s, x from window_skew_table\n" +
                "),\n" +
                "windowed as (\n" +
                "    select p, s, sum(x) over ([skew|p(NULL)] partition by p order by s) as wx from base\n" +
                "),\n" +
                "agg_source as (\n" +
                "    select w.p, w.s, w.wx, b.x as orig_x\n" +
                "    from windowed w\n" +
                "    join base b on w.p = b.p\n" +
                ")\n" +
                "select count(distinct wx), count(distinct orig_x) from agg_source;";

        setColumnStatForP(0.1);

        String plan = getCostPlan(sql);
        assertPlanHasUnionAndAnalytic(plan);
    }

    @Test
    void testWindowSkewMCVHintWithCTEAndMultiDistinct() throws Exception {
        String sql = "with cte_0 as (\n" +
                "    select p, s as o, x+1 as s from window_skew_table\n" +
                "),\n" +
                "cte_1 as (\n" +
                "    select p, s as o, sum(s) over ([skew|p(1)] partition by p order by s) as s from cte_0\n" +
                "),\n" +
                "cte_2 as (\n" +
                "    select p, o, s from cte_1\n" +
                "    union all\n" +
                "    select p, o, s from cte_0\n" +
                ")\n" +
                "select count(distinct s), count(distinct o) from cte_2;";

        setColumnStatForP(0.1);

        String plan = getCostPlan(sql);
        assertPlanHasUnionAndAnalytic(plan);
    }

    @Test
    void testTwoSkewedWindowsOverSharedCTEWithMultiDistinct() throws Exception {
        String sql = "with shared as (\n" +
                "    select p, s, x from window_skew_table\n" +
                "),\n" +
                "win1 as (\n" +
                "    select p, s, sum(x) over ([skew|p(NULL)] partition by p order by s) as wx from shared\n" +
                "),\n" +
                "win2 as (\n" +
                "    select p, s, avg(x) over ([skew|p(NULL)] partition by p order by s) as ax from shared\n" +
                "),\n" +
                "combined as (\n" +
                "    select p, s, wx as val from win1\n" +
                "    union all\n" +
                "    select p, s, cast(ax as bigint) as val from win2\n" +
                "    union all\n" +
                "    select p, s, x from shared\n" +
                ")\n" +
                "select count(distinct val), count(distinct s) from combined;";

        setColumnStatForP(0.1);

        String plan = getCostPlan(sql);
        assertPlanHasUnionAndAnalytic(plan, 4);
    }

    @Test
    void testWindowSkewDuplicatorRemapsSkewColumn() throws Exception {

        String sql = "select p, s, " +
                "sum(x) over ([skew|p(NULL)] partition by p order by s), " +
                "avg(x) over ([skew|p(NULL)] partition by p order by x) " +
                "from " + TABLE_NAME;

        setColumnStatForP(0.1);

        String plan = getCostPlan(sql);

        assertContains(plan, "UNION");
        assertContains(plan, "ANALYTIC");
        assertNullSplit(plan);
    }
}
