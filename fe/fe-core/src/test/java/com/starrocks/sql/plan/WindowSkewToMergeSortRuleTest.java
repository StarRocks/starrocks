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
import com.starrocks.common.Config;
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

class WindowSkewToMergeSortRuleTest extends PlanTestBase {
    private static final String TABLE_NAME = "force_merge_sort_table";
    private static final List<String> ALL_COLUMNS = List.of("p", "s", "x");
    private static final String BASIC_WINDOW_SQL =
            "select p, s, sum(x) over (partition by s order by p) from " + TABLE_NAME;
    private static final String BASIC_WINDOW_HINTED_SQL =
            "select p, s, sum(x) over ([merge_sort] partition by s order by p) from " + TABLE_NAME;

    @BeforeAll
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        FeConstants.runningUnitTest = true;
        Config.enable_sync_statistics_load = true;
        connectContext.getGlobalStateMgr().setStatisticStorage(new CachedStatisticStorage());

        starRocksAssert.withTable(
                """
                        CREATE TABLE `force_merge_sort_table` (
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

        if (!starRocksAssert.databaseExist("_statistics_")) {
            StatisticsMetaManager m = new StatisticsMetaManager();
            m.createStatisticsTablesForTest();
        }
    }

    @BeforeEach
    @Override
    public void setUp() {
        super.setUp();
        // Disable all skew-related rules by default; each test enables what it needs
        connectContext.getSessionVariable().setEnableSplitWindowSkewToUnion(false);
        connectContext.getSessionVariable().setEnableWindowSkewMergeSort(false);

        setTableStatistics(table(), 1000);
    }

    private OlapTable table() {
        return getOlapTable(TABLE_NAME);
    }

    private StatisticStorage storage() {
        return connectContext.getGlobalStateMgr().getStatisticStorage();
    }

    private void setSkewedStatsForS() {
        refreshAndSetColumnStat("s",
                ColumnStatistic.builder().setNullsFraction(0.3).build());
    }

    private void setNonSkewedStatsForS() {
        refreshAndSetColumnStat("s",
                ColumnStatistic.builder().setNullsFraction(0.05).build());
    }

    private String getCostPlan(String sql) throws Exception {
        return getFragmentPlan(sql, TExplainLevel.COSTS, "");
    }

    private void refreshAndSetColumnStat(String column, ColumnStatistic stat) {
        storage().refreshColumnStatistics(table(), ALL_COLUMNS, true);
        storage().addColumnStatistic(table(), column, stat);
        storage().getColumnStatistics(table(), ALL_COLUMNS);
    }

    private void assertRuleApplied(String plan) {
        assertContains(plan, "ANALYTIC");
        // When the rule fires, forceMergeSort causes a MERGING-EXCHANGE before ANALYTIC
        // instead of a regular hash-partitioned EXCHANGE
        assertContains(plan, "MERGING-EXCHANGE");
    }

    private void assertRuleNotApplied(String plan) {
        assertContains(plan, "ANALYTIC");
        assertNotContains(plan, "MERGING-EXCHANGE");
    }

    @Test
    void testSinglePartitionSkewedDoesNotTriggerEnableMode() throws Exception {
        connectContext.getSessionVariable().setEnableWindowSkewMergeSort(true);
        setSkewedStatsForS();

        // Single partition column - enable mode should NOT trigger
        String plan = getCostPlan(BASIC_WINDOW_SQL);
        assertRuleNotApplied(plan);
    }

    @Test
    void testNonSkewedPartitionKeepsShuffleDistribution() throws Exception {
        connectContext.getSessionVariable().setEnableWindowSkewMergeSort(true);
        setNonSkewedStatsForS();

        String plan = getCostPlan(BASIC_WINDOW_SQL);
        assertRuleNotApplied(plan);
    }

    @Test
    void testSinglePartitionMCVSkewedDoesNotTriggerEnableMode() throws Exception {
        connectContext.getSessionVariable().setEnableWindowSkewMergeSort(true);

        Histogram histogram = new Histogram(List.of(), Map.of("1", 300L));
        refreshAndSetColumnStat("s",
                ColumnStatistic.builder().setNullsFraction(0.0).setHistogram(histogram).build());

        // Single partition column - enable mode should NOT trigger even with MCV skew
        String plan = getCostPlan(BASIC_WINDOW_SQL);
        assertRuleNotApplied(plan);
    }

    @Test
    void testHintForcesOptimizationWithNonSkewedStats() throws Exception {
        setNonSkewedStatsForS();

        String plan = getCostPlan(BASIC_WINDOW_HINTED_SQL);
        assertRuleApplied(plan);
    }

    @Test
    void testAlreadyForcedViaHintIsIdempotent() throws Exception {
        connectContext.getSessionVariable().setEnableWindowSkewMergeSort(false);
        setSkewedStatsForS();

        // Use the [merge_sort] hint to already force the flag
        String sql = "select p, s, sum(x) over ([merge_sort] partition by s order by p) from " + TABLE_NAME;
        String plan = getCostPlan(sql);

        // Should still work without error - hint already set the flag, rule skips
        assertRuleApplied(plan);
    }

    @Test
    void testHintTakesPrecedenceOverSplitWindowSkewToUnion() throws Exception {
        connectContext.getSessionVariable().setEnableSplitWindowSkewToUnion(true);
        setSkewedStatsForS();

        String plan = getCostPlan(BASIC_WINDOW_HINTED_SQL);
        assertRuleApplied(plan);
        assertNotContains(plan, "UNION");
    }

    @Test
    void testMultiPartitionSingleSkewedColumnDoesNotTrigger() throws Exception {
        connectContext.getSessionVariable().setEnableWindowSkewMergeSort(true);

        storage().refreshColumnStatistics(table(), ALL_COLUMNS, true);
        storage().addColumnStatistic(table(), "p",
                ColumnStatistic.builder().setNullsFraction(0.3).build());
        storage().addColumnStatistic(table(), "s",
                ColumnStatistic.builder().setNullsFraction(0.05).setDistinctValuesCount(900).build());
        storage().getColumnStatistics(table(), ALL_COLUMNS);

        String sql = "select p, s, sum(x) over (partition by p, s order by x) from " + TABLE_NAME;
        String plan = getCostPlan(sql);
        // Skew on only one of the partition columns is not enough; every partition column
        // must be skewed for the rewrite to fire.
        assertRuleNotApplied(plan);
    }

    @Test
    void testMultiPartitionAllColumnsSkewedTriggers() throws Exception {
        connectContext.getSessionVariable().setEnableWindowSkewMergeSort(true);

        storage().refreshColumnStatistics(table(), ALL_COLUMNS, true);
        storage().addColumnStatistic(table(), "p",
                ColumnStatistic.builder().setNullsFraction(0.3).build());
        storage().addColumnStatistic(table(), "s",
                ColumnStatistic.builder().setNullsFraction(0.3).build());
        storage().getColumnStatistics(table(), ALL_COLUMNS);

        String sql = "select p, s, sum(x) over (partition by p, s order by x) from " + TABLE_NAME;
        String plan = getCostPlan(sql);
        assertRuleApplied(plan);
    }

    @Test
    void testMultiPartitionNoneSkewed() throws Exception {
        connectContext.getSessionVariable().setEnableWindowSkewMergeSort(true);

        storage().refreshColumnStatistics(table(), ALL_COLUMNS, true);
        storage().addColumnStatistic(table(), "p",
                ColumnStatistic.builder().setNullsFraction(0.05).build());
        storage().addColumnStatistic(table(), "s",
                ColumnStatistic.builder().setNullsFraction(0.05).build());
        storage().getColumnStatistics(table(), ALL_COLUMNS);

        String sql = "select p, s, sum(x) over (partition by p, s order by x) from " + TABLE_NAME;
        String plan = getCostPlan(sql);
        assertRuleNotApplied(plan);
    }

    @Test
    void testDoesNotFireWhenDisabled() throws Exception {
        // Both variables are off (set in @BeforeEach)
        setSkewedStatsForS();

        String plan = getCostPlan(BASIC_WINDOW_SQL);
        // Without the rule enabled, even with skewed stats, normal shuffle is used
        assertRuleNotApplied(plan);
    }

    @Test
    void testNoPartitionWindowIsUnaffected() throws Exception {
        connectContext.getSessionVariable().setEnableWindowSkewMergeSort(true);
        setSkewedStatsForS();

        // Window without PARTITION BY - rule should not apply (check() returns false)
        String sql = "select p, s, row_number() over (order by s) from " + TABLE_NAME;
        String plan = getCostPlan(sql);
        // This should still work; the rule doesn't touch non-partitioned windows
        assertContains(plan, "ANALYTIC");
    }
}
