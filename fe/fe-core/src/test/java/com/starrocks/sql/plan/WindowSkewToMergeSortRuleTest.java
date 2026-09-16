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

/**
 * Coverage for the statistics-driven WindowSkewToMergeSortRule, gated by
 * enable_window_skew_merge_sort. The [merge_sort] hint path is covered by
 * {@link WindowMergeSortHintTest}.
 */
class WindowSkewToMergeSortRuleTest extends PlanTestBase {
    private static final String TABLE_NAME = "force_merge_sort_table";
    private static final List<String> ALL_COLUMNS = List.of("p", "s", "x");
    private static final String SINGLE_PARTITION_SQL =
            "select p, s, sum(x) over (partition by s order by p) from " + TABLE_NAME;
    private static final String MULTI_PARTITION_SQL =
            "select p, s, sum(x) over (partition by p, s order by x) from " + TABLE_NAME;

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

    private String getCostPlan(String sql) throws Exception {
        return getFragmentPlan(sql, TExplainLevel.COSTS, "");
    }

    private void refreshAndSetColumnStat(String column, ColumnStatistic stat) {
        storage().refreshColumnStatistics(table(), ALL_COLUMNS, true);
        storage().addColumnStatistic(table(), column, stat);
        storage().getColumnStatistics(table(), ALL_COLUMNS);
    }

    private void setSkewedStatsForS() {
        refreshAndSetColumnStat("s", ColumnStatistic.builder().setNullsFraction(0.3).build());
    }

    private void setNonSkewedStatsForS() {
        refreshAndSetColumnStat("s", ColumnStatistic.builder().setNullsFraction(0.05).build());
    }

    private void setStatsForPAndS(ColumnStatistic pStat, ColumnStatistic sStat) {
        storage().refreshColumnStatistics(table(), ALL_COLUMNS, true);
        storage().addColumnStatistic(table(), "p", pStat);
        storage().addColumnStatistic(table(), "s", sStat);
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
    void testSinglePartitionSkewedDoesNotTrigger() throws Exception {
        connectContext.getSessionVariable().setEnableWindowSkewMergeSort(true);
        setSkewedStatsForS();

        // Single partition column is SplitWindowSkewToUnionRule's job, so this rule must not fire
        assertRuleNotApplied(getCostPlan(SINGLE_PARTITION_SQL));
    }

    @Test
    void testNonSkewedPartitionKeepsShuffleDistribution() throws Exception {
        connectContext.getSessionVariable().setEnableWindowSkewMergeSort(true);
        setNonSkewedStatsForS();

        assertRuleNotApplied(getCostPlan(SINGLE_PARTITION_SQL));
    }

    @Test
    void testSinglePartitionMCVSkewedDoesNotTrigger() throws Exception {
        connectContext.getSessionVariable().setEnableWindowSkewMergeSort(true);

        Histogram histogram = new Histogram(List.of(), Map.of("1", 300L));
        refreshAndSetColumnStat("s",
                ColumnStatistic.builder().setNullsFraction(0.0).setHistogram(histogram).build());

        // Single partition column: not triggered even with most-common-value skew
        assertRuleNotApplied(getCostPlan(SINGLE_PARTITION_SQL));
    }

    @Test
    void testMultiPartitionSingleSkewedColumnDoesNotTrigger() throws Exception {
        connectContext.getSessionVariable().setEnableWindowSkewMergeSort(true);
        setStatsForPAndS(
                ColumnStatistic.builder().setNullsFraction(0.3).build(),
                ColumnStatistic.builder().setNullsFraction(0.05).setDistinctValuesCount(900).build());

        // Skew on only one of the partition columns is not enough; every partition column
        // must be skewed for the rewrite to fire.
        assertRuleNotApplied(getCostPlan(MULTI_PARTITION_SQL));
    }

    @Test
    void testMultiPartitionAllColumnsSkewedTriggers() throws Exception {
        connectContext.getSessionVariable().setEnableWindowSkewMergeSort(true);
        setStatsForPAndS(
                ColumnStatistic.builder().setNullsFraction(0.3).build(),
                ColumnStatistic.builder().setNullsFraction(0.3).build());

        assertRuleApplied(getCostPlan(MULTI_PARTITION_SQL));
    }

    @Test
    void testMultiPartitionNoneSkewed() throws Exception {
        connectContext.getSessionVariable().setEnableWindowSkewMergeSort(true);
        setStatsForPAndS(
                ColumnStatistic.builder().setNullsFraction(0.05).build(),
                ColumnStatistic.builder().setNullsFraction(0.05).build());

        assertRuleNotApplied(getCostPlan(MULTI_PARTITION_SQL));
    }

    @Test
    void testDoesNotFireWhenDisabled() throws Exception {
        // Both variables are off (set in @BeforeEach)
        setStatsForPAndS(
                ColumnStatistic.builder().setNullsFraction(0.3).build(),
                ColumnStatistic.builder().setNullsFraction(0.3).build());

        // Without the rule enabled, even with skewed stats, normal shuffle is used
        assertRuleNotApplied(getCostPlan(MULTI_PARTITION_SQL));
    }

    @Test
    void testAlreadyForcedViaHintIsIdempotent() throws Exception {
        connectContext.getSessionVariable().setEnableWindowSkewMergeSort(true);
        setStatsForPAndS(
                ColumnStatistic.builder().setNullsFraction(0.3).build(),
                ColumnStatistic.builder().setNullsFraction(0.3).build());

        // The hint already set the flag, so check() short-circuits and the rule skips
        String sql = "select p, s, sum(x) over ([merge_sort] partition by p, s order by x) from " + TABLE_NAME;
        assertRuleApplied(getCostPlan(sql));
    }

    @Test
    void testNoPartitionWindowIsUnaffected() throws Exception {
        connectContext.getSessionVariable().setEnableWindowSkewMergeSort(true);
        setSkewedStatsForS();

        // Window without PARTITION BY - rule should not apply (check() returns false)
        String plan = getCostPlan("select p, s, row_number() over (order by s) from " + TABLE_NAME);
        assertContains(plan, "ANALYTIC");
    }
}
