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
import com.starrocks.sql.optimizer.statistics.StatisticStorage;
import com.starrocks.statistic.StatisticsMetaManager;
import com.starrocks.thrift.TExplainLevel;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

/**
 * Plan-level coverage for the [merge_sort] window hint. The hint forces the analytic to consume a
 * single globally-ordered stream, which shows up in the plan as a MERGING-EXCHANGE below ANALYTIC
 * instead of a hash-partitioned EXCHANGE.
 */
class WindowMergeSortHintTest extends PlanTestBase {
    private static final String TABLE_NAME = "merge_sort_hint_table";
    private static final List<String> ALL_COLUMNS = List.of("p", "s", "x");
    private static final String HINTED_SQL =
            "select p, s, sum(x) over ([merge_sort] partition by s order by p) from " + TABLE_NAME;

    @BeforeAll
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        FeConstants.runningUnitTest = true;
        Config.enable_sync_statistics_load = true;
        connectContext.getGlobalStateMgr().setStatisticStorage(new CachedStatisticStorage());

        starRocksAssert.withTable(
                """
                        CREATE TABLE `merge_sort_hint_table` (
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
        connectContext.getSessionVariable().setEnableSplitWindowSkewToUnion(false);
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

    private void assertMergeSortApplied(String plan) {
        assertContains(plan, "ANALYTIC");
        assertContains(plan, "MERGING-EXCHANGE");
    }

    @Test
    void testUnhintedWindowKeepsShuffleDistribution() throws Exception {
        setSkewedStatsForS();

        String plan = getCostPlan("select p, s, sum(x) over (partition by s order by p) from " + TABLE_NAME);
        assertContains(plan, "ANALYTIC");
        assertNotContains(plan, "MERGING-EXCHANGE");
    }

    @Test
    void testHintForcesMergeSortWithNonSkewedStats() throws Exception {
        setNonSkewedStatsForS();

        // The hint is unconditional: it does not consult statistics.
        assertMergeSortApplied(getCostPlan(HINTED_SQL));
    }

    @Test
    void testHintForcesMergeSortWithSkewedStats() throws Exception {
        setSkewedStatsForS();

        assertMergeSortApplied(getCostPlan(HINTED_SQL));
    }

    @Test
    void testHintTakesPrecedenceOverSplitWindowSkewToUnion() throws Exception {
        connectContext.getSessionVariable().setEnableSplitWindowSkewToUnion(true);
        setSkewedStatsForS();

        String plan = getCostPlan(HINTED_SQL);
        assertMergeSortApplied(plan);
        assertNotContains(plan, "UNION");
    }

    @Test
    void testHintOnMultiColumnPartition() throws Exception {
        setSkewedStatsForS();

        String sql = "select p, s, sum(x) over ([merge_sort] partition by p, s order by x) from " + TABLE_NAME;
        assertMergeSortApplied(getCostPlan(sql));
    }

    @Test
    void testHintRequiresOnlyOneHint() {
        setSkewedStatsForS();

        starRocksAssert.query("select p, s, sum(x) over ([merge_sort,hash] partition by s) from " + TABLE_NAME)
                .analysisError("The merge_sort hint cannot be combined with any other hint");

        starRocksAssert.query("select p, s, sum(x) over ([merge_sort,skewed] partition by s) from " + TABLE_NAME)
                .analysisError("The merge_sort hint cannot be combined with any other hint");

        starRocksAssert.query("select p, s, sum(x) over ([merge_sort,skewed,hash] partition by s) from " + TABLE_NAME)
                .analysisError("The merge_sort hint cannot be combined with any other hint");

        starRocksAssert.query("select p, s, sum(x) over ([sort,merge_sort] partition by s) from " + TABLE_NAME)
                .analysisError("The merge_sort hint cannot be combined with any other hint");
    }

    @Test
    void testHintRequiresOrderBy() {
        starRocksAssert.query("select p, s, sum(x) over ([merge_sort] partition by s) from " + TABLE_NAME)
                .analysisError("The merge_sort hint requires an ORDER BY clause in the window specification.");
    }
}
