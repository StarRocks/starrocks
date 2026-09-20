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

import com.starrocks.connector.jdbc.MockedJDBCMetadata;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

/**
 * Whether statistics a JDBC catalog reports actually reach the optimizer.
 *
 * <p>The connector is stubbed here; what is under test is the calculator that consumes it. The
 * generic external-table path used to rebuild the column statistics from
 * {@code _statistics_.column_statistics} keyed by internal table id, which a JDBC catalog never has
 * a row in — so whatever the source knew was read and then thrown away.
 */
public class JDBCStatisticsPlanTest extends ConnectorPlanTestBase {

    private static final String TBL0 = "jdbc0.partitioned_db0.tbl0";
    private static final String TBL1 = "jdbc0.partitioned_db0.tbl1";

    @BeforeAll
    public static void beforeClass() throws Exception {
        ConnectorPlanTestBase.beforeClass();
    }

    @BeforeEach
    public void setUp() {
        // Keep the two scans separate: with pushdown on, the join leaves as one merged scan and
        // there is no per-table estimate left to look at.
        connectContext.getSessionVariable().setEnableJdbcJoinPushDown(false);
        connectContext.getSessionVariable().setEnableJdbcAggPushDown(false);
        MockedJDBCMetadata.clearTableStatistics();
    }

    @AfterEach
    public void tearDown() {
        MockedJDBCMetadata.clearTableStatistics();
    }

    private static ColumnStatistic withDistinctValues(double distinctValues) {
        return ColumnStatistic.builder()
                .setDistinctValuesCount(distinctValues)
                .setNullsFraction(0)
                .setAverageRowSize(4)
                .setType(ColumnStatistic.StatisticType.ESTIMATE)
                .build();
    }

    /** Cardinality reported for the JDBC scan of {@code table} in a cost explain. */
    private static long scanCardinality(String costExplain, String table) {
        int scanAt = costExplain.indexOf("TABLE: `" + table + "`");
        Assertions.assertTrue(scanAt >= 0, "no scan of " + table + " in:\n" + costExplain);
        int cardinalityAt = costExplain.indexOf("cardinality:", scanAt);
        Assertions.assertTrue(cardinalityAt >= 0, "no cardinality after the scan in:\n" + costExplain);
        String tail = costExplain.substring(cardinalityAt + "cardinality:".length()).trim();
        return Long.parseLong(tail.split("\\s+")[0]);
    }

    /** The column-statistic line the scan reports for {@code column}. */
    private static String scanColumnStatistic(String costExplain, String table, String column) {
        int scanAt = costExplain.indexOf("TABLE: `" + table + "`");
        Assertions.assertTrue(scanAt >= 0, "no scan of " + table + " in:\n" + costExplain);
        int at = costExplain.indexOf("* " + column + "-->", scanAt);
        Assertions.assertTrue(at >= 0, "no statistic for " + column + " in:\n" + costExplain);
        return costExplain.substring(at, costExplain.indexOf('\n', at)).trim();
    }

    @Test
    public void testEqualityPredicateIsEstimatedFromTheSourcesDistinctCount() throws Exception {
        // A million rows over a hundred companies. The customer's shape: `company_id = 42` should
        // come out around ten thousand rows, which is what makes the optimizer pick the filtered
        // relation as the build side.
        MockedJDBCMetadata.setTableStatistics("tbl0", 1_000_000L, Map.of("c", withDistinctValues(100)));
        MockedJDBCMetadata.setTableStatistics("tbl1", 100L, Map.of());

        String plan = getCostExplain("select t1.a, t2.b from " + TBL0 + " t1 join " + TBL1 + " t2 "
                + "on t1.a = t2.a where t1.c = 42");
        long cardinality = scanCardinality(plan, "tbl0");
        Assertions.assertTrue(cardinality > 5_000 && cardinality < 20_000,
                "expected about 10000 rows for one of a hundred companies, got " + cardinality + " in:\n" + plan);
        Assertions.assertTrue(plan.contains("stats source: TABLE_METADATA"),
                "the estimate must be marked as coming from the source's own catalog:\n" + plan);
        Assertions.assertTrue(scanColumnStatistic(plan, "tbl0", "c").endsWith("ESTIMATE"),
                "the column the source described must not read as unknown:\n" + plan);
    }

    @Test
    public void testWithoutSourceStatisticsTheSamePredicateIsNotNarrowed() throws Exception {
        // The control: the identical query with the connector reporting nothing. This is what the
        // old code produced no matter what the source knew, because the column statistics were
        // rebuilt from a table the JDBC catalog has no row in.
        MockedJDBCMetadata.setTableStatistics("tbl0", 1_000_000L, Map.of());
        MockedJDBCMetadata.setTableStatistics("tbl1", 100L, Map.of());

        String plan = getCostExplain("select t1.a, t2.b from " + TBL0 + " t1 join " + TBL1 + " t2 "
                + "on t1.a = t2.a where t1.c = 42");
        long cardinality = scanCardinality(plan, "tbl0");
        Assertions.assertTrue(cardinality > 100_000,
                "an unknown column must fall back to the optimizer's own selectivity, got " + cardinality);
        Assertions.assertTrue(scanColumnStatistic(plan, "tbl0", "c").endsWith("UNKNOWN"),
                "a column the source said nothing about must stay unknown:\n" + plan);
    }

    @Test
    public void testRowCountFromTheConnectorReachesTheScan() throws Exception {
        // The join column gets a null fraction of zero, so the IS NOT NULL the planner derives
        // from the equi-join does not shave the row count and the two numbers stay comparable.
        MockedJDBCMetadata.setTableStatistics("tbl0", 1_000_000L, Map.of("c", withDistinctValues(1000)));
        MockedJDBCMetadata.setTableStatistics("tbl1", 77L, Map.of("c", withDistinctValues(70)));

        String plan = getCostExplain("select t1.c from " + TBL0 + " t1 join " + TBL1 + " t2 on t1.c = t2.c");
        Assertions.assertEquals(1_000_000L, scanCardinality(plan, "tbl0"));
        Assertions.assertEquals(77L, scanCardinality(plan, "tbl1"));
    }

    @Test
    public void testConnectorReportingNothingStillPlans() throws Exception {
        // Nothing registered: the connector's default Statistics carries no column entries at all.
        // Every requested column must still get one, or the cost model throws on the first lookup.
        String plan = getCostExplain("select t1.a, t2.b, t1.c from " + TBL0 + " t1 join " + TBL1 + " t2 "
                + "on t1.a = t2.a where t1.c > 5");
        Assertions.assertTrue(plan.contains("TABLE: `tbl0`"), "expected a plan, got:\n" + plan);
        Assertions.assertTrue(scanColumnStatistic(plan, "tbl0", "c").endsWith("UNKNOWN"));
    }
}
