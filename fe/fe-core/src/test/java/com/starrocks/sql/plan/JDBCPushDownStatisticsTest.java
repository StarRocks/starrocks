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
 * What a scan reports after the optimizer has pushed a join, an aggregation, a TopN or a
 * projection into it.
 *
 * <p>Such a scan reads a derived table — a {@code (SELECT ...) sr_inline} subquery — and no table
 * in the source describes it. The connector therefore refuses to answer for it: answering by the
 * table name the derived table inherited from one of its atoms is what once had the very same
 * join estimated at 100 rows or at 1,000,000 depending only on which of the two tables came first
 * in the SQL text. Refusing is honest but leaves the whole pushed-down subtree unestimated, so
 * each rule snapshots the estimate of the subtree it is about to replace onto the derived table,
 * the way Trino's {@code Rules.deriveTableStatisticsForPushdown} does.
 *
 * <p>Each test here fails without that snapshot: the scan comes back at
 * {@code Config.default_statistics_output_row_count} (one row) with {@code stats source: NONE} and
 * every column unknown.
 */
public class JDBCPushDownStatisticsTest extends ConnectorPlanTestBase {

    private static final String TBL0 = "jdbc0.partitioned_db0.tbl0";
    private static final String TBL1 = "jdbc0.partitioned_db0.tbl1";
    private static final String TBL2 = "jdbc0.partitioned_db0.tbl2";
    private static final String TBL3 = "jdbc0.partitioned_db0.tbl3";
    // The TopN rule only fires against PostgreSQL, where the ordering of every pushed column is
    // known to match the local one. Same mocked tables, addressed through the PG-flavoured catalog.
    private static final String PG_TBL0 = "jdbc_postgres.partitioned_db0.tbl0";

    @BeforeAll
    public static void beforeClass() throws Exception {
        ConnectorPlanTestBase.beforeClass();
    }

    @BeforeEach
    public void setUp() {
        connectContext.getSessionVariable().setEnableJdbcJoinPushDown(true);
        connectContext.getSessionVariable().setEnableJdbcAggPushDown(true);
        connectContext.getSessionVariable().setEnableJdbcTopNPushDown(true);
        // Single-table JDBC queries are short-circuited to default statistics one layer above the
        // connector, which would hide what these tests are about. The customer is told to turn
        // this off for the same reason.
        connectContext.getSessionVariable().setDisableTableStatsFromMetadataForSingleTable(false);
        MockedJDBCMetadata.clearTableStatistics();
        // A million rows over a hundred companies — the customer's shape.
        MockedJDBCMetadata.setTableStatistics("tbl0", 1_000_000L,
                Map.of("c", columnWithDistinctValues(100), "a", columnWithDistinctValues(500_000)));
        MockedJDBCMetadata.setTableStatistics("tbl1", 100L, Map.of("a", columnWithDistinctValues(100)));
        MockedJDBCMetadata.setTableStatistics("tbl2", 5_000L, Map.of("a", columnWithDistinctValues(5_000)));
        MockedJDBCMetadata.setTableStatistics("tbl3", 300L, Map.of("a", columnWithDistinctValues(300)));
    }

    @AfterEach
    public void tearDown() {
        MockedJDBCMetadata.clearTableStatistics();
        connectContext.getSessionVariable().setEnableJdbcJoinPushDown(false);
        connectContext.getSessionVariable().setDisableTableStatsFromMetadataForSingleTable(true);
    }

    private static ColumnStatistic columnWithDistinctValues(double distinctValues) {
        return ColumnStatistic.builder()
                .setDistinctValuesCount(distinctValues)
                .setNullsFraction(0)
                .setAverageRowSize(4)
                .setType(ColumnStatistic.StatisticType.ESTIMATE)
                .build();
    }

    /** The cardinality the pushed-down scan — the one reading a derived table — reports. */
    private static long pushedScanCardinality(String costExplain) {
        int scanAt = costExplain.indexOf("TABLE: (SELECT");
        Assertions.assertTrue(scanAt >= 0, "nothing was pushed down in:\n" + costExplain);
        int cardinalityAt = costExplain.indexOf("cardinality:", scanAt);
        Assertions.assertTrue(cardinalityAt >= 0, "no cardinality after the scan in:\n" + costExplain);
        String tail = costExplain.substring(cardinalityAt + "cardinality:".length()).trim();
        return Long.parseLong(tail.split("\\s+")[0]);
    }

    /** The cardinality of every pushed-down scan in the plan, in plan order. */
    private static long[] allPushedScanCardinalities(String costExplain) {
        java.util.List<Long> cardinalities = new java.util.ArrayList<>();
        int from = 0;
        while (true) {
            int scanAt = costExplain.indexOf("TABLE: (SELECT", from);
            if (scanAt < 0) {
                return cardinalities.stream().mapToLong(Long::longValue).toArray();
            }
            int cardinalityAt = costExplain.indexOf("cardinality:", scanAt);
            Assertions.assertTrue(cardinalityAt >= 0, "no cardinality after the scan in:\n" + costExplain);
            String tail = costExplain.substring(cardinalityAt + "cardinality:".length()).trim();
            cardinalities.add(Long.parseLong(tail.split("\\s+")[0]));
            from = scanAt + 1;
        }
    }

    /** The statistics source the pushed-down scan reports. */
    private static String pushedScanStatsSource(String costExplain) {
        int scanAt = costExplain.indexOf("TABLE: (SELECT");
        Assertions.assertTrue(scanAt >= 0, "nothing was pushed down in:\n" + costExplain);
        int at = costExplain.indexOf("stats source:", scanAt);
        Assertions.assertTrue(at >= 0, "no stats source after the scan in:\n" + costExplain);
        return costExplain.substring(at + "stats source:".length(), costExplain.indexOf('\n', at)).trim();
    }

    /** The statistic line the pushed-down scan reports for {@code column}. */
    private static String pushedScanColumnStatistic(String costExplain, String column) {
        int scanAt = costExplain.indexOf("TABLE: (SELECT");
        Assertions.assertTrue(scanAt >= 0, "nothing was pushed down in:\n" + costExplain);
        int at = costExplain.indexOf("* " + column + "-->", scanAt);
        Assertions.assertTrue(at >= 0, "no statistic for " + column + " in:\n" + costExplain);
        return costExplain.substring(at, costExplain.indexOf('\n', at)).trim();
    }

    @Test
    public void testMergedJoinScanReportsTheEstimateTheJoinCarried() throws Exception {
        String plan = getCostExplain("select t1.c, t2.b from " + TBL0 + " t1 join " + TBL1 + " t2 "
                + "on t1.a = t2.a where t1.c = 42");
        long cardinality = pushedScanCardinality(plan);
        Assertions.assertTrue(cardinality > 1,
                "the merged scan must report the join's estimate, not the default one row: " + plan);
        // Every atom had statistics from the source, so the derived estimate may say so.
        Assertions.assertEquals("TABLE_METADATA", pushedScanStatsSource(plan), plan);
        // The filtered side of the join is a hundred-thousandth of the table; a merged scan that
        // reported the whole table, or one row, would be off by orders of magnitude either way.
        Assertions.assertTrue(cardinality < 100_000, "expected the join to narrow the input: " + plan);
    }

    @Test
    public void testMergedJoinEstimateDoesNotDependOnTheOrderTheTablesAppearInTheSql() throws Exception {
        // The decisive symptom: the merged scan inherits the name of whichever atom came first, so
        // looking the statistics up by that name made the estimate a function of the SQL text.
        String forward = getCostExplain("select t1.c, t2.b from " + TBL0 + " t1 join " + TBL1 + " t2 "
                + "on t1.a = t2.a where t1.c = 42");
        String reversed = getCostExplain("select t1.c, t2.b from " + TBL1 + " t2 join " + TBL0 + " t1 "
                + "on t1.a = t2.a where t1.c = 42");
        long forwardCardinality = pushedScanCardinality(forward);
        Assertions.assertEquals(forwardCardinality, pushedScanCardinality(reversed),
                "the same join must be estimated the same either way round:\n" + forward + "\n" + reversed);
        // Equal is not enough on its own — two failures are equal too. The estimate has to be real.
        Assertions.assertTrue(forwardCardinality > 1, forward);
        Assertions.assertEquals("TABLE_METADATA", pushedScanStatsSource(reversed), reversed);
    }

    @Test
    public void testColumnStatisticsSurviveTheMerge() throws Exception {
        // The snapshot is keyed by column reference, and the merged scan re-exposes the atoms' own
        // references; the sr_c<id> aliasing exists only in the SQL text. Keying it by name — the
        // aliased name, at that — is the shape of this that does not work, and it would show up
        // here as an unknown column.
        String plan = getCostExplain("select t1.c, t2.b from " + TBL0 + " t1 join " + TBL1 + " t2 "
                + "on t1.a = t2.a where t1.c = 42");
        String statistic = pushedScanColumnStatistic(plan, "c");
        Assertions.assertTrue(statistic.endsWith("ESTIMATE"),
                "a column the source described must not read as unknown after the merge: " + statistic);
        Assertions.assertTrue(statistic.contains("[42.0, 42.0,"),
                "the equality the pushdown applied must still be visible in the column range: " + statistic);
        Assertions.assertTrue(statistic.endsWith("100.0] ESTIMATE"),
                "the source's distinct count must survive the merge: " + statistic);
        // A column the source said nothing about stays unknown rather than being invented.
        Assertions.assertTrue(pushedScanColumnStatistic(plan, "b").endsWith("UNKNOWN"), plan);
    }

    @Test
    public void testFoldedAggregateScanReportsTheGroupCountNotTheTableSize() throws Exception {
        String plan = getCostExplain("select c, count(*) from " + TBL0 + " group by c");
        Assertions.assertEquals(100L, pushedScanCardinality(plan),
                "a hundred distinct values must be estimated as a hundred groups:\n" + plan);
        Assertions.assertEquals("TABLE_METADATA", pushedScanStatsSource(plan), plan);
        // The aggregate's own output column is described too: count(*) over a million rows in a
        // hundred groups cannot exceed the million.
        Assertions.assertTrue(pushedScanColumnStatistic(plan, "count").contains("1000000.0"), plan);
    }

    @Test
    public void testAggregateFoldedOntoAMergedJoinBuildsOnTheJoinsSnapshot() throws Exception {
        // Composite push-down: the join merges first, then the aggregate folds onto the merged
        // scan. The aggregate's estimate can only be computed from the merged scan's estimate, so
        // this passes only if the snapshot the join left behind is what the second rule read.
        String plan = getCostExplain("select t1.c, count(*) from " + TBL0 + " t1 join " + TBL1 + " t2 "
                + "on t1.a = t2.a group by t1.c");
        Assertions.assertEquals(100L, pushedScanCardinality(plan),
                "the group count must come out of the join's estimate:\n" + plan);
        Assertions.assertEquals("TABLE_METADATA", pushedScanStatsSource(plan), plan);
        String countStatistic = pushedScanColumnStatistic(plan, "count");
        Assertions.assertFalse(countStatistic.contains("1000000.0"),
                "count(*) must be bounded by the join's output, not by the larger table:\n" + countStatistic);
        Assertions.assertTrue(countStatistic.endsWith("ESTIMATE"), countStatistic);
    }

    @Test
    public void testPushedTopNScanKeepsBothThePredicateAndTheLimit() throws Exception {
        // The TopN rule moves the scan's predicate into the remote SQL, where the estimator can no
        // longer see it, and leaves the limit on the operator. With a limit far above the filtered
        // row count, only the predicate decides the estimate.
        String wide = getCostExplain("select a from " + PG_TBL0 + " where c = 42 order by c limit 100000");
        Assertions.assertEquals(10_000L, pushedScanCardinality(wide),
                "one of a hundred companies out of a million rows:\n" + wide);

        // With a limit below it, the limit decides — re-applying the cap the snapshot already
        // applied must not change the answer.
        String narrow = getCostExplain("select a from " + PG_TBL0 + " where c = 42 order by c limit 10");
        Assertions.assertEquals(10L, pushedScanCardinality(narrow), narrow);
    }

    @Test
    public void testPushedProjectionScanKeepsTheRowCount() throws Exception {
        // Folding a projection changes what a row looks like, never how many there are. The scan
        // must come out of it with the row count it had a moment earlier.
        String plan = getCostExplain("select c + 1 as x from " + PG_TBL0 + " where c = 42");
        Assertions.assertEquals(10_000L, pushedScanCardinality(plan), plan);
        Assertions.assertEquals("TABLE_METADATA", pushedScanStatsSource(plan), plan);
        // And the expression it pushed keeps the statistic the optimizer derived for it.
        Assertions.assertTrue(pushedScanColumnStatistic(plan, "expr").endsWith("ESTIMATE"), plan);
    }

    @Test
    public void testAnAtomWithoutStatisticsMakesTheDerivedEstimateSaySo() throws Exception {
        // Statistics.StatsSource is observability, and the merged scan is the only place a reader
        // can see whether the numbers underneath it were real. Intermediate operators always build
        // theirs with a fresh builder, whose source is NONE, so the subtree root cannot answer
        // this — the weakest leaf does.
        MockedJDBCMetadata.clearTableStatistics();
        MockedJDBCMetadata.setTableStatistics("tbl0", 1_000_000L, Map.of("a", columnWithDistinctValues(500_000)));
        String plan = getCostExplain("select t1.c, t2.b from " + TBL0 + " t1 join " + TBL1 + " t2 "
                + "on t1.a = t2.a");
        Assertions.assertEquals("NONE", pushedScanStatsSource(plan),
                "one atom the source knew nothing about must not be reported as catalog-backed:\n" + plan);
    }

    @Test
    public void testEachMergedGroupIsEstimatedFromItsOwnSubtreeNotTheWholeJoin() throws Exception {
        // Two catalogs, each contributing a two-table join that merges separately, plus a local
        // join between the two merged scans. Each merged scan must report what its own group was
        // estimated at: snapshotting the rule's whole input instead would hand both of them the
        // same number — the estimate of the entire four-table join — and the local join above
        // them would then be costed from two figures that describe neither side.
        String plan = getCostExplain("select m1.a, p1.a from "
                + TBL0 + " m1 join " + TBL1 + " m2 on m1.a = m2.a "
                + "join jdbc_postgres.partitioned_db0.tbl2 p1 on m1.c = p1.c "
                + "join jdbc_postgres.partitioned_db0.tbl3 p2 on p1.a = p2.a");
        long[] cardinalities = allPushedScanCardinalities(plan);
        Assertions.assertEquals(2, cardinalities.length, "expected two merged scans:\n" + plan);
        Assertions.assertTrue(cardinalities[0] > 1 && cardinalities[1] > 1, plan);
        Assertions.assertNotEquals(cardinalities[0], cardinalities[1],
                "two different groups over different tables cannot have the same estimate:\n" + plan);
    }

    @Test
    public void testTheSnapshotCostsOneConnectorLookupPerTableNotOnePerRuleVisit() throws Exception {
        // The JDBC push-down rules run in an iterative rewrite, before the optimizer's own
        // statistics pass. Estimating from check(), or from a transform that declines to rewrite,
        // would cost a full subtree estimate on every visit. Four tables, four lookups: the cost
        // is bounded by the plan, and the merged scan itself never asks the connector at all
        // (it reads its snapshot instead).
        MockedJDBCMetadata.resetStatisticsCallCount();
        String plan = getCostExplain("select t1.c from " + TBL0 + " t1 "
                + "join " + TBL1 + " t2 on t1.a = t2.a "
                + "join " + TBL2 + " t3 on t1.a = t3.a "
                + "join " + TBL3 + " t4 on t1.a = t4.a");
        // 200 rows: a join down every one of the three equalities, not the million-row table the
        // merged scan inherited its name from and not the default one row.
        Assertions.assertEquals(200L, pushedScanCardinality(plan), plan);
        long calls = MockedJDBCMetadata.statisticsCallCount();
        Assertions.assertTrue(calls <= 8,
                "four tables should cost about four connector lookups, got " + calls + ":\n" + plan);
    }
}
