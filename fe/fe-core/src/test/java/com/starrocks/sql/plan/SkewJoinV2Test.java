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

import com.google.common.collect.Maps;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Table;
import com.starrocks.common.FeConstants;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.statistics.Bucket;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.Histogram;
import com.starrocks.sql.optimizer.statistics.StatisticStorage;
import com.starrocks.statistic.BasicStatsMeta;
import com.starrocks.statistic.StatsConstants;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class SkewJoinV2Test extends PlanTestBase {
    @BeforeAll
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        connectContext.getSessionVariable().setEnableOptimizerSkewJoinOptimizeV2(true);
    }

    @AfterAll
    public static void afterClass() {
        connectContext.getSessionVariable().setEnableOptimizerSkewJoinOptimizeV2(false);
        PlanTestBase.afterClass();
    }

    private static ColumnStatistic buildIntMcvColumnStat(long bucketCount, Map<String, Long> mcv, double nullFraction) {
        List<Bucket> buckets = bucketCount <= 0 ? List.of() : List.of(new Bucket(1, 3, bucketCount, 0L));
        Histogram hist = new Histogram(buckets, mcv);
        return ColumnStatistic.builder()
                .setMinValue(1)
                .setMaxValue(100000)
                .setNullsFraction(nullFraction)
                .setAverageRowSize(8)
                .setDistinctValuesCount(1000)
                .setHistogram(hist)
                .build();
    }

    private static class TestStatisticStorage implements StatisticStorage {
        private final Map<Long, Map<String, ColumnStatistic>> colStats = new HashMap<>();
        private final Map<Long, Map<String, Histogram>> histStats = new HashMap<>();

        @Override
        public ColumnStatistic getColumnStatistic(Table table, String column) {
            Map<String, ColumnStatistic> m = colStats.get(table.getId());
            if (m == null) {
                return ColumnStatistic.unknown();
            }
            return m.getOrDefault(column, ColumnStatistic.unknown());
        }

        @Override
        public List<ColumnStatistic> getColumnStatistics(Table table, List<String> columns) {
            return columns.stream().map(c -> getColumnStatistic(table, c)).toList();
        }

        @Override
        public Map<String, Histogram> getHistogramStatistics(Table table, List<String> columns) {
            Map<String, Histogram> m = histStats.getOrDefault(table.getId(), Map.of());
            Map<String, Histogram> out = new HashMap<>();
            for (String c : columns) {
                Histogram h = m.get(c);
                if (h != null) {
                    out.put(c, h);
                }
            }
            return out;
        }

        @Override
        public Map<Long, Optional<Long>> getTableStatistics(Long tableId, Collection<Partition> partitions) {
            return partitions.stream().collect(java.util.stream.Collectors.toMap(
                    Partition::getId,
                    p -> Optional.of(p.getDefaultPhysicalPartition().getBaseIndex().getRowCount())));
        }

        @Override
        public void addColumnStatistic(Table table, String column, ColumnStatistic columnStatistic) {
            colStats.computeIfAbsent(table.getId(), k -> new HashMap<>()).put(column, columnStatistic);
            if (columnStatistic != null && columnStatistic.getHistogram() != null) {
                histStats.computeIfAbsent(table.getId(), k -> new HashMap<>()).put(column, columnStatistic.getHistogram());
            }
        }
    }

    private void withMockedMcvStats(long t0Rows, long t1Rows, ColumnStatistic t0V1, ColumnStatistic t1V4, Runnable action) {
        StatisticStorage old = connectContext.getGlobalStateMgr().getStatisticStorage();
        boolean oldEnable = connectContext.getSessionVariable().isEnableStatsToOptimizeSkewJoin();

        try {
            connectContext.getGlobalStateMgr().setStatisticStorage(new TestStatisticStorage());
            connectContext.getSessionVariable().setEnableStatsToOptimizeSkewJoin(true);

            OlapTable t0 = getOlapTable("t0");
            OlapTable t1 = getOlapTable("t1");
            setTableStatistics(t0, t0Rows);
            setTableStatistics(t1, t1Rows);

            StatisticStorage storage = connectContext.getGlobalStateMgr().getStatisticStorage();
            storage.addColumnStatistic(t0, "v1", t0V1);
            storage.addColumnStatistic(t1, "v4", t1V4);

            long dbId = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test").getId();
            connectContext.getGlobalStateMgr().getAnalyzeMgr().addBasicStatsMeta(
                    new BasicStatsMeta(dbId, t0.getId(), null, StatsConstants.AnalyzeType.FULL,
                            java.time.LocalDateTime.now(), Maps.newHashMap(), t0Rows));
            connectContext.getGlobalStateMgr().getAnalyzeMgr().addBasicStatsMeta(
                    new BasicStatsMeta(dbId, t1.getId(), null, StatsConstants.AnalyzeType.FULL,
                            java.time.LocalDateTime.now(), Maps.newHashMap(), t1Rows));

            connectContext.getSessionVariable().disableJoinReorder();
            action.run();
        } finally {
            connectContext.getSessionVariable().enableJoinReorder();
            connectContext.getSessionVariable().setEnableStatsToOptimizeSkewJoin(oldEnable);
            connectContext.getGlobalStateMgr().setStatisticStorage(old);
        }
    }

    @Test
    public void testSkewJoinV2WithRightSideHint1() throws Exception {
        String sql = "select v2, v5 from t0 join[skew|t1.v4(1,2)] t1 on v1 = v4 ";
        String sqlPlan = getVerboseExplain(sql);
        assertCContains(sqlPlan, "  Input Partition: RANDOM\n" +
                "  SplitCastDataSink:\n" +
                "  OutPut Partition: HASH_PARTITIONED: 1: v1\n" +
                "  OutPut Exchange Id: 03\n" +
                "  Split expr: (1: v1 NOT IN (1, 2)) OR (1: v1 IS NULL)\n" +
                "  OutPut Partition: UNPARTITIONED\n" +
                "  OutPut Exchange Id: 07\n" +
                "  Split expr: 1: v1 IN (1, 2)");
    }

    @Test
    public void testSkewJoinV2WithRightSideHint2() throws Exception {
        String sql = "select v2, v5 from t1 join[skew|t1.v4(1,2)] t0 on v1 = v4 ";
        String sqlPlan = getVerboseExplain(sql);
        PlanTestBase.assertContains(sqlPlan, "  SplitCastDataSink:\n" +
                "  OutPut Partition: HASH_PARTITIONED: 1: v4\n" +
                "  OutPut Exchange Id: 02\n" +
                "  Split expr: (1: v4 NOT IN (1, 2)) OR (1: v4 IS NULL)\n" +
                "  OutPut Partition: RANDOM\n" +
                "  OutPut Exchange Id: 06\n" +
                "  Split expr: 1: v4 IN (1, 2)");
    }

    @Test
    public void testSkewJoinV2() throws Exception {
        String sql = "select v2, v5 from t0 join[skew|t0.v1(1,2)] t1 on v1 = v4 ";
        String sqlPlan = getVerboseExplain(sql);
        assertCContains(sqlPlan, "Input Partition: HYBRID_HASH_PARTITIONED\n" +
                "  RESULT SINK");
        
        // this is a normal union, which means its local exchanger is PASS_THROUGH
        assertCContains(sqlPlan, "10:UNION\n" +
                "  |  child exprs:\n" +
                "  |      [2: v2, BIGINT, true] | [5: v5, BIGINT, true]\n" +
                "  |      [2: v2, BIGINT, true] | [5: v5, BIGINT, true]\n" +
                "  |  pass-through-operands: all");

        // shuffle join is UNION's left child, with global runtime filter
        assertCContains(sqlPlan, "  4:HASH JOIN\n" +
                "  |  join op: INNER JOIN (PARTITIONED)\n" +
                "  |  equal join conjunct: [1: v1, BIGINT, true] = [4: v4, BIGINT, true]\n");
        // shuffle join's both child is shuffle exchange
        assertCContains(sqlPlan, " 2:EXCHANGE\n" +
                "     distribution type: SHUFFLE\n" +
                "     partition exprs: [1: v1, BIGINT, true]");

        assertCContains(sqlPlan, " |----3:EXCHANGE\n" +
                "  |       distribution type: SHUFFLE\n" +
                "  |       partition exprs: [4: v4, BIGINT, true]");

        // broadcast join is UNION's right child, with local runtime filter
        assertCContains(sqlPlan, "8:HASH JOIN\n" +
                "  |    |  join op: INNER JOIN (BROADCAST)\n" +
                "  |    |  equal join conjunct: [1: v1, BIGINT, true] = [4: v4, BIGINT, true]\n");
        // broadcast's left child is ROUND_ROBIN, right child is BROADCAST
        assertCContains(sqlPlan, "  |    6:EXCHANGE\n" +
                "  |       distribution type: ROUND_ROBIN");
        assertCContains(sqlPlan, "|    |----7:EXCHANGE\n" +
                "  |    |       distribution type: BROADCAST");

        // left table's scan node has its own fragment with split data sink
        // split data sink will split data with split expr
        // if v1 NOT IN (1, 2), use HASH_PARTITIONED
        // else use RANDOM
        assertCContains(sqlPlan, "PLAN FRAGMENT 2(F00)\n" +
                "\n" +
                "  Input Partition: RANDOM\n" +
                "  SplitCastDataSink:\n" +
                "  OutPut Partition: HASH_PARTITIONED: 1: v1\n" +
                "  OutPut Exchange Id: 02\n" +
                "  Split expr: (1: v1 NOT IN (1, 2)) OR (1: v1 IS NULL)\n" +
                "  OutPut Partition: RANDOM\n" +
                "  OutPut Exchange Id: 06\n" +
                "  Split expr: 1: v1 IN (1, 2)\n" +
                "\n" +
                "  0:OlapScanNode\n" +
                "     table: t0, rollup: t0");

        // right table's scan node has its own fragment with split data sink
        // split data sink will split data with split expr
        // if v4 NOT IN (1, 2), use HASH_PARTITIONED
        // else use UNPARTITIONED
        assertCContains(sqlPlan, "PLAN FRAGMENT 1(F01)\n" +
                "\n" +
                "  Input Partition: RANDOM\n" +
                "  SplitCastDataSink:\n" +
                "  OutPut Partition: HASH_PARTITIONED: 4: v4\n" +
                "  OutPut Exchange Id: 03\n" +
                "  Split expr: (4: v4 NOT IN (1, 2)) OR (4: v4 IS NULL)\n" +
                "  OutPut Partition: UNPARTITIONED\n" +
                "  OutPut Exchange Id: 07\n" +
                "  Split expr: 4: v4 IN (1, 2)\n" +
                "\n" +
                "  1:OlapScanNode\n" +
                "     table: t1, rollup: t1");
    }

    @Test
    public void testSkewJoinV2WithComplexPredicate1() throws Exception {
        String sql = "select v2, v5 from t0 join[skew|t0.v1(1,2)] t1 on abs(v1) = abs(v4) ";
        String sqlPlan = getVerboseExplain(sql);
        // if on predicate's input is not column from table, then split expr should use Project's output column like "7: abs"
        assertCContains(sqlPlan, "Input Partition: RANDOM\n" +
                "  SplitCastDataSink:\n" +
                "  OutPut Partition: HASH_PARTITIONED: 8: abs\n" +
                "  OutPut Exchange Id: 05\n" +
                "  Split expr: (8: abs NOT IN (abs(1), abs(2))) OR (8: abs IS NULL)\n" +
                "  OutPut Partition: UNPARTITIONED\n" +
                "  OutPut Exchange Id: 09\n" +
                "  Split expr: 8: abs IN (abs(1), abs(2))\n" +
                "\n" +
                "  3:Project\n" +
                "  |  output columns:\n" +
                "  |  5 <-> [5: v5, BIGINT, true]\n" +
                "  |  8 <-> abs[([4: v4, BIGINT, true]); args: BIGINT; result: LARGEINT; args nullable: true; result nullable:" +
                " true]\n" +
                "  |  cardinality: 1\n" +
                "  |  \n" +
                "  2:OlapScanNode\n" +
                "     table: t1, rollup: t1\n" +
                "     preAggregation: on\n" +
                "     Predicates: abs(4: v4) IS NOT NULL\n" +
                "     partitionsRatio=0/1, tabletsRatio=0/0\n" +
                "     tabletList=\n" +
                "     actualRows=0, avgRowSize=3.0\n" +
                "     cardinality: 1");
    }

    @Test
    public void testSkewJoinV2WithComplexPredicate2() throws Exception {
        String sql = "select v2, v5 from t1 join[skew|t0.v1(1,2)] t0 on abs(v1) = abs(v4) ";
        String sqlPlan = getVerboseExplain(sql);
        // if on predicate's input is not column from table, then split expr should use Project's output column like "7: abs"
        PlanTestBase.assertContains(sqlPlan, "Input Partition: RANDOM\n" +
                "  SplitCastDataSink:\n" +
                "  OutPut Partition: HASH_PARTITIONED: 8: abs\n" +
                "  OutPut Exchange Id: 05\n" +
                "  Split expr: (8: abs NOT IN (abs(1), abs(2))) OR (8: abs IS NULL)\n" +
                "  OutPut Partition: UNPARTITIONED\n" +
                "  OutPut Exchange Id: 09\n" +
                "  Split expr: 8: abs IN (abs(1), abs(2))");
        PlanTestBase.assertContains(sqlPlan, "SplitCastDataSink:\n" +
                "  OutPut Partition: HASH_PARTITIONED: 7: abs\n" +
                "  OutPut Exchange Id: 04\n" +
                "  Split expr: (7: abs NOT IN (abs(1), abs(2))) OR (7: abs IS NULL)\n" +
                "  OutPut Partition: RANDOM\n" +
                "  OutPut Exchange Id: 08\n" +
                "  Split expr: 7: abs IN (abs(1), abs(2))");
    }

    @Test
    public void testSkewJoinV2WithNullSkewHintOnly() throws Exception {
        // Hint: only NULL is skew.
        // Expected split (conceptually):
        // - skew branch: k IS NULL
        // - non-skew branch: k IS NOT NULL
        String sql = "select v2, v5 from t0 join[skew|t0.v1(null)] t1 on v1 = v4 ";
        String plan = getVerboseExplain(sql);
        assertCContains(plan, "SplitCastDataSink:\n" +
                "  OutPut Partition: HASH_PARTITIONED: 1: v1\n" +
                "  OutPut Exchange Id: 02\n" +
                "  Split expr: 1: v1 IS NOT NULL\n" +
                "  OutPut Partition: RANDOM\n" +
                "  OutPut Exchange Id: 06\n" +
                "  Split expr: 1: v1 IS NULL");
        assertCContains(plan, "SplitCastDataSink:\n" +
                "  OutPut Partition: HASH_PARTITIONED: 4: v4\n" +
                "  OutPut Exchange Id: 03\n" +
                "  Split expr: 4: v4 IS NOT NULL\n" +
                "  OutPut Partition: UNPARTITIONED\n" +
                "  OutPut Exchange Id: 07\n" +
                "  Split expr: 4: v4 IS NULL");
    }

    @Test
    public void testSkewJoinV2WithNullAndNonNullSkewHint() throws Exception {
        // Hint: both {1,2} and NULL are skew.
        // Expected split (conceptually):
        // - skew branch: k IN (1,2) OR k IS NULL
        // - non-skew branch: k NOT IN (1,2) AND k IS NOT NULL
        String sql = "select v2, v5 from t0 join[skew|t0.v1(1,2,null)] t1 on v1 = v4 ";
        String plan = getVerboseExplain(sql);
        assertCContains(plan, "Input Partition: RANDOM\n" +
                "  SplitCastDataSink:\n" +
                "  OutPut Partition: HASH_PARTITIONED: 1: v1\n" +
                "  OutPut Exchange Id: 02\n" +
                "  Split expr: (1: v1 NOT IN (1, 2)) AND (1: v1 IS NOT NULL)\n" +
                "  OutPut Partition: RANDOM\n" +
                "  OutPut Exchange Id: 06\n" +
                "  Split expr: (1: v1 IN (1, 2)) OR (1: v1 IS NULL)");
        assertCContains(plan, "Input Partition: RANDOM\n" +
                "  SplitCastDataSink:\n" +
                "  OutPut Partition: HASH_PARTITIONED: 4: v4\n" +
                "  OutPut Exchange Id: 03\n" +
                "  Split expr: (4: v4 NOT IN (1, 2)) AND (4: v4 IS NOT NULL)\n" +
                "  OutPut Partition: UNPARTITIONED\n" +
                "  OutPut Exchange Id: 07\n" +
                "  Split expr: (4: v4 IN (1, 2)) OR (4: v4 IS NULL)");
    }

    @Test
    public void testSkewJoinV2LeftOuterJoinWithRightSideHintShouldNotRewrite() throws Exception {
        // For LEFT OUTER JOIN, right-side skew rewrite is forbidden.
        String sql = "select v2, v5 from t0 left join[skew|t1.v4(1,2)] t1 on v1 = v4 ";
        String plan = getVerboseExplain(sql);
        assertNotContains(plan, "SplitCastDataSink:");
        assertNotContains(plan, "Split expr:");
    }

    @Test
    public void testSkewJoinV2AutoDetectFromMcvBasic() {
        Map<String, Long> mcv = Map.of("1", 6_000_000L, "2", 4_000_000L);
        ColumnStatistic t0V1 = buildIntMcvColumnStat(0, mcv, 0.0);
        ColumnStatistic t1V4 = buildIntMcvColumnStat(0, mcv, 0.0);

        FeConstants.runningUnitTest = true;
        withMockedMcvStats(20_000_000L, 20_000_000L, t0V1, t1V4, () -> {
            try {
                String sql = "select v2, v5 from t0 join[shuffle] t1 on v1 = v4";
                String plan = getVerboseExplain(sql);
                assertCContains(plan, "SplitCastDataSink:\n" +
                        "  OutPut Partition: HASH_PARTITIONED: 1: v1\n" +
                        "  OutPut Exchange Id: 02\n" +
                        "  Split expr: (1: v1 NOT IN (1, 2)) OR (1: v1 IS NULL)\n" +
                        "  OutPut Partition: RANDOM\n" +
                        "  OutPut Exchange Id: 06\n" +
                        "  Split expr: 1: v1 IN (1, 2)");
                assertCContains(plan, "SplitCastDataSink:\n" +
                        "  OutPut Partition: HASH_PARTITIONED: 4: v4\n" +
                        "  OutPut Exchange Id: 03\n" +
                        "  Split expr: (4: v4 NOT IN (1, 2)) OR (4: v4 IS NULL)\n" +
                        "  OutPut Partition: UNPARTITIONED\n" +
                        "  OutPut Exchange Id: 07\n" +
                        "  Split expr: 4: v4 IN (1, 2)");
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        FeConstants.runningUnitTest = false;
    }

    @Test
    public void testSkewJoinV2AutoDetectNotTriggeredWhenBelowThresholdBasic() {
        // Auto-detect should NOT trigger when MCV skew is not strong enough.
        FeConstants.runningUnitTest = true;
        Map<String, Long> mcv = Map.of("1", 2_000L, "2", 2_000L, "3", 2_000L);
        ColumnStatistic t0V1 = buildIntMcvColumnStat(100_000_000L, mcv, 0.0);
        ColumnStatistic t1V4 = buildIntMcvColumnStat(100_000_000L, mcv, 0.0);

        withMockedMcvStats(20_000_000L, 20_000_000L, t0V1, t1V4, () -> {
            try {
                String sql = "select v2, v5 from t0 join[shuffle] t1 on v1 = v4";
                String plan = getVerboseExplain(sql);
                assertNotContains(plan, "SplitCastDataSink:");
                assertNotContains(plan, "Split expr:");
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        FeConstants.runningUnitTest = false;
    }

    @Test
    public void testSkewJoinV2AutoDetectPrefersRightSideWhenMoreSkewedBasic() {
        FeConstants.runningUnitTest = true;
        Map<String, Long> leftMcv = Map.of("1", 2_000L, "2", 2_000L); // not skew
        Map<String, Long> rightMcv = Map.of("1", 12_000_000L, "2", 8_000_000L); // heavily skewed
        ColumnStatistic t0V1 = buildIntMcvColumnStat(0, leftMcv, 0.0);
        ColumnStatistic t1V4 = buildIntMcvColumnStat(0, rightMcv, 0.0);

        withMockedMcvStats(20_000_000L, 20_000_000L, t0V1, t1V4, () -> {
            try {
                String sql = "select v2, v5 from t0 join[shuffle] t1 on v1 = v4";
                String plan = getVerboseExplain(sql);
                assertCContains(plan, "SplitCastDataSink:\n" +
                        "  OutPut Partition: HASH_PARTITIONED: 4: v4\n" +
                        "  OutPut Exchange Id: 02\n" +
                        "  Split expr: (4: v4 NOT IN (1, 2)) OR (4: v4 IS NULL)\n" +
                        "  OutPut Partition: RANDOM\n" +
                        "  OutPut Exchange Id: 06\n" +
                        "  Split expr: 4: v4 IN (1, 2)");
                assertCContains(plan, "SplitCastDataSink:\n" +
                        "  OutPut Partition: HASH_PARTITIONED: 1: v1\n" +
                        "  OutPut Exchange Id: 03\n" +
                        "  Split expr: (1: v1 NOT IN (1, 2)) OR (1: v1 IS NULL)\n" +
                        "  OutPut Partition: UNPARTITIONED\n" +
                        "  OutPut Exchange Id: 07\n" +
                        "  Split expr: 1: v1 IN (1, 2)");
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        FeConstants.runningUnitTest = false;
    }

    @Test
    public void testSkewJoinV2AutoDetectNotTriggeredWhenBelowMinInputRows_basic() {
        FeConstants.runningUnitTest = true;
        Map<String, Long> mcv = Map.of("1", 10_000L, "2", 10_000L);
        ColumnStatistic t0V1 = buildIntMcvColumnStat(0, mcv, 0.0);
        ColumnStatistic t1V4 = buildIntMcvColumnStat(0, mcv, 0.0);

        withMockedMcvStats(5_000L, 5_000L, t0V1, t1V4, () -> {
            try {
                String sql = "select v2, v5 from t0 join[shuffle] t1 on v1 = v4";
                String plan = getVerboseExplain(sql);
                assertNotContains(plan, "SplitCastDataSink:");
                assertNotContains(plan, "Split expr:");
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        FeConstants.runningUnitTest = false;
    }

    @Test
    public void testSkewJoinV2AutoDetectNullSkewOnlyNotTriggeredForInnerJoin() {
        FeConstants.runningUnitTest = true;
        ColumnStatistic t0V1 = buildIntMcvColumnStat(0, Map.of(), 0.9);
        ColumnStatistic t1V4 = buildIntMcvColumnStat(0, Map.of(), 0.0);

        withMockedMcvStats(20_000_000L, 20_000_000L, t0V1, t1V4, () -> {
            try {
                String sql = "select v2, v5 from t0 join[shuffle] t1 on v1 = v4";
                String plan = getVerboseExplain(sql);
                assertNotContains(plan, "SplitCastDataSink:");
                assertNotContains(plan, "Split expr:");
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        FeConstants.runningUnitTest = false;
    }

    @Test
    public void testSkewJoinV2AutoDetectNullSkewForLeftOuterJoinBasic() {
        FeConstants.runningUnitTest = true;
        // For LEFT OUTER JOIN, NULLs on the left join key are preserved (as non-matching rows) and can be skewed.
        ColumnStatistic t0V1 = buildIntMcvColumnStat(0, Map.of(), 0.9);
        ColumnStatistic t1V4 = buildIntMcvColumnStat(0, Map.of(), 0.3);

        withMockedMcvStats(20_000_000L, 20_000_000L, t0V1, t1V4, () -> {
            try {
                String sql = "select v2, v5 from t0 left join[shuffle] t1 on v1 = v4";
                String plan = getVerboseExplain(sql);
                assertCContains(plan, "SplitCastDataSink:\n" +
                        "  OutPut Partition: HASH_PARTITIONED: 1: v1\n" +
                        "  OutPut Exchange Id: 02\n" +
                        "  Split expr: 1: v1 IS NOT NULL\n" +
                        "  OutPut Partition: RANDOM\n" +
                        "  OutPut Exchange Id: 06\n" +
                        "  Split expr: 1: v1 IS NULL");
                assertCContains(plan, "SplitCastDataSink:\n" +
                        "  OutPut Partition: HASH_PARTITIONED: 4: v4\n" +
                        "  OutPut Exchange Id: 03\n" +
                        "  Split expr: 4: v4 IS NOT NULL\n" +
                        "  OutPut Partition: UNPARTITIONED\n" +
                        "  OutPut Exchange Id: 07\n" +
                        "  Split expr: 4: v4 IS NULL");
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        FeConstants.runningUnitTest = false;
    }

    @Test
    public void testSkewJoinV2AutoDetectMcvAndNullSkewForLeftOuterJoinBasic() {
        FeConstants.runningUnitTest = true;
        Map<String, Long> mcv = Map.of("1", 12_000_000L, "2", 8_000_000L);
        ColumnStatistic t0V1 = buildIntMcvColumnStat(0, mcv, 0.9);
        ColumnStatistic t1V4 = buildIntMcvColumnStat(0, Map.of(), 0.0);

        withMockedMcvStats(20_000_000L, 20_000_000L, t0V1, t1V4, () -> {
            try {
                String sql = "select v2, v5 from t0 left join[shuffle] t1 on v1 = v4";
                String plan = getVerboseExplain(sql);
                assertCContains(plan, "Input Partition: RANDOM\n" +
                        "  SplitCastDataSink:\n" +
                        "  OutPut Partition: HASH_PARTITIONED: 1: v1\n" +
                        "  OutPut Exchange Id: 02\n" +
                        "  Split expr: (1: v1 NOT IN (1, 2)) AND (1: v1 IS NOT NULL)\n" +
                        "  OutPut Partition: RANDOM\n" +
                        "  OutPut Exchange Id: 06\n" +
                        "  Split expr: (1: v1 IN (1, 2)) OR (1: v1 IS NULL)");
                assertCContains(plan, "Input Partition: RANDOM\n" +
                        "  SplitCastDataSink:\n" +
                        "  OutPut Partition: HASH_PARTITIONED: 4: v4\n" +
                        "  OutPut Exchange Id: 03\n" +
                        "  Split expr: (4: v4 NOT IN (1, 2)) AND (4: v4 IS NOT NULL)\n" +
                        "  OutPut Partition: UNPARTITIONED\n" +
                        "  OutPut Exchange Id: 07\n" +
                        "  Split expr: (4: v4 IN (1, 2)) OR (4: v4 IS NULL)");
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        FeConstants.runningUnitTest = false;
    }

    @Test
    public void testSkewJoinV2AutoDetectWithExpressionJoinKey_basic() {
        // Auto-detect with expression join key should propagate skew values through expressions.
        // Example: k2 = cast(v1 as bigint) + 10 => skew values (1,2) become (11,12).
        Map<String, Long> baseMcv = Map.of("1", 6_000_000L, "2", 4_000_000L);
        ColumnStatistic t0V1 = buildIntMcvColumnStat(0, baseMcv, 0.0);
        ColumnStatistic t1V4 = buildIntMcvColumnStat(0, baseMcv, 0.0);

        FeConstants.runningUnitTest = true;
        withMockedMcvStats(20_000_000L, 20_000_000L, t0V1, t1V4, () -> {
            try {
                String sql = "with l as (select (cast(v1 as bigint) + 10) as k2, v2 from t0) "
                        + ", r as (select (cast(v4 as bigint) + 10) as k2, v5 from t1) "
                        + "select v2, v5 from l join[shuffle] r on l.k2 = r.k2";
                String plan = getVerboseExplain(sql);
                assertCContains(plan, "SplitCastDataSink:\n" +
                        "  OutPut Partition: HASH_PARTITIONED: 4: expr\n" +
                        "  OutPut Exchange Id: 04\n" +
                        "  Split expr: (4: expr NOT IN (11, 12)) OR (4: expr IS NULL)\n" +
                        "  OutPut Partition: RANDOM\n" +
                        "  OutPut Exchange Id: 08\n" +
                        "  Split expr: 4: expr IN (11, 12)");
                assertCContains(plan, "Input Partition: RANDOM\n" +
                        "  SplitCastDataSink:\n" +
                        "  OutPut Partition: HASH_PARTITIONED: 8: expr\n" +
                        "  OutPut Exchange Id: 05\n" +
                        "  Split expr: (8: expr NOT IN (11, 12)) OR (8: expr IS NULL)\n" +
                        "  OutPut Partition: UNPARTITIONED\n" +
                        "  OutPut Exchange Id: 09\n" +
                        "  Split expr: 8: expr IN (11, 12)");
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        FeConstants.runningUnitTest = false;
    }

    @Test
    public void testSkewJoinV2WithLeftJoin() throws Exception {
        String sql = "select v2, v5 from t0 left join[skew|t0.v1(1,2)] t1 on v1 = v4 ";
        String sqlPlan = getFragmentPlan(sql);
        assertCContains(sqlPlan, " join op: LEFT OUTER JOIN (PARTITIONED)");

        sql = "select v2 from t0 left semi join[skew|t0.v1(1,2)] t1 on v1 = v4 ";
        sqlPlan = getFragmentPlan(sql);
        assertCContains(sqlPlan, "LEFT SEMI JOIN (PARTITIONED)");

        sql = "select v2 from t0 left anti join[skew|t0.v1(1,2)] t1 on v1 = v4 ";
        sqlPlan = getFragmentPlan(sql);
        assertCContains(sqlPlan, "LEFT ANTI JOIN (PARTITIONED)");
    }

    @Test
    public void testSkewJoinV2WithOneAgg() throws Exception {
        // one phase agg need skew join's output is the same as original shuffle join
        String sql = "select v4 from t0 join[skew|t0.v1(1,2)] t1 on v1 = v4 group by v4";
        String sqlPlan = getVerboseExplain(sql);
        assertCContains(sqlPlan, "12:AGGREGATE (update finalize)\n" +
                "  |  group by: [4: v4, BIGINT, true]");

        // union's local exchange type is DIRECT
        assertCContains(sqlPlan, " 11:UNION\n" +
                "  |  child exprs:\n" +
                "  |      [4: v4, BIGINT, true]\n" +
                "  |      [4: v4, BIGINT, true]\n" +
                "  |  pass-through-operands: all\n" +
                "  |  local exchange type: DIRECT");

        // union's right child is exchange instead of broadcast join, exchange is used to force data output
        assertCContains(sqlPlan, "|----10:EXCHANGE\n" +
                "  |       distribution type: SHUFFLE\n" +
                "  |       partition exprs: [4: v4, BIGINT, true]");

        // broadcast join itself is in one fragment
        assertCContains(sqlPlan, " 9:Project\n" +
                "  |  output columns:\n" +
                "  |  4 <-> [4: v4, BIGINT, true]\n" +
                "  |  cardinality: 1\n" +
                "  |  \n" +
                "  8:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BROADCAST)");
    }

    @Test
    public void testSkewJoinV2WithTwoAgg() throws Exception {
        // 2 phase agg has no requirement for its child
        String sql = "select v3 from t0 join[skew|t0.v1(1,2)] t1 on v1 = v4 group by t0.v3";
        String sqlPlan = getVerboseExplain(sql);

        // this is 2 phase agg
        assertCContains(sqlPlan, "11:AGGREGATE (update serialize)");
        // union's local exchange type is PassThrough
        assertNotContains(sqlPlan, "local exchange type");

        // union's right child is broadcast join instead of exchange node
        assertCContains(sqlPlan, "10:UNION\n" +
                "  |  child exprs:\n" +
                "  |      [3: v3, BIGINT, true]\n" +
                "  |      [3: v3, BIGINT, true]\n" +
                "  |  pass-through-operands: all");
        assertCContains(sqlPlan, "9:Project\n" +
                "  |    |  output columns:\n" +
                "  |    |  3 <-> [3: v3, BIGINT, true]\n" +
                "  |    |  cardinality: 1\n" +
                "  |    |  \n" +
                "  |    8:HASH JOIN\n" +
                "  |    |  join op: INNER JOIN (BROADCAST)");
    }

    @Test
    public void testSkewJoinV2WithCountStar() throws Exception {
        String sql = "select count(1) from t0 join[skew|t0.v1(1,2)] t1 on v1 = v4";
        String sqlPlan = getVerboseExplain(sql);
        assertCContains(sqlPlan, "9:Project\n" +
                "  |    |  output columns:\n" +
                "  |    |  9 <-> 1\n" +
                "  |    |  cardinality: 1\n" +
                "  |    |  \n" +
                "  |    8:HASH JOIN\n" +
                "  |    |  join op: INNER JOIN (BROADCAST)");
        assertCContains(sqlPlan, "5:Project\n" +
                "  |  output columns:\n" +
                "  |  9 <-> 1\n" +
                "  |  cardinality: 1\n" +
                "  |  \n" +
                "  4:HASH JOIN");
    }

    @Test
    public void testSkewJoinV2WithAsofLeftJoin() throws Exception {
        String sql = "select v2, v5 from t0 asof left join[skew|t0.v1(1,2)] t1 on v1 = v4 and v2 >= v5";
        String sqlPlan = getVerboseExplain(sql);
        assertCContains(sqlPlan, "join op: ASOF LEFT OUTER JOIN (PARTITIONED)");
    }

    @Test
    public void testSkewJoinV2NullableForLeftJoin() throws Exception {
        starRocksAssert.withTable("CREATE TABLE s_t1 (\n" +
                "    c_key      INT NOT NULL,\n" +
                "    c_tinyint  TINYINT,\n" +
                "    c_smallint SMALLINT,\n" +
                "    c_int      INT,\n" +
                "    c_bigint   BIGINT,\n" +
                "    c_largeint LARGEINT,\n" +
                "    c_float    FLOAT,\n" +
                "    c_double   DOUBLE,\n" +
                "    c_decimal  DECIMAL(26,2),\n" +
                "    c_date     DATE,\n" +
                "    c_datetime DATETIME,\n" +
                "    c_string   STRING\n" +
                ")\n" +
                "DUPLICATE KEY(c_key)\n" +
                "DISTRIBUTED BY HASH(c_key) BUCKETS 1\n" +
                "PROPERTIES (\n" +
                "    \"replication_num\"=\"1\"\n" +
                ");");
        starRocksAssert.withTable("CREATE TABLE s_t2 (\n" +
                "    c_key      INT NOT NULL,\n" +
                "    c_tinyint  TINYINT,\n" +
                "    c_smallint SMALLINT,\n" +
                "    c_int      INT,\n" +
                "    c_bigint   BIGINT,\n" +
                "    c_largeint LARGEINT,\n" +
                "    c_float    FLOAT,\n" +
                "    c_double   DOUBLE,\n" +
                "    c_decimal  DECIMAL(26,2),\n" +
                "    c_date     DATE,\n" +
                "    c_datetime DATETIME,\n" +
                "    c_string   STRING\n" +
                ")\n" +
                "DUPLICATE KEY(c_key)\n" +
                "DISTRIBUTED BY HASH(c_key) BUCKETS 1\n" +
                "PROPERTIES (\n" +
                "    \"replication_num\"=\"1\"\n" +
                ");");
        String query = "select t1.c_key, t2.c_key from s_t1 t1 left join [skew|t1.c_bigint(1,2,99999)] s_t2 t2 " +
                "on t1.c_bigint=t2.c_int and t1.c_key = t2.c_key order by t1.c_key, t2.c_key;\n";
        String plan = getVerboseExplain(query);
        // ensure concatenate operator's right table is nullable
        PlanTestBase.assertContains(plan, "  12:UNION\n" +
                "  |  child exprs:\n" +
                "  |      [1: c_key, INT, false] | [13: c_key, INT, true]\n" +
                "  |      [1: c_key, INT, false] | [13: c_key, INT, true]");
    }
}
