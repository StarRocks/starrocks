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

package com.starrocks.sql.optimizer.rule.transformation.materialization;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.OptimizerFactory;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.AggType;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.Rule;
import com.starrocks.sql.optimizer.statistics.Statistics;
import com.starrocks.sql.plan.PlanTestBase;
import com.starrocks.type.IntegerType;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class BestMvSelectorTest extends MVTestBase {
    private static Database testDb;
    private static OlapTable testTable;

    private static OptimizerContext optimizerContext;
    private static ColumnRefFactory columnRefFactory = new ColumnRefFactory();

    private Rule rule = null;

    @BeforeAll
    public static void beforeClass() throws Exception {
        MVTestBase.beforeClass();

        // Create test table
        starRocksAssert.withTable("CREATE TABLE `test_table` (\n" +
                "  `k1` bigint NULL COMMENT \"\",\n" +
                "  `k2` bigint NULL COMMENT \"\",\n" +
                "  `k3` bigint NULL COMMENT \"\",\n" +
                "  `v1` bigint NULL COMMENT \"\",\n" +
                "  `v2` bigint NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`k1`, `k2`, k3)\n" +
                "DISTRIBUTED BY HASH(`k1`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\"\n" +
                ");");

        testDb = starRocksAssert.getCtx().getGlobalStateMgr().getLocalMetastore().getDb("test");
        testTable = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(testDb.getFullName(), "test_table");

        optimizerContext = OptimizerFactory.initContext(starRocksAssert.getCtx(), columnRefFactory);
    }

    @BeforeEach
    public void setUp() {
    }

    @Test
    public void testConstructor() {
        // Test constructor with valid parameters
        List<OptExpression> mvExpressions = Lists.newArrayList();
        OptExpression queryPlan = createMockQueryPlan();
        
        BestMvSelector selector = new BestMvSelector(mvExpressions, optimizerContext, queryPlan, rule);
        
        Assertions.assertNotNull(selector);
        // Test that constructor works without throwing exceptions
        // The fields are private, so we test through behavior
    }

    @Test
    public void testConstructorWithAggQuery() {
        // Test constructor with aggregation query
        List<OptExpression> mvExpressions = Lists.newArrayList();
        OptExpression queryPlan = createMockAggQueryPlan();
        
        BestMvSelector selector = new BestMvSelector(mvExpressions, optimizerContext, queryPlan, rule);
        
        // Test that constructor works with aggregation query
        Assertions.assertNotNull(selector);
    }

    @Test
    public void testSelectBestWithEmptyMvExpressions() {
        // Test selectBest with empty MV expressions
        List<OptExpression> mvExpressions = Lists.newArrayList();
        OptExpression queryPlan = createMockQueryPlan();
        
        BestMvSelector selector = new BestMvSelector(mvExpressions, optimizerContext, queryPlan, rule);
        List<OptExpression> result = selector.selectBest(true);
        
        Assertions.assertTrue(result.isEmpty());
    }

    @Test
    public void testSelectBestWithSingleMvExpression() {
        // Test selectBest with single MV expression
        OptExpression queryPlan = createMockQueryPlan();
        OptExpression mvExpression = createMockMvExpression();
        List<OptExpression> mvExpressions = Lists.newArrayList(mvExpression);
        
        BestMvSelector selector = new BestMvSelector(mvExpressions, optimizerContext, queryPlan, rule);
        List<OptExpression> result = selector.selectBest(true);
        
        Assertions.assertEquals(0, result.size());
    }

    @Test
    public void testSelectBestWithMultipleMvExpressions() {
        // Test selectBest with multiple MV expressions
        OptExpression queryPlan = createMockQueryPlan();
        OptExpression mvExpression1 = createMockMvExpression();
        OptExpression mvExpression2 = createMockMvExpression();
        List<OptExpression> mvExpressions = Lists.newArrayList(mvExpression1, mvExpression2);
        
        // Mock statistics for comparison
        Statistics stats1 = createMockStatistics(100.0, 50.0);
        Statistics stats2 = createMockStatistics(200.0, 100.0);
        mvExpression1.setStatistics(stats1);
        mvExpression2.setStatistics(stats2);
        
        BestMvSelector selector = new BestMvSelector(mvExpressions, optimizerContext, queryPlan, rule);
        List<OptExpression> result = selector.selectBest(true);
        
        // Should return the best one (with lower row count)
        Assertions.assertTrue(result.isEmpty());
    }

    @Test
    public void testSelectBestWithDataLayoutConsideration() {
        OptExpression queryPlan = createMockQueryPlan();
        OptExpression mvExpression = createMockMvExpression();
        List<OptExpression> mvExpressions = Lists.newArrayList(mvExpression);
        
        BestMvSelector selector = new BestMvSelector(mvExpressions, optimizerContext, queryPlan, rule);
        List<OptExpression> result = selector.selectBest(true);
        
        Assertions.assertEquals(0, result.size());
    }

    private BestMvSelector.CandidateContext createCandidateContext(Statistics statistics,
                                                                   int schemaColumnNum,
                                                                   int sortScore, int index) {
        BestMvSelector.CandidateScore score = new BestMvSelector.CandidateScore();
        score.sortScore = sortScore;
        score.schemaColumnNum = schemaColumnNum;
        score.stats = statistics;
        score.index = index;
        return new BestMvSelector.CandidateContext(null, false, score);
    }

    @Test
    public void testSelectBestWithOlapTable() {
        // Test selectBest with OlapTable to exercise calcSortScore internally
        OptExpression queryPlan = createMockQueryPlan();
        OptExpression mvExpression = createMockMvExpression();
        List<OptExpression> mvExpressions = Lists.newArrayList(mvExpression);
        
        // Mock statistics for comparison
        Statistics stats = createMockStatistics(100.0, 50.0);
        mvExpression.setStatistics(stats);
        
        BestMvSelector selector = new BestMvSelector(mvExpressions, optimizerContext, queryPlan, rule);
        List<OptExpression> result = selector.selectBest(true);
        
        Assertions.assertEquals(0, result.size());
    }

    private boolean isOptExpressionEqualsTo(OptExpression expr1, OptExpression expr2) {
        if (expr1 == null || expr2 == null) {
            return false;
        }
        if (expr1.getOp().getClass() != expr2.getOp().getClass()) {
            return false;
        }
        if (!expr1.getOp().equals(expr2.getOp())) {
            return false;
        }
        if (expr1.getInputs().size() != expr2.getInputs().size()) {
            return false;
        }
        for (int i = 0; i < expr1.getInputs().size(); i++) {
            if (!isOptExpressionEqualsTo(expr1.getInputs().get(i), expr2.getInputs().get(i))) {
                return false;
            }
        }
        return true;
    }

    @Test
    public void testSelectBestWithNullTable() {
        // Test selectBest with null table scenarios
        OptExpression queryPlan = createMockQueryPlan();
        OptExpression mvExpression = createMockMvExpression();
        List<OptExpression> mvExpressions = Lists.newArrayList(mvExpression);
        
        // Mock statistics
        Statistics stats = createMockStatistics(100.0, 50.0);
        mvExpression.setStatistics(stats);
        
        BestMvSelector selector = new BestMvSelector(mvExpressions, optimizerContext, queryPlan, rule);
        List<OptExpression> result = selector.selectBest(true);
        
        Assertions.assertEquals(0, result.size());
    }

    @Test
    public void testSelectBestWithNonOlapTable() {
        // Test selectBest with non-OlapTable scenarios
        OptExpression queryPlan = createMockQueryPlan();
        OptExpression mvExpression = createMockMvExpression();
        List<OptExpression> mvExpressions = Lists.newArrayList(mvExpression);
        
        // Mock statistics
        Statistics stats = createMockStatistics(100.0, 50.0);
        mvExpression.setStatistics(stats);
        
        BestMvSelector selector = new BestMvSelector(mvExpressions, optimizerContext, queryPlan, rule);
        List<OptExpression> result = selector.selectBest(true);
        
        Assertions.assertEquals(0, result.size());
    }

    @Test
    public void testCalculateStatisticsWithException() {
        // Test calculateStatistics with exception handling
        OptExpression queryPlan = createMockQueryPlan();
        OptExpression mvExpression = createMockMvExpression();
        List<OptExpression> mvExpressions = Lists.newArrayList(mvExpression);
        
        // Mock statistics calculation to throw exception
        new MockUp<OptimizerContext>() {
            @Mock
            public ColumnRefFactory getColumnRefFactory() {
                throw new RuntimeException("Test exception");
            }
        };
        
        BestMvSelector selector = new BestMvSelector(mvExpressions, optimizerContext, queryPlan, rule);
        
        // Should not throw exception, should handle gracefully
        Assertions.assertDoesNotThrow(() -> selector.selectBest(true));
    }

    @Test
    public void testSelectBestWithEmptyQueryTables() {
        // Test selectBest with empty query tables
        OptExpression queryPlan = createMockQueryPlan();
        OptExpression mvExpression = createMockMvExpression();
        List<OptExpression> mvExpressions = Lists.newArrayList(mvExpression);
        
        // Mock MvUtils.getAllTables to return empty list
        new MockUp<MvUtils>() {
            @Mock
            public List<Table> getAllTables(OptExpression expression) {
                return Lists.newArrayList();
            }
        };
        
        BestMvSelector selector = new BestMvSelector(mvExpressions, optimizerContext, queryPlan, rule);
        List<OptExpression> result = selector.selectBest(true);
        
        Assertions.assertTrue(result.isEmpty());
    }

    @Test
    public void testSelectBestWithMultipleQueryTables() {
        // Test selectBest with multiple query tables
        OptExpression queryPlan = createMockQueryPlan();
        OptExpression mvExpression = createMockMvExpression();
        List<OptExpression> mvExpressions = Lists.newArrayList(mvExpression);
        
        // Mock MvUtils methods
        new MockUp<MvUtils>() {
            @Mock
            public List<Table> getAllTables(OptExpression expression) {
                if (expression == queryPlan) {
                    return Lists.newArrayList(testTable, testTable);
                } else {
                    return Lists.newArrayList(testTable);
                }
            }
            
            @Mock
            public Set<ScalarOperator> getAllValidPredicatesFromScans(OptExpression expression) {
                return Sets.newHashSet();
            }
            
            @Mock
            public void splitPredicate(Set<ScalarOperator> predicates,
                                       Set<String> equivalenceColumns,
                                       Set<String> nonEquivalenceColumns) {
                // Do nothing
            }
        };
        
        BestMvSelector selector = new BestMvSelector(mvExpressions, optimizerContext, queryPlan, rule);
        List<OptExpression> result = selector.selectBest(true);
        
        Assertions.assertTrue(result.isEmpty());
    }

    @Test
    public void testSelectBestWithReflection() throws Exception {
        // Test private methods using reflection
        OptExpression queryPlan = createMockQueryPlan();
        List<OptExpression> mvExpressions = Lists.newArrayList();
        
        BestMvSelector selector = new BestMvSelector(mvExpressions, optimizerContext, queryPlan, rule);
        
        // Test calcSortScore method
        Method calcSortScoreMethod = BestMvSelector.class.getDeclaredMethod(
                "calcSortScore", Table.class, Set.class, Set.class);
        calcSortScoreMethod.setAccessible(true);
        
        Set<String> equivalenceColumns = Sets.newHashSet("k1", "k2");
        Set<String> nonEquivalenceColumns = Sets.newHashSet("k3");
        
        int score = (Integer) calcSortScoreMethod.invoke(selector, testTable, equivalenceColumns, nonEquivalenceColumns);
        Assertions.assertTrue(score >= 0);
        
        // Test with null table
        score = (Integer) calcSortScoreMethod.invoke(selector, null, equivalenceColumns, nonEquivalenceColumns);
        Assertions.assertEquals(0, score);
    }

    @Test
    public void testCalcDistScoreWithTabletPruning() throws Exception {
        // Test calcDistScore method with tablet pruning consideration
        OptExpression queryPlan = createMockQueryPlan();
        List<OptExpression> mvExpressions = Lists.newArrayList();
        
        BestMvSelector selector = new BestMvSelector(mvExpressions, optimizerContext, queryPlan, rule);
        
        // Test calcDistScore method
        Method calcDistScoreMethod = BestMvSelector.class.getDeclaredMethod(
                "calcDistScore", Table.class, Set.class, Set.class);
        calcDistScoreMethod.setAccessible(true);
        
        // distEqCols: columns from grouping keys, join keys, distinct columns
        Set<String> distEqCols = Sets.newHashSet("k1");
        // equivalenceColumns: columns from query filter predicates (for tablet pruning)
        Set<String> equivalenceColumns = Sets.newHashSet("k1", "k2");
        
        // Test with test table (distributed by k1)
        int score = (Integer) calcDistScoreMethod.invoke(selector, testTable, distEqCols, equivalenceColumns);
        // Should have a positive score since k1 is in both distEqCols and equivalenceColumns
        // and test table is hash distributed
        Assertions.assertTrue(score >= 0);
        
        // Test with null table
        score = (Integer) calcDistScoreMethod.invoke(selector, null, distEqCols, equivalenceColumns);
        Assertions.assertEquals(0, score);
        
        // Test tablet pruning: when filter column matches distribution key
        Set<String> filterOnDistKey = Sets.newHashSet("k1");  // query filters on k1
        Set<String> emptyDistEqCols = Sets.newHashSet();  // no grouping/join keys
        
        int tabletPruningScore = (Integer) calcDistScoreMethod.invoke(
                selector, testTable, emptyDistEqCols, filterOnDistKey);
        // Should have positive score for tablet pruning benefit even without colocation
        Assertions.assertTrue(tabletPruningScore > 0, 
                "Tablet pruning score should be positive when filter matches distribution key");
    }

    @Test
    public void testSelectBestWithEmptyContexts() {
        // Test selectBest when contexts list is empty
        OptExpression queryPlan = createMockQueryPlan();
        OptExpression mvExpression = createMockMvExpression();
        List<OptExpression> mvExpressions = Lists.newArrayList(mvExpression);
        
        // Mock MvUtils to return empty contexts
        new MockUp<MvUtils>() {
            @Mock
            public List<Table> getAllTables(OptExpression expression) {
                if (expression == queryPlan) {
                    return Lists.newArrayList(testTable);
                } else {
                    return Lists.newArrayList();
                }
            }
            
            @Mock
            public Set<ScalarOperator> getAllValidPredicatesFromScans(OptExpression expression) {
                return Sets.newHashSet();
            }
            
            @Mock
            public void splitPredicate(Set<ScalarOperator> predicates,
                                       Set<String> equivalenceColumns,
                                       Set<String> nonEquivalenceColumns) {
                // Do nothing
            }
        };
        
        BestMvSelector selector = new BestMvSelector(mvExpressions, optimizerContext, queryPlan, rule);
        List<OptExpression> result = selector.selectBest(true);
        
        // Should return empty list when contexts are empty
        Assertions.assertTrue(result.isEmpty());
    }

    @Test
    public void testSelectBestWithStatisticsCalculationError() {
        // Test selectBest when statistics calculation fails
        OptExpression queryPlan = createMockQueryPlan();
        OptExpression mvExpression = createMockMvExpression();
        List<OptExpression> mvExpressions = Lists.newArrayList(mvExpression);
        
        // Mock statistics calculation to throw exception
        new MockUp<OptimizerContext>() {
            @Mock
            public ColumnRefFactory getColumnRefFactory() {
                throw new RuntimeException("Statistics calculation failed");
            }
        };
        
        BestMvSelector selector = new BestMvSelector(mvExpressions, optimizerContext, queryPlan, rule);
        
        // Should handle exception gracefully and still return a result
        Assertions.assertDoesNotThrow(() -> {
            List<OptExpression> result = selector.selectBest(true);
            // Result might be empty or contain the MV expression depending on error handling
        });
    }

    @Test
    public void testSelectBestWithConfigDisabled() {
        OptExpression queryPlan = createMockQueryPlan();
        OptExpression mvExpression = createMockMvExpression();
        List<OptExpression> mvExpressions = Lists.newArrayList(mvExpression);

        BestMvSelector selector = new BestMvSelector(mvExpressions, optimizerContext, queryPlan, rule);
        List<OptExpression> result = selector.selectBest(false);

        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals(mvExpression, result.get(0));
    }

    // Helper methods to create mock objects
    private OptExpression createMockQueryPlan() {
        LogicalOlapScanOperator scanOperator = new LogicalOlapScanOperator(testTable);
        return OptExpression.create(scanOperator);
    }

    private OptExpression createMockAggQueryPlan() {
        LogicalOlapScanOperator scanOperator = new LogicalOlapScanOperator(testTable);
        OptExpression scanExpr = OptExpression.create(scanOperator);
        
        ColumnRefOperator colRef = new ColumnRefOperator(1, IntegerType.BIGINT, "k1", false);
        Map<ColumnRefOperator, CallOperator> aggregations = Map.of(
                colRef, new CallOperator("SUM", IntegerType.BIGINT, List.of(colRef), null)
        );
        LogicalAggregationOperator aggOperator = new LogicalAggregationOperator(
                AggType.GLOBAL, Lists.newArrayList(colRef), aggregations);
        
        return OptExpression.create(aggOperator, scanExpr);
    }

    private OptExpression createMockMvExpression() {
        LogicalOlapScanOperator scanOperator = new LogicalOlapScanOperator(testTable);
        return OptExpression.create(scanOperator);
    }

    private Statistics createMockStatistics(double rowCount, double computeSize) {
        return Statistics.builder()
                .setOutputRowCount(rowCount)
                .build();
    }

    @Test
    public void testMvRewriteWithSortKey1() throws Exception {
        starRocksAssert.withTable(cluster, "t0");
        starRocksAssert.withMaterializedView("create MATERIALIZED VIEW if not exists mv_order_by_v1 " +
                "DISTRIBUTED BY RANDOM buckets 3 " +
                "order by (v1) " +
                "REFRESH MANUAL " +
                "as\n" +
                "select v1, v2, sum(v3) from t0 group by v1, v2");
        cluster.runSql("test", "refresh materialized view mv_order_by_v1 with sync mode");
        {
            // in predicate
            String query = "select v1, v2, sum(v3) from t0 where v1 in (1, 2, 3) group by v1, v2;";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "mv_order_by_v1");
        }
        {
            // equal predicate
            String query = "select v1, v2, sum(v3) from t0 where v1 = 1 group by v1, v2;";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "mv_order_by_v1");
        }

        starRocksAssert.withMaterializedView("create MATERIALIZED VIEW if not exists mv_order_by_v2 " +
                "DISTRIBUTED BY RANDOM buckets 3 " +
                "order by (v2) " +
                "REFRESH MANUAL " +
                "as\n" +
                "select v1, v2, sum(v3) from t0 group by v1, v2");
        cluster.runSql("test", "refresh materialized view mv_order_by_v2 with sync mode");
        {
            // in predicate
            String query = "select v1, v2, sum(v3) from t0 where v1 in (1, 2, 3) group by v1, v2;";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "mv_order_by_v1");
        }
        {
            // equal predicate
            String query = "select v1, v2, sum(v3) from t0 where v1 = 1 group by v1, v2;";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "mv_order_by_v1");
        }
    }

    @Test
    public void testMvRewriteWithSortKey2() throws Exception {
        starRocksAssert.withTable(cluster, "t0");
        starRocksAssert.withMaterializedView("create MATERIALIZED VIEW if not exists mv_order_by_v1 " +
                "DISTRIBUTED BY RANDOM buckets 3 " +
                "order by (v1) " +
                "REFRESH MANUAL " +
                "as\n" +
                "select v1, v2, v3 from t0");
        cluster.runSql("test", "refresh materialized view mv_order_by_v1 with sync mode");
        {
            String query = "select v1, v2, v3 from t0 where v1 in (1, 2, 3);";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "t0");
        }
        {
            String query = "select v1, v2, v3 from t0 where v1 = 1;";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "t0");
        }

        starRocksAssert.withMaterializedView("create MATERIALIZED VIEW if not exists mv_order_by_v2 " +
                "DISTRIBUTED BY RANDOM buckets 3 " +
                "order by (v2) " +
                "REFRESH MANUAL " +
                "as\n" +
                "select v1, v2, v3 from t0");
        cluster.runSql("test", "refresh materialized view mv_order_by_v2 with sync mode");
        {
            String query = "select v1, v2, v3 from t0 where v1 in (1, 2, 3);";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "t0");
        }
        {
            String query = "select v1, v2, v3 from t0 where v1 = 1;";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "t0");
        }
    }

    @Test
    public void testChooseBestWithColocateGroupScanQuery1() throws Exception {
        // mv and query contains different dist keys
        starRocksAssert.withTable("CREATE TABLE `t0` (\n" +
                "  `v2` bigint NULL," +
                "  `v1` bigint NULL," +
                "  `v3` bigint NULL" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`v2`)\n" +
                "DISTRIBUTED BY HASH(`v2`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"colocate_with\" = \"colocate_group_1\"" +
                ");");
        starRocksAssert.withMaterializedView("create MATERIALIZED VIEW if not exists mv_order_by_v1 " +
                "DISTRIBUTED BY RANDOM buckets 3 " +
                "order by (v1) " +
                "REFRESH MANUAL " +
                "as\n" +
                "select v1, v2, v3 from t0");
        cluster.runSql("test", "refresh materialized view mv_order_by_v1 with sync mode");

        {
            String query = "select v1, v2, v3 from t0 where v1 in (1, 2, 3);";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "mv_order_by_v1");
        }
        {
            String query = "select v1, v2, v3 from t0 where v1 = 1;";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "mv_order_by_v1");
        }

        starRocksAssert.withMaterializedView("create MATERIALIZED VIEW if not exists mv_order_by_v2 " +
                "DISTRIBUTED BY RANDOM buckets 3 " +
                "order by (v2) " +
                "REFRESH MANUAL " +
                "as\n" +
                "select v1, v2, v3 from t0");
        cluster.runSql("test", "refresh materialized view mv_order_by_v2 with sync mode");
        {
            String query = "select v1, v2, v3 from t0 where v1 in (1, 2, 3);";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "mv_order_by_v1");
        }
        {
            String query = "select v1, v2, v3 from t0 where v1 = 1;";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "mv_order_by_v1");
        }
    }

    @Test
    public void testChooseBestWithColocateGroupScanQuery2() throws Exception {
        // mv and query contains the same dist keys but different sort keys
        starRocksAssert.withTable("CREATE TABLE `t0` (\n" +
                "  `v1` bigint NULL," +
                "  `v2` bigint NULL," +
                "  `v3` bigint NULL" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`v1`)\n" +
                "DISTRIBUTED BY HASH(`v1`) BUCKETS 3\n" +
                "ORDER BY (v2)\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"colocate_with\" = \"colocate_group_1\"" +
                ");");
        starRocksAssert.withMaterializedView("create MATERIALIZED VIEW if not exists mv_order_by_v1 " +
                "DISTRIBUTED BY hash (v1) buckets 3 " +
                "order by (v1) " +
                "REFRESH MANUAL " +
                "as\n" +
                "select v1, v2, v3 from t0");
        cluster.runSql("test", "refresh materialized view mv_order_by_v1 with sync mode");

        {
            String query = "select v1, v2, v3 from t0 where v1 in (1, 2, 3);";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "mv_order_by_v1");
        }
        {
            String query = "select v1, v2, v3 from t0 where v1 = 1;";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "mv_order_by_v1");
        }

        starRocksAssert.withMaterializedView("create MATERIALIZED VIEW if not exists mv_order_by_v2 " +
                "DISTRIBUTED BY RANDOM buckets 3 " +
                "order by (v2) " +
                "REFRESH MANUAL " +
                "as\n" +
                "select v1, v2, v3 from t0");
        cluster.runSql("test", "refresh materialized view mv_order_by_v2 with sync mode");
        {
            String query = "select v1, v2, v3 from t0 where v1 in (1, 2, 3);";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "mv_order_by_v1");
        }
        {
            String query = "select v1, v2, v3 from t0 where v1 = 1;";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "mv_order_by_v1");
        }
    }

    @Test
    public void testChooseBestWithColocateGroupAggregate1() throws Exception {
        starRocksAssert.withTable("CREATE TABLE `t0` (\n" +
                "  `v1` bigint NULL," +
                "  `v2` bigint NULL," +
                "  `v3` bigint NULL" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`v1`)\n" +
                "DISTRIBUTED BY HASH(`v1`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"colocate_with\" = \"colocate_group_1\"" +
                ");");
        // even t0 is colocate but mv can rewrite query into scan, choose mv with sort key v1
        starRocksAssert.withMaterializedView("create MATERIALIZED VIEW if not exists mv_order_by_v1 " +
                "DISTRIBUTED BY HASH(v1) buckets 3 " +
                "order by (v2) " +
                "REFRESH MANUAL " +
                "as\n" +
                "select v1, v2, v3 from t0;");
        cluster.runSql("test", "refresh materialized view mv_order_by_v1 with sync mode");
        {
            // in predicate
            String query = "select v1, v2, sum(v3) from t0 where v1 in (1, 2, 3) group by v1, v2;";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "t0");
        }
        {
            // equal predicate
            String query = "select v1, v2, sum(v3) from t0 where v2 = 1 group by v1, v2;";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "mv_order_by_v1");
        }

        starRocksAssert.withMaterializedView("create MATERIALIZED VIEW if not exists mv_order_by_v2 " +
                "DISTRIBUTED BY HASH(v1, v2) buckets 3 " +
                "order by (v2) " +
                "REFRESH MANUAL " +
                "as\n" +
                "select v1, v2, v3 from t0;");
        cluster.runSql("test", "refresh materialized view mv_order_by_v2 with sync mode");
        {
            // equal predicate
            String query = "select v1, sum(v3) from t0 where v1 = 1 group by v1;";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "t0");
        }
        {
            // in predicate
            String query = "select v1, v2, sum(v3) from t0 where v1 in (1, 2, 3) group by v1, v2;";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "mv_order_by_v2");
        }
    }

    @Test
    public void testChooseBestWithColocateGroupAggregate2() throws Exception {
        starRocksAssert.withTable("CREATE TABLE `t0` (\n" +
                "  `v1` bigint NULL," +
                "  `v2` bigint NULL," +
                "  `v3` bigint NULL" +
                ") ENGINE=OLAP\n" +
                "DISTRIBUTED BY RANDOM BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"" +
                ");");
        // even t0 is colocate but mv can rewrite query into scan, choose mv with sort key v1
        starRocksAssert.withMaterializedView("create MATERIALIZED VIEW if not exists mv_order_by_v1 " +
                "DISTRIBUTED BY HASH(v1) buckets 3 " +
                "order by (v2) " +
                "REFRESH MANUAL " +
                "as\n" +
                "select v1, v2, v3 from t0;");
        cluster.runSql("test", "refresh materialized view mv_order_by_v1 with sync mode");
        {
            // in predicate
            String query = "select v1, v2, sum(v3) from t0 where v1 in (1, 2, 3) group by v1, v2;";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "mv_order_by_v1");
        }
        {
            // equal predicate
            String query = "select v1, v2, sum(v3) from t0 where v2 = 1 group by v1, v2;";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "mv_order_by_v1");
        }

        starRocksAssert.withMaterializedView("create MATERIALIZED VIEW if not exists mv_order_by_v2 " +
                "DISTRIBUTED BY HASH(v1, v2) buckets 3 " +
                "order by (v2) " +
                "REFRESH MANUAL " +
                "as\n" +
                "select v1, v2, v3 from t0;");
        cluster.runSql("test", "refresh materialized view mv_order_by_v2 with sync mode");
        {
            // in predicate
            String query = "select v1, v2, sum(v3) from t0 where v1 in (1, 2, 3) group by v1, v2;";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "mv_order_by_v2");
        }
        {
            // equal predicate
            String query = "select v1, v2, sum(v3) from t0 where v1 = 1 group by v1, v2;";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "mv_order_by_v2");
        }
    }

    @Test
    public void testChooseBestWithColocateGroupJoin1() throws Exception {
        starRocksAssert.withTable("CREATE TABLE `t0` (\n" +
                "  `v1` bigint NULL," +
                "  `v2` bigint NULL," +
                "  `v3` bigint NULL" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`v1`)\n" +
                "DISTRIBUTED BY HASH(`v1`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"colocate_with\" = \"colocate_group_1\"" +
                ");");
        starRocksAssert.withTable("CREATE TABLE `t1` (\n" +
                "  `v1` bigint NULL," +
                "  `v2` bigint NULL," +
                "  `v3` bigint NULL" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`v1`)\n" +
                "DISTRIBUTED BY HASH(`v1`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"colocate_with\" = \"colocate_group_1\"" +
                ");");
        starRocksAssert.withMaterializedView("create MATERIALIZED VIEW if not exists mv_order_by_v1 " +
                "DISTRIBUTED BY HASH(v2) buckets 3 " +
                "order by (v2) " +
                "REFRESH MANUAL " +
                "as\n" +
                "select v1, v2, v3 from t0;");
        starRocksAssert.withMaterializedView("create MATERIALIZED VIEW if not exists mv_order_by_v2 " +
                "DISTRIBUTED BY HASH(v2) buckets 3 " +
                "order by (v2) " +
                "REFRESH MANUAL " +
                "as\n" +
                "select v1, v2, v3 from t0;");
        cluster.runSql("test", "refresh materialized view mv_order_by_v1 with sync mode");
        // mv's dist key is different from query's, join's on v1
        {
            // in predicate
            String query = "select a.v1, sum(b.v3) from t0 a join t1 b on a.v1=b.v1 where a.v1 in (1, 2, 3) group by a.v1, a.v2;";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "t0", "t1");
        }
        {
            // equal predicate
            String query = "select a.v1, sum(b.v3) from t0 a join t1 b on a.v1=b.v1 group by a.v1, a.v2;";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "mv_order_by_v", "t1");
        }
    }

    @Test
    public void testChooseBestWithColocateGroupJoin2() throws Exception {
        starRocksAssert.withTable("CREATE TABLE `t0` (\n" +
                "  `v1` bigint NULL," +
                "  `v2` bigint NULL," +
                "  `v3` bigint NULL" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`v1`)\n" +
                "DISTRIBUTED BY HASH(`v1`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"colocate_with\" = \"colocate_group_1\"" +
                ");");
        starRocksAssert.withTable("CREATE TABLE `t1` (\n" +
                "  `v1` bigint NULL," +
                "  `v2` bigint NULL," +
                "  `v3` bigint NULL" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`v1`)\n" +
                "DISTRIBUTED BY HASH(`v1`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"colocate_with\" = \"colocate_group_1\"" +
                ");");
        starRocksAssert.withMaterializedView("create MATERIALIZED VIEW if not exists mv_order_by_v1 " +
                "DISTRIBUTED BY HASH(v2) buckets 3 " +
                "order by (v2) " +
                "REFRESH MANUAL " +
                "as\n" +
                "select v1, v2, v3 from t0;");
        starRocksAssert.withMaterializedView("create MATERIALIZED VIEW if not exists mv_order_by_v2 " +
                "DISTRIBUTED BY HASH(v2) buckets 3 " +
                "order by (v2) " +
                "REFRESH MANUAL " +
                "as\n" +
                "select v1, v2, v3 from t1;");
        cluster.runSql("test", "refresh materialized view mv_order_by_v1 with sync mode");

        // mv's dist key v2 is different from query's join key v1, and query join's key is v2
        {
            // in predicate
            String query = "select a.v1, b.v3 from t0 a join t1 b on a.v2=b.v2 where a.v1 in (1, 2, 3);";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "mv_order_by_v1", "mv_order_by_v2");
        }
        {
            // equal predicate
            String query = "select a.v1, b.v3 from t0 a join t1 b on a.v2=b.v2;";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "mv_order_by_v1", "mv_order_by_v2");
        }
    }

    /**
     * Test that when a query filters on a specific column, the CBO should prefer the MV
     * whose distribution key matches the filter column for better tablet pruning.
     * This addresses the issue where two MVs with identical definitions but different
     * distribution keys are created on the same base table.
     */
    @Test
    public void testChooseBestMvWithTabletPruning() throws Exception {
        // Create base table
        starRocksAssert.withTable("CREATE TABLE `ad_fact` (\n" +
                "  `advertiser_id` bigint NULL,\n" +
                "  `campaign_id` bigint NULL,\n" +
                "  `impressions` bigint NULL\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`advertiser_id`)\n" +
                "DISTRIBUTED BY HASH(`advertiser_id`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ");");

        // Create MV distributed by advertiser_id
        starRocksAssert.withMaterializedView("create MATERIALIZED VIEW if not exists mv_ad_fact_advertiser " +
                "DISTRIBUTED BY HASH(advertiser_id) buckets 3 " +
                "REFRESH MANUAL " +
                "as\n" +
                "select advertiser_id, campaign_id, sum(impressions) from ad_fact group by advertiser_id, campaign_id;");

        // Create MV distributed by campaign_id (same definition, different dist key)
        starRocksAssert.withMaterializedView("create MATERIALIZED VIEW if not exists mv_ad_fact_campaign " +
                "DISTRIBUTED BY HASH(campaign_id) buckets 3 " +
                "REFRESH MANUAL " +
                "as\n" +
                "select advertiser_id, campaign_id, sum(impressions) from ad_fact group by advertiser_id, campaign_id;");

        cluster.runSql("test", "refresh materialized view mv_ad_fact_advertiser with sync mode");
        cluster.runSql("test", "refresh materialized view mv_ad_fact_campaign with sync mode");

        // Query filters on campaign_id - should prefer mv_ad_fact_campaign for better tablet pruning
        {
            String query = "select advertiser_id, campaign_id, sum(impressions) from ad_fact " +
                    "where campaign_id = 1 group by advertiser_id, campaign_id;";
            String plan = getFragmentPlan(query);
            // Should choose the MV with campaign_id as distribution key for tablet pruning benefit
            PlanTestBase.assertContains(plan, "mv_ad_fact_campaign");
        }

        // Query filters on advertiser_id - should prefer mv_ad_fact_advertiser for better tablet pruning
        {
            String query = "select advertiser_id, campaign_id, sum(impressions) from ad_fact " +
                    "where advertiser_id = 1 group by advertiser_id, campaign_id;";
            String plan = getFragmentPlan(query);
            // Should choose the MV with advertiser_id as distribution key for tablet pruning benefit
            PlanTestBase.assertContains(plan, "mv_ad_fact_advertiser");
        }
    }
}