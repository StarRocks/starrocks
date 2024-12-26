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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.MvPlanContext;
import com.starrocks.catalog.Table;
import com.starrocks.common.Pair;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.optimizer.CachingMvPlanContextBuilder;
import com.starrocks.sql.optimizer.MaterializationContext;
import com.starrocks.sql.optimizer.MaterializedViewOptimizer;
import com.starrocks.sql.optimizer.Memo;
import com.starrocks.sql.optimizer.MvRewritePreprocessor;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.Optimizer;
import com.starrocks.sql.optimizer.OptimizerConfig;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.PhysicalPropertySet;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalFilterOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalTreeAnchorOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.physical.PhysicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.Rule;
import com.starrocks.sql.optimizer.rule.RuleSetType;
import com.starrocks.sql.optimizer.rule.RuleType;
import com.starrocks.sql.optimizer.task.RewriteTreeTask;
import com.starrocks.sql.optimizer.task.TaskContext;
import com.starrocks.sql.optimizer.transformer.LogicalPlan;
import com.starrocks.sql.optimizer.transformer.RelationTransformer;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.utframe.UtFrameUtils;
import org.assertj.core.util.Sets;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;
import java.util.stream.Collectors;

import static com.starrocks.planner.MaterializedViewTestBase.getRefBaseTablePartitionColumn;

public class MvRewritePreprocessorTest extends MVTestBase {
    @BeforeClass
    public static void beforeClass() throws Exception {
        MVTestBase.beforeClass();
        starRocksAssert.useTable("t0");
        starRocksAssert.useTable("t1");
        starRocksAssert.useTable("t2");
    }

    private static class TimeoutRule extends Rule {

        protected TimeoutRule(RuleType type, Pattern pattern) {
            super(type, pattern);
        }

        @Override
        public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
            try {
                Thread.sleep(300);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            return null;
        }
    }

    @Test
    public void testOptimizer() throws Exception {
        Optimizer optimizer = new Optimizer();
        Assert.assertFalse(optimizer.getOptimizerConfig().isRuleBased());
        Assert.assertFalse(optimizer.getOptimizerConfig().isRuleDisable(RuleType.TF_MERGE_TWO_PROJECT));
        Assert.assertFalse(optimizer.getOptimizerConfig().isRuleSetTypeDisable(RuleSetType.AGGREGATE_REWRITE));

        OptimizerConfig optimizerConfig = new OptimizerConfig(OptimizerConfig.OptimizerAlgorithm.RULE_BASED);
        optimizerConfig.disableRule(RuleType.TF_MERGE_TWO_PROJECT);
        optimizerConfig.disableRuleSet(RuleSetType.PUSH_DOWN_PREDICATE);
        Optimizer optimizer1 = new Optimizer(optimizerConfig);
        Assert.assertTrue(optimizer1.getOptimizerConfig().isRuleBased());
        Assert.assertFalse(optimizer1.getOptimizerConfig().isRuleDisable(RuleType.TF_MERGE_TWO_AGG_RULE));
        Assert.assertTrue(optimizer1.getOptimizerConfig().isRuleDisable(RuleType.TF_MERGE_TWO_PROJECT));
        Assert.assertFalse(optimizer1.getOptimizerConfig().isRuleSetTypeDisable(RuleSetType.COLLECT_CTE));
        Assert.assertTrue(optimizer1.getOptimizerConfig().isRuleSetTypeDisable(RuleSetType.PUSH_DOWN_PREDICATE));

        String sql = "select v1, sum(v3) from t0 where v1 < 10 group by v1";
        Pair<String, ExecPlan> result = UtFrameUtils.getPlanAndFragment(connectContext, sql);
        Assert.assertNotNull(result);

        StatementBase stmt = UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        QueryStatement query = (QueryStatement) stmt;

        // 1. Build Logical plan
        ColumnRefFactory columnRefFactory = new ColumnRefFactory();
        LogicalPlan logicalPlan = new RelationTransformer(columnRefFactory, connectContext)
                .transformWithSelectLimit(query.getQueryRelation());
        OptExpression expr = optimizer.optimize(connectContext, logicalPlan.getRoot(), new PhysicalPropertySet(),
                new ColumnRefSet(logicalPlan.getOutputColumn()), columnRefFactory);
        Assert.assertTrue(expr.getInputs().get(0).getOp() instanceof PhysicalOlapScanOperator);
        Assert.assertNotNull(expr.getInputs().get(0).getOp().getPredicate());

        OptExpression expr1 = optimizer1.optimize(connectContext, logicalPlan.getRoot(), new PhysicalPropertySet(),
                new ColumnRefSet(logicalPlan.getOutputColumn()), columnRefFactory);
        Assert.assertTrue(expr1.getInputs().get(0).getOp() instanceof LogicalFilterOperator);

        // test timeout
        long timeout = connectContext.getSessionVariable().getOptimizerExecuteTimeout();
        connectContext.getSessionVariable().setOptimizerExecuteTimeout(1);
        TaskContext rootTaskContext = optimizer1.getContext().getTaskContext();
        // just give a valid RuleType
        Rule timeoutRule = new TimeoutRule(RuleType.TF_MV_ONLY_JOIN_RULE, Pattern.create(OperatorType.PATTERN));
        OptExpression tree = OptExpression.create(new LogicalTreeAnchorOperator(), logicalPlan.getRoot());
        optimizer1.getContext().getTaskScheduler().pushTask(
                new RewriteTreeTask(rootTaskContext, tree, Lists.newArrayList(timeoutRule), true));
        try {
            optimizer1.getContext().getTaskScheduler().executeTasks(rootTaskContext);
        } catch (Exception e) {
            e.getMessage().contains("StarRocks planner use long time 1 ms in logical phase");
        } finally {
            connectContext.getSessionVariable().setOptimizerExecuteTimeout(timeout);
        }
    }

    @Test
    public void testPreprocessMvNonPartitionMv() throws Exception {
        executeInsertSql(connectContext, "insert into t0 values(10, 20, 30)");
        starRocksAssert.withMaterializedView("create materialized view mv_1 distributed by hash(`v1`) " +
                "as select v1, v2, sum(v3) as total from t0 group by v1, v2");
        starRocksAssert.withMaterializedView("create materialized view mv_2 distributed by hash(`v1`) " +
                "as select v1, sum(total) as total from mv_1 group by v1");
        refreshMaterializedView("test", "mv_1");
        refreshMaterializedView("test", "mv_2");

        String sql = "select v1, sum(v3) from t0 group by v1";
        StatementBase stmt = UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        QueryStatement query = (QueryStatement) stmt;

        // 1. Build Logical plan
        ColumnRefFactory columnRefFactory = new ColumnRefFactory();
        LogicalPlan logicalPlan = new RelationTransformer(columnRefFactory, connectContext)
                .transformWithSelectLimit(query.getQueryRelation());
        Optimizer optimizer = new Optimizer();
        OptExpression expr = optimizer.optimize(connectContext, logicalPlan.getRoot(), new PhysicalPropertySet(),
                new ColumnRefSet(logicalPlan.getOutputColumn()), columnRefFactory);
        Assert.assertNotNull(expr);
        Assert.assertEquals(2, optimizer.getContext().getCandidateMvs().size());

        starRocksAssert.dropMaterializedView("mv_1");
        starRocksAssert.dropMaterializedView("mv_2");
    }

    @Test
    public void testPreprocessMvPartitionMv() throws Exception {
        starRocksAssert.withTable("CREATE TABLE test.tbl_with_mv\n" +
                "(\n" +
                "    k1 date,\n" +
                "    k2 int,\n" +
                "    v1 int sum\n" +
                ")\n" +
                "PARTITION BY RANGE(k1)\n" +
                "(\n" +
                "    PARTITION p1 values [('2022-02-01'),('2022-02-16')),\n" +
                "    PARTITION p2 values [('2022-02-16'),('2022-03-01')),\n" +
                "    PARTITION p3 values [('2022-03-01'),('2022-03-10'))\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(k2) BUCKETS 3\n" +
                "PROPERTIES('replication_num' = '1');");
        executeInsertSql(connectContext, "insert into tbl_with_mv values(\"2020-02-20\", 20, 30)");

        {
            starRocksAssert.withMaterializedView("create materialized view mv_4\n" +
                    "PARTITION BY k1\n" +
                    "distributed by hash(k2) buckets 3\n" +
                    "refresh manual\n" +
                    "as select k1, k2, v1  from tbl_with_mv;");
            refreshMaterializedView("test", "mv_4");
            executeInsertSql(connectContext, "insert into tbl_with_mv partition(p3) values(\"2020-03-05\", 20, 30)");

            String sql = "select k1, sum(v1) from tbl_with_mv group by k1";
            StatementBase stmt = UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
            QueryStatement query = (QueryStatement) stmt;

            // 1. Build Logical plan
            ColumnRefFactory columnRefFactory = new ColumnRefFactory();
            LogicalPlan logicalPlan = new RelationTransformer(columnRefFactory, connectContext)
                    .transformWithSelectLimit(query.getQueryRelation());
            Optimizer optimizer = new Optimizer();
            OptExpression expr = optimizer.optimize(connectContext, logicalPlan.getRoot(), new PhysicalPropertySet(),
                    new ColumnRefSet(logicalPlan.getOutputColumn()), columnRefFactory);
            Assert.assertNotNull(expr);
            Assert.assertEquals(1, optimizer.getContext().getCandidateMvs().size());
            MaterializationContext materializationContext = optimizer.getContext().getCandidateMvs().iterator().next();
            Assert.assertEquals("mv_4", materializationContext.getMv().getName());

            MaterializedView mv = getMv("test", "mv_4");
            Pair<Table, Column> partitionTableAndColumn = getRefBaseTablePartitionColumn(mv);
            Assert.assertEquals("tbl_with_mv", partitionTableAndColumn.first.getName());

            ScalarOperator scalarOperator = materializationContext.getMvPartialPartitionPredicate();
            if (scalarOperator != null) {
                Assert.assertTrue(scalarOperator instanceof CompoundPredicateOperator);
                Assert.assertTrue(((CompoundPredicateOperator) scalarOperator).isAnd());
            }

            refreshMaterializedView("test", "mv_4");
            executeInsertSql(connectContext, "insert into tbl_with_mv partition(p2) values(\"2020-02-20\", 20, 30)");
            Optimizer optimizer2 = new Optimizer();
            OptExpression expr2 = optimizer2.optimize(connectContext, logicalPlan.getRoot(), new PhysicalPropertySet(),
                    new ColumnRefSet(logicalPlan.getOutputColumn()), columnRefFactory);
            Assert.assertNotNull(expr2);
            MaterializationContext materializationContext2 =
                    optimizer2.getContext().getCandidateMvs().iterator().next();
            Assert.assertEquals("mv_4", materializationContext2.getMv().getName());
            ScalarOperator scalarOperator2 = materializationContext2.getMvPartialPartitionPredicate();
            if (scalarOperator2 != null) {
                Assert.assertTrue(scalarOperator2 instanceof CompoundPredicateOperator);
            }
            starRocksAssert.dropMaterializedView("mv_4");
        }

        {
            starRocksAssert.withMaterializedView("create materialized view mv_5\n" +
                    "PARTITION BY date_trunc(\"month\", k1)\n" +
                    "distributed by hash(k2) buckets 3\n" +
                    "refresh manual\n" +
                    "as select k1, k2, v1  from tbl_with_mv;");
            refreshMaterializedView("test", "mv_5");
            executeInsertSql(connectContext, "insert into tbl_with_mv partition(p3) values(\"2020-03-05\", 20, 30)");

            String sql = "select k1, sum(v1) from tbl_with_mv group by k1";
            StatementBase stmt = UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
            QueryStatement query = (QueryStatement) stmt;

            Optimizer optimizer3 = new Optimizer();
            ColumnRefFactory columnRefFactory = new ColumnRefFactory();
            LogicalPlan logicalPlan = new RelationTransformer(columnRefFactory, connectContext)
                    .transformWithSelectLimit(query.getQueryRelation());
            OptExpression expr3 = optimizer3.optimize(connectContext, logicalPlan.getRoot(), new PhysicalPropertySet(),
                    new ColumnRefSet(logicalPlan.getOutputColumn()), columnRefFactory);
            Assert.assertNotNull(expr3);
            MaterializationContext materializationContext3 =
                    optimizer3.getContext().getCandidateMvs().iterator().next();
            Assert.assertEquals("mv_5", materializationContext3.getMv().getName());
            List<LogicalScanOperator> scanExpr3 = MvUtils.getScanOperator(materializationContext3.getMvExpression());
            Assert.assertEquals(1, scanExpr3.size());
            ScalarOperator scalarOperator3 = materializationContext3.getMvPartialPartitionPredicate();
            if (scalarOperator3 != null) {
                Assert.assertNotNull(scalarOperator3);
                Assert.assertTrue(scalarOperator3 instanceof CompoundPredicateOperator);
                Assert.assertTrue(((CompoundPredicateOperator) scalarOperator3).isAnd());
                Assert.assertTrue(scalarOperator3.getChild(0) instanceof BinaryPredicateOperator);
                Assert.assertTrue(scalarOperator3.getChild(0).getChild(0) instanceof ColumnRefOperator);
                ColumnRefOperator columnRef = (ColumnRefOperator) scalarOperator3.getChild(0).getChild(0);
                Assert.assertEquals("k1", columnRef.getName());
            }
            LogicalOlapScanOperator scanOperator = materializationContext3.getScanMvOperator();
            Assert.assertEquals(1, scanOperator.getSelectedPartitionId().size());
        }
    }

    private Pair<MvRewritePreprocessor, OptExpression> buildMvProcessor(String query) {
        ColumnRefFactory columnRefFactory = new ColumnRefFactory();
        OptimizerConfig optimizerConfig = new OptimizerConfig();
        OptimizerContext context = new OptimizerContext(new Memo(), columnRefFactory, connectContext, optimizerConfig);

        try {
            StatementBase stmt = UtFrameUtils.parseStmtWithNewParser(query, connectContext);
            QueryStatement queryStatement = (QueryStatement) stmt;
            LogicalPlan logicalPlan = new RelationTransformer(columnRefFactory, connectContext)
                    .transformWithSelectLimit(queryStatement.getQueryRelation());
            ColumnRefSet requiredColumns = new ColumnRefSet(logicalPlan.getOutputColumn());
            OptExpression logicalTree = logicalPlan.getRoot();
            MvRewritePreprocessor preprocessor = new MvRewritePreprocessor(connectContext, columnRefFactory,
                    context, requiredColumns);
            return Pair.create(preprocessor, logicalTree);
        } catch (Exception e) {
            return null;
        }
    }

    // test without max related mvs limit
    @Test
    public void testChooseBestRelatedMVs1() {
        List<String> mvs = ImmutableList.of(
                // normal mv
                "create materialized view mv_1 distributed by random as select k1, v1, v2 from t1;",
                // with intersected tables
                "create materialized view mv_2 distributed by random as select t1.k1, t1.v1, t1.v2, t0.v1 as v21 " +
                        "from t1 join t0 on t1.k1=t0.v1;",
                // invalid mv
                "create materialized view mv_3 distributed by random as select k1, v1, v2 from t1 " +
                        "union all select k1, v1, v2 from t1;",
                // with intersected tables
                "create materialized view mv_4 distributed by random as select t1.k1, t1.v1, t1.v2, t0.v1 as v21 " +
                        "from t1 join t0 join t2 on t1.k1=t0.v1 and t1.k1=t2.v1;"
        );

        starRocksAssert.withMaterializedViews(mvs, (obj) -> {
            long currentTime = System.currentTimeMillis();
            for (int i = 0; i < 4; i++) {
                String mvName = String.format("mv_%s", i + 1);
                MaterializedView mv = getMv(DB_NAME, mvName);
                mv.getRefreshScheme().setLastRefreshTime(currentTime + i * 1000);
            }

            // query 1
            {
                String query = "select k1, v1, v2 from t1";
                Pair<MvRewritePreprocessor, OptExpression> result = buildMvProcessor(query);
                MvRewritePreprocessor preprocessor = result.first;
                OptExpression logicalTree = result.second;

                Set<Table> queryTables = MvUtils.getAllTables(logicalTree).stream().collect(Collectors.toSet());
                Set<MaterializedView> relatedMVs = preprocessor.getRelatedMVs(queryTables, false);
                Assert.assertEquals(4, relatedMVs.size());
                // mv2 contains extra mvs.
                Assert.assertTrue(containsMV(relatedMVs, "mv_1", "mv_2", "mv_3", "mv_4"));

                // disable plan cache will make the test more stable
                connectContext.getSessionVariable().setEnableMaterializedViewPlanCache(false);
                Set<MaterializedView> validMVs = preprocessor.chooseBestRelatedMVs(queryTables, relatedMVs, logicalTree);
                Assert.assertTrue(validMVs.size() >= 1);
                if (validMVs.size() == 2) {
                    Assert.assertTrue(containsMV(validMVs, "mv_1", "mv_3"));
                }
                connectContext.getSessionVariable().setEnableMaterializedViewPlanCache(true);

                // if mv_3 is in the plan cache
                MaterializedView mv3 = getMv("test", "mv_3");
                CachingMvPlanContextBuilder.getInstance().getPlanContext(connectContext.getSessionVariable(), mv3);
                validMVs = preprocessor.chooseBestRelatedMVs(queryTables, relatedMVs, logicalTree);
                Assert.assertEquals(1, validMVs.size());
                Assert.assertTrue(containsMV(validMVs, "mv_1"));
            }

            // query 2
            {
                String query = "select t1.k1, t1.v1, t1.v2, t0.v1 as v21 from t1 join t0 on t1.k1=t0.v1";
                Pair<MvRewritePreprocessor, OptExpression> result = buildMvProcessor(query);
                MvRewritePreprocessor preprocessor = result.first;
                OptExpression logicalTree = result.second;

                Set<Table> queryTables = MvUtils.getAllTables(logicalTree).stream().collect(Collectors.toSet());
                Set<MaterializedView> relatedMVs = preprocessor.getRelatedMVs(queryTables, false);
                Assert.assertEquals(4, relatedMVs.size());
                // mv2 contains extra mvs.
                Assert.assertTrue(containsMV(relatedMVs, "mv_1", "mv_2", "mv_3", "mv_4"));

                Set<MaterializedView> validMVs = preprocessor.chooseBestRelatedMVs(queryTables, relatedMVs, logicalTree);
                Assert.assertTrue(validMVs.size() == 2);
                Assert.assertTrue(containsMV(validMVs, "mv_1", "mv_2"));
            }
        });
    }

    @Test
    public void testChooseBestRelatedMVs2() {
        List<String> mvs = ImmutableList.of(
                // normal mv
                "create materialized view mv_1 distributed by random as select k1, v1, v2 from t1;",
                // with intersected tables
                "create materialized view mv_2 distributed by random as select t1.k1, t1.v1, t1.v2, t0.v1 as v21 " +
                        "from t1 join t0 on t1.k1=t0.v1;",
                // invalid mv
                "create materialized view mv_3 distributed by random as select k1, v1, v2 from t1 " +
                        "union all select k1, v1, v2 from t1;",
                // with intersected tables
                "create materialized view mv_4 distributed by random as select t1.k1, t1.v1, t1.v2, t0.v1 as v21 " +
                        "from t1 join t0 join t2 on t1.k1=t0.v1 and t1.k1=t2.v1;"
        );

        int oldVal = connectContext.getSessionVariable().getCboMaterializedViewRewriteRelatedMVsLimit();
        connectContext.getSessionVariable().setCboMaterializedViewRewriteRelatedMVsLimit(2);

        starRocksAssert.withMaterializedViews(mvs, (obj) -> {

            long currentTime = System.currentTimeMillis();
            for (int i = 0; i < 4; i++) {
                String mvName = String.format("mv_%s", i + 1);
                MaterializedView mv = getMv(DB_NAME, mvName);
                mv.getRefreshScheme().setLastRefreshTime(currentTime + i * 1000);
            }

            // query 1
            {
                String query = "select k1, v1, v2 from t1";
                Pair<MvRewritePreprocessor, OptExpression> result = buildMvProcessor(query);
                MvRewritePreprocessor preprocessor = result.first;
                OptExpression logicalTree = result.second;

                Set<Table> queryTables = MvUtils.getAllTables(logicalTree).stream().collect(Collectors.toSet());
                Set<MaterializedView> relatedMVs = preprocessor.getRelatedMVs(queryTables, false);
                Assert.assertEquals(4, relatedMVs.size());
                // mv2 contains extra mvs.
                Assert.assertTrue(containsMV(relatedMVs, "mv_1", "mv_2", "mv_3", "mv_4"));

                // disable plan cache will make the test more stable
                connectContext.getSessionVariable().setEnableMaterializedViewPlanCache(false);
                Set<MaterializedView> validMVs = preprocessor.chooseBestRelatedMVs(queryTables, relatedMVs, logicalTree);
                Assert.assertTrue(validMVs.size() >= 1);
                if (validMVs.size() == 2) {
                    Assert.assertTrue(containsMV(validMVs, "mv_1", "mv_3"));
                }
                connectContext.getSessionVariable().setCboMaterializedViewRewriteRelatedMVsLimit(1);
                validMVs = preprocessor.chooseBestRelatedMVs(queryTables, relatedMVs, logicalTree);
                Assert.assertEquals(1, validMVs.size());
                connectContext.getSessionVariable().setEnableMaterializedViewPlanCache(true);

                // if mv_3 is in the plan cache
                connectContext.getSessionVariable().setCboMaterializedViewRewriteRelatedMVsLimit(2);
                MaterializedView mv3 = getMv("test", "mv_3");
                CachingMvPlanContextBuilder.getInstance().getPlanContext(connectContext.getSessionVariable(), mv3);
                validMVs = preprocessor.chooseBestRelatedMVs(queryTables, relatedMVs, logicalTree);
                Assert.assertEquals(1, validMVs.size());
                Assert.assertTrue(containsMV(validMVs, "mv_1"));
            }

            // query 2
            {
                String query = "select t1.k1, t1.v1, t1.v2, t0.v1 as v21 from t1 join t0 on t1.k1=t0.v1";
                Pair<MvRewritePreprocessor, OptExpression> result = buildMvProcessor(query);
                MvRewritePreprocessor preprocessor = result.first;
                OptExpression logicalTree = result.second;

                Set<Table> queryTables = MvUtils.getAllTables(logicalTree).stream().collect(Collectors.toSet());
                Set<MaterializedView> relatedMVs = preprocessor.getRelatedMVs(queryTables, false);
                Assert.assertEquals(4, relatedMVs.size());
                // mv2 contains extra mvs.
                Assert.assertTrue(containsMV(relatedMVs, "mv_1", "mv_2", "mv_3", "mv_4"));

                connectContext.getSessionVariable().setCboMaterializedViewRewriteRelatedMVsLimit(2);
                Set<MaterializedView> validMVs = preprocessor.chooseBestRelatedMVs(queryTables, relatedMVs, logicalTree);
                Assert.assertEquals(2, validMVs.size());
                Assert.assertTrue(containsMV(validMVs, "mv_1", "mv_2"));

                // set it to 1
                connectContext.getSessionVariable().setCboMaterializedViewRewriteRelatedMVsLimit(1);
                validMVs = preprocessor.chooseBestRelatedMVs(queryTables, relatedMVs, logicalTree);
                Assert.assertEquals(1, validMVs.size());
                Assert.assertTrue(containsMV(validMVs, "mv_2"));
            }
        });
        connectContext.getSessionVariable().setCboMaterializedViewRewriteRelatedMVsLimit(oldVal);
    }

    @Test
    public void testChooseBestRelatedMVs3() {
        List<String> mvs = ImmutableList.of(
                "create materialized view mv_1 distributed by random as select k1, v1, v2 from t1 where k1 > 1;",
                "create materialized view mv_2 distributed by random as select k1, v1, v2 from t1 where k1 > 2;",
                "create materialized view mv_3 distributed by random as select k1, v1, v2 from t1 where k1 > 3;",
                "create materialized view mv_4 distributed by random as select k1, v1, v2 from t1 where k1 > 4;",
                "create materialized view mv_5 distributed by random as select k1, v1, v2 from t1 where k1 > 5;"
        );

        starRocksAssert.withMaterializedViews(mvs, (obj) -> {
            long currentTime = System.currentTimeMillis();
            {
                Queue<MvRewritePreprocessor.MVCorrelation> bestRelatedMVs = new PriorityQueue<>();
                for (int i = 0; i < 5; i++) {
                    String mvName = String.format("mv_%s", i + 1);
                    MaterializedView mv = getMv(DB_NAME, mvName);
                    mv.getRefreshScheme().setLastRefreshTime(currentTime + i * 1000);
                    MvRewritePreprocessor.MVCorrelation mvCorrelation = new MvRewritePreprocessor.MVCorrelation(mv,
                            i, i, currentTime + i * 10);
                    bestRelatedMVs.add(mvCorrelation);
                }

                for (int i = 0; i < 5; i++) {
                    MvRewritePreprocessor.MVCorrelation mvCorrelation = bestRelatedMVs.poll();
                    String mvName = String.format("mv_%s", i + 1);
                    Assert.assertEquals(mvCorrelation.getMv().getName(), mvName);
                }
            }

            {
                Queue<MvRewritePreprocessor.MVCorrelation> bestRelatedMVs = new PriorityQueue<>();
                for (int i = 0; i < 5; i++) {
                    String mvName = String.format("mv_%s", i + 1);
                    MaterializedView mv = getMv(DB_NAME, mvName);
                    mv.getRefreshScheme().setLastRefreshTime(currentTime + i * 1000);
                    MvRewritePreprocessor.MVCorrelation mvCorrelation = new MvRewritePreprocessor.MVCorrelation(mv,
                            1, i, currentTime + i * 10);
                    bestRelatedMVs.add(mvCorrelation);
                }

                for (int i = 0; i < 5; i++) {
                    MvRewritePreprocessor.MVCorrelation mvCorrelation = bestRelatedMVs.poll();
                    String mvName = String.format("mv_%s", 5 - i);
                    Assert.assertEquals(mvCorrelation.getMv().getName(), mvName);
                }
            }

            {
                Queue<MvRewritePreprocessor.MVCorrelation> bestRelatedMVs = new PriorityQueue<>();
                for (int i = 0; i < 5; i++) {
                    String mvName = String.format("mv_%s", i + 1);
                    MaterializedView mv = getMv(DB_NAME, mvName);
                    mv.getRefreshScheme().setLastRefreshTime(currentTime + i * 1000);
                    MvRewritePreprocessor.MVCorrelation mvCorrelation = new MvRewritePreprocessor.MVCorrelation(mv,
                            1, 0, currentTime + i * 10);
                    bestRelatedMVs.add(mvCorrelation);
                }

                for (int i = 0; i < 5; i++) {
                    MvRewritePreprocessor.MVCorrelation mvCorrelation = bestRelatedMVs.poll();
                    String mvName = String.format("mv_%s", i + 1);
                    Assert.assertEquals(mvCorrelation.getMv().getName(), mvName);
                }
            }
        });
    }

    @Test
    public void testChooseBestRelatedMVs4() {
        List<String> mvs = ImmutableList.of(
                "create materialized view mv_1 distributed by random as select k1, v1, v2 from t1 where k1 > 1;",
                "create materialized view mv_2 distributed by random as select k1, v1, v2 from t1 where k1 > 2;",
                "create materialized view mv_3 distributed by random as select k1, v1, v2 from t1 where k1 > 3;",
                "create materialized view mv_4 distributed by random as select k1, v1, v2 from t1 where k1 > 4;",
                "create materialized view mv_5 distributed by random as select k1, v1, v2 from t1 where k1 > 5;"
        );

        int oldVal = connectContext.getSessionVariable().getCboMaterializedViewRewriteRelatedMVsLimit();
        starRocksAssert.withMaterializedViews(mvs, (obj) -> {
            long currentTime = System.currentTimeMillis();
            for (int i = 0; i < 5; i++) {
                String mvName = String.format("mv_%s", i + 1);
                MaterializedView mv = getMv(DB_NAME, mvName);
                mv.getRefreshScheme().setLastRefreshTime(currentTime + i * 1000);
            }

            // query 1
            {
                String query = "select k1, v1, v2 from t1 where k1 > 3";
                Pair<MvRewritePreprocessor, OptExpression> result = buildMvProcessor(query);
                MvRewritePreprocessor preprocessor = result.first;
                OptExpression logicalTree = result.second;

                Set<Table> queryTables = MvUtils.getAllTables(logicalTree).stream().collect(Collectors.toSet());
                Set<MaterializedView> relatedMVs = preprocessor.getRelatedMVs(queryTables, false);
                Assert.assertEquals(5, relatedMVs.size());
                // mv2 contains extra mvs.
                Assert.assertTrue(containsMV(relatedMVs, "mv_1", "mv_2", "mv_3", "mv_4", "mv_5"));

                connectContext.getSessionVariable().setCboMaterializedViewRewriteRelatedMVsLimit(2);
                Set<MaterializedView> validMVs = preprocessor.chooseBestRelatedMVs(queryTables, relatedMVs, logicalTree);
                Assert.assertEquals(2, validMVs.size());
                Assert.assertTrue(containsMV(validMVs, "mv_4", "mv_5"));

                connectContext.getSessionVariable().setCboMaterializedViewRewriteRelatedMVsLimit(1);
                validMVs = preprocessor.chooseBestRelatedMVs(queryTables, relatedMVs, logicalTree);
                Assert.assertEquals(1, validMVs.size());
                Assert.assertTrue(containsMV(validMVs, "mv_5"));
            }
        });
        connectContext.getSessionVariable().setCboMaterializedViewRewriteRelatedMVsLimit(oldVal);
    }

    // with view
    @Test
    public void testChooseBestRelatedMVs5() {
        int mvNum = 100;

        List<String> mvs = Lists.newArrayList();
        for (int i = 0; i < mvNum; i++) {
            String view = String.format("create view test_view_%s as select k1, v1, v2 from t1 where k1 > %s", i, i);
            try {
                starRocksAssert.withView(view);
            } catch (Exception e) {
                Assert.fail();
            }
            mvs.add(String.format("create materialized view mv_%s distributed by random as select * from test_view_%s", i, i));
        }

        int oldVal = connectContext.getSessionVariable().getCboMaterializedViewRewriteRelatedMVsLimit();
        starRocksAssert.withMaterializedViews(mvs, (obj) -> {
            long currentTime = System.currentTimeMillis();
            for (int i = 0; i < mvNum; i++) {
                String mvName = String.format("mv_%s", i);
                MaterializedView mv = getMv(DB_NAME, mvName);
                mv.getRefreshScheme().setLastRefreshTime(currentTime + i * 1000);
            }

            // query 1
            {
                String query = "select k1, v1, v2 from t1 where k1 > 3";
                Pair<MvRewritePreprocessor, OptExpression> result = buildMvProcessor(query);
                MvRewritePreprocessor preprocessor = result.first;
                OptExpression logicalTree = result.second;

                Set<Table> queryTables = MvUtils.getAllTables(logicalTree).stream().collect(Collectors.toSet());
                Set<MaterializedView> relatedMVs = preprocessor.getRelatedMVs(queryTables, false);
                Assert.assertEquals(relatedMVs.size(), mvNum);
                Set<MaterializedView> validMVs = preprocessor.chooseBestRelatedMVs(queryTables, relatedMVs, logicalTree);
                Assert.assertEquals(validMVs.size(), oldVal);
                Set<String> expectMvs = Sets.newHashSet();
                for (int i = 0; i < oldVal; i++) {
                    String mvName = String.format("mv_%s", mvNum - i - 1);
                    expectMvs.add(mvName);
                }
                for (MaterializedView mv : validMVs) {
                    Assert.assertTrue(expectMvs.contains(mv.getName()));
                }
                Set<MvRewritePreprocessor.MvWithPlanContext> mvWithPlanContexts =
                        preprocessor.getMvWithPlanContext(validMVs);
                Assert.assertEquals(mvWithPlanContexts.size(), oldVal * 2);

                connectContext.getSessionVariable().setCboMaterializedViewRewriteRelatedMVsLimit(2);
                validMVs = preprocessor.chooseBestRelatedMVs(queryTables, relatedMVs, logicalTree);
                Assert.assertEquals(2, validMVs.size());
                Assert.assertTrue(containsMV(validMVs, "mv_98", "mv_99"));
                mvWithPlanContexts = preprocessor.getMvWithPlanContext(validMVs);
                Assert.assertEquals(4, mvWithPlanContexts.size());

                connectContext.getSessionVariable().setCboMaterializedViewRewriteRelatedMVsLimit(1);
                validMVs = preprocessor.chooseBestRelatedMVs(queryTables, relatedMVs, logicalTree);
                Assert.assertEquals(1, validMVs.size());
                Assert.assertTrue(containsMV(validMVs, "mv_99"));

                mvWithPlanContexts = preprocessor.getMvWithPlanContext(validMVs);
                Assert.assertEquals(2, mvWithPlanContexts.size());
            }
        });
        connectContext.getSessionVariable().setCboMaterializedViewRewriteRelatedMVsLimit(oldVal);
    }

    private boolean containsMV(Set<MaterializedView> mvs, String... expects) {
        Set<String> mvNames = mvs.stream().map(mv -> mv.getName()).collect(Collectors.toSet());
        if (mvNames.size() != Arrays.stream(expects).count()) {
            return false;
        }
        for (String exp : expects) {
            if (!mvNames.contains(exp)) {
                return false;
            }
        }
        return true;
    }

    @Test
    public void testValidPlan() {
        String sql = "create materialized view invalid_plan_mv distributed by random " +
                "as select count(distinct cnt) from (select count(v1) as cnt from t1 group by k1) t";
        starRocksAssert.withMaterializedView(sql, (obj) -> {
            String mvName = (String) obj;
            MaterializedView mv = getMv(DB_NAME, mvName);
            MvPlanContext mvPlanContext = new MaterializedViewOptimizer().optimize(mv, connectContext, true);
            Assert.assertTrue(!mvPlanContext.isValidMvPlan());
        });
    }

    @Test
    public void testValidPlanWithView() {
        String view = "create view view0 " +
                "as select count(distinct cnt) from (select count(v1) as cnt from t1 group by k1) t";
        try {
            starRocksAssert.withView(view);
        } catch (Exception e) {
            Assert.fail();
        }
        String sql = "create materialized view invalid_plan_mv distributed by random as select * from view0";
        starRocksAssert.withMaterializedView(sql, (obj) -> {
            String mvName = (String) obj;
            MaterializedView mv = getMv(DB_NAME, mvName);
            MvPlanContext mvPlanContext = new MaterializedViewOptimizer().optimize(mv, connectContext, true);
            Assert.assertTrue(!mvPlanContext.isValidMvPlan());
            mvPlanContext = new MaterializedViewOptimizer().optimize(mv, connectContext, false);
            Assert.assertTrue(mvPlanContext.isValidMvPlan());


            String query = "select * from view0";
            Pair<MvRewritePreprocessor, OptExpression> result = buildMvProcessor(query);
            MvRewritePreprocessor preprocessor = result.first;
            OptExpression logicalTree = result.second;

            Set<Table> queryTables = MvUtils.getAllTables(logicalTree).stream().collect(Collectors.toSet());
            Set<MaterializedView> relatedMVs = preprocessor.getRelatedMVs(queryTables, false);
            Assert.assertEquals(1, relatedMVs.size());
            Assert.assertTrue(containsMV(relatedMVs, "invalid_plan_mv"));

            Set<MaterializedView> validMVs = preprocessor.chooseBestRelatedMVs(queryTables, relatedMVs, logicalTree);
            Assert.assertEquals(1, validMVs.size());
            Assert.assertTrue(containsMV(validMVs, "invalid_plan_mv"));
            Set<MvRewritePreprocessor.MvWithPlanContext> mvWithPlanContexts =
                    preprocessor.getMvWithPlanContext(validMVs);
            Assert.assertEquals(1, mvWithPlanContexts.size());
        });
    }
}
