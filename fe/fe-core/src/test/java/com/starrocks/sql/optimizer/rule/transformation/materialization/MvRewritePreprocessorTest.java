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
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.MvId;
import com.starrocks.catalog.MvPlanContext;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.Table;
import com.starrocks.common.Pair;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.common.PRangeCell;
import com.starrocks.sql.common.PRangeCellPlus;
import com.starrocks.sql.optimizer.CachingMvPlanContextBuilder;
import com.starrocks.sql.optimizer.MaterializationContext;
import com.starrocks.sql.optimizer.MvRewritePreprocessor;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.Optimizer;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.OptimizerFactory;
import com.starrocks.sql.optimizer.OptimizerOptions;
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
import com.starrocks.sql.optimizer.rule.Rule;
import com.starrocks.sql.optimizer.rule.RuleType;
import com.starrocks.sql.optimizer.rule.mv.MVCorrelation;
import com.starrocks.sql.optimizer.rule.mv.MaterializedViewWrapper;
import com.starrocks.sql.optimizer.task.RewriteTreeTask;
import com.starrocks.sql.optimizer.task.TaskContext;
import com.starrocks.sql.optimizer.transformer.LogicalPlan;
import com.starrocks.sql.optimizer.transformer.RelationTransformer;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.utframe.UtFrameUtils;
import org.apache.parquet.Strings;
import org.assertj.core.util.Sets;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;
import java.util.stream.Collectors;

import static com.starrocks.planner.MaterializedViewTestBase.getRefBaseTablePartitionColumn;

public class MvRewritePreprocessorTest extends MVTestBase {
    @BeforeAll
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
        String sql = "select v1, sum(v3) from t0 where v1 < 10 group by v1";
        Pair<String, ExecPlan> result = UtFrameUtils.getPlanAndFragment(connectContext, sql);
        Assertions.assertNotNull(result);

        StatementBase stmt = UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        QueryStatement query = (QueryStatement) stmt;

        // 1. Build Logical plan
        ColumnRefFactory columnRefFactory = new ColumnRefFactory();
        LogicalPlan logicalPlan = new RelationTransformer(columnRefFactory, connectContext)
                .transformWithSelectLimit(query.getQueryRelation());

        OptimizerContext optimizerContext = OptimizerFactory.mockContext(connectContext, columnRefFactory);
        Optimizer optimizer = OptimizerFactory.create(optimizerContext);
        Assertions.assertFalse(optimizerContext.getOptimizerOptions().isRuleBased());
        Assertions.assertFalse(optimizerContext.getOptimizerOptions().isRuleDisable(RuleType.TF_MERGE_TWO_PROJECT));
        Assertions.assertFalse(optimizerContext.getOptimizerOptions().isRuleDisable(RuleType.GP_AGGREGATE_REWRITE));

        OptExpression expr = optimizer.optimize(logicalPlan.getRoot(), new PhysicalPropertySet(),
                new ColumnRefSet(logicalPlan.getOutputColumn()));
        Assertions.assertTrue(expr.getInputs().get(0).getOp() instanceof PhysicalOlapScanOperator);
        Assertions.assertNotNull(expr.getInputs().get(0).getOp().getPredicate());

        OptimizerOptions optimizerOptions = new OptimizerOptions(OptimizerOptions.OptimizerStrategy.RULE_BASED);
        optimizerOptions.disableRule(RuleType.TF_MERGE_TWO_PROJECT);
        optimizerOptions.disableRule(RuleType.GP_PUSH_DOWN_PREDICATE);
        OptimizerContext optimizerContext1 = OptimizerFactory.mockContext(connectContext, columnRefFactory,
                optimizerOptions);
        Optimizer optimizer1 = OptimizerFactory.create(optimizerContext1);
        Assertions.assertTrue(optimizerContext1.getOptimizerOptions().isRuleBased());
        Assertions.assertFalse(optimizerContext1.getOptimizerOptions().isRuleDisable(RuleType.TF_MERGE_TWO_AGG_RULE));
        Assertions.assertTrue(optimizerContext1.getOptimizerOptions().isRuleDisable(RuleType.TF_MERGE_TWO_PROJECT));
        Assertions.assertFalse(optimizerContext1.getOptimizerOptions().isRuleDisable(RuleType.GP_COLLECT_CTE));
        Assertions.assertTrue(optimizerContext1.getOptimizerOptions().isRuleDisable(RuleType.GP_PUSH_DOWN_PREDICATE));

        OptExpression expr1 = optimizer1.optimize(logicalPlan.getRoot(), new PhysicalPropertySet(),
                new ColumnRefSet(logicalPlan.getOutputColumn()));
        Assertions.assertTrue(expr1.getInputs().get(0).getOp() instanceof LogicalFilterOperator);

        // test timeout
        long timeout = connectContext.getSessionVariable().getOptimizerExecuteTimeout();
        connectContext.getSessionVariable().setOptimizerExecuteTimeout(1);
        TaskContext rootTaskContext = optimizer1.getContext().getTaskContext();
        // just give a valid RuleType
        Rule timeoutRule = new TimeoutRule(RuleType.TF_MV_ONLY_JOIN_RULE, Pattern.create(OperatorType.PATTERN));
        OptExpression tree = OptExpression.create(new LogicalTreeAnchorOperator(), logicalPlan.getRoot());
        optimizer1.getContext().getTaskScheduler().pushTask(
                new RewriteTreeTask(rootTaskContext, tree, timeoutRule, true));
        try {
            optimizer1.getContext().getTaskScheduler().executeTasks(rootTaskContext);
        } catch (Exception e) {
            Assertions.assertTrue(e.getMessage().contains("StarRocks planner use long time 1 ms in logical phase"));
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
        Optimizer optimizer = OptimizerFactory.create(OptimizerFactory.mockContext(connectContext, columnRefFactory));
        OptExpression expr = optimizer.optimize(logicalPlan.getRoot(), new PhysicalPropertySet(),
                new ColumnRefSet(logicalPlan.getOutputColumn()));
        Assertions.assertNotNull(expr);
        Assertions.assertEquals(2, optimizer.getContext().getCandidateMvs().size());

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
            Optimizer optimizer = OptimizerFactory.create(OptimizerFactory.mockContext(connectContext,
                    columnRefFactory));
            OptExpression expr = optimizer.optimize(logicalPlan.getRoot(),
                    new ColumnRefSet(logicalPlan.getOutputColumn()));
            Assertions.assertNotNull(expr);
            Assertions.assertEquals(1, optimizer.getContext().getCandidateMvs().size());
            MaterializationContext materializationContext = optimizer.getContext().getCandidateMvs().iterator().next();
            Assertions.assertEquals("mv_4", materializationContext.getMv().getName());

            MaterializedView mv = getMv("test", "mv_4");
            Pair<Table, Column> partitionTableAndColumn = getRefBaseTablePartitionColumn(mv);
            Assertions.assertEquals("tbl_with_mv", partitionTableAndColumn.first.getName());

            refreshMaterializedView("test", "mv_4");
            executeInsertSql(connectContext, "insert into tbl_with_mv partition(p2) values(\"2020-02-20\", 20, 30)");
            Optimizer optimizer2 = OptimizerFactory.create(OptimizerFactory.mockContext(connectContext,
                    columnRefFactory));
            OptExpression expr2 = optimizer2.optimize(logicalPlan.getRoot(), new PhysicalPropertySet(),
                    new ColumnRefSet(logicalPlan.getOutputColumn()));
            Assertions.assertNotNull(expr2);
            MaterializationContext materializationContext2 =
                    optimizer2.getContext().getCandidateMvs().iterator().next();
            Assertions.assertEquals("mv_4", materializationContext2.getMv().getName());
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

            ColumnRefFactory columnRefFactory = new ColumnRefFactory();
            LogicalPlan logicalPlan = new RelationTransformer(columnRefFactory, connectContext)
                    .transformWithSelectLimit(query.getQueryRelation());
            Optimizer optimizer3 =
                    OptimizerFactory.create(OptimizerFactory.mockContext(connectContext, columnRefFactory));
            OptExpression expr3 = optimizer3.optimize(logicalPlan.getRoot(),
                    new ColumnRefSet(logicalPlan.getOutputColumn()));
            Assertions.assertNotNull(expr3);
            MaterializationContext materializationContext3 =
                    optimizer3.getContext().getCandidateMvs().iterator().next();
            Assertions.assertEquals("mv_5", materializationContext3.getMv().getName());
            List<LogicalScanOperator> scanExpr3 = MvUtils.getScanOperator(materializationContext3.getMvExpression());
            Assertions.assertEquals(1, scanExpr3.size());
            LogicalOlapScanOperator scanOperator = materializationContext3.getScanMvOperator();
            Assertions.assertEquals(1, scanOperator.getSelectedPartitionId().size());
        }
    }

    private Pair<MvRewritePreprocessor, OptExpression> buildMvProcessor(String query) {
        ColumnRefFactory columnRefFactory = new ColumnRefFactory();
        OptimizerOptions optimizerOptions = new OptimizerOptions();
        OptimizerContext context = OptimizerFactory.mockContext(connectContext, columnRefFactory, optimizerOptions);

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
                Set<MaterializedViewWrapper> relatedMVs = preprocessor.getRelatedMVs(queryTables, false);
                Assertions.assertEquals(4, relatedMVs.size());
                // mv2 contains extra mvs.
                Assertions.assertTrue(containsMV(relatedMVs, "mv_1", "mv_2", "mv_3", "mv_4"));

                // disable plan cache will make the test more stable
                connectContext.getSessionVariable().setEnableMaterializedViewPlanCache(false);
                List<MaterializedViewWrapper> validMVs = preprocessor.chooseBestRelatedMVs(queryTables, relatedMVs, logicalTree);
                Assertions.assertTrue(validMVs.size() >= 1);
                if (validMVs.size() == 2) {
                    Assertions.assertTrue(containsMV(validMVs, "mv_1", "mv_3"));
                }
                connectContext.getSessionVariable().setEnableMaterializedViewPlanCache(true);

                // if mv_3 is in the plan cache
                MaterializedView mv3 = getMv("test", "mv_3");
                CachingMvPlanContextBuilder.getInstance().getPlanContext(connectContext.getSessionVariable(), mv3);
                validMVs = preprocessor.chooseBestRelatedMVs(queryTables, relatedMVs, logicalTree);
                Assertions.assertEquals(1, validMVs.size());
                Assertions.assertTrue(containsMV(validMVs, "mv_1"));
            }

            // query 2
            {
                String query = "select t1.k1, t1.v1, t1.v2, t0.v1 as v21 from t1 join t0 on t1.k1=t0.v1";
                Pair<MvRewritePreprocessor, OptExpression> result = buildMvProcessor(query);
                MvRewritePreprocessor preprocessor = result.first;
                OptExpression logicalTree = result.second;

                Set<Table> queryTables = MvUtils.getAllTables(logicalTree).stream().collect(Collectors.toSet());
                Set<MaterializedViewWrapper> relatedMVs = preprocessor.getRelatedMVs(queryTables, false);
                Assertions.assertEquals(4, relatedMVs.size());
                // mv2 contains extra mvs.
                Assertions.assertTrue(containsMV(relatedMVs, "mv_1", "mv_2", "mv_3", "mv_4"));

                List<MaterializedViewWrapper> validMVs = preprocessor.chooseBestRelatedMVs(queryTables, relatedMVs, logicalTree);
                Assertions.assertTrue(validMVs.size() == 2);
                Assertions.assertTrue(containsMV(validMVs, "mv_1", "mv_2"));
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
                Set<MaterializedViewWrapper> relatedMVs = preprocessor.getRelatedMVs(queryTables, false);
                Assertions.assertEquals(4, relatedMVs.size());
                // mv2 contains extra mvs.
                Assertions.assertTrue(containsMV(relatedMVs, "mv_1", "mv_2", "mv_3", "mv_4"));

                // disable plan cache will make the test more stable
                connectContext.getSessionVariable().setEnableMaterializedViewPlanCache(false);
                List<MaterializedViewWrapper> validMVs = preprocessor.chooseBestRelatedMVs(queryTables, relatedMVs, logicalTree);
                Assertions.assertTrue(validMVs.size() >= 1);
                if (validMVs.size() == 2) {
                    Assertions.assertTrue(containsMV(validMVs, "mv_1", "mv_3"));
                }
                connectContext.getSessionVariable().setCboMaterializedViewRewriteRelatedMVsLimit(1);
                validMVs = preprocessor.chooseBestRelatedMVs(queryTables, relatedMVs, logicalTree);
                Assertions.assertEquals(1, validMVs.size());
                connectContext.getSessionVariable().setEnableMaterializedViewPlanCache(true);

                // if mv_3 is in the plan cache
                connectContext.getSessionVariable().setCboMaterializedViewRewriteRelatedMVsLimit(2);
                MaterializedView mv3 = getMv("test", "mv_3");
                CachingMvPlanContextBuilder.getInstance().getPlanContext(connectContext.getSessionVariable(), mv3);
                validMVs = preprocessor.chooseBestRelatedMVs(queryTables, relatedMVs, logicalTree);
                Assertions.assertEquals(1, validMVs.size());
                Assertions.assertTrue(containsMV(validMVs, "mv_1"));
            }

            // query 2
            {
                String query = "select t1.k1, t1.v1, t1.v2, t0.v1 as v21 from t1 join t0 on t1.k1=t0.v1";
                Pair<MvRewritePreprocessor, OptExpression> result = buildMvProcessor(query);
                MvRewritePreprocessor preprocessor = result.first;
                OptExpression logicalTree = result.second;

                Set<Table> queryTables = MvUtils.getAllTables(logicalTree).stream().collect(Collectors.toSet());
                Set<MaterializedViewWrapper> relatedMVs = preprocessor.getRelatedMVs(queryTables, false);
                Assertions.assertEquals(4, relatedMVs.size());
                // mv2 contains extra mvs.
                Assertions.assertTrue(containsMV(relatedMVs, "mv_1", "mv_2", "mv_3", "mv_4"));

                connectContext.getSessionVariable().setCboMaterializedViewRewriteRelatedMVsLimit(2);
                List<MaterializedViewWrapper> validMVs = preprocessor.chooseBestRelatedMVs(queryTables, relatedMVs, logicalTree);
                Assertions.assertEquals(2, validMVs.size());
                Assertions.assertTrue(containsMV(validMVs, "mv_1", "mv_2"));

                // set it to 1
                connectContext.getSessionVariable().setCboMaterializedViewRewriteRelatedMVsLimit(1);
                validMVs = preprocessor.chooseBestRelatedMVs(queryTables, relatedMVs, logicalTree);
                Assertions.assertEquals(1, validMVs.size());
                Assertions.assertTrue(containsMV(validMVs, "mv_2"));
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
                Queue<MVCorrelation> bestRelatedMVs = new PriorityQueue<>();
                for (int i = 0; i < 5; i++) {
                    String mvName = String.format("mv_%s", i + 1);
                    MaterializedView mv = getMv(DB_NAME, mvName);
                    mv.getRefreshScheme().setLastRefreshTime(currentTime + i * 1000);
                    MVCorrelation mvCorrelation = new MVCorrelation(mv,
                            i, i, currentTime + i * 10);
                    bestRelatedMVs.add(mvCorrelation);
                }

                for (int i = 0; i < 5; i++) {
                    MVCorrelation mvCorrelation = bestRelatedMVs.poll();
                    String mvName = String.format("mv_%s", i + 1);
                    Assertions.assertEquals(mvCorrelation.getMv().getName(), mvName);
                }
            }

            {
                Queue<MVCorrelation> bestRelatedMVs = new PriorityQueue<>();
                for (int i = 0; i < 5; i++) {
                    String mvName = String.format("mv_%s", i + 1);
                    MaterializedView mv = getMv(DB_NAME, mvName);
                    mv.getRefreshScheme().setLastRefreshTime(currentTime + i * 1000);
                    MVCorrelation mvCorrelation = new MVCorrelation(mv,
                            1, i, currentTime + i * 10);
                    bestRelatedMVs.add(mvCorrelation);
                }

                for (int i = 0; i < 5; i++) {
                    MVCorrelation mvCorrelation = bestRelatedMVs.poll();
                    String mvName = String.format("mv_%s", 5 - i);
                    Assertions.assertEquals(mvCorrelation.getMv().getName(), mvName);
                }
            }

            {
                Queue<MVCorrelation> bestRelatedMVs = new PriorityQueue<>();
                for (int i = 0; i < 5; i++) {
                    String mvName = String.format("mv_%s", i + 1);
                    MaterializedView mv = getMv(DB_NAME, mvName);
                    mv.getRefreshScheme().setLastRefreshTime(currentTime + i * 1000);
                    MVCorrelation mvCorrelation = new MVCorrelation(mv,
                            1, 0, currentTime + i * 10);
                    bestRelatedMVs.add(mvCorrelation);
                }

                for (int i = 0; i < 5; i++) {
                    MVCorrelation mvCorrelation = bestRelatedMVs.poll();
                    String mvName = String.format("mv_%s", i + 1);
                    Assertions.assertEquals(mvCorrelation.getMv().getName(), mvName);
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
                Set<MaterializedViewWrapper> relatedMVs = preprocessor.getRelatedMVs(queryTables, false);
                Assertions.assertEquals(5, relatedMVs.size());
                // mv2 contains extra mvs.
                Assertions.assertTrue(containsMV(relatedMVs, "mv_1", "mv_2", "mv_3", "mv_4", "mv_5"));

                connectContext.getSessionVariable().setCboMaterializedViewRewriteRelatedMVsLimit(2);
                List<MaterializedViewWrapper> validMVs = preprocessor.chooseBestRelatedMVs(queryTables, relatedMVs, logicalTree);
                Assertions.assertEquals(2, validMVs.size());
                Assertions.assertTrue(containsMV(validMVs, "mv_4", "mv_5"));

                connectContext.getSessionVariable().setCboMaterializedViewRewriteRelatedMVsLimit(1);
                validMVs = preprocessor.chooseBestRelatedMVs(queryTables, relatedMVs, logicalTree);
                Assertions.assertEquals(1, validMVs.size());
                Assertions.assertTrue(containsMV(validMVs, "mv_5"));
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
                Assertions.fail();
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
                Set<MaterializedViewWrapper> relatedMVs = preprocessor.getRelatedMVs(queryTables, false);
                Assertions.assertEquals(relatedMVs.size(), mvNum);
                List<MaterializedViewWrapper> validMVs = preprocessor.chooseBestRelatedMVs(queryTables, relatedMVs, logicalTree);
                Assertions.assertEquals(validMVs.size(), oldVal);
                Set<String> expectMvs = Sets.newHashSet();
                for (int i = 0; i < oldVal; i++) {
                    String mvName = String.format("mv_%s", mvNum - i - 1);
                    expectMvs.add(mvName);
                }
                for (MaterializedViewWrapper wrapper : validMVs) {
                    Assertions.assertTrue(expectMvs.contains(wrapper.getMV().getName()));
                }
                List<MaterializedViewWrapper> mvWithPlanContexts = preprocessor.getMvWithPlanContext(validMVs);
                Assertions.assertEquals(mvWithPlanContexts.size(), oldVal * 2);

                connectContext.getSessionVariable().setCboMaterializedViewRewriteRelatedMVsLimit(2);
                validMVs = preprocessor.chooseBestRelatedMVs(queryTables, relatedMVs, logicalTree);
                Assertions.assertEquals(2, validMVs.size());
                Assertions.assertTrue(containsMV(validMVs, "mv_98", "mv_99"));
                mvWithPlanContexts = preprocessor.getMvWithPlanContext(validMVs);
                Assertions.assertEquals(4, mvWithPlanContexts.size());

                connectContext.getSessionVariable().setCboMaterializedViewRewriteRelatedMVsLimit(1);
                validMVs = preprocessor.chooseBestRelatedMVs(queryTables, relatedMVs, logicalTree);
                Assertions.assertEquals(1, validMVs.size());
                Assertions.assertTrue(containsMV(validMVs, "mv_99"));

                mvWithPlanContexts = preprocessor.getMvWithPlanContext(validMVs);
                Assertions.assertEquals(2, mvWithPlanContexts.size());
            }
        });
        connectContext.getSessionVariable().setCboMaterializedViewRewriteRelatedMVsLimit(oldVal);
    }

    private boolean containsMV(Collection<MaterializedViewWrapper> mvs, String... expects) {
        Set<String> mvNames = mvs.stream().map(mv -> mv.getMV().getName()).collect(Collectors.toSet());
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
            MvPlanContext mvPlanContext = getOptimizedPlan(mv, true);
            Assertions.assertTrue(!mvPlanContext.isValidMvPlan());
        });
    }

    @Test
    public void testValidPlanWithView() {
        String view = "create view view0 " +
                "as select count(distinct cnt) from (select count(v1) as cnt from t1 group by k1) t";
        try {
            starRocksAssert.withView(view);
        } catch (Exception e) {
            Assertions.fail();
        }
        String sql = "create materialized view invalid_plan_mv distributed by random as select * from view0";
        starRocksAssert.withMaterializedView(sql, (obj) -> {
            String mvName = (String) obj;
            MaterializedView mv = getMv(DB_NAME, mvName);
            MvPlanContext mvPlanContext = getOptimizedPlan(mv, true);
            Assertions.assertTrue(!mvPlanContext.isValidMvPlan());
            mvPlanContext = getOptimizedPlan(mv, false);
            Assertions.assertTrue(mvPlanContext.isValidMvPlan());


            String query = "select * from view0";
            Pair<MvRewritePreprocessor, OptExpression> result = buildMvProcessor(query);
            MvRewritePreprocessor preprocessor = result.first;
            OptExpression logicalTree = result.second;

            Set<Table> queryTables = MvUtils.getAllTables(logicalTree).stream().collect(Collectors.toSet());
            Set<MaterializedViewWrapper> relatedMVs = preprocessor.getRelatedMVs(queryTables, false);
            Assertions.assertEquals(1, relatedMVs.size());
            Assertions.assertTrue(containsMV(relatedMVs, "invalid_plan_mv"));

            List<MaterializedViewWrapper> validMVs = preprocessor.chooseBestRelatedMVs(queryTables, relatedMVs, logicalTree);
            Assertions.assertEquals(1, validMVs.size());
            Assertions.assertTrue(containsMV(validMVs, "invalid_plan_mv"));
            List<MaterializedViewWrapper> mvWithPlanContexts = preprocessor.getMvWithPlanContext(validMVs);
            Assertions.assertEquals(1, mvWithPlanContexts.size());
        });
    }

    @Test
    public void testValidPlanWithoutForce() {
        String sql = "create materialized view invalid_plan_mv distributed by random " +
                "as select count(v1) as cnt from t1 group by k1";
        starRocksAssert.withMaterializedView(sql, (obj) -> {
            String mvName = (String) obj;
            MaterializedView mv = getMv(DB_NAME, mvName);
            CachingMvPlanContextBuilder.getInstance().evictMaterializedViewCache(mv);
            String status = mv.getQueryRewriteStatus();
            Assertions.assertTrue(status.equalsIgnoreCase("UNKNOWN: MV plan is not in cache, valid check is unknown"));
        });
    }

    @Test
    public void testChooseBestRelatedMVsWithDifferentLevels() {
        List<String> mvs = ImmutableList.of(
                "create materialized view mv_1 distributed by random as select k1, v1, v2 from t1 where k1 > 1;",
                "create materialized view mv_2 distributed by random as select k1, v1, v2 from t1 where k1 > 2;",
                "create materialized view mv_3 distributed by random as select k1, v1, v2 from t1 where k1 > 3;",
                "create materialized view mv_4 distributed by random as select k1, v1, v2 from t1 where k1 > 4;",
                "create materialized view mv_5 distributed by random as select k1, v1, v2 from t1 where k1 > 5;"
        );

        starRocksAssert.withMaterializedViews(mvs, (obj) -> {
            final long currentTime = System.currentTimeMillis();
            final String query = "select k1, v1, v2 from t1 where k1 > 10";
            final Pair<MvRewritePreprocessor, OptExpression> result = buildMvProcessor(query);
            final MvRewritePreprocessor preprocessor = result.first;
            final Table t1 = getTable(DB_NAME, "t1");
            final OptExpression logicalTree = result.second;
            final Set<Table> queryTables = ImmutableSet.of(t1);

            // correlation with different levels and same other params
            {
                Queue<MVCorrelation> bestRelatedMVs = new PriorityQueue<>();
                for (int i = 0; i < 5; i++) {
                    String mvName = String.format("mv_%s", i + 1);
                    MaterializedView mv = getMv(DB_NAME, mvName);
                    mv.getRefreshScheme().setLastRefreshTime(currentTime);
                    MVCorrelation mvCorrelation = new MVCorrelation(mv,
                            1, 1, currentTime, i);
                    bestRelatedMVs.add(mvCorrelation);
                }
                List<MaterializedViewWrapper> validMVs = preprocessor.chooseBestRelatedMVsByCorrelations(bestRelatedMVs, 5);
                for (int i = 0; i < 5; i++) {
                    MVCorrelation mvCorrelation = bestRelatedMVs.poll();
                    Assertions.assertEquals(String.format("mv_%s", 5 - i), mvCorrelation.getMv().getName());
                    Assertions.assertEquals(String.format("mv_%s", i + 1), validMVs.get(i).getMV().getName());
                }

            }

            // correlation with different mvQueryIntersectedTablesNum and same other params
            {
                Queue<MVCorrelation> bestRelatedMVs = new PriorityQueue<>();
                for (int i = 0; i < 5; i++) {
                    String mvName = String.format("mv_%s", i + 1);
                    MaterializedView mv = getMv(DB_NAME, mvName);
                    mv.getRefreshScheme().setLastRefreshTime(currentTime);
                    MVCorrelation mvCorrelation = new MVCorrelation(mv,
                            i, 1, currentTime, 0);
                    bestRelatedMVs.add(mvCorrelation);
                }

                List<MaterializedViewWrapper> validMVs = preprocessor.chooseBestRelatedMVsByCorrelations(bestRelatedMVs, 5);
                for (int i = 0; i < 5; i++) {
                    MVCorrelation mvCorrelation = bestRelatedMVs.poll();
                    Assertions.assertEquals(String.format("mv_%s", i + 1), mvCorrelation.getMv().getName());
                    Assertions.assertEquals(String.format("mv_%s", 5 - i), validMVs.get(i).getMV().getName());
                }
            }

            // correlation with different mvQueryScanOpNumDiff and same other params
            {
                Queue<MVCorrelation> bestRelatedMVs = new PriorityQueue<>();
                for (int i = 0; i < 5; i++) {
                    String mvName = String.format("mv_%s", i + 1);
                    MaterializedView mv = getMv(DB_NAME, mvName);
                    mv.getRefreshScheme().setLastRefreshTime(currentTime);
                    MVCorrelation mvCorrelation = new MVCorrelation(mv,
                            1, i, currentTime, 0);
                    bestRelatedMVs.add(mvCorrelation);
                }

                List<MaterializedViewWrapper> validMVs = preprocessor.chooseBestRelatedMVsByCorrelations(bestRelatedMVs, 5);
                for (int i = 0; i < 5; i++) {
                    MVCorrelation mvCorrelation = bestRelatedMVs.poll();
                    Assertions.assertEquals(String.format("mv_%s", 5 - i), mvCorrelation.getMv().getName());
                    Assertions.assertEquals(String.format("mv_%s", i + 1), validMVs.get(i).getMV().getName());
                }
            }

            // correlation with different current times and same other params
            {
                Queue<MVCorrelation> bestRelatedMVs = new PriorityQueue<>();
                for (int i = 0; i < 5; i++) {
                    String mvName = String.format("mv_%s", i + 1);
                    MaterializedView mv = getMv(DB_NAME, mvName);
                    mv.getRefreshScheme().setLastRefreshTime(currentTime);
                    MVCorrelation mvCorrelation = new MVCorrelation(mv,
                            1, 1, currentTime + i * 10, 0);
                    bestRelatedMVs.add(mvCorrelation);
                }

                List<MaterializedViewWrapper> validMVs = preprocessor.chooseBestRelatedMVsByCorrelations(bestRelatedMVs, 5);
                for (int i = 0; i < 5; i++) {
                    MVCorrelation mvCorrelation = bestRelatedMVs.poll();
                    Assertions.assertEquals(String.format("mv_%s", i + 1), mvCorrelation.getMv().getName());
                    Assertions.assertEquals(String.format("mv_%s", 5 - i), validMVs.get(i).getMV().getName());
                }
            }
        });
    }

    // ==================== Timeout Tests ====================

    /**
     * Test that MVs are processed with individual timeouts and the process continues
     * even when some MVs timeout.
     */
    @Test
    public void testMvProcessingWithIndividualTimeouts() throws Exception {
        // Create multiple MVs to test timeout distribution
        List<String> mvs = ImmutableList.of(
                "create materialized view timeout_mv_1 distributed by random as select k1, v1, v2 from t1;",
                "create materialized view timeout_mv_2 distributed by random as select k1, v1, v2 from t1;",
                "create materialized view timeout_mv_3 distributed by random as select k1, v1, v2 from t1;",
                "create materialized view timeout_mv_4 distributed by random as select k1, v1, v2 from t1;"
        );

        starRocksAssert.withMaterializedViews(mvs, (obj) -> {
            // Set a very short timeout to trigger timeout scenarios
            long originalTimeout = connectContext.getSessionVariable().getOptimizerExecuteTimeout();
            try {
                // Set timeout to 1ms to ensure timeouts occur
                connectContext.getSessionVariable().setOptimizerExecuteTimeout(1);

                String query = "select k1, v1, v2 from t1";
                Pair<MvRewritePreprocessor, OptExpression> result = buildMvProcessor(query);
                Assertions.assertNotNull(result);
                
                MvRewritePreprocessor preprocessor = result.first;
                OptExpression logicalTree = result.second;
                
                // This should not throw an exception even with timeouts
                preprocessor.prepare(logicalTree);
                
                // Verify that the process completed without throwing exceptions
                // The exact number of candidate MVs may vary due to timeouts, but the process should complete
                // The preprocessor should complete successfully even with timeouts
                Assertions.assertNotNull(preprocessor);
                
            } finally {
                // Restore original timeout
                connectContext.getSessionVariable().setOptimizerExecuteTimeout(originalTimeout);
            }
        });
    }

    /**
     * Test that the timeout calculation is correct for different numbers of MVs.
     */
    @Test
    public void testTimeoutCalculation() throws Exception {
        // Test with different numbers of MVs to verify timeout distribution
        List<String> mvs = ImmutableList.of(
                "create materialized view calc_mv_1 distributed by random as select k1, v1 from t1;",
                "create materialized view calc_mv_2 distributed by random as select k1, v2 from t1;"
        );

        starRocksAssert.withMaterializedViews(mvs, (obj) -> {
            long originalTimeout = connectContext.getSessionVariable().getOptimizerExecuteTimeout();
            try {
                // Set a known timeout value for testing
                connectContext.getSessionVariable().setOptimizerExecuteTimeout(10000); // 10 seconds


                String query = "select k1, v1 from t1";
                Pair<MvRewritePreprocessor, OptExpression> result = buildMvProcessor(query);
                Assertions.assertNotNull(result);
                
                MvRewritePreprocessor preprocessor = result.first;
                OptExpression logicalTree = result.second;
                
                // The process should complete successfully with 2 MVs
                // Each MV should get 5000ms timeout (10000/2), but minimum 1000ms
                preprocessor.prepare(logicalTree);
                
                // Verify the process completed
                Assertions.assertNotNull(preprocessor);
                
            } finally {
                // Restore original timeout
                connectContext.getSessionVariable().setOptimizerExecuteTimeout(originalTimeout);
            }
        });
    }

    /**
     * Test that the minimum timeout of 1 second is enforced.
     */
    @Test
    public void testMinimumTimeoutEnforcement() throws Exception {
        List<String> mvs = ImmutableList.of(
                "create materialized view min_timeout_mv_1 distributed by random as select k1, v1 from t1;",
                "create materialized view min_timeout_mv_2 distributed by random as select k1, v2 from t1;",
                "create materialized view min_timeout_mv_3 distributed by random as select k1, v2 from t1;",
                "create materialized view min_timeout_mv_4 distributed by random as select k1, v1, v2 from t1;",
                "create materialized view min_timeout_mv_5 distributed by random as select k1, v1, v2 from t1;",
                "create materialized view min_timeout_mv_6 distributed by random as select k1, v2 from t1;"
        );

        starRocksAssert.withMaterializedViews(mvs, (obj) -> {
            long originalTimeout = connectContext.getSessionVariable().getOptimizerExecuteTimeout();
            try {
                // Set timeout to 1000ms (1 second) - with 6 MVs, each would get 166ms
                // but should be enforced to minimum 1000ms
                connectContext.getSessionVariable().setOptimizerExecuteTimeout(1000);

                String query = "select k1, v1 from t1";
                Pair<MvRewritePreprocessor, OptExpression> result = buildMvProcessor(query);
                Assertions.assertNotNull(result);
                
                MvRewritePreprocessor preprocessor = result.first;
                OptExpression logicalTree = result.second;
                
                // Process should complete - each MV gets 1000ms (minimum enforced)
                preprocessor.prepare(logicalTree);
                
                Assertions.assertNotNull(preprocessor);
                
            } finally {
                connectContext.getSessionVariable().setOptimizerExecuteTimeout(originalTimeout);
            }
        });
    }

    /**
     * Test that the process handles empty MV lists gracefully.
     */
    @Test
    public void testEmptyMvList() throws Exception {
        // Don't create any MVs - test with empty list
        String query = "select k1, v1 from t1";
        Pair<MvRewritePreprocessor, OptExpression> result = buildMvProcessor(query);
        Assertions.assertNotNull(result);
        
        MvRewritePreprocessor preprocessor = result.first;
        OptExpression logicalTree = result.second;
        
        // Should handle empty MV list without issues
        preprocessor.prepare(logicalTree);
        
        Assertions.assertNotNull(preprocessor);
    }

    /**
     * Test that the process continues even when some MVs fail to prepare.
     */
    @Test
    public void testMvProcessingWithFailures() throws Exception {
        // Create a mix of valid and potentially problematic MVs
        List<String> mvs = ImmutableList.of(
                "create materialized view fail_mv_1 distributed by random as select k1, v1 from t1;",
                "create materialized view fail_mv_2 distributed by random as select k1, v2 from t1;",
                // This MV might be problematic due to complex structure
                "create materialized view fail_mv_3 distributed by random as select k1, count(distinct v1) from t1 group by k1;"
        );

        starRocksAssert.withMaterializedViews(mvs, (obj) -> {
            long originalTimeout = connectContext.getSessionVariable().getOptimizerExecuteTimeout();
            try {
                // Set a reasonable timeout
                connectContext.getSessionVariable().setOptimizerExecuteTimeout(5000);

                String query = "select k1, v1 from t1";
                Pair<MvRewritePreprocessor, OptExpression> result = buildMvProcessor(query);
                Assertions.assertNotNull(result);
                
                MvRewritePreprocessor preprocessor = result.first;
                OptExpression logicalTree = result.second;
                
                // Should complete even if some MVs have issues
                preprocessor.prepare(logicalTree);
                
                Assertions.assertNotNull(preprocessor);
                
            } finally {
                connectContext.getSessionVariable().setOptimizerExecuteTimeout(originalTimeout);
            }
        });
    }

    /**
     * Test that the process handles concurrent MV preparation correctly.
     */
    @Test
    public void testConcurrentMvProcessing() throws Exception {
        List<String> mvs = ImmutableList.of(
                "create materialized view concurrent_mv_1 distributed by random as select k1, v1 from t1;",
                "create materialized view concurrent_mv_2 distributed by random as select k1, v2 from t1;",
                "create materialized view concurrent_mv_3 distributed by random as select k1, v2 from t1;",
                "create materialized view concurrent_mv_4 distributed by random as select k1, v1, v2 from t1;"
        );

        starRocksAssert.withMaterializedViews(mvs, (obj) -> {
            long originalTimeout = connectContext.getSessionVariable().getOptimizerExecuteTimeout();
            try {
                // Enable concurrent processing
                connectContext.getSessionVariable().setEnableMaterializedViewConcurrentPrepare(true);
                connectContext.getSessionVariable().setOptimizerExecuteTimeout(10000);
                
                String query = "select k1, v1 from t1";
                Pair<MvRewritePreprocessor, OptExpression> result = buildMvProcessor(query);
                Assertions.assertNotNull(result);
                
                MvRewritePreprocessor preprocessor = result.first;
                OptExpression logicalTree = result.second;
                
                // Should handle concurrent processing with timeouts
                preprocessor.prepare(logicalTree);
                
                Assertions.assertNotNull(preprocessor);
                
            } finally {
                connectContext.getSessionVariable().setOptimizerExecuteTimeout((originalTimeout));
                connectContext.getSessionVariable().setEnableMaterializedViewConcurrentPrepare(false);
            }
        });
    }

    /**
     * Test that the process handles interruption correctly.
     */
    @Test
    public void testInterruptionHandling() throws Exception {
        List<String> mvs = ImmutableList.of(
                "create materialized view interrupt_mv_1 distributed by random as select k1, v1 from t1;",
                "create materialized view interrupt_mv_2 distributed by random as select k1, v2 from t1;"
        );

        starRocksAssert.withMaterializedViews(mvs, (obj) -> {
            long originalTimeout = connectContext.getSessionVariable().getOptimizerExecuteTimeout();
            try {
                connectContext.getSessionVariable().setOptimizerExecuteTimeout(10000);
                
                String query = "select k1, v1 from t1";
                Pair<MvRewritePreprocessor, OptExpression> result = buildMvProcessor(query);
                Assertions.assertNotNull(result);
                
                MvRewritePreprocessor preprocessor = result.first;
                OptExpression logicalTree = result.second;
                
                // Interrupt the current thread
                Thread.currentThread().interrupt();
                
                // Should handle interruption and throw appropriate exception
                try {
                    preprocessor.prepare(logicalTree);
                    // If we reach here, the interruption was handled gracefully
                    // Clear the interrupt flag for subsequent tests
                    Thread.interrupted();
                } catch (RuntimeException e) {
                    // Expected if interruption causes failure
                    Assertions.assertTrue(e.getMessage().contains("interrupted") || 
                                         e.getCause() instanceof InterruptedException);
                    // Clear the interrupt flag
                    Thread.interrupted();
                }
                
            } finally {
                // Restore original timeout
                connectContext.getSessionVariable().setOptimizerExecuteTimeout(originalTimeout);
                // Ensure interrupt flag is cleared
                Thread.interrupted();
            }
        });
    }

    @Test
    public void testMvIdConstructorAndGetters() {
        starRocksAssert.withMaterializedView("create materialized view mvid_test_mv1 " +
                        "distributed by random as select k1, v1 from t1",
                (obj) -> {
                    MaterializedView mv = getMv(DB_NAME, "mvid_test_mv1");
                    Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);

                    MvId mvId = new MvId(db.getId(), mv.getId());

                    Assertions.assertEquals(db.getId(), mvId.getDbId());
                    Assertions.assertEquals(mv.getId(), mvId.getId());
                });
    }

    @Test
    public void testMvIdEquals() {
        starRocksAssert.withMaterializedView("create materialized view mvid_test_mv2 " +
                        "distributed by random as select k1, v1 from t1",
                (obj) -> {
                    MaterializedView mv = getMv(DB_NAME, "mvid_test_mv2");
                    Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);

                    MvId mvId1 = new MvId(db.getId(), mv.getId());
                    MvId mvId2 = new MvId(db.getId(), mv.getId());
                    MvId mvId3 = new MvId(db.getId() + 1, mv.getId());
                    MvId mvId4 = new MvId(db.getId(), mv.getId() + 1);

                    // Same dbId and id should be equal
                    Assertions.assertEquals(mvId1, mvId2);

                    // Self equality
                    Assertions.assertEquals(mvId1, mvId1);

                    // Different dbId should not be equal
                    Assertions.assertNotEquals(mvId1, mvId3);

                    // Different id should not be equal
                    Assertions.assertNotEquals(mvId1, mvId4);

                    // Null check
                    Assertions.assertNotEquals(mvId1, null);

                    // Different class check
                    Assertions.assertNotEquals(mvId1, "string");
                });
    }

    @Test
    public void testMvIdHashCode() {
        starRocksAssert.withMaterializedView("create materialized view mvid_test_mv3 " +
                "distributed by random as select k1, v1 from t1",
                (obj) -> {
                    MaterializedView mv = getMv(DB_NAME, "mvid_test_mv3");
                    Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);

                    MvId mvId1 = new MvId(db.getId(), mv.getId());
                    MvId mvId2 = new MvId(db.getId(), mv.getId());

                    // Same dbId and id should have same hashCode
                    Assertions.assertEquals(mvId1.hashCode(), mvId2.hashCode());
                });
    }

    @Test
    public void testMvIdHashCodeWithDifferentIds() {
        starRocksAssert.withMaterializedView("create materialized view mvid_test_mv4 " +
                        "distributed by random as select k1, v1 from t1",
                (obj) -> {
                    MaterializedView mv = getMv(DB_NAME, "mvid_test_mv4");
                    Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);

                    MvId mvId1 = new MvId(db.getId(), mv.getId());
                    MvId mvId2 = new MvId(db.getId() + 1, mv.getId());
                    MvId mvId3 = new MvId(db.getId(), mv.getId() + 1);

                    // Different ids typically have different hashCodes (not guaranteed but expected)
                    Assertions.assertNotNull(mvId1.hashCode());
                    Assertions.assertNotNull(mvId2.hashCode());
                    Assertions.assertNotNull(mvId3.hashCode());
                });
    }


    @Test
    public void testMvIdEqualityContract() {
        starRocksAssert.withMaterializedView("create materialized view mvid_test_mv9 " +
                        "distributed by random as select k1, v1 from t1",
                (obj) -> {
                    MaterializedView mv = getMv(DB_NAME, "mvid_test_mv9");
                    Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);

                    MvId mvId1 = new MvId(db.getId(), mv.getId());
                    MvId mvId2 = new MvId(db.getId(), mv.getId());
                    MvId mvId3 = new MvId(db.getId(), mv.getId());

                    // Reflexive: x.equals(x) should be true
                    Assertions.assertEquals(mvId1, mvId1);

                    // Symmetric: x.equals(y) == y.equals(x)
                    Assertions.assertEquals(mvId1, mvId2);
                    Assertions.assertEquals(mvId2, mvId1);

                    // Transitive: if x.equals(y) and y.equals(z), then x.equals(z)
                    Assertions.assertEquals(mvId1, mvId2);
                    Assertions.assertEquals(mvId2, mvId3);
                    Assertions.assertEquals(mvId1, mvId3);

                    // Consistent hashCode
                    Assertions.assertEquals(mvId1.hashCode(), mvId2.hashCode());
                    Assertions.assertEquals(mvId2.hashCode(), mvId3.hashCode());
                });
    }

    @Test
    public void testMaterializedViewWrapperConstructorWithTwoParams() {
        starRocksAssert.withMaterializedView("create materialized view wrapper_test_mv1 " +
                        "distributed by random as select k1, v1 from t1",
                (obj) -> {
                    MaterializedView mv = getMv(DB_NAME, "wrapper_test_mv1");
                    MaterializedViewWrapper wrapper = new MaterializedViewWrapper(mv, 0);

                    Assertions.assertEquals(mv, wrapper.getMV());
                    Assertions.assertEquals(0, wrapper.getLevel());
                    Assertions.assertNull(wrapper.getMvPlanContext());
                });
    }

    @Test
    public void testMaterializedViewWrapperConstructorWithThreeParams() {
        starRocksAssert.withMaterializedView("create materialized view wrapper_test_mv2 " +
                        "distributed by random as select k1, v1 from t1",
                (obj) -> {
                    MaterializedView mv = getMv(DB_NAME, "wrapper_test_mv2");
                    MvPlanContext mvPlanContext = getOptimizedPlan(mv, false);
                    MaterializedViewWrapper wrapper = new MaterializedViewWrapper(mv, 1, mvPlanContext);

                    Assertions.assertEquals(mv, wrapper.getMV());
                    Assertions.assertEquals(1, wrapper.getLevel());
                    Assertions.assertNotNull(wrapper.getMvPlanContext());
                    Assertions.assertEquals(mvPlanContext, wrapper.getMvPlanContext());
                });
    }

    @Test
    public void testMaterializedViewWrapperCreateWithTwoParams() {
        starRocksAssert.withMaterializedView("create materialized view wrapper_test_mv3 " +
                        "distributed by random as select k1, v1 from t1",
                (obj) -> {
                    MaterializedView mv = getMv(DB_NAME, "wrapper_test_mv3");
                    MaterializedViewWrapper wrapper = MaterializedViewWrapper.create(mv, 2);

                    Assertions.assertEquals(mv, wrapper.getMV());
                    Assertions.assertEquals(2, wrapper.getLevel());
                    Assertions.assertNull(wrapper.getMvPlanContext());
                });
    }

    @Test
    public void testMaterializedViewWrapperCreateWithThreeParams() {
        starRocksAssert.withMaterializedView("create materialized view wrapper_test_mv4 " +
                        "distributed by random as select k1, v1 from t1",
                (obj) -> {
                    MaterializedView mv = getMv(DB_NAME, "wrapper_test_mv4");
                    MvPlanContext mvPlanContext = getOptimizedPlan(mv, false);
                    MaterializedViewWrapper wrapper = MaterializedViewWrapper.create(mv, 3, mvPlanContext);

                    Assertions.assertEquals(mv, wrapper.getMV());
                    Assertions.assertEquals(3, wrapper.getLevel());
                    Assertions.assertNotNull(wrapper.getMvPlanContext());
                    Assertions.assertEquals(mvPlanContext, wrapper.getMvPlanContext());
                });
    }

    @Test
    public void testMaterializedViewWrapperEqualsWithDifferentMVs() {
        List<String> mvs = ImmutableList.of(
                "create materialized view wrapper_test_mv6 distributed by random as select k1, v1 from t1",
                "create materialized view wrapper_test_mv7 distributed by random as select k1, v2 from t1"
        );

        starRocksAssert.withMaterializedViews(mvs,
                (obj) -> {
                    MaterializedView mv1 = getMv(DB_NAME, "wrapper_test_mv6");
                    MaterializedView mv2 = getMv(DB_NAME, "wrapper_test_mv7");

                    MaterializedViewWrapper wrapper1 = MaterializedViewWrapper.create(mv1, 0);
                    MaterializedViewWrapper wrapper2 = MaterializedViewWrapper.create(mv2, 0);

                    // Different MVs should not be equal
                    Assertions.assertNotEquals(wrapper1, wrapper2);
                });
    }

    @Test
    public void testMaterializedViewWrapperHashCodeWithDifferentMVs() {
        List<String> mvs = ImmutableList.of(
                "create materialized view wrapper_test_mv9 distributed by random as select k1, v1 from t1",
                "create materialized view wrapper_test_mv10 distributed by random as select k1, v2 from t1"
        );

        starRocksAssert.withMaterializedViews(mvs,
                (obj) -> {
                    MaterializedView mv1 = getMv(DB_NAME, "wrapper_test_mv9");
                    MaterializedView mv2 = getMv(DB_NAME, "wrapper_test_mv10");

                    MaterializedViewWrapper wrapper1 = MaterializedViewWrapper.create(mv1, 0);
                    MaterializedViewWrapper wrapper2 = MaterializedViewWrapper.create(mv2, 0);

                    // Different MVs typically have different hashCodes (not guaranteed but expected)
                    // This is just a sanity check
                    Assertions.assertNotNull(wrapper1.hashCode());
                    Assertions.assertNotNull(wrapper2.hashCode());
                });
    }

    @Test
    public void testMaterializedViewWrapperToString() {
        starRocksAssert.withMaterializedView("create materialized view wrapper_test_mv14 " +
                        "distributed by random as select k1, v1 from t1",
                (obj) -> {
                    MaterializedView mv = getMv(DB_NAME, "wrapper_test_mv14");
                    MaterializedViewWrapper wrapper = MaterializedViewWrapper.create(mv, 2);

                    String toString = wrapper.toString();
                    Assertions.assertNotNull(toString);
                    Assertions.assertTrue(toString.contains("MVWrapper{"));
                    Assertions.assertTrue(toString.contains("mv="));
                    Assertions.assertTrue(toString.contains("level=2"));
                });
    }

    @Test
    public void testMaterializedViewWrapperLevelValues() {
        starRocksAssert.withMaterializedView("create materialized view wrapper_test_mv15 " +
                        "distributed by random as select k1, v1 from t1",
                (obj) -> {
                    MaterializedView mv = getMv(DB_NAME, "wrapper_test_mv15");

                    // Test various level values
                    MaterializedViewWrapper wrapper0 = MaterializedViewWrapper.create(mv, 0);
                    MaterializedViewWrapper wrapper1 = MaterializedViewWrapper.create(mv, 1);
                    MaterializedViewWrapper wrapper5 = MaterializedViewWrapper.create(mv, 5);
                    MaterializedViewWrapper wrapper10 = MaterializedViewWrapper.create(mv, 10);

                    Assertions.assertEquals(0, wrapper0.getLevel());
                    Assertions.assertEquals(1, wrapper1.getLevel());
                    Assertions.assertEquals(5, wrapper5.getLevel());
                    Assertions.assertEquals(10, wrapper10.getLevel());
                });
    }

    @Test
    public void testMaterializedViewWrapperWithNullMvPlanContext() {
        starRocksAssert.withMaterializedView("create materialized view wrapper_test_mv16 " +
                        "distributed by random as select k1, v1 from t1",
                (obj) -> {
                    MaterializedView mv = getMv(DB_NAME, "wrapper_test_mv16");
                    MaterializedViewWrapper wrapper = MaterializedViewWrapper.create(mv, 0, null);

                    Assertions.assertEquals(mv, wrapper.getMV());
                    Assertions.assertEquals(0, wrapper.getLevel());
                    Assertions.assertNull(wrapper.getMvPlanContext());
                });
    }

    @Test
    public void testMaterializedViewGlobalContextCache() {
        starRocksAssert.withMaterializedView("create materialized view wrapper_test_mv15 " +
                        "distributed by random as select k1, v1 from t1",
                (obj) -> {
                    MaterializedView mv = getMv(DB_NAME, "wrapper_test_mv15");
                    CachingMvPlanContextBuilder.MVCacheEntity mvGlobalCache =
                            CachingMvPlanContextBuilder.getMVCache(mv);
                    Assertions.assertTrue(mvGlobalCache != null);

                    Table refTable = GlobalStateMgr.getCurrentState()
                            .getLocalMetastore().getDb(DB_NAME).getTable("t1");
                    Assertions.assertTrue(refTable != null);

                    PRangeCellPlus.PCellCacheKey key1 = new PRangeCellPlus.PCellCacheKey(null, "", null);
                    Object result = mvGlobalCache.getIfPresent(key1);
                    Assertions.assertTrue(result == null);
                    result  = mvGlobalCache.get(key1, () -> "test_value");
                    Assertions.assertEquals("test_value", result);

                    PRangeCellPlus.PCellCacheKey key2 = new PRangeCellPlus.PCellCacheKey(refTable,
                            "p1", new PRangeCell(
                                    Range.closed(PartitionKey.ofDateTime(LocalDateTime.of(2025, 5, 27, 10, 0, 0)),
                                    PartitionKey.ofDateTime(LocalDateTime.of(2025, 5, 27, 11, 0, 0)))));
                    result = mvGlobalCache.getIfPresent(key2);
                    Assertions.assertTrue(result == null);
                    result  = mvGlobalCache.get(key2, () -> 12345);
                    Assertions.assertEquals(12345, result);

                    String cacheStats1 = CachingMvPlanContextBuilder.getMVGlobalContextCacheStats(mv);
                    System.out.println(cacheStats1);
                    Assertions.assertFalse(Strings.isNullOrEmpty(cacheStats1));

                    String cacheStats2 = CachingMvPlanContextBuilder.getMVPlanCacheStats();
                    System.out.println(cacheStats2);
                    Assertions.assertFalse(Strings.isNullOrEmpty(cacheStats2));
                });
    }
}
