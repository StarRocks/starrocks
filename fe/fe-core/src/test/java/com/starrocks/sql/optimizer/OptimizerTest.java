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


package com.starrocks.sql.optimizer;

import com.google.common.collect.Lists;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.common.Pair;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.PhysicalPropertySet;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalFilterOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
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
import com.starrocks.sql.optimizer.rule.transformation.materialization.MvRewriteTestBase;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MvUtils;
import com.starrocks.sql.optimizer.task.RewriteTreeTask;
import com.starrocks.sql.optimizer.task.TaskContext;
import com.starrocks.sql.optimizer.transformer.LogicalPlan;
import com.starrocks.sql.optimizer.transformer.RelationTransformer;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

public class OptimizerTest extends MvRewriteTestBase {
    @BeforeClass
<<<<<<< HEAD
    public static void setUp() throws Exception {
        // set timeout to a really long time so that ut can pass even when load of ut machine is very high
        Config.bdbje_heartbeat_timeout_second = 60;
        Config.bdbje_replica_ack_timeout_second = 60;
        Config.bdbje_lock_timeout_second = 60;
        // set some parameters to speedup test
        Config.tablet_sched_checker_interval_seconds = 1;
        Config.tablet_sched_repair_delay_factor_second = 1;
        Config.enable_new_publish_mechanism = true;
        PseudoCluster.getOrCreateWithRandomPort(true, 3);
        GlobalStateMgr.getCurrentState().getTabletChecker().setInterval(1000);
        cluster = PseudoCluster.getInstance();
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);
        starRocksAssert.withDatabase("test").useDatabase("test");
        starRocksAssert.withTable("CREATE TABLE `t0` (\n" +
                "  `v1` bigint NULL COMMENT \"\",\n" +
                "  `v2` bigint NULL COMMENT \"\",\n" +
                "  `v3` bigint NULL\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`v1`, `v2`, v3)\n" +
                "DISTRIBUTED BY HASH(`v1`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"DEFAULT\"\n" +
                ");");
    }

    @AfterClass
    public static void tearDown() throws Exception {
        PseudoCluster.getInstance().shutdown(true);
=======
    public static void before() throws Exception {
        starRocksAssert.useTable("t0");
>>>>>>> f731708af3 ([UT] Introduce MTable/MSchema to avoid creating unused tables in FE UTs (backport #36711) (backport #37580) (#37679))
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

        //1. Build Logical plan
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
            connectContext.getSessionVariable().setOptimizerExecuteTimeout(3000);
        }
    }

    @Test
    public void testPreprocessMvNonPartitionMv() throws Exception {
        Config.enable_experimental_mv = true;
        cluster.runSql("test", "insert into t0 values(10, 20, 30)");
        starRocksAssert.withMaterializedView("create materialized view mv_1 distributed by hash(`v1`) " +
                "as select v1, v2, sum(v3) as total from t0 group by v1, v2");
        starRocksAssert.withMaterializedView("create materialized view mv_2 distributed by hash(`v1`) " +
                "as select v1, sum(total) as total from mv_1 group by v1");
        refreshMaterializedView("test", "mv_1");
        refreshMaterializedView("test", "mv_2");

        String sql = "select v1, sum(v3) from t0 group by v1";
        StatementBase stmt = UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        QueryStatement query = (QueryStatement) stmt;

        //1. Build Logical plan
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
        connectContext.getSessionVariable().setOptimizerExecuteTimeout(30000000);
        Config.enable_experimental_mv = true;
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
                        "PROPERTIES('replication_num' = '1');")
                .withMaterializedView("create materialized view mv_3\n" +
                        "distributed by hash(k2) buckets 3\n" +
                        "refresh async\n" +
                        "as select k2, sum(v1) as total from tbl_with_mv group by k2;")
                .withMaterializedView("create materialized view mv_4\n" +
                        "PARTITION BY k1\n" +
                        "distributed by hash(k2) buckets 3\n" +
                        "refresh manual\n" +
                        "as select k1, k2, v1  from tbl_with_mv;");
        cluster.runSql("test", "insert into tbl_with_mv values(\"2020-02-20\", 20, 30)");
        refreshMaterializedView("test", "mv_3");
        refreshMaterializedView("test", "mv_4");

        cluster.runSql("test", "insert into tbl_with_mv partition(p3) values(\"2020-03-05\", 20, 30)");

        String sql = "select k1, sum(v1) from tbl_with_mv group by k1";
        StatementBase stmt = UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        QueryStatement query = (QueryStatement) stmt;

        //1. Build Logical plan
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
        Pair<Table, Column> partitionTableAndColumn = mv.getPartitionTableAndColumn();
        Assert.assertEquals("tbl_with_mv", partitionTableAndColumn.first.getName());

        ScalarOperator scalarOperator  = materializationContext.getMvPartialPartitionPredicate();
        Assert.assertNotNull(scalarOperator);
        Assert.assertTrue(scalarOperator instanceof CompoundPredicateOperator);
        Assert.assertTrue(((CompoundPredicateOperator) scalarOperator).isAnd());

        refreshMaterializedView("test", "mv_4");
        cluster.runSql("test", "insert into tbl_with_mv partition(p2) values(\"2020-02-20\", 20, 30)");
        Optimizer optimizer2 = new Optimizer();
        OptExpression expr2 = optimizer2.optimize(connectContext, logicalPlan.getRoot(), new PhysicalPropertySet(),
                new ColumnRefSet(logicalPlan.getOutputColumn()), columnRefFactory);
        Assert.assertNotNull(expr2);
        MaterializationContext materializationContext2 = optimizer2.getContext().getCandidateMvs().iterator().next();
        Assert.assertEquals("mv_4", materializationContext2.getMv().getName());
        ScalarOperator scalarOperator2  = materializationContext2.getMvPartialPartitionPredicate();
        Assert.assertNotNull(scalarOperator2);
        Assert.assertTrue(scalarOperator2 instanceof CompoundPredicateOperator);
        Assert.assertTrue(((CompoundPredicateOperator) scalarOperator2).isOr());

        starRocksAssert.dropMaterializedView("mv_3");
        starRocksAssert.dropMaterializedView("mv_4");

        starRocksAssert.withMaterializedView("create materialized view mv_5\n" +
                "PARTITION BY date_trunc(\"month\", k1)\n" +
                "distributed by hash(k2) buckets 3\n" +
                "refresh manual\n" +
                "as select k1, k2, v1  from tbl_with_mv;");
        refreshMaterializedView("test", "mv_5");
        cluster.runSql("test", "insert into tbl_with_mv partition(p3) values(\"2020-03-05\", 20, 30)");

        stmt = UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        query = (QueryStatement) stmt;

        Optimizer optimizer3 = new Optimizer();
        logicalPlan = new RelationTransformer(columnRefFactory, connectContext)
                .transformWithSelectLimit(query.getQueryRelation());
        OptExpression expr3 = optimizer3.optimize(connectContext, logicalPlan.getRoot(), new PhysicalPropertySet(),
                new ColumnRefSet(logicalPlan.getOutputColumn()), columnRefFactory);
        Assert.assertNotNull(expr3);
        MaterializationContext materializationContext3 = optimizer3.getContext().getCandidateMvs().iterator().next();
        Assert.assertEquals("mv_5", materializationContext3.getMv().getName());
        List<OptExpression> scanExpr3 = MvUtils.collectScanExprs(materializationContext3.getMvExpression());
        Assert.assertEquals(1, scanExpr3.size());
        ScalarOperator scalarOperator3  = materializationContext3.getMvPartialPartitionPredicate();
        Assert.assertNotNull(scalarOperator3);
        Assert.assertTrue(scalarOperator3 instanceof CompoundPredicateOperator);
        Assert.assertTrue(((CompoundPredicateOperator) scalarOperator3).isAnd());
        Assert.assertTrue(scalarOperator3.getChild(0) instanceof BinaryPredicateOperator);
        Assert.assertTrue(scalarOperator3.getChild(0).getChild(0) instanceof ColumnRefOperator);
        ColumnRefOperator columnRef = (ColumnRefOperator) scalarOperator3.getChild(0).getChild(0);
        Assert.assertEquals("k1", columnRef.getName());
        LogicalOlapScanOperator scanOperator = materializationContext3.getScanMvOperator();
        Assert.assertEquals(1, scanOperator.getSelectedPartitionId().size());
    }
}
