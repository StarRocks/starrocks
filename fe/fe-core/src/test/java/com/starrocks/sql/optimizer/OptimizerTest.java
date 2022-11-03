// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer;

import com.starrocks.analysis.Expr;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.ExpressionRangePartitionInfo;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.common.Pair;
import com.starrocks.pseudocluster.PseudoCluster;
import com.starrocks.qe.ConnectContext;
import com.starrocks.scheduler.Task;
import com.starrocks.scheduler.TaskBuilder;
import com.starrocks.scheduler.TaskManager;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.PhysicalPropertySet;
import com.starrocks.sql.optimizer.operator.logical.LogicalFilterOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.RuleSetType;
import com.starrocks.sql.optimizer.rule.RuleType;
import com.starrocks.sql.optimizer.transformer.LogicalPlan;
import com.starrocks.sql.optimizer.transformer.RelationTransformer;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

public class OptimizerTest {
    private static ConnectContext connectContext;
    private static PseudoCluster cluster;
    private static StarRocksAssert starRocksAssert;

    @BeforeClass
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
    }

    private MaterializedView getMv(String dbName, String mvName) {
        Database db = GlobalStateMgr.getCurrentState().getDb(dbName);
        Table table = db.getTable(mvName);
        Assert.assertNotNull(table);
        Assert.assertTrue(table instanceof MaterializedView);
        MaterializedView mv = (MaterializedView) table;
        return mv;
    }

    private void refreshMaterializedView(String dbName, String mvName) throws Exception {
        MaterializedView mv = getMv(dbName, mvName);
        TaskManager taskManager = GlobalStateMgr.getCurrentState().getTaskManager();
        final String mvTaskName = TaskBuilder.getMvTaskName(mv.getId());
        if (!taskManager.containTask(mvTaskName)) {
            Task task = TaskBuilder.buildMvTask(mv, "test");
            TaskBuilder.updateTaskInfo(task, mv);
            taskManager.createTask(task, false);
        }
        taskManager.executeTaskSync(mvTaskName);
    }

    @Test
    public void testPreprocessMvNonPartitionMv() throws Exception {
        Config.enable_experimental_mv = true;
        cluster.runSql("test", "insert into t0 values(10, 20, 30)");
        starRocksAssert.withNewMaterializedView("create materialized view mv_1 distributed by hash(`v1`) " +
                "as select v1, v2, sum(v3) as total from t0 group by v1, v2");
        starRocksAssert.withNewMaterializedView("create materialized view mv_2 distributed by hash(`v1`) " +
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
                        "    PARTITION p2 values [('2022-02-16'),('2022-03-01'))\n" +
                        ")\n" +
                        "DISTRIBUTED BY HASH(k2) BUCKETS 3\n" +
                        "PROPERTIES('replication_num' = '1');")
                .withNewMaterializedView("create materialized view mv_3\n" +
                        "distributed by hash(k2) buckets 3\n" +
                        "refresh async\n" +
                        "as select k2, sum(v1) as total from tbl_with_mv group by k2;")
                .withNewMaterializedView("create materialized view mv_4\n" +
                        "PARTITION BY k1\n" +
                        "distributed by hash(k2) buckets 3\n" +
                        "refresh manual\n" +
                        "as select k1, k2, v1  from tbl_with_mv;");
        cluster.runSql("test", "insert into tbl_with_mv values(\"2020-02-20\", 20, 30)");
        refreshMaterializedView("test", "mv_3");
        refreshMaterializedView("test", "mv_4");

        cluster.runSql("test", "insert into tbl_with_mv partition(p2) values(\"2020-02-21\", 20, 30)");

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
        ExpressionRangePartitionInfo partitionInfo = (ExpressionRangePartitionInfo) mv.getPartitionInfo();
        Expr partitionExpr = partitionInfo.getPartitionExprs().get(0);
        Pair<Table, Column> partitionTableAndColumn = Utils.getPartitionTableAndColumn(partitionExpr, mv.getBaseTableInfos());
        Assert.assertEquals("tbl_with_mv", partitionTableAndColumn.first.getName());

        List<OptExpression> scanExpr = Utils.collectScanExprs(logicalPlan.getRoot());
        Assert.assertEquals(1, scanExpr.size());
        Assert.assertNotNull(scanExpr.get(0).getOp().getPredicate());
        ScalarOperator scalarOperator  = scanExpr.get(0).getOp().getPredicate();
        Assert.assertTrue(scalarOperator instanceof CompoundPredicateOperator);

        starRocksAssert.dropMaterializedView("mv_3");
        starRocksAssert.dropMaterializedView("mv_4");
    }
}
