// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer;

import com.starrocks.common.Pair;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.PhysicalPropertySet;
import com.starrocks.sql.optimizer.operator.logical.LogicalFilterOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalFilterOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalOlapScanOperator;
import com.starrocks.sql.optimizer.rule.RuleSetType;
import com.starrocks.sql.optimizer.rule.RuleType;
import com.starrocks.sql.optimizer.transformer.LogicalPlan;
import com.starrocks.sql.optimizer.transformer.RelationTransformer;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.sql.plan.PlanTestBase;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.Test;

public class OptimizerTest extends PlanTestBase {
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
}
