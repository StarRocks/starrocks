// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.sql.optimizer.rule.mv;

import com.google.common.base.Preconditions;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;

import java.util.List;

// Rewrite agg -> project -> scan logic operator by RewriteContext
public class MVAggProjectScanRewrite extends MVAggRewrite {
    private MVAggProjectScanRewrite() {
    }

    private static final MVAggProjectScanRewrite instance = new MVAggProjectScanRewrite();

    public static MVAggProjectScanRewrite getInstance() {
        return instance;
    }

    public void rewriteOptExpressionTree(
            ColumnRefFactory factory,
            int relationId, OptExpression input,
            List<MaterializedViewRule.RewriteContext> rewriteContexts) {
        for (OptExpression child : input.getInputs()) {
            rewriteOptExpressionTree(factory, relationId, child, rewriteContexts);
        }

        Operator operator = input.getOp();
        if (operator instanceof LogicalAggregationOperator &&
                input.inputAt(0).inputAt(0).getOp() instanceof LogicalOlapScanOperator) {
            LogicalAggregationOperator aggOperator = (LogicalAggregationOperator) operator;
            Operator childOperator = input.inputAt(0).getOp();
            Preconditions.checkState(childOperator instanceof LogicalProjectOperator);
            LogicalProjectOperator projectOperator = (LogicalProjectOperator) childOperator;
            Operator grandchildrenOp = input.inputAt(0).inputAt(0).getOp();
            LogicalOlapScanOperator scanOperator = (LogicalOlapScanOperator) grandchildrenOp;
            if (factory.getRelationId(scanOperator.getOutputColumns().get(0).getId()) == relationId) {
                for (MaterializedViewRule.RewriteContext context : rewriteContexts) {
                    rewriteOlapScanOperator(scanOperator, context.mvColumn,
                            context.queryColumnRef, context.mvColumnRef);
                    ColumnRefOperator projectColumn = rewriteProjectOperator(projectOperator,
                            context.queryColumnRef,
                            context.mvColumnRef);
                    rewriteAggOperator(aggOperator, context.aggCall,
                            projectColumn,
                            context.mvColumn);
                }
            }
        }
    }
}
