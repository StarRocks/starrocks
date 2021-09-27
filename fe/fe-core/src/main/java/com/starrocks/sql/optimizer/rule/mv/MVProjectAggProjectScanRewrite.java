// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.sql.optimizer.rule.mv;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.analysis.Expr;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.Type;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ReplaceColumnRefRewriter;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

// Rewrite project -> agg -> project -> scan logic operator by RewriteContext
// Currently, only used for percentile_union mv.
// we need rewrite percentile_approx to percentile_approx_raw(percentile_union)
// TODO(kks): Remove this class if we support percentile_union_count aggregate function later
public class MVProjectAggProjectScanRewrite extends MVAggRewrite {
    private MVProjectAggProjectScanRewrite() {
    }

    private static final MVProjectAggProjectScanRewrite instance = new MVProjectAggProjectScanRewrite();

    public static MVProjectAggProjectScanRewrite getInstance() {
        return instance;
    }

    public void rewriteOptExpressionTree(
            ColumnRefFactory factory,
            int relationId, OptExpression input,
            List<MaterializedViewRule.RewriteContext> rewriteContexts) {
        input.attachGroupExpression(null);
        for (OptExpression child : input.getInputs()) {
            rewriteOptExpressionTree(factory, relationId, child, rewriteContexts);
        }

        if (input.getOp() instanceof LogicalProjectOperator &&
                input.inputAt(0).getOp() instanceof LogicalAggregationOperator &&
                input.inputAt(0).inputAt(0).inputAt(0).getOp() instanceof LogicalOlapScanOperator) {
            LogicalProjectOperator topProject = (LogicalProjectOperator) input.getOp();
            LogicalAggregationOperator agg = (LogicalAggregationOperator) input.inputAt(0).getOp();
            LogicalProjectOperator bellowProject = (LogicalProjectOperator)
                    input.inputAt(0).inputAt(0).getOp();
            LogicalOlapScanOperator scanOperator = (LogicalOlapScanOperator)
                    input.inputAt(0).inputAt(0).inputAt(0).getOp();
            if (factory.getRelationId(scanOperator.getOutputColumns().get(0).getId()) == relationId) {
                for (MaterializedViewRule.RewriteContext context : rewriteContexts) {
                    rewriteOlapScanOperator(input.inputAt(0).inputAt(0), scanOperator, context);
                    ColumnRefOperator projectColumn = rewriteProjectOperator(bellowProject,
                            context.queryColumnRef,
                            context.mvColumnRef);
                    rewriteAggOperator(agg, context.aggCall,
                            projectColumn,
                            context.mvColumn);
                    rewriteTopProjectOperator(agg, topProject,
                            projectColumn, context.aggCall);
                }
            }
        }
    }

    private void rewriteTopProjectOperator(LogicalAggregationOperator agg,
                                           LogicalProjectOperator project,
                                           ColumnRefOperator aggUsedColumn,
                                           CallOperator queryAgg) {
        ColumnRefOperator percentileColumn = null;
        for (Map.Entry<ColumnRefOperator, CallOperator> kv : agg.getAggregations().entrySet()) {
            if (kv.getValue().getFnName().equals(FunctionSet.PERCENTILE_UNION)
                    && kv.getValue().getUsedColumns().getFirstId() == aggUsedColumn.getId()) {
                percentileColumn = kv.getKey();
                break;
            }
        }
        Preconditions.checkState(percentileColumn != null);
        CallOperator percentileApproxRaw = new CallOperator(FunctionSet.PERCENTILE_APPROX_RAW,
                Type.DOUBLE, Lists.newArrayList(percentileColumn, queryAgg.getChild(1)),
                Expr.getBuiltinFunction(
                        FunctionSet.PERCENTILE_APPROX_RAW,
                        new Type[] {Type.PERCENTILE, Type.DOUBLE},
                        Function.CompareMode.IS_IDENTICAL));

        Map<ColumnRefOperator, ScalarOperator> rewriteMap = new HashMap<>();
        rewriteMap.put(percentileColumn, percentileApproxRaw);
        ReplaceColumnRefRewriter rewriter = new ReplaceColumnRefRewriter(rewriteMap);

        for (Map.Entry<ColumnRefOperator, ScalarOperator> kv : project.getColumnRefMap().entrySet()) {
            if (kv.getValue().getUsedColumns().contains(percentileColumn)) {
                kv.setValue(kv.getValue().accept(rewriter, null));
                break;
            }
        }
    }
}
