// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.
package com.starrocks.sql.optimizer.rule.mv;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.analysis.Expr;
import com.starrocks.catalog.AggregateType;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.Type;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalTableFunctionOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ReplaceColumnRefRewriter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.starrocks.catalog.Function.CompareMode.IS_IDENTICAL;

public class MaterializedViewRewriter extends OptExpressionVisitor<Void, MaterializedViewRule.RewriteContext> {

    public MaterializedViewRewriter() {
    }

    public Void rewrite(OptExpression optExpression, MaterializedViewRule.RewriteContext context) {
        optExpression.getOp().accept(this, optExpression, context);
        return null;
    }

    @Override
    public Void visit(OptExpression optExpression, MaterializedViewRule.RewriteContext context) {
        optExpression.getInputs().forEach(e -> rewrite(e, context));
        return null;
    }

    @Override
    public Void visitLogicalProject(OptExpression optExpression, MaterializedViewRule.RewriteContext context) {
        optExpression.getInputs().forEach(e -> rewrite(e, context));
        LogicalProjectOperator projectOperator = (LogicalProjectOperator) optExpression.getOp();

        Map<ColumnRefOperator, ScalarOperator> newProjectMap = Maps.newHashMap();
        for (Map.Entry<ColumnRefOperator, ScalarOperator> kv : projectOperator.getColumnRefMap().entrySet()) {
            if (kv.getValue().getUsedColumns().contains(context.queryColumnRef)) {
                if (kv.getValue() instanceof ColumnRefOperator) {
                    newProjectMap.put(context.mvColumnRef, context.mvColumnRef);
                } else {
                    newProjectMap.put(kv.getKey(), context.mvColumnRef);
                }
            } else {
                newProjectMap.put(kv.getKey(), kv.getValue());
            }
        }
        projectOperator.setColumnRefMap(newProjectMap);
        return null;
    }

    @Override
    public Void visitLogicalTableScan(OptExpression optExpression, MaterializedViewRule.RewriteContext context) {
        if (!OperatorType.LOGICAL_OLAP_SCAN.equals(optExpression.getOp().getOpType())) {
            return null;
        }

        LogicalOlapScanOperator scanOperator = (LogicalOlapScanOperator) optExpression.getOp();

        if (scanOperator.getColumnRefMap().containsKey(context.queryColumnRef)) {
            scanOperator.getOutputColumns().remove(context.queryColumnRef);
            scanOperator.getOutputColumns().add(context.mvColumnRef);
            scanOperator.getColumnRefMap().remove(context.queryColumnRef);
            scanOperator.getColumnRefMap().put(context.mvColumnRef, context.mvColumn);
        }
        return null;
    }

    @Override
    public Void visitLogicalAggregate(OptExpression optExpression, MaterializedViewRule.RewriteContext context) {
        optExpression.getInputs().forEach(e -> rewrite(e, context));
        LogicalAggregationOperator aggregationOperator = (LogicalAggregationOperator) optExpression.getOp();

        Map<ColumnRefOperator, ScalarOperator> replaceMap = new HashMap<>();
        replaceMap.put(context.queryColumnRef, context.mvColumnRef);
        ReplaceColumnRefRewriter replaceColumnRefRewriter = new ReplaceColumnRefRewriter(replaceMap);

        for (Map.Entry<ColumnRefOperator, CallOperator> kv : aggregationOperator.getAggregations().entrySet()) {
            String functionName = kv.getValue().getFnName();
            if (functionName.equals(context.aggCall.getFnName())
                    && kv.getValue().getUsedColumns().getFirstId() == context.queryColumnRef.getId()) {
                if (kv.getValue().getFnName().equals(FunctionSet.COUNT) && !kv.getValue().isDistinct()) {

                    CallOperator callOperator = new CallOperator(FunctionSet.SUM,
                            kv.getValue().getType(),
                            kv.getValue().getChildren(),
                            Expr.getBuiltinFunction(FunctionSet.SUM, new Type[] {Type.BIGINT}, IS_IDENTICAL));
                    kv.setValue((CallOperator) replaceColumnRefRewriter.visit(callOperator, null));
                    break;
                } else if (
                        ((functionName.equals(FunctionSet.COUNT) && kv.getValue().isDistinct())
                                || functionName.equals(FunctionSet.MULTI_DISTINCT_COUNT)) &&
                                context.mvColumn.getAggregationType() == AggregateType.BITMAP_UNION) {
                    CallOperator callOperator = new CallOperator(FunctionSet.BITMAP_UNION_COUNT,
                            kv.getValue().getType(),
                            kv.getValue().getChildren(),
                            Expr.getBuiltinFunction(FunctionSet.BITMAP_UNION_COUNT, new Type[] {Type.BITMAP},
                                    IS_IDENTICAL));
                    kv.setValue((CallOperator) replaceColumnRefRewriter.visit(callOperator, null));
                    break;
                } else if (
                        (functionName.equals(FunctionSet.NDV) || functionName.equals(FunctionSet.APPROX_COUNT_DISTINCT))
                                && context.mvColumn.getAggregationType() == AggregateType.HLL_UNION) {
                    CallOperator callOperator = new CallOperator(FunctionSet.HLL_UNION_AGG,
                            kv.getValue().getType(),
                            kv.getValue().getChildren(),
                            Expr.getBuiltinFunction(FunctionSet.HLL_UNION_AGG, new Type[] {Type.HLL}, IS_IDENTICAL));
                    kv.setValue((CallOperator) replaceColumnRefRewriter.visit(callOperator, null));
                    break;
                } else if (functionName.equals(FunctionSet.PERCENTILE_APPROX) &&
                        context.mvColumn.getAggregationType() == AggregateType.PERCENTILE_UNION) {

                    ScalarOperator child = kv.getValue().getChildren().get(0);
                    if (child instanceof CastOperator) {
                        child = child.getChild(0);
                    }
                    Preconditions.checkState(child instanceof ColumnRefOperator);
                    CallOperator callOperator = new CallOperator(FunctionSet.PERCENTILE_UNION,
                            kv.getValue().getType(),
                            Lists.newArrayList(child),
                            Expr.getBuiltinFunction(FunctionSet.PERCENTILE_UNION,
                                    new Type[] {Type.PERCENTILE}, IS_IDENTICAL));
                    kv.setValue((CallOperator) replaceColumnRefRewriter.visit(callOperator, null));
                    break;
                }
            }
        }
        return null;
    }

    @Override
    public Void visitLogicalTableFunction(OptExpression optExpression, MaterializedViewRule.RewriteContext context) {
        optExpression.getInputs().forEach(e -> rewrite(e, context));

        ColumnRefSet bitSet = new ColumnRefSet();
        LogicalTableFunctionOperator tableFunctionOperator = (LogicalTableFunctionOperator) optExpression.getOp();
        for (Integer columnId : tableFunctionOperator.getOuterColumnRefSet().getColumnIds()) {
            if (columnId == context.queryColumnRef.getId()) {
                bitSet.union(context.mvColumnRef.getId());
            } else {
                bitSet.union(columnId);
            }
        }

        tableFunctionOperator.setOuterColumnRefSet(bitSet);
        return null;
    }

    @Override
    public Void visitLogicalJoin(OptExpression optExpression, MaterializedViewRule.RewriteContext context) {
        optExpression.getInputs().forEach(e -> rewrite(e, context));

        LogicalJoinOperator joinOperator = (LogicalJoinOperator) optExpression.getOp();
        List<ColumnRefOperator> pruneOutputColumns = joinOperator.getPruneOutputColumns();

        List<ColumnRefOperator> newPruneOutputColumns = new ArrayList<>();
        for (ColumnRefOperator c : pruneOutputColumns) {
            if (c.equals(context.queryColumnRef)) {
                newPruneOutputColumns.add(context.mvColumnRef);
            } else {
                newPruneOutputColumns.add(c);
            }
        }
        joinOperator.setPruneOutputColumns(newPruneOutputColumns);
        return null;
    }
}
