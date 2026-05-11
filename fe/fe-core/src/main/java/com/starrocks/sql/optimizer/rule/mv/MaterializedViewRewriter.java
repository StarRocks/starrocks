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

package com.starrocks.sql.optimizer.rule.mv;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.sql.ast.AggregateType;
import com.starrocks.sql.ast.expression.ExprUtils;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalTableFunctionOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CaseWhenOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperatorUtil;
import com.starrocks.sql.optimizer.rewrite.ReplaceColumnRefRewriter;
import com.starrocks.sql.optimizer.rule.transformation.materialization.common.MvColumnRefSubstitutor;
import com.starrocks.type.BitmapType;
import com.starrocks.type.HLLType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.PercentileType;
import com.starrocks.type.Type;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.starrocks.catalog.Function.CompareMode.IS_IDENTICAL;

public class MaterializedViewRewriter extends OptExpressionVisitor<OptExpression, MaterializedViewRule.RewriteContext> {

    private boolean substitutionFailed = false;

    public boolean substitutionFailed() {
        return substitutionFailed;
    }

    public MaterializedViewRewriter() {
    }

    public OptExpression rewrite(OptExpression optExpression, MaterializedViewRule.RewriteContext context) {
        return optExpression.getOp().accept(this, optExpression, context);
    }

    public static boolean isCaseWhenScalarOperator(ScalarOperator operator) {
        if (operator instanceof CaseWhenOperator) {
            return true;
        }

        return operator instanceof CallOperator &&
                FunctionSet.IF.equalsIgnoreCase(((CallOperator) operator).getFnName());
    }

    @Override
    public OptExpression visit(OptExpression optExpression, MaterializedViewRule.RewriteContext context) {
        for (int childIdx = 0; childIdx < optExpression.arity(); ++childIdx) {
            optExpression.setChild(childIdx, rewrite(optExpression.inputAt(childIdx), context));
        }

        return OptExpression.create(optExpression.getOp(), optExpression.getInputs());
    }

    @Override
    public OptExpression visitLogicalProject(OptExpression optExpression, MaterializedViewRule.RewriteContext context) {
        for (int childIdx = 0; childIdx < optExpression.arity(); ++childIdx) {
            optExpression.setChild(childIdx, rewrite(optExpression.inputAt(childIdx), context));
        }

        LogicalProjectOperator projectOperator = (LogicalProjectOperator) optExpression.getOp();

        Map<ColumnRefOperator, ScalarOperator> newProjectMap = Maps.newHashMap();
        for (Map.Entry<ColumnRefOperator, ScalarOperator> kv : projectOperator.getColumnRefMap().entrySet()) {
            ColumnRefOperator queryColRef = kv.getKey();
            ScalarOperator queryScalarOperator = kv.getValue();
            if (queryScalarOperator.getUsedColumns().contains(context.queryColumnRef)) {
                if (queryScalarOperator instanceof ColumnRefOperator) {
                    newProjectMap.put(context.mvColumnRef, context.mvColumnRef);
                } else if (isCaseWhenScalarOperator(queryScalarOperator)) {
                    // Route through MvColumnRefSubstitutor so the rewritten subtree's
                    // types are re-derived bottom-up after the leaf substitution.
                    Map<ColumnRefOperator, ScalarOperator> replaceMap = new HashMap<>();
                    replaceMap.put(context.queryColumnRef, context.mvColumnRef);
                    Optional<ScalarOperator> rewritten = MvColumnRefSubstitutor.substituteAndSyncOutput(
                            queryColRef, queryScalarOperator, replaceMap);
                    if (!rewritten.isPresent()) {
                        substitutionFailed = true;
                        return optExpression;
                    }
                    newProjectMap.put(queryColRef, rewritten.get());
                } else {
                    // eg: bitmap_union(to_bitmap(a)), still rewrite to bitmap_union(to_bitmap(a))
                    newProjectMap.put(queryColRef, context.mvColumnRef);
                }
            } else {
                newProjectMap.put(queryColRef, queryScalarOperator);
            }
        }
        return OptExpression.create(new LogicalProjectOperator(newProjectMap), optExpression.getInputs());
    }

    private Projection rewriteScanProjection(Projection projection, MaterializedViewRule.RewriteContext context) {
        if (projection == null) {
            return null;
        }
        Map<ColumnRefOperator, ScalarOperator> replaceMap = new HashMap<>();
        replaceMap.put(context.queryColumnRef, context.mvColumnRef);

        Map<ColumnRefOperator, ScalarOperator> newColMap = new HashMap<>();
        boolean changed = false;
        for (Map.Entry<ColumnRefOperator, ScalarOperator> kv : projection.getColumnRefMap().entrySet()) {
            ScalarOperator expr = kv.getValue();
            if (!expr.getUsedColumns().contains(context.queryColumnRef)) {
                newColMap.put(kv.getKey(), expr);
                continue;
            }
            Optional<ScalarOperator> rewritten = MvColumnRefSubstitutor.substituteAndSyncOutput(
                    kv.getKey(), expr, replaceMap);
            if (!rewritten.isPresent()) {
                substitutionFailed = true;
                return projection;
            }
            newColMap.put(kv.getKey(), rewritten.get());
            changed = true;
        }
        if (!changed) {
            return projection;
        }
        return new Projection(newColMap, projection.getCommonSubOperatorMap(),
                projection.needReuseLambdaDependentExpr());
    }

    @Override
    public OptExpression visitLogicalTableScan(OptExpression optExpression,
                                               MaterializedViewRule.RewriteContext context) {
        if (!OperatorType.LOGICAL_OLAP_SCAN.equals(optExpression.getOp().getOpType())) {
            return optExpression;
        }

        LogicalOlapScanOperator olapScanOperator = (LogicalOlapScanOperator) optExpression.getOp();
        Projection newProjection = rewriteScanProjection(olapScanOperator.getProjection(), context);
        if (substitutionFailed) {
            return optExpression;
        }

        boolean colMetaChanged = olapScanOperator.getColRefToColumnMetaMap().containsKey(context.queryColumnRef);
        boolean projChanged = newProjection != olapScanOperator.getProjection();
        if (!colMetaChanged && !projChanged) {
            return optExpression;
        }

        LogicalOlapScanOperator.Builder builder = new LogicalOlapScanOperator.Builder();
        builder.withOperator(olapScanOperator);
        if (colMetaChanged) {
            Map<ColumnRefOperator, Column> columnRefOperatorColumnMap =
                    new HashMap<>(olapScanOperator.getColRefToColumnMetaMap());
            columnRefOperatorColumnMap.remove(context.queryColumnRef);
            columnRefOperatorColumnMap.put(context.mvColumnRef, context.mvColumn);
            builder.setColRefToColumnMetaMap(columnRefOperatorColumnMap);
        }
        if (projChanged) {
            builder.setProjection(newProjection);
        }
        return OptExpression.create(builder.build(), optExpression.getInputs());
    }

    private CallOperator rewriteAggregateFunc(ReplaceColumnRefRewriter replaceColumnRefRewriter,
                                              Column mvColumn,
                                              CallOperator queryAggFunc) {
        String functionName = queryAggFunc.getFnName();
        if (functionName.equals(FunctionSet.COUNT) && !queryAggFunc.isDistinct()) {
            CallOperator callOperator = new CallOperator(FunctionSet.SUM,
                    queryAggFunc.getType(),
                    queryAggFunc.getChildren(),
                    ExprUtils.getBuiltinFunction(FunctionSet.SUM, new Type[] {IntegerType.BIGINT}, IS_IDENTICAL));
            return (CallOperator) replaceColumnRefRewriter.rewrite(callOperator);
        } else if (functionName.equals(FunctionSet.SUM) && !queryAggFunc.isDistinct()) {
            CallOperator callOperator = new CallOperator(FunctionSet.SUM,
                    queryAggFunc.getType(),
                    queryAggFunc.getChildren(),
                    ScalarOperatorUtil.findSumFn(new Type[] {mvColumn.getType()}));
            return (CallOperator) replaceColumnRefRewriter.rewrite(callOperator);
        } else if (((functionName.equals(FunctionSet.COUNT) && queryAggFunc.isDistinct())
                || functionName.equals(FunctionSet.MULTI_DISTINCT_COUNT)) &&
                mvColumn.getAggregationType() == AggregateType.BITMAP_UNION) {
            CallOperator callOperator = new CallOperator(FunctionSet.BITMAP_UNION_COUNT,
                    queryAggFunc.getType(),
                    queryAggFunc.getChildren(),
                    ExprUtils.getBuiltinFunction(FunctionSet.BITMAP_UNION_COUNT, new Type[] {BitmapType.BITMAP},
                            IS_IDENTICAL));
            return (CallOperator) replaceColumnRefRewriter.rewrite(callOperator);
        } else if (functionName.equals(FunctionSet.BITMAP_AGG) &&
                mvColumn.getAggregationType() == AggregateType.BITMAP_UNION) {
            CallOperator callOperator = new CallOperator(FunctionSet.BITMAP_UNION,
                    queryAggFunc.getType(),
                    queryAggFunc.getChildren(),
                    ExprUtils.getBuiltinFunction(FunctionSet.BITMAP_UNION, new Type[] {BitmapType.BITMAP},
                            IS_IDENTICAL));
            return (CallOperator) replaceColumnRefRewriter.rewrite(callOperator);
        } else if (
                (functionName.equals(FunctionSet.NDV) || functionName.equals(FunctionSet.APPROX_COUNT_DISTINCT))
                        && mvColumn.getAggregationType() == AggregateType.HLL_UNION) {
            CallOperator callOperator = new CallOperator(FunctionSet.HLL_UNION_AGG,
                    queryAggFunc.getType(),
                    queryAggFunc.getChildren(),
                    ExprUtils.getBuiltinFunction(FunctionSet.HLL_UNION_AGG, new Type[] {HLLType.HLL}, IS_IDENTICAL));
            return (CallOperator) replaceColumnRefRewriter.rewrite(callOperator);
        } else if (functionName.equals(FunctionSet.PERCENTILE_APPROX) &&
                mvColumn.getAggregationType() == AggregateType.PERCENTILE_UNION) {

            ScalarOperator child = queryAggFunc.getChildren().get(0);
            if (child instanceof CastOperator) {
                child = child.getChild(0);
            }
            Preconditions.checkState(child instanceof ColumnRefOperator);
            CallOperator callOperator = new CallOperator(FunctionSet.PERCENTILE_UNION,
                    queryAggFunc.getType(),
                    Lists.newArrayList(child),
                    ExprUtils.getBuiltinFunction(FunctionSet.PERCENTILE_UNION,
                            new Type[] {PercentileType.PERCENTILE}, IS_IDENTICAL));
            return (CallOperator) replaceColumnRefRewriter.rewrite(callOperator);
        } else {
            return (CallOperator) replaceColumnRefRewriter.rewrite(queryAggFunc);
        }
    }

    @Override
    public OptExpression visitLogicalAggregate(OptExpression optExpression,
                                               MaterializedViewRule.RewriteContext context) {
        for (int childIdx = 0; childIdx < optExpression.arity(); ++childIdx) {
            optExpression.setChild(childIdx, rewrite(optExpression.inputAt(childIdx), context));
        }

        LogicalAggregationOperator aggregationOperator = (LogicalAggregationOperator) optExpression.getOp();

        Map<ColumnRefOperator, ScalarOperator> replaceMap = new HashMap<>();
        replaceMap.put(context.queryColumnRef, context.mvColumnRef);
        ReplaceColumnRefRewriter replaceColumnRefRewriter = new ReplaceColumnRefRewriter(replaceMap);

        Map<ColumnRefOperator, CallOperator> newAggMap = new HashMap<>(aggregationOperator.getAggregations());
        for (Map.Entry<ColumnRefOperator, CallOperator> kv : aggregationOperator.getAggregations().entrySet()) {
            ColumnRefOperator outputRef = kv.getKey();
            CallOperator queryAggFunc = kv.getValue();
            if (!queryAggFunc.getUsedColumns().contains(context.queryColumnRef)) {
                // Agg doesn't reference the substituted column — leave it alone.
                continue;
            }

            String functionName = queryAggFunc.getFnName();
            // For rollup-family-changing rewrites (BITMAP_UNION_COUNT, HLL_UNION_AGG,
            // PERCENTILE_APPROX), prefer rewriteAggregateFunc which constructs the
            // appropriate rollup CallOperator. Match by query function name + the
            // RewriteContext's expected agg name.
            if (functionName.equalsIgnoreCase(context.aggCall.getFnName())) {
                CallOperator newAggFunc = rewriteAggregateFunc(replaceColumnRefRewriter, context.mvColumn, queryAggFunc);
                if (newAggFunc != null) {
                    newAggMap.put(outputRef, newAggFunc);
                    outputRef.setType(newAggFunc.getType());
                    outputRef.setNullable(newAggFunc.isNullable());
                    continue;
                }
            }

            // Generic path: substitute children + re-derive types.
            Optional<ScalarOperator> coherent = MvColumnRefSubstitutor.substituteAndSyncOutput(
                    outputRef, queryAggFunc, replaceMap);
            if (!coherent.isPresent()) {
                substitutionFailed = true;
                return optExpression;
            }
            if (coherent.get() instanceof CallOperator) {
                newAggMap.put(outputRef, (CallOperator) coherent.get());
            }
        }
        return OptExpression.create(new LogicalAggregationOperator(
                aggregationOperator.getType(),
                aggregationOperator.getGroupingKeys(),
                aggregationOperator.getPartitionByColumns(),
                newAggMap,
                aggregationOperator.isSplit(),
                aggregationOperator.getLimit(),
                aggregationOperator.getPredicate()), optExpression.getInputs());
    }

    @Override
    public OptExpression visitLogicalTableFunction(OptExpression optExpression,
                                                   MaterializedViewRule.RewriteContext context) {
        for (int childIdx = 0; childIdx < optExpression.arity(); ++childIdx) {
            optExpression.setChild(childIdx, rewrite(optExpression.inputAt(childIdx), context));
        }

        List<ColumnRefOperator> newOuterColumns = Lists.newArrayList();
        LogicalTableFunctionOperator tableFunctionOperator = (LogicalTableFunctionOperator) optExpression.getOp();
        for (ColumnRefOperator col : tableFunctionOperator.getOuterColRefs()) {
            if (col.equals(context.queryColumnRef)) {
                newOuterColumns.add(context.mvColumnRef);
            } else {
                newOuterColumns.add(col);
            }
        }

        LogicalTableFunctionOperator newOperator = (new LogicalTableFunctionOperator.Builder())
                .withOperator(tableFunctionOperator)
                .setOuterColRefs(newOuterColumns)
                .build();

        return OptExpression.create(newOperator, optExpression.getInputs());
    }

    @Override
    public OptExpression visitLogicalJoin(OptExpression optExpression, MaterializedViewRule.RewriteContext context) {
        for (int childIdx = 0; childIdx < optExpression.arity(); ++childIdx) {
            optExpression.setChild(childIdx, rewrite(optExpression.inputAt(childIdx), context));
        }

        LogicalJoinOperator joinOperator = (LogicalJoinOperator) optExpression.getOp();
        return OptExpression.create(joinOperator, optExpression.getInputs());
    }
}
