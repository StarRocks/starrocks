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
import com.starrocks.type.BitmapType;
import com.starrocks.type.HLLType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.PercentileType;
import com.starrocks.type.Type;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.starrocks.catalog.Function.CompareMode.IS_IDENTICAL;

/**
 * Rewrites a query plan to read a sync materialized view (rollup) index.
 *
 * <p>The unit of rewrite is a group of {@link MaterializedViewRule.RewriteContext} that all share the
 * same base query column (their {@code queryColumnRef} is identical). All aggregates on that base column
 * are rewritten together, so the base column is removed from the scan exactly once and every rollup column
 * derived from it is added in the same pass. Handling them one-by-one used to break when two aggregates
 * referenced the same base column (e.g. {@code min(c)} and {@code max(c)}): the first context removed the
 * base column from the scan/project, after which the sibling context could no longer find it and its rollup
 * column was never projected.
 */
public class MaterializedViewRewriter
        extends OptExpressionVisitor<OptExpression, List<MaterializedViewRule.RewriteContext>> {

    public MaterializedViewRewriter() {
    }

    public OptExpression rewrite(OptExpression optExpression, List<MaterializedViewRule.RewriteContext> contexts) {
        return optExpression.getOp().accept(this, optExpression, contexts);
    }

    // Convenience overload for rewriting a single context (one aggregate on a base column).
    public OptExpression rewrite(OptExpression optExpression, MaterializedViewRule.RewriteContext context) {
        return rewrite(optExpression, Lists.newArrayList(context));
    }

    public static boolean isCaseWhenScalarOperator(ScalarOperator operator) {
        if (operator instanceof CaseWhenOperator) {
            return true;
        }

        return operator instanceof CallOperator &&
                FunctionSet.IF.equalsIgnoreCase(((CallOperator) operator).getFnName());
    }

    @Override
    public OptExpression visit(OptExpression optExpression, List<MaterializedViewRule.RewriteContext> contexts) {
        for (int childIdx = 0; childIdx < optExpression.arity(); ++childIdx) {
            optExpression.setChild(childIdx, rewrite(optExpression.inputAt(childIdx), contexts));
        }

        return OptExpression.create(optExpression.getOp(), optExpression.getInputs());
    }

    @Override
    public OptExpression visitLogicalProject(OptExpression optExpression,
                                             List<MaterializedViewRule.RewriteContext> contexts) {
        for (int childIdx = 0; childIdx < optExpression.arity(); ++childIdx) {
            optExpression.setChild(childIdx, rewrite(optExpression.inputAt(childIdx), contexts));
        }

        LogicalProjectOperator projectOperator = (LogicalProjectOperator) optExpression.getOp();
        ColumnRefOperator queryColumnRef = contexts.get(0).queryColumnRef;

        Map<ColumnRefOperator, ScalarOperator> newProjectMap = Maps.newHashMap();
        for (Map.Entry<ColumnRefOperator, ScalarOperator> kv : projectOperator.getColumnRefMap().entrySet()) {
            ColumnRefOperator queryColRef = kv.getKey();
            ScalarOperator queryScalarOperator = kv.getValue();
            if (queryScalarOperator.getUsedColumns().contains(queryColumnRef)) {
                if (queryScalarOperator instanceof ColumnRefOperator) {
                    // The base column is projected as-is; project every rollup column derived from it so all
                    // sibling aggregates (e.g. min(c) and max(c)) can read their own column above.
                    for (MaterializedViewRule.RewriteContext context : contexts) {
                        newProjectMap.put(context.mvColumnRef, context.mvColumnRef);
                    }
                } else if (isCaseWhenScalarOperator(queryScalarOperator)) {
                    // rewrite query column ref into mv agg column ref,
                    // eg: sum(case when a > 1 then b else 0 end), rewrite to sum(case when mv_column > 1 then b else 0 end)
                    for (MaterializedViewRule.RewriteContext context : contexts) {
                        Map<ColumnRefOperator, ScalarOperator> replaceMap = new HashMap<>();
                        replaceMap.put(context.queryColumnRef, context.mvColumnRef);
                        ReplaceColumnRefRewriter replaceColumnRefRewriter = new ReplaceColumnRefRewriter(replaceMap);
                        newProjectMap.put(queryColRef, replaceColumnRefRewriter.rewrite(kv.getValue()));
                    }
                } else {
                    // eg: bitmap_union(to_bitmap(a)), still rewrite to bitmap_union(to_bitmap(a))
                    for (MaterializedViewRule.RewriteContext context : contexts) {
                        newProjectMap.put(queryColRef, context.mvColumnRef);
                    }
                }
            } else {
                newProjectMap.put(queryColRef, queryScalarOperator);
            }
        }
        return OptExpression.create(new LogicalProjectOperator(newProjectMap), optExpression.getInputs());
    }

    @Override
    public OptExpression visitLogicalTableScan(OptExpression optExpression,
                                               List<MaterializedViewRule.RewriteContext> contexts) {
        if (!OperatorType.LOGICAL_OLAP_SCAN.equals(optExpression.getOp().getOpType())) {
            return optExpression;
        }

        LogicalOlapScanOperator olapScanOperator = (LogicalOlapScanOperator) optExpression.getOp();
        ColumnRefOperator queryColumnRef = contexts.get(0).queryColumnRef;

        if (olapScanOperator.getColRefToColumnMetaMap().containsKey(queryColumnRef)) {
            Map<ColumnRefOperator, Column> columnRefOperatorColumnMap =
                    new HashMap<>(olapScanOperator.getColRefToColumnMetaMap());
            // Replace the base column with every rollup column derived from it in a single pass, so two
            // aggregates on the same base column (e.g. min(c), max(c)) both get their column projected.
            columnRefOperatorColumnMap.remove(queryColumnRef);
            for (MaterializedViewRule.RewriteContext context : contexts) {
                columnRefOperatorColumnMap.put(context.mvColumnRef, context.mvColumn);
            }

            LogicalOlapScanOperator.Builder builder = new LogicalOlapScanOperator.Builder();
            LogicalOlapScanOperator newScanOperator = builder.withOperator(olapScanOperator)
                    .setColRefToColumnMetaMap(columnRefOperatorColumnMap).build();
            optExpression = OptExpression.create(newScanOperator, optExpression.getInputs());
        }
        return optExpression;
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
                                               List<MaterializedViewRule.RewriteContext> contexts) {
        for (int childIdx = 0; childIdx < optExpression.arity(); ++childIdx) {
            optExpression.setChild(childIdx, rewrite(optExpression.inputAt(childIdx), contexts));
        }

        LogicalAggregationOperator aggregationOperator = (LogicalAggregationOperator) optExpression.getOp();

        Map<ColumnRefOperator, CallOperator> newAggMap = new HashMap<>(aggregationOperator.getAggregations());
        for (Map.Entry<ColumnRefOperator, CallOperator> kv : aggregationOperator.getAggregations().entrySet()) {
            CallOperator queryAggFunc = kv.getValue();
            if (queryAggFunc.getUsedColumns().isEmpty()) {
                continue;
            }

            // Each aggregate is rewritten by the context whose function name and base column match it; the
            // group can hold several (e.g. min and max on the same base column), so every aggregate is visited.
            for (MaterializedViewRule.RewriteContext context : contexts) {
                if (queryAggFunc.getFnName().equals(context.aggCall.getFnName())
                        && queryAggFunc.getUsedColumns().getFirstId() == context.queryColumnRef.getId()) {
                    Map<ColumnRefOperator, ScalarOperator> replaceMap = new HashMap<>();
                    replaceMap.put(context.queryColumnRef, context.mvColumnRef);
                    ReplaceColumnRefRewriter replaceColumnRefRewriter = new ReplaceColumnRefRewriter(replaceMap);
                    CallOperator newAggFunc =
                            rewriteAggregateFunc(replaceColumnRefRewriter, context.mvColumn, queryAggFunc);
                    if (newAggFunc != null) {
                        newAggMap.put(kv.getKey(), newAggFunc);
                    }
                    break;
                }
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
                                                   List<MaterializedViewRule.RewriteContext> contexts) {
        for (int childIdx = 0; childIdx < optExpression.arity(); ++childIdx) {
            optExpression.setChild(childIdx, rewrite(optExpression.inputAt(childIdx), contexts));
        }

        ColumnRefOperator queryColumnRef = contexts.get(0).queryColumnRef;
        List<ColumnRefOperator> newOuterColumns = Lists.newArrayList();
        LogicalTableFunctionOperator tableFunctionOperator = (LogicalTableFunctionOperator) optExpression.getOp();
        for (ColumnRefOperator col : tableFunctionOperator.getOuterColRefs()) {
            if (col.equals(queryColumnRef)) {
                // The base column may be consumed by several aggregates (e.g. min(c), max(c)); forward
                // every rollup column derived from it so all of them stay available above.
                for (MaterializedViewRule.RewriteContext context : contexts) {
                    newOuterColumns.add(context.mvColumnRef);
                }
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
    public OptExpression visitLogicalJoin(OptExpression optExpression,
                                          List<MaterializedViewRule.RewriteContext> contexts) {
        for (int childIdx = 0; childIdx < optExpression.arity(); ++childIdx) {
            optExpression.setChild(childIdx, rewrite(optExpression.inputAt(childIdx), contexts));
        }

        LogicalJoinOperator joinOperator = (LogicalJoinOperator) optExpression.getOp();
        return OptExpression.create(joinOperator, optExpression.getInputs());
    }
}
