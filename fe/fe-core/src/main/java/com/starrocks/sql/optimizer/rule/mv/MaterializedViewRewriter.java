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
import com.starrocks.analysis.Expr;
import com.starrocks.catalog.AggregateType;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.Type;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
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
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperatorUtil;
import com.starrocks.sql.optimizer.rewrite.ReplaceColumnRefRewriter;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.starrocks.catalog.Function.CompareMode.IS_IDENTICAL;

public class MaterializedViewRewriter extends OptExpressionVisitor<OptExpression, MaterializedViewRule.RewriteContext> {

    public MaterializedViewRewriter() {
    }

    public OptExpression rewrite(OptExpression optExpression, MaterializedViewRule.RewriteContext context) {
        return optExpression.getOp().accept(this, optExpression, context);
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
        return OptExpression.create(new LogicalProjectOperator(newProjectMap), optExpression.getInputs());
    }

    @Override
    public OptExpression visitLogicalTableScan(OptExpression optExpression,
                                               MaterializedViewRule.RewriteContext context) {
        if (!OperatorType.LOGICAL_OLAP_SCAN.equals(optExpression.getOp().getOpType())) {
            return optExpression;
        }

        LogicalOlapScanOperator olapScanOperator = (LogicalOlapScanOperator) optExpression.getOp();

        if (olapScanOperator.getColRefToColumnMetaMap().containsKey(context.queryColumnRef)) {
            Map<ColumnRefOperator, Column> columnRefOperatorColumnMap =
                    new HashMap<>(olapScanOperator.getColRefToColumnMetaMap());
            columnRefOperatorColumnMap.remove(context.queryColumnRef);
            columnRefOperatorColumnMap.put(context.mvColumnRef, context.mvColumn);

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
                    Expr.getBuiltinFunction(FunctionSet.SUM, new Type[] {Type.BIGINT}, IS_IDENTICAL));
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
                    Expr.getBuiltinFunction(FunctionSet.BITMAP_UNION_COUNT, new Type[] {Type.BITMAP},
                            IS_IDENTICAL));
            return (CallOperator) replaceColumnRefRewriter.rewrite(callOperator);
        } else if (functionName.equals(FunctionSet.BITMAP_AGG) &&
                mvColumn.getAggregationType() == AggregateType.BITMAP_UNION) {
            CallOperator callOperator = new CallOperator(FunctionSet.BITMAP_UNION,
                    queryAggFunc.getType(),
                    queryAggFunc.getChildren(),
                    Expr.getBuiltinFunction(FunctionSet.BITMAP_UNION, new Type[] {Type.BITMAP},
                            IS_IDENTICAL));
            return (CallOperator) replaceColumnRefRewriter.rewrite(callOperator);
        } else if (
                (functionName.equals(FunctionSet.NDV) || functionName.equals(FunctionSet.APPROX_COUNT_DISTINCT))
                        && mvColumn.getAggregationType() == AggregateType.HLL_UNION) {
            CallOperator callOperator = new CallOperator(FunctionSet.HLL_UNION_AGG,
                    queryAggFunc.getType(),
                    queryAggFunc.getChildren(),
                    Expr.getBuiltinFunction(FunctionSet.HLL_UNION_AGG, new Type[] {Type.HLL}, IS_IDENTICAL));
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
                    Expr.getBuiltinFunction(FunctionSet.PERCENTILE_UNION,
                            new Type[] {Type.PERCENTILE}, IS_IDENTICAL));
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
            CallOperator queryAggFunc = kv.getValue();
            if (queryAggFunc.getUsedColumns().isEmpty()) {
                break;
            }

            String functionName = queryAggFunc.getFnName();
            if (functionName.equals(context.aggCall.getFnName())
                    && queryAggFunc.getUsedColumns().getFirstId() == context.queryColumnRef.getId()) {
                CallOperator newAggFunc = rewriteAggregateFunc(replaceColumnRefRewriter, context.mvColumn, queryAggFunc);
                if (newAggFunc != null) {
                    newAggMap.put(kv.getKey(), newAggFunc);
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
                aggregationOperator.getSingleDistinctFunctionPos(),
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
