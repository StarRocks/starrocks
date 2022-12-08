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

import java.util.HashMap;
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

            LogicalOlapScanOperator newScanOperator = new LogicalOlapScanOperator(
                    olapScanOperator.getTable(),
                    columnRefOperatorColumnMap,
                    olapScanOperator.getColumnMetaToColRefMap(),
                    olapScanOperator.getDistributionSpec(),
                    olapScanOperator.getLimit(),
                    olapScanOperator.getPredicate(),
                    olapScanOperator.getSelectedIndexId(),
                    olapScanOperator.getSelectedPartitionId(),
                    olapScanOperator.getPartitionNames(),
                    olapScanOperator.getSelectedTabletId(),
                    olapScanOperator.getHintsTabletIds());

            optExpression = OptExpression.create(newScanOperator, optExpression.getInputs());
        }
        return optExpression;
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
            String functionName = kv.getValue().getFnName();
            if (kv.getValue().getUsedColumns().isEmpty()) {
                break;
            }
            if (functionName.equals(context.aggCall.getFnName())
                    && kv.getValue().getUsedColumns().getFirstId() == context.queryColumnRef.getId()) {
                if (kv.getValue().getFnName().equals(FunctionSet.COUNT) && !kv.getValue().isDistinct()) {
                    CallOperator callOperator = new CallOperator(FunctionSet.SUM,
                            kv.getValue().getType(),
                            kv.getValue().getChildren(),
                            Expr.getBuiltinFunction(FunctionSet.SUM, new Type[] {Type.BIGINT}, IS_IDENTICAL));

                    newAggMap.put(kv.getKey(), (CallOperator) replaceColumnRefRewriter.rewrite(callOperator));
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
                    newAggMap.put(kv.getKey(), (CallOperator) replaceColumnRefRewriter.rewrite(callOperator));
                    break;
                } else if (
                        (functionName.equals(FunctionSet.NDV) || functionName.equals(FunctionSet.APPROX_COUNT_DISTINCT))
                                && context.mvColumn.getAggregationType() == AggregateType.HLL_UNION) {
                    CallOperator callOperator = new CallOperator(FunctionSet.HLL_UNION_AGG,
                            kv.getValue().getType(),
                            kv.getValue().getChildren(),
                            Expr.getBuiltinFunction(FunctionSet.HLL_UNION_AGG, new Type[] {Type.HLL}, IS_IDENTICAL));
                    newAggMap.put(kv.getKey(), (CallOperator) replaceColumnRefRewriter.rewrite(callOperator));
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
                    newAggMap.put(kv.getKey(), (CallOperator) replaceColumnRefRewriter.rewrite(callOperator));
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
        return OptExpression.create(tableFunctionOperator, optExpression.getInputs());
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
