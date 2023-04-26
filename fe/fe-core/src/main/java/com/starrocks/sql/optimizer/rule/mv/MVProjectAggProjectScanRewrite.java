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
import com.starrocks.analysis.Expr;
import com.starrocks.catalog.AggregateType;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.Type;
import com.starrocks.common.Pair;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ReplaceColumnRefRewriter;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.starrocks.catalog.Function.CompareMode.IS_IDENTICAL;

// Rewrite project -> agg -> project -> scan logic operator by RewriteContext
// Currently, only used for percentile_union mv.
// we need rewrite percentile_approx to percentile_approx_raw(percentile_union)
// TODO(kks): Remove this class if we support percentile_union_count aggregate function later
public class MVProjectAggProjectScanRewrite {
    private static final MVProjectAggProjectScanRewrite INSTANCE = new MVProjectAggProjectScanRewrite();

    public static MVProjectAggProjectScanRewrite getInstance() {
        return INSTANCE;
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
            LogicalProjectOperator bellowProject = (LogicalProjectOperator) input.inputAt(0).inputAt(0).getOp();
            LogicalOlapScanOperator scanOperator =
                    (LogicalOlapScanOperator) input.inputAt(0).inputAt(0).inputAt(0).getOp();

            if (factory.getRelationId(scanOperator.getOutputColumns().get(0).getId()) != relationId) {
                return;
            }

            rewriteOlapScanOperator(input.inputAt(0).inputAt(0), scanOperator, rewriteContexts);
            for (MaterializedViewRule.RewriteContext context : rewriteContexts) {
                ColumnRefOperator projectColumn =
                        rewriteProjectOperator(bellowProject, context.queryColumnRef, context.mvColumnRef);
                Pair<ColumnRefOperator, ColumnRefOperator> aggColumn =
                        rewriteAggOperator(input, context.aggCall, projectColumn, context.mvColumn, factory);
                rewriteTopProjectOperator((LogicalAggregationOperator) input.inputAt(0).getOp(), topProject, aggColumn,
                        context.aggCall);
            }
        }
    }

    private void rewriteTopProjectOperator(LogicalAggregationOperator agg, LogicalProjectOperator project,
                                           Pair<ColumnRefOperator, ColumnRefOperator> aggUsedColumn,
                                           CallOperator queryAgg) {
        CallOperator percentileApproxRaw = new CallOperator(FunctionSet.PERCENTILE_APPROX_RAW,
                Type.DOUBLE, Lists.newArrayList(aggUsedColumn.second, queryAgg.getChild(1)),
                Expr.getBuiltinFunction(
                        FunctionSet.PERCENTILE_APPROX_RAW,
                        new Type[] {Type.PERCENTILE, Type.DOUBLE},
                        Function.CompareMode.IS_IDENTICAL));

        Map<ColumnRefOperator, ScalarOperator> rewriteMap = new HashMap<>();
        rewriteMap.put(aggUsedColumn.first, percentileApproxRaw);
        ReplaceColumnRefRewriter rewriter = new ReplaceColumnRefRewriter(rewriteMap);

        for (Map.Entry<ColumnRefOperator, ScalarOperator> kv : project.getColumnRefMap().entrySet()) {
            kv.setValue(rewriter.rewrite(kv.getValue()));
        }
    }

    // Use mv column instead of query column
    protected static void rewriteOlapScanOperator(OptExpression optExpression, LogicalOlapScanOperator olapScanOperator,
                                                  List<MaterializedViewRule.RewriteContext> rewriteContexts) {
        Map<ColumnRefOperator, Column> columnRefOperatorColumnMap =
                new HashMap<>(olapScanOperator.getColRefToColumnMetaMap());

        for (MaterializedViewRule.RewriteContext rewriteContext : rewriteContexts) {
            columnRefOperatorColumnMap.remove(rewriteContext.queryColumnRef);
            columnRefOperatorColumnMap.put(rewriteContext.mvColumnRef, rewriteContext.mvColumn);
        }

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

        optExpression.setChild(0, OptExpression.create(newScanOperator));
    }

    // Use mv column instead of query column
    protected ColumnRefOperator rewriteProjectOperator(LogicalProjectOperator projectOperator,
                                                       ColumnRefOperator baseColumnRef,
                                                       ColumnRefOperator mvColumnRef) {
        for (Map.Entry<ColumnRefOperator, ScalarOperator> kv : projectOperator.getColumnRefMap().entrySet()) {
            if (kv.getValue().getUsedColumns().contains(baseColumnRef)) {
                kv.setValue(mvColumnRef);
                return kv.getKey();
            }
        }
        Preconditions.checkState(false, "shouldn't reach here");
        return null;
    }

    // TODO(kks): refactor this method later
    // query: percentile_approx(a) && mv: percentile_union(a) -> percentile_union(a)
    protected Pair<ColumnRefOperator, ColumnRefOperator> rewriteAggOperator(OptExpression optExpression,
                                                                            CallOperator agg,
                                                                            ColumnRefOperator aggUsedColumn,
                                                                            Column mvColumn,
                                                                            ColumnRefFactory factory) {
        LogicalAggregationOperator aggOperator = (LogicalAggregationOperator) optExpression.getInputs().get(0).getOp();
        Map<ColumnRefOperator, CallOperator> newAggMap = new HashMap<>(aggOperator.getAggregations());

        for (Map.Entry<ColumnRefOperator, CallOperator> kv : newAggMap.entrySet()) {
            String functionName = kv.getValue().getFnName();
            if (functionName.equals(agg.getFnName()) &&
                    kv.getValue().getUsedColumns().getFirstId() == aggUsedColumn.getId()) {
                if (functionName.equals(FunctionSet.PERCENTILE_APPROX) &&
                        mvColumn.getAggregationType() == AggregateType.PERCENTILE_UNION) {

                    kv.setValue(getPercentileFunction(kv.getValue()));

                    newAggMap.remove(kv.getKey());
                    ColumnRefOperator aggColumnRef =
                            factory.create(kv.getKey(), kv.getKey().getType(), kv.getKey().isNullable());
                    newAggMap.put(aggColumnRef, kv.getValue());

                    optExpression.setChild(0, OptExpression.create(new LogicalAggregationOperator(
                            aggOperator.getType(),
                            aggOperator.getGroupingKeys(),
                            aggOperator.getPartitionByColumns(),
                            newAggMap,
                            aggOperator.isSplit(),
                            aggOperator.getSingleDistinctFunctionPos(),
                            aggOperator.getLimit(),
                            aggOperator.getPredicate()), optExpression.inputAt(0).getInputs()));
                    return new Pair<>(kv.getKey(), aggColumnRef);
                }
            }
        }
        return null;
    }

    private CallOperator getPercentileFunction(CallOperator oldAgg) {
        Function fn = Expr.getBuiltinFunction(FunctionSet.PERCENTILE_UNION,
                new Type[] {Type.PERCENTILE}, IS_IDENTICAL);
        ScalarOperator child = oldAgg.getChildren().get(0);
        if (child instanceof CastOperator) {
            child = child.getChild(0);
        }
        Preconditions.checkState(child.isColumnRef());
        return new CallOperator(FunctionSet.PERCENTILE_UNION, oldAgg.getType(), Lists.newArrayList(child), fn);
    }
}
