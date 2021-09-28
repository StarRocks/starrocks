// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.sql.optimizer.rule.mv;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.analysis.Expr;
import com.starrocks.catalog.AggregateType;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.Type;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.starrocks.catalog.Function.CompareMode.IS_IDENTICAL;

public abstract class MVAggRewrite {
    // Use mv column instead of query column
    protected static void rewriteOlapScanOperator(
            OptExpression optExpression,
            LogicalOlapScanOperator olapScanOperator,
            MaterializedViewRule.RewriteContext rewriteContext) {
        List<ColumnRefOperator> outputColumns = new ArrayList<>(olapScanOperator.getOutputColumns());
        outputColumns.remove(rewriteContext.queryColumnRef);
        outputColumns.add(rewriteContext.mvColumnRef);

        Map<ColumnRefOperator, Column> columnRefOperatorColumnMap =
                new HashMap<>(olapScanOperator.getColRefToColumnMetaMap());
        columnRefOperatorColumnMap.remove(rewriteContext.queryColumnRef);
        columnRefOperatorColumnMap.put(rewriteContext.mvColumnRef, rewriteContext.mvColumn);

        LogicalOlapScanOperator newScanOperator = new LogicalOlapScanOperator(
                olapScanOperator.getTable(),
                outputColumns,
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
    // query: count(a) && mv: count(a) -> sum(a)
    // query: count(distinct a) && mv: bitmap_union(a) -> bitmap_union_count(a)
    // query: multi_distinct_count(a) && mv: bitmap_union(a) -> bitmap_union_count(a)
    // query: ndv(a) && mv: hll_union(a) -> hll_union_agg(a)
    // query: approx_count_distinct(a) && mv: hll_union(a) -> hll_union_agg(a)
    // query: percentile_approx(a) && mv: percentile_union(a) -> percentile_union(a)
    protected void rewriteAggOperator(OptExpression optExpression,
                                      LogicalAggregationOperator aggregationOperator,
                                      CallOperator agg,
                                      ColumnRefOperator aggUsedColumn,
                                      Column mvColumn) {

        Map<ColumnRefOperator, CallOperator> newAggMap = Maps.newHashMap();
        for (Map.Entry<ColumnRefOperator, CallOperator> kv : aggregationOperator.getAggregations().entrySet()) {
            String functionName = kv.getValue().getFnName();
            if (functionName.equals(agg.getFnName())
                    && kv.getValue().getUsedColumns().getFirstId() == aggUsedColumn.getId()) {
                if (kv.getValue().getFnName().equals(FunctionSet.COUNT) &&
                        !kv.getValue().isDistinct()) {
                    newAggMap.put(kv.getKey(), getSumFunction(kv.getValue()));
                    break;
                } else if (functionName.equals(FunctionSet.COUNT) &&
                        kv.getValue().isDistinct() &&
                        mvColumn.getAggregationType() == AggregateType.BITMAP_UNION) {
                    newAggMap.put(kv.getKey(), getBitmapUnionCountFunction(kv.getValue()));
                    break;
                } else if (functionName.equals(FunctionSet.MULTI_DISTINCT_COUNT) &&
                        mvColumn.getAggregationType() == AggregateType.BITMAP_UNION) {
                    newAggMap.put(kv.getKey(), getBitmapUnionCountFunction(kv.getValue()));
                    break;
                } else if (functionName.equals(FunctionSet.NDV) &&
                        mvColumn.getAggregationType() == AggregateType.HLL_UNION) {
                    newAggMap.put(kv.getKey(), getHLLUnionAggFunction(kv.getValue()));
                    break;
                } else if (functionName.equals(FunctionSet.APPROX_COUNT_DISTINCT) &&
                        mvColumn.getAggregationType() == AggregateType.HLL_UNION) {
                    newAggMap.put(kv.getKey(), getHLLUnionAggFunction(kv.getValue()));
                    break;
                } else if (functionName.equals(FunctionSet.PERCENTILE_APPROX) &&
                        mvColumn.getAggregationType() == AggregateType.PERCENTILE_UNION) {
                    newAggMap.put(kv.getKey(), getPercentileFunction(kv.getValue()));
                    break;
                }
            }
        }
        optExpression.setChild(0, OptExpression.create(new LogicalAggregationOperator(
                aggregationOperator.getType(),
                aggregationOperator.getGroupingKeys(),
                aggregationOperator.getPartitionByColumns(),
                newAggMap,
                aggregationOperator.isSplit(),
                aggregationOperator.getSingleDistinctFunctionPos(),
                aggregationOperator.getLimit(),
                aggregationOperator.getPredicate()), optExpression.inputAt(0).getInputs()));
    }

    private CallOperator getHLLUnionAggFunction(CallOperator oldAgg) {
        Function fn = Expr.getBuiltinFunction(FunctionSet.HLL_UNION_AGG,
                new Type[] {Type.HLL}, IS_IDENTICAL);
        return new CallOperator(FunctionSet.HLL_UNION_AGG,
                oldAgg.getType(),
                oldAgg.getChildren(),
                fn);
    }

    private CallOperator getBitmapUnionCountFunction(CallOperator oldAgg) {
        Function fn = Expr.getBuiltinFunction(FunctionSet.BITMAP_UNION_COUNT,
                new Type[] {Type.BITMAP}, IS_IDENTICAL);
        return new CallOperator(FunctionSet.BITMAP_UNION_COUNT,
                oldAgg.getType(),
                oldAgg.getChildren(),
                fn);
    }

    private CallOperator getSumFunction(CallOperator oldAgg) {
        Function fn = Expr.getBuiltinFunction(FunctionSet.SUM,
                new Type[] {Type.BIGINT}, IS_IDENTICAL);
        return new CallOperator(FunctionSet.SUM,
                oldAgg.getType(),
                oldAgg.getChildren(),
                fn);
    }

    private CallOperator getPercentileFunction(CallOperator oldAgg) {
        Function fn = Expr.getBuiltinFunction(FunctionSet.PERCENTILE_UNION,
                new Type[] {Type.PERCENTILE}, IS_IDENTICAL);
        ScalarOperator child = oldAgg.getChildren().get(0);
        if (child instanceof CastOperator) {
            child = child.getChild(0);
        }
        Preconditions.checkState(child.isColumnRef());
        return new CallOperator(FunctionSet.PERCENTILE_UNION,
                oldAgg.getType(),
                Lists.newArrayList(child),
                fn);
    }
}
