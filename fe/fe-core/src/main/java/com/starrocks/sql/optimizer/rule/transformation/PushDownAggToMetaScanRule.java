// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.rule.transformation;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.analysis.Expr;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.Type;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalMetaScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.Collections;
import java.util.List;
import java.util.Map;

// For meta scan query: select max(a), min(a), dict_merge(a) from test_all_type [_META_]
// we need to push max, min, dict_merge aggregate function infos to meta scan node
// we will generate new columns: max_a, min_a, dict_merge_a, make meta scan known what meta info to collect
public class PushDownAggToMetaScanRule extends TransformationRule {
    public PushDownAggToMetaScanRule() {
        super(RuleType.TF_PUSH_DOWN_AGG_TO_META_SCAN,
                Pattern.create(OperatorType.LOGICAL_AGGR).
                        addChildren(Pattern.create(OperatorType.LOGICAL_PROJECT, OperatorType.LOGICAL_META_SCAN)));
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        LogicalMetaScanOperator metaScan = (LogicalMetaScanOperator) input.inputAt(0).inputAt(0).getOp();
        return metaScan.getAggColumnIdToNames().isEmpty();
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalAggregationOperator agg = (LogicalAggregationOperator) input.getOp();
        LogicalProjectOperator project = (LogicalProjectOperator) input.inputAt(0).getOp();
        LogicalMetaScanOperator metaScan = (LogicalMetaScanOperator) input.inputAt(0).inputAt(0).getOp();
        ColumnRefFactory columnRefFactory = context.getColumnRefFactory();

        Preconditions.checkState(agg.getGroupingKeys().isEmpty());

        Map<Integer, String> aggColumnIdToNames = Maps.newHashMap();
        Map<ColumnRefOperator, CallOperator> newAggCalls = Maps.newHashMap();
        Map<ColumnRefOperator, Column> newScanColumnRefs = Maps.newHashMap();

        Map<ColumnRefOperator, CallOperator> aggs = agg.getAggregations();
        for (Map.Entry<ColumnRefOperator, CallOperator> kv : aggs.entrySet()) {
            CallOperator aggCall = kv.getValue();
            ColumnRefSet usedColumns = aggCall.getUsedColumns();
            Preconditions.checkArgument(usedColumns.cardinality() == 1);
            ColumnRefOperator usedColumn = columnRefFactory.getColumnRef(usedColumns.getFirstId());

            String metaColumnName = aggCall.getFnName() + "_" + usedColumn.getName();

            Type columnType = aggCall.getType();
            // DictMerge meta aggregate function is special, need change the column type from
            // VARCHAR to ARRAY_VARCHAR
            if (aggCall.getFnName().equals(FunctionSet.DICT_MERGE)) {
                columnType = Type.ARRAY_VARCHAR;
            }

            ColumnRefOperator metaColumn = columnRefFactory.create(metaColumnName,
                    columnType, aggCall.isNullable());
            aggColumnIdToNames.put(metaColumn.getId(), metaColumnName);
            newScanColumnRefs.put(metaColumn, metaScan.getColRefToColumnMetaMap().get(usedColumn));

            Function aggFunction = aggCall.getFunction();
            // DictMerge meta aggregate function is special, need change the are type from
            // VARCHAR to ARRAY_VARCHAR
            if (aggCall.getFnName().equals(FunctionSet.DICT_MERGE)) {
                aggFunction = Expr.getBuiltinFunction(aggCall.getFnName(),
                        new Type[] {Type.ARRAY_VARCHAR}, Function.CompareMode.IS_IDENTICAL);
            }

            CallOperator newAggCall = new CallOperator(aggCall.getFnName(), aggCall.getType(),
                    Collections.singletonList(metaColumn), aggFunction);
            newAggCalls.put(kv.getKey(), newAggCall);
        }

        LogicalMetaScanOperator newMetaScan =
                new LogicalMetaScanOperator(metaScan.getTable(), newScanColumnRefs, aggColumnIdToNames);

        OptExpression newProject = new OptExpression(project);
        newProject.getInputs().add(OptExpression.create(newMetaScan));

        LogicalAggregationOperator newAggOperator = new LogicalAggregationOperator(
                agg.getType(), agg.getGroupingKeys(), newAggCalls);
        return Lists.newArrayList(OptExpression.create(newAggOperator, newProject));
    }
}