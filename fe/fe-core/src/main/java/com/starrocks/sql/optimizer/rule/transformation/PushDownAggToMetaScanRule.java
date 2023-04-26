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
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.RuleType;
import org.apache.commons.collections4.CollectionUtils;

import java.util.Collections;
import java.util.List;
import java.util.Map;

// For meta scan query: select max(a), min(a), dict_merge(a) from test_all_type [_META_]
// we need to push max, min, dict_merge aggregate function info to meta scan node
// we will generate new columns: max_a, min_a, dict_merge_a, make meta scan known what meta info to collect
public class PushDownAggToMetaScanRule extends TransformationRule {
    public PushDownAggToMetaScanRule() {
        super(RuleType.TF_PUSH_DOWN_AGG_TO_META_SCAN,
                Pattern.create(OperatorType.LOGICAL_AGGR).
                        addChildren(Pattern.create(OperatorType.LOGICAL_PROJECT, OperatorType.LOGICAL_META_SCAN)));
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        LogicalAggregationOperator agg = (LogicalAggregationOperator) input.getOp();
        LogicalProjectOperator projectOperator = (LogicalProjectOperator) input.inputAt(0).getOp();
        if (CollectionUtils.isNotEmpty(agg.getGroupingKeys())) {
            return false;
        }
        for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : projectOperator.getColumnRefMap().entrySet()) {
            if (!entry.getKey().equals(entry.getValue())) {
                return false;
            }
        }

        for (CallOperator aggCall : agg.getAggregations().values()) {
            String aggFuncName = aggCall.getFnName();
            if (!aggFuncName.equalsIgnoreCase(FunctionSet.DICT_MERGE)
                    && !aggFuncName.equalsIgnoreCase(FunctionSet.MAX)
                    && !aggFuncName.equalsIgnoreCase(FunctionSet.MIN)
                    && !aggFuncName.equalsIgnoreCase(FunctionSet.COUNT)) {
                return false;
            }
        }

        LogicalMetaScanOperator metaScan = (LogicalMetaScanOperator) input.inputAt(0).inputAt(0).getOp();
        return metaScan.getAggColumnIdToNames().isEmpty();
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalAggregationOperator agg = (LogicalAggregationOperator) input.getOp();
        LogicalMetaScanOperator metaScan = (LogicalMetaScanOperator) input.inputAt(0).inputAt(0).getOp();
        ColumnRefFactory columnRefFactory = context.getColumnRefFactory();

        Preconditions.checkState(agg.getGroupingKeys().isEmpty());

        Map<Integer, String> aggColumnIdToNames = Maps.newHashMap();
        Map<ColumnRefOperator, CallOperator> newAggCalls = Maps.newHashMap();
        Map<ColumnRefOperator, Column> newScanColumnRefs = Maps.newHashMap();

        Map<ColumnRefOperator, CallOperator> aggs = agg.getAggregations();
        // this variable is introduced to solve compatibility issues,
        // see more details in the description of https://github.com/StarRocks/starrocks/pull/17619
        boolean hasCountAgg = aggs.values().stream().anyMatch(aggCall -> aggCall.getFnName().equals(FunctionSet.COUNT));

        ColumnRefOperator countPlaceHolderColumn = null;
        for (Map.Entry<ColumnRefOperator, CallOperator> kv : aggs.entrySet()) {
            CallOperator aggCall = kv.getValue();
            ColumnRefOperator usedColumn;
            if (!aggCall.getFnName().equals(FunctionSet.COUNT)) {
                ColumnRefSet usedColumns = aggCall.getUsedColumns();
                Preconditions.checkArgument(usedColumns.cardinality() == 1);
                usedColumn = columnRefFactory.getColumnRef(usedColumns.getFirstId());
            } else {
                // for count, just use the first output column as a placeholder, BE won't read this column.
                usedColumn = metaScan.getOutputColumns().get(0);
            }
            String metaColumnName = aggCall.getFnName() + "_" + usedColumn.getName();

            Type columnType = aggCall.getType();
            // DictMerge meta aggregate function is special, need change the column type from
            // VARCHAR to ARRAY_VARCHAR
            if (aggCall.getFnName().equals(FunctionSet.DICT_MERGE)) {
                columnType = Type.ARRAY_VARCHAR;
            }

            ColumnRefOperator metaColumn;
            if (aggCall.getFnName().equals(FunctionSet.COUNT)) {
                if (countPlaceHolderColumn != null) {
                    metaColumn = countPlaceHolderColumn;
                } else {
                    metaColumn = columnRefFactory.create(metaColumnName, columnType, aggCall.isNullable());
                    countPlaceHolderColumn = metaColumn;
                }
            } else {
                metaColumn = columnRefFactory.create(metaColumnName, columnType, aggCall.isNullable());
            }

            aggColumnIdToNames.put(metaColumn.getId(), metaColumnName);
            Column c = metaScan.getColRefToColumnMetaMap().get(usedColumn);
            if (hasCountAgg) {
                Column copiedColumn = new Column(c);
                copiedColumn.setIsAllowNull(true);
                newScanColumnRefs.put(metaColumn, copiedColumn);
            } else {
                newScanColumnRefs.put(metaColumn, c);
            }

            Function aggFunction = aggCall.getFunction();
            String newAggFnName = aggCall.getFnName();
            Type newAggReturnType = aggCall.getType();
            // DictMerge meta aggregate function is special, need change their types from
            // VARCHAR to ARRAY_VARCHAR
            if (aggCall.getFnName().equals(FunctionSet.DICT_MERGE)) {
                aggFunction = Expr.getBuiltinFunction(aggCall.getFnName(),
                        new Type[] {Type.ARRAY_VARCHAR}, Function.CompareMode.IS_IDENTICAL);
            }

            // rewrite count to sum
            if (aggCall.getFnName().equals(FunctionSet.COUNT)) {
                aggFunction = Expr.getBuiltinFunction(FunctionSet.SUM,
                        new Type[] {Type.BIGINT}, Function.CompareMode.IS_IDENTICAL);
                newAggFnName = FunctionSet.SUM;
                newAggReturnType = Type.BIGINT;
            }
            CallOperator newAggCall = new CallOperator(newAggFnName, newAggReturnType,
                    Collections.singletonList(metaColumn), aggFunction);
            newAggCalls.put(kv.getKey(), newAggCall);
        }

        LogicalMetaScanOperator newMetaScan =
                new LogicalMetaScanOperator(metaScan.getTable(), newScanColumnRefs, aggColumnIdToNames);

        LogicalAggregationOperator newAggOperator = new LogicalAggregationOperator(
                agg.getType(), agg.getGroupingKeys(), newAggCalls);
        // all used columns from aggCalls are from newMetaScan, we can remove the old project directly.
        return Lists.newArrayList(OptExpression.create(newAggOperator, OptExpression.create(newMetaScan)));
    }
}