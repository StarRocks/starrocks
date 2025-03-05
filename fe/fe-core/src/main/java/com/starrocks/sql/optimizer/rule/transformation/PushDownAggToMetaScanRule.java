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

        for (Map.Entry<ColumnRefOperator, CallOperator> kv : aggs.entrySet()) {
            CallOperator aggCall = kv.getValue();
            ColumnRefOperator usedColumn;

            String metaColumnName;
            if (aggCall.getFnName().equals(FunctionSet.COUNT) && aggCall.getChildren().isEmpty()) {
                usedColumn = metaScan.getOutputColumns().get(0);
                metaColumnName = "rows_" + usedColumn.getName();
            } else {
                ColumnRefSet usedColumns = aggCall.getUsedColumns();
                Preconditions.checkArgument(usedColumns.cardinality() == 1);
                usedColumn = columnRefFactory.getColumnRef(usedColumns.getFirstId());
                metaColumnName = aggCall.getFnName() + "_" + usedColumn.getName();
            }

            Type columnType = aggCall.getType();
            // DictMerge meta aggregate function is special, need change the column type from
            // VARCHAR to ARRAY_VARCHAR
            if (aggCall.getFnName().equals(FunctionSet.DICT_MERGE)) {
                columnType = Type.ARRAY_VARCHAR;
            }

            ColumnRefOperator metaColumn = columnRefFactory.create(metaColumnName, columnType, true);
            aggColumnIdToNames.put(metaColumn.getId(), metaColumnName);

            Column c = metaScan.getColRefToColumnMetaMap().get(usedColumn);
            Column copiedColumn = c.deepCopy();
            if (aggCall.getFnName().equals(FunctionSet.COUNT)) {
                // this variable is introduced to solve compatibility issues,
                // see more details in the description of https://github.com/StarRocks/starrocks/pull/17619
                copiedColumn.setType(Type.BIGINT);
            }
            copiedColumn.setIsAllowNull(true);
            newScanColumnRefs.put(metaColumn, copiedColumn);

            // DictMerge meta aggregate function is special, need change their types from
            // VARCHAR to ARRAY_VARCHAR
            if (aggCall.getFnName().equals(FunctionSet.DICT_MERGE)) {
                Function aggFunction = Expr.getBuiltinFunction(aggCall.getFnName(),
                        new Type[] {Type.ARRAY_VARCHAR, Type.INT}, Function.CompareMode.IS_IDENTICAL);

                newAggCalls.put(kv.getKey(),
                        new CallOperator(aggCall.getFnName(), aggCall.getType(),
                                List.of(metaColumn, aggCall.getChild(1)), aggFunction));
            } else if (aggCall.getFnName().equals(FunctionSet.COUNT)) {
                // rewrite count to sum
                Function aggFunction = Expr.getBuiltinFunction(FunctionSet.SUM, new Type[] {Type.BIGINT},
                        Function.CompareMode.IS_IDENTICAL);
                newAggCalls.put(kv.getKey(),
                        new CallOperator(FunctionSet.SUM, Type.BIGINT, List.of(metaColumn), aggFunction));
            } else {
                newAggCalls.put(kv.getKey(),
                        new CallOperator(aggCall.getFnName(), aggCall.getType(), List.of(metaColumn),
                                aggCall.getFunction()));
            }
        }

        LogicalMetaScanOperator newMetaScan = LogicalMetaScanOperator.builder()
                .withOperator(metaScan)
                .setColRefToColumnMetaMap(newScanColumnRefs)
                .setAggColumnIdToNames(aggColumnIdToNames)
                .build();

        LogicalAggregationOperator newAggOperator = new LogicalAggregationOperator(
                agg.getType(), agg.getGroupingKeys(), newAggCalls);
        // all used columns from aggCalls are from newMetaScan, we can remove the old project directly.
        return Lists.newArrayList(OptExpression.create(newAggOperator, OptExpression.create(newMetaScan)));
    }
}