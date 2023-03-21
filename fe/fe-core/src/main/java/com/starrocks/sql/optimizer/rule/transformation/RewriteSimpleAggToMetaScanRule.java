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
import com.starrocks.catalog.AggregateFunction;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Type;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalMetaScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.Collections;
import java.util.List;
import java.util.Map;

// for a simple min/max/count aggregation query like
// 'select min(c1),max(c2),count(*),count(not-null column) from olap_table',
// we can use MetaScan directly to avoid reading a large amount of data.
public class RewriteSimpleAggToMetaScanRule extends TransformationRule {
    public RewriteSimpleAggToMetaScanRule() {
        super(RuleType.TF_REWRITE_SIMPLE_AGG, Pattern.create(OperatorType.LOGICAL_AGGR)
                .addChildren(Pattern.create(OperatorType.LOGICAL_PROJECT, OperatorType.LOGICAL_OLAP_SCAN)));
    }

    private OptExpression buildAggMetaScanOperator(LogicalAggregationOperator aggregationOperator,
                                                   LogicalOlapScanOperator scanOperator,
                                                   OptimizerContext context) {
        ColumnRefFactory columnRefFactory = context.getColumnRefFactory();
        Map<ColumnRefOperator, CallOperator> aggs = aggregationOperator.getAggregations();

        Map<Integer, String> aggColumnIdToNames = Maps.newHashMap();
        Map<ColumnRefOperator, CallOperator> newAggCalls = Maps.newHashMap();
        Map<ColumnRefOperator, Column> newScanColumnRefs = Maps.newHashMap();
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
                usedColumn = scanOperator.getOutputColumns().get(0);
            }

            String metaColumnName = aggCall.getFnName() + "_" + usedColumn.getName();
            Type columnType = aggCall.getType();

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
            Column c = scanOperator.getColRefToColumnMetaMap().get(usedColumn);
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
        LogicalMetaScanOperator newMetaScan = new LogicalMetaScanOperator(scanOperator.getTable(),
                newScanColumnRefs, aggColumnIdToNames);
        LogicalAggregationOperator newAggOperator = new LogicalAggregationOperator(aggregationOperator.getType(),
                aggregationOperator.getGroupingKeys(), newAggCalls);

        newAggOperator.setProjection(aggregationOperator.getProjection());
        OptExpression optExpression = OptExpression.create(newAggOperator);
        optExpression.getInputs().add(OptExpression.create(newMetaScan));
        return optExpression;
    }

    @Override
    public boolean check(final OptExpression input, OptimizerContext context) {
        if (!context.getSessionVariable().isEnableRewriteSimpleAggToMetaScan()) {
            return false;
        }
        LogicalAggregationOperator aggregationOperator = (LogicalAggregationOperator) input.getOp();
        LogicalScanOperator scanOperator = (LogicalScanOperator) input.getInputs().get(0).getInputs().get(0).getOp();
        OlapTable table = (OlapTable) scanOperator.getTable();
        // we can only apply this rule to the queries met all the following conditions:
        // 1. query on DUPLICATE_KEY table
        // 2. no group by key
        // 3. no `having` condition or other filters
        // 4. no limit
        // 5. only contain MIN/MAX/COUNT agg functions, no distinct
        // 6. all arguments to agg functions are primitive columns
        // 7. no expr in arguments to agg functions
        // 8. all agg columns have zonemap index and are not null
        // 9. no deletion happens
        if (table.getKeysType() != KeysType.DUP_KEYS) {
            return false;
        }
        // no deletion
        if (table.hasDelete()) {
            return false;
        }
        // no limit
        if (scanOperator.getLimit() != -1) {
            return false;
        }
        // no filter
        if (scanOperator.getPredicate() != null) {
            return false;
        }
        List<ColumnRefOperator> groupingKeys = aggregationOperator.getGroupingKeys();
        if (groupingKeys != null && !groupingKeys.isEmpty()) {
            return false;
        }
        if (aggregationOperator.getPredicate() != null) {
            return false;
        }

        boolean allValid = aggregationOperator.getAggregations().values().stream().allMatch(
                aggregator -> {
                    AggregateFunction aggregateFunction = (AggregateFunction) aggregator.getFunction();
                    String functionName = aggregateFunction.functionName();
                    ColumnRefSet usedColumns = aggregator.getUsedColumns();
                    if (functionName.equals(FunctionSet.MAX) || functionName.equals(FunctionSet.MIN)) {
                        if (usedColumns.size() != 1) {
                            return false;
                        }
                        ColumnRefOperator usedColumn =
                                context.getColumnRefFactory().getColumnRef(usedColumns.getFirstId());
                        Column column = scanOperator.getColRefToColumnMetaMap().get(usedColumn);
                        if (column == null || column.isAllowNull()) {
                            // this is not a primitive column on table or it is nullable
                            return false;
                        }
                        // min/max column should have zonemap index
                        Type type = aggregator.getType();
                        return !(type.isStringType() || type.isComplexType());
                    } else if (functionName.equals(FunctionSet.COUNT) && !aggregator.isDistinct()) {
                        if (usedColumns.size() == 1) {
                            ColumnRefOperator usedColumn =
                                    context.getColumnRefFactory().getColumnRef(usedColumns.getFirstId());
                            Column column = scanOperator.getColRefToColumnMetaMap().get(usedColumn);
                            if (column == null || column.isAllowNull()) {
                                // this is not a primitive column on table or it is nullable
                                return false;
                            }
                            return true;
                        } else if (usedColumns.isEmpty()) {
                            List<ScalarOperator> arguments = aggregator.getArguments();
                            if (arguments.isEmpty()) {
                                // count()/count(*)
                                return true;
                            } else if (arguments.size() == 1 && !arguments.get(0).isConstantNull()) {
                                // count(non-null constant)
                                return true;
                            }
                        }
                        return false;
                    }
                    return false;
                }
        );
        return allValid;
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalAggregationOperator aggregationOperator = (LogicalAggregationOperator) input.getOp();
        LogicalScanOperator scanOperator = (LogicalScanOperator) input.getInputs().get(0).getInputs().get(0).getOp();
        OptExpression result = buildAggMetaScanOperator(aggregationOperator,
                (LogicalOlapScanOperator) scanOperator, context);
        return Lists.newArrayList(result);
    }
}
