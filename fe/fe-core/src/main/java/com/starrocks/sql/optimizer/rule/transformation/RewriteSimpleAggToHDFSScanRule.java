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
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalHiveScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalIcebergScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class RewriteSimpleAggToHDFSScanRule extends TransformationRule {
    public static final RewriteSimpleAggToHDFSScanRule HIVE_SCAN =
            new RewriteSimpleAggToHDFSScanRule(OperatorType.LOGICAL_HIVE_SCAN);
    public static final RewriteSimpleAggToHDFSScanRule ICEBERG_SCAN =
            new RewriteSimpleAggToHDFSScanRule(OperatorType.LOGICAL_ICEBERG_SCAN);
    final OperatorType scanOperatorType;

    public RewriteSimpleAggToHDFSScanRule(OperatorType logicalOperatorType) {
        super(RuleType.TF_REWRITE_SIMPLE_AGG, Pattern.create(OperatorType.LOGICAL_AGGR)
                .addChildren(Pattern.create(OperatorType.LOGICAL_PROJECT, logicalOperatorType)));
        scanOperatorType = logicalOperatorType;
    }

    private OptExpression buildAggScanOperator(LogicalAggregationOperator aggregationOperator,
                                               LogicalScanOperator scanOperator,
                                               OptimizerContext context) {
        ColumnRefFactory columnRefFactory = context.getColumnRefFactory();
        Map<ColumnRefOperator, CallOperator> aggs = aggregationOperator.getAggregations();

        Map<ColumnRefOperator, CallOperator> newAggCalls = Maps.newHashMap();
        Map<ColumnRefOperator, Column> newScanColumnRefs = Maps.newHashMap();

        // select out partition columns.
        int tableRelationId = -1;
        for (ColumnRefOperator c : scanOperator.getColRefToColumnMetaMap().keySet()) {
            int relationId = columnRefFactory.getRelationId(c.getId());
            if (tableRelationId == -1) {
                tableRelationId = relationId;
            } else {
                Preconditions.checkState(tableRelationId == relationId, "Table relation id is different across columns");
            }
            if (scanOperator.getPartitionColumns().contains(c.getName())) {
                newScanColumnRefs.put(c, scanOperator.getColRefToColumnMetaMap().get(c));
            }
        }
        Preconditions.checkState(tableRelationId != -1, "Can not find table relation id in scan operator");

        ColumnRefOperator placeholderColumn = null;

        for (Map.Entry<ColumnRefOperator, CallOperator> kv : aggs.entrySet()) {
            CallOperator aggCall = kv.getValue();
            // ___count___
            String metaColumnName = "___" + aggCall.getFnName() + "___";
            Type columnType = aggCall.getType();

            if (placeholderColumn == null) {
                Column c = new Column();
                c.setName(metaColumnName);
                c.setIsAllowNull(true);
                placeholderColumn = columnRefFactory.create(metaColumnName, columnType, aggCall.isNullable());
                columnRefFactory.updateColumnToRelationIds(placeholderColumn.getId(), tableRelationId);
                columnRefFactory.updateColumnRefToColumns(placeholderColumn, c, scanOperator.getTable());
                newScanColumnRefs.put(placeholderColumn, c);
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
                    Collections.singletonList(placeholderColumn), aggFunction);
            newAggCalls.put(kv.getKey(), newAggCall);
        }

        Map<Column, ColumnRefOperator> newScanColumnMeta = Maps.newHashMap();
        for (Map.Entry<ColumnRefOperator, Column> c : newScanColumnRefs.entrySet()) {
            newScanColumnMeta.put(c.getValue(), c.getKey());
        }

        LogicalScanOperator newMetaScan = null;

        if (scanOperator instanceof LogicalHiveScanOperator) {
            newMetaScan = new LogicalHiveScanOperator(scanOperator.getTable(),
                    newScanColumnRefs, newScanColumnMeta, scanOperator.getLimit(), scanOperator.getPredicate());
        } else if (scanOperator instanceof LogicalIcebergScanOperator) {
            newMetaScan = new LogicalIcebergScanOperator(scanOperator.getTable(),
                    newScanColumnRefs, newScanColumnMeta, scanOperator.getLimit(), scanOperator.getPredicate());
        } else {
            throw new IllegalStateException("Unknown scan operator: " + scanOperator);
        }
        try {
            newMetaScan.setScanOperatorPredicates(scanOperator.getScanOperatorPredicates());
        } catch (AnalysisException e) {
            throw new IllegalStateException("Set scan operator predicates", e);
        }
        LogicalAggregationOperator newAggOperator = new LogicalAggregationOperator(aggregationOperator.getType(),
                aggregationOperator.getGroupingKeys(), newAggCalls);

        newAggOperator.setProjection(aggregationOperator.getProjection());
        OptExpression optExpression = OptExpression.create(newAggOperator);
        optExpression.getInputs().add(OptExpression.create(newMetaScan));
        return optExpression;
    }

    @Override
    public boolean check(final OptExpression input, OptimizerContext context) {
        if (!context.getSessionVariable().isEnableRewriteSimpleAggToHdfsScan()) {
            return false;
        }
        LogicalAggregationOperator aggregationOperator = (LogicalAggregationOperator) input.getOp();
        LogicalScanOperator scanOperator = (LogicalScanOperator) input.getInputs().get(0).getInputs().get(0).getOp();

        // no limit
        if (scanOperator.getLimit() != -1) {
            return false;
        }

        // filter only involved with partition keys.
        if (scanOperator.getPredicate() != null) {
            if (!scanOperator.getPartitionColumns()
                    .containsAll(scanOperator.getPredicate().getColumnRefs().stream().map(x -> x.getName()).collect(
                            Collectors.toList()))) {
                return false;
            }
        }

        // all group by keys are partition keys.
        List<ColumnRefOperator> groupingKeys = aggregationOperator.getGroupingKeys();
        if (!scanOperator.getPartitionColumns()
                .containsAll(groupingKeys.stream().map(x -> x.getName()).collect(Collectors.toList()))) {
            return false;
        }
        // TODO(yanz): not quite sure why this limitation !
        if (aggregationOperator.getPredicate() != null) {
            return false;
        }

        if (scanOperatorType == OperatorType.LOGICAL_ICEBERG_SCAN) {
            IcebergTable icebergTable = (IcebergTable) scanOperator.getTable();
            if (!scanOperator.getPartitionColumns().isEmpty() && !icebergTable.isAllPartitionColumnsAlwaysIdentity()) {
                return false;
            }
        }

        boolean allValid = aggregationOperator.getAggregations().values().stream().allMatch(
                aggregator -> {
                    AggregateFunction aggregateFunction = (AggregateFunction) aggregator.getFunction();
                    String functionName = aggregateFunction.functionName();
                    ColumnRefSet usedColumns = aggregator.getUsedColumns();

                    if (functionName.equals(FunctionSet.COUNT) && !aggregator.isDistinct() && usedColumns.isEmpty()) {
                        List<ScalarOperator> arguments = aggregator.getArguments();
                        if (arguments.isEmpty()) {
                            // count()/count(*)
                            return true;
                        } else if (arguments.size() == 1 && !arguments.get(0).isConstantNull()) {
                            // count(non-null constant)
                            return true;
                        }
                        // TODO(yanz): not quite sure when this case happens.
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
        OptExpression result = buildAggScanOperator(aggregationOperator, scanOperator, context);
        return Lists.newArrayList(result);
    }
}
