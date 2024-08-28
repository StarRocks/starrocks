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
import com.starrocks.catalog.AggregateFunction;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.ListPartitionInfo;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.Table;
import com.starrocks.common.Pair;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.Ordering;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalTopNOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalValuesOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.RuleType;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.NotImplementedException;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Optimization rule for MIN/MAX PARTITION_COLUMN:
 * 1. Partition Prune: only choose the largest partition for MAX(col)
 * 2. Partition Values: evaluate the MAX(col) in optimizer for LIST-PARTITION
 * 3. TopN: transform the MAX(col) into TopN query
 */
public class PartitionColumnMinMaxRewriteRule extends TransformationRule {

    public PartitionColumnMinMaxRewriteRule() {
        super(RuleType.TF_PARTITION_COLUMN_MINMAX, Pattern.create(OperatorType.LOGICAL_AGGR)
                .addChildren(Pattern.create(OperatorType.LOGICAL_PROJECT, OperatorType.LOGICAL_OLAP_SCAN)));
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        LogicalAggregationOperator aggregationOperator = input.getOp().cast();
        LogicalOlapScanOperator scanOperator = input.getInputs().get(0).getInputs().get(0).getOp().cast();
        Table table = scanOperator.getTable();
        return context.getSessionVariable().isEnableRewritePartitionColumnMinMax()
                && checkTableType(table)
                && checkSpecifiedLocation(scanOperator)
                && checkMinMaxAggregation(aggregationOperator, scanOperator, context);
    }

    private boolean checkTableType(Table table) {
        return table.isNativeTableOrMaterializedView()
                && ((OlapTable) table).isPartitionedTable()
                && ((OlapTable) table).getPartitionInfo().getPartitionColumnsSize() <= 1;
    }

    private boolean checkSpecifiedLocation(LogicalOlapScanOperator scanOperator) {
        return (scanOperator.getPartitionNames() == null ||
                CollectionUtils.isEmpty(scanOperator.getPartitionNames().getPartitionNames()))
                && CollectionUtils.isEmpty(scanOperator.getHintsTabletIds())
                && CollectionUtils.isEmpty(scanOperator.getHintsReplicaIds());
    }

    /**
     * Check simple aggregation functions like MIN/MAX
     * 1. No GROUP-BY
     * 2. No HAVING
     * 3. Only MAX or MAX
     * 4. MIN/MAX only refers partition-column
     */
    private boolean checkMinMaxAggregation(LogicalAggregationOperator aggregationOperator,
                                           LogicalScanOperator scanOperator,
                                           OptimizerContext context) {
        boolean simpleAgg = aggregationOperator.getAggregations().entrySet().stream().allMatch(
                entry -> {
                    CallOperator aggregator = entry.getValue();
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
                        if (column == null) {
                            // not a colum on table
                            return false;
                        }
                        OlapTable olapTable = (OlapTable) scanOperator.getTable();
                        if (!olapTable.getPartitionColumns().contains(column)) {
                            return false;
                        }
                        return true;
                    } else {
                        return false;
                    }
                }
        );
        if (!simpleAgg) {
            return false;
        }
        List<ColumnRefOperator> groupingKeys = aggregationOperator.getGroupingKeys();
        if (groupingKeys != null && !groupingKeys.isEmpty()) {
            return false;
        }
        if (aggregationOperator.getPredicate() != null) {
            return false;
        }
        if (scanOperator.getLimit() != -1) {
            return false;
        }
        return true;
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalAggregationOperator aggregationOperator = input.getOp().cast();
        LogicalOlapScanOperator scanOperator = input.getInputs().get(0).getInputs().get(0).getOp().cast();
        OlapTable table = (OlapTable) scanOperator.getTable();
        Pair<Boolean, Boolean> minMax = checkMinMax(aggregationOperator);

        try {
            OptExpression result = null;
            if (checkRewritePartitionValues(aggregationOperator, scanOperator, table)) {
                result = optimizeWithPartitionValues(aggregationOperator, table, minMax);
            } else if (checkPartitionPrune(aggregationOperator, scanOperator, table)) {
                result = optimizeWithPartitionPrune(input, aggregationOperator, scanOperator, table, minMax);
            } else if (checkRewriteTopN(aggregationOperator, scanOperator, table, minMax)) {
                result = optimizeWithTopN(input, aggregationOperator, scanOperator, table, minMax);
            }
            if (result != null) {
                return Lists.newArrayList(result);
            }
        } catch (NotImplementedException ignore) {
            // Some not-supported partition tables
        }
        return Lists.newArrayList();
    }

    private Pair<Boolean, Boolean> checkMinMax(LogicalAggregationOperator operator) {
        boolean hasMin = false;
        boolean hasMax = false;
        for (var function : operator.getAggregations().values()) {
            String name = function.getFunction().functionName();
            if (name.equalsIgnoreCase(FunctionSet.MIN)) {
                hasMin = true;
            }
            if (name.equalsIgnoreCase(FunctionSet.MAX)) {
                hasMax = true;
            }
        }
        return Pair.create(hasMin, hasMax);
    }

    /**
     * Apply this optimization if:
     * 1. query on DUPLICATE_KEY table
     * 2. no limit
     * 3. no deletion happens
     * 4. no filter
     */
    private boolean checkPartitionPrune(LogicalAggregationOperator aggregationOperator,
                                        LogicalScanOperator scanOperator,
                                        OlapTable table) {
        if (table.getKeysType() != KeysType.DUP_KEYS) {
            return false;
        }
        if (table.hasDelete()) {
            return false;
        }
        if (scanOperator.getLimit() != -1) {
            return false;
        }
        if (scanOperator.getPredicate() != null) {
            return false;
        }
        return true;
    }

    /**
     * For DUPLICATE table:
     * 1. choose MAX partition for max(ds) function
     * 2. choose MIN partition for min(ds) function
     */
    private OptExpression optimizeWithPartitionPrune(OptExpression optExpression,
                                                     LogicalAggregationOperator aggregationOperator,
                                                     LogicalOlapScanOperator scanOperator,
                                                     OlapTable table,
                                                     Pair<Boolean, Boolean> hasMinMax) {
        List<Partition> nonEmpty = table.getNonEmptyPartitions();
        Set<Long> nonEmptyPartitionIds = nonEmpty.stream().map(Partition::getId).collect(Collectors.toSet());
        PartitionInfo partitionInfo = table.getPartitionInfo();

        List<Long> pruned = Lists.newArrayList();
        if (hasMinMax.first) {
            List<Long> sorted = partitionInfo.getSortedPartitions(true);
            sorted.retainAll(nonEmptyPartitionIds);
            if (CollectionUtils.isEmpty(sorted)) {
                return null;
            }
            pruned.add(sorted.get(0));
        }

        if (hasMinMax.second) {
            List<Long> sorted = partitionInfo.getSortedPartitions(false);
            sorted.retainAll(nonEmptyPartitionIds);
            if (CollectionUtils.isEmpty(sorted)) {
                return null;
            }
            pruned.add(sorted.get(0));
        }

        LogicalOlapScanOperator scan = new LogicalOlapScanOperator.Builder()
                .withOperator(scanOperator)
                .setSelectedPartitionId(pruned)
                .build();

        return OptExpression.create(aggregationOperator, OptExpression.create(scan));
    }

    /**
     * Apply this optimization if:
     * 1. LIST-PARTITIONED table
     * 2. DUPLICATED TABLE, no delete and no filter
     */
    private boolean checkRewritePartitionValues(LogicalAggregationOperator aggregationOperator,
                                                LogicalScanOperator scanOperator,
                                                OlapTable olapTable) {
        if (!checkPartitionPrune(aggregationOperator, scanOperator, olapTable)) {
            return false;
        }
        if (!olapTable.getPartitionInfo().isListPartition()) {
            return false;
        }
        ListPartitionInfo partitionInfo = (ListPartitionInfo) olapTable.getPartitionInfo();
        if (partitionInfo.getPartitionColumnsSize() > 1) {
            return false;
        }
        return true;
    }

    /**
     * For List Partition, we can evaluate the MAX(pt) based on the partition values
     */
    private OptExpression optimizeWithPartitionValues(LogicalAggregationOperator aggregationOperator,
                                                      OlapTable table,
                                                      Pair<Boolean, Boolean> hasMinMax) {
        ListPartitionInfo partitionInfo = (ListPartitionInfo) table.getPartitionInfo();
        List<Partition> nonEmpty = table.getNonEmptyPartitions();
        Set<Long> nonEmptyPartitionIds = nonEmpty.stream().map(Partition::getId).collect(Collectors.toSet());

        List<ScalarOperator> valueRow = Lists.newArrayList();
        List<ColumnRefOperator> columns = Lists.newArrayList();
        for (var entry : aggregationOperator.getAggregations().entrySet()) {
            if (isMin(entry.getValue())) {
                List<Long> sorted = partitionInfo.getSortedPartitions(true);
                sorted.retainAll(nonEmptyPartitionIds);
                if (CollectionUtils.isEmpty(sorted)) {
                    return null;
                }
                long minPartition = sorted.get(0);
                ListPartitionInfo.ListPartitionCell partitionValues = partitionInfo.getPartitionListExpr(minPartition);
                Preconditions.checkState(!partitionValues.isEmpty());
                ConstantOperator minValue = partitionValues.minValue().toConstant();
                valueRow.add(minValue);
                columns.add(entry.getKey());
            } else if (isMax(entry.getValue())) {
                List<Long> sorted = partitionInfo.getSortedPartitions(false);
                sorted.retainAll(nonEmptyPartitionIds);
                if (CollectionUtils.isEmpty(sorted)) {
                    return null;
                }
                long maxPartition = sorted.get(0);
                ListPartitionInfo.ListPartitionCell partitionValues = partitionInfo.getPartitionListExpr(maxPartition);
                Preconditions.checkState(!partitionValues.isEmpty());
                ConstantOperator maxValue = partitionValues.maxValue().toConstant();
                valueRow.add(maxValue);
                columns.add(entry.getKey());
            }
        }
        LogicalValuesOperator values = new LogicalValuesOperator.Builder()
                .setRows(List.of(valueRow))
                .setColumnRefSet(columns)
                .build();
        return OptExpression.create(values);
    }

    private static boolean isMax(CallOperator call) {
        return call.getFunction().functionName().equalsIgnoreCase(FunctionSet.MAX);
    }

    private static boolean isMin(CallOperator call) {
        return call.getFunction().functionName().equalsIgnoreCase(FunctionSet.MIN);
    }

    /**
     * Apply this optimization if:
     * 1. Any table type
     * 2. Either MIN or MAX
     * 3. The column has ZONEMAP index, which is translated into PRIMITIVE TYPE
     */
    private boolean checkRewriteTopN(LogicalAggregationOperator aggregationOperator,
                                     LogicalScanOperator scanOperator,
                                     OlapTable olapTable,
                                     Pair<Boolean, Boolean> minMax) {
        if (minMax.first && minMax.second) {
            return false;
        }
        boolean lackZoneMap = aggregationOperator.getAggregations().values().stream().anyMatch(x ->
                x.getType().isComplexType() || x.getType().isJsonType()
        );
        if (lackZoneMap) {
            return false;
        }
        return true;
    }

    /**
     * For PRIMARY-KEY Table and RANGE PARTITION, we cannot apply the PartitionPrune && PartitionValues, so transform
     * it into a TopN query
     */
    private OptExpression optimizeWithTopN(OptExpression optExpression,
                                           LogicalAggregationOperator aggregation,
                                           LogicalScanOperator scanOperator,
                                           OlapTable table,
                                           Pair<Boolean, Boolean> hasMinMax) {
        final long limit = 1;
        final long offset = 0;

        List<Map.Entry<ColumnRefOperator, CallOperator>> entries =
                Lists.newArrayList(aggregation.getAggregations().entrySet());
        CallOperator agg = entries.get(0).getValue();
        ColumnRefOperator columnRefOperator = agg.getColumnRefs().get(0);

        boolean asc = hasMinMax.first;
        List<Ordering> ordering = Lists.newArrayList(new Ordering(columnRefOperator, asc, false));
        LogicalTopNOperator topn = new LogicalTopNOperator(ordering, limit, offset);

        Map<ColumnRefOperator, ScalarOperator> columnRefMap = Maps.newHashMap();
        columnRefMap.put(entries.get(0).getKey(), columnRefOperator);
        LogicalProjectOperator project = new LogicalProjectOperator(columnRefMap);

        return OptExpression.create(project, OptExpression.create(topn, optExpression.getInputs().get(0).getInputs()));
    }
}
