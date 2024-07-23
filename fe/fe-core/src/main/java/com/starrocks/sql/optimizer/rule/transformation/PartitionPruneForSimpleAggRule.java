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
import com.starrocks.catalog.AggregateFunction;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.ListPartitionInfo;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.Type;
import com.starrocks.common.Pair;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.Ordering;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalTopNOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.rule.RuleType;
import org.apache.commons.collections.CollectionUtils;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class PartitionPruneForSimpleAggRule extends TransformationRule {

    public PartitionPruneForSimpleAggRule() {
        super(RuleType.TF_PRUNE_PARTITION_FOR_SIMPLE_AGG, Pattern.create(OperatorType.LOGICAL_AGGR)
                .addChildren(Pattern.create(OperatorType.LOGICAL_PROJECT, OperatorType.LOGICAL_OLAP_SCAN)));
    }

    @Override
    public boolean check(final OptExpression input, OptimizerContext context) {
        //        if (!context.getSessionVariable().isEnableRewriteSimpleAggToMetaScan()) {
        //            return false;
        //        }
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
                    }
                    return false;
                }
        );
        return allValid;
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

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalAggregationOperator aggregationOperator = (LogicalAggregationOperator) input.getOp();
        LogicalOlapScanOperator logicalOlapScanOperator = (LogicalOlapScanOperator) input.getOp();
        OlapTable table = (OlapTable) logicalOlapScanOperator.getTable();

        LogicalScanOperator scanOperator = optimizeWithPartitionPrune(logicalOlapScanOperator, table,
                checkMinMax(aggregationOperator));

        return Lists.newArrayList(OptExpression.create(scanOperator, input.getInputs()));
    }

    /**
     * For DUPLICATE table:
     * 1. choose MAX partition for max(ds) function
     * 2. choose MIN partition for min(ds) function
     */
    private LogicalScanOperator optimizeWithPartitionPrune(LogicalOlapScanOperator scanOperator,
                                                           OlapTable table,
                                                           Pair<Boolean, Boolean> hasMinMax) {
        List<Partition> nonEmpty = table.getNonEmptyPartitions();
        Set<Long> nonEmptyPartitionIds = nonEmpty.stream().map(Partition::getId).collect(Collectors.toSet());

        PartitionInfo partitionInfo = table.getPartitionInfo();
        List<Long> sorted = partitionInfo.getSortedPartitions();
        sorted.retainAll(nonEmptyPartitionIds);

        List<Long> pruned = Lists.newArrayList();
        if (CollectionUtils.isEmpty(sorted)) {
            return null;
        }

        if (hasMinMax.first) {
            pruned.add(sorted.get(0));
        }
        if (hasMinMax.second) {
            pruned.add(sorted.get(sorted.size() - 1));
        }

        return new LogicalOlapScanOperator.Builder()
                .withOperator(scanOperator)
                .setSelectedPartitionId(pruned)
                .build();
    }

    /**
     * For List Partition, we can evaluate the MAX(pt) based on the partition values
     */
    private Pair<ConstantOperator, ConstantOperator> optimizeWithPartitionValues(OlapTable table,
                                                                                 Pair<Boolean, Boolean> hasMinMax) {
        List<Partition> nonEmpty = table.getNonEmptyPartitions();
        Set<Long> nonEmptyPartitionIds = nonEmpty.stream().map(Partition::getId).collect(Collectors.toSet());
        ListPartitionInfo partitionInfo = (ListPartitionInfo) table.getPartitionInfo();
        List<Long> sorted = partitionInfo.getSortedPartitions();
        sorted.retainAll(nonEmptyPartitionIds);

        if (hasMinMax.first) {
            long minPartition = sorted.get(0);
            partitionInfo.getPartitionListExpr(minPartition);
        }
    }

    /**
     * For PRIMARY-KEY Table and RANGE PARTITION, we cannot apply the PartitionPrune && PartitionValues, so transform
     * it into a TopN query
     */
    private LogicalTopNOperator optimizeWithTop1(LogicalAggregationOperator aggregation,
                                  OlapTable table,
                                  Pair<Boolean, Boolean> hasMinMax) {
        if (hasMinMax.first && hasMinMax.second) {
            return null;
        }
        final long limit = 1;
        final long offset = 0;
        List<CallOperator> aggregations = Lists.newArrayList(aggregation.getAggregations().values());
        Preconditions.checkState(aggregations.size() == 1, "must have 1 aggregations but " + aggregations.size());
        CallOperator agg = aggregations.get(0);
        ColumnRefOperator columnRefOperator = agg.getColumnRefs().get(0);

        List<Ordering> ordering = Lists.newArrayList(new Ordering(columnRefOperator, hasMinMax.first, false));
        return new LogicalTopNOperator(ordering, limit, offset);
    }
}
