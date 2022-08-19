// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.optimizer.rule.transformation;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.MaterializedIndexMeta;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.RangePartitionInfo;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class RemoveAggregationFromAggTable extends TransformationRule {
    public RemoveAggregationFromAggTable() {
        super(RuleType.TF_REMOVE_AGGREGATION_BY_AGG_TABLE,
                Pattern.create(OperatorType.LOGICAL_AGGR).addChildren(Pattern.create(
                        OperatorType.LOGICAL_OLAP_SCAN)));
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        OptExpression scanExpression = input.getInputs().get(0);
        Preconditions.checkState(scanExpression.getOp() instanceof LogicalOlapScanOperator);
        LogicalOlapScanOperator scanOperator = (LogicalOlapScanOperator) scanExpression.getOp();
        OlapTable olapTable = (OlapTable) scanOperator.getTable();

        MaterializedIndexMeta materializedIndexMeta = olapTable.getIndexMetaByIndexId(scanOperator.getSelectedIndexId());
        // must be Aggregation
        if (!materializedIndexMeta.getKeysType().isAggregationFamily()) {
            return false;
        }

        // group by keys contain partition columns and distribution columns
        PartitionInfo partitionInfo = olapTable.getPartitionInfo();
        List<String> partitionColumnNames = Lists.newArrayList();
        if (partitionInfo instanceof RangePartitionInfo) {
            RangePartitionInfo rangePartitionInfo = (RangePartitionInfo) partitionInfo;
            partitionColumnNames.addAll(rangePartitionInfo.getPartitionColumns().stream()
                    .map(column -> column.getName().toLowerCase()).collect(Collectors.toList()));
        }

        List<String> distributionColumnNames = olapTable.getDistributionColumnNames().stream()
                .map(name -> name.toLowerCase()).collect(Collectors.toList());

        LogicalAggregationOperator aggregationOperator = (LogicalAggregationOperator) input.getOp();
        List<String> keyColumns = aggregationOperator.getGroupingKeys().stream()
                .map(columnRefOperator -> columnRefOperator.getName().toLowerCase()).collect(Collectors.toList());

        if (keyColumns.containsAll(partitionColumnNames) && keyColumns.containsAll(distributionColumnNames)) {
            return true;
        }
        return false;
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalAggregationOperator aggregationOperator = (LogicalAggregationOperator) input.getOp();
        Map<ColumnRefOperator, ScalarOperator> projectMap = Maps.newHashMap();
        if (aggregationOperator.getProjection() != null) {
            projectMap.putAll(aggregationOperator.getProjection().getColumnRefMap());
        } else {
            aggregationOperator.getGroupingKeys().forEach(g -> projectMap.put(g, g));
            projectMap.putAll(aggregationOperator.getAggregations());
        }
        LogicalProjectOperator projectOperator = new LogicalProjectOperator(projectMap);
        return Lists.newArrayList(OptExpression.create(projectOperator, input.getInputs().get(0)));
    }
}
