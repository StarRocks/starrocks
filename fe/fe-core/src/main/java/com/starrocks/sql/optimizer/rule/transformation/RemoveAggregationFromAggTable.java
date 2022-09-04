// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.optimizer.rule.transformation;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.FunctionSet;
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
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ReplaceColumnRefRewriter;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

// for Aggregation on Aggregate type table or index,
// if group keys contain all aggregate keys, partition keys and distribution keys,
// and the aggregation functions are the same as the aggregate type of the target value columns in table
// then the Aggregation can be remove and the preAggregate should be off for OlapScanOperator
public class RemoveAggregationFromAggTable extends TransformationRule {
    private static final List<String> UNSUPPORTED_FUNCTION_NAMES =
            ImmutableList.of(FunctionSet.BITMAP_UNION,
                    FunctionSet.BITMAP_UNION_COUNT,
                    FunctionSet.HLL_UNION,
                    FunctionSet.HLL_UNION_AGG,
                    FunctionSet.PERCENTILE_UNION);

    public RemoveAggregationFromAggTable() {
        super(RuleType.TF_REMOVE_AGGREGATION_BY_AGG_TABLE,
                Pattern.create(OperatorType.LOGICAL_AGGR).addChildren(Pattern.create(
                        OperatorType.LOGICAL_OLAP_SCAN)));
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        OptExpression scanExpression = input.getInputs().get(0);
        if (!(scanExpression.getOp() instanceof LogicalOlapScanOperator)) {
            return false;
        }
        LogicalOlapScanOperator scanOperator = (LogicalOlapScanOperator) scanExpression.getOp();
        if (scanOperator.getProjection() != null) {
            return false;
        }
        OlapTable olapTable = (OlapTable) scanOperator.getTable();

        MaterializedIndexMeta materializedIndexMeta = olapTable.getIndexMetaByIndexId(scanOperator.getSelectedIndexId());
        // must be Aggregation
        if (!materializedIndexMeta.getKeysType().isAggregationFamily()) {
            return false;
        }
        Set<String> keyColumnNames = Sets.newHashSet();
        List<Column> indexSchema = materializedIndexMeta.getSchema();
        for (Column column : indexSchema) {
            if (column.isKey()) {
                keyColumnNames.add(column.getName().toLowerCase());
            }
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
                .map(String::toLowerCase).collect(Collectors.toList());

        LogicalAggregationOperator aggregationOperator = (LogicalAggregationOperator) input.getOp();
        // check whether every aggregation function on column is the same as the AggregationType of the column
        for (Map.Entry<ColumnRefOperator, CallOperator> entry : aggregationOperator.getAggregations().entrySet()) {
            if (UNSUPPORTED_FUNCTION_NAMES.contains(entry.getValue().getFnName().toLowerCase())) {
                return false;
            }
            CallOperator callOperator = entry.getValue();
            if (callOperator.getChildren().size() != 1) {
                return false;
            }
            ScalarOperator argument = callOperator.getChild(0);
            if (!(argument instanceof ColumnRefOperator)) {
                return false;
            }
            ColumnRefOperator columnRefOperator = (ColumnRefOperator) argument;
            Optional<Column> columnOptional = indexSchema.stream()
                    .filter(column -> column.getName().equalsIgnoreCase(columnRefOperator.getName())).findFirst();
            if (!columnOptional.isPresent()) {
                return false;
            }
            Column column = columnOptional.get();
            if (column.getAggregationType() == null) {
                return false;
            }
            if (!column.getAggregationType().toString().equalsIgnoreCase(entry.getValue().getFnName())) {
                return false;
            }
        }
        List<String> groupKeyColumns = aggregationOperator.getGroupingKeys().stream()
                .map(columnRefOperator -> columnRefOperator.getName().toLowerCase()).collect(Collectors.toList());
        return groupKeyColumns.containsAll(keyColumnNames)
                && groupKeyColumns.containsAll(partitionColumnNames)
                && groupKeyColumns.containsAll(distributionColumnNames);
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalAggregationOperator aggregationOperator = (LogicalAggregationOperator) input.getOp();
        Map<ColumnRefOperator, ScalarOperator> projectMap = Maps.newHashMap();
        aggregationOperator.getGroupingKeys().forEach(g -> projectMap.put(g, g));
        for (Map.Entry<ColumnRefOperator, CallOperator> entry : aggregationOperator.getAggregations().entrySet()) {
            // in this case, CallOperator must have only one child ColumnRefOperator
            projectMap.put(entry.getKey(), entry.getValue().getChild(0));
        }
        if (aggregationOperator.getProjection() != null) {
            Map<ColumnRefOperator, ScalarOperator> newProjectMap =
                    Maps.newHashMap(aggregationOperator.getProjection().getColumnRefMap());
            ReplaceColumnRefRewriter rewriter = new ReplaceColumnRefRewriter(projectMap);
            for (Map.Entry<ColumnRefOperator, ScalarOperator> entry :
                    aggregationOperator.getProjection().getColumnRefMap().entrySet()) {
                ScalarOperator rewrittenOperator = rewriter.rewrite(entry.getValue());
                newProjectMap.put(entry.getKey(), rewrittenOperator);
            }
            projectMap.clear();
            projectMap.putAll(newProjectMap);
        }
        LogicalProjectOperator projectOperator = new LogicalProjectOperator(projectMap);
        return Lists.newArrayList(OptExpression.create(projectOperator, input.getInputs().get(0)));
    }
}
