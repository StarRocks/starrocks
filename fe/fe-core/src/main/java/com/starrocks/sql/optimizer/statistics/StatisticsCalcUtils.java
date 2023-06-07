// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.optimizer.statistics;

import com.google.common.base.Preconditions;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Table;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class StatisticsCalcUtils {

    private static final Logger LOG = LogManager.getLogger(StatisticsCalcUtils.class);

    private StatisticsCalcUtils() {

    }

    public static Statistics.Builder estimateScanColumns(Table table,
                                                         Map<ColumnRefOperator, Column> colRefToColumnMetaMap) {
        return estimateScanColumns(table, colRefToColumnMetaMap, null);
    }

    public static Statistics.Builder estimateScanColumns(Table table,
                                                         Map<ColumnRefOperator, Column> colRefToColumnMetaMap,
                                                         OptimizerContext optimizerContext) {
        Statistics.Builder builder = Statistics.builder();
        List<ColumnRefOperator> requiredColumns = new ArrayList<>(colRefToColumnMetaMap.keySet());
        List<ColumnStatistic> columnStatisticList =
                GlobalStateMgr.getCurrentStatisticStorage().getColumnStatistics(table,
                        requiredColumns.stream().map(ColumnRefOperator::getName).collect(Collectors.toList()));
        Preconditions.checkState(requiredColumns.size() == columnStatisticList.size());
        for (int i = 0; i < requiredColumns.size(); ++i) {
            builder.addColumnStatistic(requiredColumns.get(i), columnStatisticList.get(i));
            if (optimizerContext != null) {
                optimizerContext.getDumpInfo()
                        .addTableStatistics(table, requiredColumns.get(i).getName(), columnStatisticList.get(i));
            }
        }

        return builder;
    }

    public static long getOlapTableRowCount(Table table, Operator node) {
        return getOlapTableRowCount(table, node, null);
    }

    public static long getOlapTableRowCount(Table table, Operator node, OptimizerContext optimizerContext) {
        OlapTable olapTable = (OlapTable) table;
        List<Partition> selectedPartitions;
        if (node.isLogical()) {
            LogicalOlapScanOperator olapScanOperator = (LogicalOlapScanOperator) node;
            selectedPartitions = olapScanOperator.getSelectedPartitionId().stream().map(
                    olapTable::getPartition).collect(Collectors.toList());
        } else {
            PhysicalOlapScanOperator olapScanOperator = (PhysicalOlapScanOperator) node;
            selectedPartitions = olapScanOperator.getSelectedPartitionId().stream().map(
                    olapTable::getPartition).collect(Collectors.toList());
        }
        long rowCount = 0;
        for (Partition partition : selectedPartitions) {
            rowCount += partition.getBaseIndex().getRowCount();
            if (optimizerContext != null) {
                optimizerContext.getDumpInfo()
                        .addPartitionRowCount(table, partition.getName(), partition.getBaseIndex().getRowCount());
            }
        }
        // Currently, after FE just start, the row count of table is always 0.
        // Explicitly set table row count to 1 to make our cost estimate work.
        return Math.max(rowCount, 1);
    }

}
