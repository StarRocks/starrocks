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
import com.starrocks.statistic.BasicStatsMeta;
import com.starrocks.statistic.StatisticUtils;
import com.starrocks.statistic.StatsConstants;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class StatisticsCalcUtils {

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
        List<ColumnRefOperator> requiredColumnRefs = new ArrayList<>(colRefToColumnMetaMap.keySet());
        List<String> columns = new ArrayList<>(colRefToColumnMetaMap.values())
                .stream().map(Column::getName).collect(Collectors.toList());
        List<ColumnStatistic> columnStatisticList =
                GlobalStateMgr.getCurrentStatisticStorage().getColumnStatistics(table, columns);

        Map<String, Histogram> histogramStatistics =
                GlobalStateMgr.getCurrentStatisticStorage().getHistogramStatistics(table, columns);

        for (int i = 0; i < requiredColumnRefs.size(); ++i) {
            ColumnStatistic columnStatistic;
            if (histogramStatistics.containsKey(requiredColumnRefs.get(i).getName())) {
                columnStatistic = ColumnStatistic.buildFrom(columnStatisticList.get(i)).setHistogram(
                        histogramStatistics.get(requiredColumnRefs.get(i).getName())).build();
            } else {
                columnStatistic = columnStatisticList.get(i);
            }
            builder.addColumnStatistic(requiredColumnRefs.get(i), columnStatistic);
            if (optimizerContext != null && optimizerContext.getDumpInfo() != null) {
                optimizerContext.getDumpInfo()
                        .addTableStatistics(table, requiredColumnRefs.get(i).getName(), columnStatisticList.get(i));
            }
        }

        return builder;
    }

    public static long getTableRowCount(Table table, Operator node) {
        return getTableRowCount(table, node, null);
    }

    public static long getTableRowCount(Table table, Operator node, OptimizerContext optimizerContext) {
        if (table.isNativeTable()) {
            OlapTable olapTable = (OlapTable) table;
            List<Partition> selectedPartitions;
            Column smallColumn = table.getColumns().stream().filter(Column::isKey).findAny().orElse(null);
            if (node.isLogical()) {
                LogicalOlapScanOperator olapScanOperator = (LogicalOlapScanOperator) node;
                selectedPartitions = olapScanOperator.getSelectedPartitionId().stream().map(
                        olapTable::getPartition).collect(Collectors.toList());
                smallColumn =
                        olapScanOperator.getColRefToColumnMetaMap().values().stream().findAny().orElse(smallColumn);
            } else {
                PhysicalOlapScanOperator olapScanOperator = (PhysicalOlapScanOperator) node;
                selectedPartitions = olapScanOperator.getSelectedPartitionId().stream().map(
                        olapTable::getPartition).collect(Collectors.toList());
                smallColumn =
                        olapScanOperator.getColRefToColumnMetaMap().values().stream().findAny().orElse(smallColumn);
            }
            long rowCount = 0;

            BasicStatsMeta basicStatsMeta =
                    GlobalStateMgr.getCurrentAnalyzeMgr().getBasicStatsMetaMap().get(table.getId());
            if (basicStatsMeta != null && basicStatsMeta.getType().equals(StatsConstants.AnalyzeType.FULL)) {
                Preconditions.checkNotNull(smallColumn);
                // The basicStatsMeta.getUpdateRows() interface can get the number of
                // loaded rows in the table since the last statistics update. But this number is at the table level.
                // So here we can count the number of partitions that have changed since the last statistics update,
                // and then evenly distribute the number of updated rows at the table level to the partition boundaries
                // The purpose of this is to make the statistics of the number of rows more accurate.
                // For example, a large amount of data LOAD may cause the number of rows to change greatly.
                // This leads to very inaccurate row counts.
                int partitionCountModifiedAfterLastAnalyze = 0;
                int partitionCount = 0;
                for (Partition partition : table.getPartitions()) {
                    LocalDateTime updateDatetime = StatisticUtils.getPartitionLastUpdateTime(partition);
                    if (updateDatetime.isAfter(basicStatsMeta.getUpdateTime())) {
                        partitionCountModifiedAfterLastAnalyze++;
                    }

                    partitionCount++;
                }

                ColumnStatistic cs =
                        GlobalStateMgr.getCurrentStatisticStorage().getColumnStatistic(table, smallColumn.getName());
                long avgRowCount = (long) (cs.getRowCount() / Math.max(partitionCount, 1));
                for (Partition partition : selectedPartitions) {
                    long partitionRowCount = cs.isUnknown() ? partition.getRowCount() : avgRowCount;
                    if (partitionCountModifiedAfterLastAnalyze > 0) {
                        LocalDateTime updateDatetime = StatisticUtils.getPartitionLastUpdateTime(partition);
                        if (updateDatetime.isAfter(basicStatsMeta.getUpdateTime())) {
                            partitionRowCount +=
                                    basicStatsMeta.getUpdateRows() / partitionCountModifiedAfterLastAnalyze;
                        }
                    }

                    rowCount += partitionRowCount;
                    if (optimizerContext != null && optimizerContext.getDumpInfo() != null) {
                        optimizerContext.getDumpInfo()
                                .addPartitionRowCount(table, partition.getName(), partitionRowCount);
                    }
                }
            } else {
                for (Partition partition : selectedPartitions) {
                    rowCount += partition.getRowCount();
                    if (optimizerContext != null && optimizerContext.getDumpInfo() != null) {
                        optimizerContext.getDumpInfo()
                                .addPartitionRowCount(table, partition.getName(), partition.getRowCount());
                    }
                }
            }
            // Currently, after FE just start, the row count of table is always 0.
            // Explicitly set table row count to 1 to make our cost estimate work.
            return Math.max(rowCount, 1);
        }

        return 1;
    }

}
