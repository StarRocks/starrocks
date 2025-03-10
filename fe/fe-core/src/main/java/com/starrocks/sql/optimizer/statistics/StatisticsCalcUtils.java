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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Table;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.statistic.BasicStatsMeta;
import com.starrocks.statistic.StatisticUtils;
import com.starrocks.statistic.StatsConstants;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.jetbrains.annotations.Nullable;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
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
                GlobalStateMgr.getCurrentState().getStatisticStorage().getColumnStatistics(table, columns);

        Map<String, Histogram> histogramStatistics =
                GlobalStateMgr.getCurrentState().getStatisticStorage().getHistogramStatistics(table, columns);

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

    public static Statistics.Builder estimateMultiColumnCombinedStats(Table table,
                                                                      Statistics.Builder builder,
                                                                      Map<ColumnRefOperator, Column> colRefToColumnMetaMap) {
        if (!table.isNativeTableOrMaterializedView()) {
            return builder;
        }

        MultiColumnCombinedStatistics cachedMcStats = GlobalStateMgr.getCurrentState().getStatisticStorage()
                .getMultiColumnCombinedStatistics(table.getId());
        if (cachedMcStats == null || cachedMcStats == MultiColumnCombinedStatistics.EMPTY) {
            return builder;
        }

        Map<String, ColumnRefOperator> columnNameToRefMap = colRefToColumnMetaMap.keySet().stream()
                .collect(Collectors.toMap(ColumnRefOperator::getName, Function.identity()));

        Map<Integer, String> uniqueIdToColumnNameMap = new HashMap<>(table.getBaseSchema().size());
        table.getBaseSchema().forEach(column ->
                uniqueIdToColumnNameMap.put(column.getUniqueId(), column.getName()));


        Map<Set<Integer>, Long> distinctCounts = cachedMcStats.getDistinctCounts();
        for (Map.Entry<Set<Integer>, Long> entry : distinctCounts.entrySet()) {
            Set<Integer> uniqueColumnIds = entry.getKey();
            Long ndv = entry.getValue();

            Set<ColumnRefOperator> mcRefOperators = new HashSet<>(uniqueColumnIds.size());
            boolean allColumnsFound = true;

            for (Integer uniqueColumnId : uniqueColumnIds) {
                String columnName = uniqueIdToColumnNameMap.get(uniqueColumnId);
                if (columnName == null) {
                    allColumnsFound = false;
                    break;
                }

                ColumnRefOperator columnRef = columnNameToRefMap.get(columnName);
                if (columnRef == null) {
                    allColumnsFound = false;
                    break;
                }

                mcRefOperators.add(columnRef);
            }

            // Add the multi-column statistic if all required columns are found
            if (allColumnsFound) {
                builder = builder.addMultiColumnStatistics(mcRefOperators, new MultiColumnCombinedStats(ndv));
            }
        }

        return builder;
    }

    /**
     * Return partition-level statistics if it exists.
     * Only return the statistics if all columns and all partitions have the required statistics, otherwise return null
     */
    public static Map<Long, Statistics> getPartitionStatistics(Operator node, OlapTable table,
                                                               Map<ColumnRefOperator, Column> columns) {

        // 1. only FULL statistics has partition-level info
        BasicStatsMeta basicStatsMeta =
                GlobalStateMgr.getCurrentState().getAnalyzeMgr().getTableBasicStatsMeta(table.getId());
        StatsConstants.AnalyzeType analyzeType = basicStatsMeta == null ? null : basicStatsMeta.getType();
        if (analyzeType != StatsConstants.AnalyzeType.FULL) {
            return null;
        }
        List<Partition> selectedPartitions = getSelectedPartitions(node, table);
        if (CollectionUtils.isEmpty(selectedPartitions)) {
            return null;
        }
        List<Long> partitionIdList = selectedPartitions.stream().map(Partition::getId).collect(Collectors.toList());

        // column stats
        List<String> columnNames = columns.values().stream().map(Column::getName).collect(Collectors.toList());
        Map<Long, List<ColumnStatistic>> columnStatistics =
                GlobalStateMgr.getCurrentState().getStatisticStorage().getColumnStatisticsOfPartitionLevel(table,
                        partitionIdList, columnNames);
        if (MapUtils.isEmpty(columnStatistics)) {
            return null;
        }

        // partition rows
        Map<Long, Long> partitionRows = getPartitionRows(table, basicStatsMeta, selectedPartitions);

        Map<Long, Statistics> result = Maps.newHashMap();
        Map<String, ColumnRefOperator> columnNameMap =
                columns.entrySet().stream().collect(Collectors.toMap(x -> x.getValue().getName(), Map.Entry::getKey));
        for (var entry : columnStatistics.entrySet()) {
            Statistics.Builder builder = Statistics.builder();
            for (int i = 0; i < columnNames.size(); i++) {
                String columnName = columnNames.get(i);
                ColumnStatistic columnStatistic = entry.getValue().get(i);
                ColumnRefOperator ref = columnNameMap.get(columnName);
                builder.addColumnStatistic(ref, columnStatistic);
            }
            long partitionRow = partitionRows.get(entry.getKey());
            if (partitionRow == 1) {
                builder.setTableRowCountMayInaccurate(true);
            }
            builder.setOutputRowCount(partitionRow);
            result.put(entry.getKey(), builder.build());
        }
        return result;
    }

    public static long getTableRowCount(Table table, Operator node) {
        return getTableRowCount(table, node, null);
    }

    private static Map<Long, Long> getPartitionRows(Table table, BasicStatsMeta basicStatsMeta,
                                                    Collection<Partition> selectedPartitions) {
        // The basicStatsMeta.getUpdateRows() interface can get the number of
        // loaded rows in the table since the last statistics update. But this number is at the table level.
        // So here we can count the number of partitions that have changed since the last statistics update,
        // and then evenly distribute the number of updated rows at the table level to the partition boundaries
        // The purpose of this is to make the statistics of the number of rows more accurate.
        // For example, a large amount of data LOAD may cause the number of rows to change greatly.
        // This leads to very inaccurate row counts.
        LocalDateTime lastWorkTimestamp = GlobalStateMgr.getCurrentState().getTabletStatMgr().getLastWorkTimestamp();
        long deltaRows = deltaRows(table, basicStatsMeta.getUpdateRows());
        Map<Long, Optional<Long>> tableStatisticMap = GlobalStateMgr.getCurrentState().getStatisticStorage()
                .getTableStatistics(table.getId(), selectedPartitions);
        Map<Long, Long> result = Maps.newHashMap();
        for (Partition partition : selectedPartitions) {
            long partitionRowCount;
            Optional<Long> tableStatistic =
                    tableStatisticMap.getOrDefault(partition.getId(), Optional.empty());
            LocalDateTime updateDatetime = StatisticUtils.getPartitionLastUpdateTime(partition);
            if (tableStatistic.isEmpty()) {
                partitionRowCount = partition.getRowCount();
                if (updateDatetime.isAfter(lastWorkTimestamp)) {
                    partitionRowCount += deltaRows;
                }
            } else {
                partitionRowCount = tableStatistic.get();
                if (updateDatetime.isAfter(basicStatsMeta.getUpdateTime())) {
                    partitionRowCount += deltaRows;
                }
            }

            result.put(partition.getId(), partitionRowCount);
        }
        return result;
    }

    public static long getTableRowCount(Table table, Operator node, OptimizerContext optimizerContext) {
        if (table.isNativeTableOrMaterializedView()) {
            OlapTable olapTable = (OlapTable) table;
            Collection<Partition> selectedPartitions = getSelectedPartitions(node, olapTable);
            if (selectedPartitions == null) {
                return 1;
            }
            long rowCount = 0;

            BasicStatsMeta basicStatsMeta =
                    GlobalStateMgr.getCurrentState().getAnalyzeMgr().getTableBasicStatsMeta(table.getId());
            StatsConstants.AnalyzeType analyzeType = basicStatsMeta == null ? null : basicStatsMeta.getType();
            LocalDateTime lastWorkTimestamp = GlobalStateMgr.getCurrentState().getTabletStatMgr().getLastWorkTimestamp();
            if (StatsConstants.AnalyzeType.FULL == analyzeType) {
                Map<Long, Long> partitionRows =
                        getPartitionRows(table, basicStatsMeta, selectedPartitions);
                for (var partition : selectedPartitions) {
                    long partitionRowCount = partitionRows.get(partition.getId());
                    updateQueryDumpInfo(optimizerContext, table, partition.getName(), partitionRowCount);
                    rowCount += partitionRowCount;
                }
                return Math.max(rowCount, 1);
            }

            for (Partition partition : selectedPartitions) {
                rowCount += partition.getRowCount();
                updateQueryDumpInfo(optimizerContext, table, partition.getName(), partition.getRowCount());
            }

            // attempt use updateRows from basicStatsMeta to adjust estimated row counts
            if (StatsConstants.AnalyzeType.SAMPLE == analyzeType
                    && basicStatsMeta.getUpdateTime().isAfter(lastWorkTimestamp)) {
                long statsRowCount = Math.max(basicStatsMeta.getUpdateRows() / table.getPartitions().size(), 1)
                        * selectedPartitions.size();
                if (statsRowCount > rowCount) {
                    rowCount = statsRowCount;
                    for (Partition partition : selectedPartitions) {
                        updateQueryDumpInfo(optimizerContext, table, partition.getName(),
                                rowCount / selectedPartitions.size());
                    }
                }
            }
            // Currently, after FE just start, the row count of table is always 0.
            // Explicitly set table row count to 1 to make our cost estimate work.
            return Math.max(rowCount, 1);
        }

        return 1;
    }

    private static @Nullable List<Partition> getSelectedPartitions(Operator node, OlapTable olapTable) {
        List<Partition> selectedPartitions;
        if (node.getOpType() == OperatorType.LOGICAL_BINLOG_SCAN ||
                node.getOpType() == OperatorType.PHYSICAL_STREAM_SCAN) {
            return null;
        } else if (node.isLogical()) {
            LogicalOlapScanOperator olapScanOperator = (LogicalOlapScanOperator) node;
            if (olapScanOperator.getSelectedPartitionId() == null) {
                selectedPartitions = Lists.newArrayList(olapScanOperator.getTable().getPartitions());
            } else {
                selectedPartitions = olapScanOperator.getSelectedPartitionId().stream().map(
                        olapTable::getPartition).collect(Collectors.toList());
            }
        } else {
            PhysicalOlapScanOperator olapScanOperator = (PhysicalOlapScanOperator) node;
            if (olapScanOperator.getSelectedPartitionId() == null) {
                selectedPartitions = Lists.newArrayList(olapScanOperator.getTable().getPartitions());
            } else {
                selectedPartitions = olapScanOperator.getSelectedPartitionId().stream().map(
                        olapTable::getPartition).collect(Collectors.toList());
            }
        }
        return selectedPartitions;
    }

    private static void updateQueryDumpInfo(OptimizerContext optimizerContext, Table table,
                                            String partitionName, long rowCount) {
        if (optimizerContext != null && optimizerContext.getDumpInfo() != null) {
            try {
                optimizerContext.getDumpInfo().addPartitionRowCount(table, partitionName, rowCount);
            } catch (Exception e) {
                optimizerContext.getDumpInfo().addException(e.getMessage());
            }
        }
    }

    private static long deltaRows(Table table, long totalRowCount) {
        long tblRowCount = 0L;
        Map<Long, Optional<Long>> tableStatisticMap = GlobalStateMgr.getCurrentState().getStatisticStorage()
                .getTableStatistics(table.getId(), table.getPartitions());

        for (Partition partition : table.getPartitions()) {
            long partitionRowCount;
            Optional<Long> statistic = tableStatisticMap.getOrDefault(partition.getId(), Optional.empty());
            partitionRowCount = statistic.orElseGet(partition::getRowCount);
            tblRowCount += partitionRowCount;
        }
        if (tblRowCount < totalRowCount) {
            return Math.max(1, (totalRowCount - tblRowCount) / table.getPartitions().size());
        } else {
            return 0;
        }
    }

}
