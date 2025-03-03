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

import com.google.common.collect.Maps;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Table;
import com.starrocks.connector.statistics.ConnectorTableColumnStats;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public interface StatisticStorage {
    // partitionId: RowCount
    default Map<Long, Optional<Long>> getTableStatistics(Long tableId, Collection<Partition> partitions) {
        return partitions.stream().collect(Collectors.toMap(Partition::getId, p -> Optional.empty()));
    }

    default void refreshTableStatistic(Table table, boolean isSync) {
    }

    default void refreshColumnStatistics(Table table, List<String> columns, boolean isSync) {
    }

    default void refreshMultiColumnStatistics(Long tableId) {
    }

    /**
     * Overwrite the statistics of `targetPartition` with `sourcePartition`
     */
    default void overwritePartitionStatistics(long tableId, long sourcePartition, long targetPartition) {
    }

    default void updatePartitionStatistics(long tableId, long partition, long rows) {
    }

    ColumnStatistic getColumnStatistic(Table table, String column);

    List<ColumnStatistic> getColumnStatistics(Table table, List<String> columns);

    /**
     * Return partition-level column statistics, it may not exist
     */
    default Map<Long, List<ColumnStatistic>> getColumnStatisticsOfPartitionLevel(Table table, List<Long> partitions,
                                                                                 List<String> columns) {
        return null;
    }

    default List<ConnectorTableColumnStats> getConnectorTableStatistics(Table table, List<String> columns) {
        return columns.stream().
                map(col -> ConnectorTableColumnStats.unknown()).collect(Collectors.toList());
    }

    default List<ConnectorTableColumnStats> getConnectorTableStatisticsSync(Table table, List<String> columns) {
        return getConnectorTableStatistics(table, columns);
    }

    default Map<String, Histogram> getHistogramStatistics(Table table, List<String> columns) {
        return Maps.newHashMap();
    }

    default Map<String, Histogram> getHistogramStatisticsSync(Table table, List<String> columns) {
        return getHistogramStatistics(table, columns);
    }

    default Map<String, Histogram> getConnectorHistogramStatistics(Table table, List<String> columns) {
        return Maps.newHashMap();
    }

    default Map<String, Histogram> getConnectorHistogramStatisticsSync(Table table, List<String> columns) {
        return getConnectorHistogramStatistics(table, columns);
    }

    default MultiColumnCombinedStatistics getMultiColumnCombinedStatistics(Long tableId) {
        return MultiColumnCombinedStatistics.EMPTY;
    }

    default void expireMultiColumnStatistics(Long tableId) {
    }

    default void expireHistogramStatistics(Long tableId, List<String> columns) {
    }

    default void expireTableAndColumnStatistics(Table table, List<String> columns) {
    }

    default void expireConnectorTableColumnStatistics(Table table, List<String> columns) {
    }

    default void refreshConnectorTableColumnStatistics(Table table, List<String> columns, boolean isSync) {
    }

    default void expireConnectorHistogramStatistics(Table table, List<String> columns) {
    }

    void addColumnStatistic(Table table, String column, ColumnStatistic columnStatistic);
}
