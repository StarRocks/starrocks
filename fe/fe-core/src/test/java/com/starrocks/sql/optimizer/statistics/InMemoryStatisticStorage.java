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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class InMemoryStatisticStorage implements StatisticStorage {

    private final com.google.common.collect.Table<Long, String, ColumnStatistic> columnStatisticTable =
            com.google.common.collect.HashBasedTable.create();
    private final com.google.common.collect.Table<Long, Long, Long> partitionRowsTables =
            com.google.common.collect.HashBasedTable.create();

    @Override
    public Map<Long, Optional<Long>> getTableStatistics(Long tableId, Collection<Partition> partitions) {
        Map<Long, Optional<Long>> result = Maps.newHashMap();
        Map<Long, Long> columnMap = partitionRowsTables.row(tableId);
        for (Partition p : partitions) {
            result.put(p.getId(), Optional.ofNullable(columnMap.get(p.getId())));
        }
        return result;
    }

    @Override
    public void updatePartitionStatistics(long tableId, long partition, long rows) {
        partitionRowsTables.put(tableId, partition, rows);
    }

    @Override
    public void overwritePartitionStatistics(long tableId, long sourcePartition, long targetPartition) {
        Long stats = partitionRowsTables.get(tableId, sourcePartition);
        if (stats != null) {
            partitionRowsTables.put(tableId, targetPartition, stats);
            partitionRowsTables.remove(tableId, sourcePartition);
        }
    }

    @Override
    public ColumnStatistic getColumnStatistic(Table table, String column) {
        return columnStatisticTable.get(table.getId(), column);
    }

    @Override
    public List<ColumnStatistic> getColumnStatistics(Table table, List<String> columns) {
        var columnMap = columnStatisticTable.row(table.getId());
        return columns.stream().map(columnMap::get).collect(Collectors.toList());
    }

    @Override
    public void addColumnStatistic(Table table, String column, ColumnStatistic columnStatistic) {
        columnStatisticTable.put(table.getId(), column, columnStatistic);
    }

}
