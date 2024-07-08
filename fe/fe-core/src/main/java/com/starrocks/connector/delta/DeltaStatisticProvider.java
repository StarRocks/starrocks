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

package com.starrocks.connector.delta;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.DeltaLakeTable;
import com.starrocks.connector.PredicateSearchKey;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.Statistics;
import io.delta.kernel.types.StructType;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class DeltaStatisticProvider {
    private final Map<PredicateSearchKey, DeltaLakeFileStats> deltaLakeFileStatsMap = new HashMap<>();

    public DeltaStatisticProvider() {}

    public Statistics getCardinalityStats(StructType schema,
                                          PredicateSearchKey key,
                                          Map<ColumnRefOperator, Column> columnRefOperatorColumnMap) {
        Statistics.Builder builder = Statistics.builder();

        DeltaLakeFileStats deltaLakeFileStats;
        if (deltaLakeFileStatsMap.containsKey(key)) {
            deltaLakeFileStats = deltaLakeFileStatsMap.get(key);
        } else {
            deltaLakeFileStats = new DeltaLakeFileStats(0);
        }

        builder.setOutputRowCount(deltaLakeFileStats.getRecordCount());
        builder.addColumnStatistics(buildColumnStatistics(schema, columnRefOperatorColumnMap, deltaLakeFileStats));

        return builder.build();
    }

    public void updateFileStats(DeltaLakeTable table, PredicateSearchKey key, FileScanTask file,
                                DeltaLakeAddFileStatsSerDe fileStatsSerDe, Set<String> nonPartitionPrimitiveColumn,
                                Set<String> partitionPrimitiveColumns) {
        StructType schema = table.getDeltaMetadata().getSchema();

        DeltaLakeFileStats fileStats;
        if (deltaLakeFileStatsMap.containsKey(key)) {
            fileStats = deltaLakeFileStatsMap.get(key);
            fileStats.update(fileStatsSerDe, file.getPartitionValues(), file.getRecords(), file.getFileSize());
        } else {
            fileStats = new DeltaLakeFileStats(schema, nonPartitionPrimitiveColumn, partitionPrimitiveColumns,
                    fileStatsSerDe, file.getPartitionValues(), file.getRecords(), file.getFileSize());
            deltaLakeFileStatsMap.put(key, fileStats);
        }
    }

    public Statistics getTableStatistics(StructType schema,
                                         PredicateSearchKey key,
                                         Map<ColumnRefOperator, Column> columnRefOperatorColumnMap) {
        Statistics.Builder builder = Statistics.builder();

        DeltaLakeFileStats deltaLakeFileStats;
        if (deltaLakeFileStatsMap.containsKey(key)) {
            deltaLakeFileStats = deltaLakeFileStatsMap.get(key);
        } else {
            deltaLakeFileStats = new DeltaLakeFileStats(0);
        }

        builder.setOutputRowCount(deltaLakeFileStats.getRecordCount());
        builder.addColumnStatistics(buildColumnStatistics(schema, columnRefOperatorColumnMap, deltaLakeFileStats));

        return builder.build();
    }

    private Map<ColumnRefOperator, ColumnStatistic> buildColumnStatistics(
            StructType schema, Map<ColumnRefOperator, Column> columnRefOperatorColumns,
            DeltaLakeFileStats fileStats) {
        Map<ColumnRefOperator, ColumnStatistic> columnStatistics = new HashMap<>();

        for (Map.Entry<ColumnRefOperator, Column> entry : columnRefOperatorColumns.entrySet())  {
            if (columnStatistics.containsKey(entry.getKey())) {
                continue;
            }

            if (schema.get(entry.getValue().getName()) == null) {
                columnStatistics.put(entry.getKey(), ColumnStatistic.unknown());
            }

            columnStatistics.put(entry.getKey(), fileStats.fillColumnStats(entry.getValue()));
        }

        return columnStatistics;
    }
}
