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
import com.starrocks.connector.statistics.ConnectorNdvEstimator;
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

        for (Map.Entry<ColumnRefOperator, Column> entry : columnRefOperatorColumns.entrySet()) {
            if (columnStatistics.containsKey(entry.getKey())) {
                continue;
            }

            String colName = entry.getValue().getName();
            if (schema.get(colName) == null) {
                columnStatistics.put(entry.getKey(), ColumnStatistic.unknown());
                continue;
            }

            ColumnStatistic base = fileStats.fillColumnStats(entry.getValue());
            double ndv = estimateDeltaNdv(colName, schema, fileStats);
            ColumnStatistic stat = ColumnStatistic.buildFrom(base)
                    .setDistinctValuesCount(ndv)
                    .setType(ColumnStatistic.StatisticType.ESTIMATE)
                    .build();
            columnStatistics.put(entry.getKey(), stat);
        }

        return columnStatistics;
    }

    private static double estimateDeltaNdv(String colName, StructType schema, DeltaLakeFileStats stats) {
        ConnectorNdvEstimator.TypeCategory cat = toDeltaTypeCategory(schema, colName);
        double min = stats.getMinDouble(colName);
        double max = stats.getMaxDouble(colName);
        long rowCount = stats.getRecordCount();
        // Delta does not expose per-column compressed sizes; skip Tier 2
        double ndv = ConnectorNdvEstimator.estimate(cat, min, max, -1L, 0L, rowCount);
        return Math.max(1.0, Math.min(ndv, rowCount));
    }

    private static ConnectorNdvEstimator.TypeCategory toDeltaTypeCategory(StructType schema, String colName) {
        io.delta.kernel.types.DataType dt = schema.get(colName).getDataType();
        DeltaDataType ddt = DeltaDataType.instanceFrom(dt.getClass());
        switch (ddt) {
            case BOOLEAN:
                return ConnectorNdvEstimator.TypeCategory.BOOLEAN;
            case BYTE:
            case SMALLINT:
            case INTEGER:
            case LONG:
                return ConnectorNdvEstimator.TypeCategory.INTEGER_LIKE;
            case FLOAT:
            case DOUBLE:
            case DECIMAL:
                return ConnectorNdvEstimator.TypeCategory.FLOAT_LIKE;
            case DATE:
                // DeltaLakeFileStats.parseDate() converts to epoch-seconds via toEpochSecond()
                return ConnectorNdvEstimator.TypeCategory.DATE_IN_EPOCH_SECONDS;
            case TIMESTAMP:
            case TIMESTAMP_NTZ:
                // DeltaLakeFileStats converts timestamps to epoch-seconds
                return ConnectorNdvEstimator.TypeCategory.TIMESTAMP_IN_EPOCH_SECONDS;
            case STRING:
            case BINARY:
                return ConnectorNdvEstimator.TypeCategory.STRING_LIKE;
            default:
                return ConnectorNdvEstimator.TypeCategory.OTHER;
        }
    }
}
