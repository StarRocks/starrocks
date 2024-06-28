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
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.Statistics;
import io.delta.kernel.data.Row;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.InternalScanFileUtils;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.FileStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static io.delta.kernel.internal.InternalScanFileUtils.ADD_FILE_ORDINAL;

public class DeltaStatisticProvider {
    private static final Logger LOG = LogManager.getLogger(DeltaStatisticProvider.class);
    private final Map<PredicateSearchKey, DeltaLakeFileStats> deltaLakeFileStatsMap = new HashMap<>();

    public DeltaStatisticProvider() {}

    public Statistics getCardinalityStats(Map<ColumnRefOperator, Column> columnRefOperatorColumnMap,
                                          List<Row> fileScanTasks) {
        Statistics.Builder builder = Statistics.builder();
        long cardinality = 0;
        Set<String> currentFiles = new HashSet<>();

        for (Row file : fileScanTasks) {
            FileStatus status = InternalScanFileUtils.getAddFileStatus(file);
            String path = status.getPath();

            if (currentFiles.contains(path)) {
                continue;
            }
            currentFiles.add(path);

            Row addFileEntry = getAddFileEntry(file);
            long rows = ScanFileUtils.getFileRows(addFileEntry);
            cardinality += rows;
        }

        return builder.setOutputRowCount(cardinality)
                .addColumnStatistics(buildUnknownColumnStatistics(columnRefOperatorColumnMap.keySet()))
                .build();
    }

    public void updateFileStats(DeltaLakeTable table, PredicateSearchKey key, Row file,
                                List<String> nonPartitionPrimitiveColumn) {
        StructType schema = table.getDeltaMetadata().getSchema();

        FileStatus status = InternalScanFileUtils.getAddFileStatus(file);

        Row addFileEntry = getAddFileEntry(file);
        DeltaLakeStatsStruct fileStat = ScanFileUtils.getColumnStatistics(addFileEntry);

        DeltaLakeFileStats fileStats;
        if (deltaLakeFileStatsMap.containsKey(key)) {
            fileStats = deltaLakeFileStatsMap.get(key);
            fileStats.incrementRecordCount(fileStat.numRecords);
            fileStats.incrementSize(status.getSize());
            updateSummaryMin(fileStats, fileStat.minValues, fileStat.nullCount, fileStat.numRecords);
            updateSummaryMax(fileStats, fileStat.maxValues, fileStat.nullCount, fileStat.numRecords);
            fileStats.updateNullCount(fileStat.nullCount, nonPartitionPrimitiveColumn);
        } else {
            fileStats = new DeltaLakeFileStats(schema, nonPartitionPrimitiveColumn, fileStat.numRecords,
                    status.getSize(), fileStat.minValues, fileStat.maxValues, fileStat.nullCount);
            deltaLakeFileStatsMap.put(key, fileStats);
        }
    }

    private void updateSummaryMin(DeltaLakeFileStats deltaLakeFileStats,
                                  Map<String, Object> lowerBounds,
                                  Map<String, Object> nullCounts,
                                  long recordCount) {
        deltaLakeFileStats.updateMinStats(lowerBounds, nullCounts, recordCount, i -> (i > 0));
    }

    private void updateSummaryMax(DeltaLakeFileStats deltaLakeFileStats,
                                  Map<String, Object> upperBounds,
                                  Map<String, Object> nulCounts,
                                  long recordCount) {
        deltaLakeFileStats.updateMaxStats(upperBounds, nulCounts, recordCount, i -> (i < 0));
    }

    public Statistics getTableStatistics(DeltaLakeTable deltaLakeTable,
                                         Map<ColumnRefOperator, Column> columnRefOperatorColumnMap,
                                         ScalarOperator predicate) {
        String dbName = deltaLakeTable.getDbName();
        String tableName = deltaLakeTable.getTableName();
        Engine engine = deltaLakeTable.getDeltaEngine();
        long snapshotId = deltaLakeTable.getDeltaSnapshot().getVersion(engine);
        StructType schema = deltaLakeTable.getDeltaMetadata().getSchema();

        Statistics.Builder builder = Statistics.builder();

        PredicateSearchKey key = PredicateSearchKey.of(dbName, tableName, snapshotId, predicate);
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

    private static Row getAddFileEntry(Row file) {
        if (file.isNullAt(ADD_FILE_ORDINAL)) {
            throw new IllegalArgumentException("There is no `add` entry in the scan file row");
        } else {
            return file.getStruct(ADD_FILE_ORDINAL);
        }
    }

    public Map<ColumnRefOperator, ColumnStatistic> buildUnknownColumnStatistics(Set<ColumnRefOperator> columns) {
        return columns.stream().collect(Collectors.toMap(column -> column, column -> ColumnStatistic.unknown()));
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

            columnStatistics.put(entry.getKey(), buildColumnStatistic(entry.getValue(), fileStats));
        }

        return columnStatistics;
    }

    private ColumnStatistic buildColumnStatistic(Column column, DeltaLakeFileStats fileStats) {
        ColumnStatistic.Builder builder = ColumnStatistic.builder();
        fileStats.fillColumnStats(builder, column);
        return builder.build();
    }
}
