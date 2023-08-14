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


package com.starrocks.connector.iceberg.cost;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.connector.RemoteFileDesc;
import com.starrocks.connector.RemoteFileInfo;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.Statistics;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.types.Types;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static com.starrocks.connector.ColumnTypeConverter.fromIcebergType;
import static org.apache.iceberg.SnapshotSummary.TOTAL_EQ_DELETES_PROP;
import static org.apache.iceberg.SnapshotSummary.TOTAL_POS_DELETES_PROP;
import static org.apache.iceberg.SnapshotSummary.TOTAL_RECORDS_PROP;


public class IcebergStatisticProvider {
    private static final Logger LOG = LogManager.getLogger(IcebergStatisticProvider.class);

    public IcebergStatisticProvider() {
    }

    public Statistics getTableStatistics(IcebergTable icebergTable, ScalarOperator predicate,
                                         Map<ColumnRefOperator, Column> colRefToColumnMetaMap) {
        LOG.debug("Begin to make iceberg table statistics!");
        Table nativeTable = icebergTable.getNativeTable();
        IcebergFileStats icebergFileStats = generateIcebergFileStats(icebergTable, predicate);

        Statistics.Builder statisticsBuilder = Statistics.builder();
        double recordCount = Math.max(icebergFileStats == null ? 0 : icebergFileStats.getRecordCount(), 1);
        statisticsBuilder.addColumnStatistics(buildColumnStatistics(nativeTable, colRefToColumnMetaMap));
        statisticsBuilder.setOutputRowCount(recordCount);
        LOG.debug("Finish to make iceberg table statistics!");
        return statisticsBuilder.build();
    }

    private IcebergFileStats generateIcebergFileStats(IcebergTable icebergTable, ScalarOperator icebergPredicate) {
        Optional<Snapshot> snapshot = Optional.ofNullable(icebergTable.getNativeTable().currentSnapshot());
        if (!snapshot.isPresent()) {
            return null;
        }

        long snapshotId = snapshot.get().snapshotId();
        IcebergFileStats icebergFileStats = null;
        String catalogName = icebergTable.getCatalogName();

        List<RemoteFileInfo> splits = GlobalStateMgr.getCurrentState().getMetadataMgr().getRemoteFileInfos(
                catalogName, icebergTable, null, snapshotId, icebergPredicate, null);

        if (splits.isEmpty()) {
            return new IcebergFileStats(1);
        }

        if (icebergPredicate == null) {
            long totalRecords = Long.parseLong(snapshot.get().summary().getOrDefault(TOTAL_RECORDS_PROP, "1"));
            long totalPosDeletes = Long.parseLong(snapshot.get().summary().getOrDefault(TOTAL_POS_DELETES_PROP, "0"));
            long totalEqDeletes = Long.parseLong(snapshot.get().summary().getOrDefault(TOTAL_EQ_DELETES_PROP, "0"));
            return new IcebergFileStats(totalRecords - totalPosDeletes - totalEqDeletes);
        }

        RemoteFileDesc remoteFileDesc = splits.get(0).getFiles().get(0);
        if (remoteFileDesc == null) {
            icebergFileStats =  new IcebergFileStats(1);
        } else {
            // ScanTasks are splits of files, we need to avoid repetition.
            Set<String> files = new HashSet<>();
            for (FileScanTask fileScanTask : remoteFileDesc.getIcebergScanTasks()) {
                DataFile dataFile = fileScanTask.file();
                // ignore this data file.
                if (dataFile.recordCount() == 0) {
                    continue;
                }
                if (files.contains(dataFile.path().toString())) {
                    continue;
                }
                files.add(dataFile.path().toString());
                if (icebergFileStats == null) {
                    icebergFileStats = new IcebergFileStats(dataFile.recordCount());
                } else {
                    icebergFileStats.incrementRecordCount(dataFile.recordCount());
                }
            }
        }

        // all dataFile.recordCount() == 0
        if (icebergFileStats == null || icebergFileStats.getRecordCount() == 0) {
            return new IcebergFileStats(1);
        }

        return icebergFileStats;
    }

    private Map<ColumnRefOperator, ColumnStatistic> buildColumnStatistics(
            Table nativeTable, Map<ColumnRefOperator, Column> colRefToColumnMetaMap) {
        Map<ColumnRefOperator, ColumnStatistic> columnStatistics = new HashMap<>();
        List<Types.NestedField> columns = nativeTable.schema().columns();
        Map<Integer, String> idToColumnNames = columns.stream().
                filter(column -> !fromIcebergType(column.type()).isUnknown())
                .collect(Collectors.toMap(Types.NestedField::fieldId, Types.NestedField::name));

        for (Map.Entry<Integer, String> idColumn : idToColumnNames.entrySet()) {
            List<ColumnRefOperator> columnList = colRefToColumnMetaMap.keySet().stream().filter(
                    key -> key.getName().equalsIgnoreCase(idColumn.getValue())).collect(Collectors.toList());
            if (columnList.size() != 1) {
                LOG.debug("This column is not required column name " + idColumn.getValue() + " column list size "
                        + columnList.size());
                continue;
            }

            columnStatistics.put(columnList.get(0), ColumnStatistic.unknown());
        }
        return columnStatistics;
    }

    public void updateColumnSizes(IcebergFileStats icebergFileStats, Map<Integer, Long> addedColumnSizes) {
        Map<Integer, Long> columnSizes = icebergFileStats.getColumnSizes();
        if (!icebergFileStats.hasValidColumnMetrics() || columnSizes == null || addedColumnSizes == null) {
            return;
        }
        for (Types.NestedField column : icebergFileStats.getNonPartitionPrimitiveColumns()) {
            int id = column.fieldId();

            Long addedSize = addedColumnSizes.get(id);
            if (addedSize != null) {
                columnSizes.put(id, addedSize + columnSizes.getOrDefault(id, 0L));
            }
        }
    }

    private void updateSummaryMin(IcebergFileStats icebergFileStats,
                                  List<PartitionField> partitionFields,
                                  Map<Integer, Object> lowerBounds,
                                  Map<Integer, Long> nullCounts,
                                  long recordCount) {
        icebergFileStats.updateStats(icebergFileStats.getMinValues(), lowerBounds, nullCounts, recordCount,
                i -> (i > 0));
        updatePartitionedStats(icebergFileStats, partitionFields, icebergFileStats.getMinValues(), lowerBounds,
                i -> (i > 0));
    }

    private void updateSummaryMax(IcebergFileStats icebergFileStats,
                                  List<PartitionField> partitionFields,
                                  Map<Integer, Object> upperBounds,
                                  Map<Integer, Long> nullCounts,
                                  long recordCount) {
        icebergFileStats.updateStats(icebergFileStats.getMaxValues(), upperBounds, nullCounts, recordCount,
                i -> (i < 0));
        updatePartitionedStats(icebergFileStats, partitionFields, icebergFileStats.getMaxValues(), upperBounds,
                i -> (i < 0));
    }

    private void updatePartitionedStats(
            IcebergFileStats icebergFileStats,
            List<PartitionField> partitionFields,
            Map<Integer, Object> current,
            Map<Integer, Object> newStats,
            Predicate<Integer> predicate) {
        for (PartitionField field : partitionFields) {
            int id = field.sourceId();
            if (icebergFileStats.getCorruptedStats().contains(id)) {
                continue;
            }

            Object newValue = newStats.get(id);
            if (newValue == null) {
                continue;
            }

            Object oldValue = current.putIfAbsent(id, newValue);
            if (oldValue != null) {
                Comparator<Object> comparator = Comparators.forType(icebergFileStats.getIdToTypeMapping().get(id));
                if (predicate.test(comparator.compare(oldValue, newValue))) {
                    current.put(id, newValue);
                }
            }
        }
    }
}