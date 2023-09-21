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

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableMap;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.connector.RemoteFileDesc;
import com.starrocks.connector.RemoteFileInfo;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.Statistics;
import org.apache.iceberg.BlobMetadata;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.puffin.StandardBlobTypes;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.starrocks.connector.ColumnTypeConverter.fromIcebergType;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;

public class IcebergStatisticProvider {
    private static final Logger LOG = LogManager.getLogger(IcebergStatisticProvider.class);

    public IcebergStatisticProvider() {
    }

    public Statistics getTableStatistics(IcebergTable icebergTable, ScalarOperator predicate,
                                         Map<ColumnRefOperator, Column> colRefToColumnMetaMap,
                                         OptimizerContext session) {
        LOG.debug("Begin to make iceberg table statistics!");
        Table nativeTable = icebergTable.getNativeTable();
        Statistics.Builder statisticsBuilder = Statistics.builder();
        Optional<Snapshot> snapshot = icebergTable.getSnapshot();
        if (snapshot.isPresent()) {
            IcebergFileStats icebergFileStats = generateIcebergFileStats(icebergTable, predicate);
            Set<Integer> primitiveColumnsFiledIds = nativeTable.schema().columns().stream()
                    .filter(column -> column.type().isPrimitiveType())
                    .map(Types.NestedField::fieldId).collect(Collectors.toSet());
            Map<Integer, Long> columnNdvs = new HashMap<>();
            if (session != null && session.getSessionVariable().isEnableIcebergNdv()) {
                columnNdvs = readNdvs(icebergTable, primitiveColumnsFiledIds);
            }

            statisticsBuilder.setOutputRowCount(icebergFileStats.getRecordCount());
            if (!columnNdvs.isEmpty()) {
                statisticsBuilder.addColumnStatistics(buildColumnStatistics(
                        nativeTable, colRefToColumnMetaMap, icebergFileStats, columnNdvs));
            } else {
                statisticsBuilder.addColumnStatistics(buildUnknownColumnStatistics(colRefToColumnMetaMap.keySet()));
            }
        } else {
            statisticsBuilder.setOutputRowCount(1);
            statisticsBuilder.addColumnStatistics(buildUnknownColumnStatistics(colRefToColumnMetaMap.keySet()));
        }
        LOG.debug("Finish to make iceberg table statistics!");
        return statisticsBuilder.build();
    }

    private Map<ColumnRefOperator, ColumnStatistic> buildUnknownColumnStatistics(Set<ColumnRefOperator> columnRefOperatorSet) {
        Map<ColumnRefOperator, ColumnStatistic> columnStatistics = new HashMap<>();
        for (ColumnRefOperator columnRefOperator : columnRefOperatorSet) {
            columnStatistics.put(columnRefOperator, ColumnStatistic.unknown());
        }
        return columnStatistics;
    }

    private IcebergFileStats generateIcebergFileStats(IcebergTable icebergTable, ScalarOperator icebergPredicate) {
        String catalogName = icebergTable.getCatalogName();
        Optional<Snapshot> snapshot = icebergTable.getSnapshot();
        long snapshotId = snapshot.get().snapshotId();

        Table nativeTable = icebergTable.getNativeTable();
        List<Types.NestedField> columns = nativeTable.schema().columns();
        Map<Integer, Type.PrimitiveType> idToTypeMapping = columns.stream()
                .filter(column -> column.type().isPrimitiveType())
                .collect(Collectors.toMap(Types.NestedField::fieldId, column -> column.type().asPrimitiveType()));

        List<PartitionField> partitionFields = nativeTable.spec().fields();

        Set<Integer> identityPartitionIds = nativeTable.spec().fields().stream()
                .filter(x -> x.transform().isIdentity())
                .map(PartitionField::sourceId)
                .collect(Collectors.toSet());

        List<Types.NestedField> nonPartitionPrimitiveColumns = columns.stream()
                .filter(column -> !identityPartitionIds.contains(column.fieldId()) &&
                        column.type().isPrimitiveType())
                .collect(toImmutableList());

        IcebergFileStats icebergFileStats = null;
        List<RemoteFileInfo> splits = GlobalStateMgr.getCurrentState().getMetadataMgr().getRemoteFileInfos(
                catalogName, icebergTable, null, snapshotId, icebergPredicate, null);

        if (splits.isEmpty()) {
            return new IcebergFileStats(1);
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
                    icebergFileStats = new IcebergFileStats(
                            idToTypeMapping,
                            nonPartitionPrimitiveColumns,
                            dataFile.partition(),
                            dataFile.recordCount(),
                            dataFile.fileSizeInBytes(),
                            IcebergFileStats.toMap(idToTypeMapping, dataFile.lowerBounds()),
                            IcebergFileStats.toMap(idToTypeMapping, dataFile.upperBounds()),
                            dataFile.nullValueCounts(),
                            dataFile.columnSizes());
                } else {
                    icebergFileStats.incrementFileCount();
                    icebergFileStats.incrementRecordCount(dataFile.recordCount());
                    icebergFileStats.incrementSize(dataFile.fileSizeInBytes());
                    updateSummaryMin(icebergFileStats, partitionFields, IcebergFileStats.toMap(idToTypeMapping,
                            dataFile.lowerBounds()), dataFile.nullValueCounts(), dataFile.recordCount());
                    updateSummaryMax(icebergFileStats, partitionFields, IcebergFileStats.toMap(idToTypeMapping,
                            dataFile.upperBounds()), dataFile.nullValueCounts(), dataFile.recordCount());
                    icebergFileStats.updateNullCount(dataFile.nullValueCounts());
                    updateColumnSizes(icebergFileStats, dataFile.columnSizes());
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
            Table nativeTable, Map<ColumnRefOperator, Column> colRefToColumnMetaMap,
            IcebergFileStats icebergFileStats, Map<Integer, Long> columnNdvs) {
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

            columnStatistics.put(columnList.get(0), generateColumnStatistic(
                    idColumn.getKey(), colRefToColumnMetaMap.get(columnList.get(0)), icebergFileStats, columnNdvs));
        }
        return columnStatistics;
    }

    private ColumnStatistic generateColumnStatistic(Integer fieldId, Column column, IcebergFileStats icebergStats,
                                                    Map<Integer, Long> columnNdvs) {
        ColumnStatistic.Builder builder = ColumnStatistic.builder();
        Long ndv = columnNdvs.get(fieldId);
        if (ndv != null) {
            builder.setDistinctValuesCount(Math.min(ndv, icebergStats.getRecordCount()));
        }

        if (column.getType().isStringType()) {
            String minString = icebergStats.getMinValues().get(fieldId).toString();
            builder.setMinString(minString);
        } else if (icebergStats.getMinValue(fieldId).isPresent()) {
            builder.setMinValue(icebergStats.getMinValue(fieldId).get());
        }

        if (column.getType().isStringType()) {
            String maxString = icebergStats.getMaxValues().get(fieldId).toString();
            builder.setMinString(maxString);
        } else if (icebergStats.getMaxValue(fieldId).isPresent()) {
            builder.setMinValue(icebergStats.getMaxValue(fieldId).get());
        }

        Long nullCount = icebergStats.getNullCounts() == null ? null : icebergStats.getNullCounts().get(fieldId);
        if (nullCount != null) {
            builder.setNullsFraction(nullCount * 1.0 / Math.max(icebergStats.getRecordCount(), 1));
        }

        if (!column.getType().isStringType()) {
            builder.setAverageRowSize(column.getType().getTypeSize());
        } else {
            Long columnSize = icebergStats.getColumnSizes() == null ? null : icebergStats.getColumnSizes().get(fieldId);
            if (columnSize != null) {
                builder.setAverageRowSize(Math.max(columnSize * 1.0 / Math.max(icebergStats.getRecordCount(), 1), 1));
            }
        }
        return builder.build();
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
        if (!icebergFileStats.hasValidColumnMetrics()) {
            return;
        }

        for (PartitionField field : partitionFields) {
            int id = field.sourceId();
            if (icebergFileStats.getCorruptedStats().contains(id)) {
                continue;
            }

            Object newValue = newStats.get(id);
            if (newValue == null) {
                current.remove(id);
                icebergFileStats.getCorruptedStats().add(id);
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

    public static Map<Integer, Long> readNdvs(IcebergTable icebergTable, Set<Integer> columnIds) {
        ImmutableMap.Builder<Integer, Long> colIdToNdv = ImmutableMap.builder();
        Set<Integer> remainingColumnIds = new HashSet<>(columnIds);

        Iterator<StatisticsFile> statisticsFiles = getStatisticsFiles(
                icebergTable.getNativeTable(), icebergTable.getSnapshot().get().snapshotId());
        while (!remainingColumnIds.isEmpty() && statisticsFiles.hasNext()) {
            StatisticsFile statisticsFile = statisticsFiles.next();

            Map<Integer, BlobMetadata> thetaBlobsByFieldId = statisticsFile.blobMetadata().stream()
                    .filter(blobMetadata -> blobMetadata.type().equals(StandardBlobTypes.APACHE_DATASKETCHES_THETA_V1))
                    .filter(blobMetadata -> blobMetadata.fields().size() == 1)
                    .filter(blobMetadata -> remainingColumnIds.contains(getOnlyElement(blobMetadata.fields())))
                    // Fail loud upon duplicates (there must be none)
                    .collect(toImmutableMap(blobMetadata -> getOnlyElement(blobMetadata.fields()), identity()));

            for (Map.Entry<Integer, BlobMetadata> entry : thetaBlobsByFieldId.entrySet()) {
                int fieldId = entry.getKey();
                BlobMetadata blobMetadata = entry.getValue();
                String ndv = blobMetadata.properties().get("ndv");
                if (ndv == null) {
                    LOG.debug("Blob %s is missing ndv property", blobMetadata.type());
                    remainingColumnIds.remove(fieldId);
                } else {
                    remainingColumnIds.remove(fieldId);
                    colIdToNdv.put(fieldId, Long.parseLong(ndv));
                }
            }
        }

        return colIdToNdv.build();
    }

    public static Iterator<StatisticsFile> getStatisticsFiles(Table icebergTable, long snapshotId) {
        return new AbstractIterator<StatisticsFile>() {
            private final Map<Long, StatisticsFile> statsFileBySnapshot = icebergTable.statisticsFiles().stream()
                    .collect(toMap(
                            StatisticsFile::snapshotId,
                            identity(),
                            (a, b) -> {
                                throw new IllegalStateException(String.format(
                                        "Unexpected duplicate statistics files %s, %s", a, b));
                            },
                            HashMap::new));

            @Override
            protected StatisticsFile computeNext() {
                if (statsFileBySnapshot.isEmpty()) {
                    // Already found all statistics files
                    return endOfData();
                }

                StatisticsFile statisticsFile = statsFileBySnapshot.remove(snapshotId);
                if (statisticsFile != null) {
                    return statisticsFile;
                }
                return endOfData();
            }
        };
    }
}