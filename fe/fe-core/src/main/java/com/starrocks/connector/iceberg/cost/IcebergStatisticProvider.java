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

import com.google.common.collect.AbstractSequentialIterator;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.iceberg.IcebergFilter;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.Statistics;
import org.apache.iceberg.BlobMetadata;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionData;
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
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

import static com.google.common.base.Verify.verifyNotNull;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.Streams.stream;
import static com.starrocks.connector.ColumnTypeConverter.fromIcebergType;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;

public class IcebergStatisticProvider {
    private static final Logger LOG = LogManager.getLogger(IcebergStatisticProvider.class);
    public static final String NDV_PROP = "ndv";

    // table uuid -> <partition column id -> partition column values>
    private final Map<String, HashMultimap<Integer, Object>> uuidToPartitionFieldIdToValues = new HashMap<>();
    private final Map<IcebergFilter, IcebergFileStats> icebergFileStatistics = new HashMap<>();
    private final Multimap<IcebergFilter, String> scannedFiles = HashMultimap.create();

    public IcebergStatisticProvider() {
    }

    public Statistics getTableStatistics(IcebergTable icebergTable,
                                         Map<ColumnRefOperator, Column> colRefToColumnMetaMap,
                                         OptimizerContext session,
                                         ScalarOperator predicate) {
        Table nativeTable = icebergTable.getNativeTable();
        Statistics.Builder statisticsBuilder = Statistics.builder();
        Optional<Snapshot> snapshot = icebergTable.getSnapshot();
        String uuid = icebergTable.getUUID();
        if (snapshot.isPresent()) {
            Set<Integer> primitiveColumnsFieldIds = nativeTable.schema().columns().stream()
                    .filter(column -> column.type().isPrimitiveType())
                    .map(Types.NestedField::fieldId).collect(Collectors.toSet());
            Map<Integer, Long> colIdToNdvs = new HashMap<>();
            if (session != null && session.getSessionVariable().enableReadIcebergPuffinNdv()) {
                colIdToNdvs = readNumDistinctValues(icebergTable, primitiveColumnsFieldIds);
                if (uuidToPartitionFieldIdToValues.containsKey(uuid) && !uuidToPartitionFieldIdToValues.get(uuid).isEmpty()) {
                    HashMultimap<Integer, Object> partitionFieldIdToValue = uuidToPartitionFieldIdToValues.get(uuid);
                    Map<Integer, Long> partitionSourceIdToNdv = new HashMap<>();
                    for (PartitionField partitionField : nativeTable.spec().fields()) {
                        int sourceId = partitionField.sourceId();
                        int fieldId = partitionField.fieldId();
                        if (partitionFieldIdToValue.containsKey(fieldId)) {
                            partitionSourceIdToNdv.put(sourceId, (long) partitionFieldIdToValue.get(fieldId).size());
                        }
                    }
                    colIdToNdvs.putAll(partitionSourceIdToNdv);
                }
            }

            IcebergFilter key = IcebergFilter.of(icebergTable.getRemoteDbName(), icebergTable.getRemoteTableName(),
                    snapshot.get().snapshotId(), predicate);
            IcebergFileStats icebergFileStats;
            if (!icebergFileStatistics.containsKey(key)) {
                icebergFileStats = new IcebergFileStats(1);
            } else {
                icebergFileStats = icebergFileStatistics.get(key);
            }

            statisticsBuilder.setOutputRowCount(icebergFileStats.getRecordCount());
            statisticsBuilder.addColumnStatistics(buildColumnStatistics(
                    nativeTable, colRefToColumnMetaMap, icebergFileStats, colIdToNdvs));
        } else {
            // empty table
            statisticsBuilder.setOutputRowCount(1);
            statisticsBuilder.addColumnStatistics(buildUnknownColumnStatistics(colRefToColumnMetaMap.keySet()));
        }
        return statisticsBuilder.build();
    }

    public Map<ColumnRefOperator, ColumnStatistic> buildUnknownColumnStatistics(Set<ColumnRefOperator> columns) {
        return columns.stream().collect(Collectors.toMap(column -> column, column -> ColumnStatistic.unknown()));
    }

    public void updateIcebergFileStats(IcebergTable icebergTable, FileScanTask fileScanTask,
                                       Map<Integer, Type.PrimitiveType> idToTypeMapping,
                                       List<Types.NestedField> nonPartitionPrimitiveColumns,
                                       IcebergFilter key) {
        String uuid = icebergTable.getUUID();

        Table nativeTable = icebergTable.getNativeTable();
        List<PartitionField> partitionFields = nativeTable.spec().fields();

        DataFile dataFile = fileScanTask.file();

        // ignore this data file.
        if (dataFile.recordCount() == 0) {
            return;
        }

        if (scannedFiles.containsEntry(key, dataFile.path().toString())) {
            return;
        }

        scannedFiles.put(key, dataFile.path().toString());

        PartitionData partitionData = (PartitionData) fileScanTask.file().partition();
        for (int i = 0; i < partitionData.size(); i++) {
            Types.NestedField nestedField;
            try {
                nestedField = partitionData.getPartitionType().fields().get(i);
            } catch (Exception e) {
                LOG.error("Can not find partition field");
                continue;
            }
            if (nestedField == null) {
                LOG.error("Can not find partition field");
                continue;
            }

            int fieldId = nestedField.fieldId();
            Object partitionValue = partitionData.get(i);
            if (partitionValue == null) {
                LOG.error("Can not find partition value");
                continue;
            }

            HashMultimap<Integer, Object> idToValues = uuidToPartitionFieldIdToValues.computeIfAbsent(
                    uuid, (ignored) -> HashMultimap.create());
            idToValues.put(fieldId, partitionValue);
        }

        IcebergFileStats icebergFileStats;
        if (!icebergFileStatistics.containsKey(key)) {
            icebergFileStats = new IcebergFileStats(idToTypeMapping,
                    nonPartitionPrimitiveColumns,
                    dataFile.recordCount(),
                    dataFile.fileSizeInBytes(),
                    IcebergFileStats.toMap(idToTypeMapping, dataFile.lowerBounds()),
                    IcebergFileStats.toMap(idToTypeMapping, dataFile.upperBounds()),
                    dataFile.nullValueCounts(),
                    dataFile.columnSizes());
            icebergFileStatistics.put(key, icebergFileStats);
        } else {
            icebergFileStats = icebergFileStatistics.get(key);
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

    private Map<ColumnRefOperator, ColumnStatistic> buildColumnStatistics(
            Table nativeTable, Map<ColumnRefOperator, Column> colRefToColumnMetaMap,
            IcebergFileStats icebergFileStats, Map<Integer, Long> colIdToNdv) {
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

            columnStatistics.put(columnList.get(0), buildColumnStatistic(
                    idColumn.getKey(), colRefToColumnMetaMap.get(columnList.get(0)), icebergFileStats, colIdToNdv));
        }

        return columnStatistics;
    }

    private ColumnStatistic buildColumnStatistic(
            Integer fieldId,
            Column column,
            IcebergFileStats icebergStats,
            Map<Integer, Long> colIdToNdv) {
        ColumnStatistic.Builder builder = ColumnStatistic.builder();
        if (!column.getType().isStringType()) {
            Optional<Double> minValue = icebergStats.getMinValue(fieldId);
            minValue.ifPresent(builder::setMinValue);

            Optional<Double> maxValue = icebergStats.getMaxValue(fieldId);
            maxValue.ifPresent(builder::setMaxValue);
        }

        Long nullCount = icebergStats.getNullCounts() == null ? null : icebergStats.getNullCounts().get(fieldId);
        if (nullCount != null) {
            builder.setNullsFraction(nullCount * 1.0 / Math.max(icebergStats.getRecordCount(), 1));
        } else {
            builder.setNullsFraction(0);
        }

        builder.setAverageRowSize(1);

        Long ndv = colIdToNdv.get(fieldId);
        if (ndv != null) {
            builder.setDistinctValuesCount(Math.max(Math.min(ndv, icebergStats.getRecordCount()), 1));
            builder.setType(ColumnStatistic.StatisticType.ESTIMATE);
        } else {
            builder.setDistinctValuesCount(1);
            builder.setType(ColumnStatistic.StatisticType.UNKNOWN);
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

    public static Map<Integer, Long> readNumDistinctValues(IcebergTable icebergTable, Set<Integer> columnIds) {
        Map<Integer, Long> colIdToNdv = new HashMap<>();
        Set<Integer> remainingColumnIds = new HashSet<>(columnIds);

        long snapshotId;
        if (icebergTable.getSnapshot().isPresent()) {
            snapshotId = icebergTable.getSnapshot().get().snapshotId();
        } else {
            return colIdToNdv;
        }

        getLatestStatsFile(icebergTable.getNativeTable(), snapshotId).ifPresent(statisticsFile -> {
            Map<Integer, BlobMetadata> colIdToBlobMeta = statisticsFile.blobMetadata().stream()
                    .filter(blobMetadata -> blobMetadata.type().equals(StandardBlobTypes.APACHE_DATASKETCHES_THETA_V1))
                    .filter(blobMetadata -> blobMetadata.fields().size() == 1)
                    .filter(blobMetadata -> remainingColumnIds.contains(getOnlyElement(blobMetadata.fields())))
                    .collect(toImmutableMap(blobMetadata -> getOnlyElement(blobMetadata.fields()), identity()));

            for (Map.Entry<Integer, BlobMetadata> entry : colIdToBlobMeta.entrySet()) {
                int colId = entry.getKey();
                BlobMetadata blobMetadata = entry.getValue();
                String ndv = blobMetadata.properties().get(NDV_PROP);
                remainingColumnIds.remove(colId);
                if (ndv != null) {
                    colIdToNdv.put(colId, Long.parseLong(ndv));
                }
            }
        });

        return colIdToNdv;
    }

    public static Optional<StatisticsFile> getLatestStatsFile(Table table, long snapshotId) {
        if (table.statisticsFiles().isEmpty()) {
            return Optional.empty();
        }

        Map<Long, StatisticsFile> snapshotIdToStatsFile = table.statisticsFiles().stream()
                .collect(toMap(
                        StatisticsFile::snapshotId,
                        identity(),
                        (colId, sf) -> {
                            throw new StarRocksConnectorException("Unexpected duplicate statistics files %s, %s", colId, sf);
                        }));

        return stream(lookupSnapshots(table, snapshotId))
                .map(snapshotIdToStatsFile::get)
                .filter(Objects::nonNull)
                .findFirst();
    }

    private static Iterator<Long> lookupSnapshots(Table icebergTable, long startingSnapshotId) {
        return new AbstractSequentialIterator<Long>(startingSnapshotId) {
            @Override
            protected Long computeNext(Long parentId) {
                requireNonNull(parentId, "previous is null");

                @Nullable
                Snapshot snapshot = icebergTable.snapshot(parentId);
                if (snapshot == null) {
                    return null;
                }
                if (snapshot.parentId() == null) {
                    return null;
                }

                return verifyNotNull(snapshot.parentId(), "snapshot parent id is null");
            }
        };
    }
}