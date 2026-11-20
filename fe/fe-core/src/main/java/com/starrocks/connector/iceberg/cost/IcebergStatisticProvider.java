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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.HashMultimap;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.common.tvr.TvrTableDelta;
import com.starrocks.common.tvr.TvrVersionRange;
import com.starrocks.connector.GetRemoteFilesParams;
import com.starrocks.connector.PredicateSearchKey;
import com.starrocks.connector.statistics.ConnectorNdvEstimator;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.Statistics;
import org.apache.iceberg.BlobMetadata;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataOperations;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotSummary;
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
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static com.google.common.collect.Iterables.getOnlyElement;
import static com.starrocks.connector.ColumnTypeConverter.fromIcebergType;

public class IcebergStatisticProvider {
    private static final Logger LOG = LogManager.getLogger(IcebergStatisticProvider.class);
    public static final String NDV_PROP = "ndv";
    // How far the row count may move from the analyzed snapshot before its Puffin NDV is too stale to use.
    static final double PUFFIN_NDV_MAX_ROW_DRIFT = 0.1;

    // table uuid -> <partition column id -> partition column values>
    private final Map<String, HashMultimap<Integer, Object>> uuidToPartitionFieldIdToValues = new HashMap<>();
    private final Map<PredicateSearchKey, IcebergFileStats> icebergFileStatistics = new HashMap<>();
    private final Map<PredicateSearchKey, Set<String>> scannedFiles = new HashMap<>();

    public IcebergStatisticProvider() {
    }

    @VisibleForTesting
    void putIcebergFileStats(PredicateSearchKey key, IcebergFileStats fileStats) {
        icebergFileStatistics.put(key, fileStats);
    }

    public Statistics getCardinalityStats(
            Map<ColumnRefOperator, Column> colRefToColumnMetaMap, List<FileScanTask> fileScanTasks) {
        Statistics.Builder statisticsBuilder = Statistics.builder();
        long cardinality = 0;
        Set<String> currentFiles = new HashSet<>();
        for (FileScanTask scanTask : fileScanTasks) {
            DataFile dataFile = scanTask.file();
            String filePath = dataFile.path().toString();
            if (currentFiles.contains(filePath)) {
                continue;
            }

            currentFiles.add(filePath);
            cardinality += dataFile.recordCount();
        }

        statisticsBuilder.setOutputRowCount(cardinality);
        statisticsBuilder.addColumnStatistics(buildUnknownColumnStatistics(colRefToColumnMetaMap.keySet()));
        statisticsBuilder.setStatsSource(Statistics.StatsSource.TABLE_METADATA);
        return statisticsBuilder.build();
    }

    // Build row-count-only statistics from a pre-computed cardinality (e.g. manifest-pruned row count),
    // without enumerating DataFiles. Column statistics are left UNKNOWN here; NDV estimation is handled
    // separately by the column-statistics path.
    public Statistics buildRowCountStatistics(
            Map<ColumnRefOperator, Column> colRefToColumnMetaMap, long rowCount) {
        Statistics.Builder statisticsBuilder = Statistics.builder();
        statisticsBuilder.setOutputRowCount(Math.max(rowCount, 1));
        statisticsBuilder.addColumnStatistics(buildUnknownColumnStatistics(colRefToColumnMetaMap.keySet()));
        statisticsBuilder.setStatsSource(Statistics.StatsSource.TABLE_METADATA);
        return statisticsBuilder.build();
    }

    public Statistics getTableStatistics(IcebergTable icebergTable,
                                         Map<ColumnRefOperator, Column> colRefToColumnMetaMap,
                                         OptimizerContext session,
                                         GetRemoteFilesParams params) {
        Table nativeTable = icebergTable.getNativeTable();
        Statistics.Builder statisticsBuilder = Statistics.builder();
        String uuid = icebergTable.getUUID();
        TvrVersionRange version = params.getTableVersionRange();
        if (version.end().isPresent()) {
            // The read schema of the queried snapshot: a renamed column must not pick up another column's statistics.
            Schema readSchema = icebergTable.getReadSchema();
            Set<Integer> primitiveColumnsFieldIds = readSchema.columns().stream()
                    .filter(column -> column.type().isPrimitiveType())
                    .map(Types.NestedField::fieldId).collect(Collectors.toSet());
            Map<Integer, Long> colIdToNdvs = new HashMap<>();
            // A Puffin NDV describes a whole snapshot, not the rows an incremental delta added.
            boolean isIncrementalDelta = version instanceof TvrTableDelta && version.start() != null &&
                    version.start().isPresent();
            if (session != null && session.getSessionVariable().enableReadIcebergPuffinNdv()) {
                if (!isIncrementalDelta) {
                    colIdToNdvs = readFreshNumDistinctValues(nativeTable, primitiveColumnsFieldIds, version);
                }
                // the partition values come from the scanned files, so they describe a delta too
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

            PredicateSearchKey key = PredicateSearchKey.of(icebergTable.getCatalogDBName(),
                    icebergTable.getCatalogTableName(), params);
            IcebergFileStats icebergFileStats;
            if (!icebergFileStatistics.containsKey(key)) {
                icebergFileStats = new IcebergFileStats(1);
            } else {
                icebergFileStats = icebergFileStatistics.get(key);
            }

            statisticsBuilder.setOutputRowCount(icebergFileStats.getRecordCount());
            statisticsBuilder.addColumnStatistics(buildColumnStatistics(
                    readSchema, colRefToColumnMetaMap, icebergFileStats, colIdToNdvs));
            statisticsBuilder.setStatsSource(Statistics.StatsSource.TABLE_METADATA);
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
                                       PredicateSearchKey key) {
        String uuid = icebergTable.getUUID();

        Table nativeTable = icebergTable.getNativeTable();
        List<PartitionField> partitionFields = nativeTable.spec().fields();

        DataFile dataFile = fileScanTask.file();

        // ignore this data file.
        if (dataFile.recordCount() == 0) {
            return;
        }

        Set<String> files = scannedFiles.computeIfAbsent(key, ignored -> new HashSet<>());
        if (files.contains(dataFile.path().toString())) {
            return;
        }

        files.add(dataFile.path().toString());

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
            updateColumnSizes(icebergFileStats, dataFile.columnSizes(), dataFile.recordCount());
        }
    }

    private Map<ColumnRefOperator, ColumnStatistic> buildColumnStatistics(
            Schema readSchema, Map<ColumnRefOperator, Column> colRefToColumnMetaMap,
            IcebergFileStats icebergFileStats, Map<Integer, Long> colIdToNdv) {
        Map<ColumnRefOperator, ColumnStatistic> columnStatistics = new HashMap<>();
        List<Types.NestedField> columns = readSchema.columns();
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

        // when we rewrit plan, we will add some artificial columns which not eixst in iceberg table,
        // and we will mark those columns as unknown column statistics.
        for (ColumnRefOperator c : colRefToColumnMetaMap.keySet()) {
            if (!columnStatistics.containsKey(c)) {
                columnStatistics.put(c, ColumnStatistic.unknown());
            }
        }

        return columnStatistics;
    }

    private ColumnStatistic buildColumnStatistic(
            Integer fieldId,
            Column column,
            IcebergFileStats icebergStats,
            Map<Integer, Long> colIdToNdv) {
        ColumnStatistic.Builder builder = ColumnStatistic.builder();

        if (icebergStats.canUseStats(fieldId, icebergStats.getMinValues())) {
            if (column.getType().isStringType()) {
                String minString = icebergStats.getMinValues().get(fieldId).toString();
                builder.setMinString(minString);
            } else {
                Optional<Double> res = icebergStats.getMinValue(fieldId);
                res.ifPresent(builder::setMinValue);
            }
        }

        if (icebergStats.canUseStats(fieldId, icebergStats.getMaxValues())) {
            if (column.getType().isStringType()) {
                String maxString = icebergStats.getMaxValues().get(fieldId).toString();
                builder.setMaxString(maxString);
            } else {
                Optional<Double> res = icebergStats.getMaxValue(fieldId);
                res.ifPresent(builder::setMaxValue);
            }
        }

        Long nullCount = icebergStats.getNullCounts() == null ? null : icebergStats.getNullCounts().get(fieldId);
        if (nullCount != null) {
            builder.setNullsFraction(nullCount * 1.0 / Math.max(icebergStats.getRecordCount(), 1));
        } else {
            builder.setNullsFraction(0);
        }

        Map<Integer, Long> columnSizes = icebergStats.getColumnSizes();
        long columnSizeRecordCount = icebergStats.getColumnSizeRecordCount(fieldId);
        if (columnSizes != null && columnSizes.containsKey(fieldId) && columnSizeRecordCount > 0) {
            // columnSizes from Iceberg is compressed (physical) size, so use typeSize as lower bound
            // Use per-field columnSizeRecordCount to get accurate average when
            // some files lack column size metrics for specific fields
            double physicalSize = (double) columnSizes.get(fieldId) / columnSizeRecordCount;
            double logicalSize = column.getType().getTypeSize();
            builder.setAverageRowSize(Math.max(physicalSize, logicalSize));
        } else {
            builder.setAverageRowSize(column.getType().getTypeSize());
        }

        Long ndv = colIdToNdv.get(fieldId);
        if (ndv != null) {
            builder.setDistinctValuesCount(Math.max(Math.min(ndv, icebergStats.getRecordCount()), 1));
            builder.setType(ColumnStatistic.StatisticType.ESTIMATE);
        } else {
            double estimatedNdv = estimateNdvFallback(fieldId, icebergStats);
            builder.setDistinctValuesCount(estimatedNdv);
            builder.setType(ColumnStatistic.StatisticType.ESTIMATE);
        }

        return builder.build();
    }

    // Estimate NDV when Puffin stats are unavailable.
    // Delegates to ConnectorNdvEstimator which implements the same three-tier strategy
    // (range → size → type-fraction) in a connector-agnostic way.
    private double estimateNdvFallback(Integer fieldId, IcebergFileStats icebergStats) {
        ConnectorNdvEstimator.TypeCategory category = toTypeCategory(fieldId, icebergStats);
        Optional<Double> minVal = icebergStats.getMinValue(fieldId);
        Optional<Double> maxVal = icebergStats.getMaxValue(fieldId);
        double minDouble = minVal.orElse(Double.NaN);
        double maxDouble = maxVal.orElse(Double.NaN);

        Map<Integer, Long> columnSizes = icebergStats.getColumnSizes();
        long colSizeBytes = (columnSizes != null && columnSizes.containsKey(fieldId))
                ? columnSizes.get(fieldId) : -1L;
        long colSizeRcnt = icebergStats.getColumnSizeRecordCount(fieldId);

        int typeWidthHint = icebergTypeWidthBytes(fieldId, icebergStats);
        return ConnectorNdvEstimator.estimate(
                category, minDouble, maxDouble, colSizeBytes, colSizeRcnt,
                icebergStats.getRecordCount(), typeWidthHint);
    }

    /**
     * Returns the physical storage width (bytes) of an Iceberg primitive type for use as a
     * Tier-2 size-NDV hint.  Preserves the 4-byte vs 8-byte distinction that was in the
     * original per-type estimator before the shared-utility refactor.
     */
    private static int icebergTypeWidthBytes(Integer fieldId, IcebergFileStats icebergStats) {
        Map<Integer, Type.PrimitiveType> typeMapping = icebergStats.getIdToTypeMapping();
        if (typeMapping == null) {
            return 0;
        }
        Type.PrimitiveType type = typeMapping.get(fieldId);
        if (type instanceof Types.BooleanType) {
            return 1;
        } else if (type instanceof Types.LongType || type instanceof Types.DoubleType
                || type instanceof Types.TimestampType || type instanceof Types.TimeType) {
            return 8;
        } else if (type instanceof Types.IntegerType || type instanceof Types.FloatType
                || type instanceof Types.DateType) {
            return 4;
        }
        return 0; // STRING/BINARY/other: let the category default (4) apply
    }

    private static ConnectorNdvEstimator.TypeCategory toTypeCategory(Integer fieldId, IcebergFileStats icebergStats) {
        Map<Integer, Type.PrimitiveType> typeMapping = icebergStats.getIdToTypeMapping();
        if (typeMapping == null) {
            return ConnectorNdvEstimator.TypeCategory.OTHER;
        }
        Type.PrimitiveType type = typeMapping.get(fieldId);
        if (type instanceof Types.BooleanType) {
            return ConnectorNdvEstimator.TypeCategory.BOOLEAN;
        } else if (type instanceof Types.IntegerType || type instanceof Types.LongType) {
            return ConnectorNdvEstimator.TypeCategory.INTEGER_LIKE;
        } else if (type instanceof Types.FloatType || type instanceof Types.DoubleType) {
            return ConnectorNdvEstimator.TypeCategory.FLOAT_LIKE;
        } else if (type instanceof Types.DateType) {
            // IcebergFileStats converts epoch-days → epoch-seconds (×86400)
            return ConnectorNdvEstimator.TypeCategory.DATE_IN_EPOCH_SECONDS;
        } else if (type instanceof Types.TimestampType) {
            // IcebergFileStats converts epoch-µs → epoch-seconds (÷1_000_000)
            return ConnectorNdvEstimator.TypeCategory.TIMESTAMP_IN_EPOCH_MICROS;
        } else if (type instanceof Types.StringType || type instanceof Types.BinaryType) {
            return ConnectorNdvEstimator.TypeCategory.STRING_LIKE;
        }
        return ConnectorNdvEstimator.TypeCategory.OTHER;
    }

    public void updateColumnSizes(IcebergFileStats icebergFileStats, Map<Integer, Long> addedColumnSizes,
                                  long recordCount) {
        Map<Integer, Long> columnSizes = icebergFileStats.getColumnSizes();
        if (!icebergFileStats.hasValidColumnMetrics() || columnSizes == null || addedColumnSizes == null) {
            return;
        }
        Set<Integer> updatedFieldIds = new HashSet<>();
        for (Types.NestedField column : icebergFileStats.getNonPartitionPrimitiveColumns()) {
            int id = column.fieldId();

            Long addedSize = addedColumnSizes.get(id);
            if (addedSize != null) {
                columnSizes.put(id, addedSize + columnSizes.getOrDefault(id, 0L));
                updatedFieldIds.add(id);
            }
        }
        if (!updatedFieldIds.isEmpty()) {
            icebergFileStats.incrementColumnSizeRecordCounts(updatedFieldIds, recordCount);
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

        Set<Integer> corruptedStats = icebergFileStats.getCorruptedStats(current);
        for (PartitionField field : partitionFields) {
            int id = field.sourceId();
            if (corruptedStats.contains(id)) {
                continue;
            }

            Object newValue = newStats.get(id);
            if (newValue == null) {
                current.remove(id);
                corruptedStats.add(id);
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

    // The Puffin NDVs that still describe the queried snapshot. An NDV is fresh when the snapshot it was computed
    // on (the blob's source snapshot) is the queried one, or an ancestor the data has barely moved from since: every
    // snapshot in between only appended rows or rewrote files (compaction keeps the rows), and total-records moved by
    // at most PUFFIN_NDV_MAX_ROW_DRIFT. An overwrite or a delete in between, or a missing total-records summary,
    // makes older NDVs stale. A blob a writer carried over to a newer statistics file (e.g. Trino keeps the blobs of
    // unchanged columns across OPTIMIZE) is judged by its own source snapshot; if that snapshot has expired, the
    // statistics file's snapshot stands in for it.
    @VisibleForTesting
    static Map<Integer, Long> readFreshNumDistinctValues(Table table, Set<Integer> columnIds, TvrVersionRange version) {
        if (version == null || version.isEmpty() || table.statisticsFiles().isEmpty()) {
            return new HashMap<>();
        }
        Snapshot queried = table.snapshot(version.to().getVersion());
        if (queried == null) {
            return new HashMap<>();
        }
        // Snapshots the queried data has barely moved from, nearest first.
        Set<Long> freshSnapshotIds = new LinkedHashSet<>();
        for (Snapshot snapshot = queried; snapshot != null && withinRowDrift(snapshot, queried); ) {
            freshSnapshotIds.add(snapshot.snapshotId());
            if (!DataOperations.APPEND.equals(snapshot.operation())
                    && !DataOperations.REPLACE.equals(snapshot.operation())) {
                break;
            }
            Long parentId = snapshot.parentId();
            snapshot = parentId == null ? null : table.snapshot(parentId);
        }
        Map<Long, StatisticsFile> snapshotIdToStatsFile = new HashMap<>();
        for (StatisticsFile statisticsFile : table.statisticsFiles()) {
            snapshotIdToStatsFile.put(statisticsFile.snapshotId(), statisticsFile);
        }
        for (Long snapshotId : freshSnapshotIds) {
            StatisticsFile statisticsFile = snapshotIdToStatsFile.get(snapshotId);
            if (statisticsFile != null) {
                return readNumDistinctValues(statisticsFile, columnIds,
                        blob -> freshSnapshotIds.contains(blob.sourceSnapshotId())
                                || table.snapshot(blob.sourceSnapshotId()) == null);
            }
        }
        return new HashMap<>();
    }

    private static Map<Integer, Long> readNumDistinctValues(StatisticsFile statisticsFile, Set<Integer> columnIds,
                                                            Predicate<BlobMetadata> blobFilter) {
        Map<Integer, Long> colIdToNdv = new HashMap<>();
        for (BlobMetadata blobMetadata : statisticsFile.blobMetadata()) {
            if (!blobMetadata.type().equals(StandardBlobTypes.APACHE_DATASKETCHES_THETA_V1)
                    || blobMetadata.fields().size() != 1
                    || !columnIds.contains(getOnlyElement(blobMetadata.fields()))
                    || !blobFilter.test(blobMetadata)) {
                continue;
            }
            String ndv = blobMetadata.properties().get(NDV_PROP);
            if (ndv == null) {
                continue;
            }
            try {
                colIdToNdv.put(getOnlyElement(blobMetadata.fields()), Long.parseLong(ndv));
            } catch (NumberFormatException e) {
                // a malformed property must not fail the query; the column falls back to the estimate
                LOG.warn("Ignore malformed puffin ndv {} of field {} in {}", ndv, blobMetadata.fields(),
                        statisticsFile.path());
            }
        }
        return colIdToNdv;
    }

    private static boolean withinRowDrift(Snapshot analyzed, Snapshot queried) {
        if (analyzed.snapshotId() == queried.snapshotId()) {
            return true;
        }
        Long analyzedRows = totalRecords(analyzed);
        Long queriedRows = totalRecords(queried);
        if (analyzedRows == null || queriedRows == null) {
            return false;
        }
        return Math.abs(queriedRows - analyzedRows) <= PUFFIN_NDV_MAX_ROW_DRIFT * Math.max(analyzedRows, 1);
    }

    private static Long totalRecords(Snapshot snapshot) {
        String value = snapshot.summary() == null ? null : snapshot.summary().get(SnapshotSummary.TOTAL_RECORDS_PROP);
        if (value == null) {
            return null;
        }
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            return null;
        }
    }
}