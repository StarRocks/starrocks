// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.external.iceberg.cost;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.starrocks.external.iceberg.IcebergUtil;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.expressions.UnboundPredicate;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.stream.Collectors.toSet;

public class IcebergTableStatisticCalculator {
    private static final Logger LOG = LogManager.getLogger(IcebergTableStatisticCalculator.class);

    private final Table icebergTable;

    private IcebergTableStatisticCalculator(Table icebergTable) {
        this.icebergTable = icebergTable;
    }

    public static IcebergTableStats getTableStatistics(List<UnboundPredicate> icebergPredicates, Table icebergTable) {
        return new IcebergTableStatisticCalculator(icebergTable)
                .makeTableStatistics(icebergPredicates);
    }

    private IcebergTableStats makeTableStatistics(List<UnboundPredicate> icebergPredicates) {
        LOG.debug("Begin to make iceberg table statistics!");
        Optional<Snapshot> snapshot = IcebergUtil.getCurrentTableSnapshot(icebergTable, true);
        if (!snapshot.isPresent()) {
            return IcebergTableStats.empty();
        }

        List<Types.NestedField> columns = icebergTable.schema().columns();

        Map<Integer, Type.PrimitiveType> idToTypeMapping = columns.stream()
                .filter(column -> column.type().isPrimitiveType())
                .collect(Collectors.toMap(Types.NestedField::fieldId, column -> column.type().asPrimitiveType()));
        List<PartitionField> partitionFields = icebergTable.spec().fields();

        Set<Integer> identityPartitionIds = IcebergUtil.getIdentityPartitions(icebergTable.spec()).keySet().stream()
                .map(PartitionField::sourceId)
                .collect(toSet());

        List<Types.NestedField> nonPartitionPrimitiveColumns = columns.stream()
                .filter(column -> !identityPartitionIds.contains(column.fieldId()) && column.type().isPrimitiveType())
                .collect(toImmutableList());

        Map<Integer, String> idToColumnNames = columns.stream()
                .filter(column -> column.type().isPrimitiveType())
                .collect(Collectors.toMap(Types.NestedField::fieldId, column -> column.name()));

        TableScan tableScan = IcebergUtil.getTableScan(icebergTable,
                snapshot.get(), icebergPredicates, true);

        Summary summary = null;
        try (CloseableIterable<FileScanTask> fileScanTasks = tableScan.planFiles()) {
            for (FileScanTask fileScanTask : fileScanTasks) {
                DataFile dataFile = fileScanTask.file();
                if (summary == null) {
                    summary = new Summary(
                            idToTypeMapping,
                            nonPartitionPrimitiveColumns,
                            dataFile.partition(),
                            dataFile.recordCount(),
                            dataFile.fileSizeInBytes(),
                            Summary.toMap(idToTypeMapping, dataFile.lowerBounds()),
                            Summary.toMap(idToTypeMapping, dataFile.upperBounds()),
                            dataFile.nullValueCounts(),
                            dataFile.columnSizes());
                } else {
                    summary.incrementFileCount();
                    summary.incrementRecordCount(dataFile.recordCount());
                    summary.incrementSize(dataFile.fileSizeInBytes());
                    updateSummaryMin(summary, partitionFields, Summary.toMap(idToTypeMapping,
                            dataFile.lowerBounds()), dataFile.nullValueCounts(), dataFile.recordCount());
                    updateSummaryMax(summary, partitionFields, Summary.toMap(idToTypeMapping,
                            dataFile.upperBounds()), dataFile.nullValueCounts(), dataFile.recordCount());
                    summary.updateNullCount(dataFile.nullValueCounts());
                    updateColumnSizes(summary, dataFile.columnSizes());
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        if (summary == null) {
            return IcebergTableStats.empty();
        }

        ImmutableMap.Builder<String, IcebergColumnStats> columnStatsBuilder = ImmutableMap.builder();
        double recordCount = summary.getRecordCount();
        for (Map.Entry<Integer, String> idColumn : idToColumnNames.entrySet()) {
            int fieldId = idColumn.getKey();
            IcebergColumnStats columnStats = new IcebergColumnStats();
            Long nullCount = summary.getNullCounts().get(fieldId);
            if (nullCount != null) {
                columnStats.setNumNulls(nullCount);
            }
            if (summary.getColumnSizes() != null) {
                Long columnSize = summary.getColumnSizes().get(fieldId);
                if (columnSize != null) {
                    columnStats.setAvgSize(columnSize / recordCount);
                }
            }
            Object min = summary.getMinValues().get(fieldId);
            Object max = summary.getMaxValues().get(fieldId);
            if (min instanceof Number && max instanceof Number) {
                LOG.debug("Iceberg min value " + ((Number) min).doubleValue());
                LOG.debug("Iceberg max value " + ((Number) max).doubleValue());
                columnStats.setMinValue(((Number) min).doubleValue());
                columnStats.setMaxValue(((Number) max).doubleValue());
            }
            columnStatsBuilder.put(idColumn.getValue(), columnStats);
        }
        LOG.debug("Finish make iceberg table statistics!");
        return new IcebergTableStats(recordCount, columnStatsBuilder.build());
    }

    public List<Type> partitionTypes(List<PartitionField> partitionFields,
                                     Map<Integer, Type.PrimitiveType> idToTypeMapping) {
        ImmutableList.Builder<Type> partitionTypeBuilder = ImmutableList.builder();
        for (PartitionField partitionField : partitionFields) {
            Type.PrimitiveType sourceType = idToTypeMapping.get(partitionField.sourceId());
            Type type = partitionField.transform().getResultType(sourceType);
            partitionTypeBuilder.add(type);
        }
        return partitionTypeBuilder.build();
    }

    public void updateColumnSizes(Summary summary, Map<Integer, Long> addedColumnSizes) {
        Map<Integer, Long> columnSizes = summary.getColumnSizes();
        if (!summary.hasValidColumnMetrics() || columnSizes == null || addedColumnSizes == null) {
            return;
        }
        for (Types.NestedField column : summary.getNonPartitionPrimitiveColumns()) {
            int id = column.fieldId();

            Long addedSize = addedColumnSizes.get(id);
            if (addedSize != null) {
                columnSizes.put(id, addedSize + columnSizes.getOrDefault(id, 0L));
            }
        }
    }

    private void updateSummaryMin(Summary summary,
                                  List<PartitionField> partitionFields,
                                  Map<Integer, Object> lowerBounds,
                                  Map<Integer, Long> nullCounts,
                                  long recordCount) {
        summary.updateStats(summary.getMinValues(), lowerBounds, nullCounts, recordCount, i -> (i > 0));
        updatePartitionedStats(summary, partitionFields, summary.getMinValues(), lowerBounds, i -> (i > 0));
    }

    private void updateSummaryMax(Summary summary,
                                  List<PartitionField> partitionFields,
                                  Map<Integer, Object> upperBounds,
                                  Map<Integer, Long> nullCounts,
                                  long recordCount) {
        summary.updateStats(summary.getMaxValues(), upperBounds, nullCounts, recordCount, i -> (i < 0));
        updatePartitionedStats(summary, partitionFields, summary.getMaxValues(), upperBounds, i -> (i < 0));
    }

    private void updatePartitionedStats(
            Summary summary,
            List<PartitionField> partitionFields,
            Map<Integer, Object> current,
            Map<Integer, Object> newStats,
            Predicate<Integer> predicate) {
        for (PartitionField field : partitionFields) {
            int id = field.sourceId();
            if (summary.getCorruptedStats().contains(id)) {
                continue;
            }

            Object newValue = newStats.get(id);
            if (newValue == null) {
                continue;
            }

            Object oldValue = current.putIfAbsent(id, newValue);
            if (oldValue != null) {
                Comparator<Object> comparator = Comparators.forType(summary.getIdToTypeMapping().get(id));
                if (predicate.test(comparator.compare(oldValue, newValue))) {
                    current.put(id, newValue);
                }
            }
        }
    }
}
