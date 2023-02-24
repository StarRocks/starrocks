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

import com.google.common.collect.ImmutableList;
import com.starrocks.catalog.Column;
import com.starrocks.connector.iceberg.IcebergUtil;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.Statistics;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.stream.Collectors.toSet;

// TODO @caneGuy add cache for statistics, currently getting stats from iceberg metafiles is not time consuming
public class IcebergTableStatisticCalculator {
    private static final Logger LOG = LogManager.getLogger(IcebergTableStatisticCalculator.class);

    private final Table icebergTable;

    private IcebergTableStatisticCalculator(Table icebergTable) {
        this.icebergTable = icebergTable;
    }

    public static Statistics getTableStatistics(Expression icebergPredicate,
                                                Table icebergTable,
                                                Map<ColumnRefOperator, Column> colRefToColumnMetaMap) {
        return new IcebergTableStatisticCalculator(icebergTable)
                .makeTableStatistics(icebergPredicate, colRefToColumnMetaMap);
    }

    public static List<ColumnStatistic> getColumnStatistics(Expression icebergPredicate,
                                                            Table icebergTable,
                                                            Map<ColumnRefOperator, Column> colRefToColumnMetaMap) {
        return new IcebergTableStatisticCalculator(icebergTable)
                .makeColumnStatistics(icebergPredicate, colRefToColumnMetaMap);
    }

    private List<ColumnStatistic> makeColumnStatistics(Expression icebergPredicate,
                                                       Map<ColumnRefOperator, Column> colRefToColumnMetaMap) {
        List<ColumnStatistic> columnStatistics = new ArrayList<>();
        List<Types.NestedField> columns = icebergTable.schema().columns();
        IcebergFileStats icebergFileStats = new IcebergTableStatisticCalculator(icebergTable).
                generateIcebergFileStats(icebergPredicate, columns);

        Map<Integer, String> idToColumnNames = columns.stream().
                filter(column -> !IcebergUtil.convertColumnType(column.type()).isUnknown())
                .collect(Collectors.toMap(Types.NestedField::fieldId, Types.NestedField::name));

        double recordCount = Math.max(icebergFileStats == null ? 0 : icebergFileStats.getRecordCount(), 1);
        for (Map.Entry<Integer, String> idColumn : idToColumnNames.entrySet()) {
            List<ColumnRefOperator> columnList = colRefToColumnMetaMap.keySet().stream().filter(
                    key -> key.getName().equalsIgnoreCase(idColumn.getValue())).collect(Collectors.toList());
            if (columnList.size() != 1) {
                LOG.debug("This column is not required column name " + idColumn.getValue() + " column list size "
                        + columnList.size());
                continue;
            }

            int fieldId = idColumn.getKey();
            columnStatistics.add(
                    generateColumnStatistic(icebergFileStats, fieldId, recordCount, columnList.get(0)));
        }
        return columnStatistics;
    }

    private Statistics makeTableStatistics(Expression icebergPredicate, Map<ColumnRefOperator, Column> colRefToColumnMetaMap) {
        LOG.debug("Begin to make iceberg table statistics!");
        List<Types.NestedField> columns = icebergTable.schema().columns();
        IcebergFileStats icebergFileStats = generateIcebergFileStats(icebergPredicate, columns);

        Map<Integer, String> idToColumnNames = columns.stream()
                .filter(column -> !IcebergUtil.convertColumnType(column.type()).isUnknown())
                .collect(Collectors.toMap(Types.NestedField::fieldId, Types.NestedField::name));

        Statistics.Builder statisticsBuilder = Statistics.builder();
        double recordCount = Math.max(icebergFileStats == null ? 0 : icebergFileStats.getRecordCount(), 1);
        for (Map.Entry<Integer, String> idColumn : idToColumnNames.entrySet()) {
            List<ColumnRefOperator> columnList = colRefToColumnMetaMap.keySet().stream().filter(
                    key -> key.getName().equalsIgnoreCase(idColumn.getValue())).collect(Collectors.toList());
            if (columnList.size() != 1) {
                LOG.debug("This column is not required column name " + idColumn.getValue() + " column list size "
                        + columnList.size());
                continue;
            }

            int fieldId = idColumn.getKey();
            ColumnStatistic columnStatistic =
                    generateColumnStatistic(icebergFileStats, fieldId, recordCount, columnList.get(0));
            statisticsBuilder.addColumnStatistic(columnList.get(0), columnStatistic);
        }
        statisticsBuilder.setOutputRowCount(recordCount);
        LOG.debug("Finish to make iceberg table statistics!");
        return statisticsBuilder.build();
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

    private IcebergFileStats generateIcebergFileStats(Expression icebergPredicate,
                                                      List<Types.NestedField> columns) {
        Optional<Snapshot> snapshot = IcebergUtil.getCurrentTableSnapshot(icebergTable);
        if (!snapshot.isPresent()) {
            return null;
        }

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

        TableScan tableScan = IcebergUtil.getTableScan(icebergTable,
                snapshot.get(), icebergPredicate, true);

        IcebergFileStats icebergFileStats = null;
        for (CombinedScanTask combinedScanTask : tableScan.planTasks()) {
            for (FileScanTask fileScanTask : combinedScanTask.files()) {
                DataFile dataFile = fileScanTask.file();
                // ignore this data file.
                if (dataFile.recordCount() == 0) {
                    continue;
                }
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

        return icebergFileStats;
    }

    private ColumnStatistic generateColumnStatistic(IcebergFileStats icebergFileStats,
                                                    int fieldId,
                                                    double recordCount,
                                                    ColumnRefOperator columnRefOperator) {
        return ColumnStatistic.unknown();
    }
}
