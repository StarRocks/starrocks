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


package com.starrocks.connector.hive;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.starrocks.analysis.DateLiteral;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.analysis.NullLiteral;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.HiveMetaStoreTable;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.connector.RemoteFileDesc;
import com.starrocks.connector.RemoteFileInfo;
import com.starrocks.connector.RemoteFileOperations;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.Statistics;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.TimeZone;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Maps.immutableEntry;
import static com.starrocks.connector.PartitionUtil.toHivePartitionName;
import static java.lang.Double.NEGATIVE_INFINITY;
import static java.lang.Double.POSITIVE_INFINITY;
import static java.lang.Double.isFinite;
import static java.lang.Double.isNaN;

public class HiveStatisticsProvider {
    private static final Logger LOG = LogManager.getLogger(HiveStatisticsProvider.class);

    private final HiveMetastoreOperations hmsOps;
    private final RemoteFileOperations fileOps;

    public HiveStatisticsProvider(HiveMetastoreOperations hmsOps, RemoteFileOperations fileOps) {
        this.hmsOps = hmsOps;
        this.fileOps = fileOps;
    }

    public Statistics getTableStatistics(
            OptimizerContext session,
            Table table,
            List<ColumnRefOperator> columns,
            List<PartitionKey> partitionKeys) {
        Statistics.Builder builder = Statistics.builder();
        HiveMetaStoreTable hmsTbl = (HiveMetaStoreTable) table;
        if (hmsTbl.isUnPartitioned()) {
            HivePartitionStats tableStats = hmsOps.getTableStatistics(hmsTbl.getDbName(), hmsTbl.getTableName());
            return createUnpartitionedStats(tableStats, columns, builder, table);
        }

        int sampleSize = getSamplePartitionSize(session);
        List<String> partitionColumnNames = hmsTbl.getPartitionColumnNames();
        List<String> partitionNames = partitionKeys.stream()
                .peek(partitionKey -> checkState(partitionKey.getKeys().size() == partitionColumnNames.size(),
                        "columns size is " + partitionColumnNames.size() +
                                " but values size is " + partitionKey.getKeys().size()))
                .map(partitionKey -> toHivePartitionName(partitionColumnNames, partitionKey))
                .collect(Collectors.toList());

        List<String> sampledPartitionNames = getPartitionsSample(partitionNames, sampleSize);
        Map<String, HivePartitionStats> partitionStatistics = hmsOps.getPartitionStatistics(table, sampledPartitionNames);

        double avgRowNumPerPartition = -1;
        double totalRowNums = -1;
        avgRowNumPerPartition = getPerPartitionRowAvgNums(partitionStatistics.values());

        if (avgRowNumPerPartition <= 0) {
            builder.setOutputRowCount(getEstimatedRowCount(table, partitionKeys));
            return builder.build();
        }

        totalRowNums = avgRowNumPerPartition * partitionKeys.size();
        builder.setOutputRowCount(totalRowNums);

        for (ColumnRefOperator columnRefOperator : columns) {
            Column column = table.getColumn(columnRefOperator.getName());
            if (partitionColumnNames.contains(columnRefOperator.getName())) {
                builder.addColumnStatistic(columnRefOperator, createPartitionColumnStatistics(
                        column, partitionKeys, partitionStatistics, partitionColumnNames, avgRowNumPerPartition, totalRowNums));
            } else {
                builder.addColumnStatistic(columnRefOperator, createDataColumnStatistics(
                        column, totalRowNums, partitionStatistics.values()));
            }
        }

        return builder.build();
    }

    public Statistics createUnpartitionedStats(
            HivePartitionStats tableStats,
            List<ColumnRefOperator> columns,
            Statistics.Builder builder,
            Table table) {
        long rowNum = tableStats.getCommonStats().getRowNums();
        if (rowNum == -1) {
            builder.setOutputRowCount(getEstimatedRowCount(table, Lists.newArrayList(new PartitionKey())));
            return builder.build();
        } else {
            builder.setOutputRowCount(rowNum);
        }

        for (ColumnRefOperator columnRefOperator : columns) {
            Column column = table.getColumn(columnRefOperator.getName());
            builder.addColumnStatistic(columnRefOperator,
                    createDataColumnStatistics(column, rowNum, Collections.singleton(tableStats)));
        }

        return builder.build();
    }

    public long getEstimatedRowCount(Table table, List<PartitionKey> partitionKeys) {
        HiveMetaStoreTable hmsTbl = (HiveMetaStoreTable) table;
        List<Partition> partitions = hmsTbl.isUnPartitioned() ?
                Lists.newArrayList(hmsOps.getPartition(hmsTbl.getDbName(), hmsTbl.getTableName(), Lists.newArrayList())) :
                Lists.newArrayList(hmsOps.getPartitionByPartitionKeys(table, partitionKeys).values());

        Optional<String> hudiBasePath = table.isHiveTable() ? Optional.empty() : Optional.of(hmsTbl.getTableLocation());
        List<RemoteFileInfo> remoteFileInfos = fileOps.getRemoteFileInfoForStats(partitions, hudiBasePath);

        long totalBytes = 0;
        for (RemoteFileInfo remoteFileInfo : remoteFileInfos) {
            for (RemoteFileDesc fileDesc : remoteFileInfo.getFiles()) {
                totalBytes += fileDesc.getLength();
            }
        }

        List<Column> dataColumns = table.getColumns().stream()
                .filter(column -> hmsTbl.getDataColumnNames().contains(column.getName()))
                .collect(Collectors.toList());

        if (totalBytes <= 0) {
            return 1;
        }

        long presentRowNums = totalBytes / dataColumns.stream().mapToInt(column -> column.getType().getTypeSize()).sum();
        long presentPartitionSize = remoteFileInfos.size();
        return presentRowNums / presentPartitionSize * partitionKeys.size();
    }

    public Statistics createUnknownStatistics(
            Table table,
            List<ColumnRefOperator> columns,
            List<PartitionKey> partitionKeys,
            double presentRowNums) {
        Statistics.Builder builder = Statistics.builder();
        for (ColumnRefOperator columnRefOperator : columns) {
            builder.addColumnStatistic(columnRefOperator, ColumnStatistic.unknown());
        }

        double totalRowNums = 0;
        try {
            totalRowNums = presentRowNums >= 0 ? presentRowNums : getEstimatedRowCount(table, partitionKeys);
        } catch (Exception e) {
            LOG.warn("Failed to estimate row count on table [{}]", table);
        } finally {
            builder.setOutputRowCount(totalRowNums);
        }

        return builder.build();
    }

    private OptionalDouble getPartitionRowCount(String partitionName, Map<String, HivePartitionStats> statistics) {
        HivePartitionStats partitionStatistics = statistics.get(partitionName);
        if (partitionStatistics == null) {
            return OptionalDouble.empty();
        }
        long numRows = partitionStatistics.getCommonStats().getRowNums();
        if (numRows == -1) {
            return OptionalDouble.empty();
        }
        return OptionalDouble.of(numRows);
    }

    private ColumnStatistic createPartitionColumnStatistics(Column column, List<PartitionKey> partitionKeys,
                                                            Map<String, HivePartitionStats> statistics,
                                                            List<String> partitionColumnNames,
                                                            double avgNumPerPartition, double rowCount) {
        int index = partitionColumnNames.indexOf(column.getName());
        return ColumnStatistic.builder()
                .setDistinctValuesCount(ndvForPartitionKey(partitionKeys, index))
                .setNullsFraction(nullsFractionForPartitionKey(
                        partitionKeys, index, rowCount, avgNumPerPartition, partitionColumnNames, statistics))
                .setAverageRowSize(averageRowSizeForPartitionKey(partitionKeys, index, rowCount, column))
                .setMinValue(minForPartitionKey(partitionKeys, index, column.getType()))
                .setMaxValue(maxForPartitionKey(partitionKeys, index, column.getType()))
                .build();
    }

    private double ndvForPartitionKey(
            List<PartitionKey> partitionKeys,
            int index) {
        return partitionKeys.stream()
                .map(partitionKey -> partitionKey.getKeys().get(index))
                .distinct()
                .count();
    }

    private double nullsFractionForPartitionKey(
            List<PartitionKey> partitionKeys,
            int index,
            double totalRowNums,
            double avgNumPerPartition,
            List<String> partitionColumnNames,
            Map<String, HivePartitionStats> statistics) {
        if (totalRowNums == 0) {
            return 0;
        }

        double estimatedNullsCount = partitionKeys.stream()
                .filter(partitionKey -> partitionKey.getKeys().get(index).isNullable())
                .map(partitionKey -> toHivePartitionName(partitionColumnNames, partitionKey))
                .mapToDouble(partitionName -> getPartitionRowCount(partitionName, statistics).orElse(avgNumPerPartition))
                .sum();

        return normalizeFraction(estimatedNullsCount / totalRowNums);
    }

    private double averageRowSizeForPartitionKey(
            List<PartitionKey> partitionKeys,
            int index,
            double totalRowNums,
            Column column) {
        List<LiteralExpr> literalExpr = partitionKeys.stream()
                .map(partitionKey -> partitionKey.getKeys().get(index))
                .collect(Collectors.toList());

        if (!column.getType().isStringType()) {
            return column.getType().getTypeSize();
        }

        double estimatedSize = literalExpr.stream()
                .map(expr -> getLengthFromLiteral(expr, column.getType()))
                .mapToDouble(x -> x)
                .sum();

        return estimatedSize / totalRowNums;
    }

    private double minForPartitionKey(
            List<PartitionKey> partitionKeys,
            int index,
            Type type) {
        OptionalDouble min = partitionKeys.stream()
                .map(partitionKey -> partitionKey.getKeys().get(index))
                .filter(literalExpr -> !(literalExpr instanceof NullLiteral))
                .map(literalExpr -> getValueFromLiteral(literalExpr, type))
                .mapToDouble(x -> x)
                .min();

        return min.isPresent() ? min.getAsDouble() : NEGATIVE_INFINITY;
    }

    private double maxForPartitionKey(
            List<PartitionKey> partitionKeys,
            int index,
            Type type) {
        OptionalDouble max = partitionKeys.stream()
                .map(partitionKey -> partitionKey.getKeys().get(index))
                .filter(literalExpr -> !(literalExpr instanceof NullLiteral))
                .map(literalExpr -> getValueFromLiteral(literalExpr, type))
                .mapToDouble(x -> x)
                .max();

        return max.isPresent() ? max.getAsDouble() : POSITIVE_INFINITY;
    }

    private ColumnStatistic createDataColumnStatistics(Column column, double rowNums,
                                                       Collection<HivePartitionStats> partitionStatistics) {
        List<HiveColumnStats> columnStatistics = partitionStatistics.stream()
                .map(HivePartitionStats::getColumnStats)
                .map(statistics -> statistics.get(column.getName()))
                .filter(Objects::nonNull)
                .collect(Collectors.toList());

        if (columnStatistics.isEmpty()) {
            return ColumnStatistic.unknown();
        }

        return ColumnStatistic.builder()
                .setDistinctValuesCount(ndv(columnStatistics))
                .setNullsFraction(nullsFraction(column, partitionStatistics))
                .setAverageRowSize(averageRowSize(column, partitionStatistics, rowNums))
                .setMaxValue(max(columnStatistics))
                .setMinValue(min(columnStatistics))
                .build();
    }

    private double ndv(List<HiveColumnStats> columnStatistics) {
        OptionalDouble ndv = columnStatistics.stream()
                .map(HiveColumnStats::getNdv)
                .filter(x -> x >= 0)
                .mapToDouble(x -> x)
                .max();

        return  ndv.isPresent() ? ndv.getAsDouble() : 1;
    }

    private double nullsFraction(Column column, Collection<HivePartitionStats> partitionStatistics) {
        List<HivePartitionStats> filteredStatistics = partitionStatistics.stream()
                .filter(pStat -> pStat.getCommonStats().getRowNums() != -1)
                .filter(pStat -> pStat.getColumnStats().get(column.getName()) != null)
                .filter(pStat -> pStat.getColumnStats().get(column.getName()).getNumNulls() != -1)
                .collect(Collectors.toList());

        if (filteredStatistics.isEmpty()) {
            return 0;
        }

        long totalNullsNums = 0;
        long totalRowNums = 0;
        for (HivePartitionStats statistics : filteredStatistics) {
            long rowNums = statistics.getCommonStats().getRowNums();
            HiveColumnStats columnStatistics = statistics.getColumnStats().get(column.getName());
            long nullsNums = columnStatistics.getNumNulls();
            totalNullsNums += nullsNums;
            totalRowNums += rowNums;
        }

        if (totalNullsNums == 0 || totalRowNums == 0) {
            return 0;
        }

        return (double) totalNullsNums / totalRowNums;
    }


    private double averageRowSize(Column column, Collection<HivePartitionStats> partitionStatistics, double totalRowNums) {
        if (!column.getType().isStringType()) {
            return column.getType().getTypeSize();
        }

        List<HivePartitionStats> filteredStatistics = partitionStatistics.stream()
                .filter(pStat -> pStat.getCommonStats().getRowNums() != -1)
                .filter(pStat -> pStat.getColumnStats().get(column.getName()) != null)
                .filter(pStat -> pStat.getColumnStats().get(column.getName()).getTotalSizeBytes() != -1)
                .collect(Collectors.toList());

        if (filteredStatistics.isEmpty()) {
            return column.getType().getTypeSize();
        }

        long totalSizeWithoutNullRow = filteredStatistics.stream()
                .map(stats -> stats.getColumnStats().get(column.getName()).getTotalSizeBytes())
                .mapToLong(x -> x)
                .sum();

        Preconditions.checkState(totalRowNums > 0, "total row num is 0");

        return ((double) totalSizeWithoutNullRow) / totalRowNums;
    }

    private double max(List<HiveColumnStats> columnStatistics) {
        OptionalDouble max =  columnStatistics.stream()
                .map(HiveColumnStats::getMax)
                .filter(value -> value != POSITIVE_INFINITY)
                .mapToDouble(x -> x)
                .max();
        return max.isPresent() ? max.getAsDouble() : POSITIVE_INFINITY;
    }

    private double min(List<HiveColumnStats> columnStatistics) {
        OptionalDouble min =  columnStatistics.stream()
                .map(HiveColumnStats::getMin)
                .filter(value -> value != NEGATIVE_INFINITY)
                .mapToDouble(x -> x)
                .min();
        return min.isPresent() ? min.getAsDouble() : NEGATIVE_INFINITY;
    }

    private double getPerPartitionRowAvgNums(Collection<HivePartitionStats> statistics) {
        if (statistics.isEmpty()) {
            return -1;
        }

        long[] rowNums = statistics.stream()
                .map(HivePartitionStats::getCommonStats)
                .map(HiveCommonStats::getRowNums)
                .filter(num -> num != -1)
                .mapToLong(num -> num)
                .toArray();

        OptionalDouble avg = Arrays.stream(rowNums).average();
        return avg.isPresent() ? avg.getAsDouble() : -1;
    }

    private double getValueFromLiteral(LiteralExpr literalExpr, Type type) {
        switch (type.getPrimitiveType()) {
            case TINYINT:
            case SMALLINT:
            case INT:
            case BIGINT:
                return literalExpr.getLongValue();
            case LARGEINT:
            case FLOAT:
            case DOUBLE:
            case DECIMALV2:
            case DECIMAL32:
            case DECIMAL64:
            case DECIMAL128:
                return literalExpr.getDoubleValue();
            case DATE:
            case DATETIME:
                return (((DateLiteral) literalExpr).unixTimestamp(TimeZone.getDefault())) / 1000.0;
            default:
                return Double.NaN;
        }
    }

    private double getLengthFromLiteral(LiteralExpr literalExpr, Type type) {
        switch (type.getPrimitiveType()) {
            case CHAR:
            case VARCHAR:
                return literalExpr.getStringValue().length();
            default:
                return type.getPrimitiveType().getSlotSize();
        }
    }

    private static double normalizeFraction(double fraction) {
        checkArgument(!isNaN(fraction), "fraction is NaN");
        checkArgument(isFinite(fraction), "fraction must be finite");
        if (fraction < 0) {
            return 0;
        }
        if (fraction > 1) {
            return 1;
        }
        return fraction;
    }

    public int getSamplePartitionSize(OptimizerContext session) {
        return session.getSessionVariable().getHivePartitionStatsSampleSize();
    }

    // Use murmur3_128 hash function to break up the partitionName as randomly and scattered as possible,
    // and return an ordered list of partitionNames.
    // In order to ensure more accurate sampling, put min and max in the sampled result.
    static List<String> getPartitionsSample(List<String> partitions, int sampleSize) {
        checkArgument(sampleSize > 0, "sampleSize is expected to be greater than zero");

        if (partitions.size() <= sampleSize) {
            return partitions;
        }

        List<String> result = new ArrayList<>();
        int left = sampleSize;
        String min = partitions.get(0);
        String max = partitions.get(0);
        for (String partition : partitions) {
            if (partition.compareTo(min) < 0) {
                min = partition;
            } else if (partition.compareTo(max) > 0) {
                max = partition;
            }
        }

        result.add(min);
        left--;
        if (left > 0) {
            result.add(max);
            left--;
        }

        if (left > 0) {
            HashFunction hashFunction = Hashing.murmur3_128();
            Comparator<Map.Entry<String, Long>> hashComparator = Map.Entry.<String, Long>comparingByValue()
                    .thenComparing(Map.Entry::getKey);

            partitions.stream()
                    .filter(partition -> !result.contains(partition))
                    .map(partition -> immutableEntry(partition, hashFunction.hashUnencodedChars(partition).asLong()))
                    .sorted(hashComparator)
                    .limit(left)
                    .forEachOrdered(entry -> result.add(entry.getKey()));
        }
        return Lists.newArrayList(result);
    }
}