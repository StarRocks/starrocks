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
import com.starrocks.catalog.Type;
import com.starrocks.common.util.DateUtils;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import io.delta.kernel.types.BinaryType;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;

import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

public class DeltaLakeFileStats {
    private static final DateTimeFormatter TIMESTAMP_NTZ_FORMAT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS");
    private static final DateTimeFormatter TIMESTAMP_ZONE_FORMAT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");

    private final StructType schema;
    private final Set<String> nonPartitionPrimitiveColumns;
    private final Set<String> partitionPrimitiveColumns;
    private long recordCount;
    private long size;
    private final Map<String, Object> minValues;
    private final Map<String, Object> maxValues;
    private final Map<String, Double> nullCounts;
    private final Set<String> corruptedStats;
    private boolean hasValidColumnMetrics;

    public DeltaLakeFileStats(StructType schema, Set<String> nonPartitionPrimitiveColumns,
                              Set<String> partitionPrimitiveColumns,
                              DeltaLakeAddFileStatsSerDe fileStat,
                              Map<String, String> partitionValues,
                              long numRecords, long fileSize) {
        this.schema = schema;
        this.nonPartitionPrimitiveColumns = nonPartitionPrimitiveColumns;
        this.partitionPrimitiveColumns = partitionPrimitiveColumns;
        this.recordCount = numRecords;
        this.size = fileSize;

        if (fileStat == null || fileStat.minValues == null || fileStat.maxValues == null
                || fileStat.nullCount == null) {
            this.minValues = null;
            this.maxValues = null;
            this.nullCounts = null;
            this.corruptedStats = null;
            this.hasValidColumnMetrics = false;
        } else {
            this.minValues = new HashMap<>();
            this.maxValues = new HashMap<>();
            this.nullCounts = new HashMap<>();
            this.corruptedStats = new HashSet<>();

            this.updateStats(this.minValues, fileStat.minValues, fileStat.nullCount, numRecords, i -> (i > 0));
            this.updateStats(this.maxValues, fileStat.maxValues, fileStat.nullCount, numRecords, i -> (i < 0));
            this.updateNullCount(fileStat.nullCount);

            if (this.partitionPrimitiveColumns != null && partitionValues != null) {
                this.updatePartitionStats(this.minValues, partitionValues, i -> (i > 0));
                this.updatePartitionStats(this.maxValues, partitionValues, i -> (i < 0));
                this.updatePartitionNullCount(partitionValues, numRecords);
            }
            this.hasValidColumnMetrics = true;
        }
    }

    public DeltaLakeFileStats(long recordCount) {
        this.schema = null;
        this.nonPartitionPrimitiveColumns = null;
        this.partitionPrimitiveColumns = null;
        this.recordCount = recordCount;
        this.size = 0;
        this.minValues = null;
        this.maxValues = null;
        this.nullCounts = null;
        this.corruptedStats = null;
        this.hasValidColumnMetrics = false;
    }

    public long getRecordCount() {
        return recordCount;
    }

    public ColumnStatistic fillColumnStats(Column col) {
        ColumnStatistic.Builder builder = ColumnStatistic.builder();
        String colName = col.getName();
        Type colType = col.getType();

        // set default value
        builder.setNullsFraction(0);
        builder.setAverageRowSize(colType.getTypeSize());
        builder.setDistinctValuesCount(1);
        builder.setType(ColumnStatistic.StatisticType.UNKNOWN);

        if (schema == null) {
            return builder.build();
        }
        StructField field = schema.get(colName);
        if (field == null) {
            return builder.build();
        }
        if (nonPartitionPrimitiveColumns.contains(colName) ||
                (partitionPrimitiveColumns != null && partitionPrimitiveColumns.contains(colName))) {
            fillMinMaxValue(builder, colName, field.getDataType());
        }
        return builder.build();
    }

    private void fillMinMaxValue(ColumnStatistic.Builder builder, String colName, DataType colType) {
        if (minValues != null) {
            Object v = minValues.get(colName);
            if (v != null) {
                if (colType == StringType.STRING || colType == BinaryType.BINARY) {
                    builder.setMinString(v.toString());
                } else {
                    builder.setMinValue((double) v);
                }
            }
        }

        if (maxValues != null) {
            Object v = maxValues.get(colName);
            if (v != null) {
                if (colType == StringType.STRING || colType == BinaryType.BINARY) {
                    builder.setMaxString(v.toString());
                } else {
                    builder.setMaxValue((double) v);
                }
            }
        }

        if (nullCounts != null) {
            Double nullCount = nullCounts.get(colName);
            if (nullCount != null) {
                builder.setNullsFraction(nullCount * 1.0 / Math.max(recordCount, 1));
            }
        }
    }

    public void update(DeltaLakeAddFileStatsSerDe stat, Map<String, String> partitionValues,
                       long numRecords, long fileSize) {
        this.recordCount += numRecords;
        this.size += fileSize;

        if (stat == null) {
            return;
        }

        if (!hasValidColumnMetrics) {
            return;
        }
        if (stat.minValues == null || stat.maxValues == null || stat.nullCount == null) {
            hasValidColumnMetrics = false;
            return;
        }

        updateStats(this.minValues, stat.minValues, stat.nullCount, stat.numRecords, i -> (i > 0));
        updateStats(this.maxValues, stat.maxValues, stat.nullCount, stat.numRecords, i -> (i < 0));
        updateNullCount(stat.nullCount);

        if (partitionValues == null) {
            return;
        }

        updatePartitionStats(this.minValues, partitionValues, i -> (i > 0));
        updatePartitionStats(this.maxValues, partitionValues, i -> (i < 0));
        updatePartitionNullCount(partitionValues, numRecords);
    }

    private static Double sumNullCount(Double left, Double right) {
        return left + right;
    }

    private void updatePartitionStats(Map<String, Object> curStat, Map<String, String> newStat,
                                      Predicate<Integer> predicate) {
        for (String col : partitionPrimitiveColumns) {
            String newValue = newStat.get(col);
            if (newValue == null) {
                continue;
            }

            DataType type = schema.get(col).getDataType();
            Object newParsedValue = parsePartitionValue(type, newValue);
            Object oldValue = curStat.putIfAbsent(col, newParsedValue);
            if (oldValue != null) {
                Comparator<Object> comparator = DeltaLakeComparators.forType(type);
                if (predicate.test(comparator.compare(oldValue, newParsedValue))) {
                    curStat.put(col, newParsedValue);
                }
            }
        }
    }

    private void updateStats(Map<String, Object> curStat,
                            Map<String, Object> newStat,
                            Map<String, Object> nullCounts,
                            long recordCount,
                            Predicate<Integer> predicate) {
        for (String col : nonPartitionPrimitiveColumns) {
            if (corruptedStats.contains(col)) {
                continue;
            }

            Object newValue = newStat.get(col);
            if (newValue == null) {
                Double nullCount = (Double) nullCounts.get(col);
                if ((nullCount == null) || (nullCount.longValue() != recordCount)) {
                    curStat.remove(col);
                    corruptedStats.add(col);
                }
                continue;
            }

            DataType type = schema.get(col).getDataType();
            Object newParsedValue = parseMinMaxValue(type, newValue);
            if (newParsedValue == null) {
                continue;
            }

            Object oldValue = curStat.putIfAbsent(col, newParsedValue);
            if (oldValue != null) {
                Comparator<Object> comparator = DeltaLakeComparators.forType(type);
                if (predicate.test(comparator.compare(oldValue, newParsedValue))) {
                    curStat.put(col, newParsedValue);
                }
            }
        }
    }

    private void updateNullCount(Map<String, Object> nullCounts) {
        for (String col : nonPartitionPrimitiveColumns) {
            Object v = nullCounts.get(col);
            if (v != null) {
                this.nullCounts.merge(col, (Double) v, DeltaLakeFileStats::sumNullCount);
            }
        }
    }

    private void updatePartitionNullCount(Map<String, String> partitionValues, long numRecords) {
        for (String col : partitionPrimitiveColumns) {
            if (partitionValues.containsKey(col) && partitionValues.get(col) == null) {
                nullCounts.merge(col, (double) numRecords, DeltaLakeFileStats::sumNullCount);
            }
        }
    }

    private static double parseTimestampWithAtZone(String str) {
        OffsetDateTime time = OffsetDateTime.parse(str, TIMESTAMP_ZONE_FORMAT);
        return time.toEpochSecond();
    }

    private static double parseTimestampNTZ(String str) {
        LocalDateTime time = LocalDateTime.parse(str, TIMESTAMP_NTZ_FORMAT);
        return time.atZone(ZoneOffset.UTC).toEpochSecond();
    }

    private static double parseDate(String str) {
        LocalDateTime time = DateUtils.parseStrictDateTime(str);
        return time.atZone(ZoneOffset.UTC).toEpochSecond();
    }

    private static Object parseMinMaxValue(DataType type, Object value) {
        DeltaDataType deltaDataType = DeltaDataType.instanceFrom(type.getClass());
        switch (deltaDataType) {
            case BOOLEAN:
                return (boolean) value ? 1d : 0d;
            case BYTE:
            case SMALLINT:
            case INTEGER:
            case LONG:
            case FLOAT:
            case DOUBLE:
            case STRING:
            case BINARY:
            case DECIMAL:
                return value;
            case TIMESTAMP_NTZ:
                return parseTimestampNTZ((String) value);
            case TIMESTAMP:
                return parseTimestampWithAtZone((String) value);
            case DATE:
                return parseDate((String) value);
            default:
                return null;
        }
    }

    private Object parsePartitionValue(DataType type, String value) {
        DeltaDataType deltaDataType = DeltaDataType.instanceFrom(type.getClass());
        switch (deltaDataType) {
            case BOOLEAN:
                return value.equalsIgnoreCase("false") ? 0d : 1d;
            case BYTE:
            case SMALLINT:
            case INTEGER:
            case LONG:
            case FLOAT:
            case DOUBLE:
            case DECIMAL:
                return Double.parseDouble(value);
            case DATE:
            case TIMESTAMP:
            case TIMESTAMP_NTZ:
                return parseDate(value);
            default:
                return null;
        }
    }
}
