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
import io.delta.kernel.types.BooleanType;
import io.delta.kernel.types.ByteType;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.DateType;
import io.delta.kernel.types.DoubleType;
import io.delta.kernel.types.FloatType;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.ShortType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import io.delta.kernel.types.TimestampNTZType;
import io.delta.kernel.types.TimestampType;

import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Comparator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class DeltaLakeFileStats {
    private static final DateTimeFormatter TIMESTAMP_NTZ_FORMAT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS");
    private static final DateTimeFormatter TIMESTAMP_ZONE_FORMAT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");

    private final StructType schema;
    private final Set<String> nonPartitionPrimitiveColumns;
    private long recordCount;
    private long size;
    private final Map<String, Object> minValues;
    private final Map<String, Object> maxValues;
    private final Map<String, Object> nullCounts;
    private final Set<String> corruptedStats;
    private boolean hasValidColumnMetrics;

    public DeltaLakeFileStats(StructType schema, Set<String> nonPartitionPrimitiveColumns,
                              DeltaLakeAddFileStatsSerDe fileStat, long numRecords, long fileSize) {
        this.schema = schema;
        this.nonPartitionPrimitiveColumns = nonPartitionPrimitiveColumns;
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
            this.minValues = fileStat.minValues.entrySet().stream()
                    .filter(e -> nonPartitionPrimitiveColumns.contains(e.getKey()))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
            this.maxValues = fileStat.maxValues.entrySet().stream()
                    .filter(e -> nonPartitionPrimitiveColumns.contains(e.getKey()))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
            this.nullCounts = fileStat.nullCount.entrySet().stream()
                    .filter(e -> nonPartitionPrimitiveColumns.contains(e.getKey()))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
            this.corruptedStats = nonPartitionPrimitiveColumns.stream()
                    .filter(col -> !minValues.containsKey(col) &&
                            (!nullCounts.containsKey(col) || ((Double) nullCounts.get(col)).longValue() != recordCount))
                    .collect(Collectors.toSet());
            this.hasValidColumnMetrics = true;
        }
    }

    public DeltaLakeFileStats(long recordCount) {
        this.schema = null;
        this.nonPartitionPrimitiveColumns = null;
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
        if (nonPartitionPrimitiveColumns.contains(colName)) {
            fillNonParititionValues(builder, colName, colType, field.getDataType());
        }
        return builder.build();
    }

    private void fillNonParititionValues(ColumnStatistic.Builder builder, String colName, Type colType,
                                        DataType deltaDataType) {
        if (minValues != null) {
            if (colType.isStringType()) {
                String minString = minValues.get(colName).toString();
                builder.setMinString(minString);
            } else {
                Optional<Double> res = getBoundStatistic(colName, deltaDataType, minValues);
                res.ifPresent(builder::setMinValue);
            }
        }

        if (maxValues != null) {
            if (colType.isStringType()) {
                String maxString = maxValues.get(colName).toString();
                builder.setMaxString(maxString);
            } else {
                Optional<Double> res = getBoundStatistic(colName, deltaDataType, maxValues);
                res.ifPresent(builder::setMaxValue);
            }
        }

        if (nullCounts != null) {
            Long nullCount = getNullCount(colName);
            if (nullCount != null) {
                builder.setNullsFraction(nullCount * 1.0 / Math.max(recordCount, 1));
            }
        }
    }

    public void update(DeltaLakeAddFileStatsSerDe stat, long numRecords, long fileSize) {
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
    }

    private static Object sumNullCount(Object left, Object value) {
        return (Double) left + (Double) value;
    }

    private Optional<Double> getBoundStatistic(String colName, DataType type, Map<String, Object> boundValues) {
        Object value = boundValues.get(colName);
        if (value == null) {
            return Optional.empty();
        }
        return convertObjectToOptionalDouble(type, value);
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
            Object oldValue = curStat.putIfAbsent(col, newValue);
            if (oldValue != null) {
                Comparator<Object> comparator = DeltaLakeComparators.forType(type);
                if (predicate.test(comparator.compare(oldValue, newValue))) {
                    curStat.put(col, newValue);
                }
            }
        }
    }

    private void updateNullCount(Map<String, Object> nullCounts) {
        for (String col : nonPartitionPrimitiveColumns) {
            this.nullCounts.merge(col, nullCounts.get(col), DeltaLakeFileStats::sumNullCount);
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

    private static Optional<Double> convertObjectToOptionalDouble(DataType type, Object value) {
        // TODO: Decimal
        double result;

        if (type instanceof BooleanType) {
            result = (boolean) value ? 1 : 0;
        } else if (type instanceof ByteType) {
            result = (double) value;
        } else if (type instanceof ShortType) {
            result = (double) value;
        } else if (type instanceof IntegerType) {
            result = (double) value;
        } else if (type instanceof LongType) {
            result = (double) value;
        } else if (type instanceof FloatType) {
            result = (double) value;
        } else if (type instanceof DoubleType) {
            result = (double) value;
        } else if (type instanceof TimestampNTZType) {
            result = parseTimestampNTZ((String) value);
        } else if (type instanceof TimestampType) {
            result = parseTimestampWithAtZone((String) value);
        } else if (type instanceof DateType) {
            result = parseDate((String) value);
        } else {
            return Optional.empty();
        }

        return Optional.of(result);
    }

    private Long getNullCount(String col) {
        Object v = nullCounts.get(col);
        if (v == null) {
            return null;
        }
        return ((Double) v).longValue();
    }
}
