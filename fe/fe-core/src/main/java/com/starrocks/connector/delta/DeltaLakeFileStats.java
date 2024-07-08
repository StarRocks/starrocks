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
import com.starrocks.common.util.DateUtils;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import io.delta.kernel.types.BasePrimitiveType;
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
import java.util.List;
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
    private final List<String> nonPartitionPrimitiveColumns;
    private long recordCount;
    private long size;
    private final Map<String, Object> minValues;
    private final Map<String, Object> maxValues;
    private final Map<String, Object> nullCounts;
    private final Set<String> corruptedStats;
    private boolean hasValidColumnMetrics;

    public DeltaLakeFileStats(StructType schema,
                              List<String> nonPartitionPrimitiveColumns,
                              long recordCount,
                              long size,
                              Map<String, Object> minValues,
                              Map<String, Object> maxValues,
                              Map<String, Object> nullCounts) {
        this.schema = schema;
        this.nonPartitionPrimitiveColumns = nonPartitionPrimitiveColumns;
        this.recordCount = recordCount;
        this.size = size;
        if (minValues == null || maxValues == null || nullCounts == null) {
            this.minValues = null;
            this.maxValues = null;
            this.nullCounts = null;
            this.corruptedStats = null;
            this.hasValidColumnMetrics = false;
        } else {
            this.minValues = minValues;
            this.maxValues = maxValues;
            this.nullCounts = nullCounts;
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

    public void fillColumnStats(ColumnStatistic.Builder builder, Column col) {
        if (schema == null) {
            return;
        }

        // TODO: Currently not set avg size, will be optimized later.
        // builder.setAverageRowSize(xxx);
        builder.setType(ColumnStatistic.StatisticType.UNKNOWN);

        String colName = col.getName();
        if (!nonPartitionPrimitiveColumns.contains(colName)) {
            return;
        }

        if (minValues != null) {
            if (col.getType().isStringType()) {
                String minString = getBoundStatistic(colName, minValues).toString();
                builder.setMinString(minString);
            } else {
                Optional<Double> res = getBoundStatistic(colName, minValues);
                res.ifPresent(builder::setMinValue);
            }
        }

        if (maxValues != null) {
            if (col.getType().isStringType()) {
                String maxString = getBoundStatistic(colName, maxValues).toString();
                builder.setMinString(maxString);
            } else {
                Optional<Double> res = getBoundStatistic(colName, maxValues);
                res.ifPresent(builder::setMaxValue);
            }
        }

        if (nullCounts != null) {
            Long nullCount = getNullCount(colName);
            if (nullCount == null) {
                builder.setNullsFraction(0);
            } else {
                builder.setNullsFraction(nullCount * 1.0 / Math.max(recordCount, 1));
            }
        }
    }

    public void incrementRecordCount(long count) {
        this.recordCount += count;
    }

    public void incrementSize(long numberOfBytes) {
        this.size += numberOfBytes;
    }

    public void updateMinStats(Map<String, Object> newStat, Map<String, Object> nullCounts,
                               long recordCount, Predicate<Integer> predicate) {
        if (!hasValidColumnMetrics) {
            return;
        }

        updateStats(this.minValues, newStat, nullCounts, recordCount, predicate);
    }

    public void updateMaxStats(Map<String, Object> newStat, Map<String, Object> nullCounts,
                               long recordCount, Predicate<Integer> predicate) {
        if (!hasValidColumnMetrics) {
            return;
        }

        updateStats(this.maxValues, newStat, nullCounts, recordCount, predicate);
    }

    private static Object sumNullCount(Object left, Object value) {
        return (Double) left + (Double) value;
    }

    private Optional<Double> getBoundStatistic(String colName, Map<String, Object> boundValues) {
        if (boundValues == null) {
            return Optional.empty();
        }
        StructField field = schema.get(colName);
        if (field == null) {
            return Optional.empty();
        }
        Object value = boundValues.get(colName);
        if (value == null) {
            return Optional.empty();
        }
        return convertObjectToOptionalDouble(field.getDataType(), value);
    }

    private void updateStats(Map<String, Object> curStat,
                            Map<String, Object> newStat,
                            Map<String, Object> nullCounts,
                            long recordCount,
                            Predicate<Integer> predicate) {
        if (newStat == null || nullCounts == null) {
            hasValidColumnMetrics = false;
            return;
        }
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

    public void updateNullCount(Map<String, Object> nullCounts, List<String> nonPartitionPrimitiveColumns) {
        if (!hasValidColumnMetrics) {
            return;
        }
        if (nullCounts == null) {
            hasValidColumnMetrics = false;
            return;
        }

        for (String col : nonPartitionPrimitiveColumns) {
            DataType type = schema.get(col).getDataType();
            if (BasePrimitiveType.isPrimitiveType(type.toString())) {
                this.nullCounts.merge(col, nullCounts.get(col), DeltaLakeFileStats::sumNullCount);
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
