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

import io.delta.kernel.types.BasePrimitiveType;
import io.delta.kernel.types.BooleanType;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.DoubleType;
import io.delta.kernel.types.FloatType;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.ShortType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class DeltaLakeFileStats {
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
            this.minValues = new HashMap<>(minValues);
            this.maxValues = new HashMap<>(maxValues);
            this.nullCounts = nonPartitionPrimitiveColumns.stream()
                    .filter(col -> !nullCounts.containsKey(col))
                    .collect(Collectors.toMap(col -> col, nullCounts::get));
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

    public List<String> getNonPartitionPrimitiveColumns() {
        return nonPartitionPrimitiveColumns;
    }

    public long getRecordCount() {
        return recordCount;
    }

    public long getSize() {
        return size;
    }

    public StructType getSchema() {
        return schema;
    }

    public Map<String, Object> getMinValues() {
        return minValues;
    }

    public Optional<Double> getMinValue(String colName) {
        return getBoundStatistic(colName, minValues);
    }

    public boolean canUseStats(String colName, Map<String, Object> values) {
        if (schema == null || values == null) {
            return false;
        }

        return schema.get(colName) != null && values.get(colName) != null;
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

    public Optional<Double> getMaxValue(String colName) {
        return getBoundStatistic(colName, maxValues);
    }

    public Map<String, Object> getMaxValues() {
        return maxValues;
    }

    public Map<String, Object> getNullCounts() {
        return nullCounts;
    }

    public Long getNullCount(String col) {
        if (nullCounts == null) {
            return null;
        }
        Object v = nullCounts.get(col);
        if (v == null) {
            return null;
        }
        return ((Double) v).longValue();
    }

    public Set<String> getCorruptedStats() {
        return corruptedStats;
    }

    public boolean isHasValidColumnMetrics() {
        return hasValidColumnMetrics;
    }

    public void incrementRecordCount(long count) {
        this.recordCount += count;
    }

    public void incrementSize(long numberOfBytes) {
        this.size += numberOfBytes;
    }

    public void updateStats(Map<String, Object> curStat,
                            Map<String, Object> newStat,
                            Map<String, Object> nullCounts,
                            long recordCount,
                            Predicate<Integer> predicate) {
        if (!hasValidColumnMetrics) {
            return;
        }
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

    public static Object sumNullCount(Object left, Object value) {
        return (Double) left + (Double) value;
    }

    public static Optional<Double> convertObjectToOptionalDouble(DataType type, Object value) {
        // TODO: Timestamp, TimestampNTZ, Date, Decimal
        double result;

        if (type instanceof BooleanType) {
            result = (boolean) value ? 1 : 0;
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
        } else {
            return Optional.empty();
        }

        return Optional.of(result);
    }
}
