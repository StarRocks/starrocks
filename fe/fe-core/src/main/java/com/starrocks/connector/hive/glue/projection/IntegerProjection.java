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

package com.starrocks.connector.hive.glue.projection;

import com.starrocks.type.IntegerType;
import com.starrocks.type.Type;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Partition projection for integer ranges.
 *
 * The integer projection type defines a range of integer values with an optional interval.
 * Values can be formatted with leading zeros using the digits property.
 *
 * Example table properties:
 * <pre>
 *   'projection.year.type' = 'integer',
 *   'projection.year.range' = '2020,2030',
 *   'projection.year.interval' = '1',
 *   'projection.year.digits' = '4'
 * </pre>
 */
public class IntegerProjection implements ColumnProjection {

    // Maximum number of values to generate to prevent memory issues
    private static final int MAX_VALUES = 100000;

    private final String columnName;
    private final long leftBound;
    private final long rightBound;
    private final long interval;
    private final Optional<Integer> digits;

    /**
     * Creates an integer projection for a partition column.
     *
     * @param columnName the partition column name
     * @param rangeProperty range as "min,max" (e.g., "2020,2030")
     * @param intervalProperty optional interval between values (default: 1)
     * @param digitsProperty optional minimum digits for zero-padding
     * @throws IllegalArgumentException if range is invalid or bounds are reversed
     */
    public IntegerProjection(String columnName, String rangeProperty,
                             Optional<String> intervalProperty,
                             Optional<String> digitsProperty) {
        if (rangeProperty == null || rangeProperty.trim().isEmpty()) {
            throw new IllegalArgumentException(
                    "Integer projection for column '" + columnName + "' requires a range property");
        }

        this.columnName = columnName;

        // Parse range: "min,max"
        String[] parts = rangeProperty.split(",");
        if (parts.length != 2) {
            throw new IllegalArgumentException(
                    "Integer projection range for column '" + columnName +
                            "' must be in format 'min,max', got: " + rangeProperty);
        }

        try {
            this.leftBound = Long.parseLong(parts[0].trim());
            this.rightBound = Long.parseLong(parts[1].trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    "Integer projection range for column '" + columnName +
                            "' contains invalid numbers: " + rangeProperty, e);
        }

        if (leftBound > rightBound) {
            throw new IllegalArgumentException(
                    "Integer projection for column '" + columnName +
                            "' has invalid range: min (" + leftBound + ") > max (" + rightBound + ")");
        }

        // Parse interval (default: 1)
        this.interval = intervalProperty
                .map(s -> {
                    try {
                        long val = Long.parseLong(s.trim());
                        if (val <= 0) {
                            throw new IllegalArgumentException(
                                    "Integer projection interval must be positive, got: " + val);
                        }
                        return val;
                    } catch (NumberFormatException e) {
                        throw new IllegalArgumentException(
                                "Invalid interval value for column '" + columnName + "': " + s, e);
                    }
                })
                .orElse(1L);

        // Parse digits (optional)
        this.digits = digitsProperty.map(s -> {
            try {
                int val = Integer.parseInt(s.trim());
                if (val <= 0) {
                    throw new IllegalArgumentException(
                            "Integer projection digits must be positive, got: " + val);
                }
                return val;
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(
                        "Invalid digits value for column '" + columnName + "': " + s, e);
            }
        });

        // Validate that the range won't generate too many values.
        // Note: estimatedCount is an upper-bound estimate. When the range does not align perfectly
        // with the interval, the actual number of generated values may be smaller.
        // Use Math.subtractExact/addExact to detect overflow for extremely large ranges.
        try {
            long range = Math.subtractExact(rightBound, leftBound);
            long estimatedCount = Math.addExact(range / interval, 1);
            if (estimatedCount > MAX_VALUES) {
                throw new IllegalArgumentException(
                        "Integer projection for column '" + columnName +
                                "' is estimated to generate up to " + estimatedCount +
                                " values, exceeding limit of " + MAX_VALUES);
            }
        } catch (ArithmeticException e) {
            throw new IllegalArgumentException(
                    "Integer projection for column '" + columnName +
                            "' has range too large (arithmetic overflow).", e);
        }
    }

    @Override
    public String getColumnName() {
        return columnName;
    }

    @Override
    public List<String> getProjectedValues(Optional<Object> filterValue) {
        if (filterValue.isPresent()) {
            // Single value filter
            long filter = toLong(filterValue.get());
            if (filter >= leftBound && filter <= rightBound) {
                // Check if the filter value aligns with the interval
                if ((filter - leftBound) % interval == 0) {
                    return Collections.singletonList(formatValue(filter));
                }
            }
            return Collections.emptyList();
        }

        // Generate all values in range with safety counter to prevent infinite loops
        List<String> result = new ArrayList<>();
        for (long current = leftBound; current <= rightBound && result.size() < MAX_VALUES; current += interval) {
            result.add(formatValue(current));
        }
        return result;
    }

    private long toLong(Object value) {
        if (value instanceof Number) {
            return ((Number) value).longValue();
        }
        return Long.parseLong(value.toString().trim());
    }

    @Override
    public String formatValue(Object value) {
        long v = toLong(value);
        if (digits.isPresent()) {
            return String.format("%0" + digits.get() + "d", v);
        }
        return String.valueOf(v);
    }

    @Override
    public Type getColumnType() {
        return IntegerType.BIGINT;
    }

    public long getLeftBound() {
        return leftBound;
    }

    public long getRightBound() {
        return rightBound;
    }

    public long getInterval() {
        return interval;
    }

    public Optional<Integer> getDigits() {
        return digits;
    }

    @Override
    public String toString() {
        return "IntegerProjection{" +
                "columnName='" + columnName + '\'' +
                ", leftBound=" + leftBound +
                ", rightBound=" + rightBound +
                ", interval=" + interval +
                ", digits=" + digits +
                '}';
    }
}
