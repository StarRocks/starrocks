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

package com.starrocks.alter.reshard.presplit;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.NullVariant;
import com.starrocks.catalog.Variant;
import com.starrocks.common.util.DateUtils;
import com.starrocks.type.PrimitiveType;

import java.time.LocalDateTime;

/**
 * Type-aware {@link Variant} -> String formatter for partition source columns.
 *
 * <p>For DATE / DATETIME this normalizes to the day/second-granularity SQL forms
 * ({@code yyyy-MM-dd}, {@code yyyy-MM-dd HH:mm:ss}) that the analyzer's
 * {@link com.starrocks.sql.analyzer.AnalyzerUtils#truncateToPartitionBoundary}
 * accepts, deliberately dropping any sub-second component carried by the sampled
 * value (irrelevant once truncated to a partition boundary).
 */
public final class PartitionValueFormatter {
    private final PrimitiveType primitiveType;

    public PartitionValueFormatter(Column column) {
        this.primitiveType = column.getType().getPrimitiveType();
    }

    /**
     * Format a single Variant cell to its SQL-parseable string form.
     *
     * @return formatted string, or {@code null} when the cell is null (caller
     *         drops the row).
     */
    public String format(Variant cell) {
        if (cell == null || cell instanceof NullVariant) {
            return null;
        }
        return switch (primitiveType) {
            case DATE -> toLocalDateTime(cell).format(DateUtils.DATE_FORMATTER_UNIX);
            case DATETIME -> toLocalDateTime(cell).format(DateUtils.DATE_TIME_FORMATTER_UNIX);
            case TINYINT, SMALLINT, INT, BIGINT, LARGEINT,
                 FLOAT, DOUBLE, DECIMALV2, DECIMAL32, DECIMAL64, DECIMAL128, DECIMAL256,
                 CHAR, VARCHAR -> cell.getStringValue();
            default -> throw new IllegalArgumentException(
                    "Unsupported partition source column type: " + primitiveType);
        };
    }

    /** Whether {@code column}'s type is supported for sampling-based partition prediction. */
    public static boolean isSupportedColumnType(Column column) {
        return switch (column.getType().getPrimitiveType()) {
            case DATE, DATETIME, TINYINT, SMALLINT, INT, BIGINT, LARGEINT,
                 FLOAT, DOUBLE, DECIMALV2, DECIMAL32, DECIMAL64, DECIMAL128, DECIMAL256,
                 CHAR, VARCHAR -> true;
            default -> false;
        };
    }

    /**
     * Parse a date/datetime {@link Variant} into a {@link LocalDateTime}.
     * {@link Variant#getStringValue()} returns the StarRocks canonical text,
     * which {@link DateUtils#parseStrictDateTime} accepts directly.
     */
    private static LocalDateTime toLocalDateTime(Variant cell) {
        return DateUtils.parseStrictDateTime(cell.getStringValue());
    }
}
