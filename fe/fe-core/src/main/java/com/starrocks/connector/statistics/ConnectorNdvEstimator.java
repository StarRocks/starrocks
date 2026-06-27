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

package com.starrocks.connector.statistics;

import com.starrocks.type.Type;

/**
 * Shared, connector-agnostic NDV (Number of Distinct Values) estimator.
 *
 * <p>Implements a three-tier fallback strategy:
 * <ol>
 *   <li><b>Tier 1 – Range-based</b>: uses min/max to compute a range in native units.</li>
 *   <li><b>Tier 2 – Size-based</b>: uses compressed column-size bytes to estimate cardinality.</li>
 *   <li><b>Tier 3 – Type-fraction</b>: last resort; returns {@code rowCount * typeFraction}.</li>
 * </ol>
 *
 * <p>Each connector passes pre-computed doubles (already normalized to a comparable unit) and the
 * appropriate {@link TypeCategory} to handle unit-conversion differences between formats.
 */
public final class ConnectorNdvEstimator {

    private ConnectorNdvEstimator() {}

    /**
     * Describes how the connector stores min/max values and what the native distinct-value unit is.
     * Every connector maps its own column type to one of these categories before calling
     * {@link #estimate}.
     */
    public enum TypeCategory {
        /** Boolean columns; at most 2 distinct values. */
        BOOLEAN,
        /** Integer columns (INT, LONG). No unit conversion; range estimation is safe. */
        INTEGER_LIKE,
        /**
         * Floating-point / decimal columns (FLOAT, DOUBLE, DECIMAL).
         * Range estimation is skipped: sub-unit ranges (e.g. 0.1–0.9) produce {@code (long)(max-min)=0},
         * making rangeNdv=1 regardless of actual cardinality. Falls through to Tier 2/3.
         */
        FLOAT_LIKE,
        /**
         * Date columns whose min/max doubles are epoch-seconds.
         * Native unit = day → divide diff by 86400 to get the count of distinct days.
         */
        DATE_IN_EPOCH_SECONDS,
        /**
         * Timestamp columns whose min/max doubles are already epoch-seconds.
         * Native granularity = second → no division.
         */
        TIMESTAMP_IN_EPOCH_SECONDS,
        /**
         * Timestamp columns whose min/max doubles are epoch-seconds converted from epoch-microseconds
         * (i.e. the raw value was divided by 1 000 000 to fit a double).
         * Native unit = microsecond → multiply diff by 1 000 000.
         */
        TIMESTAMP_IN_EPOCH_MICROS,
        /** String / binary columns; range-based estimation is not applicable. */
        STRING_LIKE,
        OTHER
    }

    /**
     * Estimates NDV for a single column.
     *
     * @param category     type category of the column
     * @param minDouble    normalized min value as double, or {@link Double#NaN} if absent
     * @param maxDouble    normalized max value as double, or {@link Double#NaN} if absent
     * @param colSizeBytes compressed column size in bytes across sampled rows; {@code -1} if absent
     * @param colSizeRcnt  number of rows covered by {@code colSizeBytes}; {@code 0} if absent
     * @param rowCount     total rows in the table / scan
     * @return estimated NDV ≥ 1.0 and ≤ rowCount
     */
    public static double estimate(TypeCategory category,
                                  double minDouble, double maxDouble,
                                  long colSizeBytes, long colSizeRcnt,
                                  long rowCount) {
        if (rowCount <= 0) {
            return 1.0;
        }

        // Tier 1: range-based for types with a meaningful integral range
        if (!Double.isNaN(minDouble) && !Double.isNaN(maxDouble) && isRangeEstimable(category)) {
            double diff = diffInNativeUnits(category, maxDouble - minDouble);
            // Guard against overflow for very wide BIGINT ranges
            long range = (diff >= (double) Long.MAX_VALUE) ? Long.MAX_VALUE : Math.max(0L, (long) diff) + 1;
            double rangeNdv = Math.min(rowCount, range);
            double sizeNdv = sizeNdv(category, colSizeBytes, colSizeRcnt, rowCount);
            // Take min with size-based to stay conservative when range >> actual NDV
            return Math.max(1.0, Double.isNaN(sizeNdv) ? rangeNdv : Math.min(rangeNdv, sizeNdv));
        }

        // Tier 2: size-based
        double sizeNdv = sizeNdv(category, colSizeBytes, colSizeRcnt, rowCount);
        if (!Double.isNaN(sizeNdv)) {
            return sizeNdv;
        }

        // Tier 3: type-fraction last resort
        return typeNdv(category, rowCount);
    }

    // -------------------------------------------------------------------------
    // Sub-estimators (package-private for unit tests)
    // -------------------------------------------------------------------------

    static boolean isRangeEstimable(TypeCategory category) {
        return category == TypeCategory.BOOLEAN
                || category == TypeCategory.INTEGER_LIKE
                || category == TypeCategory.DATE_IN_EPOCH_SECONDS
                || category == TypeCategory.TIMESTAMP_IN_EPOCH_SECONDS
                || category == TypeCategory.TIMESTAMP_IN_EPOCH_MICROS;
    }

    /**
     * Converts a diff expressed in stored-double units to the native distinct-value unit so that
     * {@code diffInNativeUnits + 1} approximates the count of distinct values in the range.
     */
    static double diffInNativeUnits(TypeCategory category, double diff) {
        if (category == TypeCategory.DATE_IN_EPOCH_SECONDS) {
            // stored as epoch-seconds; native unit is day
            return diff / 86400.0;
        } else if (category == TypeCategory.TIMESTAMP_IN_EPOCH_MICROS) {
            // stored as epoch-seconds (converted from epoch-µs); native unit is microsecond
            return diff * 1_000_000.0;
        }
        return diff;
    }

    /**
     * Tier-2: estimate NDV from compressed column size.
     *
     * @return estimated NDV, or {@link Double#NaN} if size data is unavailable
     */
    static double sizeNdv(TypeCategory category, long colSizeBytes, long colSizeRcnt, long rowCount) {
        if (colSizeBytes < 0 || colSizeRcnt <= 0) {
            return Double.NaN;
        }
        double ndv = (double) colSizeBytes / minBytesPerDistinctValue(category);
        // Extrapolate to the full table when size metrics cover only a subset of rows
        if (colSizeRcnt < rowCount) {
            ndv = ndv * rowCount / colSizeRcnt;
        }
        return Math.max(1.0, Math.min(rowCount, ndv));
    }

    /**
     * Tier-3: type-fraction fallback.  Always returns a value in [1, rowCount].
     */
    public static double typeNdv(TypeCategory category, long rowCount) {
        if (category == TypeCategory.BOOLEAN) {
            return Math.min(2.0, rowCount);
        }
        double fraction = (category == TypeCategory.STRING_LIKE) ? 0.5 : 0.3;
        return Math.max(1.0, Math.min(rowCount, rowCount * fraction));
    }

    /** Min bytes per distinct compressed value; used for Tier-2 size estimation. */
    static int minBytesPerDistinctValue(TypeCategory category) {
        if (category == TypeCategory.BOOLEAN) {
            return 1;
        } else if (category == TypeCategory.TIMESTAMP_IN_EPOCH_SECONDS
                || category == TypeCategory.TIMESTAMP_IN_EPOCH_MICROS) {
            return 8;
        }
        return 4; // INTEGER_LIKE, DATE, STRING, OTHER: 4 bytes (dict-encoded strings assumed)
    }

    /**
     * Maps a StarRocks {@link Type} to the closest {@link TypeCategory}.
     * Used by connectors that work with StarRocks column metadata (e.g. JDBC).
     */
    public static TypeCategory fromStarRocksType(Type t) {
        if (t.isBoolean()) {
            return TypeCategory.BOOLEAN;
        } else if (t.isIntegerType() || t.isLargeIntType()) {
            return TypeCategory.INTEGER_LIKE;
        } else if (t.isFloatingPointType() || t.isDecimalOfAnyVersion()) {
            return TypeCategory.FLOAT_LIKE;
        } else if (t.isDateType() && !t.isDatetime()) {
            return TypeCategory.DATE_IN_EPOCH_SECONDS;
        } else if (t.isDatetime() || t.isTime()) {
            return TypeCategory.TIMESTAMP_IN_EPOCH_SECONDS;
        } else if (t.isStringType()) {
            return TypeCategory.STRING_LIKE;
        }
        return TypeCategory.OTHER;
    }
}
