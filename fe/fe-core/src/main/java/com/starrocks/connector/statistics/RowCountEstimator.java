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

import com.starrocks.catalog.Column;
import com.starrocks.common.Config;
import com.starrocks.connector.hive.HiveStorageFormat;
import com.starrocks.type.ScalarType;

import java.util.List;

/**
 * Estimates the row count of an external file-based table from its total file size,
 * schema, and storage format — all available at planning time with zero extra IO.
 *
 * <p>Formula: {@code rowCount = max(totalBytes / estimatedRowSize, 1)}
 *
 * <p>Row-size is derived from column type widths scaled by a format-specific compression
 * factor:
 * <ul>
 *   <li>Parquet / ORC: 0.25  (columnar + compression reduces on-disk size ~4–10x)</li>
 *   <li>Text / Avro / RC: 1.5  (row-oriented, minimal encoding overhead)</li>
 *   <li>Unknown format: falls back to {@link Config#connector_row_size_estimate_bytes}</li>
 * </ul>
 */
public class RowCountEstimator {

    private static final double COLUMNAR_FORMAT_FACTOR = 0.25;
    private static final double ROW_FORMAT_FACTOR = 1.5;
    private static final long MIN_ROW_SIZE_BYTES = 8L;
    // VARCHAR/CHAR representative size cap — actual stored length varies widely.
    private static final int MAX_STRING_COL_SIZE = 64;

    private RowCountEstimator() {}

    /**
     * Estimates row count from total file bytes, column list, and storage format.
     *
     * @param totalBytes  sum of all file sizes in bytes
     * @param dataColumns schema columns (used only for type-size computation)
     * @param format      storage format; {@code null} or unsupported values use the config default
     * @return estimated row count, at least 1
     */
    public static long estimate(long totalBytes, List<Column> dataColumns, HiveStorageFormat format) {
        if (totalBytes <= 0) {
            return 1L;
        }
        long rowSize = computeRowSize(dataColumns, format);
        return Math.max(totalBytes / rowSize, 1L);
    }

    private static long computeRowSize(List<Column> dataColumns, HiveStorageFormat format) {
        Double factor = factorFor(format);
        if (factor == null) {
            // Unknown format — use the config default directly as bytes/row.
            return Math.max(Config.connector_row_size_estimate_bytes, MIN_ROW_SIZE_BYTES);
        }

        long rawSize = 0;
        for (Column col : dataColumns) {
            rawSize += colTypeSize(col);
        }
        if (rawSize <= 0) {
            return Math.max(Config.connector_row_size_estimate_bytes, MIN_ROW_SIZE_BYTES);
        }
        return Math.max((long) (rawSize * factor), MIN_ROW_SIZE_BYTES);
    }

    /**
     * Returns the compression factor for the given format, or {@code null} if unknown
     * (meaning the config default should be used instead of the schema-based estimate).
     */
    private static Double factorFor(HiveStorageFormat format) {
        if (format == null) {
            return null;
        }
        switch (format) {
            case PARQUET:
            case ORC:
                return COLUMNAR_FORMAT_FACTOR;
            case TEXTFILE:
            case AVRO:
            case RCTEXT:
            case RCBINARY:
                return ROW_FORMAT_FACTOR;
            default:
                return null;
        }
    }

    private static int colTypeSize(Column col) {
        if (col.getType().isStringType() && col.getType() instanceof ScalarType) {
            int len = ((ScalarType) col.getType()).getLength();
            return (len > 0) ? Math.min(len, MAX_STRING_COL_SIZE) : MAX_STRING_COL_SIZE;
        }
        return col.getType().getTypeSize();
    }
}
