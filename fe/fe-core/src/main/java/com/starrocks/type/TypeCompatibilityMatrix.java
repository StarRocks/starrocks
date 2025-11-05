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

package com.starrocks.type;

import com.google.common.base.Preconditions;

import java.util.Arrays;
import java.util.List;

/**
 * Type compatibility matrix that records "smallest" assignment-compatible type of two types.
 * A value of any of the two types could be assigned to a slot of the assignment-compatible type.
 * For strict compatibility, this can be done without any loss of precision.
 * For non-strict compatibility, there may be loss of precision, e.g. if converting from BIGINT to FLOAT.
 */
public class TypeCompatibilityMatrix {
    private static final List<PrimitiveType> SKIP_COMPARE_TYPES = Arrays.asList(
            PrimitiveType.INVALID_TYPE, PrimitiveType.NULL_TYPE, PrimitiveType.DECIMALV2,
            PrimitiveType.DECIMAL32, PrimitiveType.DECIMAL64, PrimitiveType.DECIMAL128, PrimitiveType.DECIMAL256,
            PrimitiveType.TIME, PrimitiveType.JSON, PrimitiveType.FUNCTION,
            PrimitiveType.BINARY, PrimitiveType.VARBINARY, PrimitiveType.VARIANT);

    private static final PrimitiveType[][] COMPATIBILITY_MATRIX =
            new PrimitiveType[PrimitiveType.values().length][PrimitiveType.values().length];

    static {
        initializeMatrix();
    }

    private static void initializeMatrix() {
        for (int i = 0; i < PrimitiveType.values().length; ++i) {
            COMPATIBILITY_MATRIX[i][i] = PrimitiveType.values()[i];
        }

        // BOOLEAN
        COMPATIBILITY_MATRIX[StandardTypes.BOOLEAN.ordinal()][StandardTypes.TINYINT.ordinal()] = PrimitiveType.TINYINT;
        COMPATIBILITY_MATRIX[StandardTypes.BOOLEAN.ordinal()][StandardTypes.SMALLINT.ordinal()] = PrimitiveType.SMALLINT;
        COMPATIBILITY_MATRIX[StandardTypes.BOOLEAN.ordinal()][StandardTypes.INT.ordinal()] = PrimitiveType.INT;
        COMPATIBILITY_MATRIX[StandardTypes.BOOLEAN.ordinal()][StandardTypes.BIGINT.ordinal()] = PrimitiveType.BIGINT;
        COMPATIBILITY_MATRIX[StandardTypes.BOOLEAN.ordinal()][StandardTypes.LARGEINT.ordinal()] = PrimitiveType.LARGEINT;
        COMPATIBILITY_MATRIX[StandardTypes.BOOLEAN.ordinal()][StandardTypes.FLOAT.ordinal()] = PrimitiveType.FLOAT;
        COMPATIBILITY_MATRIX[StandardTypes.BOOLEAN.ordinal()][StandardTypes.DOUBLE.ordinal()] = PrimitiveType.DOUBLE;
        COMPATIBILITY_MATRIX[StandardTypes.BOOLEAN.ordinal()][StandardTypes.DATE.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.BOOLEAN.ordinal()][StandardTypes.DATETIME.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.BOOLEAN.ordinal()][StandardTypes.CHAR.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.BOOLEAN.ordinal()][StandardTypes.VARCHAR.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.BOOLEAN.ordinal()][StandardTypes.HLL.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.BOOLEAN.ordinal()][StandardTypes.TIME.ordinal()] = PrimitiveType.DOUBLE;
        COMPATIBILITY_MATRIX[StandardTypes.BOOLEAN.ordinal()][StandardTypes.BITMAP.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.BOOLEAN.ordinal()][StandardTypes.PERCENTILE.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.BOOLEAN.ordinal()][StandardTypes.JSON.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.BOOLEAN.ordinal()][StandardTypes.UNKNOWN_TYPE.ordinal()] = PrimitiveType.INVALID_TYPE;

        // TINYINT
        COMPATIBILITY_MATRIX[StandardTypes.TINYINT.ordinal()][StandardTypes.SMALLINT.ordinal()] = PrimitiveType.SMALLINT;
        COMPATIBILITY_MATRIX[StandardTypes.TINYINT.ordinal()][StandardTypes.INT.ordinal()] = PrimitiveType.INT;
        COMPATIBILITY_MATRIX[StandardTypes.TINYINT.ordinal()][StandardTypes.BIGINT.ordinal()] = PrimitiveType.BIGINT;
        COMPATIBILITY_MATRIX[StandardTypes.TINYINT.ordinal()][StandardTypes.LARGEINT.ordinal()] = PrimitiveType.LARGEINT;
        COMPATIBILITY_MATRIX[StandardTypes.TINYINT.ordinal()][StandardTypes.FLOAT.ordinal()] = PrimitiveType.FLOAT;
        COMPATIBILITY_MATRIX[StandardTypes.TINYINT.ordinal()][StandardTypes.DOUBLE.ordinal()] = PrimitiveType.DOUBLE;
        COMPATIBILITY_MATRIX[StandardTypes.TINYINT.ordinal()][StandardTypes.DATE.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.TINYINT.ordinal()][StandardTypes.DATETIME.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.TINYINT.ordinal()][StandardTypes.CHAR.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.TINYINT.ordinal()][StandardTypes.VARCHAR.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.TINYINT.ordinal()][StandardTypes.DECIMALV2.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.TINYINT.ordinal()][StandardTypes.HLL.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.TINYINT.ordinal()][StandardTypes.TIME.ordinal()] = PrimitiveType.DOUBLE;
        COMPATIBILITY_MATRIX[StandardTypes.TINYINT.ordinal()][StandardTypes.BITMAP.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.TINYINT.ordinal()][StandardTypes.PERCENTILE.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.TINYINT.ordinal()][StandardTypes.DECIMAL32.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.TINYINT.ordinal()][StandardTypes.DECIMAL64.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.TINYINT.ordinal()][StandardTypes.DECIMAL128.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.TINYINT.ordinal()][StandardTypes.DECIMAL256.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.TINYINT.ordinal()][StandardTypes.JSON.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.TINYINT.ordinal()][StandardTypes.UNKNOWN_TYPE.ordinal()] = PrimitiveType.INVALID_TYPE;

        // SMALLINT
        COMPATIBILITY_MATRIX[StandardTypes.SMALLINT.ordinal()][StandardTypes.INT.ordinal()] = PrimitiveType.INT;
        COMPATIBILITY_MATRIX[StandardTypes.SMALLINT.ordinal()][StandardTypes.BIGINT.ordinal()] = PrimitiveType.BIGINT;
        COMPATIBILITY_MATRIX[StandardTypes.SMALLINT.ordinal()][StandardTypes.LARGEINT.ordinal()] = PrimitiveType.LARGEINT;
        COMPATIBILITY_MATRIX[StandardTypes.SMALLINT.ordinal()][StandardTypes.FLOAT.ordinal()] = PrimitiveType.FLOAT;
        COMPATIBILITY_MATRIX[StandardTypes.SMALLINT.ordinal()][StandardTypes.DOUBLE.ordinal()] = PrimitiveType.DOUBLE;
        COMPATIBILITY_MATRIX[StandardTypes.SMALLINT.ordinal()][StandardTypes.DATE.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.SMALLINT.ordinal()][StandardTypes.DATETIME.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.SMALLINT.ordinal()][StandardTypes.CHAR.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.SMALLINT.ordinal()][StandardTypes.VARCHAR.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.SMALLINT.ordinal()][StandardTypes.DECIMALV2.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.SMALLINT.ordinal()][StandardTypes.HLL.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.SMALLINT.ordinal()][StandardTypes.TIME.ordinal()] = PrimitiveType.DOUBLE;
        COMPATIBILITY_MATRIX[StandardTypes.SMALLINT.ordinal()][StandardTypes.BITMAP.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.SMALLINT.ordinal()][StandardTypes.PERCENTILE.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.SMALLINT.ordinal()][StandardTypes.DECIMAL32.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.SMALLINT.ordinal()][StandardTypes.DECIMAL64.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.SMALLINT.ordinal()][StandardTypes.DECIMAL128.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.SMALLINT.ordinal()][StandardTypes.DECIMAL256.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.SMALLINT.ordinal()][StandardTypes.JSON.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.SMALLINT.ordinal()][StandardTypes.UNKNOWN_TYPE.ordinal()] = PrimitiveType.INVALID_TYPE;

        // INT
        COMPATIBILITY_MATRIX[StandardTypes.INT.ordinal()][StandardTypes.BIGINT.ordinal()] = PrimitiveType.BIGINT;
        COMPATIBILITY_MATRIX[StandardTypes.INT.ordinal()][StandardTypes.LARGEINT.ordinal()] = PrimitiveType.LARGEINT;
        COMPATIBILITY_MATRIX[StandardTypes.INT.ordinal()][StandardTypes.FLOAT.ordinal()] = PrimitiveType.DOUBLE;
        COMPATIBILITY_MATRIX[StandardTypes.INT.ordinal()][StandardTypes.DOUBLE.ordinal()] = PrimitiveType.DOUBLE;
        COMPATIBILITY_MATRIX[StandardTypes.INT.ordinal()][StandardTypes.DATE.ordinal()] = PrimitiveType.INT;
        COMPATIBILITY_MATRIX[StandardTypes.INT.ordinal()][StandardTypes.DATETIME.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.INT.ordinal()][StandardTypes.CHAR.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.INT.ordinal()][StandardTypes.VARCHAR.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.INT.ordinal()][StandardTypes.DECIMALV2.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.INT.ordinal()][StandardTypes.HLL.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.INT.ordinal()][StandardTypes.TIME.ordinal()] = PrimitiveType.DOUBLE;
        COMPATIBILITY_MATRIX[StandardTypes.INT.ordinal()][StandardTypes.BITMAP.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.INT.ordinal()][StandardTypes.PERCENTILE.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.INT.ordinal()][StandardTypes.DECIMAL32.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.INT.ordinal()][StandardTypes.DECIMAL64.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.INT.ordinal()][StandardTypes.DECIMAL128.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.INT.ordinal()][StandardTypes.DECIMAL256.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.INT.ordinal()][StandardTypes.JSON.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.INT.ordinal()][StandardTypes.UNKNOWN_TYPE.ordinal()] = PrimitiveType.INVALID_TYPE;

        // BIGINT
        COMPATIBILITY_MATRIX[StandardTypes.BIGINT.ordinal()][StandardTypes.LARGEINT.ordinal()] = PrimitiveType.LARGEINT;
        COMPATIBILITY_MATRIX[StandardTypes.BIGINT.ordinal()][StandardTypes.FLOAT.ordinal()] = PrimitiveType.DOUBLE;
        COMPATIBILITY_MATRIX[StandardTypes.BIGINT.ordinal()][StandardTypes.DOUBLE.ordinal()] = PrimitiveType.DOUBLE;
        COMPATIBILITY_MATRIX[StandardTypes.BIGINT.ordinal()][StandardTypes.DATE.ordinal()] = PrimitiveType.BIGINT;
        COMPATIBILITY_MATRIX[StandardTypes.BIGINT.ordinal()][StandardTypes.DATETIME.ordinal()] = PrimitiveType.BIGINT;
        COMPATIBILITY_MATRIX[StandardTypes.BIGINT.ordinal()][StandardTypes.CHAR.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.BIGINT.ordinal()][StandardTypes.VARCHAR.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.BIGINT.ordinal()][StandardTypes.DECIMALV2.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.BIGINT.ordinal()][StandardTypes.HLL.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.BIGINT.ordinal()][StandardTypes.TIME.ordinal()] = PrimitiveType.DOUBLE;
        COMPATIBILITY_MATRIX[StandardTypes.BIGINT.ordinal()][StandardTypes.BITMAP.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.BIGINT.ordinal()][StandardTypes.PERCENTILE.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.BIGINT.ordinal()][StandardTypes.DECIMAL32.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.BIGINT.ordinal()][StandardTypes.DECIMAL64.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.BIGINT.ordinal()][StandardTypes.DECIMAL128.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.BIGINT.ordinal()][StandardTypes.DECIMAL256.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.BIGINT.ordinal()][StandardTypes.JSON.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.BIGINT.ordinal()][StandardTypes.UNKNOWN_TYPE.ordinal()] = PrimitiveType.INVALID_TYPE;

        // LARGEINT
        COMPATIBILITY_MATRIX[StandardTypes.LARGEINT.ordinal()][StandardTypes.FLOAT.ordinal()] = PrimitiveType.DOUBLE;
        COMPATIBILITY_MATRIX[StandardTypes.LARGEINT.ordinal()][StandardTypes.DOUBLE.ordinal()] = PrimitiveType.DOUBLE;
        COMPATIBILITY_MATRIX[StandardTypes.LARGEINT.ordinal()][StandardTypes.DATE.ordinal()] = PrimitiveType.LARGEINT;
        COMPATIBILITY_MATRIX[StandardTypes.LARGEINT.ordinal()][StandardTypes.DATETIME.ordinal()] = PrimitiveType.LARGEINT;
        COMPATIBILITY_MATRIX[StandardTypes.LARGEINT.ordinal()][StandardTypes.CHAR.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.LARGEINT.ordinal()][StandardTypes.VARCHAR.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.LARGEINT.ordinal()][StandardTypes.DECIMALV2.ordinal()] = PrimitiveType.DECIMALV2;
        COMPATIBILITY_MATRIX[StandardTypes.LARGEINT.ordinal()][StandardTypes.HLL.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.LARGEINT.ordinal()][StandardTypes.TIME.ordinal()] = PrimitiveType.DOUBLE;
        COMPATIBILITY_MATRIX[StandardTypes.LARGEINT.ordinal()][StandardTypes.BITMAP.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.LARGEINT.ordinal()][StandardTypes.PERCENTILE.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.LARGEINT.ordinal()][StandardTypes.DECIMAL32.ordinal()] = PrimitiveType.DECIMAL32;
        COMPATIBILITY_MATRIX[StandardTypes.LARGEINT.ordinal()][StandardTypes.DECIMAL64.ordinal()] = PrimitiveType.DECIMAL64;
        COMPATIBILITY_MATRIX[StandardTypes.LARGEINT.ordinal()][StandardTypes.DECIMAL128.ordinal()] = PrimitiveType.DECIMAL128;
        COMPATIBILITY_MATRIX[StandardTypes.LARGEINT.ordinal()][StandardTypes.DECIMAL256.ordinal()] = PrimitiveType.DECIMAL256;
        COMPATIBILITY_MATRIX[StandardTypes.LARGEINT.ordinal()][StandardTypes.JSON.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.LARGEINT.ordinal()][StandardTypes.UNKNOWN_TYPE.ordinal()] = PrimitiveType.INVALID_TYPE;

        // FLOAT
        COMPATIBILITY_MATRIX[StandardTypes.FLOAT.ordinal()][StandardTypes.DOUBLE.ordinal()] = PrimitiveType.DOUBLE;
        COMPATIBILITY_MATRIX[StandardTypes.FLOAT.ordinal()][StandardTypes.DATE.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.FLOAT.ordinal()][StandardTypes.DATETIME.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.FLOAT.ordinal()][StandardTypes.CHAR.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.FLOAT.ordinal()][StandardTypes.VARCHAR.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.FLOAT.ordinal()][StandardTypes.DECIMALV2.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.FLOAT.ordinal()][StandardTypes.HLL.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.FLOAT.ordinal()][StandardTypes.TIME.ordinal()] = PrimitiveType.DOUBLE;
        COMPATIBILITY_MATRIX[StandardTypes.FLOAT.ordinal()][StandardTypes.BITMAP.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.FLOAT.ordinal()][StandardTypes.PERCENTILE.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.FLOAT.ordinal()][StandardTypes.DECIMAL32.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.FLOAT.ordinal()][StandardTypes.DECIMAL64.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.FLOAT.ordinal()][StandardTypes.DECIMAL128.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.FLOAT.ordinal()][StandardTypes.DECIMAL256.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.FLOAT.ordinal()][StandardTypes.JSON.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.FLOAT.ordinal()][StandardTypes.UNKNOWN_TYPE.ordinal()] = PrimitiveType.INVALID_TYPE;

        // DOUBLE
        COMPATIBILITY_MATRIX[StandardTypes.DOUBLE.ordinal()][StandardTypes.DATE.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.DOUBLE.ordinal()][StandardTypes.DATETIME.ordinal()] = PrimitiveType.DOUBLE;
        COMPATIBILITY_MATRIX[StandardTypes.DOUBLE.ordinal()][StandardTypes.CHAR.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.DOUBLE.ordinal()][StandardTypes.VARCHAR.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.DOUBLE.ordinal()][StandardTypes.DECIMALV2.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.DOUBLE.ordinal()][StandardTypes.HLL.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.DOUBLE.ordinal()][StandardTypes.TIME.ordinal()] = PrimitiveType.DOUBLE;
        COMPATIBILITY_MATRIX[StandardTypes.DOUBLE.ordinal()][StandardTypes.BITMAP.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.DOUBLE.ordinal()][StandardTypes.PERCENTILE.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.DOUBLE.ordinal()][StandardTypes.DECIMAL32.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.DOUBLE.ordinal()][StandardTypes.DECIMAL64.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.DOUBLE.ordinal()][StandardTypes.DECIMAL128.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.DOUBLE.ordinal()][StandardTypes.DECIMAL256.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.DOUBLE.ordinal()][StandardTypes.UNKNOWN_TYPE.ordinal()] = PrimitiveType.INVALID_TYPE;

        // DATE
        COMPATIBILITY_MATRIX[StandardTypes.DATE.ordinal()][StandardTypes.DATETIME.ordinal()] = PrimitiveType.DATETIME;
        COMPATIBILITY_MATRIX[StandardTypes.DATE.ordinal()][StandardTypes.CHAR.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.DATE.ordinal()][StandardTypes.VARCHAR.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.DATE.ordinal()][StandardTypes.DECIMALV2.ordinal()] = PrimitiveType.DECIMALV2;
        COMPATIBILITY_MATRIX[StandardTypes.DATE.ordinal()][StandardTypes.HLL.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.DATE.ordinal()][StandardTypes.TIME.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.DATE.ordinal()][StandardTypes.BITMAP.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.DATE.ordinal()][StandardTypes.PERCENTILE.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.DATE.ordinal()][StandardTypes.DECIMAL32.ordinal()] = PrimitiveType.DECIMAL32;
        COMPATIBILITY_MATRIX[StandardTypes.DATE.ordinal()][StandardTypes.DECIMAL64.ordinal()] = PrimitiveType.DECIMAL64;
        COMPATIBILITY_MATRIX[StandardTypes.DATE.ordinal()][StandardTypes.DECIMAL128.ordinal()] = PrimitiveType.DECIMAL128;
        COMPATIBILITY_MATRIX[StandardTypes.DATE.ordinal()][StandardTypes.UNKNOWN_TYPE.ordinal()] = PrimitiveType.INVALID_TYPE;

        // DATETIME
        COMPATIBILITY_MATRIX[StandardTypes.DATETIME.ordinal()][StandardTypes.CHAR.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.DATETIME.ordinal()][StandardTypes.VARCHAR.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.DATETIME.ordinal()][StandardTypes.DECIMALV2.ordinal()] = PrimitiveType.DECIMALV2;
        COMPATIBILITY_MATRIX[StandardTypes.DATETIME.ordinal()][StandardTypes.HLL.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.DATETIME.ordinal()][StandardTypes.TIME.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.DATETIME.ordinal()][StandardTypes.BITMAP.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.DATETIME.ordinal()][StandardTypes.PERCENTILE.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.DATETIME.ordinal()][StandardTypes.DECIMAL32.ordinal()] = PrimitiveType.DECIMAL32;
        COMPATIBILITY_MATRIX[StandardTypes.DATETIME.ordinal()][StandardTypes.DECIMAL64.ordinal()] = PrimitiveType.DECIMAL64;
        COMPATIBILITY_MATRIX[StandardTypes.DATETIME.ordinal()][StandardTypes.DECIMAL128.ordinal()] = PrimitiveType.DECIMAL128;
        COMPATIBILITY_MATRIX[StandardTypes.DATETIME.ordinal()][StandardTypes.UNKNOWN_TYPE.ordinal()] = PrimitiveType.INVALID_TYPE;

        // CHAR
        COMPATIBILITY_MATRIX[StandardTypes.CHAR.ordinal()][StandardTypes.VARCHAR.ordinal()] = PrimitiveType.VARCHAR;
        COMPATIBILITY_MATRIX[StandardTypes.CHAR.ordinal()][StandardTypes.DECIMALV2.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.CHAR.ordinal()][StandardTypes.HLL.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.CHAR.ordinal()][StandardTypes.TIME.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.CHAR.ordinal()][StandardTypes.BITMAP.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.CHAR.ordinal()][StandardTypes.PERCENTILE.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.CHAR.ordinal()][StandardTypes.DECIMAL32.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.CHAR.ordinal()][StandardTypes.DECIMAL64.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.CHAR.ordinal()][StandardTypes.DECIMAL128.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.CHAR.ordinal()][StandardTypes.DECIMAL256.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.CHAR.ordinal()][StandardTypes.UNKNOWN_TYPE.ordinal()] = PrimitiveType.INVALID_TYPE;

        // VARCHAR
        COMPATIBILITY_MATRIX[StandardTypes.VARCHAR.ordinal()][StandardTypes.DECIMALV2.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.VARCHAR.ordinal()][StandardTypes.HLL.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.VARCHAR.ordinal()][StandardTypes.TIME.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.VARCHAR.ordinal()][StandardTypes.BITMAP.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.VARCHAR.ordinal()][StandardTypes.PERCENTILE.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.VARCHAR.ordinal()][StandardTypes.DECIMAL32.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.VARCHAR.ordinal()][StandardTypes.DECIMAL64.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.VARCHAR.ordinal()][StandardTypes.DECIMAL128.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.VARCHAR.ordinal()][StandardTypes.DECIMAL256.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.VARCHAR.ordinal()][StandardTypes.UNKNOWN_TYPE.ordinal()] = PrimitiveType.INVALID_TYPE;

        // DECIMALV2
        COMPATIBILITY_MATRIX[StandardTypes.DECIMALV2.ordinal()][StandardTypes.HLL.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.DECIMALV2.ordinal()][StandardTypes.TIME.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.DECIMALV2.ordinal()][StandardTypes.BITMAP.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.DECIMALV2.ordinal()][StandardTypes.PERCENTILE.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.DECIMALV2.ordinal()][StandardTypes.DECIMAL32.ordinal()] = PrimitiveType.DECIMAL32;
        COMPATIBILITY_MATRIX[StandardTypes.DECIMALV2.ordinal()][StandardTypes.DECIMAL64.ordinal()] = PrimitiveType.DECIMAL64;
        COMPATIBILITY_MATRIX[StandardTypes.DECIMALV2.ordinal()][StandardTypes.DECIMAL128.ordinal()] =
                PrimitiveType.DECIMAL128;
        COMPATIBILITY_MATRIX[StandardTypes.DECIMALV2.ordinal()][StandardTypes.UNKNOWN_TYPE.ordinal()] =
                PrimitiveType.INVALID_TYPE;

        // DECIMAL32
        COMPATIBILITY_MATRIX[StandardTypes.DECIMAL32.ordinal()][StandardTypes.HLL.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.DECIMAL32.ordinal()][StandardTypes.TIME.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.DECIMAL32.ordinal()][StandardTypes.BITMAP.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.DECIMAL32.ordinal()][StandardTypes.PERCENTILE.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.DECIMAL32.ordinal()][StandardTypes.DECIMAL32.ordinal()] = PrimitiveType.DECIMAL32;
        COMPATIBILITY_MATRIX[StandardTypes.DECIMAL32.ordinal()][StandardTypes.DECIMAL64.ordinal()] = PrimitiveType.DECIMAL64;
        COMPATIBILITY_MATRIX[StandardTypes.DECIMAL32.ordinal()][StandardTypes.DECIMAL128.ordinal()] = PrimitiveType.DECIMAL128;
        COMPATIBILITY_MATRIX[StandardTypes.DECIMAL32.ordinal()][StandardTypes.DECIMAL256.ordinal()] =
                PrimitiveType.DECIMAL256;
        COMPATIBILITY_MATRIX[StandardTypes.DECIMAL32.ordinal()][StandardTypes.UNKNOWN_TYPE.ordinal()] =
                PrimitiveType.INVALID_TYPE;

        // DECIMAL64
        COMPATIBILITY_MATRIX[StandardTypes.DECIMAL64.ordinal()][StandardTypes.HLL.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.DECIMAL64.ordinal()][StandardTypes.TIME.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.DECIMAL64.ordinal()][StandardTypes.BITMAP.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.DECIMAL64.ordinal()][StandardTypes.PERCENTILE.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.DECIMAL64.ordinal()][StandardTypes.DECIMAL32.ordinal()] = PrimitiveType.DECIMAL32;
        COMPATIBILITY_MATRIX[StandardTypes.DECIMAL64.ordinal()][StandardTypes.DECIMAL64.ordinal()] = PrimitiveType.DECIMAL64;
        COMPATIBILITY_MATRIX[StandardTypes.DECIMAL64.ordinal()][StandardTypes.DECIMAL128.ordinal()] = PrimitiveType.DECIMAL128;
        COMPATIBILITY_MATRIX[StandardTypes.DECIMAL64.ordinal()][StandardTypes.DECIMAL256.ordinal()] =
                PrimitiveType.DECIMAL256;
        COMPATIBILITY_MATRIX[StandardTypes.DECIMAL64.ordinal()][StandardTypes.UNKNOWN_TYPE.ordinal()] =
                PrimitiveType.INVALID_TYPE;

        // DECIMAL128
        COMPATIBILITY_MATRIX[StandardTypes.DECIMAL128.ordinal()][StandardTypes.HLL.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.DECIMAL128.ordinal()][StandardTypes.TIME.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.DECIMAL128.ordinal()][StandardTypes.BITMAP.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.DECIMAL128.ordinal()][StandardTypes.PERCENTILE.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.DECIMAL128.ordinal()][StandardTypes.DECIMAL32.ordinal()] = PrimitiveType.DECIMAL32;
        COMPATIBILITY_MATRIX[StandardTypes.DECIMAL128.ordinal()][StandardTypes.DECIMAL64.ordinal()] = PrimitiveType.DECIMAL64;
        COMPATIBILITY_MATRIX[StandardTypes.DECIMAL128.ordinal()][StandardTypes.DECIMAL128.ordinal()] =
                PrimitiveType.DECIMAL128;
        COMPATIBILITY_MATRIX[StandardTypes.DECIMAL128.ordinal()][StandardTypes.UNKNOWN_TYPE.ordinal()] =
                PrimitiveType.INVALID_TYPE;

        // DECIMAL256
        COMPATIBILITY_MATRIX[StandardTypes.DECIMAL256.ordinal()][StandardTypes.HLL.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.DECIMAL256.ordinal()][StandardTypes.TIME.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.DECIMAL256.ordinal()][StandardTypes.BITMAP.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.DECIMAL256.ordinal()][StandardTypes.PERCENTILE.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.DECIMAL256.ordinal()][StandardTypes.DECIMAL32.ordinal()] = PrimitiveType.DECIMAL32;
        COMPATIBILITY_MATRIX[StandardTypes.DECIMAL256.ordinal()][StandardTypes.DECIMAL64.ordinal()] = PrimitiveType.DECIMAL64;
        COMPATIBILITY_MATRIX[StandardTypes.DECIMAL256.ordinal()][StandardTypes.DECIMAL128.ordinal()] =
                PrimitiveType.DECIMAL128;
        COMPATIBILITY_MATRIX[StandardTypes.DECIMAL256.ordinal()][StandardTypes.UNKNOWN_TYPE.ordinal()] =
                PrimitiveType.INVALID_TYPE;

        // HLL
        COMPATIBILITY_MATRIX[StandardTypes.HLL.ordinal()][StandardTypes.TIME.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.HLL.ordinal()][StandardTypes.BITMAP.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.HLL.ordinal()][StandardTypes.PERCENTILE.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.HLL.ordinal()][StandardTypes.UNKNOWN_TYPE.ordinal()] = PrimitiveType.INVALID_TYPE;

        // BITMAP
        COMPATIBILITY_MATRIX[StandardTypes.BITMAP.ordinal()][StandardTypes.TIME.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.BITMAP.ordinal()][StandardTypes.PERCENTILE.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.BITMAP.ordinal()][StandardTypes.UNKNOWN_TYPE.ordinal()] = PrimitiveType.INVALID_TYPE;

        COMPATIBILITY_MATRIX[StandardTypes.PERCENTILE.ordinal()][StandardTypes.BITMAP.ordinal()] =
                PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.PERCENTILE.ordinal()][StandardTypes.UNKNOWN_TYPE.ordinal()] =
                PrimitiveType.INVALID_TYPE;

        // JSON
        for (PrimitiveType type : PrimitiveType.JSON_COMPATIBLE_TYPE) {
            ScalarType scalar = TypeFactory.createType(type);
            COMPATIBILITY_MATRIX[scalar.ordinal()][StandardTypes.JSON.ordinal()] = type;
        }
        for (PrimitiveType type : PrimitiveType.JSON_UNCOMPATIBLE_TYPE) {
            ScalarType scalar = TypeFactory.createType(type);
            COMPATIBILITY_MATRIX[scalar.ordinal()][StandardTypes.JSON.ordinal()] = PrimitiveType.INVALID_TYPE;
        }

        COMPATIBILITY_MATRIX[StandardTypes.JSON.ordinal()][StandardTypes.DATE.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.JSON.ordinal()][StandardTypes.DATETIME.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.JSON.ordinal()][StandardTypes.TIME.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.JSON.ordinal()][StandardTypes.DATE.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.JSON.ordinal()][StandardTypes.DATETIME.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.JSON.ordinal()][StandardTypes.DECIMAL32.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.JSON.ordinal()][StandardTypes.DECIMAL64.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.JSON.ordinal()][StandardTypes.DECIMAL128.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.JSON.ordinal()][StandardTypes.DECIMAL256.ordinal()] = PrimitiveType.INVALID_TYPE;

        // VARIANT
        for (PrimitiveType type : PrimitiveType.VARIANT_COMPATIBLE_TYPE) {
            ScalarType scalar = TypeFactory.createType(type);
            COMPATIBILITY_MATRIX[scalar.ordinal()][StandardTypes.VARIANT.ordinal()] = PrimitiveType.VARIANT;
        }
        for (PrimitiveType type : PrimitiveType.VARIANT_INCOMPATIBLE_TYPES) {
            ScalarType scalar = TypeFactory.createType(type);
            COMPATIBILITY_MATRIX[scalar.ordinal()][StandardTypes.VARIANT.ordinal()] = PrimitiveType.INVALID_TYPE;
        }
        COMPATIBILITY_MATRIX[StandardTypes.VARIANT.ordinal()][StandardTypes.DATE.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.VARIANT.ordinal()][StandardTypes.DATETIME.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.VARIANT.ordinal()][StandardTypes.TIME.ordinal()] = PrimitiveType.INVALID_TYPE;

        COMPATIBILITY_MATRIX[StandardTypes.VARIANT.ordinal()][StandardTypes.DECIMALV2.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.VARIANT.ordinal()][StandardTypes.DECIMAL32.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.VARIANT.ordinal()][StandardTypes.DECIMAL64.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.VARIANT.ordinal()][StandardTypes.DECIMAL128.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.VARIANT.ordinal()][StandardTypes.DECIMAL256.ordinal()] = PrimitiveType.INVALID_TYPE;

        COMPATIBILITY_MATRIX[StandardTypes.VARIANT.ordinal()][StandardTypes.HLL.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.VARIANT.ordinal()][StandardTypes.BITMAP.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.VARIANT.ordinal()][StandardTypes.PERCENTILE.ordinal()] = PrimitiveType.INVALID_TYPE;

        COMPATIBILITY_MATRIX[StandardTypes.VARIANT.ordinal()][StandardTypes.JSON.ordinal()] = PrimitiveType.INVALID_TYPE;

        COMPATIBILITY_MATRIX[StandardTypes.VARIANT.ordinal()][StandardTypes.VARBINARY.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[StandardTypes.VARIANT.ordinal()][StandardTypes.UNKNOWN_TYPE.ordinal()] = PrimitiveType.INVALID_TYPE;

        // binary type
        for (PrimitiveType type : PrimitiveType.BINARY_INCOMPATIBLE_TYPE_LIST) {
            ScalarType scalar = TypeFactory.createType(type);
            COMPATIBILITY_MATRIX[scalar.ordinal()][StandardTypes.VARBINARY.ordinal()] = PrimitiveType.INVALID_TYPE;
        }

        // Check all the necessary entries that should be filled.
        for (int i = 0; i < PrimitiveType.values().length - 2; ++i) {
            for (int j = i; j < PrimitiveType.values().length - 2; ++j) {
                PrimitiveType t1 = PrimitiveType.values()[i];
                PrimitiveType t2 = PrimitiveType.values()[j];
                if (SKIP_COMPARE_TYPES.contains(t1) || SKIP_COMPARE_TYPES.contains(t2)) {
                    continue;
                }
                Preconditions.checkNotNull(COMPATIBILITY_MATRIX[i][j]);
            }
        }
    }

    /**
     * Get the compatibility result for two primitive types.
     *
     * @param t1 first primitive type
     * @param t2 second primitive type
     * @return the compatible type, or INVALID_TYPE if incompatible
     */
    public static PrimitiveType getCompatibleType(PrimitiveType t1, PrimitiveType t2) {
        return COMPATIBILITY_MATRIX[t1.ordinal()][t2.ordinal()];
    }
}
