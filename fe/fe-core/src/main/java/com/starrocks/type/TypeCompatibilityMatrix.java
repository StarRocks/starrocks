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
        COMPATIBILITY_MATRIX[Type.BOOLEAN.ordinal()][Type.TINYINT.ordinal()] = PrimitiveType.TINYINT;
        COMPATIBILITY_MATRIX[Type.BOOLEAN.ordinal()][Type.SMALLINT.ordinal()] = PrimitiveType.SMALLINT;
        COMPATIBILITY_MATRIX[Type.BOOLEAN.ordinal()][Type.INT.ordinal()] = PrimitiveType.INT;
        COMPATIBILITY_MATRIX[Type.BOOLEAN.ordinal()][Type.BIGINT.ordinal()] = PrimitiveType.BIGINT;
        COMPATIBILITY_MATRIX[Type.BOOLEAN.ordinal()][Type.LARGEINT.ordinal()] = PrimitiveType.LARGEINT;
        COMPATIBILITY_MATRIX[Type.BOOLEAN.ordinal()][Type.FLOAT.ordinal()] = PrimitiveType.FLOAT;
        COMPATIBILITY_MATRIX[Type.BOOLEAN.ordinal()][Type.DOUBLE.ordinal()] = PrimitiveType.DOUBLE;
        COMPATIBILITY_MATRIX[Type.BOOLEAN.ordinal()][Type.DATE.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.BOOLEAN.ordinal()][Type.DATETIME.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.BOOLEAN.ordinal()][Type.CHAR.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.BOOLEAN.ordinal()][Type.VARCHAR.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.BOOLEAN.ordinal()][Type.HLL.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.BOOLEAN.ordinal()][Type.TIME.ordinal()] = PrimitiveType.DOUBLE;
        COMPATIBILITY_MATRIX[Type.BOOLEAN.ordinal()][Type.BITMAP.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.BOOLEAN.ordinal()][Type.PERCENTILE.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.BOOLEAN.ordinal()][Type.JSON.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.BOOLEAN.ordinal()][Type.UNKNOWN_TYPE.ordinal()] = PrimitiveType.INVALID_TYPE;

        // TINYINT
        COMPATIBILITY_MATRIX[Type.TINYINT.ordinal()][Type.SMALLINT.ordinal()] = PrimitiveType.SMALLINT;
        COMPATIBILITY_MATRIX[Type.TINYINT.ordinal()][Type.INT.ordinal()] = PrimitiveType.INT;
        COMPATIBILITY_MATRIX[Type.TINYINT.ordinal()][Type.BIGINT.ordinal()] = PrimitiveType.BIGINT;
        COMPATIBILITY_MATRIX[Type.TINYINT.ordinal()][Type.LARGEINT.ordinal()] = PrimitiveType.LARGEINT;
        COMPATIBILITY_MATRIX[Type.TINYINT.ordinal()][Type.FLOAT.ordinal()] = PrimitiveType.FLOAT;
        COMPATIBILITY_MATRIX[Type.TINYINT.ordinal()][Type.DOUBLE.ordinal()] = PrimitiveType.DOUBLE;
        COMPATIBILITY_MATRIX[Type.TINYINT.ordinal()][Type.DATE.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.TINYINT.ordinal()][Type.DATETIME.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.TINYINT.ordinal()][Type.CHAR.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.TINYINT.ordinal()][Type.VARCHAR.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.TINYINT.ordinal()][Type.DECIMALV2.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.TINYINT.ordinal()][Type.HLL.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.TINYINT.ordinal()][Type.TIME.ordinal()] = PrimitiveType.DOUBLE;
        COMPATIBILITY_MATRIX[Type.TINYINT.ordinal()][Type.BITMAP.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.TINYINT.ordinal()][Type.PERCENTILE.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.TINYINT.ordinal()][Type.DECIMAL32.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.TINYINT.ordinal()][Type.DECIMAL64.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.TINYINT.ordinal()][Type.DECIMAL128.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.TINYINT.ordinal()][Type.DECIMAL256.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.TINYINT.ordinal()][Type.JSON.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.TINYINT.ordinal()][Type.UNKNOWN_TYPE.ordinal()] = PrimitiveType.INVALID_TYPE;

        // SMALLINT
        COMPATIBILITY_MATRIX[Type.SMALLINT.ordinal()][Type.INT.ordinal()] = PrimitiveType.INT;
        COMPATIBILITY_MATRIX[Type.SMALLINT.ordinal()][Type.BIGINT.ordinal()] = PrimitiveType.BIGINT;
        COMPATIBILITY_MATRIX[Type.SMALLINT.ordinal()][Type.LARGEINT.ordinal()] = PrimitiveType.LARGEINT;
        COMPATIBILITY_MATRIX[Type.SMALLINT.ordinal()][Type.FLOAT.ordinal()] = PrimitiveType.FLOAT;
        COMPATIBILITY_MATRIX[Type.SMALLINT.ordinal()][Type.DOUBLE.ordinal()] = PrimitiveType.DOUBLE;
        COMPATIBILITY_MATRIX[Type.SMALLINT.ordinal()][Type.DATE.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.SMALLINT.ordinal()][Type.DATETIME.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.SMALLINT.ordinal()][Type.CHAR.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.SMALLINT.ordinal()][Type.VARCHAR.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.SMALLINT.ordinal()][Type.DECIMALV2.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.SMALLINT.ordinal()][Type.HLL.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.SMALLINT.ordinal()][Type.TIME.ordinal()] = PrimitiveType.DOUBLE;
        COMPATIBILITY_MATRIX[Type.SMALLINT.ordinal()][Type.BITMAP.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.SMALLINT.ordinal()][Type.PERCENTILE.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.SMALLINT.ordinal()][Type.DECIMAL32.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.SMALLINT.ordinal()][Type.DECIMAL64.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.SMALLINT.ordinal()][Type.DECIMAL128.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.SMALLINT.ordinal()][Type.DECIMAL256.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.SMALLINT.ordinal()][Type.JSON.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.SMALLINT.ordinal()][Type.UNKNOWN_TYPE.ordinal()] = PrimitiveType.INVALID_TYPE;

        // INT
        COMPATIBILITY_MATRIX[Type.INT.ordinal()][Type.BIGINT.ordinal()] = PrimitiveType.BIGINT;
        COMPATIBILITY_MATRIX[Type.INT.ordinal()][Type.LARGEINT.ordinal()] = PrimitiveType.LARGEINT;
        COMPATIBILITY_MATRIX[Type.INT.ordinal()][Type.FLOAT.ordinal()] = PrimitiveType.DOUBLE;
        COMPATIBILITY_MATRIX[Type.INT.ordinal()][Type.DOUBLE.ordinal()] = PrimitiveType.DOUBLE;
        COMPATIBILITY_MATRIX[Type.INT.ordinal()][Type.DATE.ordinal()] = PrimitiveType.INT;
        COMPATIBILITY_MATRIX[Type.INT.ordinal()][Type.DATETIME.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.INT.ordinal()][Type.CHAR.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.INT.ordinal()][Type.VARCHAR.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.INT.ordinal()][Type.DECIMALV2.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.INT.ordinal()][Type.HLL.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.INT.ordinal()][Type.TIME.ordinal()] = PrimitiveType.DOUBLE;
        COMPATIBILITY_MATRIX[Type.INT.ordinal()][Type.BITMAP.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.INT.ordinal()][Type.PERCENTILE.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.INT.ordinal()][Type.DECIMAL32.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.INT.ordinal()][Type.DECIMAL64.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.INT.ordinal()][Type.DECIMAL128.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.INT.ordinal()][Type.DECIMAL256.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.INT.ordinal()][Type.JSON.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.INT.ordinal()][Type.UNKNOWN_TYPE.ordinal()] = PrimitiveType.INVALID_TYPE;

        // BIGINT
        COMPATIBILITY_MATRIX[Type.BIGINT.ordinal()][Type.LARGEINT.ordinal()] = PrimitiveType.LARGEINT;
        COMPATIBILITY_MATRIX[Type.BIGINT.ordinal()][Type.FLOAT.ordinal()] = PrimitiveType.DOUBLE;
        COMPATIBILITY_MATRIX[Type.BIGINT.ordinal()][Type.DOUBLE.ordinal()] = PrimitiveType.DOUBLE;
        COMPATIBILITY_MATRIX[Type.BIGINT.ordinal()][Type.DATE.ordinal()] = PrimitiveType.BIGINT;
        COMPATIBILITY_MATRIX[Type.BIGINT.ordinal()][Type.DATETIME.ordinal()] = PrimitiveType.BIGINT;
        COMPATIBILITY_MATRIX[Type.BIGINT.ordinal()][Type.CHAR.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.BIGINT.ordinal()][Type.VARCHAR.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.BIGINT.ordinal()][Type.DECIMALV2.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.BIGINT.ordinal()][Type.HLL.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.BIGINT.ordinal()][Type.TIME.ordinal()] = PrimitiveType.DOUBLE;
        COMPATIBILITY_MATRIX[Type.BIGINT.ordinal()][Type.BITMAP.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.BIGINT.ordinal()][Type.PERCENTILE.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.BIGINT.ordinal()][Type.DECIMAL32.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.BIGINT.ordinal()][Type.DECIMAL64.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.BIGINT.ordinal()][Type.DECIMAL128.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.BIGINT.ordinal()][Type.DECIMAL256.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.BIGINT.ordinal()][Type.JSON.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.BIGINT.ordinal()][Type.UNKNOWN_TYPE.ordinal()] = PrimitiveType.INVALID_TYPE;

        // LARGEINT
        COMPATIBILITY_MATRIX[Type.LARGEINT.ordinal()][Type.FLOAT.ordinal()] = PrimitiveType.DOUBLE;
        COMPATIBILITY_MATRIX[Type.LARGEINT.ordinal()][Type.DOUBLE.ordinal()] = PrimitiveType.DOUBLE;
        COMPATIBILITY_MATRIX[Type.LARGEINT.ordinal()][Type.DATE.ordinal()] = PrimitiveType.LARGEINT;
        COMPATIBILITY_MATRIX[Type.LARGEINT.ordinal()][Type.DATETIME.ordinal()] = PrimitiveType.LARGEINT;
        COMPATIBILITY_MATRIX[Type.LARGEINT.ordinal()][Type.CHAR.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.LARGEINT.ordinal()][Type.VARCHAR.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.LARGEINT.ordinal()][Type.DECIMALV2.ordinal()] = PrimitiveType.DECIMALV2;
        COMPATIBILITY_MATRIX[Type.LARGEINT.ordinal()][Type.HLL.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.LARGEINT.ordinal()][Type.TIME.ordinal()] = PrimitiveType.DOUBLE;
        COMPATIBILITY_MATRIX[Type.LARGEINT.ordinal()][Type.BITMAP.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.LARGEINT.ordinal()][Type.PERCENTILE.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.LARGEINT.ordinal()][Type.DECIMAL32.ordinal()] = PrimitiveType.DECIMAL32;
        COMPATIBILITY_MATRIX[Type.LARGEINT.ordinal()][Type.DECIMAL64.ordinal()] = PrimitiveType.DECIMAL64;
        COMPATIBILITY_MATRIX[Type.LARGEINT.ordinal()][Type.DECIMAL128.ordinal()] = PrimitiveType.DECIMAL128;
        COMPATIBILITY_MATRIX[Type.LARGEINT.ordinal()][Type.DECIMAL256.ordinal()] = PrimitiveType.DECIMAL256;
        COMPATIBILITY_MATRIX[Type.LARGEINT.ordinal()][Type.JSON.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.LARGEINT.ordinal()][Type.UNKNOWN_TYPE.ordinal()] = PrimitiveType.INVALID_TYPE;

        // FLOAT
        COMPATIBILITY_MATRIX[Type.FLOAT.ordinal()][Type.DOUBLE.ordinal()] = PrimitiveType.DOUBLE;
        COMPATIBILITY_MATRIX[Type.FLOAT.ordinal()][Type.DATE.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.FLOAT.ordinal()][Type.DATETIME.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.FLOAT.ordinal()][Type.CHAR.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.FLOAT.ordinal()][Type.VARCHAR.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.FLOAT.ordinal()][Type.DECIMALV2.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.FLOAT.ordinal()][Type.HLL.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.FLOAT.ordinal()][Type.TIME.ordinal()] = PrimitiveType.DOUBLE;
        COMPATIBILITY_MATRIX[Type.FLOAT.ordinal()][Type.BITMAP.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.FLOAT.ordinal()][Type.PERCENTILE.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.FLOAT.ordinal()][Type.DECIMAL32.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.FLOAT.ordinal()][Type.DECIMAL64.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.FLOAT.ordinal()][Type.DECIMAL128.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.FLOAT.ordinal()][Type.DECIMAL256.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.FLOAT.ordinal()][Type.JSON.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.FLOAT.ordinal()][Type.UNKNOWN_TYPE.ordinal()] = PrimitiveType.INVALID_TYPE;

        // DOUBLE
        COMPATIBILITY_MATRIX[Type.DOUBLE.ordinal()][Type.DATE.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.DOUBLE.ordinal()][Type.DATETIME.ordinal()] = PrimitiveType.DOUBLE;
        COMPATIBILITY_MATRIX[Type.DOUBLE.ordinal()][Type.CHAR.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.DOUBLE.ordinal()][Type.VARCHAR.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.DOUBLE.ordinal()][Type.DECIMALV2.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.DOUBLE.ordinal()][Type.HLL.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.DOUBLE.ordinal()][Type.TIME.ordinal()] = PrimitiveType.DOUBLE;
        COMPATIBILITY_MATRIX[Type.DOUBLE.ordinal()][Type.BITMAP.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.DOUBLE.ordinal()][Type.PERCENTILE.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.DOUBLE.ordinal()][Type.DECIMAL32.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.DOUBLE.ordinal()][Type.DECIMAL64.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.DOUBLE.ordinal()][Type.DECIMAL128.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.DOUBLE.ordinal()][Type.DECIMAL256.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.DOUBLE.ordinal()][Type.UNKNOWN_TYPE.ordinal()] = PrimitiveType.INVALID_TYPE;

        // DATE
        COMPATIBILITY_MATRIX[Type.DATE.ordinal()][Type.DATETIME.ordinal()] = PrimitiveType.DATETIME;
        COMPATIBILITY_MATRIX[Type.DATE.ordinal()][Type.CHAR.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.DATE.ordinal()][Type.VARCHAR.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.DATE.ordinal()][Type.DECIMALV2.ordinal()] = PrimitiveType.DECIMALV2;
        COMPATIBILITY_MATRIX[Type.DATE.ordinal()][Type.HLL.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.DATE.ordinal()][Type.TIME.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.DATE.ordinal()][Type.BITMAP.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.DATE.ordinal()][Type.PERCENTILE.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.DATE.ordinal()][Type.DECIMAL32.ordinal()] = PrimitiveType.DECIMAL32;
        COMPATIBILITY_MATRIX[Type.DATE.ordinal()][Type.DECIMAL64.ordinal()] = PrimitiveType.DECIMAL64;
        COMPATIBILITY_MATRIX[Type.DATE.ordinal()][Type.DECIMAL128.ordinal()] = PrimitiveType.DECIMAL128;
        COMPATIBILITY_MATRIX[Type.DATE.ordinal()][Type.UNKNOWN_TYPE.ordinal()] = PrimitiveType.INVALID_TYPE;

        // DATETIME
        COMPATIBILITY_MATRIX[Type.DATETIME.ordinal()][Type.CHAR.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.DATETIME.ordinal()][Type.VARCHAR.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.DATETIME.ordinal()][Type.DECIMALV2.ordinal()] = PrimitiveType.DECIMALV2;
        COMPATIBILITY_MATRIX[Type.DATETIME.ordinal()][Type.HLL.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.DATETIME.ordinal()][Type.TIME.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.DATETIME.ordinal()][Type.BITMAP.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.DATETIME.ordinal()][Type.PERCENTILE.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.DATETIME.ordinal()][Type.DECIMAL32.ordinal()] = PrimitiveType.DECIMAL32;
        COMPATIBILITY_MATRIX[Type.DATETIME.ordinal()][Type.DECIMAL64.ordinal()] = PrimitiveType.DECIMAL64;
        COMPATIBILITY_MATRIX[Type.DATETIME.ordinal()][Type.DECIMAL128.ordinal()] = PrimitiveType.DECIMAL128;
        COMPATIBILITY_MATRIX[Type.DATETIME.ordinal()][Type.UNKNOWN_TYPE.ordinal()] = PrimitiveType.INVALID_TYPE;

        // CHAR
        COMPATIBILITY_MATRIX[Type.CHAR.ordinal()][Type.VARCHAR.ordinal()] = PrimitiveType.VARCHAR;
        COMPATIBILITY_MATRIX[Type.CHAR.ordinal()][Type.DECIMALV2.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.CHAR.ordinal()][Type.HLL.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.CHAR.ordinal()][Type.TIME.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.CHAR.ordinal()][Type.BITMAP.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.CHAR.ordinal()][Type.PERCENTILE.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.CHAR.ordinal()][Type.DECIMAL32.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.CHAR.ordinal()][Type.DECIMAL64.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.CHAR.ordinal()][Type.DECIMAL128.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.CHAR.ordinal()][Type.DECIMAL256.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.CHAR.ordinal()][Type.UNKNOWN_TYPE.ordinal()] = PrimitiveType.INVALID_TYPE;

        // VARCHAR
        COMPATIBILITY_MATRIX[Type.VARCHAR.ordinal()][Type.DECIMALV2.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.VARCHAR.ordinal()][Type.HLL.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.VARCHAR.ordinal()][Type.TIME.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.VARCHAR.ordinal()][Type.BITMAP.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.VARCHAR.ordinal()][Type.PERCENTILE.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.VARCHAR.ordinal()][Type.DECIMAL32.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.VARCHAR.ordinal()][Type.DECIMAL64.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.VARCHAR.ordinal()][Type.DECIMAL128.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.VARCHAR.ordinal()][Type.DECIMAL256.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.VARCHAR.ordinal()][Type.UNKNOWN_TYPE.ordinal()] = PrimitiveType.INVALID_TYPE;

        // DECIMALV2
        COMPATIBILITY_MATRIX[Type.DECIMALV2.ordinal()][Type.HLL.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.DECIMALV2.ordinal()][Type.TIME.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.DECIMALV2.ordinal()][Type.BITMAP.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.DECIMALV2.ordinal()][Type.PERCENTILE.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.DECIMALV2.ordinal()][Type.DECIMAL32.ordinal()] = PrimitiveType.DECIMAL32;
        COMPATIBILITY_MATRIX[Type.DECIMALV2.ordinal()][Type.DECIMAL64.ordinal()] = PrimitiveType.DECIMAL64;
        COMPATIBILITY_MATRIX[Type.DECIMALV2.ordinal()][Type.DECIMAL128.ordinal()] = PrimitiveType.DECIMAL128;
        COMPATIBILITY_MATRIX[Type.DECIMALV2.ordinal()][Type.UNKNOWN_TYPE.ordinal()] = PrimitiveType.INVALID_TYPE;

        // DECIMAL32
        COMPATIBILITY_MATRIX[Type.DECIMAL32.ordinal()][Type.HLL.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.DECIMAL32.ordinal()][Type.TIME.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.DECIMAL32.ordinal()][Type.BITMAP.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.DECIMAL32.ordinal()][Type.PERCENTILE.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.DECIMAL32.ordinal()][Type.DECIMAL32.ordinal()] = PrimitiveType.DECIMAL32;
        COMPATIBILITY_MATRIX[Type.DECIMAL32.ordinal()][Type.DECIMAL64.ordinal()] = PrimitiveType.DECIMAL64;
        COMPATIBILITY_MATRIX[Type.DECIMAL32.ordinal()][Type.DECIMAL128.ordinal()] = PrimitiveType.DECIMAL128;
        COMPATIBILITY_MATRIX[Type.DECIMAL32.ordinal()][Type.DECIMAL256.ordinal()] = PrimitiveType.DECIMAL256;
        COMPATIBILITY_MATRIX[Type.DECIMAL32.ordinal()][Type.UNKNOWN_TYPE.ordinal()] = PrimitiveType.INVALID_TYPE;

        // DECIMAL64
        COMPATIBILITY_MATRIX[Type.DECIMAL64.ordinal()][Type.HLL.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.DECIMAL64.ordinal()][Type.TIME.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.DECIMAL64.ordinal()][Type.BITMAP.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.DECIMAL64.ordinal()][Type.PERCENTILE.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.DECIMAL64.ordinal()][Type.DECIMAL32.ordinal()] = PrimitiveType.DECIMAL32;
        COMPATIBILITY_MATRIX[Type.DECIMAL64.ordinal()][Type.DECIMAL64.ordinal()] = PrimitiveType.DECIMAL64;
        COMPATIBILITY_MATRIX[Type.DECIMAL64.ordinal()][Type.DECIMAL128.ordinal()] = PrimitiveType.DECIMAL128;
        COMPATIBILITY_MATRIX[Type.DECIMAL64.ordinal()][Type.DECIMAL256.ordinal()] = PrimitiveType.DECIMAL256;
        COMPATIBILITY_MATRIX[Type.DECIMAL64.ordinal()][Type.UNKNOWN_TYPE.ordinal()] = PrimitiveType.INVALID_TYPE;

        // DECIMAL128
        COMPATIBILITY_MATRIX[Type.DECIMAL128.ordinal()][Type.HLL.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.DECIMAL128.ordinal()][Type.TIME.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.DECIMAL128.ordinal()][Type.BITMAP.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.DECIMAL128.ordinal()][Type.PERCENTILE.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.DECIMAL128.ordinal()][Type.DECIMAL32.ordinal()] = PrimitiveType.DECIMAL32;
        COMPATIBILITY_MATRIX[Type.DECIMAL128.ordinal()][Type.DECIMAL64.ordinal()] = PrimitiveType.DECIMAL64;
        COMPATIBILITY_MATRIX[Type.DECIMAL128.ordinal()][Type.DECIMAL128.ordinal()] = PrimitiveType.DECIMAL128;
        COMPATIBILITY_MATRIX[Type.DECIMAL128.ordinal()][Type.UNKNOWN_TYPE.ordinal()] = PrimitiveType.INVALID_TYPE;

        // DECIMAL256
        COMPATIBILITY_MATRIX[Type.DECIMAL256.ordinal()][Type.HLL.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.DECIMAL256.ordinal()][Type.TIME.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.DECIMAL256.ordinal()][Type.BITMAP.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.DECIMAL256.ordinal()][Type.PERCENTILE.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.DECIMAL256.ordinal()][Type.DECIMAL32.ordinal()] = PrimitiveType.DECIMAL32;
        COMPATIBILITY_MATRIX[Type.DECIMAL256.ordinal()][Type.DECIMAL64.ordinal()] = PrimitiveType.DECIMAL64;
        COMPATIBILITY_MATRIX[Type.DECIMAL256.ordinal()][Type.DECIMAL128.ordinal()] = PrimitiveType.DECIMAL128;
        COMPATIBILITY_MATRIX[Type.DECIMAL256.ordinal()][Type.UNKNOWN_TYPE.ordinal()] = PrimitiveType.INVALID_TYPE;

        // HLL
        COMPATIBILITY_MATRIX[Type.HLL.ordinal()][Type.TIME.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.HLL.ordinal()][Type.BITMAP.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.HLL.ordinal()][Type.PERCENTILE.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.HLL.ordinal()][Type.UNKNOWN_TYPE.ordinal()] = PrimitiveType.INVALID_TYPE;

        // BITMAP
        COMPATIBILITY_MATRIX[Type.BITMAP.ordinal()][Type.TIME.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.BITMAP.ordinal()][Type.PERCENTILE.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.BITMAP.ordinal()][Type.UNKNOWN_TYPE.ordinal()] = PrimitiveType.INVALID_TYPE;

        COMPATIBILITY_MATRIX[Type.PERCENTILE.ordinal()][Type.BITMAP.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.PERCENTILE.ordinal()][Type.UNKNOWN_TYPE.ordinal()] = PrimitiveType.INVALID_TYPE;

        // JSON
        for (PrimitiveType type : PrimitiveType.JSON_COMPATIBLE_TYPE) {
            ScalarType scalar = TypeFactory.createType(type);
            COMPATIBILITY_MATRIX[scalar.ordinal()][Type.JSON.ordinal()] = type;
        }
        for (PrimitiveType type : PrimitiveType.JSON_UNCOMPATIBLE_TYPE) {
            ScalarType scalar = TypeFactory.createType(type);
            COMPATIBILITY_MATRIX[scalar.ordinal()][Type.JSON.ordinal()] = PrimitiveType.INVALID_TYPE;
        }

        COMPATIBILITY_MATRIX[Type.JSON.ordinal()][Type.DATE.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.JSON.ordinal()][Type.DATETIME.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.JSON.ordinal()][Type.TIME.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.JSON.ordinal()][Type.DATE.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.JSON.ordinal()][Type.DATETIME.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.JSON.ordinal()][Type.DECIMAL32.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.JSON.ordinal()][Type.DECIMAL64.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.JSON.ordinal()][Type.DECIMAL128.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.JSON.ordinal()][Type.DECIMAL256.ordinal()] = PrimitiveType.INVALID_TYPE;

        // VARIANT
        for (PrimitiveType type : PrimitiveType.VARIANT_COMPATIBLE_TYPE) {
            ScalarType scalar = TypeFactory.createType(type);
            COMPATIBILITY_MATRIX[scalar.ordinal()][Type.VARIANT.ordinal()] = PrimitiveType.VARIANT;
        }
        for (PrimitiveType type : PrimitiveType.VARIANT_INCOMPATIBLE_TYPES) {
            ScalarType scalar = TypeFactory.createType(type);
            COMPATIBILITY_MATRIX[scalar.ordinal()][Type.VARIANT.ordinal()] = PrimitiveType.INVALID_TYPE;
        }
        COMPATIBILITY_MATRIX[Type.VARIANT.ordinal()][Type.DATE.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.VARIANT.ordinal()][Type.DATETIME.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.VARIANT.ordinal()][Type.TIME.ordinal()] = PrimitiveType.INVALID_TYPE;

        COMPATIBILITY_MATRIX[Type.VARIANT.ordinal()][Type.DECIMALV2.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.VARIANT.ordinal()][Type.DECIMAL32.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.VARIANT.ordinal()][Type.DECIMAL64.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.VARIANT.ordinal()][Type.DECIMAL128.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.VARIANT.ordinal()][Type.DECIMAL256.ordinal()] = PrimitiveType.INVALID_TYPE;

        COMPATIBILITY_MATRIX[Type.VARIANT.ordinal()][Type.HLL.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.VARIANT.ordinal()][Type.BITMAP.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.VARIANT.ordinal()][Type.PERCENTILE.ordinal()] = PrimitiveType.INVALID_TYPE;

        COMPATIBILITY_MATRIX[Type.VARIANT.ordinal()][Type.JSON.ordinal()] = PrimitiveType.INVALID_TYPE;

        COMPATIBILITY_MATRIX[Type.VARIANT.ordinal()][Type.VARBINARY.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[Type.VARIANT.ordinal()][Type.UNKNOWN_TYPE.ordinal()] = PrimitiveType.INVALID_TYPE;

        // binary type
        for (PrimitiveType type : PrimitiveType.BINARY_INCOMPATIBLE_TYPE_LIST) {
            ScalarType scalar = TypeFactory.createType(type);
            COMPATIBILITY_MATRIX[scalar.ordinal()][Type.VARBINARY.ordinal()] = PrimitiveType.INVALID_TYPE;
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
