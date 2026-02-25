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
        COMPATIBILITY_MATRIX[BooleanType.BOOLEAN.ordinal()][IntegerType.TINYINT.ordinal()] = PrimitiveType.TINYINT;
        COMPATIBILITY_MATRIX[BooleanType.BOOLEAN.ordinal()][IntegerType.SMALLINT.ordinal()] = PrimitiveType.SMALLINT;
        COMPATIBILITY_MATRIX[BooleanType.BOOLEAN.ordinal()][IntegerType.INT.ordinal()] = PrimitiveType.INT;
        COMPATIBILITY_MATRIX[BooleanType.BOOLEAN.ordinal()][IntegerType.BIGINT.ordinal()] = PrimitiveType.BIGINT;
        COMPATIBILITY_MATRIX[BooleanType.BOOLEAN.ordinal()][IntegerType.LARGEINT.ordinal()] = PrimitiveType.LARGEINT;
        COMPATIBILITY_MATRIX[BooleanType.BOOLEAN.ordinal()][FloatType.FLOAT.ordinal()] = PrimitiveType.FLOAT;
        COMPATIBILITY_MATRIX[BooleanType.BOOLEAN.ordinal()][FloatType.DOUBLE.ordinal()] = PrimitiveType.DOUBLE;
        COMPATIBILITY_MATRIX[BooleanType.BOOLEAN.ordinal()][DateType.DATE.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[BooleanType.BOOLEAN.ordinal()][DateType.DATETIME.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[BooleanType.BOOLEAN.ordinal()][CharType.CHAR.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[BooleanType.BOOLEAN.ordinal()][VarcharType.VARCHAR.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[BooleanType.BOOLEAN.ordinal()][HLLType.HLL.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[BooleanType.BOOLEAN.ordinal()][DateType.TIME.ordinal()] = PrimitiveType.DOUBLE;
        COMPATIBILITY_MATRIX[BooleanType.BOOLEAN.ordinal()][BitmapType.BITMAP.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[BooleanType.BOOLEAN.ordinal()][PercentileType.PERCENTILE.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[BooleanType.BOOLEAN.ordinal()][JsonType.JSON.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[BooleanType.BOOLEAN.ordinal()][UnknownType.UNKNOWN_TYPE.ordinal()] = PrimitiveType.INVALID_TYPE;

        // TINYINT
        COMPATIBILITY_MATRIX[IntegerType.TINYINT.ordinal()][IntegerType.SMALLINT.ordinal()] = PrimitiveType.SMALLINT;
        COMPATIBILITY_MATRIX[IntegerType.TINYINT.ordinal()][IntegerType.INT.ordinal()] = PrimitiveType.INT;
        COMPATIBILITY_MATRIX[IntegerType.TINYINT.ordinal()][IntegerType.BIGINT.ordinal()] = PrimitiveType.BIGINT;
        COMPATIBILITY_MATRIX[IntegerType.TINYINT.ordinal()][IntegerType.LARGEINT.ordinal()] = PrimitiveType.LARGEINT;
        COMPATIBILITY_MATRIX[IntegerType.TINYINT.ordinal()][FloatType.FLOAT.ordinal()] = PrimitiveType.FLOAT;
        COMPATIBILITY_MATRIX[IntegerType.TINYINT.ordinal()][FloatType.DOUBLE.ordinal()] = PrimitiveType.DOUBLE;
        COMPATIBILITY_MATRIX[IntegerType.TINYINT.ordinal()][DateType.DATE.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[IntegerType.TINYINT.ordinal()][DateType.DATETIME.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[IntegerType.TINYINT.ordinal()][CharType.CHAR.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[IntegerType.TINYINT.ordinal()][VarcharType.VARCHAR.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[IntegerType.TINYINT.ordinal()][DecimalType.DECIMALV2.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[IntegerType.TINYINT.ordinal()][HLLType.HLL.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[IntegerType.TINYINT.ordinal()][DateType.TIME.ordinal()] = PrimitiveType.DOUBLE;
        COMPATIBILITY_MATRIX[IntegerType.TINYINT.ordinal()][BitmapType.BITMAP.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[IntegerType.TINYINT.ordinal()][PercentileType.PERCENTILE.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[IntegerType.TINYINT.ordinal()][DecimalType.DECIMAL32.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[IntegerType.TINYINT.ordinal()][DecimalType.DECIMAL64.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[IntegerType.TINYINT.ordinal()][DecimalType.DECIMAL128.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[IntegerType.TINYINT.ordinal()][DecimalType.DECIMAL256.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[IntegerType.TINYINT.ordinal()][JsonType.JSON.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[IntegerType.TINYINT.ordinal()][UnknownType.UNKNOWN_TYPE.ordinal()] = PrimitiveType.INVALID_TYPE;

        // SMALLINT
        COMPATIBILITY_MATRIX[IntegerType.SMALLINT.ordinal()][IntegerType.INT.ordinal()] = PrimitiveType.INT;
        COMPATIBILITY_MATRIX[IntegerType.SMALLINT.ordinal()][IntegerType.BIGINT.ordinal()] = PrimitiveType.BIGINT;
        COMPATIBILITY_MATRIX[IntegerType.SMALLINT.ordinal()][IntegerType.LARGEINT.ordinal()] = PrimitiveType.LARGEINT;
        COMPATIBILITY_MATRIX[IntegerType.SMALLINT.ordinal()][FloatType.FLOAT.ordinal()] = PrimitiveType.FLOAT;
        COMPATIBILITY_MATRIX[IntegerType.SMALLINT.ordinal()][FloatType.DOUBLE.ordinal()] = PrimitiveType.DOUBLE;
        COMPATIBILITY_MATRIX[IntegerType.SMALLINT.ordinal()][DateType.DATE.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[IntegerType.SMALLINT.ordinal()][DateType.DATETIME.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[IntegerType.SMALLINT.ordinal()][CharType.CHAR.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[IntegerType.SMALLINT.ordinal()][VarcharType.VARCHAR.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[IntegerType.SMALLINT.ordinal()][DecimalType.DECIMALV2.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[IntegerType.SMALLINT.ordinal()][HLLType.HLL.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[IntegerType.SMALLINT.ordinal()][DateType.TIME.ordinal()] = PrimitiveType.DOUBLE;
        COMPATIBILITY_MATRIX[IntegerType.SMALLINT.ordinal()][BitmapType.BITMAP.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[IntegerType.SMALLINT.ordinal()][PercentileType.PERCENTILE.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[IntegerType.SMALLINT.ordinal()][DecimalType.DECIMAL32.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[IntegerType.SMALLINT.ordinal()][DecimalType.DECIMAL64.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[IntegerType.SMALLINT.ordinal()][DecimalType.DECIMAL128.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[IntegerType.SMALLINT.ordinal()][DecimalType.DECIMAL256.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[IntegerType.SMALLINT.ordinal()][JsonType.JSON.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[IntegerType.SMALLINT.ordinal()][UnknownType.UNKNOWN_TYPE.ordinal()] = PrimitiveType.INVALID_TYPE;

        // INT
        COMPATIBILITY_MATRIX[IntegerType.INT.ordinal()][IntegerType.BIGINT.ordinal()] = PrimitiveType.BIGINT;
        COMPATIBILITY_MATRIX[IntegerType.INT.ordinal()][IntegerType.LARGEINT.ordinal()] = PrimitiveType.LARGEINT;
        COMPATIBILITY_MATRIX[IntegerType.INT.ordinal()][FloatType.FLOAT.ordinal()] = PrimitiveType.DOUBLE;
        COMPATIBILITY_MATRIX[IntegerType.INT.ordinal()][FloatType.DOUBLE.ordinal()] = PrimitiveType.DOUBLE;
        COMPATIBILITY_MATRIX[IntegerType.INT.ordinal()][DateType.DATE.ordinal()] = PrimitiveType.INT;
        COMPATIBILITY_MATRIX[IntegerType.INT.ordinal()][DateType.DATETIME.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[IntegerType.INT.ordinal()][CharType.CHAR.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[IntegerType.INT.ordinal()][VarcharType.VARCHAR.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[IntegerType.INT.ordinal()][DecimalType.DECIMALV2.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[IntegerType.INT.ordinal()][HLLType.HLL.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[IntegerType.INT.ordinal()][DateType.TIME.ordinal()] = PrimitiveType.DOUBLE;
        COMPATIBILITY_MATRIX[IntegerType.INT.ordinal()][BitmapType.BITMAP.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[IntegerType.INT.ordinal()][PercentileType.PERCENTILE.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[IntegerType.INT.ordinal()][DecimalType.DECIMAL32.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[IntegerType.INT.ordinal()][DecimalType.DECIMAL64.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[IntegerType.INT.ordinal()][DecimalType.DECIMAL128.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[IntegerType.INT.ordinal()][DecimalType.DECIMAL256.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[IntegerType.INT.ordinal()][JsonType.JSON.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[IntegerType.INT.ordinal()][UnknownType.UNKNOWN_TYPE.ordinal()] = PrimitiveType.INVALID_TYPE;

        // BIGINT
        COMPATIBILITY_MATRIX[IntegerType.BIGINT.ordinal()][IntegerType.LARGEINT.ordinal()] = PrimitiveType.LARGEINT;
        COMPATIBILITY_MATRIX[IntegerType.BIGINT.ordinal()][FloatType.FLOAT.ordinal()] = PrimitiveType.DOUBLE;
        COMPATIBILITY_MATRIX[IntegerType.BIGINT.ordinal()][FloatType.DOUBLE.ordinal()] = PrimitiveType.DOUBLE;
        COMPATIBILITY_MATRIX[IntegerType.BIGINT.ordinal()][DateType.DATE.ordinal()] = PrimitiveType.BIGINT;
        COMPATIBILITY_MATRIX[IntegerType.BIGINT.ordinal()][DateType.DATETIME.ordinal()] = PrimitiveType.BIGINT;
        COMPATIBILITY_MATRIX[IntegerType.BIGINT.ordinal()][CharType.CHAR.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[IntegerType.BIGINT.ordinal()][VarcharType.VARCHAR.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[IntegerType.BIGINT.ordinal()][DecimalType.DECIMALV2.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[IntegerType.BIGINT.ordinal()][HLLType.HLL.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[IntegerType.BIGINT.ordinal()][DateType.TIME.ordinal()] = PrimitiveType.DOUBLE;
        COMPATIBILITY_MATRIX[IntegerType.BIGINT.ordinal()][BitmapType.BITMAP.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[IntegerType.BIGINT.ordinal()][PercentileType.PERCENTILE.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[IntegerType.BIGINT.ordinal()][DecimalType.DECIMAL32.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[IntegerType.BIGINT.ordinal()][DecimalType.DECIMAL64.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[IntegerType.BIGINT.ordinal()][DecimalType.DECIMAL128.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[IntegerType.BIGINT.ordinal()][DecimalType.DECIMAL256.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[IntegerType.BIGINT.ordinal()][JsonType.JSON.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[IntegerType.BIGINT.ordinal()][UnknownType.UNKNOWN_TYPE.ordinal()] = PrimitiveType.INVALID_TYPE;

        // LARGEINT
        COMPATIBILITY_MATRIX[IntegerType.LARGEINT.ordinal()][FloatType.FLOAT.ordinal()] = PrimitiveType.DOUBLE;
        COMPATIBILITY_MATRIX[IntegerType.LARGEINT.ordinal()][FloatType.DOUBLE.ordinal()] = PrimitiveType.DOUBLE;
        COMPATIBILITY_MATRIX[IntegerType.LARGEINT.ordinal()][DateType.DATE.ordinal()] = PrimitiveType.LARGEINT;
        COMPATIBILITY_MATRIX[IntegerType.LARGEINT.ordinal()][DateType.DATETIME.ordinal()] = PrimitiveType.LARGEINT;
        COMPATIBILITY_MATRIX[IntegerType.LARGEINT.ordinal()][CharType.CHAR.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[IntegerType.LARGEINT.ordinal()][VarcharType.VARCHAR.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[IntegerType.LARGEINT.ordinal()][DecimalType.DECIMALV2.ordinal()] = PrimitiveType.DECIMALV2;
        COMPATIBILITY_MATRIX[IntegerType.LARGEINT.ordinal()][HLLType.HLL.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[IntegerType.LARGEINT.ordinal()][DateType.TIME.ordinal()] = PrimitiveType.DOUBLE;
        COMPATIBILITY_MATRIX[IntegerType.LARGEINT.ordinal()][BitmapType.BITMAP.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[IntegerType.LARGEINT.ordinal()][PercentileType.PERCENTILE.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[IntegerType.LARGEINT.ordinal()][DecimalType.DECIMAL32.ordinal()] = PrimitiveType.DECIMAL32;
        COMPATIBILITY_MATRIX[IntegerType.LARGEINT.ordinal()][DecimalType.DECIMAL64.ordinal()] = PrimitiveType.DECIMAL64;
        COMPATIBILITY_MATRIX[IntegerType.LARGEINT.ordinal()][DecimalType.DECIMAL128.ordinal()] = PrimitiveType.DECIMAL128;
        COMPATIBILITY_MATRIX[IntegerType.LARGEINT.ordinal()][DecimalType.DECIMAL256.ordinal()] = PrimitiveType.DECIMAL256;
        COMPATIBILITY_MATRIX[IntegerType.LARGEINT.ordinal()][JsonType.JSON.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[IntegerType.LARGEINT.ordinal()][UnknownType.UNKNOWN_TYPE.ordinal()] = PrimitiveType.INVALID_TYPE;

        // FLOAT
        COMPATIBILITY_MATRIX[FloatType.FLOAT.ordinal()][FloatType.DOUBLE.ordinal()] = PrimitiveType.DOUBLE;
        COMPATIBILITY_MATRIX[FloatType.FLOAT.ordinal()][DateType.DATE.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[FloatType.FLOAT.ordinal()][DateType.DATETIME.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[FloatType.FLOAT.ordinal()][CharType.CHAR.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[FloatType.FLOAT.ordinal()][VarcharType.VARCHAR.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[FloatType.FLOAT.ordinal()][DecimalType.DECIMALV2.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[FloatType.FLOAT.ordinal()][HLLType.HLL.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[FloatType.FLOAT.ordinal()][DateType.TIME.ordinal()] = PrimitiveType.DOUBLE;
        COMPATIBILITY_MATRIX[FloatType.FLOAT.ordinal()][BitmapType.BITMAP.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[FloatType.FLOAT.ordinal()][PercentileType.PERCENTILE.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[FloatType.FLOAT.ordinal()][DecimalType.DECIMAL32.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[FloatType.FLOAT.ordinal()][DecimalType.DECIMAL64.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[FloatType.FLOAT.ordinal()][DecimalType.DECIMAL128.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[FloatType.FLOAT.ordinal()][DecimalType.DECIMAL256.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[FloatType.FLOAT.ordinal()][JsonType.JSON.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[FloatType.FLOAT.ordinal()][UnknownType.UNKNOWN_TYPE.ordinal()] = PrimitiveType.INVALID_TYPE;

        // DOUBLE
        COMPATIBILITY_MATRIX[FloatType.DOUBLE.ordinal()][DateType.DATE.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[FloatType.DOUBLE.ordinal()][DateType.DATETIME.ordinal()] = PrimitiveType.DOUBLE;
        COMPATIBILITY_MATRIX[FloatType.DOUBLE.ordinal()][CharType.CHAR.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[FloatType.DOUBLE.ordinal()][VarcharType.VARCHAR.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[FloatType.DOUBLE.ordinal()][DecimalType.DECIMALV2.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[FloatType.DOUBLE.ordinal()][HLLType.HLL.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[FloatType.DOUBLE.ordinal()][DateType.TIME.ordinal()] = PrimitiveType.DOUBLE;
        COMPATIBILITY_MATRIX[FloatType.DOUBLE.ordinal()][BitmapType.BITMAP.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[FloatType.DOUBLE.ordinal()][PercentileType.PERCENTILE.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[FloatType.DOUBLE.ordinal()][DecimalType.DECIMAL32.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[FloatType.DOUBLE.ordinal()][DecimalType.DECIMAL64.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[FloatType.DOUBLE.ordinal()][DecimalType.DECIMAL128.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[FloatType.DOUBLE.ordinal()][DecimalType.DECIMAL256.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[FloatType.DOUBLE.ordinal()][UnknownType.UNKNOWN_TYPE.ordinal()] = PrimitiveType.INVALID_TYPE;

        // DATE
        COMPATIBILITY_MATRIX[DateType.DATE.ordinal()][DateType.DATETIME.ordinal()] = PrimitiveType.DATETIME;
        COMPATIBILITY_MATRIX[DateType.DATE.ordinal()][CharType.CHAR.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[DateType.DATE.ordinal()][VarcharType.VARCHAR.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[DateType.DATE.ordinal()][DecimalType.DECIMALV2.ordinal()] = PrimitiveType.DECIMALV2;
        COMPATIBILITY_MATRIX[DateType.DATE.ordinal()][HLLType.HLL.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[DateType.DATE.ordinal()][DateType.TIME.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[DateType.DATE.ordinal()][BitmapType.BITMAP.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[DateType.DATE.ordinal()][PercentileType.PERCENTILE.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[DateType.DATE.ordinal()][DecimalType.DECIMAL32.ordinal()] = PrimitiveType.DECIMAL32;
        COMPATIBILITY_MATRIX[DateType.DATE.ordinal()][DecimalType.DECIMAL64.ordinal()] = PrimitiveType.DECIMAL64;
        COMPATIBILITY_MATRIX[DateType.DATE.ordinal()][DecimalType.DECIMAL128.ordinal()] = PrimitiveType.DECIMAL128;
        COMPATIBILITY_MATRIX[DateType.DATE.ordinal()][UnknownType.UNKNOWN_TYPE.ordinal()] = PrimitiveType.INVALID_TYPE;

        // DATETIME
        COMPATIBILITY_MATRIX[DateType.DATETIME.ordinal()][CharType.CHAR.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[DateType.DATETIME.ordinal()][VarcharType.VARCHAR.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[DateType.DATETIME.ordinal()][DecimalType.DECIMALV2.ordinal()] = PrimitiveType.DECIMALV2;
        COMPATIBILITY_MATRIX[DateType.DATETIME.ordinal()][HLLType.HLL.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[DateType.DATETIME.ordinal()][DateType.TIME.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[DateType.DATETIME.ordinal()][BitmapType.BITMAP.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[DateType.DATETIME.ordinal()][PercentileType.PERCENTILE.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[DateType.DATETIME.ordinal()][DecimalType.DECIMAL32.ordinal()] = PrimitiveType.DECIMAL32;
        COMPATIBILITY_MATRIX[DateType.DATETIME.ordinal()][DecimalType.DECIMAL64.ordinal()] = PrimitiveType.DECIMAL64;
        COMPATIBILITY_MATRIX[DateType.DATETIME.ordinal()][DecimalType.DECIMAL128.ordinal()] = PrimitiveType.DECIMAL128;
        COMPATIBILITY_MATRIX[DateType.DATETIME.ordinal()][UnknownType.UNKNOWN_TYPE.ordinal()] = PrimitiveType.INVALID_TYPE;

        // CHAR
        COMPATIBILITY_MATRIX[CharType.CHAR.ordinal()][VarcharType.VARCHAR.ordinal()] = PrimitiveType.VARCHAR;
        COMPATIBILITY_MATRIX[CharType.CHAR.ordinal()][DecimalType.DECIMALV2.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[CharType.CHAR.ordinal()][HLLType.HLL.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[CharType.CHAR.ordinal()][DateType.TIME.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[CharType.CHAR.ordinal()][BitmapType.BITMAP.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[CharType.CHAR.ordinal()][PercentileType.PERCENTILE.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[CharType.CHAR.ordinal()][DecimalType.DECIMAL32.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[CharType.CHAR.ordinal()][DecimalType.DECIMAL64.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[CharType.CHAR.ordinal()][DecimalType.DECIMAL128.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[CharType.CHAR.ordinal()][DecimalType.DECIMAL256.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[CharType.CHAR.ordinal()][UnknownType.UNKNOWN_TYPE.ordinal()] = PrimitiveType.INVALID_TYPE;

        // VARCHAR
        COMPATIBILITY_MATRIX[VarcharType.VARCHAR.ordinal()][DecimalType.DECIMALV2.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[VarcharType.VARCHAR.ordinal()][HLLType.HLL.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[VarcharType.VARCHAR.ordinal()][DateType.TIME.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[VarcharType.VARCHAR.ordinal()][BitmapType.BITMAP.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[VarcharType.VARCHAR.ordinal()][PercentileType.PERCENTILE.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[VarcharType.VARCHAR.ordinal()][DecimalType.DECIMAL32.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[VarcharType.VARCHAR.ordinal()][DecimalType.DECIMAL64.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[VarcharType.VARCHAR.ordinal()][DecimalType.DECIMAL128.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[VarcharType.VARCHAR.ordinal()][DecimalType.DECIMAL256.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[VarcharType.VARCHAR.ordinal()][UnknownType.UNKNOWN_TYPE.ordinal()] = PrimitiveType.INVALID_TYPE;

        // DECIMALV2
        COMPATIBILITY_MATRIX[DecimalType.DECIMALV2.ordinal()][HLLType.HLL.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[DecimalType.DECIMALV2.ordinal()][DateType.TIME.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[DecimalType.DECIMALV2.ordinal()][BitmapType.BITMAP.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[DecimalType.DECIMALV2.ordinal()][PercentileType.PERCENTILE.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[DecimalType.DECIMALV2.ordinal()][DecimalType.DECIMAL32.ordinal()] = PrimitiveType.DECIMAL32;
        COMPATIBILITY_MATRIX[DecimalType.DECIMALV2.ordinal()][DecimalType.DECIMAL64.ordinal()] = PrimitiveType.DECIMAL64;
        COMPATIBILITY_MATRIX[DecimalType.DECIMALV2.ordinal()][DecimalType.DECIMAL128.ordinal()] = PrimitiveType.DECIMAL128;
        COMPATIBILITY_MATRIX[DecimalType.DECIMALV2.ordinal()][UnknownType.UNKNOWN_TYPE.ordinal()] =
                PrimitiveType.INVALID_TYPE;

        // DECIMAL32
        COMPATIBILITY_MATRIX[DecimalType.DECIMAL32.ordinal()][HLLType.HLL.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[DecimalType.DECIMAL32.ordinal()][DateType.TIME.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[DecimalType.DECIMAL32.ordinal()][BitmapType.BITMAP.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[DecimalType.DECIMAL32.ordinal()][PercentileType.PERCENTILE.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[DecimalType.DECIMAL32.ordinal()][DecimalType.DECIMAL32.ordinal()] = PrimitiveType.DECIMAL32;
        COMPATIBILITY_MATRIX[DecimalType.DECIMAL32.ordinal()][DecimalType.DECIMAL64.ordinal()] = PrimitiveType.DECIMAL64;
        COMPATIBILITY_MATRIX[DecimalType.DECIMAL32.ordinal()][DecimalType.DECIMAL128.ordinal()] = PrimitiveType.DECIMAL128;
        COMPATIBILITY_MATRIX[DecimalType.DECIMAL32.ordinal()][DecimalType.DECIMAL256.ordinal()] =
                PrimitiveType.DECIMAL256;
        COMPATIBILITY_MATRIX[DecimalType.DECIMAL32.ordinal()][UnknownType.UNKNOWN_TYPE.ordinal()] =
                PrimitiveType.INVALID_TYPE;

        // DECIMAL64
        COMPATIBILITY_MATRIX[DecimalType.DECIMAL64.ordinal()][HLLType.HLL.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[DecimalType.DECIMAL64.ordinal()][DateType.TIME.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[DecimalType.DECIMAL64.ordinal()][BitmapType.BITMAP.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[DecimalType.DECIMAL64.ordinal()][PercentileType.PERCENTILE.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[DecimalType.DECIMAL64.ordinal()][DecimalType.DECIMAL32.ordinal()] = PrimitiveType.DECIMAL32;
        COMPATIBILITY_MATRIX[DecimalType.DECIMAL64.ordinal()][DecimalType.DECIMAL64.ordinal()] = PrimitiveType.DECIMAL64;
        COMPATIBILITY_MATRIX[DecimalType.DECIMAL64.ordinal()][DecimalType.DECIMAL128.ordinal()] =
                PrimitiveType.DECIMAL128;
        COMPATIBILITY_MATRIX[DecimalType.DECIMAL64.ordinal()][DecimalType.DECIMAL256.ordinal()] =
                PrimitiveType.DECIMAL256;
        COMPATIBILITY_MATRIX[DecimalType.DECIMAL64.ordinal()][UnknownType.UNKNOWN_TYPE.ordinal()] =
                PrimitiveType.INVALID_TYPE;

        // DECIMAL128
        COMPATIBILITY_MATRIX[DecimalType.DECIMAL128.ordinal()][HLLType.HLL.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[DecimalType.DECIMAL128.ordinal()][DateType.TIME.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[DecimalType.DECIMAL128.ordinal()][BitmapType.BITMAP.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[DecimalType.DECIMAL128.ordinal()][PercentileType.PERCENTILE.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[DecimalType.DECIMAL128.ordinal()][DecimalType.DECIMAL32.ordinal()] = PrimitiveType.DECIMAL32;
        COMPATIBILITY_MATRIX[DecimalType.DECIMAL128.ordinal()][DecimalType.DECIMAL64.ordinal()] = PrimitiveType.DECIMAL64;
        COMPATIBILITY_MATRIX[DecimalType.DECIMAL128.ordinal()][DecimalType.DECIMAL128.ordinal()] = PrimitiveType.DECIMAL128;
        COMPATIBILITY_MATRIX[DecimalType.DECIMAL128.ordinal()][UnknownType.UNKNOWN_TYPE.ordinal()] =
                PrimitiveType.INVALID_TYPE;

        // DECIMAL256
        COMPATIBILITY_MATRIX[DecimalType.DECIMAL256.ordinal()][HLLType.HLL.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[DecimalType.DECIMAL256.ordinal()][DateType.TIME.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[DecimalType.DECIMAL256.ordinal()][BitmapType.BITMAP.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[DecimalType.DECIMAL256.ordinal()][PercentileType.PERCENTILE.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[DecimalType.DECIMAL256.ordinal()][DecimalType.DECIMAL32.ordinal()] = PrimitiveType.DECIMAL32;
        COMPATIBILITY_MATRIX[DecimalType.DECIMAL256.ordinal()][DecimalType.DECIMAL64.ordinal()] = PrimitiveType.DECIMAL64;
        COMPATIBILITY_MATRIX[DecimalType.DECIMAL256.ordinal()][DecimalType.DECIMAL128.ordinal()] = PrimitiveType.DECIMAL128;
        COMPATIBILITY_MATRIX[DecimalType.DECIMAL256.ordinal()][UnknownType.UNKNOWN_TYPE.ordinal()] =
                PrimitiveType.INVALID_TYPE;

        // HLL
        COMPATIBILITY_MATRIX[HLLType.HLL.ordinal()][DateType.TIME.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[HLLType.HLL.ordinal()][BitmapType.BITMAP.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[HLLType.HLL.ordinal()][PercentileType.PERCENTILE.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[HLLType.HLL.ordinal()][UnknownType.UNKNOWN_TYPE.ordinal()] = PrimitiveType.INVALID_TYPE;

        // BITMAP
        COMPATIBILITY_MATRIX[BitmapType.BITMAP.ordinal()][DateType.TIME.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[BitmapType.BITMAP.ordinal()][PercentileType.PERCENTILE.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[BitmapType.BITMAP.ordinal()][UnknownType.UNKNOWN_TYPE.ordinal()] = PrimitiveType.INVALID_TYPE;

        COMPATIBILITY_MATRIX[PercentileType.PERCENTILE.ordinal()][BitmapType.BITMAP.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[PercentileType.PERCENTILE.ordinal()][UnknownType.UNKNOWN_TYPE.ordinal()] =
                PrimitiveType.INVALID_TYPE;

        // JSON
        for (PrimitiveType type : PrimitiveType.JSON_COMPATIBLE_TYPE) {
            ScalarType scalar = TypeFactory.createType(type);
            COMPATIBILITY_MATRIX[scalar.ordinal()][JsonType.JSON.ordinal()] = type;
        }
        for (PrimitiveType type : PrimitiveType.JSON_UNCOMPATIBLE_TYPE) {
            ScalarType scalar = TypeFactory.createType(type);
            COMPATIBILITY_MATRIX[scalar.ordinal()][JsonType.JSON.ordinal()] = PrimitiveType.INVALID_TYPE;
        }

        COMPATIBILITY_MATRIX[JsonType.JSON.ordinal()][DateType.DATE.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[JsonType.JSON.ordinal()][DateType.DATETIME.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[JsonType.JSON.ordinal()][DateType.TIME.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[JsonType.JSON.ordinal()][DateType.DATE.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[JsonType.JSON.ordinal()][DateType.DATETIME.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[JsonType.JSON.ordinal()][DecimalType.DECIMAL32.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[JsonType.JSON.ordinal()][DecimalType.DECIMAL64.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[JsonType.JSON.ordinal()][DecimalType.DECIMAL128.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[JsonType.JSON.ordinal()][DecimalType.DECIMAL256.ordinal()] = PrimitiveType.INVALID_TYPE;

        // VARIANT
        for (PrimitiveType type : PrimitiveType.VARIANT_COMPATIBLE_TYPE) {
            ScalarType scalar = TypeFactory.createType(type);
            COMPATIBILITY_MATRIX[scalar.ordinal()][VariantType.VARIANT.ordinal()] = PrimitiveType.VARIANT;
        }
        for (PrimitiveType type : PrimitiveType.VARIANT_INCOMPATIBLE_TYPES) {
            ScalarType scalar = TypeFactory.createType(type);
            COMPATIBILITY_MATRIX[scalar.ordinal()][VariantType.VARIANT.ordinal()] = PrimitiveType.INVALID_TYPE;
        }
        COMPATIBILITY_MATRIX[VariantType.VARIANT.ordinal()][DateType.DATE.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[VariantType.VARIANT.ordinal()][DateType.DATETIME.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[VariantType.VARIANT.ordinal()][DateType.TIME.ordinal()] = PrimitiveType.INVALID_TYPE;

        COMPATIBILITY_MATRIX[VariantType.VARIANT.ordinal()][DecimalType.DECIMALV2.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[VariantType.VARIANT.ordinal()][DecimalType.DECIMAL32.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[VariantType.VARIANT.ordinal()][DecimalType.DECIMAL64.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[VariantType.VARIANT.ordinal()][DecimalType.DECIMAL128.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[VariantType.VARIANT.ordinal()][DecimalType.DECIMAL256.ordinal()] = PrimitiveType.INVALID_TYPE;

        COMPATIBILITY_MATRIX[VariantType.VARIANT.ordinal()][HLLType.HLL.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[VariantType.VARIANT.ordinal()][BitmapType.BITMAP.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[VariantType.VARIANT.ordinal()][PercentileType.PERCENTILE.ordinal()] = PrimitiveType.INVALID_TYPE;

        COMPATIBILITY_MATRIX[VariantType.VARIANT.ordinal()][JsonType.JSON.ordinal()] = PrimitiveType.INVALID_TYPE;

        COMPATIBILITY_MATRIX[VariantType.VARIANT.ordinal()][VarbinaryType.VARBINARY.ordinal()] = PrimitiveType.INVALID_TYPE;
        COMPATIBILITY_MATRIX[VariantType.VARIANT.ordinal()][UnknownType.UNKNOWN_TYPE.ordinal()] = PrimitiveType.INVALID_TYPE;

        // binary type
        for (PrimitiveType type : PrimitiveType.BINARY_INCOMPATIBLE_TYPE_LIST) {
            ScalarType scalar = TypeFactory.createType(type);
            COMPATIBILITY_MATRIX[scalar.ordinal()][VarbinaryType.VARBINARY.ordinal()] = PrimitiveType.INVALID_TYPE;
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
