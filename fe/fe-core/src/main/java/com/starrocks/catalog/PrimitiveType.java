// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/catalog/PrimitiveType.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.catalog;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Lists;
import com.starrocks.mysql.MysqlColType;
import com.starrocks.thrift.TPrimitiveType;

import java.util.ArrayList;
import java.util.List;

public enum PrimitiveType {
    INVALID_TYPE("INVALID_TYPE", -1, TPrimitiveType.INVALID_TYPE),
    // NULL_TYPE - used only in LiteralPredicate and NullLiteral to make NULLs compatible
    // with all other types.
    NULL_TYPE("NULL_TYPE", 1, TPrimitiveType.NULL_TYPE),
    BOOLEAN("BOOLEAN", 1, TPrimitiveType.BOOLEAN),
    TINYINT("TINYINT", 1, TPrimitiveType.TINYINT),
    SMALLINT("SMALLINT", 2, TPrimitiveType.SMALLINT),
    INT("INT", 4, TPrimitiveType.INT),
    BIGINT("BIGINT", 8, TPrimitiveType.BIGINT),
    LARGEINT("LARGEINT", 16, TPrimitiveType.LARGEINT),
    FLOAT("FLOAT", 4, TPrimitiveType.FLOAT),
    DOUBLE("DOUBLE", 8, TPrimitiveType.DOUBLE),
    DATE("DATE", 16, TPrimitiveType.DATE),
    DATETIME("DATETIME", 16, TPrimitiveType.DATETIME),
    // Fixed length char array.
    CHAR("CHAR", 16, TPrimitiveType.CHAR),
    // 8-byte pointer and 4-byte length indicator (12 bytes total).
    // Aligning to 8 bytes so 16 total.
    VARCHAR("VARCHAR", 16, TPrimitiveType.VARCHAR),

    DECIMALV2("DECIMALV2", 16, TPrimitiveType.DECIMALV2),

    HLL("HLL", 16, TPrimitiveType.HLL),
    TIME("TIME", 8, TPrimitiveType.TIME),
    // we use OBJECT type represent BITMAP type in Backend
    BITMAP("BITMAP", 16, TPrimitiveType.OBJECT),
    PERCENTILE("PERCENTILE", 16, TPrimitiveType.PERCENTILE),
    DECIMAL32("DECIMAL32", 4, TPrimitiveType.DECIMAL32),
    DECIMAL64("DECIMAL64", 8, TPrimitiveType.DECIMAL64),
    DECIMAL128("DECIMAL128", 16, TPrimitiveType.DECIMAL128),
    // Unsupported scalar types.
    BINARY("BINARY", -1, TPrimitiveType.BINARY);

    private static final int DATE_INDEX_LEN = 3;
    private static final int DATETIME_INDEX_LEN = 8;
    private static final int VARCHAR_INDEX_LEN = 20;
    private static final int DECIMAL_INDEX_LEN = 12;

    private static ImmutableSetMultimap<PrimitiveType, PrimitiveType> implicitCastMap;
    private static ArrayList<PrimitiveType> integerTypes;
    /**
     * Matrix that records "smallest" assignment-compatible type of two types
     * (INVALID_TYPE if no such type exists, ie, if the input types are fundamentally
     * incompatible). A value of any of the two types could be assigned to a slot
     * of the assignment-compatible type without loss of precision.
     * <p/>
     * We chose not to follow MySQL's type casting behavior as described here:
     * http://dev.mysql.com/doc/refman/5.0/en/type-conversion.html
     * for the following reasons:
     * conservative casting in arithmetic exprs: TINYINT + TINYINT -> BIGINT
     * comparison of many types as double: INT < FLOAT -> comparison as DOUBLE
     * special cases when dealing with dates and timestamps
     */
    private static PrimitiveType[][] compatibilityMatrix;

    static {
        ImmutableSetMultimap.Builder<PrimitiveType, PrimitiveType> builder = ImmutableSetMultimap.builder();
        // Nulltype
        builder.put(NULL_TYPE, BOOLEAN);
        builder.put(NULL_TYPE, TINYINT);
        builder.put(NULL_TYPE, SMALLINT);
        builder.put(NULL_TYPE, INT);
        builder.put(NULL_TYPE, BIGINT);
        builder.put(NULL_TYPE, LARGEINT);
        builder.put(NULL_TYPE, FLOAT);
        builder.put(NULL_TYPE, DOUBLE);
        builder.put(NULL_TYPE, DATE);
        builder.put(NULL_TYPE, DATETIME);
        builder.put(NULL_TYPE, DECIMALV2);
        builder.put(NULL_TYPE, CHAR);
        builder.put(NULL_TYPE, VARCHAR);
        builder.put(NULL_TYPE, BITMAP);
        builder.put(NULL_TYPE, TIME);
        builder.put(NULL_TYPE, DECIMAL32);
        builder.put(NULL_TYPE, DECIMAL64);
        builder.put(NULL_TYPE, DECIMAL128);
        // Boolean
        builder.put(BOOLEAN, BOOLEAN);
        builder.put(BOOLEAN, TINYINT);
        builder.put(BOOLEAN, SMALLINT);
        builder.put(BOOLEAN, INT);
        builder.put(BOOLEAN, BIGINT);
        builder.put(BOOLEAN, LARGEINT);
        builder.put(BOOLEAN, FLOAT);
        builder.put(BOOLEAN, DOUBLE);
        builder.put(BOOLEAN, DATE);
        builder.put(BOOLEAN, DATETIME);
        builder.put(BOOLEAN, DECIMALV2);
        builder.put(BOOLEAN, CHAR);
        builder.put(BOOLEAN, VARCHAR);
        builder.put(BOOLEAN, DECIMAL32);
        builder.put(BOOLEAN, DECIMAL64);
        builder.put(BOOLEAN, DECIMAL128);
        builder.put(BOOLEAN, TIME);
        // Tinyint
        builder.put(TINYINT, BOOLEAN);
        builder.put(TINYINT, TINYINT);
        builder.put(TINYINT, SMALLINT);
        builder.put(TINYINT, INT);
        builder.put(TINYINT, BIGINT);
        builder.put(TINYINT, LARGEINT);
        builder.put(TINYINT, FLOAT);
        builder.put(TINYINT, DOUBLE);
        builder.put(TINYINT, DATE);
        builder.put(TINYINT, DATETIME);
        builder.put(TINYINT, DECIMALV2);
        builder.put(TINYINT, CHAR);
        builder.put(TINYINT, VARCHAR);
        builder.put(TINYINT, DECIMAL32);
        builder.put(TINYINT, DECIMAL64);
        builder.put(TINYINT, DECIMAL128);
        builder.put(TINYINT, TIME);
        // Smallint
        builder.put(SMALLINT, BOOLEAN);
        builder.put(SMALLINT, TINYINT);
        builder.put(SMALLINT, SMALLINT);
        builder.put(SMALLINT, INT);
        builder.put(SMALLINT, BIGINT);
        builder.put(SMALLINT, LARGEINT);
        builder.put(SMALLINT, FLOAT);
        builder.put(SMALLINT, DOUBLE);
        builder.put(SMALLINT, DATE);
        builder.put(SMALLINT, DATETIME);
        builder.put(SMALLINT, DECIMALV2);
        builder.put(SMALLINT, CHAR);
        builder.put(SMALLINT, VARCHAR);
        builder.put(SMALLINT, DECIMAL32);
        builder.put(SMALLINT, DECIMAL64);
        builder.put(SMALLINT, DECIMAL128);
        builder.put(SMALLINT, TIME);
        // Int
        builder.put(INT, BOOLEAN);
        builder.put(INT, TINYINT);
        builder.put(INT, SMALLINT);
        builder.put(INT, INT);
        builder.put(INT, BIGINT);
        builder.put(INT, LARGEINT);
        builder.put(INT, FLOAT);
        builder.put(INT, DOUBLE);
        builder.put(INT, DATE);
        builder.put(INT, DATETIME);
        builder.put(INT, DECIMALV2);
        builder.put(INT, CHAR);
        builder.put(INT, VARCHAR);
        builder.put(INT, DECIMAL32);
        builder.put(INT, DECIMAL64);
        builder.put(INT, DECIMAL128);
        builder.put(INT, TIME);
        // Bigint
        builder.put(BIGINT, BOOLEAN);
        builder.put(BIGINT, TINYINT);
        builder.put(BIGINT, SMALLINT);
        builder.put(BIGINT, INT);
        builder.put(BIGINT, BIGINT);
        builder.put(BIGINT, LARGEINT);
        builder.put(BIGINT, FLOAT);
        builder.put(BIGINT, DOUBLE);
        builder.put(BIGINT, DATE);
        builder.put(BIGINT, DATETIME);
        builder.put(BIGINT, DECIMALV2);
        builder.put(BIGINT, CHAR);
        builder.put(BIGINT, VARCHAR);
        builder.put(BIGINT, DECIMAL32);
        builder.put(BIGINT, DECIMAL64);
        builder.put(BIGINT, DECIMAL128);
        builder.put(BIGINT, TIME);
        // Largeint
        builder.put(LARGEINT, BOOLEAN);
        builder.put(LARGEINT, TINYINT);
        builder.put(LARGEINT, SMALLINT);
        builder.put(LARGEINT, INT);
        builder.put(LARGEINT, BIGINT);
        builder.put(LARGEINT, LARGEINT);
        builder.put(LARGEINT, FLOAT);
        builder.put(LARGEINT, DOUBLE);
        builder.put(LARGEINT, DATE);
        builder.put(LARGEINT, DATETIME);
        builder.put(LARGEINT, DECIMALV2);
        builder.put(LARGEINT, CHAR);
        builder.put(LARGEINT, VARCHAR);
        builder.put(LARGEINT, DECIMAL32);
        builder.put(LARGEINT, DECIMAL64);
        builder.put(LARGEINT, DECIMAL128);
        builder.put(LARGEINT, TIME);
        // Float
        builder.put(FLOAT, BOOLEAN);
        builder.put(FLOAT, TINYINT);
        builder.put(FLOAT, SMALLINT);
        builder.put(FLOAT, INT);
        builder.put(FLOAT, BIGINT);
        builder.put(FLOAT, LARGEINT);
        builder.put(FLOAT, FLOAT);
        builder.put(FLOAT, DOUBLE);
        builder.put(FLOAT, DATE);
        builder.put(FLOAT, DATETIME);
        builder.put(FLOAT, DECIMALV2);
        builder.put(FLOAT, CHAR);
        builder.put(FLOAT, VARCHAR);
        builder.put(FLOAT, DECIMAL32);
        builder.put(FLOAT, DECIMAL64);
        builder.put(FLOAT, DECIMAL128);
        builder.put(FLOAT, TIME);
        // Double
        builder.put(DOUBLE, BOOLEAN);
        builder.put(DOUBLE, TINYINT);
        builder.put(DOUBLE, SMALLINT);
        builder.put(DOUBLE, INT);
        builder.put(DOUBLE, BIGINT);
        builder.put(DOUBLE, LARGEINT);
        builder.put(DOUBLE, FLOAT);
        builder.put(DOUBLE, DOUBLE);
        builder.put(DOUBLE, DATE);
        builder.put(DOUBLE, DATETIME);
        builder.put(DOUBLE, DECIMALV2);
        builder.put(DOUBLE, CHAR);
        builder.put(DOUBLE, VARCHAR);
        builder.put(DOUBLE, DECIMAL32);
        builder.put(DOUBLE, DECIMAL64);
        builder.put(DOUBLE, DECIMAL128);
        builder.put(DOUBLE, TIME);
        // Date
        builder.put(DATE, BOOLEAN);
        builder.put(DATE, TINYINT);
        builder.put(DATE, SMALLINT);
        builder.put(DATE, INT);
        builder.put(DATE, BIGINT);
        builder.put(DATE, LARGEINT);
        builder.put(DATE, FLOAT);
        builder.put(DATE, DOUBLE);
        builder.put(DATE, DATE);
        builder.put(DATE, DATETIME);
        builder.put(DATE, DECIMALV2);
        builder.put(DATE, CHAR);
        builder.put(DATE, VARCHAR);
        builder.put(DATE, DECIMAL32);
        builder.put(DATE, DECIMAL64);
        builder.put(DATE, DECIMAL128);
        builder.put(DATE, TIME);
        // Datetime
        builder.put(DATETIME, BOOLEAN);
        builder.put(DATETIME, TINYINT);
        builder.put(DATETIME, SMALLINT);
        builder.put(DATETIME, INT);
        builder.put(DATETIME, BIGINT);
        builder.put(DATETIME, LARGEINT);
        builder.put(DATETIME, FLOAT);
        builder.put(DATETIME, DOUBLE);
        builder.put(DATETIME, DATE);
        builder.put(DATETIME, DATETIME);
        builder.put(DATETIME, TIME);
        builder.put(DATETIME, DECIMALV2);
        builder.put(DATETIME, CHAR);
        builder.put(DATETIME, VARCHAR);
        builder.put(DATETIME, DECIMAL32);
        builder.put(DATETIME, DECIMAL64);
        builder.put(DATETIME, DECIMAL128);
        builder.put(DATETIME, TIME);
        // Char
        builder.put(CHAR, CHAR);
        builder.put(CHAR, VARCHAR);
        // Varchar
        builder.put(VARCHAR, BOOLEAN);
        builder.put(VARCHAR, TINYINT);
        builder.put(VARCHAR, SMALLINT);
        builder.put(VARCHAR, INT);
        builder.put(VARCHAR, BIGINT);
        builder.put(VARCHAR, LARGEINT);
        builder.put(VARCHAR, FLOAT);
        builder.put(VARCHAR, DOUBLE);
        builder.put(VARCHAR, DATE);
        builder.put(VARCHAR, DATETIME);
        builder.put(VARCHAR, DECIMALV2);
        builder.put(VARCHAR, CHAR);
        builder.put(VARCHAR, VARCHAR);
        builder.put(VARCHAR, HLL);
        builder.put(VARCHAR, BITMAP);
        builder.put(VARCHAR, DECIMAL32);
        builder.put(VARCHAR, DECIMAL64);
        builder.put(VARCHAR, DECIMAL128);
        builder.put(VARCHAR, TIME);
        // DecimalV2
        builder.put(DECIMALV2, BOOLEAN);
        builder.put(DECIMALV2, TINYINT);
        builder.put(DECIMALV2, SMALLINT);
        builder.put(DECIMALV2, INT);
        builder.put(DECIMALV2, BIGINT);
        builder.put(DECIMALV2, LARGEINT);
        builder.put(DECIMALV2, FLOAT);
        builder.put(DECIMALV2, DOUBLE);
        builder.put(DECIMALV2, DECIMALV2);
        builder.put(DECIMALV2, CHAR);
        builder.put(DECIMALV2, VARCHAR);
        builder.put(DECIMALV2, DECIMAL32);
        builder.put(DECIMALV2, DECIMAL64);
        builder.put(DECIMALV2, DECIMAL128);
        builder.put(DECIMALV2, TIME);

        // Decimal32
        builder.put(DECIMAL32, BOOLEAN);
        builder.put(DECIMAL32, TINYINT);
        builder.put(DECIMAL32, SMALLINT);
        builder.put(DECIMAL32, INT);
        builder.put(DECIMAL32, BIGINT);
        builder.put(DECIMAL32, LARGEINT);
        builder.put(DECIMAL32, FLOAT);
        builder.put(DECIMAL32, DOUBLE);
        builder.put(DECIMAL32, DECIMALV2);
        builder.put(DECIMAL32, CHAR);
        builder.put(DECIMAL32, VARCHAR);
        builder.put(DECIMAL32, DECIMAL32);
        builder.put(DECIMAL32, DECIMAL64);
        builder.put(DECIMAL32, DECIMAL128);
        builder.put(DECIMAL32, TIME);

        // Decimal64
        builder.put(DECIMAL64, BOOLEAN);
        builder.put(DECIMAL64, TINYINT);
        builder.put(DECIMAL64, SMALLINT);
        builder.put(DECIMAL64, INT);
        builder.put(DECIMAL64, BIGINT);
        builder.put(DECIMAL64, LARGEINT);
        builder.put(DECIMAL64, FLOAT);
        builder.put(DECIMAL64, DOUBLE);
        builder.put(DECIMAL64, DECIMALV2);
        builder.put(DECIMAL64, CHAR);
        builder.put(DECIMAL64, VARCHAR);
        builder.put(DECIMAL64, DECIMAL32);
        builder.put(DECIMAL64, DECIMAL64);
        builder.put(DECIMAL64, DECIMAL128);
        builder.put(DECIMAL64, TIME);

        // Decimal128
        builder.put(DECIMAL128, BOOLEAN);
        builder.put(DECIMAL128, TINYINT);
        builder.put(DECIMAL128, SMALLINT);
        builder.put(DECIMAL128, INT);
        builder.put(DECIMAL128, BIGINT);
        builder.put(DECIMAL128, LARGEINT);
        builder.put(DECIMAL128, FLOAT);
        builder.put(DECIMAL128, DOUBLE);
        builder.put(DECIMAL128, DECIMALV2);
        builder.put(DECIMAL128, CHAR);
        builder.put(DECIMAL128, VARCHAR);
        builder.put(DECIMAL128, DECIMAL32);
        builder.put(DECIMAL128, DECIMAL64);
        builder.put(DECIMAL128, DECIMAL128);
        builder.put(DECIMAL128, TIME);

        // HLL
        builder.put(HLL, HLL);

        // BITMAP
        builder.put(BITMAP, BITMAP);

        //TIME
        builder.put(TIME, BOOLEAN);
        builder.put(TIME, TINYINT);
        builder.put(TIME, SMALLINT);
        builder.put(TIME, INT);
        builder.put(TIME, BIGINT);
        builder.put(TIME, LARGEINT);
        builder.put(TIME, FLOAT);
        builder.put(TIME, DOUBLE);
        builder.put(TIME, DECIMALV2);
        builder.put(TIME, CHAR);
        builder.put(TIME, VARCHAR);
        builder.put(TIME, DECIMAL32);
        builder.put(TIME, DECIMAL64);
        builder.put(TIME, DECIMAL128);
        builder.put(TIME, TIME);
        builder.put(TIME, DATE);
        builder.put(TIME, DATETIME);

        //PERCENTILE
        builder.put(PERCENTILE, PERCENTILE);

        implicitCastMap = builder.build();
    }

    static {
        integerTypes = Lists.newArrayList();
        integerTypes.add(TINYINT);
        integerTypes.add(SMALLINT);
        integerTypes.add(INT);
        integerTypes.add(BIGINT);
        integerTypes.add(LARGEINT);
    }

    static {
        compatibilityMatrix = new PrimitiveType[BINARY.ordinal() + 1][BINARY.ordinal() + 1];

        // NULL_TYPE is compatible with any type and results in the non-null type.
        compatibilityMatrix[NULL_TYPE.ordinal()][NULL_TYPE.ordinal()] = NULL_TYPE;
        compatibilityMatrix[NULL_TYPE.ordinal()][BOOLEAN.ordinal()] = BOOLEAN;
        compatibilityMatrix[NULL_TYPE.ordinal()][TINYINT.ordinal()] = TINYINT;
        compatibilityMatrix[NULL_TYPE.ordinal()][SMALLINT.ordinal()] = SMALLINT;
        compatibilityMatrix[NULL_TYPE.ordinal()][INT.ordinal()] = INT;
        compatibilityMatrix[NULL_TYPE.ordinal()][BIGINT.ordinal()] = BIGINT;
        compatibilityMatrix[NULL_TYPE.ordinal()][LARGEINT.ordinal()] = LARGEINT;
        compatibilityMatrix[NULL_TYPE.ordinal()][FLOAT.ordinal()] = FLOAT;
        compatibilityMatrix[NULL_TYPE.ordinal()][DOUBLE.ordinal()] = DOUBLE;
        compatibilityMatrix[NULL_TYPE.ordinal()][DATE.ordinal()] = DATE;
        compatibilityMatrix[NULL_TYPE.ordinal()][DATETIME.ordinal()] = DATETIME;
        compatibilityMatrix[NULL_TYPE.ordinal()][CHAR.ordinal()] = CHAR;
        compatibilityMatrix[NULL_TYPE.ordinal()][VARCHAR.ordinal()] = VARCHAR;
        compatibilityMatrix[NULL_TYPE.ordinal()][DECIMALV2.ordinal()] = DECIMALV2;
        compatibilityMatrix[NULL_TYPE.ordinal()][TIME.ordinal()] = TIME;
        compatibilityMatrix[NULL_TYPE.ordinal()][BITMAP.ordinal()] = BITMAP;
        compatibilityMatrix[NULL_TYPE.ordinal()][PERCENTILE.ordinal()] = PERCENTILE;
        compatibilityMatrix[NULL_TYPE.ordinal()][DECIMAL32.ordinal()] = DECIMAL32;
        compatibilityMatrix[NULL_TYPE.ordinal()][DECIMAL64.ordinal()] = DECIMAL64;
        compatibilityMatrix[NULL_TYPE.ordinal()][DECIMAL128.ordinal()] = DECIMAL128;

        compatibilityMatrix[BOOLEAN.ordinal()][BOOLEAN.ordinal()] = BOOLEAN;
        compatibilityMatrix[BOOLEAN.ordinal()][TINYINT.ordinal()] = TINYINT;
        compatibilityMatrix[BOOLEAN.ordinal()][SMALLINT.ordinal()] = SMALLINT;
        compatibilityMatrix[BOOLEAN.ordinal()][INT.ordinal()] = INT;
        compatibilityMatrix[BOOLEAN.ordinal()][BIGINT.ordinal()] = BIGINT;
        compatibilityMatrix[BOOLEAN.ordinal()][LARGEINT.ordinal()] = LARGEINT;
        compatibilityMatrix[BOOLEAN.ordinal()][FLOAT.ordinal()] = FLOAT;
        compatibilityMatrix[BOOLEAN.ordinal()][DOUBLE.ordinal()] = DOUBLE;
        compatibilityMatrix[BOOLEAN.ordinal()][DATE.ordinal()] = INVALID_TYPE;
        compatibilityMatrix[BOOLEAN.ordinal()][DATETIME.ordinal()] = INVALID_TYPE;
        compatibilityMatrix[BOOLEAN.ordinal()][CHAR.ordinal()] = INVALID_TYPE;
        compatibilityMatrix[BOOLEAN.ordinal()][VARCHAR.ordinal()] = INVALID_TYPE;
        compatibilityMatrix[BOOLEAN.ordinal()][DECIMALV2.ordinal()] = DECIMALV2;
        compatibilityMatrix[BOOLEAN.ordinal()][TIME.ordinal()] = TIME;
        compatibilityMatrix[BOOLEAN.ordinal()][DECIMAL32.ordinal()] = DECIMAL32;
        compatibilityMatrix[BOOLEAN.ordinal()][DECIMAL64.ordinal()] = DECIMAL64;
        compatibilityMatrix[BOOLEAN.ordinal()][DECIMAL128.ordinal()] = DECIMAL128;

        compatibilityMatrix[TINYINT.ordinal()][TINYINT.ordinal()] = TINYINT;
        compatibilityMatrix[TINYINT.ordinal()][SMALLINT.ordinal()] = SMALLINT;
        compatibilityMatrix[TINYINT.ordinal()][INT.ordinal()] = INT;
        compatibilityMatrix[TINYINT.ordinal()][BIGINT.ordinal()] = BIGINT;
        compatibilityMatrix[TINYINT.ordinal()][LARGEINT.ordinal()] = LARGEINT;
        compatibilityMatrix[TINYINT.ordinal()][FLOAT.ordinal()] = FLOAT;
        compatibilityMatrix[TINYINT.ordinal()][DOUBLE.ordinal()] = DOUBLE;
        compatibilityMatrix[TINYINT.ordinal()][DATE.ordinal()] = INVALID_TYPE;
        compatibilityMatrix[TINYINT.ordinal()][DATETIME.ordinal()] = INVALID_TYPE;
        compatibilityMatrix[TINYINT.ordinal()][CHAR.ordinal()] = INVALID_TYPE;
        compatibilityMatrix[TINYINT.ordinal()][VARCHAR.ordinal()] = INVALID_TYPE;
        compatibilityMatrix[TINYINT.ordinal()][DECIMALV2.ordinal()] = DECIMALV2;
        compatibilityMatrix[TINYINT.ordinal()][TIME.ordinal()] = TIME;
        compatibilityMatrix[TINYINT.ordinal()][DECIMAL32.ordinal()] = DECIMAL32;
        compatibilityMatrix[TINYINT.ordinal()][DECIMAL64.ordinal()] = DECIMAL64;
        compatibilityMatrix[TINYINT.ordinal()][DECIMAL128.ordinal()] = DECIMAL128;

        compatibilityMatrix[SMALLINT.ordinal()][SMALLINT.ordinal()] = SMALLINT;
        compatibilityMatrix[SMALLINT.ordinal()][INT.ordinal()] = INT;
        compatibilityMatrix[SMALLINT.ordinal()][BIGINT.ordinal()] = BIGINT;
        compatibilityMatrix[SMALLINT.ordinal()][LARGEINT.ordinal()] = LARGEINT;
        compatibilityMatrix[SMALLINT.ordinal()][FLOAT.ordinal()] = FLOAT;
        compatibilityMatrix[SMALLINT.ordinal()][DOUBLE.ordinal()] = DOUBLE;
        compatibilityMatrix[SMALLINT.ordinal()][DATE.ordinal()] = INVALID_TYPE;
        compatibilityMatrix[SMALLINT.ordinal()][DATETIME.ordinal()] = INVALID_TYPE;
        compatibilityMatrix[SMALLINT.ordinal()][CHAR.ordinal()] = INVALID_TYPE;
        compatibilityMatrix[SMALLINT.ordinal()][VARCHAR.ordinal()] = INVALID_TYPE;
        compatibilityMatrix[SMALLINT.ordinal()][DECIMALV2.ordinal()] = DECIMALV2;
        compatibilityMatrix[SMALLINT.ordinal()][TIME.ordinal()] = TIME;
        compatibilityMatrix[SMALLINT.ordinal()][DECIMAL32.ordinal()] = DECIMAL32;
        compatibilityMatrix[SMALLINT.ordinal()][DECIMAL64.ordinal()] = DECIMAL64;
        compatibilityMatrix[SMALLINT.ordinal()][DECIMAL128.ordinal()] = DECIMAL128;

        compatibilityMatrix[INT.ordinal()][INT.ordinal()] = INT;
        compatibilityMatrix[INT.ordinal()][BIGINT.ordinal()] = BIGINT;
        compatibilityMatrix[INT.ordinal()][LARGEINT.ordinal()] = LARGEINT;
        compatibilityMatrix[INT.ordinal()][FLOAT.ordinal()] = FLOAT;
        compatibilityMatrix[INT.ordinal()][DOUBLE.ordinal()] = DOUBLE;
        compatibilityMatrix[INT.ordinal()][DATE.ordinal()] = INVALID_TYPE;
        compatibilityMatrix[INT.ordinal()][DATETIME.ordinal()] = INVALID_TYPE;
        compatibilityMatrix[INT.ordinal()][CHAR.ordinal()] = INVALID_TYPE;
        compatibilityMatrix[INT.ordinal()][VARCHAR.ordinal()] = INVALID_TYPE;
        compatibilityMatrix[INT.ordinal()][DECIMALV2.ordinal()] = DECIMALV2;
        compatibilityMatrix[INT.ordinal()][TIME.ordinal()] = TIME;
        compatibilityMatrix[INT.ordinal()][DECIMAL32.ordinal()] = DECIMAL32;
        compatibilityMatrix[INT.ordinal()][DECIMAL64.ordinal()] = DECIMAL64;
        compatibilityMatrix[INT.ordinal()][DECIMAL128.ordinal()] = DECIMAL128;

        compatibilityMatrix[BIGINT.ordinal()][BIGINT.ordinal()] = BIGINT;
        compatibilityMatrix[BIGINT.ordinal()][LARGEINT.ordinal()] = LARGEINT;
        compatibilityMatrix[BIGINT.ordinal()][FLOAT.ordinal()] = DOUBLE;
        compatibilityMatrix[BIGINT.ordinal()][DOUBLE.ordinal()] = DOUBLE;
        compatibilityMatrix[BIGINT.ordinal()][DATE.ordinal()] = INVALID_TYPE;
        compatibilityMatrix[BIGINT.ordinal()][DATETIME.ordinal()] = INVALID_TYPE;
        compatibilityMatrix[BIGINT.ordinal()][CHAR.ordinal()] = INVALID_TYPE;
        compatibilityMatrix[BIGINT.ordinal()][VARCHAR.ordinal()] = INVALID_TYPE;
        compatibilityMatrix[BIGINT.ordinal()][DECIMALV2.ordinal()] = DECIMALV2;
        compatibilityMatrix[BIGINT.ordinal()][TIME.ordinal()] = TIME;
        compatibilityMatrix[BIGINT.ordinal()][DECIMAL32.ordinal()] = DECIMAL32;
        compatibilityMatrix[BIGINT.ordinal()][DECIMAL64.ordinal()] = DECIMAL64;
        compatibilityMatrix[BIGINT.ordinal()][DECIMAL128.ordinal()] = DECIMAL128;

        compatibilityMatrix[LARGEINT.ordinal()][LARGEINT.ordinal()] = LARGEINT;
        compatibilityMatrix[LARGEINT.ordinal()][FLOAT.ordinal()] = DOUBLE;
        compatibilityMatrix[LARGEINT.ordinal()][DOUBLE.ordinal()] = DOUBLE;
        compatibilityMatrix[LARGEINT.ordinal()][DATE.ordinal()] = INVALID_TYPE;
        compatibilityMatrix[LARGEINT.ordinal()][DATETIME.ordinal()] = INVALID_TYPE;
        compatibilityMatrix[LARGEINT.ordinal()][CHAR.ordinal()] = INVALID_TYPE;
        compatibilityMatrix[LARGEINT.ordinal()][VARCHAR.ordinal()] = INVALID_TYPE;
        compatibilityMatrix[LARGEINT.ordinal()][DECIMALV2.ordinal()] = DECIMALV2;
        compatibilityMatrix[LARGEINT.ordinal()][TIME.ordinal()] = TIME;
        compatibilityMatrix[LARGEINT.ordinal()][DECIMAL32.ordinal()] = DECIMAL32;
        compatibilityMatrix[LARGEINT.ordinal()][DECIMAL64.ordinal()] = DECIMAL64;
        compatibilityMatrix[LARGEINT.ordinal()][DECIMAL128.ordinal()] = DECIMAL128;

        compatibilityMatrix[FLOAT.ordinal()][FLOAT.ordinal()] = FLOAT;
        compatibilityMatrix[FLOAT.ordinal()][DOUBLE.ordinal()] = DOUBLE;
        compatibilityMatrix[FLOAT.ordinal()][DATE.ordinal()] = INVALID_TYPE;
        compatibilityMatrix[FLOAT.ordinal()][DATETIME.ordinal()] = INVALID_TYPE;
        compatibilityMatrix[FLOAT.ordinal()][CHAR.ordinal()] = INVALID_TYPE;
        compatibilityMatrix[FLOAT.ordinal()][VARCHAR.ordinal()] = INVALID_TYPE;
        compatibilityMatrix[FLOAT.ordinal()][DECIMALV2.ordinal()] = DECIMALV2;
        compatibilityMatrix[FLOAT.ordinal()][TIME.ordinal()] = TIME;
        compatibilityMatrix[FLOAT.ordinal()][DECIMAL32.ordinal()] = DECIMAL32;
        compatibilityMatrix[FLOAT.ordinal()][DECIMAL64.ordinal()] = DECIMAL64;
        compatibilityMatrix[FLOAT.ordinal()][DECIMAL128.ordinal()] = DECIMAL128;

        compatibilityMatrix[DOUBLE.ordinal()][DOUBLE.ordinal()] = DOUBLE;
        compatibilityMatrix[DOUBLE.ordinal()][DATE.ordinal()] = INVALID_TYPE;
        compatibilityMatrix[DOUBLE.ordinal()][DATETIME.ordinal()] = INVALID_TYPE;
        compatibilityMatrix[DOUBLE.ordinal()][CHAR.ordinal()] = INVALID_TYPE;
        compatibilityMatrix[DOUBLE.ordinal()][VARCHAR.ordinal()] = INVALID_TYPE;
        compatibilityMatrix[DOUBLE.ordinal()][DECIMALV2.ordinal()] = DECIMALV2;
        compatibilityMatrix[DOUBLE.ordinal()][TIME.ordinal()] = TIME;
        compatibilityMatrix[DOUBLE.ordinal()][DECIMAL32.ordinal()] = DECIMAL32;
        compatibilityMatrix[DOUBLE.ordinal()][DECIMAL64.ordinal()] = DECIMAL64;
        compatibilityMatrix[DOUBLE.ordinal()][DECIMAL128.ordinal()] = DECIMAL128;

        compatibilityMatrix[DATE.ordinal()][DATE.ordinal()] = DATE;
        compatibilityMatrix[DATE.ordinal()][DATETIME.ordinal()] = DATETIME;
        compatibilityMatrix[DATE.ordinal()][CHAR.ordinal()] = INVALID_TYPE;
        compatibilityMatrix[DATE.ordinal()][VARCHAR.ordinal()] = INVALID_TYPE;
        compatibilityMatrix[DATE.ordinal()][DECIMALV2.ordinal()] = INVALID_TYPE;
        compatibilityMatrix[DATE.ordinal()][TIME.ordinal()] = INVALID_TYPE;
        compatibilityMatrix[DATE.ordinal()][DECIMAL32.ordinal()] = INVALID_TYPE;
        compatibilityMatrix[DATE.ordinal()][DECIMAL64.ordinal()] = INVALID_TYPE;
        compatibilityMatrix[DATE.ordinal()][DECIMAL128.ordinal()] = INVALID_TYPE;

        compatibilityMatrix[DATETIME.ordinal()][DATETIME.ordinal()] = DATETIME;
        compatibilityMatrix[DATETIME.ordinal()][CHAR.ordinal()] = INVALID_TYPE;
        compatibilityMatrix[DATETIME.ordinal()][VARCHAR.ordinal()] = INVALID_TYPE;
        compatibilityMatrix[DATETIME.ordinal()][DECIMALV2.ordinal()] = INVALID_TYPE;
        compatibilityMatrix[DATETIME.ordinal()][TIME.ordinal()] = INVALID_TYPE;
        compatibilityMatrix[DATETIME.ordinal()][DECIMAL32.ordinal()] = INVALID_TYPE;
        compatibilityMatrix[DATETIME.ordinal()][DECIMAL64.ordinal()] = INVALID_TYPE;
        compatibilityMatrix[DATETIME.ordinal()][DECIMAL128.ordinal()] = INVALID_TYPE;

        compatibilityMatrix[CHAR.ordinal()][CHAR.ordinal()] = CHAR;
        compatibilityMatrix[CHAR.ordinal()][VARCHAR.ordinal()] = VARCHAR;
        compatibilityMatrix[CHAR.ordinal()][DECIMALV2.ordinal()] = INVALID_TYPE;
        compatibilityMatrix[CHAR.ordinal()][TIME.ordinal()] = INVALID_TYPE;
        compatibilityMatrix[CHAR.ordinal()][DECIMAL32.ordinal()] = INVALID_TYPE;
        compatibilityMatrix[CHAR.ordinal()][DECIMAL64.ordinal()] = INVALID_TYPE;
        compatibilityMatrix[CHAR.ordinal()][DECIMAL128.ordinal()] = INVALID_TYPE;

        compatibilityMatrix[VARCHAR.ordinal()][VARCHAR.ordinal()] = VARCHAR;
        compatibilityMatrix[VARCHAR.ordinal()][DECIMALV2.ordinal()] = INVALID_TYPE;
        compatibilityMatrix[VARCHAR.ordinal()][TIME.ordinal()] = INVALID_TYPE;
        compatibilityMatrix[VARCHAR.ordinal()][DECIMAL32.ordinal()] = INVALID_TYPE;
        compatibilityMatrix[VARCHAR.ordinal()][DECIMAL64.ordinal()] = INVALID_TYPE;
        compatibilityMatrix[VARCHAR.ordinal()][DECIMAL128.ordinal()] = INVALID_TYPE;

        compatibilityMatrix[DECIMALV2.ordinal()][DECIMALV2.ordinal()] = DECIMALV2;
        compatibilityMatrix[DECIMALV2.ordinal()][TIME.ordinal()] = INVALID_TYPE;
        compatibilityMatrix[DECIMALV2.ordinal()][DECIMAL32.ordinal()] = DECIMAL32;
        compatibilityMatrix[DECIMALV2.ordinal()][DECIMAL64.ordinal()] = DECIMAL64;
        compatibilityMatrix[DECIMALV2.ordinal()][DECIMAL128.ordinal()] = DECIMAL128;

        compatibilityMatrix[DECIMAL32.ordinal()][DECIMALV2.ordinal()] = DECIMALV2;
        compatibilityMatrix[DECIMAL32.ordinal()][TIME.ordinal()] = INVALID_TYPE;
        compatibilityMatrix[DECIMAL32.ordinal()][DECIMAL32.ordinal()] = DECIMAL32;
        compatibilityMatrix[DECIMAL32.ordinal()][DECIMAL64.ordinal()] = DECIMAL64;
        compatibilityMatrix[DECIMAL32.ordinal()][DECIMAL128.ordinal()] = DECIMAL128;

        compatibilityMatrix[DECIMAL64.ordinal()][DECIMALV2.ordinal()] = DECIMALV2;
        compatibilityMatrix[DECIMAL64.ordinal()][TIME.ordinal()] = INVALID_TYPE;
        compatibilityMatrix[DECIMAL64.ordinal()][DECIMAL32.ordinal()] = DECIMAL32;
        compatibilityMatrix[DECIMAL64.ordinal()][DECIMAL64.ordinal()] = DECIMAL64;
        compatibilityMatrix[DECIMAL64.ordinal()][DECIMAL128.ordinal()] = DECIMAL128;

        compatibilityMatrix[DECIMAL128.ordinal()][DECIMALV2.ordinal()] = DECIMALV2;
        compatibilityMatrix[DECIMAL128.ordinal()][TIME.ordinal()] = INVALID_TYPE;
        compatibilityMatrix[DECIMAL128.ordinal()][DECIMAL32.ordinal()] = DECIMAL32;
        compatibilityMatrix[DECIMAL128.ordinal()][DECIMAL64.ordinal()] = DECIMAL64;
        compatibilityMatrix[DECIMAL128.ordinal()][DECIMAL128.ordinal()] = DECIMAL128;

        compatibilityMatrix[HLL.ordinal()][HLL.ordinal()] = HLL;
        compatibilityMatrix[HLL.ordinal()][TIME.ordinal()] = INVALID_TYPE;

        compatibilityMatrix[BITMAP.ordinal()][BITMAP.ordinal()] = BITMAP;

        compatibilityMatrix[TIME.ordinal()][TIME.ordinal()] = TIME;

        compatibilityMatrix[PERCENTILE.ordinal()][PERCENTILE.ordinal()] = PERCENTILE;
    }

    static {
        // NULL_TYPE is compatible with any type and results in the non-null type.
        compatibilityMatrix[NULL_TYPE.ordinal()][NULL_TYPE.ordinal()] = NULL_TYPE;
        compatibilityMatrix[NULL_TYPE.ordinal()][BOOLEAN.ordinal()] = BOOLEAN;
        compatibilityMatrix[NULL_TYPE.ordinal()][TINYINT.ordinal()] = TINYINT;
        compatibilityMatrix[NULL_TYPE.ordinal()][SMALLINT.ordinal()] = SMALLINT;
        compatibilityMatrix[NULL_TYPE.ordinal()][INT.ordinal()] = INT;
    }

    private final String description;
    private final int slotSize;  // size of tuple slot for this type
    private final TPrimitiveType thriftType;
    private boolean isTimeType = false;

    private PrimitiveType(String description, int slotSize, TPrimitiveType thriftType) {
        this.description = description;
        this.slotSize = slotSize;
        this.thriftType = thriftType;
    }

    public static ArrayList<PrimitiveType> getIntegerTypes() {
        return integerTypes;
    }

    // Check whether 'type' can cast to 'target'
    public static boolean isImplicitCast(PrimitiveType type, PrimitiveType target) {
        return implicitCastMap.get(type).contains(target);
    }

    public static PrimitiveType fromThrift(TPrimitiveType tPrimitiveType) {
        switch (tPrimitiveType) {
            case INVALID_TYPE:
                return INVALID_TYPE;
            case NULL_TYPE:
                return NULL_TYPE;
            case BOOLEAN:
                return BOOLEAN;
            case TINYINT:
                return TINYINT;
            case SMALLINT:
                return SMALLINT;
            case INT:
                return INT;
            case BIGINT:
                return BIGINT;
            case LARGEINT:
                return LARGEINT;
            case FLOAT:
                return FLOAT;
            case DOUBLE:
                return DOUBLE;
            case VARCHAR:
                return VARCHAR;
            case CHAR:
                return CHAR;
            case HLL:
                return HLL;
            case OBJECT:
                return BITMAP;
            case PERCENTILE:
                return PERCENTILE;
            case DECIMAL32:
                return DECIMALV2;
            case DECIMAL64:
                return DECIMAL64;
            case DECIMAL128:
                return DECIMAL128;
            case DATE:
                return DATE;
            case DATETIME:
                return DATETIME;
            case TIME:
                return TIME;
            case BINARY:
                return BINARY;
            default:
                return INVALID_TYPE;
        }
    }

    public static List<TPrimitiveType> toThrift(PrimitiveType[] types) {
        List<TPrimitiveType> result = Lists.newArrayList();
        for (PrimitiveType t : types) {
            result.add(t.toThrift());
        }
        return result;
    }

    public static int getMaxSlotSize() {
        return DECIMALV2.slotSize;
    }

    /**
     * Return type t such that values from both t1 and t2 can be assigned to t
     * without loss of precision. Returns INVALID_TYPE if there is no such type
     * or if any of t1 and t2 is INVALID_TYPE.
     */
    public static PrimitiveType getAssignmentCompatibleType(PrimitiveType t1, PrimitiveType t2) {
        if (!t1.isValid() || !t2.isValid()) {
            return INVALID_TYPE;
        }

        PrimitiveType smallerType = (t1.ordinal() < t2.ordinal() ? t1 : t2);
        PrimitiveType largerType = (t1.ordinal() > t2.ordinal() ? t1 : t2);
        PrimitiveType result = compatibilityMatrix[smallerType.ordinal()][largerType.ordinal()];
        Preconditions.checkNotNull(result);
        return result;
    }

    /**
     * compute the wider DecimalV3 type between t1 and t2, the wide order is DECIMAL32 < DECIMAL64 < DECIMAL128
     *
     * @param t1
     * @param t2
     * @return wider type
     */
    public static PrimitiveType getWiderDecimalV3Type(PrimitiveType t1, PrimitiveType t2) {
        Preconditions.checkState(t1.isDecimalV3Type() && t2.isDecimalV3Type());
        if (t1.equals(DECIMAL32)) {
            return t2;
        } else if (t2.equals(DECIMAL32)) {
            return t1;
        } else if (t1.equals(DECIMAL64)) {
            return t2;
        } else if (t2.equals(DECIMAL64)) {
            return t1;
        } else {
            return DECIMAL128;
        }
    }

    public static int getMaxPrecisionOfDecimal(PrimitiveType t) {
        switch (t) {
            case DECIMALV2:
                return 27;
            case DECIMAL32:
                return 9;
            case DECIMAL64:
                return 18;
            case DECIMAL128:
                return 38;
            default:
                Preconditions.checkState(t.isDecimalOfAnyVersion());
                return -1;
        }
    }

    public static int getDefaultScaleOfDecimal(PrimitiveType t) {
        switch (t) {
            case DECIMALV2:
                return 27;
            case DECIMAL32:
                return 9;
            case DECIMAL64:
                return 18;
            case DECIMAL128:
                return 38;
            default:
                Preconditions.checkState(t.isDecimalOfAnyVersion());
                return -1;
        }
    }

    public void setTimeType() {
        isTimeType = true;
    }

    @Override
    public String toString() {
        return description;
    }

    public TPrimitiveType toThrift() {
        return thriftType;
    }

    public int getSlotSize() {
        return slotSize;
    }

    public int getTypeSize() {
        int typeSize = 0;
        switch (this) {
            case NULL_TYPE:
            case BOOLEAN:
            case TINYINT:
                typeSize = 1;
                break;
            case SMALLINT:
                typeSize = 2;
                break;
            case INT:
            case DECIMAL32:
            case DATE:
                typeSize = 4;
                break;
            case BIGINT:
            case DECIMAL64:
            case DOUBLE:
            case FLOAT:
            case TIME:
            case DATETIME:
                typeSize = 8;
                break;
            case LARGEINT:
            case DECIMALV2:
            case DECIMAL128:
                typeSize = 16;
                break;
            case CHAR:
            case VARCHAR:
                // use 16 as char type estimate size
                typeSize = 16;
                break;
            case HLL:
                // 16KB
                typeSize = 16 * 1024;
                break;
            case BITMAP:
            case PERCENTILE:
                // 1MB
                typeSize = 1024 * 1024;
                break;
        }
        return typeSize;
    }

    public boolean isFixedPointType() {
        return this == TINYINT
                || this == SMALLINT
                || this == INT
                || this == BIGINT
                || this == LARGEINT;
    }

    public boolean isFloatingPointType() {
        return this == FLOAT || this == DOUBLE;
    }

    public boolean isDecimalV2Type() {
        return this == DECIMALV2;
    }

    public boolean isDecimalOfAnyVersion() {
        return isDecimalV2Type() || isDecimalV3Type();
    }

    public boolean isDecimalV3Type() {
        return this == DECIMAL32 || this == DECIMAL64 || this == DECIMAL128;
    }

    public boolean isNumericType() {
        return isFixedPointType() || isFloatingPointType() || isDecimalV2Type() || isDecimalV3Type();
    }

    public boolean isValid() {
        return this != INVALID_TYPE;
    }

    public boolean isNull() {
        return this == NULL_TYPE;
    }

    public boolean isDateType() {
        return (this == DATE || this == DATETIME);
    }

    public boolean isStringType() {
        return (this == VARCHAR || this == CHAR || this == HLL);
    }

    public boolean isCharFamily() {
        return (this == VARCHAR || this == CHAR);
    }

    public boolean isIntegerType() {
        return (this == TINYINT || this == SMALLINT
                || this == INT || this == BIGINT);
    }

    // TODO(zhaochun): Add Mysql Type to it's private field
    public MysqlColType toMysqlType() {
        switch (this) {
            // MySQL use Tinyint(1) to represent boolean
            case BOOLEAN:
            case TINYINT:
                return MysqlColType.MYSQL_TYPE_TINY;
            case SMALLINT:
                return MysqlColType.MYSQL_TYPE_SHORT;
            case INT:
                return MysqlColType.MYSQL_TYPE_LONG;
            case BIGINT:
                return MysqlColType.MYSQL_TYPE_LONGLONG;
            case FLOAT:
                return MysqlColType.MYSQL_TYPE_FLOAT;
            case DOUBLE:
                return MysqlColType.MYSQL_TYPE_DOUBLE;
            case TIME:
                return MysqlColType.MYSQL_TYPE_TIME;
            case DATE:
                return MysqlColType.MYSQL_TYPE_DATE;
            case DATETIME: {
                if (isTimeType) {
                    return MysqlColType.MYSQL_TYPE_TIME;
                } else {
                    return MysqlColType.MYSQL_TYPE_DATETIME;
                }
            }
            case DECIMALV2:
            case DECIMAL32:
            case DECIMAL64:
            case DECIMAL128:
                return MysqlColType.MYSQL_TYPE_NEWDECIMAL;
            case VARCHAR:
                return MysqlColType.MYSQL_TYPE_VAR_STRING;
            default:
                return MysqlColType.MYSQL_TYPE_STRING;
        }
    }

    public int getOlapColumnIndexSize() {
        switch (this) {
            case DATE:
                return DATE_INDEX_LEN;
            case DATETIME:
                return DATETIME_INDEX_LEN;
            case VARCHAR:
                return VARCHAR_INDEX_LEN;
            case CHAR:
                // char index size is length
                return -1;
            case DECIMALV2:
                return DECIMAL_INDEX_LEN;
            default:
                return this.getSlotSize();
        }
    }
}
