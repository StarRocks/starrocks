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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/catalog/Type.java

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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Lists;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Pair;
import com.starrocks.mysql.MysqlColType;
import com.starrocks.thrift.TColumnType;
import com.starrocks.thrift.TPrimitiveType;
import com.starrocks.thrift.TScalarType;
import com.starrocks.thrift.TStructField;
import com.starrocks.thrift.TTypeDesc;
import com.starrocks.thrift.TTypeNode;
import com.starrocks.thrift.TTypeNodeType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Abstract class describing an Impala data type (scalar/complex type).
 * Mostly contains static type instances and helper methods for convenience, as well
 * as abstract methods that subclasses must implement.
 */
public abstract class Type implements Cloneable {
    // used for nested type such as map and struct
    protected Boolean[] selectedFields;

    public static final int CHARSET_BINARY = 63;
    public static final int CHARSET_UTF8 = 33;

    // Maximum nesting depth of a type. This limit may be changed after running more
    // performance tests.
    public static int MAX_NESTING_DEPTH = 15;

    // DECIMAL, NULL, and INVALID_TYPE  are handled separately.
    private static final List<PrimitiveType> SKIP_COMPARE_TYPES = Arrays.asList(
            PrimitiveType.INVALID_TYPE, PrimitiveType.NULL_TYPE, PrimitiveType.DECIMALV2,
            PrimitiveType.DECIMAL32, PrimitiveType.DECIMAL64, PrimitiveType.DECIMAL128,
            PrimitiveType.TIME, PrimitiveType.JSON, PrimitiveType.FUNCTION,
            PrimitiveType.BINARY, PrimitiveType.VARBINARY);

    // Static constant types for scalar types that don't require additional information.
    public static final ScalarType INVALID = new ScalarType(PrimitiveType.INVALID_TYPE);
    public static final ScalarType NULL = new ScalarType(PrimitiveType.NULL_TYPE);
    public static final ScalarType BOOLEAN = new ScalarType(PrimitiveType.BOOLEAN);
    public static final ScalarType TINYINT = new ScalarType(PrimitiveType.TINYINT);
    public static final ScalarType SMALLINT = new ScalarType(PrimitiveType.SMALLINT);
    public static final ScalarType INT = new ScalarType(PrimitiveType.INT);
    public static final ScalarType BIGINT = new ScalarType(PrimitiveType.BIGINT);
    public static final ScalarType LARGEINT = new ScalarType(PrimitiveType.LARGEINT);
    public static final ScalarType FLOAT = new ScalarType(PrimitiveType.FLOAT);
    public static final ScalarType DOUBLE = new ScalarType(PrimitiveType.DOUBLE);
    public static final ScalarType DATE = new ScalarType(PrimitiveType.DATE);
    public static final ScalarType DATETIME = new ScalarType(PrimitiveType.DATETIME);
    public static final ScalarType TIME = new ScalarType(PrimitiveType.TIME);
    public static final ScalarType DEFAULT_DECIMALV2 = ScalarType.createDecimalV2Type(ScalarType.DEFAULT_PRECISION,
            ScalarType.DEFAULT_SCALE);
    public static final ScalarType DEFAULT_DECIMAL32 =
            ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL32, 9, 3);
    public static final ScalarType DEFAULT_DECIMAL64 =
            ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL64, 18, 6);
    public static final ScalarType DEFAULT_DECIMAL128 =
            ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL128, 38, 9);
    public static final ScalarType DECIMALV2 = DEFAULT_DECIMALV2;

    public static final ScalarType DECIMAL32 = ScalarType.createWildcardDecimalV3Type(PrimitiveType.DECIMAL32);
    public static final ScalarType DECIMAL64 = ScalarType.createWildcardDecimalV3Type(PrimitiveType.DECIMAL64);
    public static final ScalarType DECIMAL128 = ScalarType.createWildcardDecimalV3Type(PrimitiveType.DECIMAL128);

    // DECIMAL64_INT and DECIMAL128_INT for integer casting to decimal
    public static final ScalarType DECIMAL_ZERO =
            ScalarType.createDecimalV3TypeForZero(0);
    public static final ScalarType DECIMAL32_INT =
            ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL64, 9, 0);
    public static final ScalarType DECIMAL64_INT =
            ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL64, 18, 0);
    public static final ScalarType DECIMAL128_INT =
            ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL128, 38, 0);

    public static final ScalarType VARCHAR = ScalarType.createVarcharType(-1);
    public static final ScalarType STRING = ScalarType.createVarcharType(ScalarType.MAX_VARCHAR_LENGTH);
    public static final ScalarType DEFAULT_STRING = ScalarType.createDefaultString();
    public static final ScalarType HLL = ScalarType.createHllType();
    public static final ScalarType CHAR = ScalarType.createCharType(-1);
    public static final ScalarType BITMAP = new ScalarType(PrimitiveType.BITMAP);
    public static final ScalarType PERCENTILE = new ScalarType(PrimitiveType.PERCENTILE);
    public static final ScalarType JSON = new ScalarType(PrimitiveType.JSON);
    public static final ScalarType UNKNOWN_TYPE = ScalarType.createUnknownType();
    public static final ScalarType FUNCTION = new ScalarType(PrimitiveType.FUNCTION);
    public static final ScalarType VARBINARY = new ScalarType(PrimitiveType.VARBINARY);

    public static final PseudoType ANY_ELEMENT = PseudoType.ANY_ELEMENT;
    public static final PseudoType ANY_ARRAY = PseudoType.ANY_ARRAY;
    public static final PseudoType ANY_MAP = PseudoType.ANY_MAP;
    public static final PseudoType ANY_STRUCT = PseudoType.ANY_STRUCT;

    public static final Type ARRAY_NULL = new ArrayType(Type.NULL);
    public static final Type ARRAY_BOOLEAN = new ArrayType(Type.BOOLEAN);
    public static final Type ARRAY_TINYINT = new ArrayType(Type.TINYINT);
    public static final Type ARRAY_SMALLINT = new ArrayType(Type.SMALLINT);
    public static final Type ARRAY_INT = new ArrayType(Type.INT);
    public static final Type ARRAY_BIGINT = new ArrayType(Type.BIGINT);
    public static final Type ARRAY_LARGEINT = new ArrayType(Type.LARGEINT);
    public static final Type ARRAY_FLOAT = new ArrayType(Type.FLOAT);
    public static final Type ARRAY_DOUBLE = new ArrayType(Type.DOUBLE);
    public static final Type ARRAY_DECIMALV2 = new ArrayType(Type.DECIMALV2);
    public static final Type ARRAY_DATE = new ArrayType(Type.DATE);
    public static final Type ARRAY_DATETIME = new ArrayType(Type.DATETIME);
    public static final Type ARRAY_VARCHAR = new ArrayType(Type.VARCHAR);
    public static final Type ARRAY_JSON = new ArrayType(Type.JSON);
    public static final Type ARRAY_DECIMAL32 = new ArrayType(Type.DECIMAL32);
    public static final Type ARRAY_DECIMAL64 = new ArrayType(Type.DECIMAL64);
    public static final Type ARRAY_DECIMAL128 = new ArrayType(Type.DECIMAL128);

    public static final ImmutableList<ScalarType> INTEGER_TYPES =
            ImmutableList.of(BOOLEAN, TINYINT, SMALLINT, INT, BIGINT, LARGEINT);

    // TODO(lism): DOUBLE type should be the first because `registerBuiltinSumAggFunction` replies
    // the order to implicitly cast.
    public static final ImmutableList<ScalarType> FLOAT_TYPES =
            ImmutableList.of(DOUBLE, FLOAT);

    // NOTE: DECIMAL_TYPES not contain DECIMALV2
    public static final ImmutableList<ScalarType> DECIMAL_TYPES =
            ImmutableList.of(DECIMAL32, DECIMAL64, DECIMAL128);

    public static final ImmutableList<ScalarType> DATE_TYPES =
            ImmutableList.of(Type.DATE, Type.DATETIME);
    public static final ImmutableList<ScalarType> STRING_TYPES =
            ImmutableList.of(Type.CHAR, Type.VARCHAR);
    private static final ImmutableList<ScalarType> NUMERIC_TYPES =
            ImmutableList.<ScalarType>builder()
                    .addAll(INTEGER_TYPES)
                    .addAll(FLOAT_TYPES)
                    .add(DECIMALV2)
                    .addAll(DECIMAL_TYPES)
                    .build();

    protected static final ImmutableList<Type> SUPPORTED_TYPES =
            ImmutableList.<Type>builder()
                    .add(NULL)
                    .addAll(INTEGER_TYPES)
                    .addAll(FLOAT_TYPES)
                    .addAll(DECIMAL_TYPES)
                    .add(VARCHAR)
                    .add(HLL)
                    .add(BITMAP)
                    .add(PERCENTILE)
                    .add(CHAR)
                    .add(DATE)
                    .add(DATETIME)
                    .add(DECIMALV2)
                    .add(TIME)
                    .add(ANY_ARRAY)
                    .add(ANY_MAP)
                    .add(ANY_STRUCT)
                    .add(JSON)
                    .add(FUNCTION)
                    .add(VARBINARY)
                    .add(UNKNOWN_TYPE)
                    .build();

    protected static final ImmutableList<Type> SUPPORT_SCALAR_TYPE_LIST =
            ImmutableList.copyOf(SUPPORTED_TYPES.stream().filter(Type::isScalarType).collect(Collectors.toList()));

    protected static final ImmutableSortedMap<String, ScalarType> STATIC_TYPE_MAP =
            ImmutableSortedMap.<String, ScalarType>orderedBy(String.CASE_INSENSITIVE_ORDER)
                    .put("DECIMAL", Type.DEFAULT_DECIMALV2) // generic name for decimal
                    .put("STRING", Type.DEFAULT_STRING)
                    .put("INTEGER", Type.INT)
                    .put("UNSIGNED", Type.INT)
                    .putAll(SUPPORT_SCALAR_TYPE_LIST.stream()
                            .collect(Collectors.toMap(x -> x.getPrimitiveType().toString(), x -> (ScalarType) x)))
                    .build();

    protected static final ImmutableMap<PrimitiveType, ScalarType> PRIMITIVE_TYPE_SCALAR_TYPE_MAP =
            ImmutableMap.<PrimitiveType, ScalarType>builder()
                    .putAll(SUPPORT_SCALAR_TYPE_LIST.stream()
                            .collect(Collectors.toMap(Type::getPrimitiveType, x -> (ScalarType) x)))
                    .put(INVALID.getPrimitiveType(), INVALID)
                    .build();

    /**
     * Matrix that records "smallest" assignment-compatible type of two types
     * (INVALID_TYPE if no such type exists, ie, if the input types are fundamentally
     * incompatible). A value of any of the two types could be assigned to a slot
     * of the assignment-compatible type. For strict compatibility, this can be done
     * without any loss of precision. For non-strict compatibility, there may be loss of
     * precision, e.g. if converting from BIGINT to FLOAT.
     * <p>
     * We chose not to follow MySQL's type casting behavior as described here:
     * http://dev.mysql.com/doc/refman/5.0/en/type-conversion.html
     * for the following reasons:
     * conservative casting in arithmetic exprs: TINYINT + TINYINT -> BIGINT
     * comparison of many types as double: INT < FLOAT -> comparison as DOUBLE
     * special cases when dealing with dates and timestamps.
     */
    protected static PrimitiveType[][] compatibilityMatrix =
            new PrimitiveType[PrimitiveType.values().length][PrimitiveType.values().length];

    static {

        for (int i = 0; i < PrimitiveType.values().length; ++i) {
            // Each type is compatible with itself.
            compatibilityMatrix[i][i] = PrimitiveType.values()[i];
        }

        // BOOLEAN
        compatibilityMatrix[BOOLEAN.ordinal()][TINYINT.ordinal()] = PrimitiveType.TINYINT;
        compatibilityMatrix[BOOLEAN.ordinal()][SMALLINT.ordinal()] = PrimitiveType.SMALLINT;
        compatibilityMatrix[BOOLEAN.ordinal()][INT.ordinal()] = PrimitiveType.INT;
        compatibilityMatrix[BOOLEAN.ordinal()][BIGINT.ordinal()] = PrimitiveType.BIGINT;
        compatibilityMatrix[BOOLEAN.ordinal()][LARGEINT.ordinal()] = PrimitiveType.LARGEINT;
        compatibilityMatrix[BOOLEAN.ordinal()][FLOAT.ordinal()] = PrimitiveType.FLOAT;
        compatibilityMatrix[BOOLEAN.ordinal()][DOUBLE.ordinal()] = PrimitiveType.DOUBLE;
        compatibilityMatrix[BOOLEAN.ordinal()][DATE.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[BOOLEAN.ordinal()][DATETIME.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[BOOLEAN.ordinal()][CHAR.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[BOOLEAN.ordinal()][VARCHAR.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[BOOLEAN.ordinal()][HLL.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[BOOLEAN.ordinal()][TIME.ordinal()] = PrimitiveType.DOUBLE;
        compatibilityMatrix[BOOLEAN.ordinal()][BITMAP.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[BOOLEAN.ordinal()][PERCENTILE.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[BOOLEAN.ordinal()][JSON.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[BOOLEAN.ordinal()][UNKNOWN_TYPE.ordinal()] = PrimitiveType.INVALID_TYPE;

        // TINYINT
        compatibilityMatrix[TINYINT.ordinal()][SMALLINT.ordinal()] = PrimitiveType.SMALLINT;
        compatibilityMatrix[TINYINT.ordinal()][INT.ordinal()] = PrimitiveType.INT;
        compatibilityMatrix[TINYINT.ordinal()][BIGINT.ordinal()] = PrimitiveType.BIGINT;
        compatibilityMatrix[TINYINT.ordinal()][LARGEINT.ordinal()] = PrimitiveType.LARGEINT;
        // 8 bit integer fits in mantissa of both float and double.
        compatibilityMatrix[TINYINT.ordinal()][FLOAT.ordinal()] = PrimitiveType.FLOAT;
        compatibilityMatrix[TINYINT.ordinal()][DOUBLE.ordinal()] = PrimitiveType.DOUBLE;
        compatibilityMatrix[TINYINT.ordinal()][DATE.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[TINYINT.ordinal()][DATETIME.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[TINYINT.ordinal()][CHAR.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[TINYINT.ordinal()][VARCHAR.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[TINYINT.ordinal()][DECIMALV2.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[TINYINT.ordinal()][HLL.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[TINYINT.ordinal()][TIME.ordinal()] = PrimitiveType.DOUBLE;
        compatibilityMatrix[TINYINT.ordinal()][BITMAP.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[TINYINT.ordinal()][PERCENTILE.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[TINYINT.ordinal()][DECIMAL32.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[TINYINT.ordinal()][DECIMAL64.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[TINYINT.ordinal()][DECIMAL128.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[TINYINT.ordinal()][JSON.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[TINYINT.ordinal()][UNKNOWN_TYPE.ordinal()] = PrimitiveType.INVALID_TYPE;

        // SMALLINT
        compatibilityMatrix[SMALLINT.ordinal()][INT.ordinal()] = PrimitiveType.INT;
        compatibilityMatrix[SMALLINT.ordinal()][BIGINT.ordinal()] = PrimitiveType.BIGINT;
        compatibilityMatrix[SMALLINT.ordinal()][LARGEINT.ordinal()] = PrimitiveType.LARGEINT;
        // 16 bit integer fits in mantissa of both float and double.
        compatibilityMatrix[SMALLINT.ordinal()][FLOAT.ordinal()] = PrimitiveType.FLOAT;
        compatibilityMatrix[SMALLINT.ordinal()][DOUBLE.ordinal()] = PrimitiveType.DOUBLE;
        compatibilityMatrix[SMALLINT.ordinal()][DATE.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[SMALLINT.ordinal()][DATETIME.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[SMALLINT.ordinal()][CHAR.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[SMALLINT.ordinal()][VARCHAR.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[SMALLINT.ordinal()][DECIMALV2.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[SMALLINT.ordinal()][HLL.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[SMALLINT.ordinal()][TIME.ordinal()] = PrimitiveType.DOUBLE;
        compatibilityMatrix[SMALLINT.ordinal()][BITMAP.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[SMALLINT.ordinal()][PERCENTILE.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[SMALLINT.ordinal()][DECIMAL32.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[SMALLINT.ordinal()][DECIMAL64.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[SMALLINT.ordinal()][DECIMAL128.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[SMALLINT.ordinal()][JSON.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[SMALLINT.ordinal()][UNKNOWN_TYPE.ordinal()] = PrimitiveType.INVALID_TYPE;

        // INT
        compatibilityMatrix[INT.ordinal()][BIGINT.ordinal()] = PrimitiveType.BIGINT;
        compatibilityMatrix[INT.ordinal()][LARGEINT.ordinal()] = PrimitiveType.LARGEINT;
        compatibilityMatrix[INT.ordinal()][FLOAT.ordinal()] = PrimitiveType.DOUBLE;
        compatibilityMatrix[INT.ordinal()][DOUBLE.ordinal()] = PrimitiveType.DOUBLE;
        compatibilityMatrix[INT.ordinal()][DATE.ordinal()] = PrimitiveType.INT;
        compatibilityMatrix[INT.ordinal()][DATETIME.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[INT.ordinal()][CHAR.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[INT.ordinal()][VARCHAR.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[INT.ordinal()][DECIMALV2.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[INT.ordinal()][HLL.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[INT.ordinal()][TIME.ordinal()] = PrimitiveType.DOUBLE;
        compatibilityMatrix[INT.ordinal()][BITMAP.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[INT.ordinal()][PERCENTILE.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[INT.ordinal()][DECIMAL32.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[INT.ordinal()][DECIMAL64.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[INT.ordinal()][DECIMAL128.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[INT.ordinal()][JSON.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[INT.ordinal()][UNKNOWN_TYPE.ordinal()] = PrimitiveType.INVALID_TYPE;

        compatibilityMatrix[BIGINT.ordinal()][LARGEINT.ordinal()] = PrimitiveType.LARGEINT;
        compatibilityMatrix[BIGINT.ordinal()][FLOAT.ordinal()] = PrimitiveType.DOUBLE;
        // TODO: we're breaking the definition of strict compatibility for BIGINT + DOUBLE,
        // but this forces function overloading to consider the DOUBLE overload first.
        compatibilityMatrix[BIGINT.ordinal()][DOUBLE.ordinal()] = PrimitiveType.DOUBLE;
        compatibilityMatrix[BIGINT.ordinal()][DATE.ordinal()] = PrimitiveType.BIGINT;
        compatibilityMatrix[BIGINT.ordinal()][DATETIME.ordinal()] = PrimitiveType.BIGINT;
        compatibilityMatrix[BIGINT.ordinal()][CHAR.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[BIGINT.ordinal()][VARCHAR.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[BIGINT.ordinal()][DECIMALV2.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[BIGINT.ordinal()][HLL.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[BIGINT.ordinal()][TIME.ordinal()] = PrimitiveType.DOUBLE;
        compatibilityMatrix[BIGINT.ordinal()][BITMAP.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[BIGINT.ordinal()][PERCENTILE.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[BIGINT.ordinal()][DECIMAL32.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[BIGINT.ordinal()][DECIMAL64.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[BIGINT.ordinal()][DECIMAL128.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[BIGINT.ordinal()][JSON.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[BIGINT.ordinal()][UNKNOWN_TYPE.ordinal()] = PrimitiveType.INVALID_TYPE;

        // LARGEINT
        compatibilityMatrix[LARGEINT.ordinal()][FLOAT.ordinal()] = PrimitiveType.DOUBLE;
        compatibilityMatrix[LARGEINT.ordinal()][DOUBLE.ordinal()] = PrimitiveType.DOUBLE;
        compatibilityMatrix[LARGEINT.ordinal()][DATE.ordinal()] = PrimitiveType.LARGEINT;
        compatibilityMatrix[LARGEINT.ordinal()][DATETIME.ordinal()] = PrimitiveType.LARGEINT;
        compatibilityMatrix[LARGEINT.ordinal()][CHAR.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[LARGEINT.ordinal()][VARCHAR.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[LARGEINT.ordinal()][DECIMALV2.ordinal()] = PrimitiveType.DECIMALV2;
        compatibilityMatrix[LARGEINT.ordinal()][HLL.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[LARGEINT.ordinal()][TIME.ordinal()] = PrimitiveType.DOUBLE;
        compatibilityMatrix[LARGEINT.ordinal()][BITMAP.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[LARGEINT.ordinal()][PERCENTILE.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[LARGEINT.ordinal()][DECIMAL32.ordinal()] = PrimitiveType.DECIMAL32;
        compatibilityMatrix[LARGEINT.ordinal()][DECIMAL64.ordinal()] = PrimitiveType.DECIMAL64;
        compatibilityMatrix[LARGEINT.ordinal()][DECIMAL128.ordinal()] = PrimitiveType.DECIMAL128;
        compatibilityMatrix[LARGEINT.ordinal()][JSON.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[LARGEINT.ordinal()][UNKNOWN_TYPE.ordinal()] = PrimitiveType.INVALID_TYPE;

        // FLOAT
        compatibilityMatrix[FLOAT.ordinal()][DOUBLE.ordinal()] = PrimitiveType.DOUBLE;
        compatibilityMatrix[FLOAT.ordinal()][DATE.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[FLOAT.ordinal()][DATETIME.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[FLOAT.ordinal()][CHAR.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[FLOAT.ordinal()][VARCHAR.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[FLOAT.ordinal()][DECIMALV2.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[FLOAT.ordinal()][HLL.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[FLOAT.ordinal()][TIME.ordinal()] = PrimitiveType.DOUBLE;
        compatibilityMatrix[FLOAT.ordinal()][BITMAP.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[FLOAT.ordinal()][PERCENTILE.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[FLOAT.ordinal()][DECIMAL32.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[FLOAT.ordinal()][DECIMAL64.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[FLOAT.ordinal()][DECIMAL128.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[FLOAT.ordinal()][JSON.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[FLOAT.ordinal()][UNKNOWN_TYPE.ordinal()] = PrimitiveType.INVALID_TYPE;

        // DOUBLE
        compatibilityMatrix[DOUBLE.ordinal()][DATE.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[DOUBLE.ordinal()][DATETIME.ordinal()] = PrimitiveType.DOUBLE;
        compatibilityMatrix[DOUBLE.ordinal()][CHAR.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[DOUBLE.ordinal()][VARCHAR.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[DOUBLE.ordinal()][DECIMALV2.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[DOUBLE.ordinal()][HLL.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[DOUBLE.ordinal()][TIME.ordinal()] = PrimitiveType.DOUBLE;
        compatibilityMatrix[DOUBLE.ordinal()][BITMAP.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[DOUBLE.ordinal()][PERCENTILE.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[DOUBLE.ordinal()][DECIMAL32.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[DOUBLE.ordinal()][DECIMAL64.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[DOUBLE.ordinal()][DECIMAL128.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[DOUBLE.ordinal()][UNKNOWN_TYPE.ordinal()] = PrimitiveType.INVALID_TYPE;

        // DATE
        compatibilityMatrix[DATE.ordinal()][DATETIME.ordinal()] = PrimitiveType.DATETIME;
        compatibilityMatrix[DATE.ordinal()][CHAR.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[DATE.ordinal()][VARCHAR.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[DATE.ordinal()][DECIMALV2.ordinal()] = PrimitiveType.DECIMALV2;
        compatibilityMatrix[DATE.ordinal()][HLL.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[DATE.ordinal()][TIME.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[DATE.ordinal()][BITMAP.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[DATE.ordinal()][PERCENTILE.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[DATE.ordinal()][DECIMAL32.ordinal()] = PrimitiveType.DECIMAL32;
        compatibilityMatrix[DATE.ordinal()][DECIMAL64.ordinal()] = PrimitiveType.DECIMAL64;
        compatibilityMatrix[DATE.ordinal()][DECIMAL128.ordinal()] = PrimitiveType.DECIMAL128;
        compatibilityMatrix[DATE.ordinal()][UNKNOWN_TYPE.ordinal()] = PrimitiveType.INVALID_TYPE;

        // DATETIME
        compatibilityMatrix[DATETIME.ordinal()][CHAR.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[DATETIME.ordinal()][VARCHAR.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[DATETIME.ordinal()][DECIMALV2.ordinal()] = PrimitiveType.DECIMALV2;
        compatibilityMatrix[DATETIME.ordinal()][HLL.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[DATETIME.ordinal()][TIME.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[DATETIME.ordinal()][BITMAP.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[DATETIME.ordinal()][PERCENTILE.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[DATETIME.ordinal()][DECIMAL32.ordinal()] = PrimitiveType.DECIMAL32;
        compatibilityMatrix[DATETIME.ordinal()][DECIMAL64.ordinal()] = PrimitiveType.DECIMAL64;
        compatibilityMatrix[DATETIME.ordinal()][DECIMAL128.ordinal()] = PrimitiveType.DECIMAL128;
        compatibilityMatrix[DATETIME.ordinal()][UNKNOWN_TYPE.ordinal()] = PrimitiveType.INVALID_TYPE;

        // We can convert some but not all string values to timestamps.
        // CHAR
        compatibilityMatrix[CHAR.ordinal()][VARCHAR.ordinal()] = PrimitiveType.VARCHAR;
        compatibilityMatrix[CHAR.ordinal()][DECIMALV2.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[CHAR.ordinal()][HLL.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[CHAR.ordinal()][TIME.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[CHAR.ordinal()][BITMAP.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[CHAR.ordinal()][PERCENTILE.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[CHAR.ordinal()][DECIMAL32.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[CHAR.ordinal()][DECIMAL64.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[CHAR.ordinal()][DECIMAL128.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[CHAR.ordinal()][UNKNOWN_TYPE.ordinal()] = PrimitiveType.INVALID_TYPE;

        // VARCHAR
        compatibilityMatrix[VARCHAR.ordinal()][DECIMALV2.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[VARCHAR.ordinal()][HLL.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[VARCHAR.ordinal()][TIME.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[VARCHAR.ordinal()][BITMAP.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[VARCHAR.ordinal()][PERCENTILE.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[VARCHAR.ordinal()][DECIMAL32.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[VARCHAR.ordinal()][DECIMAL64.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[VARCHAR.ordinal()][DECIMAL128.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[VARCHAR.ordinal()][UNKNOWN_TYPE.ordinal()] = PrimitiveType.INVALID_TYPE;

        // DECIMALV2
        compatibilityMatrix[DECIMALV2.ordinal()][HLL.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[DECIMALV2.ordinal()][TIME.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[DECIMALV2.ordinal()][BITMAP.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[DECIMALV2.ordinal()][PERCENTILE.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[DECIMALV2.ordinal()][DECIMAL32.ordinal()] = PrimitiveType.DECIMAL32;
        compatibilityMatrix[DECIMALV2.ordinal()][DECIMAL64.ordinal()] = PrimitiveType.DECIMAL64;
        compatibilityMatrix[DECIMALV2.ordinal()][DECIMAL128.ordinal()] = PrimitiveType.DECIMAL128;
        compatibilityMatrix[DECIMALV2.ordinal()][UNKNOWN_TYPE.ordinal()] = PrimitiveType.INVALID_TYPE;

        // DECIMAL32
        compatibilityMatrix[DECIMAL32.ordinal()][HLL.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[DECIMAL32.ordinal()][TIME.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[DECIMAL32.ordinal()][BITMAP.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[DECIMAL32.ordinal()][PERCENTILE.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[DECIMAL32.ordinal()][DECIMAL32.ordinal()] = PrimitiveType.DECIMAL32;
        compatibilityMatrix[DECIMAL32.ordinal()][DECIMAL64.ordinal()] = PrimitiveType.DECIMAL64;
        compatibilityMatrix[DECIMAL32.ordinal()][DECIMAL128.ordinal()] = PrimitiveType.DECIMAL128;
        compatibilityMatrix[DECIMAL32.ordinal()][UNKNOWN_TYPE.ordinal()] = PrimitiveType.INVALID_TYPE;

        // DECIMAL64
        compatibilityMatrix[DECIMAL64.ordinal()][HLL.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[DECIMAL64.ordinal()][TIME.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[DECIMAL64.ordinal()][BITMAP.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[DECIMAL64.ordinal()][PERCENTILE.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[DECIMAL64.ordinal()][DECIMAL32.ordinal()] = PrimitiveType.DECIMAL32;
        compatibilityMatrix[DECIMAL64.ordinal()][DECIMAL64.ordinal()] = PrimitiveType.DECIMAL64;
        compatibilityMatrix[DECIMAL64.ordinal()][DECIMAL128.ordinal()] = PrimitiveType.DECIMAL128;
        compatibilityMatrix[DECIMAL64.ordinal()][UNKNOWN_TYPE.ordinal()] = PrimitiveType.INVALID_TYPE;

        // DECIMAL128
        compatibilityMatrix[DECIMAL128.ordinal()][HLL.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[DECIMAL128.ordinal()][TIME.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[DECIMAL128.ordinal()][BITMAP.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[DECIMAL128.ordinal()][PERCENTILE.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[DECIMAL128.ordinal()][DECIMAL32.ordinal()] = PrimitiveType.DECIMAL32;
        compatibilityMatrix[DECIMAL128.ordinal()][DECIMAL64.ordinal()] = PrimitiveType.DECIMAL64;
        compatibilityMatrix[DECIMAL128.ordinal()][DECIMAL128.ordinal()] = PrimitiveType.DECIMAL128;
        compatibilityMatrix[DECIMAL128.ordinal()][UNKNOWN_TYPE.ordinal()] = PrimitiveType.INVALID_TYPE;

        // HLL
        compatibilityMatrix[HLL.ordinal()][TIME.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[HLL.ordinal()][BITMAP.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[HLL.ordinal()][PERCENTILE.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[HLL.ordinal()][UNKNOWN_TYPE.ordinal()] = PrimitiveType.INVALID_TYPE;

        // BITMAP
        compatibilityMatrix[BITMAP.ordinal()][TIME.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[BITMAP.ordinal()][PERCENTILE.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[BITMAP.ordinal()][UNKNOWN_TYPE.ordinal()] = PrimitiveType.INVALID_TYPE;

        compatibilityMatrix[PERCENTILE.ordinal()][BITMAP.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[PERCENTILE.ordinal()][UNKNOWN_TYPE.ordinal()] = PrimitiveType.INVALID_TYPE;

        // JSON
        for (PrimitiveType type : PrimitiveType.JSON_COMPATIBLE_TYPE) {
            ScalarType scalar = ScalarType.createType(type);
            compatibilityMatrix[scalar.ordinal()][JSON.ordinal()] = type;
        }
        for (PrimitiveType type : PrimitiveType.JSON_UNCOMPATIBLE_TYPE) {
            ScalarType scalar = ScalarType.createType(type);
            compatibilityMatrix[scalar.ordinal()][JSON.ordinal()] = PrimitiveType.INVALID_TYPE;
        }

        compatibilityMatrix[JSON.ordinal()][DATE.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[JSON.ordinal()][DATETIME.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[JSON.ordinal()][TIME.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[JSON.ordinal()][DATE.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[JSON.ordinal()][DATETIME.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[JSON.ordinal()][DECIMAL32.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[JSON.ordinal()][DECIMAL64.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[JSON.ordinal()][DECIMAL128.ordinal()] = PrimitiveType.INVALID_TYPE;

        // binary type
        for (PrimitiveType type : PrimitiveType.BINARY_INCOMPATIBLE_TYPE_LIST) {
            ScalarType scalar = ScalarType.createType(type);
            compatibilityMatrix[scalar.ordinal()][VARBINARY.ordinal()] = PrimitiveType.INVALID_TYPE;
        }

        // Check all the necessary entries that should be filled.
        // ignore binary
        for (int i = 0; i < PrimitiveType.values().length - 2; ++i) {
            for (int j = i; j < PrimitiveType.values().length - 2; ++j) {
                PrimitiveType t1 = PrimitiveType.values()[i];
                PrimitiveType t2 = PrimitiveType.values()[j];
                if (SKIP_COMPARE_TYPES.contains(t1) || SKIP_COMPARE_TYPES.contains(t2)) {
                    continue;
                }
                Preconditions.checkNotNull(compatibilityMatrix[i][j]);
            }
        }
    }

    public static List<ScalarType> getIntegerTypes() {
        return INTEGER_TYPES;
    }

    public static List<ScalarType> getNumericTypes() {
        return NUMERIC_TYPES;
    }

    /**
     * The output of this is stored directly in the hive metastore as the column type.
     * The string must match exactly.
     */
    public final String toSql() {
        return toSql(0);
    }

    /**
     * Recursive helper for toSql() to be implemented by subclasses. Keeps track of the
     * nesting depth and terminates the recursion if MAX_NESTING_DEPTH is reached.
     */
    protected abstract String toSql(int depth);

    /**
     * Same as toSql() but adds newlines and spaces for better readability of nested types.
     */
    public String prettyPrint() {
        return prettyPrint(0);
    }

    /**
     * Pretty prints this type with lpad number of leading spaces. Used to implement
     * prettyPrint() with space-indented nested types.
     */
    protected abstract String prettyPrint(int lpad);

    /**
     * Used for Nest Type
     */
    public void setSelectedField(ComplexTypeAccessPath accessPath, boolean needSetChildren) {
        throw new IllegalStateException("setSelectedField() is not implemented for type " + toSql());
    }

    /**
     * Used for Nest Type
     */
    public void selectAllFields() {
        throw new IllegalStateException("selectAllFields() is not implemented for type " + toSql());
    }

    public void pruneUnusedSubfields() {
        throw new IllegalStateException("pruneUnusedFields() is not implemented for type " + toSql());
    }

    /**
     * used for test
     */
    public Boolean[] getSelectedFields() {
        return selectedFields;
    }

    public boolean isInvalid() {
        return isScalarType(PrimitiveType.INVALID_TYPE);
    }

    public boolean isValid() {
        return !isInvalid();
    }

    public boolean isUnknown() {
        return isScalarType(PrimitiveType.UNKNOWN_TYPE);
    }

    public boolean isNull() {
        return isScalarType(PrimitiveType.NULL_TYPE);
    }

    public boolean isBoolean() {
        return isScalarType(PrimitiveType.BOOLEAN);
    }

    public boolean isDecimalV2() {
        return getPrimitiveType().isDecimalV2Type();
    }

    public boolean isChar() {
        return isScalarType(PrimitiveType.CHAR);
    }

    public boolean isVarchar() {
        return isScalarType(PrimitiveType.VARCHAR);
    }

    public boolean isWildcardDecimal() {
        return false;
    }

    public boolean isWildcardVarchar() {
        return false;
    }

    public boolean isWildcardChar() {
        return false;
    }

    public boolean isDecimalV3() {
        return getPrimitiveType().isDecimalV3Type();
    }

    public boolean isDecimalOfAnyVersion() {
        return isDecimalV2() || isDecimalV3();
    }

    public boolean isStringType() {
        return PrimitiveType.STRING_TYPE_LIST.contains(this.getPrimitiveType());
    }

    // only metric types have the following constraint:
    // 1. don't support as key column
    // 2. don't support filter
    // 3. don't support group by
    // 4. don't support index
    public boolean isOnlyMetricType() {
        return isScalarType(PrimitiveType.HLL) || isScalarType(PrimitiveType.BITMAP) ||
                isScalarType(PrimitiveType.PERCENTILE);
    }

    public static List<Type> getSupportedTypes() {
        return SUPPORTED_TYPES;
    }

    // TODO(dhc): fix this
    public static Type fromPrimitiveType(PrimitiveType type) {
        switch (type) {
            case BOOLEAN:
                return Type.BOOLEAN;
            case TINYINT:
                return Type.TINYINT;
            case SMALLINT:
                return Type.SMALLINT;
            case INT:
                return Type.INT;
            case BIGINT:
                return Type.BIGINT;
            case LARGEINT:
                return Type.LARGEINT;
            case FLOAT:
                return Type.FLOAT;
            case DOUBLE:
                return Type.DOUBLE;
            case DATE:
                return Type.DATE;
            case DATETIME:
                return Type.DATETIME;
            case TIME:
                return Type.TIME;
            case DECIMALV2:
                return Type.DECIMALV2;
            case CHAR:
                return Type.CHAR;
            case VARCHAR:
                return Type.VARCHAR;
            case HLL:
                return Type.HLL;
            case BITMAP:
                return Type.BITMAP;
            case PERCENTILE:
                return Type.PERCENTILE;
            case DECIMAL32:
                return Type.DECIMAL32;
            case DECIMAL64:
                return Type.DECIMAL64;
            case DECIMAL128:
                return Type.DECIMAL128;
            case JSON:
                return Type.JSON;
            case FUNCTION:
                return Type.FUNCTION;
            case VARBINARY:
                return Type.VARBINARY;
            default:
                return null;
        }
    }

    public boolean canApplyToNumeric() {
        // TODO(mofei) support sum, avg for JSON
        return !isOnlyMetricType() && !isJsonType() && !isFunctionType() && !isBinaryType() && !isStructType() &&
                !isMapType();
    }

    public boolean canJoinOn() {
        return !isOnlyMetricType() && !isJsonType() && !isFunctionType() && !isBinaryType() && !isStructType() &&
                !isMapType();
    }

    public boolean canGroupBy() {
        // TODO(mofei) support group by for JSON
        return !isOnlyMetricType() && !isJsonType() && !isFunctionType() && !isBinaryType() && !isStructType() &&
                !isMapType();
    }

    public boolean canOrderBy() {
        // TODO(mofei) support order by for JSON
        return !isOnlyMetricType() && !isJsonType() && !isFunctionType() && !isBinaryType() && !isStructType() &&
                !isMapType();
    }

    public boolean canPartitionBy() {
        // TODO(mofei) support partition by for JSON
        return !isOnlyMetricType() && !isJsonType() && !isFunctionType() && !isBinaryType() && !isStructType() &&
                !isMapType();
    }

    public boolean canDistinct() {
        // TODO(mofei) support distinct by for JSON
        return !isOnlyMetricType() && !isJsonType() && !isFunctionType() && !isBinaryType() && !isStructType() &&
                !isMapType();
    }

    public boolean canStatistic() {
        // TODO(mofei) support statistic by for JSON
        return !isOnlyMetricType() && !isJsonType() && !isComplexType() && !isFunctionType()
                && !isBinaryType();
    }

    public boolean canDistributedBy() {
        // TODO(mofei) support distributed by for JSON
        return !isComplexType() && !isFloatingPointType() && !isOnlyMetricType() && !isJsonType()
                && !isFunctionType() && !isBinaryType();
    }

    public boolean isKeyType() {
        // TODO(zhuming): support define a key column of type array.
        return !(isFloatingPointType() || isComplexType() || isOnlyMetricType() || isJsonType() || isBinaryType());
    }

    public boolean canBeWindowFunctionArgumentTypes() {
        return !(isNull() || isChar() || isTime() || isComplexType()
                || isPseudoType() || isFunctionType() || isBinaryType());
    }

    /**
     * Can be a key of materialized view
     */
    public boolean canBeMVKey() {
        return canDistributedBy();
    }

    public boolean supportBloomFilter() {
        return isScalarType() && !isFloatingPointType() && !isTinyint() && !isBoolean() && !isDecimalV3() &&
                !isJsonType() && !isOnlyMetricType() && !isFunctionType() && !isBinaryType();
    }

    public static final String ONLY_METRIC_TYPE_ERROR_MSG =
            "Type percentile/hll/bitmap/json/struct/map not support aggregation/group-by/order-by/union/join";

    public boolean isHllType() {
        return isScalarType(PrimitiveType.HLL);
    }

    public boolean isBitmapType() {
        return isScalarType(PrimitiveType.BITMAP);
    }

    public boolean isJsonType() {
        return isScalarType(PrimitiveType.JSON);
    }

    public boolean isPercentile() {
        return isScalarType(PrimitiveType.PERCENTILE);
    }

    public boolean isScalarType() {
        return this instanceof ScalarType;
    }

    public boolean isScalarType(PrimitiveType t) {
        return isScalarType() && this.getPrimitiveType() == t;
    }

    public boolean isFixedPointType() {
        return PrimitiveType.INTEGER_TYPE_LIST.contains(getPrimitiveType());
    }

    public boolean isFloatingPointType() {
        return isScalarType(PrimitiveType.FLOAT) || isScalarType(PrimitiveType.DOUBLE);
    }

    public boolean isIntegerType() {
        return isScalarType(PrimitiveType.TINYINT) || isScalarType(PrimitiveType.SMALLINT)
                || isScalarType(PrimitiveType.INT) || isScalarType(PrimitiveType.BIGINT);
    }

    public boolean isLargeIntType() {
        return isScalarType(PrimitiveType.LARGEINT);
    }

    public boolean isNumericType() {
        return isFixedPointType() || isFloatingPointType() || isDecimalV2() || isDecimalV3();
    }

    public boolean isExactNumericType() {
        return isFixedPointType() || isDecimalV2() || isDecimalV3();
    }

    public boolean isNativeType() {
        return isFixedPointType() || isFloatingPointType() || isBoolean();
    }

    public boolean isDateType() {
        return isScalarType(PrimitiveType.DATE) || isScalarType(PrimitiveType.DATETIME);
    }

    public boolean isDatetime() {
        return isScalarType(PrimitiveType.DATETIME);
    }

    public boolean isTime() {
        return isScalarType(PrimitiveType.TIME);
    }

    public boolean isComplexType() {
        return isStructType() || isCollectionType();
    }

    public boolean isCollectionType() {
        return isMapType() || isArrayType();
    }

    public boolean isMapType() {
        return this instanceof MapType;
    }

    public boolean isArrayType() {
        return this instanceof ArrayType;
    }

    public boolean isStructType() {
        return this instanceof StructType;
    }

    public boolean isDate() {
        return isScalarType(PrimitiveType.DATE);
    }

    public boolean isTinyint() {
        return isScalarType(PrimitiveType.TINYINT);
    }

    public boolean isSmallint() {
        return isScalarType(PrimitiveType.SMALLINT);
    }

    public boolean isInt() {
        return isScalarType(PrimitiveType.INT);
    }

    public boolean isBigint() {
        return isScalarType(PrimitiveType.BIGINT);
    }

    public boolean isLargeint() {
        return isScalarType(PrimitiveType.LARGEINT);
    }

    public boolean isFloat() {
        return isScalarType(PrimitiveType.FLOAT);
    }

    public boolean isDouble() {
        return isScalarType(PrimitiveType.DOUBLE);
    }

    public boolean isPseudoType() {
        return this instanceof PseudoType;
    }

    public boolean isFunctionType() {
        return isScalarType(PrimitiveType.FUNCTION);
    }

    public boolean isBinaryType() {
        return isScalarType(PrimitiveType.VARBINARY);
    }

    /**
     * Returns true if Impala supports this type in the metdata. It does not mean we
     * can manipulate data of this type. For tables that contain columns with these
     * types, we can safely skip over them.
     */
    public boolean isSupported() {
        return true;
    }

    public PrimitiveType getPrimitiveType() {
        return PrimitiveType.INVALID_TYPE;
    }

    /**
     * Returns the size in bytes of the fixed-length portion that a slot of this type
     * occupies in a tuple.
     */
    public int getSlotSize() {
        // 8-byte pointer and 4-byte length indicator (12 bytes total).
        // Per struct alignment rules, there is an extra 4 bytes of padding to align to 8
        // bytes so 16 bytes total.
        if (isComplexType()) {
            return 16;
        }
        throw new IllegalStateException("getSlotSize() not implemented for type " + toSql());
    }

    // Return type data size, used for compute optimizer column statistics
    public int getTypeSize() {
        // TODO(ywb): compute the collection type size later.
        if (isCollectionType()) {
            return 16;
        }
        throw new IllegalStateException("getTypeSize() not implemented for type " + toSql());
    }

    public TTypeDesc toThrift() {
        TTypeDesc container = new TTypeDesc();
        container.setTypes(new ArrayList<TTypeNode>());
        toThrift(container);
        return container;
    }

    public TColumnType toColumnTypeThrift() {
        return null;
    }

    /**
     * Subclasses should override this method to add themselves to the thrift container.
     */
    public abstract void toThrift(TTypeDesc container);

    /**
     * Returns true if this type is equal to t, or if t is a wildcard variant of this
     * type. Subclasses should override this as appropriate. The default implementation
     * here is to avoid special-casing logic in callers for concrete types.
     */
    public boolean matchesType(Type t) {
        return false;
    }

    /**
     * Returns true if t1 can be implicitly cast to t2 according to Impala's casting rules.
     * Implicit casts are always allowed when no loss of precision would result (i.e. every
     * value of t1 can be represented exactly by a value of t2). Implicit casts are allowed
     * in certain other cases such as casting numeric types to floating point types and
     * converting strings to timestamps.
     * If strict is true, only consider casts that result in no loss of precision.
     * TODO: Support casting of non-scalar types.
     */
    public static boolean isImplicitlyCastable(Type t1, Type t2, boolean strict) {
        if (t1.isScalarType() && t2.isScalarType()) {
            return ScalarType.isImplicitlyCastable((ScalarType) t1, (ScalarType) t2, strict);
        }
        if (t1.isArrayType() && t2.isArrayType()) {
            return isImplicitlyCastable(((ArrayType) t1).getItemType(), ((ArrayType) t2).getItemType(), strict);
        }
        return false;
    }

    // isAssignable means that assigning or casting rhs to lhs never overflows.
    // only both integer part width and fraction part width of lhs is not narrower than counterparts
    // of rhs, then rhs can be assigned to lhs. for integer types, integer part width is computed by
    // calling Type::getPrecision and its scale is 0.
    public static boolean isAssignable2Decimal(ScalarType lhs, ScalarType rhs) {
        int lhsIntPartWidth;
        int lhsScale;
        int rhsIntPartWidth;
        int rhsScale;
        if (lhs.isFixedPointType()) {
            lhsIntPartWidth = lhs.getPrecision();
            lhsScale = 0;
        } else {
            lhsIntPartWidth = lhs.getScalarPrecision() - lhs.getScalarScale();
            lhsScale = lhs.getScalarScale();
        }

        if (rhs.isFixedPointType()) {
            rhsIntPartWidth = rhs.getPrecision();
            rhsScale = 0;
        } else {
            rhsIntPartWidth = rhs.getScalarPrecision() - rhs.getScalarScale();
            rhsScale = rhs.getScalarScale();
        }

        // when lhs is integer, for instance, tinyint, lhsIntPartWidth is 3, it cannot holds
        // a DECIMAL(3, 0).
        if (lhs.isFixedPointType() && rhs.isDecimalOfAnyVersion()) {
            return lhsIntPartWidth > rhsIntPartWidth && lhsScale >= rhsScale;
        } else {
            return lhsIntPartWidth >= rhsIntPartWidth && lhsScale >= rhsScale;
        }
    }

    public static boolean canCastToAsFunctionParameter(Type from, Type to) {
        if (from.isNull()) {
            return true;
        } else if (from.isScalarType() && to.isScalarType()) {
            return ScalarType.canCastTo((ScalarType) from, (ScalarType) to);
        } else if (from.isArrayType() && to.isArrayType()) {
            return canCastTo(((ArrayType) from).getItemType(), ((ArrayType) to).getItemType());
        } else {
            return false;
        }
    }

    public static boolean canCastTo(Type from, Type to) {
        if (from.isNull()) {
            return true;
        } else if (from.isStringType() && to.isBitmapType()) {
            return true;
        } else if (from.isScalarType() && to.isScalarType()) {
            return ScalarType.canCastTo((ScalarType) from, (ScalarType) to);
        } else if (from.isArrayType() && to.isArrayType()) {
            return canCastTo(((ArrayType) from).getItemType(), ((ArrayType) to).getItemType());
        } else if (from.isMapType() && to.isMapType()) {
            MapType fromMap = (MapType) from;
            MapType toMap = (MapType) to;
            return canCastTo(fromMap.getKeyType(), toMap.getKeyType()) &&
                    canCastTo(fromMap.getValueType(), toMap.getValueType());
        } else if (from.isStructType() && to.isStructType()) {
            StructType fromStruct = (StructType) from;
            StructType toStruct = (StructType) to;
            if (fromStruct.getFields().size() != toStruct.getFields().size()) {
                return false;
            }
            for (int i = 0; i < fromStruct.getFields().size(); ++i) {
                if (!canCastTo(fromStruct.getField(i).getType(), toStruct.getField(i).getType())) {
                    return false;
                }
            }
            return true;
        } else if (from.isStringType() && to.isArrayType()) {
            return true;
        } else if (from.isJsonType() && to.isArrayScalar()) {
            // now we only support cast json to one dimensional array
            return true;
        } else {
            return false;
        }
    }

    public boolean isArrayScalar() {
        if (!isArrayType()) {
            return false;
        }
        ArrayType array = (ArrayType) this;
        return array.getItemType().isScalarType();
    }

    /**
     * Return type t such that values from both t1 and t2 can be assigned to t without an
     * explicit cast. If strict, does not consider conversions that would result in loss
     * of precision (e.g. converting decimal to float). Returns INVALID_TYPE if there is
     * no such type or if any of t1 and t2 is INVALID_TYPE.
     * TODO: Support non-scalar types.
     */
    public static Type getAssignmentCompatibleType(Type t1, Type t2, boolean strict) {
        if (t1.isScalarType() && t2.isScalarType()) {
            return ScalarType.getAssignmentCompatibleType((ScalarType) t1, (ScalarType) t2, strict);
        }
        return ScalarType.INVALID;
    }

    /**
     * Returns true if this type exceeds the MAX_NESTING_DEPTH, false otherwise.
     */
    public boolean exceedsMaxNestingDepth() {
        return exceedsMaxNestingDepth(0);
    }

    /**
     * Helper for exceedsMaxNestingDepth(). Recursively computes the max nesting depth,
     * terminating early if MAX_NESTING_DEPTH is reached. Returns true if this type
     * exceeds the MAX_NESTING_DEPTH, false otherwise.
     * <p>
     * Examples of types and their nesting depth:
     * INT --> 1
     * STRUCT<f1:INT> --> 2
     * STRUCT<f1:STRUCT<f2:INT>> --> 3
     * ARRAY<INT> --> 2
     * ARRAY<STRUCT<f1:INT>> --> 3
     * MAP<STRING,INT> --> 2
     * MAP<STRING,STRUCT<f1:INT>> --> 3
     */
    private boolean exceedsMaxNestingDepth(int d) {
        if (d >= MAX_NESTING_DEPTH) {
            return true;
        }
        if (isStructType()) {
            StructType structType = (StructType) this;
            for (StructField f : structType.getFields()) {
                if (f.getType().exceedsMaxNestingDepth(d + 1)) {
                    return true;
                }
            }
        } else if (isArrayType()) {
            ArrayType arrayType = (ArrayType) this;
            return arrayType.getItemType().exceedsMaxNestingDepth(d + 1);
        } else if (isMapType()) {
            MapType mapType = (MapType) this;
            return mapType.getValueType().exceedsMaxNestingDepth(d + 1);
        } else {
            Preconditions.checkState(isScalarType());
        }
        return false;
    }

    public static List<TTypeDesc> toThrift(Type[] types) {
        return toThrift(Lists.newArrayList(types));
    }

    public static List<TTypeDesc> toThrift(ArrayList<Type> types) {
        ArrayList<TTypeDesc> result = Lists.newArrayList();
        for (Type t : types) {
            result.add(t.toThrift());
        }
        return result;
    }

    public static Type fromThrift(TTypeDesc thrift) {
        Preconditions.checkState(thrift.types.size() > 0);
        Pair<Type, Integer> t = fromThrift(thrift, 0);
        Preconditions.checkState(t.second.equals(thrift.getTypesSize()));
        return t.first;
    }

    /**
     * Constructs a ColumnType rooted at the TTypeNode at nodeIdx in TColumnType.
     * Returned pair: The resulting ColumnType and the next nodeIdx that is not a child
     * type of the result.
     */
    protected static Pair<Type, Integer> fromThrift(TTypeDesc col, int nodeIdx) {
        TTypeNode node = col.getTypes().get(nodeIdx);
        Type type = null;
        int tmpNodeIdx = nodeIdx;
        switch (node.getType()) {
            case SCALAR: {
                Preconditions.checkState(node.isSetScalar_type());
                TScalarType scalarType = node.getScalar_type();
                if (scalarType.getType() == TPrimitiveType.CHAR) {
                    Preconditions.checkState(scalarType.isSetLen());
                    type = ScalarType.createCharType(scalarType.getLen());
                } else if (scalarType.getType() == TPrimitiveType.VARCHAR) {
                    Preconditions.checkState(scalarType.isSetLen());
                    type = ScalarType.createVarcharType(scalarType.getLen());
                } else if (scalarType.getType() == TPrimitiveType.VARBINARY) {
                    type = ScalarType.createVarbinary(scalarType.getLen());
                } else if (scalarType.getType() == TPrimitiveType.HLL) {
                    type = ScalarType.createHllType();
                } else if (scalarType.getType() == TPrimitiveType.DECIMAL) {
                    Preconditions.checkState(scalarType.isSetPrecision()
                            && scalarType.isSetScale());
                    type = ScalarType.createDecimalV2Type(scalarType.getPrecision(),
                            scalarType.getScale());
                } else if (scalarType.getType() == TPrimitiveType.DECIMALV2) {
                    Preconditions.checkState(scalarType.isSetPrecision()
                            && scalarType.isSetScale());
                    type = ScalarType.createDecimalV2Type(scalarType.getPrecision(),
                            scalarType.getScale());
                } else if (scalarType.getType() == TPrimitiveType.DECIMAL32 ||
                        scalarType.getType() == TPrimitiveType.DECIMAL64 ||
                        scalarType.getType() == TPrimitiveType.DECIMAL128) {
                    Preconditions.checkState(scalarType.isSetPrecision() && scalarType.isSetScale());
                    type = ScalarType.createDecimalV3Type(
                            PrimitiveType.fromThrift(scalarType.getType()),
                            scalarType.getPrecision(),
                            scalarType.getScale());
                } else {
                    type = ScalarType.createType(
                            PrimitiveType.fromThrift(scalarType.getType()));
                }
                ++tmpNodeIdx;
                break;
            }
            case ARRAY: {
                Preconditions.checkState(tmpNodeIdx + 1 < col.getTypesSize());
                Pair<Type, Integer> childType = fromThrift(col, tmpNodeIdx + 1);
                type = new ArrayType(childType.first);
                tmpNodeIdx = childType.second;
                break;
            }
            case MAP: {
                Preconditions.checkState(tmpNodeIdx + 2 < col.getTypesSize());
                Pair<Type, Integer> keyType = fromThrift(col, tmpNodeIdx + 1);
                Pair<Type, Integer> valueType = fromThrift(col, keyType.second);
                type = new MapType(keyType.first, valueType.first);
                tmpNodeIdx = valueType.second;
                break;
            }
            case STRUCT: {
                Preconditions.checkState(tmpNodeIdx + node.getStruct_fieldsSize() < col.getTypesSize());
                ArrayList<StructField> structFields = Lists.newArrayList();
                ++tmpNodeIdx;
                for (int i = 0; i < node.getStruct_fieldsSize(); ++i) {
                    TStructField thriftField = node.getStruct_fields().get(i);
                    String name = thriftField.getName();
                    String comment = null;
                    if (thriftField.isSetComment()) {
                        comment = thriftField.getComment();
                    }
                    Pair<Type, Integer> res = fromThrift(col, tmpNodeIdx);
                    tmpNodeIdx = res.second;
                    structFields.add(new StructField(name, res.first, comment));
                }
                type = new StructType(structFields);
                break;
            }
        }
        return new Pair<Type, Integer>(type, tmpNodeIdx);
    }

    /**
     * Utility function to get the primitive type of a thrift type that is known
     * to be scalar.
     */
    public TPrimitiveType getTPrimitiveType(TTypeDesc ttype) {
        Preconditions.checkState(ttype.getTypesSize() == 1);
        Preconditions.checkState(ttype.types.get(0).getType() == TTypeNodeType.SCALAR);
        return ttype.types.get(0).scalar_type.getType();
    }

    /**
     * JDBC data type description
     * Returns the column size for this type.
     * For numeric data this is the maximum precision.
     * For character data this is the length in characters.
     * For datetime types this is the length in characters of the String representation
     * (assuming the maximum allowed precision of the fractional seconds component).
     * For binary data this is the length in bytes.
     * Null is returned for for data types where the column size is not applicable.
     */
    public Integer getColumnSize() {
        if (!isScalarType()) {
            return null;
        }
        if (isNumericType()) {
            return getPrecision();
        }
        ScalarType t = (ScalarType) this;
        switch (t.getPrimitiveType()) {
            case CHAR:
            case VARCHAR:
            case HLL:
                return t.getLength();
            default:
                return null;
        }
    }

    /**
     * JDBC data type description
     * For numeric types, returns the maximum precision for this type.
     * For non-numeric types, returns null.
     */
    public Integer getPrecision() {
        if (!isScalarType()) {
            return null;
        }
        ScalarType t = (ScalarType) this;
        switch (t.getPrimitiveType()) {
            case TINYINT:
                return 3;
            case SMALLINT:
                return 5;
            case INT:
                return 10;
            case BIGINT:
                return 19;
            case LARGEINT:
                return 39;
            case FLOAT:
                return 7;
            case DOUBLE:
                return 15;
            case DECIMALV2:
            case DECIMAL32:
            case DECIMAL64:
            case DECIMAL128:
                return t.decimalPrecision();
            default:
                return null;
        }
    }

    /**
     * JDBC data type description
     * Returns the number of fractional digits for this type, or null if not applicable.
     * For timestamp/time types, returns the number of digits in the fractional seconds
     * component.
     */
    public Integer getDecimalDigits() {
        if (!isScalarType()) {
            return null;
        }
        ScalarType t = (ScalarType) this;
        switch (t.getPrimitiveType()) {
            case BOOLEAN:
            case TINYINT:
            case SMALLINT:
            case INT:
            case BIGINT:
                return 0;
            case FLOAT:
                return 7;
            case DOUBLE:
                return 15;
            case DECIMALV2:
            case DECIMAL32:
            case DECIMAL64:
            case DECIMAL128:
                return t.decimalScale();
            default:
                return null;
        }
    }

    public Type getResultType() {
        switch (this.getPrimitiveType()) {
            case BOOLEAN:
            case TINYINT:
            case SMALLINT:
            case INT:
            case BIGINT:
                return BIGINT;
            case LARGEINT:
                return LARGEINT;
            case FLOAT:
            case DOUBLE:
                return DOUBLE;
            case DATE:
            case DATETIME:
            case TIME:
            case CHAR:
            case VARCHAR:
            case HLL:
            case BITMAP:
            case PERCENTILE:
            case JSON:
                return VARCHAR;
            case DECIMALV2:
                return DECIMALV2;
            case DECIMAL32:
            case DECIMAL64:
            case DECIMAL128:
                return this;
            case FUNCTION:
                return FUNCTION;
            default:
                return INVALID;

        }
    }

    public static Type getCmpType(Type t1, Type t2) {
        if (t1.getPrimitiveType() == PrimitiveType.NULL_TYPE) {
            return t2;
        }
        if (t2.getPrimitiveType() == PrimitiveType.NULL_TYPE) {
            return t1;
        }

        if (t1.isScalarType() && t2.isScalarType() && (t1.isDecimalV3() || t2.isDecimalV3())) {
            return getAssignmentCompatibleType(t1, t2, false);
        }

        if (t1.getPrimitiveType().equals(t2.getPrimitiveType())) {
            return t1;
        }

        if (t1.isJsonType() || t2.isJsonType()) {
            return JSON;
        }

        PrimitiveType t1ResultType = t1.getResultType().getPrimitiveType();
        PrimitiveType t2ResultType = t2.getResultType().getPrimitiveType();
        // Following logical is compatible with MySQL.
        if ((t1ResultType == PrimitiveType.VARCHAR && t2ResultType == PrimitiveType.VARCHAR)) {
            return Type.VARCHAR;
        }
        if (t1ResultType == PrimitiveType.BIGINT && t2ResultType == PrimitiveType.BIGINT) {
            return getAssignmentCompatibleType(t1, t2, false);
        }

        if ((t1ResultType == PrimitiveType.BIGINT
                || t1ResultType == PrimitiveType.DECIMALV2)
                && (t2ResultType == PrimitiveType.BIGINT
                || t2ResultType == PrimitiveType.DECIMALV2)) {
            return Type.DECIMALV2;
        }
        if ((t1ResultType == PrimitiveType.BIGINT
                || t1ResultType == PrimitiveType.LARGEINT)
                && (t2ResultType == PrimitiveType.BIGINT
                || t2ResultType == PrimitiveType.LARGEINT)) {
            return Type.LARGEINT;
        }
        return Type.DOUBLE;
    }

    private static Type getCommonScalarType(ScalarType t1, ScalarType t2) {
        return ScalarType.getAssignmentCompatibleType(t1, t2, true);
    }

    private static Type getCommonArrayType(ArrayType t1, ArrayType t2) {
        Type item1 = t1.getItemType();
        Type item2 = t2.getItemType();
        Type common = getCommonType(item1, item2);
        return common.isValid() ? new ArrayType(common) : common;
    }

    /**
     * Given two types, return the common supertype of them.
     *
     * @return the common type, INVALID if no common type exists.
     */
    public static Type getCommonType(Type t1, Type t2) {
        if (t1.isScalarType() && t2.isScalarType()) {
            return getCommonScalarType((ScalarType) t1, (ScalarType) t2);
        }
        if (t1.isArrayType() && t2.isArrayType()) {
            return getCommonArrayType((ArrayType) t1, (ArrayType) t2);
        }
        if (t1.isNull() || t2.isNull()) {
            return t1.isNull() ? t2 : t1;
        }
        return Type.INVALID;
    }

    public static Type getCommonType(Type[] argTypes, int fromIndex, int toIndex) {
        Preconditions.checkState(argTypes != null);
        Preconditions.checkState(0 <= fromIndex && fromIndex < toIndex && toIndex <= argTypes.length);
        Type commonType = argTypes[fromIndex];
        for (int i = fromIndex + 1; i < toIndex; ++i) {
            commonType = ScalarType.getCommonType(commonType, argTypes[i]);
        }
        return commonType;
    }

    public Type getNumResultType() {
        switch (getPrimitiveType()) {
            case BOOLEAN:
            case TINYINT:
                return Type.TINYINT;
            case SMALLINT:
                return Type.SMALLINT;
            case INT:
                return Type.INT;
            case BIGINT:
                return Type.BIGINT;
            case LARGEINT:
                return Type.LARGEINT;
            case FLOAT:
            case DOUBLE:
            case DATE:
            case DATETIME:
            case TIME:
            case CHAR:
            case VARCHAR:
                return Type.DOUBLE;
            case DECIMALV2:
                return Type.DECIMALV2;
            case DECIMAL32:
            case DECIMAL64:
            case DECIMAL128:
                return this.getResultType();
            default:
                return Type.INVALID;

        }
    }

    public int getIndexSize() {
        if (this.getPrimitiveType() == PrimitiveType.CHAR) {
            return ((ScalarType) this).getLength();
        } else {
            return this.getPrimitiveType().getOlapColumnIndexSize();
        }
    }

    /**
     * https://dev.mysql.com/doc/internals/en/com-query-response.html#column-definition
     * column_length (4) -- maximum length of the field
     * <p>
     * Maximum length of result of evaluating this item, in number of bytes.
     * - For character or blob data types, max char length multiplied by max
     * character size (collation.mbmaxlen).
     * - For decimal type, it is the precision in digits plus sign (unless
     * unsigned) plus decimal point (unless it has zero decimals).
     * - For other numeric types, the default or specific display length.
     * - For date/time types, the display length (10 for DATE, 10 + optional FSP
     * for TIME, 19 + optional fsp for datetime/timestamp). fsp is the fractional seconds precision.
     */
    public int getMysqlResultSetFieldLength() {
        switch (this.getPrimitiveType()) {
            case BOOLEAN:
                return 1;
            case TINYINT:
                return 4;
            case SMALLINT:
                return 6;
            case INT:
                return 11;
            case BIGINT:
                return 20;
            case LARGEINT:
                return 40;
            case DATE:
                return 10;
            case DATETIME:
                return 19;
            case FLOAT:
                return 12;
            case DOUBLE:
                return 22;
            case DECIMALV2:
            case DECIMAL32:
            case DECIMAL64:
            case DECIMAL128:
                // precision + (scale > 0 ? 1 : 0) + (unsigned_flag || !precision ? 0 : 1));
                ScalarType decimalType = (ScalarType) this;
                int length = decimalType.getScalarPrecision();
                // when precision is 0 it means that original length was also 0.
                if (length == 0) {
                    return 0;
                }
                // scale > 0
                if (decimalType.getScalarScale() > 0) {
                    length += 1;
                }
                // one byte for sign
                // one byte for zero, if precision == scale
                // one byte for overflow but valid decimal
                return length + 3;
            case CHAR:
            case VARCHAR:
            case HLL:
            case BITMAP:
            case VARBINARY:
                ScalarType charType = ((ScalarType) this);
                int charLength = charType.getLength();
                if (charLength == -1) {
                    charLength = 64;
                }
                // utf8 charset
                return charLength * 3;
            default:
                // Treat ARRAY/MAP/STRUCT as VARCHAR(-1)
                return 60;
        }
    }

    /**
     * @return scalar scale if type is decimal
     * 31 if type is float or double
     * 0 others
     * <p>
     * https://dev.mysql.com/doc/internals/en/com-query-response.html#column-definition
     * decimals (1) -- max shown decimal digits
     * 0x00 for integers and static strings
     * 0x1f for dynamic strings, double, float
     * 0x00 to 0x51 for decimals
     */
    public int getMysqlResultSetFieldDecimals() {
        switch (this.getPrimitiveType()) {
            case DECIMALV2:
            case DECIMAL32:
            case DECIMAL64:
            case DECIMAL128:
                return ((ScalarType) this).getScalarScale();
            case FLOAT:
            case DOUBLE:
                return 31;
            default:
                return 0;
        }
    }

    /**
     * @return 33 (utf8_general_ci) if type is char varchar hll or bitmap
     * 63 (binary) others
     * <p>
     * https://dev.mysql.com/doc/internals/en/com-query-response.html#column-definition
     * character_set (2) -- is the column character set and is defined in Protocol::CharacterSet.
     */
    public int getMysqlResultSetFieldCharsetIndex() {
        switch (this.getPrimitiveType()) {
            case CHAR:
            case VARCHAR:
            case HLL:
            case BITMAP:
            // Because mysql does not have a large int type, mysql will treat it as hex after exceeding bigint
            case LARGEINT:
            case JSON:
                return CHARSET_UTF8;
            default:
                return CHARSET_BINARY;
        }
    }

    public MysqlColType getMysqlResultType() {
        if (isScalarType()) {
            return getPrimitiveType().toMysqlType();
        }
        return Type.VARCHAR.getPrimitiveType().toMysqlType();
    }

    @Override
    public Type clone() {
        try {
            return (Type) super.clone();
        } catch (CloneNotSupportedException ex) {
            throw new Error("Something impossible just happened", ex);
        }
    }

    // getInnermostType() is only used for array
    public static Type getInnermostType(Type type) throws AnalysisException {
        if (type.isScalarType() || type.isStructType() || type.isMapType()) {
            return type;
        }
        if (type.isArrayType()) {
            return getInnermostType(((ArrayType) type).getItemType());
        }
        throw new AnalysisException("Cannot get innermost type of '" + type + "'");
    }

    public String canonicalName() {
        return toString();
    }
}
