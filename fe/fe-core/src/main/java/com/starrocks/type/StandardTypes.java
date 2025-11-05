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

public class StandardTypes {
    // Static constant types for scalar types that don't require additional information.
    public static final ScalarType INVALID = new ScalarType(PrimitiveType.INVALID_TYPE);
    public static final ScalarType NULL = new ScalarType(PrimitiveType.NULL_TYPE);
    public static final Type ARRAY_NULL = new ArrayType(NULL);
    public static final ScalarType BOOLEAN = new ScalarType(PrimitiveType.BOOLEAN);
    public static final Type ARRAY_BOOLEAN = new ArrayType(BOOLEAN);
    public static final ScalarType TINYINT = new ScalarType(PrimitiveType.TINYINT);
    public static final Type ARRAY_TINYINT = new ArrayType(TINYINT);
    public static final ScalarType SMALLINT = new ScalarType(PrimitiveType.SMALLINT);
    public static final Type ARRAY_SMALLINT = new ArrayType(SMALLINT);
    public static final ScalarType INT = new ScalarType(PrimitiveType.INT);
    public static final Type ARRAY_INT = new ArrayType(INT);
    public static final ScalarType BIGINT = new ScalarType(PrimitiveType.BIGINT);
    public static final Type ARRAY_BIGINT = new ArrayType(BIGINT);
    public static final ScalarType LARGEINT = new ScalarType(PrimitiveType.LARGEINT);
    public static final Type ARRAY_LARGEINT = new ArrayType(LARGEINT);
    public static final ScalarType FLOAT = new ScalarType(PrimitiveType.FLOAT);
    public static final Type ARRAY_FLOAT = new ArrayType(FLOAT);
    public static final ScalarType DOUBLE = new ScalarType(PrimitiveType.DOUBLE);
    public static final Type ARRAY_DOUBLE = new ArrayType(DOUBLE);
    public static final ScalarType DATE = new ScalarType(PrimitiveType.DATE);
    public static final Type ARRAY_DATE = new ArrayType(DATE);
    public static final ScalarType DATETIME = new ScalarType(PrimitiveType.DATETIME);
    public static final Type ARRAY_DATETIME = new ArrayType(DATETIME);
    public static final ScalarType TIME = new ScalarType(PrimitiveType.TIME);
    public static final ScalarType DEFAULT_DECIMALV2 = TypeFactory.createDecimalV2Type(ScalarType.DEFAULT_PRECISION,
            ScalarType.DEFAULT_SCALE);
    public static final ScalarType DECIMALV2 = DEFAULT_DECIMALV2;
    public static final Type ARRAY_DECIMALV2 = new ArrayType(DECIMALV2);
    public static final ScalarType DEFAULT_DECIMAL32 =
            TypeFactory.createDecimalV3Type(PrimitiveType.DECIMAL32, 9, 3);
    public static final ScalarType DEFAULT_DECIMAL64 =
            TypeFactory.createDecimalV3Type(PrimitiveType.DECIMAL64, 18, 6);
    public static final ScalarType DEFAULT_DECIMAL128 =
            TypeFactory.createDecimalV3Type(PrimitiveType.DECIMAL128, 38, 9);
    public static final ScalarType DEFAULT_DECIMAL256 =
            TypeFactory.createDecimalV3Type(PrimitiveType.DECIMAL256, 76, 12);
    public static final ScalarType DECIMAL32 = TypeFactory.createWildcardDecimalV3Type(PrimitiveType.DECIMAL32);
    public static final Type ARRAY_DECIMAL32 = new ArrayType(DECIMAL32);
    public static final ScalarType DECIMAL64 = TypeFactory.createWildcardDecimalV3Type(PrimitiveType.DECIMAL64);
    public static final Type ARRAY_DECIMAL64 = new ArrayType(DECIMAL64);
    public static final ScalarType DECIMAL128 = TypeFactory.createWildcardDecimalV3Type(PrimitiveType.DECIMAL128);
    public static final Type ARRAY_DECIMAL128 = new ArrayType(DECIMAL128);
    public static final ScalarType DECIMAL256 = TypeFactory.createWildcardDecimalV3Type(PrimitiveType.DECIMAL256);
    // DECIMAL64_INT and DECIMAL128_INT for integer casting to decimal
    public static final ScalarType DECIMAL_ZERO =
            TypeFactory.createDecimalV3TypeForZero(0);
    public static final ScalarType DECIMAL32_INT =
            TypeFactory.createDecimalV3Type(PrimitiveType.DECIMAL64, 9, 0);
    public static final ScalarType DECIMAL64_INT =
            TypeFactory.createDecimalV3Type(PrimitiveType.DECIMAL64, 18, 0);
    public static final ScalarType DECIMAL128_INT =
            TypeFactory.createDecimalV3Type(PrimitiveType.DECIMAL128, 38, 0);
    public static final ScalarType VARCHAR = TypeFactory.createVarcharType(-1);
    public static final Type MAP_VARCHAR_VARCHAR = new MapType(VARCHAR, VARCHAR);
    public static final Type ARRAY_VARCHAR = new ArrayType(VARCHAR);
    public static final ScalarType STRING = TypeFactory.createVarcharType(TypeFactory.getOlapMaxVarcharLength());
    public static final ScalarType DEFAULT_STRING = TypeFactory.createDefaultString();
    public static final ScalarType HLL = TypeFactory.createHllType();
    public static final ScalarType CHAR = TypeFactory.createCharType(-1);
    public static final ScalarType BITMAP = new ScalarType(PrimitiveType.BITMAP);
    public static final ScalarType PERCENTILE = new ScalarType(PrimitiveType.PERCENTILE);
    public static final ScalarType JSON = new ScalarType(PrimitiveType.JSON);
    public static final Type ARRAY_JSON = new ArrayType(JSON);
    public static final ScalarType VARIANT = new ScalarType(PrimitiveType.VARIANT);
    public static final ScalarType UNKNOWN_TYPE = TypeFactory.createUnknownType();
    public static final ScalarType FUNCTION = new ScalarType(PrimitiveType.FUNCTION);
    public static final ScalarType VARBINARY = new ScalarType(PrimitiveType.VARBINARY);
    public static final PseudoType ANY_ELEMENT = PseudoType.ANY_ELEMENT;
    public static final PseudoType ANY_ARRAY = PseudoType.ANY_ARRAY;
    public static final PseudoType ANY_MAP = PseudoType.ANY_MAP;
    public static final PseudoType ANY_STRUCT = PseudoType.ANY_STRUCT;
}
