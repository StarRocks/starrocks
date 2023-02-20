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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Lists;
import com.starrocks.mysql.MysqlColType;
import com.starrocks.thrift.TPrimitiveType;

import java.util.Arrays;
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

    JSON("JSON", 16, TPrimitiveType.JSON),

    FUNCTION("FUNCTION", 8, TPrimitiveType.FUNCTION),

    BINARY("BINARY", -1, TPrimitiveType.BINARY),
    VARBINARY("VARBINARY", 16, TPrimitiveType.VARBINARY),

    // If external table column type is unsupported, it will be converted to UNKNOWN_TYPE
    UNKNOWN_TYPE("UNKNOWN_TYPE", -1, TPrimitiveType.INVALID_TYPE);

    private static final int DATE_INDEX_LEN = 3;
    private static final int DATETIME_INDEX_LEN = 8;
    private static final int VARCHAR_INDEX_LEN = 20;
    private static final int DECIMAL_INDEX_LEN = 12;

    private static final ImmutableSetMultimap<PrimitiveType, PrimitiveType> IMPLICIT_CAST_MAP;

    public static final ImmutableList<PrimitiveType> INTEGER_TYPE_LIST =
            ImmutableList.of(TINYINT, SMALLINT, INT, BIGINT, LARGEINT);

    public static final ImmutableList<PrimitiveType> FLOAT_TYPE_LIST =
            ImmutableList.of(FLOAT, DOUBLE, DECIMALV2, DECIMAL32, DECIMAL64, DECIMAL128);

    public static final ImmutableList<PrimitiveType> NUMBER_TYPE_LIST =
            ImmutableList.<PrimitiveType>builder()
                    .addAll(INTEGER_TYPE_LIST)
                    .addAll(FLOAT_TYPE_LIST)
                    .build();

    public static final ImmutableList<PrimitiveType> STRING_TYPE_LIST =
            ImmutableList.of(CHAR, VARCHAR);

    public static final ImmutableList<PrimitiveType> JSON_COMPATIBLE_TYPE =
            new ImmutableList.Builder<PrimitiveType>()
                    .add(BOOLEAN)
                    .addAll(NUMBER_TYPE_LIST)
                    .addAll(STRING_TYPE_LIST)
                    .build();
    // TODO(mofei) support them
    public static final ImmutableList<PrimitiveType> JSON_UNCOMPATIBLE_TYPE =
            ImmutableList.of(DATE, DATETIME, TIME, HLL, BITMAP, PERCENTILE, FUNCTION, VARBINARY);

    private static final ImmutableList<PrimitiveType> TIME_TYPE_LIST =
            ImmutableList.of(TIME, DATE, DATETIME);

    private static final ImmutableList<PrimitiveType> BINARY_TYPE_LIST = ImmutableList.of(VARBINARY);

    private static final ImmutableList<PrimitiveType> BASIC_TYPE_LIST =
            ImmutableList.<PrimitiveType>builder()
                    .add(NULL_TYPE)
                    .add(BOOLEAN)
                    .addAll(NUMBER_TYPE_LIST)
                    .addAll(TIME_TYPE_LIST)
                    .addAll(STRING_TYPE_LIST)
                    .addAll(BINARY_TYPE_LIST)
                    .build();

    public static final ImmutableList<PrimitiveType> BINARY_INCOMPATIBLE_TYPE_LIST =
            ImmutableList.<PrimitiveType>builder()
                    .add(NULL_TYPE)
                    .add(BOOLEAN)
                    .addAll(NUMBER_TYPE_LIST)
                    .addAll(TIME_TYPE_LIST)
                    .addAll(STRING_TYPE_LIST)
                    .build();
    private static final ImmutableSortedSet<String> VARIABLE_TYPE_SET =
            ImmutableSortedSet.orderedBy(String.CASE_INSENSITIVE_ORDER)
                    .add(PrimitiveType.CHAR.toString())
                    .add(PrimitiveType.VARCHAR.toString())
                    .add(PrimitiveType.DECIMALV2.toString())
                    .add(PrimitiveType.DECIMAL32.toString())
                    .add(PrimitiveType.DECIMAL64.toString())
                    .add(PrimitiveType.DECIMAL128.toString())
                    .add("DECIMAL") // generic name for all decimal types
                    .build();

    public static boolean isVariableType(String typeName) {
        return VARIABLE_TYPE_SET.contains(typeName);
    }

    public static boolean isStaticType(String typeName) {
        return !VARIABLE_TYPE_SET.contains(typeName);
    }

    static {
        ImmutableSetMultimap.Builder<PrimitiveType, PrimitiveType> builder = ImmutableSetMultimap.builder();
        builder.putAll(NULL_TYPE, BASIC_TYPE_LIST);
        builder.putAll(NULL_TYPE, ImmutableList.of(HLL, BITMAP, PERCENTILE, JSON, VARBINARY));

        builder.putAll(BOOLEAN, BASIC_TYPE_LIST);
        builder.putAll(TINYINT, BASIC_TYPE_LIST);
        builder.putAll(SMALLINT, BASIC_TYPE_LIST);
        builder.putAll(INT, BASIC_TYPE_LIST);
        builder.putAll(BIGINT, BASIC_TYPE_LIST);
        builder.putAll(LARGEINT, BASIC_TYPE_LIST);
        builder.putAll(FLOAT, BASIC_TYPE_LIST);
        builder.putAll(DOUBLE, BASIC_TYPE_LIST);
        builder.putAll(DATE, BASIC_TYPE_LIST);
        builder.putAll(DATETIME, BASIC_TYPE_LIST);
        builder.putAll(VARCHAR, BASIC_TYPE_LIST);
        builder.putAll(CHAR, BASIC_TYPE_LIST);

        // Decimal
        for (PrimitiveType decimalType : Arrays.asList(DECIMALV2, DECIMAL32, DECIMAL64, DECIMAL128)) {
            builder.putAll(decimalType, BOOLEAN);
            builder.putAll(decimalType, NUMBER_TYPE_LIST);
            builder.putAll(decimalType, STRING_TYPE_LIST);
            builder.putAll(decimalType, TIME);
        }

        // TIME
        builder.putAll(TIME, BASIC_TYPE_LIST);

        builder.put(HLL, HLL);
        builder.put(BITMAP, BITMAP);
        builder.put(PERCENTILE, PERCENTILE);

        // BINARY
        builder.putAll(VARBINARY, BASIC_TYPE_LIST);

        // JSON
        builder.putAll(JSON, JSON);
        builder.putAll(JSON, NULL_TYPE);

        builder.putAll(FUNCTION, FUNCTION);

        for (PrimitiveType type : JSON_COMPATIBLE_TYPE) {
            builder.put(type, JSON);
            builder.put(JSON, type);
        }

        IMPLICIT_CAST_MAP = builder.build();
    }

    private final String description;
    private final int slotSize;  // size of tuple slot for this type
    private final TPrimitiveType thriftType;
    private boolean isTimeType = false;

    PrimitiveType(String description, int slotSize, TPrimitiveType thriftType) {
        this.description = description;
        this.slotSize = slotSize;
        this.thriftType = thriftType;
    }

    public static ImmutableList<PrimitiveType> getIntegerTypeList() {
        return INTEGER_TYPE_LIST;
    }

    // Check whether 'type' can cast to 'target'
    public static boolean isImplicitCast(PrimitiveType type, PrimitiveType target) {
        if (type.equals(target)) {
            return true;
        }
        return IMPLICIT_CAST_MAP.get(type).contains(target);
    }

    public static PrimitiveType fromThrift(TPrimitiveType tPrimitiveType) {
        switch (tPrimitiveType) {
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
            case VARBINARY:
                return VARBINARY;
            case JSON:
                return JSON;
            case FUNCTION:
                return FUNCTION;
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

    public static PrimitiveType getDecimalPrimitiveType(int precision) {
        PrimitiveType type = INVALID_TYPE;
        if (precision > 0 && precision <= getMaxPrecisionOfDecimal(DECIMAL32)) {
            return DECIMAL32;
        } else if (precision <= getMaxPrecisionOfDecimal(DECIMAL64)) {
            return DECIMAL64;
        } else if (precision <= getMaxPrecisionOfDecimal(DECIMAL128)) {
            return DECIMAL128;
        }
        Preconditions.checkState(type.isDecimalOfAnyVersion());
        return type;
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
            case INVALID_TYPE:
            case BINARY:
            case UNKNOWN_TYPE:
            case FUNCTION:
                break;
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
            case VARBINARY:
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
            case JSON:
                typeSize = 1024;
                break;
            default:
                Preconditions.checkState(false, "unknown type " + this);
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

    public boolean isVariableLengthType() {
        switch (this) {
            case CHAR:
            case VARCHAR:
            case VARBINARY:
            case HLL:
                return true;
        }
        return false;
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

    public boolean isJsonType() {
        return this == JSON;
    }

    public boolean isFunctionType() {
        return this == FUNCTION;
    }

    public boolean isBinaryType() {
        return this == BINARY || this == VARBINARY;
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
            case VARBINARY:
                return MysqlColType.MYSQL_TYPE_BLOB;
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
