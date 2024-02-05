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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/catalog/AggregateType.java

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

import com.google.common.collect.Lists;
import com.starrocks.thrift.TAggregationType;

import java.util.EnumMap;
import java.util.EnumSet;
import java.util.List;

public enum AggregateType {
    SUM("SUM"),
    MIN("MIN"),
    MAX("MAX"),
    REPLACE("REPLACE"),
    REPLACE_IF_NOT_NULL("REPLACE_IF_NOT_NULL"),
    HLL_UNION("HLL_UNION"),
    NONE("NONE"),
    BITMAP_UNION("BITMAP_UNION"),
    PERCENTILE_UNION("PERCENTILE_UNION");

    private static EnumMap<AggregateType, EnumSet<PrimitiveType>> compatibilityMap;

    static {
        compatibilityMap = new EnumMap<>(AggregateType.class);
        List<PrimitiveType> primitiveTypeList = Lists.newArrayList();

        primitiveTypeList.add(PrimitiveType.INT);
        primitiveTypeList.add(PrimitiveType.BIGINT);
        primitiveTypeList.add(PrimitiveType.LARGEINT);
        primitiveTypeList.add(PrimitiveType.FLOAT);
        primitiveTypeList.add(PrimitiveType.DOUBLE);
        primitiveTypeList.add(PrimitiveType.DECIMALV2);
        primitiveTypeList.add(PrimitiveType.DECIMAL64);
        primitiveTypeList.add(PrimitiveType.DECIMAL128);
        compatibilityMap.put(SUM, EnumSet.copyOf(primitiveTypeList));

        primitiveTypeList.clear();
        primitiveTypeList.add(PrimitiveType.TINYINT);
        primitiveTypeList.add(PrimitiveType.SMALLINT);
        primitiveTypeList.add(PrimitiveType.INT);
        primitiveTypeList.add(PrimitiveType.BIGINT);
        primitiveTypeList.add(PrimitiveType.LARGEINT);
        primitiveTypeList.add(PrimitiveType.FLOAT);
        primitiveTypeList.add(PrimitiveType.DOUBLE);
        primitiveTypeList.add(PrimitiveType.DECIMALV2);
        primitiveTypeList.add(PrimitiveType.DATE);
        primitiveTypeList.add(PrimitiveType.DATETIME);
        primitiveTypeList.add(PrimitiveType.CHAR);
        primitiveTypeList.add(PrimitiveType.VARCHAR);
        primitiveTypeList.add(PrimitiveType.DECIMAL32);
        primitiveTypeList.add(PrimitiveType.DECIMAL64);
        primitiveTypeList.add(PrimitiveType.DECIMAL128);
        compatibilityMap.put(MIN, EnumSet.copyOf(primitiveTypeList));

        primitiveTypeList.clear();
        primitiveTypeList.add(PrimitiveType.TINYINT);
        primitiveTypeList.add(PrimitiveType.SMALLINT);
        primitiveTypeList.add(PrimitiveType.INT);
        primitiveTypeList.add(PrimitiveType.BIGINT);
        primitiveTypeList.add(PrimitiveType.LARGEINT);
        primitiveTypeList.add(PrimitiveType.FLOAT);
        primitiveTypeList.add(PrimitiveType.DOUBLE);
        primitiveTypeList.add(PrimitiveType.DECIMALV2);
        primitiveTypeList.add(PrimitiveType.DATE);
        primitiveTypeList.add(PrimitiveType.DATETIME);
        primitiveTypeList.add(PrimitiveType.CHAR);
        primitiveTypeList.add(PrimitiveType.VARCHAR);
        primitiveTypeList.add(PrimitiveType.DECIMAL32);
        primitiveTypeList.add(PrimitiveType.DECIMAL64);
        primitiveTypeList.add(PrimitiveType.DECIMAL128);
        compatibilityMap.put(MAX, EnumSet.copyOf(primitiveTypeList));

        primitiveTypeList.clear();

        EnumSet<PrimitiveType> replaceObject = EnumSet.allOf(PrimitiveType.class);
        replaceObject.remove(PrimitiveType.INVALID_TYPE);
        compatibilityMap.put(REPLACE, EnumSet.copyOf(replaceObject));

        // all types except bitmap, hll, percentile and complex types.
        EnumSet<PrimitiveType> excObject = EnumSet.allOf(PrimitiveType.class);
        excObject.remove(PrimitiveType.BITMAP);
        excObject.remove(PrimitiveType.HLL);
        excObject.remove(PrimitiveType.PERCENTILE);
        excObject.remove(PrimitiveType.INVALID_TYPE);

        compatibilityMap.put(REPLACE_IF_NOT_NULL, EnumSet.copyOf(excObject));

        primitiveTypeList.clear();
        primitiveTypeList.add(PrimitiveType.HLL);
        compatibilityMap.put(HLL_UNION, EnumSet.copyOf(primitiveTypeList));

        primitiveTypeList.clear();
        primitiveTypeList.add(PrimitiveType.BITMAP);
        compatibilityMap.put(BITMAP_UNION, EnumSet.copyOf(primitiveTypeList));

        primitiveTypeList.clear();
        primitiveTypeList.add(PrimitiveType.PERCENTILE);
        compatibilityMap.put(PERCENTILE_UNION, EnumSet.copyOf(primitiveTypeList));

        compatibilityMap.put(NONE, EnumSet.copyOf(excObject));
    }

    private final String sqlName;

    AggregateType(String sqlName) {
        this.sqlName = sqlName;
    }

    public static boolean checkPrimitiveTypeCompatibility(AggregateType aggType, PrimitiveType priType) {
        return compatibilityMap.get(aggType).contains(priType);
    }

    public String toSql() {
        return sqlName;
    }

    @Override
    public String toString() {
        return toSql();
    }

    /**
     * NONE is particular, which is equals to null to make some buggy code compatible
     */
    public static boolean isNullOrNone(AggregateType type) {
        return type == null || type == NONE;
    }

    public boolean checkCompatibility(Type type) {
        return checkPrimitiveTypeCompatibility(this, type.getPrimitiveType()) ||
                (this.isReplaceFamily() && type.isArrayType());
    }

    public static Type extendedPrecision(Type type) {
        if (type.isDecimalV3()) {
            return ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL128, 38, ((ScalarType) type).getScalarScale());
        }
        return type;
    }

    public boolean isReplaceFamily() {
        switch (this) {
            case REPLACE:
            case REPLACE_IF_NOT_NULL:
                return true;
            default:
                return false;
        }
    }

    public TAggregationType toThrift() {
        switch (this) {
            case SUM:
                return TAggregationType.SUM;
            case MAX:
                return TAggregationType.MAX;
            case MIN:
                return TAggregationType.MIN;
            case REPLACE:
                return TAggregationType.REPLACE;
            case REPLACE_IF_NOT_NULL:
                return TAggregationType.REPLACE_IF_NOT_NULL;
            case NONE:
                return TAggregationType.NONE;
            case HLL_UNION:
                return TAggregationType.HLL_UNION;
            case BITMAP_UNION:
                return TAggregationType.BITMAP_UNION;
            case PERCENTILE_UNION:
                return TAggregationType.PERCENTILE_UNION;
            default:
                return null;
        }
    }
}

