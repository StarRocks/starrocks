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

package com.starrocks.sql.analyzer;

import com.google.common.collect.Lists;
import com.starrocks.catalog.AggregateType;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.Type;

import java.util.EnumMap;
import java.util.EnumSet;
import java.util.List;

public class AggregateTypeAnalyzer {
    private static final EnumMap<AggregateType, EnumSet<PrimitiveType>> AGGREGATE_COMPATIBILITY_MAP;

    static {
        AGGREGATE_COMPATIBILITY_MAP = new EnumMap<>(AggregateType.class);
        List<PrimitiveType> primitiveTypeList = Lists.newArrayList();

        primitiveTypeList.add(PrimitiveType.INT);
        primitiveTypeList.add(PrimitiveType.BIGINT);
        primitiveTypeList.add(PrimitiveType.LARGEINT);
        primitiveTypeList.add(PrimitiveType.FLOAT);
        primitiveTypeList.add(PrimitiveType.DOUBLE);
        primitiveTypeList.add(PrimitiveType.DECIMALV2);
        primitiveTypeList.add(PrimitiveType.DECIMAL64);
        primitiveTypeList.add(PrimitiveType.DECIMAL128);
        AGGREGATE_COMPATIBILITY_MAP.put(AggregateType.SUM, EnumSet.copyOf(primitiveTypeList));

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
        AGGREGATE_COMPATIBILITY_MAP.put(AggregateType.MIN, EnumSet.copyOf(primitiveTypeList));

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
        AGGREGATE_COMPATIBILITY_MAP.put(AggregateType.MAX, EnumSet.copyOf(primitiveTypeList));

        primitiveTypeList.clear();

        EnumSet<PrimitiveType> replaceObject = EnumSet.allOf(PrimitiveType.class);
        replaceObject.remove(PrimitiveType.INVALID_TYPE);
        AGGREGATE_COMPATIBILITY_MAP.put(AggregateType.REPLACE, EnumSet.copyOf(replaceObject));

        // all types except bitmap, hll, percentile and complex types.
        EnumSet<PrimitiveType> excObject = EnumSet.allOf(PrimitiveType.class);
        excObject.remove(PrimitiveType.HLL);
        excObject.remove(PrimitiveType.PERCENTILE);
        excObject.remove(PrimitiveType.INVALID_TYPE);
        AGGREGATE_COMPATIBILITY_MAP.put(AggregateType.REPLACE_IF_NOT_NULL, EnumSet.copyOf(excObject));

        primitiveTypeList.clear();
        primitiveTypeList.add(PrimitiveType.HLL);
        AGGREGATE_COMPATIBILITY_MAP.put(AggregateType.HLL_UNION, EnumSet.copyOf(primitiveTypeList));

        primitiveTypeList.clear();
        primitiveTypeList.add(PrimitiveType.BITMAP);
        AGGREGATE_COMPATIBILITY_MAP.put(AggregateType.BITMAP_UNION, EnumSet.copyOf(primitiveTypeList));

        // percentile
        primitiveTypeList.clear();
        primitiveTypeList.add(PrimitiveType.PERCENTILE);
        AGGREGATE_COMPATIBILITY_MAP.put(AggregateType.PERCENTILE_UNION, EnumSet.copyOf(primitiveTypeList));

        // none
        AGGREGATE_COMPATIBILITY_MAP.put(AggregateType.NONE, EnumSet.copyOf(excObject));
    }

    public static boolean checkCompatibility(AggregateType aggregateType, Type type) {
        // AGG_STATE_UNION can accept all types, we cannot use compatibilityMap to check it.
        if (aggregateType == AggregateType.AGG_STATE_UNION) {
            return true;
        }

        PrimitiveType priType = type.getPrimitiveType();
        return AGGREGATE_COMPATIBILITY_MAP.get(aggregateType).contains(priType)
                || (aggregateType.isReplaceFamily() && type.isComplexType());
    }
}
