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


package com.starrocks.connector.delta;

import io.delta.kernel.types.ArrayType;
import io.delta.kernel.types.BinaryType;
import io.delta.kernel.types.BooleanType;
import io.delta.kernel.types.ByteType;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.DateType;
import io.delta.kernel.types.DecimalType;
import io.delta.kernel.types.DoubleType;
import io.delta.kernel.types.FloatType;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.MapType;
import io.delta.kernel.types.ShortType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructType;
import io.delta.kernel.types.TimestampNTZType;
import io.delta.kernel.types.TimestampType;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * An Enum representing Delta's {@link DataType} class types.
 */
public enum DeltaDataType {
    ARRAY(ArrayType.class),
    BINARY(BinaryType.class),
    BYTE(ByteType.class),
    BOOLEAN(BooleanType.class),
    DATE(DateType.class),
    DECIMAL(DecimalType.class),
    DOUBLE(DoubleType.class),
    FLOAT(FloatType.class),
    INTEGER(IntegerType.class),
    LONG(LongType.class),
    MAP(MapType.class),
    SMALLINT(ShortType.class),
    TIMESTAMP(TimestampType.class),
    STRING(StringType.class),
    STRUCT(StructType.class),
    TIMESTAMP_NTZ(TimestampNTZType.class),
    OTHER(null);

    private static final Map<Class<?>, DeltaDataType> LOOKUP_MAP;

    static {
        Map<Class<?>, DeltaDataType> tmpMap = new HashMap<>();
        for (DeltaDataType type : DeltaDataType.values()) {
            tmpMap.put(type.deltaDataTypeClass, type);
        }
        LOOKUP_MAP = Collections.unmodifiableMap(tmpMap);
    }

    private final Class<? extends DataType> deltaDataTypeClass;

    DeltaDataType(Class<? extends DataType> deltaDataTypeClass) {
        this.deltaDataTypeClass = deltaDataTypeClass;
    }

    /**
     * @param deltaDataType A concrete implementation of {@link DataType} class that we would
     *                      like to map to {@link com.starrocks.catalog.Type} instance.
     * @return mapped instance of {@link DeltaDataType} Enum.
     */
    public static DeltaDataType instanceFrom(Class<? extends DataType> deltaDataType) {
        return LOOKUP_MAP.getOrDefault(deltaDataType, OTHER);
    }

    public static final Set<DeltaDataType> PRIMITIVE_TYPES = new HashSet<>() {
        {
            add(DeltaDataType.BOOLEAN);
            add(DeltaDataType.BYTE);
            add(DeltaDataType.SMALLINT);
            add(DeltaDataType.INTEGER);
            add(DeltaDataType.LONG);
            add(DeltaDataType.FLOAT);
            add(DeltaDataType.DOUBLE);
            add(DeltaDataType.DATE);
            add(DeltaDataType.TIMESTAMP);
            add(DeltaDataType.TIMESTAMP_NTZ);
            add(DeltaDataType.BINARY);
            add(DeltaDataType.STRING);
        }
    };

    public static boolean isPrimitiveType(DataType type) {
        return PRIMITIVE_TYPES.contains(instanceFrom(type.getClass()));
    }

    public static boolean canUseStatsType(DataType type) {
        return isPrimitiveType(type) || type instanceof DecimalType;
    }
}