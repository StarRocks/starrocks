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

import io.delta.standalone.types.ArrayType;
import io.delta.standalone.types.BinaryType;
import io.delta.standalone.types.BooleanType;
import io.delta.standalone.types.ByteType;
import io.delta.standalone.types.DataType;
import io.delta.standalone.types.DateType;
import io.delta.standalone.types.DecimalType;
import io.delta.standalone.types.DoubleType;
import io.delta.standalone.types.FloatType;
import io.delta.standalone.types.IntegerType;
import io.delta.standalone.types.LongType;
import io.delta.standalone.types.MapType;
import io.delta.standalone.types.NullType;
import io.delta.standalone.types.ShortType;
import io.delta.standalone.types.StringType;
import io.delta.standalone.types.StructType;
import io.delta.standalone.types.TimestampType;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * An Enum representing Delta's {@link DataType} class types.
 *
 * <p>
 * This Enum can be used for example to build switch statement based on Delta's DataType type.
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
    NULL(NullType.class),
    SMALLINT(ShortType.class),
    TIMESTAMP(TimestampType.class),
    TINYINT(ByteType.class),
    STRING(StringType.class),
    STRUCT(StructType.class),
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
     *                        like to map to {@link com.starrocks.catalog.Type} instance.
     * @return mapped instance of {@link DeltaDataType} Enum.
     */
    public static DeltaDataType instanceFrom(Class<? extends DataType> deltaDataType) {
        return LOOKUP_MAP.getOrDefault(deltaDataType, OTHER);
    }
}