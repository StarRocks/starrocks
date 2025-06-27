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

package com.starrocks.connector.iceberg;

import com.google.common.base.Preconditions;
import com.starrocks.analysis.SlotDescriptor;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public final class IcebergUtil {
    private IcebergUtil() {
    }

    public static String fileName(String path) {
        return path.substring(path.lastIndexOf('/') + 1);
    }

    public static class MinMaxValue {
        Object minValue;
        Object maxValue;
        int nullValueCount;
    }

    private static final Set<Type.TypeID> MIN_MAX_SUPPORTED_TYPES = Set.of(
            Type.TypeID.BOOLEAN,
            Type.TypeID.INTEGER,
            Type.TypeID.LONG,
            Type.TypeID.FLOAT,
            Type.TypeID.DOUBLE,
            Type.TypeID.DATE,
            Type.TypeID.TIME
    );

    public static Map<Integer, MinMaxValue> parseMinMaxValueBySlots(Schema schema,
                                                                    Map<Integer, ByteBuffer> lowerBounds,
                                                                    Map<Integer, ByteBuffer> upperBounds,
                                                                    Map<Integer, Integer> nullValueCounts,
                                                                    List<SlotDescriptor> slots) {

        Preconditions.checkArgument(nullValueCounts != null, "nullValueCounts cannot be null");
        lowerBounds = lowerBounds == null ? Map.of() : lowerBounds;
        upperBounds = upperBounds == null ? Map.of() : upperBounds;
        Map<Integer, MinMaxValue> minMaxValues = new HashMap<>();
        for (SlotDescriptor slot : slots) {
            // has to be a scalar type
            if (!slot.getType().isScalarType()) {
                continue;
            }
            Types.NestedField field = schema.findField(slot.getColumn().getName());
            if (field == null) {
                continue;
            }
            Type type = field.type();
            if (!type.isPrimitiveType()) {
                continue;
            }
            // we are not sure if there are null values or not.
            // so result maybe is null, so we can not use min/max value.
            if (!nullValueCounts.containsKey(field.fieldId())) {
                continue;
            }
            // create the min/max value object to put into map
            MinMaxValue minMaxValue = new MinMaxValue();
            minMaxValues.put(field.fieldId(), minMaxValue);
            minMaxValue.nullValueCount = nullValueCounts.get(field.fieldId());
            if (minMaxValue.nullValueCount != 0) {
                // If there are null values, we don't need to parse min/max values
                // because the result of min/max will be null for sure.
                continue;
            }
            if (!MIN_MAX_SUPPORTED_TYPES.contains(type.typeId())) {
                continue; // Skip unsupported types
            }
            // parse lower and upper bounds
            Object low = Conversions.fromByteBuffer(field.type(), lowerBounds.get(field.fieldId()));
            Object high = Conversions.fromByteBuffer(field.type(), upperBounds.get(field.fieldId()));
            minMaxValue.minValue = low;
            minMaxValue.maxValue = high;
        }
        return minMaxValues;
    }
}
