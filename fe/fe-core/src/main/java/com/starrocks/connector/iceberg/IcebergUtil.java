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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.starrocks.planner.SlotDescriptor;
import com.starrocks.thrift.TExprMinMaxValue;
import com.starrocks.thrift.TExprNodeType;
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
    public static String fileName(String path) {
        return path.substring(path.lastIndexOf('/') + 1);
    }

    public static class MinMaxValue {
        Object minValue;
        Object maxValue;
        long nullValueCount;
        long valueCount;

        private boolean toThrift(SlotDescriptor slot, TExprMinMaxValue texpr) {
            texpr.setHas_null((nullValueCount > 0));
            texpr.setAll_null((valueCount == nullValueCount));
            if (valueCount == nullValueCount) {
                texpr.setType(TExprNodeType.NULL_LITERAL);
                return true;
            }
            if (minValue == null || maxValue == null) {
                return false;
            }
            switch (slot.getType().getPrimitiveType()) {
                case BOOLEAN:
                    texpr.setType(TExprNodeType.BOOL_LITERAL);
                    texpr.setMin_int_value((Boolean) minValue ? 1 : 0);
                    texpr.setMax_int_value((Boolean) maxValue ? 1 : 0);
                    break;
                case TINYINT:
                case SMALLINT:
                case INT:
                case DATE:
                    texpr.setType(TExprNodeType.INT_LITERAL);
                    texpr.setMin_int_value((Integer) minValue);
                    texpr.setMax_int_value((Integer) maxValue);
                    break;
                case BIGINT:
                case TIME:
                    texpr.setType(TExprNodeType.INT_LITERAL);
                    texpr.setMin_int_value((Long) minValue);
                    texpr.setMax_int_value((Long) maxValue);
                    break;
                case FLOAT:
                    texpr.setType(TExprNodeType.FLOAT_LITERAL);
                    texpr.setMin_float_value((Float) minValue);
                    texpr.setMax_float_value((Float) maxValue);
                    break;
                case DOUBLE:
                    texpr.setType(TExprNodeType.FLOAT_LITERAL);
                    texpr.setMin_float_value((Double) minValue);
                    texpr.setMax_float_value((Double) maxValue);
                    break;
                default:
                    // Unsupported type for min/max optimization
                    return false;
            }
            return true;
        }

        public void toThrift(Map<Integer, TExprMinMaxValue> tExprMinMaxValueMap, SlotDescriptor slot) {
            TExprMinMaxValue texpr = new TExprMinMaxValue();
            if (toThrift(slot, texpr)) {
                tExprMinMaxValueMap.put(slot.getId().asInt(), texpr);
            }
        }
    }

    private static final Set<Type.TypeID> MIN_MAX_SUPPORTED_TYPES = Set.of(
            // TODO(yanz): to support more types.
            // datetime and timestamp: need to consider timezone.
            // decimal: need to consider precision and scale.
            // binary: min/max is not accurate for binary type.
            Type.TypeID.BOOLEAN,
            Type.TypeID.INTEGER,
            Type.TypeID.LONG,
            Type.TypeID.FLOAT,
            Type.TypeID.DOUBLE,
            Type.TypeID.DATE,
            Type.TypeID.TIME
    );

    @VisibleForTesting
    public static Map<Integer, MinMaxValue> parseMinMaxValueBySlots(Schema schema,
                                                                    Map<Integer, ByteBuffer> lowerBounds,
                                                                    Map<Integer, ByteBuffer> upperBounds,
                                                                    Map<Integer, Long> nullValueCounts,
                                                                    Map<Integer, Long> valueCounts,
                                                                    List<SlotDescriptor> slots) {

        Preconditions.checkArgument(nullValueCounts != null && valueCounts != null,
                "nullValueCounts and valueCounts cannot be null");
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
            // Skip unsupported types
            if (!MIN_MAX_SUPPORTED_TYPES.contains(type.typeId())) {
                continue;
            }
            if (!nullValueCounts.containsKey(field.fieldId()) || !valueCounts.containsKey(field.fieldId())) {
                continue;
            }
            // create the min/max value object to put into map
            MinMaxValue minMaxValue = new MinMaxValue();
            minMaxValues.put(field.fieldId(), minMaxValue);
            minMaxValue.nullValueCount = nullValueCounts.get(field.fieldId());
            minMaxValue.valueCount = valueCounts.get(field.fieldId());
            // parse lower and upper bounds
            Object low = Conversions.fromByteBuffer(field.type(), lowerBounds.get(field.fieldId()));
            Object high = Conversions.fromByteBuffer(field.type(), upperBounds.get(field.fieldId()));
            minMaxValue.minValue = low;
            minMaxValue.maxValue = high;
        }
        return minMaxValues;
    }

    public static Map<Integer, TExprMinMaxValue> toThriftMinMaxValueBySlots(Schema schema,
                                                                            Map<Integer, ByteBuffer> lowerBounds,
                                                                            Map<Integer, ByteBuffer> upperBounds,
                                                                            Map<Integer, Long> nullValueCounts,
                                                                            Map<Integer, Long> valueCounts,
                                                                            List<SlotDescriptor> slots) {
        Map<Integer, TExprMinMaxValue> result = new HashMap<>();
        Map<Integer, MinMaxValue> minMaxValues =
                parseMinMaxValueBySlots(schema, lowerBounds, upperBounds, nullValueCounts, valueCounts, slots);
        for (SlotDescriptor slot : slots) {
            int slotId = slot.getId().asInt();
            MinMaxValue minMaxValue = minMaxValues.get(slotId);
            if (minMaxValue == null) {
                continue; // No min/max value for this slot
            }
            minMaxValue.toThrift(result, slot);
        }
        return result;
    }
}