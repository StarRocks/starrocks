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

import com.starrocks.analysis.SlotDescriptor;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

public final class IcebergUtil {
    private IcebergUtil() {
    }

    public static String fileName(String path) {
        return path.substring(path.lastIndexOf('/') + 1);
    }

    public static class MinMaxValue {
        Object minValue;
        Object maxValue;
    }

    public static Map<Integer, MinMaxValue> parseMinMaxValueOfSlots(Schema schema,
                                                                    Map<Integer, ByteBuffer> lowerBound,
                                                                    Map<Integer, ByteBuffer> upperBound,
                                                                    List<SlotDescriptor> slots) {
        lowerBound = lowerBound == null ? Map.of() : lowerBound;
        upperBound = upperBound == null ? Map.of() : upperBound;
        Map<Integer, MinMaxValue> minMaxValues = Map.of();
        for (SlotDescriptor slot : slots) {
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
        }
        return minMaxValues;
    }
}
