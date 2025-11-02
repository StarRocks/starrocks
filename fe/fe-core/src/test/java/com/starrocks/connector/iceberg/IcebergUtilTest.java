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

import com.starrocks.catalog.Column;
import com.starrocks.catalog.Type;
import com.starrocks.planner.SlotDescriptor;
import com.starrocks.planner.SlotId;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class IcebergUtilTest {

    @Test
    public void testFileName() {
        assertEquals("1.orc", IcebergUtil.fileName("hdfs://tans/user/hive/warehouse/max-test/1.orc"));
        assertEquals("2.orc", IcebergUtil.fileName("cos://tans/user/hive/warehouse/max-test/2.orc"));
        assertEquals("3.orc", IcebergUtil.fileName("s3://tans/user/hive/warehouse/max-test/3.orc"));
        assertEquals("4.orc", IcebergUtil.fileName("gs://tans/user/hive/warehouse/max-test/4.orc"));
    }

    @Test
    public void testParseMinMaxValueBySlots() {
        Schema schema =
                new Schema(required(3, "id", Types.IntegerType.get()),
                        required(5, "date", Types.StringType.get()));
        List<SlotDescriptor> slots = List.of(
                new SlotDescriptor(new SlotId(3), "id", Type.INT, true),
                new SlotDescriptor(new SlotId(5), "date", Type.STRING, true)
        );
        slots.get(0).setColumn(new Column("id", Type.INT, true));
        slots.get(1).setColumn(new Column("date", Type.STRING, true));
        var lowerBounds = Map.of(3, ByteBuffer.wrap(new byte[] {1, 0, 0, 0}),
                5, ByteBuffer.wrap("2023-01-01".getBytes()));
        var upperBounds = Map.of(3, ByteBuffer.wrap(new byte[] {10, 0, 0, 0}),
                5, ByteBuffer.wrap("2023-01-10".getBytes()));
        var valueCounts = Map.of(3, (long) 10, 5, (long) 10);

        {
            var nullValueCounts = Map.of(3, (long) 0, 5, (long) 0);
            var result =
                    IcebergUtil.parseMinMaxValueBySlots(schema, lowerBounds, upperBounds, nullValueCounts, valueCounts, slots);
            assertEquals(1, result.size());
            assertEquals(1, result.get(3).minValue);
            assertEquals(10, result.get(3).maxValue);
            assertEquals(0, result.get(3).nullValueCount);
            assertEquals(10, result.get(3).valueCount);
        }
        {
            var nullValueCounts = Map.of(3, (long) 0);
            var result =
                    IcebergUtil.parseMinMaxValueBySlots(schema, lowerBounds, upperBounds, nullValueCounts, valueCounts, slots);
            assertEquals(1, result.size());
            assertEquals(1, result.get(3).minValue);
            assertEquals(10, result.get(3).maxValue);
            assertEquals(0, result.get(3).nullValueCount);
            assertEquals(10, result.get(3).valueCount);
        }
        {
            var nullValueCounts = new HashMap<Integer, Long>();
            var result =
                    IcebergUtil.parseMinMaxValueBySlots(schema, lowerBounds, upperBounds, nullValueCounts, valueCounts, slots);
            assertEquals(0, result.size());
        }
        {
            var nullValueCounts = Map.of(3, (long) 1, 5, (long) 0);
            var result =
                    IcebergUtil.parseMinMaxValueBySlots(schema, lowerBounds, upperBounds, nullValueCounts, valueCounts, slots);
            assertEquals(1, result.size());
            assertEquals(1, result.get(3).minValue);
            assertEquals(10, result.get(3).maxValue);
            assertEquals(1, result.get(3).nullValueCount);
            assertEquals(10, result.get(3).valueCount);
        }
    }
}