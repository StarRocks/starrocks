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

import org.apache.iceberg.PartitionData;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

public class IcebergPartitionsTableScannerTest {
    private static final Schema SCHEMA =
            new Schema(Types.NestedField.optional(1, "ts", Types.TimestampType.withZone()));

    private static IcebergPartitionsTableScanner scannerFor(PartitionSpec spec) throws Exception {
        Map<String, String> params = new HashMap<>();
        params.put("required_fields", "partition_value");
        params.put("metadata_column_names", "partition_value");
        params.put("metadata_column_types", "struct<ts:datetime>");
        params.put("serialized_table", "");
        params.put("split_info", "");
        IcebergPartitionsTableScanner scanner = new IcebergPartitionsTableScanner(4096, params);

        // doOpen() needs a real deserialized table, so seed the state it would have produced instead.
        setField(scanner, "schema", SCHEMA);
        setField(scanner, "partitionFields", spec.fields());
        setField(scanner, "reusedRecord", GenericRecord.create(spec.partitionType()));
        return scanner;
    }

    private static void setField(Object target, String name, Object value) throws Exception {
        Field field = IcebergPartitionsTableScanner.class.getDeclaredField(name);
        field.setAccessible(true);
        field.set(target, value);
    }

    private static Object partitionValuesOf(IcebergPartitionsTableScanner scanner, PartitionData data) throws Exception {
        Method method = IcebergPartitionsTableScanner.class
                .getDeclaredMethod("getPartitionValues", PartitionData.class);
        method.setAccessible(true);
        return method.invoke(scanner, data);
    }

    @Test
    public void testIdentityTimestampTzPartitionValues() throws Exception {
        PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).identity("ts").build();
        PartitionData data = new PartitionData(spec.partitionType());

        // A NULL timestamptz partition value must stay NULL. Unboxing it blows up before the value
        // ever reaches the column vector.
        data.set(0, null);
        GenericRecord nullRecord = (GenericRecord) partitionValuesOf(scannerFor(spec), data);
        Assertions.assertNull(nullRecord.getField("ts"));

        // A non-null timestamptz partition value is still converted from micros to millis.
        data.set(0, 1786000000123456L);
        GenericRecord record = (GenericRecord) partitionValuesOf(scannerFor(spec), data);
        Assertions.assertEquals(1786000000123L, record.getField("ts"));
    }
}
