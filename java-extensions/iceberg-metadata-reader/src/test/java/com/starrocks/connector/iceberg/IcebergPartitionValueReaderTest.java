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

import com.starrocks.jni.connector.ColumnType;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.time.LocalDateTime;
import java.util.UUID;

public class IcebergPartitionValueReaderTest {
    private static final Schema SCHEMA =
            new Schema(Types.NestedField.optional(1, "ts", Types.TimestampType.withZone()));

    private static GenericRecord partitionValuesOf(PartitionSpec spec, PartitionData data) {
        return new IcebergPartitionValueReader(spec.fields(), SCHEMA).read(data);
    }

    @Test
    public void testIdentityTimestampTzPartitionValues() throws Exception {
        PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).identity("ts").build();
        PartitionData data = new PartitionData(spec.partitionType());

        // A NULL timestamptz partition value must stay NULL. Unboxing it blows up before the value
        // ever reaches the column vector.
        data.set(0, null);
        GenericRecord nullRecord = partitionValuesOf(spec, data);
        Assertions.assertNull(nullRecord.getField("ts"));

        // A non-null timestamptz partition value is still converted from micros to millis.
        data.set(0, 1786000000123456L);
        GenericRecord record = partitionValuesOf(spec, data);
        Assertions.assertEquals(1786000000123L, record.getField("ts"));
    }

    @Test
    public void testIdentityTimestampBinaryAndUuidPartitionValues() {
        Schema schema = new Schema(
                Types.NestedField.optional(1, "ts", Types.TimestampType.withoutZone()),
                Types.NestedField.optional(2, "b", Types.BinaryType.get()),
                Types.NestedField.optional(3, "u", Types.UUIDType.get()));
        PartitionSpec spec = PartitionSpec.builderFor(schema).identity("ts").identity("b").identity("u").build();
        PartitionData data = new PartitionData(spec.partitionType());
        UUID uuid = UUID.fromString("0f8fad5b-d9cb-469f-a165-70867728950e");
        // 1970-01-02 00:00:00.000001 as micros
        data.set(0, 86400000001L);
        data.set(1, ByteBuffer.wrap(new byte[] {1, 2, 3}));
        data.set(2, uuid);

        GenericRecord record = new IcebergPartitionValueReader(spec.fields(), schema).read(data);
        // a timestamp without zone is a wall-clock time, not shifted by the session time zone
        Assertions.assertEquals(LocalDateTime.of(1970, 1, 2, 0, 0, 0, 1000), record.getField("ts"));
        Assertions.assertEquals(LocalDateTime.of(1970, 1, 2, 0, 0, 0, 1000),
                new IcebergMetadataColumnValue(record.getField("ts"), "Asia/Shanghai")
                        .getDateTime(ColumnType.TypeValue.DATETIME));
        Assertions.assertArrayEquals(new byte[] {1, 2, 3}, (byte[]) record.getField("b"));
        Assertions.assertEquals(uuid.toString(), record.getField("u"));
    }
}
