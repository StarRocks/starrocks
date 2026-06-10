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

package com.starrocks.lance.reader;

import com.starrocks.jni.connector.ColumnType;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class LanceColumnValueTest {

    private BufferAllocator allocator;

    @BeforeEach
    public void setUp() {
        allocator = new RootAllocator(Long.MAX_VALUE);
    }

    @AfterEach
    public void tearDown() {
        allocator.close();
    }

    @Test
    public void testGetBoolean() {
        try (BitVector vector = new BitVector("bool", allocator)) {
            vector.allocateNew(2);
            vector.setSafe(0, 1);
            vector.setSafe(1, 0);
            vector.setValueCount(2);

            assertTrue(new LanceColumnValue(vector, 0).getBoolean());
            assertFalse(new LanceColumnValue(vector, 1).getBoolean());
        }
    }

    @Test
    public void testGetByte() {
        try (TinyIntVector vector = new TinyIntVector("tinyint", allocator)) {
            vector.allocateNew(1);
            vector.setSafe(0, 42);
            vector.setValueCount(1);

            assertEquals((byte) 42, new LanceColumnValue(vector, 0).getByte());
        }
    }

    @Test
    public void testGetShort() {
        try (SmallIntVector vector = new SmallIntVector("smallint", allocator)) {
            vector.allocateNew(1);
            vector.setSafe(0, 1234);
            vector.setValueCount(1);

            assertEquals((short) 1234, new LanceColumnValue(vector, 0).getShort());
        }
    }

    @Test
    public void testGetInt() {
        try (IntVector vector = new IntVector("int", allocator)) {
            vector.allocateNew(1);
            vector.setSafe(0, 123456);
            vector.setValueCount(1);

            assertEquals(123456, new LanceColumnValue(vector, 0).getInt());
        }
    }

    @Test
    public void testGetLong() {
        try (BigIntVector vector = new BigIntVector("bigint", allocator)) {
            vector.allocateNew(1);
            vector.setSafe(0, 9876543210L);
            vector.setValueCount(1);

            assertEquals(9876543210L, new LanceColumnValue(vector, 0).getLong());
        }
    }

    @Test
    public void testGetFloat() {
        try (Float4Vector vector = new Float4Vector("float", allocator)) {
            vector.allocateNew(1);
            vector.setSafe(0, 3.14f);
            vector.setValueCount(1);

            assertEquals(3.14f, new LanceColumnValue(vector, 0).getFloat(), 0.001f);
        }
    }

    @Test
    public void testGetDouble() {
        try (Float8Vector vector = new Float8Vector("double", allocator)) {
            vector.allocateNew(1);
            vector.setSafe(0, 2.718281828);
            vector.setValueCount(1);

            assertEquals(2.718281828, new LanceColumnValue(vector, 0).getDouble(), 0.0000001);
        }
    }

    @Test
    public void testGetString() {
        try (VarCharVector vector = new VarCharVector("varchar", allocator)) {
            vector.allocateNew(1);
            vector.setSafe(0, "hello lance".getBytes(StandardCharsets.UTF_8));
            vector.setValueCount(1);

            assertEquals("hello lance",
                    new LanceColumnValue(vector, 0).getString(ColumnType.TypeValue.STRING));
        }
    }

    @Test
    public void testGetBytes() {
        try (VarBinaryVector vector = new VarBinaryVector("binary", allocator)) {
            byte[] data = {0x01, 0x02, 0x03};
            vector.allocateNew(1);
            vector.setSafe(0, data);
            vector.setValueCount(1);

            assertArrayEquals(data, new LanceColumnValue(vector, 0).getBytes());
        }
    }

    @Test
    public void testGetDate() {
        try (DateDayVector vector = new DateDayVector("date", allocator)) {
            // 2024-01-15 is day 19737 since epoch
            int daysSinceEpoch = (int) LocalDate.of(2024, 1, 15).toEpochDay();
            vector.allocateNew(1);
            vector.setSafe(0, daysSinceEpoch);
            vector.setValueCount(1);

            assertEquals(LocalDate.of(2024, 1, 15),
                    new LanceColumnValue(vector, 0).getDate());
        }
    }

    @Test
    public void testGetDateTime() {
        try (TimeStampMicroVector vector = new TimeStampMicroVector("timestamp", allocator)) {
            // 2024-01-15T10:30:00 in micros since epoch
            LocalDateTime expected = LocalDateTime.of(2024, 1, 15, 10, 30, 0);
            long micros = expected.toInstant(ZoneOffset.UTC).getEpochSecond() * 1_000_000;
            vector.allocateNew(1);
            vector.setSafe(0, micros);
            vector.setValueCount(1);

            assertEquals(expected,
                    new LanceColumnValue(vector, 0).getDateTime(ColumnType.TypeValue.DATETIME));
        }
    }
}
