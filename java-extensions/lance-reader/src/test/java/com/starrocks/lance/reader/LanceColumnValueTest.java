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
import org.apache.arrow.vector.DateMilliVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.LargeVarBinaryVector;
import org.apache.arrow.vector.LargeVarCharVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.TimeStampNanoVector;
import org.apache.arrow.vector.TimeStampSecVector;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.UInt1Vector;
import org.apache.arrow.vector.UInt2Vector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;

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
    public void testGetDate64AcrossEpoch() {
        try (DateMilliVector vector = new DateMilliVector("date64", allocator)) {
            long[] millis = {0, 86_400_000L, -86_400_000L, -1, 1_705_276_800_000L};
            LocalDate[] dates = {LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 2),
                    LocalDate.of(1969, 12, 31), LocalDate.of(1969, 12, 31), LocalDate.of(2024, 1, 15)};
            vector.allocateNew(millis.length);
            for (int i = 0; i < millis.length; i++) {
                vector.setSafe(i, millis[i]);
            }
            vector.setValueCount(millis.length);
            for (int i = 0; i < millis.length; i++) {
                assertEquals(dates[i], new LanceColumnValue(vector, i).getDate());
            }
        }
    }

    @Test
    public void testGetDateTime() {
        LocalDateTime expected = LocalDateTime.of(2024, 1, 15, 10, 30, 0, 123_000_000);
        assertTimestamp(expected.withNano(0), new TimeStampSecVector("seconds", allocator), 1);
        assertTimestamp(expected, new TimeStampMilliVector("millis", allocator), 1_000);
        assertTimestamp(expected, new TimeStampMicroVector("micros", allocator), 1_000_000);
        assertTimestamp(expected, new TimeStampNanoVector("nanos", allocator), 1_000_000_000);
    }

    @Test
    public void testUnpackList() {
        try (ListVector vector = ListVector.empty("list", allocator)) {
            vector.addOrGetVector(FieldType.nullable(new ArrowType.Int(32, true)));
            vector.allocateNew();
            IntVector data = (IntVector) vector.getDataVector();
            int start = vector.startNewValue(0);
            data.setSafe(start, 10);
            data.setSafe(start + 1, 20);
            vector.endValue(0, 2);
            vector.setValueCount(1);

            List<com.starrocks.jni.connector.ColumnValue> values = new ArrayList<>();
            new LanceColumnValue(vector, 0).unpackArray(values);

            assertEquals(2, values.size());
            assertEquals(10, values.get(0).getInt());
            assertEquals(20, values.get(1).getInt());
        }
    }

    @Test
    public void testUnpackFixedSizeList() {
        try (FixedSizeListVector vector = FixedSizeListVector.empty("vector", 2, allocator)) {
            vector.addOrGetVector(FieldType.nullable(
                    new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)));
            vector.allocateNew();
            Float4Vector data = (Float4Vector) vector.getDataVector();
            vector.setNotNull(0);
            data.setSafe(0, 1.5f);
            data.setSafe(1, 2.5f);
            vector.setValueCount(1);

            List<com.starrocks.jni.connector.ColumnValue> values = new ArrayList<>();
            new LanceColumnValue(vector, 0).unpackArray(values);

            assertEquals(2, values.size());
            assertEquals(1.5f, values.get(0).getFloat());
            assertEquals(2.5f, values.get(1).getFloat());
        }
    }

    @Test
    public void testUnsigned8Boundaries() {
        try (UInt1Vector vector = new UInt1Vector("uint8", allocator)) {
            vector.allocateNew(2);
            vector.setSafe(0, 0);
            vector.setSafe(1, 255);
            vector.setValueCount(2);
            assertEquals((short) 0, new LanceColumnValue(vector, 0).getShort());
            assertEquals((short) 255, new LanceColumnValue(vector, 1).getShort());
        }
    }

    @Test
    public void testUnsigned16Boundaries() {
        try (UInt2Vector vector = new UInt2Vector("uint16", allocator)) {
            vector.allocateNew(2);
            vector.setSafe(0, 0);
            vector.setSafe(1, 65535);
            vector.setValueCount(2);
            assertEquals(0, new LanceColumnValue(vector, 0).getInt());
            assertEquals(65535, new LanceColumnValue(vector, 1).getInt());
        }
    }

    @Test
    public void testUnsigned32Boundaries() {
        try (UInt4Vector vector = new UInt4Vector("uint32", allocator)) {
            vector.allocateNew(2);
            vector.setSafe(0, 0);
            vector.setSafe(1, -1);
            vector.setValueCount(2);
            assertEquals(0L, new LanceColumnValue(vector, 0).getLong());
            assertEquals(4294967295L, new LanceColumnValue(vector, 1).getLong());
        }
    }

    @Test
    public void testUnsigned64Boundaries() {
        try (UInt8Vector vector = new UInt8Vector("uint64", allocator)) {
            vector.allocateNew(2);
            vector.setSafe(0, 0);
            vector.setSafe(1, -1L);
            vector.setValueCount(2);
            assertEquals(BigDecimal.ZERO, new LanceColumnValue(vector, 0).getDecimal());
            assertEquals(new BigDecimal("18446744073709551615"), new LanceColumnValue(vector, 1).getDecimal());
        }
    }

    @Test
    public void testLargeStringAndBinary() {
        try (LargeVarCharVector text = new LargeVarCharVector("text", allocator);
                LargeVarBinaryVector binary = new LargeVarBinaryVector("binary", allocator)) {
            String unicode = "Lance 中文 🌍";
            byte[] bytes = {0, (byte) 255, 42};
            text.allocateNew();
            binary.allocateNew();
            text.setSafe(0, unicode.getBytes(StandardCharsets.UTF_8));
            binary.setSafe(0, bytes);
            text.setValueCount(1);
            binary.setValueCount(1);
            assertEquals(unicode, new LanceColumnValue(text, 0).getString(ColumnType.TypeValue.STRING));
            assertArrayEquals(bytes, new LanceColumnValue(binary, 0).getBytes());
        }
    }

    private void assertTimestamp(LocalDateTime expected, TimeStampVector vector, long unitsPerSecond) {
        try (TimeStampVector closeableVector = vector) {
            long value = expected.toInstant(ZoneOffset.UTC).getEpochSecond() * unitsPerSecond;
            if (unitsPerSecond > 1) {
                value += expected.getNano() / (1_000_000_000 / unitsPerSecond);
            }
            closeableVector.allocateNew(1);
            closeableVector.setSafe(0, value);
            closeableVector.setValueCount(1);

            assertEquals(expected,
                    new LanceColumnValue(closeableVector, 0).getDateTime(ColumnType.TypeValue.DATETIME));
        }
    }
}
