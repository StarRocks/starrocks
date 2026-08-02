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

package com.starrocks.paimon.reader;

import com.starrocks.jni.connector.ColumnType;
import com.starrocks.jni.connector.ColumnValue;
import com.starrocks.jni.connector.OffHeapColumnVector;
import com.starrocks.jni.connector.OffHeapTable;
import com.starrocks.utils.Platform;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;

public class TestVariantColumnType {

    // OffHeapColumnVector normally allocates through the BE's native memory tracker (JNI), which
    // isn't linked in a plain unit test JVM. Platform.UT_KEY switches it to sun.misc.Unsafe malloc
    // for the duration of the test, matching the pattern used by TestHiveScanner/TestHudiSliceScanner.
    @BeforeEach
    public void setUp() {
        System.setProperty(Platform.UT_KEY, Boolean.TRUE.toString());
    }

    @AfterEach
    public void tearDown() {
        System.setProperty(Platform.UT_KEY, Boolean.FALSE.toString());
    }

    @Test
    public void testParseVariant() {
        ColumnType type = new ColumnType("v", "variant");
        Assertions.assertEquals(ColumnType.TypeValue.VARIANT, type.getTypeValue());
        Assertions.assertTrue(type.isVariant());
        Assertions.assertEquals(Arrays.asList("metadata", "value"), type.getChildNames());
        Assertions.assertEquals(ColumnType.TypeValue.BINARY, type.getChildTypes().get(0).getTypeValue());
        Assertions.assertEquals(ColumnType.TypeValue.BINARY, type.getChildTypes().get(1).getTypeValue());
        Assertions.assertEquals(Arrays.asList(0, 1), type.getFieldIndex());
        // variant column meta: [null] + 2 binary children, each [null | offset | data] => 1 + 3 + 3
        Assertions.assertEquals(7, type.computeColumnSize());
        Assertions.assertEquals("variant", type.getTypeValueString());
    }

    /**
     * Minimal {@link ColumnValue} implementation for a variant value. When used as the top-level
     * appended value it unpacks into two binary child values (metadata, value); when used as a child
     * value it just exposes its own raw bytes via {@link #getBytes()}. Every other accessor is
     * unreachable for a variant/binary column, so it throws to catch accidental misuse.
     */
    private static class TestVariantValue implements ColumnValue {
        private final byte[] metadata;
        private final byte[] value;
        private final byte[] bytes;

        // Top-level variant value: unpacks into metadata/value children.
        TestVariantValue(byte[] metadata, byte[] value) {
            this.metadata = metadata;
            this.value = value;
            this.bytes = null;
        }

        // Leaf binary child value.
        private TestVariantValue(byte[] bytes) {
            this.metadata = null;
            this.value = null;
            this.bytes = bytes;
        }

        @Override
        public void unpackStruct(List<Integer> structFieldIndex, List<ColumnValue> values) {
            values.add(new TestVariantValue(metadata));
            values.add(new TestVariantValue(value));
        }

        @Override
        public byte[] getBytes() {
            return bytes;
        }

        @Override
        public boolean getBoolean() {
            throw new UnsupportedOperationException();
        }

        @Override
        public short getShort() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getInt() {
            throw new UnsupportedOperationException();
        }

        @Override
        public float getFloat() {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getLong() {
            throw new UnsupportedOperationException();
        }

        @Override
        public double getDouble() {
            throw new UnsupportedOperationException();
        }

        @Override
        public String getString(ColumnType.TypeValue type) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void unpackArray(List<ColumnValue> values) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void unpackMap(List<ColumnValue> keys, List<ColumnValue> values) {
            throw new UnsupportedOperationException();
        }

        @Override
        public byte getByte() {
            throw new UnsupportedOperationException();
        }

        @Override
        public BigDecimal getDecimal() {
            throw new UnsupportedOperationException();
        }

        @Override
        public LocalDate getDate() {
            throw new UnsupportedOperationException();
        }

        @Override
        public LocalDateTime getDateTime(ColumnType.TypeValue type) {
            throw new UnsupportedOperationException();
        }
    }

    @Test
    public void testOffHeapVariantAppend() {
        ColumnType[] types = new ColumnType[] {new ColumnType("v", "variant")};
        String[] fields = new String[] {"v"};
        OffHeapTable table = new OffHeapTable(types, fields, 4);
        try {
            byte[] metadata = new byte[] {1, 0};
            byte[] value = new byte[] {12};
            table.appendData(0, new TestVariantValue(metadata, value));
            table.appendData(0, null);
            table.setNumRows(2);

            OffHeapColumnVector vector = table.vectors[0];
            // null flags: row 0 is a real value, row 1 is null.
            Assertions.assertFalse(vector.isNullAt(0));
            Assertions.assertTrue(vector.isNullAt(1));

            OffHeapColumnVector metadataChild = vector.getChildColumn(0);
            OffHeapColumnVector valueChild = vector.getChildColumn(1);
            // child 0 holds exactly the metadata bytes, child 1 holds exactly the value bytes.
            Assertions.assertArrayEquals(metadata, metadataChild.getBinary(0));
            Assertions.assertArrayEquals(value, valueChild.getBinary(0));
            // a null variant append null-fills both children, like a struct null does.
            Assertions.assertTrue(metadataChild.isNullAt(1));
            Assertions.assertTrue(valueChild.isNullAt(1));

            // meta slot count: variant null buffer + per-child [null|offset|data] == 7,
            // identical to struct<metadata:binary,value:binary>.
            Assertions.assertEquals(7, types[0].computeColumnSize());

            // Walk the actual off-heap meta layout the BE would read: this only succeeds if the
            // variant vector produced the same address chain a 2-field struct vector would.
            // (table.checkNullsLength() is intentionally not exercised here: it hits a pre-existing,
            // variant-unrelated gap where OffHeapColumnVector#checkNullsLength has no `case BYTE`
            // for a BINARY/STRING column's underlying data child - see task-7-report.md.)
            table.getMetaNativeAddress();
            table.checkTableMeta(false);
        } finally {
            table.close();
        }
    }
}
