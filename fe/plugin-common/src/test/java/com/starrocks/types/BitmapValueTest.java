// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.types;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;

public class BitmapValueTest {
    static BitmapValue emptyBitmap;
    static BitmapValue singleBitmap;
    static BitmapValue largeBitmap;

    @BeforeClass
    public static void beforeClass() throws Exception {
        emptyBitmap = new BitmapValue();
        singleBitmap = new BitmapValue();
        singleBitmap.add(1);
        largeBitmap = new BitmapValue();
        for (long i = 0; i < 20; i++) {
            largeBitmap.add(i);
        }
    }

    private void check_bitmap(BitmapValue bitmap, long start, long end) {
        Assert.assertEquals(bitmap.cardinality(), end - start);
        for (long i = start; i < end; i++) {
            Assert.assertTrue(bitmap.contains(i));
        }
    }

    @Test
    public void testBitmapToBytesAndBitmapFromBytes() throws IOException {
        BitmapValue bitmap = BitmapValue.bitmapFromBytes(BitmapValue.bitmapToBytes(emptyBitmap));
        check_bitmap(bitmap, 0, 0);

        bitmap = BitmapValue.bitmapFromBytes(BitmapValue.bitmapToBytes(singleBitmap));
        check_bitmap(bitmap, 1, 2);

        bitmap = BitmapValue.bitmapFromBytes(BitmapValue.bitmapToBytes(largeBitmap));
        check_bitmap(bitmap, 0, 20);
    }

    @Test
    public void testVarint64IntEncode() throws IOException {
        long[] sourceValue = {0, 1000, Integer.MAX_VALUE, Long.MAX_VALUE};
        for (long value : sourceValue) {
            ByteArrayOutputStream byteArrayOutput = new ByteArrayOutputStream();
            DataOutput output = new DataOutputStream(byteArrayOutput);
            Codec.encodeVarint64(value, output);
            assertEquals(value, Codec.decodeVarint64(
                    new DataInputStream(new ByteArrayInputStream(byteArrayOutput.toByteArray()))));
        }
    }

    @Test
    public void testBitmapTypeTransfer() {
        BitmapValue bitmapValue = new BitmapValue();
        assertEquals(BitmapValue.EMPTY, bitmapValue.getBitmapType());

        bitmapValue.add(1);
        assertEquals(BitmapValue.SINGLE_VALUE, bitmapValue.getBitmapType());

        bitmapValue.add(2);
        assertEquals(BitmapValue.BITMAP_VALUE, bitmapValue.getBitmapType());

        bitmapValue.clear();
        assertEquals(BitmapValue.EMPTY, bitmapValue.getBitmapType());
    }

    @Test
    public void testBitmapValueAdd() {
        // test add int
        BitmapValue bitmapValue1 = new BitmapValue();
        for (int i = 0; i < 10; i++) {
            bitmapValue1.add(i);
        }
        for (int i = 0; i < 10; i++) {
            Assert.assertTrue(bitmapValue1.contains(i));
        }
        Assert.assertFalse(bitmapValue1.contains(11));

        // test add long
        BitmapValue bitmapValue2 = new BitmapValue();
        for (long i = Long.MAX_VALUE; i > Long.MAX_VALUE - 10; i--) {
            bitmapValue2.add(i);
        }
        for (long i = Long.MAX_VALUE; i > Long.MAX_VALUE - 10; i--) {
            Assert.assertTrue(bitmapValue2.contains(i));
        }
        Assert.assertFalse(bitmapValue2.contains(0));

        // test add int and long
        for (int i = 0; i < 10; i++) {
            bitmapValue2.add(i);
        }

        for (long i = Long.MAX_VALUE; i > Long.MAX_VALUE - 10; i--) {
            Assert.assertTrue(bitmapValue2.contains(i));
        }
        for (int i = 0; i < 10; i++) {
            Assert.assertTrue(bitmapValue2.contains(i));
        }
        Assert.assertFalse(bitmapValue2.contains(100));

        // test distinct
        BitmapValue bitmapValue = new BitmapValue();
        bitmapValue.add(1);
        bitmapValue.add(1);
        assertEquals(BitmapValue.SINGLE_VALUE, bitmapValue.getBitmapType());
        assertEquals(1, bitmapValue.cardinality());
    }

    @Test
    public void testBitmapValueAnd() {
        // empty and empty
        BitmapValue bitmapValue1 = new BitmapValue();
        BitmapValue bitmapValue1_1 = new BitmapValue();
        bitmapValue1.and(bitmapValue1_1);
        assertEquals(BitmapValue.EMPTY, bitmapValue1.getBitmapType());
        assertEquals(0, bitmapValue1.cardinality());

        // empty and single value
        BitmapValue bitmapValue2 = new BitmapValue();
        BitmapValue bitmapValue2_1 = new BitmapValue();
        bitmapValue2_1.add(1);
        bitmapValue2.and(bitmapValue2_1);
        assertEquals(BitmapValue.EMPTY, bitmapValue2.getBitmapType());
        assertEquals(0, bitmapValue2.cardinality());

        // empty and bitmap
        BitmapValue bitmapValue3 = new BitmapValue();
        BitmapValue bitmapValue3_1 = new BitmapValue();
        bitmapValue3_1.add(1);
        bitmapValue3_1.add(2);
        bitmapValue3.and(bitmapValue3_1);
        assertEquals(BitmapValue.EMPTY, bitmapValue2.getBitmapType());
        assertEquals(0, bitmapValue3.cardinality());

        // single value and empty
        BitmapValue bitmapValue4 = new BitmapValue();
        bitmapValue4.add(1);
        BitmapValue bitmapValue4_1 = new BitmapValue();
        bitmapValue4.and(bitmapValue4_1);
        assertEquals(BitmapValue.EMPTY, bitmapValue4.getBitmapType());
        assertEquals(0, bitmapValue4.cardinality());

        // single value and single value
        BitmapValue bitmapValue5 = new BitmapValue();
        bitmapValue5.add(1);
        BitmapValue bitmapValue5_1 = new BitmapValue();
        bitmapValue5_1.add(1);
        bitmapValue5.and(bitmapValue5_1);
        assertEquals(BitmapValue.SINGLE_VALUE, bitmapValue5.getBitmapType());
        Assert.assertTrue(bitmapValue5.contains(1));

        bitmapValue5.clear();
        bitmapValue5_1.clear();
        bitmapValue5.add(1);
        bitmapValue5_1.add(2);
        bitmapValue5.and(bitmapValue5_1);
        assertEquals(BitmapValue.EMPTY, bitmapValue5.getBitmapType());

        // single value and bitmap
        BitmapValue bitmapValue6 = new BitmapValue();
        bitmapValue6.add(1);
        BitmapValue bitmapValue6_1 = new BitmapValue();
        bitmapValue6_1.add(1);
        bitmapValue6_1.add(2);
        bitmapValue6.and(bitmapValue6_1);
        assertEquals(BitmapValue.SINGLE_VALUE, bitmapValue6.getBitmapType());

        bitmapValue6.clear();
        bitmapValue6.add(3);
        bitmapValue6.and(bitmapValue6_1);
        assertEquals(BitmapValue.EMPTY, bitmapValue6.getBitmapType());

        // bitmap and empty
        BitmapValue bitmapValue7 = new BitmapValue();
        bitmapValue7.add(1);
        bitmapValue7.add(2);
        BitmapValue bitmapValue7_1 = new BitmapValue();
        bitmapValue7.and(bitmapValue7_1);
        assertEquals(BitmapValue.EMPTY, bitmapValue7.getBitmapType());

        // bitmap and single value
        BitmapValue bitmapValue8 = new BitmapValue();
        bitmapValue8.add(1);
        bitmapValue8.add(2);
        BitmapValue bitmapValue8_1 = new BitmapValue();
        bitmapValue8_1.add(1);
        bitmapValue8.and(bitmapValue8_1);
        assertEquals(BitmapValue.SINGLE_VALUE, bitmapValue8.getBitmapType());

        bitmapValue8.clear();
        bitmapValue8.add(2);
        bitmapValue8.add(3);
        bitmapValue8.and(bitmapValue8_1);
        assertEquals(BitmapValue.EMPTY, bitmapValue8.getBitmapType());

        // bitmap and bitmap
        BitmapValue bitmapValue9 = new BitmapValue();
        bitmapValue9.add(1);
        bitmapValue9.add(2);
        BitmapValue bitmapValue9_1 = new BitmapValue();
        bitmapValue9_1.add(2);
        bitmapValue9_1.add(3);
        bitmapValue9.and(bitmapValue9_1);
        assertEquals(BitmapValue.SINGLE_VALUE, bitmapValue9.getBitmapType());

        bitmapValue9.clear();
        bitmapValue9.add(4);
        bitmapValue9.add(5);
        bitmapValue9.and(bitmapValue9_1);
        assertEquals(BitmapValue.EMPTY, bitmapValue9.getBitmapType());

        bitmapValue9.clear();
        bitmapValue9.add(2);
        bitmapValue9.add(3);
        bitmapValue9.add(4);
        bitmapValue9.and(bitmapValue9_1);
        assertEquals(BitmapValue.BITMAP_VALUE, bitmapValue9.getBitmapType());
        assertEquals(bitmapValue9, bitmapValue9_1);

    }

    @Test
    public void testBitmapValueOr() {
        // empty or empty
        BitmapValue bitmapValue1 = new BitmapValue();
        BitmapValue bitmapValue1_1 = new BitmapValue();
        bitmapValue1.or(bitmapValue1_1);
        assertEquals(BitmapValue.EMPTY, bitmapValue1.getBitmapType());

        // empty or single value
        BitmapValue bitmapValue2 = new BitmapValue();
        BitmapValue bitmapValue2_1 = new BitmapValue();
        bitmapValue2_1.add(1);
        bitmapValue2.or(bitmapValue2_1);
        assertEquals(BitmapValue.SINGLE_VALUE, bitmapValue2.getBitmapType());

        // empty or bitmap
        BitmapValue bitmapValue3 = new BitmapValue();
        BitmapValue bitmapValue3_1 = new BitmapValue();
        bitmapValue3_1.add(1);
        bitmapValue3_1.add(2);
        bitmapValue3.or(bitmapValue3_1);
        assertEquals(BitmapValue.BITMAP_VALUE, bitmapValue3.getBitmapType());

        // single or and empty
        BitmapValue bitmapValue4 = new BitmapValue();
        BitmapValue bitmapValue4_1 = new BitmapValue();
        bitmapValue4.add(1);
        bitmapValue4.or(bitmapValue4_1);
        assertEquals(BitmapValue.SINGLE_VALUE, bitmapValue4.getBitmapType());

        // single or and single value
        BitmapValue bitmapValue5 = new BitmapValue();
        BitmapValue bitmapValue5_1 = new BitmapValue();
        bitmapValue5.add(1);
        bitmapValue5_1.add(1);
        bitmapValue5.or(bitmapValue5_1);
        assertEquals(BitmapValue.SINGLE_VALUE, bitmapValue5.getBitmapType());

        bitmapValue5.clear();
        bitmapValue5.add(2);
        bitmapValue5.or(bitmapValue5_1);
        assertEquals(BitmapValue.BITMAP_VALUE, bitmapValue5.getBitmapType());

        // single or and bitmap
        BitmapValue bitmapValue6 = new BitmapValue();
        BitmapValue bitmapValue6_1 = new BitmapValue();
        bitmapValue6.add(1);
        bitmapValue6_1.add(1);
        bitmapValue6_1.add(2);
        bitmapValue6.or(bitmapValue6_1);
        assertEquals(BitmapValue.BITMAP_VALUE, bitmapValue6.getBitmapType());

        // bitmap or empty
        BitmapValue bitmapValue7 = new BitmapValue();
        bitmapValue7.add(1);
        bitmapValue7.add(2);
        BitmapValue bitmapValue7_1 = new BitmapValue();
        bitmapValue7.or(bitmapValue7_1);
        assertEquals(BitmapValue.BITMAP_VALUE, bitmapValue7.getBitmapType());

        // bitmap or single value
        BitmapValue bitmapValue8 = new BitmapValue();
        bitmapValue8.add(1);
        bitmapValue8.add(2);
        BitmapValue bitmapValue8_1 = new BitmapValue();
        bitmapValue8_1.add(1);
        bitmapValue8.or(bitmapValue8_1);
        assertEquals(BitmapValue.BITMAP_VALUE, bitmapValue8.getBitmapType());

        // bitmap or bitmap
        BitmapValue bitmapValue9 = new BitmapValue();
        bitmapValue9.add(1);
        bitmapValue9.add(2);
        BitmapValue bitmapValue9_1 = new BitmapValue();
        bitmapValue9.or(bitmapValue9_1);
        assertEquals(BitmapValue.BITMAP_VALUE, bitmapValue9.getBitmapType());
    }

    @Test
    public void testBitmapValueSerializeAndDeserialize() throws IOException {
        // empty
        BitmapValue serializeBitmapValue = new BitmapValue();
        ByteArrayOutputStream emptyOutputStream = new ByteArrayOutputStream();
        DataOutput output = new DataOutputStream(emptyOutputStream);
        serializeBitmapValue.serialize(output);

        DataInputStream emptyInputStream =
                new DataInputStream(new ByteArrayInputStream(emptyOutputStream.toByteArray()));
        BitmapValue deserializeBitmapValue = new BitmapValue();
        deserializeBitmapValue.deserialize(emptyInputStream);

        assertEquals(serializeBitmapValue, deserializeBitmapValue);

        // single value
        BitmapValue serializeSingleValueBitmapValue = new BitmapValue();
        // unsigned 32-bit
        long unsigned32bit = Integer.MAX_VALUE;
        serializeSingleValueBitmapValue.add(unsigned32bit + 1);
        ByteArrayOutputStream singleValueOutputStream = new ByteArrayOutputStream();
        DataOutput singleValueOutput = new DataOutputStream(singleValueOutputStream);
        serializeSingleValueBitmapValue.serialize(singleValueOutput);
        // check serialize by little endian
        Assert.assertEquals("[1, 0, 0, 0, -128]", Arrays.toString(singleValueOutputStream.toByteArray()));

        DataInputStream singleValueInputStream =
                new DataInputStream(new ByteArrayInputStream(singleValueOutputStream.toByteArray()));
        BitmapValue deserializeSingleValueBitmapValue = new BitmapValue();
        deserializeSingleValueBitmapValue.deserialize(singleValueInputStream);
        assertEquals(serializeSingleValueBitmapValue, deserializeSingleValueBitmapValue);

        // unsigned 64-bit
        serializeSingleValueBitmapValue = new BitmapValue();
        long unsigned64bit = 4294967297L; // 2^32 + 1
        serializeSingleValueBitmapValue.add(unsigned64bit);
        singleValueOutputStream = new ByteArrayOutputStream();
        singleValueOutput = new DataOutputStream(singleValueOutputStream);
        serializeSingleValueBitmapValue.serialize(singleValueOutput);
        // check serialize by little endian
        Assert.assertEquals("[3, 1, 0, 0, 0, 1, 0, 0, 0]", Arrays.toString(singleValueOutputStream.toByteArray()));

        singleValueInputStream = new DataInputStream(new ByteArrayInputStream(singleValueOutputStream.toByteArray()));
        deserializeSingleValueBitmapValue = new BitmapValue();
        deserializeSingleValueBitmapValue.deserialize(singleValueInputStream);
        assertEquals(serializeSingleValueBitmapValue, deserializeSingleValueBitmapValue);

        // bitmap
        // case 1 : 32-bit bitmap
        BitmapValue serializeBitmapBitmapValue = new BitmapValue();
        for (int i = 0; i < 10; i++) {
            serializeBitmapBitmapValue.add(i);
        }
        ByteArrayOutputStream bitmapOutputStream = new ByteArrayOutputStream();
        DataOutput bitmapOutput = new DataOutputStream(bitmapOutputStream);
        serializeBitmapBitmapValue.serialize(bitmapOutput);

        DataInputStream bitmapInputStream =
                new DataInputStream(new ByteArrayInputStream(bitmapOutputStream.toByteArray()));
        BitmapValue deserializeBitmapBitmapValue = new BitmapValue();
        deserializeBitmapBitmapValue.deserialize(bitmapInputStream);

        assertEquals(serializeBitmapBitmapValue, deserializeBitmapBitmapValue);

        // bitmap
        // case 2 : 64-bit bitmap
        BitmapValue serializeBitmapBitmapValue64 = new BitmapValue();
        for (long i = Long.MAX_VALUE; i > Long.MAX_VALUE - 10; i--) {
            serializeBitmapBitmapValue64.add(i);
        }
        ByteArrayOutputStream bitmapOutputStream64 = new ByteArrayOutputStream();
        DataOutput bitmapOutput64 = new DataOutputStream(bitmapOutputStream64);
        serializeBitmapBitmapValue64.serialize(bitmapOutput64);

        DataInputStream bitmapInputStream64 =
                new DataInputStream(new ByteArrayInputStream(bitmapOutputStream64.toByteArray()));
        BitmapValue deserializeBitmapBitmapValue64 = new BitmapValue();
        deserializeBitmapBitmapValue64.deserialize(bitmapInputStream64);

        assertEquals(serializeBitmapBitmapValue64, deserializeBitmapBitmapValue64);
    }

    @Test
    public void testIs32BitsEnough() {
        BitmapValue bitmapValue = new BitmapValue();
        bitmapValue.add(0);
        bitmapValue.add(2);
        bitmapValue.add(Integer.MAX_VALUE);
        // unsigned 32-bit
        long unsigned32bit = Integer.MAX_VALUE;
        bitmapValue.add(unsigned32bit + 1);

        Assert.assertTrue(bitmapValue.is32BitsEnough());

        bitmapValue.add(Long.MAX_VALUE);
        Assert.assertFalse(bitmapValue.is32BitsEnough());
    }

    @Test
    public void testCardinality() {
        BitmapValue bitmapValue = new BitmapValue();

        assertEquals(0, bitmapValue.cardinality());

        bitmapValue.add(0);
        bitmapValue.add(0);
        bitmapValue.add(-1);
        bitmapValue.add(-1);
        bitmapValue.add(Integer.MAX_VALUE);
        bitmapValue.add(Integer.MAX_VALUE);
        bitmapValue.add(-Integer.MAX_VALUE);
        bitmapValue.add(-Integer.MAX_VALUE);
        bitmapValue.add(Long.MAX_VALUE);
        bitmapValue.add(Long.MAX_VALUE);
        bitmapValue.add(-Long.MAX_VALUE);
        bitmapValue.add(-Long.MAX_VALUE);

        assertEquals(6, bitmapValue.cardinality());
    }

    @Test
    public void testContains() {
        // empty
        BitmapValue bitmapValue = new BitmapValue();
        Assert.assertFalse(bitmapValue.contains(1));

        // single value
        bitmapValue.add(1);
        Assert.assertTrue(bitmapValue.contains(1));
        Assert.assertFalse(bitmapValue.contains(2));

        // bitmap
        bitmapValue.add(2);
        Assert.assertTrue(bitmapValue.contains(1));
        Assert.assertTrue(bitmapValue.contains(2));
        Assert.assertFalse(bitmapValue.contains(12));
    }

    @Test
    public void testEqual() {
        // empty == empty
        BitmapValue emp1 = new BitmapValue();
        BitmapValue emp2 = new BitmapValue();
        assertEquals(emp1, emp2);
        // empty == single value
        emp2.add(1);
        Assert.assertNotEquals(emp1, emp2);
        // empty == bitmap
        emp2.add(2);
        Assert.assertNotEquals(emp1, emp2);

        // single value = empty
        BitmapValue sgv = new BitmapValue();
        sgv.add(1);
        BitmapValue emp3 = new BitmapValue();
        Assert.assertNotEquals(sgv, emp3);
        // single value = single value
        BitmapValue sgv1 = new BitmapValue();
        sgv1.add(1);
        BitmapValue sgv2 = new BitmapValue();
        sgv2.add(2);
        assertEquals(sgv, sgv1);
        Assert.assertNotEquals(sgv, sgv2);
        // single value = bitmap
        sgv2.add(3);
        Assert.assertNotEquals(sgv, sgv2);

        // bitmap == empty
        BitmapValue bitmapValue = new BitmapValue();
        bitmapValue.add(1);
        bitmapValue.add(2);
        BitmapValue emp4 = new BitmapValue();
        Assert.assertNotEquals(bitmapValue, emp4);
        // bitmap == singlevalue
        BitmapValue sgv3 = new BitmapValue();
        sgv3.add(1);
        Assert.assertNotEquals(bitmapValue, sgv3);
        // bitmap == bitmap
        BitmapValue bitmapValue1 = new BitmapValue();
        bitmapValue1.add(1);
        BitmapValue bitmapValue2 = new BitmapValue();
        bitmapValue2.add(1);
        bitmapValue2.add(2);
        assertEquals(bitmapValue, bitmapValue2);
        Assert.assertNotEquals(bitmapValue, bitmapValue1);
    }

    @Test
    public void testToString() {
        Assert.assertEquals(emptyBitmap.toString(), "{}");
        Assert.assertEquals(singleBitmap.toString(), "{1}");
        Assert.assertEquals(largeBitmap.toString(), "{0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19}");
    }

    @Test
    public void testSerializeToString() {
        Assert.assertEquals(emptyBitmap.serializeToString(), "");
        Assert.assertEquals(singleBitmap.serializeToString(), "1");
        Assert.assertEquals(largeBitmap.serializeToString(), "0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19");
    }
}
