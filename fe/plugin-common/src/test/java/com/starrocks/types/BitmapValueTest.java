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
    static BitmapValue mediumBitmap;
    static BitmapValue largeBitmap;

    @BeforeClass
    public static void beforeClass() throws Exception {
        emptyBitmap = new BitmapValue();
        singleBitmap = new BitmapValue();
        mediumBitmap = new BitmapValue();
        largeBitmap = new BitmapValue();

        singleBitmap.add(1);
        for (long i = 0; i < 10; i++) {
            mediumBitmap.add(i);
        }
        for (long i = 0; i < 40; i++) {
            largeBitmap.add(i);
        }
    }

    private void check_bitmap(BitmapValue bitmap, int bitmapType, long start, long end) {
        Assert.assertEquals(bitmap.getBitmapType(), bitmapType);
        Assert.assertEquals(bitmap.cardinality(), end - start);
        for (long i = start; i < end; i++) {
            Assert.assertTrue(bitmap.contains(i));
        }
    }

    private void check_bitmap(BitmapValue bitmap, int bitmapType, long start1, long end1, long start2, long end2) {
        Assert.assertEquals(bitmap.getBitmapType(), bitmapType);
        Assert.assertEquals(bitmap.cardinality(), (end1 - start1) + (end2 - start2));
        for (long i = start1; i < end1; i++) {
            Assert.assertTrue(bitmap.contains(i));
        }
        for (long i = start2; i < end2; i++) {
            Assert.assertTrue(bitmap.contains(i));
        }
    }

    @Test
    public void testBitmapToBytesAndBitmapFromBytes() throws IOException {
        BitmapValue bitmap = BitmapValue.bitmapFromBytes(BitmapValue.bitmapToBytes(emptyBitmap));
        check_bitmap(bitmap, BitmapValue.EMPTY, 0, 0);

        bitmap = BitmapValue.bitmapFromBytes(BitmapValue.bitmapToBytes(singleBitmap));
        check_bitmap(bitmap, BitmapValue.SINGLE_VALUE, 1, 2);

        bitmap = BitmapValue.bitmapFromBytes(BitmapValue.bitmapToBytes(mediumBitmap));
        check_bitmap(bitmap, BitmapValue.SET_VALUE, 0, 10);

        bitmap = BitmapValue.bitmapFromBytes(BitmapValue.bitmapToBytes(largeBitmap));
        check_bitmap(bitmap, BitmapValue.BITMAP_VALUE, 0, 40);
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

        bitmapValue.add(0);
        assertEquals(BitmapValue.SINGLE_VALUE, bitmapValue.getBitmapType());

        bitmapValue.add(1);
        assertEquals(BitmapValue.SET_VALUE, bitmapValue.getBitmapType());

        for (long i = 2; i < 32; i++) {
            bitmapValue.add(i);
            assertEquals(BitmapValue.SET_VALUE, bitmapValue.getBitmapType());
        }
        bitmapValue.add(32);
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
        check_bitmap(bitmapValue1, BitmapValue.SET_VALUE, 0, 10);

        // test add long
        BitmapValue bitmapValue2 = new BitmapValue();
        for (long i = Long.MAX_VALUE - 1; i > Long.MAX_VALUE - 11; i--) {
            bitmapValue2.add(i);
        }
        check_bitmap(bitmapValue2, BitmapValue.SET_VALUE, Long.MAX_VALUE - 10, Long.MAX_VALUE);

        // test add int and long
        for (int i = 0; i < 10; i++) {
            bitmapValue2.add(i);
        }
        check_bitmap(bitmapValue2, BitmapValue.SET_VALUE, 0, 10, Long.MAX_VALUE - 10, Long.MAX_VALUE);

        // test distinct
        BitmapValue bitmapValue = new BitmapValue();
        bitmapValue.add(1);
        bitmapValue.add(1);
        check_bitmap(bitmapValue, BitmapValue.SINGLE_VALUE, 1, 2);
    }

    @Test
    public void testBitmapValueAnd() throws IOException {
        // empty and empty
        BitmapValue bitmap = new BitmapValue(emptyBitmap);
        bitmap.and(emptyBitmap);
        check_bitmap(bitmap, BitmapValue.EMPTY, 0, 0);

        // empty and single
        bitmap = new BitmapValue(emptyBitmap);
        bitmap.and(singleBitmap);
        check_bitmap(bitmap, BitmapValue.EMPTY, 0, 0);

        // empty and set
        bitmap = new BitmapValue(emptyBitmap);
        bitmap.and(mediumBitmap);
        check_bitmap(bitmap, BitmapValue.EMPTY, 0, 0);

        // empty and bitmap
        bitmap = new BitmapValue(emptyBitmap);
        bitmap.and(largeBitmap);
        check_bitmap(bitmap, BitmapValue.EMPTY, 0, 0);

        // single and empty
        bitmap = new BitmapValue(singleBitmap);
        bitmap.and(emptyBitmap);
        check_bitmap(bitmap, BitmapValue.EMPTY, 0, 0);

        // single and single (equal)
        bitmap = new BitmapValue(singleBitmap);
        bitmap.and(new BitmapValue(2));
        check_bitmap(bitmap, BitmapValue.EMPTY, 0, 0);

        // single and single (not equal)
        bitmap = new BitmapValue(singleBitmap);
        bitmap.and(new BitmapValue(1));
        check_bitmap(bitmap, BitmapValue.SINGLE_VALUE, 1, 2);

        // single and bitmap (not contains)
        bitmap = new BitmapValue(singleBitmap);
        bitmap.and(new BitmapValue(2, 4));
        check_bitmap(bitmap, BitmapValue.EMPTY, 0, 0);

        // single and bitmap (contains)
        bitmap = new BitmapValue(singleBitmap);
        bitmap.and(largeBitmap);
        check_bitmap(bitmap, BitmapValue.SINGLE_VALUE, 1, 2);

        // single and set (not contains)
        bitmap = new BitmapValue(singleBitmap);
        bitmap.and(new BitmapValue(100, 101));
        check_bitmap(bitmap, BitmapValue.EMPTY, 0, 0);

        // single and set (contains)
        bitmap = new BitmapValue(singleBitmap);
        bitmap.and(mediumBitmap);
        check_bitmap(bitmap, BitmapValue.SINGLE_VALUE, 1, 2);

        // bitmap and empty
        bitmap = new BitmapValue(largeBitmap);
        bitmap.and(emptyBitmap);
        check_bitmap(bitmap, BitmapValue.EMPTY, 0, 0);

        // bitmap and single (contains)
        bitmap = new BitmapValue(largeBitmap);
        bitmap.and(singleBitmap);
        check_bitmap(bitmap, BitmapValue.SINGLE_VALUE, 1, 2);

        // bitmap and single (not contains)
        bitmap = new BitmapValue(largeBitmap);
        bitmap.and(new BitmapValue(1000));
        check_bitmap(bitmap, BitmapValue.EMPTY, 0, 0);

        // bitmap and bitmap (-> bitmap)
        bitmap = new BitmapValue(largeBitmap);
        bitmap.and(new BitmapValue(20, 60));
        check_bitmap(bitmap, BitmapValue.BITMAP_VALUE, 20, 40);

        // bitmap and bitmap (-> empty)
        bitmap = new BitmapValue(largeBitmap);
        bitmap.and(new BitmapValue(100, 180));
        check_bitmap(bitmap, BitmapValue.EMPTY, 0, 0);

        // bitmap and bitmap (-> single)
        bitmap = new BitmapValue(largeBitmap);
        bitmap.and(new BitmapValue(39, 100));
        check_bitmap(bitmap, BitmapValue.SINGLE_VALUE, 39, 40);

        // bitmap and set (->set)
        bitmap = new BitmapValue(largeBitmap);
        bitmap.and(mediumBitmap);
        check_bitmap(bitmap, BitmapValue.SET_VALUE, 0, 10);

        // bitmap and set (->empty)
        bitmap = new BitmapValue(largeBitmap);
        bitmap.and(new BitmapValue(100, 120));
        check_bitmap(bitmap, BitmapValue.SET_VALUE, 0, 0);

        // bitmap and set (->single)
        bitmap = new BitmapValue(largeBitmap);
        bitmap.and(new BitmapValue(30, 50));
        check_bitmap(bitmap, BitmapValue.SET_VALUE, 30, 40);

        // set and empty
        bitmap = new BitmapValue(mediumBitmap);
        bitmap.and(emptyBitmap);
        check_bitmap(bitmap, BitmapValue.EMPTY, 0, 0);

        // set and single (contains)
        bitmap = new BitmapValue(mediumBitmap);
        bitmap.and(singleBitmap);
        check_bitmap(bitmap, BitmapValue.SINGLE_VALUE, 1, 2);

        // set and single (not contains)
        bitmap = new BitmapValue(mediumBitmap);
        bitmap.and(new BitmapValue(100));
        check_bitmap(bitmap, BitmapValue.EMPTY, 0, 0);

        // set and set
        bitmap = new BitmapValue(mediumBitmap);
        bitmap.and(new BitmapValue(5, 20));
        check_bitmap(bitmap, BitmapValue.SET_VALUE, 5, 10);
    }

    @Test
    public void testBitmapValueOr() throws IOException {
        // empty or empty
        BitmapValue bitmap = new BitmapValue(emptyBitmap);
        bitmap.or(emptyBitmap);
        check_bitmap(bitmap, BitmapValue.EMPTY, 0, 0);

        // empty or single
        bitmap = new BitmapValue(emptyBitmap);
        bitmap.or(singleBitmap);
        check_bitmap(bitmap, BitmapValue.SINGLE_VALUE, 1, 2);

        // empty or set
        bitmap = new BitmapValue(emptyBitmap);
        bitmap.or(mediumBitmap);
        check_bitmap(bitmap, BitmapValue.SET_VALUE, 0, 10);

        // empty or bitmap
        bitmap = new BitmapValue(emptyBitmap);
        bitmap.or(largeBitmap);
        check_bitmap(bitmap, BitmapValue.BITMAP_VALUE, 0, 40);

        // single or empty
        bitmap = new BitmapValue(singleBitmap);
        bitmap.or(emptyBitmap);
        check_bitmap(bitmap, BitmapValue.SINGLE_VALUE, 1, 2);

        // single or single (equal)
        bitmap = new BitmapValue(singleBitmap);
        bitmap.or(singleBitmap);
        check_bitmap(bitmap, BitmapValue.SINGLE_VALUE, 1, 2);

        // single or single (not equal)
        bitmap = new BitmapValue(singleBitmap);
        bitmap.or(new BitmapValue(2));
        check_bitmap(bitmap, BitmapValue.SET_VALUE, 1, 3);

        // single or bitmap
        bitmap = new BitmapValue(singleBitmap);
        bitmap.or(new BitmapValue(10, 70));
        check_bitmap(bitmap, BitmapValue.BITMAP_VALUE, 1, 2, 10, 70);

        // single or set (->set)
        bitmap = new BitmapValue(singleBitmap);
        bitmap.or(new BitmapValue(10, 20));
        check_bitmap(bitmap, BitmapValue.SET_VALUE, 1, 2, 10, 20);

        // single or set (->bitmap)
        bitmap = new BitmapValue(singleBitmap);
        bitmap.or(new BitmapValue(10, 42));
        check_bitmap(bitmap, BitmapValue.BITMAP_VALUE, 1, 2, 10, 42);

        // single or set (->set)
        bitmap = new BitmapValue(singleBitmap);
        bitmap.or(new BitmapValue(5, 10));
        check_bitmap(bitmap, BitmapValue.SET_VALUE, 1, 2, 5, 10);

        // bitmap or empty
        bitmap = new BitmapValue(largeBitmap);
        bitmap.or(emptyBitmap);
        check_bitmap(bitmap, BitmapValue.BITMAP_VALUE, 0, 40);

        // bitmap or single
        bitmap = new BitmapValue(largeBitmap);
        bitmap.or(new BitmapValue(100));
        check_bitmap(bitmap, BitmapValue.BITMAP_VALUE, 0, 40, 100, 101);

        // bitmap or bitmap
        bitmap = new BitmapValue(largeBitmap);
        bitmap.or(new BitmapValue(30, 80));
        check_bitmap(bitmap, BitmapValue.BITMAP_VALUE, 0, 80);

        // bitmap or set
        bitmap = new BitmapValue(largeBitmap);
        bitmap.or(new BitmapValue(30, 50));
        check_bitmap(bitmap, BitmapValue.BITMAP_VALUE, 0, 50);

        // set or empty
        bitmap = new BitmapValue(mediumBitmap);
        bitmap.or(emptyBitmap);
        check_bitmap(bitmap, BitmapValue.SET_VALUE, 0, 10);

        // set or single (->map)
        bitmap = new BitmapValue(0, 32);
        bitmap.or(new BitmapValue(32));
        check_bitmap(bitmap, BitmapValue.BITMAP_VALUE, 0, 33);

        // set or single (->set)
        bitmap = new BitmapValue(mediumBitmap);
        bitmap.or(new BitmapValue(10));
        check_bitmap(bitmap, BitmapValue.SET_VALUE, 0, 11);

        // set or bitmap
        bitmap = new BitmapValue(mediumBitmap);
        bitmap.or(new BitmapValue(8, 100));
        check_bitmap(bitmap, BitmapValue.BITMAP_VALUE, 0, 100);

        // set or set
        bitmap = new BitmapValue(mediumBitmap);
        bitmap.or(new BitmapValue(3, 15));
        check_bitmap(bitmap, BitmapValue.SET_VALUE, 0, 15);
    }

    @Test
    public void testBitmapValueSerializeAndDeserialize() throws IOException {
        // empty
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        DataOutput output = new DataOutputStream(outputStream);
        emptyBitmap.serialize(output);
        Assert.assertEquals("[0]", Arrays.toString(outputStream.toByteArray()));

        DataInputStream inputStream = new DataInputStream(new ByteArrayInputStream(outputStream.toByteArray()));
        BitmapValue outputBitmap = new BitmapValue();
        outputBitmap.deserialize(inputStream);
        assertEquals(emptyBitmap, outputBitmap);

        // single value (uint32)
        BitmapValue inputBitmap = new BitmapValue((long) Integer.MAX_VALUE + 1);
        outputStream = new ByteArrayOutputStream();
        output = new DataOutputStream(outputStream);
        inputBitmap.serialize(output);
        // check serialize by little endian
        Assert.assertEquals("[1, 0, 0, 0, -128]", Arrays.toString(outputStream.toByteArray()));

        inputStream = new DataInputStream(new ByteArrayInputStream(outputStream.toByteArray()));
        outputBitmap = new BitmapValue();
        outputBitmap.deserialize(inputStream);
        assertEquals(inputBitmap, outputBitmap);

        // unsigned 64-bit
        inputBitmap = new BitmapValue();
        long unsigned64bit = 4294967297L; // 2^32 + 1
        inputBitmap.add(unsigned64bit);
        outputStream = new ByteArrayOutputStream();
        output = new DataOutputStream(outputStream);
        inputBitmap.serialize(output);
        // check serialize by little endian
        Assert.assertEquals("[3, 1, 0, 0, 0, 1, 0, 0, 0]", Arrays.toString(outputStream.toByteArray()));

        inputStream = new DataInputStream(new ByteArrayInputStream(outputStream.toByteArray()));
        outputBitmap = new BitmapValue();
        outputBitmap.deserialize(inputStream);
        assertEquals(inputBitmap, outputBitmap);

        // set
        inputBitmap = new BitmapValue(1, 3);
        outputStream = new ByteArrayOutputStream();
        output = new DataOutputStream(outputStream);
        inputBitmap.serialize(output);
        Assert.assertEquals("[10, 2, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0]",
                Arrays.toString(outputStream.toByteArray()));

        // bitmap
        // case 1 : 32-bit bitmap
        inputBitmap = new BitmapValue(0, 10);
        outputStream = new ByteArrayOutputStream();
        output = new DataOutputStream(outputStream);
        inputBitmap.serialize(output);

        inputStream = new DataInputStream(new ByteArrayInputStream(outputStream.toByteArray()));
        outputBitmap = new BitmapValue();
        outputBitmap.deserialize(inputStream);

        assertEquals(inputBitmap, outputBitmap);

        // bitmap
        // case 2 : 64-bit bitmap
        inputBitmap = new BitmapValue(Long.MAX_VALUE - 9, Long.MAX_VALUE);
        inputBitmap.add(Long.MAX_VALUE);
        outputStream = new ByteArrayOutputStream();
        output = new DataOutputStream(outputStream);
        inputBitmap.serialize(output);

        inputStream = new DataInputStream(new ByteArrayInputStream(outputStream.toByteArray()));
        outputBitmap = new BitmapValue();
        outputBitmap.deserialize(inputStream);

        assertEquals(inputBitmap, outputBitmap);
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
        Assert.assertFalse(emptyBitmap.contains(1));

        // single value
        Assert.assertTrue(singleBitmap.contains(1));
        Assert.assertFalse(singleBitmap.contains(2));

        // bitmap
        Assert.assertTrue(largeBitmap.contains(1));
        Assert.assertFalse(largeBitmap.contains(100));

        // set
        Assert.assertTrue(mediumBitmap.contains(1));
        Assert.assertFalse(mediumBitmap.contains(20));
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
        Assert.assertEquals(mediumBitmap.toString(), "{0,1,2,3,4,5,6,7,8,9}");
        Assert.assertEquals(largeBitmap.toString(), "{0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22," +
                "23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39}");
    }

    @Test
    public void testSerializeToString() {
        Assert.assertEquals(emptyBitmap.serializeToString(), "");
        Assert.assertEquals(singleBitmap.serializeToString(), "1");
        Assert.assertEquals(mediumBitmap.serializeToString(), "0,1,2,3,4,5,6,7,8,9");
        Assert.assertEquals(largeBitmap.serializeToString(), "0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20," +
                "21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39");
    }
}
