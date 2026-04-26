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

package com.starrocks.common;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;

public class Int128Test {

    @Test
    public void testOfLong() {
        // Test zero
        Int128 zero = Int128.of(0L);
        Assertions.assertEquals(0L, zero.getHigh());
        Assertions.assertEquals(0L, zero.getLow());
        Assertions.assertTrue(zero.isInLong());
        Assertions.assertEquals("0", zero.toString());

        // Test positive number
        Int128 positive = Int128.of(12345L);
        Assertions.assertEquals(0L, positive.getHigh());
        Assertions.assertEquals(12345L, positive.getLow());
        Assertions.assertTrue(positive.isInLong());
        Assertions.assertEquals("12345", positive.toString());

        // Test negative number
        Int128 negative = Int128.of(-12345L);
        Assertions.assertEquals(-1L, negative.getHigh());
        Assertions.assertEquals(-12345L, negative.getLow());
        Assertions.assertTrue(negative.isInLong());
        Assertions.assertEquals("-12345", negative.toString());

        // Test Long.MAX_VALUE
        Int128 maxLong = Int128.of(Long.MAX_VALUE);
        Assertions.assertEquals(0L, maxLong.getHigh());
        Assertions.assertEquals(Long.MAX_VALUE, maxLong.getLow());
        Assertions.assertTrue(maxLong.isInLong());
        Assertions.assertEquals(String.valueOf(Long.MAX_VALUE), maxLong.toString());

        // Test Long.MIN_VALUE
        Int128 minLong = Int128.of(Long.MIN_VALUE);
        Assertions.assertEquals(-1L, minLong.getHigh());
        Assertions.assertEquals(Long.MIN_VALUE, minLong.getLow());
        Assertions.assertTrue(minLong.isInLong());
        Assertions.assertEquals(String.valueOf(Long.MIN_VALUE), minLong.toString());
    }

    @Test
    public void testOfString() {
        // Test zero
        Int128 zero = Int128.of("0");
        Assertions.assertEquals(0L, zero.getHigh());
        Assertions.assertEquals(0L, zero.getLow());
        Assertions.assertEquals("0", zero.toString());

        // Test positive number
        Int128 positive = Int128.of("123456789012345678901234567890");
        Assertions.assertEquals("123456789012345678901234567890", positive.toString());

        // Test negative number
        Int128 negative = Int128.of("-123456789012345678901234567890");
        Assertions.assertEquals("-123456789012345678901234567890", negative.toString());

        // Test large positive number close to max
        String maxValue = "170141183460469231731687303715884105727"; // 2^127 - 1
        Int128 max = Int128.of(maxValue);
        Assertions.assertEquals(maxValue, max.toString());
        Assertions.assertEquals(Long.MAX_VALUE, max.getHigh());
        Assertions.assertEquals(-1L, max.getLow());

        // Test large negative number close to min
        String minValue = "-170141183460469231731687303715884105728"; // -2^127
        Int128 min = Int128.of(minValue);
        Assertions.assertEquals(minValue, min.toString());
        Assertions.assertEquals(Long.MIN_VALUE, min.getHigh());
        Assertions.assertEquals(0L, min.getLow());
    }

    @Test
    public void testOfBigInteger() {
        // Test zero
        Int128 zero = Int128.of(BigInteger.ZERO);
        Assertions.assertEquals(0L, zero.getHigh());
        Assertions.assertEquals(0L, zero.getLow());

        // Test BigInteger.ONE
        Int128 one = Int128.of(BigInteger.ONE);
        Assertions.assertEquals(0L, one.getHigh());
        Assertions.assertEquals(1L, one.getLow());

        // Test large positive BigInteger
        BigInteger largePositive = new BigInteger("123456789012345678901234567890");
        Int128 intLargePositive = Int128.of(largePositive);
        Assertions.assertEquals(largePositive, intLargePositive.toBigInteger());

        // Test large negative BigInteger
        BigInteger largeNegative = new BigInteger("-123456789012345678901234567890");
        Int128 intLargeNegative = Int128.of(largeNegative);
        Assertions.assertEquals(largeNegative, intLargeNegative.toBigInteger());

        // Test max value
        BigInteger maxValue = BigInteger.ONE.shiftLeft(127).subtract(BigInteger.ONE);
        Int128 max = Int128.of(maxValue);
        Assertions.assertEquals(maxValue, max.toBigInteger());

        // Test min value
        BigInteger minValue = BigInteger.ONE.shiftLeft(127).negate();
        Int128 min = Int128.of(minValue);
        Assertions.assertEquals(minValue, min.toBigInteger());
    }

    @Test
    public void testOfBigIntegerOverflowPositive() {
        // Test overflow: 2^127 (too large)
        BigInteger overflow = BigInteger.ONE.shiftLeft(127);
        Assertions.assertThrows(IllegalArgumentException.class, () -> Int128.of(overflow));
    }

    @Test
    public void testOfBigIntegerOverflowNegative() {
        // Test overflow: -2^127 - 1 (too small)
        BigInteger overflow = BigInteger.ONE.shiftLeft(127).negate().subtract(BigInteger.ONE);
        Assertions.assertThrows(IllegalArgumentException.class, () -> Int128.of(overflow));
    }

    @Test
    public void testIsInLong() {
        // Numbers that fit in long
        Assertions.assertTrue(Int128.of(0L).isInLong());
        Assertions.assertTrue(Int128.of(1L).isInLong());
        Assertions.assertTrue(Int128.of(-1L).isInLong());
        Assertions.assertTrue(Int128.of(Long.MAX_VALUE).isInLong());
        Assertions.assertTrue(Int128.of(Long.MIN_VALUE).isInLong());

        // Numbers that don't fit in long
        BigInteger largeThanLongMax = BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.ONE);
        Assertions.assertFalse(Int128.of(largeThanLongMax).isInLong());

        BigInteger smallerThanLongMin = BigInteger.valueOf(Long.MIN_VALUE).subtract(BigInteger.ONE);
        Assertions.assertFalse(Int128.of(smallerThanLongMin).isInLong());

        // Edge case: exactly 2^64 - 1 (unsigned max of low, but doesn't fit in long)
        BigInteger unsignedLongMax = BigInteger.ONE.shiftLeft(64).subtract(BigInteger.ONE);
        Assertions.assertFalse(Int128.of(unsignedLongMax).isInLong());
    }

    @Test
    public void testToLong() {
        Assertions.assertEquals(0L, Int128.of(0L).toLong());
        Assertions.assertEquals(12345L, Int128.of(12345L).toLong());
        Assertions.assertEquals(-12345L, Int128.of(-12345L).toLong());
        Assertions.assertEquals(Long.MAX_VALUE, Int128.of(Long.MAX_VALUE).toLong());
        Assertions.assertEquals(Long.MIN_VALUE, Int128.of(Long.MIN_VALUE).toLong());

        // For numbers outside long range, toLong() returns the low 64 bits
        BigInteger large = BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.ONE);
        Int128 largeInt128 = Int128.of(large);
        Assertions.assertEquals(Long.MIN_VALUE, largeInt128.toLong());
    }

    @Test
    public void testToBigInteger() {
        // Test zero
        Assertions.assertEquals(BigInteger.ZERO, Int128.of(0L).toBigInteger());

        // Test positive numbers
        Assertions.assertEquals(BigInteger.valueOf(12345L), Int128.of(12345L).toBigInteger());
        Assertions.assertEquals(BigInteger.valueOf(Long.MAX_VALUE), Int128.of(Long.MAX_VALUE).toBigInteger());

        // Test negative numbers
        Assertions.assertEquals(BigInteger.valueOf(-12345L), Int128.of(-12345L).toBigInteger());
        Assertions.assertEquals(BigInteger.valueOf(Long.MIN_VALUE), Int128.of(Long.MIN_VALUE).toBigInteger());

        // Test large numbers
        BigInteger large = new BigInteger("123456789012345678901234567890");
        Assertions.assertEquals(large, Int128.of(large).toBigInteger());

        BigInteger negLarge = new BigInteger("-123456789012345678901234567890");
        Assertions.assertEquals(negLarge, Int128.of(negLarge).toBigInteger());

        // Test max and min values
        BigInteger maxValue = BigInteger.ONE.shiftLeft(127).subtract(BigInteger.ONE);
        Assertions.assertEquals(maxValue, Int128.of(maxValue).toBigInteger());

        BigInteger minValue = BigInteger.ONE.shiftLeft(127).negate();
        Assertions.assertEquals(minValue, Int128.of(minValue).toBigInteger());
    }

    @Test
    public void testToByteArray() {
        // Test zero
        Int128 zero = Int128.of(0L);
        byte[] zeroBytes = zero.toByteArray();
        Assertions.assertEquals(16, zeroBytes.length);
        for (byte b : zeroBytes) {
            Assertions.assertEquals(0, b);
        }

        // Test positive number
        Int128 positive = Int128.of(0x123456789ABCDEF0L);
        byte[] positiveBytes = positive.toByteArray();
        Assertions.assertEquals(16, positiveBytes.length);
        // Check high bytes are 0
        for (int i = 0; i < 8; i++) {
            Assertions.assertEquals(0, positiveBytes[i]);
        }
        // Check low bytes
        Assertions.assertEquals((byte) 0x12, positiveBytes[8]);
        Assertions.assertEquals((byte) 0x34, positiveBytes[9]);
        Assertions.assertEquals((byte) 0x56, positiveBytes[10]);
        Assertions.assertEquals((byte) 0x78, positiveBytes[11]);
        Assertions.assertEquals((byte) 0x9A, positiveBytes[12]);
        Assertions.assertEquals((byte) 0xBC, positiveBytes[13]);
        Assertions.assertEquals((byte) 0xDE, positiveBytes[14]);
        Assertions.assertEquals((byte) 0xF0, positiveBytes[15]);

        // Test -1 (all bits set)
        Int128 negOne = Int128.of(-1L);
        byte[] negOneBytes = negOne.toByteArray();
        Assertions.assertEquals(16, negOneBytes.length);
        for (byte b : negOneBytes) {
            Assertions.assertEquals((byte) 0xFF, b);
        }

        // Test roundtrip with BigInteger
        BigInteger original = new BigInteger("123456789012345678901234567890");
        Int128 int128 = Int128.of(original);
        BigInteger restored = new BigInteger(int128.toByteArray());
        Assertions.assertEquals(original, restored);
    }

    @Test
    public void testToString() {
        // Test zero
        Assertions.assertEquals("0", Int128.of(0L).toString());

        // Test positive numbers
        Assertions.assertEquals("1", Int128.of(1L).toString());
        Assertions.assertEquals("12345", Int128.of(12345L).toString());
        Assertions.assertEquals(String.valueOf(Long.MAX_VALUE), Int128.of(Long.MAX_VALUE).toString());

        // Test negative numbers
        Assertions.assertEquals("-1", Int128.of(-1L).toString());
        Assertions.assertEquals("-12345", Int128.of(-12345L).toString());
        Assertions.assertEquals(String.valueOf(Long.MIN_VALUE), Int128.of(Long.MIN_VALUE).toString());

        // Test large positive number
        String largePositive = "123456789012345678901234567890";
        Assertions.assertEquals(largePositive, Int128.of(largePositive).toString());

        // Test large negative number
        String largeNegative = "-123456789012345678901234567890";
        Assertions.assertEquals(largeNegative, Int128.of(largeNegative).toString());

        // Test max value
        String maxValue = "170141183460469231731687303715884105727";
        Assertions.assertEquals(maxValue, Int128.of(maxValue).toString());

        // Test min value
        String minValue = "-170141183460469231731687303715884105728";
        Assertions.assertEquals(minValue, Int128.of(minValue).toString());

        // Test unsigned long max (2^64 - 1)
        BigInteger unsignedLongMax = BigInteger.ONE.shiftLeft(64).subtract(BigInteger.ONE);
        Assertions.assertEquals("18446744073709551615", Int128.of(unsignedLongMax).toString());
    }

    @Test
    public void testCompareTo() {
        // Test equality
        Assertions.assertEquals(0, Int128.of(0L).compareTo(Int128.of(0L)));
        Assertions.assertEquals(0, Int128.of(12345L).compareTo(Int128.of(12345L)));
        Assertions.assertEquals(0, Int128.of(-12345L).compareTo(Int128.of(-12345L)));

        // Test less than
        Assertions.assertTrue(Int128.of(0L).compareTo(Int128.of(1L)) < 0);
        Assertions.assertTrue(Int128.of(-1L).compareTo(Int128.of(0L)) < 0);
        Assertions.assertTrue(Int128.of(-12345L).compareTo(Int128.of(-12344L)) < 0);
        Assertions.assertTrue(Int128.of(Long.MIN_VALUE).compareTo(Int128.of(Long.MAX_VALUE)) < 0);

        // Test greater than
        Assertions.assertTrue(Int128.of(1L).compareTo(Int128.of(0L)) > 0);
        Assertions.assertTrue(Int128.of(0L).compareTo(Int128.of(-1L)) > 0);
        Assertions.assertTrue(Int128.of(-12344L).compareTo(Int128.of(-12345L)) > 0);
        Assertions.assertTrue(Int128.of(Long.MAX_VALUE).compareTo(Int128.of(Long.MIN_VALUE)) > 0);

        // Test large numbers
        BigInteger large1 = new BigInteger("123456789012345678901234567890");
        BigInteger large2 = new BigInteger("123456789012345678901234567891");
        Assertions.assertTrue(Int128.of(large1).compareTo(Int128.of(large2)) < 0);
        Assertions.assertTrue(Int128.of(large2).compareTo(Int128.of(large1)) > 0);

        // Test negative large numbers
        BigInteger negLarge1 = new BigInteger("-123456789012345678901234567890");
        BigInteger negLarge2 = new BigInteger("-123456789012345678901234567891");
        Assertions.assertTrue(Int128.of(negLarge1).compareTo(Int128.of(negLarge2)) > 0);
        Assertions.assertTrue(Int128.of(negLarge2).compareTo(Int128.of(negLarge1)) < 0);

        // Test max and min values
        BigInteger maxValue = BigInteger.ONE.shiftLeft(127).subtract(BigInteger.ONE);
        BigInteger minValue = BigInteger.ONE.shiftLeft(127).negate();
        Assertions.assertTrue(Int128.of(minValue).compareTo(Int128.of(maxValue)) < 0);
        Assertions.assertTrue(Int128.of(maxValue).compareTo(Int128.of(minValue)) > 0);

        // Test comparison with different high values
        BigInteger val1 = BigInteger.valueOf(Long.MAX_VALUE);
        BigInteger val2 = BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.ONE);
        Assertions.assertTrue(Int128.of(val1).compareTo(Int128.of(val2)) < 0);

        // Test unsigned comparison of low when high is equal
        Int128 a = Int128.of(BigInteger.ONE.shiftLeft(64)); // high=1, low=0
        Int128 b = Int128.of(BigInteger.ONE.shiftLeft(64).add(BigInteger.ONE)); // high=1, low=1
        Assertions.assertTrue(a.compareTo(b) < 0);
    }

    @Test
    public void testEquals() {
        // Test equality with same values
        Int128 a = Int128.of(12345L);
        Int128 b = Int128.of(12345L);
        Assertions.assertEquals(a, b);
        Assertions.assertEquals(b, a);

        // Test equality with zero
        Assertions.assertEquals(Int128.of(0L), Int128.of(0L));

        // Test equality with negative numbers
        Assertions.assertEquals(Int128.of(-12345L), Int128.of(-12345L));

        // Test equality with large numbers
        BigInteger large = new BigInteger("123456789012345678901234567890");
        Assertions.assertEquals(Int128.of(large), Int128.of(large));

        // Test inequality
        Assertions.assertNotEquals(Int128.of(0L), Int128.of(1L));
        Assertions.assertNotEquals(Int128.of(-1L), Int128.of(1L));
        Assertions.assertNotEquals(Int128.of(12345L), Int128.of(12346L));

        // Test inequality with large numbers
        BigInteger large1 = new BigInteger("123456789012345678901234567890");
        BigInteger large2 = new BigInteger("123456789012345678901234567891");
        Assertions.assertNotEquals(Int128.of(large1), Int128.of(large2));

        // Test with null
        Assertions.assertNotEquals(Int128.of(0L), null);

        // Test with different type
        Assertions.assertNotEquals(Int128.of(0L), BigInteger.ZERO);
    }

    @Test
    public void testHashCode() {
        // Test same values have same hash code
        Int128 a = Int128.of(12345L);
        Int128 b = Int128.of(12345L);
        Assertions.assertEquals(a.hashCode(), b.hashCode());

        // Test zero
        Int128 zero1 = Int128.of(0L);
        Int128 zero2 = Int128.of(0L);
        Assertions.assertEquals(zero1.hashCode(), zero2.hashCode());

        // Test large numbers
        BigInteger large = new BigInteger("123456789012345678901234567890");
        Int128 large1 = Int128.of(large);
        Int128 large2 = Int128.of(large);
        Assertions.assertEquals(large1.hashCode(), large2.hashCode());

        // Note: Different values may have different hash codes (not guaranteed, but
        // likely)
        // This is not a strict requirement, but good practice
        Int128 val1 = Int128.of(1L);
        Int128 val2 = Int128.of(2L);
        // We don't assert inequality here because hash collisions are allowed
    }

    @Test
    public void testEdgeCases() {
        // Test boundary between positive and negative in low word
        BigInteger justAboveLongMax = BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.ONE);
        Int128 val = Int128.of(justAboveLongMax);
        Assertions.assertEquals(0L, val.getHigh());
        Assertions.assertEquals(Long.MIN_VALUE, val.getLow());
        Assertions.assertFalse(val.isInLong());

        // Test 2^64 - 1 (max unsigned long, but positive Int128)
        BigInteger unsignedLongMax = BigInteger.ONE.shiftLeft(64).subtract(BigInteger.ONE);
        Int128 val2 = Int128.of(unsignedLongMax);
        Assertions.assertEquals(0L, val2.getHigh());
        Assertions.assertEquals(-1L, val2.getLow());
        Assertions.assertFalse(val2.isInLong());
        Assertions.assertEquals("18446744073709551615", val2.toString());

        // Test 2^64 (requires high word)
        BigInteger twoToThe64 = BigInteger.ONE.shiftLeft(64);
        Int128 val3 = Int128.of(twoToThe64);
        Assertions.assertEquals(1L, val3.getHigh());
        Assertions.assertEquals(0L, val3.getLow());
        Assertions.assertFalse(val3.isInLong());

        // Test -2^64
        BigInteger negTwoToThe64 = BigInteger.ONE.shiftLeft(64).negate();
        Int128 val4 = Int128.of(negTwoToThe64);
        Assertions.assertEquals(-1L, val4.getHigh());
        Assertions.assertEquals(0L, val4.getLow());
        Assertions.assertFalse(val4.isInLong());
    }

    @Test
    public void testRoundTrip() {
        // Test round-trip: long -> Int128 -> long
        long[] longValues = { 0L, 1L, -1L, Long.MAX_VALUE, Long.MIN_VALUE, 12345L, -67890L };
        for (long val : longValues) {
            Int128 int128 = Int128.of(val);
            Assertions.assertEquals(val, int128.toLong());
        }

        // Test round-trip: BigInteger -> Int128 -> BigInteger (for values in range)
        String[] stringValues = {
                "0",
                "1",
                "-1",
                "123456789012345678901234567890",
                "-123456789012345678901234567890",
                "170141183460469231731687303715884105727", // max
                "-170141183460469231731687303715884105728" // min
        };
        for (String strVal : stringValues) {
            BigInteger big = new BigInteger(strVal);
            Int128 int128 = Int128.of(big);
            Assertions.assertEquals(big, int128.toBigInteger());
            Assertions.assertEquals(strVal, int128.toString());
        }

        // Test round-trip: String -> Int128 -> String
        for (String strVal : stringValues) {
            Int128 int128 = Int128.of(strVal);
            Assertions.assertEquals(strVal, int128.toString());
        }
    }
}
