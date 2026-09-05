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

package com.starrocks.udf;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class UDFHelperDecimalTest {

    private static ByteBuffer littleEndian(int capacity) {
        return ByteBuffer.allocate(capacity).order(ByteOrder.LITTLE_ENDIAN);
    }

    private static ByteBuffer nullFlags(byte... flags) {
        ByteBuffer buf = ByteBuffer.allocate(flags.length);
        buf.put(flags);
        buf.flip();
        return buf;
    }

    // DECIMAL32: stored as int32 (4B, little-endian) with implicit scale.
    @Test
    public void testCreateBoxedDecimal32NonNullable() {
        ByteBuffer data = littleEndian(3 * 4);
        data.putInt(12345);   // 123.45 at scale 2
        data.putInt(-6789);   // -67.89
        data.putInt(0);       // 0.00
        data.flip();

        Object[] result = UDFHelper.createBoxedDecimalArray(UDFHelper.TYPE_DECIMAL32, 3, 2, null, data);
        Assertions.assertEquals(3, result.length);
        Assertions.assertEquals(new BigDecimal("123.45"), result[0]);
        Assertions.assertEquals(new BigDecimal("-67.89"), result[1]);
        Assertions.assertEquals(new BigDecimal("0.00"), result[2]);
    }

    @Test
    public void testCreateBoxedDecimal32Nullable() {
        ByteBuffer data = littleEndian(2 * 4);
        data.putInt(100);
        data.putInt(200);
        data.flip();
        ByteBuffer nulls = nullFlags((byte) 0, (byte) 1);

        Object[] result = UDFHelper.createBoxedDecimalArray(UDFHelper.TYPE_DECIMAL32, 2, 1, nulls, data);
        Assertions.assertEquals(new BigDecimal("10.0"), result[0]);
        Assertions.assertNull(result[1]);
    }

    // DECIMAL64: stored as int64 (8B, little-endian).
    @Test
    public void testCreateBoxedDecimal64Range() {
        ByteBuffer data = littleEndian(3 * 8);
        data.putLong(9_223_372_036_854_775_807L);  // INT64_MAX
        data.putLong(-9_223_372_036_854_775_808L); // INT64_MIN
        data.putLong(0L);
        data.flip();

        Object[] result = UDFHelper.createBoxedDecimalArray(UDFHelper.TYPE_DECIMAL64, 3, 3, null, data);
        Assertions.assertEquals(new BigDecimal("9223372036854775.807"), result[0]);
        Assertions.assertEquals(new BigDecimal("-9223372036854775.808"), result[1]);
        Assertions.assertEquals(new BigDecimal("0.000"), result[2]);
    }

    // DECIMAL128: stored as 16 little-endian bytes; sign is the MSB of the top byte.
    @Test
    public void testCreateBoxedDecimal128Positive() {
        BigInteger unscaled = new BigInteger("12345678901234567890123456789"); // positive, < 2^127
        ByteBuffer data = writeInt128LE(unscaled);

        Object[] result = UDFHelper.createBoxedDecimalArray(UDFHelper.TYPE_DECIMAL128, 1, 5, null, data);
        Assertions.assertEquals(new BigDecimal(unscaled, 5), result[0]);
    }

    @Test
    public void testCreateBoxedDecimal128Negative() {
        BigInteger unscaled = new BigInteger("-42"); // small negative
        ByteBuffer data = writeInt128LE(unscaled);

        Object[] result = UDFHelper.createBoxedDecimalArray(UDFHelper.TYPE_DECIMAL128, 1, 0, null, data);
        Assertions.assertEquals(new BigDecimal(unscaled, 0), result[0]);
    }

    @Test
    public void testCreateBoxedDecimal128MultiRowWithNulls() {
        BigInteger a = new BigInteger("10000000000");
        BigInteger b = new BigInteger("-20000000000");
        ByteBuffer data = ByteBuffer.allocate(3 * 16);
        putInt128LE(data, a);
        putInt128LE(data, BigInteger.ZERO); // will be nulled out
        putInt128LE(data, b);
        data.flip();

        ByteBuffer nulls = nullFlags((byte) 0, (byte) 1, (byte) 0);
        Object[] result = UDFHelper.createBoxedDecimalArray(UDFHelper.TYPE_DECIMAL128, 3, 4, nulls, data);
        Assertions.assertEquals(new BigDecimal(a, 4), result[0]);
        Assertions.assertNull(result[1]);
        Assertions.assertEquals(new BigDecimal(b, 4), result[2]);
    }

    // DECIMAL256: 32 little-endian bytes. BE layout is {uint128 low; int128 high}, which
    // is byte-equivalent to a little-endian int256.
    @Test
    public void testCreateBoxedDecimal256WideValue() {
        // 2^200 - a value that requires > 128 bits and is positive.
        BigInteger unscaled = BigInteger.ONE.shiftLeft(200);
        ByteBuffer data = writeInt256LE(unscaled);

        Object[] result = UDFHelper.createBoxedDecimalArray(UDFHelper.TYPE_DECIMAL256, 1, 10, null, data);
        Assertions.assertEquals(new BigDecimal(unscaled, 10), result[0]);
    }

    @Test
    public void testCreateBoxedDecimal256NegativeWide() {
        BigInteger unscaled = BigInteger.ONE.shiftLeft(190).negate();
        ByteBuffer data = writeInt256LE(unscaled);

        Object[] result = UDFHelper.createBoxedDecimalArray(UDFHelper.TYPE_DECIMAL256, 1, 0, null, data);
        Assertions.assertEquals(new BigDecimal(unscaled, 0), result[0]);
    }

    @Test
    public void testCreateBoxedDecimal256WithNulls() {
        BigInteger a = BigInteger.ONE.shiftLeft(130);
        ByteBuffer data = ByteBuffer.allocate(2 * 32);
        putInt256LE(data, a);
        putInt256LE(data, BigInteger.ZERO);
        data.flip();
        ByteBuffer nulls = nullFlags((byte) 0, (byte) 1);

        Object[] result = UDFHelper.createBoxedDecimalArray(UDFHelper.TYPE_DECIMAL256, 2, 8, nulls, data);
        Assertions.assertEquals(new BigDecimal(a, 8), result[0]);
        Assertions.assertNull(result[1]);
    }

    // --- overflow / precision check ------------------------------------

    // In-range values rescale and pass the precision check.
    @Test
    public void testRescaleAndCheckInRange() {
        BigInteger limit = BigInteger.TEN.pow(5);
        BigInteger unscaled = UDFHelper.rescaleAndCheck(new BigDecimal("123.45"), 5, 2, limit);
        Assertions.assertEquals(new BigInteger("12345"), unscaled);
        // Values rounded HALF_UP to fit the target scale are accepted.
        BigInteger rounded = UDFHelper.rescaleAndCheck(new BigDecimal("1.235"), 5, 2, limit);
        Assertions.assertEquals(new BigInteger("124"), rounded);
    }

    // Value exceeds 10^precision after rescaling -> ArithmeticException.
    @Test
    public void testRescaleAndCheckPrecisionOverflow() {
        BigInteger limit = BigInteger.TEN.pow(5);
        Assertions.assertThrows(ArithmeticException.class,
                () -> UDFHelper.rescaleAndCheck(new BigDecimal("1000.00"), 5, 2, limit));
        Assertions.assertThrows(ArithmeticException.class,
                () -> UDFHelper.rescaleAndCheck(new BigDecimal("-999999.99"), 5, 2, limit));
    }

    // Rounding HALF_UP can push a value into overflow if it sits right on the boundary.
    @Test
    public void testRescaleAndCheckRoundingCanOverflow() {
        BigInteger limit = BigInteger.TEN.pow(3); // DECIMAL(3, 0): max 999
        Assertions.assertThrows(ArithmeticException.class,
                () -> UDFHelper.rescaleAndCheck(new BigDecimal("999.5"), 3, 0, limit));
    }

    // --- helpers --------------------------------------------------------

    // Serialise a BigInteger as a 16-byte little-endian signed integer (int128).
    private static ByteBuffer writeInt128LE(BigInteger value) {
        ByteBuffer buf = ByteBuffer.allocate(16);
        putInt128LE(buf, value);
        buf.flip();
        return buf;
    }

    private static void putInt128LE(ByteBuffer out, BigInteger value) {
        signExtendLEInto(value, out, 16);
    }

    // Serialise a BigInteger as 32 little-endian bytes (int256).
    private static ByteBuffer writeInt256LE(BigInteger value) {
        ByteBuffer buf = ByteBuffer.allocate(32);
        putInt256LE(buf, value);
        buf.flip();
        return buf;
    }

    private static void putInt256LE(ByteBuffer out, BigInteger value) {
        signExtendLEInto(value, out, 32);
    }

    // Expand `value` to exactly `width` bytes, written little-endian (lowest byte first),
    // sign-extending into the high bytes to match the on-disk representation used by
    // DECIMAL128 / DECIMAL256 columns on x86/ARM.
    private static void signExtendLEInto(BigInteger value, ByteBuffer out, int width) {
        byte[] be = value.toByteArray(); // big-endian, two's complement
        if (be.length > width) {
            throw new AssertionError("value does not fit into " + width + " bytes");
        }
        byte sign = (be[0] < 0) ? (byte) 0xFF : 0;
        byte[] buf = new byte[width];
        for (int j = be.length; j < width; ++j) {
            buf[j] = sign;
        }
        for (int j = 0; j < be.length; ++j) {
            buf[j] = be[be.length - 1 - j];
        }
        out.put(buf);
    }
}
