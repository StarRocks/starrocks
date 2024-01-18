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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/analysis/DecimalLiteralTest.java

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

package com.starrocks.analysis;

import com.google.common.base.Strings;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Type;
import com.starrocks.common.conf.Config;
import com.starrocks.common.exception.AnalysisException;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;

public class DecimalLiteralTest {

    @Test
    public void testHashValue() throws AnalysisException {
        BigDecimal decimal = new BigDecimal("-123456789123456789.123456789");
        DecimalLiteral literal = new DecimalLiteral(decimal);

        ByteBuffer buffer = literal.getHashValue(Type.DECIMALV2);
        long longValue = buffer.getLong();
        int fracValue = buffer.getInt();
        System.out.println("long: " + longValue);
        System.out.println("frac: " + fracValue);
        Assert.assertEquals(-123456789123456789L, longValue);
        Assert.assertEquals(-123456789, fracValue);

        // if DecimalLiteral need to cast to Decimal and Decimalv2, need to cast
        // to themselves
        Assert.assertEquals(literal, literal.uncheckedCastTo(Type.DECIMALV2));

        Assert.assertEquals(1, literal.compareLiteral(new NullLiteral()));
    }

    @Test
    public void testGetHashValueOfDecimal128p27s9() throws AnalysisException {
        String[] testCases = new String[] {
                "0.0",
                Strings.repeat("9", 18) + "." + Strings.repeat("9", 9),
                "+" + Strings.repeat("9", 18) + "." + Strings.repeat("9", 9),
                "0.1",
                "-0.1",
                "123456789123456789.9654321",
                "-123456789123456789.9654321",
                "1." + Strings.repeat("9", 9),
                "-1." + Strings.repeat("0", 8) + "1",
                "3.1415926",
                "-3.1415926",
                "0.000000001",
                "-0.000000001",
        };
        for (String tc : testCases) {
            DecimalLiteral decimalLiteral = new DecimalLiteral(tc);
            ByteBuffer a = decimalLiteral.getHashValue(Type.DECIMALV2);
            ByteBuffer b = decimalLiteral.getHashValue(ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL128, 27, 9));
            Assert.assertEquals(a.limit(), 12);
            Assert.assertEquals(a.limit(), b.limit());
            Assert.assertEquals(a.getLong(), b.getLong());
            Assert.assertEquals(a.getInt(), b.getInt());
        }
    }

    @Test
    public void testGetHashValueOfDecimal128p30s10() throws AnalysisException {
        String[] testCases = new String[] {
                "0.0",
                Strings.repeat("9", 20) + "." + Strings.repeat("9", 10),
                "+" + Strings.repeat("9", 20) + "." + Strings.repeat("9", 10),
                "0.1",
                "-0.1",
                "123456789123456789.9654321",
                "-123456789123456789.9654321",
                "1." + Strings.repeat("9", 9),
                "-1." + Strings.repeat("0", 8) + "1",
                "3.1415926",
                "-3.1415926",
                "0.000000001",
                "-0.000000001",
        };
        for (String tc : testCases) {
            DecimalLiteral decimalLiteral = new DecimalLiteral(tc);
            BigDecimal scaleFactor = new BigDecimal("1" + Strings.repeat("0", 10));
            BigInteger bigInt = decimalLiteral.getValue().multiply(scaleFactor).toBigInteger();
            LargeIntLiteral largeIntLiteral = new LargeIntLiteral(bigInt.toString());
            ByteBuffer a = largeIntLiteral.getHashValue(Type.LARGEINT);
            ByteBuffer b =
                    decimalLiteral.getHashValue(ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL128, 30, 10));
            Assert.assertEquals(a.limit(), 16);
            Assert.assertEquals(a.limit(), b.limit());
            Assert.assertEquals(a.getLong(), b.getLong());
            Assert.assertEquals(a.getLong(), b.getLong());
        }
    }

    @Test
    public void testGetHashValueOfDecimal64p15s5() throws AnalysisException {
        String[] testCases = new String[] {
                "0.0",
                Strings.repeat("9", 10) + "." + Strings.repeat("9", 5),
                "+" + Strings.repeat("9", 10) + "." + Strings.repeat("9", 5),
                "0.1",
                "-0.1",
                "1234567891.96543",
                "-1234567891.96543",
                "1." + Strings.repeat("9", 5),
                "-1." + Strings.repeat("0", 4) + "1",
                "3.14",
                "-3.14",
                "0.00001",
                "-0.00001",
        };
        for (String tc : testCases) {
            DecimalLiteral decimalLiteral = new DecimalLiteral(tc);
            BigDecimal scaleFactor = new BigDecimal("1" + Strings.repeat("0", 5));
            long bigInt = decimalLiteral.getValue().multiply(scaleFactor).longValue();
            IntLiteral largeIntLiteral = new IntLiteral(bigInt);
            ByteBuffer a = largeIntLiteral.getHashValue(Type.BIGINT);
            ByteBuffer b = decimalLiteral.getHashValue(ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL64, 15, 5));
            Assert.assertEquals(a.limit(), 8);
            Assert.assertEquals(a.limit(), b.limit());
            Assert.assertEquals(a.getLong(), b.getLong());
        }
    }

    @Test
    public void testGetHashValueOfDecimal32p7s2() throws AnalysisException {
        String[] testCases = new String[] {
                "0.0",
                Strings.repeat("5", 5) + "." + Strings.repeat("9", 2),
                "+" + Strings.repeat("5", 5) + "." + Strings.repeat("9", 2),
                "0.1",
                "-0.1",
                "12345.96",
                "-12345.96",
                "1." + Strings.repeat("9", 2),
                "-1." + Strings.repeat("0", 1) + "1",
                "3.14",
                "-3.14",
                "0.01",
                "-0.01",
        };
        for (String tc : testCases) {
            DecimalLiteral decimalLiteral = new DecimalLiteral(tc);
            BigDecimal scaleFactor = new BigDecimal("1" + Strings.repeat("0", 2));
            long bigInt = decimalLiteral.getValue().multiply(scaleFactor).intValue();
            IntLiteral intLiteral = new IntLiteral(bigInt);
            ByteBuffer a = intLiteral.getHashValue(Type.INT);
            ByteBuffer b = decimalLiteral.getHashValue(ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL32, 7, 2));
            Assert.assertEquals(a.limit(), 4);
            Assert.assertEquals(a.limit(), b.limit());
            Assert.assertEquals(a.getInt(), b.getInt());
        }
    }

    @Test
    public void testDealWithSingularDecimalLiteralNormal() throws AnalysisException {
        Config.enable_decimal_v3 = true;
        DecimalLiteral decimalLiteral;
        Type type;
        decimalLiteral = new DecimalLiteral(Strings.repeat("9", 38));
        Assert.assertEquals(decimalLiteral.getType(), ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL128, 38, 0));

        decimalLiteral = new DecimalLiteral("123456789012345678901234567890.1234567890");
        Assert.assertEquals(decimalLiteral.getType(), ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL128, 38, 8));
        Assert.assertEquals(decimalLiteral.getStringValue(), "123456789012345678901234567890.12345679");

        decimalLiteral = new DecimalLiteral(new BigDecimal("12345678901234567890.12345678901234567890"));
        Assert.assertEquals(decimalLiteral.getType(), ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL128, 38, 18));
        Assert.assertEquals(decimalLiteral.getStringValue(), "12345678901234567890.123456789012345679");

        type = ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL64, 18, 4);
        decimalLiteral = new DecimalLiteral("12345678.90", type);
        Assert.assertEquals(decimalLiteral.getType(), ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL64, 10, 2));
        decimalLiteral = (DecimalLiteral) decimalLiteral.uncheckedCastTo(type);
        Assert.assertEquals(decimalLiteral.getType(), type);

        type = ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL64, 18, 6);
        decimalLiteral = new DecimalLiteral("12345678.1234567890123", type);
        Assert.assertEquals(decimalLiteral.getType(), ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL64, 14, 6));
        decimalLiteral = (DecimalLiteral) decimalLiteral.uncheckedCastTo(type);
        Assert.assertEquals(decimalLiteral.getType(), type);
        Assert.assertEquals(decimalLiteral.getStringValue(), "12345678.123457");
        type = ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL64, 17, 5);
        decimalLiteral = (DecimalLiteral) decimalLiteral.uncheckedCastTo(type);
        Assert.assertEquals(decimalLiteral.getType(), type);
        Assert.assertEquals(decimalLiteral.getStringValue(), "12345678.12346");
    }

    @Test(expected = Throwable.class)
    public void testDealWithSingularDecimalLiteralAbnormal0() throws AnalysisException {
        DecimalLiteral decimalLiteral = new DecimalLiteral(Strings.repeat("9", 39));
    }

    @Test(expected = Throwable.class)
    public void testDealWithSingularDecimalLiteralAbnormal1() {
        BigDecimal decimal = new BigDecimal(Strings.repeat("9", 39));
        DecimalLiteral decimalLiteral = new DecimalLiteral(decimal);
    }

    @Test(expected = Throwable.class)
    public void testDealWithSingularDecimalLiteralAbnormal2() throws AnalysisException {
        Type type = ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL64, 9, 2);
        DecimalLiteral decimalLiteral = new DecimalLiteral("1234567890.1235", type);
    }

    @Test(expected = Throwable.class)
    public void testDealWithSingularDecimalLiteralAbnormal3() throws AnalysisException {
        Type type = ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL64, 9, 2);
        DecimalLiteral decimalLiteral = new DecimalLiteral("92233720368547758.08");
        decimalLiteral.uncheckedCastTo(type);
    }

    @Test
    public void testCheckLiteralOverflowFail() throws AnalysisException {
        BigDecimal decimal32Values[] = {
                new BigDecimal("2147483.6476"),
                new BigDecimal("-2147483.6489"),
                new BigDecimal("2147483.648"),
                new BigDecimal("-2147483.649"),
        };
        ScalarType decimal32p4s3 = ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL32, 4, 3);
        for (BigDecimal dec32 : decimal32Values) {
            try {
                DecimalLiteral.checkLiteralOverflowInBinaryStyle(dec32, decimal32p4s3);
                Assert.fail("should throw exception");
            } catch (Exception ignored) {
            }
        }

        BigDecimal decimal64Values[] = {
                new BigDecimal("9223372036854.775808"),
                new BigDecimal("9223372036854.7758085"),
                new BigDecimal("-9223372036854.775809"),
                new BigDecimal("-9223372036854.7758086"),
        };
        ScalarType decimal64p10s6 = ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL64, 10, 6);
        for (BigDecimal dec64 : decimal64Values) {
            try {
                DecimalLiteral.checkLiteralOverflowInBinaryStyle(dec64, decimal64p10s6);
                Assert.fail("should throw exception");
            } catch (Exception ignored) {
            }
        }

        BigDecimal decimal128Values[] = {
                new BigDecimal("1701411834604692317316873037.15884105728"),
                new BigDecimal("1701411834604692317316873037.158841057285"),
                new BigDecimal("-1701411834604692317316873037.15884105729"),
                new BigDecimal("-1701411834604692317316873037.158841057286"),
        };
        ScalarType decimal128p36s11 = ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL128, 36, 11);
        for (BigDecimal dec128 : decimal128Values) {
            try {
                DecimalLiteral.checkLiteralOverflowInBinaryStyle(dec128, decimal128p36s11);
                Assert.fail("should throw exception");
            } catch (Exception ignored) {
            }
        }
    }

    @Test
    public void testCheckLiteralOverflowSuccess() throws AnalysisException {
        BigDecimal decimal32Values[] = {
                new BigDecimal("2147483.647"),
                new BigDecimal("2147483.6474"),
                new BigDecimal("2147483.6465"),
                new BigDecimal("2147483.0001"),
                new BigDecimal("0.0001"),
                new BigDecimal("0.0"),
                new BigDecimal("-2147483.648"),
                new BigDecimal("-2147483.6484"),
                new BigDecimal("-2147483.6475"),
                new BigDecimal("-0.0001"),
        };
        ScalarType decimal32p4s3 = ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL32, 4, 3);
        for (BigDecimal dec32 : decimal32Values) {
            try {
                DecimalLiteral.checkLiteralOverflowInBinaryStyle(dec32, decimal32p4s3);
            } catch (Exception ignored) {
                Assert.fail("should not throw exception");
            }
        }

        BigDecimal decimal64Values[] = {
                new BigDecimal("9223372036854.775807"),
                new BigDecimal("9223372036854.7758074"),
                new BigDecimal("9223372036854.7758065"),
                new BigDecimal("-9223372036854.775808"),
                new BigDecimal("-9223372036854.7758084"),
                new BigDecimal("-9223372036854.7758079"),
                new BigDecimal("-0.000001"),
                new BigDecimal("0.000001"),
                new BigDecimal("0.0"),
        };
        ScalarType decimal64p10s6 = ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL64, 10, 6);
        for (BigDecimal dec64 : decimal64Values) {
            try {
                DecimalLiteral.checkLiteralOverflowInBinaryStyle(dec64, decimal64p10s6);
            } catch (Exception ignored) {
                Assert.fail("should not throw exception");
            }
        }

        BigDecimal decimal128Values[] = {
                new BigDecimal("1701411834604692317316873037.15884105727"),
                new BigDecimal("1701411834604692317316873037.158841057274"),
                new BigDecimal("1701411834604692317316873037.158841057265"),
                new BigDecimal("-1701411834604692317316873037.15884105728"),
                new BigDecimal("-1701411834604692317316873037.158841057284"),
                new BigDecimal("-1701411834604692317316873037.158841057275"),
                new BigDecimal("-0.00000000001"),
                new BigDecimal("0.00000000001"),
                new BigDecimal("0.0"),
        };
        ScalarType decimal128p36s11 = ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL128, 36, 11);
        for (BigDecimal dec128 : decimal128Values) {
            try {
                DecimalLiteral.checkLiteralOverflowInBinaryStyle(dec128, decimal128p36s11);
            } catch (Exception ignored) {
                Assert.fail("should not throw exception");
            }
        }
    }

    @Test
    public void testCheckLiteralOverflowInDecimalStyleFail() throws AnalysisException {
        BigDecimal decimal32Values[] = {
                new BigDecimal("100000.0000"),
                new BigDecimal("99999.99995"),
                new BigDecimal("-100000.0000"),
                new BigDecimal("-99999.99995"),
        };
        ScalarType decimal32p9s4 = ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL32, 9, 4);
        for (BigDecimal dec32 : decimal32Values) {
            Assert.assertFalse(DecimalLiteral.checkLiteralOverflowInDecimalStyle(dec32, decimal32p9s4));
        }

        BigDecimal decimal64Values[] = {
                new BigDecimal("1000000000000.000000"),
                new BigDecimal("999999999999.9999995"),
                new BigDecimal("-1000000000000.000000"),
                new BigDecimal("-999999999999.9999995"),
        };
        ScalarType decimal64p18s6 = ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL64, 18, 6);
        for (BigDecimal dec64 : decimal64Values) {
            Assert.assertFalse(DecimalLiteral.checkLiteralOverflowInDecimalStyle(dec64, decimal64p18s6));
        }

        BigDecimal decimal128Values[] = {
                new BigDecimal("1000000000000000000000000000.00000000000"),
                new BigDecimal("999999999999999999999999999.999999999995"),
                new BigDecimal("-1000000000000000000000000000.00000000000"),
                new BigDecimal("-999999999999999999999999999.999999999995"),
        };
        ScalarType decimal128p38s11 = ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL128, 38, 11);
        for (BigDecimal dec128 : decimal128Values) {
            Assert.assertFalse(DecimalLiteral.checkLiteralOverflowInDecimalStyle(dec128, decimal128p38s11));
        }
    }

    @Test
    public void testCheckLiteralOverflowInDecimalStyleSuccess() throws AnalysisException {
        BigDecimal decimal32Values[] = {
                new BigDecimal("99999.99994"),
                new BigDecimal("99999.9999"),
                new BigDecimal("99999.999"),
                new BigDecimal("-99999.99994"),
                new BigDecimal("-99999.9999"),
                new BigDecimal("-99999.999"),
                new BigDecimal("0.0001"),
                new BigDecimal("0.0"),
                new BigDecimal("-0.0001"),
        };
        ScalarType decimal32p9s4 = ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL32, 9, 4);
        for (BigDecimal dec32 : decimal32Values) {
            Assert.assertTrue(DecimalLiteral.checkLiteralOverflowInDecimalStyle(dec32, decimal32p9s4));
        }

        BigDecimal decimal64Values[] = {
                new BigDecimal("999999999999.9999994"),
                new BigDecimal("999999999999.999999"),
                new BigDecimal("999999999999.99999"),
                new BigDecimal("-999999999999.9999994"),
                new BigDecimal("-999999999999.999999"),
                new BigDecimal("-999999999999.99999"),
                new BigDecimal("-0.000001"),
                new BigDecimal("0.000001"),
                new BigDecimal("0.0"),
        };
        ScalarType decimal64p18s6 = ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL64, 18, 6);
        for (BigDecimal dec64 : decimal64Values) {
            Assert.assertTrue(DecimalLiteral.checkLiteralOverflowInDecimalStyle(dec64, decimal64p18s6));
        }

        BigDecimal decimal128Values[] = {
                new BigDecimal("999999999999999999999999999.999999999994"),
                new BigDecimal("999999999999999999999999999.99999999999"),
                new BigDecimal("999999999999999999999999999.9999999999"),
                new BigDecimal("-999999999999999999999999999.999999999994"),
                new BigDecimal("-999999999999999999999999999.99999999999"),
                new BigDecimal("-999999999999999999999999999.9999999999"),
                new BigDecimal("-0.00000000001"),
                new BigDecimal("0.00000000001"),
                new BigDecimal("0.0"),
        };
        ScalarType decimal128p38s11 = ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL128, 38, 11);
        for (BigDecimal dec128 : decimal128Values) {
            Assert.assertTrue(DecimalLiteral.checkLiteralOverflowInDecimalStyle(dec128, decimal128p38s11));
        }
    }

    @Test
    public void testPackDecimal() {
        BigInteger[] bigIntegers = new BigInteger[] {
                BigInteger.ZERO,
                BigInteger.ONE,
                BigInteger.ONE.shiftLeft(31).subtract(BigInteger.ONE),
                BigInteger.ONE.shiftLeft(31).negate(),
                BigInteger.ONE.shiftLeft(32).subtract(BigInteger.ONE),
                BigInteger.ONE.shiftLeft(32).negate(),
                BigInteger.ONE.shiftLeft(63).subtract(BigInteger.ONE),
                BigInteger.ONE.shiftLeft(63).negate(),
                BigInteger.ONE.shiftLeft(64).subtract(BigInteger.ONE),
                BigInteger.ONE.shiftLeft(64).negate(),
                BigInteger.ONE.shiftLeft(126).subtract(BigInteger.ONE),
                BigInteger.ONE.shiftLeft(126).negate(),
        };
        for (BigInteger integer : bigIntegers) {
            BigDecimal decimal = new BigDecimal(integer, 3);
            DecimalLiteral decimalLiteral = new DecimalLiteral(decimal);
            ByteBuffer packed = decimalLiteral.packDecimal();
            int numBytes = packed.limit();
            byte[] bytes = new byte[numBytes];
            packed.get(bytes);
            int i = 0, j = numBytes - 1;
            while (i < j) {
                byte tmp = bytes[j];
                bytes[j] = bytes[i];
                bytes[i] = tmp;
                ++i;
                --j;
            }
            BigInteger expected = new BigInteger(bytes);
            Assert.assertEquals(expected, integer);
        }
    }
}
