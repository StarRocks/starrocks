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

package com.starrocks.catalog;

import com.starrocks.thrift.TTuple;
import com.starrocks.thrift.TVariant;
import com.starrocks.type.BooleanType;
import com.starrocks.type.CharType;
import com.starrocks.type.DateType;
import com.starrocks.type.FloatType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.Type;
import com.starrocks.type.TypeSerializer;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;

public class VariantTest {

    // ==================== BoolVariant Tests ====================

    @Test
    public void testBoolVariantFromBoolean() {
        BoolVariant trueVariant = new BoolVariant(true);
        Assertions.assertEquals(1L, trueVariant.getLongValue());
        Assertions.assertEquals("TRUE", trueVariant.getStringValue());
        Assertions.assertEquals(BooleanType.BOOLEAN, trueVariant.getType());

        BoolVariant falseVariant = new BoolVariant(false);
        Assertions.assertEquals(0L, falseVariant.getLongValue());
        Assertions.assertEquals("FALSE", falseVariant.getStringValue());
    }

    @Test
    public void testBoolVariantFromString() {
        BoolVariant v1 = new BoolVariant("true");
        Assertions.assertEquals(1L, v1.getLongValue());

        BoolVariant v2 = new BoolVariant("TRUE");
        Assertions.assertEquals(1L, v2.getLongValue());

        BoolVariant v3 = new BoolVariant("1");
        Assertions.assertEquals(1L, v3.getLongValue());

        BoolVariant v4 = new BoolVariant("false");
        Assertions.assertEquals(0L, v4.getLongValue());

        BoolVariant v5 = new BoolVariant("FALSE");
        Assertions.assertEquals(0L, v5.getLongValue());

        BoolVariant v6 = new BoolVariant("0");
        Assertions.assertEquals(0L, v6.getLongValue());

        // Test with whitespace
        BoolVariant v7 = new BoolVariant("  true  ");
        Assertions.assertEquals(1L, v7.getLongValue());
    }

    @Test
    public void testBoolVariantInvalidString() {
        Assertions.assertThrows(RuntimeException.class, () -> new BoolVariant("invalid"));
        Assertions.assertThrows(RuntimeException.class, () -> new BoolVariant("2"));
    }

    @Test
    public void testBoolVariantCompareTo() {
        BoolVariant trueVar = new BoolVariant(true);
        BoolVariant falseVar = new BoolVariant(false);

        Assertions.assertTrue(trueVar.compareTo(falseVar) > 0);
        Assertions.assertTrue(falseVar.compareTo(trueVar) < 0);
        Assertions.assertEquals(0, trueVar.compareTo(new BoolVariant(true)));
    }

    // ==================== IntVariant Tests ====================

    @Test
    public void testIntVariantTinyInt() {
        IntVariant v1 = new IntVariant(IntegerType.TINYINT, (byte) 127);
        Assertions.assertEquals(127L, v1.getLongValue());
        Assertions.assertEquals("127", v1.getStringValue());

        IntVariant v2 = new IntVariant(IntegerType.TINYINT, (byte) -128);
        Assertions.assertEquals(-128L, v2.getLongValue());

        // Out of range
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> new IntVariant(IntegerType.TINYINT, 128));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> new IntVariant(IntegerType.TINYINT, -129));
    }

    @Test
    public void testIntVariantSmallInt() {
        IntVariant v1 = new IntVariant(IntegerType.SMALLINT, (short) 32767);
        Assertions.assertEquals(32767L, v1.getLongValue());

        IntVariant v2 = new IntVariant(IntegerType.SMALLINT, (short) -32768);
        Assertions.assertEquals(-32768L, v2.getLongValue());

        // Out of range
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> new IntVariant(IntegerType.SMALLINT, 32768));
    }

    @Test
    public void testIntVariantInt() {
        IntVariant v1 = new IntVariant(IntegerType.INT, Integer.MAX_VALUE);
        Assertions.assertEquals((long) Integer.MAX_VALUE, v1.getLongValue());

        IntVariant v2 = new IntVariant(IntegerType.INT, Integer.MIN_VALUE);
        Assertions.assertEquals((long) Integer.MIN_VALUE, v2.getLongValue());

        // Out of range
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> new IntVariant(IntegerType.INT, (long) Integer.MAX_VALUE + 1));
    }

    @Test
    public void testIntVariantBigInt() {
        IntVariant v1 = new IntVariant(IntegerType.BIGINT, Long.MAX_VALUE);
        Assertions.assertEquals(Long.MAX_VALUE, v1.getLongValue());

        IntVariant v2 = new IntVariant(IntegerType.BIGINT, Long.MIN_VALUE);
        Assertions.assertEquals(Long.MIN_VALUE, v2.getLongValue());
    }

    @Test
    public void testIntVariantFromString() {
        IntVariant v1 = new IntVariant(IntegerType.INT, "12345");
        Assertions.assertEquals(12345L, v1.getLongValue());

        IntVariant v2 = new IntVariant(IntegerType.BIGINT, "-9876543210");
        Assertions.assertEquals(-9876543210L, v2.getLongValue());

        Assertions.assertThrows(RuntimeException.class,
                () -> new IntVariant(IntegerType.INT, "not a number"));
    }

    @Test
    public void testIntVariantCompareTo() {
        IntVariant v1 = new IntVariant(IntegerType.INT, 100);
        IntVariant v2 = new IntVariant(IntegerType.INT, 200);
        IntVariant v3 = new IntVariant(IntegerType.BIGINT, 100);

        Assertions.assertTrue(v1.compareTo(v2) < 0);
        Assertions.assertTrue(v2.compareTo(v1) > 0);
        Assertions.assertEquals(0, v1.compareTo(v3));
    }

    // ==================== LargeIntVariant Tests ====================

    @Test
    public void testLargeIntVariantFromString() {
        LargeIntVariant v1 = new LargeIntVariant("12345678901234567890");
        Assertions.assertEquals("12345678901234567890", v1.getStringValue());

        LargeIntVariant v2 = new LargeIntVariant("-98765432109876543210");
        Assertions.assertEquals("-98765432109876543210", v2.getStringValue());
    }

    @Test
    public void testLargeIntVariantFromBigInteger() {
        BigInteger value = new BigInteger("170141183460469231731687303715884105727"); // 2^127 - 1
        LargeIntVariant v = new LargeIntVariant(value);
        Assertions.assertEquals(value.toString(), v.getStringValue());
    }

    @Test
    public void testLargeIntVariantSmallValues() {
        // When high == 0, should use optimized path
        LargeIntVariant v1 = new LargeIntVariant("100");
        Assertions.assertEquals(100L, v1.getLongValue());
        Assertions.assertEquals("100", v1.getStringValue());

        LargeIntVariant v2 = new LargeIntVariant("0");
        Assertions.assertEquals(0L, v2.getLongValue());
    }

    @Test
    public void testLargeIntVariantOutOfRange() {
        // 2^127 should be out of range
        BigInteger outOfRange = BigInteger.ONE.shiftLeft(127);
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> new LargeIntVariant(outOfRange));
    }

    @Test
    public void testLargeIntVariantCompareTo() {
        LargeIntVariant v1 = new LargeIntVariant("100");
        LargeIntVariant v2 = new LargeIntVariant("200");
        LargeIntVariant v3 = new LargeIntVariant("12345678901234567890");

        Assertions.assertTrue(v1.compareTo(v2) < 0);
        Assertions.assertTrue(v2.compareTo(v1) > 0);
        Assertions.assertTrue(v3.compareTo(v2) > 0);

        // Compare with IntVariant
        IntVariant intVar = new IntVariant(IntegerType.BIGINT, 100);
        Assertions.assertEquals(0, v1.compareTo(intVar));
    }

    // ==================== StringVariant Tests ====================

    @Test
    public void testStringVariantBasic() {
        StringVariant v1 = new StringVariant(VarcharType.VARCHAR, "hello");
        Assertions.assertEquals("hello", v1.getStringValue());
        Assertions.assertEquals(VarcharType.VARCHAR, v1.getType());
    }

    @Test
    public void testStringVariantGetLongValue() {
        StringVariant v1 = new StringVariant(VarcharType.VARCHAR, "12345");
        Assertions.assertEquals(12345L, v1.getLongValue());

        StringVariant v2 = new StringVariant(VarcharType.VARCHAR, "not a number");
        Assertions.assertThrows(RuntimeException.class, v2::getLongValue);
    }

    @Test
    public void testStringVariantCompareTo() {
        StringVariant v1 = new StringVariant(VarcharType.VARCHAR, "abc");
        StringVariant v2 = new StringVariant(VarcharType.VARCHAR, "abd");
        StringVariant v3 = new StringVariant(VarcharType.VARCHAR, "abc");

        Assertions.assertTrue(v1.compareTo(v2) < 0);
        Assertions.assertTrue(v2.compareTo(v1) > 0);
        Assertions.assertEquals(0, v1.compareTo(v3));
    }

    @Test
    public void testStringVariantCompareDifferentLengths() {
        StringVariant v1 = new StringVariant(VarcharType.VARCHAR, "abc");
        StringVariant v2 = new StringVariant(VarcharType.VARCHAR, "abcd");

        Assertions.assertTrue(v1.compareTo(v2) < 0);
        Assertions.assertTrue(v2.compareTo(v1) > 0);
    }

    @Test
    public void testStringVariantCompareWithNullBytes() {
        StringVariant v1 = new StringVariant(VarcharType.VARCHAR, "abc\0");
        StringVariant v2 = new StringVariant(VarcharType.VARCHAR, "abc");

        // When one string has null byte at the end, they should be equal
        Assertions.assertEquals(0, v1.compareTo(v2));
    }

    @Test
    public void testStringVariantEmpty() {
        StringVariant v1 = new StringVariant(VarcharType.VARCHAR, "");
        Assertions.assertEquals("", v1.getStringValue());

        StringVariant v2 = new StringVariant(VarcharType.VARCHAR, "a");
        Assertions.assertTrue(v1.compareTo(v2) < 0);
    }

    // ==================== Variant.toThrift / Tuple.toThrift Tests ====================

    @Test
    public void testVariantToThriftNumeric() {
        Variant intVariant = new IntVariant(IntegerType.INT, 123);
        TVariant tInt = intVariant.toThrift();

        Assertions.assertTrue(tInt.isSetType());
        Assertions.assertEquals(TypeSerializer.toThrift(IntegerType.INT), tInt.getType());
        // Numeric variants are encoded via the `value` field.
        Assertions.assertTrue(tInt.isSetValue());
        Assertions.assertEquals("123", tInt.getValue());
    }

    @Test
    public void testVariantToThriftBoolean() {
        Variant boolVariant = new BoolVariant(true);
        TVariant tBool = boolVariant.toThrift();

        Assertions.assertTrue(tBool.isSetType());
        Assertions.assertEquals(TypeSerializer.toThrift(BooleanType.BOOLEAN), tBool.getType());
        // Boolean variants are also encoded via the `value` field.
        Assertions.assertTrue(tBool.isSetValue());
        Assertions.assertEquals("TRUE", tBool.getValue());
    }

    @Test
    public void testVariantToThriftString() {
        Variant strVariant = new StringVariant(VarcharType.VARCHAR, "hello");
        TVariant tStr = strVariant.toThrift();

        Assertions.assertTrue(tStr.isSetType());
        Assertions.assertEquals(TypeSerializer.toThrift(VarcharType.VARCHAR), tStr.getType());
        Assertions.assertTrue(tStr.isSetValue());
        Assertions.assertEquals("hello", tStr.getValue());
    }

    @Test
    public void testVariantToThriftDateTime() {
        Variant dtVariant = new DateVariant(DateType.DATETIME, "2024-01-15T10:30:00");
        TVariant tDt = dtVariant.toThrift();

        Assertions.assertTrue(tDt.isSetType());
        Assertions.assertEquals(TypeSerializer.toThrift(DateType.DATETIME), tDt.getType());
        // Date/time are encoded via the `value` field in TVariant.
        Assertions.assertTrue(tDt.isSetValue());
        Assertions.assertFalse(tDt.getValue().isEmpty());
    }

    @Test
    public void testTupleToThrift() {
        Variant v1 = new IntVariant(IntegerType.BIGINT, 42L);
        Variant v2 = new StringVariant(VarcharType.VARCHAR, "world");
        Tuple tuple = new Tuple(Arrays.asList(v1, v2));

        TTuple tTuple = tuple.toThrift();
        Assertions.assertEquals(2, tTuple.getValuesSize());

        TVariant t1 = tTuple.getValues().get(0);
        TVariant t2 = tTuple.getValues().get(1);

        Assertions.assertTrue(t2.isSetValue());
        Assertions.assertEquals("world", t2.getValue());

        // Verify that the embedded type descriptors are aligned with original Types.
        Type type1 = v1.getType();
        Type type2 = v2.getType();
        Assertions.assertEquals(TypeSerializer.toThrift(type1), t1.getType());
        Assertions.assertEquals(TypeSerializer.toThrift(type2), t2.getType());
    }

    // ==================== DateVariant Tests ====================

    @Test
    public void testDateTimeVariantFromString() {
        DateVariant v1 = new DateVariant(DateType.DATETIME, "2024-01-15T10:30:00");
        Assertions.assertNotNull(v1.getStringValue());
        Assertions.assertTrue(v1.getLongValue() > 0);
    }

    @Test
    public void testDateTimeVariantFromInstant() {
        Instant instant = Instant.parse("2024-01-15T10:30:00Z");
        DateVariant v1 = new DateVariant(DateType.DATETIME, instant);

        Assertions.assertNotNull(v1.getStringValue());
        Assertions.assertEquals(instant.getEpochSecond() * 1000000 + instant.getNano() / 1000,
                v1.getLongValue());
    }

    @Test
    public void testDateTimeVariantCompareTo() {
        DateVariant v1 = new DateVariant(DateType.DATETIME, "2024-01-15T10:30:00");
        DateVariant v2 = new DateVariant(DateType.DATETIME, "2024-01-15T10:30:01");
        DateVariant v3 = new DateVariant(DateType.DATETIME, "2024-01-15T10:30:00");

        Assertions.assertTrue(v1.compareTo(v2) < 0);
        Assertions.assertTrue(v2.compareTo(v1) > 0);
        Assertions.assertEquals(0, v1.compareTo(v3));
    }

    @Test
    public void testDateTimeVariantWithNanos() {
        Instant instant1 = Instant.ofEpochSecond(1000, 123456789);
        Instant instant2 = Instant.ofEpochSecond(1000, 123456790);

        DateVariant v1 = new DateVariant(DateType.DATETIME, instant1);
        DateVariant v2 = new DateVariant(DateType.DATETIME, instant2);

        Assertions.assertTrue(v1.compareTo(v2) < 0);
    }

    // ==================== Variant.of() Factory Method Tests ====================

    @Test
    public void testVariantOfBoolean() {
        Variant v1 = Variant.of(BooleanType.BOOLEAN, "true");
        Assertions.assertTrue(v1 instanceof BoolVariant);
        Assertions.assertEquals(1L, v1.getLongValue());

        Variant v2 = Variant.of(BooleanType.BOOLEAN, "false");
        Assertions.assertEquals(0L, v2.getLongValue());
    }

    @Test
    public void testVariantOfIntTypes() {
        Variant v1 = Variant.of(IntegerType.TINYINT, "100");
        Assertions.assertTrue(v1 instanceof IntVariant);
        Assertions.assertEquals(100L, v1.getLongValue());

        Variant v2 = Variant.of(IntegerType.SMALLINT, "1000");
        Assertions.assertTrue(v2 instanceof IntVariant);

        Variant v3 = Variant.of(IntegerType.INT, "100000");
        Assertions.assertTrue(v3 instanceof IntVariant);

        Variant v4 = Variant.of(IntegerType.BIGINT, "10000000000");
        Assertions.assertTrue(v4 instanceof IntVariant);
    }

    @Test
    public void testVariantOfLargeInt() {
        Variant v = Variant.of(IntegerType.LARGEINT, "12345678901234567890");
        Assertions.assertTrue(v instanceof LargeIntVariant);
        Assertions.assertEquals("12345678901234567890", v.getStringValue());
    }

    @Test
    public void testVariantOfStringTypes() {
        Variant v1 = Variant.of(VarcharType.VARCHAR, "test");
        Assertions.assertTrue(v1 instanceof StringVariant);
        Assertions.assertEquals("test", v1.getStringValue());

        Variant v2 = Variant.of(CharType.CHAR, "char");
        Assertions.assertTrue(v2 instanceof StringVariant);
    }

    @Test
    public void testVariantOfDateTime() {
        Variant v1 = Variant.of(DateType.DATETIME, "2024-01-15T10:30:00");
        Assertions.assertTrue(v1 instanceof DateVariant);

        Variant v2 = Variant.of(DateType.DATE, "2024-01-15T00:00:00");
        Assertions.assertTrue(v2 instanceof DateVariant);
    }

    @Test
    public void testVariantOfUnsupportedType() {
        Assertions.assertThrows(RuntimeException.class,
                () -> Variant.of(FloatType.FLOAT, "1.23"));
    }

    // ==================== Variant.compatibleCompare() Tests ====================

    @Test
    public void testCompatibleCompareSameType() {
        Variant v1 = new IntVariant(IntegerType.INT, 100);
        Variant v2 = new IntVariant(IntegerType.INT, 200);

        Assertions.assertTrue(Variant.compatibleCompare(v1, v2) < 0);
        Assertions.assertTrue(Variant.compatibleCompare(v2, v1) > 0);
        Assertions.assertEquals(0, Variant.compatibleCompare(v1, v1));
    }

    @Test
    public void testCompatibleCompareDifferentTypes() {
        {
            Variant v1 = new IntVariant(IntegerType.INT, 100);
            Variant v2 = new IntVariant(IntegerType.BIGINT, 100);

            // Should be equal after type conversion
            Assertions.assertEquals(0, Variant.compatibleCompare(v1, v2));
        }

        {
            Variant v1 = new IntVariant(IntegerType.BIGINT, 100);
            Variant v2 = new LargeIntVariant(100);

            // Should be equal after type conversion
            Assertions.assertEquals(0, Variant.compatibleCompare(v1, v2));
        }
    }

    @Test
    public void testCompatibleCompareList() {
        List<Variant> list1 = Arrays.asList(
                new IntVariant(IntegerType.INT, 1),
                new IntVariant(IntegerType.INT, 2),
                new IntVariant(IntegerType.INT, 3));

        List<Variant> list2 = Arrays.asList(
                new IntVariant(IntegerType.INT, 1),
                new IntVariant(IntegerType.INT, 2),
                new IntVariant(IntegerType.INT, 4));

        Assertions.assertTrue(Variant.compatibleCompare(list1, list2) < 0);
        Assertions.assertTrue(Variant.compatibleCompare(list2, list1) > 0);
        Assertions.assertEquals(0, Variant.compatibleCompare(list1, list1));
    }

    @Test
    public void testCompatibleCompareListDifferentLengths() {
        List<Variant> list1 = Arrays.asList(
                new IntVariant(IntegerType.INT, 1),
                new IntVariant(IntegerType.INT, 2));

        List<Variant> list2 = Arrays.asList(
                new IntVariant(IntegerType.INT, 1),
                new IntVariant(IntegerType.INT, 2),
                new IntVariant(IntegerType.INT, 3));

        Assertions.assertTrue(Variant.compatibleCompare(list1, list2) < 0);
        Assertions.assertTrue(Variant.compatibleCompare(list2, list1) > 0);
    }

    @Test
    public void testCompatibleCompareEmptyList() {
        List<Variant> empty = Arrays.asList();
        List<Variant> nonEmpty = Arrays.asList(new IntVariant(IntegerType.INT, 1));

        Assertions.assertTrue(Variant.compatibleCompare(empty, nonEmpty) < 0);
        Assertions.assertTrue(Variant.compatibleCompare(nonEmpty, empty) > 0);
        Assertions.assertEquals(0, Variant.compatibleCompare(empty, empty));
    }

    // ==================== Cross-type Comparison Tests ====================

    @Test
    public void testBoolVariantCompareWithLargeInt() {
        BoolVariant boolVar = new BoolVariant(true);
        IntVariant intVar = new IntVariant(IntegerType.INT, 1);
        LargeIntVariant largeIntVar = new LargeIntVariant("1");

        Assertions.assertEquals(0, boolVar.compareTo(intVar));
        Assertions.assertEquals(0, intVar.compareTo(boolVar));
        Assertions.assertEquals(0, boolVar.compareTo(largeIntVar));
        Assertions.assertEquals(0, largeIntVar.compareTo(boolVar));
    }

    @Test
    public void testIntVariantCompareWithLargeInt() {
        IntVariant intVar = new IntVariant(IntegerType.BIGINT, 12345);
        LargeIntVariant largeIntVar = new LargeIntVariant("12345");

        Assertions.assertEquals(0, intVar.compareTo(largeIntVar));
        Assertions.assertEquals(0, largeIntVar.compareTo(intVar));
    }

    // ==================== Edge Cases ====================

    @Test
    public void testZeroValues() {
        BoolVariant boolZero = new BoolVariant(false);
        IntVariant intZero = new IntVariant(IntegerType.INT, 0);
        LargeIntVariant largeIntZero = new LargeIntVariant("0");

        Assertions.assertEquals(0L, boolZero.getLongValue());
        Assertions.assertEquals(0L, intZero.getLongValue());
        Assertions.assertEquals(0L, largeIntZero.getLongValue());
    }

    @Test
    public void testNegativeValues() {
        IntVariant v1 = new IntVariant(IntegerType.INT, -100);
        Assertions.assertEquals(-100L, v1.getLongValue());
        Assertions.assertEquals("-100", v1.getStringValue());

        LargeIntVariant v2 = new LargeIntVariant("-12345678901234567890");
        Assertions.assertEquals("-12345678901234567890", v2.getStringValue());
    }

    // ==================== BoolVariant equals() and hashCode() Tests ====================

    @Test
    public void testBoolVariantEqualsReflexive() {
        BoolVariant v = new BoolVariant(true);
        Assertions.assertTrue(v.equals(v));
    }

    @Test
    public void testBoolVariantEqualsSymmetric() {
        BoolVariant v1 = new BoolVariant(true);
        BoolVariant v2 = new BoolVariant(true);
        Assertions.assertTrue(v1.equals(v2));
        Assertions.assertTrue(v2.equals(v1));
    }

    @Test
    public void testBoolVariantEqualsNull() {
        BoolVariant v = new BoolVariant(true);
        Assertions.assertFalse(v.equals(null));
    }

    @Test
    public void testBoolVariantEqualsDifferentClass() {
        BoolVariant v = new BoolVariant(true);
        Assertions.assertFalse(v.equals("true"));
        Assertions.assertFalse(v.equals(Integer.valueOf(1)));
    }

    @Test
    public void testBoolVariantEqualsSameValue() {
        BoolVariant v1 = new BoolVariant(true);
        BoolVariant v2 = new BoolVariant(true);
        Assertions.assertTrue(v1.equals(v2));

        BoolVariant v3 = new BoolVariant(false);
        BoolVariant v4 = new BoolVariant(false);
        Assertions.assertTrue(v3.equals(v4));
    }

    @Test
    public void testBoolVariantEqualsDifferentValue() {
        BoolVariant v1 = new BoolVariant(true);
        BoolVariant v2 = new BoolVariant(false);
        Assertions.assertFalse(v1.equals(v2));
        Assertions.assertFalse(v2.equals(v1));
    }

    @Test
    public void testBoolVariantHashCodeConsistency() {
        BoolVariant v = new BoolVariant(true);
        int hash1 = v.hashCode();
        int hash2 = v.hashCode();
        Assertions.assertEquals(hash1, hash2);
    }

    @Test
    public void testBoolVariantHashCodeEqualObjects() {
        BoolVariant v1 = new BoolVariant(true);
        BoolVariant v2 = new BoolVariant(true);
        Assertions.assertTrue(v1.equals(v2));
        Assertions.assertEquals(v1.hashCode(), v2.hashCode());

        BoolVariant v3 = new BoolVariant(false);
        BoolVariant v4 = new BoolVariant(false);
        Assertions.assertTrue(v3.equals(v4));
        Assertions.assertEquals(v3.hashCode(), v4.hashCode());
    }

    // ==================== IntVariant equals() and hashCode() Tests ====================

    @Test
    public void testIntVariantEqualsReflexive() {
        IntVariant v = new IntVariant(IntegerType.INT, 100);
        Assertions.assertTrue(v.equals(v));
    }

    @Test
    public void testIntVariantEqualsSymmetric() {
        IntVariant v1 = new IntVariant(IntegerType.INT, 100);
        IntVariant v2 = new IntVariant(IntegerType.INT, 100);
        Assertions.assertTrue(v1.equals(v2));
        Assertions.assertTrue(v2.equals(v1));
    }

    @Test
    public void testIntVariantEqualsNull() {
        IntVariant v = new IntVariant(IntegerType.INT, 100);
        Assertions.assertFalse(v.equals(null));
    }

    @Test
    public void testIntVariantEqualsDifferentClass() {
        IntVariant v = new IntVariant(IntegerType.INT, 100);
        Assertions.assertFalse(v.equals("100"));
        Assertions.assertFalse(v.equals(Integer.valueOf(100)));
        // IntVariant should not equal LargeIntVariant even with same value
        Assertions.assertFalse(v.equals(new LargeIntVariant(100)));
    }

    @Test
    public void testIntVariantEqualsSameValue() {
        IntVariant v1 = new IntVariant(IntegerType.INT, 12345);
        IntVariant v2 = new IntVariant(IntegerType.INT, 12345);
        Assertions.assertTrue(v1.equals(v2));
    }

    @Test
    public void testIntVariantEqualsDifferentValue() {
        IntVariant v1 = new IntVariant(IntegerType.INT, 100);
        IntVariant v2 = new IntVariant(IntegerType.INT, 200);
        Assertions.assertFalse(v1.equals(v2));
    }

    @Test
    public void testIntVariantEqualsDifferentTypes() {
        // Different integer types with same value should still be equal
        IntVariant v1 = new IntVariant(IntegerType.INT, 100);
        IntVariant v2 = new IntVariant(IntegerType.BIGINT, 100);
        Assertions.assertTrue(v1.equals(v2));
    }

    @Test
    public void testIntVariantHashCodeConsistency() {
        IntVariant v = new IntVariant(IntegerType.INT, 12345);
        int hash1 = v.hashCode();
        int hash2 = v.hashCode();
        Assertions.assertEquals(hash1, hash2);
    }

    @Test
    public void testIntVariantHashCodeEqualObjects() {
        IntVariant v1 = new IntVariant(IntegerType.INT, 12345);
        IntVariant v2 = new IntVariant(IntegerType.INT, 12345);
        Assertions.assertTrue(v1.equals(v2));
        Assertions.assertEquals(v1.hashCode(), v2.hashCode());
    }

    @Test
    public void testIntVariantHashCodeNegativeValues() {
        IntVariant v1 = new IntVariant(IntegerType.INT, -100);
        IntVariant v2 = new IntVariant(IntegerType.INT, -100);
        Assertions.assertTrue(v1.equals(v2));
        Assertions.assertEquals(v1.hashCode(), v2.hashCode());
    }

    // ==================== LargeIntVariant equals() and hashCode() Tests ====================

    @Test
    public void testLargeIntVariantEqualsReflexive() {
        LargeIntVariant v = new LargeIntVariant("12345678901234567890");
        Assertions.assertTrue(v.equals(v));
    }

    @Test
    public void testLargeIntVariantEqualsSymmetric() {
        LargeIntVariant v1 = new LargeIntVariant("12345678901234567890");
        LargeIntVariant v2 = new LargeIntVariant("12345678901234567890");
        Assertions.assertTrue(v1.equals(v2));
        Assertions.assertTrue(v2.equals(v1));
    }

    @Test
    public void testLargeIntVariantEqualsNull() {
        LargeIntVariant v = new LargeIntVariant("12345678901234567890");
        Assertions.assertFalse(v.equals(null));
    }

    @Test
    public void testLargeIntVariantEqualsDifferentClass() {
        LargeIntVariant v = new LargeIntVariant("12345678901234567890");
        Assertions.assertFalse(v.equals("12345678901234567890"));
        Assertions.assertFalse(v.equals(new IntVariant(IntegerType.BIGINT, 100)));
    }

    @Test
    public void testLargeIntVariantEqualsSameValue() {
        LargeIntVariant v1 = new LargeIntVariant("12345678901234567890");
        LargeIntVariant v2 = new LargeIntVariant("12345678901234567890");
        Assertions.assertTrue(v1.equals(v2));
    }

    @Test
    public void testLargeIntVariantEqualsDifferentValue() {
        LargeIntVariant v1 = new LargeIntVariant("12345678901234567890");
        LargeIntVariant v2 = new LargeIntVariant("98765432109876543210");
        Assertions.assertFalse(v1.equals(v2));
    }

    @Test
    public void testLargeIntVariantEqualsSmallValues() {
        LargeIntVariant v1 = new LargeIntVariant(100);
        LargeIntVariant v2 = new LargeIntVariant("100");
        Assertions.assertTrue(v1.equals(v2));
    }

    @Test
    public void testLargeIntVariantEqualsNegativeValues() {
        LargeIntVariant v1 = new LargeIntVariant("-12345678901234567890");
        LargeIntVariant v2 = new LargeIntVariant("-12345678901234567890");
        Assertions.assertTrue(v1.equals(v2));
    }

    @Test
    public void testLargeIntVariantHashCodeConsistency() {
        LargeIntVariant v = new LargeIntVariant("12345678901234567890");
        int hash1 = v.hashCode();
        int hash2 = v.hashCode();
        Assertions.assertEquals(hash1, hash2);
    }

    @Test
    public void testLargeIntVariantHashCodeEqualObjects() {
        LargeIntVariant v1 = new LargeIntVariant("12345678901234567890");
        LargeIntVariant v2 = new LargeIntVariant("12345678901234567890");
        Assertions.assertTrue(v1.equals(v2));
        Assertions.assertEquals(v1.hashCode(), v2.hashCode());
    }

    // ==================== StringVariant equals() and hashCode() Tests ====================

    @Test
    public void testStringVariantEqualsReflexive() {
        StringVariant v = new StringVariant(VarcharType.VARCHAR, "hello");
        Assertions.assertTrue(v.equals(v));
    }

    @Test
    public void testStringVariantEqualsSymmetric() {
        StringVariant v1 = new StringVariant(VarcharType.VARCHAR, "hello");
        StringVariant v2 = new StringVariant(VarcharType.VARCHAR, "hello");
        Assertions.assertTrue(v1.equals(v2));
        Assertions.assertTrue(v2.equals(v1));
    }

    @Test
    public void testStringVariantEqualsNull() {
        StringVariant v = new StringVariant(VarcharType.VARCHAR, "hello");
        Assertions.assertFalse(v.equals(null));
    }

    @Test
    public void testStringVariantEqualsDifferentClass() {
        StringVariant v = new StringVariant(VarcharType.VARCHAR, "hello");
        Assertions.assertFalse(v.equals("hello"));
        Assertions.assertFalse(v.equals(new IntVariant(IntegerType.INT, 100)));
    }

    @Test
    public void testStringVariantEqualsSameValue() {
        StringVariant v1 = new StringVariant(VarcharType.VARCHAR, "test string");
        StringVariant v2 = new StringVariant(VarcharType.VARCHAR, "test string");
        Assertions.assertTrue(v1.equals(v2));
    }

    @Test
    public void testStringVariantEqualsDifferentValue() {
        StringVariant v1 = new StringVariant(VarcharType.VARCHAR, "hello");
        StringVariant v2 = new StringVariant(VarcharType.VARCHAR, "world");
        Assertions.assertFalse(v1.equals(v2));
    }

    @Test
    public void testStringVariantEqualsEmptyString() {
        StringVariant v1 = new StringVariant(VarcharType.VARCHAR, "");
        StringVariant v2 = new StringVariant(VarcharType.VARCHAR, "");
        Assertions.assertTrue(v1.equals(v2));
    }

    @Test
    public void testStringVariantEqualsCaseSensitive() {
        StringVariant v1 = new StringVariant(VarcharType.VARCHAR, "Hello");
        StringVariant v2 = new StringVariant(VarcharType.VARCHAR, "hello");
        Assertions.assertFalse(v1.equals(v2));
    }

    @Test
    public void testStringVariantHashCodeConsistency() {
        StringVariant v = new StringVariant(VarcharType.VARCHAR, "hello");
        int hash1 = v.hashCode();
        int hash2 = v.hashCode();
        Assertions.assertEquals(hash1, hash2);
    }

    @Test
    public void testStringVariantHashCodeEqualObjects() {
        StringVariant v1 = new StringVariant(VarcharType.VARCHAR, "test");
        StringVariant v2 = new StringVariant(VarcharType.VARCHAR, "test");
        Assertions.assertTrue(v1.equals(v2));
        Assertions.assertEquals(v1.hashCode(), v2.hashCode());
    }

    // ==================== DateVariant equals() and hashCode() Tests ====================

    @Test
    public void testDateTimeVariantEqualsReflexive() {
        DateVariant v = new DateVariant(DateType.DATETIME, "2024-01-15T10:30:00");
        Assertions.assertTrue(v.equals(v));
    }

    @Test
    public void testDateTimeVariantEqualsSymmetric() {
        DateVariant v1 = new DateVariant(DateType.DATETIME, "2024-01-15T10:30:00");
        DateVariant v2 = new DateVariant(DateType.DATETIME, "2024-01-15T10:30:00");
        Assertions.assertTrue(v1.equals(v2));
        Assertions.assertTrue(v2.equals(v1));
    }

    @Test
    public void testDateTimeVariantEqualsNull() {
        DateVariant v = new DateVariant(DateType.DATETIME, "2024-01-15T10:30:00");
        Assertions.assertFalse(v.equals(null));
    }

    @Test
    public void testDateTimeVariantEqualsDifferentClass() {
        DateVariant v = new DateVariant(DateType.DATETIME, "2024-01-15T10:30:00");
        Assertions.assertFalse(v.equals("2024-01-15T10:30:00"));
        Assertions.assertFalse(v.equals(new IntVariant(IntegerType.INT, 100)));
    }

    @Test
    public void testDateTimeVariantEqualsSameValue() {
        Instant instant = Instant.parse("2024-01-15T10:30:00Z");
        DateVariant v1 = new DateVariant(DateType.DATETIME, instant);
        DateVariant v2 = new DateVariant(DateType.DATETIME, instant);
        Assertions.assertTrue(v1.equals(v2));
    }

    @Test
    public void testDateTimeVariantEqualsDifferentValue() {
        DateVariant v1 = new DateVariant(DateType.DATETIME, "2024-01-15T10:30:00");
        DateVariant v2 = new DateVariant(DateType.DATETIME, "2024-01-15T10:30:01");
        Assertions.assertFalse(v1.equals(v2));
    }

    @Test
    public void testDateTimeVariantEqualsWithNanos() {
        Instant instant1 = Instant.ofEpochSecond(1000, 123456789);
        Instant instant2 = Instant.ofEpochSecond(1000, 123456789);
        Instant instant3 = Instant.ofEpochSecond(1000, 123456790);

        DateVariant v1 = new DateVariant(DateType.DATETIME, instant1);
        DateVariant v2 = new DateVariant(DateType.DATETIME, instant2);
        DateVariant v3 = new DateVariant(DateType.DATETIME, instant3);

        Assertions.assertTrue(v1.equals(v2));
        Assertions.assertFalse(v1.equals(v3));
    }

    @Test
    public void testDateTimeVariantHashCodeConsistency() {
        DateVariant v = new DateVariant(DateType.DATETIME, "2024-01-15T10:30:00");
        int hash1 = v.hashCode();
        int hash2 = v.hashCode();
        Assertions.assertEquals(hash1, hash2);
    }

    @Test
    public void testDateTimeVariantHashCodeEqualObjects() {
        DateVariant v1 = new DateVariant(DateType.DATETIME, "2024-01-15T10:30:00");
        DateVariant v2 = new DateVariant(DateType.DATETIME, "2024-01-15T10:30:00");
        Assertions.assertTrue(v1.equals(v2));
        Assertions.assertEquals(v1.hashCode(), v2.hashCode());
    }

    // ==================== Cross-Variant equals() Tests ====================

    @Test
    public void testCrossVariantEquals() {
        // Different types should not be equal via equals()
        BoolVariant boolVar = new BoolVariant(true);
        IntVariant intVar = new IntVariant(IntegerType.INT, 1);
        LargeIntVariant largeIntVar = new LargeIntVariant("1");
        StringVariant stringVar = new StringVariant(VarcharType.VARCHAR, "1");

        Assertions.assertFalse(boolVar.equals(intVar));
        Assertions.assertFalse(boolVar.equals(largeIntVar));
        Assertions.assertFalse(boolVar.equals(stringVar));
        Assertions.assertFalse(intVar.equals(largeIntVar));
        Assertions.assertFalse(intVar.equals(stringVar));
        Assertions.assertFalse(largeIntVar.equals(stringVar));
    }
}
