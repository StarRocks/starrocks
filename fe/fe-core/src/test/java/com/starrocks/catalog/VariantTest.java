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

import com.baidu.bjf.remoting.protobuf.Codec;
import com.baidu.bjf.remoting.protobuf.ProtobufProxy;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;
import com.google.gson.annotations.SerializedName;
import com.google.protobuf.CodedOutputStream;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.proto.PScalarType;
import com.starrocks.proto.PTypeDesc;
import com.starrocks.proto.PTypeNode;
import com.starrocks.proto.VariantPB;
import com.starrocks.proto.VariantTypePB;
import com.starrocks.thrift.TPrimitiveType;
import com.starrocks.thrift.TTuple;
import com.starrocks.thrift.TTypeNodeType;
import com.starrocks.thrift.TVariant;
import com.starrocks.type.BooleanType;
import com.starrocks.type.CharType;
import com.starrocks.type.DateType;
import com.starrocks.type.FloatType;
import com.starrocks.type.HLLType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.PrimitiveType;
import com.starrocks.type.ScalarType;
import com.starrocks.type.Type;
import com.starrocks.type.TypeDeserializer;
import com.starrocks.type.TypeFactory;
import com.starrocks.type.TypeSerializer;
import com.starrocks.type.VarbinaryType;
import com.starrocks.type.VarcharType;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TField;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.protocol.TStruct;
import org.apache.thrift.protocol.TType;
import org.apache.thrift.transport.TMemoryBuffer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.TreeSet;

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

        // A trailing NUL is a real extra byte: BE memcompare (be/src/base/string/memcmp.h)
        // has no NUL special case, so the longer string sorts after the shorter one.
        Assertions.assertTrue(v1.compareTo(v2) > 0);
        Assertions.assertTrue(v2.compareTo(v1) < 0);
    }

    @Test
    public void testStringVariantNonAsciiUnsignedOrder() {
        // FE StringVariant ordering must equal BE unsigned-byte order.
        StringVariant z = new StringVariant(VarcharType.VARCHAR, "z");
        StringVariant e = new StringVariant(VarcharType.VARCHAR, "é"); // é, 0xC3 0xA9
        Assertions.assertTrue(z.compareTo(e) < 0);
        Assertions.assertTrue(e.compareTo(z) > 0);
        StringVariant a = new StringVariant(VarcharType.VARCHAR, "a");
        StringVariant cjk = new StringVariant(VarcharType.VARCHAR, "中"); // 中, 0xE4 0xB8 0xAD
        Assertions.assertTrue(a.compareTo(cjk) < 0);
        StringVariant abc = new StringVariant(VarcharType.VARCHAR, "abc");
        StringVariant abcd = new StringVariant(VarcharType.VARCHAR, "abcd");
        Assertions.assertTrue(abc.compareTo(abcd) < 0);
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
        Assertions.assertEquals("123", new String(tInt.getValue(), StandardCharsets.UTF_8));
    }

    @Test
    public void testVariantToThriftBoolean() {
        Variant boolVariant = new BoolVariant(true);
        TVariant tBool = boolVariant.toThrift();

        Assertions.assertTrue(tBool.isSetType());
        Assertions.assertEquals(TypeSerializer.toThrift(BooleanType.BOOLEAN), tBool.getType());
        // Boolean variants are also encoded via the `value` field.
        Assertions.assertTrue(tBool.isSetValue());
        Assertions.assertEquals("TRUE", new String(tBool.getValue(), StandardCharsets.UTF_8));
    }

    @Test
    public void testVariantToThriftString() {
        Variant strVariant = new StringVariant(VarcharType.VARCHAR, "hello");
        TVariant tStr = strVariant.toThrift();

        Assertions.assertTrue(tStr.isSetType());
        Assertions.assertEquals(TypeSerializer.toThrift(VarcharType.VARCHAR), tStr.getType());
        Assertions.assertTrue(tStr.isSetValue());
        Assertions.assertEquals("hello", new String(tStr.getValue(), StandardCharsets.UTF_8));
    }

    @Test
    public void testCharVariantTruncatesAtFirstNul() {
        // A CHAR value is canonicalized to its prefix before the first '\0', matching the BE's
        // strnlen view of a CHAR value everywhere (storage read + routing-boundary parse). VARCHAR
        // keeps the raw bytes. The NUL byte is built numerically to keep the source ASCII-clean.
        String nulValue = new String(new byte[] {'a', 0, 'z'}, StandardCharsets.UTF_8);

        Variant charVariant = Variant.of(TypeFactory.createCharType(16), nulValue);
        Assertions.assertEquals("a", charVariant.getStringValue());
        // The truncation flows into serialization too (the BE receives "a", not "a\0z").
        Assertions.assertEquals("a", new String(charVariant.toThrift().getValue(), StandardCharsets.UTF_8));

        // VARCHAR keeps the raw NUL-bearing value (the BE does not strnlen a VARCHAR value).
        Assertions.assertEquals(nulValue, Variant.of(VarcharType.VARCHAR, nulValue).getStringValue());

        // A NUL-free CHAR value is unchanged.
        Assertions.assertEquals("abc", Variant.of(TypeFactory.createCharType(16), "abc").getStringValue());
    }

    @Test
    public void testVariantToThriftDateTime() {
        Variant dtVariant = new DateVariant(DateType.DATETIME, "2024-01-15T10:30:00");
        TVariant tDt = dtVariant.toThrift();

        Assertions.assertTrue(tDt.isSetType());
        Assertions.assertEquals(TypeSerializer.toThrift(DateType.DATETIME), tDt.getType());
        // Date/time are encoded via the `value` field in TVariant.
        Assertions.assertTrue(tDt.isSetValue());
        Assertions.assertTrue(tDt.getValue().length > 0);
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
        Assertions.assertEquals("world", new String(t2.getValue(), StandardCharsets.UTF_8));

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

    // The backend parses split-boundary values with datum_from_string, which only
    // accepts the StarRocks canonical date/datetime text ("yyyy-MM-dd[ HH:mm:ss]").
    // getStringValue() (and thus the proto/thrift value) must emit that form, not an
    // ISO-8601 instant ("...T...Z"), or the storage engine rejects every date/datetime
    // split boundary and silently skips the split.

    @Test
    public void testDateTimeStringValueIsBackendCanonical() {
        Variant v = new DateVariant(DateType.DATETIME, "2024-01-15T10:30:00");
        Assertions.assertEquals("2024-01-15 10:30:00", v.getStringValue());
        Assertions.assertEquals("2024-01-15 10:30:00", new String(v.toProto().value, StandardCharsets.UTF_8));
        Assertions.assertEquals("2024-01-15 10:30:00", new String(v.toThrift().getValue(), StandardCharsets.UTF_8));
    }

    @Test
    public void testDateStringValueIsBackendCanonical() {
        Variant v = new DateVariant(DateType.DATE, "2024-01-15");
        Assertions.assertEquals("2024-01-15", v.getStringValue());
        Assertions.assertEquals("2024-01-15", new String(v.toProto().value, StandardCharsets.UTF_8));
        Assertions.assertEquals("2024-01-15", new String(v.toThrift().getValue(), StandardCharsets.UTF_8));
    }

    @Test
    public void testDateTimeStringValueKeepsMicros() {
        Variant v = new DateVariant(DateType.DATETIME, Instant.ofEpochSecond(0, 123456000));
        Assertions.assertEquals("1970-01-01 00:00:00.123456", v.getStringValue());
    }

    @Test
    public void testDateTimeStringValueIsLocaleIndependent() {
        Locale previous = Locale.getDefault();
        try {
            Locale.setDefault(Locale.forLanguageTag("ar-SA-u-nu-arab"));
            Variant v = new DateVariant(DateType.DATETIME, Instant.ofEpochSecond(0, 123456000));
            Assertions.assertEquals("1970-01-01 00:00:00.123456", v.getStringValue());
        } finally {
            Locale.setDefault(previous);
        }
    }

    @Test
    public void testDateTimeStringValueRoundTrips() {
        DateVariant original = new DateVariant(DateType.DATETIME, "2024-01-15T10:30:00");
        Variant reparsed = Variant.of(DateType.DATETIME, original.getStringValue());
        Assertions.assertEquals(original, reparsed);
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

    @Test
    public void testVariantMinMaxHelpersForBooleanAndInt() {
        Variant boolMin = Variant.minVariant(BooleanType.BOOLEAN);
        Variant boolMax = Variant.maxVariant(BooleanType.BOOLEAN);
        Assertions.assertTrue(boolMin instanceof MinVariant);
        Assertions.assertTrue(boolMax instanceof MaxVariant);

        Variant intMin = Variant.minVariant(IntegerType.INT);
        Variant intMax = Variant.maxVariant(IntegerType.INT);
        Assertions.assertTrue(intMin instanceof MinVariant);
        Assertions.assertTrue(intMax instanceof MaxVariant);
    }

    @Test
    public void testVariantMinMaxHelpersForDate() {
        Variant minDate = Variant.minVariant(DateType.DATE);
        Variant maxDate = Variant.maxVariant(DateType.DATE);
        Variant middleDate = Variant.of(DateType.DATE, "2024-01-01");

        Assertions.assertTrue(minDate instanceof MinVariant);
        Assertions.assertTrue(maxDate instanceof MaxVariant);
        Assertions.assertTrue(Variant.compatibleCompare(minDate, middleDate) <= 0);
        Assertions.assertTrue(Variant.compatibleCompare(middleDate, maxDate) <= 0);
        Assertions.assertTrue(Variant.compatibleCompare(minDate, maxDate) < 0);
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

    @Test
    public void testMinMaxVariantToThrift() {
        Variant minDate = Variant.minVariant(DateType.DATE);
        TVariant tMin = minDate.toThrift();
        Assertions.assertTrue(tMin.isSetVariant_type());
        Assertions.assertEquals(com.starrocks.thrift.TVariantType.MINIMUM, tMin.getVariant_type());

        Variant maxDate = Variant.maxVariant(DateType.DATE);
        TVariant tMax = maxDate.toThrift();
        Assertions.assertTrue(tMax.isSetVariant_type());
        Assertions.assertEquals(com.starrocks.thrift.TVariantType.MAXIMUM, tMax.getVariant_type());
    }

    @Test
    public void testMinMaxVariantFromThrift() {
        TVariant tMin = new TVariant();
        tMin.setType(TypeSerializer.toThrift(DateType.DATE));
        tMin.setVariant_type(com.starrocks.thrift.TVariantType.MINIMUM);
        Variant minVariant = Variant.fromThrift(tMin);
        Assertions.assertTrue(minVariant instanceof MinVariant);
        Assertions.assertEquals(DateType.DATE, minVariant.getType());

        TVariant tMax = new TVariant();
        tMax.setType(TypeSerializer.toThrift(DateType.DATE));
        tMax.setVariant_type(com.starrocks.thrift.TVariantType.MAXIMUM);
        Variant maxVariant = Variant.fromThrift(tMax);
        Assertions.assertTrue(maxVariant instanceof MaxVariant);
        Assertions.assertEquals(DateType.DATE, maxVariant.getType());
    }

    @Test
    public void testMinMaxVariantFromProto() {
        // Build a simple scalar PTypeDesc for DATE, consistent with TypeDeserializer.fromProtobuf.
        PTypeDesc typeDesc = new PTypeDesc();
        typeDesc.types = new ArrayList<>();

        PTypeNode node = new PTypeNode();
        node.type = TTypeNodeType.SCALAR.getValue();
        node.scalarType = new PScalarType();
        node.scalarType.type = TPrimitiveType.DATE.getValue();
        typeDesc.types.add(node);

        VariantPB pbMin = new VariantPB();
        pbMin.type = typeDesc;
        pbMin.variantType = VariantTypePB.MINIMUM;
        Variant minVariant = Variant.fromProto(pbMin);
        Assertions.assertTrue(minVariant instanceof MinVariant);
        Assertions.assertEquals(DateType.DATE, minVariant.getType());

        VariantPB pbMax = new VariantPB();
        pbMax.type = typeDesc;
        pbMax.variantType = VariantTypePB.MAXIMUM;
        Variant maxVariant = Variant.fromProto(pbMax);
        Assertions.assertTrue(maxVariant instanceof MaxVariant);
        Assertions.assertEquals(DateType.DATE, maxVariant.getType());
    }

    @Test
    public void testNullVariantFromThrift() {
        TVariant tNull = new TVariant();
        tNull.setType(TypeSerializer.toThrift(IntegerType.INT));
        tNull.setVariant_type(com.starrocks.thrift.TVariantType.NULL_VALUE);
        Variant nullVariant = Variant.fromThrift(tNull);
        Assertions.assertTrue(nullVariant instanceof NullVariant);
        Assertions.assertEquals(IntegerType.INT, nullVariant.getType());
    }

    @Test
    public void testNullVariantFromProto() {
        PTypeDesc typeDesc = new PTypeDesc();
        typeDesc.types = new ArrayList<>();

        PTypeNode node = new PTypeNode();
        node.type = TTypeNodeType.SCALAR.getValue();
        node.scalarType = new PScalarType();
        node.scalarType.type = TPrimitiveType.INT.getValue();
        typeDesc.types.add(node);

        VariantPB pbNull = new VariantPB();
        pbNull.type = typeDesc;
        pbNull.variantType = VariantTypePB.NULL_VALUE;
        Variant nullVariant = Variant.fromProto(pbNull);
        Assertions.assertTrue(nullVariant instanceof NullVariant);
        Assertions.assertEquals(IntegerType.INT, nullVariant.getType());
    }

    @Test
    public void testToProtoNormalVariant() {
        Variant variant = Variant.of(IntegerType.INT, "123");
        VariantPB pb = variant.toProto();
        Assertions.assertEquals(VariantTypePB.NORMAL_VALUE, pb.variantType);
        Assertions.assertEquals("123", new String(pb.value, StandardCharsets.UTF_8));
        Assertions.assertEquals(IntegerType.INT, TypeDeserializer.fromProtobuf(pb.type));
    }

    @Test
    public void testToProtoSpecialVariants() {
        Variant min = Variant.minVariant(DateType.DATE);
        VariantPB pbMin = min.toProto();
        Assertions.assertEquals(VariantTypePB.MINIMUM, pbMin.variantType);
        Assertions.assertNull(pbMin.value);
        Assertions.assertEquals(DateType.DATE, TypeDeserializer.fromProtobuf(pbMin.type));

        Variant max = Variant.maxVariant(DateType.DATE);
        VariantPB pbMax = max.toProto();
        Assertions.assertEquals(VariantTypePB.MAXIMUM, pbMax.variantType);
        Assertions.assertNull(pbMax.value);
        Assertions.assertEquals(DateType.DATE, TypeDeserializer.fromProtobuf(pbMax.type));

        Variant nullVar = Variant.nullVariant(IntegerType.INT);
        VariantPB pbNull = nullVar.toProto();
        Assertions.assertEquals(VariantTypePB.NULL_VALUE, pbNull.variantType);
        Assertions.assertNull(pbNull.value);
        Assertions.assertEquals(IntegerType.INT, TypeDeserializer.fromProtobuf(pbNull.type));
    }

    @Test
    public void testToProtoStringLength() {
        VarcharType varcharType = new VarcharType(10);
        Variant variant = Variant.of(varcharType, "abc");
        VariantPB pb = variant.toProto();
        Type type = TypeDeserializer.fromProtobuf(pb.type);
        Assertions.assertTrue(type instanceof ScalarType);
        Assertions.assertEquals(10, ((ScalarType) type).getLength());
    }

    @Test
    public void testNullVariantCompareOrder() {
        Variant min = Variant.minVariant(IntegerType.INT);
        Variant max = Variant.maxVariant(IntegerType.INT);
        Variant nullVar = Variant.nullVariant(IntegerType.INT);
        Variant intVar = new IntVariant(IntegerType.INT, 1);

        Assertions.assertTrue(min.compareTo(nullVar) < 0);
        Assertions.assertTrue(nullVar.compareTo(intVar) < 0);
        Assertions.assertTrue(intVar.compareTo(max) < 0);
    }

    @Test
    public void testNullVariantToThriftAndStringValue() {
        Variant nullVar = Variant.nullVariant(IntegerType.INT);
        TVariant tNull = nullVar.toThrift();
        Assertions.assertTrue(tNull.isSetVariant_type());
        Assertions.assertEquals(com.starrocks.thrift.TVariantType.NULL_VALUE, tNull.getVariant_type());
        Assertions.assertFalse(tNull.isSetValue());
        Assertions.assertEquals("NULL", nullVar.toString());
    }

    @Test
    public void testNullVariantEqualsHashCodeAndLongValue() {
        Variant nullInt1 = Variant.nullVariant(IntegerType.INT);
        Variant nullInt2 = Variant.nullVariant(IntegerType.INT);
        Variant nullBigint = Variant.nullVariant(IntegerType.BIGINT);

        Assertions.assertEquals(nullInt1, nullInt2);
        Assertions.assertEquals(nullInt1.hashCode(), nullInt2.hashCode());
        Assertions.assertNotEquals(nullInt1, nullBigint);
        Assertions.assertThrows(IllegalStateException.class, nullInt1::getLongValue);
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

    @Test
    public void testVariantOfMinMax() {
        // Min/Max sentinels should only be created via dedicated helpers, not via Variant.of("MIN"/"MAX").
        Variant maxInt = Variant.maxVariant(IntegerType.INT);
        Variant minInt = Variant.minVariant(IntegerType.INT);
        Assertions.assertTrue(maxInt instanceof MaxVariant);
        Assertions.assertTrue(minInt instanceof MinVariant);
        Assertions.assertEquals(IntegerType.INT, maxInt.getType());
        Assertions.assertEquals(IntegerType.INT, minInt.getType());

        // String type: "MAX"/"MIN" should be treated as normal string values.
        Variant strMaxLiteral = Variant.of(VarcharType.VARCHAR, "MAX");
        Variant strMinLiteral = Variant.of(VarcharType.VARCHAR, "MIN");
        Assertions.assertTrue(strMaxLiteral instanceof StringVariant);
        Assertions.assertTrue(strMinLiteral instanceof StringVariant);

        // Min/Max sentinels for string types are only created explicitly.
        Variant maxStrSentinel = Variant.maxVariant(VarcharType.VARCHAR);
        Variant minStrSentinel = Variant.minVariant(VarcharType.VARCHAR);
        Assertions.assertTrue(maxStrSentinel instanceof MaxVariant);
        Assertions.assertTrue(minStrSentinel instanceof MinVariant);

        // Ensure sentinels are different from literal string variants.
        Assertions.assertNotEquals(maxStrSentinel, strMaxLiteral);
        Assertions.assertNotEquals(minStrSentinel, strMinLiteral);
    }

    @Test
    public void testMinMaxVariantEqualsHashCode() {
        Variant min1 = Variant.minVariant(IntegerType.INT);
        Variant min2 = Variant.minVariant(IntegerType.BIGINT);
        Variant max1 = Variant.maxVariant(IntegerType.INT);
        Variant max2 = Variant.maxVariant(IntegerType.BIGINT);

        // equals and hashCode are type-sensitive
        Assertions.assertNotEquals(min1, min2);
        Assertions.assertNotEquals(min1.hashCode(), min2.hashCode());
        Assertions.assertNotEquals(max1, max2);
        Assertions.assertNotEquals(max1.hashCode(), max2.hashCode());

        // compareTo is type-insensitive (assumes callers handle type compatibility)
        Assertions.assertEquals(0, min1.compareTo(min2));
        Assertions.assertEquals(0, max1.compareTo(max2));

        // In-equality between Min and Max
        Assertions.assertNotEquals(min1, max1);
        Assertions.assertTrue(min1.compareTo(max1) < 0);
    }

    // ==================== DecimalVariant Tests ====================

    @Test
    public void testDecimalVariantBasics() {
        ScalarType type = TypeFactory.createDecimalV3Type(PrimitiveType.DECIMAL64, 18, 2);
        Variant v = Variant.of(type, "1234.56");
        Assertions.assertTrue(v instanceof DecimalVariant);
        Assertions.assertEquals("1234.56", v.getStringValue());
        Assertions.assertEquals(type, v.getType());
        Assertions.assertEquals(1234L, v.getLongValue());
    }

    @Test
    public void testDecimalVariantToPlainStringNoScientificNotation() {
        // new BigDecimal("0.0000001").toString() would render "1E-7"; toPlainString must not.
        ScalarType type = TypeFactory.createDecimalV3Type(PrimitiveType.DECIMAL128, 38, 10);
        Variant v = Variant.of(type, "0.0000001");
        Assertions.assertEquals("0.0000001", v.getStringValue());

        Variant negative = Variant.of(type, "-12345.6789012345");
        Assertions.assertEquals("-12345.6789012345", negative.getStringValue());
    }

    @Test
    public void testDecimalVariantCompareToIsValueOrdered() {
        ScalarType type = TypeFactory.createDecimalV3Type(PrimitiveType.DECIMAL64, 18, 4);
        Variant low = Variant.of(type, "1.0000");
        Variant high = Variant.of(type, "2.5000");
        Variant negative = Variant.of(type, "-3.0000");
        Assertions.assertTrue(low.compareTo(high) < 0);
        Assertions.assertTrue(high.compareTo(low) > 0);
        Assertions.assertTrue(negative.compareTo(low) < 0);
        // BigDecimal.compareTo is scale-independent: 2.5 == 2.50000.
        Assertions.assertEquals(0, high.compareTo(Variant.of(type, "2.50000")));
    }

    @Test
    public void testDecimalVariantInvalidStringThrows() {
        ScalarType type = TypeFactory.createDecimalV3Type(PrimitiveType.DECIMAL64, 18, 2);
        Assertions.assertThrows(IllegalArgumentException.class, () -> Variant.of(type, "not-a-number"));
    }

    @Test
    public void testDecimalV2VariantConstructs() {
        ScalarType type = TypeFactory.createDecimalV2Type(27, 9);
        Variant v = Variant.of(type, "123.456789");
        Assertions.assertTrue(v instanceof DecimalVariant);
        Assertions.assertEquals("123.456789", v.getStringValue());
    }

    @Test
    public void testDecimalVariantRejectsNonDecimalType() {
        // The (Type, BigDecimal) constructor is public; guard against a non-decimal type so a
        // misuse fails loudly here instead of producing a Variant whose ScalarType cast in
        // BoundaryPlanner.validateValueAgainstColumn would later throw a ClassCastException.
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> new DecimalVariant(IntegerType.BIGINT, BigDecimal.ONE));
    }

    @Test
    public void testDecimalVariantPreservesExactFractionalText() {
        // FE does NOT pre-round or reject over-scale samples: it stores and sorts the exact
        // value. The backend applies the single authoritative half-up rounding to the column
        // scale when it parses the boundary (the same path it uses for the loaded data), so FE
        // and BE stay consistent and an over-scale sample never yields a wrong boundary — at
        // worst an over-precision integer part triggers the substrate's identical-fallback.
        // Rejecting it in FE would regress a decimal load to SAMPLE_FAILED (no pre-split).
        ScalarType type = TypeFactory.createDecimalV3Type(PrimitiveType.DECIMAL64, 18, 2);
        Variant v = Variant.of(type, "1.239");
        Assertions.assertEquals("1.239", v.getStringValue());
        Assertions.assertTrue(v.compareTo(Variant.of(type, "1.24")) < 0);
    }

    @Test
    public void testDecimalVariantEqualsAndHashCode() {
        ScalarType type = TypeFactory.createDecimalV3Type(PrimitiveType.DECIMAL64, 18, 2);
        Variant a = Variant.of(type, "12.34");
        Variant b = Variant.of(type, "12.34");
        Assertions.assertEquals(a, b);
        Assertions.assertEquals(a.hashCode(), b.hashCode());
        Assertions.assertNotEquals(a, null);
        Assertions.assertNotEquals(a, Variant.of(IntegerType.BIGINT, "12"));
        // equals is value-based and consistent with compareTo: the same value at a different
        // scale is equal and hashes equally (so Tuple/Set dedup works for decimal boundaries).
        Variant scaled = Variant.of(type, "12.3400");
        Assertions.assertEquals(a, scaled);
        Assertions.assertEquals(a.hashCode(), scaled.hashCode());
        Assertions.assertEquals(0, a.compareTo(scaled));
    }

    @Test
    public void testDecimalVariantGsonRoundTrip() {
        ScalarType type = TypeFactory.createDecimalV3Type(PrimitiveType.DECIMAL64, 18, 2);
        Variant original = Variant.of(type, "1234.56");

        String json = GsonUtils.GSON.toJson(original, Variant.class);
        Variant restored = GsonUtils.GSON.fromJson(json, Variant.class);

        Assertions.assertTrue(restored instanceof DecimalVariant);
        Assertions.assertEquals("1234.56", restored.getStringValue());
        ScalarType restoredType = (ScalarType) restored.getType();
        Assertions.assertEquals(PrimitiveType.DECIMAL64, restoredType.getPrimitiveType());
        Assertions.assertEquals(18, restoredType.getScalarPrecision());
        Assertions.assertEquals(2, restoredType.getScalarScale());
        Assertions.assertEquals(0, original.compareTo(restored));
    }

    @Test
    public void testDecimalVariantThriftRoundTrip() {
        ScalarType type = TypeFactory.createDecimalV3Type(PrimitiveType.DECIMAL64, 18, 4);
        Variant v = Variant.of(type, "12345.6789");
        TVariant t = v.toThrift();
        Assertions.assertEquals("12345.6789", new String(t.getValue(), StandardCharsets.UTF_8));

        Variant back = Variant.fromThrift(t);
        Assertions.assertTrue(back instanceof DecimalVariant);
        ScalarType backType = (ScalarType) back.getType();
        Assertions.assertEquals(PrimitiveType.DECIMAL64, backType.getPrimitiveType());
        Assertions.assertEquals(18, backType.getScalarPrecision());
        Assertions.assertEquals(4, backType.getScalarScale());
        Assertions.assertEquals(0, v.compareTo(back));
    }

    @Test
    public void testDecimalVariantProtoRoundTrip() {
        ScalarType type = TypeFactory.createDecimalV3Type(PrimitiveType.DECIMAL128, 38, 10);
        Variant v = Variant.of(type, "-0.0000000001");
        VariantPB pb = v.toProto();
        Assertions.assertEquals("-0.0000000001", new String(pb.value, StandardCharsets.UTF_8));

        Variant back = Variant.fromProto(pb);
        Assertions.assertTrue(back instanceof DecimalVariant);
        ScalarType backType = (ScalarType) back.getType();
        Assertions.assertEquals(PrimitiveType.DECIMAL128, backType.getPrimitiveType());
        Assertions.assertEquals(38, backType.getScalarPrecision());
        Assertions.assertEquals(10, backType.getScalarScale());
        Assertions.assertEquals(0, v.compareTo(back));
    }

    @Test
    public void testDecimalV2ProtoRoundTripPreservesScale() {
        // Regression guard: TypeDeserializer.createType used precision as the DECIMALV2
        // scale, so a round-trip silently widened scale to precision.
        ScalarType type = TypeFactory.createDecimalV2Type(27, 9);
        Variant v = Variant.of(type, "123.456");
        VariantPB pb = v.toProto();

        Variant back = Variant.fromProto(pb);
        ScalarType backType = (ScalarType) back.getType();
        Assertions.assertEquals(PrimitiveType.DECIMALV2, backType.getPrimitiveType());
        Assertions.assertEquals(27, backType.getScalarPrecision());
        Assertions.assertEquals(9, backType.getScalarScale());
    }

    // ==================== StringVariant raw bytes Tests ====================

    private static byte[] bytes(int... values) {
        byte[] result = new byte[values.length];
        for (int i = 0; i < values.length; i++) {
            result[i] = (byte) values[i];
        }
        return result;
    }

    private static final Type CHAR_TYPE = TypeFactory.createCharType(16);
    private static final Type VARBINARY_TYPE = TypeFactory.createVarbinary(16);

    // The two variants are the same value: equal both ways, compare as 0 and hash alike.
    private static void assertSameValue(Variant expected, Variant actual) {
        Assertions.assertEquals(expected, actual);
        Assertions.assertEquals(actual, expected);
        Assertions.assertEquals(0, expected.compareTo(actual));
        Assertions.assertEquals(0, actual.compareTo(expected));
        Assertions.assertEquals(expected.hashCode(), actual.hashCode());
    }

    @Test
    public void testStringVariantFromValidBytes() {
        byte[] raw = bytes(0xC3, 0xA9);
        Variant fromBytes = new StringVariant(VarcharType.VARCHAR, raw);
        Variant fromString = Variant.of(VarcharType.VARCHAR, "é");
        Assertions.assertEquals("é", fromBytes.getStringValue());
        Assertions.assertArrayEquals(raw, fromBytes.getBytesValue());
        assertSameValue(fromString, fromBytes);
        String json = GsonUtils.GSON.toJson(fromBytes, Variant.class);
        Assertions.assertEquals(GsonUtils.GSON.toJson(fromString, Variant.class), json);
    }

    @Test
    public void testStringVariantFromInvalidBytes() {
        for (Type type : new Type[] {VarcharType.VARCHAR, VARBINARY_TYPE}) {
            byte[] c3 = bytes('a', 0xC3);
            byte[] ff = bytes('a', 0xFF);
            Variant v1 = new StringVariant(type, c3);
            Variant v2 = new StringVariant(type, ff);
            Assertions.assertEquals("a�", v1.getStringValue());
            Assertions.assertEquals("a�", v2.getStringValue());
            Assertions.assertArrayEquals(c3, v1.getBytesValue());
            Assertions.assertArrayEquals(ff, v2.getBytesValue());
            Assertions.assertNotEquals(v1, v2);
            Assertions.assertTrue(v1.compareTo(v2) < 0);
            Assertions.assertTrue(v2.compareTo(v1) > 0);
            Assertions.assertNotEquals(v1.hashCode(), v2.hashCode());
            Variant replacement = new StringVariant(type, bytes('a', 0xEF, 0xBF, 0xBD));
            Assertions.assertNotEquals(replacement, v1);
            Assertions.assertNotEquals(v1, replacement);
        }
    }

    @Test
    public void testStringVariantCharCutsAtNulInBytes() {
        Variant fromBytes = new StringVariant(CHAR_TYPE, bytes('a', 'b', 0, 0xFF));
        Variant fromString = Variant.of(CHAR_TYPE, "ab\0x");
        assertSameValue(fromString, fromBytes);
        Assertions.assertArrayEquals("ab".getBytes(StandardCharsets.UTF_8), fromBytes.getBytesValue());
    }

    @Test
    public void testStringVariantOrderMatchesUnsignedBytes() {
        byte[][] raws = {
                bytes('a'),
                bytes('a', 0x7F),
                bytes('a', 0xC3, 0xA9),
                bytes('a', 0xEF, 0xBF, 0xBD),
                bytes('a', 0xF0, 0x9F, 0x98, 0x80),
                bytes('a', 0xFF),
                bytes('a', 0xC3),
        };
        List<Variant> variants = new ArrayList<>();
        for (byte[] raw : raws) {
            variants.add(new StringVariant(VarcharType.VARCHAR, raw));
        }
        List<Variant> shuffled = new ArrayList<>(variants);
        Collections.reverse(shuffled);
        Collections.sort(shuffled);
        List<byte[]> expected = new ArrayList<>(Arrays.asList(raws));
        expected.sort(Arrays::compareUnsigned);
        for (int i = 0; i < raws.length; i++) {
            Assertions.assertArrayEquals(expected.get(i), shuffled.get(i).getBytesValue(), "position " + i);
        }
    }

    @Test
    public void testStringVariantBytesAreOwned() {
        byte[] input = bytes('a', 0xC3);
        StringVariant variant = new StringVariant(VarcharType.VARCHAR, input);
        StringVariant other = new StringVariant(VarcharType.VARCHAR, bytes('a', 0xFF));
        Tuple tuple = new Tuple(Arrays.asList(variant));
        Tuple otherTuple = new Tuple(Arrays.asList(other));
        TreeSet<Tuple> tree = new TreeSet<>();
        tree.add(tuple);
        tree.add(otherTuple);
        Set<Tuple> hash = new HashSet<>();
        hash.add(tuple);
        hash.add(otherTuple);

        input[1] = (byte) 0xFF;
        byte[] returned = variant.getBytesValue();
        returned[1] = (byte) 0xFF;

        Assertions.assertArrayEquals(bytes('a', 0xC3), variant.getBytesValue());
        Assertions.assertEquals(tuple, tree.first());
        Assertions.assertTrue(tree.contains(tuple));
        Assertions.assertTrue(hash.contains(tuple));
        Assertions.assertEquals(2, hash.size());
    }

    // Records as the released writer persisted them for valid UTF-8 values, pinned verbatim.
    static final String LEGACY_VARCHAR =
            "{\"clazz\":\"StringVariant\",\"value\":\"café\","
                    + "\"type\":{\"type\":\"VARCHAR\",\"len\":-1,\"precision\":0,\"scale\":0}}";
    static final String LEGACY_VARBINARY =
            "{\"clazz\":\"StringVariant\",\"value\":\"abc\","
                    + "\"type\":{\"clazz\":\"ScalarType\",\"type\":\"VARBINARY\",\"len\":16,"
                    + "\"precision\":0,\"scale\":0}}";
    static final String LEGACY_CHAR =
            "{\"clazz\":\"StringVariant\",\"value\":\"ab\","
                    + "\"type\":{\"clazz\":\"ScalarType\",\"type\":\"CHAR\",\"len\":16,"
                    + "\"precision\":0,\"scale\":0}}";
    static final String LEGACY_HLL =
            "{\"clazz\":\"StringVariant\",\"value\":\"x\","
                    + "\"type\":{\"type\":\"HLL\",\"len\":16385,\"precision\":0,\"scale\":0}}";
    static final String LEGACY_BINARY =
            "{\"clazz\":\"StringVariant\",\"value\":\"ab\","
                    + "\"type\":{\"clazz\":\"ScalarType\",\"type\":\"BINARY\",\"len\":8,"
                    + "\"precision\":0,\"scale\":0}}";
    static final String LEGACY_NULL_TYPE_EMPTY = "{\"clazz\":\"StringVariant\",\"value\":\"\"}";

    private static Type binaryType() {
        ScalarType type = new ScalarType(PrimitiveType.BINARY);
        type.setLength(8);
        return type;
    }

    private static Variant legacyCharRecord(String text) {
        Assertions.assertTrue(LEGACY_CHAR.contains("\"value\":\"ab\""));
        return GsonUtils.GSON.fromJson(LEGACY_CHAR.replace("\"value\":\"ab\"", "\"value\":\"" + text + "\""),
                Variant.class);
    }

    private static void assertSameType(Type expected, Type actual) {
        Assertions.assertEquals(expected.getPrimitiveType(), actual.getPrimitiveType());
        Assertions.assertEquals(((ScalarType) expected).getLength(), ((ScalarType) actual).getLength());
    }

    // Valid UTF-8 values are written exactly as the released writer wrote them.
    @Test
    public void testValidValuesKeepReleasedJson() {
        Object[][] cases = {
                {VarcharType.VARCHAR, "café", LEGACY_VARCHAR},
                {VARBINARY_TYPE, "abc", LEGACY_VARBINARY},
                {CHAR_TYPE, "ab", LEGACY_CHAR},
                {HLLType.HLL, "x", LEGACY_HLL},
                {binaryType(), "ab", LEGACY_BINARY},
        };
        for (Object[] c : cases) {
            Type type = (Type) c[0];
            String text = (String) c[1];
            String expected = (String) c[2];
            Assertions.assertEquals(expected, GsonUtils.GSON.toJson(Variant.of(type, text), Variant.class));
            Assertions.assertEquals(expected,
                    GsonUtils.GSON.toJson(Variant.of(type, text.getBytes(StandardCharsets.UTF_8)), Variant.class));
        }
    }

    @Test
    public void testNullTypeValueOmitsType() {
        Assertions.assertEquals(LEGACY_NULL_TYPE_EMPTY, GsonUtils.GSON.toJson(new StringVariant(null, ""), Variant.class));
    }

    @Test
    public void testInvalidValueUsesBytesObject() {
        Object[][] cases = {
                {VarcharType.VARCHAR, bytes('a', 0xFF)},
                {VARBINARY_TYPE, bytes('a', 0, 0xFF)},
                {VarbinaryType.VARBINARY, bytes('a', 0, 0xFF)},
        };
        for (Object[] c : cases) {
            Type type = (Type) c[0];
            byte[] raw = (byte[]) c[1];
            Variant original = Variant.of(type, raw);
            String json = GsonUtils.GSON.toJson(original, Variant.class);
            JsonObject object = JsonParser.parseString(json).getAsJsonObject();
            Assertions.assertEquals("StringVariant", object.get("clazz").getAsString(), json);
            Assertions.assertTrue(object.get("value").isJsonObject(), json);
            Assertions.assertEquals(Base64.getEncoder().encodeToString(raw),
                    object.getAsJsonObject("value").get("bytes").getAsString(), json);
            Assertions.assertTrue(object.has("type"), json);

            Variant loaded = GsonUtils.GSON.fromJson(json, Variant.class);
            Assertions.assertArrayEquals(raw, loaded.getBytesValue());
            assertSameType(type, loaded.getType());
            assertSameValue(original, loaded);
        }
    }

    // The two shapes are told apart by JSON type only: text that looks like base64 is still text.
    @Test
    public void testBase64LookingTextStaysText() {
        Variant original = Variant.of(VarcharType.VARCHAR, "YcM=");
        String json = GsonUtils.GSON.toJson(original, Variant.class);
        JsonObject object = JsonParser.parseString(json).getAsJsonObject();
        Assertions.assertTrue(object.get("value").isJsonPrimitive(), json);
        Assertions.assertEquals("YcM=", object.get("value").getAsString(), json);
        Variant loaded = GsonUtils.GSON.fromJson(json, Variant.class);
        Assertions.assertArrayEquals(bytes('Y', 'c', 'M', '='), loaded.getBytesValue());
    }

    @Test
    public void testReplacementCharacterIsText() {
        String valid = GsonUtils.GSON.toJson(
                Variant.of(VarcharType.VARCHAR, bytes('a', 0xEF, 0xBF, 0xBD)), Variant.class);
        Assertions.assertTrue(JsonParser.parseString(valid).getAsJsonObject().get("value").isJsonPrimitive(), valid);
        String invalid = GsonUtils.GSON.toJson(Variant.of(VarcharType.VARCHAR, bytes('a', 0xFF)), Variant.class);
        Assertions.assertTrue(JsonParser.parseString(invalid).getAsJsonObject().get("value").isJsonObject(), invalid);
    }

    private static class OlderReaderRecord {
        @SerializedName("value")
        String value;
    }

    // A reader that expects "value" to be a string (the released one) must fail on the bytes object.
    @Test
    public void testOlderReaderRejectsInvalidShape() {
        String invalid = GsonUtils.GSON.toJson(Variant.of(VarcharType.VARCHAR, bytes('a', 0xFF)), Variant.class);
        Assertions.assertThrows(JsonSyntaxException.class, () -> new Gson().fromJson(invalid, OlderReaderRecord.class));
        String valid = GsonUtils.GSON.toJson(Variant.of(VarcharType.VARCHAR, "café"), Variant.class);
        Assertions.assertEquals("café", new Gson().fromJson(valid, OlderReaderRecord.class).value);
    }

    @Test
    public void testEmptyBytesObjectReadsEmptyValue() {
        Variant loaded = GsonUtils.GSON.fromJson("{\"clazz\":\"StringVariant\",\"value\":{\"bytes\":\"\"},"
                + "\"type\":{\"type\":\"VARCHAR\",\"len\":-1,\"precision\":0,\"scale\":0}}", Variant.class);
        Assertions.assertEquals(0, loaded.getBytesValue().length);
        Assertions.assertEquals(PrimitiveType.VARCHAR, loaded.getType().getPrimitiveType());
    }

    @Test
    public void testCharBytesObjectCutsAtNul() {
        String encoded = Base64.getEncoder().encodeToString(bytes('a', 'b', 0, 0xFF));
        Variant loaded = GsonUtils.GSON.fromJson("{\"clazz\":\"StringVariant\",\"value\":{\"bytes\":\""
                + encoded + "\"},\"type\":{\"clazz\":\"ScalarType\",\"type\":\"CHAR\",\"len\":16,"
                + "\"precision\":0,\"scale\":0}}", Variant.class);
        Assertions.assertArrayEquals(bytes('a', 'b'), loaded.getBytesValue());
        Assertions.assertEquals(PrimitiveType.CHAR, loaded.getType().getPrimitiveType());
    }

    @Test
    public void testUnreadableValueRejected() {
        String type = "\"type\":{\"type\":\"VARCHAR\",\"len\":-1,\"precision\":0,\"scale\":0}";
        String[] values = {
                null, "null", "12", "true", "[\"a\"]", "{}", "{\"other\":\"YQ==\"}",
                "{\"bytes\":1234}", "{\"bytes\":true}", "{\"bytes\":null}", "{\"bytes\":[\"YQ==\"]}",
                "{\"bytes\":\"!\"}",
        };
        for (String value : values) {
            String json = "{\"clazz\":\"StringVariant\","
                    + (value == null ? "" : "\"value\":" + value + ",") + type + "}";
            Assertions.assertThrows(JsonParseException.class,
                    () -> GsonUtils.GSON.fromJson(json, Variant.class), json);
        }
    }

    @Test
    public void testLegacyStringVariantRecordsLoad() {
        Object[][] cases = {
                {LEGACY_VARCHAR, VarcharType.VARCHAR, "café"},
                {LEGACY_VARBINARY, VARBINARY_TYPE, "abc"},
                {LEGACY_CHAR, CHAR_TYPE, "ab"},
                {LEGACY_HLL, HLLType.HLL, "x"},
                {LEGACY_BINARY, binaryType(), "ab"},
        };
        for (Object[] c : cases) {
            String json = (String) c[0];
            Type type = (Type) c[1];
            Variant loaded = GsonUtils.GSON.fromJson(json, Variant.class);
            Assertions.assertInstanceOf(StringVariant.class, loaded, json);
            assertSameValue(Variant.of(type, (String) c[2]), loaded);
            assertSameType(type, loaded.getType());
            // A loaded type is a plain ScalarType, so only the label and the text are compared here.
            JsonObject rewritten = JsonParser.parseString(GsonUtils.GSON.toJson(loaded, Variant.class))
                    .getAsJsonObject();
            Assertions.assertEquals("StringVariant", rewritten.get("clazz").getAsString());
            Assertions.assertEquals(c[2], rewritten.get("value").getAsString());
        }

        // A CHAR value ends at its first NUL; the type comes after the value in the legacy field order.
        Variant legacyChar = legacyCharRecord("ab\\u0000z");
        assertSameValue(Variant.of(CHAR_TYPE, "ab"), legacyChar);
        Assertions.assertArrayEquals(bytes('a', 'b'), legacyChar.getBytesValue());

        // VARCHAR keeps every byte.
        Variant varchar = GsonUtils.GSON.fromJson(
                LEGACY_VARCHAR.replace("\"value\":\"café\"", "\"value\":\"ab\\u0000z\""), Variant.class);
        Assertions.assertArrayEquals(bytes('a', 'b', 0, 'z'), varchar.getBytesValue());
        Assertions.assertNotEquals(Variant.of(VarcharType.VARCHAR, "ab"), varchar);

        // The legacy record without a type loads with a null type and an empty value.
        Variant empty = GsonUtils.GSON.fromJson(LEGACY_NULL_TYPE_EMPTY, Variant.class);
        Assertions.assertInstanceOf(StringVariant.class, empty);
        Assertions.assertNull(empty.getType());
        Assertions.assertEquals(0, empty.getBytesValue().length);
        Assertions.assertEquals(LEGACY_NULL_TYPE_EMPTY, GsonUtils.GSON.toJson(empty, Variant.class));
    }

    @Test
    public void testHllAndBinaryKeepNulBytes() {
        Type[] types = {HLLType.HLL, binaryType()};
        for (Type type : types) {
            byte[] raw = bytes('a', 0, 'z');
            Assertions.assertArrayEquals(raw, Variant.of(type, raw).getBytesValue());
            Assertions.assertArrayEquals(raw, Variant.of(type, "a\0z").getBytesValue());
            Assertions.assertInstanceOf(StringVariant.class, Variant.of(type, raw));
            Assertions.assertInstanceOf(StringVariant.class, Variant.of(type, "a\0z"));
        }
    }

    @Test
    public void testCompareToOtherVariantIsLexical() {
        Assertions.assertTrue(Variant.of(VarcharType.VARCHAR, "10").compareTo(new IntVariant(IntegerType.INT, 2)) < 0);
    }

    @Test
    public void testCompatibleCompareRebuildsFromBytes() {
        Variant legacyChar = legacyCharRecord("12\\u0000z");
        Variant twelve = new IntVariant(IntegerType.INT, 12);
        Assertions.assertEquals(0, Variant.compatibleCompare(legacyChar, twelve));
        Assertions.assertEquals(0, Variant.compatibleCompare(twelve, legacyChar));
    }

    // ==================== Wire bytes Tests ====================

    private static byte[] repeated(int ch, int count, int... tail) {
        byte[] result = new byte[count + tail.length];
        Arrays.fill(result, 0, count, (byte) ch);
        for (int i = 0; i < tail.length; i++) {
            result[count + i] = (byte) tail[i];
        }
        return result;
    }

    private static VariantPB protoRoundTrip(Variant variant) throws Exception {
        Codec<VariantPB> codec = ProtobufProxy.create(VariantPB.class);
        return codec.decode(codec.encode(variant.toProto()));
    }

    private static TVariant thriftRoundTrip(Variant variant) throws Exception {
        return thriftRoundTrip(variant, new TBinaryProtocol.Factory());
    }

    private static TVariant thriftRoundTrip(Variant variant, TProtocolFactory protocol) throws Exception {
        byte[] encoded = new TSerializer(protocol).serialize(variant.toThrift());
        TVariant decoded = new TVariant();
        new TDeserializer(protocol).deserialize(decoded, encoded);
        return decoded;
    }

    @Test
    public void testWireRoundTripKeepsInvalidBytes() throws Exception {
        List<Type> types = Arrays.asList(CHAR_TYPE, VarcharType.VARCHAR, VarbinaryType.VARBINARY);
        List<byte[]> values = Arrays.asList(
                repeated('p', 10, 0xC3, 0xA9, 0xFF),
                repeated('p', 35, 0xC3),
                bytes('a', 'b', 'c'));
        for (Type type : types) {
            for (byte[] raw : values) {
                Variant variant = Variant.of(type, raw);
                Variant fromProto = Variant.fromProto(protoRoundTrip(variant));
                Variant fromThrift = Variant.fromThrift(thriftRoundTrip(variant));
                Variant fromCompact = Variant.fromThrift(thriftRoundTrip(variant, new TCompactProtocol.Factory()));
                Assertions.assertArrayEquals(raw, fromProto.getBytesValue(), type.toString());
                Assertions.assertArrayEquals(raw, fromThrift.getBytesValue(), type.toString());
                Assertions.assertArrayEquals(raw, fromCompact.getBytesValue(), type.toString());
                Assertions.assertEquals(variant, fromProto);
                Assertions.assertEquals(variant, fromThrift);
                Assertions.assertEquals(variant, fromCompact);
            }
        }
    }

    @Test
    public void testWireOutputIsOwnedAndNormalized() {
        byte[] raw = repeated('p', 10, 0xC3, 0xA9, 0xFF);
        Variant variant = Variant.of(VarcharType.VARCHAR, raw);
        Tuple tuple = new Tuple(Collections.singletonList(variant));
        Set<Tuple> hashSet = new HashSet<>(Collections.singletonList(tuple));
        Set<Tuple> treeSet = new TreeSet<>(Collections.singletonList(tuple));

        VariantPB pb = variant.toProto();
        Arrays.fill(pb.value, (byte) 'x');

        Assertions.assertArrayEquals(raw, variant.getBytesValue());
        Assertions.assertTrue(hashSet.contains(tuple));
        Assertions.assertTrue(treeSet.contains(tuple));
        Assertions.assertEquals(variant, Variant.of(VarcharType.VARCHAR, raw));

        Variant legacy = legacyCharRecord("ab\\u0000z");
        Assertions.assertArrayEquals(bytes('a', 'b'), legacy.toProto().value);
        Assertions.assertArrayEquals(bytes('a', 'b'), legacy.toThrift().getValue());
    }

    // The binary field is written exactly as the old string field was, so peers on either side of the change read
    // each other's values.
    @Test
    public void testThriftBinaryValueMatchesLegacyString() throws Exception {
        String text = "pé😀";
        for (TProtocolFactory protocol : Arrays.asList(new TBinaryProtocol.Factory(), new TCompactProtocol.Factory())) {
            TVariant variant = new TVariant();
            variant.setValue(text.getBytes(StandardCharsets.UTF_8));
            byte[] generated = new TSerializer(protocol).serialize(variant);

            TMemoryBuffer buffer = new TMemoryBuffer(64);
            TProtocol legacy = protocol.getProtocol(buffer);
            legacy.writeStructBegin(new TStruct("TVariant"));
            legacy.writeFieldBegin(new TField("value", TType.STRING, (short) 2));
            legacy.writeString(text);
            legacy.writeFieldEnd();
            legacy.writeFieldStop();
            legacy.writeStructEnd();
            Assertions.assertArrayEquals(Arrays.copyOf(buffer.getArray(), buffer.length()), generated);
        }
    }

    @Test
    public void testWireBytesMatchWhatBeWrites() throws Exception {
        byte[] raw = repeated('p', 10, 0xC3, 0xA9, 0xFF);
        Variant variant = Variant.of(VarcharType.VARCHAR, raw);

        VariantPB pb = variant.toProto();
        Codec<VariantPB> codec = ProtobufProxy.create(VariantPB.class);
        byte[] encoded = codec.encode(pb);

        // protobuf-java writes the same three fields in field-number order, as the BE's
        // generated writer does.
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        CodedOutputStream cos = CodedOutputStream.newInstance(out);
        Codec<PTypeDesc> typeCodec = ProtobufProxy.create(PTypeDesc.class);
        cos.writeByteArray(1, typeCodec.encode(pb.type));
        cos.writeByteArray(2, raw);
        cos.writeEnum(3, VariantTypePB.NORMAL_VALUE.value());
        cos.flush();
        Assertions.assertArrayEquals(out.toByteArray(), encoded);
    }

    @Test
    public void testWireRoundTripEmptyValue() throws Exception {
        Variant emptyVarchar = Variant.of(VarcharType.VARCHAR, "");
        Variant nulChar = Variant.of(CHAR_TYPE, bytes(0, 'a'));
        for (Variant variant : Arrays.asList(emptyVarchar, nulChar)) {
            Variant expected = Variant.of(variant.getType(), "");
            Assertions.assertEquals(0, variant.toProto().value.length);
            Assertions.assertEquals(0, variant.toThrift().getValue().length);
            Variant fromProto = Variant.fromProto(protoRoundTrip(variant));
            Variant fromThrift = Variant.fromThrift(thriftRoundTrip(variant));
            Assertions.assertEquals(0, fromProto.getBytesValue().length);
            Assertions.assertEquals(0, fromThrift.getBytesValue().length);
            Assertions.assertEquals(expected, fromProto);
            Assertions.assertEquals(expected, fromThrift);
        }
    }

    @Test
    public void testWireRoundTripNonStringUnchanged() throws Exception {
        List<Variant> variants = Arrays.asList(
                Variant.of(IntegerType.INT, "123"),
                Variant.of(IntegerType.BIGINT, "-9876543210"),
                Variant.of(IntegerType.LARGEINT, "12345678901234567890"),
                Variant.of(DateType.DATE, "2024-01-15"),
                Variant.of(DateType.DATETIME, "2024-01-15 10:30:00"),
                Variant.of(TypeFactory.createDecimalV3Type(PrimitiveType.DECIMAL64, 18, 4), "12345.6789"),
                Variant.of(BooleanType.BOOLEAN, "true"),
                Variant.nullVariant(IntegerType.INT),
                Variant.minVariant(DateType.DATE),
                Variant.maxVariant(DateType.DATE));
        for (Variant variant : variants) {
            Variant fromProto = Variant.fromProto(protoRoundTrip(variant));
            Variant fromThrift = Variant.fromThrift(thriftRoundTrip(variant));
            Assertions.assertEquals(variant, fromProto, variant.toString());
            Assertions.assertEquals(variant, fromThrift, variant.toString());
            Assertions.assertEquals(0, variant.compareTo(fromProto));
            Assertions.assertEquals(0, variant.compareTo(fromThrift));
        }
    }
}
