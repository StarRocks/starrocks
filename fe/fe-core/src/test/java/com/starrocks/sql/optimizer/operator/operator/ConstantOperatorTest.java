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

package com.starrocks.sql.optimizer.operator.operator;

import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Type;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.Optional;

public class ConstantOperatorTest {
    @Test
    public void testCastToCharTruncatesToLength() {
        // POST-1335 / QuickSight: CAST(1775580223839 AS CHAR(10)) -> "1775580223" (first 10 chars).
        ConstantOperator bigint = ConstantOperator.createBigint(1775580223839L);
        ConstantOperator asChar10 = bigint.castToExplicitly(ScalarType.createCharType(10)).get();
        Assertions.assertEquals("1775580223", asChar10.getVarchar());
        Assertions.assertEquals(10, asChar10.getVarchar().length());

        // Chained: CAST(CAST(1775580223839 AS CHAR(10)) AS DECIMAL) -> 1775580223.
        ConstantOperator asDecimal = asChar10.castTo(Type.DEFAULT_DECIMAL128).get();
        Assertions.assertEquals(1775580223L, asDecimal.getDecimal().longValue());

        // String source truncates too.
        Assertions.assertEquals("hello",
                ConstantOperator.createVarchar("hello world").castToExplicitly(ScalarType.createCharType(5)).get().getVarchar());
    }

    @Test
    public void testCastToVarcharAndWildcardCharDoNotTruncate() {
        ConstantOperator bigint = ConstantOperator.createBigint(1775580223839L);
        // VARCHAR(N) is not truncated.
        Assertions.assertEquals("1775580223839", bigint.castTo(ScalarType.createVarcharType(10)).get().getVarchar());
        // Wildcard CHAR (no length, len == -1) is not truncated.
        Assertions.assertEquals("1775580223839", bigint.castTo(Type.CHAR).get().getVarchar());
    }

    @Test
    public void testGenericCharConversionPreservesOversizedValues() {
        Assertions.assertEquals("1775580223839",
                ConstantOperator.createBigint(1775580223839L).castTo(ScalarType.createCharType(10)).get().getVarchar());
        Assertions.assertEquals("hello world",
                ConstantOperator.createVarchar("hello world").castTo(ScalarType.createCharType(5)).get().getVarchar());
    }

    @Test
    public void testExplicitCharCastCountsCodePoints() {
        ConstantOperator value = ConstantOperator.createVarchar("中😀文abc");
        Assertions.assertEquals("中😀", value.castToExplicitly(ScalarType.createCharType(2)).get().getVarchar());
        Assertions.assertEquals("", value.castToExplicitly(ScalarType.createCharType(0)).get().getVarchar());
        Assertions.assertEquals("中😀文abc", value.castToExplicitly(ScalarType.createCharType(6)).get().getVarchar());
    }

    @Test
    public void testCastToDateValid() throws Exception {
        String[][] testCases = {
                // YYYY-MM-dd or yy-MM-dd
                {"1997-10-07", "1997-10-07T00:00", "1997-10-07T00:00"},
                {"0097-10-07", "0097-10-07T00:00", "0097-10-07T00:00"},
                {"2020-02-29", "2020-02-29T00:00", "2020-02-29T00:00"}, // Leap year.
                {"97-10-07", "1997-10-07T00:00", "1997-10-07T00:00"},
                {"99-10-07", "1999-10-07T00:00", "1999-10-07T00:00"},
                {"70-10-07", "1970-10-07T00:00", "1970-10-07T00:00"},
                {"69-10-07", "2069-10-07T00:00", "2069-10-07T00:00"},
                {"00-10-07", "2000-10-07T00:00", "2000-10-07T00:00"},
                {"1997-1-07", "1997-01-07T00:00", "1997-01-07T00:00"},
                {"1997-1-7", "1997-01-07T00:00", "1997-01-07T00:00"},
                {"19971117", "1997-11-17T00:00", "1997-11-17T00:00"},

                // YYYY-MM-dd HH:mm:ss or yy-MM-dd HH:mm:ss
                {"1997-10-07 10:11:12", "1997-10-07T00:00", "1997-10-07T10:11:12"},
                {"0097-10-07 10:11:12", "0097-10-07T00:00", "0097-10-07T10:11:12"},
                {"2020-02-29 10:11:12", "2020-02-29T00:00", "2020-02-29T10:11:12"}, // Leap year.
                {"97-10-07 10:11:12", "1997-10-07T00:00", "1997-10-07T10:11:12"},
                {"99-10-07 10:11:12", "1999-10-07T00:00", "1999-10-07T10:11:12"},
                {"70-10-07 10:11:12", "1970-10-07T00:00", "1970-10-07T10:11:12"},
                {"69-10-07 10:11:12", "2069-10-07T00:00", "2069-10-07T10:11:12"},
                {"00-10-07 10:11:12", "2000-10-07T00:00", "2000-10-07T10:11:12"},
                // YYYY-MM-dd HH:mm:ss.SSS
                {"1997-10-07 10:11:12.123", "1997-10-07T00:00", "1997-10-07T10:11:12.123"},
                {"0097-10-07 10:11:12.123", "0097-10-07T00:00", "0097-10-07T10:11:12.123"},
                {"2020-02-29 10:11:12.123", "2020-02-29T00:00", "2020-02-29T10:11:12.123"}, // Leap year.
        };

        for (String[] c : testCases) {
            ConstantOperator in = ConstantOperator.createVarchar(c[0]);
            Assertions.assertEquals(c[1], in.castTo(Type.DATE).get().getDate().toString());
            Assertions.assertEquals(c[2], in.castTo(Type.DATETIME).get().getDate().toString());
        }
    }

    @Test
    public void testCastToDateInvalid() {
        String[] testCases = {
                // Invalid year.
                "20190-05-31",
                "20190-05-31 10:11:12",
                "20190-05-31 10:11:12.123",
                "1-05-31",
                "1-05-31 10:11:12",
                "1-05-31 10:11:12.123",
                // Invalid month.
                "2019-16-31",
                "2019-16-31 10:11:12",
                "2019-16-31 10:11:12.123",
                "1997117",
                "199711",
                // Invalid day.
                "2019-02-29",
                "2019-02-29 10:11:12",
                "2019-02-29 10:11:12.123",
                "2019-04-31",
                "2019-04-31 10:11:12",
                "2019-04-31 10:11:12.123",
                "2019-05-32",
                "2019-05-32 10:11:12",
                "2019-05-32 10:11:12.123",
                // Invalid hour, minute, or second.
                "2019-05-31 25:11:12",
                "2019-05-31 25:11:12.123",
                "2019-05-31 10:61:12",
                "2019-05-31 10:61:12.123",
                "2019-05-31 10:11:61",
                "2019-05-31 10:11:61.123",
                "2019-05-31 10:11:12.1234567",

                // Other invalid formats.
                "2019-05-31-1",
                "2019-05-31-1 10:11:12",
                "2019-05-31-1 10:11:12.123",
                "not-date",
        };
        for (String c : testCases) {
            ConstantOperator in = ConstantOperator.createVarchar(c);
            Assertions.assertFalse(in.castTo(Type.DATE).isPresent());
            Assertions.assertFalse(in.castTo(Type.DATETIME).isPresent());
        }
    }

    @Test
    public void testCastDateToNumber() throws Exception {
        ConstantOperator date = ConstantOperator.createDate(LocalDateTime.of(2023, 01, 01, 0, 0));
        ConstantOperator datetime = ConstantOperator.createDatetime(LocalDateTime.of(2023, 01, 01, 0, 0, 0));

        ConstantOperator intNumber = ConstantOperator.createInt(20230101);
        Assertions.assertEquals(intNumber, date.castTo(Type.INT).get());

        ConstantOperator dateBigintNumber = ConstantOperator.createBigint(20230101L);
        Assertions.assertEquals(dateBigintNumber, date.castTo(Type.BIGINT).get());

        ConstantOperator datetimeBigintNumber = ConstantOperator.createBigint(20230101000000L);
        Assertions.assertEquals(datetimeBigintNumber, datetime.castTo(Type.BIGINT).get());

        ConstantOperator dateLargeintNumber = ConstantOperator.createLargeInt(new BigInteger("20230101"));
        Assertions.assertEquals(dateLargeintNumber, date.castTo(Type.LARGEINT).get());

        ConstantOperator datetimeLargeintNumber = ConstantOperator.createLargeInt(new BigInteger("20230101000000"));
        Assertions.assertEquals(datetimeLargeintNumber, datetime.castTo(Type.LARGEINT).get());

        ConstantOperator dateFloatNumber = ConstantOperator.createFloat(20230101);
        Assertions.assertEquals(dateFloatNumber, date.castTo(Type.FLOAT).get());

        ConstantOperator datetimeFloatNumber = ConstantOperator.createFloat(20230101000000L);
        Assertions.assertEquals(datetimeFloatNumber, datetime.castTo(Type.FLOAT).get());

        ConstantOperator dateDoubleNumber = ConstantOperator.createDouble(20230101);
        Assertions.assertEquals(dateDoubleNumber, date.castTo(Type.DOUBLE).get());

        ConstantOperator datetimeDoubleNumber = ConstantOperator.createDouble(20230101000000L);
        Assertions.assertEquals(datetimeDoubleNumber, datetime.castTo(Type.DOUBLE).get());
    }

    @Test
    public void testCastTimeToDateTime() {
        LocalDateTime now = LocalDateTime.now().withNano(0);
        ConstantOperator time = ConstantOperator.createTime(now.getHour() * 3600D + now.getMinute() * 60D + now.getSecond());
        ConstantOperator datetime = ConstantOperator.createDatetime(now);
        Assertions.assertEquals(datetime, time.castTo(Type.DATETIME).get());
    }

    @Test
    public void testExplicitBinaryCharCastTruncatesDecodedUtf8() {
        ConstantOperator binary = ConstantOperator.createBinary("中😀文".getBytes(StandardCharsets.UTF_8),
                Type.VARBINARY);
        Assertions.assertEquals("中😀", binary.castToExplicitly(ScalarType.createCharType(2)).get().getVarchar());
        Assertions.assertEquals("中😀文", binary.castTo(ScalarType.createCharType(2)).get().getVarchar());
        ConstantOperator invalid = ConstantOperator.createBinary(new byte[] {(byte) 0xe4, (byte) 0xb8},
                Type.VARBINARY);
        Assertions.assertTrue(invalid.castToExplicitly(ScalarType.createCharType(1)).isEmpty());
    }

    @Test
    public void testCastBinaryToStringKeepsTheBytes() {
        byte[] bytes = "zz".getBytes(StandardCharsets.UTF_8);
        ConstantOperator binary = ConstantOperator.createBinary(bytes, Type.VARBINARY);

        // The BE casts binary to string by handing the underlying bytes over untouched, folding must agree.
        String folded = binary.castTo(Type.VARCHAR).get().getVarchar();
        Assertions.assertEquals("zz", folded);
        Assertions.assertFalse(folded.startsWith("[B@"), folded);

        // The folded value used to be the identity hash code of the byte[], so it differed on every call.
        Assertions.assertEquals(folded, binary.castTo(Type.VARCHAR).get().getVarchar());
        Assertions.assertEquals(binary.castTo(Type.VARCHAR).get(), binary.castTo(Type.VARCHAR).get());

        // An equal but distinct constant, i.e. a different byte[] instance, folds to the same value.
        ConstantOperator otherInstance =
                ConstantOperator.createBinary("zz".getBytes(StandardCharsets.UTF_8), Type.VARBINARY);
        Assertions.assertEquals(folded, otherInstance.castTo(Type.VARCHAR).get().getVarchar());

        // CHAR and sized VARCHAR take the same path, the BE does not truncate to the target length either.
        Assertions.assertEquals("zz", binary.castTo(Type.CHAR).get().getVarchar());
        Assertions.assertEquals("zz", binary.castTo(ScalarType.createVarcharType(1)).get().getVarchar());
    }

    @Test
    public void testCastBinaryToBinaryKeepsTheBytes() {
        byte[] bytes = "zz".getBytes(StandardCharsets.UTF_8);
        ConstantOperator binary = ConstantOperator.createBinary(bytes, Type.VARBINARY);

        Assertions.assertArrayEquals(bytes, binary.castTo(Type.VARBINARY).get().getBinary());
        Assertions.assertArrayEquals(bytes, binary.castTo(ScalarType.createVarbinary(10)).get().getBinary());
    }

    @Test
    public void testCastNonUtf8BinaryToStringIsNotFolded() {
        // 0xFF is not valid UTF-8: decoding it would substitute U+FFFD and the folded constant would no
        // longer carry the bytes the BE would produce, so the cast has to be left to the BE.
        ConstantOperator binary = ConstantOperator.createBinary(new byte[] {(byte) 0xFF}, Type.VARBINARY);
        Assertions.assertEquals(Optional.empty(), binary.castTo(Type.VARCHAR));
        Assertions.assertEquals(Optional.empty(), binary.castTo(Type.CHAR));
    }

    @Test
    public void testCastBinaryToOtherTypesIsNotFolded() {
        ConstantOperator binary =
                ConstantOperator.createBinary("12".getBytes(StandardCharsets.UTF_8), Type.VARBINARY);

        // The BE only supports casting a binary value to a string, folding anything else would make the
        // constant path disagree with the non-folded one.
        Assertions.assertEquals(Optional.empty(), binary.castTo(Type.INT));
        Assertions.assertEquals(Optional.empty(), binary.castTo(Type.BIGINT));
        Assertions.assertEquals(Optional.empty(), binary.castTo(Type.DOUBLE));
        Assertions.assertEquals(Optional.empty(), binary.castTo(Type.DATE));

        // A null binary constant used to be folded into the literal string "null".
        Assertions.assertEquals(Optional.empty(),
                ConstantOperator.createNull(Type.VARBINARY).castTo(Type.VARCHAR));
    }

    @Test
    public void testDistance() {
        {
            // tinyint
            ConstantOperator var1 = ConstantOperator.createTinyInt((byte) 10);
            ConstantOperator var2 = ConstantOperator.createTinyInt((byte) 20);
            Assertions.assertEquals(10, var1.distance(var2));
            Assertions.assertEquals(-10, var2.distance(var1));
        }

        {
            // smallint
            ConstantOperator var1 = ConstantOperator.createSmallInt((short) 10);
            ConstantOperator var2 = ConstantOperator.createSmallInt((short) 20);
            Assertions.assertEquals(10, var1.distance(var2));
            Assertions.assertEquals(-10, var2.distance(var1));
        }

        {
            // int
            ConstantOperator var1 = ConstantOperator.createInt(10);
            ConstantOperator var2 = ConstantOperator.createInt(20);
            Assertions.assertEquals(10, var1.distance(var2));
            Assertions.assertEquals(-10, var2.distance(var1));
        }

        {
            // long
            ConstantOperator var1 = ConstantOperator.createBigint(10);
            ConstantOperator var2 = ConstantOperator.createBigint(20);
            Assertions.assertEquals(10, var1.distance(var2));
            Assertions.assertEquals(-10, var2.distance(var1));
        }

        {
            // large int
            ConstantOperator var1 = ConstantOperator.createLargeInt(BigInteger.valueOf(10));
            ConstantOperator var2 = ConstantOperator.createLargeInt(BigInteger.valueOf(20));
            Assertions.assertEquals(10, var1.distance(var2));
            Assertions.assertEquals(-10, var2.distance(var1));
        }

        {
            // date
            ConstantOperator var1 = ConstantOperator.createDate(LocalDateTime.of(2023, 10, 5, 0, 0, 0));
            ConstantOperator var2 = ConstantOperator.createDate(LocalDateTime.of(2023, 10, 15, 0, 0, 0));
            Assertions.assertEquals(10, var1.distance(var2));
            Assertions.assertEquals(-10, var2.distance(var1));
        }

        {
            // datetime
            ConstantOperator var1 = ConstantOperator.createDatetime(LocalDateTime.of(2023, 10, 5, 0, 0, 0));
            ConstantOperator var2 = ConstantOperator.createDatetime(LocalDateTime.of(2023, 10, 5, 0, 0, 10));
            Assertions.assertEquals(10, var1.distance(var2));
            Assertions.assertEquals(-10, var2.distance(var1));
        }
    }
}
