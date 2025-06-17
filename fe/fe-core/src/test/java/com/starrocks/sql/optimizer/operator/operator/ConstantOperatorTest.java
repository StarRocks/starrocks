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

import com.starrocks.catalog.Type;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigInteger;
import java.time.LocalDateTime;

public class ConstantOperatorTest {
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
            Assert.assertEquals(c[1], in.castTo(Type.DATE).get().getDate().toString());
            Assert.assertEquals(c[2], in.castTo(Type.DATETIME).get().getDate().toString());
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
            Assert.assertFalse(in.castTo(Type.DATE).isPresent());
            Assert.assertFalse(in.castTo(Type.DATETIME).isPresent());
        }
    }

    @Test
    public void testCastDateToNumber() throws Exception {
        ConstantOperator date = ConstantOperator.createDate(LocalDateTime.of(2023, 01, 01, 0, 0));
        ConstantOperator datetime = ConstantOperator.createDatetime(LocalDateTime.of(2023, 01, 01, 0, 0, 0));

        ConstantOperator intNumber = ConstantOperator.createInt(20230101);
        Assert.assertEquals(intNumber, date.castTo(Type.INT).get());

        ConstantOperator dateBigintNumber = ConstantOperator.createBigint(20230101L);
        Assert.assertEquals(dateBigintNumber, date.castTo(Type.BIGINT).get());

        ConstantOperator datetimeBigintNumber = ConstantOperator.createBigint(20230101000000L);
        Assert.assertEquals(datetimeBigintNumber, datetime.castTo(Type.BIGINT).get());

        ConstantOperator dateLargeintNumber = ConstantOperator.createLargeInt(new BigInteger("20230101"));
        Assert.assertEquals(dateLargeintNumber, date.castTo(Type.LARGEINT).get());

        ConstantOperator datetimeLargeintNumber = ConstantOperator.createLargeInt(new BigInteger("20230101000000"));
        Assert.assertEquals(datetimeLargeintNumber, datetime.castTo(Type.LARGEINT).get());

        ConstantOperator dateFloatNumber = ConstantOperator.createFloat(20230101);
        Assert.assertEquals(dateFloatNumber, date.castTo(Type.FLOAT).get());

        ConstantOperator datetimeFloatNumber = ConstantOperator.createFloat(20230101000000L);
        Assert.assertEquals(datetimeFloatNumber, datetime.castTo(Type.FLOAT).get());

        ConstantOperator dateDoubleNumber = ConstantOperator.createDouble(20230101);
        Assert.assertEquals(dateDoubleNumber, date.castTo(Type.DOUBLE).get());

        ConstantOperator datetimeDoubleNumber = ConstantOperator.createDouble(20230101000000L);
        Assert.assertEquals(datetimeDoubleNumber, datetime.castTo(Type.DOUBLE).get());
    }

    @Test
    public void testCastTimeToDateTime() {
        LocalDateTime now = LocalDateTime.now().withNano(0);
        ConstantOperator time = ConstantOperator.createTime(now.getHour() * 3600D + now.getMinute() * 60D + now.getSecond());
        ConstantOperator datetime = ConstantOperator.createDatetime(now);
        Assert.assertEquals(datetime, time.castTo(Type.DATETIME).get());
    }

    @Test
    public void testDistance() {
        {
            // tinyint
            ConstantOperator var1 = ConstantOperator.createTinyInt((byte) 10);
            ConstantOperator var2 = ConstantOperator.createTinyInt((byte) 20);
            Assert.assertEquals(10, var1.distance(var2));
            Assert.assertEquals(-10, var2.distance(var1));
        }

        {
            // smallint
            ConstantOperator var1 = ConstantOperator.createSmallInt((short) 10);
            ConstantOperator var2 = ConstantOperator.createSmallInt((short) 20);
            Assert.assertEquals(10, var1.distance(var2));
            Assert.assertEquals(-10, var2.distance(var1));
        }

        {
            // int
            ConstantOperator var1 = ConstantOperator.createInt(10);
            ConstantOperator var2 = ConstantOperator.createInt(20);
            Assert.assertEquals(10, var1.distance(var2));
            Assert.assertEquals(-10, var2.distance(var1));
        }

        {
            // long
            ConstantOperator var1 = ConstantOperator.createBigint(10);
            ConstantOperator var2 = ConstantOperator.createBigint(20);
            Assert.assertEquals(10, var1.distance(var2));
            Assert.assertEquals(-10, var2.distance(var1));
        }

        {
            // large int
            ConstantOperator var1 = ConstantOperator.createLargeInt(BigInteger.valueOf(10));
            ConstantOperator var2 = ConstantOperator.createLargeInt(BigInteger.valueOf(20));
            Assert.assertEquals(10, var1.distance(var2));
            Assert.assertEquals(-10, var2.distance(var1));
        }

        {
            // date
            ConstantOperator var1 = ConstantOperator.createDate(LocalDateTime.of(2023, 10, 5, 0, 0, 0));
            ConstantOperator var2 = ConstantOperator.createDate(LocalDateTime.of(2023, 10, 15, 0, 0, 0));
            Assert.assertEquals(10, var1.distance(var2));
            Assert.assertEquals(-10, var2.distance(var1));
        }

        {
            // datetime
            ConstantOperator var1 = ConstantOperator.createDatetime(LocalDateTime.of(2023, 10, 5, 0, 0, 0));
            ConstantOperator var2 = ConstantOperator.createDatetime(LocalDateTime.of(2023, 10, 5, 0, 0, 10));
            Assert.assertEquals(10, var1.distance(var2));
            Assert.assertEquals(-10, var2.distance(var1));
        }
    }
}
