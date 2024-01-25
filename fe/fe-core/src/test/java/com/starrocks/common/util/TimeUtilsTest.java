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

package com.starrocks.common.util;

import com.starrocks.analysis.DateLiteral;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.ScalarType;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.DdlException;
import com.starrocks.common.FeConstants;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.time.ZoneId;
import java.util.LinkedList;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

public class TimeUtilsTest {

    @Mocked
    TimeUtils timeUtils;

    @Before
    public void setUp() {
        TimeZone tz = TimeZone.getTimeZone(ZoneId.of("Asia/Shanghai"));
        new Expectations(timeUtils) {
            {
                TimeUtils.getTimeZone();
                minTimes = 0;
                result = tz;
            }
        };
    }

    @Test
    public void testNormal() {
        Assert.assertNotNull(TimeUtils.getCurrentFormatTime());
        Assert.assertTrue(TimeUtils.getEstimatedTime(0L) > 0);

        Assert.assertEquals(-62167420800000L, TimeUtils.MIN_DATE.getTime());
        Assert.assertEquals(253402185600000L, TimeUtils.MAX_DATE.getTime());
        Assert.assertEquals(-62167420800000L, TimeUtils.MIN_DATETIME.getTime());
        Assert.assertEquals(253402271999000L, TimeUtils.MAX_DATETIME.getTime());
    }

    @Test
    public void testDateParse() {
        // date
        List<String> validDateList = new LinkedList<>();
        validDateList.add("2013-12-02");
        validDateList.add("2013-12-02");
        validDateList.add("2013-12-2");
        validDateList.add("2013-12-2");
        validDateList.add("9999-12-31");
        validDateList.add("1900-01-01");
        validDateList.add("2013-2-28");
        validDateList.add("0000-01-01");
        for (String validDate : validDateList) {
            try {
                TimeUtils.parseDate(validDate, PrimitiveType.DATE);
            } catch (AnalysisException e) {
                e.printStackTrace();
                System.out.println(validDate);
                Assert.fail();
            }
        }

        List<String> invalidDateList = new LinkedList<>();
        invalidDateList.add("2013-12-02 ");
        invalidDateList.add(" 2013-12-02");
        invalidDateList.add("20131-2-28");
        invalidDateList.add("a2013-2-28");
        invalidDateList.add("2013-22-28");
        invalidDateList.add("2013-2-29");
        invalidDateList.add("2013-2-28 2:3:4");
        for (String invalidDate : invalidDateList) {
            try {
                TimeUtils.parseDate(invalidDate, PrimitiveType.DATE);
                Assert.fail();
            } catch (AnalysisException e) {
                Assert.assertTrue(e.getMessage().contains("Invalid"));
            }
        }

        // datetime
        List<String> validDateTimeList = new LinkedList<>();
        validDateTimeList.add("2013-12-02 13:59:59");
        validDateTimeList.add("2013-12-2 13:59:59");
        validDateTimeList.add("2013-12-2 1:59:59");
        validDateTimeList.add("2013-12-2 3:1:1");
        validDateTimeList.add("9999-12-31 23:59:59");
        validDateTimeList.add("1900-01-01 00:00:00");
        validDateTimeList.add("2013-2-28 23:59:59");
        validDateTimeList.add("2013-2-28 2:3:4");
        validDateTimeList.add("2014-05-07 19:8:50");
        validDateTimeList.add("0000-01-01 00:00:00");
        for (String validDateTime : validDateTimeList) {
            try {
                TimeUtils.parseDate(validDateTime, PrimitiveType.DATETIME);
            } catch (AnalysisException e) {
                e.printStackTrace();
                System.out.println(validDateTime);
                Assert.fail();
            }
        }

        List<String> invalidDateTimeList = new LinkedList<>();
        invalidDateTimeList.add("2013-12-02  12:12:10");
        invalidDateTimeList.add(" 2013-12-02 12:12:10 ");
        invalidDateTimeList.add("20131-2-28 12:12:10");
        invalidDateTimeList.add("a2013-2-28 12:12:10");
        invalidDateTimeList.add("2013-22-28 12:12:10");
        invalidDateTimeList.add("2013-2-29 12:12:10");
        invalidDateTimeList.add("2013-2-28");
        invalidDateTimeList.add("2013-13-01 12:12:12");
        for (String invalidDateTime : invalidDateTimeList) {
            try {
                TimeUtils.parseDate(invalidDateTime, PrimitiveType.DATETIME);
                Assert.fail();
            } catch (AnalysisException e) {
                Assert.assertTrue(e.getMessage().contains("Invalid"));
            }
        }
    }

    @Test
    public void testDateTrans() throws AnalysisException {
        Assert.assertEquals(FeConstants.NULL_STRING, TimeUtils.longToTimeString(-2));

        long timestamp = 1426125600000L;
        Assert.assertEquals("2015-03-12 10:00:00", TimeUtils.longToTimeString(timestamp));

        DateLiteral date = new DateLiteral("2015-03-01", ScalarType.DATE);
        Assert.assertEquals(20150301000000L, date.getLongValue());

        DateLiteral datetime = new DateLiteral("2015-03-01 12:00:00", ScalarType.DATETIME);
        Assert.assertEquals(20150301120000L, datetime.getLongValue());
    }

    @Test
    public void testTimezone() throws AnalysisException {
        try {
            Assert.assertEquals("CST", TimeUtils.checkTimeZoneValidAndStandardize("CST"));
            Assert.assertEquals("+08:00", TimeUtils.checkTimeZoneValidAndStandardize("+08:00"));
            Assert.assertEquals("+08:00", TimeUtils.checkTimeZoneValidAndStandardize("+8:00"));
            Assert.assertEquals("-08:00", TimeUtils.checkTimeZoneValidAndStandardize("-8:00"));
            Assert.assertEquals("+08:00", TimeUtils.checkTimeZoneValidAndStandardize("8:00"));
        } catch (DdlException ex) {
            Assert.fail();
        }
        try {
            TimeUtils.checkTimeZoneValidAndStandardize("FOO");
            Assert.fail();
        } catch (DdlException ex) {
            Assert.assertTrue(ex.getMessage().contains("Unknown or incorrect time zone: 'FOO'"));
        }
    }

    @Test
    public void testConvertTimeUnitValuetoSecond() {
        long dayRes = TimeUtils.convertTimeUnitValueToSecond(2, TimeUnit.DAYS);
        long hourRes = TimeUtils.convertTimeUnitValueToSecond(2, TimeUnit.HOURS);
        long minuteRes = TimeUtils.convertTimeUnitValueToSecond(2, TimeUnit.MINUTES);
        long secondRes = TimeUtils.convertTimeUnitValueToSecond(2, TimeUnit.SECONDS);
        long milRes = TimeUtils.convertTimeUnitValueToSecond(2, TimeUnit.MILLISECONDS);
        long micRes = TimeUtils.convertTimeUnitValueToSecond(2, TimeUnit.MICROSECONDS);
        long nanoRes = TimeUtils.convertTimeUnitValueToSecond(2, TimeUnit.NANOSECONDS);
        Assert.assertEquals(dayRes, 2 * 24 * 60 * 60);
        Assert.assertEquals(hourRes, 2 * 60 * 60);
        Assert.assertEquals(minuteRes, 2 * 60);
        Assert.assertEquals(secondRes, 2);
        Assert.assertEquals(milRes, 2 / 1000);
        Assert.assertEquals(micRes, 2 / 1000 / 1000);
        Assert.assertEquals(nanoRes, 2 / 1000 / 1000 / 1000);
    }

    @Test
    public void testGetNextValidTimeSecond() {
        // 2022-04-21 20:45:11
        long startTimeSecond = 1650545111L;
        // 2022-04-21 23:32:11
        long targetTimeSecond = 1650555131L;
        try {
            TimeUtils.getNextValidTimeSecond(startTimeSecond, targetTimeSecond, 2, TimeUnit.NANOSECONDS);
        } catch (DdlException e) {
            Assert.assertEquals("Can not get next valid time second," +
                    "startTimeSecond:1650545111 period:2 timeUnit:NANOSECONDS", e.getMessage());
        }
        try {
            TimeUtils.getNextValidTimeSecond(startTimeSecond, targetTimeSecond, 2, TimeUnit.MILLISECONDS);
        } catch (DdlException e) {
            Assert.assertEquals("Can not get next valid time second," +
                    "startTimeSecond:1650545111 period:2 timeUnit:MILLISECONDS", e.getMessage());
        }
        try {
            // 2022-04-21 23:32:12
            Assert.assertEquals(1650555132L, TimeUtils.getNextValidTimeSecond(startTimeSecond, targetTimeSecond,
                    1000, TimeUnit.MILLISECONDS));
            // 2022-04-21 23:32:12
            Assert.assertEquals(1650555132L, TimeUtils.getNextValidTimeSecond(startTimeSecond, targetTimeSecond,
                    1, TimeUnit.SECONDS));
            // 2022-04-21 23:32:16
            Assert.assertEquals(1650555136L, TimeUtils.getNextValidTimeSecond(startTimeSecond, targetTimeSecond,
                    5, TimeUnit.SECONDS));
            // 2022-04-21 23:32:15
            Assert.assertEquals(1650555135L, TimeUtils.getNextValidTimeSecond(startTimeSecond, targetTimeSecond,
                    7, TimeUnit.SECONDS));
            // 2022-04-21 23:32:12
            Assert.assertEquals(1650555132L, TimeUtils.getNextValidTimeSecond(startTimeSecond, targetTimeSecond,
                    11, TimeUnit.SECONDS));
            // 2022-04-21 23:33:31
            Assert.assertEquals(1650555211L, TimeUtils.getNextValidTimeSecond(startTimeSecond, targetTimeSecond,
                    101, TimeUnit.SECONDS));
            // 2022-04-21 23:48:20
            Assert.assertEquals(1650556100L, TimeUtils.getNextValidTimeSecond(startTimeSecond, targetTimeSecond,
                    999, TimeUnit.SECONDS));
            // 2022-04-21 23:45:11
            Assert.assertEquals(1650555911L, TimeUtils.getNextValidTimeSecond(startTimeSecond, targetTimeSecond,
                    3, TimeUnit.HOURS));
            // 2022-04-22 03:45:11
            Assert.assertEquals(1650570311L, TimeUtils.getNextValidTimeSecond(startTimeSecond, targetTimeSecond,
                    7, TimeUnit.HOURS));
            // 2022-04-30 20:45:11
            Assert.assertEquals(1651322711L, TimeUtils.getNextValidTimeSecond(startTimeSecond, targetTimeSecond,
                    9, TimeUnit.DAYS));
            // 2022-04-21 23:32:18
            Assert.assertEquals(1650555138L, TimeUtils.getNextValidTimeSecond(1650555138L, targetTimeSecond,
                    9, TimeUnit.DAYS));
        } catch (DdlException e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testGetMilliseconds() {
        Assert.assertEquals(0, TimeUtils.getMilliseconds("0ms"));
        Assert.assertEquals(0, TimeUtils.getMilliseconds("0s"));
        Assert.assertEquals(0, TimeUtils.getMilliseconds("0m"));
        Assert.assertEquals(1011, TimeUtils.getMilliseconds("1011ms"));
        Assert.assertEquals(1011, TimeUtils.getMilliseconds("1011.222ms"));
        Assert.assertEquals(1011000, TimeUtils.getMilliseconds("1011s"));
        Assert.assertEquals(1011222, TimeUtils.getMilliseconds("1011.222s"));
        Assert.assertEquals(1011222, TimeUtils.getMilliseconds("1011.222333s"));
        Assert.assertEquals(60000, TimeUtils.getMilliseconds("1m"));
        Assert.assertEquals(90000, TimeUtils.getMilliseconds("1.5m"));
    }
}
