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

package com.starrocks.common.util;

import org.junit.Assert;
import org.junit.Test;

import java.time.DateTimeException;
import java.time.Period;

public class PeriodStyleTest {

    @Test
    public void testLongYears() {
        Assert.assertEquals(Period.ofYears(1), PeriodStyle.LONG.parse("1 year"));
        Assert.assertEquals(Period.ofYears(1), PeriodStyle.LONG.parse("1 years"));
        Assert.assertEquals(Period.ofYears(1), PeriodStyle.LONG.parse(" 1 years"));
        Assert.assertEquals(Period.ofYears(1), PeriodStyle.LONG.parse("1 years "));
        Assert.assertEquals(Period.ofYears(1), PeriodStyle.LONG.parse(" 1 years "));
        Assert.assertEquals(Period.ofYears(1), PeriodStyle.LONG.parse(" 1  years "));
        Assert.assertEquals(Period.ofYears(1), PeriodStyle.LONG.parse(" 1\tyears "));

        Assert.assertEquals(Period.ofYears(3), PeriodStyle.LONG.parse("3 year"));
        Assert.assertEquals(Period.ofYears(3), PeriodStyle.LONG.parse("3 years"));
        Assert.assertEquals(Period.ofYears(3), PeriodStyle.LONG.parse("3 YEARS"));

        Assert.assertEquals("0 day", PeriodStyle.LONG.toString(Period.ofYears(0)));
        Assert.assertEquals("1 years", PeriodStyle.LONG.toString(Period.ofYears(1)));
        Assert.assertEquals("3 years", PeriodStyle.LONG.toString(Period.ofYears(3)));

        Assert.assertThrows(DateTimeException.class, () -> PeriodStyle.LONG.parse("a year"));
        Assert.assertThrows(DateTimeException.class, () -> PeriodStyle.LONG.parse("3 yer"));
        Assert.assertThrows(DateTimeException.class, () -> PeriodStyle.LONG.parse("3y"));
        Assert.assertThrows(DateTimeException.class, () -> PeriodStyle.LONG.parse("3Y"));
    }

    @Test
    public void testLongMonths() {
        Assert.assertEquals(Period.ofMonths(1), PeriodStyle.LONG.parse("1 month"));
        Assert.assertEquals(Period.ofMonths(1), PeriodStyle.LONG.parse("1 months"));
        Assert.assertEquals(Period.ofMonths(1), PeriodStyle.LONG.parse(" 1 months"));
        Assert.assertEquals(Period.ofMonths(1), PeriodStyle.LONG.parse("1 months "));
        Assert.assertEquals(Period.ofMonths(1), PeriodStyle.LONG.parse(" 1 months "));
        Assert.assertEquals(Period.ofMonths(1), PeriodStyle.LONG.parse(" 1  months "));
        Assert.assertEquals(Period.ofMonths(1), PeriodStyle.LONG.parse(" 1\tmonths "));

        Assert.assertEquals(Period.ofMonths(3), PeriodStyle.LONG.parse("3 month"));
        Assert.assertEquals(Period.ofMonths(3), PeriodStyle.LONG.parse("3 months"));
        Assert.assertEquals(Period.ofMonths(3), PeriodStyle.LONG.parse("3 MONTHS"));

        Assert.assertEquals("0 day", PeriodStyle.LONG.toString(Period.ofMonths(0)));
        Assert.assertEquals("1 months", PeriodStyle.LONG.toString(Period.ofMonths(1)));
        Assert.assertEquals("3 months", PeriodStyle.LONG.toString(Period.ofMonths(3)));

        Assert.assertThrows(DateTimeException.class, () -> PeriodStyle.LONG.parse("a month"));
        Assert.assertThrows(DateTimeException.class, () -> PeriodStyle.LONG.parse("3 mont"));
        Assert.assertThrows(DateTimeException.class, () -> PeriodStyle.LONG.parse("3M"));
        Assert.assertThrows(DateTimeException.class, () -> PeriodStyle.LONG.parse("3m"));
    }

    @Test
    public void testLongWeeks() {
        Assert.assertEquals(Period.ofWeeks(1), PeriodStyle.LONG.parse("1 week"));
        Assert.assertEquals(Period.ofWeeks(1), PeriodStyle.LONG.parse("1 weeks"));
        Assert.assertEquals(Period.ofWeeks(1), PeriodStyle.LONG.parse(" 1 weeks"));
        Assert.assertEquals(Period.ofWeeks(1), PeriodStyle.LONG.parse("1 weeks "));
        Assert.assertEquals(Period.ofWeeks(1), PeriodStyle.LONG.parse(" 1 weeks "));
        Assert.assertEquals(Period.ofWeeks(1), PeriodStyle.LONG.parse(" 1  weeks "));
        Assert.assertEquals(Period.ofWeeks(1), PeriodStyle.LONG.parse(" 1\tweeks "));

        Assert.assertEquals(Period.ofWeeks(3), PeriodStyle.LONG.parse("3 week"));
        Assert.assertEquals(Period.ofWeeks(3), PeriodStyle.LONG.parse("3 weeks"));
        Assert.assertEquals(Period.ofWeeks(3), PeriodStyle.LONG.parse("3 WEEKS"));

        Assert.assertEquals("0 day", PeriodStyle.LONG.toString(Period.ofWeeks(0)));
        Assert.assertEquals("7 days", PeriodStyle.LONG.toString(Period.ofWeeks(1)));
        Assert.assertEquals("21 days", PeriodStyle.LONG.toString(Period.ofWeeks(3)));

        Assert.assertThrows(DateTimeException.class, () -> PeriodStyle.LONG.parse("a weeks"));
        Assert.assertThrows(DateTimeException.class, () -> PeriodStyle.LONG.parse("3 we"));
        Assert.assertThrows(DateTimeException.class, () -> PeriodStyle.LONG.parse("3w"));
        Assert.assertThrows(DateTimeException.class, () -> PeriodStyle.LONG.parse("3W"));
    }

    @Test
    public void testLongDays() {
        Assert.assertEquals(Period.ofDays(3), PeriodStyle.LONG.parse("3 days"));
        Assert.assertEquals(Period.ofDays(3), PeriodStyle.LONG.parse("3  days"));
        Assert.assertEquals(Period.ofDays(3), PeriodStyle.LONG.parse(" 3 DAYS"));
        Assert.assertEquals(Period.ofDays(3), PeriodStyle.LONG.parse(" 3 DAYS "));

        Assert.assertEquals("0 day", PeriodStyle.LONG.toString(Period.ZERO));
        Assert.assertEquals("1 days", PeriodStyle.LONG.toString(Period.ofDays(1)));
        Assert.assertEquals("3 days", PeriodStyle.LONG.toString(Period.ofDays(3)));
        Assert.assertEquals("7 days", PeriodStyle.LONG.toString(Period.ofWeeks(1)));

        Assert.assertThrows(DateTimeException.class, () -> PeriodStyle.LONG.parse("a days"));
        Assert.assertThrows(DateTimeException.class, () -> PeriodStyle.LONG.parse("3 dya"));
        Assert.assertThrows(DateTimeException.class, () -> PeriodStyle.LONG.parse("3d"));
        Assert.assertThrows(DateTimeException.class, () -> PeriodStyle.LONG.parse("3D"));
    }

    @Test
    public void testLongCombined() {
        Assert.assertEquals(Period.of(2, 3, 12), PeriodStyle.LONG.parse("2 years 3 months 12 days"));
        Assert.assertEquals(Period.of(2, 3, 12), PeriodStyle.LONG.parse(" 2 YEARS 3 Months 12  Days "));
        Assert.assertEquals(Period.of(0, 3, 12), PeriodStyle.LONG.parse("  3 Months 12  Days "));
        Assert.assertEquals(Period.of(0, 0, 26), PeriodStyle.LONG.parse("\t2 WeeKs 12  Days "));

        Assert.assertEquals("2 years 3 months 4 days", PeriodStyle.LONG.toString(Period.of(2, 3, 4)));
        Assert.assertEquals("3 months 4 days", PeriodStyle.LONG.toString(Period.of(0, 3, 4)));
    }
}
