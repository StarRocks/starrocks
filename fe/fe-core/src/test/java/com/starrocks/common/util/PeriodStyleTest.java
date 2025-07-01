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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.DateTimeException;
import java.time.Period;

public class PeriodStyleTest {

    @Test
    public void testLongYears() {
        Assertions.assertEquals(Period.ofYears(1), PeriodStyle.LONG.parse("1 year"));
        Assertions.assertEquals(Period.ofYears(1), PeriodStyle.LONG.parse("1 years"));
        Assertions.assertEquals(Period.ofYears(1), PeriodStyle.LONG.parse(" 1 years"));
        Assertions.assertEquals(Period.ofYears(1), PeriodStyle.LONG.parse("1 years "));
        Assertions.assertEquals(Period.ofYears(1), PeriodStyle.LONG.parse(" 1 years "));
        Assertions.assertEquals(Period.ofYears(1), PeriodStyle.LONG.parse(" 1  years "));
        Assertions.assertEquals(Period.ofYears(1), PeriodStyle.LONG.parse(" 1\tyears "));

        Assertions.assertEquals(Period.ofYears(3), PeriodStyle.LONG.parse("3 year"));
        Assertions.assertEquals(Period.ofYears(3), PeriodStyle.LONG.parse("3 years"));
        Assertions.assertEquals(Period.ofYears(3), PeriodStyle.LONG.parse("3 YEARS"));

        Assertions.assertEquals("0 day", PeriodStyle.LONG.toString(Period.ofYears(0)));
        Assertions.assertEquals("1 years", PeriodStyle.LONG.toString(Period.ofYears(1)));
        Assertions.assertEquals("3 years", PeriodStyle.LONG.toString(Period.ofYears(3)));

        Assertions.assertThrows(DateTimeException.class, () -> PeriodStyle.LONG.parse("a year"));
        Assertions.assertThrows(DateTimeException.class, () -> PeriodStyle.LONG.parse("3 yer"));
        Assertions.assertThrows(DateTimeException.class, () -> PeriodStyle.LONG.parse("3y"));
        Assertions.assertThrows(DateTimeException.class, () -> PeriodStyle.LONG.parse("3Y"));
    }

    @Test
    public void testLongMonths() {
        Assertions.assertEquals(Period.ofMonths(1), PeriodStyle.LONG.parse("1 month"));
        Assertions.assertEquals(Period.ofMonths(1), PeriodStyle.LONG.parse("1 months"));
        Assertions.assertEquals(Period.ofMonths(1), PeriodStyle.LONG.parse(" 1 months"));
        Assertions.assertEquals(Period.ofMonths(1), PeriodStyle.LONG.parse("1 months "));
        Assertions.assertEquals(Period.ofMonths(1), PeriodStyle.LONG.parse(" 1 months "));
        Assertions.assertEquals(Period.ofMonths(1), PeriodStyle.LONG.parse(" 1  months "));
        Assertions.assertEquals(Period.ofMonths(1), PeriodStyle.LONG.parse(" 1\tmonths "));

        Assertions.assertEquals(Period.ofMonths(3), PeriodStyle.LONG.parse("3 month"));
        Assertions.assertEquals(Period.ofMonths(3), PeriodStyle.LONG.parse("3 months"));
        Assertions.assertEquals(Period.ofMonths(3), PeriodStyle.LONG.parse("3 MONTHS"));

        Assertions.assertEquals("0 day", PeriodStyle.LONG.toString(Period.ofMonths(0)));
        Assertions.assertEquals("1 months", PeriodStyle.LONG.toString(Period.ofMonths(1)));
        Assertions.assertEquals("3 months", PeriodStyle.LONG.toString(Period.ofMonths(3)));

        Assertions.assertThrows(DateTimeException.class, () -> PeriodStyle.LONG.parse("a month"));
        Assertions.assertThrows(DateTimeException.class, () -> PeriodStyle.LONG.parse("3 mont"));
        Assertions.assertThrows(DateTimeException.class, () -> PeriodStyle.LONG.parse("3M"));
        Assertions.assertThrows(DateTimeException.class, () -> PeriodStyle.LONG.parse("3m"));
    }

    @Test
    public void testLongWeeks() {
        Assertions.assertEquals(Period.ofWeeks(1), PeriodStyle.LONG.parse("1 week"));
        Assertions.assertEquals(Period.ofWeeks(1), PeriodStyle.LONG.parse("1 weeks"));
        Assertions.assertEquals(Period.ofWeeks(1), PeriodStyle.LONG.parse(" 1 weeks"));
        Assertions.assertEquals(Period.ofWeeks(1), PeriodStyle.LONG.parse("1 weeks "));
        Assertions.assertEquals(Period.ofWeeks(1), PeriodStyle.LONG.parse(" 1 weeks "));
        Assertions.assertEquals(Period.ofWeeks(1), PeriodStyle.LONG.parse(" 1  weeks "));
        Assertions.assertEquals(Period.ofWeeks(1), PeriodStyle.LONG.parse(" 1\tweeks "));

        Assertions.assertEquals(Period.ofWeeks(3), PeriodStyle.LONG.parse("3 week"));
        Assertions.assertEquals(Period.ofWeeks(3), PeriodStyle.LONG.parse("3 weeks"));
        Assertions.assertEquals(Period.ofWeeks(3), PeriodStyle.LONG.parse("3 WEEKS"));

        Assertions.assertEquals("0 day", PeriodStyle.LONG.toString(Period.ofWeeks(0)));
        Assertions.assertEquals("7 days", PeriodStyle.LONG.toString(Period.ofWeeks(1)));
        Assertions.assertEquals("21 days", PeriodStyle.LONG.toString(Period.ofWeeks(3)));

        Assertions.assertThrows(DateTimeException.class, () -> PeriodStyle.LONG.parse("a weeks"));
        Assertions.assertThrows(DateTimeException.class, () -> PeriodStyle.LONG.parse("3 we"));
        Assertions.assertThrows(DateTimeException.class, () -> PeriodStyle.LONG.parse("3w"));
        Assertions.assertThrows(DateTimeException.class, () -> PeriodStyle.LONG.parse("3W"));
    }

    @Test
    public void testLongDays() {
        Assertions.assertEquals(Period.ofDays(3), PeriodStyle.LONG.parse("3 days"));
        Assertions.assertEquals(Period.ofDays(3), PeriodStyle.LONG.parse("3  days"));
        Assertions.assertEquals(Period.ofDays(3), PeriodStyle.LONG.parse(" 3 DAYS"));
        Assertions.assertEquals(Period.ofDays(3), PeriodStyle.LONG.parse(" 3 DAYS "));

        Assertions.assertEquals("0 day", PeriodStyle.LONG.toString(Period.ZERO));
        Assertions.assertEquals("1 days", PeriodStyle.LONG.toString(Period.ofDays(1)));
        Assertions.assertEquals("3 days", PeriodStyle.LONG.toString(Period.ofDays(3)));
        Assertions.assertEquals("7 days", PeriodStyle.LONG.toString(Period.ofWeeks(1)));

        Assertions.assertThrows(DateTimeException.class, () -> PeriodStyle.LONG.parse("a days"));
        Assertions.assertThrows(DateTimeException.class, () -> PeriodStyle.LONG.parse("3 dya"));
        Assertions.assertThrows(DateTimeException.class, () -> PeriodStyle.LONG.parse("3d"));
        Assertions.assertThrows(DateTimeException.class, () -> PeriodStyle.LONG.parse("3D"));
    }

    @Test
    public void testLongCombined() {
        Assertions.assertEquals(Period.of(2, 3, 12), PeriodStyle.LONG.parse("2 years 3 months 12 days"));
        Assertions.assertEquals(Period.of(2, 3, 12), PeriodStyle.LONG.parse(" 2 YEARS 3 Months 12  Days "));
        Assertions.assertEquals(Period.of(0, 3, 12), PeriodStyle.LONG.parse("  3 Months 12  Days "));
        Assertions.assertEquals(Period.of(0, 0, 26), PeriodStyle.LONG.parse("\t2 WeeKs 12  Days "));

        Assertions.assertEquals("2 years 3 months 4 days", PeriodStyle.LONG.toString(Period.of(2, 3, 4)));
        Assertions.assertEquals("3 months 4 days", PeriodStyle.LONG.toString(Period.of(0, 3, 4)));
    }
}
