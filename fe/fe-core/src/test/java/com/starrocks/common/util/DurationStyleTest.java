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
import java.time.Duration;

public class DurationStyleTest {
    @Test
    public void testLongHours() {
        Assert.assertEquals(Duration.ofHours(1), DurationStyle.LONG.parse("1 hour"));
        Assert.assertEquals(Duration.ofHours(1), DurationStyle.LONG.parse("1 hours"));
        Assert.assertEquals(Duration.ofHours(1), DurationStyle.LONG.parse(" 1 hours"));
        Assert.assertEquals(Duration.ofHours(1), DurationStyle.LONG.parse("1 hours "));
        Assert.assertEquals(Duration.ofHours(1), DurationStyle.LONG.parse(" 1 hours "));
        Assert.assertEquals(Duration.ofHours(1), DurationStyle.LONG.parse(" 1  hours "));
        Assert.assertEquals(Duration.ofHours(1), DurationStyle.LONG.parse(" 1\thours "));

        Assert.assertEquals(Duration.ofHours(3), DurationStyle.LONG.parse("3 hour"));
        Assert.assertEquals(Duration.ofHours(3), DurationStyle.LONG.parse("3 hours"));
        Assert.assertEquals(Duration.ofHours(3), DurationStyle.LONG.parse("3 HOURS"));

        Assert.assertEquals("0 second", DurationStyle.LONG.toString(Duration.ofHours(0)));
        Assert.assertEquals("1 hours", DurationStyle.LONG.toString(Duration.ofHours(1)));
        Assert.assertEquals("3 hours", DurationStyle.LONG.toString(Duration.ofHours(3)));

        Assert.assertThrows(DateTimeException.class, () -> DurationStyle.LONG.parse("a hour"));
        Assert.assertThrows(DateTimeException.class, () -> DurationStyle.LONG.parse("3 hur"));
        Assert.assertThrows(DateTimeException.class, () -> DurationStyle.LONG.parse("3h"));
        Assert.assertThrows(DateTimeException.class, () -> DurationStyle.LONG.parse("3H"));
    }

    @Test
    public void testLongMinutes() {
        Assert.assertEquals(Duration.ofMinutes(1), DurationStyle.LONG.parse("1 minute"));
        Assert.assertEquals(Duration.ofMinutes(1), DurationStyle.LONG.parse("1 minutes"));
        Assert.assertEquals(Duration.ofMinutes(1), DurationStyle.LONG.parse(" 1 minutes"));
        Assert.assertEquals(Duration.ofMinutes(1), DurationStyle.LONG.parse("1 minutes "));
        Assert.assertEquals(Duration.ofMinutes(1), DurationStyle.LONG.parse(" 1 minutes "));
        Assert.assertEquals(Duration.ofMinutes(1), DurationStyle.LONG.parse(" 1  minutes "));
        Assert.assertEquals(Duration.ofMinutes(1), DurationStyle.LONG.parse(" 1\tMINUTES "));

        Assert.assertEquals(Duration.ofMinutes(3), DurationStyle.LONG.parse("3 minute"));
        Assert.assertEquals(Duration.ofMinutes(3), DurationStyle.LONG.parse("3 minutes"));
        Assert.assertEquals(Duration.ofMinutes(3), DurationStyle.LONG.parse("3 MINUTES"));

        Assert.assertEquals("0 second", DurationStyle.LONG.toString(Duration.ofMinutes(0)));
        Assert.assertEquals("1 minutes", DurationStyle.LONG.toString(Duration.ofMinutes(1)));
        Assert.assertEquals("3 minutes", DurationStyle.LONG.toString(Duration.ofMinutes(3)));

        Assert.assertThrows(DateTimeException.class, () -> DurationStyle.LONG.parse("a minute"));
        Assert.assertThrows(DateTimeException.class, () -> DurationStyle.LONG.parse("3 min"));
        Assert.assertThrows(DateTimeException.class, () -> DurationStyle.LONG.parse("3m"));
        Assert.assertThrows(DateTimeException.class, () -> DurationStyle.LONG.parse("3M"));
    }

    @Test
    public void testLongSeconds() {
        Assert.assertEquals(Duration.ofSeconds(1), DurationStyle.LONG.parse("1 seconds"));
        Assert.assertEquals(Duration.ofSeconds(10), DurationStyle.LONG.parse("10 seconds"));
        Assert.assertThrows(DateTimeException.class, () -> DurationStyle.LONG.parse("a seconds"));
        Assert.assertThrows(DateTimeException.class, () -> DurationStyle.LONG.parse("3 scond"));
        Assert.assertThrows(DateTimeException.class, () -> DurationStyle.LONG.parse("3s"));
        Assert.assertThrows(DateTimeException.class, () -> DurationStyle.LONG.parse("3S"));
    }

    @Test
    public void testLongDays() {
        Assert.assertEquals(Duration.ofDays(1), DurationStyle.LONG.parse("1 days"));
        Assert.assertEquals(Duration.ofDays(2), DurationStyle.LONG.parse("2 days"));
    }

    @Test
    public void testLongCombined() {
        Assert.assertEquals(Duration.parse("P2DT3H4M5S"), DurationStyle.LONG.parse("2 days 3 Hours\t 4 minute 5 seconds"));
    }
}
