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
import java.time.Duration;

public class DurationStyleTest {
    @Test
    public void testLongHours() {
        Assertions.assertEquals(Duration.ofHours(1), DurationStyle.LONG.parse("1 hour"));
        Assertions.assertEquals(Duration.ofHours(1), DurationStyle.LONG.parse("1 hours"));
        Assertions.assertEquals(Duration.ofHours(1), DurationStyle.LONG.parse(" 1 hours"));
        Assertions.assertEquals(Duration.ofHours(1), DurationStyle.LONG.parse("1 hours "));
        Assertions.assertEquals(Duration.ofHours(1), DurationStyle.LONG.parse(" 1 hours "));
        Assertions.assertEquals(Duration.ofHours(1), DurationStyle.LONG.parse(" 1  hours "));
        Assertions.assertEquals(Duration.ofHours(1), DurationStyle.LONG.parse(" 1\thours "));

        Assertions.assertEquals(Duration.ofHours(3), DurationStyle.LONG.parse("3 hour"));
        Assertions.assertEquals(Duration.ofHours(3), DurationStyle.LONG.parse("3 hours"));
        Assertions.assertEquals(Duration.ofHours(3), DurationStyle.LONG.parse("3 HOURS"));

        Assertions.assertEquals("0 second", DurationStyle.LONG.toString(Duration.ofHours(0)));
        Assertions.assertEquals("1 hours", DurationStyle.LONG.toString(Duration.ofHours(1)));
        Assertions.assertEquals("3 hours", DurationStyle.LONG.toString(Duration.ofHours(3)));

        Assertions.assertThrows(DateTimeException.class, () -> DurationStyle.LONG.parse("a hour"));
        Assertions.assertThrows(DateTimeException.class, () -> DurationStyle.LONG.parse("3 hur"));
        Assertions.assertThrows(DateTimeException.class, () -> DurationStyle.LONG.parse("3h"));
        Assertions.assertThrows(DateTimeException.class, () -> DurationStyle.LONG.parse("3H"));
    }

    @Test
    public void testLongMinutes() {
        Assertions.assertEquals(Duration.ofMinutes(1), DurationStyle.LONG.parse("1 minute"));
        Assertions.assertEquals(Duration.ofMinutes(1), DurationStyle.LONG.parse("1 minutes"));
        Assertions.assertEquals(Duration.ofMinutes(1), DurationStyle.LONG.parse(" 1 minutes"));
        Assertions.assertEquals(Duration.ofMinutes(1), DurationStyle.LONG.parse("1 minutes "));
        Assertions.assertEquals(Duration.ofMinutes(1), DurationStyle.LONG.parse(" 1 minutes "));
        Assertions.assertEquals(Duration.ofMinutes(1), DurationStyle.LONG.parse(" 1  minutes "));
        Assertions.assertEquals(Duration.ofMinutes(1), DurationStyle.LONG.parse(" 1\tMINUTES "));

        Assertions.assertEquals(Duration.ofMinutes(3), DurationStyle.LONG.parse("3 minute"));
        Assertions.assertEquals(Duration.ofMinutes(3), DurationStyle.LONG.parse("3 minutes"));
        Assertions.assertEquals(Duration.ofMinutes(3), DurationStyle.LONG.parse("3 MINUTES"));

        Assertions.assertEquals("0 second", DurationStyle.LONG.toString(Duration.ofMinutes(0)));
        Assertions.assertEquals("1 minutes", DurationStyle.LONG.toString(Duration.ofMinutes(1)));
        Assertions.assertEquals("3 minutes", DurationStyle.LONG.toString(Duration.ofMinutes(3)));

        Assertions.assertThrows(DateTimeException.class, () -> DurationStyle.LONG.parse("a minute"));
        Assertions.assertThrows(DateTimeException.class, () -> DurationStyle.LONG.parse("3 min"));
        Assertions.assertThrows(DateTimeException.class, () -> DurationStyle.LONG.parse("3m"));
        Assertions.assertThrows(DateTimeException.class, () -> DurationStyle.LONG.parse("3M"));
    }

    @Test
    public void testLongSeconds() {
        Assertions.assertEquals(Duration.ofSeconds(1), DurationStyle.LONG.parse("1 seconds"));
        Assertions.assertEquals(Duration.ofSeconds(10), DurationStyle.LONG.parse("10 seconds"));
        Assertions.assertThrows(DateTimeException.class, () -> DurationStyle.LONG.parse("a seconds"));
        Assertions.assertThrows(DateTimeException.class, () -> DurationStyle.LONG.parse("3 scond"));
        Assertions.assertThrows(DateTimeException.class, () -> DurationStyle.LONG.parse("3s"));
        Assertions.assertThrows(DateTimeException.class, () -> DurationStyle.LONG.parse("3S"));
    }

    @Test
    public void testLongDays() {
        Assertions.assertEquals(Duration.ofDays(1), DurationStyle.LONG.parse("1 days"));
        Assertions.assertEquals(Duration.ofDays(2), DurationStyle.LONG.parse("2 days"));
    }

    @Test
    public void testLongCombined() {
        Assertions.assertEquals(Duration.parse("P2DT3H4M5S"), DurationStyle.LONG.parse("2 days 3 Hours\t 4 minute 5 seconds"));
    }
}
