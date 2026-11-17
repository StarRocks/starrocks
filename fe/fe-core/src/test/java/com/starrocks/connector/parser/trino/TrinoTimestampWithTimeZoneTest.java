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

package com.starrocks.connector.parser.trino;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

/**
 * Cases ported from Trino's TestTimestampWithTimeZone, TestAtTimeZone and the TIMESTAMP WITH TIME ZONE docs.
 * StarRocks has no TIMESTAMP WITH TIME ZONE type: such a value is the wall clock of its instant in the session
 * time zone, which is also how timestamptz columns are read. So the expected values below are the Trino instants
 * rendered in the session time zone, and everything that only depends on the instant must not depend on the
 * session time zone.
 */
public class TrinoTimestampWithTimeZoneTest extends TrinoTestBase {
    private static final List<String> SESSION_TIME_ZONES =
            List.of("UTC", "Asia/Shanghai", "America/Los_Angeles", "Pacific/Apia");

    private String originTimeZone;

    @BeforeAll
    public static void beforeClass() throws Exception {
        TrinoTestBase.beforeClass();
    }

    @BeforeEach
    public void saveTimeZone() {
        originTimeZone = connectContext.getSessionVariable().getTimeZone();
    }

    @AfterEach
    public void restoreTimeZone() {
        connectContext.getSessionVariable().setTimeZone(originTimeZone);
    }

    private void setTimeZone(String timeZone) {
        connectContext.getSessionVariable().setTimeZone(timeZone);
    }

    private void assertLiteral(String literal, String expected) throws Exception {
        assertPlanContains("select TIMESTAMP '" + literal + "'", "'" + expected + "'");
    }

    // https://trino.io/docs/current/language/types.html#timestamp-with-time-zone
    @Test
    public void testLiterals() throws Exception {
        setTimeZone("UTC");
        // IANA zone with a non-whole-hour offset (+05:45)
        assertLiteral("2020-05-01 12:34:56 Asia/Kathmandu", "2020-05-01 06:49:56");
        assertLiteral("2020-05-01 12:34:56.1 Asia/Kathmandu", "2020-05-01 06:49:56.100000");
        assertLiteral("2020-05-01 12:34:56.123 Asia/Kathmandu", "2020-05-01 06:49:56.123000");
        assertLiteral("2020-05-01 12:34:56.123456 Asia/Kathmandu", "2020-05-01 06:49:56.123456");
        // offsets
        assertLiteral("2020-05-01 12:34:56 -00:35", "2020-05-01 13:09:56");
        assertLiteral("2020-05-01 12:34:56.123456 -00:35", "2020-05-01 13:09:56.123456");
        assertLiteral("2001-01-02 +07:09", "2001-01-01 16:51:00");
        assertLiteral("2001-1-2 3:4:5.321+07:09", "2001-01-01 19:55:05.321000");
        assertLiteral("2001-1-2 3:4:5+07:09", "2001-01-01 19:55:05");
        assertLiteral("2001-1-2 3:4+07:09", "2001-01-01 19:55:00");
        assertLiteral("2001-1-2 +07:09", "2001-01-01 16:51:00");
        // IANA zone, with and without time of day
        assertLiteral("2001-01-02 03:04:05.321 Europe/Berlin", "2001-01-02 02:04:05.321000");
        assertLiteral("2001-01-02 03:04:05 Europe/Berlin", "2001-01-02 02:04:05");
        assertLiteral("2001-01-02 03:04 Europe/Berlin", "2001-01-02 02:04:00");
        assertLiteral("2001-01-02 Europe/Berlin", "2001-01-01 23:00:00");
        // examples in the docs, including the UTC aliases
        assertLiteral("2001-08-22 03:04:05.321 UTC", "2001-08-22 03:04:05.321000");
        assertLiteral("2001-08-22 03:04:05.321 -08:30", "2001-08-22 11:34:05.321000");
        assertLiteral("2001-08-22 03:04:05.321 GMT-08:30", "2001-08-22 11:34:05.321000");
        assertLiteral("2001-08-22 03:04:05.321 America/New_York", "2001-08-22 07:04:05.321000");
        assertLiteral("2001-08-22 03:04:05 GMT", "2001-08-22 03:04:05");
        assertLiteral("2001-08-22 03:04:05 UT", "2001-08-22 03:04:05");
        assertLiteral("2001-08-22 03:04:05 Z", "2001-08-22 03:04:05");

        setTimeZone("Asia/Shanghai");
        assertLiteral("2024-01-01 09:00:00 Asia/Tokyo", "2024-01-01 08:00:00");
        assertLiteral("2024-01-01 09:00:00 UTC", "2024-01-01 17:00:00");
        assertLiteral("2024-01-01 09:00:00 Asia/Shanghai", "2024-01-01 09:00:00");
        assertLiteral("2001-01-02 Europe/Berlin", "2001-01-02 07:00:00");
    }

    // The offset is resolved in FE, so without constant folding BE only converts from a fixed offset to the session
    // zone and gets the same result as FE folding
    @Test
    public void testOffsetResolvedBeforeBackend() throws Exception {
        setTimeZone("UTC");
        boolean origin = connectContext.getSessionVariable().isDisableFunctionFoldConstants();
        connectContext.getSessionVariable().setDisableFunctionFoldConstants(true);
        try {
            assertPlanContains("select TIMESTAMP '2001-08-22 03:04:05 Z'", "convert_tz('2001-08-22 03:04:05', 'UTC'");
            assertPlanContains("select TIMESTAMP '2001-08-22 03:04:05 UT'", "convert_tz('2001-08-22 03:04:05', 'UTC'");
            assertPlanContains("select TIMESTAMP '2001-08-22 03:04:05 GMT-08:30'",
                    "convert_tz('2001-08-22 03:04:05', '-08:30'");
            assertPlanContains("select TIMESTAMP '2001-08-22 03:04:05 +07:09'",
                    "convert_tz('2001-08-22 03:04:05', '+07:09'");
            assertPlanContains("select TIMESTAMP '2001-08-22 03:04:05 Asia/Kathmandu'",
                    "convert_tz('2001-08-22 03:04:05', '+05:45'");
            // ambiguous wall clock in a DST fall-back takes the earlier offset (PDT)
            assertPlanContains("select TIMESTAMP '2024-11-03 01:30:00 America/Los_Angeles'",
                    "convert_tz('2024-11-03 01:30:00', '-07:00'");
            // local mean time has an offset with seconds, keep the zone id
            assertPlanContains("select TIMESTAMP '1500-05-01 12:34:56 Asia/Kathmandu'",
                    "convert_tz('1500-05-01 12:34:56', 'Asia/Kathmandu'");
            // local mean time -14:21 is outside the offsets BE parses, keep the zone id
            assertPlanContains("select TIMESTAMP '1800-01-01 00:00:00 Pacific/Guam'",
                    "convert_tz('1800-01-01 00:00:00', 'Pacific/Guam'");
            assertPlanContains("select TIMESTAMP '2001-08-22 03:04:05 -12:30'",
                    "convert_tz('2001-08-22 03:04:05', '-12:30'");
        } finally {
            connectContext.getSessionVariable().setDisableFunctionFoldConstants(origin);
        }
    }

    // Trino DateTimes.toEpochSecond: a skipped wall clock is invalid, an ambiguous one takes the earlier offset
    @Test
    public void testDaylightSavingTransitions() throws Exception {
        setTimeZone("UTC");
        assertLiteral("2024-11-03 01:30:00 America/Los_Angeles", "2024-11-03 08:30:00");
        assertLiteral("2024-03-10 01:59:59 America/Los_Angeles", "2024-03-10 09:59:59");
        assertLiteral("2024-03-10 03:00:00 America/Los_Angeles", "2024-03-10 10:00:00");
        analyzeFail("select TIMESTAMP '2024-03-10 02:30:00 America/Los_Angeles'", "Invalid date literal");
    }

    // literals without a time zone go through the same parsing
    @Test
    public void testLiteralsWithoutTimeZone() throws Exception {
        assertLiteral("2001-1-2 3:4:5", "2001-01-02 03:04:05");
        assertLiteral("2001-1-2 3:4", "2001-01-02 03:04:00");
        assertLiteral("2001-1-2", "2001-01-02 00:00:00");
        assertLiteral("2020-05-01 12:34:56.123456", "2020-05-01 12:34:56.123456");
    }

    // the instant is outside the DATETIME range in UTC, but the session wall clock is not
    @Test
    public void testLiteralsAtDatetimeBounds() throws Exception {
        setTimeZone("-05:00");
        assertLiteral("9999-12-31 23:00:00 -05:00", "9999-12-31 23:00:00");
        setTimeZone("+08:00");
        assertLiteral("0000-01-01 01:00:00 +08:00", "0000-01-01 01:00:00");
    }

    @Test
    public void testInvalidLiterals() {
        analyzeFail("select TIMESTAMP '2020-13-01 Asia/Kathmandu'", "Invalid date literal");
        analyzeFail("select TIMESTAMP '2020-05-01 12:34:56 No/Such_Zone'", "Invalid date literal");
        analyzeFail("select TIMESTAMP '2020-13-01-12'", "Invalid date literal");
        // out of the DATETIME range
        analyzeFail("select TIMESTAMP '123001-01-02 03:04:05.321 Europe/Berlin'", "Invalid date literal");
        analyzeFail("select TIMESTAMP '+123001-01-02 03:04:05.321 Europe/Berlin'", "Invalid date literal");
        analyzeFail("select TIMESTAMP '-123001-01-02 03:04:05.321 Europe/Berlin'", "Invalid date literal");
        // Trino allows up to 12 fractional digits, DATETIME keeps microseconds
        analyzeFail("select TIMESTAMP '2020-05-01 12:34:56.1234567 Asia/Kathmandu'", "Invalid date literal");
    }

    // https://trino.io/docs/current/functions/datetime.html#time-zone-conversion
    @Test
    public void testAtTimeZone() throws Exception {
        for (String timeZone : SESSION_TIME_ZONES) {
            setTimeZone(timeZone);
            assertPlanContains("select TIMESTAMP '2012-10-31 01:00 UTC' AT TIME ZONE 'America/Los_Angeles'",
                    "'2012-10-30 18:00:00'");
            assertPlanContains("select at_timezone(TIMESTAMP '2020-05-01 12:34:56 +07:09', 'America/Los_Angeles')",
                    "'2020-04-30 22:25:56'");
            assertPlanContains("select at_timezone(TIMESTAMP '2020-05-01 12:34:56.123456 +07:09', " +
                    "'America/Los_Angeles')", "'2020-04-30 22:25:56.123456'");
        }
    }

    @Test
    public void testToUnixTime() throws Exception {
        for (String timeZone : SESSION_TIME_ZONES) {
            setTimeZone(timeZone);
            assertPlanContains("select to_unixtime(TIMESTAMP '2020-05-10 12:34:56 Asia/Kathmandu')", "1589093396");
        }
    }

    // Trino testOrdering / testJoin: values compare as instants, whatever zone they are written in
    @Test
    public void testCompareAcrossTimeZones() throws Exception {
        for (String timeZone : SESSION_TIME_ZONES) {
            setTimeZone(timeZone);
            assertPlanContains("select TIMESTAMP '2020-05-10 04:00:00 America/New_York' = " +
                    "TIMESTAMP '2020-05-10 01:00:00 America/Los_Angeles'", "TRUE");
            assertPlanContains("select TIMESTAMP '2020-05-10 03:00:00 America/New_York' < " +
                    "TIMESTAMP '2020-05-10 01:00:00 America/Los_Angeles'", "TRUE");
            assertPlanContains("select TIMESTAMP '2020-05-10 01:00:00 America/Los_Angeles' < " +
                    "TIMESTAMP '2020-05-10 02:00:00 America/Los_Angeles'", "TRUE");
        }
    }
}
