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

import com.starrocks.sql.common.UnsupportedException;
import com.starrocks.sql.parser.SqlParser;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

// Expected values are Trino's
public class TrinoWithTimezoneTest extends TrinoTestBase {
    private String originTimeZone;
    private boolean originDialectDowngrade;

    @BeforeAll
    public static void beforeClass() throws Exception {
        TrinoTestBase.beforeClass();
    }

    @BeforeEach
    public void saveSessionVariables() {
        originTimeZone = connectContext.getSessionVariable().getTimeZone();
        originDialectDowngrade = connectContext.getSessionVariable().isEnableDialectDowngrade();
    }

    @AfterEach
    public void restoreSessionVariables() {
        connectContext.getSessionVariable().setTimeZone(originTimeZone);
        connectContext.getSessionVariable().setEnableDialectDowngrade(originDialectDowngrade);
    }

    // The conversion doesn't go through the session time zone, so the result doesn't depend on it
    @Test
    public void testConvertedAsWallClockTime() throws Exception {
        for (String timeZone : List.of("UTC", "Asia/Shanghai", "America/Los_Angeles")) {
            connectContext.getSessionVariable().setTimeZone(timeZone);
            assertPlanContains("select cast(with_timezone(TIMESTAMP '2024-08-19 14:36:41', 'America/Los_Angeles') " +
                    "AT TIME ZONE 'UTC' AS TIMESTAMP)", "'2024-08-19 21:36:41'");
            assertPlanContains("select cast(at_timezone(with_timezone(TIMESTAMP '2024-08-19 14:36:41', 'UTC'), " +
                    "'America/Bogota') AS TIMESTAMP)", "'2024-08-19 09:36:41'");
            assertPlanContains("select date(at_timezone(with_timezone(TIMESTAMP '2024-08-19 23:36:41', 'UTC'), " +
                    "'Asia/Tokyo'))", "'2024-08-20'");
            assertPlanContains("select hour(with_timezone(TIMESTAMP '2024-08-19 14:36:41', '+03:00') " +
                    "AT TIME ZONE 'UTC')", "11");
            // America/Sao_Paulo skipped midnight on 2018-11-04; Trino's date_trunc moves to the end of the gap
            assertPlanContains("select cast(date_trunc('day', at_timezone(with_timezone(" +
                    "TIMESTAMP '2018-11-04 12:00:00', 'UTC'), 'America/Sao_Paulo')) AS TIMESTAMP)",
                    "'2018-11-04 01:00:00'");
        }
    }

    @Test
    public void testColumnsWithFixedOffsetZone() throws Exception {
        String values = " from (values TIMESTAMP '2024-08-19 14:36:41') t(ts)";
        assertPlanContains("select cast(with_timezone(ts, 'UTC') AT TIME ZONE 'America/Los_Angeles' AS TIMESTAMP)" +
                values, "convert_tz(1: ts, 'UTC', 'America/Los_Angeles')");
        // A format rounds the instant before the conversion
        assertPlanContains("select date_format(at_timezone(with_timezone(ts, '+03:00'), 'UTC'), '%Y-%m-%d')" + values,
                "convert_tz(time_slice(microseconds_add(", "'millisecond', 'floor'), '+03:00', 'UTC')");
        assertPlanContains("select cast(with_timezone(ts, '+00:00') AT TIME ZONE 'UTC' AS DATE)" + values,
                "convert_tz(1: ts, 'UTC', 'UTC')");
        // The wall-clock time of with_timezone(ts, zone) is ts
        String plan = getFragmentPlan("select date(with_timezone(ts, 'UTC'))" + values);
        Assertions.assertFalse(plan.contains("convert_tz"), plan);
    }

    // Trino rounds a zoned value half up to p digits in a cast to TIMESTAMP(p) (TIMESTAMP is TIMESTAMP(3)), and to
    // milliseconds when formatting it: second() of the first is 42, and the format gives 14:36:42
    @Test
    public void testRoundsLikeTrino() throws Exception {
        assertPlanContains("select second(CAST(with_timezone(TIMESTAMP '2024-08-19 14:36:41.6', 'UTC') " +
                "AS TIMESTAMP(0)))", "date_trunc('second', microseconds_add(", "500000)");
        assertPlanContains("select CAST(with_timezone(TIMESTAMP '2024-08-19 14:36:41.9996', 'UTC') AS TIMESTAMP)",
                "time_slice(microseconds_add(", "500), 1, 'millisecond', 'floor')");
        assertPlanContains("select date_format(with_timezone(TIMESTAMP '2024-08-19 14:36:41.9996', 'UTC'), " +
                "'%H:%i:%s')", "time_slice(microseconds_add(");
        assertPlanContains("select format_datetime(with_timezone(TIMESTAMP '2024-08-19 14:36:41.9996', 'UTC'), " +
                "'HH:mm:ss')", "time_slice(microseconds_add(");
        // A format rounds the instant, then reads it in its zone: 01:59:59.9996 PST is 03:00:00 PDT. A cast rounds the
        // wall-clock time in the zone instead, so the same value cast to TIMESTAMP is 02:00:00
        assertPlanContains("select date_format(with_timezone(TIMESTAMP '2024-03-10 01:59:59.9996', " +
                "'America/Los_Angeles'), '%H:%i:%s')", "convert_tz(time_slice(microseconds_add(",
                "'-08:00', 'America/Los_Angeles')");
        assertPlanContains("select cast(with_timezone(TIMESTAMP '2024-03-10 09:59:59.9996', 'UTC') " +
                "AT TIME ZONE 'America/Los_Angeles' AS TIMESTAMP)",
                "time_slice(microseconds_add('2024-03-10 01:59:59.999600', 500), 1, 'millisecond', 'floor')");
        // A TIMESTAMP(6) cast of the value is rounded again in a cast to a lower precision and in a format
        String values = " from (values TIMESTAMP '2024-08-19 14:36:41.6') t(ts)";
        assertPlanContains("select second(CAST(CAST(with_timezone(ts, 'UTC') AS TIMESTAMP(6)) AS TIMESTAMP(0)))" +
                values, "date_trunc('second', microseconds_add(", "500000)");
        assertPlanContains("select date_format(CAST(with_timezone(ts, 'UTC') AS TIMESTAMP(6)), '%H:%i:%s')" + values,
                "time_slice(microseconds_add(", "500), 1, 'millisecond', 'floor')");
        // A TIMESTAMP(p) cast in the with_timezone argument rounds as in Trino
        assertPlanContains("select second(with_timezone(CAST(ts AS TIMESTAMP(0)), 'UTC'))" + values,
                "date_trunc('second', microseconds_add(");
        String plan = getFragmentPlan("select CAST(with_timezone(TIMESTAMP '2024-08-19 14:36:41.6', 'UTC') " +
                "AS TIMESTAMP(6))");
        Assertions.assertFalse(plan.contains("microseconds_add"), plan);
    }

    // A cast with fractional seconds can be returned or compared; through a derived table only whole seconds can pass
    @Test
    public void testConsumersOfCasts() throws Exception {
        String values = " from (values TIMESTAMP '2024-08-19 14:36:41.6') t(ts)";
        assertPlanContains("select CAST(with_timezone(ts, 'UTC') AS TIMESTAMP)" + values +
                " where CAST(with_timezone(ts, 'UTC') AS TIMESTAMP) > TIMESTAMP '2024-08-19 00:00:00'", "time_slice(");
        assertPlanContains("select x from (select CAST(with_timezone(ts, 'UTC') AS TIMESTAMP(0)) x" + values + ") s",
                "date_trunc('second', microseconds_add(");
        // In a statement with with_timezone, every TIMESTAMP(p) cast rounds as in Trino: one compared with a
        // with_timezone value, and one that reaches with_timezone through a derived table (Trino: 42)
        String plan = getFragmentPlan("select 1 from (values (TIMESTAMP '2024-08-19 14:36:41.9996', " +
                "TIMESTAMP '2024-08-19 14:36:41.9996')) t(a, b) where CAST(with_timezone(a, 'UTC') AS TIMESTAMP) = " +
                "CAST(b AS TIMESTAMP)");
        Assertions.assertEquals(2, StringUtils.countMatches(plan, "time_slice("), plan);
        assertPlanContains("select second(with_timezone(x, 'UTC')) from (select CAST(ts AS TIMESTAMP(0)) x" + values +
                ") s", "date_trunc('second', microseconds_add(");
        // A literal that already fits isn't rounded, so it stays a constant
        plan = getFragmentPlan("select 1" + values + " where CAST(with_timezone(ts, 'UTC') AS TIMESTAMP) >= " +
                "CAST('2024-08-01' AS TIMESTAMP)");
        Assertions.assertEquals(1, StringUtils.countMatches(plan, "time_slice("), plan);
        // A literal in another form is rounded: Trino takes one-digit seconds (Trino: 2)
        assertPlanContains("select second(with_timezone(CAST('2024-08-19 14:36:1.6' AS TIMESTAMP(0)), 'UTC'))",
                "date_trunc('second', microseconds_add(");
    }

    // Below a day, truncation is exact only in a fixed-offset zone; Trino promotes a DATE argument to TIMESTAMP
    @Test
    public void testSubDayTruncation() throws Exception {
        assertPlanContains("select cast(date_trunc('hour', at_timezone(with_timezone(" +
                "TIMESTAMP '2024-08-19 14:36:41', 'UTC'), '+05:30')) AS TIMESTAMP)", "'2024-08-19 20:00:00'");
        assertPlanContains("select cast(date_trunc('hour', with_timezone(DATE '2024-08-19', 'UTC')) AS TIMESTAMP)",
                "'2024-08-19 00:00:00'");
    }

    // A literal's offset in a DST zone is resolved here, so the BE only converts from a fixed offset
    @Test
    public void testLiteralInDaylightSavingZone() throws Exception {
        boolean origin = connectContext.getSessionVariable().isDisableFunctionFoldConstants();
        connectContext.getSessionVariable().setDisableFunctionFoldConstants(true);
        try {
            assertPlanContains("select cast(with_timezone(TIMESTAMP '2024-08-19 14:36:41', 'America/Los_Angeles') " +
                    "AT TIME ZONE 'UTC' AS TIMESTAMP)", "convert_tz('2024-08-19 14:36:41', '-07:00', 'UTC')");
        } finally {
            connectContext.getSessionVariable().setDisableFunctionFoldConstants(origin);
        }
    }

    @Test
    public void testRejected() {
        connectContext.getSessionVariable().setEnableDialectDowngrade(true);
        String values = " from (values TIMESTAMP '2024-08-19 14:36:41') t(ts)";
        for (String sql : List.of(
                // Consumers that read the value as an instant or show its zone
                "select with_timezone(TIMESTAMP '2024-08-19 14:36:41', 'UTC')",
                "select at_timezone(with_timezone(TIMESTAMP '2024-08-19 14:36:41', 'UTC'), 'Asia/Tokyo')",
                "select to_unixtime(with_timezone(TIMESTAMP '2024-08-19 14:36:41', 'UTC') AT TIME ZONE 'Asia/Tokyo')",
                "select with_timezone(TIMESTAMP '2024-08-19 14:36:41', 'UTC') > TIMESTAMP '2024-08-19 00:00:00'",
                "select cast(with_timezone(TIMESTAMP '2024-08-19 14:36:41', 'UTC') AT TIME ZONE 'Asia/Tokyo' AS VARCHAR)",
                "select format_datetime(at_timezone(with_timezone(TIMESTAMP '2024-08-19 14:36:41', 'UTC'), " +
                        "'Asia/Tokyo'), 'yyyy-MM-dd HH:mm ZZ')",
                "select date_trunc('day', with_timezone(TIMESTAMP '2024-08-19 14:36:41', 'UTC'))",
                // Only a direct consumer is checked
                "select cast(date_trunc('month', date_trunc('day', at_timezone(with_timezone(" +
                        "TIMESTAMP '2024-08-19 14:36:41', 'UTC'), 'Asia/Tokyo'))) AS TIMESTAMP)",
                "select cast(coalesce(with_timezone(TIMESTAMP '2024-08-19 14:36:41', 'UTC') AT TIME ZONE " +
                        "'Asia/Tokyo', NULL) AS DATE)",
                "with t as (select with_timezone(TIMESTAMP '2024-08-19 14:36:41', 'UTC') AT TIME ZONE 'Asia/Tokyo' " +
                        "as x) select cast(x AS TIMESTAMP) from t",
                "select cast(at_timezone(with_timezone(TIMESTAMP '2024-08-19 14:36:41', 'UTC'), 'Asia/Tokyo') " +
                        "over () AS TIMESTAMP)",
                // Trino truncates below a day in the value's offset, which a 30-minute DST shift tells apart
                "select cast(date_trunc('hour', with_timezone(TIMESTAMP '2024-10-06 02:45:00', " +
                        "'Australia/Lord_Howe')) AS TIMESTAMP)",
                // A precision without a matching rounding unit
                "select cast(with_timezone(TIMESTAMP '2024-08-19 14:36:41.66', 'UTC') AS TIMESTAMP(1))",
                // TIME rounds differently from TIMESTAMP(p)
                "select cast(cast(with_timezone(TIMESTAMP '2024-08-19 14:36:41', 'UTC') AS TIMESTAMP(6)) AS TIME)",
                // Trino prints a Java-style mask as written; StarRocks formats it
                "select date_format(with_timezone(TIMESTAMP '2024-08-19 14:36:41', 'UTC'), 'yyyy-MM-dd')",
                // Joda masks the BE formats differently or not at all
                "select format_datetime(with_timezone(TIMESTAMP '2024-08-19 14:36:41', 'UTC'), 'HH:mm:ss.SSSSSSS')",
                "select format_datetime(with_timezone(TIMESTAMP '2024-08-19 14:36:41', 'UTC'), 'xxxx-ww')",
                "select format_datetime(with_timezone(TIMESTAMP '2024-08-19 14:36:41', 'UTC'), 'HH''''mm')",
                // Output the BE formatters don't have room for
                "select date_format(with_timezone(TIMESTAMP '2024-08-19 14:36:41', 'UTC'), '" + "%Y".repeat(32) + "')",
                "select date_format(with_timezone(TIMESTAMP '2024-08-19 14:36:41', 'UTC'), '" + "%r".repeat(12) + "')",
                // The BE rejects a field that would reach the 127th byte
                "select date_format(with_timezone(TIMESTAMP '2024-08-19 14:36:41', 'UTC'), '" + "x".repeat(123) +
                        "%Y')",
                // Trino drops an unpaired trailing %; the BE prints it
                "select date_format(with_timezone(TIMESTAMP '2024-08-19 14:36:41', 'UTC'), '%Y%')",
                "select format_datetime(with_timezone(TIMESTAMP '2024-08-19 14:36:41', 'UTC'), '" +
                        "EEEE ".repeat(16) + "')",
                // A cast reads the value as a wall-clock time, but the format around it still prints other than Trino
                "select date_format(CAST(with_timezone(TIMESTAMP '2024-08-19 14:36:41', 'UTC') AS TIMESTAMP(6)), " +
                        "'yyyy-MM-dd')",
                "select date_format(CAST(with_timezone(ts, 'UTC') AS DATE), 'yyyy-MM-dd')" + values,
                "select format_datetime(CAST(with_timezone(ts, 'UTC') AS TIMESTAMP), fmt) " +
                        "from (values (TIMESTAMP '2024-08-19 14:36:41', 'HH:mm')) t(ts, fmt)",
                // or through a derived table or a CTE, which isn't tracked, so the whole statement is rejected
                "select date_format(ts, 'yyyy-MM-dd') from (select CAST(with_timezone(" +
                        "TIMESTAMP '2024-08-19 14:36:41', 'UTC') AS TIMESTAMP(6)) as ts) t",
                "with t as (select CAST(with_timezone(TIMESTAMP '2024-08-19 14:36:41', 'UTC') AS DATE) as d) " +
                        "select date_format(d, 'yyyy-MM-dd') from t",
                // Fractional seconds that Trino rounds again where a derived table, a CTE or another function hands
                // the value to a format or a cast (Trino: 14:36:42 and 42)
                "select date_format(ts, '%H:%i:%s') from (select CAST(with_timezone(" +
                        "TIMESTAMP '2024-08-19 14:36:41.9996', 'UTC') AS TIMESTAMP(6)) as ts) t",
                "select second(CAST(ts AS TIMESTAMP(0))) from (select CAST(with_timezone(" +
                        "TIMESTAMP '2024-08-19 14:36:41.6', 'UTC') AS TIMESTAMP(6)) as ts) t",
                "with t as (select CAST(with_timezone(TIMESTAMP '2024-08-19 14:36:41.6', 'UTC') AS TIMESTAMP) as x) " +
                        "select x from t",
                "select date_format(coalesce(CAST(with_timezone(ts, 'UTC') AS TIMESTAMP(6)), ts), '%H:%i:%s')" +
                        values,
                "select CAST(CAST(with_timezone(ts, 'UTC') AS TIMESTAMP) AS VARCHAR)" + values,
                // Trino rejects with_timezone as a window function
                "select cast(with_timezone(TIMESTAMP '2024-08-19 14:36:41', 'UTC') over () AS DATE)",
                // A DST zone attached to a column, or to a literal it skips or repeats: Trino and convert_tz resolve
                // those differently (Trino: 10:30, 08:30 and 01:30 UTC)
                "select cast(with_timezone(ts, 'America/Los_Angeles') AT TIME ZONE 'UTC' AS TIMESTAMP)" + values,
                "select cast(with_timezone(TIMESTAMP '2024-03-10 02:30:00', 'America/Los_Angeles') AT TIME ZONE " +
                        "'UTC' AS TIMESTAMP)",
                "select cast(with_timezone(TIMESTAMP '2024-11-03 01:30:00', 'America/Los_Angeles') AT TIME ZONE " +
                        "'UTC' AS TIMESTAMP)",
                "select cast(with_timezone(TIMESTAMP '2024-10-27 02:30:00', 'Europe/Berlin') AT TIME ZONE " +
                        "'UTC' AS TIMESTAMP)",
                // Trino rejects a zoned argument and an offset past +14:00; a function source may differ on its own
                // (from_unixtime(-1) is NULL in StarRocks; Trino: 1969)
                "select year(with_timezone(CAST(from_unixtime(-1) AS TIMESTAMP(6)), 'UTC'))",
                "select hour(with_timezone(date_add('hour', 1, ts), 'UTC'))" + values,
                // Trino rounds digits past microseconds, which StarRocks drops (Trino: 42)
                "select second(CAST(with_timezone(CAST('2024-08-19 14:36:41.4999995' AS TIMESTAMP(6)), 'UTC') " +
                        "AS TIMESTAMP(0)))",
                "select CAST(with_timezone(now(), 'UTC') AS DATE)",
                "select CAST(with_timezone(from_unixtime(0), 'UTC') AS DATE)",
                "select CAST(with_timezone(ts AT TIME ZONE 'UTC', 'UTC') AS DATE)" + values,
                "select CAST(with_timezone(TIMESTAMP '2024-01-01 00:00:00', '+14:30') AT TIME ZONE 'UTC' AS TIMESTAMP)",
                // These parses return TIMESTAMP(3) in Trino and drop digits that StarRocks keeps (Trino: 14:36:41)
                "select date_format(with_timezone(date_parse('2024-08-19 14:36:41.999999', " +
                        "'%Y-%m-%d %H:%i:%s.%f'), 'UTC'), '%H:%i:%s')",
                "select date_format(with_timezone(CAST(parse_datetime('2024-08-19 14:36:41.999999', " +
                        "'yyyy-MM-dd HH:mm:ss.SSSSSS') AS TIMESTAMP(6)), 'UTC'), '%H:%i:%s')",
                // Zones the BE would resolve differently, or not at all
                "select cast(with_timezone(ts, 'utc') AT TIME ZONE 'UTC' AS TIMESTAMP)" + values,
                "select cast(with_timezone(ts, 'GMT+8') AT TIME ZONE 'UTC' AS TIMESTAMP)" + values,
                "select cast(with_timezone(ts, 'CST') AT TIME ZONE 'UTC' AS TIMESTAMP)" + values,
                "select cast(with_timezone(ts, 'UTC') AT TIME ZONE 'america/los_angeles' AS TIMESTAMP)" + values,
                "select cast(with_timezone(ts, '-13:00') AT TIME ZONE 'UTC' AS TIMESTAMP)" + values,
                "select cast(with_timezone(ts, zone) AT TIME ZONE 'UTC' AS TIMESTAMP) " +
                        "from (values (TIMESTAMP '2024-08-19 14:36:41', 'UTC')) t(ts, zone)")) {
            UnsupportedException e = Assertions.assertThrows(UnsupportedException.class,
                    () -> SqlParser.parse(sql, connectContext.getSessionVariable()), sql);
            Assertions.assertTrue(e.getMessage().contains("with_timezone is supported only"), e.getMessage());
            Assertions.assertFalse(e.getMessage().contains("StarRocks parser also can not parse"), e.getMessage());
        }
    }
}
