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

import org.junit.BeforeClass;
import org.junit.Test;

public class TrinoFunctionTransformTest extends TrinoTestBase {
    @BeforeClass
    public static void beforeClass() throws Exception {
        TrinoTestBase.beforeClass();
    }

    @Test
    public void testAggFnTransform() throws Exception {
        String sql = "select approx_distinct(v1) from t0; ";
        assertPlanContains(sql, "output: approx_count_distinct(1: v1)");

        sql = "select arbitrary(v1) from t0; ";
        assertPlanContains(sql, "output: any_value(1: v1)");

        sql = "select approx_percentile(v1, 0.99) from t0;";
        assertPlanContains(sql, "output: percentile_approx(CAST(1: v1 AS DOUBLE), 0.99)");

        sql = "select stddev(v1) from t0;";
        assertPlanContains(sql, "output: stddev_samp(1: v1)");

        sql = "select stddev_pop(v1) from t0;";
        assertPlanContains(sql, "output: stddev(1: v1)");

        sql = "select variance(v1) from t0;";
        assertPlanContains(sql, "output: var_samp(1: v1)");

        sql = "select var_pop(v1) from t0;";
        assertPlanContains(sql, "output: variance(1: v1)");

        sql = "select count_if(v1) from t0;";
        assertPlanContains(sql, "  2:AGGREGATE (update finalize)\n" +
                "  |  output: count(if(CAST(1: v1 AS BOOLEAN), 1, NULL))");
    }

    @Test
    public void testArrayFnTransform() throws Exception {
        String sql = "select array_union(c1, c2) from test_array";
        assertPlanContains(sql, "array_distinct(array_concat(2: c1, CAST(3: c2 AS ARRAY<VARCHAR");

        sql = "select concat(array[1,2,3], array[4,5,6]) from test_array";
        assertPlanContains(sql, "array_concat([1,2,3], [4,5,6])");

        sql = "select concat(c1, c2) from test_array";
        assertPlanContains(sql, "array_concat(2: c1, CAST(3: c2 AS ARRAY<VARCHAR(65533)>))");

        sql = "select concat(c1, c2, array[1,2], array[3,4]) from test_array";
        assertPlanContains(sql, "array_concat(2: c1, CAST(3: c2 AS ARRAY<VARCHAR(65533)>), " +
                "CAST([1,2] AS ARRAY<VARCHAR(65533)>), " +
                "CAST([3,4] AS ARRAY<VARCHAR(65533)>)");

        sql = "select concat(c2, 2) from test_array";
        assertPlanContains(sql, "array_concat(3: c2, CAST([2] AS ARRAY<INT>))");

        sql = "select contains(array[1,2,3], 1)";
        assertPlanContains(sql, "array_contains([1,2,3], 1)");

        sql = "select slice(array[1,2,3,4], 2, 2)";
        assertPlanContains(sql, "array_slice([1,2,3,4], 2, 2)");
    }

    @Test
    public void testArrayFnWithLambdaExpr() throws Exception {
        String sql = "select filter(array[], x -> true);";
        assertPlanContains(sql, "array_filter([], array_map(<slot 2> -> TRUE, []))");

        sql = "select filter(array[5, -6, NULL, 7], x -> x > 0);";
        assertPlanContains(sql, " array_filter([5,-6,NULL,7], array_map(<slot 2> -> <slot 2> > 0, [5,-6,NULL,7]))");

        sql = "select filter(array[5, NULL, 7, NULL], x -> x IS NOT NULL);";
        assertPlanContains(sql, "array_filter([5,NULL,7,NULL], array_map(<slot 2> -> <slot 2> IS NOT NULL, [5,NULL,7,NULL]))");

        sql = "select array_sort(array[1, 2, 3])";
        assertPlanContains(sql, "array_sort([1,2,3])");
    }

    @Test
    public void testDateFnTransform() throws Exception {
        String sql = "select to_unixtime(TIMESTAMP '2023-04-22 00:00:00');";
        assertPlanContains(sql, "1682092800");

        sql = "select date_parse('2022/10/20/05', '%Y/%m/%d/%H');";
        assertPlanContains(sql, "2022-10-20 05:00:00");

        sql = "SELECT date_parse('20141221','%Y%m%d');";
        assertPlanContains(sql, "'2014-12-21'");

        sql = "select date_parse('2014-12-21 12:34:56', '%Y-%m-%d %H:%i:%s');";
        assertPlanContains(sql, "2014-12-21 12:34:56");

        sql = "select day_of_week(timestamp '2022-03-06 01:02:03');";
        assertPlanContains(sql, "dayofweek_iso('2022-03-06 01:02:03')");

        sql = "select dow(timestamp '2022-03-06 01:02:03');";
        assertPlanContains(sql, "dayofweek_iso('2022-03-06 01:02:03')");

        sql = "select dow(date '2022-03-06');";
        assertPlanContains(sql, "dayofweek_iso('2022-03-06 00:00:00')");

        sql = "select day_of_month(timestamp '2022-03-06 01:02:03');";
        assertPlanContains(sql, "dayofmonth('2022-03-06 01:02:03')");

        sql = "select day_of_month(date '2022-03-06');";
        assertPlanContains(sql, "dayofmonth('2022-03-06 00:00:00')");

        sql = "select day_of_year(timestamp '2022-03-06 01:02:03');";
        assertPlanContains(sql, "dayofyear('2022-03-06 01:02:03')");

        sql = "select day_of_year(date '2022-03-06');";
        assertPlanContains(sql, "dayofyear('2022-03-06 00:00:00')");

        sql = "select doy(timestamp '2022-03-06 01:02:03');";
        assertPlanContains(sql, "dayofyear('2022-03-06 01:02:03')");

        sql = "select doy(date '2022-03-06');";
        assertPlanContains(sql, "dayofyear('2022-03-06 00:00:00')");

        sql = "select week_of_year(timestamp '2023-01-01 00:00:00');";
        assertPlanContains(sql, "week_iso('2023-01-01 00:00:00')");

        sql = "select week(timestamp '2023-01-01');";
        assertPlanContains(sql, "week_iso('2023-01-01 00:00:00')");

        sql = "select week_of_year(date '2023-01-01');";
        assertPlanContains(sql, "week_iso('2023-01-01 00:00:00')");

        sql = "select week(date '2023-01-01');";
        assertPlanContains(sql, "week_iso('2023-01-01 00:00:00')");

        sql = "select format_datetime(TIMESTAMP '2023-06-25 11:10:20', 'yyyyMMdd HH:mm:ss')";
        assertPlanContains(sql, "jodatime_format('2023-06-25 11:10:20', 'yyyyMMdd HH:mm:ss')");

        sql = "select format_datetime(date '2023-06-25', 'yyyyMMdd HH:mm:ss');";
        assertPlanContains(sql, "jodatime_format('2023-06-25', 'yyyyMMdd HH:mm:ss')");

        sql = "select last_day_of_month(timestamp '2023-07-01 00:00:00');";
        assertPlanContains(sql, "last_day('2023-07-01 00:00:00', 'month')");

        sql = "select last_day_of_month(date '2023-07-01');";
        assertPlanContains(sql, "last_day('2023-07-01 00:00:00', 'month')");
    }

    @Test
    public void testDateAddFnTransform() throws Exception {
        String sql = "select date_add('day', 1, TIMESTAMP '2014-03-08 09:00:00');";
        assertPlanContains(sql, "2014-03-09 09:00:00");

        sql = "select date_add('second', 1, TIMESTAMP '2014-03-08 09:00:00');";
        assertPlanContains(sql, "2014-03-08 09:00:01");

        sql = "select date_add('minute', 1, TIMESTAMP '2014-03-08 09:00:00');";
        assertPlanContains(sql, "2014-03-08 09:01:00");

        sql = "select date_add('hour', 1, TIMESTAMP '2014-03-08 09:00:00');";
        assertPlanContains(sql, "2014-03-08 10:00:00");

        sql = "select date_add('week', 1, TIMESTAMP '2014-03-08 09:00:00');";
        assertPlanContains(sql, "weeks_add('2014-03-08 09:00:00', 1)");

        sql = "select date_add('month', 1, TIMESTAMP '2014-03-08 09:00:00');";
        assertPlanContains(sql, "'2014-04-08 09:00:00'");

        sql = "select date_add('year', 1, TIMESTAMP '2014-03-08 09:00:00');";
        assertPlanContains(sql, "'2015-03-08 09:00:00'");

        sql = "select date_add('second', 2, th) from tall;";
        assertPlanContains(sql, "seconds_add(8: th, 2)");

        sql = "select date_add('minute', 2, th) from tall;";
        assertPlanContains(sql, "minutes_add(8: th, 2)");

        sql = "select date_add('hour', 2, th) from tall;";
        assertPlanContains(sql, "hours_add(8: th, 2)");

        sql = "select date_add('day', 2, th) from tall;";
        assertPlanContains(sql, "days_add(8: th, 2)");

        sql = "select date_add('week', 2, th) from tall;";
        assertPlanContains(sql, "weeks_add(8: th, 2)");

        sql = "select date_add('month', 2, th) from tall;";
        assertPlanContains(sql, "months_add(8: th, 2)");

        sql = "select date_add('year', 2, th) from tall;";
        assertPlanContains(sql, "years_add(8: th, 2)");

        sql = "select date_add('quarter', 2, TIMESTAMP '2014-03-08 09:00:00');";
        assertPlanContains(sql, "quarters_add('2014-03-08 09:00:00', 2)");

        sql = "select date_add('quarter', -1, TIMESTAMP '2014-03-08 09:00:00');";
        assertPlanContains(sql, "quarters_add('2014-03-08 09:00:00', -1)");

        sql = "select date_add('millisecond', 20, TIMESTAMP '2014-03-08 09:00:00');";
        assertPlanContains(sql, "milliseconds_add('2014-03-08 09:00:00', 20)");

        sql = "select date_add('millisecond', -100, TIMESTAMP '2014-03-08 09:00:00');";
        assertPlanContains(sql, "milliseconds_add('2014-03-08 09:00:00', -100)");
    }

    @Test
    public void testStringFnTransform() throws Exception {
        String sql = "select chr(56)";
        assertPlanContains(sql, "char(56)");

        sql = "select codepoint('a')";
        assertPlanContains(sql, "ascii('a')");

        sql = "select position('aa' in 'bccaab');";
        assertPlanContains(sql, "locate('aa', 'bccaab')");

        sql = "select strpos('bccaab', 'aa');";
        assertPlanContains(sql, "locate('aa', 'bccaab')");

        sql = "select length('aaa');";
        assertPlanContains(sql, "char_length('aaa')");
    }

    @Test
    public void testJsonFnTransform() throws Exception {
        String sql = "select json_array_length('[1, 2, 3]')";
        assertPlanContains(sql, "json_length(CAST('[1, 2, 3]' AS JSON))");

        sql = "select json_parse('{\"a\": {\"b\": 1}}');";
        assertPlanContains(sql, "parse_json('{\"a\": {\"b\": 1}}')");

        sql = "select json_extract(json_parse('{\"a\": {\"b\": 1}}'), '$.a.b')";
        assertPlanContains(sql, "json_query(parse_json('{\"a\": {\"b\": 1}}'), '$.a.b')");

        sql = "select json_extract(JSON '{\"a\": {\"b\": 1}}', '$.a.b');";
        assertPlanContains(sql, "json_query(CAST('{\"a\": {\"b\": 1}}' AS JSON), '$.a.b')");

        sql = "select json_format(JSON '[1, 2, 3]')";
        assertPlanContains(sql, "'[1, 2, 3]'");

        sql = "select json_format(json_parse('{\"a\": {\"b\": 1}}'))";
        assertPlanContains(sql, "CAST(parse_json('{\"a\": {\"b\": 1}}') AS VARCHAR)");

        sql = "select json_size('{\"x\": {\"a\": 1, \"b\": 2}}', '$.x');";
        assertPlanContains(sql, "json_length(CAST('{\"x\": {\"a\": 1, \"b\": 2}}' AS JSON), '$.x')");

        sql = "select json_extract_scalar('[1, 2, 3]', '$[2]');";
        assertPlanContains(sql, "CAST(json_query(CAST('[1, 2, 3]' AS JSON), '$[2]') AS VARCHAR)");

        sql = "select json_extract_scalar(JSON '{\"a\": {\"b\": 1}}', '$.a.b');";
        assertPlanContains(sql, "CAST(json_query(CAST('{\"a\": {\"b\": 1}}' AS JSON), '$.a.b') AS VARCHAR)");

        sql = "select json_extract_scalar(json_parse('{\"a\": {\"b\": 1}}'), '$.a.b');";
        assertPlanContains(sql, "CAST(json_query(parse_json('{\"a\": {\"b\": 1}}'), '$.a.b') AS VARCHAR)");
    }

    @Test
    public void testBitFnTransform() throws Exception {
        String sql = "select bitwise_and(19,25)";
        assertPlanContains(sql, "17");

        sql = "select bitwise_not(19)";
        assertPlanContains(sql, "~ 19");

        sql = "select bitwise_or(19,25)";
        assertPlanContains(sql, "27");

        sql = "select bitwise_xor(19,25)";
        assertPlanContains(sql, "10");

        sql = "select bitwise_left_shift(1, 2)";
        assertPlanContains(sql, "1 BITSHIFTLEFT 2");

        sql = "select bitwise_right_shift(8, 3)";
        assertPlanContains(sql, "8 BITSHIFTRIGHT 3");
    }

    @Test
    public void testUnicodeFnTransform() throws Exception {
        String sql = "select to_utf8('123')";
        assertPlanContains(sql, "to_binary('123', 'utf8')");

        sql = "select from_utf8(to_utf8('123'))";
        assertPlanContains(sql, "from_binary(to_binary('123', 'utf8'), 'utf8')");
    }

    @Test
    public void testBinaryFunction() throws Exception {
        String sql = "select x'0012'";
        assertPlanContains(sql, "'0012'");

        sql = "select md5(x'0012');";
        assertPlanContains(sql, "md5(from_binary('0012', 'utf8'))");

        sql = "select md5(tk) from tall";
        assertPlanContains(sql, "md5(from_binary(11: tk, 'utf8'))");

        sql = "select to_hex(tk) from tall";
        assertPlanContains(sql, "hex(11: tk)");

        sql = "select sha256(x'aaaa');";
        assertPlanContains(sql, "sha2(from_binary('AAAA', 'utf8'), 256)");

        sql = "select sha256(tk) from tall";
        assertPlanContains(sql, "sha2(from_binary(11: tk, 'utf8'), 256)");
        System.out.println(getFragmentPlan(sql));
    }

}
