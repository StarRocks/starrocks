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
        assertPlanContains(sql, "array_concat(2: c1, CAST(3: c2 AS ARRAY<VARCHAR>))");

        sql = "select concat(c1, c2, array[1,2], array[3,4]) from test_array";
        assertPlanContains(sql, "array_concat(2: c1, CAST(3: c2 AS ARRAY<VARCHAR>), CAST([1,2] AS ARRAY<VARCHAR>), " +
                "CAST([3,4] AS ARRAY<VARCHAR>)");

        sql = "select concat(c2, 2) from test_array";
        assertPlanContains(sql, "array_concat(3: c2, CAST([2] AS ARRAY<INT>))");

        sql = "select contains(array[1,2,3], 1)";
        assertPlanContains(sql, "array_contains([1,2,3], 1)");

        sql = "select slice(array[1,2,3,4], 2, 2)";
        assertPlanContains(sql, "array_slice([1,2,3,4], 2, 2)");
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
        assertPlanContains(sql, "dayofweek('2022-03-06 01:02:03')");

        sql = "select dow(timestamp '2022-03-06 01:02:03');";
        assertPlanContains(sql, "dayofweek('2022-03-06 01:02:03')");

        sql = "select dow(date '2022-03-06');";
        assertPlanContains(sql, "dayofweek('2022-03-06 00:00:00')");

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
    }
}
