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
package com.starrocks.connector.parser.pinot;

import org.junit.BeforeClass;
import org.junit.Test;

public class PinotQueryTest extends PinotTestBase {
    @BeforeClass
    public static void beforeClass() throws Exception {
        PinotTestBase.beforeClass();
        starRocksAssert.getCtx().getSessionVariable().setCboPushDownAggregateMode(-1);
    }

    @Test
    public void testSelectClause() throws Exception {
        String sql = "select * from test.t0;";
        assertPlanContains(sql, "OUTPUT EXPRS:1: v1 | 2: v2 | 3: v3");

        sql = "select * from test.t0 limit 100;";
        assertPlanContains(sql, "limit: 100");

        sql = "select \"v1\", \"v2\" from test.t0";
        assertPlanContains(sql, "1:Project\n" +
                "  |  <slot 4> : 'v1'\n" +
                "  |  <slot 5> : 'v2'");
    }

    @Test
    public void testSelectAggregation() throws Exception {
        String sql = "select count(*) from test.t0";
        assertPlanContains(sql, "2:AGGREGATE (update finalize)\n" +
                "  |  output: count(*)");

        sql = "select max(v1) from test.t0";
        assertPlanContains(sql, "1:AGGREGATE (update finalize)\n" +
                "  |  output: max(1: v1)");

        sql = "select sum(v2) from test.t0";
        assertPlanContains(sql, "1:AGGREGATE (update finalize)\n" +
                "  |  output: sum(2: v2)");
    }

    @Test
    public void testSelectGroupingOnAggregation() throws Exception {
        String sql = "select min(v1), max(v2) from test.t0 group by v3";
        assertPlanContains(sql, "1:AGGREGATE (update finalize)\n" +
                "  |  output: min(1: v1), max(2: v2)\n" +
                "  |  group by: 3: v3");
    }

    @Test
    public void testSelectOrderingOnAggregation() throws Exception {
        String sql = "select min(v1), max(v2) from test.t0 group by v3 order by min(v1)";

        assertPlanContains(sql, "3:SORT\n" +
                "  |  order by: <slot 4> 4: min ASC\n" +
                "  |  offset: 0");
    }

    @Test
    public void testSelectFilter() throws Exception {
        String sql = "select * from test.t0 where v1 > 10";
        assertPlanContains(sql, "PREDICATES: 1: v1 > 10");

        sql = "select * from test.t0 where v1 > 10 and v2 < 20";
        assertPlanContains(sql, "PREDICATES: 1: v1 > 10, 2: v2 < 20");

        sql = "select * from test.t0 where v1 > 10 or v2 < 20";
        assertPlanContains(sql, "PREDICATES: (1: v1 > 10) OR (2: v2 < 20)");

        sql = "SELECT COUNT(*) \n" +
                "FROM test.tall\n" +
                "  WHERE ta = 'foo'\n" +
                "  AND td BETWEEN 1 AND 20\n" +
                "  OR (tg < 42 AND ta IN ('hello', 'goodbye') AND ta NOT IN (42, 69));";

        assertPlanContains(sql, "PREDICATES: ((1: ta = 'foo') AND ((4: td >= 1) AND (4: td <= 20))) OR (((7: tg < 42) " +
                "AND (1: ta IN ('hello', 'goodbye'))) AND (1: ta NOT IN ('42', '69'))), " +
                "1: ta IN ('foo', 'hello', 'goodbye')");
    }

    @Test
    public void testOrderOnSelection() throws Exception {
        String sql = "select * from test.t0 order by v1";
        assertPlanContains(sql, "1:SORT\n" +
                "  |  order by: <slot 1> 1: v1 ASC\n" +
                "  |  offset: 0");

        sql = "select * from test.t0 order by v1 desc";
        assertPlanContains(sql, "1:SORT\n" +
                "  |  order by: <slot 1> 1: v1 DESC\n" +
                "  |  offset: 0");

        sql = "select * from test.t0 order by v1 desc, v2 asc";
        assertPlanContains(sql, "1:SORT\n" +
                "  |  order by: <slot 1> 1: v1 DESC, <slot 2> 2: v2 ASC\n" +
                "  |  offset: 0");
    }

    @Test
    public void testPaginationOnSelection() throws Exception {
        String sql = "select v1, v2 from test.t0 where v2 > 10 order by v2 desc limit 10, 20";
        assertPlanContains(sql, "2:MERGING-EXCHANGE\n" +
                "     offset: 10\n" +
                "     limit: 20");
    }

    @Test
    public void testCaseWhen() throws Exception {
        String sql = "select case when v1 > 10 then 1 else 0 end from test.t0";
        assertPlanContains(sql, "1:Project\n" +
                "  |  <slot 4> : if(1: v1 > 10, 1, 0)");

        sql = "select sum(case when v1 > 10 then 1 when v1 < 10 then 0 else 2 end) as total_cost from test.t0";
        assertPlanContains(sql, "");
    }

    @Test
    public void testJoin() throws Exception {
        String sql = "select * from test.t0 join test.t1 on t0.v1 = t1.v4";
        assertPlanContains(sql, "4:HASH JOIN\n" +
                "  |  join op: INNER JOIN (PARTITIONED)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 1: v1 = 4: v4");

        sql = "select * from test.t0 join test.t1 on t0.v1 = t1.v4 join test.t2 on t0.v1 = t2.v7";
        assertPlanContains(sql, "7:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BUCKET_SHUFFLE(S))\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 1: v1 = 7: v7\n" +
                "  |  \n" +
                "  |----6:EXCHANGE\n" +
                "  |    \n" +
                "  4:HASH JOIN\n" +
                "  |  join op: INNER JOIN (PARTITIONED)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 1: v1 = 4: v4");

        sql = "select * from test.t0 left join test.t1 on t0.v1 = t1.v4";
        assertPlanContains(sql, " 4:HASH JOIN\n" +
                "  |  join op: LEFT OUTER JOIN (PARTITIONED)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 1: v1 = 4: v4");

        sql = "select * from test.t0 right join test.t1 on t0.v1 = t1.v4";
        assertPlanContains(sql, "4:HASH JOIN\n" +
                "  |  join op: RIGHT OUTER JOIN (PARTITIONED)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 1: v1 = 4: v4");

        sql = "select * from test.t0 full join test.t1 on t0.v1 = t1.v4";
        assertPlanContains(sql, "4:HASH JOIN\n" +
                "  |  join op: FULL OUTER JOIN (PARTITIONED)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 1: v1 = 4: v4");

        sql = "select * from test.t0 join test.t1 on t0.v1 = t1.v4 where t0.v1 > 10";
        assertPlanContains(sql, "PREDICATES: 1: v1 > 10");
    }

    @Test
    public void testDateTruncFunction() throws Exception {
        String sql = "select dateTrunc('week', th) AS ts\n" +
                "FROM test.tall\n";

        assertPlanContains(sql, "1:Project\n" +
                "  |  <slot 12> : unix_timestamp(date_trunc('week', 8: th)) * 1000");

        sql = "select dateTrunc('week', th, 'MILLISECONDS') AS ts\n" +
                "FROM test.tall";

        assertPlanContains(sql, "1:Project\n" +
                "  |  <slot 12> : unix_timestamp(date_trunc('week', 8: th)) * 1000");

        sql = "select dateTrunc(\n" +
                "  'week', \n" +
                "  th, \n" +
                "  'MILLISECONDS', \n" +
                "  'UTC', \n" +
                "  'SECONDS'\n" +
                ") AS ts\n" +
                "FROM test.tall";
        assertPlanContains(sql, "1:Project\n" +
                "  |  <slot 12> : floor(CAST(CAST(unix_timestamp(convert_tz(date_trunc('week', 8: th), 'UTC', 'UTC')) " +
                "AS DECIMAL128(18,0)) * 1.0 AS DOUBLE))");

        sql = "select dateTrunc(\n" +
                "  'week', \n" +
                "  th, \n" +
                "  'MILLISECONDS', \n" +
                "  'CET', \n" +
                "  'SECONDS'\n" +
                ") AS ts\n" +
                "FROM test.tall";
        assertPlanContains(sql, "1:Project\n" +
                "  |  <slot 12> : floor(CAST(CAST(unix_timestamp(convert_tz(date_trunc('week', 8: th), " +
                "'CET', 'UTC')) AS DECIMAL128(18,0)) * 1.0 AS DOUBLE))");

        sql = "select dateTrunc(\n" +
                "  'quarter', \n" +
                "  th, \n" +
                "  'MILLISECONDS', \n" +
                "  'America/Los_Angeles', \n" +
                "  'HOURS'\n" +
                ") AS ts\n" +
                "FROM test.tall";
        assertPlanContains(sql, "1:Project\n" +
                "  |  <slot 12> : floor(CAST(CAST(unix_timestamp(convert_tz(date_trunc('quarter', 8: th), " +
                "'America/Los_Angeles', 'UTC')) AS DECIMAL128(18,0)) * 0.0002777777777777778 AS DOUBLE))");
    }

    @Test
    public void testDateTimeConvertFunction() throws Exception {
        String sql = "select DATETIMECONVERT(\n" +
                "         th, \n" +
                "         '1:MILLISECONDS:EPOCH', \n" +
                "         '1:DAYS:EPOCH', \n" +
                "         '1:DAYS'\n" +
                "       ) AS convertedTime\n" +
                "from test.tall";
        assertPlanContains(sql, "1:Project\n" +
                "  |  <slot 12> : floor(CAST(CAST(unix_timestamp(time_slice(8: th, 1, 'day', 'floor')) " +
                "AS DECIMAL128(18,0)) * 0.000011574074074074073 AS DOUBLE))");

        sql = "select DATETIMECONVERT(\n" +
                "         th, \n" +
                "         '1:MILLISECONDS:EPOCH', \n" +
                "         '1:MILLISECONDS:EPOCH', \n" +
                "         '1:DAYS'\n" +
                "       ) AS convertedTime\n" +
                "from test.tall";
        assertPlanContains(sql, "1:Project\n" +
                "  |  <slot 12> : floor(CAST(CAST(unix_timestamp(time_slice(8: th, 1, 'day', 'floor')) " +
                "AS DECIMAL128(18,0)) * 1000.0 AS DOUBLE))");

        sql = "select DATETIMECONVERT(\n" +
                "         th, \n" +
                "         '1:MILLISECONDS:EPOCH', \n" +
                "         '1:MILLISECONDS:EPOCH', \n" +
                "         '1:DAYS',\n" +
                "         'Europe/Berlin'\n" +
                "       ) AS convertedTime\n" +
                "from test.tall";
        assertPlanContains(sql, "1:Project\n" +
                "  |  <slot 12> : floor(CAST(CAST(unix_timestamp(convert_tz(time_slice(8: th, 1, 'day', 'floor'), " +
                "'Europe/Berlin', 'UTC')) AS DECIMAL128(18,0)) * 1000.0 AS DOUBLE))");

        sql = "select DATETIMECONVERT(\n" +
                "         th, \n" +
                "         '1:MILLISECONDS:EPOCH', \n" +
                "         '1:MILLISECONDS:EPOCH', \n" +
                "         '15:MINUTES'\n" +
                "       ) AS convertedTime\n" +
                "from test.tall";
        assertPlanContains(sql, "1:Project\n" +
                "  |  <slot 12> : floor(CAST(CAST(unix_timestamp(time_slice(8: th, 15, 'minute', 'floor')) " +
                "AS DECIMAL128(18,0)) * 1000.0 AS DOUBLE))");


        sql = "select DATETIMECONVERT(\n" +
                "         th, \n" +
                "         '1:MILLISECONDS:EPOCH', \n" +
                "         '1:DAYS:SIMPLE_DATE_FORMAT:yyyy-MM-dd', \n" +
                "         '1:DAYS'\n" +
                "       ) AS convertedTime\n" +
                "from test.tall";
        assertPlanContains(sql, "1:Project\n" +
                "  |  <slot 12> : date_format(time_slice(convert_tz(8: th, 'UTC', 'UTC'), 1, 'day', 'floor'), '%Y-%m-%d')");

        sql = "select DATETIMECONVERT(\n" +
                "         th, \n" +
                "         '1:MILLISECONDS:EPOCH', \n" +
                "         '1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm tz(Pacific/Kiritimati)', \n" +
                "         '1:MILLISECONDS'\n" +
                "       ) AS convertedTime\n" +
                "from test.tall";
        assertPlanContains(sql, "1:Project\n" +
                "  |  <slot 12> : date_format(time_slice(convert_tz(8: th, 'UTC', 'Pacific/Kiritimati'), 1, " +
                "'millisecond', 'floor'), '%Y-%m-%d %H:%i')");


        sql = "select DATETIMECONVERT(\n" +
                "         th, \n" +
                "         '1:MILLISECONDS:EPOCH', \n" +
                "         '1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm tz(Pacific/Kiritimati)', \n" +
                "         '1:DAYS'\n" +
                "       ) AS convertedTime\n" +
                "from test.tall";
        assertPlanContains(sql, "1:Project\n" +
                "  |  <slot 12> : date_format(time_slice(convert_tz(8: th, 'UTC', 'Pacific/Kiritimati'), 1, " +
                "'day', 'floor'), '%Y-%m-%d %H:%i')");

        sql = "select DATETIMECONVERT(\n" +
                "         th, \n" +
                "         '1:MILLISECONDS:EPOCH', \n" +
                "         '1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm', \n" +
                "         '1:DAYS',\n" +
                "         'Europe/Berlin'\n" +
                "       ) AS convertedTime\n" +
                "from test.tall";
        assertPlanContains(sql, "1:Project\n" +
                "  |  <slot 12> : date_format(convert_tz(time_slice(convert_tz(8: th, 'UTC', 'UTC'), 1, " +
                "'day', 'floor'), 'Europe/Berlin', 'UTC'), '%Y-%m-%d %H:%i')");
    }

    @Test
    public void testToDateTimeFunction() throws Exception {
        String sql = "SELECT ToDateTime(th, 'yyyy-MM-dd') AS dateTimeString from test.tall";
        assertPlanContains(sql, "1:Project\n" +
                "  |  <slot 12> : date_format(8: th, '%Y-%m-%d')");

        sql = "SELECT ToDateTime(\n" +
                "    th, \n" +
                "    'yyyy-MM-dd hh:mm:ss a'\n" +
                "    ) AS dateTimeString from test.tall";
        assertPlanContains(sql, "1:Project\n" +
                "  |  <slot 12> : date_format(8: th, '%Y-%m-%d %I:%i:%S %p')");

        sql = "SELECT ToDateTime(\n" +
                "    th, \n" +
                "    'yyyy-MM-dd HH:mm:ss Z',\n" +
                "    'CET'\n" +
                "    ) AS dateTimeString from test.tall";
        assertPlanContains(sql, "1:Project\n" +
                "  |  <slot 12> : convert_tz(CAST(date_format(8: th, '%Y-%m-%d %H:%i:%S ') " +
                "AS DATETIME), 'Asia/Shanghai', 'CET')");
    }

    @Test
    public void testFromDateTimeFunction() throws Exception {
        String sql = "select FromDateTime(th, 'yyyy-MM-dd') AS epochMillis from test.tall";
        assertPlanContains(sql, "1:Project\n" +
                "  |  <slot 12> : unix_timestamp(str_to_date(CAST(8: th AS VARCHAR), '%Y-%m-%d')) * 1000");

        sql = "select FromDateTime(\n" +
                "    th, \n" +
                "    'yyyy-MM-dd hh:mm:ss a'\n" +
                "    ) AS epochMillis from test.tall";
        assertPlanContains(sql, "1:Project\n" +
                "  |  <slot 12> : unix_timestamp(str_to_date(CAST(8: th AS VARCHAR), '%Y-%m-%d %I:%i:%S %p')) * 1000");

        sql = "select FromDateTime(\n" +
                "    th, \n" +
                "    'yyyy-MM-dd''T''HH:mm:ss'\n" +
                "    ) AS epochMillis from test.tall";
        assertPlanContains(sql, "1:Project\n" +
                "  |  <slot 12> : unix_timestamp(str_to_date(CAST(8: th AS VARCHAR), '%Y-%m-%dT%H:%i:%S')) * 1000");
    }

    @Test
    public void testAggregationFunction() throws Exception {

        String sql = "select DISTINCTCOUNTHLL(ta) AS value from test.tall";
        assertPlanContains(sql, "1:AGGREGATE (update finalize)\n" +
                "  |  output: approx_count_distinct(1: ta)");

        sql = "select PERCENTILETDIGEST(tg, 50, 1000) AS value from test.tall";
        assertPlanContains(sql, "1:AGGREGATE (update finalize)\n" +
                "  |  output: percentile_approx(CAST(7: tg AS DOUBLE), 0.5, 1000.0)");

        sql = "select PERCENTILETDIGEST(tg, 99.9) AS value from test.tall";
        assertPlanContains(sql, "1:AGGREGATE (update finalize)\n" +
                "  |  output: percentile_approx(CAST(7: tg AS DOUBLE), 0.999)");
    }

    @Test
    public void testTextMatchFunction() throws Exception {
        String sql = "select text_match(ta, '\"distributed system\"') AS value from test.tall";
        assertPlanContains(sql, " 1:Project\n" +
                "  |  <slot 12> : regexp(1: ta, '\\\\bdistributed system\\\\b')");

        sql = "select text_match(ta, 'Java') AS value from test.tall";
        assertPlanContains(sql, "1:Project\n" +
                "  |  <slot 12> : regexp(1: ta, '\\\\bJava\\\\b')");

        sql = "select text_match(ta, 'Java AND C++') AS value from test.tall";
        assertPlanContains(sql, "1:Project\n" +
                "  |  <slot 12> : (regexp(1: ta, '\\\\bJava\\\\b')) AND (regexp(1: ta, '\\\\bC++\\\\b'))");

        sql = "select text_match(ta, 'stream*') AS value from test.tall";
        assertPlanContains(sql, "1:Project\n" +
                "  |  <slot 12> : regexp(1: ta, '^stream.*')");

        sql = "select text_match(ta, '/Exception.*/') AS value from test.tall";
        assertPlanContains(sql, "1:Project\n" +
                "  |  <slot 12> : regexp(1: ta, 'Exception.*')");

        sql = "select text_match(ta, 'Java C++') AS value from test.tall";
        assertPlanContains(sql, "1:Project\n" +
                "  |  <slot 12> : (regexp(1: ta, '\\\\bJava\\\\b')) OR (regexp(1: ta, '\\\\bC++\\\\b'))");

        sql = "select text_match(ta, '\"Machine learning\" AND gpu AND python') AS value from test.tall";
        assertPlanContains(sql, "1:Project\n" +
                "  |  <slot 12> : ((regexp(1: ta, '\\\\bMachine learning\\\\b')) AND " +
                "(regexp(1: ta, '\\\\bgpu\\\\b'))) AND (regexp(1: ta, '\\\\bpython\\\\b'))");
    }
}