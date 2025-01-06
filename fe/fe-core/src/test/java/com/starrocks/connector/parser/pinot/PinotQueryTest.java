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

        assertPlanContains(sql, "PREDICATES: ((1: ta = 'foo') AND ((4: td >= 1) AND (4: td <= 20))) OR (((7: tg < 42) AND (1: ta IN ('hello', 'goodbye'))) AND (1: ta NOT IN ('42', '69'))), 1: ta IN ('foo', 'hello', 'goodbye')");
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
        String sql = "select CAST(DATETRUNC('HOUR', th, 'MILLISECONDS') AS LARGEINT) AS \"time\" from test.tall";
        assertPlanContains(sql, "1:Project\n" +
                "  |  <slot 12> : date_trunc('hour', 8: th)");

        sql = "select DATETRUNC('DAY', tg) from test.tall";

        assertPlanContains(sql, "1:Project\n" +
                "  |  <slot 12> : date_trunc('day', CAST(from_unixtime(CAST(CAST(7: tg AS DOUBLE) / 1000.0 AS BIGINT)) AS DATETIME))");

    }

    @Test
    public void testDateTimeConvertFunction() throws Exception {
        String sql = "select DATETIMECONVERT(th, '1:MILLISECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '10:MINUTES') from test.tall";
        assertPlanContains(sql, "1:Project\n" +
                "  |  <slot 12> : minutes_sub(date_trunc('minute', 8: th), CAST(minute(8: th) % 10 AS INT))");

        sql = "select DATETIMECONVERT(tg, '1:MILLISECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '10:MINUTES') from test.tall";
        assertPlanContains(sql, "1:Project\n" +
                "  |  <slot 12> : minutes_sub(date_trunc('minute', CAST(7: tg AS DATETIME)), CAST(minute(CAST(from_unixtime(CAST(CAST(7: tg AS DOUBLE) / 1000.0 AS BIGINT)) AS DATETIME)) % 10 AS INT))");
    }

    @Test
    public void testToDateTimeFunction() throws Exception {
        String sql = "select ToDateTime(th, 'yyyy-MM-dd') from test.tall";
        assertPlanContains(sql, "1:Project\n" +
                "  |  <slot 12> : date_format(8: th, '%Y-%m-%d')");

        sql = "select ToDateTime(tg, 'yyyy-MM-dd') from test.tall";
        assertPlanContains(sql, "1:Project\n" +
                "  |  <slot 12> : from_unixtime(CAST(CAST(7: tg AS DOUBLE) / 1000.0 AS BIGINT), 'yyyy-MM-dd')");
    }

    @Test
    public void testFromDateTimeFunction() throws Exception {
        String sql = "select FromDateTime(tg, 'yyyy-MM-dd''T''HH:mm:ss.SSSZ') from test.tall";
        assertPlanContains(sql, "1:Project\n" +
                "  |  <slot 12> : str_to_date(CAST(7: tg AS VARCHAR), '%Y-%m-%d\\'T\\'%H:%i:%s.%f')");
    }

    //actual case in the working environment
    @Test
    public void testDateTruncComplexFunction() throws Exception {
        String sql = "SELECT\n" +
                "    dateTrunc(\n" +
                "        'hour',\n" +
                "        td,\n" +
                "        'MILLISECONDS',\n" +
                "        'UTC',\n" +
                "        'MILLISECONDS'\n" +
                "    ) AS \"time\"\n" +
                "FROM\n" +
                "    test.tall";

        assertPlanContains(sql, "1:Project\n" +
                "  |  <slot 12> : date_trunc('hour', CAST(from_unixtime(CAST(CAST(4: td AS DOUBLE) / 1000.0 AS BIGINT)) AS DATETIME))");

        sql = "SELECT\n" +
                "    DATETRUNC('DAY', th, 'MILLISECONDS') AS \"date\"\n" +
                "FROM\n" +
                "    test.tall\n";
        assertPlanContains(sql, "1:Project\n" +
                "  |  <slot 12> : date_trunc('day', 8: th)");

        sql = "select CAST(\n" +
                "        DATETRUNC(\n" +
                "            'week',\n" +
                "            th,\n" +
                "            'MILLISECONDS',\n" +
                "            'UTC',\n" +
                "            'MILLISECONDS'\n" +
                "        ) AS LARGEINT\n" +
                "    ) AS \"time\" FROM\n" +
                "    test.tall\n" +
                "WHERE\n" +
                "    \"th\" >= 1719541399910\n" +
                "    AND \"th\" <= 1735093399910";

        assertPlanContains(sql, "1:Project\n" +
                "  |  <slot 12> : CAST(date_trunc('week', 8: th) AS LARGEINT)\n" +
                "  |  \n" +
                "  0:OlapScanNode\n" +
                "     TABLE: tall\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: CAST('th' AS DOUBLE) >= 1.71954139991E12, CAST('th' AS DOUBLE) <= 1.73509339991E12");
    }

    @Test
    public void testToDateTimeComplexFunction() throws Exception {
        String sql = "select\n" +
                "    Todatetime(datetrunc('DAY', tg), 'yyyy-MM-dd') as _timestamp from\n" +
                "    test.tall\n" +
                "where\n" +
                "        tg >= Fromdatetime(\n" +
                "        concat('2024-12-19', '00:00:00', ' '),\n" +
                "        'yyyy-MM-dd HH:mm:ss'\n" +
                "    )\n" +
                "    AND tg <= Fromdatetime(\n" +
                "        concat('2024-12-25', '23:59:59', ' '),\n" +
                "        'yyyy-MM-dd HH:mm:ss'\n" +
                "    )\n" +
                "    AND Fromdatetime('2024-12-19', 'yyyy-MM-dd') <= tg";

        assertPlanContains(sql, "1:Project\n" +
                "  |  <slot 12> : from_unixtime(CAST(CAST(date_trunc('day', CAST(from_unixtime(CAST(CAST(7: tg AS DOUBLE) / 1000.0 AS BIGINT)) AS DATETIME)) AS DOUBLE) / 1000.0 AS BIGINT), 'yyyy-MM-dd')\n" +
                "  |  \n" +
                "  0:OlapScanNode\n" +
                "     TABLE: tall\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: CAST(7: tg AS DOUBLE) >= CAST(str_to_date('2024-12-1900:00:00 ', '%Y-%m-%d %H:%i:%s') AS DOUBLE), CAST(7: tg AS DOUBLE) <= CAST(str_to_date('2024-12-2523:59:59 ', '%Y-%m-%d %H:%i:%s') AS DOUBLE), 7: tg >= 20241219");


        sql = "SELECT\n" +
                "    Todatetime(\n" +
                "        dateTrunc(\n" +
                "            'day',\n" +
                "            FromDateTime(\n" +
                "                ToDateTime(\n" +
                "                    FromDateTime(th, 'yyyy-MM-dd HH:mm:ss'),\n" +
                "                    'yyyy-MM-dd',\n" +
                "                    '+08:00'\n" +
                "                ),\n" +
                "                'yyyy-MM-dd'\n" +
                "            ),\n" +
                "            'MILLISECONDS'\n" +
                "        ),\n" +
                "        'YYYY-MM-dd'\n" +
                "    ) AS __timestamp from\n" +
                "    test.tall\n" +
                "where\n" +
                "     th >= SUBSTR('2024-12-17 16:04:12.151', 0, 19)\n" +
                "    AND th <= '2024-12-24 16:04:12.151'\n" +
                "    AND Fromdatetime(\n" +
                "        '2024-12-17 16:04:12.151',\n" +
                "        'yyyy-MM-dd HH:mm:ss.SSS'\n" +
                "    ) <= tg";

        assertPlanContains(sql, "1:Project\n" +
                "  |  <slot 12> : from_unixtime(CAST(CAST(date_trunc('day', CAST(from_unixtime(CAST(CAST(str_to_date(from_unixtime(CAST(CAST(str_to_date(CAST(8: th AS VARCHAR), '%Y-%m-%d %H:%i:%s') AS DOUBLE) / 1000.0 AS BIGINT), 'yyyy-MM-dd'), '%Y-%m-%d') AS DOUBLE) / 1000.0 AS BIGINT)) AS DATETIME)) AS DOUBLE) / 1000.0 AS BIGINT), 'YYYY-MM-dd')\n" +
                "  |  \n" +
                "  0:OlapScanNode\n" +
                "     TABLE: tall\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: 8: th >= CAST('' AS DATETIME), 8: th <= '2024-12-24 16:04:12.151000', CAST(7: tg AS DOUBLE) >= CAST('2024-12-17 16:04:12.151000' AS DOUBLE)");
    }


}
