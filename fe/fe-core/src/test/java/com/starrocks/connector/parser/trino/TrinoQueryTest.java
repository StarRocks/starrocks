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

public class TrinoQueryTest extends TrinoTestBase {
    @BeforeClass
    public static void beforeClass() throws Exception {
        TrinoTestBase.beforeClass();
    }

    @Test
    public void testSelectStarClause() throws Exception {
        String sql = "select * from t0;";
        assertPlanContains(sql, "OUTPUT EXPRS:1: v1 | 2: v2 | 3: v3");

        sql = "select t0.* from t0";
        assertPlanContains(sql, "OUTPUT EXPRS:1: v1 | 2: v2 | 3: v3");

        sql = "select test.t0.* from test.t0";
        assertPlanContains(sql, "OUTPUT EXPRS:1: v1 | 2: v2 | 3: v3");

        sql = "select test.t0.* from default_catalog.test.t0";
        assertPlanContains(sql, "OUTPUT EXPRS:1: v1 | 2: v2 | 3: v3");

        sql = "select default_catalog.test.t0.* from default_catalog.test.t0";
        assertPlanContains(sql, "OUTPUT EXPRS:1: v1 | 2: v2 | 3: v3");
    }

    @Test
    public void testSelectPrefixExpression() throws Exception {
        String sql = "select v1 from t0";
        assertPlanContains(sql, "OUTPUT EXPRS:1: v1");

        sql = "select t0.v1 from t0";
        assertPlanContains(sql, "OUTPUT EXPRS:1: v1");

        sql = "select test.t0.v1 from test.t0";
        assertPlanContains(sql, "OUTPUT EXPRS:1: v1");

        sql = "select default_catalog.test.t0.v1 from test.t0";
        assertPlanContains(sql, "OUTPUT EXPRS:1: v1");
    }

    @Test
    public void testSelectArithmeticExpression() throws Exception {
        String sql = "select v1 + 1 from t0";
        assertPlanContains(sql, "<slot 4> : 1: v1 + 1");

        sql = "select v1 + 's' from t0";
        assertPlanContains(sql, "<slot 4> : CAST(1: v1 AS DOUBLE) + CAST('s' AS DOUBLE)");

        sql = "select v1 + 1.234e5 from t0";
        assertPlanContains(sql, " <slot 4> : CAST(1: v1 AS DECIMAL128(20,0)) + 123400.0");

        sql = "select v1 + 1.234 from t0";
        assertPlanContains(sql, "<slot 4> : CAST(1: v1 AS DECIMAL128(22,0)) + 1.234");

        sql = "select v1 - v2 * 10 / 5 from t0";
        assertPlanContains(sql, "<slot 4> : CAST(1: v1 AS DOUBLE) - CAST(2: v2 * 10 AS DOUBLE) / 5.0");
    }

    @Test
    public void testCastExpression() throws Exception {
        String sql = "select cast(tb as varchar(10)) from tall";
        assertPlanContains(sql, "<slot 11> : CAST(2: tb AS VARCHAR(10))");

        sql = "select cast(tb as char(10)) from tall";
        assertPlanContains(sql, "<slot 11> : CAST(2: tb AS CHAR(10))");

        sql = "select cast(tb as int) from tall";
        assertPlanContains(sql, "<slot 11> : CAST(2: tb AS INT)");

        sql = "select cast(ti as datetime) from tall";
        assertPlanContains(sql, "<slot 11> : CAST(9: ti AS DATETIME)");

        sql = "select cast(th as date) from tall";
        assertPlanContains(sql, "<slot 11> : CAST(8: th AS DATE)");

        sql = "select cast(th as time) from tall";
        assertPlanContains(sql, "<slot 11> : CAST(8: th AS TIME)");

        sql = "select cast(ti as timestamp) from tall";
        assertPlanContains(sql, "<slot 11> : CAST(9: ti AS DATETIME)");
    }

    @Test
    public void testSelectLiteral() throws Exception {
        String sql = "select date '1998-12-01'";
        assertPlanContains(sql, "<slot 2> : '1998-12-01'");
        System.out.println(getFragmentPlan(sql));
    }

    @Test
    public void testSelectAnalytic() throws Exception {
        String sql = "select sum(v1) over(partition by v2) from t0";
        assertPlanContains(sql, "3:ANALYTIC\n" +
                "  |  functions: [, sum(1: v1), ]\n" +
                "  |  partition by: 2: v2");

        sql = "select sum(v1) over(partition by v2 order by v3) from t0";
        assertPlanContains(sql, "3:ANALYTIC\n" +
                "  |  functions: [, sum(1: v1), ]\n" +
                "  |  partition by: 2: v2\n" +
                "  |  order by: 3: v3 ASC\n" +
                "  |  window: RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW");

        sql = "select lead(v1,1,0) over(partition by v2 order by v3) from t0";
        assertPlanContains(sql, "3:ANALYTIC\n" +
                "  |  functions: [, lead(1: v1, 1, 0), ]\n" +
                "  |  partition by: 2: v2\n" +
                "  |  order by: 3: v3 ASC\n" +
                "  |  window: ROWS BETWEEN UNBOUNDED PRECEDING AND 1 FOLLOWING");

        sql = "select lag(v1) over(partition by v2 order by v3) from t0";
        assertPlanContains(sql, "3:ANALYTIC\n" +
                "  |  functions: [, lag(1: v1, 1, NULL), ]\n" +
                "  |  partition by: 2: v2\n" +
                "  |  order by: 3: v3 ASC\n" +
                "  |  window: ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING");

        sql = "select first_value(v1) over(partition by v2 order by v3 range between unbounded preceding and unbounded " +
                "following) from t0";
        assertPlanContains(sql, "3:ANALYTIC\n" +
                "  |  functions: [, first_value(1: v1), ]\n" +
                "  |  partition by: 2: v2\n" +
                "  |  order by: 3: v3 ASC\n" +
                "  |  window: ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW");

        sql = "select last_value(v1) over(partition by v2 order by v3 rows 6 preceding) from t0";
        assertPlanContains(sql, "3:ANALYTIC\n" +
                "  |  functions: [, last_value(1: v1), ]\n" +
                "  |  partition by: 2: v2\n" +
                "  |  order by: 3: v3 ASC\n" +
                "  |  window: ROWS BETWEEN 6 PRECEDING AND CURRENT ROW");

        sql = "select row_number() over(partition by v2 order by v3) from t0";
        assertPlanContains(sql, "3:ANALYTIC\n" +
                "  |  functions: [, row_number(), ]\n" +
                "  |  partition by: 2: v2\n" +
                "  |  order by: 3: v3 ASC\n" +
                "  |  window: ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW");

        sql = "select rank() over(partition by v2 order by v3) from t0";
        assertPlanContains(sql, "3:ANALYTIC\n" +
                "  |  functions: [, rank(), ]\n" +
                "  |  partition by: 2: v2\n" +
                "  |  order by: 3: v3 ASC\n" +
                "  |  window: RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW");

        sql = "select dense_rank() over(partition by v2 order by v3) from t0";
        assertPlanContains(sql, " 3:ANALYTIC\n" +
                "  |  functions: [, dense_rank(), ]\n" +
                "  |  partition by: 2: v2\n" +
                "  |  order by: 3: v3 ASC\n" +
                "  |  window: RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW");

        sql = "select sum(v1) over(partition by v2 order by v3 range between unbounded preceding and unbounded following) " +
                "from t0";
        assertPlanContains(sql, "window: RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING");

        sql = "select count(v1) over(partition by v2 order by v3 rows between unbounded preceding and unbounded following) " +
                "from t0";
        assertPlanContains(sql, "window: ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING");

        sql = "select min(v1) over(partition by v2 order by v3 rows current row) from t0";
        assertPlanContains(sql, "window: ROWS BETWEEN CURRENT ROW AND CURRENT ROW");

        sql = "select min(v1) over(partition by v2 order by v3 range UNBOUNDED PRECEDING) from t0";
        assertPlanContains(sql, "window: RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW");

        sql = "select min(v1) over(partition by v2 order by v3 rows 10 PRECEDING) from t0";
        assertPlanContains(sql, "window: ROWS BETWEEN 10 PRECEDING AND CURRENT ROW");
    }

    @Test
    public void testSelectGroupBy() throws Exception {
        String sql = "select v1, count(v2) from t0 group by v1";
        assertPlanContains(sql, "output: count(2: v2)\n" +
                "  |  group by: 1: v1");

        sql = "select v1 % 5 as k1, count(v2) from t0 group by k1";
        assertPlanContains(sql, " 2:AGGREGATE (update finalize)\n" +
                "  |  output: count(2: v2)\n" +
                "  |  group by: 4: expr\n" +
                "  |  \n" +
                "  1:Project\n" +
                "  |  <slot 2> : 2: v2\n" +
                "  |  <slot 4> : 1: v1 % 5");

        sql = "select v1, v2, sum(v3) from t0 group by v1,v2";
        assertPlanContains(sql, "output: sum(3: v3)\n" +
                "  |  group by: 1: v1, 2: v2");

        sql = "select v1+1, sum(v2) from t0 group by v1+1";
        assertPlanContains(sql, "group by: 4: expr");

        sql = "select v1+1, sum(v2) from t0 group by v1";
        assertPlanContains(sql, "group by: 1: v1");

        sql = "select v1+1, v1, sum(v2) from t0 group by v1";
        assertPlanContains(sql, "2:Project\n" +
                "  |  <slot 1> : 1: v1\n" +
                "  |  <slot 4> : 4: sum\n" +
                "  |  <slot 5> : 1: v1 + 1");

        sql = "select v1, v2, sum(v3) from t0 group by rollup(v1,v2)";
        assertPlanContains(sql, "group by: 1: v1, 2: v2, 5: GROUPING_ID");

        sql = "select v1, v2, sum(v3) from t0 group by CUBE(v1,v2)";
        assertPlanContains(sql, "group by: 1: v1, 2: v2, 5: GROUPING_ID");

        sql = "select v1, v2, sum(v3) from t0 group by GROUPING SETS ((v1, v2),(v1),(v2),())";
        assertPlanContains(sql, "group by: 1: v1, 2: v2, 5: GROUPING_ID");
    }

    @Test
    public void testBinaryPredicate() throws Exception {
        String sql = "select v1 from t0 where v2 = 1";
        assertPlanContains(sql, "2: v2 = 1");

        sql = "select v1 from t0 where v2 = v1";
        assertPlanContains(sql, "2: v2 = 1: v1");

        sql = "select v1 from t0 where v1 = 2 and v2 =1";
        assertPlanContains(sql, "1: v1 = 2, 2: v2 = 1");
    }

    @Test
    public void testLikePredicate() throws Exception {
        String sql = "select * from part where  p_type like \'%COPPER\'";
        assertPlanContains(sql, "P_TYPE LIKE '%COPPER'");

        sql = "select * from part where  p_type not like \'%COPPER\'";
        assertPlanContains(sql, "NOT (5: P_TYPE LIKE '%COPPER')");
    }

    @Test
    public void testBetweenPredicate() throws Exception {
        String sql = "select l_extendedprice*l_discount as revenue from lineitem where l_discount between 0.02 and 0.04";
        assertPlanContains(sql, "7: L_DISCOUNT >= 0.02, 7: L_DISCOUNT <= 0.04");

        sql = "select l_extendedprice*l_discount as revenue from lineitem where l_discount not between 0.02 and 0.04";
        assertPlanContains(sql, "(7: L_DISCOUNT < 0.02) OR (7: L_DISCOUNT > 0.04)");
    }

    @Test
    public void testAggFunction() throws Exception {
        String sql = "select count(v1) from t0";
        assertPlanContains(sql, "output: count(1: v1)");

        sql = "select count(*) from t0";
        assertPlanContains(sql, "output: count(*)");

        sql = "select count(1) from t0";
        assertPlanContains(sql, "output: count(1)");

        sql = "select count(distinct v1) from t0";
        assertPlanContains(sql, "output: multi_distinct_count(1: v1)");
    }

    @Test
    public void testCaseWhen() throws Exception {
        String sql = "select v1, sum(case when v2 =1 then 1 else 0 end) from t0 group by v1";
        assertPlanContains(sql, "<slot 4> : if(2: v2 = 1, 1, 0)");

        sql = "select v1, sum(case v2 when 1 then 1 else 0 end) from t0 group by v1";
        assertPlanContains(sql, "<slot 4> : if(2: v2 = 1, 1, 0)");

        sql = "select sum(case when 1=1 then v2 else NULL end) from t0";
        assertPlanContains(sql, "output: sum(2: v2)");

        sql = "select sum(case v1 when false then v2 when v2 then 1 else NULL end) from t0";
        assertPlanContains(sql, "output: sum(CASE 1: v1 WHEN 0 THEN 2: v2 WHEN 2: v2 THEN 1 ELSE NULL END)");

        sql = "select count(case when v1 then 1 end) from t0";
        assertPlanContains(sql, "<slot 1> : 1: v1", "output: count(if(CAST(1: v1 AS BOOLEAN), 1, NULL))");
    }


    @Test
    public void testLimit() throws Exception {
        String sql = "select * from t0 limit 10";
        assertPlanContains(sql, "limit: 10");

        sql = "select v1 from t0 order by v1 limit 20";
        assertPlanContains(sql, "limit: 20");
    }

    @Test
    public void testHaving() throws Exception {
        String sql = "select sum(v1) from t0 having sum(v1) > 0";
        assertPlanContains(sql, "having: 4: sum > 0");

        sql = "select v2,sum(v1) from t0 group by v2 having v2 > 0";
        assertPlanContains(sql, "PREDICATES: 2: v2 > 0");

        sql = "select sum(v1) from t0 having avg(v1) - avg(v2) > 10";
        assertPlanContains(sql, "5: avg - 6: avg > 10.0");
    }

    @Test
    public void testSort() throws Exception {
        String sql = "select v1 from t0 order by v1";
        assertPlanContains(sql, "order by: <slot 1> 1: v1 ASC");

        sql = "select v1 from t0 order by v1 asc ,v2 desc";
        assertPlanContains(sql, "order by: <slot 1> 1: v1 ASC, <slot 2> 2: v2 DESC");

        sql = "select v1 as v from t0 order by v+1";
        assertPlanContains(sql, "order by: <slot 4> 4: expr ASC");

        sql = "select v1+1 as v from t0 order by v";
        assertPlanContains(sql, "order by: <slot 4> 4: expr ASC");

        sql = "select v1, sum(v2) as v from t0 group by v1 order by sum(v2)";
        assertPlanContains(sql, "order by: <slot 4> 4: sum ASC");

        sql = "select v1+1 as v from t0 group by v1+1 order by v";
        assertPlanContains(sql, "order by: <slot 4> 4: expr ASC");
    }

    @Test
    public void testJoin() throws Exception {
        String sql = "select v1, v2 from t0,t1";
        assertPlanContains(sql, "join op: CROSS JOIN");

        sql = "select v1, v2 from t0 inner join t1 on t0.v1 = t1.v4";
        assertPlanContains(sql, "equal join conjunct: 1: v1 = 4: v4");

        sql = "select a.v1 from (select v1, v2, v5, v4 from t0 inner join t1 on t0.v1 = t1.v4) a";
        assertPlanContains(sql, "equal join conjunct: 1: v1 = 4: v4");

        sql = "select * from t0 a join t1 b on a.v1=b.v4";
        assertPlanContains(sql, "equal join conjunct: 1: v1 = 4: v4");

        sql = "select * from t0 a join (select * from t1) b on a.v1=b.v4";
        assertPlanContains(sql, "equal join conjunct: 1: v1 = 4: v4");

        sql = "select v1 from t0 left join t1 on v1 = v4";
        assertPlanContains(sql, "equal join conjunct: 1: v1 = 4: v4", "join op: LEFT OUTER JOIN (PARTITIONED)");

        sql = "select v1 from t0 right join t1 on v1 = v4";
        assertPlanContains(sql, "join op: RIGHT OUTER JOIN (PARTITIONED)", "equal join conjunct: 1: v1 = 4: v4");

        sql = "select v1 from t0 full join t1 on v1 = v4";
        assertPlanContains(sql, "join op: FULL OUTER JOIN (PARTITIONED)", "equal join conjunct: 1: v1 = 4: v4");

        sql = "SELECT * FROM t0,t1 INNER JOIN t2 on t1.v4 = t2.v7";
        assertPlanContains(sql, " join op: INNER JOIN (PARTITIONED)", "join op: CROSS JOIN");

        sql = "select * from t0 inner join t1 on t0.v1 = t1.v4 inner join t2 on t0.v2 = t2.v7";
        assertPlanContains(sql, "equal join conjunct: 2: v2 = 7: v7", "equal join conjunct: 1: v1 = 4: v4");

        sql = "select * from t0 a join t0 b using(v1)";
        assertPlanContains(sql, "equal join conjunct: 1: v1 = 4: v1");

        sql = "select * from t0 x,t0 y inner join t0 z using(v1)";
        assertPlanContains(sql, "join op: CROSS JOIN", "equal join conjunct: 4: v1 = 7: v1");

        sql = "select * from (t0 a join t0 b using(v1)) , t1";
        assertPlanContains(sql, "equal join conjunct: 1: v1 = 4: v1");
    }
}
