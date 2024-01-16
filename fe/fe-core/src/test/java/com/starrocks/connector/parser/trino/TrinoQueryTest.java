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

import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
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
    public void testDateExpression() throws Exception {
        String sql = "select current_date";
        analyzeSuccess(sql);

        sql = "select current_time";
        analyzeSuccess(sql);

        sql = "select current_timestamp";
        analyzeSuccess(sql);

        sql = "select localtimestamp";
        analyzeSuccess(sql);

        sql = "select localtime";
        analyzeSuccess(sql);

        sql = "select timestamp '2021-01-01 00:00:00'";
        assertPlanContains(sql, "'2021-01-01 00:00:00'");

        sql = "select timestamp '2021-01-01 10:01:02.123456'";
        assertPlanContains(sql, "'2021-01-01 10:01:02.123456'");

        sql = "select date '2021-01-01'";
        assertPlanContains(sql, "'2021-01-01'");

        sql = "select timestamp '2023-07-01'";
        assertPlanContains(sql, "'2023-07-01 00:00:00'");
    }

    @Test
    public void testCastExpression() throws Exception {
        String sql = "select cast(tb as varchar(10)) from tall";
        assertPlanContains(sql, "CAST(2: tb AS VARCHAR(10))");

        sql = "select cast(tb as char(10)) from tall";
        assertPlanContains(sql, "CAST(2: tb AS CHAR(10))");

        sql = "select cast(tb as int) from tall";
        assertPlanContains(sql, "CAST(2: tb AS INT)");

        sql = "select cast(ti as datetime) from tall";
        assertPlanContains(sql, "CAST(9: ti AS DATETIME)");

        sql = "select cast(th as date) from tall";
        assertPlanContains(sql, "CAST(8: th AS DATE)");

        sql = "select cast(th as time) from tall";
        assertPlanContains(sql, "CAST(8: th AS TIME)");

        sql = "select cast(ti as timestamp) from tall";
        assertPlanContains(sql, "CAST(9: ti AS DATETIME)");
    }

    @Test
    public void testNullifExpression() throws Exception {
        String sql = "select nullif(1, 2)";
        assertPlanContains(sql, "<slot 2> : nullif(1, 2)");

        sql = "select nullif(v1, v2) from t0";
        assertPlanContains(sql, "<slot 4> : nullif(1: v1, 2: v2)");
    }

    @Test
    public void testIfExpression() throws Exception {
        String sql = "select if(1, 2, 3)";
        assertPlanContains(sql, "<slot 2> : 2");

        sql = "select if(v1, v2, v3) from t0";
        assertPlanContains(sql, "<slot 4> : if(CAST(1: v1 AS BOOLEAN), 2: v2, 3: v3)");

        sql = "select if(v1, v2) from t0";
        assertPlanContains(sql, "<slot 4> : if(CAST(1: v1 AS BOOLEAN), 2: v2, NULL)");

        sql = "select * from t0 where if (v1, v2, v3) = 2";
        assertPlanContains(sql, "PREDICATES: if(CAST(1: v1 AS BOOLEAN), 2: v2, 3: v3) = 2");
    }

    @Test
    public void testDecimal() throws Exception {
        String sql = "select cast(tj as decimal32) from tall";
        analyzeFail(sql, "Unknown type: decimal32");

        sql = "select cast(tj as decimal64) from tall";
        analyzeFail(sql, "Unknown type: decimal64");

        sql = "select cast(tj as decimal128) from tall";
        analyzeFail(sql, "Unknown type: decimal128");

        sql = "select cast(tj as decimal) from tall";
        assertPlanContains(sql, "CAST(10: tj AS DECIMAL128(38,0))");

        sql = "select cast(tj as decimal(10, 2)) from tall";
        assertPlanContains(sql, "CAST(10: tj AS DECIMAL64(10,2))");

        sql = "select cast(tj as decimal(10)) from tall";
        assertPlanContains(sql, "CAST(10: tj AS DECIMAL64(10,0))");

        sql = "select cast(tj as decimal(28, 2)) from tall";
        assertPlanContains(sql, "CAST(10: tj AS DECIMAL128(28,2))");

        sql = "select cast(tj as decimal(28)) from tall";
        assertPlanContains(sql, "CAST(10: tj AS DECIMAL128(28,0))");
    }

    @Test
    public void testSelectLiteral() throws Exception {
        String sql = "select date '1998-12-01'";
        assertPlanContains(sql, "<slot 2> : '1998-12-01'");
    }

    @Test
    public void testSelectAnalytic() throws Exception {
        String sql = "select sum(v1) over(partition by v2) from t0";
        assertPlanContains(sql, "2:ANALYTIC\n" +
                "  |  functions: [, sum(1: v1), ]\n" +
                "  |  partition by: 2: v2");

        sql = "select sum(v1) over(partition by v2 order by v3) from t0";
        assertPlanContains(sql, "2:ANALYTIC\n" +
                "  |  functions: [, sum(1: v1), ]\n" +
                "  |  partition by: 2: v2\n" +
                "  |  order by: 3: v3 ASC\n" +
                "  |  window: RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW");

        sql = "select lead(v1,1,0) over(partition by v2 order by v3) from t0";
        assertPlanContains(sql, "2:ANALYTIC\n" +
                "  |  functions: [, lead(1: v1, 1, 0), ]\n" +
                "  |  partition by: 2: v2\n" +
                "  |  order by: 3: v3 ASC\n" +
                "  |  window: ROWS BETWEEN UNBOUNDED PRECEDING AND 1 FOLLOWING");

        sql = "select lag(v1) over(partition by v2 order by v3) from t0";
        assertPlanContains(sql, "2:ANALYTIC\n" +
                "  |  functions: [, lag(1: v1, 1, NULL), ]\n" +
                "  |  partition by: 2: v2\n" +
                "  |  order by: 3: v3 ASC\n" +
                "  |  window: ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING");

        sql =
                "select first_value(v1) over(partition by v2 order by v3 range between unbounded preceding and unbounded " +
                        "following) from t0";
        assertPlanContains(sql, "2:ANALYTIC\n" +
                "  |  functions: [, first_value(1: v1), ]\n" +
                "  |  partition by: 2: v2\n" +
                "  |  order by: 3: v3 ASC\n" +
                "  |  window: ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW");

        sql = "select last_value(v1) over(partition by v2 order by v3 rows 6 preceding) from t0";
        assertPlanContains(sql, "2:ANALYTIC\n" +
                "  |  functions: [, last_value(1: v1), ]\n" +
                "  |  partition by: 2: v2\n" +
                "  |  order by: 3: v3 ASC\n" +
                "  |  window: ROWS BETWEEN 6 PRECEDING AND CURRENT ROW");

        sql = "select row_number() over(partition by v2 order by v3) from t0";
        assertPlanContains(sql, "2:ANALYTIC\n" +
                "  |  functions: [, row_number(), ]\n" +
                "  |  partition by: 2: v2\n" +
                "  |  order by: 3: v3 ASC\n" +
                "  |  window: ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW");

        sql = "select rank() over(partition by v2 order by v3) from t0";
        assertPlanContains(sql, "2:ANALYTIC\n" +
                "  |  functions: [, rank(), ]\n" +
                "  |  partition by: 2: v2\n" +
                "  |  order by: 3: v3 ASC\n" +
                "  |  window: RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW");

        sql = "select dense_rank() over(partition by v2 order by v3) from t0";
        assertPlanContains(sql, " 2:ANALYTIC\n" +
                "  |  functions: [, dense_rank(), ]\n" +
                "  |  partition by: 2: v2\n" +
                "  |  order by: 3: v3 ASC\n" +
                "  |  window: RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW");

        sql = "select cume_dist() over(partition by v2 order by v3) from t0";
        assertPlanContains(sql, " 2:ANALYTIC\n" +
                "  |  functions: [, cume_dist(), ]\n" +
                "  |  partition by: 2: v2\n" +
                "  |  order by: 3: v3 ASC\n" +
                "  |  window: RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW");

        sql = "select percent_rank() over(partition by v2 order by v3) from t0";
        assertPlanContains(sql, " 2:ANALYTIC\n" +
                "  |  functions: [, percent_rank(), ]\n" +
                "  |  partition by: 2: v2\n" +
                "  |  order by: 3: v3 ASC\n" +
                "  |  window: RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW");

        sql =
                "select sum(v1) over(partition by v2 order by v3 range between unbounded preceding and unbounded following) " +
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
    public void testSelectArray() throws Exception {
        String sql = "select c1[1] from test_array";
        assertPlanContains(sql, "<slot 4> : 2: c1[1]");

        sql = "select c0, sum(c2[1]) from test_array group by c0";
        assertPlanContains(sql, "1:Project\n" +
                "  |  <slot 1> : 1: c0\n" +
                "  |  <slot 4> : 3: c2[1]");

        sql = "select distinct c2 from test_array order by c2[1];";
        assertPlanContains(sql, "2:Project\n" +
                "  |  <slot 3> : 3: c2\n" +
                "  |  <slot 4> : 3: c2[1]");
        sql = "select array[array[1,2],array[3,4]][1][2]";
        assertPlanContains(sql, "[[1,2],[3,4]][1][2]");

        sql = "select array[][1]";
        assertPlanContains(sql, "[][1]");

        sql = "select array[v1 = 1, v2 = 2, true] from t0";
        assertPlanContains(sql, "<slot 4> : [1: v1 = 1,2: v2 = 2,TRUE]");

        sql = "select array[v1,v2] from t0";
        assertPlanContains(sql, "[1: v1,2: v2]");

        sql = "select array[NULL][1] + 1, array[1,2,3][1] + array[array[1,2,3],array[1,1,1]][2][2];";
        assertPlanContains(sql, "1:Project\n" +
                "  |  <slot 2> : NULL\n" +
                "  |  <slot 3> : CAST([1,2,3][1] AS SMALLINT) + CAST(" +
                "[[1,2,3],[1,1,1]][2][2] AS SMALLINT)");

        sql = "select c0, c2[1] + array[1,2,3][1] as v, sum(c2[1]) from test_array group by c0, v order by v";
        assertPlanContains(sql, "1:Project\n" +
                "  |  <slot 1> : 1: c0\n" +
                "  |  <slot 4> : CAST(7: expr AS BIGINT) + CAST([1,2,3][1] AS BIGINT)\n" +
                "  |  <slot 5> : 7: expr\n" +
                "  |  common expressions:\n" +
                "  |  <slot 7> : 3: c2[1]");

        sql = "select element_at(array[1,2,3], 1)";
        assertPlanContains(sql, "[1,2,3][1]");

        sql = "select element_at(c1, 2) from test_array";
        assertPlanContains(sql, "2: c1[2]");

        sql = "select element_at(array[1,2,3], 1, 0)";
        try {
            getFragmentPlan(sql);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("element_at function must have 2 arguments"));
        }
    }

    @Test
    public void testSelectArrayFunction() throws Exception {
        String sql =  "select array_distinct(c1) from test_array";
        assertPlanContains(sql, "array_distinct(2: c1)");

        sql =  "select array_intersect(c1, array['star','rocks']) from test_array";
        assertPlanContains(sql, "array_intersect(2: c1, ['star','rocks'])");

        sql = "select array_join(c1, '_') from test_array";
        assertPlanContains(sql, "array_join(2: c1, '_')");

        sql = "select array_max(c1) from test_array";
        assertPlanContains(sql, "array_max(2: c1)");

        sql = "select array_min(c1) from test_array";
        assertPlanContains(sql, "array_min(2: c1)");

        sql = "select array_position(array[1,2,3], 2) from test_array";
        assertPlanContains(sql, "array_position([1,2,3], 2)");

        sql = "select array_remove(array[1,2,3], 2) from test_array";
        assertPlanContains(sql, "array_remove([1,2,3], 2)");

        sql = "select array_sort(c1) from test_array";
        assertPlanContains(sql, "array_sort(2: c1)");

        sql =  "select arrays_overlap(c1, array['star','rocks']) from test_array";
        assertPlanContains(sql, "arrays_overlap(2: c1, ['star','rocks'])");
    }

    @Test
    public void testSelectLambdaFunction() throws Exception {
        // trino do not support array_map function, just test the lambda function
        String sql = "select array_map(array['a1_a2','a1_a2'], x->split(x, '_'));";
        assertPlanContains(sql, "array_map(<slot 2> -> split(<slot 2>, '_'), ['a1_a2','a1_a2'])");

        sql = "select transform(split('a1_a2,b1_b2', ','), x->split(x, '_'));";
        assertPlanContains(sql, "array_map(<slot 2> -> split(<slot 2>, '_'), split('a1_a2,b1_b2', ','))");

        sql = "select all_match(array[1,2,3], x-> x>0);";
        assertPlanContains(sql, "all_match(array_map(<slot 2> -> <slot 2> > 0, [1,2,3]))");

        sql = "select any_match(array[1,2,null], x -> x is not null);";
        assertPlanContains(sql, "any_match(array_map(<slot 2> -> <slot 2> IS NOT NULL, [1,2,NULL]))");
    }

    @Test
    public void testSelectStruct() throws Exception {
        String sql = "select c0, c1.a from test_struct";
        assertPlanContains(sql, "1:Project\n" +
                "  |  <slot 1> : 1: c0\n" +
                "  |  <slot 4> : 2: c1.a[false]");

        sql = "select c0, test_struct.c1.a from test_struct";
        assertPlanContains(sql, "<slot 4> : 2: c1.a[false]");

        sql = "select c0, test.test_struct.c1.a from test_struct";
        assertPlanContains(sql, "<slot 4> : 2: c1.a[false]");

        sql = "select c0, default_catalog.test.test_struct.c1.a from test_struct";
        assertPlanContains(sql, "<slot 4> : 2: c1.a[false]");

        sql = "select c1.a[10].b from test_struct";
        assertPlanContains(sql, "1:Project\n" +
                "  |  <slot 4> : 2: c1.a[true][10].b[true]");

        sql = "select c2.a, c2.b from test_struct";
        assertPlanContains(sql, "  1:Project\n" +
                "  |  <slot 4> : 3: c2.a[false]\n" +
                "  |  <slot 5> : 3: c2.b[false]");

        sql = "select c2.a + c2.b from test_struct";
        assertPlanContains(sql, "1:Project\n" +
                "  |  <slot 4> : CAST(3: c2.a[true] AS DOUBLE) + 3: c2.b[true]");

        sql = "select sum(c2.b) from test_struct group by c2.a";
        assertPlanContains(sql, "1:Project\n" +
                "  |  <slot 4> : 3: c2.a[false]\n" +
                "  |  <slot 5> : 3: c2.b[false]");
    }

    public void testSelectRow() throws Exception {
        String sql = "select row(1,2)";
        assertPlanContains(sql, " <slot 2> : row(1, 2)");

        sql = "select row(1.1, 2.2, 3.3)";
        assertPlanContains(sql, "<slot 2> : row(1.1, 2.2, 3.3)");

        sql = "select row(1, 'xxx', 1.23)";
        assertPlanContains(sql, "<slot 2> : row(1, 'xxx', 1.23)");
    }

    @Test
    public void testSelectMap() throws Exception {
        String sql = "select c0, c1[1] from test_map";
        assertPlanContains(sql, "1:Project\n" +
                "  |  <slot 1> : 1: c0\n" +
                "  |  <slot 5> : 2: c1[1]");

        sql = "select c0 from test_map where c1[1] > 10";
        assertPlanContains(sql, "PREDICATES: 2: c1[1] > 10");

        sql = "select avg(c1[1]) from test_map where c1[1] is not null";
        assertPlanContains(sql, "2:AGGREGATE (update finalize)\n" +
                "  |  output: avg(2: c1[1])");

        sql = "select c2[2][1] from test_map";
        assertPlanContains(sql, "<slot 5> : 3: c2[2][1]");

        sql = "select c3['10'] from test_map";
        assertPlanContains(sql, "1:Project\n" +
                "  |  <slot 5> : 4: c3['10']");

        analyzeFail("select c3[\"10\"] from test_map");
    }

    @Test
    public void testSelectMapFunction() throws Exception {
        String sql = "select map_keys(c1) from test_map";
        assertPlanContains(sql, "<slot 5> : map_keys(2: c1)");

        sql = "select map_values(c1) from test_map";
        assertPlanContains(sql, "<slot 5> : map_values(2: c1)");

        sql = "select map() from test_map";
        assertPlanContains(sql, "map{}");

        sql = "select map(array[1,2,3],array['a','b','c']);";
        assertPlanContains(sql, "map_from_arrays([1,2,3], ['a','b','c'])");

        sql = "select map_filter(map(array[10, 20, 30], array['a', NULL, 'c']), (k, v) -> v IS NOT NULL);";
        assertPlanContains(sql, "map_filter(7: map_from_arrays, map_values(map_apply((<slot 2>, <slot 3>) -> " +
                "map{<slot 2>:<slot 3> IS NOT NULL}, 7: map_from_arrays)))");

        sql = "select transform_keys(MAP(ARRAY [1, 2, 3], ARRAY ['a', 'b', 'c']), (k, v) -> k + 1);";
        assertPlanContains(sql, "map_apply((<slot 2>, <slot 3>) -> map{CAST(<slot 2> AS SMALLINT) + 1:<slot 3>}, " +
                "map_from_arrays([1,2,3], ['a','b','c']))");

        sql = "select transform_values(map(array [1, 2, 3], array ['a', 'b', 'c']), (k, v) -> k * k);";
        assertPlanContains(sql, "map_apply((<slot 2>, <slot 3>) -> map{<slot 2>:CAST(<slot 2> AS SMALLINT) * " +
                "CAST(<slot 2> AS SMALLINT)}, map_from_arrays([1,2,3], ['a','b','c']))");
    }

    @Test
    public void testAliasRelation() throws Exception {
        String sql = "select * from (select 1,2,3)";
        assertPlanContains(sql, "1:Project\n" +
                "  |  <slot 2> : 1\n" +
                "  |  <slot 3> : 2\n" +
                "  |  <slot 4> : 3");

        sql = "select 1 from (select 1,2)";
        assertPlanContains(sql, "1:Project\n" +
                "  |  <slot 4> : 1");

        sql = "select v1+1 from (select v1,v2 from t0)";
        assertPlanContains(sql, "1:Project\n" +
                "  |  <slot 4> : 1: v1 + 1");

        sql = "select a.v1+1 from (select v1, v2 from t0) a";
        assertPlanContains(sql, "<slot 4> : 1: v1 + 1");

        sql = "select v1 from (select v1 ,v4 from t0 join t1 on v2 = v5)";
        assertPlanContains(sql, " 5:Project\n" +
                "  |  <slot 1> : 1: v1");

        sql = "select v1 from (select v1 from t0 cross join t1)";
        assertPlanContains(sql, "5:Project\n" +
                "  |  <slot 1> : 1: v1");

        sql = "select\n" +
                "    o_year,\n" +
                "    sum(case\n" +
                "            when nation = 'IRAN' then volume\n" +
                "            else 0\n" +
                "        end) / sum(volume) as mkt_share\n" +
                "from\n" +
                "    (\n" +
                "        select\n" +
                "            extract(year from o_orderdate) as o_year,\n" +
                "            l_extendedprice * (1 - l_discount) as volume,\n" +
                "            n2.n_name as nation\n" +
                "        from\n" +
                "            part,\n" +
                "            supplier,\n" +
                "            lineitem,\n" +
                "            orders,\n" +
                "            customer,\n" +
                "            nation n1,\n" +
                "            nation n2,\n" +
                "            region\n" +
                "        where\n" +
                "                p_partkey = l_partkey\n" +
                "          and s_suppkey = l_suppkey\n" +
                "          and l_orderkey = o_orderkey\n" +
                "          and o_custkey = c_custkey\n" +
                "          and c_nationkey = n1.n_nationkey\n" +
                "          and n1.n_regionkey = r_regionkey\n" +
                "          and r_name = 'MIDDLE EAST'\n" +
                "          and s_nationkey = n2.n_nationkey\n" +
                "          and o_orderdate between date '1995-01-01' and date '1996-12-31'\n" +
                "          and p_type = 'ECONOMY ANODIZED STEEL'\n" +
                "    ) \n" +
                "group by\n" +
                "    o_year\n" +
                "order by\n" +
                "    o_year ";
        assertPlanContains(sql, "  40:Project\n" +
                "  |  <slot 69> : 69: year\n" +
                "  |  <slot 74> : 72: sum / 73: sum");
    }

    @Test
    public void testAliasCaseInsensitive() throws Exception {
        String sql = "select T.v1 from (select * from t0) t";
        assertPlanContains(sql, "t0");

        sql = "select t.v1 from (select * from t0) as T";
        assertPlanContains(sql, "t0");

        sql = "select t.v1 from t0 T";
        assertPlanContains(sql, "t0");

        sql = "select T.v1 from (select * from t0 join t1 on v2 = v5) t";
        assertPlanContains(sql, "INNER JOIN ");
    }

    @Test
    public void testSelectSetOperation() throws Exception {
        connectContext.getSessionVariable().setCboPushDownTopNLimit(0);
        String sql = "select * from t0 union select * from t1 union select * from t0";
        assertPlanContains(sql, "7:AGGREGATE (update serialize)\n" +
                "  |  STREAMING\n" +
                "  |  group by: 10: v1, 11: v2, 12: v3\n" +
                "  |  \n" +
                "  0:UNION");

        sql = "select * from t0 intersect select * from t1 intersect select * from t0";
        assertPlanContains(sql, "0:INTERSECT");

        sql = "select v1 from t0 except select v4 from t1 except select v2 from t0";
        assertPlanContains(sql, "0:EXCEPT");

        sql = "select v1 from t0 union all select v4 from t1 union select v2 from t0 limit 10";
        assertPlanContains(sql, "0:UNION", "11:AGGREGATE (merge finalize)\n" +
                "  |  group by: 11: v1\n" +
                "  |  limit: 10");

        sql = "select * from (select * from t0 union all select * from t1 union all select * from t0) tt;";
        assertPlanContains(sql, "0:UNION");

        sql = "select * from (select v1 from t0 union all select v4 from t1 union all select v3 from t0) tt order by v1 " +
                "limit 2;";
        assertPlanContains(sql, "0:UNION", "7:TOP-N\n" +
                "  |  order by: <slot 10> 10: v1 ASC\n" +
                "  |  offset: 0\n" +
                "  |  limit: 2");

        sql = "select * from (select v1 from t0 intersect select v4 from t1 intersect select v3 from t0 limit 10) tt " +
                "order by v1 limit 2;";
        assertPlanContains(sql, "0:INTERSECT\n" +
                        "  |  limit: 10",
                "8:TOP-N\n" +
                        "  |  order by: <slot 10> 10: v1 ASC\n" +
                        "  |  offset: 0\n" +
                        "  |  limit: 2");
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
    public void testSelectCTE() throws Exception {
        String sql = "with c1(a,b,c) as (select * from t0) select c1.* from c1";
        assertPlanContains(sql, "1: v1 | 2: v2 | 3: v3");

        sql = "with c1(a,b,c) as (select * from t0) select t.* from c1 t";
        assertPlanContains(sql, "1: v1 | 2: v2 | 3: v3");

        sql = "with c1 as (select * from t0) select c1.* from c1";
        assertPlanContains(sql, "1: v1 | 2: v2 | 3: v3");

        sql = "with c1 as (select * from t0) select a.* from c1 a";
        assertPlanContains(sql, "1: v1 | 2: v2 | 3: v3");

        sql = "with c1(a,b,c) as (select * from t0), c2 as (select * from t1) select c2.*,t.* from c1 t,c2";
        assertPlanContains(sql, "3:NESTLOOP JOIN");

        sql = "with tbl1 as (select v1, v2 from t0), tbl2 as (select v4, v5 from t1) select tbl1.*, tbl2.* from " +
                "tbl1 join tbl2 on tbl1.v1 = tbl2.v4";
        assertPlanContains(sql, "4:HASH JOIN\n" +
                "  |  join op: INNER JOIN (PARTITIONED)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 1: v1 = 4: v4");

        sql = "with cte1 as ( with cte1 as (select * from t0) select * from cte1) select * from cte1";
        assertPlanContains(sql, "1: v1 | 2: v2 | 3: v3");

        sql = "with cte1 as (select * from test.t0), cte2 as (select * from cte1) select * from cte1";
        assertPlanContains(sql, "4: v1 | 5: v2 | 6: v3");

        sql = "with cte1(c1,c2) as (select v1,v2 from test.t0) select c1,c2 from cte1";
        assertPlanContains(sql, "1: v1 | 2: v2");

        sql = "with x0 as (select * from t0), x1 as (select * from t1) " +
                "select * from (select * from x0 union all select * from x1 union all select * from x0) tt;";
        assertPlanContains(sql, "0:UNION");

        sql = "with x0 as (select * from t0), x1 as (select * from x0) " +
                "select * from (select * from x0 union all select * from x1 union all select * from x0) tt;";
        assertPlanContains(sql, "0:UNION");

        sql = "with x0 as (select * from t0) " +
                "select * from (with x1 as (select * from t1) select * from x1 join x0 on x1.v4 = x0.v1) tt";
        assertPlanContains(sql, "4:HASH JOIN\n" +
                "  |  join op: INNER JOIN (PARTITIONED)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 1: v4 = 4: v1");

        sql = "with x0 as (select * from t0) " +
                "select * from x0 x,t1 y where v1 in (select v2 from x0 z where z.v1 = x.v1)";
        assertPlanContains(sql, "8:NESTLOOP JOIN", "LEFT SEMI JOIN (PARTITIONED)");
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
    public void testRegexp() throws Exception {
        String sql = "select regexp_like('1a 2b 14m', '\\d+b')";
        assertPlanContains(sql, "regexp('1a 2b 14m', '\\\\d+b')");

        sql = "select regexp_like('abc123','abc*');";
        assertPlanContains(sql, "regexp('abc123', 'abc*')");

        sql = "select regexp_extract('1a 2b 14m', '\\d+');";
        assertPlanContains(sql, "if(3: regexp_extract = '', NULL, 3: regexp_extract)\n" +
                "  |  common expressions:\n" +
                "  |  <slot 3> : regexp_extract('1a 2b 14m', '\\\\d+', 0)");

        sql = "select regexp_extract('1abb 2b 14m', '[a-z]+');";
        assertPlanContains(sql, "if(3: regexp_extract = '', NULL, 3: regexp_extract)\n" +
                "  |  common expressions:\n" +
                "  |  <slot 3> : regexp_extract('1abb 2b 14m', '[a-z]+', 0)");

        sql = "select regexp_extract('1abb 2b 14m', '[a-z]+', 1);";
        assertPlanContains(sql, "if(3: regexp_extract = '', NULL, 3: regexp_extract)\n" +
                "  |  common expressions:\n" +
                "  |  <slot 3> : regexp_extract('1abb 2b 14m', '[a-z]+', 1)");
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

    @Test
    public void testExplain() throws Exception {
        String sql = "explain (TYPE logical) select v1, v2 from t0,t1";
        Assert.assertTrue(getExplain(sql), StringUtils.containsIgnoreCase(getExplain(sql),
                "SCAN [t1] => [8:auto_fill_col]\n" +
                        "                    Estimates: {row: 1, cpu: 9.00, memory: 0.00, network: 0.00, cost: 4.50}\n" +
                        "                    partitionRatio: 0/1, tabletRatio: 0/0\n" +
                        "                    8:auto_fill_col := 1"));

        sql = "explain select v1, v2 from t0,t1";
        Assert.assertTrue(StringUtils.containsIgnoreCase(getExplain(sql),
                "2:Project\n" +
                        "  |  <slot 8> : 1\n" +
                        "  |  \n" +
                        "  1:OlapScanNode\n" +
                        "     TABLE: t1\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     partitions=0/1"));

        sql = "explain (Type DISTRIBUTED)select v1, v2 from t0,t1";
        Assert.assertTrue(StringUtils.containsIgnoreCase(getExplain(sql),
                "5:Project\n" +
                        "  |  output columns:\n" +
                        "  |  1 <-> [1: v1, BIGINT, true]\n" +
                        "  |  2 <-> [2: v2, BIGINT, true]\n" +
                        "  |  cardinality: 1\n" +
                        "  |  \n" +
                        "  4:NESTLOOP JOIN"));

        sql = "explain (Type io)select v1, v2 from t0,t1";
        Assert.assertTrue(StringUtils.containsIgnoreCase(getExplain(sql),
                "5:Project\n" +
                        "  |  output columns:\n" +
                        "  |  1 <-> [1: v1, BIGINT, true]\n" +
                        "  |  2 <-> [2: v2, BIGINT, true]\n" +
                        "  |  cardinality: 1\n" +
                        "  |  column statistics: \n" +
                        "  |  * v1-->[-Infinity, Infinity, 0.0, 1.0, 1.0] UNKNOWN\n" +
                        "  |  * v2-->[-Infinity, Infinity, 0.0, 1.0, 1.0] UNKNOWN"));
    }

    @Test
    public void testIntervalLiteral() throws Exception {
        String sql = "select timestamp '2022-01-01' + interval '1' year;";
        assertPlanContains(sql, "<slot 2> : '2023-01-01 00:00:00'");

        sql = "select timestamp '2022-01-01' + interval '1' year + interval '1' month;";
        assertPlanContains(sql, "<slot 2> : '2023-02-01 00:00:00'");

        sql = "select timestamp '2022-01-01' + interval '1' year + interval '1' month + interval '1' day;";
        assertPlanContains(sql, "<slot 2> : '2023-02-02 00:00:00'");

        sql = "select timestamp '2022-01-01' + interval '1' year + interval '1' month + interval '1' day + interval '1' hour;";
        assertPlanContains(sql, "<slot 2> : '2023-02-02 01:00:00'");

        sql = "select timestamp '2022-01-01' + interval '1' year + interval '1' month + interval '1' day + interval '1' hour + " +
                "interval '1' minute;";
        assertPlanContains(sql, "<slot 2> : '2023-02-02 01:01:00'");

        sql = "select timestamp '2022-01-01' + interval '1' year + interval '1' month + interval '1' day + interval '1' hour + " +
                "interval '1' minute + interval '1' second;";
        assertPlanContains(sql, "<slot 2> : '2023-02-02 01:01:01'");

        sql = "select interval '1' year + timestamp '2022-01-01';";
        assertPlanContains(sql, "<slot 2> : '2023-01-01 00:00:00'");
    }

    @Test
    public void testIntervalDateLiteral() throws Exception {
        String sql = "select date '2022-01-01' + interval '1' year;";
        assertPlanContains(sql, "<slot 2> : '2023-01-01'");

        sql = "select date '2022-01-01' + interval '1' year + interval '1' month;";
        assertPlanContains(sql, "<slot 2> : '2023-02-01'");

        sql = "select date '2022-01-01' + interval '1' year + interval '1' month + interval '1' day;";
        assertPlanContains(sql, "<slot 2> : '2023-02-02'");

        sql = "select date '2022-01-01' + interval '1' year + interval '1' month + interval '1' day + interval '1' hour;";
        assertPlanContains(sql, "<slot 2> : '2023-02-02'");

        sql = "select date '2022-01-01' + interval '1' year + interval '1' month + interval '1' day + interval '1' hour + " +
                "interval '1' minute;";
        assertPlanContains(sql, "<slot 2> : '2023-02-02'");

        sql = "select date '2022-01-01' + interval '1' year + interval '1' month + interval '1' day + interval '1' hour + " +
                "interval '1' minute + interval '1' second;";
        assertPlanContains(sql, "<slot 2> : '2023-02-02'");

        sql = "select interval '1' year + date '2022-01-01';";
        assertPlanContains(sql, "<slot 2> : '2023-01-01'");
    }

    @Test
    public void selectDoubleLiteral() throws Exception {
        String sql = "select 1.0";
        assertPlanContains(sql, "<slot 2> : 1.0");

        sql = "select  -1.79E+309;";
        analyzeFail(sql);

        sql = "select  -1.79E+3;";
        assertPlanContains(sql, "<slot 2> : -1790.0");

        sql = "select  -1.79E+10;";
        assertPlanContains(sql, "<slot 2> : -17900000000");

        sql = "select approx_percentile(2.25, -1.79E+309)";
        analyzeFail(sql);

        sql = "select approx_percentile(2.25, 1.79E-10)";
        assertPlanContains(sql, "percentile_approx(2.25, 1.79E-10)");

        sql = "select approx_percentile(2.25, 0.4)";
        assertPlanContains(sql, "percentile_approx(2.25, 0.4)");
    }

    @Test
    public void testTrim() throws Exception {
        String sql = "select trim(' abc ');";
        assertPlanContains(sql, "<slot 2> : trim(' abc ')");

        sql = "select trim('!' from '!foo!');";
        assertPlanContains(sql, "<slot 2> : trim('!foo!', '!')");

        sql = "select trim(leading from '  abcd');";
        assertPlanContains(sql, "<slot 2> : ltrim('  abcd')");

        sql = "select trim(leading 'a' from '  abcd');";
        assertPlanContains(sql, "<slot 2> : ltrim('  abcd', 'a')");

        sql = "select trim(both '$' FROM '$var$');";
        assertPlanContains(sql, "<slot 2> : trim('$var$', '$')");

        sql = "select trim(both from '  abcd');";
        assertPlanContains(sql, "<slot 2> : trim('  abcd')");

        sql = "select trim(trailing 'ER' from upper('worker'));";
        assertPlanContains(sql, "<slot 2> : rtrim(upper('worker'), 'ER')");

        sql = "select trim(trailing from '  abcd');";
        assertPlanContains(sql, "<slot 2> : rtrim('  abcd')");
    }

    @Test
    public void testTry() throws Exception {
        String sql = "select try_cast('aa' as int)";
        assertPlanContains(sql, "<slot 2> : CAST('aa' AS INT)");

        sql = "select try_cast(ta as bigint) + 1 from tall";
        assertPlanContains(sql, "CAST(1: ta AS BIGINT) + 1");

        sql = "select try(cast ('aa' as int))";
        assertPlanContains(sql, "<slot 2> : CAST('aa' AS INT)");

        sql = "select try(2 / 0)";
        assertPlanContains(sql, "<slot 2> : NULL");

        sql = "select try(100 / v1) from t0 where v1 = 0";
        assertPlanContains(sql, "100.0 / CAST(1: v1 AS DOUBLE)");

        sql = "select coalesce(try(100 / v1), 1) from t0 where v1 = 0";
        assertPlanContains(sql, "coalesce(100.0 / CAST(1: v1 AS DOUBLE), 1.0)");
    }

    @Test
    public void testUnaryExpression() throws Exception {
        String sql = "select -abs(1);";
        assertPlanContains(sql, "-1 * CAST(abs(1) AS INT)");

        sql = "select +abs(-1);";
        assertPlanContains(sql, "abs(-1)");

        sql = "select \n" +
                "  - if(\n" +
                "    day_of_week(\n" +
                "      cast('2023-01-01' AS date)\n" +
                "    )= 7, \n" +
                "    0, \n" +
                "    day_of_week(\n" +
                "      cast('2023-01-01' AS date)\n" +
                "    )\n" +
                "  );";
        assertPlanContains(sql, "-1 * CAST(if(3: dayofweek_iso = 7, 0, 3: dayofweek_iso) AS BIGINT)");
    }

    @Test
    public void testJsonArray() throws Exception {
        String sql = "select json_array(1, true, 'starrocks',1.1);";
        assertPlanContains(sql, "json_array(CAST(1 AS JSON), CAST(TRUE AS JSON), CAST('starrocks' AS JSON), CAST(1.1 AS JSON))");

        sql = "select json_array()";
        assertPlanContains(sql, "json_array()");

        sql = "select json_array(ta, tb, tc, tg) from tall;";
        assertPlanContains(sql, "json_array(CAST(1: ta AS JSON), CAST(2: tb AS JSON), CAST(3: tc AS JSON), CAST(7: tg AS JSON))");

        sql = "SELECT json_array_get('[\"a\", [3, 9], \"c\"]', 0);";
        assertPlanContains(sql, "json_query(parse_json('[\"a\", [3, 9], \"c\"]'), '$.[0]')");

        sql = "select json_array_get(json_array(true, 12e-1, 'text'), 2);";
        assertPlanContains(sql, "json_query(json_array(CAST(TRUE AS JSON), CAST(1.2 AS JSON), CAST('text' AS JSON)), '$.[2]')");

        sql = "SELECT json_array_get(cast('[true, 12e-1, \"text\"]' as json), 1);";
        assertPlanContains(sql, "json_query(CAST('[true, 12e-1, \"text\"]' AS JSON), '$.[1]')");
    }

    @Test
    public void testJsonQuery() throws Exception {
        String sql = "select json_query('[true, 12e-1, \"text\"]', 'lax $[1]');";
        assertPlanContains(sql, "json_query(CAST('[true, 12e-1, \"text\"]' AS JSON), '$[1]')");

        sql = "select json_query('[true, 12e-1, \"text\"]', 'strict $[1]');";
        assertPlanContains(sql, " json_query(CAST('[true, 12e-1, \"text\"]' AS JSON), '$[1]')");

        sql = "select json_query('{\"comment\" : \"nice\", \"children\" : [10, 13, 16]}', 'lax $.children');";
        assertPlanContains(sql, "json_query(CAST('{\"comment\" : \"nice\", \"children\" : [10, 13, 16]}' AS JSON), " +
                "'$.children')");
    }

    @Test
    public void testSelectValue() throws Exception {
        String sql = "select * from (values (1, 2, 3))";
        assertPlanContains(sql, "1 | 2 | 3");

        sql = "select * from (values (1, 2, 3)) as t0(a,b,c);";
        assertPlanContains(sql, "1: a | 2: b | 3: c");

        sql = "select * from (values 1, 2, 3)";
        assertPlanContains(sql, "constant exprs: \n" +
                "         1\n" +
                "         2\n" +
                "         3");

        sql = "select * from (values (1), (2), (3))";
        assertPlanContains(sql, "1\n" +
                "         2\n" +
                "         3");

        sql = "select * from (values (0,1),(1,2),(2,3)) t0 (a,b) ;";
        assertPlanContains(sql, "0 | 1\n" +
                "         1 | 2\n" +
                "         2 | 3");

        sql = "select * from (values (0,(1,2)),(1,(2,3)),(2,(3,4)))  t0 (a,b) ;";
        assertPlanContains(sql, "0 | row(1, 2)\n" +
                "         1 | row(2, 3)\n" +
                "         2 | row(3, 4)");
    }

    @Test
    public void testSelectReal() throws Exception {
        String sql = "select real '10.3'";
        assertPlanContains(sql, "<slot 2> : 10.3");

        sql = "select cast('1.1' as real)";
        assertPlanContains(sql, "<slot 2> : 1.1");

        sql = "select cast(v1 / v2 as real) from t0";
        assertPlanContains(sql, "CAST(CAST(1: v1 AS DOUBLE) / CAST(2: v2 AS DOUBLE) AS FLOAT)");
    }

    @Test
    public void testUnnest() throws Exception {
        String sql = "select * from (\n" +
                "    select 1 gid, array[11,12,13] aid, array[14,15,16] as bid union all select 2 gid, array[21,22,23] as aid, " +
                "    array[24,25,26] as bid\n" +
                ") a\n" +
                "cross join unnest(array[1, 2]) as plat(plat)";
        assertPlanContains(sql, "tableFunctionName: unnest");

        sql = "select * from (\n" +
                "    select 1 gid, array[11,12,13] aid, array[14,15,16] as bid union all select 2 gid, array[21,22,23] as aid, " +
                "    array[24,25,26] as bid\n" +
                ") a\n" +
                "cross join unnest(array[row(1, aid), row(2, bid)]) as plat(plat, pid)";
        assertPlanContains(sql, "[1,2]", "[10: expr,11: expr]");

        sql = "select * from (\n" +
                "select 1 gid, array[11,12,13] aid, array[14,15,16] as bid union all select 2 gid, array[21,22,23] as aid, " +
                "array[24,25,26] as bid\n" +
                ") a\n" +
                "cross join unnest(array[row(1,2), row(3,4)],array[2,3]) as plat(a,b,c);";
        assertPlanContains(sql, "[1,3]", "[2,4]", "[2,3]");

        sql = "select * from (\n" +
                "select 1 gid, array[11,12,13] aid, array[14,15,16] as bid union all select 2 gid, array[21,22,23] as aid, " +
                "array[24,25,26] as bid\n" +
                ") a\n" +
                "cross join unnest(array[0,0], array[row(1,2), row(3,4)],array[2,3]) as plat(a,b,c,d);";
        assertPlanContains(sql, "[0,0]", "[1,3]", "[2,4]", "[2,3]");

        sql = "select * from (\n" +
                "    select 1 gid, array[11,12,13] aid, array[14,15,16] as bid union all select 2 gid, array[21,22,23] as aid, " +
                "  array[24,25,26] as bid\n" +
                ") a\n" +
                "cross join unnest(array[row(1, aid), row(2, bid)]) as plat(plat, pid)\n" +
                "cross join unnest(plat.pid) as t(plat_id);";
        assertPlanContains(sql, "[1,2]", "[10: expr,11: expr]", "returnTypes: [TINYINT, ARRAY<TINYINT>]",
                "returnTypes: [TINYINT]");
    }

    @Test
    public void testRandom() throws Exception {
        String sql = "select rand();";
        assertPlanContains(sql, "<slot 2> : rand()");

        sql = "select rand(100);";
        assertPlanContains(sql, "<slot 2> : floor(random() * 100.0)");

        sql = "select rand(10, 100);";
        assertPlanContains(sql, "floor(random() * 90.0 + 10.0)");

        sql = "select random();";
        assertPlanContains(sql, "<slot 2> : random()");

        sql = "select random(100);";
        assertPlanContains(sql, "<slot 2> : floor(random() * 100.0)");

        sql = "select rand(10, 100);";
        assertPlanContains(sql, "<slot 2> : floor(random() * 90.0 + 10.0)");
    }
}
