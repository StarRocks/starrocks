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

package com.starrocks.sql.plan;

import com.google.common.collect.Sets;
import com.starrocks.analysis.AnalyticExpr;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.utframe.StarRocksAssert;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class ArrayTypeTest extends PlanTestBase {
    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    @BeforeClass
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        StarRocksAssert starRocksAssert = new StarRocksAssert(connectContext);
        starRocksAssert.withTable("create table test_array(c0 INT, c1 array<varchar(65533)>, c2 array<int>) " +
                " duplicate key(c0) distributed by hash(c0) buckets 1 " +
                "properties('replication_num'='1');");

        starRocksAssert.withTable("CREATE TABLE adec ( \n" +
                "v1 bigint not null ,\n" +
                "i_1 Array<INT> NOT NULL ,\n" +
                "s_1 Array<String> NULL ,\n" +
                "d_1 Array<DECIMAL(26, 2)> NOT NULL ,\n" +
                "d_2 Array<DECIMAL64(4, 3)> NULL ,\n" +
                "d_3 Array<DECIMAL128(25, 19)> NOT NULL ,\n" +
                "d_4 Array<DECIMAL32(8, 5)> NULL ,\n" +
                "d_5 Array<DECIMAL(16, 3)> NULL ,\n" +
                "d_6 Array<DECIMAL128(18, 6)> NOT NULL \n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`v1`)\n" +
                "DISTRIBUTED BY HASH(`v1`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\"\n" +
                ");");
    }

    @Test
    public void testConcatArray() throws Exception {
        String sql = "select concat(c1, c2) from test_array";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "array_concat(2: c1, CAST(3: c2 AS ARRAY<VARCHAR(65533)>))");

        sql = "select concat(c1, c0) from test_array";
        plan = getFragmentPlan(sql);
        assertContains(plan, "array_concat(2: c1, CAST([1: c0] AS ARRAY<VARCHAR(65533)>))");

        sql = "select concat(c0, c2) from test_array";
        plan = getFragmentPlan(sql);
        assertContains(plan, "array_concat([1: c0], 3: c2)");

        sql = "select concat(t1a, t1b, t1c) from test_all_type_not_null";
        plan = getFragmentPlan(sql);
        assertContains(plan, "concat(1: t1a, CAST(2: t1b AS VARCHAR), CAST(3: t1c AS VARCHAR))");

        sql = "select concat(i_1, s_1, d_1) from adec";
        plan = getFragmentPlan(sql);
        assertContains(plan, "array_concat(CAST(2: i_1 AS ARRAY<VARCHAR(65533)>), 3: s_1, " +
                "CAST(4: d_1 AS ARRAY<VARCHAR(65533)>))");

        sql = "select concat(d_1, d_2) from adec";
        plan = getFragmentPlan(sql);
        assertContains(plan, "array_concat(CAST(4: d_1 AS ARRAY<DECIMAL128(27,3)>), " +
                "CAST(5: d_2 AS ARRAY<DECIMAL128(27,3)>))");

        sql = "select concat(d_1, d_2, d_3, d_4, d_5, d_6) from adec";
        plan = getFragmentPlan(sql);
        assertContains(plan, "array_concat(CAST(4: d_1 AS ARRAY<DOUBLE>), CAST(5: d_2 AS ARRAY<DOUBLE>), " +
                "CAST(6: d_3 AS ARRAY<DOUBLE>), CAST(7: d_4 AS ARRAY<DOUBLE>), CAST(8: d_5 AS ARRAY<DOUBLE>), " +
                "CAST(9: d_6 AS ARRAY<DOUBLE>))");

        sql = "select concat(i_1, s_1, d_1, d_2, d_3, d_4, d_5, d_6) from adec";
        plan = getFragmentPlan(sql);
        assertContains(plan, "array_concat(CAST(2: i_1 AS ARRAY<VARCHAR(65533)>), 3: s_1, " +
                "CAST(4: d_1 AS ARRAY<VARCHAR(65533)>), CAST(5: d_2 AS ARRAY<VARCHAR(65533)>), " +
                "CAST(6: d_3 AS ARRAY<VARCHAR(65533)>), CAST(7: d_4 AS ARRAY<VARCHAR(65533)>), " +
                "CAST(8: d_5 AS ARRAY<VARCHAR(65533)>), CAST(9: d_6 AS ARRAY<VARCHAR(65533)>))");

        sql = "select concat(v1, [1,2,3], s_1) from adec";
        plan = getFragmentPlan(sql);
        assertContains(plan, "array_concat(CAST([1: v1] AS ARRAY<VARCHAR(65533)>), " +
                "CAST([1,2,3] AS ARRAY<VARCHAR(65533)>), 3: s_1)");

        sql = "select concat(1,2, [1,2])";
        plan = getFragmentPlan(sql);
        assertContains(plan, "array_concat([1], [2], [1,2])");

        sql = "select concat(1,2, [1,2], 'a', 'b')";
        plan = getFragmentPlan(sql);
        assertContains(plan, "array_concat(CAST([1] AS ARRAY<VARCHAR>), CAST([2] AS ARRAY<VARCHAR>), " +
                "CAST([1,2] AS ARRAY<VARCHAR>), ['a'], ['b'])");

        sql = "select concat(1,2, [1,2], 'a', 'b', 1.1)";
        plan = getFragmentPlan(sql);
        assertContains(plan, "array_concat(CAST([1] AS ARRAY<VARCHAR>), CAST([2] AS ARRAY<VARCHAR>), " +
                "CAST([1,2] AS ARRAY<VARCHAR>), ['a'], ['b'], CAST([1.1] AS ARRAY<VARCHAR>)");
    }

    @Test
    public void testSelectArrayElementFromArrayColumn() throws Exception {
        String sql = "select v3[1] from tarray";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("1:Project\n" +
                "  |  <slot 4> : 3: v3[1]"));
    }

    @Test
    public void testArrayElementWithFunction() throws Exception {
        String sql = "select v1, sum(v3[1]) from tarray group by v1";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("1:Project\n" +
                "  |  <slot 1> : 1: v1\n" +
                "  |  <slot 4> : 3: v3[1]"));
    }

    @Test
    public void testArrayCountDistinctWithOrderBy() throws Exception {
        String sql = "select distinct v3 from tarray order by v3[1];";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("2:Project\n" +
                "  |  <slot 3> : 3: v3\n" +
                "  |  <slot 4> : 3: v3[1]"));
    }

    @Test
    public void testArrayElementExpr() throws Exception {
        String sql = "select [][1] + 1, [1,2,3][1] + [[1,2,3],[1,1,1]][2][2]";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  |  <slot 2> : NULL\n" +
                "  |  <slot 3> : CAST([1,2,3][1] AS SMALLINT) + " +
                "CAST([[1,2,3],[1,1,1]][2][2] AS SMALLINT)"));

        sql = "select v1, v3[1] + [1,2,3][1] as v, sum(v3[1]) from tarray group by v1, v order by v";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("2:AGGREGATE (update finalize)\n" +
                "  |  output: sum(5: expr)\n" +
                "  |  group by: 1: v1, 4: expr\n" +
                "  |  \n" +
                "  1:Project\n" +
                "  |  <slot 1> : 1: v1\n" +
                "  |  <slot 4> : 7: expr + CAST([1,2,3][1] AS BIGINT)\n" +
                "  |  <slot 5> : 7: expr\n" +
                "  |  common expressions:\n" +
                "  |  <slot 7> : 3: v3[1]"));
    }

    @Test
    public void testSelectDistinctArrayWithOrderBy() throws Exception {
        String sql = "select distinct v1 from tarray order by v1+1";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("2:Project\n" +
                "  |  <slot 1> : 1: v1\n" +
                "  |  <slot 4> : 1: v1 + 1"));
    }

    @Test
    public void testSelectDistinctArrayWithOrderBy2() throws Exception {
        String sql = "select distinct v1+1 as v from tarray order by v+1";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("3:Project\n" +
                "  |  <slot 4> : 4: expr\n" +
                "  |  <slot 5> : 4: expr + 1\n"));
        Assert.assertTrue(plan.contains("1:Project\n" +
                "  |  <slot 4> : 1: v1 + 1"));
    }

    @Test
    public void testSelectMultidimensionalArray() throws Exception {
        String sql = "select [[1,2],[3,4]][1][2]";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("[[1,2],[3,4]][1][2]"));
    }

    @Test
    public void testSelectArrayElement() throws Exception {
        String sql = "select [1,2][1]";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("[1,2][1]"));

        sql = "select [][1]";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("[][1]"));

        sql = "select [][1] from t0";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("[][1]"));

        sql = "select [][1] from (values(1,2,3), (4,5,6)) t";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("[][1]"));

        sql = "select [v1,v2] from t0";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("1:Project\n" +
                "  |  <slot 4> : [1: v1,2: v2]"));

        sql = "select [v1 = 1, v2 = 2, true] from t0";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("<slot 4> : [1: v1 = 1,2: v2 = 2,TRUE]"));
    }

    @Test
    public void testCountDistinctArray() throws Exception {
        String sql = "select count(*), count(c1), count(distinct c1) from test_array";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("AGGREGATE (merge serialize)"));
    }

    @Test
    public void testArrayFunctionFilter() throws Exception {
        String sql = "select * from test_array where array_length(c1) between 2 and 3;";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("PREDICATES: array_length(2: c1) >= 2, array_length(2: c1) <= 3"));
    }

    @Test
    public void testArrayDifferenceArgs1() throws Exception {
        String sql = "select array_difference(c2) from test_array";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("array_difference(3: c2)"));

        sql = "select array_difference(c1) from test_array";
        expectedEx.expect(SemanticException.class);
        expectedEx.expectMessage("array_difference function only support numeric array types");
        getFragmentPlan(sql);
    }

    @Test
    public void testArrayDifferenceArgs2() throws Exception {
        String sql = "select array_difference(c0) from test_array";
        expectedEx.expect(SemanticException.class);
        getFragmentPlan(sql);
    }

    @Test
    public void testArrayDifferenceArgs3() throws Exception {
        String sql = "select array_difference(c1, 3) from test_array";
        expectedEx.expect(SemanticException.class);
        getFragmentPlan(sql);
    }

    @Test
    public void testArrayDifferenceNullAndEmpty() throws Exception {
        String sql = "select array_difference(null)";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("NULL"));

        sql = "select array_difference([])";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("array_difference"));
    }

    @Test
    public void testArrayClone() throws Exception {
        String sql =
                "select array_contains([v],1), array_contains([v],2) " +
                        "from (select v1+1 as v from t0,t1 group by v) t group by 1,2";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan, plan.contains("9:Project\n" +
                "  |  <slot 8> : array_contains([7: expr], 1)\n" +
                "  |  <slot 9> : array_contains([7: expr], 2)"));
    }

    @Test
    public void testArrayWindowFunction() throws Exception {
        for (String fnName : Sets.newHashSet(AnalyticExpr.LASTVALUE, AnalyticExpr.FIRSTVALUE)) {
            String sql = String.format("select %s(v3) over() from tarray", fnName.toLowerCase());
            expectedEx.expect(SemanticException.class);
            expectedEx.expectMessage(
                    String.format("No matching function with signature: %s(array<bigint(20)>)", fnName.toLowerCase()));
            getFragmentPlan(sql);
        }
    }

    @Test
    public void testEmptyArray() throws Exception {
        {
            String sql = "select cast([] as array<varchar(200)>)";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  1:Project\n" +
                    "  |  <slot 2> : CAST([] AS ARRAY<VARCHAR(200)>)\n" +
                    "  |  \n" +
                    "  0:UNION\n" +
                    "     constant exprs: \n" +
                    "         NULL");

            String thriftPlan = getThriftPlan(sql);
            assertNotContains(thriftPlan, "NULL_TYPE");
        }
        {
            String sql = "select cast(null as array<varchar(200)>)";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  1:Project\n" +
                    "  |  <slot 2> : NULL\n" +
                    "  |  \n" +
                    "  0:UNION\n" +
                    "     constant exprs: \n" +
                    "         NULL");
        }
        {
            String sql = "select array_append([[1,2,3]], [])";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "<slot 2> : array_append([[1,2,3]], [])");
        }
        {
            String sql = "select array_append([[1,2,3]], [null])";
            String plan = getFragmentPlan(sql);
            assertContains(plan,
                    "<slot 2> : array_append([[1,2,3]], [NULL])");
        }
        {
            starRocksAssert.withTable("create table test_literal_array_insert_t0(" +
                    "c0 bigint, " +
                    "c1 ARRAY<ARRAY<bigint>> not null, " +
                    "c2 array<Array<bigint>> not null" +
                    ")" +
                    "duplicate key(c0) " +
                    "distributed by hash(c0) " +
                    "buckets 1 properties('replication_num'='1');\n");

            String sql = "insert into test_literal_array_insert_t0 values " +
                    "(4,[],[])";
            getFragmentPlan(sql);

            sql = "insert into test_literal_array_insert_t0 values " +
                    "(4,[],[]), " +
                    "(9223372036854775807,[[9223372036854775807]],[[9223372036854775807]]);\n";
            getFragmentPlan(sql);
        }
        {
            String sql = "select array_append([],null)";
            getThriftPlan(sql);
        }
        {
            String sql = "select [][1]";
            getThriftPlan(sql);
        }
        {
            String sql = "select array_append([], [])";
            getThriftPlan(sql);
        }
        {
            String sql = "select array_append([[]], [])";
            getThriftPlan(sql);
        }
    }

    @Test
    public void testNestedArrayLambdaFunctions() throws Exception {
        String sql = "WITH `CASE_006` AS\n" +
                "  (SELECT array_map((arg_001) -> (arg_001), `c1`) AS `argument_003`,\n" +
                "          array_map((arg_002) -> (CAST(1 AS BIGINT)), `c1`) AS `argument_004`\n" +
                "   FROM test_array)\n" +
                "\n" +
                "select argument_004, ARRAY_FILTER((x, y) -> y IS NOT NULL, " +
                "`argument_003`, `argument_004`) AS `source_target_005` from CASE_006;";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "  |  common expressions:\n" +
                "  |  <slot 14> : array_map(<slot 5> -> 1, 8: c1)");

        sql = "WITH `CASE_006` AS\n" +
                "  (SELECT array_map((arg_001) -> (arg_001), `c1`) AS `argument_003`,\n" +
                "          array_map((arg_002) -> (arg_002 + 1), `c1`) AS `argument_004`\n" +
                "   FROM test_array)\n" +
                "\n" +
                "select argument_004, ARRAY_FILTER((x, y) -> y IS NOT NULL, " +
                "`argument_003`, `argument_004`) AS `source_target_005` from CASE_006;";
        plan = getFragmentPlan(sql);
        assertContains(plan, "  |  common expressions:\n" +
                "  |  <slot 14> : array_map(<slot 5> -> CAST(<slot 5> AS DOUBLE) + 1.0, 8: c1)");
    }

    @Test
    public void testArraySortDecimalType() throws Exception {
        String sql = "select array_sort(d_1) from adec;";
        String plan = getVerboseExplain(sql);
        assertContains(plan, "array_sort[([4: d_1, ARRAY<DECIMAL128(26,2)>, false]); " +
                "args: INVALID_TYPE; result: ARRAY<DECIMAL128(26,2)>;");

        sql = "select array_sort(d_2) from adec;";
        plan = getVerboseExplain(sql);
        assertContains(plan, "array_sort[([5: d_2, ARRAY<DECIMAL64(4,3)>, true]); " +
                "args: INVALID_TYPE; result: ARRAY<DECIMAL64(4,3)>;");

        sql = "select array_sort(d_4) from adec;";
        plan = getVerboseExplain(sql);
        assertContains(plan, "array_sort[([7: d_4, ARRAY<DECIMAL32(8,5)>, true]);" +
                " args: INVALID_TYPE; result: ARRAY<DECIMAL32(8,5)>;");

        sql = "select array_sort(d_5) from adec;";
        plan = getVerboseExplain(sql);
        assertContains(plan, "array_sort[([8: d_5, ARRAY<DECIMAL64(16,3)>, true]); " +
                "args: INVALID_TYPE; result: ARRAY<DECIMAL64(16,3)>;");
    }

    @Test
    public void testArrayIntersectDecimalType() throws Exception {
        String sql = "select array_intersect(d_1, d_2) from adec;";
        String plan = getVerboseExplain(sql);
        assertCContains(plan,
                "array_intersect[(cast([4: d_1, ARRAY<DECIMAL128(26,2)>, false] as ARRAY<DECIMAL128(27,3)>), " +
                        "cast([5: d_2, ARRAY<DECIMAL64(4,3)>, true] as ARRAY<DECIMAL128(27,3)>)); " +
                        "args: INVALID_TYPE; result: ARRAY<DECIMAL128(27,3)>;");

        sql = "select array_intersect(d_1, d_3) from adec;";
        plan = getVerboseExplain(sql);
        assertCContains(plan, " array_intersect[(cast([4: d_1, ARRAY<DECIMAL128(26,2)>, false] as ARRAY<DOUBLE>), " +
                "cast([6: d_3, ARRAY<DECIMAL128(25,19)>, false] as ARRAY<DOUBLE>)); args: INVALID_TYPE; " +
                "result: ARRAY<DOUBLE>;");

        sql = "select array_intersect(d_3, d_4) from adec;";
        plan = getVerboseExplain(sql);
        assertCContains(plan, "array_intersect[([6: d_3, ARRAY<DECIMAL128(25,19)>, false], " +
                "cast([7: d_4, ARRAY<DECIMAL32(8,5)>, true] as ARRAY<DECIMAL128(25,19)>)); args: INVALID_TYPE; " +
                "result: ARRAY<DECIMAL128(25,19)>;");

        sql = "select array_intersect(d_3, d_6) from adec;";
        plan = getVerboseExplain(sql);
        assertCContains(plan, "array_intersect[([6: d_3, ARRAY<DECIMAL128(25,19)>, false], " +
                "cast([9: d_6, ARRAY<DECIMAL128(18,6)>, false] as ARRAY<DECIMAL128(31,19)>)); args: INVALID_TYPE; " +
                "result: ARRAY<DECIMAL128(31,19)>;");

        sql = "select array_intersect(d_1, i_1) from adec;";
        plan = getVerboseExplain(sql);
        assertCContains(plan, "10 <-> array_intersect[([4: d_1, ARRAY<DECIMAL128(26,2)>, false], " +
                "cast([2: i_1, ARRAY<INT>, false] as ARRAY<DECIMAL128(26,2)>)); args: INVALID_TYPE; " +
                "result: ARRAY<DECIMAL128(26,2)>;");

        sql = "select array_intersect(d_3, s_1) from adec;";
        plan = getVerboseExplain(sql);
        assertCContains(plan, "array_intersect[(cast([6: d_3, ARRAY<DECIMAL128(25,19)>, false] " +
                "as ARRAY<VARCHAR(65533)>), [3: s_1, ARRAY<VARCHAR(65533)>, true]); args: INVALID_TYPE; " +
                "result: ARRAY<VARCHAR(65533)>;");
    }

    @Test
    public void testArrayMinMaxDecimalType() throws Exception {
        String sql = "select array_min(d_1) from adec;";
        String plan = getVerboseExplain(sql);
        assertContains(plan, "array_min[([4: d_1, ARRAY<DECIMAL128(26,2)>, false]); " +
                "args: INVALID_TYPE; result: DECIMAL128(26,2)");

        sql = "select array_min(d_2) from adec;";
        plan = getVerboseExplain(sql);
        assertContains(plan, "array_min[([5: d_2, ARRAY<DECIMAL64(4,3)>, true]); " +
                "args: INVALID_TYPE; result: DECIMAL64(4,3);");

        sql = "select array_max(d_4) from adec;";
        plan = getVerboseExplain(sql);
        assertContains(plan, "array_max[([7: d_4, ARRAY<DECIMAL32(8,5)>, true]);" +
                " args: INVALID_TYPE; result: DECIMAL32(8,5);");

        sql = "select array_max(d_5) from adec;";
        plan = getVerboseExplain(sql);
        assertContains(plan, "array_max[([8: d_5, ARRAY<DECIMAL64(16,3)>, true]); " +
                "args: INVALID_TYPE; result: DECIMAL64(16,3);");
    }

    @Test
    public void testArraySumAvgDecimalType() throws Exception {
        String sql = "select array_sum(d_1) from adec;";
        String plan = getVerboseExplain(sql);
        assertContains(plan, "array_sum[([4: d_1, ARRAY<DECIMAL128(26,2)>, false]); " +
                "args: INVALID_TYPE; result: DECIMAL128(38,2);");

        sql = "select array_sum(d_2) from adec;";
        plan = getVerboseExplain(sql);
        assertContains(plan, "array_sum[([5: d_2, ARRAY<DECIMAL64(4,3)>, true]); " +
                "args: INVALID_TYPE; result: DECIMAL128(38,3);");

        sql = "select array_sum(d_3) from adec;";
        plan = getVerboseExplain(sql);
        assertContains(plan, "array_sum[(cast([6: d_3, ARRAY<DECIMAL128(25,19)>, false] as" +
                " ARRAY<DECIMAL128(38,18)>)); args: INVALID_TYPE; result: DECIMAL128(38,18);");

        sql = "select array_avg(d_4) from adec;";
        plan = getVerboseExplain(sql);
        assertContains(plan, "array_avg[([7: d_4, ARRAY<DECIMAL32(8,5)>, true]);" +
                " args: INVALID_TYPE; result: DECIMAL128(38,11);");

        sql = "select array_avg(d_5) from adec;";
        plan = getVerboseExplain(sql);
        assertContains(plan, "array_avg[([8: d_5, ARRAY<DECIMAL64(16,3)>, true]); " +
                "args: INVALID_TYPE; result: DECIMAL128(38,9);");

        sql = "select array_avg(d_6) from adec;";
        plan = getVerboseExplain(sql);
        assertContains(plan, "array_avg[([9: d_6, ARRAY<DECIMAL128(18,6)>, false]); " +
                "args: INVALID_TYPE; result: DECIMAL128(38,12);");
    }

    @Test
    public void testArrayDifferenceDecimalType() throws Exception {
        String sql = "select array_difference(d_1) from adec;";
        String plan = getVerboseExplain(sql);
        assertContains(plan, "array_difference[([4: d_1, ARRAY<DECIMAL128(26,2)>, false]); " +
                "args: INVALID_TYPE; result: ARRAY<DECIMAL128(38,2)>;");

        sql = "select array_difference(d_2) from adec;";
        plan = getVerboseExplain(sql);
        assertContains(plan, "array_difference[([5: d_2, ARRAY<DECIMAL64(4,3)>, true]); " +
                "args: INVALID_TYPE; result: ARRAY<DECIMAL64(18,3)>;");

        sql = "select array_difference(d_3) from adec;";
        plan = getVerboseExplain(sql);
        assertContains(plan, "array_difference[([6: d_3, ARRAY<DECIMAL128(25,19)>, false]); " +
                "args: INVALID_TYPE; result: ARRAY<DECIMAL128(38,19)>;");

        sql = "select array_difference(d_4) from adec;";
        plan = getVerboseExplain(sql);
        assertContains(plan, "array_difference[([7: d_4, ARRAY<DECIMAL32(8,5)>, true]); " +
                "args: INVALID_TYPE; result: ARRAY<DECIMAL64(18,5)>;");

        sql = "select array_difference(d_5) from adec;";
        plan = getVerboseExplain(sql);
        assertContains(plan, "array_difference[([8: d_5, ARRAY<DECIMAL64(16,3)>, true]); " +
                "args: INVALID_TYPE; result: ARRAY<DECIMAL64(18,3)>;");

        sql = "select array_difference(d_6) from adec;";
        plan = getVerboseExplain(sql);
        assertContains(plan, "array_difference[([9: d_6, ARRAY<DECIMAL128(18,6)>, false]); " +
                "args: INVALID_TYPE; result: ARRAY<DECIMAL128(38,6)>;");
    }

    @Test
    public void testArraysOverlapDecimalType() throws Exception {
        String sql = "select arrays_overlap(d_1, d_1) from adec;";
        String plan = getVerboseExplain(sql);
        assertContains(plan, "arrays_overlap[([4: d_1, ARRAY<DECIMAL128(26,2)>, false], " +
                "[4: d_1, ARRAY<DECIMAL128(26,2)>, false]); args: INVALID_TYPE,INVALID_TYPE; result: BOOLEAN;");

        sql = "select arrays_overlap(d_1, d_2) from adec;";
        plan = getVerboseExplain(sql);
        assertContains(plan, "arrays_overlap[(" +
                "cast([4: d_1, ARRAY<DECIMAL128(26,2)>, false] as ARRAY<DECIMAL128(27,3)>), " +
                "cast([5: d_2, ARRAY<DECIMAL64(4,3)>, true] as ARRAY<DECIMAL128(27,3)>));");

        sql = "select arrays_overlap(d_1, d_3) from adec;";
        plan = getVerboseExplain(sql);
        assertContains(plan, "arrays_overlap[" +
                "(cast([4: d_1, ARRAY<DECIMAL128(26,2)>, false] as ARRAY<DOUBLE>), " +
                "cast([6: d_3, ARRAY<DECIMAL128(25,19)>, false] as ARRAY<DOUBLE>));");

        sql = "select arrays_overlap(d_1, d_4) from adec;";
        plan = getVerboseExplain(sql);
        assertContains(plan, "arrays_overlap[(" +
                "cast([4: d_1, ARRAY<DECIMAL128(26,2)>, false] as ARRAY<DECIMAL128(29,5)>), " +
                "cast([7: d_4, ARRAY<DECIMAL32(8,5)>, true] as ARRAY<DECIMAL128(29,5)>));");

        sql = "select arrays_overlap(d_2, d_3) from adec;";
        plan = getVerboseExplain(sql);
        assertContains(plan, "arrays_overlap[" +
                "(cast([5: d_2, ARRAY<DECIMAL64(4,3)>, true] as ARRAY<DECIMAL128(25,19)>), " +
                "[6: d_3, ARRAY<DECIMAL128(25,19)>, false]);");

        sql = "select arrays_overlap(d_2, d_2) from adec;";
        plan = getVerboseExplain(sql);
        assertContains(plan, "arrays_overlap[([5: d_2, ARRAY<DECIMAL64(4,3)>, true], " +
                "[5: d_2, ARRAY<DECIMAL64(4,3)>, true]);");

        sql = "select arrays_overlap(d_6, d_6) from adec;";
        plan = getVerboseExplain(sql);
        assertContains(plan, "arrays_overlap[([9: d_6, ARRAY<DECIMAL128(18,6)>, false], " +
                "[9: d_6, ARRAY<DECIMAL128(18,6)>, false]);");

        sql = "select arrays_overlap(i_1, d_1) from adec;";
        plan = getVerboseExplain(sql);
        assertContains(plan, "arrays_overlap[" +
                "(cast([2: i_1, ARRAY<INT>, false] as ARRAY<DECIMAL128(26,2)>), " +
                "[4: d_1, ARRAY<DECIMAL128(26,2)>, false]);");

        sql = "select arrays_overlap(s_1, d_1) from adec;";
        plan = getVerboseExplain(sql);
        assertContains(plan, "arrays_overlap[(" +
                "[3: s_1, ARRAY<VARCHAR(65533)>, true], " +
                "cast([4: d_1, ARRAY<DECIMAL128(26,2)>, false] as ARRAY<VARCHAR(65533)>));");
    }

    @Test
    public void testArrayConcat() throws Exception {
        String sql = "select  array_concat(d_4, d_6) from adec;";
        String plan = getVerboseExplain(sql);
        assertCContains(plan, "array_concat[" +
                "(cast([7: d_4, ARRAY<DECIMAL32(8,5)>, true] as ARRAY<DECIMAL128(18,6)>), " +
                "[9: d_6, ARRAY<DECIMAL128(18,6)>, false]); args: INVALID_TYPE; result: ARRAY<DECIMAL128(18,6)>;");
    }

    @Test
    public void testArrayPolymorphicFunction() throws Exception {
        String sql = "select array_append(i_1, 1.0) from adec;";
        String plan = getVerboseExplain(sql);
        assertCContains(plan, "array_append[" +
                "(cast([2: i_1, ARRAY<INT>, false] as ARRAY<DECIMAL64(11,1)>), 1.0); args: INVALID_TYPE,DECIMAL64; " +
                "result: ARRAY<DECIMAL64(11,1)>;");

        sql = "select array_append(d_2, 1.0) from adec;";
        plan = getVerboseExplain(sql);
        assertCContains(plan, "array_append[([5: d_2, ARRAY<DECIMAL64(4,3)>, true], 1.0); " +
                "args: INVALID_TYPE,DECIMAL64; result: ARRAY<DECIMAL64(4,3)>;");

        sql = "select array_append(d_2, '1') from adec;";
        plan = getVerboseExplain(sql);
        assertCContains(plan, "array_append[" +
                "(cast([5: d_2, ARRAY<DECIMAL64(4,3)>, true] as ARRAY<VARCHAR>), '1'); " +
                "args: INVALID_TYPE,VARCHAR; result: ARRAY<VARCHAR>;");

        sql = "select array_append(s_1, 1.0) from adec;";
        plan = getVerboseExplain(sql);
        assertCContains(plan, "array_append[([3: s_1, ARRAY<VARCHAR(65533)>, true], '1.0');" +
                " args: INVALID_TYPE,VARCHAR; result: ARRAY<VARCHAR(65533)>;");

        sql = "select array_contains(i_1, 'a') from adec;";
        plan = getVerboseExplain(sql);
        assertCContains(plan, "array_contains[(cast([2: i_1, ARRAY<INT>, false] as ARRAY<VARCHAR>), 'a'); " +
                "args: INVALID_TYPE,VARCHAR;");

        sql = "select array_contains(s_1, 'a') from adec;";
        plan = getVerboseExplain(sql);
        assertCContains(plan, "array_contains[([3: s_1, ARRAY<VARCHAR(65533)>, true], 'a'); " +
                "args: INVALID_TYPE,VARCHAR; result: BOOLEAN;");

        sql = "select array_contains(d_1, 2) from adec;";
        plan = getVerboseExplain(sql);
        assertCContains(plan, "array_contains[([4: d_1, ARRAY<DECIMAL128(26,2)>, false], 2); " +
                "args: INVALID_TYPE,DECIMAL128; result: BOOLEAN;");

        sql = "select array_contains(d_4, '2') from adec;";
        plan = getVerboseExplain(sql);
        assertCContains(plan, "array_contains[" +
                "(cast([7: d_4, ARRAY<DECIMAL32(8,5)>, true] as ARRAY<VARCHAR>), '2'); args: INVALID_TYPE,VARCHAR;");

        sql = "select array_contains(d_4, v1) from adec;";
        plan = getVerboseExplain(sql);
        assertCContains(plan, "array_contains[" +
                "(cast([7: d_4, ARRAY<DECIMAL32(8,5)>, true] as ARRAY<DECIMAL128(24,5)>), " +
                "cast([1: v1, BIGINT, false] as DECIMAL128(24,5))); args: INVALID_TYPE,DECIMAL128;");

        sql = "select array_position(d_4, v1) from adec;";
        plan = getVerboseExplain(sql);
        assertCContains(plan, "array_position[" +
                "(cast([7: d_4, ARRAY<DECIMAL32(8,5)>, true] as ARRAY<DECIMAL128(24,5)>), " +
                "cast([1: v1, BIGINT, false] as DECIMAL128(24,5)));");

        sql = "select array_position(d_3, v1) from adec;";
        plan = getVerboseExplain(sql);
        assertCContains(plan, "array_position[([6: d_3, ARRAY<DECIMAL128(25,19)>, false], " +
                "cast([1: v1, BIGINT, false] as DECIMAL128(38,19)));");

        sql = "select array_sortby(d_3, s_1) from adec;";
        plan = getVerboseExplain(sql);
        assertCContains(plan, "array_sortby[" +
                "([6: d_3, ARRAY<DECIMAL128(25,19)>, false], [3: s_1, ARRAY<VARCHAR(65533)>, true]); " +
                "args: INVALID_TYPE,INVALID_TYPE; result: ARRAY<DECIMAL128(25,19)>;");

        sql = "select array_sortby(d_2, d_6) from adec;";
        plan = getVerboseExplain(sql);
        assertCContains(plan, "array_sortby[([5: d_2, ARRAY<DECIMAL64(4,3)>, true], " +
                "[9: d_6, ARRAY<DECIMAL128(18,6)>, false]); " +
                "args: INVALID_TYPE,INVALID_TYPE; result: ARRAY<DECIMAL64(4,3)>;");

        sql = "select array_sortby(d_1, d_5) from adec;";
        plan = getVerboseExplain(sql);
        assertCContains(plan, "array_sortby[([4: d_1, ARRAY<DECIMAL128(26,2)>, false], " +
                "[8: d_5, ARRAY<DECIMAL64(16,3)>, true]); " +
                "args: INVALID_TYPE,INVALID_TYPE; result: ARRAY<DECIMAL128(26,2)>;");

        sql = "select array_slice(d_1, 1) from adec;";
        plan = getVerboseExplain(sql);
        assertCContains(plan, "array_slice[([4: d_1, ARRAY<DECIMAL128(26,2)>, false], 1); " +
                "args: INVALID_TYPE,BIGINT; result: ARRAY<DECIMAL128(26,2)>;");

        sql = "select array_slice(d_2, 1, 3) from adec;";
        plan = getVerboseExplain(sql);
        assertCContains(plan, "array_slice[([5: d_2, ARRAY<DECIMAL64(4,3)>, true], 1, 3); " +
                "args: INVALID_TYPE,BIGINT,BIGINT; result: ARRAY<DECIMAL64(4,3)>;");

    }

    @Test
    public void testArrayAgg() throws Exception {
        String sql = "select  array_agg(i_1) from adec;";
        String plan = getVerboseExplain(sql);
        assertCContains(plan, "aggregate: array_agg[([2: i_1, ARRAY<INT>, false]); " +
                "args: INVALID_TYPE; result: ARRAY<ARRAY<INT>>;");
    }

    @Test
    public void testEmptyArrayOlap() throws Exception {
        String sql = "select arrays_overlap([[],[]],[])";
        String plan = getVerboseExplain(sql);
        assertCContains(plan, "arrays_overlap[([[],[]], []); " +
                "args: INVALID_TYPE,INVALID_TYPE; " +
                "result: BOOLEAN; args nullable: true; result nullable: true]");
    }
}
