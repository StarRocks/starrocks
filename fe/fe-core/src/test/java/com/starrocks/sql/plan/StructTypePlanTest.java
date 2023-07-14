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

import com.starrocks.common.FeConstants;
import com.starrocks.utframe.StarRocksAssert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class StructTypePlanTest extends PlanTestBase {

    @BeforeClass
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        StarRocksAssert starRocksAssert = new StarRocksAssert(connectContext);
        FeConstants.runningUnitTest = true;
        starRocksAssert.withTable("create table test(c0 INT, " +
                "c1 struct<a int, b array<struct<a int, b int>>>," +
                "c2 struct<a int, b int>," +
                "c3 struct<a int, b int, c struct<a int, b int>, d array<int>>) " +
                "duplicate key(c0) distributed by hash(c0) buckets 1 " +
                "properties('replication_num'='1');");
        starRocksAssert.withTable("create table test1(c0 INT, " +
                "c1 struct<a int, b array<struct<a int, b int>>>," +
                "c2 struct<a int, b int>," +
                "c2_0 struct<a int, b varchar(10)>, " +
                "c3 struct<a int, b int, c struct<a int, b int>, d array<int>>) " +
                "duplicate key(c0) distributed by hash(c0) buckets 1 " +
                "properties('replication_num'='1');");
        FeConstants.runningUnitTest = false;
    }

    @Before
    public void setup() throws Exception {
        connectContext.getSessionVariable().setEnablePruneComplexTypes(true);
        connectContext.getSessionVariable().setCboPruneSubfield(true);
    }

    @Test
    public void testStruct() throws Exception {
        String sql = "select * from test1 union all select * from test1";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "0:UNION\n" +
                "  |  \n" +
                "  |----4:EXCHANGE\n" +
                "  |    \n" +
                "  2:EXCHANGE");
        sql = "select c2 from test1 union all select c2_0 from test1";
        plan = getFragmentPlan(sql);
        assertContains(plan, "CAST(3: c2 AS struct<a int(11), b varchar(10)>)");
    }

    @Test
    public void testSubfield() throws Exception {
        String sql = "select c1.a from test";
        assertVerbosePlanContains(sql, "[/c1/a]");

        connectContext.getSessionVariable().setEnablePruneComplexTypes(false);
        assertVerbosePlanContains(sql, "Pruned type: 2 <-> [STRUCT<a int(11), b ARRAY<STRUCT<a int(11), b int(11)>>>]");
    }

    @Test
    public void testStructMultiSelect() throws Exception {
        String sql = "select c2.a, c2.b from test";
        assertVerbosePlanContains(sql, "Pruned type: 3 <-> [STRUCT<a int(11), b int(11)>]");
    }

    @Test
    public void testSubfieldWindow() throws Exception {
        String sql = "select c3.a, row_number() over (partition by c0 order by c2.a) from test";
        assertVerbosePlanContains(sql, "[/c2/a, /c3/a]");
    }

    @Test
    public void testSelectArrayStruct() throws Exception {
        String sql = "select c1.b[10].a from test";
        assertVerbosePlanContains(sql, "[/c1/b]");
    }

    @Test
    public void testJoinOn() throws Exception {
        String sql = "select t1.c0 from test as t1 join test as t2 on t1.c2.a=t2.c2.a";
        assertVerbosePlanContains(sql, "[/c2/a]");
    }

    @Test
    public void testAggregate() throws Exception {
        String sql = "select sum(c2.a) as t from test";
        assertVerbosePlanContains(sql, "[/c2/a]");

        sql = "select sum(c2.b) as t from test group by c2.a";
        assertVerbosePlanContains(sql, "Pruned type: 3 <-> [STRUCT<a int(11), b int(11)>]");

        sql = "select count(c1.b[10].a) from test";
        assertVerbosePlanContains(sql, "[/c1/b]");

        sql = "select count(c3.c.b) from test group by c1.a, c2.b";
        assertVerbosePlanContains(sql, "[/c1/a, /c2/b, /c3/c/b]");

        sql = "select sum(c3.c.b + c2.a) as t from test group by c1.a, c2.b";
        assertVerbosePlanContains(sql, "[/c1/a, /c2/a, /c2/b, /c3/c/b]");
    }

    @Test
    public void testSelectPredicate() throws Exception {
        String sql = "select c0 from test where (c0+c2.a)>5";
        assertVerbosePlanContains(sql, "[/c2/a]");
    }

    @Test
    public void testSelectStar() throws Exception {
        String sql = "select *, c1.b[10].a from test";
        assertVerbosePlanContains(sql, "Pruned type: 2 <-> [STRUCT<a int(11), b ARRAY<STRUCT<a int(11), b int(11)>>>]");
        assertVerbosePlanContains(sql, "Pruned type: 3 <-> [STRUCT<a int(11), b int(11)>]");
        assertVerbosePlanContains(sql,
                "Pruned type: 4 <-> [STRUCT<a int(11), b int(11), c STRUCT<a int(11), b int(11)>, d ARRAY<int(11)>>]");
    }

    @Test
    public void testSubQuery() throws Exception {
        String sql = "select c2.a from (select c1, c2 from test) t";
        assertVerbosePlanContains(sql, "[/c2/a]");
        sql =
                "select t1.a, rn from (select c3.c as t1, row_number() over (partition by c0 order by c2.a) as rn from test) as t";
        assertVerbosePlanContains(sql, "[/c2/a, /c3/c/a]");
    }

    @Test
    public void testStructWithWindow() throws Exception {
        String sql = "select sum(c2.b) over(partition by c2.a order by c0) from test";
        assertPlanContains(sql, " 3:ANALYTIC\n" +
                "  |  functions: [, sum(7: c2.b), ]\n" +
                "  |  partition by: 9: c2.a\n" +
                "  |  order by: 1: c0 ASC\n" +
                "  |  window: RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW");

        sql = "select sum(c0) over(partition by c2.a order by c2.b) from test";
        assertPlanContains(sql, " 3:ANALYTIC\n" +
                "  |  functions: [, sum(5: c0), ]\n" +
                "  |  partition by: 9: c2.a\n" +
                "  |  order by: 10: c2.b ASC\n" +
                "  |  window: RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW");

        sql = "select sum(c1.b[10].b) over(partition by c2.a order by c2.b) from test";
        assertPlanContains(sql, "3:ANALYTIC\n" +
                "  |  functions: [, sum(6: c1.b[10].b), ]\n" +
                "  |  partition by: 9: c2.a\n" +
                "  |  order by: 10: c2.b ASC\n" +
                "  |  window: RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW");
    }

    @Test
    public void testExcept() throws Exception {
        String sql = "select c2.a + 1 from test except select c3.a from test";
        assertVerbosePlanContains(sql, "[/c3/a]");
        assertVerbosePlanContains(sql, "[/c2/a]");
    }

    @Test
    public void testIntersect() throws Exception {
        String sql = "select c2.a + 1 from test intersect select c3.a from test";
        assertVerbosePlanContains(sql, "[/c3/a]");
        assertVerbosePlanContains(sql, "[/c2/a]");
    }

    @Test
    public void testLambda() throws Exception {
        String sql = "select array_map(x->x, [])";
        assertVerbosePlanContains(sql, "[]");
        sql = "select array_map(x->x+1, c3.d) from test";
        assertVerbosePlanContains(sql, "[/c3/d]");
        sql = "select array_sortby(x->x+1, c3.d) from test";
        assertVerbosePlanContains(sql, "[/c3/d]");
        sql = "select array_filter((x,y) -> x<y, c3.d, c3.d) from test";
        assertVerbosePlanContains(sql, "[/c3/d]");
        sql = "select map_values(col_map), map_keys(col_map) from (select map_from_arrays([],[]) as col_map)A";
        assertPlanContains(sql, "[], []");
    }

    @Test
    public void testCast() throws Exception {
        String sql = "select cast(row(null, null, null) as STRUCT<a int, b MAP<int, int>, c ARRAY<INT>>); ";
        String plan = getVerboseExplain(sql);
        assertContains(plan, "cast(row[(NULL, NULL, NULL); args: BOOLEAN,BOOLEAN,BOOLEAN; " +
                "result: struct<col1 boolean, col2 boolean, col3 boolean>; args nullable: true; result nullable: true] " +
                "as struct<a int(11), b map<int(11),int(11)>, c array<int(11)>>)");
    }
}
