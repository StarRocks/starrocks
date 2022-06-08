// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

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
                "  |  <slot 3> : CAST(ARRAY<tinyint(4)>[1,2,3][1] AS SMALLINT) + CAST(ARRAY<ARRAY<tinyint(4)>>[[1,2,3],[1,1,1]][2][2] AS SMALLINT)"));

        sql = "select v1, v3[1] + [1,2,3][1] as v, sum(v3[1]) from tarray group by v1, v order by v";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("2:AGGREGATE (update finalize)\n" +
                "  |  output: sum(5: expr)\n" +
                "  |  group by: 1: v1, 4: expr\n" +
                "  |  \n" +
                "  1:Project\n" +
                "  |  <slot 1> : 1: v1\n" +
                "  |  <slot 4> : 3: v3[1] + CAST(ARRAY<tinyint(4)>[1,2,3][1] AS BIGINT)\n" +
                "  |  <slot 5> : 3: v3[1]\n"));
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
        Assert.assertTrue(plan.contains("ARRAY<ARRAY<tinyint(4)>>[[1,2],[3,4]][1][2]"));
    }

    @Test
    public void testSelectArrayElement() throws Exception {
        String sql = "select [1,2][1]";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("ARRAY<tinyint(4)>[1,2][1]"));

        sql = "select [][1]";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("ARRAY<unknown type: NULL_TYPE>[][1]"));

        sql = "select [][1] from t0";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("ARRAY<unknown type: NULL_TYPE>[][1]"));

        sql = "select [][1] from (values(1,2,3), (4,5,6)) t";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("ARRAY<unknown type: NULL_TYPE>[][1]"));

        sql = "select [v1,v2] from t0";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("1:Project\n" +
                "  |  <slot 4> : ARRAY<bigint(20)>[1: v1,2: v2]"));

        sql = "select [v1 = 1, v2 = 2, true] from t0";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("<slot 4> : ARRAY<boolean>[1: v1 = 1,2: v2 = 2,TRUE]"));
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
        expectedEx.expectMessage("No matching function with signature: array_difference(int(11)).");
        getFragmentPlan(sql);
    }

    @Test
    public void testArrayDifferenceArgs3() throws Exception {
        String sql = "select array_difference(c1, 3) from test_array";
        expectedEx.expect(SemanticException.class);
        expectedEx.expectMessage(
                "No matching function with signature: array_difference(ARRAY<varchar(65533)>, tinyint(4)).");
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
                "select array_contains([v],1), array_contains([v],2) from (select v1+1 as v from t0,t1 group by v) t group by 1,2";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  8:Project\n" +
                "  |  <slot 8> : array_contains(ARRAY<bigint(20)>[7: expr], 1)\n" +
                "  |  <slot 9> : array_contains(ARRAY<bigint(20)>[7: expr], 2)"));
    }

    @Test
    public void testArrayWindowFunction() throws Exception {
        for (String fnName : Sets.newHashSet(AnalyticExpr.LASTVALUE, AnalyticExpr.FIRSTVALUE)) {
            String sql = String.format("select %s(v3) over() from tarray", fnName.toLowerCase());
            expectedEx.expect(SemanticException.class);
            expectedEx.expectMessage(
                    String.format("No matching function with signature: %s(ARRAY<bigint(20)>)", fnName.toLowerCase()));
            getFragmentPlan(sql);
        }
    }
}
