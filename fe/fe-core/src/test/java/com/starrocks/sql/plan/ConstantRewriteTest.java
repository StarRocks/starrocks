// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.plan;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class ConstantRewriteTest extends PlanTestBase {
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testRewriteAgg() throws Exception {
        String sql = "select v1,sum(v3) from t0 where v1 = 1 group by v1";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "group by: 1: v1");

        sql = "select v1,v2,sum(v3) from t0 where v1 = 1 group by v1,v2";
        plan = getFragmentPlan(sql);
        assertContains(plan, "2:Project\n" +
                "  |  <slot 1> : 1\n" +
                "  |  <slot 2> : 2: v2\n" +
                "  |  <slot 4> : 4: sum\n" +
                "  |  \n" +
                "  1:AGGREGATE (update finalize)\n" +
                "  |  output: sum(3: v3)\n" +
                "  |  group by: 2: v2");

        sql = "select v1,v2,sum(v3) from t0 where v2 = 1 group by v1,v2";
        plan = getFragmentPlan(sql);
        assertContains(plan, "2:Project\n" +
                "  |  <slot 1> : 1: v1\n" +
                "  |  <slot 2> : 1\n" +
                "  |  <slot 4> : 4: sum\n" +
                "  |  \n" +
                "  1:AGGREGATE (update finalize)\n" +
                "  |  output: sum(3: v3)\n" +
                "  |  group by: 1: v1");

        sql = "select v1,v2,sum(v3) from t0 where v2 = 2 and v1 = 1 group by v1,v2";
        plan = getFragmentPlan(sql);
        assertContains(plan, "2:Project\n" +
                "  |  <slot 1> : 1: v1\n" +
                "  |  <slot 2> : 2\n" +
                "  |  <slot 4> : 4: sum\n" +
                "  |  \n" +
                "  1:AGGREGATE (update finalize)\n" +
                "  |  output: sum(3: v3)\n" +
                "  |  group by: 1: v1");
    }

    @Test
    public void testRewriteSort() throws Exception {
        String sql = "select v1, v2 from t0 where v1 = 1 order by v1";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "PLAN FRAGMENT 0\n" +
                " OUTPUT EXPRS:1: v1 | 2: v2\n" +
                "  PARTITION: RANDOM\n" +
                "\n" +
                "  RESULT SINK\n" +
                "\n" +
                "  0:OlapScanNode\n" +
                "     TABLE: t0\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: 1: v1 = 1\n" +
                "     partitions=0/1\n" +
                "     rollup: t0\n" +
                "     tabletRatio=0/0\n" +
                "     tabletList=\n" +
                "     cardinality=1\n" +
                "     avgRowSize=2.0\n" +
                "     numNodes=0");

        sql = "select v1, v2 from t0 where v1 = 1 order by v1, v2";
        plan = getFragmentPlan(sql);
        assertContains(plan, "order by: <slot 2> 2: v2 ASC");

        sql = "select v1, v2 from t0 where v1 = 1 order by v2, v1";
        plan = getFragmentPlan(sql);
        assertContains(plan, "order by: <slot 2> 2: v2 ASC");
    }

    @Test
    public void testOuterJoin() throws Exception {
        //Assert v1 = 1 push down to Join right child t1
        String sql = "select v1, v2 from (select v1, v2 from t0 where v1 = 1) a inner join " +
                "(select v4, v5 from t1) b on a.v1 = b.v4";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "PREDICATES: 4: v4 IS NOT NULL, 4: v4 = 1");

        //Assert v4 = 4 push down to Join left child t0
        sql = "select v1, v2 from (select v1, v2 from t0) a inner join " +
                "(select v4, v5 from t1 where v4 = 4) b on a.v1 = b.v4";
        plan = getFragmentPlan(sql);
        assertContains(plan, "PREDICATES: 1: v1 IS NOT NULL, 1: v1 = 4");

        //Assert v4 = 4 can't push down to Join left child t0 where op is left outer
        sql = "select v1, v2 from (select v1, v2 from t0) a left outer join " +
                "(select v4, v5 from t1 where v4 = 4) b on a.v1 = b.v4";
        plan = getFragmentPlan(sql);
        assertContains(plan, "0:OlapScanNode\n" +
                "     TABLE: t0\n" +
                "     PREAGGREGATION: ON\n" +
                "     partitions=0/1\n" +
                "     rollup: t0\n" +
                "     tabletRatio=0/0\n" +
                "     tabletList=\n" +
                "     cardinality=1\n" +
                "     avgRowSize=2.0\n" +
                "     numNodes=0");

        //Assert v4 = 4 can push down to Join left child t0 where op is right outer
        sql = "select v1, v2 from (select v1, v2 from t0) a right outer join " +
                "(select v4, v5 from t1 where v4 = 4) b on a.v1 = b.v4";
        plan = getFragmentPlan(sql);
        assertContains(plan, "PREDICATES: 1: v1 = 4");

        //Assert v4 = 4 can't push down to Join left child t0 where op is left outer
        sql = "select v1, v2 from (select v1, v2 from t0 where v1 = 1) a left outer join " +
                "(select v4, v5 from t1) b on a.v1 = b.v4";
        plan = getFragmentPlan(sql);
        assertContains(plan, "PREDICATES: 4: v4 = 1");

        //Assert v4 = 4 can push down to Join left child t0 where op is right outer
        sql = "select v1, v2 from (select v1, v2 from t0 where v1 = 1) a right outer join " +
                "(select v4, v5 from t1) b on a.v1 = b.v4";
        plan = getFragmentPlan(sql);
        assertContains(plan, "2:OlapScanNode\n" +
                "     TABLE: t1\n" +
                "     PREAGGREGATION: ON\n" +
                "     partitions=0/1\n" +
                "     rollup: t1\n" +
                "     tabletRatio=0/0\n" +
                "     tabletList=\n" +
                "     cardinality=1\n" +
                "     avgRowSize=1.0\n" +
                "     numNodes=0");

        //Assert v1 = 1 push down to right when op is left semi
        sql = "select v1, v2 from (select v1, v2 from t0 where v1 = 1) a left semi join " +
                "(select v4, v5 from t1) b on a.v1 = b.v4";
        plan = getFragmentPlan(sql);
        assertContains(plan, "PREDICATES: 4: v4 IS NOT NULL, 4: v4 = 1");

        sql = "select v1, v2 from (select v1, v2 from t0) a left semi join " +
                "(select v4, v5 from t1 where v4 = 4) b on a.v1 = b.v4";
        plan = getFragmentPlan(sql);
        assertContains(plan, "PREDICATES: 1: v1 = 4");

        //Assert v4 = 4 push down to Join left child t0
        sql = "select v4, v5 from (select v1, v2 from t0) a right semi join " +
                "(select v4, v5 from t1 where v4 = 4) b on a.v1 = b.v4";
        plan = getFragmentPlan(sql);
        assertContains(plan, "PREDICATES: 1: v1 IS NOT NULL, 1: v1 = 4");

        sql = "select v4, v5 from (select v1, v2 from t0 where v1 = 1) a right semi join " +
                "(select v4, v5 from t1) b on a.v1 = b.v4";
        plan = getFragmentPlan(sql);
        assertContains(plan, "PREDICATES: 4: v4 = 1");


        //Assert v1 = 1 push down to right when op is left anti
        sql = "select v1, v2 from (select v1, v2 from t0 where v1 = 1) a left anti join " +
                "(select v4, v5 from t1) b on a.v1 = b.v4";
        plan = getFragmentPlan(sql);
        assertContains(plan, "PREDICATES: 4: v4 = 1");

        sql = "select v1, v2 from (select v1, v2 from t0) a left anti join " +
                "(select v4, v5 from t1 where v4 = 4) b on a.v1 = b.v4";
        plan = getFragmentPlan(sql);
        assertContains(plan, "0:OlapScanNode\n" +
                "     TABLE: t0\n" +
                "     PREAGGREGATION: ON\n" +
                "     partitions=0/1\n" +
                "     rollup: t0\n" +
                "     tabletRatio=0/0\n" +
                "     tabletList=\n" +
                "     cardinality=1\n" +
                "     avgRowSize=2.0\n" +
                "     numNodes=0");

        //Assert v4 = 4 push down to Join left child t0
        sql = "select v4, v5 from (select v1, v2 from t0) a right anti join " +
                "(select v4, v5 from t1 where v4 = 4) b on a.v1 = b.v4";
        plan = getFragmentPlan(sql);
        assertContains(plan, "PREDICATES: 1: v1 = 4");

        sql = "select v4, v5 from (select v1, v2 from t0 where v1 = 1) a right anti join " +
                "(select v4, v5 from t1) b on a.v1 = b.v4";
        plan = getFragmentPlan(sql);
        assertContains(plan, "0:OlapScanNode\n" +
                "     TABLE: t1\n" +
                "     PREAGGREGATION: ON\n" +
                "     partitions=0/1\n" +
                "     rollup: t1\n" +
                "     tabletRatio=0/0\n" +
                "     tabletList=\n" +
                "     cardinality=1\n" +
                "     avgRowSize=2.0\n" +
                "     numNodes=0");
    }

    @Test
    public void test() throws Exception {
        connectContext.getSessionVariable().setCboCteReuse(false);

        String sql = "select v1, v2 in (select v5 + v4 from t1 where v4 = 1 and abs(v1 + v4) = v1 + v5 ) from t0";
        String plan = getFragmentPlan(sql);
        System.out.println(plan);
    }
}
