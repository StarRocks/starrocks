// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.plan;

import org.junit.Assert;
import org.junit.Test;

public class MergeJoinTest extends PlanTestBase {


    @Test
    public void testInnerJoinWithPredicate() throws Exception {
        PlanTestBase.connectContext.getSessionVariable().setJoinImplementationMode("merge");
        String sql = "SELECT * from t0 join test_all_type on t0.v1 = test_all_type.t1d where t0.v1 = 1;";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("PREDICATES: 1: v1 = 1"));
    }

    @Test
    public void testJoinColumnsPrune() throws Exception {
        PlanTestBase.connectContext.getSessionVariable().setJoinImplementationMode("merge");
        String sql = " select count(a.v3) from t0 a join t0 b on a.v3 = b.v3;";
        getFragmentPlan(sql);

        sql = " select a.v2 from t0 a join t0 b on a.v3 = b.v3;";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("Project\n"
                + "  |  <slot 2> : 2: v2"));
    }

    @Test
    public void testCorssJoinWithPredicate() throws Exception {
        PlanTestBase.connectContext.getSessionVariable().setJoinImplementationMode("merge");
        String sql = "SELECT * from t0 join test_all_type where t0.v1 = 2;";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("PREDICATES: 1: v1 = 2"));
    }

}
