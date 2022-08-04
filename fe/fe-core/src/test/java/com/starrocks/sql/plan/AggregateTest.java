// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.plan;

import com.starrocks.common.FeConstants;
import org.junit.Assert;
import org.junit.Test;

public class AggregateTest extends PlanTestBase {
    @Test
    public void testGroupByConstantWithAggPrune() throws Exception {
        connectContext.getSessionVariable().setNewPlanerAggStage(4);
        FeConstants.runningUnitTest = true;
        String sql = "select count(distinct L_ORDERKEY) from lineitem group by 1.0001";
        String plan = getFragmentPlan(sql);
        assertContains(plan, " 4:AGGREGATE (update serialize)\n" +
                "  |  STREAMING\n" +
                "  |  output: count(1: L_ORDERKEY)\n" +
                "  |  group by: 18: expr\n" +
                "  |  \n" +
                "  3:Project\n" +
                "  |  <slot 1> : 1: L_ORDERKEY\n" +
                "  |  <slot 18> : 1.0001\n" +
                "  |  \n" +
                "  2:AGGREGATE (update serialize)\n" +
                "  |  group by: 1: L_ORDERKEY");

        sql = "select count(distinct L_ORDERKEY) from lineitem group by 1.0001, 2.0001";
        plan = getFragmentPlan(sql);
        assertContains(plan, "4:AGGREGATE (update serialize)\n" +
                "  |  STREAMING\n" +
                "  |  output: count(1: L_ORDERKEY)\n" +
                "  |  group by: 18: expr\n" +
                "  |  \n" +
                "  3:Project\n" +
                "  |  <slot 1> : 1: L_ORDERKEY\n" +
                "  |  <slot 18> : 1.0001\n" +
                "  |  \n" +
                "  2:AGGREGATE (update serialize)\n" +
                "  |  group by: 1: L_ORDERKEY");

        sql = "select count(distinct L_ORDERKEY), count(L_PARTKEY) from lineitem group by 1.0001";
        plan = getFragmentPlan(sql);
        assertContains(plan, "4:AGGREGATE (update serialize)\n" +
                "  |  STREAMING\n" +
                "  |  output: count(1: L_ORDERKEY), count(20: count)\n" +
                "  |  group by: 18: expr\n" +
                "  |  \n" +
                "  3:Project\n" +
                "  |  <slot 1> : 1: L_ORDERKEY\n" +
                "  |  <slot 18> : 1.0001\n" +
                "  |  <slot 20> : 20: count\n" +
                "  |  \n" +
                "  2:AGGREGATE (update serialize)\n" +
                "  |  output: count(2: L_PARTKEY)\n" +
                "  |  group by: 1: L_ORDERKEY");
        FeConstants.runningUnitTest = false;
        connectContext.getSessionVariable().setNewPlanerAggStage(0);
    }

    @Test
    public void testAggregateDuplicatedExprs() throws Exception {
        String plan = getFragmentPlan("SELECT " +
                "sum(arrays_overlap(v3, [1])) as q1, " +
                "sum(arrays_overlap(v3, [1])) as q2, " +
                "sum(arrays_overlap(v3, [1])) as q3 FROM tarray;");
        assertContains(plan, "  2:AGGREGATE (update finalize)\n" +
                "  |  output: sum(4: arrays_overlap)\n" +
                "  |  group by: \n" +
                "  |  \n" +
                "  1:Project\n" +
                "  |  <slot 4> : arrays_overlap(3: v3, CAST(ARRAY<tinyint(4)>[1] AS ARRAY<BIGINT>))\n" +
                "  |  \n" +
                "  0:OlapScanNode");
    }

    @Test
    public void testOuterJoinSatisfyAgg() throws Exception {
        connectContext.getSessionVariable().setNewPlanerAggStage(1);
        String sql = "select distinct t0.v1  from t0 full outer join[shuffle] t1 on t0.v1 = t1.v4;";
        String plan = getFragmentPlan(sql);
        System.out.println(plan);
        assertContains(plan, "  7:AGGREGATE (update finalize)\n" +
                "  |  group by: 1: v1\n" +
                "  |  \n" +
                "  6:EXCHANGE");
        connectContext.getSessionVariable().setNewPlanerAggStage(0);
    }

    @Test
    public void testLocalAggregateWithMultiStage() throws Exception {
        FeConstants.runningUnitTest = true;
        connectContext.getSessionVariable().setNewPlanerAggStage(2);
        String sql = "select distinct L_ORDERKEY from lineitem_partition_colocate where L_ORDERKEY = 59633893 ;";
        ExecPlan plan = getExecPlan(sql);
        Assert.assertTrue(plan.getFragments().get(1).getPlanRoot().isColocate());

        connectContext.getSessionVariable().setNewPlanerAggStage(3);
        sql = "select count(distinct L_ORDERKEY) " +
                "from lineitem_partition_colocate where L_ORDERKEY = 59633893 group by L_ORDERKEY;";
        plan = getExecPlan(sql);
        Assert.assertTrue(plan.getFragments().get(1).getPlanRoot().getChild(0).isColocate());

        sql = "select count(distinct L_ORDERKEY) from lineitem_partition_colocate";
        plan = getExecPlan(sql);
        Assert.assertTrue(plan.getFragments().get(1).getPlanRoot().getChild(0).isColocate());

        sql = "select count(*) from lineitem_partition_colocate";
        plan = getExecPlan(sql);
        Assert.assertFalse(plan.getFragments().get(1).getPlanRoot().isColocate());

        connectContext.getSessionVariable().setNewPlanerAggStage(2);
        sql = "select count(distinct L_ORDERKEY) " +
                "from lineitem_partition_colocate where L_ORDERKEY = 59633893 group by L_ORDERKEY;";
        plan = getExecPlan(sql);
        Assert.assertTrue(plan.getFragments().get(1).getPlanRoot().getChild(0).isColocate());

        connectContext.getSessionVariable().setNewPlanerAggStage(0);
        FeConstants.runningUnitTest = false;
    }
}
