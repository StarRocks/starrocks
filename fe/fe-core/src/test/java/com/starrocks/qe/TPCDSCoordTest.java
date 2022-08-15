// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.qe;

import com.starrocks.common.FeConstants;
import com.starrocks.planner.PlanFragmentId;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.sql.plan.TPCDSPlanTest;
import com.starrocks.sql.plan.TPCDSPlanTestBase;
import com.starrocks.thrift.TUniqueId;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class TPCDSCoordTest extends TPCDSPlanTestBase {

    @BeforeClass
    public static void beforeClass() throws Exception {
        TPCDSPlanTest.beforeClass();
    }

    @AfterClass
    public static void afterClass() {
    }

    @After
    public void tearDown() {
        ConnectContext ctx = starRocksAssert.getCtx();
        FeConstants.runningUnitTest = false;
        ctx.getSessionVariable().setEnablePipelineEngine(true);
    }

    @Test
    public void testQuery20() throws Exception {
        FeConstants.runningUnitTest = true;
        ConnectContext ctx = starRocksAssert.getCtx();
        ctx.setExecutionId(new TUniqueId(0x33, 0x0));
        ConnectContext.threadLocalInfo.set(ctx);
        ctx.getSessionVariable().setParallelExecInstanceNum(8);
        ctx.getSessionVariable().setEnablePipelineEngine(false);
        setTPCDSFactor(1);

        // make sure global runtime filter been push-downed to two fragments.
        String sql =
                "select * from (select a.inv_item_sk as x, b.inv_warehouse_sk from inventory a join inventory b on a.inv_item_sk = b.inv_item_sk ) t1 join [shuffle] item t0  on t0.i_item_sk = t1.x;";
        String plan = UtFrameUtils.getVerboseFragmentPlan(ctx, sql);
        String[] ss = plan.split("\\n");
        List<String> fragments = new ArrayList<>();
        String currentFragment = null;
        for (String s : ss) {
            if (s.indexOf("PLAN FRAGMENT") != -1) {
                currentFragment = s;
            }
            if (s.indexOf("filter_id = 1") != -1) {
                if (fragments.size() == 0 || !fragments.get(fragments.size() - 1).equals(currentFragment)) {
                    fragments.add(currentFragment);
                }
            }
        }
        // 1 fragment to generate filter(1)
        // 2 fragements to consumer filter(1)
        Assert.assertEquals(fragments.size(), 3);

        System.out.println(plan);
        ExecPlan execPlan = UtFrameUtils.getPlanAndFragment(ctx, sql).second;
        Coordinator coord = new Coordinator(ctx, execPlan.getFragments(), execPlan.getScanNodes(),
                execPlan.getDescTbl().toThrift());
        coord.prepareExec();

        PlanFragmentId topFragmentId = coord.getFragments().get(0).getFragmentId();
        Coordinator.FragmentExecParams params = coord.getFragmentExecParamsMap().get(topFragmentId);
        Assert.assertEquals(params.runtimeFilterParams.id_to_prober_params.get(1).size(), 10);
    }
}
