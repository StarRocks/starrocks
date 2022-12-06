// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.analyzer;

import com.starrocks.common.Config;
import com.starrocks.common.Pair;
import com.starrocks.sql.ast.CreateMaterializedViewStatement;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.sql.plan.PlanTestBase;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class MaterializedViewPlanTest extends PlanTestBase {

    private boolean enableExperimentMV = false;

    @Before
    public void before() {
        connectContext.getSessionVariable().setEnableIncrementalRefreshMv(true);
        enableExperimentMV = Config.enable_experimental_mv;
        Config.enable_experimental_mv = true;
    }

    @After
    public void after() {
        connectContext.getSessionVariable().setEnableIncrementalRefreshMv(false);
        Config.enable_experimental_mv = enableExperimentMV;
    }

    @Test
    public void testCreateIncrementalMV() throws Exception {
        String sql = "create materialized view rtmv \n" +
                "distributed by hash(v1) " +
                "refresh incremental as " +
                "select v1, count(*) as cnt from t0 join t1 on t0.v1 = t1.v4 group by v1";

        Pair<CreateMaterializedViewStatement, ExecPlan> pair = UtFrameUtils.planMVMaintenance(connectContext, sql);
        String plan = UtFrameUtils.printPlan(pair.second);
        Assert.assertEquals("- Output => [1:v1, 7:count]\n" +
                "    - StreamAgg[1:v1]\n" +
                "            Estimates: {row: 1, cpu: ?, memory: ?, network: ?, cost: 1.0}\n" +
                "            7:count := count()\n" +
                "        - StreamJoin/INNER JOIN [1:v1 = 4:v4] => [1:v1]\n" +
                "                Estimates: {row: 1, cpu: ?, memory: ?, network: ?, cost: 1.0}\n" +
                "            - SCAN [t0] => [1:v1]\n" +
                "                    Estimates: {row: 1, cpu: ?, memory: ?, network: ?, cost: 0.5}\n" +
                "                    partitionRatio: 0/1, tabletRatio: 0/0\n" +
                "                    predicate: 1:v1 IS NOT NULL\n" +
                "            - SCAN [t1] => [4:v4]\n" +
                "                    Estimates: {row: 1, cpu: ?, memory: ?, network: ?, cost: 0.5}\n" +
                "                    partitionRatio: 0/1, tabletRatio: 0/0\n" +
                "                    predicate: 4:v4 IS NOT NULL\n", plan);
    }
}
