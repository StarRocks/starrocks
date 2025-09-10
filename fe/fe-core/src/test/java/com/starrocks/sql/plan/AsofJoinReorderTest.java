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

import com.starrocks.catalog.OlapTable;
import com.starrocks.common.FeConstants;
import com.starrocks.server.GlobalStateMgr;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class AsofJoinReorderTest extends PlanTestBase {
    @BeforeAll
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();

        GlobalStateMgr globalStateMgr = connectContext.getGlobalStateMgr();

        OlapTable t0 = (OlapTable) globalStateMgr.getLocalMetastore().getDb("test").getTable("t0");
        setTableStatistics(t0, 1);

        OlapTable t1 = (OlapTable) globalStateMgr.getLocalMetastore().getDb("test").getTable("t1");
        setTableStatistics(t1, 10);
        OlapTable t2 = (OlapTable) globalStateMgr.getLocalMetastore().getDb("test").getTable("t2");
        setTableStatistics(t2, 100000);

        OlapTable t3 = (OlapTable) globalStateMgr.getLocalMetastore().getDb("test").getTable("t3");
        setTableStatistics(t3, 1000000000);
        connectContext.getSessionVariable().setMaxTransformReorderJoins(2);
        FeConstants.runningUnitTest = true;
    }

    @Test
    public void testAsofLeftJoin() throws Exception {
        String sql = "select t1.* from t0 asof join t1 on v1 > v4 and v2 = v5 left join t2 on v1 < v7 ";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "6:HASH JOIN\n" +
                "  |  join op: ASOF INNER JOIN (PARTITIONED)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 2: v2 = 5: v5\n" +
                "  |  asof join conjunct: 1: v1 > 4: v4\n" +
                "  |  \n" +
                "  |----5:EXCHANGE\n" +
                "  |    \n" +
                "  3:EXCHANGE");
        assertContains(plan, "EXCHANGE ID: 03\n" +
                "    HASH_PARTITIONED: 2: v2\n" +
                "\n" +
                "  2:OlapScanNode\n" +
                "     TABLE: t0");

        assertContains(plan, "EXCHANGE ID: 05\n" +
                "    HASH_PARTITIONED: 5: v5\n" +
                "\n" +
                "  4:OlapScanNode\n" +
                "     TABLE: t1");

        assertContains(plan, "9:NESTLOOP JOIN\n" +
                "  |  join op: RIGHT OUTER JOIN\n" +
                "  |  colocate: false, reason: \n" +
                "  |  other join predicates: 1: v1 < 7: v7\n" +
                "  |  \n" +
                "  |----8:EXCHANGE\n" +
                "  |    \n" +
                "  1:EXCHANGE");
    }

    @Test
    void testLeftJoinReorderGreedy() throws Exception {
        connectContext.getSessionVariable().disableDPJoinReorder();
        String sql = "select v6 from t1 " +
                "left join (select t1.v5 from t1 asof join t3 on t1.v4 = t3.v10 and t1.v5 > t3.v11 join t0 join t2) a " +
                "on t1.v6 = a.v5";
        String planFragment = getFragmentPlan(sql);
        Assertions.assertTrue(planFragment.contains("5:HASH JOIN\n" +
                "  |  join op: ASOF INNER JOIN (BUCKET_SHUFFLE)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 4: v4 = 7: v10\n" +
                "  |  asof join conjunct: 5: v5 > 8: v11\n" +
                "  |  \n" +
                "  |----4:EXCHANGE\n" +
                "  |    \n" +
                "  2:OlapScanNode\n" +
                "     TABLE: t1"));
    }

    @Test
    void testInnerJoinReorderDP() throws Exception {
        connectContext.getSessionVariable().enableDPJoinReorder();
        String sql = "select * from t1 " +
                "asof join t3 on t1.v4 = t3.v10 and t1.v5 > t3.v11 " +
                "asof join t0 on t1.v4 = t0.v2 and t1.v5 > t0.v2 " +
                "join t2 on t1.v5 = t2.v8 ";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "7:HASH JOIN\n" +
                "  |  join op: ASOF INNER JOIN (BUCKET_SHUFFLE)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 1: v4 = 8: v2\n" +
                "  |  asof join conjunct: 2: v5 > 8: v2\n" +
                "  |  \n" +
                "  |----6:EXCHANGE\n" +
                "  |    \n" +
                "  4:HASH JOIN\n" +
                "  |  join op: ASOF INNER JOIN (BUCKET_SHUFFLE)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 1: v4 = 4: v10\n" +
                "  |  asof join conjunct: 2: v5 > 5: v11\n" +
                "  |  \n" +
                "  |----3:EXCHANGE\n" +
                "  |    \n" +
                "  1:OlapScanNode\n" +
                "     TABLE: t1");
    }

    @Test
    void testTwoJoinRootGreedy() throws Exception {
        connectContext.getSessionVariable().enableGreedyJoinReorder();
        String sql = "select t0.v1 from t1 " +
                "asof join t3 on t1.v4 = t3.v10 and t1.v5 > t3.v11 " +
                "asof join t0 on t1.v4 = t0.v2 and t0.v2 > t3.v12 " +
                "join (select * from t1 join t3 on t1.v4 = t3.v10 join t0 on t1.v4 = t0.v2 join t2 on t1.v5 = t2.v8) as a  " +
                "on t1.v5 = a.v8 ";
        String planFragment = getFragmentPlan(sql);
        assertContains(planFragment, "17:HASH JOIN\n" +
                "  |  join op: ASOF INNER JOIN (BUCKET_SHUFFLE)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 1: v4 = 8: v2\n" +
                "  |  asof join conjunct: 6: v12 < 8: v2\n" +
                "  |  \n" +
                "  |----16:EXCHANGE\n" +
                "  |    \n" +
                "  14:Project\n" +
                "  |  <slot 1> : 1: v4\n" +
                "  |  <slot 2> : 2: v5\n" +
                "  |  <slot 6> : 6: v12\n" +
                "  |  \n" +
                "  13:HASH JOIN\n" +
                "  |  join op: ASOF INNER JOIN (BUCKET_SHUFFLE)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 1: v4 = 4: v10\n" +
                "  |  asof join conjunct: 2: v5 > 5: v11\n" +
                "  |  \n" +
                "  |----12:EXCHANGE\n" +
                "  |    \n" +
                "  10:OlapScanNode\n" +
                "     TABLE: t1");

    }

    @Test
    void testTwoJoinRootDP() throws Exception {
        connectContext.getSessionVariable().enableDPJoinReorder();
        connectContext.getSessionVariable().disableGreedyJoinReorder();
        String sql = "select t0.v1 from t1 " +
                "join t3 on t1.v4 = t3.v10 " +
                "asof join t0 on t3.v11 = t0.v2 and t0.v2 > t3.v12 " +
                "join (select * from t1 join t3 on t1.v4 = t3.v10 join t0 on t1.v4 = t0.v2 join t2 on t1.v5 = t2.v8) as a  " +
                "on t1.v5 = a.v8 ";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "17:HASH JOIN\n" +
                "  |  join op: ASOF INNER JOIN (BROADCAST)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 5: v11 = 8: v2\n" +
                "  |  asof join conjunct: 6: v12 < 8: v2\n" +
                "  |  \n" +
                "  |----16:EXCHANGE\n" +
                "  |    \n" +
                "  14:Project\n" +
                "  |  <slot 2> : 2: v5\n" +
                "  |  <slot 5> : 5: v11\n" +
                "  |  <slot 6> : 6: v12\n" +
                "  |  \n" +
                "  13:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BUCKET_SHUFFLE)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 4: v10 = 1: v4\n" +
                "  |  \n" +
                "  |----12:EXCHANGE\n" +
                "  |    \n" +
                "  10:OlapScanNode\n" +
                "     TABLE: t3");
    }

}
