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
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.StatisticStorage;
import mockit.Expectations;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class InnerToSemiTest extends PlanWithCostTestBase {
    @Before
    public void before() throws Exception {
        FeConstants.enableJoinReorderInLogicalPhase = true;
        long t0Rows = 1000_000_000L;
        long t1Rows = 1000L;
        long t2Rows = 10_000_000L;

        GlobalStateMgr globalStateMgr = connectContext.getGlobalStateMgr();
        OlapTable t0 = (OlapTable) globalStateMgr.getLocalMetastore().getDb("test").getTable("t0");
        OlapTable t1 = (OlapTable) globalStateMgr.getLocalMetastore().getDb("test").getTable("t1");
        OlapTable t2 = (OlapTable) globalStateMgr.getLocalMetastore().getDb("test").getTable("t2");
        StatisticStorage ss = GlobalStateMgr.getCurrentState().getStatisticStorage();
        new Expectations(ss) {
            {
                ss.getColumnStatistic(t0, "v1");
                result = new ColumnStatistic(1, 2, 0, 4, t0Rows / 300.0);
                minTimes = 0;
                ss.getColumnStatistic(t0, "v2");
                result = new ColumnStatistic(1, 4000000, 0, 4, t0Rows / 2000.0);
                minTimes = 0;
                ss.getColumnStatistic(t0, "v3");
                result = new ColumnStatistic(1, 2000000, 0, 4, t0Rows / 300.0);
                minTimes = 0;

                ss.getColumnStatistic(t1, "v4");
                result = new ColumnStatistic(1, 2, 0, 4, t1Rows);
                minTimes = 0;
                ss.getColumnStatistic(t1, "v5");
                result = new ColumnStatistic(1, 100000, 0, 4, t1Rows / 100.0);
                minTimes = 0;
                ss.getColumnStatistic(t1, "v6");
                result = new ColumnStatistic(1, 200000, 0, 4, t1Rows / 1000.0);
                minTimes = 0;

                ss.getColumnStatistic(t2, "v7");
                result = new ColumnStatistic(1, 2, 0, 4, t2Rows / 200.0);
                minTimes = 0;
                ss.getColumnStatistic(t2, "v8");
                result = new ColumnStatistic(1, 100000, 0, 4, t2Rows / 2000.0);
                minTimes = 0;
                ss.getColumnStatistic(t2, "v9");
                result = new ColumnStatistic(1, 200000, 0, 4, t2Rows / 20000.0);
                minTimes = 0;
            }
        };
        connectContext.getSessionVariable().setCboPushDownAggregateMode(-1);
    }

    @After
    public void after() {
        FeConstants.enableJoinReorderInLogicalPhase = false;
    }

    @Test
    public void testInnerToSemi() throws Exception {
        String sql = "select distinct(t2.v9) from t0 join t1 on t0.v1=t1.v4 join t2 on t0.v2 = t2.v8;";
        String plan = getLogicalFragmentPlan(sql);
        assertContains(plan, "LEFT SEMI JOIN (join-predicate [8: v8 = 2: v2] post-join-predicate [null])\n" +
                "                SCAN (columns[8: v8, 9: v9] predicate[8: v8 IS NOT NULL])\n" +
                "                EXCHANGE BROADCAST\n" +
                "                    LEFT SEMI JOIN (join-predicate [1: v1 = 4: v4] post-join-predicate [null])");

        sql = "select distinct(t0.v1) from t0 join t1 on t0.v1 = t1.v4 join t2 on t1.v4 = t2.v7;";
        plan = getLogicalFragmentPlan(sql);
        assertContains(plan, " LEFT SEMI JOIN (join-predicate [1: v1 = 7: v7] post-join-predicate [null])\n" +
                "        LEFT SEMI JOIN (join-predicate [1: v1 = 4: v4] post-join-predicate [null])");

    }
}
