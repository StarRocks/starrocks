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
import com.starrocks.catalog.OlapTable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.MultiColumnCombinedStatistics;
import com.starrocks.sql.optimizer.statistics.StatisticStorage;
import mockit.Expectations;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class AggregatePushDownWithCostTest extends PlanWithCostTestBase {
    @Before
    public void before() throws Exception {
        GlobalStateMgr globalStateMgr = connectContext.getGlobalStateMgr();
        OlapTable t0 = (OlapTable) globalStateMgr.getLocalMetastore().getDb("test").getTable("t0");
        OlapTable t1 = (OlapTable) globalStateMgr.getLocalMetastore().getDb("test").getTable("t1");
        OlapTable t2 = (OlapTable) globalStateMgr.getLocalMetastore().getDb("test").getTable("t2");
        OlapTable t3 = (OlapTable) globalStateMgr.getLocalMetastore().getDb("test").getTable("t3");

        long t0Rows = 1000_000_000L;
        long t1Rows = 1000L;
        long t2Rows = 10_000_000L;
        long t3Rows = 100_000_000L;

        setTableStatistics(t0, t0Rows);
        setTableStatistics(t1, t1Rows);
        setTableStatistics(t2, t2Rows);
        setTableStatistics(t3, t3Rows);

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

                ss.getColumnStatistic(t3, "v10");
                result = new ColumnStatistic(1, 2, 0, 4, t3Rows / 10.0);
                minTimes = 0;
                ss.getColumnStatistic(t3, "v11");
                result = new ColumnStatistic(1, 100000, 0, 4, t3Rows / 200.0);
                minTimes = 0;
                ss.getColumnStatistic(t3, "v12");
                result = new ColumnStatistic(1, 200000, 0, 4, t3Rows / 20000.0);
                minTimes = 0;

                // for multi-column statistics
                ss.getMultiColumnCombinedStatistics(t0.getId());
                result = new MultiColumnCombinedStatistics(Sets.newHashSet(t0.getColumn("v1").getUniqueId(),
                        t0.getColumn("v3").getUniqueId()), 5555555);
                minTimes = 0;

                ss.getMultiColumnCombinedStatistics(t3.getId());
                result = new MultiColumnCombinedStatistics(Sets.newHashSet(t3.getColumn("v10").getUniqueId(),
                        t3.getColumn("v11").getUniqueId()), 1);
                minTimes = 0;
            }
        };

        connectContext.getSessionVariable().setCboPushDownAggregateMode(0);
        connectContext.getSessionVariable().setCboPushDownAggregateOnBroadcastJoin(true);
    }

    @Test
    public void testAggAfterNonBroadcastJoin() throws Exception {
        String sql;
        String plan;

        sql = "select " +
                "/*+SET_VAR(cbo_push_down_aggregate_mode=0,cbo_push_down_aggregate_on_broadcast_join=true)*/ sum(v3)\n" +
                "from \n" +
                "    t0 \n" +
                "    join t3 on t0.v1 = t3.v10\n" +
                "    join t2 on t0.v2 = t2.v7\n" +
                "group by t2.v9, t3.v11";
        plan = getFragmentPlan(sql);
        assertContains(plan, "  4:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BUCKET_SHUFFLE)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 1: v1 = 4: v10\n" +
                "  |  \n" +
                "  |----3:EXCHANGE\n" +
                "  |    \n" +
                "  1:AGGREGATE (update finalize)\n" +
                "  |  output: sum(3: v3)\n" +
                "  |  group by: 1: v1, 2: v2\n" +
                "  |  \n" +
                "  0:OlapScanNode");
    }

    @Test
    public void testAggOnBroadcastJoin() throws Exception {
        String sql;
        String plan;

        String sqlTemplate1 = "select " +
                "/*+SET_VAR(cbo_push_down_aggregate_mode=%d,cbo_push_down_aggregate_on_broadcast_join=%s)*/ sum(v3)\n" +
                "from \n" +
                "    t0 \n" +
                "    join t1 on t0.v1 = t1.v4\n" +
                "    join t2 on t0.v2 = t2.v7\n" +
                "group by t2.v9, t1.v5";
        String sqlTemplate2 = "select " +
                "/*+SET_VAR(cbo_push_down_aggregate_mode=%d,cbo_push_down_aggregate_on_broadcast_join=%s)*/ sum(v3)\n" +
                "from \n" +
                "    t0 \n" +
                "    join t2 on t0.v2 = t2.v7\n" +
                "    join t1 on t0.v1 = t1.v4\n" +
                "group by t2.v9, t1.v5";

        sql = String.format(sqlTemplate1, 0, "true");
        plan = getFragmentPlan(sql);
        assertContains(plan, "  5:AGGREGATE (update serialize)\n" +
                "  |  STREAMING\n" +
                "  |  output: sum(3: v3)\n" +
                "  |  group by: 2: v2, 5: v5\n" +
                "  |  \n" +
                "  4:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BUCKET_SHUFFLE)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 1: v1 = 4: v4\n" +
                "  |  \n" +
                "  |----3:EXCHANGE\n" +
                "  |    \n" +
                "  1:OlapScanNode\n" +
                "     TABLE: t0");

        sql = String.format(sqlTemplate1, 0, "false");
        plan = getFragmentPlan(sql);
        assertContains(plan, "  2:AGGREGATE (update finalize)\n" +
                "  |  output: sum(3: v3)\n" +
                "  |  group by: 1: v1, 2: v2\n" +
                "  |  \n" +
                "  1:OlapScanNode\n" +
                "     TABLE: t0");

        sql = String.format(sqlTemplate2, 0, "true");
        plan = getFragmentPlan(sql);
        assertContains(plan, "  5:AGGREGATE (update serialize)\n" +
                "  |  STREAMING\n" +
                "  |  output: sum(3: v3)\n" +
                "  |  group by: 2: v2, 8: v5\n" +
                "  |  \n" +
                "  4:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BUCKET_SHUFFLE)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 1: v1 = 7: v4\n" +
                "  |  \n" +
                "  |----3:EXCHANGE\n" +
                "  |    \n" +
                "  1:OlapScanNode\n" +
                "     TABLE: t0");

        sql = String.format(sqlTemplate2, 1, "false");
        plan = getFragmentPlan(sql);
        assertContains(plan, "  2:AGGREGATE (update finalize)\n" +
                "  |  output: sum(3: v3)\n" +
                "  |  group by: 1: v1, 2: v2\n" +
                "  |  \n" +
                "  1:OlapScanNode\n" +
                "     TABLE: t0");

        sql = String.format(sqlTemplate2, 2, "false");
        plan = getFragmentPlan(sql);
        assertContains(plan, "  2:AGGREGATE (update finalize)\n" +
                "  |  output: sum(3: v3)\n" +
                "  |  group by: 1: v1, 2: v2\n" +
                "  |  \n" +
                "  1:OlapScanNode\n" +
                "     TABLE: t0");
    }

    @Test
    public void testAggOnUnion() throws Exception {
        String sql;
        String plan;

        // Aggregate is pushed down to Broadcast Join for both children of UNION.
        //                                        Aggregate(v2,v5)
        //                                            Union
        //                   Join(v2=v7)                                   Join(v2=v7)
        //     Aggregate(v2,v5)      Aggregate(v7)            Aggregate(v2,v5)      Aggregate(v7)
        //       Join(v1=v4)             t2(v7)                 Join(v1=v4)             t2(v7)
        // t0(v1,v2)  t1(v4,v5)                          t0(v1,v2)  t1(v4,v5)
        sql = "select sum(v2)\n" +
                "from (\n" +
                "select v2, v5\n" +
                "from \n" +
                "    t0 \n" +
                "    join t1 on t0.v1 = t1.v4\n" +
                "    join t2 on t0.v2 = t2.v7\n" +
                "union\n" +
                "select v2, v5\n" +
                "from \n" +
                "    t0 \n" +
                "    join t1 on t0.v1 = t1.v4\n" +
                "    join t2 on t0.v2 = t2.v7\n" +
                ") t\n" +
                "group by v5";
        plan = getFragmentPlan(sql);
        assertContains(plan, "  5:AGGREGATE (update serialize)\n" +
                "  |  STREAMING\n" +
                "  |  group by: 2: v2, 5: v5\n" +
                "  |  \n" +
                "  4:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BUCKET_SHUFFLE)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 1: v1 = 4: v4\n" +
                "  |  \n" +
                "  |----3:EXCHANGE\n" +
                "  |    \n" +
                "  1:OlapScanNode\n" +
                "     TABLE: t0");
        assertContains(plan, "  9:AGGREGATE (update finalize)\n" +
                "  |  group by: 7: v7\n" +
                "  |  \n" +
                "  8:OlapScanNode\n" +
                "     TABLE: t2");
        assertContains(plan, "  18:AGGREGATE (update serialize)\n" +
                "  |  STREAMING\n" +
                "  |  group by: 11: v2, 14: v5\n" +
                "  |  \n" +
                "  17:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BUCKET_SHUFFLE)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 10: v1 = 13: v4\n" +
                "  |  \n" +
                "  |----16:EXCHANGE\n" +
                "  |    \n" +
                "  14:OlapScanNode\n" +
                "     TABLE: t0");
        assertContains(plan, "  22:AGGREGATE (update finalize)\n" +
                "  |  group by: 16: v7\n" +
                "  |  \n" +
                "  21:OlapScanNode\n" +
                "     TABLE: t2");

        // Aggregate is pushed down to Broadcast Join for one child of UNION and to Scan for the other.
        //                                        Aggregate(v2,v5)
        //                                            Union
        //                   Join(v2=v7)                             \
        //     Aggregate(v2,v5)      Aggregate(v7)                 Join(v2=v7)
        //       Join(v1=v4)             t2(v7)           Aggregate(v2)    Aggregate(v7,v8)
        // t0(v1,v2)  t1(v4,v5)                              t0(v2)          t2(v7,v8)
        sql = "select sum(v2)\n" +
                "from (\n" +
                "select v2, v5\n" +
                "from \n" +
                "    t0 \n" +
                "    join t1 on t0.v1 = t1.v4\n" +
                "    join t2 on t0.v2 = t2.v7\n" +
                "union\n" +
                "select v2, v8\n" +
                "from \n" +
                "    t0 \n" +
                "    join t2 on t0.v2 = t2.v7\n" +
                ") t\n" +
                "group by v5";
        plan = getFragmentPlan(sql);
        assertContains(plan, "  5:AGGREGATE (update serialize)\n" +
                "  |  STREAMING\n" +
                "  |  group by: 2: v2, 5: v5\n" +
                "  |  \n" +
                "  4:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BUCKET_SHUFFLE)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 1: v1 = 4: v4\n" +
                "  |  \n" +
                "  |----3:EXCHANGE\n" +
                "  |    \n" +
                "  1:OlapScanNode\n" +
                "     TABLE: t0");
        assertContains(plan, "  9:AGGREGATE (update finalize)\n" +
                "  |  group by: 7: v7\n" +
                "  |  \n" +
                "  8:OlapScanNode\n" +
                "     TABLE: t2");
        assertContains(plan, "  17:AGGREGATE (update serialize)\n" +
                "  |  STREAMING\n" +
                "  |  group by: 11: v2\n" +
                "  |  \n" +
                "  16:OlapScanNode\n" +
                "     TABLE: t0");
        assertContains(plan, "  15:AGGREGATE (update finalize)\n" +
                "  |  group by: 13: v7, 14: v8\n" +
                "  |  \n" +
                "  14:OlapScanNode\n" +
                "     TABLE: t2");

        // Cannot push down for the second union child.
        sql = "select sum(v2)\n" +
                "from (\n" +
                "select v2, v5\n" +
                "from \n" +
                "    t0 \n" +
                "    join t1 on t0.v1 = t1.v4\n" +
                "    join t2 on t0.v2 = t2.v7\n" +
                "union\n" +
                "select v2, v11\n" +
                "from \n" +
                "    t0 \n" +
                "    join t3 on t0.v2 = t3.v10\n" +
                ") t\n" +
                "group by v5";
        plan = getFragmentPlan(sql);
        Assert.assertEquals(4, StringUtils.countMatches(plan, ":AGGREGATE "));
    }

    @Test
    public void testAggWithMultiColumnStats() throws Exception {
        String sql = "select count(1) from t0 group by v1, v3";
        String plan = getCostExplain(sql);
        assertCContains(plan, "1:AGGREGATE (update finalize)\n" +
                "  |  aggregate: count[(1); args: TINYINT; result: BIGINT; args nullable: false; result nullable: false]\n" +
                "  |  group by: [1: v1, BIGINT, true], [3: v3, BIGINT, true]\n" +
                "  |  cardinality: 5555555\n" +
                "  |  column statistics: \n" +
                "  |  * v1-->[1.0, 2.0, 0.0, 4.0, 3333333.3333333335] ESTIMATE\n" +
                "  |  * v3-->[1.0, 2000000.0, 0.0, 4.0, 3333333.3333333335] ESTIMATE\n" +
                "  |  * count-->[0.0, 1.0E9, 0.0, 8.0, 5555555.0] ESTIMATE");

        sql = "select count(1) from t0 group by v1, abs(v3)";
        plan = getCostExplain(sql);
        assertCContains(plan, "3:Project\n" +
                "  |  output columns:\n" +
                "  |  5 <-> [5: count, BIGINT, false]\n" +
                "  |  cardinality: 1000000000");

        sql = "select count(1) from t3 group by v10, v11, v12";
        plan = getCostExplain(sql);
        assertCContains(plan, "1:AGGREGATE (update finalize)\n" +
                "  |  aggregate: count[(1); args: TINYINT; result: BIGINT; args nullable: false; result nullable: false]\n" +
                "  |  group by: [1: v10, BIGINT, true], [2: v11, BIGINT, true], [3: v12, BIGINT, true]\n" +
                "  |  cardinality: 3750");
    }
}
