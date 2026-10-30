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
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.StatisticStorage;
import mockit.Expectations;
import org.junit.jupiter.api.Test;

public class PruneShuffleColumnRuleTest extends PlanWithCostTestBase {

    @Test
    public void testPruneShuffleColumns() throws Exception {
        GlobalStateMgr globalStateMgr = connectContext.getGlobalStateMgr();
        OlapTable t0 = (OlapTable) globalStateMgr.getLocalMetastore().getDb("test").getTable("t0");
        OlapTable t1 = (OlapTable) globalStateMgr.getLocalMetastore().getDb("test").getTable("t1");

        StatisticStorage ss = GlobalStateMgr.getCurrentState().getStatisticStorage();
        new Expectations(ss) {
            {
                ss.getColumnStatistic(t0, "v1");
                result = new ColumnStatistic(1, 2, 0, 4, 3);

                ss.getColumnStatistic(t0, "v2");
                result = new ColumnStatistic(1, 4000000, 0, 4, 4000000);

                ss.getColumnStatistic(t0, "v3");
                result = new ColumnStatistic(1, 2000000, 0, 4, 2000000);

                ss.getColumnStatistic(t1, "v4");
                result = new ColumnStatistic(1, 2, 0, 4, 3);

                ss.getColumnStatistic(t1, "v5");
                result = new ColumnStatistic(1, 100000, 0, 4, 100000);

                ss.getColumnStatistic(t1, "v6");
                result = new ColumnStatistic(1, 200000, 0, 4, 200000);
            }
        };

        setTableStatistics(t0, 4000000);
        setTableStatistics(t1, 100000);

        String sql = "select * from t0 join[shuffle] t1 on t0.v2 = t1.v5 and t0.v3 = t1.v6";
        String plan = getVerboseExplain(sql);

        assertContains(plan, """
                  Input Partition: RANDOM
                  OutPut Partition: HASH_PARTITIONED: 2: v2
                  OutPut Exchange Id: 01\
                """);

        assertContains(plan, """
                Input Partition: RANDOM
                  OutPut Partition: HASH_PARTITIONED: 5: v5
                  OutPut Exchange Id: 03""");

        assertContains(plan, """
                  |  build runtime filters:
                  |  - filter_id = 0, build_expr = (5: v5), remote = true
                """);

        assertContains(plan, "probe runtime filters:\n" +
                "     - filter_id = 0, probe_expr = (2: v2)");
    }

    // Regression test: PruneShuffleColumnRule must sync intermediate join outputProperty.
    //
    // In a nested shuffle join (inner JOIN outer), the outer join's requiredProperties[0] is
    // the same PhysicalPropertySet object as the inner join's outputProperty (set in
    // extractBestPlan). When PruneShuffleColumnRule prunes multi-column Exchange specs to 1
    // column but leaves the inner join's outputProperty with 2 columns, the outer join reads
    // a stale column count and generates probePartitionByExprs with 2 columns. The GRF then
    // carries wrong partition_exprs across the inner join's Exchange nodes, causing incorrect
    // filtering at runtime.
    @Test
    public void testNestedShuffleJoinGRFPartitionExprsConsistencyAfterPrune() throws Exception {
        GlobalStateMgr globalStateMgr = connectContext.getGlobalStateMgr();
        OlapTable t0 = (OlapTable) globalStateMgr.getLocalMetastore().getDb("test").getTable("t0");
        OlapTable t1 = (OlapTable) globalStateMgr.getLocalMetastore().getDb("test").getTable("t1");

        StatisticStorage ss = GlobalStateMgr.getCurrentState().getStatisticStorage();
        new Expectations(ss) {
            {
                ss.getColumnStatistic(t0, "v1");
                result = new ColumnStatistic(1, 2, 0, 4, 3);

                ss.getColumnStatistic(t0, "v2");
                result = new ColumnStatistic(1, 4000000, 0, 4, 4000000);

                ss.getColumnStatistic(t0, "v3");
                result = new ColumnStatistic(1, 2000000, 0, 4, 2000000);

                ss.getColumnStatistic(t1, "v4");
                result = new ColumnStatistic(1, 2, 0, 4, 3);

                ss.getColumnStatistic(t1, "v5");
                result = new ColumnStatistic(1, 1000000, 0, 4, 1000000);

                ss.getColumnStatistic(t1, "v6");
                result = new ColumnStatistic(1, 500000, 0, 4, 500000);
            }
        };

        setTableStatistics(t0, 4000000);
        setTableStatistics(t1, 1000000);

        // (t0 JOIN t1 a) JOIN t1 b — two-level shuffle join on (v2=v5, v3=v6).
        // PruneShuffleColumnRule prunes all three Exchanges to 1 column.
        // The inner join's outputProperty must be synced so the outer join uses the
        // correct 1-column probePartitionByExprs when pushing down GRF.
        String sql = "select t0.v1, t0.v2, t0.v3, a.v4, a.v5, a.v6, b.v4 as b_v4, b.v5 as b_v5, b.v6 as b_v6 from t0" +
                "  join[shuffle] t1 a on t0.v2 = a.v5 and t0.v3 = a.v6" +
                "  join[shuffle] t1 b on t0.v2 = b.v5 and t0.v3 = b.v6";
        String plan = getVerboseExplain(sql);

        // All Exchange nodes must be pruned to 1 column
        assertNotContains(plan, "HASH_PARTITIONED: 2: v2, 3: v3");
        assertNotContains(plan, "HASH_PARTITIONED: 5: v5, 6: v6");

        // GRF probe partition_exprs must be 1 column — before the fix this was 2 columns
        // because the inner join's outputProperty was not updated after Exchange pruning.
        assertNotContains(plan, "partition_exprs = (2: v2, 3: v3)");
        assertNotContains(plan, "partition_exprs = (5: v5, 6: v6)");
    }
}