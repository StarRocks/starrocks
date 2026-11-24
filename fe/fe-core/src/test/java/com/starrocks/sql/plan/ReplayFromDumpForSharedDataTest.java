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

import com.starrocks.common.Config;
import com.starrocks.common.Pair;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.EditLog;
import com.starrocks.qe.SessionVariable;
import com.starrocks.server.RunMode;
import com.starrocks.sql.optimizer.dump.QueryDumpInfo;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import com.starrocks.warehouse.cngroup.ComputeResource;
import com.starrocks.warehouse.cngroup.WarehouseComputeResourceProvider;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class ReplayFromDumpForSharedDataTest extends ReplayFromDumpTestBase {
    @BeforeAll
    public static void beforeClass() throws Exception {
        new MockUp<WarehouseComputeResourceProvider>() {
            @Mock
            public boolean isResourceAvailable(ComputeResource computeResource) {
                return true;
            }
        };
        UtFrameUtils.createMinStarRocksCluster(RunMode.SHARED_DATA);

        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);
        Config.show_execution_groups = false;
        Config.enable_virtual_columns = false;
        // Should disable Dynamic Partition in replay dump test
        Config.dynamic_partition_enable = false;
        Config.tablet_sched_disable_colocate_overall_balance = true;
        UtFrameUtils.setDefaultConfigForAsyncMVTest(connectContext);
        // set default config for timeliness mvs
        UtFrameUtils.mockTimelinessForAsyncMVTest(connectContext);

        new MockUp<EditLog>() {
            @Mock
            protected void logEdit(short op, Writable writable) {
                return;
            }
        };
    }

    @Test
    public void testReplicationNum() throws Exception {
        String dumpInfo = getDumpInfoFromFile("query_dump/shared_data_query_test");
        QueryDumpInfo queryDumpInfo = getDumpInfoFromJson(dumpInfo);
        SessionVariable sessionVariable = queryDumpInfo.getSessionVariable();
        Pair<QueryDumpInfo, String> replayPair = getCostPlanFragment(dumpInfo, sessionVariable);
        Assertions.assertTrue(replayPair.second.contains("mv_name_1"), replayPair.second);
    }

    // TPC-H 100G lineitem joined with orders/supplier, captured on 1 FE + 3 BE with analyzed statistics. Pushing
    // the count below the join made each query 2-3x slower: grouping lineitem by (l_orderkey, l_linenumber) keeps
    // 583M of 600M rows, and grouping it by l_suppkey replaces a broadcast join of the 1M-row supplier.
    @ParameterizedTest(name = "{0}")
    @ValueSource(strings = {"high_ndv_join_key", "high_ndv_join_key_filter", "broadcastable_join",
            "broadcastable_join_filter"})
    public void testNoAggPushDownWithoutRowReduction(String name) throws Exception {
        String dumpInfo = getDumpInfoFromFile("query_dump/agg_push_down_below_join/" + name);
        SessionVariable sessionVariable = getDumpInfoFromJson(dumpInfo).getSessionVariable();
        Assertions.assertEquals(0, sessionVariable.getCboPushDownAggregateMode());
        String plan = getPlanFragmentWithAggPushdown(dumpInfo, sessionVariable, TExplainLevel.NORMAL).second;
        // only the two phases of the aggregation above the join
        Assertions.assertEquals(2, plan.split(":AGGREGATE \\(").length - 1, plan);
        if (name.startsWith("broadcastable_join")) {
            Assertions.assertTrue(plan.contains("INNER JOIN (BROADCAST)"), plan);
        }
    }

    // TPC-DS 100G q38, captured on the same cluster. Each INTERSECT branch pushes `DISTINCT customer_sk, d_date`
    // below the join with customer. The NDV product calls that unreduced, but the columns are correlated (one ticket,
    // one customer, one day) and the push down reduces each branch about 12x, so all three must stay pushed.
    @Test
    public void testAggPushDownWithCorrelatedGroupBy() throws Exception {
        String dumpInfo = getDumpInfoFromFile("query_dump/agg_push_down_below_join/tpcds_q38_100g");
        SessionVariable sessionVariable = getDumpInfoFromJson(dumpInfo).getSessionVariable();
        String plan = getPlanFragmentWithAggPushdown(dumpInfo, sessionVariable, TExplainLevel.NORMAL).second;
        Assertions.assertEquals(14, plan.split(":AGGREGATE \\(").length - 1, plan);
    }
}
