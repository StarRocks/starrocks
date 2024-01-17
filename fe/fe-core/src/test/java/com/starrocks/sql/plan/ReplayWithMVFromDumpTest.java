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
import com.starrocks.catalog.MaterializedView;
import com.starrocks.common.FeConstants;
import com.starrocks.common.Pair;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.optimizer.dump.QueryDumpInfo;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Set;

public class ReplayWithMVFromDumpTest extends ReplayFromDumpTestBase {

    @BeforeClass
    public static void beforeClass() throws Exception {
        ReplayFromDumpTestBase.beforeClass();
        connectContext.getSessionVariable().setEnableMVOptimizerTraceLog(true);
        connectContext.getSessionVariable().setEnableQueryDebugTrace(true);

        new MockUp<MaterializedView>() {
            @Mock
            Set<String> getPartitionNamesToRefreshForMv(boolean isQueryRewrite) {
                return Sets.newHashSet();
            }
        };

        new MockUp<UtFrameUtils>() {
            @Mock
            boolean isPrintPlanTableNames() {
                return true;
            }
        };
    }

    @Before
    public void before() throws Exception {
        super.before();
    }

    @After
    public void after() {
    }

    @Test
    public void testMV_JoinAgg1() throws Exception {
        FeConstants.isReplayFromQueryDump = true;
        String jsonStr = getDumpInfoFromFile("query_dump/materialized-view/join_agg1");
        // Table and mv have no stats, mv rewrite is ok.
        Pair<QueryDumpInfo, String> replayPair = getCostPlanFragment(jsonStr, null);
        Assert.assertTrue(replayPair.second, replayPair.second.contains("table: mv1, rollup: mv1"));
        FeConstants.isReplayFromQueryDump = false;
    }

    @Test
    public void testMock_MV_JoinAgg1() throws Exception {
        FeConstants.isReplayFromQueryDump = true;
        String jsonStr = getDumpInfoFromFile("query_dump/materialized-view/mock_join_agg1");
        // Table and mv have no stats, mv rewrite is ok.
        Pair<QueryDumpInfo, String> replayPair = getCostPlanFragment(jsonStr, null);
        Assert.assertTrue(replayPair.second, replayPair.second.contains("table: tbl_mock_001, rollup: tbl_mock_001"));
        FeConstants.isReplayFromQueryDump = false;
    }

    @Test
    public void testMV_JoinAgg2() throws Exception {
        FeConstants.isReplayFromQueryDump = true;
        String jsonStr = getDumpInfoFromFile("query_dump/materialized-view/join_agg2");
        connectContext.getSessionVariable()
                .setMaterializedViewRewriteMode(SessionVariable.MaterializedViewRewriteMode.FORCE.toString());
        Pair<QueryDumpInfo, String> replayPair = getCostPlanFragment(jsonStr, connectContext.getSessionVariable());
        Assert.assertTrue(replayPair.second.contains("table: mv1, rollup: mv1"));
        FeConstants.isReplayFromQueryDump = false;
    }

    @Test
    public void testMV_JoinAgg3() throws Exception {
        FeConstants.isReplayFromQueryDump = true;
        // Table and mv have no stats, mv rewrite is ok.
        Pair<QueryDumpInfo, String> replayPair =
                getPlanFragment(getDumpInfoFromFile("query_dump/materialized-view/join_agg3"),
                        connectContext.getSessionVariable(), TExplainLevel.NORMAL);
        Assert.assertTrue(replayPair.second.contains("line_order_flat_mv"));
        FeConstants.isReplayFromQueryDump = false;
    }

    @Test
    public void testMV_JoinAgg4() throws Exception {
        FeConstants.isReplayFromQueryDump = true;
        connectContext.getSessionVariable().setMaterializedViewRewriteMode(
                SessionVariable.MaterializedViewRewriteMode.MODE_FORCE.toString());
        Pair<QueryDumpInfo, String> replayPair =
                getPlanFragment(getDumpInfoFromFile("query_dump/materialized-view/join_agg4"),
                        connectContext.getSessionVariable(), TExplainLevel.NORMAL);
        Assert.assertTrue(replayPair.second.contains("line_order_flat_mv"));
        FeConstants.isReplayFromQueryDump = false;
    }

    @Test
    public void testMV_MVOnMV1() throws Exception {
        FeConstants.isReplayFromQueryDump = true;
        Pair<QueryDumpInfo, String> replayPair =
                getPlanFragment(getDumpInfoFromFile("query_dump/materialized-view/mv_on_mv1"),
                        null, TExplainLevel.NORMAL);
        Assert.assertTrue(replayPair.second.contains("mv2"));
        FeConstants.isReplayFromQueryDump = false;
    }

    @Test
    public void testMVJoin() throws Exception {
        connectContext.getSessionVariable().setMaterializedViewRewriteMode("force");
        Pair<QueryDumpInfo, String> replayPair =
                getPlanFragment(getDumpInfoFromFile("query_dump/materialized-view/mv_join"),
                        connectContext.getSessionVariable(), TExplainLevel.NORMAL);
        Assert.assertTrue(replayPair.second, replayPair.second.contains("16:HASH JOIN\n" +
                "  |  join op: LEFT OUTER JOIN (PARTITIONED)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 384: cast = 209: tenant_id\n" +
                "  |  equal join conjunct: 385: cast = 210: poi_id\n" +
                "  |  equal join conjunct: 36: cate_id = 227: if"));
    }

    @Test
    public void testMock_MV_MVOnMV1() throws Exception {
        FeConstants.isReplayFromQueryDump = true;
        Pair<QueryDumpInfo, String> replayPair =
                getPlanFragment(getDumpInfoFromFile("query_dump/materialized-view/mock_mv_on_mv1"),
                        null, TExplainLevel.NORMAL);
        Assert.assertTrue(replayPair.second.contains("tbl_mock_017"));
        FeConstants.isReplayFromQueryDump = false;
    }

    @Test
    @Ignore
    public void testMVOnMV2() throws Exception {
        connectContext.getSessionVariable().setMaterializedViewRewriteMode("force");
        Pair<QueryDumpInfo, String> replayPair =
                getPlanFragment(getDumpInfoFromFile("query_dump/materialized-view/mv_on_mv2"),
                        connectContext.getSessionVariable(), TExplainLevel.NORMAL);
        Assert.assertTrue(replayPair.second.contains("test_mv2"));
    }

    @Test
    public void testMV_AggWithHaving1() throws Exception {
        Pair<QueryDumpInfo, String> replayPair =
                getPlanFragment(getDumpInfoFromFile("query_dump/materialized-view/agg_with_having1"),
                        connectContext.getSessionVariable(), TExplainLevel.NORMAL);
        Assert.assertTrue(replayPair.second.contains("TEST_MV_2"));
    }

    @Test
    public void testMV_AggWithHaving2() throws Exception {
        Pair<QueryDumpInfo, String> replayPair =
                getPlanFragment(getDumpInfoFromFile("query_dump/materialized-view/agg_with_having2"),
                        connectContext.getSessionVariable(), TExplainLevel.NORMAL);
        Assert.assertTrue(replayPair.second.contains("TEST_MV_2"));
    }

    @Test
    @Ignore
    public void testMV_AggWithHaving3() throws Exception {
        Pair<QueryDumpInfo, String> replayPair =
                getPlanFragment(getDumpInfoFromFile("query_dump/materialized-view/agg_with_having3"),
                        connectContext.getSessionVariable(), TExplainLevel.NORMAL);
        Assert.assertTrue(replayPair.second.contains("TEST_MV_3"));
    }

    @Test
    @Ignore
    public void testMock_MV_AggWithHaving3() throws Exception {
        Pair<QueryDumpInfo, String> replayPair =
                getPlanFragment(getDumpInfoFromFile("query_dump/materialized-view/mock_agg_with_having3"),
                        connectContext.getSessionVariable(), TExplainLevel.NORMAL);
        Assert.assertTrue(replayPair.second.contains("tbl_mock_023"));
    }

    @Test
    public void testMock_MV_CostBug() throws Exception {
        FeConstants.isReplayFromQueryDump = true;
        connectContext.getSessionVariable().setMaterializedViewRewriteMode("force");
        Pair<QueryDumpInfo, String> replayPair =
                getPlanFragment(getDumpInfoFromFile("query_dump/materialized-view/mv_with_cost_bug1"),
                        connectContext.getSessionVariable(), TExplainLevel.NORMAL);
        Assert.assertTrue(replayPair.second, replayPair.second.contains("mv_35"));
        connectContext.getSessionVariable().setMaterializedViewRewriteMode("default");
        FeConstants.isReplayFromQueryDump = false;
    }
}
