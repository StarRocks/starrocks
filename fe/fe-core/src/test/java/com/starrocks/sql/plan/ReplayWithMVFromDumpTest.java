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
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Set;

import static com.starrocks.sql.plan.PlanTestNoneDBBase.assertContains;
import static com.starrocks.sql.plan.PlanTestNoneDBBase.assertNotContains;

public class ReplayWithMVFromDumpTest extends ReplayFromDumpTestBase {

    @BeforeClass
    public static void beforeClass() throws Exception {
        ReplayFromDumpTestBase.beforeClass();
        UtFrameUtils.setDefaultConfigForAsyncMVTest(connectContext);

        new MockUp<MaterializedView>() {
            /**
             * {@link MaterializedView#getPartitionNamesToRefreshForMv(Set, boolean)}
             */
            @Mock
            public boolean getPartitionNamesToRefreshForMv(Set<String> toRefreshPartitions,
                                                           boolean isQueryRewrite) {
                return true;
            }
        };

        new MockUp<UtFrameUtils>() {
            /**
             * {@link UtFrameUtils#isPrintPlanTableNames()}
             */
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
        assertContains(replayPair.second, "table: mv1, rollup: mv1");
        FeConstants.isReplayFromQueryDump = false;
    }

    @Test
    public void testMock_MV_JoinAgg1() throws Exception {
        FeConstants.isReplayFromQueryDump = true;
        String jsonStr = getDumpInfoFromFile("query_dump/materialized-view/mock_join_agg1");
        // Table and mv have no stats, mv rewrite is ok.
        Pair<QueryDumpInfo, String> replayPair = getCostPlanFragment(jsonStr, null);
        // Rewrite OK when enhance rule based mv rewrite.
        assertContains(replayPair.second, "table: test_mv0, rollup: test_mv0");
        FeConstants.isReplayFromQueryDump = false;
    }

    @Test
    public void testMV_JoinAgg2() throws Exception {
        FeConstants.isReplayFromQueryDump = true;
        String jsonStr = getDumpInfoFromFile("query_dump/materialized-view/join_agg2");
        connectContext.getSessionVariable()
                .setMaterializedViewRewriteMode(SessionVariable.MaterializedViewRewriteMode.FORCE.toString());
        Pair<QueryDumpInfo, String> replayPair = getCostPlanFragment(jsonStr, connectContext.getSessionVariable());
        assertContains(replayPair.second, "table: mv1, rollup: mv1");
        FeConstants.isReplayFromQueryDump = false;
    }

    @Test
    public void testMV_JoinAgg3() throws Exception {
        FeConstants.isReplayFromQueryDump = true;
        // Table and mv have no stats, mv rewrite is ok.
        Pair<QueryDumpInfo, String> replayPair =
                getPlanFragment(getDumpInfoFromFile("query_dump/materialized-view/join_agg3"),
                        connectContext.getSessionVariable(), TExplainLevel.NORMAL);
        assertContains(replayPair.second, "line_order_flat_mv");
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
        assertContains(replayPair.second, "line_order_flat_mv");
        FeConstants.isReplayFromQueryDump = false;
    }

    @Test
    public void testMV_MVOnMV1() throws Exception {
        FeConstants.isReplayFromQueryDump = true;
        Pair<QueryDumpInfo, String> replayPair =
                getPlanFragment(getDumpInfoFromFile("query_dump/materialized-view/mv_on_mv1"),
                        null, TExplainLevel.NORMAL);
        assertContains(replayPair.second, "mv2");
        FeConstants.isReplayFromQueryDump = false;
    }

    @Test
    public void testMock_MV_MVOnMV1() throws Exception {
        FeConstants.isReplayFromQueryDump = true;
        Pair<QueryDumpInfo, String> replayPair =
                getPlanFragment(getDumpInfoFromFile("query_dump/materialized-view/mock_mv_on_mv1"),
                        null, TExplainLevel.NORMAL);
        assertContains(replayPair.second, "tbl_mock_017");
        FeConstants.isReplayFromQueryDump = false;
    }

    @Test
    public void testMVOnMV2() throws Exception {
        connectContext.getSessionVariable().setMaterializedViewRewriteMode("force");
        Pair<QueryDumpInfo, String> replayPair =
                getPlanFragment(getDumpInfoFromFile("query_dump/materialized-view/mv_on_mv2"),
                        connectContext.getSessionVariable(), TExplainLevel.NORMAL);
        connectContext.getSessionVariable().setMaterializedViewRewriteMode("default");
        assertContains(replayPair.second, "test_mv2");
    }

    @Test
    public void testMV_AggWithHaving1() throws Exception {
        Pair<QueryDumpInfo, String> replayPair =
                getPlanFragment(getDumpInfoFromFile("query_dump/materialized-view/agg_with_having1"),
                        connectContext.getSessionVariable(), TExplainLevel.NORMAL);
        assertContains(replayPair.second, "TEST_MV_2");
    }

    @Test
    public void testMV_AggWithHaving2() throws Exception {
        Pair<QueryDumpInfo, String> replayPair =
                getPlanFragment(getDumpInfoFromFile("query_dump/materialized-view/agg_with_having2"),
                        connectContext.getSessionVariable(), TExplainLevel.NORMAL);
        assertContains(replayPair.second, "TEST_MV_2");
    }

    @Test
    public void testMV_AggWithHaving3() throws Exception {
        Pair<QueryDumpInfo, String> replayPair =
                getPlanFragment(getDumpInfoFromFile("query_dump/materialized-view/agg_with_having3"),
                        connectContext.getSessionVariable(), TExplainLevel.NORMAL);
        assertContains(replayPair.second, "TEST_MV_2");
    }

    @Test
    public void testMock_MV_AggWithHaving3() throws Exception {
        Pair<QueryDumpInfo, String> replayPair =
                getPlanFragment(getDumpInfoFromFile("query_dump/materialized-view/mock_agg_with_having3"),
                        connectContext.getSessionVariable(), TExplainLevel.NORMAL);
        // Rewrite OK since rule based mv is enhanced
        assertContains(replayPair.second, "test_mv2");
    }

    @Test
    public void testMock_MV_CostBug() throws Exception {
        FeConstants.isReplayFromQueryDump = true;
        connectContext.getSessionVariable().setMaterializedViewRewriteMode("force");
        Pair<QueryDumpInfo, String> replayPair =
                getPlanFragment(getDumpInfoFromFile("query_dump/materialized-view/mv_with_cost_bug1"),
                        connectContext.getSessionVariable(), TExplainLevel.NORMAL);
        assertContains(replayPair.second, "mv_35");
        connectContext.getSessionVariable().setMaterializedViewRewriteMode("default");
        FeConstants.isReplayFromQueryDump = false;
    }

    @Test
    public void testMVWithDictRewrite() throws Exception {
        try {
            FeConstants.USE_MOCK_DICT_MANAGER = true;
            Pair<QueryDumpInfo, String> replayPair =
                    getCostPlanFragment(getDumpInfoFromFile("query_dump/tpch_query11_mv_rewrite"));
            assertContains(replayPair.second,
                    "DictDecode(78: n_name, [<place-holder> = 'GERMANY'])");
        } finally {
            FeConstants.USE_MOCK_DICT_MANAGER = false;
        }
    }

    /**
     * Test synchronous materialized view rewrite with global dict optimization.
     */
    @Test
    public void testSyncMVRewriteWithDict() throws Exception {
        Pair<QueryDumpInfo, String> replayPair =
                getPlanFragment(getDumpInfoFromFile("query_dump/materialized-view/mv_rewrite_with_dict_opt1"),
                        connectContext.getSessionVariable(), TExplainLevel.NORMAL);
        // TODO: support synchronous materialized view in query dump
        // String sql = "create materialized view mv_tbl_mock_001 " +
        //        "as select nmock_002, nmock_003, nmock_004, " +
        //        "nmock_005, nmock_006, nmock_007, nmock_008, nmock_009, nmock_010, " +
        //        "nmock_011, nmock_012, nmock_013, nmock_014, nmock_015, nmock_016, " +
        //        "nmock_017, nmock_018, nmock_019, nmock_020, nmock_021, nmock_022, nmock_023, " +
        //        "nmock_024, nmock_025, nmock_026, nmock_027, nmock_028, nmock_029, nmock_030, nmock_031, " +
        //        "nmock_032, nmock_033, nmock_034, nmock_035, nmock_036, nmock_037, nmock_038, nmock_039, " +
        //        "nmock_040, nmock_041 from tbl_mock_001 order by nmock_002;";
        assertNotContains(replayPair.second, "mv_tbl_mock_001");
    }
}