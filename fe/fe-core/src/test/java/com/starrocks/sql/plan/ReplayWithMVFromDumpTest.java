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

<<<<<<< HEAD
import com.starrocks.catalog.MaterializedView;
import com.starrocks.common.FeConstants;
import com.starrocks.common.Pair;
import com.starrocks.common.profile.Tracers;
=======
import com.starrocks.common.FeConstants;
import com.starrocks.common.Pair;
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.common.QueryDebugOptions;
import com.starrocks.sql.optimizer.dump.QueryDumpInfo;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.utframe.UtFrameUtils;
<<<<<<< HEAD
import mockit.Mock;
import mockit.MockUp;
=======
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
<<<<<<< HEAD
import org.junit.Ignore;
import org.junit.Test;

import java.util.Set;

import static com.starrocks.sql.plan.PlanTestNoneDBBase.assertContains;
=======
import org.junit.Test;

import static com.starrocks.sql.plan.PlanTestNoneDBBase.assertContains;
import static com.starrocks.sql.plan.PlanTestNoneDBBase.assertNotContains;
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))

public class ReplayWithMVFromDumpTest extends ReplayFromDumpTestBase {

    @BeforeClass
    public static void beforeClass() throws Exception {
        ReplayFromDumpTestBase.beforeClass();
        UtFrameUtils.setDefaultConfigForAsyncMVTest(connectContext);
<<<<<<< HEAD

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
=======
        // set default config for timeliness mvs
        UtFrameUtils.mockTimelinessForAsyncMVTest(connectContext);
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
    }

    @Before
    public void before() throws Exception {
        super.before();
    }

    @After
    public void after() {
    }

<<<<<<< HEAD
    @Ignore
=======
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
    @Test
    public void testMV_JoinAgg1() throws Exception {
        FeConstants.isReplayFromQueryDump = true;
        String jsonStr = getDumpInfoFromFile("query_dump/materialized-view/join_agg1");
        // Table and mv have no stats, mv rewrite is ok.
        Pair<QueryDumpInfo, String> replayPair = getCostPlanFragment(jsonStr, null);
<<<<<<< HEAD
        Assert.assertTrue(replayPair.second, replayPair.second.contains("table: mv1, rollup: mv1"));
=======
        assertContains(replayPair.second, "table: mv1, rollup: mv1");
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
        FeConstants.isReplayFromQueryDump = false;
    }

    @Test
    public void testMock_MV_JoinAgg1() throws Exception {
        FeConstants.isReplayFromQueryDump = true;
        String jsonStr = getDumpInfoFromFile("query_dump/materialized-view/mock_join_agg1");
        // Table and mv have no stats, mv rewrite is ok.
        Pair<QueryDumpInfo, String> replayPair = getCostPlanFragment(jsonStr, null);
<<<<<<< HEAD
        Assert.assertTrue(replayPair.second, replayPair.second.contains("table: test_mv0"));
=======
        // Rewrite OK when enhance rule based mv rewrite.
        assertContains(replayPair.second, "table: test_mv0, rollup: test_mv0");
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
        FeConstants.isReplayFromQueryDump = false;
    }

    @Test
    public void testMV_JoinAgg2() throws Exception {
        FeConstants.isReplayFromQueryDump = true;
        String jsonStr = getDumpInfoFromFile("query_dump/materialized-view/join_agg2");
<<<<<<< HEAD
        Tracers.register(connectContext);
        Tracers.init(connectContext, Tracers.Mode.LOGS, "MV");
        connectContext.getSessionVariable()
                .setMaterializedViewRewriteMode(SessionVariable.MaterializedViewRewriteMode.FORCE.toString());
        Pair<QueryDumpInfo, String> replayPair = getCostPlanFragment(jsonStr, connectContext.getSessionVariable());
        String pr = Tracers.printLogs();
        Tracers.close();
        Assert.assertTrue(replayPair.second.contains("table: mv1, rollup: mv1"));
=======
        connectContext.getSessionVariable()
                .setMaterializedViewRewriteMode(SessionVariable.MaterializedViewRewriteMode.FORCE.toString());
        Pair<QueryDumpInfo, String> replayPair = getCostPlanFragment(jsonStr, connectContext.getSessionVariable());
        assertContains(replayPair.second, "table: mv1, rollup: mv1");
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
        FeConstants.isReplayFromQueryDump = false;
    }

    @Test
    public void testMV_JoinAgg3() throws Exception {
        FeConstants.isReplayFromQueryDump = true;
        // Table and mv have no stats, mv rewrite is ok.
        Pair<QueryDumpInfo, String> replayPair =
                getPlanFragment(getDumpInfoFromFile("query_dump/materialized-view/join_agg3"),
                        connectContext.getSessionVariable(), TExplainLevel.NORMAL);
<<<<<<< HEAD
        Assert.assertTrue(replayPair.second.contains("line_order_flat_mv"));
=======
        assertContains(replayPair.second, "line_order_flat_mv");
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
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
<<<<<<< HEAD
        Assert.assertTrue(replayPair.second.contains("line_order_flat_mv"));
=======
        assertContains(replayPair.second, "line_order_flat_mv");
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
        FeConstants.isReplayFromQueryDump = false;
    }

    @Test
    public void testMV_MVOnMV1() throws Exception {
        FeConstants.isReplayFromQueryDump = true;
        Pair<QueryDumpInfo, String> replayPair =
                getPlanFragment(getDumpInfoFromFile("query_dump/materialized-view/mv_on_mv1"),
                        null, TExplainLevel.NORMAL);
<<<<<<< HEAD
        Assert.assertTrue(replayPair.second.contains("mv2"));
=======
        assertContains(replayPair.second, "mv2");
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
        FeConstants.isReplayFromQueryDump = false;
    }

    @Test
    public void testMock_MV_MVOnMV1() throws Exception {
        FeConstants.isReplayFromQueryDump = true;
        Pair<QueryDumpInfo, String> replayPair =
                getPlanFragment(getDumpInfoFromFile("query_dump/materialized-view/mock_mv_on_mv1"),
                        null, TExplainLevel.NORMAL);
<<<<<<< HEAD
        Assert.assertTrue(replayPair.second.contains("tbl_mock_017"));
=======
        assertContains(replayPair.second, "tbl_mock_017");
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
        FeConstants.isReplayFromQueryDump = false;
    }

    @Test
    public void testMVOnMV2() throws Exception {
        connectContext.getSessionVariable().setMaterializedViewRewriteMode("force");
<<<<<<< HEAD
        Pair<QueryDumpInfo, String> replayPair =
                getPlanFragment(getDumpInfoFromFile("query_dump/materialized-view/mv_on_mv2"),
                        connectContext.getSessionVariable(), TExplainLevel.NORMAL);
        Assert.assertTrue(replayPair.second.contains("test_mv2"));
        connectContext.getSessionVariable().setMaterializedViewRewriteMode("default");
=======
        // TODO: How to remove the join reorder noise?
        connectContext.getSessionVariable().disableJoinReorder();
        Pair<QueryDumpInfo, String> replayPair =
                getPlanFragment(getDumpInfoFromFile("query_dump/materialized-view/mv_on_mv2"),
                        connectContext.getSessionVariable(), TExplainLevel.NORMAL);
        connectContext.getSessionVariable().setMaterializedViewRewriteMode("default");
        connectContext.getSessionVariable().enableJoinReorder();
        assertContains(replayPair.second, "test_mv2");
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
    }

    @Test
    public void testMV_AggWithHaving1() throws Exception {
        Pair<QueryDumpInfo, String> replayPair =
                getPlanFragment(getDumpInfoFromFile("query_dump/materialized-view/agg_with_having1"),
                        connectContext.getSessionVariable(), TExplainLevel.NORMAL);
<<<<<<< HEAD
        Assert.assertTrue(replayPair.second.contains("TEST_MV_2"));
=======
        assertContains(replayPair.second, "TEST_MV_2");
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
    }

    @Test
    public void testMV_AggWithHaving2() throws Exception {
        Pair<QueryDumpInfo, String> replayPair =
                getPlanFragment(getDumpInfoFromFile("query_dump/materialized-view/agg_with_having2"),
                        connectContext.getSessionVariable(), TExplainLevel.NORMAL);
<<<<<<< HEAD
        Assert.assertTrue(replayPair.second.contains("TEST_MV_2"));
=======
        assertContains(replayPair.second, "TEST_MV_2");
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
    }

    @Test
    public void testMV_AggWithHaving3() throws Exception {
        Pair<QueryDumpInfo, String> replayPair =
                getPlanFragment(getDumpInfoFromFile("query_dump/materialized-view/agg_with_having3"),
                        connectContext.getSessionVariable(), TExplainLevel.NORMAL);
<<<<<<< HEAD
        Assert.assertTrue(replayPair.second, replayPair.second.contains("TEST_MV_2"));
    }

    @Test
    @Ignore
=======
        assertContains(replayPair.second, "TEST_MV_2");
    }

    @Test
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
    public void testMock_MV_AggWithHaving3() throws Exception {
        Pair<QueryDumpInfo, String> replayPair =
                getPlanFragment(getDumpInfoFromFile("query_dump/materialized-view/mock_agg_with_having3"),
                        connectContext.getSessionVariable(), TExplainLevel.NORMAL);
<<<<<<< HEAD
        Assert.assertTrue(replayPair.second, replayPair.second.contains("tbl_mock_021"));
=======
        // Rewrite OK since rule based mv is enhanced
        assertContains(replayPair.second, "test_mv2");
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
    }

    @Test
    public void testMock_MV_CostBug() throws Exception {
        FeConstants.isReplayFromQueryDump = true;
        connectContext.getSessionVariable().setMaterializedViewRewriteMode("force");
<<<<<<< HEAD
        Tracers.register(connectContext);
        Tracers.init(connectContext, Tracers.Mode.LOGS, "ALL");
        Pair<QueryDumpInfo, String> replayPair =
                getPlanFragment(getDumpInfoFromFile("query_dump/materialized-view/mv_with_cost_bug1"),
                        connectContext.getSessionVariable(), TExplainLevel.NORMAL);
        Assert.assertTrue(replayPair.second, replayPair.second.contains("mv_35"));
        String pr = Tracers.printLogs();
        System.out.println(pr);
=======
        Pair<QueryDumpInfo, String> replayPair =
                getPlanFragment(getDumpInfoFromFile("query_dump/materialized-view/mv_with_cost_bug1"),
                        connectContext.getSessionVariable(), TExplainLevel.NORMAL);
        assertContains(replayPair.second, "mv_35");
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
        connectContext.getSessionVariable().setMaterializedViewRewriteMode("default");
        FeConstants.isReplayFromQueryDump = false;
    }

    @Test
    public void testMVWithDictRewrite() throws Exception {
        try {
            FeConstants.USE_MOCK_DICT_MANAGER = true;
            Pair<QueryDumpInfo, String> replayPair =
                    getCostPlanFragment(getDumpInfoFromFile("query_dump/tpch_query11_mv_rewrite"));
<<<<<<< HEAD
            assertContains(replayPair.second, "DictExpr(60: n_name,[<place-holder> = 'GERMANY'])");
=======
            assertContains(replayPair.second,
                    "DictDecode(78: n_name, [<place-holder> = 'GERMANY'])");
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
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
<<<<<<< HEAD
        Assert.assertFalse(replayPair.second, replayPair.second.contains("mv_tbl_mock_001"));
=======
        assertNotContains(replayPair.second, "mv_tbl_mock_001");
    }

    @Test
    public void testViewDeltaRewriter() throws Exception {
        QueryDebugOptions debugOptions = new QueryDebugOptions();
        debugOptions.setEnableQueryTraceLog(true);
        connectContext.getSessionVariable().setQueryDebugOptions(debugOptions.toString());
        Pair<QueryDumpInfo, String> replayPair =
                getPlanFragment(getDumpInfoFromFile("query_dump/view_delta"),
                        connectContext.getSessionVariable(), TExplainLevel.NORMAL);
        Assert.assertTrue(replayPair.second, replayPair.second.contains("mv_yyf_trade_water3"));
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
    }

    @Test
    public void testMV_CountStarRewrite() throws Exception {
        QueryDebugOptions debugOptions = new QueryDebugOptions();
        debugOptions.setEnableQueryTraceLog(true);
        connectContext.getSessionVariable().setQueryDebugOptions(debugOptions.toString());
        Pair<QueryDumpInfo, String> replayPair =
                getPlanFragment(getDumpInfoFromFile("query_dump/materialized-view/count_star_rewrite"),
                        connectContext.getSessionVariable(), TExplainLevel.NORMAL);
        assertContains(replayPair.second, "tbl_mock_067");
        // NOTE: OUTPUT EXPRS must refer to coalesce column ref
        assertContains(replayPair.second, " OUTPUT EXPRS:59: count\n" +
                "  PARTITION: RANDOM\n" +
                "\n" +
                "  RESULT SINK\n" +
                "\n" +
                "  3:Project\n" +
                "  |  <slot 59> : coalesce(80: count, 0)");
    }
<<<<<<< HEAD
}
=======
}
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
