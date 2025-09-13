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

import com.starrocks.common.FeConstants;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MVTestBase;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.MethodOrderer.MethodName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import static com.starrocks.sql.plan.PlanTestNoneDBBase.assertContains;
import static com.starrocks.sql.plan.PlanTestNoneDBBase.assertNotContains;

@TestMethodOrder(MethodName.class)
public class ReplayWithMVFromDumpTest extends ReplayFromDumpTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        ReplayFromDumpTestBase.beforeClass();
        UtFrameUtils.setDefaultConfigForAsyncMVTest(connectContext);
        // set default config for timeliness mvs
        UtFrameUtils.mockTimelinessForAsyncMVTest(connectContext);
        connectContext.getSessionVariable().setMaterializedViewRewriteMode("force");
        connectContext.getSessionVariable().setEnableViewBasedMvRewrite(true);
        FeConstants.isReplayFromQueryDump = true;
        MVTestBase.disableMVRewriteConsiderDataLayout();
    }

    @Test
    public void testMV_JoinAgg1() throws Exception {
        // Table and mv have no stats, mv rewrite is ok.
        String plan = getPlanFragment("query_dump/materialized-view/join_agg1", TExplainLevel.COSTS);
        assertContains(plan, "table: mv1, rollup: mv1");
    }

    @Test
    public void testMock_MV_JoinAgg1() throws Exception {
        // Table and mv have no stats, mv rewrite is ok.
        String plan = getPlanFragment("query_dump/materialized-view/mock_join_agg1", TExplainLevel.COSTS);
        // Rewrite OK when enhance rule based mv rewrite.
        assertContains(plan, "table: test_mv0, rollup: test_mv0");
    }

    @Test
    public void testMV_JoinAgg2() throws Exception {
        String plan = getPlanFragment("query_dump/materialized-view/join_agg2", TExplainLevel.COSTS);
        assertContains(plan, "table: mv1, rollup: mv1");
    }

    @Test
    public void testMV_JoinAgg3() throws Exception {
        // Table and mv have no stats, mv rewrite is ok.
        String plan =
                getPlanFragment("query_dump/materialized-view/join_agg3", TExplainLevel.NORMAL);
        assertContains(plan, "line_order_flat_mv");
    }

    @Test
    public void testMV_JoinAgg4() throws Exception {
        String plan =
                getPlanFragment("query_dump/materialized-view/join_agg4", TExplainLevel.NORMAL);
        assertContains(plan, "line_order_flat_mv");
    }

    @Test
    public void testMV_MVOnMV1() throws Exception {
        String plan = getPlanFragment("query_dump/materialized-view/mv_on_mv1", TExplainLevel.NORMAL);
        assertContains(plan, "mv2");
    }

    @Test
    public void testMock_MV_MVOnMV1() throws Exception {
        String plan = getPlanFragment("query_dump/materialized-view/mock_mv_on_mv1", TExplainLevel.NORMAL);
        assertContains(plan, "tbl_mock_017");
    }

    @Test
    public void testMVOnMV2() throws Exception {
        // TODO: How to remove the join reorder noise?
        connectContext.getSessionVariable().disableJoinReorder();
        String plan = getPlanFragment("query_dump/materialized-view/mv_on_mv2", TExplainLevel.NORMAL);
        connectContext.getSessionVariable().enableJoinReorder();
        assertContains(plan, "test_mv2");
    }

    @Test
    public void testMV_AggWithHaving1() throws Exception {
        String plan =
                getPlanFragment("query_dump/materialized-view/agg_with_having1", TExplainLevel.NORMAL);
        assertContains(plan, "TEST_MV_2");
    }

    @Test
    public void testMV_AggWithHaving2() throws Exception {
        String plan =
                getPlanFragment("query_dump/materialized-view/agg_with_having2", TExplainLevel.NORMAL);
        assertContains(plan, "TEST_MV_2");
    }

    @Test
    public void testMV_AggWithHaving3() throws Exception {
        String plan = getPlanFragment("query_dump/materialized-view/agg_with_having3", TExplainLevel.NORMAL);
        assertContains(plan, "TEST_MV_2");
    }

    @Test
    public void testMock_MV_AggWithHaving3() throws Exception {
        String plan = getPlanFragment("query_dump/materialized-view/mock_agg_with_having3", TExplainLevel.NORMAL);
        // Rewrite OK since rule based mv is enhanced
        assertContains(plan, "test_mv2");
    }

    @Test
    public void testMock_MV_CostBug() throws Exception {
        String plan = getPlanFragment("query_dump/materialized-view/mv_with_cost_bug1",
                TExplainLevel.NORMAL);
        assertContains(plan, "mv_35");
    }

    @Test
    public void testMVWithDictRewrite() throws Exception {
        try {
            FeConstants.USE_MOCK_DICT_MANAGER = true;
            String plan = getPlanFragment("query_dump/tpch_query11_mv_rewrite", TExplainLevel.COSTS);
            assertContains(plan, "DictDecode([79: n_name, INT, false], [<place-holder> = 'GERMANY'])");
        } finally {
            FeConstants.USE_MOCK_DICT_MANAGER = false;
        }
    }

    /**
     * Test synchronous materialized view rewrite with global dict optimization.
     */
    @Test
    public void testSyncMVRewriteWithDict() throws Exception {
        String plan = getPlanFragment("query_dump/materialized-view/mv_rewrite_with_dict_opt1",
                TExplainLevel.NORMAL);
        // TODO: support synchronous materialized view in query dump
        // String sql = "create materialized view mv_tbl_mock_001 " +
        //        "as select nmock_002, nmock_003, nmock_004, " +
        //        "nmock_005, nmock_006, nmock_007, nmock_008, nmock_009, nmock_010, " +
        //        "nmock_011, nmock_012, nmock_013, nmock_014, nmock_015, nmock_016, " +
        //        "nmock_017, nmock_018, nmock_019, nmock_020, nmock_021, nmock_022, nmock_023, " +
        //        "nmock_024, nmock_025, nmock_026, nmock_027, nmock_028, nmock_029, nmock_030, nmock_031, " +
        //        "nmock_032, nmock_033, nmock_034, nmock_035, nmock_036, nmock_037, nmock_038, nmock_039, " +
        //        "nmock_040, nmock_041 from tbl_mock_001 order by nmock_002;";
        assertNotContains(plan, "mv_tbl_mock_001");
    }

    @Test
    public void testViewDeltaRewriter() throws Exception {
        String plan = getPlanFragment("query_dump/view_delta", TExplainLevel.NORMAL);
        assertContains(plan, "mv_yyf_trade_water3");
    }

    @Test
    public void testMV_CountStarRewrite() throws Exception {
        String plan = getPlanFragment("query_dump/materialized-view/count_star_rewrite",
                TExplainLevel.NORMAL);
        assertContains(plan, "tbl_mock_067");
        // NOTE: OUTPUT EXPRS must refer to coalesce column ref
        assertContains(plan, " OUTPUT EXPRS:59: count\n" +
                "  PARTITION: RANDOM\n" +
                "\n" +
                "  RESULT SINK\n" +
                "\n" +
                "  3:Project\n" +
                "  |  <slot 59> : coalesce(80: count, 0)");
    }

    @Test
    public void testViewBasedRewrite1() throws Exception {
        String plan = getPlanFragment("query_dump/materialized-view/view_based_rewrite1", TExplainLevel.NORMAL);
        PlanTestBase.assertContains(plan, "tbl_mock_255", "MaterializedView: true");
    }

    @Test
    public void testViewBasedRewrite2() throws Exception {
        String plan = getPlanFragment("query_dump/materialized-view/view_based_rewrite2", TExplainLevel.NORMAL);
        PlanTestBase.assertContains(plan, "tbl_mock_239", "MaterializedView: true");
    }

    @Test
    public void testViewBasedRewrite3() throws Exception {
        String plan = getPlanFragment("query_dump/view_based_rewrite1", TExplainLevel.NORMAL);
        PlanTestBase.assertContains(plan, "single_mv_ads_biz_customer_combine_td_for_task_2y");
    }

    @Test
    public void testChooseBest() throws Exception {
        String plan = getPlanFragment("query_dump/materialized-view/choose_best_mv1", TExplainLevel.NORMAL);
        PlanTestBase.assertContains(plan, "rocketview_v4", "rocketview_v4_mv2");
    }

    @Test
    public void testZ_AggPushDownRewriteBugs1() throws Exception {
        connectContext.getSessionVariable().setMaterializedViewRewriteMode("default");
        String plan = getPlanFragment("query_dump/materialized-view/mv_rewrite_bugs1", TExplainLevel.COSTS);
        assertContains(plan, "mv_dim_table1_1");
        assertContains(plan, "mv_fact_table1");
        assertContains(plan, "  14:Project\n" +
                "  |  output columns:\n" +
                "  |  179 <-> [209: sum, DOUBLE, true] / cast([210: sum, BIGINT, true] as DOUBLE)");
        connectContext.getSessionVariable().setMaterializedViewRewriteMode("force");
    }
}