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

import com.starrocks.catalog.View;
import com.starrocks.common.FeConstants;
import com.starrocks.common.Pair;
import com.starrocks.common.profile.Tracers;
import com.starrocks.qe.SessionVariable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.common.QueryDebugOptions;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.base.CTEProperty;
import com.starrocks.sql.optimizer.dump.QueryDumpInfo;
import com.starrocks.sql.optimizer.rule.RuleSet;
import com.starrocks.sql.optimizer.rule.transformation.JoinAssociativityRule;
import com.starrocks.sql.optimizer.transformer.MVTransformerContext;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.stream.Stream;

public class ReplayFromDumpTest extends ReplayFromDumpTestBase {
    @Test
    public void testForceRuleBasedRewrite() throws Exception {
        QueryDumpInfo queryDumpInfo = getDumpInfoFromJson(getDumpInfoFromFile("query_dump/force_rule_based_mv_rewrite"));
        SessionVariable sessionVariable = queryDumpInfo.getSessionVariable();
        sessionVariable.setEnableForceRuleBasedMvRewrite(true);
        Pair<QueryDumpInfo, String> replayPair =
                getCostPlanFragment(getDumpInfoFromFile("query_dump/force_rule_based_mv_rewrite"), sessionVariable);
        Assertions.assertTrue(replayPair.second.contains("partition_flat_consumptions_partition_drinks_dates"),
                replayPair.second);
    }

    @Test
    public void testForceRuleBasedRewriteMonth() throws Exception {
        QueryDumpInfo queryDumpInfo = getDumpInfoFromJson(getDumpInfoFromFile("query_dump/force_rule_based_mv_rewrite_month"));
        SessionVariable sessionVariable = queryDumpInfo.getSessionVariable();
        sessionVariable.setEnableForceRuleBasedMvRewrite(true);
        Pair<QueryDumpInfo, String> replayPair =
                getCostPlanFragment(getDumpInfoFromFile("query_dump/force_rule_based_mv_rewrite_month"), sessionVariable);
        Assertions.assertTrue(replayPair.second.contains("partition_flat_consumptions_partition_drinks_roll_month"),
                replayPair.second);
    }

    @Test
    public void testForceRuleBasedRewriteYear() throws Exception {
        QueryDumpInfo queryDumpInfo = getDumpInfoFromJson(getDumpInfoFromFile("query_dump/force_rule_based_mv_rewrite_year"));
        SessionVariable sessionVariable = queryDumpInfo.getSessionVariable();
        sessionVariable.setEnableForceRuleBasedMvRewrite(true);
        Pair<QueryDumpInfo, String> replayPair =
                getCostPlanFragment(getDumpInfoFromFile("query_dump/force_rule_based_mv_rewrite_year"), sessionVariable);
        Assertions.assertTrue(replayPair.second.contains("flat_consumptions_drinks_dates_roll_year"), replayPair.second);
    }

    @Test
    public void testTPCH17WithUseAnalytic() throws Exception {
        QueryDumpInfo queryDumpInfo = getDumpInfoFromJson(getDumpInfoFromFile("query_dump/tpch17"));
        SessionVariable sessionVariable = queryDumpInfo.getSessionVariable();
        sessionVariable.setNewPlanerAggStage(2);
        Pair<QueryDumpInfo, String> replayPair =
                getCostPlanFragment(getDumpInfoFromFile("query_dump/tpch17"), sessionVariable);
        Assertions.assertTrue(replayPair.second.contains("  8:ANALYTIC\n" +
                "  |  functions: [, avg[([5: L_QUANTITY, DOUBLE, false]); args: DOUBLE; " +
                "result: DOUBLE; args nullable: false; result nullable: true], ]\n" +
                "  |  partition by: [18: P_PARTKEY, INT, false]"), replayPair.second);
    }

    @Test
    public void testReplyOnlineCase_JoinEliminateNulls() throws Exception {
        QueryDumpInfo queryDumpInfo = getDumpInfoFromJson(getDumpInfoFromFile("query_dump/join_eliminate_nulls"));
        SessionVariable sessionVariable = queryDumpInfo.getSessionVariable();
        sessionVariable.setNewPlanerAggStage(2);
        Pair<QueryDumpInfo, String> replayPair =
                getCostPlanFragment(getDumpInfoFromFile("query_dump/join_eliminate_nulls"), sessionVariable);
        Assertions.assertTrue(replayPair.second.contains("11:NESTLOOP JOIN\n" +
                "  |  join op: INNER JOIN\n" +
                "  |  other join predicates: CASE [174: type, VARCHAR, true] WHEN '1' THEN concat[('ocms_', [90: name, VARCHAR,"
                + " true]); args: VARCHAR; result: VARCHAR; args nullable: true; result nullable: true] = 'ocms_fengyang56' "
                + "WHEN '0' THEN TRUE ELSE FALSE END\n"
                + "  |  limit: 10"), replayPair.second);
        Assertions.assertTrue(replayPair.second.contains("  4:HASH JOIN\n"
                + "  |  join op: RIGHT OUTER JOIN (PARTITIONED)\n"
                + "  |  equal join conjunct: [tid, BIGINT, true] = [5: customer_id, BIGINT, true]\n"
                + "  |  build runtime filters:\n"
                + "  |  - filter_id = 0, build_expr = (5: customer_id), remote = true"), replayPair.second);
        sessionVariable.setNewPlanerAggStage(0);
    }

    @Test
    public void testReplayTPCDS02() throws Exception {
        Pair<QueryDumpInfo, String> replayPair = getCostPlanFragment(getDumpInfoFromFile("query_dump/tpcds02"));
        SessionVariable replaySessionVariable = replayPair.first.getSessionVariable();
        Assertions.assertEquals(replaySessionVariable.getParallelExecInstanceNum(), 4);
        Assertions.assertTrue(replayPair.second.contains("  |----24:EXCHANGE\n" +
                "  |       distribution type: BROADCAST\n" +
                "  |       cardinality: 65744\n" +
                "  |    \n" +
                "  18:UNION\n" +
                "  |  output exprs:\n" +
                "  |      [321, INT, true] | [322, DECIMAL64(7,2), true]\n" +
                "  |  child exprs:\n" +
                "  |      [255: ws_sold_date_sk, INT, true] | [276: ws_ext_sales_price, DECIMAL64(7,2), true]\n" +
                "  |      [289: cs_sold_date_sk, INT, true] | [310: cs_ext_sales_price, DECIMAL64(7,2), true]\n" +
                "  |  pass-through-operands: all\n" +
                "  |  cardinality: 194398472\n" +
                "  |  column statistics: \n" +
                "  |  * ws_sold_date_sk-->[-Infinity, Infinity, 0.0, 1.0, 1.0] UNKNOWN\n" +
                "  |  * ws_ext_sales_price-->[-Infinity, Infinity, 0.0, 1.0, 1.0] UNKNOWN"), replayPair.second);
    }

    @Test
    public void testSSB10() throws Exception {
        Pair<QueryDumpInfo, String> replayPair = getCostPlanFragment(getDumpInfoFromFile("query_dump/ssb10"));
        Assertions.assertTrue(replayPair.second.contains("  14:Project\n" +
                "  |  output columns:\n" +
                "  |  13 <-> [13: lo_revenue, INT, false]\n" +
                "  |  22 <-> [22: d_year, INT, false]\n" +
                "  |  38 <-> [38: c_city, VARCHAR, false]\n" +
                "  |  46 <-> [46: s_city, VARCHAR, false]\n" +
                "  |  cardinality: 28532"), replayPair.second);
        Assertions.assertTrue(replayPair.second.contains("  |----7:EXCHANGE\n" +
                "  |       distribution type: BROADCAST\n" +
                "  |       cardinality: 30"), replayPair.second);
    }

    @Test
    public void testTPCDS54() throws Exception {
        Pair<QueryDumpInfo, String> replayPair = getCostPlanFragment(getDumpInfoFromFile("query_dump/tpcds54"));
        // Check the size of the left and right tables
        Assertions.assertTrue(replayPair.second.contains("51:NESTLOOP JOIN\n" +
                "  |  join op: INNER JOIN\n" +
                "  |  other join predicates: cast([208: d_month_seq, INT, true] as BIGINT) <= [291: expr, BIGINT, " +
                "true]\n" +
                "  |  cardinality: 18262\n" +
                "  |  column statistics: \n" +
                "  |  * d_date_sk-->[2415022.0, 2488070.0, 0.0, 4.0, 18262.25] ESTIMATE\n" +
                "  |  * d_month_seq-->[0.0, 2400.0, 0.0, 4.0, 2398.0] ESTIMATE\n" +
                "  |  * expr-->[3.0, 2403.0, 0.0, 8.0, 30.135726072607262] ESTIMATE\n"), replayPair.second);
        Assertions.assertTrue(replayPair.second.contains("  |----18:EXCHANGE\n" +
                "  |       distribution type: SHUFFLE\n" +
                "  |       partition exprs: [70: cs_bill_customer_sk, INT, true]\n" +
                "  |       cardinality: 6304\n" +
                "  |    \n" +
                "  2:OlapScanNode"), replayPair.second);
    }

    @Test
    public void testTPCDS23_1() throws Exception {
        Pair<QueryDumpInfo, String> replayPair =
                getPlanFragment(getDumpInfoFromFile("query_dump/tpcds23_1"), null, TExplainLevel.NORMAL);
        Assertions.assertTrue(replayPair.second.contains("MultiCastDataSinks\n"
                        + "  STREAM DATA SINK\n"
                        + "    EXCHANGE ID: 55\n"
                        + "    RANDOM\n"
                        + "  STREAM DATA SINK\n"
                        + "    EXCHANGE ID: 76\n"
                        + "    RANDOM\n"
                        + "\n"
                        + "  39:Project\n"
                        + "  |  <slot 99> : 99: c_customer_sk\n"
                        + "  |  \n"
                        + "  38:NESTLOOP JOIN\n"
                        + "  |  join op: INNER JOIN\n"
                        + "  |  colocate: false, reason: \n"
                        + "  |  other join predicates: CAST(118: sum AS DOUBLE) > CAST(0.5 * 190: max AS DOUBLE)"),
                replayPair.second);
    }

    @Test
    public void testGroupByLimit() throws Exception {
        // check can generate 1 phase with limit 1
        // This test has column statistics and accurate table row count
        SessionVariable sessionVariable = GlobalStateMgr.getCurrentState().getVariableMgr().newSessionVariable();
        sessionVariable.setNewPlanerAggStage(1);
        Pair<QueryDumpInfo, String> replayPair =
                getCostPlanFragment(getDumpInfoFromFile("query_dump/groupby_limit"), sessionVariable);
        Assertions.assertTrue(replayPair.second.contains(
                "aggregate: multi_distinct_count[([1: LO_ORDERKEY, INT, false])"), replayPair.second);
    }

    @Test
    public void testTPCDS78() throws Exception {
        // check outer join with isNull predicate on inner table
        // The estimate cardinality of join should not be 0.
        Pair<QueryDumpInfo, String> replayPair = getCostPlanFragment(getDumpInfoFromFile("query_dump/tpcds78"));
        Assertions.assertTrue(replayPair.second.contains("3:HASH JOIN\n"
                + "  |  join op: LEFT OUTER JOIN (BUCKET_SHUFFLE)\n"
                + "  |  equal join conjunct: [2: ss_ticket_number, INT, false] = [25: sr_ticket_number, INT, true]\n"
                + "  |  equal join conjunct: [1: ss_item_sk, INT, false] = [24: sr_item_sk, INT, true]\n"
                + "  |  other predicates: [25: sr_ticket_number, INT, true] IS NULL\n"
                + "  |  output columns: 1, 3, 5, 11, 12, 14\n"
                + "  |  cardinality: 37372757"), replayPair.second);
        Assertions.assertTrue(replayPair.second.contains("15:HASH JOIN\n"
                + "  |  join op: LEFT OUTER JOIN (BUCKET_SHUFFLE)\n"
                + "  |  equal join conjunct: [76: ws_order_number, INT, false] = [110: wr_order_number, INT, true]\n"
                + "  |  equal join conjunct: [75: ws_item_sk, INT, false] = [109: wr_item_sk, INT, true]\n"
                + "  |  other predicates: [110: wr_order_number, INT, true] IS NULL\n"
                + "  |  output columns: 75, 77, 80, 93, 94, 96\n"
                + "  |  cardinality: 7914602"), replayPair.second);
    }

    @Test
    public void testTPCDS94() throws Exception {
        Pair<QueryDumpInfo, String> replayPair = getCostPlanFragment(getDumpInfoFromFile("query_dump/tpcds94"));
        // check ANTI JOIN cardinality is not 0
        Assertions.assertTrue(replayPair.second.contains("21:HASH JOIN\n" +
                "  |    |  join op: RIGHT ANTI JOIN (PARTITIONED)\n" +
                "  |    |  equal join conjunct: [138: wr_order_number, INT, false] = [2: ws_order_number, INT, " +
                "false]\n" +
                "  |    |  build runtime filters:\n" +
                "  |    |  - filter_id = 3, build_expr = (2: ws_order_number), remote = true\n" +
                "  |    |  output columns: 2, 17, 29, 34\n" +
                "  |    |  cardinality: 26765"), replayPair.second);
        Assertions.assertTrue(replayPair.second.contains("23:HASH JOIN\n" +
                "  |  join op: RIGHT SEMI JOIN (BUCKET_SHUFFLE(S))\n" +
                "  |  equal join conjunct: [103: ws_order_number, INT, false] = [2: ws_order_number, INT, false]\n" +
                "  |  other join predicates: [17: ws_warehouse_sk, INT, true] != [118: ws_warehouse_sk, INT, true]\n" +
                "  |  build runtime filters:"), replayPair.second);
    }

    @Test
    public void testTPCDS22() throws Exception {
        Pair<QueryDumpInfo, String> replayPair = getCostPlanFragment(getDumpInfoFromFile("query_dump/tpcds22"));
        // check d_date_sk distinct values has adjusted according to the cardinality
        Assertions.assertTrue(replayPair.second.contains("  4:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BROADCAST)\n" +
                "  |  equal join conjunct: [1: inv_date_sk, INT, false] = [5: d_date_sk, INT, false]\n" +
                "  |  build runtime filters:\n" +
                "  |  - filter_id = 0, build_expr = (5: d_date_sk), remote = false\n" +
                "  |  output columns: 2, 4\n" +
                "  |  cardinality: 399330000\n" +
                "  |  column statistics: \n" +
                "  |  * inv_date_sk-->[2450815.0, 2452635.0, 0.0, 4.0, 260.0] ESTIMATE\n" +
                "  |  * inv_item_sk-->[1.0, 204000.0, 0.0, 4.0, 200414.0] ESTIMATE\n" +
                "  |  * inv_quantity_on_hand-->[0.0, 1000.0, 0.05000724964315228, 4.0, 1006.0] ESTIMATE\n" +
                "  |  * d_date_sk-->[2450815.0, 2452635.0, 0.0, 4.0, 260.0] ESTIMATE\n" +
                "  |  \n" +
                "  |----3:EXCHANGE\n" +
                "  |       distribution type: BROADCAST\n" +
                "  |       cardinality: 335"), replayPair.second);
    }

    @Test
    public void testTPCDS64() throws Exception {
        Pair<QueryDumpInfo, String> replayPair =
                getPlanFragment(getDumpInfoFromFile("query_dump/tpcds64"), null, TExplainLevel.NORMAL);
        Assertions.assertTrue(replayPair.second.contains("  86:SELECT\n" +
                "  |  predicates: 457: d_year = 1999"), replayPair.second);
    }

    @Test
    public void testCrossReorder() throws Exception {
        connectContext.getSessionVariable().setEnableLocalShuffleAgg(false);
        RuleSet mockRule = new RuleSet() {
            @Override
            public void addJoinTransformationRules() {
                this.getTransformRules().clear();
                this.getTransformRules().add(JoinAssociativityRule.INNER_JOIN_ASSOCIATIVITY_RULE);
            }
        };

        new MockUp<OptimizerContext>() {
            @Mock
            public RuleSet getRuleSet() {
                return mockRule;
            }
        };

        Pair<QueryDumpInfo, String> replayPair =
                getPlanFragment(getDumpInfoFromFile("query_dump/cross_reorder"), null, TExplainLevel.NORMAL);
        Assertions.assertTrue(replayPair.second.contains("  13:NESTLOOP JOIN\n" +
                "  |  join op: INNER JOIN\n" +
                "  |  colocate: false, reason: \n" +
                "  |  other join predicates: CAST(CASE WHEN CAST(6: v3 AS BOOLEAN) THEN CAST(11: v2 AS VARCHAR) " +
                "WHEN CAST(3: v3 AS BOOLEAN) THEN '123' ELSE CAST(12: v3 AS VARCHAR) END AS DOUBLE) > " +
                        "1.0, (CAST(2: v2 AS DECIMAL128(38,9)) = CAST(8: v2 AS DECIMAL128(38,9))) OR (3: v3 = 8: v2)\n"),
                replayPair.second);
        connectContext.getSessionVariable().setEnableLocalShuffleAgg(true);
    }

    @Test
    public void testJoinReorderPushColumnsNoHandleProject() throws Exception {
        Pair<QueryDumpInfo, String> replayPair =
                getPlanFragment(getDumpInfoFromFile("query_dump/join_reorder"), null, TExplainLevel.NORMAL);
        Assertions.assertTrue(replayPair.second.contains("  |  <slot 40> : CAST(15: id_smallint AS INT)\n" +
                        "  |  <slot 41> : CAST(23: id_date AS DATETIME)\n" +
                        "  |  \n" +
                        "  6:OlapScanNode\n" +
                        "     TABLE: external_es_table_without_null"),
                replayPair.second);
    }

    @Test
    public void testMultiCountDistinct() throws Exception {
        Pair<QueryDumpInfo, String> replayPair =
                getPlanFragment(getDumpInfoFromFile("query_dump/multi_count_distinct"), null, TExplainLevel.NORMAL);
        String plan = replayPair.second;
        Assertions.assertTrue(plan.contains("AGGREGATE (update serialize)\n" +
                "  |  STREAMING\n" +
                "  |  output: multi_distinct_count(6: order_id), multi_distinct_count(11: delivery_phone)," +
                " multi_distinct_count(128: case), max(103: count)\n" +
                "  |  group by: 40: city, 116: division_en, 104: department, 106: category, 126: concat, " +
                "127: concat, 9: upc, 108: upc_desc"), plan);
    }

    @Test
    public void testDecodeLimitWithProject() throws Exception {
        FeConstants.USE_MOCK_DICT_MANAGER = true;
        Pair<QueryDumpInfo, String> replayPair =
                getPlanFragment(getDumpInfoFromFile("query_dump/decode_limit_with_project"), null,
                        TExplainLevel.NORMAL);
        String plan = replayPair.second;
        Assertions.assertTrue(plan.contains(" 11:Decode\n" +
                "  |  <dict id 41> : <string id 18>\n" +
                "  |  <dict id 42> : <string id 23>"), plan);
        FeConstants.USE_MOCK_DICT_MANAGER = false;
    }

    @Test
    public void testCountDistinctWithLimit() throws Exception {
        Pair<QueryDumpInfo, String> replayPair =
                getPlanFragment(getDumpInfoFromFile("query_dump/count_distinct_limit"), null, TExplainLevel.NORMAL);
        Assertions.assertTrue(replayPair.second.contains("1:AGGREGATE (update serialize)\n" +
                "  |  STREAMING\n" +
                "  |  group by: 5: lo_suppkey, 10: lo_extendedprice, 13: lo_revenue"), replayPair.second);
        Assertions.assertTrue(replayPair.second.contains("4:AGGREGATE (update finalize)\n" +
                "  |  output: count(5: lo_suppkey)\n" +
                "  |  group by: 10: lo_extendedprice, 13: lo_revenue\n" +
                "  |  limit: 1"), replayPair.second);
    }

    @Test
    public void testEighteenTablesJoin() throws Exception {
        Pair<QueryDumpInfo, String> replayPair =
                getPlanFragment(getDumpInfoFromFile("query_dump/eighteen_tables_join"), null, TExplainLevel.NORMAL);
        // check optimizer finish task
        Assertions.assertTrue(replayPair.second.contains("52:HASH JOIN\n" +
                "  |  join op: INNER JOIN (COLOCATE)"), replayPair.second);
    }

    @Test
    public void testLocalAggregateWithoutTableRowCount() throws Exception {
        Pair<QueryDumpInfo, String> replayPair =
                getPlanFragment(getDumpInfoFromFile("query_dump/local_agg_without_table_rowcount"), null,
                        TExplainLevel.NORMAL);
        // check local aggregate
        Assertions.assertTrue(replayPair.second.contains("1:AGGREGATE (update finalize)\n" +
                "  |  output: multi_distinct_count(4: lo_partkey)"), replayPair.second);
    }

    @Test
    public void testLogicalAggWithOneTablet() throws Exception {
        Pair<QueryDumpInfo, String> replayPair =
                getPlanFragment(getDumpInfoFromFile("query_dump/local_agg_with_one_tablet"), null,
                        TExplainLevel.NORMAL);
        Assertions.assertTrue(replayPair.second.contains("1:AGGREGATE (update finalize)\n" +
                "  |  output: multi_distinct_count(4: t0d)"), replayPair.second);
    }

    @Test
    public void testSelectSubqueryWithMultiJoin() throws Exception {
        Pair<QueryDumpInfo, String> replayPair =
                getPlanFragment(getDumpInfoFromFile("query_dump/select_sbuquery_with_multi_join"), null,
                        TExplainLevel.NORMAL);
        Assertions.assertTrue(replayPair.second.contains("13:Project\n"
                + "  |  <slot 31> : bitmap_and(20: bitmap_agg, 27: bitmap_agg)\n"
                + "  |  \n"
                + "  12:NESTLOOP JOIN\n"
                + "  |  join op: CROSS JOIN\n"
                + "  |  colocate: false, reason: \n"
                + "  |  \n"
                + "  |----11:EXCHANGE\n"
                + "  |    \n"
                + "  5:Project\n"
                + "  |  <slot 20> : 17: bitmap_agg"), replayPair.second);
    }

    @Test
    public void testTPCHRandom() throws Exception {
        Pair<QueryDumpInfo, String> replayPair =
                getPlanFragment(getDumpInfoFromFile("query_dump/tpch_random"), null, TExplainLevel.NORMAL);
        // check optimizer could extract best plan
        Assertions.assertTrue(replayPair.second.contains("15:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BUCKET_SHUFFLE)"), replayPair.second);
    }

    @Test
    public void testInsertWithView() throws Exception {
        Pair<QueryDumpInfo, String> replayPair =
                getPlanFragment(getDumpInfoFromFile("query_dump/insert_view"), null, TExplainLevel.NORMAL);
        Assertions.assertTrue(UtFrameUtils.matchPlanWithoutId(" 2:Project\n" +
                "  |  <slot 2> : 2: t2_c2\n" +
                "  |  <slot 11> : CAST(CAST(1: t2_c1 AS BIGINT) + 1 AS INT)", replayPair.second), replayPair.second);
        Assertions.assertTrue(replayPair.second.contains("OLAP TABLE SINK"), replayPair.second);
    }

    @Test
    public void testMergeGroupWithDeleteBestExpression() throws Exception {
        Pair<QueryDumpInfo, String> replayPair =
                getPlanFragment(getDumpInfoFromFile("query_dump/merge_group_delete_best_expression"), null,
                        TExplainLevel.NORMAL);
        // check without exception
        Assertions.assertTrue(replayPair.second.contains("14:HASH JOIN\n" +
                "  |  join op: INNER JOIN (PARTITIONED)"), replayPair.second);
    }

    @Test
    public void testJoinReOrderPruneColumns() throws Exception {
        Pair<QueryDumpInfo, String> replayPair =
                getPlanFragment(getDumpInfoFromFile("query_dump/join_reorder_prune_columns"), null,
                        TExplainLevel.NORMAL);
        // check without exception
        Assertions.assertTrue(replayPair.second.contains("<slot 186> : 186: S_SUPPKEY"), replayPair.second);
    }

    @Test
    public void testMultiViewWithDbName() throws Exception {
        Pair<QueryDumpInfo, String> replayPair =
                getPlanFragment(getDumpInfoFromFile("query_dump/multi_view_with_db"), null, TExplainLevel.NORMAL);
        Assertions.assertTrue(replayPair.second.contains(" 0:OlapScanNode\n" +
                "     TABLE: t3"), replayPair.second);
    }

    @Test
    public void testMultiViewCrossJoin() throws Exception {
        Pair<QueryDumpInfo, String> replayPair =
                getPlanFragment(getDumpInfoFromFile("query_dump/multi_view_cross_join"), null, TExplainLevel.NORMAL);
        // check without exception
        Assertions.assertTrue(replayPair.second.contains("40:Project\n" +
                "  |  <slot 1> : 1: c_0_0"), replayPair.second);
    }

    @Test
    public void testMultiViewPruneColumns() throws Exception {
        Pair<QueryDumpInfo, String> replayPair =
                getPlanFragment(getDumpInfoFromFile("query_dump/multi_view_prune_columns"), null, TExplainLevel.NORMAL);
        // check without exception
        Assertions.assertTrue(replayPair.second.contains("<slot 1> : 1: c_1_0"), replayPair.second);
    }

    @Test
    public void testCorrelatedSubqueryWithEqualsExpressions() throws Exception {
        Pair<QueryDumpInfo, String> replayPair =
                getPlanFragment(getDumpInfoFromFile("query_dump/correlated_subquery_with_equals_expression"), null,
                        TExplainLevel.NORMAL);
        Assertions.assertTrue(replayPair.second.contains("  22:NESTLOOP JOIN\n" +
                "  |  join op: INNER JOIN\n" +
                "  |  colocate: false, reason: \n" +
                "  |  other join predicates: if(19: c_0_0 != 1: c_0_0, 4: c_0_3, 20: c_0_3) = '1969-12-28', " +
                "if(((1: c_0_0 IS NULL) AND (NOT ((21: countRows IS NULL) OR (21: countRows = 0)))) OR " +
                "((22: countNotNulls < 21: countRows) AND (((NOT ((21: countRows IS NULL) OR (21: countRows = 0))) " +
                "AND (1: c_0_0 IS NOT NULL)) AND (16: c_0_0 IS NULL))), TRUE, FALSE)\n"), replayPair.second);
        Assertions.assertTrue(replayPair.second.contains("  20:HASH JOIN\n" +
                "  |  join op: LEFT OUTER JOIN (PARTITIONED)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 1: c_0_0 = 16: c_0_0\n" +
                "  |  other join predicates: if(16: c_0_0 != 1: c_0_0, 4: c_0_3, 17: c_0_3) = '1969-12-28'"), replayPair.second);
    }

    @Test
    public void testGatherWindowCTE2() throws Exception {
        Pair<QueryDumpInfo, String> replayPair =
                getPlanFragment(getDumpInfoFromFile("query_dump/gather_window_cte"), null, TExplainLevel.COSTS);
        Assertions.assertTrue(UtFrameUtils.matchPlanWithoutId("  0:UNION\n" +
                "  |  output exprs:\n" +
                "  |      [16, DATE, false] | [17, BIGINT, true] | [18, DECIMAL128(27,19), true]\n" +
                "  |  child exprs:\n" +
                "  |      [2: c_0_0, DATE, false] | [7: row_number(), BIGINT, true] " +
                "| [8: last_value(4: c_0_2), DECIMAL128(27,19), true]\n" +
                "  |      [9: c_0_0, DATE, false] | [14: row_number(), BIGINT, true] " +
                "| [15: last_value(11: c_0_2), DECIMAL128(27,19), true]", replayPair.second), replayPair.second);
    }

    @Test
    public void testMultiSubqueries() throws Exception {
        Pair<QueryDumpInfo, String> replayPair =
                getPlanFragment(getDumpInfoFromFile("query_dump/subquery_statistics"), null, TExplainLevel.COSTS);
        Assertions.assertTrue(replayPair.second.contains("  96:AGGREGATE (update serialize)\n" +
                "  |  aggregate: count[(*); args: ; result: BIGINT; args nullable: false; result nullable: false]\n" +
                "  |  hasNullableGenerateChild: true\n" +
                "  |  cardinality: 1\n" +
                "  |  column statistics: \n" +
                "  |  * count-->[0.0, 1.0420273298435367, 0.0, 8.0, 1.0] ESTIMATE\n" +
                "  |  \n" +
                "  95:Project\n" +
                "  |  output columns:\n" +
                "  |  549 <-> 1\n" +
                "  |  hasNullableGenerateChild: true\n" +
                "  |  cardinality: 1\n" +
                "  |  column statistics: \n" +
                "  |  * auto_fill_col-->[1.0, 1.0, 0.0, 1.0, 1.0] ESTIMATE"), replayPair.second);
    }

    @Test
    public void testCorrelatedPredicateRewrite() throws Exception {
        connectContext.getSessionVariable().setSemiJoinDeduplicateMode(-1);
        connectContext.getSessionVariable().setEnableInnerJoinToSemi(false);
        Pair<QueryDumpInfo, String> replayPair =
                getPlanFragment(getDumpInfoFromFile("query_dump/union_with_subquery"), null, TExplainLevel.COSTS);
        Assertions.assertTrue(replayPair.second.contains("1201:HASH JOIN\n" +
                "  |  join op: RIGHT OUTER JOIN (BUCKET_SHUFFLE(S))\n" +
                "  |  equal join conjunct: [3802: ref_id, BIGINT, true] = [3681: customer_id, BIGINT, true]"), replayPair.second);
    }

    @Test
    public void testGroupByDistinctColumnSkewHint() throws Exception {
        Pair<QueryDumpInfo, String> replayPair =
                getPlanFragment(getDumpInfoFromFile("query_dump/group_by_count_distinct_skew_hint"), null,
                        TExplainLevel.NORMAL);
        Assertions.assertTrue(
                replayPair.second.contains("CAST(murmur_hash3_32(CAST(42: case AS VARCHAR)) % 512 AS SMALLINT)"),
                replayPair.second);
    }

    @Test
    public void testGroupByDistinctColumnOptimization() throws Exception {
        Pair<QueryDumpInfo, String> replayPair =
                getPlanFragment(getDumpInfoFromFile("query_dump/group_by_count_distinct_optimize"), null,
                        TExplainLevel.NORMAL);
        Assertions.assertTrue(
                replayPair.second.contains("CAST(murmur_hash3_32(CAST(42: case AS VARCHAR)) % 512 AS SMALLINT)"),
                replayPair.second);
    }

    @Test
    public void testPushDownDistinctAggBelowWindowRewrite() throws Exception {
        Pair<QueryDumpInfo, String> replayPair =
                getPlanFragment(getDumpInfoFromFile("query_dump/pushdown_distinct_agg_below_window"), null,
                        TExplainLevel.COSTS);
        Assertions.assertTrue(replayPair.second.contains("  1:AGGREGATE (update finalize)\n" +
                "  |  aggregate: sum[([3: gross, DECIMAL128(10,2), false]); args: DECIMAL128; " +
                "result: DECIMAL128(38,2); args nullable: false; result nullable: true]\n" +
                "  |  group by: [1: country, VARCHAR, true], [2: trans_date, DATE, false]\n" +
                "  |  cardinality: 49070\n"), replayPair.second);
    }

    @Test
    public void testSSBRightOuterJoinCase() throws Exception {
        Pair<QueryDumpInfo, String> replayPair =
                getPlanFragment(getDumpInfoFromFile("query_dump/right_outer_join_case"), null,
                        TExplainLevel.COSTS);
        Assertions.assertTrue(replayPair.second.contains("4:NESTLOOP JOIN\n" +
                "  |  join op: RIGHT OUTER JOIN"));
    }

    @Test
    public void testHiveTPCH02UsingResource() throws Exception {
        Pair<QueryDumpInfo, String> replayPair =
                getPlanFragment(getDumpInfoFromFile("query_dump/hive_tpch02_resource"), null, TExplainLevel.COSTS);
        Assertions.assertTrue(replayPair.second.contains("6:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BROADCAST)\n" +
                "  |  equal join conjunct: [24: n_regionkey, INT, true] = [26: r_regionkey, INT, true]\n" +
                "  |  build runtime filters:\n" +
                "  |  - filter_id = 0, build_expr = (26: r_regionkey), remote = false\n" +
                "  |  output columns: 22, 23\n" +
                "  |  cardinality: 23"), replayPair.second);
    }

    @Test
    public void testHiveTPCH05UsingResource() throws Exception {
        Pair<QueryDumpInfo, String> replayPair =
                getPlanFragment(getDumpInfoFromFile("query_dump/hive_tpch05_resource"), null, TExplainLevel.COSTS);
        Assertions.assertTrue(replayPair.second.contains("  20:HASH JOIN\n" +
                "  |  join op: INNER JOIN (PARTITIONED)\n" +
                "  |  equal join conjunct: [10: o_custkey, INT, true] = [1: c_custkey, INT, true]\n" +
                "  |  build runtime filters:\n" +
                "  |  - filter_id = 3, build_expr = (1: c_custkey), remote = false\n" +
                "  |  output columns: 4, 9\n" +
                "  |  cardinality: 22765073"), replayPair.second);
    }

    @Test
    public void testHiveTPCH08UsingResource() throws Exception {
        FeConstants.isReplayFromQueryDump = true;
        Pair<QueryDumpInfo, String> replayPair =
                getPlanFragment(getDumpInfoFromFile("query_dump/hive_tpch08_resource"), null, TExplainLevel.COSTS);
        Assertions.assertTrue(replayPair.second.contains("5:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BROADCAST)\n" +
                "  |  equal join conjunct: [52: n_regionkey, INT, true] = [58: r_regionkey, INT, true]\n" +
                "  |  build runtime filters:\n" +
                "  |  - filter_id = 0, build_expr = (58: r_regionkey), remote = false\n" +
                "  |  output columns: 50\n" +
                "  |  cardinality: 5"), replayPair.second);
        FeConstants.isReplayFromQueryDump = false;
    }

    @Test
    public void testHiveTPCH02UsingCatalog() throws Exception {
        FeConstants.isReplayFromQueryDump = true;
        Pair<QueryDumpInfo, String> replayPair =
                getPlanFragment(getDumpInfoFromFile("query_dump/hive_tpch02_catalog"), null,
                        TExplainLevel.NORMAL);
        Assertions.assertTrue(replayPair.second.contains("19:HASH JOIN\n" +
                "  |  join op: INNER JOIN (PARTITIONED)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 17: ps_partkey = 1: p_partkey"), replayPair.second);
        FeConstants.isReplayFromQueryDump = false;
    }

    @Test
    public void testHiveTPCH05UsingCatalog() throws Exception {
        FeConstants.isReplayFromQueryDump = true;
        Pair<QueryDumpInfo, String> replayPair =
                getPlanFragment(getDumpInfoFromFile("query_dump/hive_tpch05_catalog"), null, TExplainLevel.NORMAL);
        Assertions.assertTrue(replayPair.second.contains("15:HASH JOIN\n" +
                "  |  join op: INNER JOIN (PARTITIONED)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 20: l_suppkey = 34: s_suppkey\n" +
                "  |  equal join conjunct: 4: c_nationkey = 37: s_nationkey\n" +
                "  |  \n" +
                "  |----14:EXCHANGE\n" +
                "  |    \n" +
                "  12:EXCHANGE"), replayPair.second);
        FeConstants.isReplayFromQueryDump = false;
    }

    @Test
    public void testHiveTPCH08UsingCatalog() throws Exception {
        FeConstants.isReplayFromQueryDump = true;
        Pair<QueryDumpInfo, String> replayPair =
                getPlanFragment(getDumpInfoFromFile("query_dump/hive_tpch08_catalog"), null, TExplainLevel.NORMAL);
        Assertions.assertTrue(replayPair.second.contains(" 33:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BROADCAST)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 52: n_regionkey = 58: r_regionkey"), replayPair.second);
        FeConstants.isReplayFromQueryDump = false;
    }

    @Test
    public void testPruneCTEProperty() throws Exception {
        String jsonStr = getDumpInfoFromFile("query_dump/cte_reuse");
        connectContext.getSessionVariable().disableJoinReorder();
        Pair<String, ExecPlan> result = UtFrameUtils.getNewPlanAndFragmentFromDump(connectContext,
                getDumpInfoFromJson(jsonStr));
        OptExpression expression = result.second.getPhysicalPlan().inputAt(1);
        Assertions.assertEquals(new CTEProperty(1), expression.getLogicalProperty().getUsedCTEs());
        Assertions.assertEquals(4, result.second.getCteProduceFragments().size());
    }

    @Test
    public void testReduceJoinTransformation1() throws Exception {
        Pair<QueryDumpInfo, String> replayPair =
                getPlanFragment(getDumpInfoFromFile("query_dump/reduce_transformation_1"),
                        null, TExplainLevel.NORMAL);
        Assertions.assertTrue(replayPair.second.contains("33:AGGREGATE (update finalize)\n" +
                "  |  output: multi_distinct_count(212: case)\n" +
                "  |  group by: 34: cast, 33: cast, 38: handle, 135: concat, 136: case, 36: cast"), replayPair.second);
    }

    @Test
    public void testReduceJoinTransformation2() throws Exception {
        Pair<QueryDumpInfo, String> replayPair =
                getPlanFragment(getDumpInfoFromFile("query_dump/reduce_transformation_2"),
                        null, TExplainLevel.NORMAL);
        Assertions.assertTrue(replayPair.second.contains("38:HASH JOIN\n" +
                "  |  join op: LEFT OUTER JOIN (BROADCAST)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 398: substring = 361: date\n" +
                "  |  other join predicates: 209: zid = 298: zid"), replayPair.second);
    }

    @Test
    public void testReduceJoinTransformation3() throws Exception {
        Pair<QueryDumpInfo, String> replayPair =
                getPlanFragment(getDumpInfoFromFile("query_dump/reduce_transformation_3"),
                        null, TExplainLevel.NORMAL);
        Assertions.assertTrue(replayPair.second.contains("  25:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BUCKET_SHUFFLE)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 71: order_id = 2: orderid\n" +
                "  |  \n" +
                "  |----24:EXCHANGE"), replayPair.second);
    }

    @Test
    public void testUnionAllWithTopNRuntimeFilter() throws Exception {
        QueryDumpInfo queryDumpInfo =
                getDumpInfoFromJson(getDumpInfoFromFile("query_dump/union_all_with_topn_runtime_filter"));
        SessionVariable sessionVariable = queryDumpInfo.getSessionVariable();
        sessionVariable.setScanOrToUnionThreshold(-1);
        sessionVariable.setScanOrToUnionLimit(10);
        sessionVariable.setSelectRatioThreshold(20.0);
        Pair<QueryDumpInfo, String> replayPair =
                getPlanFragment(getDumpInfoFromFile("query_dump/union_all_with_topn_runtime_filter"),
                        sessionVariable, TExplainLevel.VERBOSE);
        String plan = replayPair.second;

        // tbl_mock_015
        Assertions.assertTrue(plan.contains("probe runtime filters:\n" +
                "     - filter_id = 4, probe_expr = (<slot 79> 79: mock_004)"), plan);
        Assertions.assertTrue(plan.contains("probe runtime filters:\n" +
                "     - filter_id = 3, probe_expr = (<slot 62> 62: mock_004)"), plan);

        // table: tbl_mock_001, rollup: tbl_mock_001
        Assertions.assertTrue(plan.contains("probe runtime filters:\n" +
                "     - filter_id = 1, probe_expr = (<slot 110> 110: mock_004)"), plan);
        Assertions.assertTrue(plan.contains("probe runtime filters:\n" +
                "     - filter_id = 0, probe_expr = (<slot 96> 96: mock_004)\n"), plan);

    }

    @Test
    public void testJoinWithArray() throws Exception {
        Pair<QueryDumpInfo, String> replayPair =
                getPlanFragment(getDumpInfoFromFile("query_dump/join_with_array"),
                        null, TExplainLevel.NORMAL);
        Assertions.assertTrue(replayPair.second.contains("  4:HASH JOIN\n" +
                "  |  join op: INNER JOIN (COLOCATE)\n" +
                "  |  colocate: true\n" +
                "  |  equal join conjunct: 64: mock_007 = 1: mock_007\n" +
                "  |  equal join conjunct: 108: any_value = 41: mock_008\n" +
                "  |  \n" +
                "  |----3:OlapScanNode\n" +
                "  |       TABLE: tbl_mock_024"), replayPair.second);
    }

    @Test
    public void testTwoStageAgg() throws Exception {
        Pair<QueryDumpInfo, String> replayPair =
                getPlanFragment(getDumpInfoFromFile("query_dump/two_stage_agg"),
                        null, TExplainLevel.COSTS);
        Assertions.assertTrue(replayPair.second.contains("1:AGGREGATE (update serialize)\n" +
                "  |  STREAMING"), replayPair.second);

        Assertions.assertTrue(replayPair.second.contains("0:OlapScanNode\n" +
                "     table: lineorder_2, rollup: lineorder_2"), replayPair.second);
    }

    @Test
    public void testPushDistinctAggDownWindow() throws Exception {
        Pair<QueryDumpInfo, String> replayPair =
                getPlanFragment(getDumpInfoFromFile("query_dump/pushdown_distinct_agg_below_window2"),
                        null, TExplainLevel.NORMAL);
        System.out.println(replayPair.second);
        Assertions.assertTrue(replayPair.second.contains("  3:ANALYTIC\n" +
                "  |  functions: [, sum(5: sum), ]\n" +
                "  |  partition by: 1: TIME\n" +
                "  |  \n" +
                "  2:SORT\n" +
                "  |  order by: <slot 1> 1: TIME ASC\n" +
                "  |  analytic partition by: 1: TIME\n" +
                "  |  offset: 0\n" +
                "  |  \n" +
                "  1:AGGREGATE (update finalize)\n" +
                "  |  output: sum(2: NUM)\n" +
                "  |  group by: 1: TIME"), replayPair.second);
    }

    @Test
    public void testNestedViewWithCTE() throws Exception {
        Pair<QueryDumpInfo, String> replayPair =
                getPlanFragment(getDumpInfoFromFile("query_dump/nested_view_with_cte"),
                        null, TExplainLevel.NORMAL);
        PlanTestBase.assertContains(replayPair.second, "Project\n" +
                "  |  <slot 7368> : 7368: count\n" +
                "  |  limit: 100");
        PlanTestBase.assertContains(replayPair.second, "AGGREGATE (merge finalize)\n" +
                "  |  output: count(7368: count)\n" +
                "  |  group by: 24: mock_038, 15: mock_003, 108: mock_109, 4: mock_005, 2: mock_110, 2134: case\n" +
                "  |  limit: 100");
    }

    @Test
    public void testRBOMvOnView() throws Exception {
        String dumpInfo = getDumpInfoFromFile("query_dump/mv_on_view");
        Pair<QueryDumpInfo, String> replayPair = getCostPlanFragment(dumpInfo);
        Assertions.assertTrue(replayPair.second.contains("mv_LEAF_ACC_CUBE_SHADOW_VIEW_fb70da80"), replayPair.second);
    }

    @Test
    public void testCBOMvOnView() throws Exception {
        String dumpInfo = getDumpInfoFromFile("query_dump/mv_on_view");
        QueryDumpInfo queryDumpInfo = getDumpInfoFromJson(dumpInfo);
        SessionVariable sessionVariable = queryDumpInfo.getSessionVariable();
        sessionVariable.setEnableCBOViewBasedMvRewrite(true);
        Pair<QueryDumpInfo, String> replayPair = getCostPlanFragment(dumpInfo, sessionVariable);
        Assertions.assertTrue(replayPair.second.contains("mv_LEAF_ACC_CUBE_SHADOW_VIEW_fb70da80"), replayPair.second);
        sessionVariable.setEnableCBOViewBasedMvRewrite(false);
    }

    @Test
    public void testCBONestedMvRewriteDrinks() throws Exception {
        QueryDumpInfo queryDumpInfo = getDumpInfoFromJson(getDumpInfoFromFile("query_dump/force_rule_based_mv_rewrite_drinks"));
        SessionVariable sessionVariable = queryDumpInfo.getSessionVariable();
        sessionVariable.setEnableForceRuleBasedMvRewrite(false);
        Pair<QueryDumpInfo, String> replayPair =
                getCostPlanFragment(getDumpInfoFromFile("query_dump/force_rule_based_mv_rewrite_drinks"), sessionVariable);
        Assertions.assertTrue(replayPair.second.contains("partition_flat_consumptions_partition_drinks"), replayPair.second);
    }

    @Test
    public void testCBONestedMvRewriteDates() throws Exception {
        QueryDumpInfo queryDumpInfo = getDumpInfoFromJson(getDumpInfoFromFile("query_dump/force_rule_based_mv_rewrite"));
        SessionVariable sessionVariable = queryDumpInfo.getSessionVariable();
        sessionVariable.setEnableForceRuleBasedMvRewrite(false);
        Pair<QueryDumpInfo, String> replayPair =
                getCostPlanFragment(getDumpInfoFromFile("query_dump/force_rule_based_mv_rewrite"), sessionVariable);
        Assertions.assertTrue(replayPair.second.contains("partition_flat_consumptions_partition_drinks_dates"),
                replayPair.second);
    }

    @Test
    public void testCBONestedMvRewriteMonth() throws Exception {
        QueryDumpInfo queryDumpInfo = getDumpInfoFromJson(getDumpInfoFromFile("query_dump/force_rule_based_mv_rewrite_month"));
        SessionVariable sessionVariable = queryDumpInfo.getSessionVariable();
        sessionVariable.setEnableForceRuleBasedMvRewrite(false);
        Pair<QueryDumpInfo, String> replayPair =
                getCostPlanFragment(getDumpInfoFromFile("query_dump/force_rule_based_mv_rewrite_month"), sessionVariable);
        Assertions.assertTrue(replayPair.second.contains("partition_flat_consumptions_partition_drinks_roll_month"),
                replayPair.second);
    }

    @Test
    public void testCBONestedMvRewriteYear() throws Exception {
        QueryDumpInfo queryDumpInfo = getDumpInfoFromJson(getDumpInfoFromFile("query_dump/force_rule_based_mv_rewrite_year"));
        SessionVariable sessionVariable = queryDumpInfo.getSessionVariable();
        sessionVariable.setEnableForceRuleBasedMvRewrite(false);
        Pair<QueryDumpInfo, String> replayPair =
                getCostPlanFragment(getDumpInfoFromFile("query_dump/force_rule_based_mv_rewrite_year"), sessionVariable);
        Assertions.assertTrue(replayPair.second.contains("flat_consumptions_drinks_dates_roll_year"), replayPair.second);
    }

    @Test
    public void testNormalizeNonTrivialProject() throws Exception {
        SessionVariable sv = new SessionVariable();
        sv.setPipelineDop(1);
        sv.setEnableQueryCache(true);
        try {
            FeConstants.USE_MOCK_DICT_MANAGER = true;
            sv.setEnableLowCardinalityOptimize(true);
            Pair<QueryDumpInfo, String> replayPair =
                    getPlanFragment(getDumpInfoFromFile("query_dump/normalize_non_trivial_project"), sv,
                            TExplainLevel.NORMAL);
            Assertions.assertTrue(replayPair.second != null && replayPair.second.contains("TABLE: tbl_mock_017"),
                    replayPair.second);
        } finally {
            FeConstants.USE_MOCK_DICT_MANAGER = false;
        }
    }

    @Test
    public void testListPartitionPrunerWithNEExpr() throws Exception {
        Pair<QueryDumpInfo, String> replayPair =
                getCostPlanFragment(getDumpInfoFromFile("query_dump/list_partition_prune_dump"));
        // partitions should not be pruned
        Assertions.assertTrue(!replayPair.second.contains("partitionsRatio=2/3, tabletsRatio=20/20"), replayPair.second);
        Assertions.assertTrue(replayPair.second.contains("0:OlapScanNode\n" +
                "     table: partitions2_keys311, rollup: partitions2_keys311\n" +
                "     preAggregation: on\n" +
                "     Predicates: [7: undef_signed_not_null, VARCHAR, false] != 'j'\n" +
                "     partitionsRatio=3/3, tabletsRatio=30/30"), replayPair.second);
    }

    @Test
    public void testTopNPushDownBelowUnionAll() throws Exception {
        Pair<QueryDumpInfo, String> replayPair =
                getPlanFragment(getDumpInfoFromFile("query_dump/topn_push_down_union"),
                        connectContext.getSessionVariable(), TExplainLevel.NORMAL);

        // Topn should be pushed down below union all and contains no duplicated ording columns
        PlanTestBase.assertContains(replayPair.second, "  26:TOP-N\n" +
                "  |  order by: <slot 240> 240: expr ASC, <slot 241> 241: cast DESC, <slot 206> 206: mock_025 DESC\n" +
                "  |  offset: 0\n" +
                "  |  limit: 200");
        PlanTestBase.assertContains(replayPair.second, "17:TOP-N\n" +
                "  |  order by: <slot 165> 165: cast ASC, <slot 153> 153: cast DESC, <slot 166> 166: expr ASC, " +
                "<slot 167> 167: cast DESC\n" +
                "  |  offset: 0\n" +
                "  |  limit: 200");
    }

    @Test
    public void testNoCTEOperatorPropertyDerived() throws Exception {
        Pair<QueryDumpInfo, String> replayPair =
                getPlanFragment(getDumpInfoFromFile("query_dump/no_cte_operator_test"),
                        null, TExplainLevel.NORMAL);
        Assertions.assertTrue(replayPair.second.contains("23:Project\n" +
                "  |  <slot 193> : 193: mock_081\n" +
                "  |  <slot 194> : 194: mock_089\n" +
                "  |  <slot 391> : 391: case\n" +
                "  |  <slot 396> : 396: rank()"), replayPair.second);
        Assertions.assertTrue(replayPair.second.contains(" 20:SORT\n" +
                "  |  order by: <slot 194> 194: mock_089 ASC," +
                " <slot 395> 395: case ASC, <slot 193> 193: mock_081 ASC, " +
                "<slot 233> 233: mock_065 ASC\n" +
                "  |  analytic partition by: 194: mock_089, 395: case, 193: mock_081\n" +
                "  |  offset: 0\n" +
                "  |  \n" +
                "  19:EXCHANGE"), replayPair.second);
    }

    @Test
    public void testTimeoutDeepJoinCostPrune() throws Exception {
        Tracers.register(connectContext);
        Tracers.init(connectContext, "TIMER", "optimizer");
        connectContext.getSessionVariable().setOptimizerExecuteTimeout(-1);

        Pair<QueryDumpInfo, String> replayPair =
                getPlanFragment(getDumpInfoFromFile("query_dump/deep_join_cost"),
                        connectContext.getSessionVariable(), TExplainLevel.NORMAL);
        String ss = Tracers.printScopeTimer();
        int start = ss.indexOf("EnforceAndCostTask[") + "EnforceAndCostTask[".length();
        int end = ss.indexOf("]", start);
        long count = Long.parseLong(ss.substring(start, end));
        Assertions.assertTrue(count < 10000, ss);
    }

    @Test
    public void testDistinctConstantRewrite() throws Exception {
        Pair<QueryDumpInfo, String> replayPair =
                getPlanFragment(getDumpInfoFromFile("query_dump/distinct_constant"),
                        connectContext.getSessionVariable(), TExplainLevel.NORMAL);
        Assertions.assertTrue(replayPair.second.contains("4:AGGREGATE (update serialize)\n" +
                "  |  output: multi_distinct_count(1)"), replayPair.second);
        Assertions.assertTrue(replayPair.second.contains("9:AGGREGATE (update serialize)\n" +
                "  |  output: multi_distinct_count(NULL)"), replayPair.second);
    }

    @Test
    public void testSplitOrderBy() throws Exception {
        Pair<QueryDumpInfo, String> replayPair =
                getPlanFragment(getDumpInfoFromFile("query_dump/split_order_by"),
                        null, TExplainLevel.NORMAL);
        Assertions.assertTrue(replayPair.second.contains("21:MERGING-EXCHANGE"), replayPair.second);
        Assertions.assertTrue(replayPair.second.contains("20:TOP-N"), replayPair.second);
        Assertions.assertTrue(replayPair.second.contains("15:MERGING-EXCHANGE"), replayPair.second);
        Assertions.assertTrue(replayPair.second.contains("14:TOP-N"), replayPair.second);

    }

    @Test
    public void testQueryCacheSetOperator() throws Exception {

        String savedSv = connectContext.getSessionVariable().getJsonString();
        try {
            connectContext.getSessionVariable().setEnableQueryCache(true);
            QueryDumpInfo dumpInfo = getDumpInfoFromJson(getDumpInfoFromFile("query_dump/query_cache_set_operator"));
            ExecPlan execPlan = UtFrameUtils.getPlanFragmentFromQueryDump(connectContext, dumpInfo);
            Assertions.assertTrue(execPlan.getFragments().stream().anyMatch(frag -> frag.getCacheParam() != null));
        } finally {
            connectContext.getSessionVariable().replayFromJson(savedSv);
        }
    }

    @Test
    @Disabled
    public void testQueryTimeout() {
        Assertions.assertThrows(StarRocksPlannerException.class,
                () -> getPlanFragment(getDumpInfoFromFile("query_dump/query_timeout"), null, TExplainLevel.NORMAL));
    }

    @Test
    public void testQueryCacheMisuseExogenousRuntimeFilter() throws Exception {
        String savedSv = connectContext.getSessionVariable().getJsonString();
        try {
            connectContext.getSessionVariable().setEnableQueryCache(true);
            QueryDumpInfo dumpInfo =
                    getDumpInfoFromJson(getDumpInfoFromFile("query_dump/query_cache_misuse_exogenous_runtime_filter"));
            ExecPlan execPlan = UtFrameUtils.getPlanFragmentFromQueryDump(connectContext, dumpInfo);
            Assertions.assertTrue(execPlan.getFragments().stream().noneMatch(frag -> frag.getCacheParam() != null));
            Assertions.assertTrue(
                    execPlan.getFragments().stream().anyMatch(frag -> !frag.getProbeRuntimeFilters().isEmpty()));
        } finally {
            connectContext.getSessionVariable().replayFromJson(savedSv);
        }
    }

    @Test
    public void testPruneTableNPE() throws Exception {
        String savedSv = connectContext.getSessionVariable().getJsonString();
        try {
            connectContext.getSessionVariable().setEnableCboTablePrune(true);
            connectContext.getSessionVariable().setEnableRboTablePrune(true);
            Pair<QueryDumpInfo, String> replayPair =
                    getPlanFragment(getDumpInfoFromFile("query_dump/prune_table_npe"),
                            null, TExplainLevel.NORMAL);
            long numHashJoins = Stream.of(replayPair.second.split("\n"))
                    .filter(ln -> ln.contains("HASH JOIN")).count();
            Assertions.assertEquals(numHashJoins, 2);
        } finally {
            connectContext.getSessionVariable().replayFromJson(savedSv);
        }
    }

    @Test
    public void testJoinInitError() throws Exception {
        Pair<QueryDumpInfo, String> replayPair =
                getCostPlanFragment(getDumpInfoFromFile("query_dump/join_init_error"));
        Assertions.assertTrue(replayPair.second.contains("HASH JOIN"), replayPair.second);
    }

    @Test
    public void testPushdownSubfield() throws Exception {
        String dumpString = getDumpInfoFromFile("query_dump/pushdown_subfield");
        QueryDumpInfo queryDumpInfo = getDumpInfoFromJson(dumpString);
        queryDumpInfo.getSessionVariable().setEnableJSONV2Rewrite(false);
        Pair<QueryDumpInfo, String> replayPair = getPlanFragment(dumpString, queryDumpInfo.getSessionVariable(),
                TExplainLevel.NORMAL);
        Assertions.assertTrue(replayPair.second.contains(
                "get_json_string(107: mock_031, '$.\"fY21_Territory_Score__c\"')\n" +
                "  |  \n" +
                "  9:OlapScanNode\n" +
                        "     TABLE: tbl_mock_103"), replayPair.second);
    }

    @Test
    public void testEliminateConstantCTEAndNestLoopJoin() throws Exception {
        String dumpString = getDumpInfoFromFile("query_dump/eliminate_nestloop_join");
        QueryDumpInfo queryDumpInfo = getDumpInfoFromJson(dumpString);
        Pair<QueryDumpInfo, String> replayPair = getPlanFragment(dumpString, queryDumpInfo.getSessionVariable(),
                TExplainLevel.NORMAL);
        Assertions.assertTrue(replayPair.second.contains("  29:NESTLOOP JOIN\n" +
                "  |  join op: CROSS JOIN\n" +
                "  |  colocate: false, reason: \n" +
                "  |  \n" +
                "  |----28:EXCHANGE\n" +
                "  |    \n" +
                "  18:Project\n" +
                "  |  <slot 119> : 119: mock_189\n" +
                "  |  \n" +
                "  17:HASH JOIN\n" +
                "  |  join op: INNER JOIN (PARTITIONED)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 286: mock_278 = 67: mock_216\n" +
                "  |  \n" +
                "  |----16:EXCHANGE\n" +
                "  |    \n" +
                "  3:EXCHANGE\n" +
                "\n" +
                "PLAN FRAGMENT 6\n" +
                " OUTPUT EXPRS:\n" +
                "  PARTITION: UNPARTITIONED\n" +
                "\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 28\n" +
                "    UNPARTITIONED\n" +
                "\n" +
                "  27:Project\n" +
                "  |  <slot 603> : array_contains(601: array_agg, 'asdfasdfasdf')\n" +
                "  |  \n" +
                "  26:AGGREGATE (merge finalize)\n" +
                "  |  output: array_agg(601: array_agg)\n" +
                "  |  group by: \n" +
                "  |  \n" +
                "  25:EXCHANGE"), replayPair.second);
    }

    @Test
    public void testUnionWithEmptyInput() throws Exception {
        String dumpString = getDumpInfoFromFile("query_dump/union_with_empty_input");
        QueryDumpInfo queryDumpInfo = getDumpInfoFromJson(dumpString);
        Pair<QueryDumpInfo, String> replayPair = getPlanFragment(dumpString, queryDumpInfo.getSessionVariable(),
                TExplainLevel.NORMAL);
        Assertions.assertTrue(replayPair.second.contains("0:EMPTYSET"), replayPair.second);
    }

    @Test
    public void testLowCardinalityWithCte() throws Exception {
        try {
            FeConstants.USE_MOCK_DICT_MANAGER = true;
            String dumpString = getDumpInfoFromFile("query_dump/low_cardinality_with_cte");
            QueryDumpInfo queryDumpInfo = getDumpInfoFromJson(dumpString);
            Pair<QueryDumpInfo, String> replayPair = getPlanFragment(dumpString, queryDumpInfo.getSessionVariable(),
                    TExplainLevel.NORMAL);
            Assertions.assertTrue(replayPair.second.contains("  37:HASH JOIN\n"
                    + "  |  join op: RIGHT OUTER JOIN (BUCKET_SHUFFLE(S))\n"
                    + "  |  colocate: false, reason: \n"
                    + "  |  equal join conjunct: 555: coalesce = 130: mock_011\n"
                    + "  |  equal join conjunct: 556: coalesce = 131: mock_007\n"
                    + "  |  equal join conjunct: 558: coalesce = 133: mock_046\n"
                    + "  |  equal join conjunct: 559: coalesce = 134: mock_050\n"
                    + "  |  \n"
                    + "  |----36:AGGREGATE (merge finalize)\n"
                    + "  |    |  group by: 130: mock_011, 131: mock_007, 133: mock_046, 134: mock_050\n"
                    + "  |    |  \n"
                    + "  |    35:EXCHANGE"), replayPair.second);
        } finally {
            FeConstants.USE_MOCK_DICT_MANAGER = false;
        }
    }

    @Test
    public void testExpressionReuseTimeout() throws Exception {
        String dumpString = getDumpInfoFromFile("query_dump/expr_reuse_timeout");
        Tracers.register(connectContext);
        Tracers.init(Tracers.Mode.TIMER, Tracers.Module.OPTIMIZER, false, false);
        QueryDumpInfo queryDumpInfo = getDumpInfoFromJson(dumpString);
        Pair<QueryDumpInfo, String> replayPair = getPlanFragment(dumpString, queryDumpInfo.getSessionVariable(),
                TExplainLevel.NORMAL);
        System.out.println(Tracers.printScopeTimer());
        String ss = Tracers.printScopeTimer();
        int start = ss.indexOf("PhysicalRewrite[") + "PhysicalRewrite[".length();
        int end = ss.indexOf("]", start);
        long count = Long.parseLong(ss.substring(start, end));
        Assertions.assertTrue(count < 1000, ss);
    }

    @Test
    public void testForceReuseCTEWithHugeCTE() throws Exception {
        String dumpString = getDumpInfoFromFile("query_dump/big_cte_with_force_reuse");
        QueryDumpInfo queryDumpInfo = getDumpInfoFromJson(dumpString);
        Pair<QueryDumpInfo, String> replayPair = getPlanFragment(dumpString, queryDumpInfo.getSessionVariable(),
                TExplainLevel.NORMAL);
        Assertions.assertTrue(replayPair.second.contains("MultiCastDataSinks\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 1607\n" +
                "    RANDOM\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 1612\n" +
                "    RANDOM\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 1627\n" +
                "    RANDOM\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 1644\n" +
                "    RANDOM\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 1650\n" +
                "    RANDOM\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 1664\n" +
                "    RANDOM\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 1691\n" +
                "    RANDOM\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 1697\n" +
                "    RANDOM\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 1711\n" +
                "    RANDOM"), replayPair.second);
    }

    @Test
    public void testPruneUnionEmpty() throws Exception {
        try {
            FeConstants.enablePruneEmptyOutputScan = true;
            String dumpString = getDumpInfoFromFile("query_dump/prune_union_empty");
            QueryDumpInfo queryDumpInfo = getDumpInfoFromJson(dumpString);
            Pair<QueryDumpInfo, String> replayPair = getPlanFragment(dumpString, queryDumpInfo.getSessionVariable(),
                    TExplainLevel.NORMAL);
            Assertions.assertTrue(replayPair.second.contains("1:EMPTYSET"), replayPair.second);
        } finally {
            FeConstants.enablePruneEmptyOutputScan = false;
        }
    }

    @Test
    public void testUnnestLowCardinalityOptimization() throws Exception {
        try {
            FeConstants.USE_MOCK_DICT_MANAGER = true;
            String dumpString = getDumpInfoFromFile("query_dump/unnest_low_cardinality_optimization");
            QueryDumpInfo queryDumpInfo = getDumpInfoFromJson(dumpString);
            Pair<QueryDumpInfo, String> replayPair = getPlanFragment(dumpString, queryDumpInfo.getSessionVariable(),
                    TExplainLevel.NORMAL);
            String plan = replayPair.second;
            Assertions.assertTrue(plan.contains("  30:Project\n" +
                    "  |  <slot 113> : 113: mock_field_111\n" +
                    "  |  <slot 278> : lower(142: jl_str)\n" +
                    "  |  \n" +
                    "  29:TableValueFunction\n" +
                    "  |  tableFunctionName: unnest\n" +
                    "  |  columns: [unnest]\n" +
                    "  |  returnTypes: [VARCHAR(65533)]"), plan);
        } finally {
            FeConstants.USE_MOCK_DICT_MANAGER = false;
        }
    }

    @Test
    public void testViewBasedRewrite1() throws Exception {
        String fileContent = getDumpInfoFromFile("query_dump/view_based_rewrite1");
        QueryDumpInfo queryDumpInfo = getDumpInfoFromJson(fileContent);
        SessionVariable sessionVariable = queryDumpInfo.getSessionVariable();
        QueryDebugOptions debugOptions = new QueryDebugOptions();
        debugOptions.setEnableQueryTraceLog(true);
        sessionVariable.setQueryDebugOptions(debugOptions.toString());
        new MockUp<MVTransformerContext>() {
            /**
             * {@link com.starrocks.sql.optimizer.transformer.MVTransformerContext#isEnableViewBasedMVRewrite(View)} ()}
             */
            @Mock
            public boolean isEnableViewBasedMVRewrite(View view) {
                return true;
            }
        };
        Pair<QueryDumpInfo, String> replayPair = getCostPlanFragment(fileContent, sessionVariable);
        String plan = replayPair.second;
        PlanTestBase.assertContains(plan, "single_mv_ads_biz_customer_combine_td_for_task_2y");
    }
}
