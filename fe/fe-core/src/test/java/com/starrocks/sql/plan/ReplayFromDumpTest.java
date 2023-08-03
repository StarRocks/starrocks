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

import com.google.common.collect.Lists;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.common.Pair;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.qe.VariableMgr;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.base.CTEProperty;
import com.starrocks.sql.optimizer.dump.QueryDumpInfo;
import com.starrocks.sql.optimizer.rule.RuleSet;
import com.starrocks.sql.optimizer.rule.transformation.JoinAssociativityRule;
import com.starrocks.system.BackendCoreStat;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ReplayFromDumpTest {
    public static ConnectContext connectContext;
    public static StarRocksAssert starRocksAssert;

    public static List<String> MODEL_LISTS = Lists.newArrayList("[end]", "[dump]", "[result]", "[fragment]",
            "[fragment statistics]");

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        // Should disable Dynamic Partition in replay dump test
        Config.dynamic_partition_enable = false;
        Config.tablet_sched_disable_colocate_overall_balance = true;
        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
        connectContext.getSessionVariable().setOptimizerExecuteTimeout(30000);
        connectContext.getSessionVariable().setJoinImplementationMode("auto");
        connectContext.getSessionVariable().setCboPushDownAggregateMode(-1);
        starRocksAssert = new StarRocksAssert(connectContext);
        FeConstants.runningUnitTest = true;
        FeConstants.showLocalShuffleColumnsInExplain = false;
    }

    @Before
    public void before() {
        BackendCoreStat.reset();
        connectContext.getSessionVariable().setCboPushDownAggregateMode(-1);
    }

    @AfterClass
    public static void afterClass() throws Exception {
        connectContext.getSessionVariable().setEnableLocalShuffleAgg(true);
        FeConstants.showLocalShuffleColumnsInExplain = true;
    }

    public String getModelContent(String filename, String model) {
        String path = Objects.requireNonNull(ClassLoader.getSystemClassLoader().getResource("sql")).getPath();
        File file = new File(path + "/" + filename + ".sql");
        StringBuilder modelContentBuilder = new StringBuilder();

        String tempStr;
        boolean hasModelContent = false;
        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
            while ((tempStr = reader.readLine()) != null) {
                if (tempStr.equals("[" + model + "]")) {
                    hasModelContent = true;
                    continue;
                }

                if (MODEL_LISTS.contains(tempStr)) {
                    hasModelContent = false;
                }

                if (hasModelContent) {
                    modelContentBuilder.append(tempStr).append("\n");
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }
        return modelContentBuilder.toString();
    }

    public QueryDumpInfo getDumpInfoFromJson(String dumpInfoString) {
        return GsonUtils.GSON.fromJson(dumpInfoString, QueryDumpInfo.class);
    }

    public SessionVariable getTestSessionVariable() {
        SessionVariable sessionVariable = VariableMgr.newSessionVariable();
        sessionVariable.setMaxTransformReorderJoins(8);
        sessionVariable.setEnableGlobalRuntimeFilter(true);
        sessionVariable.setEnableMultiColumnsOnGlobbalRuntimeFilter(true);
        return sessionVariable;
    }

    public void compareDumpWithOriginTest(String filename) throws Exception {
        String dumpString = getModelContent(filename, "dump");
        String originCostPlan = Stream.of(getModelContent(filename, "fragment statistics").split("\n")).
                filter(s -> !s.contains("tabletList")).collect(Collectors.joining("\n"));
        String replayCostPlan = Stream.of(
                        PlanTestBase.format(getCostPlanFragment(dumpString, getTestSessionVariable()).second).split("\n"))
                .filter(s -> !s.contains("tabletList")).collect(Collectors.joining("\n"));
        Assert.assertEquals(originCostPlan, replayCostPlan);
    }

    protected String getDumpInfoFromFile(String fileName) throws Exception {
        String path = Objects.requireNonNull(ClassLoader.getSystemClassLoader().getResource("sql")).getPath();
        File file = new File(path + "/" + fileName + ".json");
        StringBuilder sb = new StringBuilder();
        String tempStr;
        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
            while ((tempStr = reader.readLine()) != null) {
                sb.append(tempStr);
            }
        } catch (IOException e) {
            e.printStackTrace();
            Assert.fail();
        }

        return sb.toString();
    }

    private Pair<QueryDumpInfo, String> getCostPlanFragment(String dumpJonStr) throws Exception {
        return getCostPlanFragment(dumpJonStr, null);
    }

    protected Pair<QueryDumpInfo, String> getCostPlanFragment(String dumpJsonStr, SessionVariable sessionVariable)
            throws Exception {
        return getPlanFragment(dumpJsonStr, sessionVariable, TExplainLevel.COSTS);
    }

    protected Pair<QueryDumpInfo, String> getPlanFragment(String dumpJsonStr, SessionVariable sessionVariable,
                                                          TExplainLevel level) throws Exception {
        QueryDumpInfo queryDumpInfo = getDumpInfoFromJson(dumpJsonStr);
        if (sessionVariable != null) {
            queryDumpInfo.setSessionVariable(sessionVariable);
        }
        queryDumpInfo.getSessionVariable().setOptimizerExecuteTimeout(30000000);
        queryDumpInfo.getSessionVariable().setCboPushDownAggregateMode(-1);
        return new Pair<>(queryDumpInfo,
                UtFrameUtils.getNewPlanAndFragmentFromDump(connectContext, queryDumpInfo).second.
                        getExplainString(level));
    }

    @Test
    public void testTPCH1() throws Exception {
        compareDumpWithOriginTest("tpchcost/q1");
    }

    @Test
    public void testTPCH5() throws Exception {
        compareDumpWithOriginTest("tpchcost/q5");
    }

    @Test
    public void testTPCH7() throws Exception {
        compareDumpWithOriginTest("tpchcost/q7");
    }

    @Test
    public void testTPCH17WithUseAnalytic() throws Exception {
        QueryDumpInfo queryDumpInfo = getDumpInfoFromJson(getDumpInfoFromFile("query_dump/tpch17"));
        SessionVariable sessionVariable = queryDumpInfo.getSessionVariable();
        sessionVariable.setNewPlanerAggStage(2);
        Pair<QueryDumpInfo, String> replayPair =
                getCostPlanFragment(getDumpInfoFromFile("query_dump/tpch17"), sessionVariable);
        Assert.assertTrue(replayPair.second, replayPair.second.contains("  8:ANALYTIC\n" +
                "  |  functions: [, avg[([5: L_QUANTITY, DOUBLE, false]); args: DOUBLE; " +
                "result: DOUBLE; args nullable: false; result nullable: true], ]\n" +
                "  |  partition by: [18: P_PARTKEY, INT, false]"));
    }

    @Test
    public void testReplyOnlineCase_JoinEliminateNulls() throws Exception {
        QueryDumpInfo queryDumpInfo = getDumpInfoFromJson(getDumpInfoFromFile("query_dump/join_eliminate_nulls"));
        SessionVariable sessionVariable = queryDumpInfo.getSessionVariable();
        sessionVariable.setNewPlanerAggStage(2);
        Pair<QueryDumpInfo, String> replayPair =
                getCostPlanFragment(getDumpInfoFromFile("query_dump/join_eliminate_nulls"), sessionVariable);
        Assert.assertTrue(replayPair.second, replayPair.second.contains("11:NESTLOOP JOIN\n" +
                "  |  join op: INNER JOIN\n" +
                "  |  other join predicates: CASE 174: type WHEN '1' THEN concat('ocms_', 90: name) = 'ocms_fengyang56' " +
                "WHEN '0' THEN TRUE ELSE FALSE END\n" +
                "  |  limit: 10"));
        Assert.assertTrue(replayPair.second, replayPair.second.contains("4:HASH JOIN\n" +
                "  |  join op: RIGHT OUTER JOIN (PARTITIONED)\n" +
                "  |  equal join conjunct: [tid, BIGINT, true] = [5: customer_id, BIGINT, true]\n" +
                "  |  output columns: 3, 90"));
        sessionVariable.setNewPlanerAggStage(0);
    }

    @Test
    public void testTPCH20() throws Exception {
        compareDumpWithOriginTest("tpchcost/q20");
    }

    @Test
    public void testReplayTPCDS02() throws Exception {
        Pair<QueryDumpInfo, String> replayPair = getCostPlanFragment(getDumpInfoFromFile("query_dump/tpcds02"));
        SessionVariable replaySessionVariable = replayPair.first.getSessionVariable();
        Assert.assertEquals(replaySessionVariable.getParallelExecInstanceNum(), 4);
        Assert.assertTrue(replayPair.second, replayPair.second.contains("  |----24:EXCHANGE\n" +
                "  |       distribution type: BROADCAST\n" +
                "  |       cardinality: 65744\n" +
                "  |    \n" +
                "  18:UNION\n" +
                "  |  output exprs:\n" +
                "  |      [391, INT, true] | [392, DECIMAL64(7,2), true]\n" +
                "  |  child exprs:\n" +
                "  |      [325: ws_sold_date_sk, INT, true] | [346: ws_ext_sales_price, DECIMAL64(7,2), true]\n" +
                "  |      [359: cs_sold_date_sk, INT, true] | [380: cs_ext_sales_price, DECIMAL64(7,2), true]"));
    }

    @Test
    public void testSSB10() throws Exception {
        Pair<QueryDumpInfo, String> replayPair = getCostPlanFragment(getDumpInfoFromFile("query_dump/ssb10"));
        Assert.assertTrue(replayPair.second, replayPair.second.contains("  14:Project\n" +
                "  |  output columns:\n" +
                "  |  13 <-> [13: lo_revenue, INT, false]\n" +
                "  |  22 <-> [22: d_year, INT, false]\n" +
                "  |  38 <-> [38: c_city, VARCHAR, false]\n" +
                "  |  46 <-> [46: s_city, VARCHAR, false]\n" +
                "  |  cardinality: 28532"));
        Assert.assertTrue(replayPair.second, replayPair.second.contains("  |----7:EXCHANGE\n" +
                "  |       distribution type: BROADCAST\n" +
                "  |       cardinality: 30"));
    }

    @Test
    public void testTPCDS54() throws Exception {
        Pair<QueryDumpInfo, String> replayPair = getCostPlanFragment(getDumpInfoFromFile("query_dump/tpcds54"));
        // Check the size of the left and right tables
        Assert.assertTrue(replayPair.second, replayPair.second.contains("  49:NESTLOOP JOIN\n" +
                "  |  join op: INNER JOIN\n" +
                "  |  other join predicates: cast([934: d_month_seq, INT, true] as BIGINT) <= [1017: expr, BIGINT, true]\n" +
                "  |  cardinality: 18262\n" +
                "  |  column statistics: \n" +
                "  |  * d_date_sk-->[2415022.0, 2488070.0, 0.0, 4.0, 18262.25] ESTIMATE\n" +
                "  |  * d_month_seq-->[0.0, 2400.0, 0.0, 4.0, 2398.0] ESTIMATE\n" +
                "  |  * expr-->[3.0, 2403.0, 0.0, 8.0, 30.13572607260726] ESTIMATE\n" +
                "  |  \n" +
                "  |----48:EXCHANGE\n" +
                "  |       distribution type: BROADCAST\n" +
                "  |       cardinality: 1\n" +
                "  |    \n" +
                "  41:Project\n" +
                "  |  output columns:\n" +
                "  |  931 <-> [931: d_date_sk, INT, false]\n" +
                "  |  934 <-> [934: d_month_seq, INT, true]\n" +
                "  |  cardinality: 36525\n" +
                "  |  column statistics: \n" +
                "  |  * d_date_sk-->[2415022.0, 2488070.0, 0.0, 4.0, 36524.5] ESTIMATE\n" +
                "  |  * d_month_seq-->[0.0, 2400.0, 0.0, 4.0, 2398.0] ESTIMATE\n" +
                "  |  \n" +
                "  40:NESTLOOP JOIN\n" +
                "  |  join op: INNER JOIN\n" +
                "  |  other join predicates: cast([934: d_month_seq, INT, true] as BIGINT) >= [987: expr, BIGINT, true]\n" +
                "  |  cardinality: 36525\n" +
                "  |  column statistics: \n" +
                "  |  * d_date_sk-->[2415022.0, 2488070.0, 0.0, 4.0, 36524.5] ESTIMATE\n" +
                "  |  * d_month_seq-->[0.0, 2400.0, 0.0, 4.0, 2398.0] ESTIMATE\n" +
                "  |  * expr-->[1.0, 2401.0, 0.0, 8.0, 30.13572607260726] ESTIMATE\n" +
                "  |  \n" +
                "  |----39:EXCHANGE\n" +
                "  |       distribution type: BROADCAST\n" +
                "  |       cardinality: 1\n" +
                "  |    \n" +
                "  32:OlapScanNode\n" +
                "     table: date_dim, rollup: date_dim\n"));
        Assert.assertTrue(replayPair.second, replayPair.second.contains("  |----18:EXCHANGE\n" +
                "  |       distribution type: SHUFFLE\n" +
                "  |       partition exprs: [796: cs_bill_customer_sk, INT, true]\n" +
                "  |       cardinality: 6304\n" +
                "  |    \n" +
                "  2:OlapScanNode\n" +
                "     table: customer, rollup: customer"));
    }

    @Test
    public void testTPCDS23_1() throws Exception {
        Pair<QueryDumpInfo, String> replayPair =
                getPlanFragment(getDumpInfoFromFile("query_dump/tpcds23_1"), null, TExplainLevel.NORMAL);
        Assert.assertTrue(replayPair.second, replayPair.second.contains("  MultiCastDataSinks\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 52\n" +
                "    RANDOM\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 73\n" +
                "    RANDOM\n" +
                "\n" +
                "  40:Project\n" +
                "  |  <slot 171> : 171: c_customer_sk\n" +
                "  |  \n" +
                "  39:NESTLOOP JOIN\n" +
                "  |  join op: INNER JOIN\n" +
                "  |  colocate: false, reason: \n" +
                "  |  other join predicates: CAST(190: sum AS DOUBLE) > CAST(0.5 * 262: max AS DOUBLE)"));
    }

    @Test
    public void testTPCH01() throws Exception {
        Pair<QueryDumpInfo, String> replayPair = getCostPlanFragment(getDumpInfoFromFile("query_dump/tpch01"));
        // check q1 has two agg phase with default column statistics
        Assert.assertTrue(replayPair.second, replayPair.second.contains("AGGREGATE (merge finalize)"));
        Assert.assertTrue(replayPair.second, replayPair.second.contains("AGGREGATE (update serialize)"));
    }

    @Test
    public void testGroupByLimit() throws Exception {
        // check can generate 1 phase with limit 1
        // This test has column statistics and accurate table row count
        SessionVariable sessionVariable = VariableMgr.newSessionVariable();
        sessionVariable.setNewPlanerAggStage(1);
        Pair<QueryDumpInfo, String> replayPair =
                getCostPlanFragment(getDumpInfoFromFile("query_dump/groupby_limit"), sessionVariable);
        Assert.assertTrue(replayPair.second, replayPair.second.contains(
                "aggregate: multi_distinct_count[([1: LO_ORDERKEY, INT, false])"));
    }

    @Test
    public void testTPCDS77() throws Exception {
        Pair<QueryDumpInfo, String> replayPair = getCostPlanFragment(getDumpInfoFromFile("query_dump/tpcds77"));
        // check can generate plan without exception
        System.out.println(replayPair.second);
    }

    @Test
    public void testTPCDS78() throws Exception {
        // check outer join with isNull predicate on inner table
        // The estimate cardinality of join should not be 0.
        Pair<QueryDumpInfo, String> replayPair = getCostPlanFragment(getDumpInfoFromFile("query_dump/tpcds78"));
        System.out.println(replayPair.second);
        Assert.assertTrue(replayPair.second, replayPair.second.contains("3:HASH JOIN\n" +
                "  |  join op: LEFT OUTER JOIN (BUCKET_SHUFFLE)\n" +
                "  |  equal join conjunct: [257: ss_ticket_number, INT, false] = [280: sr_ticket_number, INT, true]\n" +
                "  |  equal join conjunct: [256: ss_item_sk, INT, false] = [279: sr_item_sk, INT, true]\n" +
                "  |  other predicates: 280: sr_ticket_number IS NULL\n" +
                "  |  output columns: 256, 258, 260, 266, 267, 269, 280\n" +
                "  |  cardinality: 37372757"));
        Assert.assertTrue(replayPair.second, replayPair.second.contains("15:HASH JOIN\n" +
                "  |  join op: LEFT OUTER JOIN (BUCKET_SHUFFLE)\n" +
                "  |  equal join conjunct: [331: ws_order_number, INT, false] = [365: wr_order_number, INT, true]\n" +
                "  |  equal join conjunct: [330: ws_item_sk, INT, false] = [364: wr_item_sk, INT, true]\n" +
                "  |  other predicates: 365: wr_order_number IS NULL\n" +
                "  |  output columns: 330, 332, 335, 348, 349, 351, 365\n" +
                "  |  cardinality: 7914602"));
    }

    @Test
    public void testTPCDS94() throws Exception {
        Pair<QueryDumpInfo, String> replayPair = getCostPlanFragment(getDumpInfoFromFile("query_dump/tpcds94"));
        // check ANTI JOIN cardinality is not 0
        Assert.assertTrue(replayPair.second, replayPair.second.contains("21:HASH JOIN\n" +
                "  |    |  join op: RIGHT ANTI JOIN (PARTITIONED)\n" +
                "  |    |  equal join conjunct: [138: wr_order_number, INT, false] = [2: ws_order_number, INT, false]\n" +
                "  |    |  build runtime filters:\n" +
                "  |    |  - filter_id = 3, build_expr = (2: ws_order_number), remote = true\n" +
                "  |    |  output columns: 2, 17, 29, 34\n" +
                "  |    |  cardinality: 26765"));
        Assert.assertTrue(replayPair.second, replayPair.second.contains("23:HASH JOIN\n" +
                "  |  join op: RIGHT SEMI JOIN (BUCKET_SHUFFLE(S))\n" +
                "  |  equal join conjunct: [103: ws_order_number, INT, false] = [2: ws_order_number, INT, false]\n" +
                "  |  other join predicates: [17: ws_warehouse_sk, INT, true] != [118: ws_warehouse_sk, INT, true]\n" +
                "  |  build runtime filters:"));
    }

    @Test
    public void testTPCDS22() throws Exception {
        Pair<QueryDumpInfo, String> replayPair = getCostPlanFragment(getDumpInfoFromFile("query_dump/tpcds22"));
        // check d_date_sk distinct values has adjusted according to the cardinality
        Assert.assertTrue(replayPair.second, replayPair.second.contains("  4:HASH JOIN\n" +
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
                "  |       cardinality: 335"));
    }

    @Test
    public void testTPCDS54WithJoinHint() throws Exception {
        Pair<QueryDumpInfo, String> replayPair =
                getPlanFragment(getDumpInfoFromFile("query_dump/tpcds54_with_join_hint"), null, TExplainLevel.NORMAL);
        // checkout join order as hint
        Assert.assertTrue(replayPair.second, replayPair.second.contains(" 19:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BROADCAST)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 729: ss_sold_date_sk = 750: d_date_sk\n" +
                "  |  \n" +
                "  |----18:EXCHANGE\n" +
                "  |    \n" +
                "  0:OlapScanNode\n" +
                "     TABLE: store_sales"));
    }

    @Test
    public void testTPCDS64() throws Exception {
        Pair<QueryDumpInfo, String> replayPair =
                getPlanFragment(getDumpInfoFromFile("query_dump/tpcds64"), null, TExplainLevel.NORMAL);
        Assert.assertTrue(replayPair.second, replayPair.second.contains(" 83:SELECT\n" +
                "  |  predicates: 521: d_year = 1999"));
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
        Assert.assertTrue(replayPair.second, replayPair.second.contains("  13:NESTLOOP JOIN\n" +
                "  |  join op: INNER JOIN\n" +
                "  |  colocate: false, reason: \n" +
                "  |  other join predicates: CASE WHEN CAST(6: v3 AS BOOLEAN) THEN CAST(11: v2 AS VARCHAR) " +
                "WHEN CAST(3: v3 AS BOOLEAN) THEN '123' ELSE CAST(12: v3 AS VARCHAR) END > '1', " +
                "(2: v2 = CAST(8: v2 AS VARCHAR(1048576))) OR (3: v3 = 8: v2)\n"));
        connectContext.getSessionVariable().setEnableLocalShuffleAgg(true);
    }

    @Test
    public void testJoinReorderPushColumnsNoHandleProject() throws Exception {
        Pair<QueryDumpInfo, String> replayPair =
                getPlanFragment(getDumpInfoFromFile("query_dump/join_reorder"), null, TExplainLevel.NORMAL);
        Assert.assertTrue(replayPair.second,
                replayPair.second.contains("  |  <slot 40> : CAST(15: id_smallint AS INT)\n" +
                        "  |  <slot 41> : CAST(23: id_date AS DATETIME)\n" +
                        "  |  \n" +
                        "  5:OlapScanNode\n" +
                        "     TABLE: external_es_table_without_null"));
    }

    @Test
    public void testMultiCountDistinct() throws Exception {
        Pair<QueryDumpInfo, String> replayPair =
                getPlanFragment(getDumpInfoFromFile("query_dump/multi_count_distinct"), null, TExplainLevel.NORMAL);
        String plan = replayPair.second;
        Assert.assertTrue(plan, plan.contains("34:AGGREGATE (update finalize)\n" +
                "  |  output: multi_distinct_count(6: order_id), multi_distinct_count(11: delivery_phone)," +
                " multi_distinct_count(128: case), max(103: count)\n" +
                "  |  group by: 40: city, 116: division_en, 104: department, 106: category, 126: concat, " +
                "127: concat, 9: upc, 108: upc_desc"));
    }

    @Test
    public void testJoinWithPipelineDop() throws Exception {
        Pair<QueryDumpInfo, String> replayPair =
                getPlanFragment(getDumpInfoFromFile("query_dump/join_pipeline_dop"), null, TExplainLevel.NORMAL);
        Assert.assertTrue(replayPair.second, replayPair.second.contains("25:HASH JOIN\n" +
                "  |  join op: INNER JOIN (PARTITIONED)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 5: ss_customer_sk = 52: c_customer_sk"));
    }

    @Test
    public void testDecodeLimitWithProject() throws Exception {
        FeConstants.USE_MOCK_DICT_MANAGER = true;
        Pair<QueryDumpInfo, String> replayPair =
                getPlanFragment(getDumpInfoFromFile("query_dump/decode_limit_with_project"), null,
                        TExplainLevel.NORMAL);
        String plan = replayPair.second;
        Assert.assertTrue(plan, plan.contains("13:Decode\n" +
                "  |  <dict id 42> : <string id 18>\n" +
                "  |  <dict id 43> : <string id 23>"));
        FeConstants.USE_MOCK_DICT_MANAGER = false;
    }

    @Test
    public void testCountDistinctWithLimit() throws Exception {
        // check use two stage agg
        Pair<QueryDumpInfo, String> replayPair =
                getPlanFragment(getDumpInfoFromFile("query_dump/count_distinct_limit"), null, TExplainLevel.NORMAL);
        Assert.assertTrue(replayPair.second, replayPair.second.contains("1:AGGREGATE (update serialize)\n" +
                "  |  STREAMING\n" +
                "  |  output: multi_distinct_count(5: lo_suppkey)"));
        Assert.assertTrue(replayPair.second, replayPair.second.contains("3:AGGREGATE (merge finalize)\n" +
                "  |  output: multi_distinct_count(18: count)\n" +
                "  |  group by: 10: lo_extendedprice, 13: lo_revenue"));
    }

    @Test
    public void testEighteenTablesJoin() throws Exception {
        Pair<QueryDumpInfo, String> replayPair =
                getPlanFragment(getDumpInfoFromFile("query_dump/eighteen_tables_join"), null, TExplainLevel.NORMAL);
        // check optimizer finish task
        Assert.assertTrue(replayPair.second, replayPair.second.contains("52:HASH JOIN\n" +
                "  |  join op: INNER JOIN (COLOCATE)"));
    }

    @Test
    public void testLocalAggregateWithoutTableRowCount() throws Exception {
        Pair<QueryDumpInfo, String> replayPair =
                getPlanFragment(getDumpInfoFromFile("query_dump/local_agg_without_table_rowcount"), null,
                        TExplainLevel.NORMAL);
        // check local aggregate
        Assert.assertTrue(replayPair.second, replayPair.second.contains("1:AGGREGATE (update finalize)\n" +
                "  |  output: multi_distinct_count(4: lo_partkey)"));
    }

    @Test
    public void testLogicalAggWithOneTablet() throws Exception {
        Pair<QueryDumpInfo, String> replayPair =
                getPlanFragment(getDumpInfoFromFile("query_dump/local_agg_with_one_tablet"), null,
                        TExplainLevel.NORMAL);
        Assert.assertTrue(replayPair.second, replayPair.second.contains("1:AGGREGATE (update finalize)\n" +
                "  |  output: multi_distinct_count(4: t0d)"));
    }

    @Test
    public void testSelectSubqueryWithMultiJoin() throws Exception {
        Pair<QueryDumpInfo, String> replayPair =
                getPlanFragment(getDumpInfoFromFile("query_dump/select_sbuquery_with_multi_join"), null,
                        TExplainLevel.NORMAL);
        Assert.assertTrue(replayPair.second, replayPair.second.contains("  20:Project\n" +
                "  |  <slot 33> : bitmap_and(21: bitmap_union, 29: bitmap_union)\n" +
                "  |  \n" +
                "  19:NESTLOOP JOIN\n" +
                "  |  join op: CROSS JOIN\n" +
                "  |  colocate: false, reason: \n" +
                "  |  \n" +
                "  |----18:EXCHANGE\n" +
                "  |    \n" +
                "  11:Project\n" +
                "  |  <slot 21> : 18: bitmap_union"));
    }

    @Test
    public void testTPCHRandom() throws Exception {
        Pair<QueryDumpInfo, String> replayPair =
                getPlanFragment(getDumpInfoFromFile("query_dump/tpch_random"), null, TExplainLevel.NORMAL);
        // check optimizer could extract best plan
        Assert.assertTrue(replayPair.second, replayPair.second.contains("11:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BUCKET_SHUFFLE)"));
    }

    @Test
    public void testInsertWithView() throws Exception {
        Pair<QueryDumpInfo, String> replayPair =
                getPlanFragment(getDumpInfoFromFile("query_dump/insert_view"), null, TExplainLevel.NORMAL);
        Assert.assertTrue(replayPair.second, replayPair.second.contains(" 2:Project\n" +
                "  |  <slot 2> : 2: t2_c2\n" +
                "  |  <slot 11> : CAST(CAST(1: t2_c1 AS BIGINT) + 1 AS INT)"));
        Assert.assertTrue(replayPair.second, replayPair.second.contains("OLAP TABLE SINK"));
    }

    @Test
    public void testMergeGroupWithDeleteBestExpression() throws Exception {
        Pair<QueryDumpInfo, String> replayPair =
                getPlanFragment(getDumpInfoFromFile("query_dump/merge_group_delete_best_expression"), null,
                        TExplainLevel.NORMAL);
        // check without exception
        Assert.assertTrue(replayPair.second, replayPair.second.contains("14:HASH JOIN\n" +
                "  |  join op: INNER JOIN (PARTITIONED)"));
    }

    @Test
    public void testJoinReOrderPruneColumns() throws Exception {
        Pair<QueryDumpInfo, String> replayPair =
                getPlanFragment(getDumpInfoFromFile("query_dump/join_reorder_prune_columns"), null,
                        TExplainLevel.NORMAL);
        // check without exception
        Assert.assertTrue(replayPair.second, replayPair.second.contains("<slot 186> : 186: S_SUPPKEY"));
    }

    @Test
    public void testMultiViewWithDbName() throws Exception {
        Pair<QueryDumpInfo, String> replayPair =
                getPlanFragment(getDumpInfoFromFile("query_dump/multi_view_with_db"), null, TExplainLevel.NORMAL);
        Assert.assertTrue(replayPair.second, replayPair.second.contains(" 0:OlapScanNode\n" +
                "     TABLE: t3"));
    }

    @Test
    public void testMultiViewCrossJoin() throws Exception {
        Pair<QueryDumpInfo, String> replayPair =
                getPlanFragment(getDumpInfoFromFile("query_dump/multi_view_cross_join"), null, TExplainLevel.NORMAL);
        // check without exception
        Assert.assertTrue(replayPair.second, replayPair.second.contains(" 40:Project\n" +
                "  |  <slot 1> : 1: c_0_0"));
    }

    @Test
    public void testMultiViewPruneColumns() throws Exception {
        Pair<QueryDumpInfo, String> replayPair =
                getPlanFragment(getDumpInfoFromFile("query_dump/multi_view_prune_columns"), null, TExplainLevel.NORMAL);
        // check without exception
        Assert.assertTrue(replayPair.second, replayPair.second.contains("  206:Project\n" +
                "  |  <slot 1> : 1: c_1_0"));
    }

    @Test
    public void testCorrelatedSubqueryWithEqualsExpressions() throws Exception {
        Pair<QueryDumpInfo, String> replayPair =
                getPlanFragment(getDumpInfoFromFile("query_dump/correlated_subquery_with_equals_expression"), null,
                        TExplainLevel.NORMAL);
        Assert.assertTrue(replayPair.second, replayPair.second.contains(" 22:NESTLOOP JOIN\n" +
                "  |  join op: INNER JOIN\n" +
                "  |  colocate: false, reason: \n" +
                "  |  other join predicates: if(19: c_0_0 != 1: c_0_0, 4: c_0_3, 20: c_0_3) = '1969-12-28', " +
                "CASE WHEN (21: countRows IS NULL) OR (21: countRows = 0) THEN FALSE WHEN 1: c_0_0 IS NULL THEN NULL " +
                "WHEN 16: c_0_0 IS NOT NULL THEN TRUE WHEN 22: countNotNulls < 21: countRows THEN NULL ELSE FALSE END IS NULL"));
        Assert.assertTrue(replayPair.second, replayPair.second.contains("15:HASH JOIN\n" +
                "  |  join op: LEFT OUTER JOIN (PARTITIONED)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 1: c_0_0 = 16: c_0_0\n" +
                "  |  other join predicates: if(16: c_0_0 != 1: c_0_0, 4: c_0_3, 17: c_0_3) = '1969-12-28'"));
    }

    @Test
    public void testGatherWindowCTE2() throws Exception {
        Pair<QueryDumpInfo, String> replayPair =
                getPlanFragment(getDumpInfoFromFile("query_dump/gather_window_cte"), null, TExplainLevel.COSTS);
        Assert.assertTrue(replayPair.second, replayPair.second.contains("  0:UNION\n" +
                "  |  output exprs:\n" +
                "  |      [16, DATE, false] | [17, BIGINT, true] | [18, DECIMAL128(27,19), true]\n" +
                "  |  child exprs:\n" +
                "  |      [2: c_0_0, DATE, false] | [7: row_number(), BIGINT, true] " +
                "| [8: last_value(4: c_0_2), DECIMAL128(27,19), true]\n" +
                "  |      [9: c_0_0, DATE, false] | [14: row_number(), BIGINT, true] " +
                "| [15: last_value(11: c_0_2), DECIMAL128(27,19), true]"));
    }

    @Test
    public void testMultiSubqueries() throws Exception {
        Pair<QueryDumpInfo, String> replayPair =
                getPlanFragment(getDumpInfoFromFile("query_dump/subquery_statistics"), null, TExplainLevel.COSTS);
        Assert.assertTrue(replayPair.second, replayPair.second.contains("  95:AGGREGATE (update serialize)\n" +
                "  |  aggregate: count[(*); args: ; result: BIGINT; args nullable: false; result nullable: false]\n" +
                "  |  hasNullableGenerateChild: true\n" +
                "  |  cardinality: 1\n" +
                "  |  column statistics: \n" +
                "  |  * count-->[0.0, 1.0420273298435367, 0.0, 8.0, 1.0] ESTIMATE\n" +
                "  |  \n" +
                "  94:Project\n" +
                "  |  output columns:\n" +
                "  |  548 <-> 1\n" +
                "  |  hasNullableGenerateChild: true\n" +
                "  |  cardinality: 1\n" +
                "  |  column statistics: \n" +
                "  |  * auto_fill_col-->[1.0, 1.0, 0.0, 1.0, 1.0] ESTIMATE"));
    }

    @Test
    public void testCorrelatedPredicateRewrite() throws Exception {
        Pair<QueryDumpInfo, String> replayPair =
                getPlanFragment(getDumpInfoFromFile("query_dump/union_with_subquery"), null, TExplainLevel.COSTS);
        Assert.assertTrue(replayPair.second, replayPair.second.contains("1110:HASH JOIN\n" +
                "  |  join op: RIGHT OUTER JOIN (BUCKET_SHUFFLE(S))\n" +
                "  |  equal join conjunct: [3807: ref_id, BIGINT, true] = [3680: deal_id, BIGINT, true]"));
    }

    @Test
    public void testManyPartitions() throws Exception {
        Pair<QueryDumpInfo, String> replayPair =
                getPlanFragment(getDumpInfoFromFile("query_dump/many_partitions"), null, TExplainLevel.NORMAL);
        Assert.assertTrue(replayPair.second, replayPair.second.contains("6:OlapScanNode\n" +
                "     TABLE: segment_profile\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: 11: segment_id = 6259, 12: version = 20221221\n" +
                "     partitions=17727/17727\n" +
                "     rollup: segment_profile\n" +
                "     tabletRatio=88635/88635"));
    }

    @Test
    public void testGroupByDistinctColumnSkewHint() throws Exception {
        Pair<QueryDumpInfo, String> replayPair =
                getPlanFragment(getDumpInfoFromFile("query_dump/group_by_count_distinct_skew_hint"), null,
                        TExplainLevel.NORMAL);
        Assert.assertTrue(replayPair.second, replayPair.second.contains("  9:Project\n" +
                "  |  <slot 39> : 39: year\n" +
                "  |  <slot 42> : 42: case\n" +
                "  |  <slot 45> : CAST(murmur_hash3_32(CAST(42: case AS VARCHAR)) % 512 AS SMALLINT)" +
                ""));
    }

    @Test
    public void testGroupByDistinctColumnOptimization() throws Exception {
        Pair<QueryDumpInfo, String> replayPair =
                getPlanFragment(getDumpInfoFromFile("query_dump/group_by_count_distinct_optimize"), null,
                        TExplainLevel.NORMAL);
        Assert.assertTrue(replayPair.second, replayPair.second.contains("  9:Project\n" +
                "  |  <slot 39> : 39: year\n" +
                "  |  <slot 42> : 42: case\n" +
                "  |  <slot 45> : CAST(murmur_hash3_32(CAST(42: case AS VARCHAR)) % 512 AS SMALLINT)" +
                ""));
    }

    @Test
    public void testSSBRightOuterJoinCase() throws Exception {
        Pair<QueryDumpInfo, String> replayPair =
                getPlanFragment(getDumpInfoFromFile("query_dump/right_outer_join_case"), null,
                        TExplainLevel.COSTS);
        Assert.assertTrue(replayPair.second.contains("4:NESTLOOP JOIN\n" +
                "  |  join op: RIGHT OUTER JOIN"));
    }

    @Test
    public void testTPCH11() throws Exception {
        try {
            FeConstants.USE_MOCK_DICT_MANAGER = true;
            Pair<QueryDumpInfo, String> replayPair =
                    getCostPlanFragment(getDumpInfoFromFile("query_dump/tpch_query11_mv_rewrite"));
            Assert.assertTrue(replayPair.second, replayPair.second.contains(
                    "n_name,[<place-holder> = 'GERMANY'])\n" +
                            "     dict_col=n_name"));
        } finally {
            FeConstants.USE_MOCK_DICT_MANAGER = false;
        }
    }

    @Test
    public void testPruneCTEProperty() throws Exception {
        String jsonStr = getDumpInfoFromFile("query_dump/cte_reuse");
        connectContext.getSessionVariable().disableJoinReorder();
        Pair<String, ExecPlan> result = UtFrameUtils.getNewPlanAndFragmentFromDump(connectContext,
                getDumpInfoFromJson(jsonStr));
        OptExpression expression = result.second.getPhysicalPlan().inputAt(1);
        Assert.assertEquals(new CTEProperty(1), expression.getLogicalProperty().getUsedCTEs());
        Assert.assertEquals(4, result.second.getCteProduceFragments().size());
    }

    @Test
    public void testHiveTPCH02UsingCatalog() throws Exception {
        FeConstants.isReplayFromQueryDump = true;
        Pair<QueryDumpInfo, String> replayPair =
                getPlanFragment(getDumpInfoFromFile("query_dump/hive_tpch02_catalog"), null,
                        TExplainLevel.NORMAL);
        Assert.assertTrue(replayPair.second, replayPair.second.contains("5:HASH JOIN\n" +
                "  |  join op: INNER JOIN (PARTITIONED)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 17: ps_partkey = 1: p_partkey"));
        FeConstants.isReplayFromQueryDump = false;
    }

    @Test
    public void testHiveTPCH05UsingCatalog() throws Exception {
        FeConstants.isReplayFromQueryDump = true;
        Pair<QueryDumpInfo, String> replayPair =
                getPlanFragment(getDumpInfoFromFile("query_dump/hive_tpch05_catalog"), null, TExplainLevel.NORMAL);
        Assert.assertTrue(replayPair.second, replayPair.second.contains("15:HASH JOIN\n" +
                "  |  join op: INNER JOIN (PARTITIONED)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 20: l_suppkey = 34: s_suppkey\n" +
                "  |  equal join conjunct: 4: c_nationkey = 37: s_nationkey\n" +
                "  |  \n" +
                "  |----14:EXCHANGE\n" +
                "  |    \n" +
                "  12:EXCHANGE"));
        FeConstants.isReplayFromQueryDump = false;
    }

    @Test
    public void testHiveTPCH08UsingCatalog() throws Exception {
        FeConstants.isReplayFromQueryDump = true;
        Pair<QueryDumpInfo, String> replayPair =
                getPlanFragment(getDumpInfoFromFile("query_dump/hive_tpch08_catalog"), null, TExplainLevel.NORMAL);
        Assert.assertTrue(replayPair.second, replayPair.second.contains(" 33:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BROADCAST)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 52: n_regionkey = 58: r_regionkey"));
        FeConstants.isReplayFromQueryDump = false;
    }

    @Test
    public void testHiveTPCH02UsingResource() throws Exception {
        Pair<QueryDumpInfo, String> replayPair =
                getPlanFragment(getDumpInfoFromFile("query_dump/hive_tpch02_resource"), null, TExplainLevel.COSTS);
        System.out.println(replayPair.second);
        Assert.assertTrue(replayPair.second, replayPair.second.contains("6:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BROADCAST)\n" +
                "  |  equal join conjunct: [24: n_regionkey, INT, true] = [26: r_regionkey, INT, true]\n" +
                "  |  build runtime filters:\n" +
                "  |  - filter_id = 0, build_expr = (26: r_regionkey), remote = false\n" +
                "  |  output columns: 22, 23\n" +
                "  |  cardinality: 23"));
    }

    @Test
    public void testHiveTPCH05UsingResource() throws Exception {
        Pair<QueryDumpInfo, String> replayPair =
                getPlanFragment(getDumpInfoFromFile("query_dump/hive_tpch05_resource"), null, TExplainLevel.COSTS);
        Assert.assertTrue(replayPair.second, replayPair.second.contains("20:HASH JOIN\n" +
                "  |  join op: INNER JOIN (PARTITIONED)\n" +
                "  |  equal join conjunct: [10: o_custkey, INT, true] = [1: c_custkey, INT, true]\n" +
                "  |  output columns: 4, 9\n" +
                "  |  cardinality: 22765073"));
    }

    @Test
    public void testHiveTPCH08UsingResource() throws Exception {
        Pair<QueryDumpInfo, String> replayPair =
                getPlanFragment(getDumpInfoFromFile("query_dump/hive_tpch08_resource"), null, TExplainLevel.COSTS);
        Assert.assertTrue(replayPair.second, replayPair.second.contains("21:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BROADCAST)\n" +
                "  |  equal join conjunct: [52: n_regionkey, INT, true] = [58: r_regionkey, INT, true]\n" +
                "  |  build runtime filters:\n" +
                "  |  - filter_id = 3, build_expr = (58: r_regionkey), remote = false\n" +
                "  |  output columns: 50\n" +
                "  |  cardinality: 5"));
    }
}
