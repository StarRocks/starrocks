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
import com.starrocks.common.Pair;
import com.starrocks.planner.OlapScanNode;
import com.starrocks.planner.PlanFragment;
import com.starrocks.planner.ScanNode;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.system.BackendResourceStat;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.utframe.UtFrameUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class AggregateTest extends PlanTestBase {

    @Test
    public void testHaving() throws Exception {
        String sql = "select v2 from t0 group by v2 having v2 > 0";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "PREDICATES: 2: v2 > 0");

        sql = "select sum(v1) from t0 group by v2 having v2 > 0";
        plan = getFragmentPlan(sql);
        assertContains(plan, "PREDICATES: 2: v2 > 0");

        sql = "select sum(v1) from t0 group by v2 having sum(v1) > 0";
        plan = getFragmentPlan(sql);
        assertContains(plan, "having: 4: sum > 0");
    }

    @Test
    public void testHavingNullableSubQuery() throws Exception {
        FeConstants.runningUnitTest = true;
        {
            String sql = "SELECT t1a, count(*)\n" +
                    "FROM test_all_type_not_null\n" +
                    "GROUP BY t1a\n" +
                    "HAVING ANY_VALUE(t1a <= (\n" +
                    "\tSELECT t1a\n" +
                    "\tFROM test_all_type_not_null\n" +
                    "\tWHERE t1a = 'not exists'\n" +
                    "));";
            String plan = getVerboseExplain(sql);
            assertContains(plan, "  7:AGGREGATE (update finalize)\n" +
                    "  |  aggregate: count[(*); args: ; result: BIGINT; args nullable: false; " +
                    "result nullable: false], any_value[([22: expr, BOOLEAN, true]); args: BOOLEAN; " +
                    "result: BOOLEAN; args nullable: true; result nullable: true]\n" +
                    "  |  group by: [1: t1a, VARCHAR, false]\n" +
                    "  |  having: [24: any_value, BOOLEAN, true]\n" +
                    "  |  cardinality: 1\n" +
                    "  |  \n" +
                    "  6:Project\n" +
                    "  |  output columns:\n" +
                    "  |  1 <-> [1: t1a, VARCHAR, false]\n" +
                    "  |  22 <-> [1: t1a, VARCHAR, false] <= [11: t1a, VARCHAR, true]\n" +
                    "  |  cardinality: 1");
        }
        {
            String sql = "SELECT t1a, count(*)\n" +
                    "FROM test_all_type_not_null\n" +
                    "GROUP BY t1a\n" +
                    "HAVING ANY_VALUE(t1a <= '1');";
            String plan = getVerboseExplain(sql);
            assertContains(plan, "2:AGGREGATE (update finalize)\n" +
                    "  |  aggregate: count[(*); args: ; result: BIGINT; args nullable: false; " +
                    "result nullable: false], any_value[([11: expr, BOOLEAN, false]); " +
                    "args: BOOLEAN; result: BOOLEAN; args nullable: false; result nullable: true]\n" +
                    "  |  group by: [1: t1a, VARCHAR, false]\n" +
                    "  |  having: [13: any_value, BOOLEAN, true]\n" +
                    "  |  cardinality: 1\n" +
                    "  |  \n" +
                    "  1:Project\n" +
                    "  |  output columns:\n" +
                    "  |  1 <-> [1: t1a, VARCHAR, false]\n" +
                    "  |  11 <-> [1: t1a, VARCHAR, false] <= '1'\n" +
                    "  |  cardinality: 1");
        }
        FeConstants.runningUnitTest = false;
    }

    @Test
    public void testCountDistinctBitmapHll() throws Exception {
        String sql = "select count(distinct v1), count(distinct v2), count(distinct v3), count(distinct v4), " +
                "count(distinct b1), count(distinct b2), count(distinct b3), count(distinct b4) from test_object;";
        getFragmentPlan(sql);

        sql = "select count(distinct v1), count(distinct v2), " +
                "count(distinct h1), count(distinct h2) from test_object";
        getFragmentPlan(sql);
    }

    @Test
    public void testHaving2() throws Exception {
        String sql = "SELECT 8 from t0 group by v1 having avg(v2) < 63;";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "having: 4: avg < 63.0");
    }

    @Test
    public void testGroupByNull() throws Exception {
        String sql = "select count(*) from test_all_type group by null";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "<slot 11> : NULL");
    }

    @Test
    public void testSumDistinctConst() throws Exception {
        String sql = "select sum(2), sum(distinct 2) from test_all_type";
        String thriftPlan = getThriftPlan(sql);
        Assertions.assertTrue(thriftPlan.contains("function_name:multi_distinct_sum"));
    }

    @Test
    public void testGroupByAsAnalyze() throws Exception {
        String sql = "select BITOR(825279661, 1960775729) as a from test_all_type group by a";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "  1:Project\n" +
                "  |  <slot 11> : 1979700413");
        assertContains(plan, "  2:EXCHANGE\n" +
                "     limit: 1");
    }

    @Test
    public void testHavingAsAnalyze() throws Exception {
        String sql = "select count(*) as count1 from test_all_type having count1 > 1";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "having: 11: count > 1");
    }

    @Test
    public void testGroupByAsAnalyze2() throws Exception {
        String sql = "select v1 as v2 from t0 group by v1, v2;";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "group by: 1: v1, 2: v2");
    }

    @Test
    public void testDistinctRedundant() throws Exception {
        String sql = "SELECT DISTINCT + + v1, v1 AS col2 FROM t0;";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "  |  group by: 1: v1\n");
    }

    @Test
    public void testColocateAgg() throws Exception {
        FeConstants.runningUnitTest = true;
        String queryStr;
        String explainString;
        queryStr = "select k2, count(k3) from nocolocate3 group by k2";
        explainString = getFragmentPlan(queryStr);
        Assertions.assertTrue(explainString.contains("  3:AGGREGATE (merge finalize)\n"
                + "  |  output: count(4: count)\n"
                + "  |  group by: 2: k2\n"
                + "  |  \n"
                + "  2:EXCHANGE\n"
                + "\n"
                + "PLAN FRAGMENT 2\n"
                + " OUTPUT EXPRS:"));
        FeConstants.runningUnitTest = false;
    }

    @Test
    public void testDistinctWithGroupBy1() throws Exception {
        connectContext.getSessionVariable().setNewPlanerAggStage(3);
        String queryStr = "select avg(v1), count(distinct v1) from t0 group by v1";
        String explainString = getFragmentPlan(queryStr);
        assertContains(explainString, " 4:AGGREGATE (update finalize)\n" +
                "  |  output: avg(4: avg), count(1: v1)\n" +
                "  |  group by: 1: v1\n" +
                "  |  \n" +
                "  3:AGGREGATE (merge serialize)\n" +
                "  |  output: avg(4: avg)\n" +
                "  |  group by: 1: v1");
        connectContext.getSessionVariable().setNewPlanerAggStage(0);
    }

    @Test
    public void testGroupBy2() throws Exception {
        String queryStr = "select avg(v2) from t0 group by v2";

        int originInstanceNum = connectContext.getSessionVariable().getParallelExecInstanceNum();
        int originPipelineDop = connectContext.getSessionVariable().getPipelineDop();
        try {
            int cpuCores = 8;
            int expectedTotalDop = cpuCores / 2;
            {
                BackendResourceStat.getInstance().setCachedAvgNumHardwareCores(cpuCores);
                Pair<String, ExecPlan> plan = UtFrameUtils.getPlanAndFragment(connectContext, queryStr);
                String explainString = plan.second.getExplainString(TExplainLevel.NORMAL);
                assertContains(explainString, "2:Project\n" +
                        "  |  <slot 4> : 4: avg\n" +
                        "  |  \n" +
                        "  1:AGGREGATE (update finalize)\n" +
                        "  |  output: avg(2: v2)\n" +
                        "  |  group by: 2: v2\n" +
                        "  |  \n" +
                        "  0:OlapScanNode\n" +
                        "     TABLE: t0");

                PlanFragment aggPlan = plan.second.getFragments().get(0);
                String aggPlanStr = aggPlan.getExplainString(TExplainLevel.NORMAL);
                assertContains(aggPlanStr, "  2:Project\n"
                        + "  |  <slot 4> : 4: avg\n"
                        + "  |  \n"
                        + "  1:AGGREGATE (update finalize)\n"
                        + "  |  output: avg(2: v2)\n"
                        + "  |  group by: 2: v2\n"
                        + "  |  \n"
                        + "  0:OlapScanNode");
                Assertions.assertEquals(expectedTotalDop, aggPlan.getPipelineDop());
                Assertions.assertEquals(1, aggPlan.getParallelExecNum());
            }

            // Manually set dop
            {
                final int pipelineDop = 2;
                final int instanceNum = 2;
                connectContext.getSessionVariable().setPipelineDop(pipelineDop);
                connectContext.getSessionVariable().setParallelExecInstanceNum(instanceNum);
                Pair<String, ExecPlan> plan = UtFrameUtils.getPlanAndFragment(connectContext, queryStr);
                String explainString = plan.second.getExplainString(TExplainLevel.NORMAL);
                Assertions.assertTrue(explainString.contains("  2:Project\n"
                        + "  |  <slot 4> : 4: avg\n"
                        + "  |  \n"
                        + "  1:AGGREGATE (update finalize)\n"
                        + "  |  output: avg(2: v2)\n"
                        + "  |  group by: 2: v2\n"
                        + "  |  \n"
                        + "  0:OlapScanNode"));

                PlanFragment aggPlan = plan.second.getFragments().get(0);
                Assertions.assertEquals(1, aggPlan.getParallelExecNum());
                Assertions.assertEquals(pipelineDop, aggPlan.getPipelineDop());
            }
        } finally {
            connectContext.getSessionVariable().setPipelineDop(originPipelineDop);
            connectContext.getSessionVariable().setPipelineDop(originInstanceNum);
            BackendResourceStat.getInstance().setCachedAvgNumHardwareCores(1);
        }
    }

    @Test
    public void testAggregateConst() throws Exception {
        String sql = "select 'a', v2, sum(v1) from t0 group by 'a', v2; ";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "  2:Project\n"
                + "  |  <slot 2> : 2: v2\n"
                + "  |  <slot 5> : 5: sum\n"
                + "  |  <slot 6> : 'a'\n"
                + "  |  \n"
                + "  1:AGGREGATE (update finalize)\n"
                + "  |  output: sum(1: v1)\n"
                + "  |  group by: 2: v2\n");
    }

    @Test
    public void testAggregateAllConst() throws Exception {
        String sql = "select 'a', 'b' from t0 group by 'a', 'b'; ";
        String plan = getFragmentPlan(sql);
        // aggregate node will be pruned by PruneGroupByKeysRule
        assertContains(plan, "  1:Project\n" +
                "  |  <slot 4> : 'a'\n" +
                "  |  <slot 6> : 'b'");
        assertContains(plan, "  2:EXCHANGE\n" +
                "     limit: 1");
    }

    @Test
    public void testAggConstPredicate() throws Exception {
        String queryStr = "select MIN(v1) from t0 having abs(1) = 2";
        String explainString = getFragmentPlan(queryStr);
        Assertions.assertTrue(explainString.contains("  1:AGGREGATE (update finalize)\n"
                + "  |  output: min(1: v1)\n"
                + "  |  group by: \n"
                + "  |  having: abs(1) = 2\n"), explainString);
    }

    @Test
    public void testSumDistinctSmallInt() throws Exception {
        String sql = " select sum(distinct t1b) from test_all_type;";
        String thriftPlan = getThriftPlan(sql);
        Assertions.assertTrue(thriftPlan.contains("arg_types:[TTypeDesc(types:" +
                "[TTypeNode(type:SCALAR, scalar_type:TScalarType(type:SMALLINT))])]"));
    }

    @Test
    public void testCountDistinctWithMultiColumns() throws Exception {
        String sql = "select count(distinct t1b,t1c) from test_all_type";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "6:AGGREGATE (merge finalize)");
        assertContains(plan, "4:AGGREGATE (update serialize)\n" +
                "  |  output: count(if(2: t1b IS NULL, NULL, 3: t1c))");
    }

    @Test
    public void testCountDistinctWithIfNested() throws Exception {
        String sql = "select count(distinct t1b,t1c,t1d) from test_all_type";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "output: count(if(2: t1b IS NULL, NULL, if(3: t1c IS NULL, NULL, 4: t1d)))");

        sql = "select count(distinct t1b,t1c,t1d,t1e) from test_all_type group by t1f";
        plan = getFragmentPlan(sql);
        assertContains(plan,
                "output: count(if(2: t1b IS NULL, NULL, if(3: t1c IS NULL, NULL, if(4: t1d IS NULL, NULL, 5: t1e))))");
    }

    @Test
    public void testCountDistinctGroupByWithMultiColumns() throws Exception {
        String sql = "select count(distinct t1b,t1c) from test_all_type group by t1d";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "4:AGGREGATE (update finalize)\n" +
                "  |  output: count(if(2: t1b IS NULL, NULL, 3: t1c))");
    }

    @Test
    public void testCountDistinctWithDiffMultiColumns() throws Exception {
        String sql = "select count(distinct t1b,t1c), count(distinct t1b,t1d) from test_all_type";
        String plan = getFragmentPlan(sql);
        assertContains(plan, " 18:NESTLOOP JOIN\n" +
                "  |  join op: CROSS JOIN");
    }

    @Test
    public void testCountDistinctWithSameMultiColumns() throws Exception {
        String sql = "select count(distinct t1b,t1c), count(distinct t1b,t1c) from test_all_type";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "6:AGGREGATE (merge finalize)");

        sql = "select count(distinct t1b,t1c), count(distinct t1b,t1c) from test_all_type group by t1d";
        plan = getFragmentPlan(sql);
        assertContains(plan, "4:AGGREGATE (update finalize)");
    }

    @Test
    public void testNullableSameWithChildrenFunctions() throws Exception {
        String sql = "select distinct day(id_datetime) from test_all_type_partition_by_datetime";
        String plan = getVerboseExplain(sql);
        assertContains(plan, " 1:Project\n" +
                "  |  output columns:\n" +
                "  |  11 <-> day[([2: id_datetime, DATETIME, false]); args: DATETIME; " +
                "result: TINYINT; args nullable: false; result nullable: false]");

        sql = "select distinct 2 * v1 from t0_not_null";
        plan = getVerboseExplain(sql);
        assertContains(plan, "2:AGGREGATE (update finalize)\n" +
                "  |  group by: [4: expr, BIGINT, false]");

        sql = "select distinct cast(2.0 as decimal) * v1 from t0_not_null";
        plan = getVerboseExplain(sql);
        assertContains(plan, "2:AGGREGATE (update finalize)\n" +
                "  |  group by: [4: expr, DECIMAL128(28,0), true]");
    }

    @Test
    public void testCountDistinctMultiColumns2() throws Exception {
        connectContext.getSessionVariable().setNewPlanerAggStage(2);
        String sql = "select count(distinct L_SHIPMODE,L_ORDERKEY) from lineitem";
        String plan = getFragmentPlan(sql);
        // check use 4 stage agg plan
        assertContains(plan, "6:AGGREGATE (merge finalize)\n" +
                "  |  output: count(18: count)");
        assertContains(plan, "4:AGGREGATE (update serialize)\n" +
                "  |  output: count(if(15: L_SHIPMODE IS NULL, NULL, 1: L_ORDERKEY))\n" +
                "  |  group by: \n" +
                "  |  \n" +
                "  3:AGGREGATE (merge serialize)\n" +
                "  |  group by: 1: L_ORDERKEY, 15: L_SHIPMODE");
        assertContains(plan, "1:AGGREGATE (update serialize)\n" +
                "  |  STREAMING\n" +
                "  |  group by: 1: L_ORDERKEY, 15: L_SHIPMODE");

        sql = "select count(distinct L_SHIPMODE,L_ORDERKEY) from lineitem group by L_PARTKEY";
        plan = getFragmentPlan(sql);
        // check use 3 stage agg plan
        assertContains(plan, " 4:AGGREGATE (update finalize)\n" +
                "  |  output: count(if(15: L_SHIPMODE IS NULL, NULL, 1: L_ORDERKEY))\n" +
                "  |  group by: 2: L_PARTKEY\n" +
                "  |  \n" +
                "  3:AGGREGATE (merge serialize)\n" +
                "  |  group by: 1: L_ORDERKEY, 2: L_PARTKEY, 15: L_SHIPMODE");
        assertContains(plan, "1:AGGREGATE (update serialize)\n" +
                "  |  STREAMING\n" +
                "  |  group by: 1: L_ORDERKEY, 2: L_PARTKEY, 15: L_SHIPMODE");
        connectContext.getSessionVariable().setNewPlanerAggStage(0);
    }

    @Test
    public void testCountDistinctBoolTwoPhase() throws Exception {
        connectContext.getSessionVariable().setNewPlanerAggStage(2);
        String sql = "select count(distinct id_bool) from test_bool";
        String plan = getCostExplain(sql);
        assertContains(plan, "aggregate: multi_distinct_count[([11: id_bool, BOOLEAN, true]); " +
                "args: BOOLEAN; result: VARBINARY;");

        sql = "select sum(distinct id_bool) from test_bool";
        plan = getCostExplain(sql);
        assertContains(plan, "aggregate: multi_distinct_sum[([11: id_bool, BOOLEAN, true]); " +
                "args: BOOLEAN; result: VARBINARY;");
        connectContext.getSessionVariable().setNewPlanerAggStage(0);
    }

    @Test
    public void testCountDistinctFloatTwoPhase() throws Exception {
        connectContext.getSessionVariable().setNewPlanerAggStage(2);
        String sql = "select count(distinct t1e) from test_all_type";
        String plan = getCostExplain(sql);
        assertContains(plan, "aggregate: multi_distinct_count[([5: t1e, FLOAT, true]); " +
                "args: FLOAT; result: VARBINARY; args nullable: true; result nullable: false");
        connectContext.getSessionVariable().setNewPlanerAggStage(0);
    }

    @Test
    public void testCountDistinctGroupByFunction() throws Exception {
        FeConstants.runningUnitTest = true;
        String sql = "select L_LINENUMBER, date_trunc(\"day\",L_SHIPDATE) as day ,count(distinct L_ORDERKEY) from " +
                "lineitem group by L_LINENUMBER, day";
        String plan = getFragmentPlan(sql);
        // check use three stage aggregate
        assertContains(plan, "  4:AGGREGATE (merge finalize)\n" +
                "  |  output: count(18: count)\n" +
                "  |  group by: 4: L_LINENUMBER, 11: L_SHIPDATE\n" +
                "  |  \n" +
                "  3:EXCHANGE\n" +
                "\n" +
                "PLAN FRAGMENT 2\n" +
                " OUTPUT EXPRS:\n" +
                "  PARTITION: RANDOM\n" +
                "\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 03\n" +
                "    HASH_PARTITIONED: 4: L_LINENUMBER, 11: L_SHIPDATE\n" +
                "\n" +
                "  2:AGGREGATE (update serialize)\n" +
                "  |  STREAMING\n" +
                "  |  output: count(1: L_ORDERKEY)\n" +
                "  |  group by: 4: L_LINENUMBER, 11: L_SHIPDATE\n" +
                "  |  \n" +
                "  1:AGGREGATE (update finalize)\n" +
                "  |  group by: 4: L_LINENUMBER, 11: L_SHIPDATE, 1: L_ORDERKEY");
        FeConstants.runningUnitTest = false;
    }

    @Test
    public void testReplicatedAgg() throws Exception {
        connectContext.getSessionVariable().setEnableReplicationJoin(true);

        String sql = "select value, SUM(id) from join1 group by value";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "  1:AGGREGATE (update finalize)\n" +
                "  |  output: sum(2: id)\n" +
                "  |  group by: 3: value\n" +
                "  |  \n" +
                "  0:OlapScanNode");

        connectContext.getSessionVariable().setEnableReplicationJoin(false);
    }

    @Test
    public void testDuplicateAggregateFn() throws Exception {
        String sql = "select bitmap_union_count(b1) from test_object having count(distinct b1) > 2;";
        String plan = getFragmentPlan(sql);
        assertContains(plan, " OUTPUT EXPRS:13: bitmap_union_count\n" +
                "  PARTITION: RANDOM\n" +
                "\n" +
                "  RESULT SINK\n" +
                "\n" +
                "  1:AGGREGATE (update finalize)\n" +
                "  |  output: bitmap_union_count(5: b1)\n" +
                "  |  group by: \n" +
                "  |  having: 13: bitmap_union_count > 2");
    }

    @Test
    public void testDuplicateAggregateFn2() throws Exception {
        String sql = "select bitmap_union_count(b1), count(distinct b1) from test_object;";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "  2:Project\n" +
                "  |  <slot 13> : 13: bitmap_union_count\n" +
                "  |  <slot 14> : clone(13: bitmap_union_count)\n" +
                "  |  \n" +
                "  1:AGGREGATE (update finalize)\n" +
                "  |  output: bitmap_union_count(5: b1)");
    }

    @Test
    public void testIntersectCount() throws Exception {
        connectContext.getSessionVariable().setNewPlanerAggStage(2);
        String sql = "select intersect_count(b1, v1, 999999) from test_object;";
        String plan = getThriftPlan(sql);
        assertContains(plan, "int_literal:TIntLiteral(value:999999), " +
                "output_scale:-1, " +
                "has_nullable_child:false, is_nullable:false, is_monotonic:true, is_index_only_filter:false)])], " +
                "intermediate_tuple_id:2");
        connectContext.getSessionVariable().setNewPlanerAggStage(0);
    }

    @Test
    public void testMergeAggregateNormal() throws Exception {
        String sql;
        String plan;

        sql = "select distinct x1 from (select distinct v1 as x1 from t0) as q";
        plan = getFragmentPlan(sql);
        assertContains(plan, "1:AGGREGATE (update finalize)\n" +
                "  |  group by: 1: v1");

        sql = "select sum(x1) from (select sum(v1) as x1 from t0) as q";
        plan = getFragmentPlan(sql);
        assertContains(plan, "1:AGGREGATE (update finalize)\n" +
                "  |  output: sum(1: v1)\n" +
                "  |  group by:");

        sql = "select SUM(x1) from (select v2, sum(v1) as x1 from t0 group by v2) as q";
        plan = getFragmentPlan(sql);
        assertContains(plan, "1:AGGREGATE (update finalize)\n" +
                "  |  output: sum(1: v1)\n" +
                "  |  group by:");

        sql = "select v2, SUM(x1) from (select v2, v3, sum(v1) as x1 from t0 group by v2, v3) as q group by v2";
        plan = getFragmentPlan(sql);
        assertContains(plan, "  1:AGGREGATE (update finalize)\n" +
                "  |  output: sum(1: v1)\n" +
                "  |  group by: 2: v2\n" +
                "  |  \n" +
                "  0:OlapScanNode\n" +
                "     TABLE: t0");

        sql = "select SUM(x1) from (select v2, sum(distinct v1), sum(v3) as x1 from t0 group by v2) as q";
        plan = getFragmentPlan(sql);
        assertContains(plan, "  1:AGGREGATE (update finalize)\n" +
                "  |  output: sum(3: v3)\n" +
                "  |  group by: \n" +
                "  |  \n" +
                "  0:OlapScanNode\n" +
                "     TABLE: t0");

        sql = "select MAX(x1) from (select v2 as x1 from t0 union select v3 from t0) as q";
        plan = getFragmentPlan(sql);
        assertContains(plan, "  7:AGGREGATE (merge finalize)\n" +
                "  |  output: max(8: v2)\n" +
                "  |  group by: \n" +
                "  |  \n" +
                "  6:EXCHANGE\n" +
                "\n" +
                "PLAN FRAGMENT 1\n" +
                " OUTPUT EXPRS:\n" +
                "  PARTITION: RANDOM\n" +
                "\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 06\n" +
                "    UNPARTITIONED\n" +
                "\n" +
                "  5:AGGREGATE (update serialize)\n" +
                "  |  output: max(7: v2)\n" +
                "  |  group by: \n" +
                "  |  \n" +
                "  0:UNION");

        sql = "select MIN(x1) from (select distinct v2 as x1 from t0) as q";
        plan = getFragmentPlan(sql);
        assertContains(plan, "  1:AGGREGATE (update finalize)\n" +
                "  |  output: min(2: v2)\n" +
                "  |  group by: \n" +
                "  |  \n" +
                "  0:OlapScanNode\n" +
                "     TABLE: t0");

        sql = "select MIN(x1) from (select v2 as x1 from t0 group by v2) as q";
        plan = getFragmentPlan(sql);
        assertContains(plan, "  1:AGGREGATE (update finalize)\n" +
                "  |  output: min(2: v2)\n" +
                "  |  group by: \n" +
                "  |  \n" +
                "  0:OlapScanNode\n" +
                "     TABLE: t0");
    }

    @Test
    public void testMergeAggregateFailed() throws Exception {
        String sql;
        String plan;
        sql = "select avg(x1) from (select avg(v1) as x1 from t0) as q";
        plan = getFragmentPlan(sql);
        assertContains(plan, "  1:AGGREGATE (update finalize)\n" +
                "  |  output: avg(1: v1)\n" +
                "  |  group by: \n" +
                "  |  \n" +
                "  0:OlapScanNode");

        sql = "select SUM(v2) from (select v2, sum(v1) as x1 from t0 group by v2) as q";
        plan = getFragmentPlan(sql);
        assertContains(plan, "  2:AGGREGATE (update serialize)\n" +
                "  |  output: sum(2: v2)\n" +
                "  |  group by: \n" +
                "  |  \n" +
                "  1:AGGREGATE (update finalize)\n" +
                "  |  group by: 2: v2\n");
        sql = "select SUM(v2) from (select v2, sum(distinct v2) as x1 from t0 group by v2) as q";
        plan = getFragmentPlan(sql);
        assertContains(plan, "  2:AGGREGATE (update serialize)\n" +
                "  |  output: sum(2: v2)\n" +
                "  |  group by: \n" +
                "  |  \n" +
                "  1:AGGREGATE (update finalize)\n" +
                "  |  group by: 2: v2\n");
        sql = "select sum(distinct x1) from (select v2, sum(v2) as x1 from t0 group by v2) as q";
        plan = getFragmentPlan(sql);
        assertContains(plan, "  1:AGGREGATE (update finalize)\n" +
                "  |  output: sum(2: v2)\n" +
                "  |  group by: 2: v2\n" +
                "  |  \n" +
                "  0:OlapScanNode\n");

        sql = "select SUM(x1) from (select v2 as x1 from t0 union select v3 from t0) as q";
        plan = getFragmentPlan(sql);
        assertContains(plan, "  7:AGGREGATE (merge finalize)\n" +
                "  |  group by: 7: v2\n" +
                "  |  \n" +
                "  6:EXCHANGE\n" +
                "\n" +
                "PLAN FRAGMENT 2\n" +
                " OUTPUT EXPRS:\n" +
                "  PARTITION: RANDOM\n" +
                "\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 06\n" +
                "    HASH_PARTITIONED: 7: v2\n" +
                "\n" +
                "  5:AGGREGATE (update serialize)\n" +
                "  |  STREAMING\n" +
                "  |  group by: 7: v2\n");

        sql = "select SUM(x1) from (select v2 as x1 from t0 group by v2) as q";
        plan = getFragmentPlan(sql);
        assertContains(plan, "  2:AGGREGATE (update serialize)\n" +
                "  |  output: sum(2: v2)\n" +
                "  |  group by: \n" +
                "  |  \n" +
                "  1:AGGREGATE (update finalize)\n" +
                "  |  group by: 2: v2\n" +
                "  |  \n" +
                "  0:OlapScanNode");
    }

    @Test
    public void testMultiCountDistinctAggPhase() throws Exception {
        FeConstants.runningUnitTest = true;
        String sql = "select count(distinct t1a,t1b), avg(t1c) from test_all_type";
        String plan = getVerboseExplain(sql);
        assertContains(plan, " 2:AGGREGATE (update serialize)\n" +
                "  |  aggregate: count[(if[(1: t1a IS NULL, NULL, [2: t1b, SMALLINT, true]); " +
                "args: BOOLEAN,SMALLINT,SMALLINT; result: SMALLINT; args nullable: true; result nullable: true]); " +
                "args: SMALLINT; result: BIGINT; args nullable: true; result nullable: false], " +
                "avg[([12: avg, VARBINARY, true]); args: INT; result: VARBINARY; args nullable: true; " +
                "result nullable: true]");
        assertContains(plan, " 1:AGGREGATE (update serialize)\n" +
                "  |  aggregate: avg[([3: t1c, INT, true]); args: INT; result: VARBINARY; " +
                "args nullable: true; result nullable: true]\n" +
                "  |  group by: [1: t1a, VARCHAR, true], [2: t1b, SMALLINT, true]");
        FeConstants.runningUnitTest = false;
    }

    @Test
    public void testMultiCountDistinctType() throws Exception {
        FeConstants.runningUnitTest = true;
        String sql = "select count(distinct t1a,t1b) from test_all_type";
        String plan = getVerboseExplain(sql);
        assertContains(plan, "2:AGGREGATE (update serialize)\n" +
                "  |  aggregate: count[(if[(1: t1a IS NULL, NULL, [2: t1b, SMALLINT, true]); " +
                "args: BOOLEAN,SMALLINT,SMALLINT; result: SMALLINT; args nullable: true; " +
                "result nullable: true]); args: SMALLINT; result: BIGINT; args nullable: true; result nullable: false]");
        assertContains(plan, "4:AGGREGATE (merge finalize)\n" +
                "  |  aggregate: count[([11: count, BIGINT, false]); args: SMALLINT; " +
                "result: BIGINT; args nullable: true; result nullable: false]");
        FeConstants.runningUnitTest = false;
    }

    @Test
    public void testMultiCountDistinct() throws Exception {
        String queryStr = "select count(distinct k1, k2) from baseall group by k3";
        String explainString = getFragmentPlan(queryStr);
        Assertions.assertTrue(explainString.contains("group by: 1: k1, 2: k2, 3: k3"), explainString);

        queryStr = "select count(distinct k1) from baseall";
        explainString = getFragmentPlan(queryStr);
        Assertions.assertTrue(explainString.contains("multi_distinct_count(1: k1)"), explainString);

        queryStr = "select count(distinct k1, k2),  count(distinct k4) from baseall group by k3";
        explainString = getFragmentPlan(queryStr);
        Assertions.assertTrue(explainString.contains("13:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BUCKET_SHUFFLE(S))\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 16: k3 <=> 17: k3"), explainString);
    }

    @Test
    public void testVarianceStddevAnalyze() throws Exception {
        String sql = "select stddev_pop(1222) from (select 1) t;";
        assertPlanContains(sql, "2:AGGREGATE (update finalize)\n" +
                "  |  output: stddev_pop(1222)\n" +
                "  |  group by: \n" +
                "  |  \n" +
                "  1:Project\n" +
                "  |  <slot 5> : 1\n" +
                "  |  \n" +
                "  0:UNION\n" +
                "     constant exprs: \n" +
                "         NULL");
    }

    @Test
    public void testCountDistinctRewrite() throws Exception {
        String sql = "select count(distinct id) from test.bitmap_table";
        starRocksAssert.query(sql).explainContains("count(1: id)", "multi_distinct_count(1: id)");

        sql = "select count(distinct id2) from test.bitmap_table";
        starRocksAssert.query(sql).explainContains("count(2: id2)", "bitmap_union_count(2: id2)");

        sql = "select sum(id) / count(distinct id2) from test.bitmap_table";
        starRocksAssert.query(sql).explainContains("output: sum(1: id), bitmap_union_count(2: id2)");

        sql = "select count(distinct id2) from test.hll_table";
        starRocksAssert.query(sql).explainContains("hll_union_agg(2: id2)", "3: count");

        sql = "select sum(id) / count(distinct id2) from test.hll_table";
        starRocksAssert.query(sql).explainContains("sum(1: id), hll_union_agg(2: id2)");

        sql = "select count(distinct id2) from test.bitmap_table group by id order by count(distinct id2)";
        starRocksAssert.query(sql).explainContains();

        sql = "select count(distinct id2) from test.bitmap_table having count(distinct id2) > 0";
        starRocksAssert.query(sql)
                .explainContains("bitmap_union_count(2: id2)", "having: 3: count > 0");

        sql = "select count(distinct id2) from test.bitmap_table order by count(distinct id2)";
        starRocksAssert.query(sql).explainContains("3: count", "3:MERGING-EXCHANGE",
                "order by: <slot 3> 3: count ASC",
                "output: bitmap_union_count(2: id2)");
    }

    @Test
    public void testAggregateTwoLevelToOneLevelOptimization() throws Exception {
        String sql = "SELECT c2, count(*) FROM db1.tbl3 WHERE c1<10 GROUP BY c2;";
        String plan = getFragmentPlan(sql);
        Assertions.assertEquals(1, StringUtils.countMatches(plan, "AGGREGATE (update finalize)"));

        sql = " SELECT c2, count(*) FROM (SELECT t1.c2 as c2 FROM db1.tbl3 as t1 INNER JOIN [shuffle] db1.tbl4 " +
                "as t2 ON t1.c2=t2.c2 WHERE t1.c1<10) as t3 GROUP BY c2;";
        plan = getFragmentPlan(sql);
        Assertions.assertEquals(1, StringUtils.countMatches(plan, "AGGREGATE (update finalize)"));

        sql = "SELECT c2, count(*) FROM db1.tbl5 GROUP BY c2;";
        plan = getFragmentPlan(sql);
        Assertions.assertEquals(1, StringUtils.countMatches(plan, "AGGREGATE (update finalize)"));

        sql = "SELECT c3, count(*) FROM db1.tbl4 GROUP BY c3;";
        plan = getFragmentPlan(sql);
        Assertions.assertEquals(1, StringUtils.countMatches(plan, "AGGREGATE (update finalize)"));
    }

    @Test
    public void testDistinctPushDown() throws Exception {
        String sql = "select distinct k1 from (select distinct k1 from test.pushdown_test) t where k1 > 1";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "RESULT SINK\n" +
                "\n" +
                "  1:AGGREGATE (update finalize)\n" +
                "  |  group by: 1: k1\n" +
                "  |  \n" +
                "  0:OlapScanNode");
    }

    @Test
    public void testDistinctBinaryPredicateNullable() throws Exception {
        String sql = "select distinct L_ORDERKEY < L_PARTKEY from lineitem";
        String plan = getVerboseExplain(sql);
        assertContains(plan, " 2:AGGREGATE (update finalize)\n" +
                "  |  group by: [18: expr, BOOLEAN, false]");

        sql = "select distinct v1 <=> v2 from t0";
        plan = getVerboseExplain(sql);
        assertContains(plan, "2:AGGREGATE (update finalize)\n" +
                "  |  group by: [4: expr, BOOLEAN, false]");
    }

    @Test
    public void testArrayAggFunctionWithColocateTable() throws Exception {
        FeConstants.runningUnitTest = true;
        String sql = "select L_ORDERKEY,retention([true,true]) from lineitem_partition_colocate group by L_ORDERKEY;";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "partitions=7/7");
        assertContains(plan, "1:AGGREGATE (update finalize)\n" +
                "  |  output: retention([TRUE,TRUE])\n" +
                "  |  group by: 1: L_ORDERKEY");

        sql = "select v1,retention([true,true]) from t0 group by v1";
        plan = getFragmentPlan(sql);
        assertContains(plan, "1:AGGREGATE (update finalize)\n" +
                "  |  output: retention([TRUE,TRUE])");

        sql = "select retention([])";
        plan = getVerboseExplain(sql);
        assertContains(plan, "  1:AGGREGATE (update finalize)\n" +
                "  |  aggregate: retention[([]); args: INVALID_TYPE; result: ARRAY<BOOLEAN>; " +
                "args nullable: true; result nullable: true]\n" +
                "  |  cardinality: 1");

        FeConstants.runningUnitTest = false;
    }

    @Test
    public void testWindowFunnel() throws Exception {
        FeConstants.runningUnitTest = true;
        String sql = "select L_ORDERKEY,window_funnel(1800, L_SHIPDATE, 0, [L_PARTKEY = 1]) " +
                "from lineitem_partition_colocate group by L_ORDERKEY;";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "window_funnel(1800, 11: L_SHIPDATE, 0, 18: expr)");

        sql = "select L_ORDERKEY,window_funnel(1800, L_SHIPDATE, 1, [L_PARTKEY = 1]) " +
                "from lineitem_partition_colocate group by L_ORDERKEY;";
        plan = getFragmentPlan(sql);
        assertContains(plan, "window_funnel(1800, 11: L_SHIPDATE, 1, 18: expr)");

        sql = "select L_ORDERKEY,window_funnel(1800, L_SHIPDATE, 2, [L_PARTKEY = 1]) " +
                "from lineitem_partition_colocate group by L_ORDERKEY;";
        plan = getFragmentPlan(sql);
        assertContains(plan, "window_funnel(1800, 11: L_SHIPDATE, 2, 18: expr)");

        sql = "select L_ORDERKEY,window_funnel(1800, L_SHIPDATE, 3, [L_PARTKEY = 1]) " +
                "from lineitem_partition_colocate group by L_ORDERKEY;";
        plan = getFragmentPlan(sql);
        assertContains(plan, "window_funnel(1800, 11: L_SHIPDATE, 3, 18: expr)");

        sql = "select L_ORDERKEY,window_funnel(1800, L_LINENUMBER, 3, [L_PARTKEY = 1]) " +
                "from lineitem_partition_colocate group by L_ORDERKEY;";
        plan = getFragmentPlan(sql);
        assertContains(plan, "window_funnel(1800, 4: L_LINENUMBER, 3, 18: expr)");
        FeConstants.runningUnitTest = false;
    }

    @Test
    public void testWindowFunnelWithInvalidDecimalWindow() {
        FeConstants.runningUnitTest = true;
        Throwable exception = assertThrows(SemanticException.class, () -> {
            String sql = "select L_ORDERKEY,window_funnel(-1, L_SHIPDATE, 3, [L_PARTKEY = 1]) " +
                    "from lineitem_partition_colocate group by L_ORDERKEY;";
            try {
                getFragmentPlan(sql);
            } finally {
                FeConstants.runningUnitTest = false;
            }
        });
        assertThat(exception.getMessage(), containsString("window argument must >= 0"));
    }

    @Test
    public void testWindowFunnelWithConstantMultipleStage() throws Exception {
        FeConstants.runningUnitTest = true;
        String sql = "select /*+ SET_VAR (streaming_preaggregation_mode = 'auto',new_planner_agg_stage='3')*/ "
                + "1,2,(window_funnel(900,lo_orderdate,0,[(case when ((((cast(lo_orderdate as BIGINT)) + 1) % 3) "
                + "= 0) then 'A' when ((((cast(lo_orderdate as BIGINT)) + 1) % 3) = 1) then 'B' else 'C' end) = 'A', "
                + "(case when ((((cast(lo_orderdate as BIGINT)) + 1) % 3) = 0) then 'A' when ((((cast(lo_orderdate  "
                + "as BIGINT)) + 1) % 3) = 1) then 'B' else 'C' end) = 'B',(case when ((((cast(lo_orderdate as "
                +
                "BIGINT)) + 1) % 3) = 0) then 'A' when ((((cast(lo_orderdate as BIGINT)) + 1) % 3) = 1) then 'B' else "
                + "'C' end) = 'C'])) as __col_4, (count(distinct lo_orderdate)) "
                + "as __col_18 from lineorder_flat_for_mv group by 1,2";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "window_funnel(42: window_funnel, 900, 0)");
        FeConstants.runningUnitTest = false;
    }

    @Test
    public void testWindowFunnelWithOutMultipleStage() throws Exception {
        FeConstants.runningUnitTest = true;
        String sql = "select /*+ SET_VAR (streaming_preaggregation_mode = 'auto',new_planner_agg_stage='3')*/ "
                + "1,2,(window_funnel(900,lo_orderdate,0,[(case when ((((cast(lo_orderdate as BIGINT)) + 1) % 3) "
                + "= 0) then 'A' when ((((cast(lo_orderdate as BIGINT)) + 1) % 3) = 1) then 'B' else 'C' end) = 'A', "
                + "(case when ((((cast(lo_orderdate as BIGINT)) + 1) % 3) = 0) then 'A' when ((((cast(lo_orderdate  "
                + "as BIGINT)) + 1) % 3) = 1) then 'B' else 'C' end) = 'B',(case when ((((cast(lo_orderdate as "
                +
                "BIGINT)) + 1) % 3) = 0) then 'A' when ((((cast(lo_orderdate as BIGINT)) + 1) % 3) = 1) then 'B' else "
                + "'C' end) = 'C'])) as __col_4, (count(distinct lo_orderdate)) "
                + "as __col_18 from lineorder_flat_for_mv";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "window_funnel(40: window_funnel, 900, 0)");
        FeConstants.runningUnitTest = false;
    }

    @Test
    public void testWindowFunnelWithMultipleStage() throws Exception {
        FeConstants.runningUnitTest = true;
        String sql = "select /*+ SET_VAR (streaming_preaggregation_mode = 'auto',new_planner_agg_stage='3') */ " +
                "year, k, __col_4,__col_18 from  (select  date_trunc('year',lo_orderdate) as year,left(c_name,1) as k," +
                "(window_funnel(900,lo_orderdate,0,[(case when ((((cast(lo_orderdate as BIGINT)) + 1) % 3) = 0) then 'A' " +
                "when ((((cast(lo_orderdate as BIGINT)) + 1) % 3) = 1) then 'B' else 'C' end) = 'A'," +
                "(case when ((((cast(lo_orderdate as BIGINT)) + 1) % 3) = 0) then 'A' " +
                "when ((((cast(lo_orderdate as BIGINT)) + 1) % 3) = 1) then 'B' else 'C' end) = 'B'," +
                "(case when ((((cast(lo_orderdate as BIGINT)) + 1) % 3) = 0) then 'A' " +
                "when ((((cast(lo_orderdate as BIGINT)) + 1) % 3) = 1) then 'B' else 'C' end) = 'C'])) as __col_4, " +
                "(count(distinct lo_orderdate)) as __col_18 from lineorder_flat_for_mv group " +
                "by date_trunc('year',lo_orderdate)," +
                "left(c_name,1) ) t;";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "window_funnel(42: window_funnel, 900, 0)");
        FeConstants.runningUnitTest = false;
    }

    @Test
    public void testLocalAggregateWithMultiStage() throws Exception {
        FeConstants.runningUnitTest = true;
        connectContext.getSessionVariable().setNewPlanerAggStage(2);
        String sql = "select distinct L_ORDERKEY from lineitem_partition_colocate where L_ORDERKEY = 59633893 ;";
        ExecPlan plan = getExecPlan(sql);
        Assertions.assertTrue(plan.getFragments().get(1).getPlanRoot().isColocate());

        connectContext.getSessionVariable().setNewPlanerAggStage(3);
        sql = "select count(distinct L_ORDERKEY) " +
                "from lineitem_partition_colocate where L_ORDERKEY = 59633893 group by L_ORDERKEY;";
        plan = getExecPlan(sql);
        Assertions.assertTrue(plan.getFragments().get(1).getPlanRoot().getChild(0).isColocate());

        sql = "select count(distinct L_ORDERKEY) from lineitem_partition_colocate";
        plan = getExecPlan(sql);
        Assertions.assertTrue(plan.getFragments().get(1).getPlanRoot().getChild(0).isColocate());

        sql = "select count(*) from lineitem_partition_colocate";
        plan = getExecPlan(sql);
        Assertions.assertFalse(plan.getFragments().get(1).getPlanRoot().isColocate());

        connectContext.getSessionVariable().setNewPlanerAggStage(2);
        sql = "select count(distinct L_ORDERKEY) " +
                "from lineitem_partition_colocate where L_ORDERKEY = 59633893 group by L_ORDERKEY;";
        plan = getExecPlan(sql);
        Assertions.assertTrue(plan.getFragments().get(1).getPlanRoot().getChild(0).isColocate());

        connectContext.getSessionVariable().setNewPlanerAggStage(0);
        FeConstants.runningUnitTest = false;
    }

    @Test
    public void testJoinOnPredicateEqualityPropertyInfo() throws Exception {
        FeConstants.runningUnitTest = true;
        {
            String sql = "select count(*) from ( select * from t0 join[bucket] t1 on t0.v1 = t1.v4 ) s1 group by s1.v4";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  5:AGGREGATE (update finalize)\n" +
                    "  |  output: count(*)\n" +
                    "  |  group by: 4: v4\n" +
                    "  |  \n" +
                    "  4:Project\n" +
                    "  |  <slot 4> : 4: v4\n" +
                    "  |  \n" +
                    "  3:HASH JOIN\n" +
                    "  |  join op: INNER JOIN (BUCKET_SHUFFLE)\n" +
                    "  |  colocate: false, reason: \n" +
                    "  |  equal join conjunct: 1: v1 = 4: v4");
        }
        {
            // Output group by column
            String sql = "select count(*), s1.v4 " +
                    "from ( select * from t0 join[bucket] t1 on t0.v1 = t1.v4 ) s1 group by s1.v4";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  5:AGGREGATE (update finalize)\n" +
                    "  |  output: count(*)\n" +
                    "  |  group by: 4: v4\n" +
                    "  |  \n" +
                    "  4:Project\n" +
                    "  |  <slot 4> : 4: v4\n" +
                    "  |  \n" +
                    "  3:HASH JOIN\n" +
                    "  |  join op: INNER JOIN (BUCKET_SHUFFLE)\n" +
                    "  |  colocate: false, reason: \n" +
                    "  |  equal join conjunct: 1: v1 = 4: v4");
        }
        FeConstants.runningUnitTest = false;
    }

    @Test
    public void testColocateJoinOnPredicateEqualityPropertyInfo() throws Exception {
        FeConstants.runningUnitTest = true;
        {
            String sql = "select count(*) from ( select * from colocate_t0 " +
                    "join[colocate] colocate_t1 on colocate_t0.v1 = colocate_t1.v4 ) s1 group by s1.v4";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  4:AGGREGATE (update finalize)\n" +
                    "  |  output: count(*)\n" +
                    "  |  group by: 4: v4\n" +
                    "  |  \n" +
                    "  3:Project\n" +
                    "  |  <slot 4> : 4: v4\n" +
                    "  |  \n" +
                    "  2:HASH JOIN\n" +
                    "  |  join op: INNER JOIN (COLOCATE)\n" +
                    "  |  colocate: true\n" +
                    "  |  equal join conjunct: 1: v1 = 4: v4");
        }
        {
            String sql =
                    "select s1.v1, sum(1) from ( select * from t1 join[bucket] t0 on t1.v4 = t0.v1 ) s1 " +
                            "group by s1.v1";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  5:AGGREGATE (update finalize)\n" +
                    "  |  output: sum(1)\n" +
                    "  |  group by: 4: v1\n" +
                    "  |  \n" +
                    "  4:Project\n" +
                    "  |  <slot 4> : 4: v1\n" +
                    "  |  \n" +
                    "  3:HASH JOIN\n" +
                    "  |  join op: INNER JOIN (BUCKET_SHUFFLE)\n" +
                    "  |  colocate: false, reason: \n" +
                    "  |  equal join conjunct: 1: v4 = 4: v1");
        }
        FeConstants.runningUnitTest = false;
    }

    @Test
    public void testAggWithSubquery() throws Exception {
        String sql = "select sum(case when v4 = (select v1 from t0) then v4 end) from t1";
        String plan = getFragmentPlan(sql);

        Assertions.assertTrue(plan.contains("  7:AGGREGATE (update serialize)\n" +
                "  |  output: sum(if(1: v4 = 4: v1, 1: v4, NULL))\n" +
                "  |  group by: \n" +
                "  |  \n" +
                "  6:Project\n" +
                "  |  <slot 1> : 1: v4\n" +
                "  |  <slot 4> : 4: v1\n" +
                "  |  \n" +
                "  5:NESTLOOP JOIN\n" +
                "  |  join op: CROSS JOIN\n" +
                "  |  colocate: false, reason: \n" +
                "  |  \n" +
                "  |----4:EXCHANGE\n" +
                "  |    \n" +
                "  0:OlapScanNode"));

        Assertions.assertTrue(plan.contains("  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 04\n" +
                "    UNPARTITIONED\n" +
                "\n" +
                "  3:ASSERT NUMBER OF ROWS\n" +
                "  |  assert number of rows: LE 1\n" +
                "  |  \n" +
                "  2:EXCHANGE"));

        Assertions.assertTrue(plan.contains("PLAN FRAGMENT 3\n" +
                " OUTPUT EXPRS:\n" +
                "  PARTITION: RANDOM\n" +
                "\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 02\n" +
                "    UNPARTITIONED\n" +
                "\n" +
                "  1:OlapScanNode\n" +
                "     TABLE: t0"));
    }

    @Test
    public void testOnlyFullGroupBy() throws Exception {
        long sqlmode = connectContext.getSessionVariable().getSqlMode();
        connectContext.getSessionVariable().setSqlMode(0);
        connectContext.getSessionVariable().setEnableEliminateAgg(false);
        String sql = "select v1, v2 from t0 group by v1";
        String plan = getFragmentPlan(sql);
        Assertions.assertTrue(plan.contains("PLAN FRAGMENT 0\n" +
                " OUTPUT EXPRS:1: v1 | 4: any_value\n" +
                "  PARTITION: RANDOM\n" +
                "\n" +
                "  RESULT SINK\n" +
                "\n" +
                "  1:AGGREGATE (update finalize)\n" +
                "  |  output: any_value(2: v2)\n" +
                "  |  group by: 1: v1\n" +
                "  |  \n" +
                "  0:OlapScanNode\n" +
                "     TABLE: t0\n" +
                "     PREAGGREGATION: ON\n" +
                "     partitions=0/1\n" +
                "     rollup: t0\n" +
                "     tabletRatio=0/0\n" +
                "     tabletList=\n" +
                "     cardinality=1\n" +
                "     avgRowSize=2.0\n"), plan);

        sql = "select v1, sum(v2) from t0";
        plan = getFragmentPlan(sql);
        Assertions.assertTrue(plan.contains("PLAN FRAGMENT 0\n" +
                " OUTPUT EXPRS:5: any_value | 4: sum\n" +
                "  PARTITION: RANDOM\n" +
                "\n" +
                "  RESULT SINK\n" +
                "\n" +
                "  1:AGGREGATE (update finalize)\n" +
                "  |  output: sum(2: v2), any_value(1: v1)\n" +
                "  |  group by: \n" +
                "  |  \n" +
                "  0:OlapScanNode\n" +
                "     TABLE: t0\n" +
                "     PREAGGREGATION: ON\n" +
                "     partitions=0/1\n" +
                "     rollup: t0\n" +
                "     tabletRatio=0/0\n" +
                "     tabletList=\n" +
                "     cardinality=1\n" +
                "     avgRowSize=2.0\n"));

        sql = "select max(v2) from t0 having v1 = 1";
        plan = getFragmentPlan(sql);
        Assertions.assertTrue(plan.contains("PLAN FRAGMENT 0\n" +
                " OUTPUT EXPRS:4: max\n" +
                "  PARTITION: RANDOM\n" +
                "\n" +
                "  RESULT SINK\n" +
                "\n" +
                "  2:Project\n" +
                "  |  <slot 4> : 4: max\n" +
                "  |  \n" +
                "  1:AGGREGATE (update finalize)\n" +
                "  |  output: max(2: v2), any_value(1: v1)\n" +
                "  |  group by: \n" +
                "  |  having: 5: any_value = 1\n" +
                "  |  \n" +
                "  0:OlapScanNode\n" +
                "     TABLE: t0\n" +
                "     PREAGGREGATION: ON\n" +
                "     partitions=0/1\n" +
                "     rollup: t0\n" +
                "     tabletRatio=0/0\n" +
                "     tabletList=\n" +
                "     cardinality=1\n" +
                "     avgRowSize=2.0\n"));

        sql = "select v1, max(v2) from t0 having v1 = 1";
        plan = getFragmentPlan(sql);
        Assertions.assertTrue(plan.contains("PLAN FRAGMENT 0\n" +
                " OUTPUT EXPRS:5: any_value | 4: max\n" +
                "  PARTITION: RANDOM\n" +
                "\n" +
                "  RESULT SINK\n" +
                "\n" +
                "  1:AGGREGATE (update finalize)\n" +
                "  |  output: max(2: v2), any_value(1: v1)\n" +
                "  |  group by: \n" +
                "  |  having: 5: any_value = 1\n" +
                "  |  \n" +
                "  0:OlapScanNode\n" +
                "     TABLE: t0\n" +
                "     PREAGGREGATION: ON\n" +
                "     partitions=0/1\n" +
                "     rollup: t0\n" +
                "     tabletRatio=0/0\n" +
                "     tabletList=\n" +
                "     cardinality=1\n" +
                "     avgRowSize=2.0\n"));

        sql = "select v1 from t0 group by v2 order by v3";
        plan = getFragmentPlan(sql);
        Assertions.assertTrue(plan.contains("PLAN FRAGMENT 0\n" +
                " OUTPUT EXPRS:4: any_value\n" +
                "  PARTITION: UNPARTITIONED\n" +
                "\n" +
                "  RESULT SINK\n" +
                "\n" +
                "  5:Project\n" +
                "  |  <slot 4> : 4: any_value\n" +
                "  |  \n" +
                "  4:MERGING-EXCHANGE\n" +
                "\n" +
                "PLAN FRAGMENT 1\n" +
                " OUTPUT EXPRS:\n" +
                "  PARTITION: RANDOM\n" +
                "\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 04\n" +
                "    UNPARTITIONED\n" +
                "\n" +
                "  3:SORT\n" +
                "  |  order by: <slot 5> 5: any_value ASC\n" +
                "  |  offset: 0\n" +
                "  |  \n" +
                "  2:Project\n" +
                "  |  <slot 4> : 4: any_value\n" +
                "  |  <slot 5> : 5: any_value\n" +
                "  |  \n" +
                "  1:AGGREGATE (update finalize)\n" +
                "  |  output: any_value(1: v1), any_value(3: v3)\n" +
                "  |  group by: 2: v2\n" +
                "  |  \n" +
                "  0:OlapScanNode\n" +
                "     TABLE: t0\n" +
                "     PREAGGREGATION: ON\n" +
                "     partitions=0/1\n" +
                "     rollup: t0\n" +
                "     tabletRatio=0/0\n" +
                "     tabletList=\n" +
                "     cardinality=1\n" +
                "     avgRowSize=3.0\n"));

        sql = "select v1,abs(v1) + 1 from t0 group by v2 order by v3";
        plan = getFragmentPlan(sql);
        Assertions.assertTrue(plan.contains("PLAN FRAGMENT 0\n" +
                " OUTPUT EXPRS:4: any_value | 6: expr\n" +
                "  PARTITION: UNPARTITIONED\n" +
                "\n" +
                "  RESULT SINK\n" +
                "\n" +
                "  5:Project\n" +
                "  |  <slot 4> : 4: any_value\n" +
                "  |  <slot 6> : 6: expr\n" +
                "  |  \n" +
                "  4:MERGING-EXCHANGE\n" +
                "\n" +
                "PLAN FRAGMENT 1\n" +
                " OUTPUT EXPRS:\n" +
                "  PARTITION: RANDOM\n" +
                "\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 04\n" +
                "    UNPARTITIONED\n" +
                "\n" +
                "  3:SORT\n" +
                "  |  order by: <slot 5> 5: any_value ASC\n" +
                "  |  offset: 0\n" +
                "  |  \n" +
                "  2:Project\n" +
                "  |  <slot 4> : 4: any_value\n" +
                "  |  <slot 5> : 5: any_value\n" +
                "  |  <slot 6> : abs(4: any_value) + 1\n" +
                "  |  \n" +
                "  1:AGGREGATE (update finalize)\n" +
                "  |  output: any_value(1: v1), any_value(3: v3)\n" +
                "  |  group by: 2: v2\n" +
                "  |  \n" +
                "  0:OlapScanNode\n" +
                "     TABLE: t0\n" +
                "     PREAGGREGATION: ON\n" +
                "     partitions=0/1\n" +
                "     rollup: t0\n" +
                "     tabletRatio=0/0\n" +
                "     tabletList=\n" +
                "     cardinality=1\n" +
                "     avgRowSize=3.0\n"));

        sql = "select lead(v2) over(partition by v1) from t0 group by v1";
        plan = getFragmentPlan(sql);
        Assertions.assertTrue(plan.contains("" +
                "  1:AGGREGATE (update finalize)\n" +
                "  |  output: any_value(2: v2)\n" +
                "  |  group by: 1: v1"));

        sql = "select lead(v2) over(partition by v3) from t0 group by v1";
        plan = getFragmentPlan(sql);
        Assertions.assertTrue(plan.contains(
                "  1:AGGREGATE (update finalize)\n" +
                        "  |  output: any_value(2: v2), any_value(3: v3)\n" +
                        "  |  group by: 1: v1"));

        sql = "select lead(v2) over(partition by v1 order by v3) from t0 group by v1";
        plan = getFragmentPlan(sql);
        Assertions.assertTrue(plan.contains(
                "  1:AGGREGATE (update finalize)\n" +
                        "  |  output: any_value(2: v2), any_value(3: v3)\n" +
                        "  |  group by: 1: v1"));

        sql = "select v1, v2,sum(if (v2 =2,1,2)) from t0 group by v1";
        plan = getFragmentPlan(sql);
        Assertions.assertTrue(plan.contains("PLAN FRAGMENT 0\n" +
                " OUTPUT EXPRS:1: v1 | 6: any_value | 5: sum\n" +
                "  PARTITION: RANDOM\n" +
                "\n" +
                "  RESULT SINK\n" +
                "\n" +
                "  2:AGGREGATE (update finalize)\n" +
                "  |  output: sum(4: if), any_value(2: v2)\n" +
                "  |  group by: 1: v1\n" +
                "  |  \n" +
                "  1:Project\n" +
                "  |  <slot 1> : 1: v1\n" +
                "  |  <slot 2> : 2: v2\n" +
                "  |  <slot 4> : if(2: v2 = 2, 1, 2)\n" +
                "  |  \n" +
                "  0:OlapScanNode\n" +
                "     TABLE: t0\n" +
                "     PREAGGREGATION: ON\n" +
                "     partitions=0/1\n" +
                "     rollup: t0\n" +
                "     tabletRatio=0/0\n" +
                "     tabletList=\n" +
                "     cardinality=1\n" +
                "     avgRowSize=3.0\n"));

        connectContext.getSessionVariable().setSqlMode(sqlmode);
    }

    @Test
    public void testMultiCountDistinctWithNoneGroup() throws Exception {
        FeConstants.runningUnitTest = true;
        String sql = "select count(distinct t1b), count(distinct t1c) from test_all_type";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "  MultiCastDataSinks\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 01\n" +
                "    RANDOM\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 09\n" +
                "    RANDOM");
        assertContains(plan, "  18:NESTLOOP JOIN\n" +
                "  |  join op: CROSS JOIN\n" +
                "  |  colocate: false, reason: \n");
        assertContains(plan, "  3:AGGREGATE (update serialize)\n" +
                "  |  STREAMING\n" +
                "  |  group by: 13: t1b");
        assertContains(plan, "  11:AGGREGATE (update serialize)\n" +
                "  |  STREAMING\n" +
                "  |  group by: 14: t1c");
        FeConstants.runningUnitTest = false;
    }

    @Test
    public void testMultiCountDistinctWithNoneGroup1() throws Exception {
        FeConstants.runningUnitTest = true;
        String sql = "with tmp1 as (select 'a' as a from dual), tmp2 as (select 'b' as b from dual) " +
                "select count(distinct t1b), count(distinct t1c), count(distinct t1.a), count(distinct t2.b) " +
                "from test_all_type join tmp1 t1 join tmp2 t2 join tmp1 t3 join tmp2 t4";
        Pair<String, ExecPlan> pair = UtFrameUtils.getPlanAndFragment(connectContext, sql);
        System.out.println(pair.first);
        assertContains(pair.first, "CTEAnchor(cteid=1)");
        FeConstants.runningUnitTest = false;
    }

    @Test
    public void testMultiCountDistinctWithNoneGroup2() throws Exception {
        FeConstants.runningUnitTest = true;
        String sql = "select count(distinct t1b), count(distinct t1c), sum(t1c), max(t1b) from test_all_type";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "MultiCastDataSinks\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 01\n" +
                "    RANDOM\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 09\n" +
                "    RANDOM\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 19\n" +
                "    RANDOM");
        assertContains(plan, "21:AGGREGATE (update serialize)\n" +
                "  |  output: sum(18: t1c), max(17: t1b)\n" +
                "  |  group by: \n" +
                "  |  \n" +
                "  20:Project\n" +
                "  |  <slot 17> : 2: t1b\n" +
                "  |  <slot 18> : 3: t1c");
        assertContains(plan, "6:AGGREGATE (update serialize)\n" +
                "  |  output: count(15: t1b)\n" +
                "  |  group by: \n" +
                "  |  \n" +
                "  5:AGGREGATE (merge serialize)\n" +
                "  |  group by: 15: t1b");
        FeConstants.runningUnitTest = false;
    }

    @Test
    public void testMultiCountDistinctWithNoneGroup3() throws Exception {
        FeConstants.runningUnitTest = true;
        String sql = "select count(distinct t1b), count(distinct t1c) from test_all_type";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "18:NESTLOOP JOIN\n" +
                "  |  join op: CROSS JOIN");
        FeConstants.runningUnitTest = false;
    }

    @Test
    public void testMultiCountDistinctWithNoneGroup4() throws Exception {
        FeConstants.runningUnitTest = true;
        String sql = "select count(distinct t1b + 1), count(distinct t1c + 2) from test_all_type";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "1:Project\n" +
                "  |  <slot 11> : CAST(2: t1b AS INT) + 1\n" +
                "  |  <slot 12> : CAST(3: t1c AS BIGINT) + 2");
        FeConstants.runningUnitTest = false;
    }

    @Test
    public void testMultiAvgDistinctWithNoneGroup() throws Exception {
        String sql = "select avg(distinct t1b) from test_all_type";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "4:AGGREGATE (update serialize)\n" +
                "  |  output: avg(2: t1b)\n" +
                "  |  group by: \n" +
                "  |  \n" +
                "  3:AGGREGATE (merge serialize)\n" +
                "  |  group by: 2: t1b");

        sql = "select avg(distinct t1b), count(distinct t1b) from test_all_type";
        plan = getFragmentPlan(sql);
        assertContains(plan, "4:AGGREGATE (update serialize)\n" +
                "  |  output: avg(2: t1b), count(2: t1b)\n" +
                "  |  group by: \n" +
                "  |  \n" +
                "  3:AGGREGATE (merge serialize)\n" +
                "  |  group by: 2: t1b");

        sql = "select avg(distinct t1b), count(distinct t1b), sum(distinct t1b) from test_all_type";
        plan = getFragmentPlan(sql);
        assertContains(plan, "4:AGGREGATE (update serialize)\n" +
                "  |  output: avg(2: t1b), count(2: t1b), sum(2: t1b)\n" +
                "  |  group by: \n" +
                "  |  \n" +
                "  3:AGGREGATE (merge serialize)\n" +
                "  |  group by: 2: t1b");

        sql =
                "select avg(distinct t1b + 1), count(distinct t1b+1), sum(distinct t1b + 1), count(t1b) from test_all_type";
        plan = getFragmentPlan(sql);
        assertContains(plan, "7:AGGREGATE (merge finalize)\n" +
                "  |  output: avg(12: avg), count(13: count), sum(14: sum), count(15: count)\n" +
                "  |  group by:");

        sql =
                "select avg(distinct t1b + 1), count(distinct t1b), sum(distinct t1c), count(t1c), sum(t1c) from test_all_type";
        plan = getFragmentPlan(sql);
        assertContains(plan, "47:Project\n" +
                "  |  <slot 12> : CAST(19: sum AS DOUBLE) / CAST(21: count AS DOUBLE)\n" +
                "  |  <slot 13> : 13: count\n" +
                "  |  <slot 14> : 14: sum\n" +
                "  |  <slot 15> : 15: count\n" +
                "  |  <slot 16> : 16: sum");

        sql = "select avg(distinct 1), count(distinct null), count(distinct 1) from test_all_type";
        plan = getFragmentPlan(sql);
        assertContains(plan, "4:AGGREGATE (merge finalize)\n" +
                "  |  output: avg(11: avg), multi_distinct_count(12: count), multi_distinct_count(13: count)");

        sql = "select avg(distinct 1), count(distinct null), count(distinct 1), " +
                "count(distinct (t1a + t1c)), sum(t1c) from test_all_type";
        plan = getFragmentPlan(sql);
        assertContains(plan, "7:AGGREGATE (merge finalize)\n" +
                "  |  output: avg(12: avg), count(13: count), count(14: count), count(15: count), sum(16: sum)");
        assertContains(plan, "5:AGGREGATE (update serialize)\n" +
                "  |  output: avg(1), count(NULL), count(1), count(11: expr), sum(16: sum)\n" +
                "  |  group by: \n" +
                "  |  \n" +
                "  4:AGGREGATE (merge serialize)\n" +
                "  |  output: sum(16: sum)\n" +
                "  |  group by: 11: expr");
    }

    @Test
    public void testMultiDistinctAggregate() throws Exception {
        String sql = "select count(distinct t1b), count(distinct t1b, t1c) from test_all_type";
        connectContext.getSessionVariable().setOptimizerExecuteTimeout(-1);
        String plan = getFragmentPlan(sql);
        assertContains(plan, "MultiCastDataSinks\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 01\n" +
                "    RANDOM\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 09\n" +
                "    RANDOM");
        assertContains(plan, "  18:NESTLOOP JOIN\n" +
                "  |  join op: CROSS JOIN\n" +
                "  |  colocate: false, reason: \n");

        sql =
                "select count(distinct t1b) as cn_t1b, count(distinct t1b, t1c) cn_t1b_t1c from test_all_type group by t1a";
        plan = getFragmentPlan(sql);
        assertContains(plan, "13:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BUCKET_SHUFFLE(S))\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 13: t1a <=> 15: t1a");

        sql = "select count(distinct t1b) as cn_t1b, " +
                "count(distinct t1b, t1c) cn_t1b_t1c from test_all_type group by t1a,t1b,t1c";
        plan = getFragmentPlan(sql);
        assertContains(plan, "13:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BUCKET_SHUFFLE(S))\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 13: t1a <=> 16: t1a\n" +
                "  |  equal join conjunct: 14: t1b <=> 17: t1b\n" +
                "  |  equal join conjunct: 15: t1c <=> 18: t1c");

        sql = "select avg(distinct t1b) as cn_t1b, sum(distinct t1b), " +
                "count(distinct t1b, t1c) cn_t1b_t1c from test_all_type group by t1c";
        plan = getFragmentPlan(sql);
        assertContains(plan, "13:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BUCKET_SHUFFLE(S))\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 15: t1c <=> 17: t1c\n");
        assertContains(plan, "20:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BUCKET_SHUFFLE(S))\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 15: t1c <=> 20: t1c");

        sql = "select avg(distinct t1b) as cn_t1b, sum(distinct t1b), " +
                "count(distinct t1b, t1c) cn_t1b_t1c from test_all_type group by t1c, t1b+1";
        plan = getFragmentPlan(sql);
        assertContains(plan, "1:Project\n" +
                "  |  <slot 2> : 2: t1b\n" +
                "  |  <slot 3> : 3: t1c\n" +
                "  |  <slot 11> : CAST(2: t1b AS INT) + 1");
        assertContains(plan, "21:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BUCKET_SHUFFLE(S))\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 16: t1c <=> 23: t1c\n" +
                "  |  equal join conjunct: 17: expr <=> 24: expr");

        sql = "select avg(distinct t1b) as cn_t1b, sum(t1b), " +
                "count(distinct t1b, t1c) cn_t1b_t1c from test_all_type group by t1c, t1b+1";
        plan = getFragmentPlan(sql);
        assertContains(plan, "27:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BUCKET_SHUFFLE(S))\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 16: t1c <=> 27: t1c\n" +
                "  |  equal join conjunct: 17: expr <=> 28: expr");
        // count distinct with grouping sets

        sql = "select avg(distinct t1b) as cn_t1b, sum(distinct t1b), " +
                "count(distinct t1b, t1c) cn_t1b_t1c from test_all_type group by rollup(t1c, t1b)";
        plan = getFragmentPlan(sql);
        // make sure repeat + project + multi cast sink
        assertContains(plan, "  MultiCastDataSinks\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 04\n" +
                "    RANDOM\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 10\n" +
                "    RANDOM\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 17\n" +
                "    RANDOM\n" +
                "\n" +
                "  3:Project\n" +
                "  |  <slot 2> : 2: t1b\n" +
                "  |  <slot 3> : 3: t1c\n" +
                "  |  <slot 13> : 13: expr\n" +
                "  |  <slot 14> : 14: expr\n" +
                "  |  <slot 18> : 18: GROUPING_ID\n" +
                "  |  \n" +
                "  2:REPEAT_NODE\n" +
                "  |  repeat: repeat 2 lines [[], [3], [2, 3]]");
    }

    @Test
    public void testSumString() throws Exception {
        String sql = "select sum(N_COMMENT) from nation";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "output: sum(CAST(4: N_COMMENT AS DOUBLE))");
    }

    @Test
    public void testGroupByConstant() throws Exception {
        connectContext.getSessionVariable().setNewPlanerAggStage(4);
        String sql = "select count(distinct L_PARTKEY) from lineitem group by 1.0001";
        String plan = getFragmentPlan(sql);
        // check four phase aggregate
        assertContains(plan, "7:AGGREGATE (merge finalize)\n" +
                "  |  output: count(19: count)\n" +
                "  |  group by: 18: expr");

        sql = "select count(distinct L_PARTKEY) from lineitem group by 1.0001, 2.0001";
        plan = getFragmentPlan(sql);
        // check four phase aggregate
        assertContains(plan, "7:AGGREGATE (merge finalize)\n" +
                "  |  output: count(20: count)\n" +
                "  |  group by: 18: expr");

        sql = "select count(distinct L_PARTKEY + 1) from lineitem group by 1.0001";
        plan = getFragmentPlan(sql);
        assertContains(plan, "7:AGGREGATE (merge finalize)\n" +
                "  |  output: count(20: count)\n" +
                "  |  group by: 18: expr");

        sql = "select count(distinct L_SUPPKEY), count(L_PARTKEY) from lineitem group by 1.0001";
        plan = getFragmentPlan(sql);
        assertContains(plan, "8:Project\n" +
                "  |  <slot 19> : 19: count\n" +
                "  |  <slot 20> : 20: count\n" +
                "  |  \n" +
                "  7:AGGREGATE (merge finalize)\n" +
                "  |  output: count(19: count), count(20: count)\n" +
                "  |  group by: 18: expr");

        connectContext.getSessionVariable().setNewPlanerAggStage(0);
    }

    @Test
    public void testGroupByConstantWithAggPrune() throws Exception {
        FeConstants.runningUnitTest = true;

        String sql = "select count(distinct L_ORDERKEY) from lineitem group by 1.0001";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "3:AGGREGATE (update serialize)\n" +
                "  |  STREAMING\n" +
                "  |  output: count(1: L_ORDERKEY)\n" +
                "  |  group by: 18: expr\n" +
                "  |  \n" +
                "  2:AGGREGATE (update finalize)\n" +
                "  |  group by: 18: expr, 1: L_ORDERKEY\n" +
                "  |  \n" +
                "  1:Project\n" +
                "  |  <slot 1> : 1: L_ORDERKEY\n" +
                "  |  <slot 18> : 1.0001\n" +
                "  |  \n" +
                "  0:OlapScanNode");

        sql = "select count(distinct L_ORDERKEY) from lineitem group by 1.0001, 2.0001";
        plan = getFragmentPlan(sql);
        assertContains(plan, "3:AGGREGATE (update serialize)\n" +
                "  |  STREAMING\n" +
                "  |  output: count(1: L_ORDERKEY)\n" +
                "  |  group by: 18: expr\n" +
                "  |  \n" +
                "  2:AGGREGATE (update finalize)\n" +
                "  |  group by: 18: expr, 1: L_ORDERKEY\n" +
                "  |  \n" +
                "  1:Project\n" +
                "  |  <slot 1> : 1: L_ORDERKEY\n" +
                "  |  <slot 18> : 1.0001\n" +
                "  |  \n" +
                "  0:OlapScanNode");

        sql = "select count(distinct L_ORDERKEY), count(L_PARTKEY) from lineitem group by 1.0001";
        plan = getFragmentPlan(sql);
        assertContains(plan, "3:AGGREGATE (update serialize)\n" +
                "  |  STREAMING\n" +
                "  |  output: count(1: L_ORDERKEY), sum(21: count)\n" +
                "  |  group by: 18: expr\n" +
                "  |  \n" +
                "  2:AGGREGATE (update finalize)\n" +
                "  |  output: count(2: L_PARTKEY)\n" +
                "  |  group by: 18: expr, 1: L_ORDERKEY\n" +
                "  |  \n" +
                "  1:Project\n" +
                "  |  <slot 1> : 1: L_ORDERKEY\n" +
                "  |  <slot 2> : 2: L_PARTKEY\n" +
                "  |  <slot 18> : 1.0001\n" +
                "  |  \n" +
                "  0:OlapScanNode");

        FeConstants.runningUnitTest = false;
    }

    @Test
    public void testAggregateDuplicatedExprs() throws Exception {
        String plan = getFragmentPlan("SELECT " +
                "sum(arrays_overlap(v3, [1])) as q1, " +
                "sum(arrays_overlap(v3, [1])) as q2, " +
                "sum(arrays_overlap(v3, [1])) as q3 FROM tarray;");
        assertContains(plan, "  2:AGGREGATE (update finalize)\n" +
                "  |  output: sum(4: arrays_overlap)\n" +
                "  |  group by: \n" +
                "  |  \n" +
                "  1:Project\n" +
                "  |  <slot 4> : arrays_overlap(3: v3, CAST([1] AS ARRAY<BIGINT>))\n" +
                "  |  \n" +
                "  0:OlapScanNode");
    }

    @Test
    public void testOuterJoinSatisfyAgg() throws Exception {
        connectContext.getSessionVariable().setNewPlanerAggStage(1);
        String sql = "select distinct t0.v1  from t0 full outer join[shuffle] t1 on t0.v1 = t1.v4;";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "  7:AGGREGATE (update finalize)\n" +
                "  |  group by: 1: v1\n" +
                "  |  \n" +
                "  6:EXCHANGE");
        connectContext.getSessionVariable().setNewPlanerAggStage(0);
    }

    @Test
    public void testAvgCountDistinctWithHaving() throws Exception {
        String sql = "select avg(distinct s_suppkey), count(distinct s_acctbal) " +
                "from supplier having avg(distinct s_suppkey) > 3 ;";
        connectContext.getSessionVariable().setOptimizerExecuteTimeout(-1);
        String plan = getFragmentPlan(sql);
        assertContains(plan, "  28:NESTLOOP JOIN\n" +
                "  |  join op: INNER JOIN\n" +
                "  |  colocate: false, reason: \n" +
                "  |  other join predicates: CAST(12: sum AS DOUBLE) / CAST(14: count AS DOUBLE) > 3.0");
    }

    @Test
    public void testSortedStreamingAggregate() throws Exception {
        connectContext.getSessionVariable().setEnableSortAggregate(true);
        String sql;
        String plan;
        {
            sql = "select v1, sum(v3) from t0 group by v1";
            plan = getCostExplain(sql);
            assertContains(plan, "  1:AGGREGATE (update finalize)\n" +
                    "  |  aggregate: sum[([3: v3, BIGINT, true]); args: BIGINT; result: BIGINT;" +
                    " args nullable: true; result nullable: true]\n" +
                    "  |  group by: [1: v1, BIGINT, true]\n" +
                    "  |  sorted streaming: true");
        }
        {
            sql = "select count(l) from (select sum(length(v3)) l from t0 group by v1) tx";
            plan = getCostExplain(sql);
            assertContains(plan, "  2:AGGREGATE (update finalize)\n" +
                    "  |  aggregate: sum[([4: length, INT, true]); args: INT; result: BIGINT; args nullable: true;" +
                    " result nullable: true]\n" +
                    "  |  group by: [1: v1, BIGINT, true]\n" +
                    "  |  sorted streaming: true");
        }
        {
            try {
                FeConstants.USE_MOCK_DICT_MANAGER = true;
                connectContext.getSessionVariable().setEnableLowCardinalityOptimize(true);
                sql = "select sum(t1b), t1a from test_all_type group by t1a";
                plan = getCostExplain(sql);
                assertContains(plan, "sorted streaming: true");
                assertContains(plan, "Decode");
            } finally {
                connectContext.getSessionVariable().setEnableLowCardinalityOptimize(false);
                FeConstants.USE_MOCK_DICT_MANAGER = false;
            }
        }
        connectContext.getSessionVariable().setEnableSortAggregate(false);
    }

    @Test
    public void testBucketAggregate() throws Exception {
        FeConstants.runningUnitTest = true;
        connectContext.getSessionVariable().setEnablePartitionBucketOptimize(true);
        String sql;
        String plan;
        {
            sql = "select distinct L_ORDERKEY,L_SHIPDATE from lineitem_partition_colocate;";
            ExecPlan execPlan = getExecPlan(sql);
            ScanNode scanNode = execPlan.getScanNodes().get(0);
            plan = getFragmentPlan(sql);
            Assertions.assertTrue(((OlapScanNode) scanNode).getWithoutColocateRequirement());
            assertContains(plan, "  1:AGGREGATE (update finalize)\n" +
                    "  |  group by: 1: L_ORDERKEY, 11: L_SHIPDATE");
        }
        connectContext.getSessionVariable().setEnablePartitionBucketOptimize(false);
        FeConstants.runningUnitTest = false;
    }

    @Test
    public void testMergeAggPruneColumnPruneWindow() throws Exception {
        String sql = "select v2 " +
                "from ( " +
                "   select v2, x3 " +
                "   from (select v2, sum(v1) over (partition by v3) as x3 from t0) as tt0 " +
                "   group by v2, x3 " +
                ") ttt0 " +
                "group by v2";
        String plan = getFragmentPlan(sql);
        Assertions.assertFalse(plan.contains("ANALYTIC"));
        Assertions.assertEquals(1, StringUtils.countMatches(plan, ":AGGREGATE"));
    }

    @Test
    public void testExtractProject() throws Exception {
        connectContext.getSessionVariable().setEnableRewriteSumByAssociativeRule(false);
        String sql;
        String plan;

        sql = "select sum(t1c + 1) from test_all_type";
        plan = getFragmentPlan(sql);
        assertContains(plan, "  2:AGGREGATE (update finalize)\n" +
                "  |  output: sum(CAST(3: t1c AS BIGINT) + 1)\n" +
                "  |  group by: \n" +
                "  |  \n" +
                "  1:Project\n" +
                "  |  <slot 3> : 3: t1c\n" +
                "  |  \n" +
                "  0:OlapScanNode");

        sql = "select sum(t1c), sum(t1c + 1) from test_all_type";
        plan = getFragmentPlan(sql);
        assertContains(plan, "  2:AGGREGATE (update finalize)\n" +
                "  |  output: sum(3: t1c), sum(CAST(3: t1c AS BIGINT) + 1)\n" +
                "  |  group by: \n" +
                "  |  \n" +
                "  1:Project\n" +
                "  |  <slot 3> : 3: t1c");

        sql = "select sum(t1c + 1), sum(t1c + 2) from test_all_type";
        plan = getFragmentPlan(sql);
        assertContains(plan, "  2:AGGREGATE (update finalize)\n" +
                "  |  output: sum(15: cast + 1), sum(15: cast + 2)\n" +
                "  |  group by: \n" +
                "  |  \n" +
                "  1:Project\n" +
                "  |  <slot 15> : 15: cast\n" +
                "  |  common expressions:\n" +
                "  |  <slot 15> : CAST(3: t1c AS BIGINT)");

        sql = "select sum(t1c), sum(t1c + 1), sum(t1c + 2) from test_all_type";
        plan = getFragmentPlan(sql);
        assertContains(plan, "  2:AGGREGATE (update finalize)\n" +
                "  |  output: sum(3: t1c), sum(16: cast + 1), sum(16: cast + 2)\n" +
                "  |  group by: \n" +
                "  |  \n" +
                "  1:Project\n" +
                "  |  <slot 3> : 3: t1c\n" +
                "  |  <slot 16> : 16: cast\n" +
                "  |  common expressions:\n" +
                "  |  <slot 16> : CAST(3: t1c AS BIGINT)");

        sql = "select sum(t1c + 1), sum(t1c + 1 + 2), sum(t1d + 1 + 3) from test_all_type";
        plan = getFragmentPlan(sql);
        assertContains(plan, "  2:AGGREGATE (update finalize)\n" +
                "  |  output: sum(11: expr), sum(18: add + 2), sum(4: t1d + 1 + 3)\n" +
                "  |  group by: \n" +
                "  |  \n" +
                "  1:Project\n" +
                "  |  <slot 4> : 4: t1d\n" +
                "  |  <slot 11> : clone(18: add)\n" +
                "  |  <slot 18> : 18: add\n" +
                "  |  common expressions:\n" +
                "  |  <slot 17> : CAST(3: t1c AS BIGINT)\n" +
                "  |  <slot 18> : 17: cast + 1");
        connectContext.getSessionVariable().setEnableRewriteSumByAssociativeRule(true);

        connectContext.getSessionVariable().setNewPlanerAggStage(3);
        sql = "select count(distinct t1c, upper(id_datetime)) from test_all_type";
        plan = getFragmentPlan(sql);
        assertContains(plan, "  5:AGGREGATE (update serialize)\n" +
                "  |  output: count(if(3: t1c IS NULL, NULL, 11: upper))\n" +
                "  |  group by: \n" +
                "  |  \n" +
                "  4:AGGREGATE (merge serialize)\n" +
                "  |  group by: 3: t1c, 11: upper");
        connectContext.getSessionVariable().setNewPlanerAggStage(0);

    }

    @Test
    public void testSimpleAggRewrite() throws Exception {
        connectContext.getSessionVariable().setEnableRewriteSimpleAggToMetaScan(true);
        // normal case
        String sql = "select min(t1b),max(t1b),min(id_datetime),count(t1b),count(t1c) from test_all_type_not_null";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "  1:AGGREGATE (update serialize)\n" +
                "  |  output: min(min_t1b), max(max_t1b), min(min_id_datetime), sum(count_t1b), sum(count_t1b)\n" +
                "  |  group by: \n" +
                "  |  \n" +
                "  0:MetaScan\n" +
                "     Table: test_all_type_not_null\n" +
                "     <id 16> : min_t1b\n" +
                "     <id 17> : max_t1b\n" +
                "     <id 18> : min_id_datetime\n" +
                "     <id 19> : count_t1b");

        // The following cases will not use MetaScan because some conditions are not met
        // with group by key
        sql = "select t1b,max(id_datetime) from test_all_type_not_null group by t1b";
        plan = getFragmentPlan(sql);
        assertContains(plan, "  1:AGGREGATE (update finalize)\n" +
                "  |  output: max(8: id_datetime)\n" +
                "  |  group by: 2: t1b\n" +
                "  |  \n" +
                "  0:OlapScanNode\n" +
                "     TABLE: test_all_type_not_null");
        // with expr in agg function
        sql = "select min(t1b+1),max(t1b) from test_all_type_not_null";
        plan = getFragmentPlan(sql);
        assertContains(plan, "  2:AGGREGATE (update finalize)\n" +
                "  |  output: min(CAST(2: t1b AS INT) + 1), max(2: t1b)\n" +
                "  |  group by: \n" +
                "  |  \n" +
                "  1:Project\n" +
                "  |  <slot 2> : 2: t1b\n" +
                "  |  \n" +
                "  0:OlapScanNode");
        // with unsupported type in agg function
        sql = "select min(t1b),max(t1a) from test_all_type_not_null";
        plan = getFragmentPlan(sql);
        assertContains(plan, "  1:AGGREGATE (update finalize)\n" +
                "  |  output: min(2: t1b), max(1: t1a)\n" +
                "  |  group by: \n" +
                "  |  \n" +
                "  0:OlapScanNode");
        // with filter
        sql = "select min(t1b) from test_all_type_not_null where t1c > 10";
        plan = getFragmentPlan(sql);
        assertContains(plan, "  2:AGGREGATE (update finalize)\n" +
                "  |  output: min(2: t1b)\n" +
                "  |  group by: \n" +
                "  |  \n" +
                "  1:Project\n" +
                "  |  <slot 2> : 2: t1b\n" +
                "  |  \n" +
                "  0:OlapScanNode");

        sql = "select min(t1b) from test_all_type_not_null having abs(1) = 2";
        plan = getFragmentPlan(sql);
        assertContains(plan, "  1:AGGREGATE (update finalize)\n" +
                "  |  output: min(2: t1b)\n" +
                "  |  group by: \n" +
                "  |  having: abs(1) = 2\n" +
                "  |  \n" +
                "  0:OlapScanNode");
        // with nullable column
        sql = "select min(t1b) from test_all_type";
        plan = getFragmentPlan(sql);
        assertContains(plan, "  1:AGGREGATE (update finalize)\n" +
                "  |  output: min(2: t1b)\n" +
                "  |  group by: \n" +
                "  |  \n" +
                "  0:OlapScanNode");
        sql = "select count(t1b) from test_all_type";
        plan = getFragmentPlan(sql);
        assertContains(plan, "  1:AGGREGATE (update finalize)\n" +
                "  |  output: count(2: t1b)\n" +
                "  |  group by: \n" +
                "  |  \n" +
                "  0:OlapScanNode");
        // with count distinct, shouldn't apply RewriteSimpleAggToMetaScanRule
        sql = "select count(distinct t1b) from test_all_type_not_null";
        plan = getFragmentPlan(sql);
        assertContains(plan, "  1:AGGREGATE (update finalize)\n" +
                "  |  output: multi_distinct_count(2: t1b)\n" +
                "  |  group by: \n" +
                "  |  \n" +
                "  0:OlapScanNode");
    }

    @Test
    public void testBitmapUnionAggRewrite() throws Exception {
        connectContext.getSessionVariable().setEnableRewriteSimpleAggToMetaScan(true);
        // normal case
        String sql = "select " +
                "bitmap_union(to_bitmap(t1a))," + // varchar
                "bitmap_union(to_bitmap(t1b))," + // smallint
                "bitmap_union(to_bitmap(t1c))," + // int
                "bitmap_union(to_bitmap(t1d))," + // bigint
                "bitmap_union(to_bitmap(t1e))," + // float
                "bitmap_union(to_bitmap(t1f))," + //double
                "bitmap_union(to_bitmap(t1g))," + // bigint
                "bitmap_union(to_bitmap(id_datetime))," + // datetime
                "bitmap_union(to_bitmap(id_date))," + // date
                "bitmap_union(to_bitmap(id_decimal))" + //decimal
                "from test_all_type_not_null";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "2:AGGREGATE (update finalize)\n" +
                "  |  output: bitmap_union(to_bitmap(1: t1a)), " +
                "bitmap_agg(2: t1b), " +
                "bitmap_agg(3: t1c), " +
                "bitmap_agg(4: t1d), " +
                "bitmap_union(to_bitmap(CAST(5: t1e AS VARCHAR))), " +
                "bitmap_union(to_bitmap(CAST(6: t1f AS VARCHAR))), " +
                "bitmap_agg(7: t1g), " +
                "bitmap_union(to_bitmap(CAST(8: id_datetime AS VARCHAR))), " +
                "bitmap_union(to_bitmap(CAST(9: id_date AS VARCHAR))), " +
                "bitmap_union(to_bitmap(CAST(10: id_decimal AS VARCHAR)))\n" +
                "  |  group by: ");

        sql = "select bitmap_count(bitmap_union(to_bitmap(if(v1 = 1, v2, -999)))) as c1, \n" +
                "bitmap_count(bitmap_union(to_bitmap(if(v1 = 1, v3, -999)))) as c2,\n" +
                "bitmap_count(bitmap_union(to_bitmap(if(v1 = 1, v2, -999)))) - " +
                "bitmap_count(bitmap_union(to_bitmap(if(v1 = 1, v3, -999))))\n" +
                "from t0;";
        plan = getFragmentPlan(sql);
        assertContains(plan, "<slot 8> : 11: bitmap_count\n" +
                "  |  <slot 9> : 12: bitmap_count\n" +
                "  |  <slot 10> : 11: bitmap_count - 12: bitmap_count");
    }

    @Test
    public void testCountDistinctImplementation1() throws Exception {
        // normal case
        String sql = "select " +
                "count(distinct t1a)," + // varchar
                "count(distinct t1b)," + // smallint
                "count(distinct t1c)," + // int
                "count(distinct t1d)," + // bigint
                "count(distinct t1e)," + // float
                "count(distinct t1f)," + //double
                "count(distinct t1g)," + // bigint
                "count(distinct id_datetime)," + // datetime
                "count(distinct id_date)," + // date
                "count(distinct id_decimal)" + //decimal
                "from test_all_type_not_null";
        connectContext.getSessionVariable().setCountDistinctImplementation("ndv");
        String plan = getFragmentPlan(sql);
        assertContains(plan, "  |  output: ndv(7: t1g), ndv(8: id_datetime), " +
                "ndv(9: id_date), ndv(10: id_decimal), ndv(1: t1a), ndv(2: t1b), " +
                "ndv(3: t1c), ndv(4: t1d), ndv(5: t1e), ndv(6: t1f)\n" +
                "  |  group by: ");
        sql = "select count(distinct if(v1 = 1, v2, -999)) as c1, \n" +
                "count(distinct if(v1 = 1, v3, -999)) as c2,\n" +
                "count(distinct if(v1 = 1, v2, -999)) - " +
                "count(distinct if(v1 = 1, v3, -999))\n" +
                "from t0;";
        plan = getFragmentPlan(sql);
        assertContains(plan, " |  output: ndv(if(9: expr, 2: v2, -999)), " +
                "ndv(if(9: expr, 3: v3, -999))\n" +
                "  |  group by: ");

        sql = "select count(distinct v1, v2) from t0;";
        plan = getFragmentPlan(sql);
        assertContains(plan, "  1:AGGREGATE (update serialize)\n" +
                        "  |  STREAMING\n" +
                        "  |  group by: 1: v1, 2: v2");
        connectContext.getSessionVariable().setCountDistinctImplementation("default");
    }

    @Test
    public void testCountDistinctImplementation2() throws Exception {
        // normal case
        String sql = "select " +
                "count(distinct t1a)," + // varchar
                "count(distinct t1b)," + // smallint
                "count(distinct t1c)," + // int
                "count(distinct t1d)," + // bigint
                "count(distinct t1e)," + // float
                "count(distinct t1f)," + //double
                "count(distinct t1g)," + // bigint
                "count(distinct id_datetime)," + // datetime
                "count(distinct id_date)," + // date
                "count(distinct id_decimal)" + //decimal
                "from test_all_type_not_null";
        connectContext.getSessionVariable().setCountDistinctImplementation("multi_count_distinct");
        String plan = getFragmentPlan(sql);
        assertContains(plan, "  |  output: multi_distinct_count(7: t1g), multi_distinct_count(8: id_datetime), " +
                "multi_distinct_count(9: id_date), multi_distinct_count(10: id_decimal), " +
                "multi_distinct_count(1: t1a), multi_distinct_count(2: t1b), " +
                "multi_distinct_count(3: t1c), multi_distinct_count(4: t1d), " +
                "multi_distinct_count(5: t1e), multi_distinct_count(6: t1f)\n" +
                "  |  group by: ");
        sql = "select count(distinct if(v1 = 1, v2, -999)) as c1, \n" +
                "count(distinct if(v1 = 1, v3, -999)) as c2,\n" +
                "count(distinct if(v1 = 1, v2, -999)) - " +
                "count(distinct if(v1 = 1, v3, -999))\n" +
                "from t0;";
        plan = getFragmentPlan(sql);
        assertContains(plan, "  2:AGGREGATE (update finalize)\n" +
                "  |  output: multi_distinct_count(if(9: expr, 2: v2, -999)), " +
                "multi_distinct_count(if(9: expr, 3: v3, -999))\n" +
                "  |  group by: ");
        sql = "select count(distinct v1, v2) from t0;";
        plan = getFragmentPlan(sql);
        assertContains(plan, "  1:AGGREGATE (update serialize)\n" +
                "  |  STREAMING\n" +
                "  |  group by: 1: v1, 2: v2");
        connectContext.getSessionVariable().setCountDistinctImplementation("default");
    }

    @Test
    public void testGroupByLiteral() throws Exception {
        String sql = "select -9223372036854775808 group by TRUE;";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "  1:Project\n" +
                "  |  <slot 3> : -9223372036854775808");
    }

    @Test
    public void testRewriteSumByAssociativeRule() throws Exception {
        // 1. different types
        String sql = "select sum(t1b+1),sum(t1c+1),sum(t1d+1),sum(t1e+1),sum(t1f+1),sum(t1g+1),sum(id_decimal+1)" +
                " from test_all_type";
        String plan = getVerboseExplain(sql);
        // for each sum(col + 1), should rewrite to sum(col) + count(col) * 1
        assertContains(plan, "  3:Project\n" +
                "  |  output columns:\n" +
                "  |  18 <-> [25: sum, BIGINT, true] + [26: count, BIGINT, true] * 1\n" +
                "  |  19 <-> [27: sum, BIGINT, true] + [28: count, BIGINT, true] * 1\n" +
                "  |  20 <-> [29: sum, BIGINT, true] + [30: count, BIGINT, true] * 1\n" +
                "  |  21 <-> [32: sum, DOUBLE, true] + cast([33: count, BIGINT, true] as DOUBLE) * 1.0\n" +
                "  |  22 <-> [34: sum, DOUBLE, true] + cast([35: count, BIGINT, true] as DOUBLE) * 1.0\n" +
                "  |  23 <-> [36: sum, BIGINT, true] + [37: count, BIGINT, true] * 1\n" +
                "  |  24 <-> [38: sum, DECIMAL128(38,2), true] + cast([39: count, BIGINT, true] as DECIMAL128(18,0)) * 1");
        // if a column can cast to target type safely, we can remove the implicit cast directly.
        // in this case, t1b is SMALLINT,and need to be cast to INT implicitly before calculate sum,
        // after we rewrite sum(add(cast t1b as int), 1),
        // there must be no project node between aggregate node and olap scan node
        sql = "select sum(t1b+1) from test_all_type";
        plan = getFragmentPlan(sql);
        assertContains(plan, "  2:Project\n" +
                "  |  <slot 12> : 13: sum + 14: count * 1\n" +
                "  |  \n" +
                "  1:AGGREGATE (update finalize)\n" +
                "  |  output: sum(2: t1b), count(2: t1b)\n" +
                "  |  group by: \n" +
                "  |  \n" +
                "  0:OlapScanNode");
        // apply this rule more than once
        // 1. sum(add(add(cast t1b as int, 1), 1)) => sum(add(cast t1b as int, 1)) + count(add(cast t1b as int, 1) * 1
        // 2. sum(add(cast t1b as int,1)) => sum(t1b) + count(t1b) * 1
        // so the final result is sum(t1b) + count(t1b) * 1 + count(add(cast t1b as int. 1)) * 1
        sql = "select sum(t1b+1+1) from test_all_type";
        plan = getFragmentPlan(sql);
        assertContains(plan, "  3:Project\n" +
                "  |  <slot 12> : 16: sum + 17: count * 1 + 15: count * 1\n" +
                "  |  \n" +
                "  2:AGGREGATE (update finalize)\n" +
                "  |  output: count(2: t1b), count(CAST(2: t1b AS INT) + 1), sum(2: t1b)\n" +
                "  |  group by: \n" +
                "  |  \n" +
                "  1:Project\n" +
                "  |  <slot 2> : 2: t1b\n" +
                "  |  \n" +
                "  0:OlapScanNode");

        // should make sure the argument of count appears in project node
        sql = "select sum(id_decimal + 1 + 2) from test_all_type";
        plan = getFragmentPlan(sql);
        assertContains(plan, "  3:Project\n" +
                "  |  <slot 12> : 16: sum + CAST(17: count AS DECIMAL128(18,0)) * 1 + CAST(15: count AS DECIMAL128(18,0)) * 2\n" +
                "  |  \n" +
                "  2:AGGREGATE (update finalize)\n" +
                "  |  output: count(10: id_decimal), count(CAST(10: id_decimal AS DECIMAL64(12,2)) + 1), sum(10: id_decimal)\n" +
                "  |  group by: \n" +
                "  |  \n" +
                "  1:Project\n" +
                "  |  <slot 10> : 10: id_decimal\n" +
                "  |  \n" +
                "  0:OlapScanNode");

        // 2. aggregate result reuse
        sql = "select sum(t1b), sum(t1b+1), sum(t1b+2) from test_all_type";
        // if a column appears multiple times in different sum functions,
        // we can reuse the results of sum and count
        plan = getVerboseExplain(sql);
        assertContains(plan, "  2:Project\n" +
                "  |  output columns:\n" +
                "  |  13 <-> [18: sum, BIGINT, true]\n" +
                "  |  14 <-> [18: sum, BIGINT, true] + [17: count, BIGINT, true] * 1\n" +
                "  |  15 <-> [18: sum, BIGINT, true] + [17: count, BIGINT, true] * 2");

        sql = "select sum(id_decimal), sum(id_decimal+1.0), sum(id_decimal+1.00), sum(id_decimal+1.000), " +
                "sum(id_decimal+1.000000000000000000) from test_all_type";
        plan = getVerboseExplain(sql);
        // for decimal sum with different scales,
        // the original ADD operator need to cast id_decimal to decimal128 with different scales,
        // e.g.
        // sum(id_decimal+1.0) -> sum(add(cast(cast(id_decimal as decimal128(28,9) as decimal128(37,9)))), 1.0)
        // sum(id_decimal+1.00) -> sum(add(cast(cast(id_decimal as decimal128(28,9) as decimal128(36,9)))), 1.00)
        // after applying RewriteSumByAssociativeRule, we can remove all unnecessary cast
        // and reuse the result of sum(id_decimal) and count() multiple times.
        assertContains(plan, "  2:Project\n" +
                "  |  output columns:\n" +
                "  |  15 <-> [20: sum, DECIMAL128(38,2), true]\n" +
                "  |  16 <-> [20: sum, DECIMAL128(38,2), true] + [28: cast, DECIMAL128(18,0), true] * 1.0\n" +
                "  |  17 <-> [20: sum, DECIMAL128(38,2), true] + [28: cast, DECIMAL128(18,0), true] * 1.00\n" +
                "  |  18 <-> [20: sum, DECIMAL128(38,2), true] + [28: cast, DECIMAL128(18,0), true] * 1.000\n" +
                "  |  19 <-> [20: sum, DECIMAL128(38,2), true] + [28: cast, DECIMAL128(18,0), true] * 1.000000000000000000\n" +
                "  |  common expressions:\n" +
                "  |  28 <-> cast([21: count, BIGINT, true] as DECIMAL128(18,0))");

        // 3. mix sum and other agg functions
        sql = "select avg(t1b), max(t1b), sum(t1b), sum(t1b+1) from test_all_type";
        plan = getVerboseExplain(sql);
        assertContains(plan, "  2:Project\n" +
                "  |  output columns:\n" +
                "  |  12 <-> [12: avg, DOUBLE, true]\n" +
                "  |  13 <-> [13: max, SMALLINT, true]\n" +
                "  |  14 <-> [14: sum, BIGINT, true]\n" +
                "  |  15 <-> [14: sum, BIGINT, true] + [17: count, BIGINT, true] * 1");

        // 4. with group by key
        // if the number of agg function can be reduced after applying this rule, do it
        sql = "select t1c, sum(t1b),sum(t1b+1),sum(t1b+2) from test_all_type group by t1c";
        plan = getVerboseExplain(sql);
        assertContains(plan, "  2:Project\n" +
                "  |  output columns:\n" +
                "  |  3 <-> [3: t1c, INT, true]\n" +
                "  |  13 <-> [18: sum, BIGINT, true]\n" +
                "  |  14 <-> [18: sum, BIGINT, true] + [17: count, BIGINT, true] * 1\n" +
                "  |  15 <-> [18: sum, BIGINT, true] + [17: count, BIGINT, true] * 2");
        assertContains(plan, "  |  group by: [3: t1c, INT, true]");
        // if the number of agg function cannot be reduced after applying this rule, skip it
        sql = "select t1c, sum(t1b+1),avg(t1b) from test_all_type group by t1c";
        plan = getVerboseExplain(sql);
        assertContains(plan, "  1:Project\n" +
                "  |  output columns:\n" +
                "  |  2 <-> [2: t1b, SMALLINT, true]\n" +
                "  |  3 <-> [3: t1c, INT, true]\n" +
                "  |  11 <-> cast([2: t1b, SMALLINT, true] as INT) + 1");

        // 4.2 with group by key and having
        sql = "select t1c, sum(t1b)+2,sum(t1b+1),sum(t1b+2)+1 from test_all_type group by t1c having sum(t1b+1) > 10";
        plan = getVerboseExplain(sql);
        assertContains(plan, "  2:Project\n" +
                "  |  output columns:\n" +
                "  |  3 <-> [3: t1c, INT, true]\n" +
                "  |  14 <-> [18: sum, BIGINT, true] + [19: count, BIGINT, true] * 1\n" +
                "  |  16 <-> [18: sum, BIGINT, true] + 2\n" +
                "  |  17 <-> [18: sum, BIGINT, true] + [19: count, BIGINT, true] * 2 + 1");
        assertContains(plan, "  |  group by: [3: t1c, INT, true]\n" +
                "  |  having: [18: sum, BIGINT, true] + [19: count, BIGINT, true] * 1 > 10");
        // 5. agg after join
        sql = "select sum(t2.t1b+1) from test_all_type as t1 join test_all_type_not_null as t2 " +
                "on t1.t1c = t2.t1c";
        plan = getVerboseExplain(sql);
        assertContains(plan, "  5:AGGREGATE (update serialize)\n" +
                "  |  aggregate: sum[([12: t1b, SMALLINT, false]); " +
                "args: SMALLINT; result: BIGINT; args nullable: false; result nullable: true], " +
                "count[([12: t1b, SMALLINT, false]); " +
                "args: SMALLINT; result: BIGINT; args nullable: false; result nullable: true]");
        sql = "select sum(t2.t1b+1) from test_all_type as t1 left outer join test_all_type_not_null as t2 " +
                "on t1.t1c = t2.t1c";
        plan = getVerboseExplain(sql);
        assertContains(plan, "  6:AGGREGATE (update serialize)\n" +
                "  |  aggregate: sum[([12: t1b, SMALLINT, true]); " +
                "args: SMALLINT; result: BIGINT; args nullable: true; result nullable: true], " +
                "count[([12: t1b, SMALLINT, true]); " +
                "args: SMALLINT; result: BIGINT; args nullable: true; result nullable: true]\n" +
                "  |  hasNullableGenerateChild: true");

        sql = "select sum(cast(t1b as int) + cast('1.1' as int)) from test_all_type";
        plan = getVerboseExplain(sql);
        assertContains(plan, "  2:AGGREGATE (update finalize)\n" +
                "  |  aggregate: sum[(cast([2: t1b, SMALLINT, true] as BIGINT) + cast(cast('1.1' as INT) as BIGINT)); " +
                "args: BIGINT; result: BIGINT; args nullable: true; result nullable: true]");
    }

    @Test
    public void testPruneGroupByKeysRule() throws Exception {
        String sql = "select t1b,t1b+1,count(*) from test_all_type group by 1,2";
        String plan = getFragmentPlan(sql);
        // t1b+1 will be pruned
        assertContains(plan, "  2:Project\n" +
                "  |  <slot 2> : 2: t1b\n" +
                "  |  <slot 11> : CAST(2: t1b AS INT) + 1\n" +
                "  |  <slot 12> : 12: count\n" +
                "  |  \n" +
                "  1:AGGREGATE (update finalize)\n" +
                "  |  output: count(*)\n" +
                "  |  group by: 2: t1b");

        sql = "select t1b,t1b+1,count(*) from test_all_type group by 2,1";
        plan = getFragmentPlan(sql);
        // both keys will be reserved because expr occurs in the first place
        assertContains(plan, "  2:AGGREGATE (update finalize)\n" +
                "  |  output: count(*)\n" +
                "  |  group by: 11: expr, 2: t1b\n" +
                "  |  \n" +
                "  1:Project\n" +
                "  |  <slot 2> : 2: t1b\n" +
                "  |  <slot 11> : CAST(2: t1b AS INT) + 1");

        // only the keys after original column will be pruned
        sql = "select t1b+1,t1b,t1b+2,count(*) from test_all_type group by 1,2,3";
        plan = getFragmentPlan(sql);
        assertContains(plan, "  3:Project\n" +
                "  |  <slot 2> : 2: t1b\n" +
                "  |  <slot 11> : 11: expr\n" +
                "  |  <slot 12> : CAST(2: t1b AS INT) + 2\n" +
                "  |  <slot 13> : 13: count\n" +
                "  |  \n" +
                "  2:AGGREGATE (update finalize)\n" +
                "  |  output: count(*)\n" +
                "  |  group by: 11: expr, 2: t1b\n" +
                "  |  \n" +
                "  1:Project\n" +
                "  |  <slot 2> : 2: t1b\n" +
                "  |  <slot 11> : CAST(2: t1b AS INT) + 1");

        sql = "select t1b,t1c,t1b+1,t1c+1,count(*) from test_all_type group by 1,2,3,4";
        plan = getFragmentPlan(sql);
        // t1b+1, t1c+1 will be pruned
        assertContains(plan, "  2:Project\n" +
                "  |  <slot 2> : 2: t1b\n" +
                "  |  <slot 3> : 3: t1c\n" +
                "  |  <slot 11> : CAST(2: t1b AS INT) + 1\n" +
                "  |  <slot 12> : CAST(3: t1c AS BIGINT) + 1\n" +
                "  |  <slot 13> : 13: count\n" +
                "  |  \n" +
                "  1:AGGREGATE (update finalize)\n" +
                "  |  output: count(*)\n" +
                "  |  group by: 2: t1b, 3: t1c");
        // the first group by key is not simple column ref, can't be pruned
        sql = "select 1 from test_all_type group by t1b+rand(), t1b+rand()+1";
        plan = getFragmentPlan(sql);
        assertContains(plan, "  3:Project\n" +
                "  |  <slot 13> : 1\n" +
                "  |  \n" +
                "  2:AGGREGATE (update finalize)\n" +
                "  |  group by: 11: expr, 12: expr\n" +
                "  |  \n" +
                "  1:Project\n" +
                "  |  <slot 11> : 14: cast + rand()\n" +
                "  |  <slot 12> : 14: cast + rand() + 1.0\n" +
                "  |  common expressions:\n" +
                "  |  <slot 14> : CAST(2: t1b AS DOUBLE)");
        sql =
                "select cast(id_decimal as decimal(38,2)),cast(id_decimal as decimal(37,2)) from test_all_type group by 1, 2";
        plan = getFragmentPlan(sql);
        assertContains(plan, "  2:AGGREGATE (update finalize)\n" +
                "  |  group by: 11: cast, 12: cast\n" +
                "  |  \n" +
                "  1:Project\n" +
                "  |  <slot 11> : CAST(10: id_decimal AS DECIMAL128(38,2))\n" +
                "  |  <slot 12> : CAST(10: id_decimal AS DECIMAL128(37,2))");
        // complex projections, aggregations and group by keys
        sql = "select v4,abs(v4),cast(v5 as largeint),max(v4+v5+v6) from t1 group by v4,abs(v4),cast(v5 as largeint);";
        plan = getFragmentPlan(sql);
        assertContains(plan, "  3:Project\n" +
                "  |  <slot 1> : 1: v4\n" +
                "  |  <slot 4> : abs(1: v4)\n" +
                "  |  <slot 5> : 5: cast\n" +
                "  |  <slot 7> : 7: max\n" +
                "  |  \n" +
                "  2:AGGREGATE (update finalize)\n" +
                "  |  output: max(6: expr)\n" +
                "  |  group by: 1: v4, 5: cast\n" +
                "  |  \n" +
                "  1:Project\n" +
                "  |  <slot 1> : 1: v4\n" +
                "  |  <slot 5> : CAST(2: v5 AS LARGEINT)\n" +
                "  |  <slot 6> : 1: v4 + 2: v5 + 3: v6");
        // if all group by keys are constant and the query has aggregations
        // we should reserve one key to ensure the correct result
        sql = "select 'a',count(t1b) from test_all_type where t1c>10 group by 'c'";
        plan = getFragmentPlan(sql);
        assertContains(plan, "  2:AGGREGATE (update finalize)\n" +
                "  |  output: count(2: t1b)\n" +
                "  |  group by: 11: expr\n" +
                "  |  \n" +
                "  1:Project\n" +
                "  |  <slot 2> : 2: t1b\n" +
                "  |  <slot 11> : 'c'");
        sql = "select 'a','b',count(t1b) from test_all_type where t1c>10 group by 'c','d'";
        plan = getFragmentPlan(sql);
        assertContains(plan, "  2:AGGREGATE (update finalize)\n" +
                "  |  output: count(2: t1b)\n" +
                "  |  group by: 11: expr\n" +
                "  |  \n" +
                "  1:Project\n" +
                "  |  <slot 2> : 2: t1b\n" +
                "  |  <slot 11> : 'c'");
        // if all group by keys and projections are constant, we can remove the agg node and add a limit operator.
        sql = "select 'a','b' from test_all_type group by 'c','d'";
        plan = getFragmentPlan(sql);
        assertContains(plan, "  1:Project\n" +
                "  |  <slot 13> : 'a'\n" +
                "  |  <slot 14> : 'b'");
        assertContains(plan, "  2:EXCHANGE\n" +
                "     limit: 1");
    }

    @Test
    public void testPruneGroupByKeysRule2() throws Exception {
        String sql = "select 1 from test_all_type group by NULL " +
                "having (NOT (((DROUND(0.09733420538671422) ) IS NOT NULL)))";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "3:Project\n" +
                "  |  <slot 12> : 1\n" +
                "  |  \n" +
                "  2:AGGREGATE (update finalize)\n" +
                "  |  group by: 11: expr\n" +
                "  |  having: dround(0.09733420538671422) IS NULL\n" +
                "  |  \n" +
                "  1:Project\n" +
                "  |  <slot 11> : NULL");
    }

    @Test
    public void testPruneGroupByKeysRule3() throws Exception {
        String sql = "select count(*), sum(t1b) from test_all_type group by NULL " +
                "having (NOT (((DROUND(0.09733420538671422) ) IS NOT NULL)))";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "3:Project\n" +
                "  |  <slot 12> : 12: count\n" +
                "  |  <slot 13> : 13: sum\n" +
                "  |  \n" +
                "  2:AGGREGATE (update finalize)\n" +
                "  |  output: count(*), sum(2: t1b)\n" +
                "  |  group by: 11: expr\n" +
                "  |  having: dround(0.09733420538671422) IS NULL");
    }

    @Test
    public void testPruneGroupByKeysRule4() throws Exception {
        String sql = "select v1, rand_col, v2, v3, count(1) from (select v1, UUID() v2," +
                " case when v1 in (1, 6) then round(rand() * 100) else 1 end as rand_col," +
                " v1 > v1 + 1 or v1 > rand() v3 from t0) a group by 1, 2, 3, 4;";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "2:AGGREGATE (update finalize)\n" +
                "  |  output: count(1)\n" +
                "  |  group by: 1: v1, 5: case, 4: uuid, 6: expr");
    }

    @Test
    public void testDistinctRewrite() throws Exception {
        FeConstants.runningUnitTest = true;
        String sql = "select count(distinct t1a), sum(t1c) from test_all_type group by t1b";
        String plan = getVerboseExplain(sql);
        assertContains(plan, "sum[([13: sum, BIGINT, true]); args: BIGINT; result: BIGINT; args nullable: true;");

        sql = "select multi_distinct_count(t1a), max(t1c) from test_all_type group by t1b, t1c";
        plan = getVerboseExplain(sql);
        assertContains(plan, "12 <-> [3: t1c, INT, true]");

        sql = "select sum(distinct v1), hll_union(hll_hash(v3)) from test_object group by v2";
        plan = getVerboseExplain(sql);
        assertContains(plan, "hll_union[([16: hll_union, HLL, true]); args: HLL; result: HLL; args nullable: true;");

        sql = "select count(distinct v1), BITMAP_UNION(b1) from test_object group by v2, v3";
        plan = getVerboseExplain(sql);
        assertContains(plan, "bitmap_union[([15: bitmap_union, BITMAP, true]); args: BITMAP; result: BITMAP; " +
                "args nullable: true;");

        sql = "select count(distinct t1a), PERCENTILE_UNION(PERCENTILE_HASH(t1f)) from test_all_type group by t1c";
        plan = getVerboseExplain(sql);
        assertContains(plan, "percentile_union[([14: percentile_union, PERCENTILE, true]); args: PERCENTILE; " +
                "result: PERCENTILE; args nullable: true;");
        FeConstants.runningUnitTest = false;
    }

    @Test
    public void testMultiCountDistinctWithMoreGroupBy() throws Exception {
        FeConstants.runningUnitTest = true;
        String sql = "select count(distinct t1c), count(distinct t1d), count(distinct t1e)" +
                "from test_all_type group by t1a, t1b";

        String plan = getFragmentPlan(sql);
        assertContains(plan, "multi_distinct_count");

        sql = "select count(distinct t1c), count(distinct t1d), count(distinct t1e)" +
                "from test_all_type group by t1a";

        plan = getFragmentPlan(sql);
        assertNotContains(plan, "multi_distinct_count");
        FeConstants.runningUnitTest = false;
    }

    @Test
    public void testRemoveExchange() throws Exception {
        int oldValue = connectContext.getSessionVariable().getNewPlannerAggStage();
        connectContext.getSessionVariable().setNewPlanerAggStage(1);
        String sql = "select sum(v1) from t0";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "RESULT SINK\n" +
                "\n" +
                "  1:AGGREGATE (update finalize)");

        sql = "select sum(v1 + v2) from t0 group by v3";
        plan = getFragmentPlan(sql);
        assertContains(plan, "RESULT SINK\n" +
                "\n" +
                "  3:Project\n" +
                "  |  <slot 5> : 5: sum\n" +
                "  |  \n" +
                "  2:AGGREGATE (update finalize)\n" +
                "  |  output: sum(4: expr)");
        connectContext.getSessionVariable().setNewPlanerAggStage(oldValue);
    }

    @Test
    public void testDistinctConst() throws Exception {
        FeConstants.runningUnitTest = true;
        String sql = "SELECT DISTINCT 16 col0 FROM t0 AS cor0 LEFT JOIN t1 AS cor1 ON NULL IS NULL";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "6:Project\n" +
                "  |  <slot 7> : 16\n" +
                "  |  limit: 1");
        sql = "SELECT DISTINCT 61 AS col0 FROM t0 AS cor0 LEFT JOIN t1 AS cor1 ON NOT NULL IS NOT NULL, t0 AS cor2;";
        plan = getFragmentPlan(sql);
        assertContains(plan, "11:Project\n" +
                "  |  <slot 10> : 61\n" +
                "  |  limit: 1");
    }

    @Test
    public void testPercentileFunctionConst() throws Exception {
        // For compatibility
        String sql = "select percentile_approx(1, cast(0.4 as DOUBLE));";
        String plan = getCostExplain(sql);
        assertContains(plan, "percentile_approx[(1.0, 0.4); args: DOUBLE,DOUBLE");

        sql = "with cc as (select 1 as a) select percentile_approx(1, cc.a) from cc;";
        plan = getFragmentPlan(sql);
        assertContains(plan, "2:AGGREGATE (update finalize)\n" +
                "  |  output: percentile_approx(1.0, 1.0)\n" +
                "  |  group by: ");
        Exception exception = Assertions.assertThrows(StarRocksPlannerException.class, () -> {
            String testSql = "with cc as (select 1 as a, v1 from t0) select percentile_approx(1, cc.a, cc.v1) from cc;";
            getFragmentPlan(testSql);
        });
        Assertions.assertTrue(exception.getMessage().contains("the third parameter's type is numeric constant type"));

        Throwable exception2 = assertThrows(SemanticException.class, () -> {
            getCostExplain("select percentile_approx(1, cast(1.3 as DOUBLE));");
        });
        assertThat(exception2.getMessage(), containsString("Getting analyzing error. " +
                "Detail message: percentile_approx second parameter'value must be between 0 and 1."));

        // should success
        getCostExplain("select percentile_cont(1, cast(0.4 as DOUBLE));");
        getCostExplain("select PERCENTILE_DISC(1, cast(0.4 as DOUBLE));");
    }

    @Test
    public void testSplitTheTopGlobalAgg() throws Exception {
        String sql = "select count(distinct v2), count(v3) from t0 join t1 group by v3";
        String plan = getFragmentPlan(sql);
        assertCContains(plan, "9:AGGREGATE (update finalize)\n" +
                "  |  output: count(2: v2), count(8: count)\n" +
                "  |  group by: 3: v3\n" +
                "  |  \n" +
                "  8:AGGREGATE (merge serialize)\n" +
                "  |  output: count(8: count)\n" +
                "  |  group by: 2: v2, 3: v3");

        // no need to split the top global agg for the below cases
        sql = "select count(distinct v2), count(v3) from t0 join t1";
        plan = getFragmentPlan(sql);
        assertCContains(plan, "STREAM DATA SINK\n" +
                        "    EXCHANGE ID: 07\n" +
                        "    HASH_PARTITIONED: 2: v2",
                "9:AGGREGATE (update serialize)\n" +
                        "  |  output: count(2: v2), count(8: count)\n" +
                        "  |  group by: ");

        sql = "select count(distinct v2), count(v3) from t0 join t1 group by 'a'";
        plan = getFragmentPlan(sql);
        assertCContains(plan, "STREAM DATA SINK\n" +
                "    EXCHANGE ID: 10\n" +
                "    HASH_PARTITIONED: 7: expr\n" +
                "\n" +
                "  9:AGGREGATE (update serialize)\n" +
                "  |  STREAMING\n" +
                "  |  output: count(2: v2), count(9: count)\n" +
                "  |  group by: 7: expr");
    }

    @Test
    public void testStreamingAgg() throws Exception {
        String sql = "select /*+ SET_VAR (streaming_preaggregation_mode = 'force_streaming') */ " +
                "count(*) from t0 join t1 group by v3";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "6:AGGREGATE (update serialize)\n" +
                "  |  STREAMING\n" +
                "  |  output: count(*)\n" +
                "  |  group by: 3: v3");

        sql = "select /*+ SET_VAR (streaming_preaggregation_mode = 'force_streaming') */ " +
                "count(distinct v2), sum(v4) from t0 join t1 group by v3";
        plan = getFragmentPlan(sql);
        assertCContains(plan, "4:AGGREGATE (update serialize)\n" +
                        "  |  STREAMING\n" +
                        "  |  output: sum(4: v4)\n" +
                        "  |  group by: 2: v2, 3: v3",
                "7:AGGREGATE (update finalize)\n" +
                        "  |  output: count(2: v2), sum(8: sum)\n" +
                        "  |  group by: 3: v3");

        sql = "select /*+ SET_VAR (streaming_preaggregation_mode = 'force_streaming') */ " +
                "count(distinct v2), array_length(array_agg(v1)) from t0 join t1 group by 'a'";
        plan = getFragmentPlan(sql);
        assertCContains(plan, "6:AGGREGATE (update serialize)\n" +
                        "  |  STREAMING\n" +
                        "  |  output: array_agg(1: v1)\n" +
                        "  |  group by: 2: v2",
                "8:AGGREGATE (merge serialize)\n" +
                        "  |  output: array_agg(9: array_agg)\n" +
                        "  |  group by: 2: v2");

        sql = "select /*+ SET_VAR (streaming_preaggregation_mode = 'force_streaming') */ " +
                "count(distinct v2), array_length(array_agg(v1)) from t0 join t1 group by v4";
        plan = getFragmentPlan(sql);
        assertCContains(plan, "7:AGGREGATE (update finalize)\n" +
                "  |  output: count(2: v2), array_agg(8: array_agg)\n" +
                "  |  group by: 4: v4\n" +
                "  |  \n" +
                "  6:AGGREGATE (merge serialize)\n" +
                "  |  output: array_agg(8: array_agg)\n" +
                "  |  group by: 2: v2, 4: v4");

        sql = "select /*+ SET_VAR (new_planner_agg_stage = 4) */ " +
                "count(distinct v2), array_length(array_agg(v1)) from t0 join t1 group by v4";
        plan = getFragmentPlan(sql);
        assertCContains(plan, "STREAM DATA SINK\n" +
                        "    EXCHANGE ID: 08\n" +
                        "    HASH_PARTITIONED: 4: v4\n" +
                        "\n" +
                        "  7:AGGREGATE (update serialize)\n" +
                        "  |  STREAMING\n" +
                        "  |  output: count(2: v2), array_agg(8: array_agg)\n" +
                        "  |  group by: 4: v4",
                "9:AGGREGATE (merge finalize)\n" +
                        "  |  output: count(7: count), array_agg(8: array_agg)\n" +
                        "  |  group by: 4: v4\n" +
                        "  |  \n" +
                        "  8:EXCHANGE");

    }

    @Test
    public void testCountDistinctWithLimit() throws Exception {
        String sql = "select count(distinct v1), count(distinct v2) from t0 limit 10";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "1:AGGREGATE (update finalize)\n" +
                "  |  output: multi_distinct_count(1: v1), multi_distinct_count(2: v2)");

        sql = "select count(distinct v1), count(distinct v2) from t0 group by v3 limit 10";
        plan = getFragmentPlan(sql);
        assertContains(plan, "1:AGGREGATE (update finalize)\n" +
                "  |  output: multi_distinct_count(1: v1), multi_distinct_count(2: v2)\n" +
                "  |  group by: 3: v3");

        sql = "select /*+ SET_VAR (prefer_cte_rewrite = true) */ count(distinct v1), count(distinct v2) from t0 " +
                "limit 10";
        plan = getFragmentPlan(sql);
        assertContains(plan, " 18:NESTLOOP JOIN\n" +
                "  |  join op: CROSS JOIN\n" +
                "  |  colocate: false, reason: \n" +
                "  |  limit: 10");

        sql = "select /*+ SET_VAR (prefer_cte_rewrite = true) */ count(distinct v1), count(distinct v2) from t0 " +
                "group by v3 limit 10";
        plan = getFragmentPlan(sql);
        assertContains(plan, "13:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BUCKET_SHUFFLE(S))\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 7: v3 <=> 9: v3\n" +
                "  |  limit: 10");
    }

    @Test
    public void testOrderByWithAgg() throws Exception {
        String sql = "select round(count(t1e) * 100.0 / min(t1f), 4) as potential_customer_rate, " +
                "min(t1f) as t1f, min(t1f) as t1f from test_all_type_not_null " +
                "group by t1a, t1b " +
                "order by round(count(t1e) * 100.0 / min(t1f) / min(t1f), 4), min(t1f), abs(t1f)";

        String plan = getFragmentPlan(sql);
        System.out.println(plan);
        assertCContains(plan, "1:AGGREGATE (update finalize)\n" +
                        "  |  output: count(5: t1e), min(6: t1f)\n" +
                        "  |  group by: 1: t1a, 2: t1b",
                "order by: <slot 14> 14: round ASC, <slot 12> 12: min ASC, <slot 15> 15: abs ASC");
    }

    @Test
    public void testApproxTopK() throws Exception {
        {
            String sql = "select approx_top_k(L_LINENUMBER) from lineitem";
            getFragmentPlan(sql);
            sql = "select approx_top_k(L_LINENUMBER, 10000) from lineitem";
            getFragmentPlan(sql);
            sql = "select approx_top_k(L_LINENUMBER, 100, 10000) from lineitem";
            getFragmentPlan(sql);
            sql = "select approx_top_k(L_LINENUMBER, 10000, 10000) from lineitem";
            getFragmentPlan(sql);
            sql = "select approx_top_k(L_LINENUMBER, 1, 1) from lineitem";
            getFragmentPlan(sql);
        }
        {
            Exception exception = Assertions.assertThrows(SemanticException.class, () -> {
                String sql = "select approx_top_k(L_LINENUMBER, '111') from lineitem";
                getFragmentPlan(sql);
            });
            String expectedMessage = "The second parameter of APPROX_TOP_K must be a constant positive integer";
            String actualMessage = exception.getMessage();
            Assertions.assertTrue(actualMessage.contains(expectedMessage));
        }
        {
            Exception exception = Assertions.assertThrows(SemanticException.class, () -> {
                String sql = "select approx_top_k(L_LINENUMBER, 1, '111') from lineitem";
                getFragmentPlan(sql);
            });
            String expectedMessage = "The third parameter of APPROX_TOP_K must be a constant positive integer";
            String actualMessage = exception.getMessage();
            Assertions.assertTrue(actualMessage.contains(expectedMessage));
        }
        {
            Exception exception = Assertions.assertThrows(SemanticException.class, () -> {
                String sql = "select approx_top_k(L_LINENUMBER, 100001) from lineitem";
                getFragmentPlan(sql);
            });
            String expectedMessage = "The maximum number of the second parameter is 100000";
            String actualMessage = exception.getMessage();
            Assertions.assertTrue(actualMessage.contains(expectedMessage));
        }
        {
            Exception exception = Assertions.assertThrows(SemanticException.class, () -> {
                String sql = "select approx_top_k(L_LINENUMBER, 0) from lineitem";
                getFragmentPlan(sql);
            });
            String expectedMessage = "The second parameter of APPROX_TOP_K must be a constant positive integer";
            String actualMessage = exception.getMessage();
            Assertions.assertTrue(actualMessage.contains(expectedMessage));
        }
        {
            Exception exception = Assertions.assertThrows(SemanticException.class, () -> {
                String sql = "select approx_top_k(L_LINENUMBER, 1, 100001) from lineitem";
                getFragmentPlan(sql);
            });
            String expectedMessage = "The maximum number of the third parameter is 100000";
            String actualMessage = exception.getMessage();
            Assertions.assertTrue(actualMessage.contains(expectedMessage));
        }
        {
            Exception exception = Assertions.assertThrows(SemanticException.class, () -> {
                String sql = "select approx_top_k(L_LINENUMBER, 1, -1) from lineitem";
                getFragmentPlan(sql);
            });
            String expectedMessage = "The third parameter of APPROX_TOP_K must be a constant positive integer";
            String actualMessage = exception.getMessage();
            Assertions.assertTrue(actualMessage.contains(expectedMessage));
        }
        {
            Exception exception = Assertions.assertThrows(SemanticException.class, () -> {
                String sql = "select approx_top_k(L_LINENUMBER, 100, 99) from lineitem";
                getFragmentPlan(sql);
            });
            String expectedMessage = "The second parameter must be smaller than or equal to the third parameter";
            String actualMessage = exception.getMessage();
            Assertions.assertTrue(actualMessage.contains(expectedMessage));
        }
    }

    @Test
    public void testMannWhitneyUTest() throws Exception {
        {
            String sql = "select mann_whitney_u_test(L_LINENUMBER, L_LINENUMBER) from lineitem";
            getFragmentPlan(sql);
            sql = "select mann_whitney_u_test(L_LINENUMBER, L_LINENUMBER, 'two-sided') from lineitem";
            getFragmentPlan(sql);
            sql = "select mann_whitney_u_test(L_LINENUMBER, L_LINENUMBER, 'greater') from lineitem";
            getFragmentPlan(sql);
            sql = "select mann_whitney_u_test(L_LINENUMBER, L_LINENUMBER, 'less') from lineitem";
            getFragmentPlan(sql);
            sql = "select mann_whitney_u_test(L_LINENUMBER, L_LINENUMBER, 'two-sided', 0) from lineitem";
            getFragmentPlan(sql);
        }
        {
            Exception exception = Assertions.assertThrows(SemanticException.class, () -> {
                String sql = "select mann_whitney_u_test(L_LINENUMBER, L_LINENUMBER, 3) from lineitem";
                getFragmentPlan(sql);
            });
            String expectedMessage = "third parameter should be a string literal";
            String actualMessage = exception.getMessage();
            Assertions.assertTrue(actualMessage.contains(expectedMessage));
        }
        {
            Exception exception = Assertions.assertThrows(SemanticException.class, () -> {
                String sql = "select mann_whitney_u_test(L_LINENUMBER, L_LINENUMBER, 'two_sided') from lineitem";
                getFragmentPlan(sql);
            });
            String expectedMessage = "third parameter should be one of ['two-sided', 'greater', 'less']";
            String actualMessage = exception.getMessage();
            Assertions.assertTrue(actualMessage.contains(expectedMessage));
        }
        {
            Exception exception = Assertions.assertThrows(SemanticException.class, () -> {
                String sql = "select mann_whitney_u_test(L_LINENUMBER, L_LINENUMBER, 'two-sided', -1) from lineitem";
                getFragmentPlan(sql);
            });
            String expectedMessage = "mann_whitney_u_test's fourth parameter should be a non-negative int literal.";
            String actualMessage = exception.getMessage();
            Assertions.assertTrue(actualMessage.contains(expectedMessage));
        }
        {
            Exception exception = Assertions.assertThrows(SemanticException.class, () -> {
                String sql = "select mann_whitney_u_test(L_LINENUMBER, L_LINENUMBER, 'two-sided', 0.1) from lineitem";
                getFragmentPlan(sql);
            });
            String expectedMessage = "mann_whitney_u_test's fourth parameter should be a non-negative int literal.";
            String actualMessage = exception.getMessage();
            Assertions.assertTrue(actualMessage.contains(expectedMessage));
        }
    }

    @Test
    public void testTableAliasCountDistinctHaving() throws Exception {
        String sql = "select " +
                "   count(distinct xx.v2) as j1, " +
                "   xx.v2 as v2 " +
                "from test.t0 as xx " +
                "group by xx.v2 " +
                "having count(distinct xx.v2) > 0";
        connectContext.getSessionVariable().setEnableGroupbyUseOutputAlias(true);
        String plan = getFragmentPlan(sql);
        connectContext.getSessionVariable().setEnableGroupbyUseOutputAlias(false);
        assertContains(plan, "  1:AGGREGATE (update finalize)\n" +
                "  |  output: multi_distinct_count(2: v2)\n" +
                "  |  group by: 2: v2\n" +
                "  |  having: 4: count > 0");
        assertContains(plan, "0:OlapScanNode\n" +
                "     TABLE: t0\n" +
                "     PREAGGREGATION: ON\n" +
                "     partitions=0/1");
    }

    @Test
    public void testLegacyGroupConcat() throws Exception {
        String sql = "select /*+ set_var(sql_mode = GROUP_CONCAT_LEGACY) */ group_concat(v1) from t0";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "output: group_concat(CAST(1: v1 AS VARCHAR), ', ')");

        sql =
                "select /*+ set_var('sql_mode' = 'GROUP_CONCAT_LEGACY, ONLY_full_group_by') */ group_concat(v1, '-') from t0";
        plan = getFragmentPlan(sql);
        assertContains(plan, "output: group_concat(CAST(1: v1 AS VARCHAR), '-')");

        sql = "select /*+ set_var(sql_mode = '68719476768') */ group_concat(v1, '-') from t0";
        plan = getFragmentPlan(sql);
        assertContains(plan, "output: group_concat(CAST(1: v1 AS VARCHAR), '-')");

        sql = "select /*+ set_var(sql_mode = 68719476768) */ group_concat(v1, '-') from t0";
        plan = getFragmentPlan(sql);
        assertContains(plan, "output: group_concat(CAST(1: v1 AS VARCHAR), '-')");

        // overwrite the GROUP_CONCAT_LEGACY
        sql =
                "select /*+ set_var(sql_mode = 68719476768) */ /*+ set_var(sql_mode = 32) */ group_concat(v1, '-') from t0";
        plan = getFragmentPlan(sql);
        assertContains(plan, "output: group_concat(CAST(1: v1 AS VARCHAR), '-', ',')");
    }

    @Test
    public void testCountDistinctGlobalAgg() throws Exception {
        String sql = "select /*+SET_VAR(new_planner_agg_stage=1)*/ " +
                "count(distinct t1d) from test_all_type";
        String plan = getVerboseExplain(sql);
        assertContains(plan, "AGGREGATE (update finalize)");
        assertContains(plan, "aggregate: multi_distinct_count[([4: t1d, BIGINT, true]); " +
                "args: BIGINT; result: BIGINT; args nullable: true; result nullable: false]");

        sql = "select /*+SET_VAR(new_planner_agg_stage=1)*/ " +
                "count(distinct id_decimal) from test_all_type";
        plan = getVerboseExplain(sql);
        assertContains(plan, "AGGREGATE (update finalize)");
        assertContains(plan, "multi_distinct_count[([10: id_decimal, DECIMAL64(10,2), true]); " +
                "args: DECIMAL64; result: BIGINT; args nullable: true; result nullable: false]");

        sql = "select /*+SET_VAR(new_planner_agg_stage=1)*/ " +
                "sum(distinct id_decimal) from test_all_type";
        plan = getVerboseExplain(sql);
        assertContains(plan, "AGGREGATE (update finalize)");
        assertContains(plan, "aggregate: multi_distinct_sum[([10: id_decimal, DECIMAL64(10,2), true]); " +
                "args: DECIMAL64; result: DECIMAL128(38,2); args nullable: true; result nullable: true]");
    }

    @Test
    public void testArrayAggGlobalAgg() throws Exception {
        String sql = "select /*+SET_VAR(new_planner_agg_stage=1)*/ " +
                "array_agg(distinct t1g) from test_all_type";
        String plan = getVerboseExplain(sql);
        assertContains(plan, "AGGREGATE (update finalize)");
        assertContains(plan, "aggregate: array_agg_distinct[([7: t1g, BIGINT, true]); " +
                "args: BIGINT; result: ARRAY<BIGINT>; args nullable: true; result nullable: true]");

        sql = "select /*+SET_VAR(new_planner_agg_stage=1)*/ " +
                "array_agg(distinct t1f) from test_all_type";
        plan = getVerboseExplain(sql);
        assertContains(plan, "AGGREGATE (update finalize)");
        assertContains(plan, "aggregate: array_agg_distinct[([6: t1f, DOUBLE, true]); " +
                "args: DOUBLE; result: ARRAY<DOUBLE>; args nullable: true; result nullable: true]");
    }

    @Test
    public void testCountIfTypeCheck() throws Exception {
        String sql = "select count_if(v1 is null) from t0";
        String plan = getVerboseExplain(sql);
        System.out.println(plan);
        assertContains(plan, "aggregate: count_if[(1, [4: expr, BOOLEAN, false]); " +
                "args: TINYINT,BOOLEAN; result: BIGINT; args nullable: false; result nullable: false]");
    }

    @Test
    public void testOuterJoinBelowDistinctAgg() throws Exception {
        String sql = "select count(distinct v1), count(v4), abs(1) as a " +
                "from (select * from t0 left join t1 on v1 = v5) t group by a having max(v6) > a";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "10:Project\n" +
                "  |  <slot 7> : 7: abs\n" +
                "  |  <slot 8> : 8: count\n" +
                "  |  <slot 9> : 9: count\n" +
                "  |  \n" +
                "  9:AGGREGATE (merge finalize)\n" +
                "  |  output: count(8: count), count(9: count), max(10: max)\n" +
                "  |  group by: 7: abs\n" +
                "  |  having: 10: max > CAST(abs(1) AS BIGINT)");
    }

    @Test
    public void testRemoveGroupByConstant() throws Exception {
        String sql = "select count(*), abs(1) as a, abs(2) as b, 'c' from t0 group by a, b, 'c'";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "3:Project\n" +
                "  |  <slot 4> : 4: abs\n" +
                "  |  <slot 7> : 7: count\n" +
                "  |  <slot 8> : abs(2)\n" +
                "  |  <slot 9> : 'c'\n" +
                "  |  \n" +
                "  2:AGGREGATE (update finalize)\n" +
                "  |  output: count(*)\n" +
                "  |  group by: 4: abs");

        sql = "select count(*), abs(1) as a, abs(2) as b from t0 group by a, b, v1 having a > b";
        plan = getFragmentPlan(sql);
        assertContains(plan, "1:AGGREGATE (update finalize)\n" +
                "  |  output: count(*)\n" +
                "  |  group by: 1: v1\n" +
                "  |  having: abs(1) > abs(2)");

        sql = "select count(*), abs(1) as a, abs(2) as b from t0 group by a + b, v1 having a > b";
        plan = getFragmentPlan(sql);
        assertContains(plan, "2:Project\n" +
                "  |  <slot 5> : 5: count\n" +
                "  |  <slot 6> : abs(1)\n" +
                "  |  <slot 7> : abs(2)\n" +
                "  |  \n" +
                "  1:AGGREGATE (update finalize)\n" +
                "  |  output: count(*)\n" +
                "  |  group by: 1: v1\n" +
                "  |  having: abs(1) > abs(2)");

        sql = "select max(a), a from (select v1, abs(1) as a, abs(2) as b from t0) t group by a, v1";
        plan = getFragmentPlan(sql);
        assertContains(plan, "2:Project\n" +
                "  |  <slot 7> : abs(1)\n" +
                "  |  <slot 8> : 8: max\n" +
                "  |  \n" +
                "  1:AGGREGATE (update finalize)\n" +
                "  |  output: max(abs(1))\n" +
                "  |  group by: 1: v1");
    }

    @Test
    public void testMultiCountDistinctWithHavingLimit() throws Exception {
        FeConstants.runningUnitTest = true;
        String sql = "select count(distinct t1b) as x, count(distinct t1c) as y from test_all_type having x = 2";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "  8:AGGREGATE (merge finalize)\n" +
                "  |  output: count(11: count)\n" +
                "  |  group by: \n" +
                "  |  having: 11: count = 2");
        FeConstants.runningUnitTest = false;

        sql = "select count(distinct t1b) as x, count(distinct t1c) as y from test_all_type having x = 2 limit 10";
        plan = getFragmentPlan(sql);
        assertContains(plan, "  1:AGGREGATE (update finalize)\n" +
                "  |  output: multi_distinct_count(2: t1b), multi_distinct_count(3: t1c)\n" +
                "  |  group by: \n" +
                "  |  having: 11: count = 2\n" +
                "  |  limit: 10");

        connectContext.getSessionVariable().setOptimizerExecuteTimeout(-1);
        sql = "select avg(distinct t1b) as x, count(distinct t1c) as y from test_all_type having x = 2 limit 10";
        plan = getFragmentPlan(sql);
        assertContains(plan, "  1:AGGREGATE (update finalize)\n" +
                "  |  output: multi_distinct_count(3: t1c), multi_distinct_count(2: t1b), multi_distinct_sum(2: t1b)\n" +
                "  |  group by: \n" +
                "  |  having: CAST(14: multi_distinct_sum AS DOUBLE) / CAST(13: multi_distinct_count AS DOUBLE) = 2.0\n" +
                "  |  limit: 10");
    }

    @Test
    public void testAvgDecimalScale() throws Exception {
        String sql = "select avg(v2 - 1.86659630566164 * (v3 - 3.062175673706)) from t0 group by v1;";
        String plan = getVerboseExplain(sql);
        assertContains(plan, "3:Project\n" +
                "  |  output columns:\n" +
                "  |  5 <-> [5: avg, DECIMAL128(38,18), true]\n" +
                "  |  cardinality: 1");
    }

    @Test
    public void testOnlyGroupByLimit() throws Exception {
        FeConstants.runningUnitTest = true;
        String sql = "select distinct v1 + v2 as vx from t0 limit 10";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "  2:AGGREGATE (update serialize)\n" +
                "  |  STREAMING\n" +
                "  |  group by: 4: expr\n" +
                "  |  limit: 10");
        FeConstants.runningUnitTest = false;
    }

    @Test
    public void testHavingAggregate() throws Exception {
        String sql = "select * from (" +
                "select sum(v1), f2, v3 from " +
                "   (select v1, v2, v2 + 2 as f2, v3 from t0) cc " +
                "group by v2, f2, v3 having (f2 + sum(v1)) > 0" +
                ") xx ";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "  1:AGGREGATE (update finalize)\n" +
                "  |  output: sum(1: v1)\n" +
                "  |  group by: 2: v2, 3: v3\n" +
                "  |  having: 2: v2 + 2 + 5: sum > 0");
    }

    @Test
    public void testOneTabletMultiDistinctFunctionHasSingleDistinctColAndOneGroupBy() throws Exception {
        String sql = "select\n" +
                "  t1.k1\n" +
                "  , count(distinct k2) \n" +
                "  , array_agg(distinct k2)\n" +
                "from\n" +
                "  db1.tbl1 t1\n" +
                "group by\n" +
                "  k1\n" +
                "  ;";
        String plan = getFragmentPlan(sql);
        assertNotContains(plan, " 1:AGGREGATE (update finalize)\n" +
                "  |  output: count(DISTINCT 2: k2), array_agg(DISTINCT 2: k2)\n" +
                "  |  group by: 1: k1");

        assertContains(plan, "1:AGGREGATE (update finalize)\n" +
                "  |  output: multi_distinct_count(2: k2), array_agg_distinct(2: k2)\n" +
                "  |  group by: 1: k1\n" +
                "  |  \n" +
                "  0:OlapScanNode\n" +
                "     TABLE: tbl1");
    }
}
