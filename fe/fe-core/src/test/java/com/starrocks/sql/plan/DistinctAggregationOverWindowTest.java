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

import com.google.api.client.util.Lists;
import com.google.common.collect.Sets;
import com.starrocks.catalog.AggregateFunction;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.common.FeConstants;
import com.starrocks.common.Pair;
import com.starrocks.planner.AggregationNode;
import com.starrocks.planner.AnalyticEvalNode;
import com.starrocks.sql.ast.expression.FunctionCallExpr;
import com.starrocks.type.IntegerType;
import com.starrocks.type.StructType;
import com.starrocks.type.Type;
import com.starrocks.utframe.StarRocksAssert;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;

public class DistinctAggregationOverWindowTest extends PlanTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        FeConstants.unitTestView = false;
        PlanTestBase.beforeClass();
        StarRocksAssert starRocksAssert = new StarRocksAssert(connectContext);
        starRocksAssert.withTable("CREATE TABLE `s1` (    \n" +
                "  `v1` bigint(20) NULL COMMENT \"\",    \n" +
                "  `v2` int(11) NULL COMMENT \"\",    \n" +
                "  `s1` struct<count bigint, sum double, avg double> NULL COMMENT \"\",    \n" +
                "  `a1` array<varchar(65533)> NULL COMMENT \"\",    \n" +
                "  `a2` array<varchar(65533)> NULL COMMENT \"\"    \n" +
                ") ENGINE=OLAP    \n" +
                "DUPLICATE KEY(`v1`)    \n" +
                "COMMENT \"OLAP\"    \n" +
                "DISTRIBUTED BY HASH(`v1`) BUCKETS 10    \n" +
                "PROPERTIES (    \n" +
                "\"replication_num\" = \"1\",       \n" +
                "\"in_memory\" = \"false\",    \n" +
                "\"enable_persistent_index\" = \"true\",    \n" +
                "\"replicated_storage\" = \"false\",    \n" +
                "\"light_schema_change\" = \"true\",    \n" +
                "\"compression\" = \"LZ4\"    \n" +
                ");");
    }

    @Test
    public void testCountDistinctWithPartition() throws Exception {

        String sql = "select v1,v2,v3,\n" +
                "      count (distinct v3) \n" +
                "      \tover(partition by v1, v2)\n" +
                "from t0;";

        String plan = getFragmentPlan(sql);
        assertCContains(plan, "  10:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BUCKET_SHUFFLE(S))\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 1: v1 <=> 5: v1\n" +
                "  |  equal join conjunct: 2: v2 <=> 6: v2\n" +
                "  |  \n" +
                "  |----9:AGGREGATE (update finalize)\n" +
                "  |    |  output: count(7: v3)\n" +
                "  |    |  group by: 5: v1, 6: v2\n" +
                "  |    |  \n" +
                "  |    8:AGGREGATE (merge serialize)\n" +
                "  |    |  group by: 5: v1, 6: v2, 7: v3\n" +
                "  |    |  \n" +
                "  |    7:EXCHANGE\n");

        assertCContains(plan, "  MultiCastDataSinks\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 01\n" +
                "    RANDOM\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 04\n" +
                "    RANDOM\n" +
                "\n" +
                "  0:OlapScanNode\n" +
                "     TABLE: t0");
    }

    @Test
    public void testMultiDistinctAggregationsWithPartition() throws Exception {

        String sql = "select \n" +
                "v1, v2, v3, \n" +
                "count(distinct v3) over(partition by v1, v2) cnt,\n" +
                "sum(distinct v3) over(partition by v1, v2) sum,\n" +
                "avg(distinct v3) over(partition by v1, v2) avg\n" +
                "from t0";

        String plan = getFragmentPlan(sql);
        assertCContains(plan, "  11:Project\n" +
                "  |  <slot 1> : 1: v1\n" +
                "  |  <slot 2> : 2: v2\n" +
                "  |  <slot 3> : 3: v3\n" +
                "  |  <slot 4> : 4: count(distinct 3: v3)\n" +
                "  |  <slot 5> : 5: sum(distinct 3: v3)\n" +
                "  |  <slot 6> : 6: avg(distinct 3: v3)\n" +
                "  |  \n" +
                "  10:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BUCKET_SHUFFLE(S))\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 7: v1 <=> 1: v1\n" +
                "  |  equal join conjunct: 8: v2 <=> 2: v2");

        assertCContains(plan, "  6:AGGREGATE (update finalize)\n" +
                "  |  output: count(9: v3), sum(9: v3), avg(9: v3)\n" +
                "  |  group by: 7: v1, 8: v2\n" +
                "  |  \n" +
                "  5:AGGREGATE (merge serialize)\n" +
                "  |  group by: 7: v1, 8: v2, 9: v3");
    }

    @Test
    public void testMultiDistinctAggregationsMixedWithOthersWithPartition() throws Exception {
        String sql = "select \n" +
                "v1, v2, v3, \n" +
                "count(distinct v3) over(partition by v1, v2) cnt,\n" +
                "sum(distinct v3) over(partition by v1, v2) sum,\n" +
                "avg(distinct v3) over(partition by v1, v2) avg,\n" +
                "count(v3) over(partition by v1, v2) cnt1,\n" +
                "sum(v3) over(partition by v1, v2) sum1,\n" +
                "avg(v3) over(partition by v1, v2) avg1,\n" +
                "rank() over(partition by v1, v2) r\n" +
                "from t0;";
        String plan = getFragmentPlan(sql);
        assertCContains(plan, "  13:ANALYTIC\n" +
                "  |  functions: [, rank(), ]\n" +
                "  |  partition by: 1: v1, 2: v2\n" +
                "  |  \n" +
                "  12:SORT\n" +
                "  |  order by: <slot 1> 1: v1 ASC, <slot 2> 2: v2 ASC\n" +
                "  |  analytic partition by: 1: v1, 2: v2\n" +
                "  |  offset: 0\n" +
                "  |  \n" +
                "  11:Project\n" +
                "  |  <slot 1> : 1: v1\n" +
                "  |  <slot 2> : 2: v2\n" +
                "  |  <slot 3> : 3: v3\n" +
                "  |  <slot 4> : 4: count(distinct 3: v3)\n" +
                "  |  <slot 5> : 5: sum(distinct 3: v3)\n" +
                "  |  <slot 6> : 6: avg(distinct 3: v3)\n" +
                "  |  <slot 7> : 7: count(3: v3)\n" +
                "  |  <slot 8> : 8: sum(3: v3)\n" +
                "  |  <slot 9> : 9: avg(3: v3)\n" +
                "  |  \n" +
                "  10:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BUCKET_SHUFFLE(S))\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 11: v1 <=> 1: v1\n" +
                "  |  equal join conjunct: 12: v2 <=> 2: v2");
        assertCContains(plan, "  6:AGGREGATE (update finalize)\n" +
                "  |  output: count(13: v3), sum(13: v3), avg(13: v3), count(7: count(3: v3)), " +
                "sum(8: sum(3: v3)), avg(9: avg(3: v3))\n" +
                "  |  group by: 11: v1, 12: v2\n" +
                "  |  \n" +
                "  5:AGGREGATE (merge serialize)\n" +
                "  |  output: count(7: count(3: v3)), sum(8: sum(3: v3)), avg(9: avg(3: v3))\n" +
                "  |  group by: 11: v1, 12: v2, 13: v3");
    }

    @Test
    public void testMultiDistinctAggregationsMixedWithOthersWithMultiPartition() throws Exception {
        String sql = "select \n" +
                "v1, v2, v3, \n" +
                "count(distinct v3) over(partition by v1, v2) cnt,\n" +
                "sum(distinct v2) over(partition by v1, v3) sum,\n" +
                "avg(distinct v1) over(partition by v3, v2) avg,\n" +
                "count(v1) over(partition by v2, v3) cnt1,\n" +
                "sum(v2) over(partition by v3, v1) sum1,\n" +
                "avg(v3) over(partition by v2, v1) avg1,\n" +
                "rank() over(partition by v1, v2) r \n" +
                "from t0;";
        String plan = getFragmentPlan(sql);
        long numJoins = Arrays.stream(plan.split("\n")).filter(ln -> ln.contains("HASH JOIN")).count();
        Assertions.assertEquals(3, numJoins);
        long numAnalytics = Arrays.stream(plan.split("\n")).filter(ln -> ln.contains("ANALYTIC")).count();
        Assertions.assertEquals(1, numAnalytics);

        assertCContains(plan, "  34:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BROADCAST)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 3: v3 <=> 22: v3\n" +
                "  |  equal join conjunct: 2: v2 <=> 21: v2");

        assertCContains(plan, "  23:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BUCKET_SHUFFLE(S))\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 1: v1 <=> 14: v1\n" +
                "  |  equal join conjunct: 3: v3 <=> 16: v3\n" +
                "  |  \n" +
                "  |----22:AGGREGATE (update finalize)\n" +
                "  |    |  output: sum(15: v2), sum(8: sum(2: v2))\n" +
                "  |    |  group by: 14: v1, 16: v3");
        assertCContains(plan, "  10:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BUCKET_SHUFFLE(S))\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 11: v1 <=> 1: v1\n" +
                "  |  equal join conjunct: 12: v2 <=> 2: v2\n" +
                "  |  \n" +
                "  |----9:EXCHANGE\n" +
                "  |    \n" +
                "  6:AGGREGATE (update finalize)\n" +
                "  |  output: count(13: v3), avg(5: avg(3: v3))\n" +
                "  |  group by: 11: v1, 12: v2");
    }

    @Test
    public void testCountDistinctWithoutPartition() throws Exception {

        String sql = "select v1,v2,v3,\n" +
                "      count (distinct v3) \n" +
                "      \tover()\n" +
                "from t0;";

        String plan = getFragmentPlan(sql);
        assertCContains(plan, "  12:NESTLOOP JOIN\n" +
                "  |  join op: CROSS JOIN");
        assertCContains(plan, "  8:AGGREGATE (update serialize)\n" +
                "  |  output: count(7: v3)\n" +
                "  |  group by: \n" +
                "  |  \n" +
                "  7:AGGREGATE (merge serialize)\n" +
                "  |  group by: 7: v3");
    }

    @Test
    public void testMultiDistinctAggregationsWithoutPartition() throws Exception {

        String sql = "select \n" +
                "v1, v2, v3, \n" +
                "count(distinct v3) over() cnt,\n" +
                "sum(distinct v3) over() sum,\n" +
                "avg(distinct v3) over() avg\n" +
                "from t0";

        String plan = getFragmentPlan(sql);
        assertCContains(plan, "  12:NESTLOOP JOIN\n" +
                "  |  join op: CROSS JOIN");
        assertCContains(plan, "  10:AGGREGATE (merge finalize)\n" +
                "  |  output: count(4: count(distinct 3: v3)), " +
                "sum(5: sum(distinct 3: v3)), " +
                "avg(6: avg(distinct 3: v3))\n" +
                "  |  group by: ");
    }

    @Test
    public void testMultiDistinctAggregationsMixedWithOthersWithoutPartition() throws Exception {
        String sql = "select \n" +
                "v1, v2, v3, \n" +
                "count(distinct v3) over() cnt,\n" +
                "sum(distinct v3) over() sum,\n" +
                "avg(distinct v3) over() avg,\n" +
                "count(v3) over() cnt1,\n" +
                "sum(v3) over() sum1,\n" +
                "avg(v3) over() avg1,\n" +
                "rank() over() r \n" +
                "from t0";
        String plan = getFragmentPlan(sql);
        assertCContains(plan, "  13:ANALYTIC\n" +
                "  |  functions: [, rank(), ]\n" +
                "  |  \n" +
                "  12:NESTLOOP JOIN\n" +
                "  |  join op: CROSS JOIN\n" +
                "  |  colocate: false, reason: \n" +
                "  |  \n" +
                "  |----11:EXCHANGE\n" +
                "  |    \n" +
                "  8:AGGREGATE (merge finalize)\n" +
                "  |  output: count(4: count(distinct 3: v3)), sum(5: sum(distinct 3: v3)), " +
                "avg(6: avg(distinct 3: v3)), count(7: count(3: v3)), sum(8: sum(3: v3)), avg(9: avg(3: v3))\n" +
                "  |  group by: \n");
    }

    @Test
    public void testCountDistinctWithCte() throws Exception {
        String sql = "select v1, v2, v3, cnt\n" +
                "from (select v1, v2, v3, count(distinct v3) over(partition by v1, v2) cnt\n" +
                "from t0) t\n" +
                "order by cnt\n" +
                "limit 10;";
        String plan = getFragmentPlan(sql);
        assertCContains(plan, "  12:TOP-N\n" +
                "  |  order by: <slot 4> 4: count(distinct 3: v3) ASC\n" +
                "  |  offset: 0\n" +
                "  |  limit: 10\n" +
                "  |  \n" +
                "  11:Project\n" +
                "  |  <slot 1> : 1: v1\n" +
                "  |  <slot 2> : 2: v2\n" +
                "  |  <slot 3> : 3: v3\n" +
                "  |  <slot 4> : 4: count(distinct 3: v3)\n" +
                "  |  \n" +
                "  10:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BUCKET_SHUFFLE(S))\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 1: v1 <=> 5: v1\n" +
                "  |  equal join conjunct: 2: v2 <=> 6: v2");
    }

    @Test
    public void testDistinctAggAsSubquery() throws Exception {
        String sql = "select  (sum(murmur_hash3_32(ifnull(v1,0))+murmur_hash3_32(ifnull(v2,0))+" +
                "murmur_hash3_32(ifnull(v3,0))+murmur_hash3_32(ifnull(cnt,0))+murmur_hash3_32(ifnull(sum,0))+" +
                "murmur_hash3_32(ifnull(avg,0)))) as fingerprint from (select\n" +
                "v1,v2,v3,\n" +
                "count(distinct v3) over(partition by v1,v2) cnt,\n" +
                "sum(distinct v3) over(partition by v1,v2) sum,\n" +
                "avg(distinct v3) over(partition by v1,v2) avg\n" +
                "from t0) as t;";
        String plan = getFragmentPlan(sql);
        assertCContains(plan, "  12:AGGREGATE (update serialize)\n" +
                "  |  output: sum(7: expr)\n" +
                "  |  group by: \n" +
                "  |  \n" +
                "  11:Project\n" +
                "  |  <slot 7> : CAST(murmur_hash3_32(CAST(ifnull(1: v1, 0) AS VARCHAR)) AS BIGINT) + " +
                "CAST(murmur_hash3_32(CAST(ifnull(2: v2, 0) AS VARCHAR)) AS BIGINT) + " +
                "CAST(murmur_hash3_32(CAST(ifnull(3: v3, 0) AS VARCHAR)) AS BIGINT) + " +
                "CAST(murmur_hash3_32(CAST(ifnull(4: count(distinct 3: v3), 0) AS VARCHAR)) AS BIGINT) + " +
                "CAST(murmur_hash3_32(CAST(ifnull(5: sum(distinct 3: v3), 0) AS VARCHAR)) AS BIGINT) + " +
                "CAST(murmur_hash3_32(CAST(ifnull(6: avg(distinct 3: v3), 0.0) AS VARCHAR)) AS BIGINT)\n" +
                "  |  \n" +
                "  10:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BUCKET_SHUFFLE(S))\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 9: v1 <=> 1: v1\n" +
                "  |  equal join conjunct: 10: v2 <=> 2: v2");
    }

    @Test
    public void testFramedWindow() throws Exception {
        String sql = "with cte as(\n" +
                "select v1,v2,v3,\n" +
                " count(distinct v3)over(partition by v1 order by v2) as dist_count,\n" +
                " sum(distinct v3)over(partition by v1 order by v2) as dist_sum,\n" +
                " avg(distinct v3)over(partition by v1 order by v2) as dist_avg\n" +
                "from t0\n" +
                ")\n" +
                "select sum(murmur_hash3_32(v1)),\n" +
                "       sum(murmur_hash3_32(v2)),\n" +
                "       sum(murmur_hash3_32(v3)),\n" +
                "       sum(murmur_hash3_32(dist_count)),\n" +
                "       sum(murmur_hash3_32(dist_sum)),\n" +
                "       sum(murmur_hash3_32(dist_avg))\n" +
                "from cte;";
        String plan = getFragmentPlan(sql);
        assertCContains(plan, "  3:Project\n" +
                "  |  <slot 1> : 1: v1\n" +
                "  |  <slot 2> : 2: v2\n" +
                "  |  <slot 3> : 3: v3\n" +
                "  |  <slot 10> : murmur_hash3_32(CAST(19: fused_multi_distinct_count_sum_avg.count[true] AS " +
                "VARCHAR))\n" +
                "  |  <slot 11> : murmur_hash3_32(CAST(19: fused_multi_distinct_count_sum_avg.sum[true] AS " +
                "VARCHAR))\n" +
                "  |  <slot 12> : murmur_hash3_32(CAST(19: fused_multi_distinct_count_sum_avg.avg[true] AS " +
                "VARCHAR))\n" +
                "  |  \n" +
                "  2:ANALYTIC\n" +
                "  |  functions: [, fused_multi_distinct_count_sum_avg(3: v3), ]\n" +
                "  |  partition by: 1: v1\n" +
                "  |  order by: 2: v2 ASC\n" +
                "  |  window: RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW");

        connectContext.getSessionVariable().setOptimizeDistinctAggOverFramedWindow(1);
        plan = getFragmentPlan(sql);
        connectContext.getSessionVariable().setOptimizeDistinctAggOverFramedWindow(0);
        assertCContains(plan, "  MultiCastDataSinks\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 01\n" +
                "    RANDOM\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 11\n" +
                "    RANDOM\n" +
                "\n" +
                "  0:OlapScanNode\n" +
                "     TABLE: t0");

        assertCContains(plan, "  5:AGGREGATE (merge finalize)\n" +
                "  |  group by: 20: v1, 21: v2, 22: ");

        assertCContains(plan, "  9:Project\n" +
                "  |  <slot 4> : 19: fused_multi_distinct_count_sum_avg.count[false]\n" +
                "  |  <slot 5> : 19: fused_multi_distinct_count_sum_avg.sum[false]\n" +
                "  |  <slot 6> : 19: fused_multi_distinct_count_sum_avg.avg[false]\n" +
                "  |  <slot 20> : 20: v1\n" +
                "  |  <slot 21> : 21: v2\n" +
                "  |  \n" +
                "  8:ANALYTIC\n" +
                "  |  functions: [, fused_multi_distinct_count_sum_avg(22: v3), ]\n" +
                "  |  partition by: 20: v1\n" +
                "  |  order by: 21: v2 ASC\n" +
                "  |  window: RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW");
    }

    @Test
    public void testDistinctDecimalFramedWindow() throws Exception {
        String sql = "select v1,v2,v3,\n" +
                " count(distinct cast(v3 as decimal(19,2)))over(partition by v1 order by v2) as dist_count,\n" +
                " sum(distinct cast(v3 as decimal(19,2)))over(partition by v1 order by v2) as dist_sum,\n" +
                " avg(distinct cast(v3 as decimal(19,2)))over(partition by v1 order by v2) as dist_avg\n" +
                "from t0 ";
        String plan = getVerboseExplain(sql);
        assertCContains(plan, "  1:Project\n" +
                "  |  output columns:\n" +
                "  |  1 <-> [1: v1, BIGINT, true]\n" +
                "  |  2 <-> [2: v2, BIGINT, true]\n" +
                "  |  3 <-> [3: v3, BIGINT, true]\n" +
                "  |  7 <-> cast([3: v3, BIGINT, true] as DECIMAL128(19,2))");

        assertCContains(plan, "  3:ANALYTIC\n" +
                "  |  functions: [, fused_multi_distinct_count_sum_avg[([7: cast, DECIMAL128(19,2), true]); args: " +
                "DECIMAL128; result: struct<count bigint(20), sum decimal(38, 2), avg decimal(38, 8)>; " +
                "args nullable: true; result nullable: true], ]\n" +
                "  |  partition by: [1: v1, BIGINT, true]\n" +
                "  |  order by: [2: v2, BIGINT, true] ASC\n" +
                "  |  window: RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW");

        assertCContains(plan, "  4:Project\n" +
                "  |  output columns:\n" +
                "  |  1 <-> [1: v1, BIGINT, true]\n" +
                "  |  2 <-> [2: v2, BIGINT, true]\n" +
                "  |  3 <-> [3: v3, BIGINT, true]\n" +
                "  |  4 <-> [8: fused_multi_distinct_count_sum_avg, struct<count bigint(20), sum decimal(38, 2), " +
                "avg decimal(38, 8)>, true].count[false]\n" +
                "  |  5 <-> [8: fused_multi_distinct_count_sum_avg, struct<count bigint(20), sum decimal(38, 2), " +
                "avg decimal(38, 8)>, true].sum[false]\n" +
                "  |  6 <-> [8: fused_multi_distinct_count_sum_avg, struct<count bigint(20), sum decimal(38, 2), " +
                "avg decimal(38, 8)>, true].avg[false]");
    }

    private Type getAggregateFunctionReturnType(String sql, String... functionNames) throws Exception {
        ExecPlan execPlan = getExecPlan(sql);
        Set<String> names = Sets.newHashSet(functionNames);

        List<AggregationNode> aggNodes = Lists.newArrayList();
        execPlan.getTopFragment().getPlanRoot().collect(AggregationNode.class, aggNodes);
        Optional<Type> optReturnType = aggNodes.stream()
                .flatMap(agg -> agg.getAggInfo().getAggregateExprs().stream())
                .filter(fcall -> names.contains(fcall.getFnName().getFunction()))
                .findFirst()
                .map(fcall -> ((AggregateFunction) fcall.getFn()).getReturnType());

        if (optReturnType.isPresent()) {
            return optReturnType.get();
        }

        List<AnalyticEvalNode> windowNodes = Lists.newArrayList();
        execPlan.getTopFragment().getPlanRoot().collect(AnalyticEvalNode.class, windowNodes);
        optReturnType = windowNodes.stream()
                .flatMap(win -> win.getAnalyticFnCalls().stream().map(e -> (FunctionCallExpr) e))
                .filter(fcall -> names.contains(fcall.getFnName().getFunction()))
                .findFirst()
                .map(fcall -> ((AggregateFunction) fcall.getFn()).getReturnType());

        Assertions.assertTrue(optReturnType.isPresent());
        return optReturnType.get();
    }

    @SafeVarargs
    final void checkFusedMultiDistinct(String sqlFmt, String type, String fusedFunName,
                                       Pair<String, Type>... fieldTypes)
            throws Exception {
        String sql = sqlFmt.replaceAll("\\{TYPE\\}", type);
        StructType fusedType = (StructType) getAggregateFunctionReturnType(sql, fusedFunName);
        connectContext.getSessionVariable().setOptimizeDistinctAggOverFramedWindow(1);
        StructType fusedType2 = (StructType) getAggregateFunctionReturnType(sql, fusedFunName);
        connectContext.getSessionVariable().setOptimizeDistinctAggOverFramedWindow(0);
        Assertions.assertEquals(IntegerType.BIGINT, fusedType.getField(FunctionSet.COUNT).getType());
        for (Pair<String, Type> ftype : fieldTypes) {
            Assertions.assertEquals(ftype.second, fusedType.getField(ftype.first).getType());
            Assertions.assertEquals(ftype.second, fusedType2.getField(ftype.first).getType());
        }
    }

    @Test
    public void testFusedMultiDistinct() throws Exception {
        String countSumAvgSqlFmt = "select v1,v2,v3,\n" +
                " count(distinct cast(v3 as {TYPE}))over(partition by v1 order by v2) as dist_count,\n" +
                " sum(distinct cast(v3 as {TYPE}))over(partition by v1 order by v2) as dist_sum,\n" +
                " avg(distinct cast(v3 as {TYPE}))over(partition by v1 order by v2) as dist_avg\n" +
                "from t0 ";

        String countSumSqlFmt = "select v1,v2,v3,\n" +
                " count(distinct cast(v3 as {TYPE}))over(partition by v1 order by v2) as dist_count,\n" +
                " sum(distinct cast(v3 as {TYPE}))over(partition by v1 order by v2) as dist_sum\n" +
                "from t0 ";

        String countAvgSqlFmt = "select v1,v2,v3,\n" +
                " count(distinct cast(v3 as {TYPE}))over(partition by v1 order by v2) as dist_count,\n" +
                " avg(distinct cast(v3 as {TYPE}))over(partition by v1 order by v2) as dist_avg\n" +
                "from t0 ";

        String sumAvgSqlFmt = "select v1,v2,v3,\n" +
                " sum(distinct cast(v3 as {TYPE}))over(partition by v1 order by v2) as dist_sum,\n" +
                " avg(distinct cast(v3 as {TYPE}))over(partition by v1 order by v2) as dist_avg\n" +
                "from t0 ";

        String countSqlFmt = "select v1,v2,v3,\n" +
                "count(distinct cast(v3 as {TYPE}))over(partition by v1 order by v2) as dist_count\n" +
                "from t0 ";

        String sumSqlFmt = "select v1,v2,v3,\n" +
                " sum(distinct cast(v3 as {TYPE}))over(partition by v1 order by v2) as dist_sum\n" +
                "from t0 ";

        String avgSqlFmt = "select v1,v2,v3,\n" +
                " avg(distinct cast(v3 as {TYPE}))over(partition by v1 order by v2) as dist_avg\n" +
                "from t0 ";

        String checkSumSqlFmt = "select sum(distinct cast(v3 as {TYPE})) from t0";
        String checkAvgSqlFmt = "select avg(distinct cast(v3 as {TYPE})) from t0";
        String[] types = new String[] {
                "decimal(7,2)",
                "decimal(19,2)",
                "decimal(38,19)",
                "decimal(38,20)",
                "decimal(38,18)",
                "decimal(39,18)",
                "boolean",
                "tinyint",
                "int",
                "smallint",
                "bigint",
                "largeint",
                "float",
                "double",
                "string",
                "varchar",
                "char",
        };

        for (String type : types) {
            String checkSumSql = checkSumSqlFmt.replaceAll("\\{TYPE\\}", type);
            String checkAvgSql = checkAvgSqlFmt.replaceAll("\\{TYPE\\}", type);
            Type sumType =
                    getAggregateFunctionReturnType(checkSumSql, FunctionSet.SUM, FunctionSet.MULTI_DISTINCT_SUM);
            Type avgType = getAggregateFunctionReturnType(checkAvgSql, FunctionSet.AVG);
            checkFusedMultiDistinct(countSumAvgSqlFmt, type, FunctionSet.FUSED_MULTI_DISTINCT_COUNT_SUM_AVG,
                    Pair.create(FunctionSet.SUM, sumType), Pair.create(FunctionSet.AVG, avgType));

            checkFusedMultiDistinct(countSumSqlFmt, type, FunctionSet.FUSED_MULTI_DISTINCT_COUNT_SUM,
                    Pair.create(FunctionSet.SUM, sumType));

            checkFusedMultiDistinct(countAvgSqlFmt, type, FunctionSet.FUSED_MULTI_DISTINCT_COUNT_AVG,
                    Pair.create(FunctionSet.AVG, avgType));

            checkFusedMultiDistinct(sumAvgSqlFmt, type, FunctionSet.FUSED_MULTI_DISTINCT_COUNT_SUM_AVG,
                    Pair.create(FunctionSet.SUM, sumType), Pair.create(FunctionSet.AVG, avgType));

            checkFusedMultiDistinct(countSqlFmt, type, FunctionSet.FUSED_MULTI_DISTINCT_COUNT);

            checkFusedMultiDistinct(sumSqlFmt, type, FunctionSet.FUSED_MULTI_DISTINCT_COUNT_SUM,
                    Pair.create(FunctionSet.SUM, sumType));

            checkFusedMultiDistinct(avgSqlFmt, type, FunctionSet.FUSED_MULTI_DISTINCT_COUNT_AVG,
                    Pair.create(FunctionSet.AVG, avgType));
        }

        String[] timeTypes = new String[] {"date", "datetime"};
        for (String type : timeTypes) {
            checkFusedMultiDistinct(countSqlFmt, type, FunctionSet.FUSED_MULTI_DISTINCT_COUNT);
        }
    }

    @Test
    public void testComplexSql() throws Exception {
        String sql = "with cte as(\n" +
                "select distinct v1,v2,v3,\n" +
                " count(distinct v3)over(partition by v1 order by v2) as dist_count,\n" +
                " sum(distinct v3)over(partition by v1 order by v2) as dist_sum,\n" +
                " avg(distinct v3)over(partition by v1 order by v2) as dist_avg\n" +
                "from t0\n" +
                "),\n" +
                "cte1 as(\n" +
                "select v1,v2,v3,\n" +
                " count(distinct cast(v3 as decimal(19,2)))over(partition by v1 order by v2) as dist_count,\n" +
                " sum(distinct cast(v3 as decimal(19,2)))over(partition by v1 order by v2) as dist_sum,\n" +
                " avg(distinct cast(v3 as decimal(19,2)))over(partition by v1 order by v2) as dist_avg\n" +
                "from t0 \n" +
                ")\n" +
                "select cte.dist_count = cte1.dist_count, cte.dist_sum = cte1.dist_sum, cte.dist_avg = cte1.dist_avg\n" +
                "from cte join cte1 on cte.v1 = cte1.v1 and cte.v2 = cte1.v2 and cte.v3 = cte1.v3;";
        String plan = getFragmentPlan(sql);
        assertCContains(plan, "  10:Project\n" +
                "  |  <slot 1> : 1: v1\n" +
                "  |  <slot 2> : 2: v2\n" +
                "  |  <slot 3> : 3: v3\n" +
                "  |  <slot 4> : 16: fused_multi_distinct_count_sum_avg.count[false]\n" +
                "  |  <slot 5> : 16: fused_multi_distinct_count_sum_avg.sum[false]\n" +
                "  |  <slot 6> : 16: fused_multi_distinct_count_sum_avg.avg[false]\n" +
                "  |  \n" +
                "  9:SELECT\n" +
                "  |  predicates: 2: v2 IS NOT NULL, 3: v3 IS NOT NULL\n" +
                "  |  \n" +
                "  8:ANALYTIC\n" +
                "  |  functions: [, fused_multi_distinct_count_sum_avg(3: v3), ]\n" +
                "  |  partition by: 1: v1\n" +
                "  |  order by: 2: v2 ASC\n" +
                "  |  window: RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW");
        assertCContains(plan, "  5:Project\n" +
                "  |  <slot 7> : 7: v1\n" +
                "  |  <slot 8> : 8: v2\n" +
                "  |  <slot 9> : 9: v3\n" +
                "  |  <slot 10> : 18: fused_multi_distinct_count_sum_avg.count[false]\n" +
                "  |  <slot 11> : 18: fused_multi_distinct_count_sum_avg.sum[false]\n" +
                "  |  <slot 12> : 18: fused_multi_distinct_count_sum_avg.avg[false]\n" +
                "  |  \n" +
                "  4:SELECT\n" +
                "  |  predicates: 8: v2 IS NOT NULL, 9: v3 IS NOT NULL\n" +
                "  |  \n" +
                "  3:ANALYTIC\n" +
                "  |  functions: [, fused_multi_distinct_count_sum_avg(17: cast), ]\n" +
                "  |  partition by: 7: v1\n" +
                "  |  order by: 8: v2 ASC\n" +
                "  |  window: RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW\n" +
                "  |  \n" +
                "  2:SORT\n" +
                "  |  order by: <slot 7> 7: v1 ASC, <slot 8> 8: v2 ASC\n" +
                "  |  analytic partition by: 7: v1\n" +
                "  |  offset: 0\n" +
                "  |  \n" +
                "  1:Project\n" +
                "  |  <slot 7> : 7: v1\n" +
                "  |  <slot 8> : 8: v2\n" +
                "  |  <slot 9> : 9: v3\n" +
                "  |  <slot 17> : CAST(9: v3 AS DECIMAL128(19,2))");
        assertCContains(plan, "  13:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BROADCAST)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 7: v1 = 1: v1\n" +
                "  |  equal join conjunct: 8: v2 = 2: v2\n" +
                "  |  equal join conjunct: 9: v3 = 3: v3");
    }

    @Test
    public void testMixedFunctionsOverFramedWindow() throws Exception {
        String sql = "select v1,v2,v3,\n" +
                " count(distinct cast(v3 as decimal(19,2)))over(partition by v1 order by v2) as dist_count,\n" +
                " sum(distinct cast(v3 as decimal(19,2)))over(partition by v1 order by v2) as dist_sum,\n" +
                " avg(distinct cast(v3 as decimal(19,2)))over(partition by v1 order by v2) as dist_avg,\n" +
                " sum(cast(v3 as decimal(19,2)))over(partition by v1 order by v2) as sum,\n" +
                " avg(cast(v3 as decimal(19,2)))over(partition by v1 order by v2) as avg,\n" +
                " count(cast(v3 as decimal(19,2)))over(partition by v1 order by v2) as count,\n" +
                " rank()over(partition by v1 order by v2) as rank,\n" +
                " dense_rank()over(partition by v1 order by v2) as d_rank,\n" +
                " row_number()over(partition by v1 order by v2) as r\n" +
                "from t0 ";
        String plan = getFragmentPlan(sql);
        assertCContains(plan, "  5:ANALYTIC\n" +
                "  |  functions: [, row_number(), ]\n" +
                "  |  partition by: 1: v1\n" +
                "  |  order by: 2: v2 ASC\n" +
                "  |  window: ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW\n" +
                "  |  \n" +
                "  4:Project\n" +
                "  |  <slot 1> : 1: v1\n" +
                "  |  <slot 2> : 2: v2\n" +
                "  |  <slot 3> : 3: v3\n" +
                "  |  <slot 4> : 14: fused_multi_distinct_count_sum_avg.count[false]\n" +
                "  |  <slot 5> : 14: fused_multi_distinct_count_sum_avg.sum[false]\n" +
                "  |  <slot 6> : 14: fused_multi_distinct_count_sum_avg.avg[false]\n" +
                "  |  <slot 7> : 7: sum(cast(3: v3 as DECIMAL128(19,2)))\n" +
                "  |  <slot 8> : 8: avg(cast(3: v3 as DECIMAL128(19,2)))\n" +
                "  |  <slot 9> : 9: count(cast(3: v3 as DECIMAL128(19,2)))\n" +
                "  |  <slot 10> : 10: rank()\n" +
                "  |  <slot 11> : 11: dense_rank()\n" +
                "  |  \n" +
                "  3:ANALYTIC\n" +
                "  |  functions: [, sum(CAST(3: v3 AS DECIMAL128(19,2))), ], " +
                "[, avg(CAST(3: v3 AS DECIMAL128(19,2))), ], [, count(CAST(3: v3 AS DECIMAL128(19,2))), ], " +
                "[, rank(), ], [, dense_rank(), ], [, fused_multi_distinct_count_sum_avg(13: cast), ]\n" +
                "  |  partition by: 1: v1\n" +
                "  |  order by: 2: v2 ASC\n" +
                "  |  window: RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW");
        connectContext.getSessionVariable().setOptimizeDistinctAggOverFramedWindow(1);
        plan = getFragmentPlan(sql);
        connectContext.getSessionVariable().setOptimizeDistinctAggOverFramedWindow(0);
        assertCContains(plan, "  16:Project\n" +
                "  |  <slot 1> : 1: v1\n" +
                "  |  <slot 2> : 2: v2\n" +
                "  |  <slot 3> : 3: v3\n" +
                "  |  <slot 4> : 4: count(distinct cast(3: v3 as DECIMAL128(19,2)))\n" +
                "  |  <slot 5> : 5: sum(distinct cast(3: v3 as DECIMAL128(19,2)))\n" +
                "  |  <slot 6> : 6: avg(distinct cast(3: v3 as DECIMAL128(19,2)))\n" +
                "  |  \n" +
                "  15:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BROADCAST)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 15: v1 <=> 1: v1\n" +
                "  |  equal join conjunct: 16: v2 <=> 2: v2\n" +
                "  |  \n" +
                "  |----14:EXCHANGE\n" +
                "  |    \n" +
                "  11:AGGREGATE (update finalize)\n" +
                "  |  group by: 4: count(distinct cast(3: v3 as DECIMAL128(19,2))), " +
                "5: sum(distinct cast(3: v3 as DECIMAL128(19,2))), " +
                "6: avg(distinct cast(3: v3 as DECIMAL128(19,2))), 15: v1, 16: v2\n" +
                "  |  \n" +
                "  10:Project\n" +
                "  |  <slot 4> : 14: fused_multi_distinct_count_sum_avg.count[false]\n" +
                "  |  <slot 5> : 14: fused_multi_distinct_count_sum_avg.sum[false]\n" +
                "  |  <slot 6> : 14: fused_multi_distinct_count_sum_avg.avg[false]\n" +
                "  |  <slot 15> : 15: v1\n" +
                "  |  <slot 16> : 16: v2\n" +
                "  |  \n" +
                "  9:ANALYTIC\n" +
                "  |  functions: [, fused_multi_distinct_count_sum_avg(18: cast), ]\n" +
                "  |  partition by: 15: v1\n" +
                "  |  order by: 16: v2 ASC\n" +
                "  |  window: RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW");

        assertCContains(plan, "  20:ANALYTIC\n" +
                "  |  functions: [, sum(CAST(3: v3 AS DECIMAL128(19,2))), ], " +
                "[, avg(CAST(3: v3 AS DECIMAL128(19,2))), ], " +
                "[, count(CAST(3: v3 AS DECIMAL128(19,2))), ], " +
                "[, rank(), ], [, dense_rank(), ]\n" +
                "  |  partition by: 1: v1\n" +
                "  |  order by: 2: v2 ASC\n" +
                "  |  window: RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW");
    }
}
