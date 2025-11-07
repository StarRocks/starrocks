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

import com.starrocks.utframe.StarRocksAssert;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

public class DistinctAggregationOverWindowTest extends PlanTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        StarRocksAssert starRocksAssert = new StarRocksAssert(connectContext);
        starRocksAssert.withTable("CREATE TABLE `s1` (    \n" +
                "  `v1` bigint(20) NULL COMMENT \"\",    \n" +
                "  `v2` int(11) NULL COMMENT \"\",    \n" +
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
}
