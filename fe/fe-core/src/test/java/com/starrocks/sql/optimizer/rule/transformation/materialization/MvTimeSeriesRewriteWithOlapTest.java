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

package com.starrocks.sql.optimizer.rule.transformation.materialization;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.starrocks.sql.plan.PlanTestBase;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.util.List;
import java.util.Map;

import static com.starrocks.sql.optimizer.rule.transformation.materialization.common.AggregateFunctionRollupUtils.REWRITE_ROLLUP_FUNCTION_MAP;
import static com.starrocks.sql.optimizer.rule.transformation.materialization.common.AggregateFunctionRollupUtils.SAFE_REWRITE_ROLLUP_FUNCTION_MAP;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class MvTimeSeriesRewriteWithOlapTest extends MvRewriteTestBase {

    @BeforeClass
    public static void beforeClass() throws Exception {
        MvRewriteTestBase.beforeClass();
        starRocksAssert.withTable("CREATE TABLE t0(\n" +
                " k1 datetime,\n" +
                " v1 INT,\n" +
                " v2 INT)\n" +
                " DUPLICATE KEY(k1)\n" +
                " PARTITION BY RANGE(k1)\n" +
                " (\n" +
                "   PARTITION p1 VALUES LESS THAN ('2024-01-01'),\n" +
                "   PARTITION p2 VALUES LESS THAN ('2024-01-02'),\n" +
                "   PARTITION p3 VALUES LESS THAN ('2024-01-03'),\n" +
                "   PARTITION p4 VALUES LESS THAN ('2024-02-03'),\n" +
                "   PARTITION p5 VALUES LESS THAN ('2024-03-03')\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(k1);");
        cluster.runSql("test", "insert into t0 values (\"2024-01-01 02:00:00\",1,1), " +
                "(\"2024-01-02\",1,1),(\"2024-01-03\",1,2);");
    }

    @AfterClass
    public static void afterClass() throws Exception {
        starRocksAssert.dropTable("t0");
    }

    @Test
    public void testAggregateTimeSeriesRollupWithoutGroupBy() throws Exception {
        String mv1 = "create MATERIALIZED VIEW test_mv1\n" +
                "PARTITION BY (dt)\n" +
                "DISTRIBUTED BY RANDOM\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ")   as select date_trunc('day', k1) as dt, sum(v1) as sum_v1, max(v2) as max_v2 " +
                "from t0 group by date_trunc('day', k1);";
        starRocksAssert.withRefreshedMaterializedView(mv1);
        // date column should be the same with date_trunc('day', ct)
        String query = "select sum(v1), max(v2) from t0 " +
                "where k1 >= '2024-01-01 01:00:00'";
        String plan = getFragmentPlan(query);
        PlanTestBase.assertContains(plan, "     TABLE: test_mv1\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: 13: dt >= '2024-01-01 01:00:00'\n" +
                "     partitions=62/63");
        PlanTestBase.assertContains(plan, "     TABLE: t0\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: 20: k1 >= '2024-01-01 01:00:00', date_trunc('day', 20: k1) < '2024-01-01 01:00:00'\n" +
                "     partitions=1/5");
        starRocksAssert.dropMaterializedView("test_mv1");
    }

    @Test
    public void testAggregateTimeSeriesRollupWithGroupBy() throws Exception {
        String mv1 = "create MATERIALIZED VIEW test_mv1\n" +
                "PARTITION BY (dt)\n" +
                "DISTRIBUTED BY RANDOM\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ")   as select date_trunc('day', k1) as dt, sum(v1) as sum_v1, max(v2) as max_v2 " +
                "from t0 group by date_trunc('day', k1);";
        starRocksAssert.withRefreshedMaterializedView(mv1);
        // date column should be the same with date_trunc('day', ct)
        String query = "select date_trunc('day', k1), sum(v1), max(v2) from t0 " +
                "where k1 >= '2024-01-01 01:00:00' group by date_trunc('day', k1)";
        String plan = getFragmentPlan(query, "Optimizer");
        PlanTestBase.assertContains(plan, "     TABLE: test_mv1\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: 13: dt >= '2024-01-01 01:00:00'\n" +
                "     partitions=62/63");
        PlanTestBase.assertContains(plan, "     TABLE: t0\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: 20: k1 >= '2024-01-01 01:00:00', date_trunc('day', 20: k1) < '2024-01-01 01:00:00'\n" +
                "     partitions=1/5");
        starRocksAssert.dropMaterializedView("test_mv1");
    }

    @Test
    public void testAggregateTimeSeriesRollupWithNestedMV() throws Exception {
        starRocksAssert.withRefreshedMaterializedView("create MATERIALIZED VIEW test_mv1\n" +
                "PARTITION BY (dt)\n" +
                "DISTRIBUTED BY RANDOM\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") as select date_trunc('day', k1) as dt, sum(v1) as sum_v1, max(v2) as max_v2 " +
                "from t0 group by date_trunc('day', k1);");
        starRocksAssert.withRefreshedMaterializedView("create MATERIALIZED VIEW test_mv2\n" +
                "PARTITION BY (dt)\n" +
                "DISTRIBUTED BY RANDOM\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ")   as select date_trunc('month', dt) as dt, sum(sum_v1) as sum_v1, max(max_v2) as max_v2 " +
                "from test_mv1 group by date_trunc('month', dt);");
        // date column should be the same with date_trunc('day', ct)
        String query = "select sum(v1), max(v2) from t0 " +
                "where k1 >= '2024-01-01 01:00:00'";
        String plan = getFragmentPlan(query);
        PlanTestBase.assertContains(plan, "     TABLE: test_mv2\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: 36: dt >= '2024-01-01 01:00:00'\n" +
                "     partitions=3/4");
        PlanTestBase.assertContains(plan, "     TABLE: test_mv1\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: 43: dt >= '2024-01-01 01:00:00', date_trunc('month', 43: dt) < '2024-01-01 01:00:00'\n" +
                "     partitions=31/63");
        PlanTestBase.assertContains(plan, "     TABLE: t0\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: 23: k1 >= '2024-01-01 01:00:00', date_trunc('day', 23: k1) < '2024-01-01 01:00:00'\n" +
                "     partitions=1/5");
        starRocksAssert.dropMaterializedView("test_mv1");
    }

    @Test
    public void testAggregateTimeSeriesWithCountDistinct() throws Exception {
        // one query contains count(distinct) agg function, it can be rewritten.
        starRocksAssert.withRefreshedMaterializedView("create MATERIALIZED VIEW test_mv1\n" +
                "PARTITION BY (dt)\n" +
                "DISTRIBUTED BY RANDOM\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") as select date_trunc('day', k1) as dt, array_agg_distinct(v1) as sum_v1 " +
                "from t0 group by date_trunc('day', k1);");
        String query = "select count(distinct v1) " +
                "from t0 " +
                "where k1 >= '2024-01-01 01:00:00'";
        String plan = getFragmentPlan(query);
        System.out.println(plan);
        PlanTestBase.assertContains(plan, "     TABLE: test_mv1\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: 9: dt >= '2024-01-01 01:00:00'\n" +
                "     partitions=62/63");
        PlanTestBase.assertContains(plan, "     TABLE: t0\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: 14: k1 >= '2024-01-01 01:00:00', date_trunc('day', 14: k1) < '2024-01-01 01:00:00'\n" +
                "     partitions=1/5");
        PlanTestBase.assertContains(plan, "  16:AGGREGATE (update serialize)\n" +
                "  |  output: array_unique_agg(18: count)\n" +
                "  |  group by: \n" +
                "  |  \n" +
                "  0:UNION\n" +
                "  |  \n" +
                "  |----15:EXCHANGE\n" +
                "  |    \n" +
                "  6:EXCHANGE");
        starRocksAssert.dropMaterializedView("test_mv1");
    }

    @Test
    public void testAggTimeSeriesWithRollupFunctionsOneByOne() throws Exception {
        String mvAggArg = "v1";
        String queryAggArg = "v1";
        for (Map.Entry<String, String> e : REWRITE_ROLLUP_FUNCTION_MAP.entrySet()) {
            String funcName = e.getKey();
            String mvAggFunc = getAggFunction(funcName, mvAggArg);
            String queryAggFunc = getAggFunction(funcName, queryAggArg);
            starRocksAssert.withRefreshedMaterializedView(String.format("create MATERIALIZED VIEW test_mv1\n" +
                    "PARTITION BY (dt)\n" +
                    "DISTRIBUTED BY RANDOM\n" +
                    "as select date_trunc('day', k1) as dt, %s as agg_v1 " +
                    "from t0 group by date_trunc('day', k1);", mvAggFunc));
            {
                String query = String.format("select %s " +
                        "from t0 " +
                        "where k1 >= '2024-01-01 01:00:00'", queryAggFunc);
                String plan = getFragmentPlan(query);
                PlanTestBase.assertContains(plan, "     TABLE: test_mv1");
                PlanTestBase.assertContains(plan, "     TABLE: t0");
            }
            {
                String query = String.format("select date_trunc('day', k1), %s from t0 " +
                        "where k1 >= '2024-01-01 01:00:00' group by date_trunc('day', k1)", queryAggFunc);
                String plan = getFragmentPlan(query);
                PlanTestBase.assertContains(plan, "     TABLE: test_mv1");
                PlanTestBase.assertContains(plan, "     TABLE: t0");
            }
            starRocksAssert.dropMaterializedView("test_mv1");
        }
    }

    @Test
    public void testAggTimeSeriesWithMultiRollupFunctions() throws Exception {
        // one query contains multi agg functions, all can be rewritten.
        String aggArg = "v1";
        List<String> aggFuncs = Lists.newArrayList();
        for (Map.Entry<String, String> e : SAFE_REWRITE_ROLLUP_FUNCTION_MAP.entrySet()) {
            String funcName = e.getKey();
            String mvAggFunc = getAggFunction(funcName, aggArg);
            aggFuncs.add(mvAggFunc);
        }
        String agg = Joiner.on(", ").join(aggFuncs);
        starRocksAssert.withRefreshedMaterializedView(String.format("create MATERIALIZED VIEW test_mv1\n" +
                "PARTITION BY (dt)\n" +
                "DISTRIBUTED BY RANDOM\n" +
                "as select date_trunc('day', k1) as dt, %s\n" +
                "from t0 group by date_trunc('day', k1);", agg));
        {
            String query = String.format("select %s " +
                    "from t0 " +
                    "where k1 >= '2024-01-01 01:00:00'", agg);
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "     TABLE: test_mv1");
            PlanTestBase.assertContains(plan, "     TABLE: t0");
        }
        {
            String query = String.format("select date_trunc('day', k1), %s from t0 " +
                    "where k1 >= '2024-01-01 01:00:00' group by date_trunc('day', k1)", agg);
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "     TABLE: test_mv1");
            PlanTestBase.assertContains(plan, "     TABLE: t0");
        }
        starRocksAssert.dropMaterializedView("test_mv1");
    }

    @Test
    public void testAggTimeSeriesWithMultiRepeatedRollupFunctions() throws Exception {
        // one query contains multi same agg functions, all can be rewritten.
        String aggArg = "v1";
        List<String> aggFuncs = Lists.newArrayList();
        for (Map.Entry<String, String> e : SAFE_REWRITE_ROLLUP_FUNCTION_MAP.entrySet()) {
            String funcName = e.getKey();
            String mvAggFunc = getAggFunction(funcName, aggArg);
            aggFuncs.add(mvAggFunc);
        }

        int repeatTimes = 4;
        for (String aggFunc : aggFuncs) {
            if (aggFunc.contains("bitmap_union")) {
                continue;
            }
            List<String> repeatAggs = Lists.newArrayList();
            for (int i = 0; i < repeatTimes; i++) {
                repeatAggs.add(String.format("%s as agg%s", aggFunc, i));
            }
            String agg = Joiner.on(", ").join(repeatAggs);
            starRocksAssert.withRefreshedMaterializedView(String.format("create MATERIALIZED VIEW test_mv1\n" +
                    "PARTITION BY (dt)\n" +
                    "DISTRIBUTED BY RANDOM\n" +
                    "as select date_trunc('day', k1) as dt, %s " +
                    "from t0 group by date_trunc('day', k1);", agg));
            {
                String query = String.format("select %s from t0 where k1 >= '2024-01-01 01:00:00'", agg);
                String plan = getFragmentPlan(query);
                PlanTestBase.assertContains(plan, "     TABLE: test_mv1");
                PlanTestBase.assertContains(plan, "     TABLE: t0");
            }
            {
                String query = String.format("select date_trunc('day', k1), %s from t0 " +
                        "where k1 >= '2024-01-01 01:00:00' group by date_trunc('day', k1)", agg);
                String plan = getFragmentPlan(query);
                PlanTestBase.assertContains(plan, "     TABLE: test_mv1");
                PlanTestBase.assertContains(plan, "     TABLE: t0");
            }
            starRocksAssert.dropMaterializedView("test_mv1");
        }
    }
}
