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
import com.starrocks.catalog.FunctionSet;
import com.starrocks.sql.plan.PlanTestBase;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.util.List;
import java.util.Map;

import static com.starrocks.sql.optimizer.rule.transformation.materialization.common.AggregateFunctionRollupUtils.SAFE_REWRITE_ROLLUP_FUNCTION_MAP;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class MvTimeSeriesRewriteWithOlapTest extends MVTestBase {

    @BeforeClass
    public static void beforeClass() throws Exception {
        MVTestBase.beforeClass();

        connectContext.getSessionVariable().setMaterializedViewRewriteMode("force");
        connectContext.getSessionVariable().setEnableLowCardinalityOptimize(false);
        UtFrameUtils.mockTimelinessForAsyncMVTest(connectContext);
        UtFrameUtils.mockLogicalScanIsEmptyOutputRows(false);

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

        starRocksAssert.withTable("CREATE TABLE `t1` (\n" +
                "    `k1`  date not null, \n" +
                "    `k2`  datetime not null, \n" +
                "    `k3`  char(20), \n" +
                "    `k4`  varchar(20), \n" +
                "    `k5`  boolean, \n" +
                "    `k6`  tinyint, \n" +
                "    `k7`  smallint, \n" +
                "    `k8`  int, \n" +
                "    `k9`  bigint, \n" +
                "    `k10` largeint, \n" +
                "    `k11` float, \n" +
                "    `k12` double, \n" +
                "    `k13` decimal(27,9) ) \n" +
                "DUPLICATE KEY(`k1`, `k2`, `k3`, `k4`, `k5`) \n" +
                "PARTITION BY RANGE(`k2`) \n" +
                "(\n" +
                "PARTITION p20201022 VALUES [(\"2020-10-22\"), (\"2020-10-23\")), \n" +
                "PARTITION p20201023 VALUES [(\"2020-10-23\"), (\"2020-10-24\")), \n" +
                "PARTITION p20201024 VALUES [(\"2020-10-24\"), (\"2020-10-25\"))\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(`k1`, `k2`, `k3`) BUCKETS 3 \n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") ;");
        cluster.runSql("test", "INSERT INTO t1 VALUES " +
                "('2020-10-22','2020-10-22 12:12:12','k3','k4',0,1,2,2,4,5,1.1,1.12,2.889),\n" +
                "('2020-10-23','2020-10-23 12:12:12','k3','k4',0,1,2,3,4,5,1.1,1.12,2.889),\n" +
                "('2020-10-24','2020-10-24 12:12:12','k3','k4',0,1,2,3,4,5,1.1,1.12,2.889);");

        starRocksAssert.withTable("CREATE TABLE t2(\n" +
                " ts datetime,\n" +
                " v1 INT,\n" +
                " v2 INT)\n" +
                " DUPLICATE KEY(ts)\n" +
                " PARTITION BY date_trunc('day', ts)\n" +
                "DISTRIBUTED BY HASH(ts);");
        cluster.runSql("test", "INSERT INTO t2 VALUES \n" +
                "  ('2020-01-22 12:12:12', 0,1),\n" +
                "  ('2020-02-23 12:12:12',1,1),\n" +
                "  ('2020-03-24 12:12:12',1,2),\n" +
                "  ('2020-04-25 12:12:12',3,3),\n" +
                "  ('2020-05-22 12:12:12', 0,1),\n" +
                "  ('2020-06-23 12:12:12',1,1),\n" +
                "  ('2020-07-24 12:12:12',1,2),\n" +
                "  ('2020-08-24 12:12:12',1,2),\n" +
                "  ('2020-09-24 12:12:12',1,2),\n" +
                "  ('2020-10-25 12:12:12',3,3);");
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
                "     PREDICATES: date_trunc('day', 22: k1) < '2024-01-01 01:00:00', 22: k1 >= '2024-01-01 01:00:00'\n" +
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
        {
            // date column should be the same with date_trunc('day', ct)
            String query = "select date_trunc('day', k1), sum(v1), max(v2) from t0 " +
                    "where k1 >= '2024-01-01 01:00:00' group by date_trunc('day', k1)";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "     TABLE: test_mv1\n" +
                    "     PREAGGREGATION: ON\n" +
                    "     PREDICATES: 13: dt >= '2024-01-01 01:00:00'\n" +
                    "     partitions=62/63");
            PlanTestBase.assertContains(plan, "     TABLE: t0\n" +
                    "     PREAGGREGATION: ON\n" +
                    "     PREDICATES: date_trunc('day', 24: k1) < '2024-01-01 01:00:00', 24: k1 >= '2024-01-01 01:00:00'\n" +
                    "     partitions=1/5");
        }
        {
            // date column should be the same with date_trunc('day', ct)
            String query = "select date_trunc('day', k1), sum(v1), max(v2) from t0 " +
                    "where k1 <= '2024-01-01 01:00:00' group by date_trunc('day', k1)";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "     TABLE: test_mv1\n" +
                    "     PREAGGREGATION: ON\n" +
                    "     PREDICATES: 14: dt <= '2023-12-31 01:00:00'");
            PlanTestBase.assertContains(plan, "     TABLE: t0\n" +
                    "     PREAGGREGATION: ON\n" +
                    "     PREDICATES: date_trunc('day', 24: k1) > '2023-12-31 01:00:00', 24: k1 <= '2024-01-01 01:00:00'\n" +
                    "     partitions=2/5");
        }
        {
            // date column should be the same with date_trunc('day', ct)
            String query = "select date_trunc('day', k1), sum(v1), max(v2) from t0 " +
                    "where k1 <= '2024-02-01 01:00:00' and k1 >= '2024-01-01 01:00:00' group by date_trunc('day', k1)";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "     TABLE: test_mv1\n" +
                    "     PREAGGREGATION: ON\n" +
                    "     PREDICATES: 14: dt >= '2024-01-01 01:00:00', 14: dt <= '2024-01-31 01:00:00'\n" +
                    "     partitions=31/63");
            PlanTestBase.assertContains(plan, "     TABLE: t0\n" +
                    "     PREAGGREGATION: ON\n" +
                    "     PREDICATES: (date_trunc('day', 24: k1) > '2024-01-31 01:00:00') OR " +
                    "(date_trunc('day', 24: k1) < '2024-01-01 01:00:00'), 24: k1 <= '2024-02-01 01:00:00', " +
                    "24: k1 >= '2024-01-01 01:00:00'\n" +
                    "     partitions=2/5");
        }
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
        String query = "select sum(v1), max(v2) from t0 where k1 >= '2024-01-01 01:00:00'";
        String plan = getFragmentPlan(query);
        PlanTestBase.assertContains(plan, "     TABLE: test_mv2\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: 36: dt >= '2024-01-01 01:00:00'\n" +
                "     partitions=3/4");
        PlanTestBase.assertContains(plan, "     TABLE: test_mv1\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: date_trunc('month', 45: dt) < '2024-01-01 01:00:00', 45: dt >= '2024-01-01 01:00:00'\n" +
                "     partitions=31/63");
        PlanTestBase.assertContains(plan, "     TABLE: t0\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: date_trunc('day', 25: k1) < '2024-01-01 01:00:00', 25: k1 >= '2024-01-01 01:00:00'\n" +
                "     partitions=1/5");
        starRocksAssert.dropMaterializedView("test_mv1");
        starRocksAssert.dropMaterializedView("test_mv2");
    }

    @Test
    public void testAggregateTimeSeriesRollupWithMultiMVs() throws Exception {
        starRocksAssert.withRefreshedMaterializedView("CREATE MATERIALIZED VIEW IF NOT EXISTS test_mv1\n" +
                "PARTITION BY (dt)\n" +
                "REFRESH DEFERRED MANUAL\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") as select date_trunc('hour', ts) as dt, sum(v1) as sum_v1, sum(v2) as sum_v2\n" +
                "from t2 group by date_trunc('hour', ts);");
        starRocksAssert.withRefreshedMaterializedView("CREATE MATERIALIZED VIEW IF NOT EXISTS test_mv2\n" +
                "PARTITION BY (dt)\n" +
                "REFRESH DEFERRED MANUAL\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") as select date_trunc('day', ts) as dt, sum(v1) as sum_v1, sum(v2) as sum_v2\n" +
                "from t2 group by date_trunc('day', ts);");
        starRocksAssert.withRefreshedMaterializedView("CREATE MATERIALIZED VIEW IF NOT EXISTS test_mv3\n" +
                "PARTITION BY (dt)\n" +
                "REFRESH DEFERRED MANUAL\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") as select date_trunc('month', ts) as dt, sum(v1) as sum_v1, sum(v2) as sum_v2\n" +
                "from t2 group by date_trunc('month', ts);");

        // date column should be the same with date_trunc('day', ct)
        String query = "select sum(v1), sum(v2) from t2 where ts >= '2020-03-23 12:12:00' order by 1;";
        String plan = getFragmentPlan(query);
        PlanTestBase.assertContains(plan, "     TABLE: test_mv3\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: 19: dt >= '2020-03-23 12:12:00'");
        PlanTestBase.assertContains(plan, "     TABLE: test_mv2\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: 43: dt >= '2020-03-23 12:12:00', date_trunc('month', 43: dt) < '2020-03-22 12:12:00'\n");
        PlanTestBase.assertContains(plan, "     TABLE: t2\n" +
                "     PREAGGREGATION: ON");
        starRocksAssert.dropMaterializedView("test_mv1");
        starRocksAssert.dropMaterializedView("test_mv2");
        starRocksAssert.dropMaterializedView("test_mv3");
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
        PlanTestBase.assertContains(plan, "     TABLE: test_mv1\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: 9: dt >= '2024-01-01 01:00:00'\n" +
                "     partitions=62/63");
        PlanTestBase.assertContains(plan, "     TABLE: t0\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: date_trunc('day', 14: k1) < '2024-01-01 01:00:00', 14: k1 >= '2024-01-01 01:00:00'\n" +
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

    private void testAggTimeSeriesWithRollupFunctionsOneByOne(String funcName) throws Exception {
        String mvAggArg = "v1";
        String queryAggArg = "v1";
        String mvAggFunc = getAggFunction(funcName, mvAggArg);
        String queryAggFunc = getAggFunction(funcName, queryAggArg);
        String mvName = "test_mv0";
        starRocksAssert.withMaterializedView(String.format("create MATERIALIZED VIEW %s\n" +
                "PARTITION BY (dt)\n" +
                "DISTRIBUTED BY RANDOM\n" +
                "as select date_trunc('day', k1) as dt, %s as agg_v1 " +
                "from t0 group by date_trunc('day', k1);", mvName, mvAggFunc));
        {
            String query = String.format("select %s " +
                    "from t0 " +
                    "where k1 >= '2024-01-01 01:00:00'", queryAggFunc);
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "     TABLE: " + mvName);
            PlanTestBase.assertContains(plan, "     TABLE: t0");
        }
        {
            String query = String.format("select date_trunc('day', k1), %s from t0 " +
                    "where k1 >= '2024-01-01 01:00:00' group by date_trunc('day', k1)", queryAggFunc);
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "     TABLE: " + mvName);
            PlanTestBase.assertContains(plan, "     TABLE: t0");
        }
        starRocksAssert.dropMaterializedView(mvName);
    }

    @Test
    public void testAggTimeSeriesWithRollupFunctionsSum() throws Exception {
        testAggTimeSeriesWithRollupFunctionsOneByOne(FunctionSet.SUM);
    }

    @Test
    public void testAggTimeSeriesWithRollupFunctionsMax() throws Exception {
        testAggTimeSeriesWithRollupFunctionsOneByOne(FunctionSet.MAX);
    }

    @Test
    public void testAggTimeSeriesWithRollupFunctionsMin() throws Exception {
        testAggTimeSeriesWithRollupFunctionsOneByOne(FunctionSet.MIN);
    }

    @Test
    public void testAggTimeSeriesWithRollupFunctionsBitmapUnion() throws Exception {
        testAggTimeSeriesWithRollupFunctionsOneByOne(FunctionSet.BITMAP_UNION);
    }

    @Test
    public void testAggTimeSeriesWithRollupFunctionsHLL() throws Exception {
        testAggTimeSeriesWithRollupFunctionsOneByOne(FunctionSet.HLL_UNION);
    }

    @Test
    public void testAggTimeSeriesWithRollupFunctionsPERCENTILE_UNION() throws Exception {
        testAggTimeSeriesWithRollupFunctionsOneByOne(FunctionSet.PERCENTILE_UNION);
    }

    @Test
    public void testAggTimeSeriesWithRollupFunctionsANY_VALUE() throws Exception {
        testAggTimeSeriesWithRollupFunctionsOneByOne(FunctionSet.ANY_VALUE);
    }

    @Test
    public void testAggTimeSeriesWithRollupFunctionsBITMAP_AGG() throws Exception {
        testAggTimeSeriesWithRollupFunctionsOneByOne(FunctionSet.BITMAP_AGG);
    }

    @Test
    public void testAggTimeSeriesWithRollupFunctionsARRAY_AGG_DISTINCT() throws Exception {
        testAggTimeSeriesWithRollupFunctionsOneByOne(FunctionSet.ARRAY_AGG_DISTINCT);
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
            starRocksAssert.withMaterializedView(String.format("create MATERIALIZED VIEW test_mv1\n" +
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

    @Test
    public void testAggregateTimeSeriesRollupWithGroupBy1() throws Exception {
        String mv1 = "CREATE MATERIALIZED VIEW IF NOT EXISTS test_mv1\n" +
                "PARTITION BY `dt`\n" +
                "REFRESH DEFERRED MANUAL\n" +
                "as \n" +
                "select k1, date_trunc('day', k2) as dt, sum(k6), sum(k7), sum(k8), count(1) as cnt from t1 " +
                "group by k1, date_trunc('day', k2);";
        starRocksAssert.withRefreshedMaterializedView(mv1);
        {
            // date column should be the same with date_trunc('day', ct)
            String query = "select k1, date_trunc('year', k2) as dt, sum(k6), sum(k7), sum(k8), count(1) as cnt from t1 " +
                    "where k2 > '2020-10-23 12:12:00' and k2 < '2020-10-24 12:12:00' " +
                    "group by k1, date_trunc('year', k2) order by 1;";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertNotContains(plan, "test_mv1");
        }
        starRocksAssert.dropMaterializedView("test_mv1");
    }
}
