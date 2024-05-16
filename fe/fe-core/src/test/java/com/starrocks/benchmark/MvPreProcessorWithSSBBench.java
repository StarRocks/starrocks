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

package com.starrocks.benchmark;

import com.carrotsearch.junitbenchmarks.BenchmarkOptions;
import com.carrotsearch.junitbenchmarks.BenchmarkRule;
import com.google.common.collect.Lists;
import com.starrocks.common.FeConstants;
import com.starrocks.planner.MaterializedViewTestBase;
import com.starrocks.sql.plan.PlanTestBase;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

@Ignore
public class MvPreProcessorWithSSBBench extends MaterializedViewTestBase {

    private static final int MV_NUMS = 1000;
    private static final int BENCHMARK_RUNS = 10;

    @Rule
    public TestRule mvPartitionCompensateBench = new BenchmarkRule();

    @BeforeClass
    public static void beforeClass() throws Exception {
        FeConstants.USE_MOCK_DICT_MANAGER = true;
        MaterializedViewTestBase.beforeClass();

        starRocksAssert.useDatabase(MATERIALIZED_DB_NAME);

        // create SSB tables
        // put lineorder last because it depends on other tables for foreign key constraints
        createTables("sql/ssb/", Lists.newArrayList("customer", "dates", "supplier", "part", "lineorder"));

        // create lineorder_flat_mv
        for (int i = 0; i < MV_NUMS; i++) {
            String mv = String.format("CREATE MATERIALIZED VIEW lineorder_flat_mv_%s\n" +
                    "DISTRIBUTED BY RANDOM\n" +
                    "REFRESH DEFERRED MANUAL\n" +
                    "AS SELECT\n" +
                    "       l.LO_ORDERKEY AS LO_ORDERKEY,\n" +
                    "       l.LO_LINENUMBER AS LO_LINENUMBER,\n" +
                    "       l.LO_CUSTKEY AS LO_CUSTKEY,\n" +
                    "       l.LO_PARTKEY AS LO_PARTKEY,\n" +
                    "       l.LO_SUPPKEY AS LO_SUPPKEY,\n" +
                    "       l.LO_ORDERDATE AS LO_ORDERDATE,\n" +
                    "       l.LO_ORDERPRIORITY AS LO_ORDERPRIORITY,\n" +
                    "       l.LO_SHIPPRIORITY AS LO_SHIPPRIORITY,\n" +
                    "       l.LO_QUANTITY AS LO_QUANTITY,\n" +
                    "       l.LO_EXTENDEDPRICE AS LO_EXTENDEDPRICE,\n" +
                    "       l.LO_ORDTOTALPRICE AS LO_ORDTOTALPRICE,\n" +
                    "       l.LO_DISCOUNT AS LO_DISCOUNT,\n" +
                    "       l.LO_REVENUE AS LO_REVENUE,\n" +
                    "       l.LO_SUPPLYCOST AS LO_SUPPLYCOST,\n" +
                    "       l.LO_TAX AS LO_TAX,\n" +
                    "       l.LO_COMMITDATE AS LO_COMMITDATE,\n" +
                    "       l.LO_SHIPMODE AS LO_SHIPMODE,\n" +
                    "       c.C_NAME AS C_NAME,\n" +
                    "       c.C_ADDRESS AS C_ADDRESS,\n" +
                    "       c.C_CITY AS C_CITY,\n" +
                    "       c.C_NATION AS C_NATION,\n" +
                    "       c.C_REGION AS C_REGION,\n" +
                    "       c.C_PHONE AS C_PHONE,\n" +
                    "       c.C_MKTSEGMENT AS C_MKTSEGMENT,\n" +
                    "       s.S_NAME AS S_NAME,\n" +
                    "       s.S_ADDRESS AS S_ADDRESS,\n" +
                    "       s.S_CITY AS S_CITY,\n" +
                    "       s.S_NATION AS S_NATION,\n" +
                    "       s.S_REGION AS S_REGION,\n" +
                    "       s.S_PHONE AS S_PHONE,\n" +
                    "       p.P_NAME AS P_NAME,\n" +
                    "       p.P_MFGR AS P_MFGR,\n" +
                    "       p.P_CATEGORY AS P_CATEGORY,\n" +
                    "       p.P_BRAND AS P_BRAND,\n" +
                    "       p.P_COLOR AS P_COLOR,\n" +
                    "       p.P_TYPE AS P_TYPE,\n" +
                    "       p.P_SIZE AS P_SIZE,\n" +
                    "       p.P_CONTAINER AS P_CONTAINER,\n" +
                    "       d.d_date AS d_date,\n" +
                    "       d.d_dayofweek AS d_dayofweek,\n" +
                    "       d.d_month AS d_month,\n" +
                    "       d.d_year AS d_year,\n" +
                    "       d.d_yearmonthnum AS d_yearmonthnum,\n" +
                    "       d.d_yearmonth AS d_yearmonth,\n" +
                    "       d.d_daynuminweek AS d_daynuminweek,\n" +
                    "       d.d_daynuminmonth AS d_daynuminmonth,\n" +
                    "       d.d_daynuminyear AS d_daynuminyear,\n" +
                    "       d.d_monthnuminyear AS d_monthnuminyear,\n" +
                    "       d.d_weeknuminyear AS d_weeknuminyear,\n" +
                    "       d.d_sellingseason AS d_sellingseason,\n" +
                    "       d.d_lastdayinweekfl AS d_lastdayinweekfl,\n" +
                    "       d.d_lastdayinmonthfl AS d_lastdayinmonthfl,\n" +
                    "       d.d_holidayfl AS d_holidayfl,\n" +
                    "       d.d_weekdayfl AS d_weekdayfl\n" +
                    "   FROM lineorder AS l\n" +
                    "            INNER JOIN customer AS c ON c.C_CUSTKEY = l.LO_CUSTKEY\n" +
                    "            INNER JOIN supplier AS s ON s.S_SUPPKEY = l.LO_SUPPKEY\n" +
                    "            INNER JOIN part AS p ON p.P_PARTKEY = l.LO_PARTKEY\n" +
                    "            INNER JOIN dates AS d ON l.lo_orderdate = d.d_datekey;\n", i);
            System.out.println("create table :" + i);
            starRocksAssert.withMaterializedView(mv);
        }

        // no use plan cache to test mv prepare cost
        connectContext.getSessionVariable().setEnableMaterializedViewPlanCache(false);
    }

    @AfterClass
    public static void afterClass() {
        for (int i = 0; i < MV_NUMS; i++) {
            String mv = String.format("lineorder_flat_mv_%s", i);
            try {
                starRocksAssert.dropMaterializedView(mv);
            } catch (Exception e) {
                // ignore exception
            }
        }
    }

    @Test
    @BenchmarkOptions(warmupRounds = 1, benchmarkRounds = BENCHMARK_RUNS)
    // MvPreProcessorWithSSBBench.testPartitionPredicate: [measured 10 out of 11 rounds, threads: 1 (sequential)]
    // round: 0.32 [+- 0.08], round.block: 0.00 [+- 0.00], round.gc: 0.00 [+- 0.00], GC.calls: 4, GC.time: 0.05,
    // time.total: 4.40, time.warmup: 1.21, time.bench: 3.20
    public void testPartitionPredicate1() throws Exception {
        String query = "select sum(LO_EXTENDEDPRICE * LO_DISCOUNT) AS revenue\n" +
                "from lineorder\n" +
                "join dates on lo_orderdate = d_datekey\n" +
                "where weekofyear(LO_ORDERDATE) = 6 AND LO_ORDERDATE >= 19940101 and LO_ORDERDATE <= 19941231\n" +
                "and lo_discount between 5 and 7\n" +
                "and lo_quantity between 26 and 35;";
        String plan = getFragmentPlan(query);
        PlanTestBase.assertContains(plan, "lineorder_flat_mv");
    }

    @Test
    @BenchmarkOptions(warmupRounds = 1, benchmarkRounds = BENCHMARK_RUNS)
    // MvPreProcessorWithSSBBench.testPartitionPredicate2: [measured 10 out of 11 rounds, threads: 1 (sequential)]
    // round: 2.93 [+- 0.10], round.block: 0.00 [+- 0.00], round.gc: 0.00 [+- 0.00], GC.calls: 51, GC.time: 1.21,
    // time.total: 33.49, time.warmup: 4.16, time.bench: 29.33
    public void testPartitionPredicate2() throws Exception {
        String query = "select sum(LO_EXTENDEDPRICE * LO_DISCOUNT) AS revenue\n" +
                "from lineorder\n" +
                "join dates on lo_orderdate = d_datekey\n" +
                "where weekofyear(LO_ORDERDATE) = 6 AND LO_ORDERDATE >= 19940101 and LO_ORDERDATE <= 19941231\n" +
                "and lo_discount between 5 and 7\n" +
                "and lo_quantity between 26 and 35;";
        int oldVal = connectContext.getSessionVariable().getCboMaterializedViewRewriteRelatedMVsLimit();
        connectContext.getSessionVariable().setCboMaterializedViewRewriteRelatedMVsLimit(1000);
        String plan = getFragmentPlan(query);
        PlanTestBase.assertContains(plan, "lineorder_flat_mv");
        connectContext.getSessionVariable().setCboMaterializedViewRewriteRelatedMVsLimit(oldVal);
    }
}
