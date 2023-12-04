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
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class AggregatePushDownTest extends PlanTestBase {
    @BeforeClass
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        FeConstants.runningUnitTest = true;
        starRocksAssert.withDatabase("test_window_db");
        starRocksAssert.withTable("CREATE TABLE if not exists trans\n" +
                "(\n" +
                "region VARCHAR(128)  NULL,\n" +
                "order_date DATE NOT NULL,\n" +
                "income DECIMAL128(10, 2) NOT NULL\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`region`, `order_date`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`region`, `order_date`) BUCKETS 128\n" +
                "PROPERTIES(\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\"\n" +
                ")");
        connectContext.getSessionVariable().setNewPlanerAggStage(1);
        connectContext.getSessionVariable().setCboPushDownAggregateMode(1);
        connectContext.getSessionVariable().setEnableRewriteSumByAssociativeRule(false);
    }

    @Test
    public void testPushDown() {
        runFileUnitTest("optimized-plan/agg-pushdown");
    }

    @Test
    public void testPushDownPreAgg() {
        connectContext.getSessionVariable().setCboPushDownAggregate("local");
        try {
            runFileUnitTest("optimized-plan/preagg-pushdown");
        } finally {
            connectContext.getSessionVariable().setCboPushDownAggregate("global");
        }
    }

    @Test
    public void testPushDownDistinctAggBelowWindow()
            throws Exception {
        String q1 = "SELECT DISTINCT \n" +
                "  COALESCE(region, 'Other') AS region, \n" +
                "  order_date, \n" +
                "  SUM(income) OVER ( PARTITION BY  COALESCE(region, 'Other'), " +
                "   order_date) AS gp_income,  \n" +
                "  SUM(income) OVER ( PARTITION BY  COALESCE(region, 'Other'), " +
                "   MONTH(order_date) ORDER BY order_date) AS gp_income_MTD,\n" +
                "  SUM(income) OVER ( PARTITION BY  COALESCE(region, 'Other'), " +
                "   YEAR (order_date), QUARTER(order_date) ORDER BY order_date) AS gp_income_QTD,\n" +
                "  SUM(income) OVER ( PARTITION BY  COALESCE(region, 'Other'), " +
                "   YEAR (order_date) ORDER BY order_date) AS gp_income_YTD  \n" +
                "FROM  trans\n" +
                "where month(order_date)=1\n" +
                "order by region, order_date";
        String plan = UtFrameUtils.getVerboseFragmentPlan(connectContext, q1);
        Assert.assertTrue(plan, plan.contains("  1:AGGREGATE (update finalize)\n" +
                "  |  aggregate: sum[([3: income, DECIMAL128(10,2), false]); args: DECIMAL128; " +
                "result: DECIMAL128(38,2); args nullable: false; result nullable: true]\n" +
                "  |  group by: [1: region, VARCHAR, true], [2: order_date, DATE, false]\n"));

        Assert.assertTrue(plan.contains("  0:OlapScanNode\n" +
                "     table: trans, rollup: trans\n" +
                "     preAggregation: on\n" +
                "     Predicates: month[([2: order_date, DATE, false]); args: DATE; result: TINYINT; " +
                "args nullable: false; result nullable: false] = 1\n" +
                ""));
    }

    @Test
    public void testPushDownDistinctAggBelowWindow_1() throws Exception {
        // unsupported window func ref cols from partition by cols
        String sql = "select distinct t1d from (select *, sum(t1e) over (partition by t1d) as cnt from test_all_type ) " +
                "t where cnt > 1 limit 10;";

        String plan = getFragmentPlan(sql);
        assertContains(plan, "5:SELECT\n" +
                "  |  predicates: 11: sum(5: t1e) > 1.0\n" +
                "  |  \n" +
                "  4:ANALYTIC\n" +
                "  |  functions: [, sum(12: sum), ]\n" +
                "  |  partition by: 4: t1d\n" +
                "  |  \n" +
                "  3:SORT\n" +
                "  |  order by: <slot 4> 4: t1d ASC\n" +
                "  |  offset: 0\n" +
                "  |  \n" +
                "  2:AGGREGATE (update finalize)\n" +
                "  |  output: sum(5: t1e)\n" +
                "  |  group by: 4: t1d\n" +
                "  |  \n" +
                "  1:EXCHANGE");
    }

    @Test
    public void testPushDownDistinctAggBelowWindow_2() throws Exception {
        // unsupported window func ref cols from partition by cols
        String sql = "select distinct t1d from (select *, sum(t1d) over (partition by t1d, t1e) as cnt from " +
                "test_all_type ) t where cnt > 1 limit 10;";

        String plan = getFragmentPlan(sql);
        assertContains(plan, "4:ANALYTIC\n" +
                "  |  functions: [, sum(12: sum), ]\n" +
                "  |  partition by: 4: t1d, 5: t1e\n" +
                "  |  \n" +
                "  3:SORT\n" +
                "  |  order by: <slot 4> 4: t1d ASC, <slot 5> 5: t1e ASC\n" +
                "  |  offset: 0\n" +
                "  |  \n" +
                "  2:AGGREGATE (update finalize)\n" +
                "  |  output: sum(4: t1d)\n" +
                "  |  group by: 4: t1d, 5: t1e\n" +
                "  |  \n" +
                "  1:EXCHANGE");
    }

    @Test
    public void testPushDownDistinctAggBelowWindow_3() throws Exception {
        // unsupported window func ref cols from partition by cols
        String sql = "select distinct t1c from (select *, sum(t1d) over (partition by t1e order by t1d) as cnt from " +
                "test_all_type ) t where cnt > 1 limit 10;";

        String plan = getFragmentPlan(sql);
        assertContains(plan, "2:AGGREGATE (update finalize)\n" +
                "  |  output: sum(4: t1d)\n" +
                "  |  group by: 3: t1c, 4: t1d, 5: t1e\n" +
                "  |  \n" +
                "  1:EXCHANGE");
    }

    @Test
    public void testNotPushDownDistinctAggBelowWindow_1() throws Exception {
        // unsupported count function
        String sql = "select distinct t1d from (select *, count(1) over (partition by t1d) as cnt from test_all_type ) " +
                "t where cnt > 1 limit 10;";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "4:SELECT\n" +
                "  |  predicates: 11: count(1) > 1\n" +
                "  |  \n" +
                "  3:ANALYTIC\n" +
                "  |  functions: [, count(1), ]\n" +
                "  |  partition by: 4: t1d\n" +
                "  |  \n" +
                "  2:SORT\n" +
                "  |  order by: <slot 4> 4: t1d ASC\n" +
                "  |  offset: 0\n" +
                "  |  \n" +
                "  1:EXCHANGE");
    }

    @Test
    public void testPruneColsAfterPushdownAgg_1() throws Exception {
        String sql = "select L_PARTKEY from lineitem_partition where L_SHIPDATE >= '1992-01-01' and L_SHIPDATE < '1993-01-01'";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "1:Project\n" +
                "  |  <slot 2> : 2: L_PARTKEY\n" +
                "  |  \n" +
                "  0:OlapScanNode\n" +
                "     TABLE: lineitem_partition\n" +
                "     PREAGGREGATION: ON\n" +
                "     partitions=1/7");
    }

    @Test
    public void testPruneColsAfterPushdownAgg_2() throws Exception {
        String sql = "select max(L_ORDERKEY), sum(2), L_PARTKEY from lineitem_partition " +
                "join t0 on L_PARTKEY = v1 " +
                "where L_SHIPDATE >= '1992-01-01' and L_SHIPDATE < '1993-01-01' group by L_PARTKEY";
        String plan = getFragmentPlan(sql);
        assertCContains(plan, "1:Project\n" +
                "  |  <slot 1> : 1: L_ORDERKEY\n" +
                "  |  <slot 2> : 2: L_PARTKEY\n" +
                "  |  <slot 26> : 2\n" +
                "  |  \n" +
                "  0:OlapScanNode\n" +
                "     TABLE: lineitem_partition\n" +
                "     PREAGGREGATION: ON\n" +
                "     partitions=1/7",
                "7:HASH JOIN\n" +
                        "  |  join op: INNER JOIN (BROADCAST)\n" +
                        "  |  colocate: false, reason: \n" +
                        "  |  equal join conjunct: 23: cast = 18: v1\n" +
                        "  |  \n" +
                        "  |----6:EXCHANGE\n" +
                        "  |    \n" +
                        "  4:Project\n" +
                        "  |  <slot 2> : 2: L_PARTKEY\n" +
                        "  |  <slot 23> : CAST(2: L_PARTKEY AS BIGINT)\n" +
                        "  |  <slot 24> : 24: max\n" +
                        "  |  <slot 25> : 25: sum\n" +
                        "  |  \n" +
                        "  3:AGGREGATE (update finalize)\n" +
                        "  |  output: sum(26: expr), max(1: L_ORDERKEY)\n" +
                        "  |  group by: 2: L_PARTKEY\n" +
                        "  |  \n" +
                        "  2:EXCHANGE");
    }
}
