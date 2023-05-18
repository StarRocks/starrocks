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
        Assert.assertTrue(plan.contains("  1:AGGREGATE (update finalize)\n" +
                "  |  aggregate: sum[([3: income, DECIMAL128(10,2), false]); args: DECIMAL128; " +
                "result: DECIMAL128(38,2); args nullable: false; result nullable: true]\n" +
                "  |  group by: [2: order_date, DATE, false], [1: region, VARCHAR, true]\n"));

        Assert.assertTrue(plan.contains("  0:OlapScanNode\n" +
                "     table: trans, rollup: trans\n" +
                "     preAggregation: on\n" +
                "     Predicates: month[([2: order_date, DATE, false]); args: DATE; result: TINYINT; " +
                "args nullable: false; result nullable: false] = 1\n" +
                ""));
    }
}
