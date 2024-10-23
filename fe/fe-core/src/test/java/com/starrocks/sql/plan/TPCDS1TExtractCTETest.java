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

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;


public class TPCDS1TExtractCTETest extends TPCDS1TTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        TPCDSPlanTestBase.beforeClass();
        connectContext.getSessionVariable().setCboCTERuseRatio(0);
        connectContext.getSessionVariable().setOptimizerExecuteTimeout(-1);
        connectContext.getSessionVariable().setCboExtractCommonPlan(true);
    }

    @AfterAll
    public static void afterClass() {
        connectContext.getSessionVariable().setCboExtractCommonPlan(true);
    }

    @Test
    public void testQuery09() throws Exception {
        String plan = getFragmentPlan(Q09);
        assertContains(plan, "MultiCastDataSinks");
        assertContains(plan, "2:AGGREGATE (update serialize)\n" +
                "  |  output: count(if(461: expr, 1, NULL)), avg(if(461: expr, 394: ss_ext_discount_amt, NULL))");
    }

    @Test
    public void testQuery28() throws Exception {
        String plan = getFragmentPlan(Q28);
        assertContains(plan, "MultiCastDataSinks");
        assertContains(plan, "  2:AGGREGATE (update serialize)\n" +
                "  |  output: avg(192: if), count(192: if), multi_distinct_count(192: if), " +
                "count(196: if), multi_distinct_count(196: if)");
    }

    @Test
    public void testQuery44() throws Exception {
        String plan = getFragmentPlan(Q44);
        assertContains(plan, "MultiCastDataSinks");
        assertContains(plan, "  4:AGGREGATE (merge finalize)\n" +
                "  |  output: avg(192: avg)\n" +
                "  |  group by: 179: ss_item_sk");
        assertContains(plan, "  9:AGGREGATE (merge finalize)\n" +
                "  |  output: avg(168: avg)\n" +
                "  |  group by: 165: ss_store_sk");
    }

    @Test
    public void testQuery65() throws Exception {
        String plan = getFragmentPlan(Q65);
        assertContains(plan, "MultiCastDataSinks");
        assertContains(plan, "  8:AGGREGATE (merge finalize)\n" +
                "  |  output: sum(208: sum)\n" +
                "  |  group by: 177: ss_store_sk, 167: ss_item_sk");
    }

    @Test
    public void testQuery88() throws Exception {
        String plan = getFragmentPlan(Q88);
        assertContains(plan, "MultiCastDataSinks");
        assertContains(plan, "AGGREGATE (merge finalize)\n" +
                "  |  output: count(641: count), count(643: count)");
    }

    @Test
    public void testQuery90() throws Exception {
        String plan = getFragmentPlan(Q90);
        assertContains(plan, "MultiCastDataSinks");
        assertContains(plan, "AGGREGATE (update serialize)\n" +
                "  |  output: count(if((172: t_hour >= 8)");
    }

    @Test
    public void testCommonOnlyGroupBy() throws Exception {
        String plan = getFragmentPlan(
                "with x as (select distinct c_last_name, c_customer_id, c_birth_day from customer)" +
                        "select /*+SET_VAR(cbo_cte_reuse_rate=-1)*/ count(*), sum(a.c_birth_day) " +
                        "from x a join x b on a.c_customer_id = b.c_customer_id " +
                        "where a.c_last_name = 'abc';");
        assertNotContains(plan, "MultiCastDataSinks");
        assertContains(plan, "  3:AGGREGATE (merge finalize)\n" +
                "  |  group by: 28: c_last_name, 20: c_customer_id, 30: c_birth_day");

        plan = getFragmentPlan(
                "with x as (select distinct c_last_name, c_customer_id, c_birth_day from customer)" +
                        "select /*+SET_VAR(cbo_cte_reuse_rate=-1)*/ count(*), sum(a.c_birth_day) " +
                        "from x a join x b on a.c_customer_id = b.c_customer_id " +
                        "where a.c_customer_id = 123;");
        assertContains(plan, "MultiCastDataSinks");
        assertContains(plan, "OlapScanNode\n" +
                "     TABLE: customer\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: 64: c_customer_id = '123'");
    }

    @Test
    public void testAggregateFilter() throws Exception {
        String plan = getFragmentPlan(
                "select * from " +
                        "(select c_customer_id, sum(c_birth_day) " +
                        "from customer group by c_customer_id having sum(c_birth_day) > 10 " +
                        "union all " +
                        "select c_customer_id, sum(c_birth_day) " +
                        "from customer group by c_customer_id) cc order by c_customer_id limit 2");
        assertContains(plan, "MultiCastDataSinks");
        assertContains(plan, "AGGREGATE (merge finalize)\n" +
                "  |  output: sum(59: sum)\n" +
                "  |  group by: 48: c_customer_id\n" +
                "  |  ");
        assertContains(plan, "SELECT\n" +
                "  |  predicates: 19: sum > 10");
    }

    @Test
    public void testAggregateFilter2() throws Exception {
        String
                plan = getFragmentPlan(
                "select * from " +
                        "(select c_customer_id, sum(c_birth_day) " +
                        "from customer group by c_customer_id having sum(c_birth_day) > 10 " +
                        "union all " +
                        "select c_customer_id, sum(c_birth_day) " +
                        "from customer group by c_customer_id having sum(c_birth_day) > 20 " +
                        ") cc order by c_customer_id limit 2");
        assertContains(plan, "MultiCastDataSinks");
        assertContains(plan, "AGGREGATE (merge finalize)\n" +
                "  |  output: sum(59: sum)\n" +
                "  |  group by: 48: c_customer_id\n" +
                "  |  having: (59: sum > 20) OR (59: sum > 10), 59: sum > 10\n" +
                "  |  \n" +
                "  2:EXCHANGE");
        assertContains(plan, "SELECT\n" +
                "  |  predicates: 19: sum > 10");
        assertContains(plan, "SELECT\n" +
                "  |  predicates: 38: sum > 20");
    }

    @Test
    public void testAggregateFilter3() throws Exception {
        String plan = getFragmentPlan(
                "select * from " +
                        "(select c_customer_id, sum(c_birth_day) " +
                        "from customer group by c_customer_id having sum(c_birth_day) > 10 " +
                        "union all " +
                        "select c_customer_id, sum(c_birth_day) " +
                        "from customer group by c_customer_id having sum(c_birth_day) > 10 " +
                        ") cc order by c_customer_id limit 2");
        assertContains(plan, "MultiCastDataSinks");
        assertContains(plan, "AGGREGATE (merge finalize)\n" +
                "  |  output: sum(59: sum)\n" +
                "  |  group by: 48: c_customer_id\n" +
                "  |  having: 59: sum > 10, 59: sum > 10\n" +
                "  |  \n" +
                "  2:EXCHANGE");
    }
}
