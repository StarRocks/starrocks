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
import org.junit.jupiter.api.Disabled;
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
                "  |  output: count_if(1, 418: expr), avg_if(420: ss_ext_discount_amt, 418: expr)");
    }

    @Test
    public void testQuery28() throws Exception {
        String plan = getFragmentPlan(Q28);
        assertContains(plan, "MultiCastDataSinks");
        assertContains(plan, "  2:AGGREGATE (update serialize)\n" +
                "  |  output: avg(192: if), count(192: if), multi_distinct_count(192: if), " +
                "count(196: if), multi_distinct_count(196: if)");
    }

    @Disabled
    @Test
    public void testQuery44() throws Exception {
        String plan = getFragmentPlan(Q44);
        assertContains(plan, "MultiCastDataSinks");
        assertContains(plan, "AGGREGATE (merge finalize)\n" +
                "  |  output: avg(168: avg)\n" +
                "  |  group by: 155: ss_item_sk");
        assertContains(plan, "AGGREGATE (merge finalize)\n" +
                "  |  output: avg(192: avg)\n" +
                "  |  group by: 189: ss_store_sk");
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
        assertContains(plan, "15:AGGREGATE (update serialize)\n" +
                "  |  output: count_if(1, 640: expr), count_if(1, 642: expr)");
    }

    @Test
    public void testQuery90() throws Exception {
        String plan = getFragmentPlan(Q90);
        assertContains(plan, "MultiCastDataSinks");
        assertContains(plan, " 15:AGGREGATE (update serialize)\n" +
                "  |  output: count_if(1, 197: expr), count_if(1, 199: expr)\n" +
                "  |  group by: \n" +
                "  |  \n" +
                "  14:Project\n" +
                "  |  <slot 197> : (172: t_hour >= 8) AND (172: t_hour <= 9)");
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

    @Test
    public void testUnionAggregateFilter3() throws Exception {
        String plan = getFragmentPlan("select c_customer_id, sum(c_birth_day) " +
                "from customer group by c_customer_id having sum(c_birth_day) > 10 " +
                "union all " +
                "select c_customer_id, sum(c_birth_day) " +
                "from customer group by c_customer_id having sum(c_birth_day) > 20 " +
                "union all " +
                "select c_customer_id, sum(c_birth_day) " +
                "from customer group by c_customer_id having sum(c_birth_day) > 30 ");
        assertContains(plan, "MultiCastDataSinks\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 05\n" +
                "    RANDOM\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 09\n" +
                "    RANDOM\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 13\n" +
                "    RANDOM");
    }

    @Test
    public void testFallbackAggIf() throws Exception {
        String sql = "select\n" +
                "  BITMAP_AND(\n" +
                "    (\n" +
                "      select\n" +
                "        BITMAP_AND(\n" +
                "          (\n" +
                "            select\n" +
                "              BITMAP_AND(\n" +
                "                (\n" +
                "                  select\n" +
                "                    bmp\n" +
                "                  from\n" +
                "                    (\n" +
                "                      select\n" +
                "                        '1' as bmp_order,\n" +
                "                        bitmap_union(to_bitmap(c_customer_id)) as bmp,\n" +
                "                        '0' as agg_type\n" +
                "                      from\n" +
                "                        customer\n" +
                "                      where\n" +
                "                        c_birth_country = 'USA1'\n" +
                "                        and (c_birth_year = 2011)\n" +
                "                    ) as t1\n" +
                "                ),\n" +
                "                (\n" +
                "                  select\n" +
                "                    bmp\n" +
                "                  from\n" +
                "                    (\n" +
                "                      select\n" +
                "                        '2' as bmp_order,\n" +
                "                        bitmap_union(to_bitmap(c_customer_id)) as bmp,\n" +
                "                        '2' as agg_type\n" +
                "                      from\n" +
                "                        customer\n" +
                "                      where\n" +
                "                        c_birth_country = 'USA'\n" +
                "                        and (c_birth_year = 1995)\n" +
                "                    ) as t2\n" +
                "                )\n" +
                "              )\n" +
                "          ),\n" +
                "          (\n" +
                "            select\n" +
                "              bmp\n" +
                "            from\n" +
                "              (\n" +
                "                select\n" +
                "                  '3' as bmp_order,\n" +
                "                  bitmap_union(to_bitmap(c_customer_id)) as bmp,\n" +
                "                  '2' as agg_type\n" +
                "                from\n" +
                "                  customer\n" +
                "                where\n" +
                "                  c_birth_country = 'USA'\n" +
                "                  and (\n" +
                "                    c_birth_year BETWEEN 1990 and 2000\n" +
                "                  )\n" +
                "              ) as t3\n" +
                "          )\n" +
                "        )\n" +
                "    ),\n" +
                "    (\n" +
                "      select\n" +
                "        bmp\n" +
                "      from\n" +
                "        (\n" +
                "          select\n" +
                "            '4' as bmp_order,\n" +
                "            bitmap_union(to_bitmap(c_customer_id)) as bmp,\n" +
                "            '2' as agg_type\n" +
                "          from\n" +
                "            customer\n" +
                "          where\n" +
                "            c_birth_country = 'USA'\n" +
                "            and (c_birth_year = '1993')\n" +
                "        ) as t4\n" +
                "    )\n" +
                "  );\n";
        String plan = getFragmentPlan(sql);
        assertCContains(plan, "  MultiCastDataSinks\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 05\n" +
                "    RANDOM\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 08\n" +
                "    RANDOM\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 16\n" +
                "    RANDOM\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 23\n" +
                "    RANDOM\n" +
                "\n" +
                "  4:AGGREGATE (merge finalize)\n" +
                "  |  output: bitmap_union(130: bitmap_union), bitmap_union(124: bitmap_union), " +
                "bitmap_union(126: bitmap_union), bitmap_union(128: bitmap_union)\n" +
                "  |  group by: ");

        assertCContains(plan, "  2:AGGREGATE (update serialize)\n" +
                "  |  output: bitmap_union(if((132: expr) AND (104: c_birth_year = 1993), 131: to_bitmap, NULL)), " +
                "bitmap_union(if((101: c_birth_country = 'USA1') AND (104: c_birth_year = 2011), " +
                "131: to_bitmap, NULL)), bitmap_union(if((132: expr) AND (104: c_birth_year = 1995), " +
                "131: to_bitmap, NULL)), bitmap_union(if(((132: expr) AND (104: c_birth_year >= 1990)) " +
                "AND (104: c_birth_year <= 2000), 131: to_bitmap, NULL))\n" +
                "  |  group by: \n" +
                "  |  \n" +
                "  1:Project\n" +
                "  |  <slot 101> : 101: c_birth_country\n" +
                "  |  <slot 104> : 104: c_birth_year\n" +
                "  |  <slot 131> : 131: to_bitmap\n" +
                "  |  <slot 132> : 132: expr\n" +
                "  |  common expressions:\n" +
                "  |  <slot 131> : to_bitmap(108: c_customer_id)\n" +
                "  |  <slot 132> : 101: c_birth_country = 'USA'");
    }

    @Test
    public void testFilterProject() throws Exception {
        String sql = "select * from ( \n"
                + " SELECT c_current_addr_sk,sum(c_birth_day) FROM customer "
                + " GROUP BY c_current_addr_sk \n"
                + "  union all \n"
                + " SELECT c_current_addr_sk,sum(c_birth_day) FROM customer "
                + " where c_current_addr_sk > 100"
                + " GROUP BY c_current_addr_sk \n"
                + ") t ORDER BY 1; \n";

        String plan = getFragmentPlan(sql);
        assertContains(plan, "  2:AGGREGATE (update serialize)\n"
                + "  |  STREAMING\n"
                + "  |  output: sum(42: c_birth_day), sum_if(61: c_birth_day, 60: expr)\n"
                + "  |  group by: 45: c_current_addr_sk\n"
                + "  |  \n"
                + "  1:Project\n"
                + "  |  <slot 42> : 42: c_birth_day\n"
                + "  |  <slot 45> : 45: c_current_addr_sk\n"
                + "  |  <slot 60> : 45: c_current_addr_sk > 100\n"
                + "  |  <slot 61> : clone(42: c_birth_day)");
    }
}
