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

import com.starrocks.common.Config;
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
        Config.enable_virtual_columns = true;
    }

    @AfterAll
    public static void afterClass() {
        connectContext.getSessionVariable().setCboExtractCommonPlan(true);
        Config.enable_virtual_columns = false;
    }

    @Test
    public void testQuery09() throws Exception {
        String plan = getFragmentPlan(Q09);
        assertContains(plan, "MultiCastDataSinks");
        assertContains(plan, "2:AGGREGATE (update serialize)\n" +
                "  |  output: count_if(1, 480: expr), avg_if(468: ss_ext_discount_amt, 480: expr)");
    }

    @Test
    public void testQuery28() throws Exception {
        String plan = getFragmentPlan(Q28);
        assertContains(plan, "MultiCastDataSinks");
        assertContains(plan, "2:AGGREGATE (update serialize)\n" +
                "  |  output: avg(198: if), count(198: if), multi_distinct_count(198: if), avg(202: if), count(202: if), " +
                "multi_distinct_count(202: if)");
    }

    @Disabled
    @Test
    public void testQuery44() throws Exception {
        String plan = getFragmentPlan(Q44);
        assertContains(plan, "MultiCastDataSinks");
        assertContains(plan, "10:AGGREGATE (merge finalize)\n" +
                "  |  output: avg(186: avg)\n" +
                "  |  group by: 173: ss_item_sk");
        assertContains(plan, "4:AGGREGATE (merge finalize)\n" +
                "  |  output: avg(210: avg)\n" +
                "  |  group by: 207: ss_store_sk");
    }

    @Test
    public void testQuery65() throws Exception {
        String plan = getFragmentPlan(Q65);
        assertContains(plan, "MultiCastDataSinks");
        assertContains(plan, "8:AGGREGATE (merge finalize)\n" +
                "  |  output: sum(226: sum)\n" +
                "  |  group by: 195: ss_store_sk, 185: ss_item_sk");
    }

    @Test
    public void testQuery88() throws Exception {
        String plan = getFragmentPlan(Q88);
        assertContains(plan, "MultiCastDataSinks");
        assertContains(plan, "15:AGGREGATE (update serialize)\n" +
                "  |  output: count_if(1, 736: expr), count_if(1, 738: expr)");
    }

    @Test
    public void testQuery90() throws Exception {
        String plan = getFragmentPlan(Q90);
        assertContains(plan, "MultiCastDataSinks");
        assertContains(plan, "15:AGGREGATE (update serialize)\n" +
                "  |  output: count_if(1, 221: expr), count_if(1, 223: expr)\n" +
                "  |  group by: \n" +
                "  |  \n" +
                "  14:Project\n" +
                "  |  <slot 221> : (196: t_hour >= 8) AND (196: t_hour <= 9)");
    }

    @Test
    public void testCommonOnlyGroupBy() throws Exception {
        String plan = getFragmentPlan(
                "with x as (select distinct c_last_name, c_customer_id, c_birth_day from customer)" +
                        "select /*+SET_VAR(cbo_cte_reuse_rate=-1)*/ count(*), sum(a.c_birth_day) " +
                        "from x a join x b on a.c_customer_id = b.c_customer_id " +
                        "where a.c_last_name = 'abc';");
        assertNotContains(plan, "MultiCastDataSinks");
        assertContains(plan, "3:AGGREGATE (merge finalize)\n" +
                "  |  group by: 31: c_last_name, 23: c_customer_id, 33: c_birth_day");

        plan = getFragmentPlan(
                "with x as (select distinct c_last_name, c_customer_id, c_birth_day from customer)" +
                        "select /*+SET_VAR(cbo_cte_reuse_rate=-1)*/ count(*), sum(a.c_birth_day) " +
                        "from x a join x b on a.c_customer_id = b.c_customer_id " +
                        "where a.c_customer_id = 123;");
        assertContains(plan, "MultiCastDataSinks");
        assertContains(plan, "0:OlapScanNode\n" +
                "     TABLE: customer\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: 73: c_customer_id = '123'");
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
        assertContains(plan, "3:AGGREGATE (merge finalize)\n" +
                "  |  output: sum(65: sum)\n" +
                "  |  group by: 54: c_customer_id");
        assertContains(plan, "7:SELECT\n" +
                "  |  predicates: 22: sum > 10");
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
        assertContains(plan, "3:AGGREGATE (merge finalize)\n" +
                "  |  output: sum(65: sum)\n" +
                "  |  group by: 54: c_customer_id\n" +
                "  |  having: (65: sum > 20) OR (65: sum > 10), 65: sum > 10\n" +
                "  |  ");
        assertContains(plan, "7:SELECT\n" +
                "  |  predicates: 22: sum > 10");
        assertContains(plan, "13:SELECT\n" +
                "  |  predicates: 44: sum > 20");
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
        assertContains(plan, "3:AGGREGATE (merge finalize)\n" +
                "  |  output: sum(65: sum)\n" +
                "  |  group by: 54: c_customer_id\n" +
                "  |  having: 65: sum > 10, 65: sum > 10\n" +
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
        assertCContains(plan, "MultiCastDataSinks\n" +
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
                "  |  output: bitmap_union(136: bitmap_union), bitmap_union(138: bitmap_union), bitmap_union(140: " +
                "bitmap_union), bitmap_union(142: bitmap_union)\n" +
                "  |  group by: ");

        assertCContains(plan, "2:AGGREGATE (update serialize)\n" +
                "  |  output: bitmap_union(if((113: c_birth_country = 'USA1') AND (116: c_birth_year = 2011), 143: to_bitmap, " +
                "NULL)), bitmap_union(if((144: expr) AND (116: c_birth_year = 1995), 143: to_bitmap, NULL)), bitmap_union(if((" +
                "(144: expr) AND (116: c_birth_year >= 1990)) AND (116: c_birth_year <= 2000), 143: to_bitmap, NULL)), " +
                "bitmap_union(if((144: expr) AND (116: c_birth_year = 1993), 143: to_bitmap, NULL))\n" +
                "  |  group by: \n" +
                "  |  \n" +
                "  1:Project\n" +
                "  |  <slot 113> : 113: c_birth_country\n" +
                "  |  <slot 116> : 116: c_birth_year\n" +
                "  |  <slot 143> : 143: to_bitmap\n" +
                "  |  <slot 144> : 144: expr\n" +
                "  |  common expressions:\n" +
                "  |  <slot 144> : 113: c_birth_country = 'USA'\n" +
                "  |  <slot 143> : to_bitmap(120: c_customer_id)");
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
        assertContains(plan, "2:AGGREGATE (update serialize)\n" +
                "  |  STREAMING\n" +
                "  |  output: sum(48: c_birth_day), sum_if(67: c_birth_day, 66: expr), any_value_if(TRUE, 66: expr)\n" +
                "  |  group by: 51: c_current_addr_sk\n" +
                "  |  \n" +
                "  1:Project\n" +
                "  |  <slot 48> : 48: c_birth_day\n" +
                "  |  <slot 51> : 51: c_current_addr_sk\n" +
                "  |  <slot 66> : 51: c_current_addr_sk > 100\n" +
                "  |  <slot 67> : clone(48: c_birth_day)");

        assertContains(plan, "4:AGGREGATE (merge finalize)\n" +
                "  |  output: sum(65: sum), sum_if(68: sum), any_value_if(69: row_hit, TRUE)\n" +
                "  |  group by: 51: c_current_addr_sk");

        assertContains(plan, "11:SELECT\n" +
                "  |  predicates: 70: row_hit IS NOT NULL\n" +
                "  |  \n" +
                "  10:Project\n" +
                "  |  <slot 27> : 51: c_current_addr_sk\n" +
                "  |  <slot 44> : 68: sum\n" +
                "  |  <slot 70> : 69: row_hit");
    }

    @Test
    public void testUnionPredicate() throws Exception {
        String sql  = "SELECT\n"
                + "      CURRENT_TIMESTAMP() AS MessageDateAndTime,\n"
                + "      3 AS BatchID,\n"
                + "      MessageSource,\n"
                + "      MessageText,\n"
                + "      'Validation' AS MessageType,\n"
                + "      MessageData\n"
                + "    FROM (\n"
                + "      SELECT\n"
                + "        'DimCustomer' AS MessageSource,\n"
                + "        'Row count' AS MessageText,\n"
                + "        COUNT(1) AS MessageData\n"
                + "      FROM customer\n"
                + "      UNION ALL\n"
                + "      SELECT\n"
                + "        'DimCustomer' AS MessageSource,\n"
                + "        'Inactive customers' AS MessageText,\n"
                + "        COUNT(1)\n"
                + "      FROM customer\n"
                + "      where\n"
                + "        c_current_hdemo_sk\n"
                + "        and c_salutation = 'Inactive'\n"
                + "    ) t;\n";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "0:OlapScanNode\n" +
                "     TABLE: customer\n" +
                "     PREAGGREGATION: ON\n" +
                "     partitions=1/1\n" +
                "     rollup: customer\n" +
                "     tabletRatio=5/5");
    }

    @Test
    public void testUnionCount() throws Exception {
        String sql = "select count(*) from store_sales where ss_item_sk = 2 group by ss_sold_date_sk union" +
                " select count(*) from store_sales where ss_item_sk = 3 group by ss_sold_date_sk";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "MultiCastDataSinks\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 07\n" +
                "    RANDOM\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 11\n" +
                "    RANDOM\n" +
                "\n" +
                "  5:Project\n" +
                "  |  <slot 80> : 80: count\n" +
                "  |  <slot 82> : 82: count\n" +
                "  |  \n" +
                "  4:AGGREGATE (merge finalize)\n" +
                "  |  output: count_if(82: count, 1), count_if(80: count, 1)\n" +
                "  |  group by: 74: ss_sold_date_sk\n" +
                "  |  having: (82: count > 0) OR (80: count > 0)");
        assertContains(plan, "13:SELECT\n" +
                "  |  predicates: 54: count > 0\n" +
                "  |  \n" +
                "  12:Project\n" +
                "  |  <slot 54> : 82: count\n" +
                "  |  \n" +
                "  11:EXCHANGE");

        sql = "select count(1) from store_sales where ss_item_sk = 2 group by ss_sold_date_sk union" +
                " select count(1) from store_sales where ss_item_sk = 3 group by ss_sold_date_sk";
        plan = getFragmentPlan(sql);
        assertContains(plan, "MultiCastDataSinks\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 07\n" +
                "    RANDOM\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 12\n" +
                "    RANDOM\n" +
                "\n" +
                "  5:Project\n" +
                "  |  <slot 80> : 80: count\n" +
                "  |  <slot 81> : 81: row_hit\n" +
                "  |  <slot 83> : 83: count\n" +
                "  |  <slot 84> : 84: row_hit\n" +
                "  |  \n" +
                "  4:AGGREGATE (merge finalize)\n" +
                "  |  output: any_value_if(81: row_hit, TRUE), count_if(83: count, 1), any_value_if(84: row_hit, TRUE), " +
                "count_if(80: count, 1)\n" +
                "  |  group by: 74: ss_sold_date_sk\n" +
                "  |  having: (84: row_hit IS NOT NULL) OR (81: row_hit IS NOT NULL)");
        assertContains(plan, "2:AGGREGATE (update serialize)\n" +
                "  |  STREAMING\n" +
                "  |  output: any_value_if(TRUE, 79: expr), count_if(1, 82: expr), any_value_if(TRUE, 82: expr), count_if(1, " +
                "79: expr)\n" +
                "  |  group by: 74: ss_sold_date_sk");
        assertContains(plan, "9:SELECT\n" +
                "  |  predicates: 85: row_hit IS NOT NULL");
    }

    @Test
    public void testSumAvg() throws Exception {
        String sql = "select sum(ss_store_sk), avg(ss_promo_sk) from store_sales where ss_item_sk =2 group by ss_sold_date_sk " +
                "except select sum(ss_store_sk), avg(ss_promo_sk) from store_sales where ss_item_sk = 3 group by ss_sold_date_sk";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "output: sum_if(84: sum), avg_if(86: avg), any_value_if(87: row_hit, TRUE), sum_if(89: sum), " +
                "avg_if(90: avg), any_value_if(91: row_hit, TRUE)\n" +
                "  |  group by: 77: ss_sold_date_sk");
    }
}
