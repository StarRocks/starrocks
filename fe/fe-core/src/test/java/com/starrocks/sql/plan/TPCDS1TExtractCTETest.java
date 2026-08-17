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
                "  |  output: count_if(1, 544: expr), avg_if(532: ss_ext_discount_amt, 544: expr), " +
                "avg_if(534: ss_net_paid, 544: expr)");
    }

    @Test
    public void testQuery28() throws Exception {
        String plan = getFragmentPlan(Q28);
        assertContains(plan, "MultiCastDataSinks");
        assertContains(plan, "2:AGGREGATE (update serialize)\n" +
                "  |  output: count(222: if), count(226: if), multi_distinct_count(226: if), " +
                "avg(226: if), avg(230: if), count(230: if), " +
                "multi_distinct_count(230: if)");
    }

    @Test
    public void testQuery44() throws Exception {
        String plan = getFragmentPlan(Q44);
        assertContains(plan, "MultiCastDataSinks");
        assertContains(plan, "10:AGGREGATE (merge finalize)\n" +
                "  |  output: avg(210: avg)\n" +
                "  |  group by: 197: ss_item_sk");
        assertContains(plan, "4:AGGREGATE (merge finalize)\n" +
                "  |  output: avg(234: avg)\n" +
                "  |  group by: 231: ss_store_sk");
    }

    @Test
    public void testQuery65() throws Exception {
        String plan = getFragmentPlan(Q65);
        assertContains(plan, "MultiCastDataSinks");
        assertContains(plan, "8:AGGREGATE (merge finalize)\n" +
                "  |  output: sum(250: sum)\n" +
                "  |  group by: 219: ss_store_sk, 209: ss_item_sk");
    }

    @Test
    public void testQuery88() throws Exception {
        String plan = getFragmentPlan(Q88);
        assertContains(plan, "MultiCastDataSinks");
        assertContains(plan, "15:AGGREGATE (update serialize)\n" +
                "  |  output: count_if(1, 864: expr), count_if(1, 866: expr), count_if(1, 852: expr), count_if(1, 854: expr)");
    }

    @Test
    public void testQuery90() throws Exception {
        String plan = getFragmentPlan(Q90);
        assertContains(plan, "MultiCastDataSinks");
        assertContains(plan, "15:AGGREGATE (update serialize)\n" +
                "  |  output: count_if(1, 253: expr), count_if(1, 255: expr)\n" +
                "  |  group by: \n" +
                "  |  \n" +
                "  14:Project\n" +
                "  |  <slot 253> : (228: t_hour >= 8) AND (228: t_hour <= 9)");
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
                "  |  group by: 35: c_last_name, 27: c_customer_id, 37: c_birth_day");

        plan = getFragmentPlan(
                "with x as (select distinct c_last_name, c_customer_id, c_birth_day from customer)" +
                        "select /*+SET_VAR(cbo_cte_reuse_rate=-1)*/ count(*), sum(a.c_birth_day) " +
                        "from x a join x b on a.c_customer_id = b.c_customer_id " +
                        "where a.c_customer_id = 123;");
        assertContains(plan, "MultiCastDataSinks");
        assertContains(plan, "0:OlapScanNode\n" +
                "     TABLE: customer\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: 85: c_customer_id = '123'");
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
                "  |  output: sum(73: sum)\n" +
                "  |  group by: 62: c_customer_id");
        assertContains(plan, "7:SELECT\n" +
                "  |  predicates: 26: sum > 10");
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
                "  |  output: sum(73: sum)\n" +
                "  |  group by: 62: c_customer_id\n" +
                "  |  having: (73: sum > 20) OR (73: sum > 10), 73: sum > 10\n" +
                "  |  ");
        assertContains(plan, "7:SELECT\n" +
                "  |  predicates: 26: sum > 10");
        assertContains(plan, "13:SELECT\n" +
                "  |  predicates: 52: sum > 20");
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
                "  |  output: sum(73: sum)\n" +
                "  |  group by: 62: c_customer_id\n" +
                "  |  having: 73: sum > 10, 73: sum > 10\n" +
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
                "  |  output: bitmap_union_if(162: bitmap_union), bitmap_union_if(153: bitmap_union), " +
                "bitmap_union_if(156: bitmap_union), bitmap_union_if(159: bitmap_union)\n" +
                "  |  group by: ");

        assertCContains(plan, "2:AGGREGATE (update serialize)\n" +
                "  |  output: bitmap_union_if(161: to_bitmap, (164: expr) AND (132: c_birth_year = 1993)), " +
                "bitmap_union_if(to_bitmap(136: c_customer_id), (129: c_birth_country = 'USA1') AND " +
                "(132: c_birth_year = 2011)), bitmap_union_if(to_bitmap(136: c_customer_id), (164: expr) AND " +
                "(132: c_birth_year = 1995)), bitmap_union_if(to_bitmap(136: c_customer_id), " +
                "((164: expr) AND (132: c_birth_year >= 1990)) AND (132: c_birth_year <= 2000))\n" +
                "  |  group by: \n" +
                "  |  \n" +
                "  1:Project\n" +
                "  |  <slot 129> : 129: c_birth_country\n" +
                "  |  <slot 132> : 132: c_birth_year\n" +
                "  |  <slot 136> : 136: c_customer_id\n" +
                "  |  <slot 161> : 163: to_bitmap\n" +
                "  |  <slot 164> : 164: expr\n" +
                "  |  common expressions:\n" +
                "  |  <slot 163> : to_bitmap(136: c_customer_id)\n" +
                "  |  <slot 164> : 129: c_birth_country = 'USA'");
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
                "  |  output: sum(56: c_birth_day), sum_if(75: c_birth_day, 74: expr), any_value_if(TRUE, 74: expr)\n" +
                "  |  group by: 59: c_current_addr_sk\n" +
                "  |  \n" +
                "  1:Project\n" +
                "  |  <slot 56> : 56: c_birth_day\n" +
                "  |  <slot 59> : 59: c_current_addr_sk\n" +
                "  |  <slot 74> : 59: c_current_addr_sk > 100\n" +
                "  |  <slot 75> : clone(56: c_birth_day)");

        assertContains(plan, "4:AGGREGATE (merge finalize)\n" +
                "  |  output: sum(73: sum), sum_if(76: sum), any_value_if(77: row_hit)\n" +
                "  |  group by: 59: c_current_addr_sk");

        assertContains(plan, "11:SELECT\n" +
                "  |  predicates: 78: row_hit IS NOT NULL\n" +
                "  |  \n" +
                "  10:Project\n" +
                "  |  <slot 31> : 59: c_current_addr_sk\n" +
                "  |  <slot 52> : 76: sum\n" +
                "  |  <slot 78> : 77: row_hit");
    }

    @Test
    public void testUnionPredicate() throws Exception {
        String sql = "SELECT\n"
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
                "  |  <slot 88> : 88: count\n" +
                "  |  <slot 90> : 90: count\n" +
                "  |  \n" +
                "  4:AGGREGATE (merge finalize)\n" +
                "  |  output: count_if(88: count), count_if(90: count)\n" +
                "  |  group by: 82: ss_sold_date_sk\n" +
                "  |  having: (90: count > 0) OR (88: count > 0)");
        assertContains(plan, "13:SELECT\n" +
                "  |  predicates: 62: count > 0\n" +
                "  |  \n" +
                "  12:Project\n" +
                "  |  <slot 62> : 90: count\n" +
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
                "  |  <slot 88> : 88: count\n" +
                "  |  <slot 89> : 89: row_hit\n" +
                "  |  <slot 91> : 91: count\n" +
                "  |  <slot 92> : 92: row_hit\n" +
                "  |  \n" +
                "  4:AGGREGATE (merge finalize)\n" +
                "  |  output: count_if(88: count), any_value_if(89: row_hit), count_if(91: count), " +
                "any_value_if(92: row_hit)\n" +
                "  |  group by: 82: ss_sold_date_sk\n" +
                "  |  having: (92: row_hit IS NOT NULL) OR (89: row_hit IS NOT NULL)");
        assertContains(plan, "2:AGGREGATE (update serialize)\n" +
                "  |  STREAMING\n" +
                "  |  output: count_if(1, 87: expr), any_value_if(TRUE, 87: expr), count_if(1, 90: expr), " +
                "any_value_if(TRUE, 90: expr)\n" +
                "  |  group by: 82: ss_sold_date_sk");
        assertContains(plan, "9:SELECT\n" +
                "  |  predicates: 93: row_hit IS NOT NULL");
    }

    @Test
    public void testSumAvg() throws Exception {
        String sql = "select sum(ss_store_sk), avg(ss_promo_sk) from store_sales " +
                "where ss_item_sk =2 group by ss_sold_date_sk " +
                "except select sum(ss_store_sk), avg(ss_promo_sk) from store_sales " +
                "where ss_item_sk = 3 group by ss_sold_date_sk";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "output: sum_if(97: sum), avg_if(98: avg), " +
                "any_value_if(99: row_hit, TRUE), sum_if(92: sum), " +
                "avg_if(94: avg), any_value_if(95: row_hit, TRUE)\n" +
                "  |  group by: 85: ss_sold_date_sk");
    }
}
