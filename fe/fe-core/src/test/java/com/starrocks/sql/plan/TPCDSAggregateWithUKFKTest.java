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

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class TPCDSAggregateWithUKFKTest extends TPCDS1TTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        TPCDS1TTestBase.beforeClass();
        prepareUniqueKeys();
        prepareForeignKeys();

        connectContext.getSessionVariable().setEnableUKFKOpt(true);
    }

    @Test
    public void testPruneGroupBys() throws Exception {
        String sql;
        String plan;

        // Prune itemdesc and i_class in Aggregation, and then eliminate item table.
        sql = "select item_sk, cnt from (\n" +
                "  select substr(i_item_desc, 1, 30) itemdesc, i_item_sk item_sk, i_class, d_date solddate, count(*) cnt\n" +
                "  from store_sales\n" +
                "    , date_dim\n" +
                "    , item\n" +
                "  where ss_sold_date_sk = d_date_sk\n" +
                "    and ss_item_sk = i_item_sk\n" +
                "    and d_year in (2000, 2000 + 1, 2000 + 2, 2000 + 3)\n" +
                "  group by substr(i_item_desc, 1, 30), i_item_sk, i_class, d_date\n" +
                "  having count(*) > 4\n" +
                ")t;\n";
        plan = getFragmentPlan(sql);
        assertNotContains(plan, "TABLE: item");
        assertContains(plan, "group by: 52: i_item_sk, 26: d_date");

        // Cannot prune itemdesc in Aggregation, since it is used as an output column.
        // Prune i_class in Aggregation.
        sql = "select item_sk, itemdesc, cnt from (\n" +
                "  select substr(i_item_desc, 1, 30) itemdesc, i_item_sk item_sk, i_class, d_date solddate, count(*) cnt\n" +
                "  from store_sales\n" +
                "    , date_dim\n" +
                "    , item\n" +
                "  where ss_sold_date_sk = d_date_sk\n" +
                "    and ss_item_sk = i_item_sk\n" +
                "    and d_year in (2000, 2000 + 1, 2000 + 2, 2000 + 3)\n" +
                "  group by substr(i_item_desc, 1, 30), i_item_sk, i_class, d_date\n" +
                "  having count(*) > 4\n" +
                ")t;\n";
        plan = getFragmentPlan(sql);
        assertContains(plan, "TABLE: item");
        assertContains(plan, "group by: 74: substr, 52: i_item_sk, 26: d_date");

        // Cannot prune itemdesc and i_class in Aggregation, since they are used as output columns.
        sql = "select item_sk, itemdesc, i_class, cnt from (\n" +
                "  select substr(i_item_desc, 1, 30) itemdesc, i_item_sk item_sk, i_class, d_date solddate, count(*) cnt\n" +
                "  from store_sales\n" +
                "    , date_dim\n" +
                "    , item\n" +
                "  where ss_sold_date_sk = d_date_sk\n" +
                "    and ss_item_sk = i_item_sk\n" +
                "    and d_year in (2000, 2000 + 1, 2000 + 2, 2000 + 3)\n" +
                "  group by substr(i_item_desc, 1, 30), i_item_sk, i_class, d_date\n" +
                "  having count(*) > 4\n" +
                ")t;\n";
        plan = getFragmentPlan(sql);
        assertContains(plan, "TABLE: item");
        assertContains(plan, "74: substr, 52: i_item_sk, 62: i_class, 26: d_date");

        // Prune itemdesc and i_class in Aggregation, and then eliminate item table.
        sql = "select item_sk, cnt from (\n" +
                "  select substr(i_item_desc, 1, 30) itemdesc, i_item_sk item_sk, i_class, d_date solddate, count(*) cnt\n" +
                "  from store_sales\n" +
                "    , date_dim\n" +
                "    , item\n" +
                "  where ss_sold_date_sk = d_date_sk\n" +
                "    and ss_item_sk = i_item_sk\n" +
                "    and d_year in (2000, 2000 + 1, 2000 + 2, 2000 + 3)\n" +
                "  group by substr(i_item_desc, 1, 30), i_item_sk, i_class, d_date_sk, d_date, d_current_month\n" +
                "  having count(*) > 4\n" +
                ")t;\n";
        plan = getFragmentPlan(sql);
        assertNotContains(plan, "TABLE: item");
        assertContains(plan, "group by: 52: i_item_sk, 24: d_date_sk");
    }

    @Test
    public void testEliminateAggRule() throws Exception {
        String sql;
        String plan;

        // Agg (Group by ss_customer_sk, d_year) -> Join(ss_customer_sk=c_customer_sk) -> Agg(Group by c_customer_id, d_year, ...)
        // Agg(Group by c_customer_id, d_year, ...) could be eliminated.
        sql = "\n" +
                "with w1 as (\n" +
                "  select \n" +
                "    sum(((ss_ext_list_price-ss_ext_wholesale_cost-ss_ext_discount_amt)+ss_ext_sales_price)/2) year_total,\n" +
                "    ss_customer_sk,\n" +
                "    d_year\n" +
                "  from store_sales, date_dim\n" +
                "  where ss_sold_date_sk = d_date_sk and d_year between 2001 and 2002\n" +
                "  group by ss_customer_sk, d_year\n" +
                ")\n" +
                "select c_customer_id customer_id\n" +
                "      ,c_first_name customer_first_name\n" +
                "      ,c_last_name customer_last_name\n" +
                "      ,c_preferred_cust_flag customer_preferred_cust_flag\n" +
                "      ,c_birth_country customer_birth_country\n" +
                "      ,c_login customer_login\n" +
                "      ,c_email_address customer_email_address\n" +
                "      ,d_year dyear\n" +
                "      ,sum(year_total) year_total\n" +
                "      ,'s' sale_type\n" +
                "from customer\n" +
                "    , w1\n" +
                "where c_customer_sk = ss_customer_sk\n" +
                "group by c_customer_id\n" +
                "        ,c_first_name\n" +
                "        ,c_last_name\n" +
                "        ,c_preferred_cust_flag\n" +
                "        ,c_birth_country\n" +
                "        ,c_login\n" +
                "        ,c_email_address\n" +
                "        ,d_year";
        plan = getFragmentPlan(sql);
        assertContains(plan, "  11:Project\n" +
                "  |  <slot 2> : 2: c_customer_id\n" +
                "  |  <slot 9> : 9: c_first_name\n" +
                "  |  <slot 10> : 10: c_last_name\n" +
                "  |  <slot 11> : 11: c_preferred_cust_flag\n" +
                "  |  <slot 15> : 15: c_birth_country\n" +
                "  |  <slot 16> : 16: c_login\n" +
                "  |  <slot 17> : 17: c_email_address\n" +
                "  |  <slot 48> : 48: d_year\n" +
                "  |  <slot 72> : 71: sum\n" +
                "  |  <slot 73> : 's'\n" +
                "  |  \n" +
                "  10:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BROADCAST)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 23: ss_customer_sk = 1: c_customer_sk\n" +
                "  |  \n" +
                "  |----9:EXCHANGE\n" +
                "  |    \n" +
                "  7:AGGREGATE (merge finalize)\n" +
                "  |  output: sum(71: sum)\n" +
                "  |  group by: 23: ss_customer_sk, 48: d_year");

        // Agg (Group by ss_customer_sk, d_year) -> Join(ss_customer_sk=c_customer_sk) -> Agg(Group by c_customer_id, ...)
        // Agg(Group by c_customer_id, ...) could not be eliminated.
        sql = "\n" +
                "with w1 as (\n" +
                "  select \n" +
                "    sum(((ss_ext_list_price-ss_ext_wholesale_cost-ss_ext_discount_amt)+ss_ext_sales_price)/2) year_total,\n" +
                "    ss_customer_sk,\n" +
                "    d_year\n" +
                "  from store_sales, date_dim\n" +
                "  where ss_sold_date_sk = d_date_sk and d_year between 2001 and 2002\n" +
                "  group by ss_customer_sk, d_year\n" +
                ")\n" +
                "select c_customer_id customer_id\n" +
                "      ,c_first_name customer_first_name\n" +
                "      ,c_last_name customer_last_name\n" +
                "      ,c_preferred_cust_flag customer_preferred_cust_flag\n" +
                "      ,c_birth_country customer_birth_country\n" +
                "      ,c_login customer_login\n" +
                "      ,c_email_address customer_email_address\n" +
                "      ,sum(year_total) year_total\n" +
                "      ,'s' sale_type\n" +
                "from customer\n" +
                "    , w1\n" +
                "where c_customer_sk = ss_customer_sk\n" +
                "group by c_customer_id\n" +
                "        ,c_first_name\n" +
                "        ,c_last_name\n" +
                "        ,c_preferred_cust_flag\n" +
                "        ,c_birth_country\n" +
                "        ,c_login\n" +
                "        ,c_email_address\n";
        plan = getFragmentPlan(sql);
        assertContains(plan, "  13:AGGREGATE (update serialize)\n" +
                "  |  STREAMING\n" +
                "  |  output: sum(71: sum)\n" +
                "  |  group by: 2: c_customer_id, 9: c_first_name, 10: c_last_name, " +
                "11: c_preferred_cust_flag, 15: c_birth_country, 16: c_login, 17: c_email_address\n" +
                "  |  \n" +
                "  12:Project\n" +
                "  |  <slot 2> : 2: c_customer_id\n" +
                "  |  <slot 9> : 9: c_first_name\n" +
                "  |  <slot 10> : 10: c_last_name\n" +
                "  |  <slot 11> : 11: c_preferred_cust_flag\n" +
                "  |  <slot 15> : 15: c_birth_country\n" +
                "  |  <slot 16> : 16: c_login\n" +
                "  |  <slot 17> : 17: c_email_address\n" +
                "  |  <slot 71> : 71: sum\n" +
                "  |  \n" +
                "  11:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BROADCAST)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 23: ss_customer_sk = 1: c_customer_sk\n" +
                "  |  \n" +
                "  |----10:EXCHANGE\n" +
                "  |    \n" +
                "  8:Project\n" +
                "  |  <slot 23> : 23: ss_customer_sk\n" +
                "  |  <slot 71> : 71: sum\n" +
                "  |  \n" +
                "  7:AGGREGATE (merge finalize)\n" +
                "  |  output: sum(71: sum)\n" +
                "  |  group by: 23: ss_customer_sk, 48: d_year");
    }
}
