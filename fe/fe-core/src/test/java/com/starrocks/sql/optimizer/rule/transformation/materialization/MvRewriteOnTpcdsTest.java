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

import com.starrocks.planner.MaterializedViewTestBase;
import com.starrocks.sql.plan.TPCDSPlanTestBase;
import org.junit.BeforeClass;
import org.junit.Test;

public class MvRewriteOnTpcdsTest extends MaterializedViewTestBase {
    @BeforeClass
    public static void beforeClass() throws Exception {
        TPCDSPlanTestBase.beforeClass();
        connectContext.getSessionVariable().setMaterializedViewRewriteMode("force");
    }

    @Test
    public void testQuery16() throws Exception {
        connectContext.executeSql("drop materialized view if exists __mv__ta0008");
        createAndRefreshMV("test", "CREATE MATERIALIZED VIEW __mv__ta0008 (_ca0005, _ca0006, _ca0007)\n" +
                "DISTRIBUTED BY hash(_ca0005)\n" +
                "REFRESH ASYNC START(\"2023-12-01 10:00:00\") EVERY(INTERVAL 1 DAY)\n" +
                "PROPERTIES (\n" +
                "  \"replication_num\" = \"1\"\n" +
                ")\n" +
                "AS\n" +
                "SELECT\n" +
                "  (count(DISTINCT _ta0002.cs_order_number)) AS _ca0005\n" +
                "  ,(sum(_ta0002.cs_ext_ship_cost)) AS _ca0006\n" +
                "  ,(sum(_ta0002.cs_net_profit)) AS _ca0007\n" +
                "FROM\n" +
                "  (\n" +
                "    SELECT\n" +
                "      catalog_sales.cs_order_number\n" +
                "      ,catalog_sales.cs_net_profit\n" +
                "      ,catalog_sales.cs_ext_ship_cost\n" +
                "    FROM\n" +
                "      catalog_sales\n" +
                "      INNER JOIN\n" +
                "      date_dim\n" +
                "      ON (catalog_sales.cs_ship_date_sk = date_dim.d_date_sk)\n" +
                "      INNER JOIN\n" +
                "      customer_address\n" +
                "      ON (catalog_sales.cs_ship_addr_sk = customer_address.ca_address_sk)\n" +
                "      INNER JOIN\n" +
                "      call_center\n" +
                "      ON (catalog_sales.cs_call_center_sk = call_center.cc_call_center_sk)\n" +
                "      LEFT SEMI JOIN\n" +
                "      (\n" +
                "        SELECT\n" +
                "          catalog_sales.cs_order_number\n" +
                "          ,catalog_sales.cs_warehouse_sk\n" +
                "        FROM\n" +
                "          catalog_sales\n" +
                "      ) _ta0000\n" +
                "      ON (catalog_sales.cs_order_number = _ta0000.cs_order_number)\n" +
                "         AND (catalog_sales.cs_warehouse_sk != _ta0000.cs_warehouse_sk)\n" +
                "      LEFT ANTI JOIN\n" +
                "      catalog_returns\n" +
                "      ON (catalog_sales.cs_order_number = catalog_returns.cr_order_number)\n" +
                "    WHERE\n" +
                "      (date_dim.d_date <= \"2002-04-02\")\n" +
                "      AND call_center.cc_county in (\"Williamson County\", \"Williamson County\"," +
                " \"Williamson County\", \"Williamson County\", \"Williamson County\")\n" +
                "      AND (customer_address.ca_state = \"GA\")\n" +
                "      AND (catalog_sales.cs_ship_date_sk IS NOT NULL)\n" +
                "      AND (\"2002-02-01\" <= date_dim.d_date)\n" +
                "  ) _ta0002;");

        {
            MVRewriteChecker checker = sql(TPCDSPlanTestBase.Q16);
            checker.contains("__mv__ta0008");
        }
    }

    @Test
    public void testQuery39() throws Exception {
        connectContext.executeSql("drop materialized view if exists __mv__ta0006");
        createAndRefreshMV("test", "CREATE MATERIALIZED VIEW __mv__ta0006 " +
                "DISTRIBUTED BY HASH (d_moy, d_year, w_warehouse_name, w_warehouse_sk, i_item_sk)\n" +
                "REFRESH ASYNC START(\"2023-12-01 10:00:00\") EVERY(INTERVAL 1 DAY)\n" +
                "PROPERTIES (\n" +
                "  \"replication_num\" = \"1\"\n" +
                ")\n" +
                "AS\n" +
                "SELECT\n" +
                "  _ta0000.d_moy\n" +
                "  ,_ta0000.d_year\n" +
                "  ,_ta0000.w_warehouse_name\n" +
                "  ,_ta0000.w_warehouse_sk\n" +
                "  ,_ta0000.i_item_sk\n" +
                "  , (stddev_samp(_ta0000.inv_quantity_on_hand)) AS _ca0003\n" +
                "  ,(sum(_ta0000.inv_quantity_on_hand)) AS _ca0004\n" +
                "  ,(count(_ta0000.inv_quantity_on_hand)) AS _ca0005\n" +
                "FROM\n" +
                "  (\n" +
                "    SELECT\n" +
                "      warehouse.w_warehouse_sk\n" +
                "      ,warehouse.w_warehouse_name\n" +
                "      ,date_dim.d_moy\n" +
                "      ,inventory.inv_quantity_on_hand\n" +
                "      ,item.i_item_sk\n" +
                "      ,date_dim.d_year\n" +
                "    FROM\n" +
                "      inventory\n" +
                "      INNER JOIN\n" +
                "      item\n" +
                "      ON (inventory.inv_item_sk = item.i_item_sk)\n" +
                "      INNER JOIN\n" +
                "      warehouse\n" +
                "      ON (inventory.inv_warehouse_sk = warehouse.w_warehouse_sk)\n" +
                "      INNER JOIN\n" +
                "      date_dim\n" +
                "      ON (inventory.inv_date_sk = date_dim.d_date_sk)\n" +
                "  ) _ta0000\n" +
                "GROUP BY\n" +
                "  _ta0000.d_moy\n" +
                "  , _ta0000.d_year\n" +
                "  , _ta0000.w_warehouse_name\n" +
                "  , _ta0000.w_warehouse_sk\n" +
                "  , _ta0000.i_item_sk;");
        {
            // q39-1
            MVRewriteChecker checker = sql(TPCDSPlanTestBase.Q39_1);
            checker.contains("__mv__ta0006");
        }

        {
            // q39-2
            MVRewriteChecker checker = sql(TPCDSPlanTestBase.Q39_2);
            checker.contains("__mv__ta0006");
        }
    }

    @Test
    public void testQuery83() throws Exception {
        connectContext.executeSql("drop materialized view if exists __mv__ta0007");
        createAndRefreshMV("test", "CREATE MATERIALIZED VIEW __mv__ta0007 (_ca0006, i_item_id)\n" +
                "DISTRIBUTED BY HASH (i_item_id)\n" +
                "REFRESH ASYNC START(\"2023-12-01 10:00:00\") EVERY(INTERVAL 1 DAY)\n" +
                "PROPERTIES (\n" +
                "  \"replication_num\" = \"1\"\n" +
                ")\n" +
                "AS\n" +
                "SELECT\n" +
                "  (sum(_ta0003.sr_return_quantity)) AS _ca0006\n" +
                "  ,_ta0003.i_item_id\n" +
                "FROM\n" +
                "  (\n" +
                "    SELECT\n" +
                "      item.i_item_id\n" +
                "      ,store_returns.sr_return_quantity\n" +
                "    FROM\n" +
                "      store_returns\n" +
                "      INNER JOIN\n" +
                "      item\n" +
                "      ON (store_returns.sr_item_sk = item.i_item_sk)\n" +
                "      INNER JOIN\n" +
                "      date_dim\n" +
                "      ON (store_returns.sr_returned_date_sk = date_dim.d_date_sk)\n" +
                "      LEFT SEMI JOIN\n" +
                "      (\n" +
                "        SELECT\n" +
                "          _ta0000.d_date\n" +
                "        FROM\n" +
                "          (\n" +
                "            SELECT\n" +
                "              date_dim.d_date\n" +
                "              ,date_dim.d_week_seq\n" +
                "            FROM\n" +
                "              date_dim\n" +
                "          ) _ta0000\n" +
                "          LEFT SEMI JOIN\n" +
                "          (\n" +
                "            SELECT\n" +
                "              date_dim.d_week_seq\n" +
                "              ,date_dim.d_date\n" +
                "            FROM\n" +
                "              date_dim\n" +
                "          ) _ta0001\n" +
                "          ON (_ta0000.d_week_seq = _ta0001.d_week_seq)\n" +
                "             AND (_ta0001.d_week_seq IS NOT NULL)\n" +
                "             AND _ta0001.d_date in (\"2000-06-30\", \"2000-09-27\", \"2000-11-17\")\n" +
                "      ) _ta0002\n" +
                "      ON (date_dim.d_date = _ta0002.d_date)\n" +
                "         AND (_ta0002.d_date IS NOT NULL)\n" +
                "  ) _ta0003\n" +
                "GROUP BY\n" +
                "  _ta0003.i_item_id;");
        {
            MVRewriteChecker checker = sql(TPCDSPlanTestBase.Q83);
            checker.contains("__mv__ta0007");
        }
    }
}
