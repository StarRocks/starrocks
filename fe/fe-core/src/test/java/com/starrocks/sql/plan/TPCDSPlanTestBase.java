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

import com.google.common.collect.ImmutableMap;
import com.starrocks.catalog.OlapTable;
import org.junit.BeforeClass;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class TPCDSPlanTestBase extends PlanTestBase {
    private static final Map<String, Long> ROW_COUNT_MAP = ImmutableMap.<String, Long>builder()
            .put("call_center", 6L)
            .put("catalog_page", 11718L)
            .put("catalog_returns", 144067L)
            .put("catalog_sales", 1441548L)
            .put("customer", 100000L)
            .put("customer_address", 50000L)
            .put("customer_demographics", 1920800L)
            .put("date_dim", 73049L)
            .put("household_demographics", 7200L)
            .put("income_band", 20L)
            .put("inventory", 11745000L)
            .put("item", 18000L)
            .put("promotion", 300L)
            .put("reason", 35L)
            .put("ship_mode", 20L)
            .put("store", 12L)
            .put("store_returns", 287514L)
            .put("store_sales", 2880404L)
            .put("time_dim", 86400L)
            .put("warehouse", 5L)
            .put("web_page", 60L)
            .put("web_returns", 71763L)
            .put("web_sales", 719384L)
            .put("web_site", 30L)
            .build();

    public void setTPCDSFactor(int factor) {
        ROW_COUNT_MAP.forEach((t, v) -> {
            OlapTable table = getOlapTable(t);
            setTableStatistics(table, v * factor);
        });
    }

    public Map<String, Long> getTPCDSTableStats() {
        Map<String, Long> m = new HashMap<>();
        ROW_COUNT_MAP.forEach((t, v) -> {
            OlapTable table = getOlapTable(t);
            m.put(t, table.getRowCount());
        });
        return m;
    }

    public void setTPCDSTableStats(Map<String, Long> m) {
        ROW_COUNT_MAP.forEach((t, s) -> {
            if (m.containsKey(t)) {
                long v = m.get(t);
                OlapTable table = getOlapTable(t);
                setTableStatistics(table, v);
            }
        });
    }

    @BeforeClass
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();

        starRocksAssert.dropTable("customer");

        starRocksAssert.withTable("CREATE TABLE `call_center` (\n" +
                "  `cc_call_center_sk` int(11) NOT NULL COMMENT \"\",\n" +
                "  `cc_call_center_id` char(16) NOT NULL COMMENT \"\",\n" +
                "  `cc_rec_start_date` date NULL COMMENT \"\",\n" +
                "  `cc_rec_end_date` date NULL COMMENT \"\",\n" +
                "  `cc_closed_date_sk` int(11) NULL COMMENT \"\",\n" +
                "  `cc_open_date_sk` int(11) NULL COMMENT \"\",\n" +
                "  `cc_name` varchar(50) NULL COMMENT \"\",\n" +
                "  `cc_class` varchar(50) NULL COMMENT \"\",\n" +
                "  `cc_employees` int(11) NULL COMMENT \"\",\n" +
                "  `cc_sq_ft` int(11) NULL COMMENT \"\",\n" +
                "  `cc_hours` char(20) NULL COMMENT \"\",\n" +
                "  `cc_manager` varchar(40) NULL COMMENT \"\",\n" +
                "  `cc_mkt_id` int(11) NULL COMMENT \"\",\n" +
                "  `cc_mkt_class` char(50) NULL COMMENT \"\",\n" +
                "  `cc_mkt_desc` varchar(100) NULL COMMENT \"\",\n" +
                "  `cc_market_manager` varchar(40) NULL COMMENT \"\",\n" +
                "  `cc_division` int(11) NULL COMMENT \"\",\n" +
                "  `cc_division_name` varchar(50) NULL COMMENT \"\",\n" +
                "  `cc_company` int(11) NULL COMMENT \"\",\n" +
                "  `cc_company_name` char(50) NULL COMMENT \"\",\n" +
                "  `cc_street_number` char(10) NULL COMMENT \"\",\n" +
                "  `cc_street_name` varchar(60) NULL COMMENT \"\",\n" +
                "  `cc_street_type` char(15) NULL COMMENT \"\",\n" +
                "  `cc_suite_number` char(10) NULL COMMENT \"\",\n" +
                "  `cc_city` varchar(60) NULL COMMENT \"\",\n" +
                "  `cc_county` varchar(30) NULL COMMENT \"\",\n" +
                "  `cc_state` char(2) NULL COMMENT \"\",\n" +
                "  `cc_zip` char(10) NULL COMMENT \"\",\n" +
                "  `cc_country` varchar(20) NULL COMMENT \"\",\n" +
                "  `cc_gmt_offset` decimal(5, 2) NULL COMMENT \"\",\n" +
                "  `cc_tax_percentage` decimal(5, 2) NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`cc_call_center_sk`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`cc_call_center_sk`) BUCKETS 5\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\"\n" +
                ");");

        starRocksAssert.withTable("CREATE TABLE `catalog_page` (\n" +
                "  `cp_catalog_page_sk` int(11) NOT NULL COMMENT \"\",\n" +
                "  `cp_catalog_page_id` char(16) NOT NULL COMMENT \"\",\n" +
                "  `cp_start_date_sk` int(11) NULL COMMENT \"\",\n" +
                "  `cp_end_date_sk` int(11) NULL COMMENT \"\",\n" +
                "  `cp_department` varchar(50) NULL COMMENT \"\",\n" +
                "  `cp_catalog_number` int(11) NULL COMMENT \"\",\n" +
                "  `cp_catalog_page_number` int(11) NULL COMMENT \"\",\n" +
                "  `cp_description` varchar(100) NULL COMMENT \"\",\n" +
                "  `cp_type` varchar(100) NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`cp_catalog_page_sk`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`cp_catalog_page_sk`) BUCKETS 5\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\"\n" +
                ");");

        starRocksAssert.withTable("CREATE TABLE `catalog_returns` (\n" +
                "  `cr_item_sk` int(11) NOT NULL COMMENT \"\",\n" +
                "  `cr_order_number` int(11) NOT NULL COMMENT \"\",\n" +
                "  `cr_returned_date_sk` int(11) NULL COMMENT \"\",\n" +
                "  `cr_returned_time_sk` int(11) NULL COMMENT \"\",\n" +
                "  `cr_refunded_customer_sk` int(11) NULL COMMENT \"\",\n" +
                "  `cr_refunded_cdemo_sk` int(11) NULL COMMENT \"\",\n" +
                "  `cr_refunded_hdemo_sk` int(11) NULL COMMENT \"\",\n" +
                "  `cr_refunded_addr_sk` int(11) NULL COMMENT \"\",\n" +
                "  `cr_returning_customer_sk` int(11) NULL COMMENT \"\",\n" +
                "  `cr_returning_cdemo_sk` int(11) NULL COMMENT \"\",\n" +
                "  `cr_returning_hdemo_sk` int(11) NULL COMMENT \"\",\n" +
                "  `cr_returning_addr_sk` int(11) NULL COMMENT \"\",\n" +
                "  `cr_call_center_sk` int(11) NULL COMMENT \"\",\n" +
                "  `cr_catalog_page_sk` int(11) NULL COMMENT \"\",\n" +
                "  `cr_ship_mode_sk` int(11) NULL COMMENT \"\",\n" +
                "  `cr_warehouse_sk` int(11) NULL COMMENT \"\",\n" +
                "  `cr_reason_sk` int(11) NULL COMMENT \"\",\n" +
                "  `cr_return_quantity` int(11) NULL COMMENT \"\",\n" +
                "  `cr_return_amount` decimal(7, 2) NULL COMMENT \"\",\n" +
                "  `cr_return_tax` decimal(7, 2) NULL COMMENT \"\",\n" +
                "  `cr_return_amt_inc_tax` decimal(7, 2) NULL COMMENT \"\",\n" +
                "  `cr_fee` decimal(7, 2) NULL COMMENT \"\",\n" +
                "  `cr_return_ship_cost` decimal(7, 2) NULL COMMENT \"\",\n" +
                "  `cr_refunded_cash` decimal(7, 2) NULL COMMENT \"\",\n" +
                "  `cr_reversed_charge` decimal(7, 2) NULL COMMENT \"\",\n" +
                "  `cr_store_credit` decimal(7, 2) NULL COMMENT \"\",\n" +
                "  `cr_net_loss` decimal(7, 2) NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`cr_item_sk`, `cr_order_number`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`cr_item_sk`, `cr_order_number`) BUCKETS 5\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\"\n" +
                ");");

        starRocksAssert.withTable("CREATE TABLE `catalog_sales` (\n" +
                "  `cs_item_sk` int(11) NOT NULL COMMENT \"\",\n" +
                "  `cs_order_number` int(11) NOT NULL COMMENT \"\",\n" +
                "  `cs_sold_date_sk` int(11) NULL COMMENT \"\",\n" +
                "  `cs_sold_time_sk` int(11) NULL COMMENT \"\",\n" +
                "  `cs_ship_date_sk` int(11) NULL COMMENT \"\",\n" +
                "  `cs_bill_customer_sk` int(11) NULL COMMENT \"\",\n" +
                "  `cs_bill_cdemo_sk` int(11) NULL COMMENT \"\",\n" +
                "  `cs_bill_hdemo_sk` int(11) NULL COMMENT \"\",\n" +
                "  `cs_bill_addr_sk` int(11) NULL COMMENT \"\",\n" +
                "  `cs_ship_customer_sk` int(11) NULL COMMENT \"\",\n" +
                "  `cs_ship_cdemo_sk` int(11) NULL COMMENT \"\",\n" +
                "  `cs_ship_hdemo_sk` int(11) NULL COMMENT \"\",\n" +
                "  `cs_ship_addr_sk` int(11) NULL COMMENT \"\",\n" +
                "  `cs_call_center_sk` int(11) NULL COMMENT \"\",\n" +
                "  `cs_catalog_page_sk` int(11) NULL COMMENT \"\",\n" +
                "  `cs_ship_mode_sk` int(11) NULL COMMENT \"\",\n" +
                "  `cs_warehouse_sk` int(11) NULL COMMENT \"\",\n" +
                "  `cs_promo_sk` int(11) NULL COMMENT \"\",\n" +
                "  `cs_quantity` int(11) NULL COMMENT \"\",\n" +
                "  `cs_wholesale_cost` decimal(7, 2) NULL COMMENT \"\",\n" +
                "  `cs_list_price` decimal(7, 2) NULL COMMENT \"\",\n" +
                "  `cs_sales_price` decimal(7, 2) NULL COMMENT \"\",\n" +
                "  `cs_ext_discount_amt` decimal(7, 2) NULL COMMENT \"\",\n" +
                "  `cs_ext_sales_price` decimal(7, 2) NULL COMMENT \"\",\n" +
                "  `cs_ext_wholesale_cost` decimal(7, 2) NULL COMMENT \"\",\n" +
                "  `cs_ext_list_price` decimal(7, 2) NULL COMMENT \"\",\n" +
                "  `cs_ext_tax` decimal(7, 2) NULL COMMENT \"\",\n" +
                "  `cs_coupon_amt` decimal(7, 2) NULL COMMENT \"\",\n" +
                "  `cs_ext_ship_cost` decimal(7, 2) NULL COMMENT \"\",\n" +
                "  `cs_net_paid` decimal(7, 2) NULL COMMENT \"\",\n" +
                "  `cs_net_paid_inc_tax` decimal(7, 2) NULL COMMENT \"\",\n" +
                "  `cs_net_paid_inc_ship` decimal(7, 2) NULL COMMENT \"\",\n" +
                "  `cs_net_paid_inc_ship_tax` decimal(7, 2) NULL COMMENT \"\",\n" +
                "  `cs_net_profit` decimal(7, 2) NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`cs_item_sk`, `cs_order_number`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`cs_item_sk`, `cs_order_number`) BUCKETS 5\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\"\n" +
                ");");

        starRocksAssert.withTable("CREATE TABLE `customer` (\n" +
                "  `c_customer_sk` int(11) NOT NULL COMMENT \"\",\n" +
                "  `c_customer_id` char(16) NOT NULL COMMENT \"\",\n" +
                "  `c_current_cdemo_sk` int(11) NULL COMMENT \"\",\n" +
                "  `c_current_hdemo_sk` int(11) NULL COMMENT \"\",\n" +
                "  `c_current_addr_sk` int(11) NULL COMMENT \"\",\n" +
                "  `c_first_shipto_date_sk` int(11) NULL COMMENT \"\",\n" +
                "  `c_first_sales_date_sk` int(11) NULL COMMENT \"\",\n" +
                "  `c_salutation` char(10) NULL COMMENT \"\",\n" +
                "  `c_first_name` char(20) NULL COMMENT \"\",\n" +
                "  `c_last_name` char(30) NULL COMMENT \"\",\n" +
                "  `c_preferred_cust_flag` char(1) NULL COMMENT \"\",\n" +
                "  `c_birth_day` int(11) NULL COMMENT \"\",\n" +
                "  `c_birth_month` int(11) NULL COMMENT \"\",\n" +
                "  `c_birth_year` int(11) NULL COMMENT \"\",\n" +
                "  `c_birth_country` varchar(20) NULL COMMENT \"\",\n" +
                "  `c_login` char(13) NULL COMMENT \"\",\n" +
                "  `c_email_address` char(50) NULL COMMENT \"\",\n" +
                "  `c_last_review_date` char(10) NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`c_customer_sk`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`c_customer_sk`) BUCKETS 5\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\"\n" +
                ");");

        starRocksAssert.withTable("CREATE TABLE `customer_address` (\n" +
                "  `ca_address_sk` int(11) NOT NULL COMMENT \"\",\n" +
                "  `ca_address_id` char(16) NOT NULL COMMENT \"\",\n" +
                "  `ca_street_number` char(10) NULL COMMENT \"\",\n" +
                "  `ca_street_name` varchar(60) NULL COMMENT \"\",\n" +
                "  `ca_street_type` char(15) NULL COMMENT \"\",\n" +
                "  `ca_suite_number` char(10) NULL COMMENT \"\",\n" +
                "  `ca_city` varchar(60) NULL COMMENT \"\",\n" +
                "  `ca_county` varchar(30) NULL COMMENT \"\",\n" +
                "  `ca_state` char(2) NULL COMMENT \"\",\n" +
                "  `ca_zip` char(10) NULL COMMENT \"\",\n" +
                "  `ca_country` varchar(20) NULL COMMENT \"\",\n" +
                "  `ca_gmt_offset` decimal(5, 2) NULL COMMENT \"\",\n" +
                "  `ca_location_type` char(20) NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`ca_address_sk`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`ca_address_sk`) BUCKETS 5\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\"\n" +
                ");");

        starRocksAssert.withTable("CREATE TABLE `customer_demographics` (\n" +
                "  `cd_demo_sk` int(11) NOT NULL COMMENT \"\",\n" +
                "  `cd_gender` char(1) NULL COMMENT \"\",\n" +
                "  `cd_marital_status` char(1) NULL COMMENT \"\",\n" +
                "  `cd_education_status` char(20) NULL COMMENT \"\",\n" +
                "  `cd_purchase_estimate` int(11) NULL COMMENT \"\",\n" +
                "  `cd_credit_rating` char(10) NULL COMMENT \"\",\n" +
                "  `cd_dep_count` int(11) NULL COMMENT \"\",\n" +
                "  `cd_dep_employed_count` int(11) NULL COMMENT \"\",\n" +
                "  `cd_dep_college_count` int(11) NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`cd_demo_sk`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`cd_demo_sk`) BUCKETS 5\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\"\n" +
                ");");

        starRocksAssert.withTable("CREATE TABLE `date_dim` (\n" +
                "  `d_date_sk` int(11) NOT NULL COMMENT \"\",\n" +
                "  `d_date_id` char(16) NOT NULL COMMENT \"\",\n" +
                "  `d_date` date NULL COMMENT \"\",\n" +
                "  `d_month_seq` int(11) NULL COMMENT \"\",\n" +
                "  `d_week_seq` int(11) NULL COMMENT \"\",\n" +
                "  `d_quarter_seq` int(11) NULL COMMENT \"\",\n" +
                "  `d_year` int(11) NULL COMMENT \"\",\n" +
                "  `d_dow` int(11) NULL COMMENT \"\",\n" +
                "  `d_moy` int(11) NULL COMMENT \"\",\n" +
                "  `d_dom` int(11) NULL COMMENT \"\",\n" +
                "  `d_qoy` int(11) NULL COMMENT \"\",\n" +
                "  `d_fy_year` int(11) NULL COMMENT \"\",\n" +
                "  `d_fy_quarter_seq` int(11) NULL COMMENT \"\",\n" +
                "  `d_fy_week_seq` int(11) NULL COMMENT \"\",\n" +
                "  `d_day_name` char(9) NULL COMMENT \"\",\n" +
                "  `d_quarter_name` char(6) NULL COMMENT \"\",\n" +
                "  `d_holiday` char(1) NULL COMMENT \"\",\n" +
                "  `d_weekend` char(1) NULL COMMENT \"\",\n" +
                "  `d_following_holiday` char(1) NULL COMMENT \"\",\n" +
                "  `d_first_dom` int(11) NULL COMMENT \"\",\n" +
                "  `d_last_dom` int(11) NULL COMMENT \"\",\n" +
                "  `d_same_day_ly` int(11) NULL COMMENT \"\",\n" +
                "  `d_same_day_lq` int(11) NULL COMMENT \"\",\n" +
                "  `d_current_day` char(1) NULL COMMENT \"\",\n" +
                "  `d_current_week` char(1) NULL COMMENT \"\",\n" +
                "  `d_current_month` char(1) NULL COMMENT \"\",\n" +
                "  `d_current_quarter` char(1) NULL COMMENT \"\",\n" +
                "  `d_current_year` char(1) NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`d_date_sk`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`d_date_sk`) BUCKETS 5\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\"\n" +
                ");");

        starRocksAssert.withTable("CREATE TABLE `household_demographics` (\n" +
                "  `hd_demo_sk` int(11) NOT NULL COMMENT \"\",\n" +
                "  `hd_income_band_sk` int(11) NULL COMMENT \"\",\n" +
                "  `hd_buy_potential` char(15) NULL COMMENT \"\",\n" +
                "  `hd_dep_count` int(11) NULL COMMENT \"\",\n" +
                "  `hd_vehicle_count` int(11) NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`hd_demo_sk`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`hd_demo_sk`) BUCKETS 5\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\"\n" +
                ");");

        starRocksAssert.withTable("CREATE TABLE `income_band` (\n" +
                "  `ib_income_band_sk` int(11) NOT NULL COMMENT \"\",\n" +
                "  `ib_lower_bound` int(11) NULL COMMENT \"\",\n" +
                "  `ib_upper_bound` int(11) NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`ib_income_band_sk`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`ib_income_band_sk`) BUCKETS 5\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\"\n" +
                ");");

        starRocksAssert.withTable("CREATE TABLE `inventory` (\n" +
                "  `inv_date_sk` int(11) NOT NULL COMMENT \"\",\n" +
                "  `inv_item_sk` int(11) NOT NULL COMMENT \"\",\n" +
                "  `inv_warehouse_sk` int(11) NOT NULL COMMENT \"\",\n" +
                "  `inv_quantity_on_hand` int(11) NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`inv_date_sk`, `inv_item_sk`, `inv_warehouse_sk`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`inv_date_sk`, `inv_item_sk`, `inv_warehouse_sk`) BUCKETS 5\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\"\n" +
                ");");

        starRocksAssert.withTable("CREATE TABLE `item` (\n" +
                "  `i_item_sk` int(11) NOT NULL COMMENT \"\",\n" +
                "  `i_item_id` char(16) NOT NULL COMMENT \"\",\n" +
                "  `i_rec_start_date` date NULL COMMENT \"\",\n" +
                "  `i_rec_end_date` date NULL COMMENT \"\",\n" +
                "  `i_item_desc` varchar(200) NULL COMMENT \"\",\n" +
                "  `i_current_price` decimal(7, 2) NULL COMMENT \"\",\n" +
                "  `i_wholesale_cost` decimal(7, 2) NULL COMMENT \"\",\n" +
                "  `i_brand_id` int(11) NULL COMMENT \"\",\n" +
                "  `i_brand` char(50) NULL COMMENT \"\",\n" +
                "  `i_class_id` int(11) NULL COMMENT \"\",\n" +
                "  `i_class` char(50) NULL COMMENT \"\",\n" +
                "  `i_category_id` int(11) NULL COMMENT \"\",\n" +
                "  `i_category` char(50) NULL COMMENT \"\",\n" +
                "  `i_manufact_id` int(11) NULL COMMENT \"\",\n" +
                "  `i_manufact` char(50) NULL COMMENT \"\",\n" +
                "  `i_size` char(20) NULL COMMENT \"\",\n" +
                "  `i_formulation` char(20) NULL COMMENT \"\",\n" +
                "  `i_color` char(20) NULL COMMENT \"\",\n" +
                "  `i_units` char(10) NULL COMMENT \"\",\n" +
                "  `i_container` char(10) NULL COMMENT \"\",\n" +
                "  `i_manager_id` int(11) NULL COMMENT \"\",\n" +
                "  `i_product_name` char(50) NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`i_item_sk`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`i_item_sk`) BUCKETS 5\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\"\n" +
                ");");

        starRocksAssert.withTable("CREATE TABLE `promotion` (\n" +
                "  `p_promo_sk` int(11) NOT NULL COMMENT \"\",\n" +
                "  `p_promo_id` char(16) NOT NULL COMMENT \"\",\n" +
                "  `p_start_date_sk` int(11) NULL COMMENT \"\",\n" +
                "  `p_end_date_sk` int(11) NULL COMMENT \"\",\n" +
                "  `p_item_sk` int(11) NULL COMMENT \"\",\n" +
                "  `p_cost` decimal(15, 2) NULL COMMENT \"\",\n" +
                "  `p_response_target` int(11) NULL COMMENT \"\",\n" +
                "  `p_promo_name` char(50) NULL COMMENT \"\",\n" +
                "  `p_channel_dmail` char(1) NULL COMMENT \"\",\n" +
                "  `p_channel_email` char(1) NULL COMMENT \"\",\n" +
                "  `p_channel_catalog` char(1) NULL COMMENT \"\",\n" +
                "  `p_channel_tv` char(1) NULL COMMENT \"\",\n" +
                "  `p_channel_radio` char(1) NULL COMMENT \"\",\n" +
                "  `p_channel_press` char(1) NULL COMMENT \"\",\n" +
                "  `p_channel_event` char(1) NULL COMMENT \"\",\n" +
                "  `p_channel_demo` char(1) NULL COMMENT \"\",\n" +
                "  `p_channel_details` varchar(100) NULL COMMENT \"\",\n" +
                "  `p_purpose` char(15) NULL COMMENT \"\",\n" +
                "  `p_discount_active` char(1) NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`p_promo_sk`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`p_promo_sk`) BUCKETS 5\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\"\n" +
                ");");

        starRocksAssert.withTable("CREATE TABLE `reason` (\n" +
                "  `r_reason_sk` int(11) NOT NULL COMMENT \"\",\n" +
                "  `r_reason_id` char(16) NOT NULL COMMENT \"\",\n" +
                "  `r_reason_desc` char(100) NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`r_reason_sk`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`r_reason_sk`) BUCKETS 5\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\"\n" +
                ");");

        starRocksAssert.withTable("CREATE TABLE `ship_mode` (\n" +
                "  `sm_ship_mode_sk` int(11) NOT NULL COMMENT \"\",\n" +
                "  `sm_ship_mode_id` char(16) NOT NULL COMMENT \"\",\n" +
                "  `sm_type` char(30) NULL COMMENT \"\",\n" +
                "  `sm_code` char(10) NULL COMMENT \"\",\n" +
                "  `sm_carrier` char(20) NULL COMMENT \"\",\n" +
                "  `sm_contract` char(20) NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`sm_ship_mode_sk`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`sm_ship_mode_sk`) BUCKETS 5\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\"\n" +
                ");");

        starRocksAssert.withTable("CREATE TABLE `store` (\n" +
                "  `s_store_sk` int(11) NOT NULL COMMENT \"\",\n" +
                "  `s_store_id` char(16) NOT NULL COMMENT \"\",\n" +
                "  `s_rec_start_date` date NULL COMMENT \"\",\n" +
                "  `s_rec_end_date` date NULL COMMENT \"\",\n" +
                "  `s_closed_date_sk` int(11) NULL COMMENT \"\",\n" +
                "  `s_store_name` varchar(50) NULL COMMENT \"\",\n" +
                "  `s_number_employees` int(11) NULL COMMENT \"\",\n" +
                "  `s_floor_space` int(11) NULL COMMENT \"\",\n" +
                "  `s_hours` char(20) NULL COMMENT \"\",\n" +
                "  `s_manager` varchar(40) NULL COMMENT \"\",\n" +
                "  `s_market_id` int(11) NULL COMMENT \"\",\n" +
                "  `s_geography_class` varchar(100) NULL COMMENT \"\",\n" +
                "  `s_market_desc` varchar(100) NULL COMMENT \"\",\n" +
                "  `s_market_manager` varchar(40) NULL COMMENT \"\",\n" +
                "  `s_division_id` int(11) NULL COMMENT \"\",\n" +
                "  `s_division_name` varchar(50) NULL COMMENT \"\",\n" +
                "  `s_company_id` int(11) NULL COMMENT \"\",\n" +
                "  `s_company_name` varchar(50) NULL COMMENT \"\",\n" +
                "  `s_street_number` varchar(10) NULL COMMENT \"\",\n" +
                "  `s_street_name` varchar(60) NULL COMMENT \"\",\n" +
                "  `s_street_type` char(15) NULL COMMENT \"\",\n" +
                "  `s_suite_number` char(10) NULL COMMENT \"\",\n" +
                "  `s_city` varchar(60) NULL COMMENT \"\",\n" +
                "  `s_county` varchar(30) NULL COMMENT \"\",\n" +
                "  `s_state` char(2) NULL COMMENT \"\",\n" +
                "  `s_zip` char(10) NULL COMMENT \"\",\n" +
                "  `s_country` varchar(20) NULL COMMENT \"\",\n" +
                "  `s_gmt_offset` decimal(5, 2) NULL COMMENT \"\",\n" +
                "  `s_tax_precentage` decimal(5, 2) NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`s_store_sk`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`s_store_sk`) BUCKETS 5\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\"\n" +
                ");");

        starRocksAssert.withTable("CREATE TABLE `store_returns` (\n" +
                "  `sr_item_sk` int(11) NOT NULL COMMENT \"\",\n" +
                "  `sr_ticket_number` int(11) NOT NULL COMMENT \"\",\n" +
                "  `sr_returned_date_sk` int(11) NULL COMMENT \"\",\n" +
                "  `sr_return_time_sk` int(11) NULL COMMENT \"\",\n" +
                "  `sr_customer_sk` int(11) NULL COMMENT \"\",\n" +
                "  `sr_cdemo_sk` int(11) NULL COMMENT \"\",\n" +
                "  `sr_hdemo_sk` int(11) NULL COMMENT \"\",\n" +
                "  `sr_addr_sk` int(11) NULL COMMENT \"\",\n" +
                "  `sr_store_sk` int(11) NULL COMMENT \"\",\n" +
                "  `sr_reason_sk` int(11) NULL COMMENT \"\",\n" +
                "  `sr_return_quantity` int(11) NULL COMMENT \"\",\n" +
                "  `sr_return_amt` decimal(7, 2) NULL COMMENT \"\",\n" +
                "  `sr_return_tax` decimal(7, 2) NULL COMMENT \"\",\n" +
                "  `sr_return_amt_inc_tax` decimal(7, 2) NULL COMMENT \"\",\n" +
                "  `sr_fee` decimal(7, 2) NULL COMMENT \"\",\n" +
                "  `sr_return_ship_cost` decimal(7, 2) NULL COMMENT \"\",\n" +
                "  `sr_refunded_cash` decimal(7, 2) NULL COMMENT \"\",\n" +
                "  `sr_reversed_charge` decimal(7, 2) NULL COMMENT \"\",\n" +
                "  `sr_store_credit` decimal(7, 2) NULL COMMENT \"\",\n" +
                "  `sr_net_loss` decimal(7, 2) NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`sr_item_sk`, `sr_ticket_number`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`sr_item_sk`, `sr_ticket_number`) BUCKETS 5\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\"\n" +
                ");");

        starRocksAssert.withTable("CREATE TABLE `store_sales` (\n" +
                "  `ss_item_sk` int(11) NOT NULL COMMENT \"\",\n" +
                "  `ss_ticket_number` int(11) NOT NULL COMMENT \"\",\n" +
                "  `ss_sold_date_sk` int(11) NULL COMMENT \"\",\n" +
                "  `ss_sold_time_sk` int(11) NULL COMMENT \"\",\n" +
                "  `ss_customer_sk` int(11) NULL COMMENT \"\",\n" +
                "  `ss_cdemo_sk` int(11) NULL COMMENT \"\",\n" +
                "  `ss_hdemo_sk` int(11) NULL COMMENT \"\",\n" +
                "  `ss_addr_sk` int(11) NULL COMMENT \"\",\n" +
                "  `ss_store_sk` int(11) NULL COMMENT \"\",\n" +
                "  `ss_promo_sk` int(11) NULL COMMENT \"\",\n" +
                "  `ss_quantity` int(11) NULL COMMENT \"\",\n" +
                "  `ss_wholesale_cost` decimal(7, 2) NULL COMMENT \"\",\n" +
                "  `ss_list_price` decimal(7, 2) NULL COMMENT \"\",\n" +
                "  `ss_sales_price` decimal(7, 2) NULL COMMENT \"\",\n" +
                "  `ss_ext_discount_amt` decimal(7, 2) NULL COMMENT \"\",\n" +
                "  `ss_ext_sales_price` decimal(7, 2) NULL COMMENT \"\",\n" +
                "  `ss_ext_wholesale_cost` decimal(7, 2) NULL COMMENT \"\",\n" +
                "  `ss_ext_list_price` decimal(7, 2) NULL COMMENT \"\",\n" +
                "  `ss_ext_tax` decimal(7, 2) NULL COMMENT \"\",\n" +
                "  `ss_coupon_amt` decimal(7, 2) NULL COMMENT \"\",\n" +
                "  `ss_net_paid` decimal(7, 2) NULL COMMENT \"\",\n" +
                "  `ss_net_paid_inc_tax` decimal(7, 2) NULL COMMENT \"\",\n" +
                "  `ss_net_profit` decimal(7, 2) NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`ss_item_sk`, `ss_ticket_number`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`ss_item_sk`, `ss_ticket_number`) BUCKETS 5\n" +
                "rollup (r1(ss_sold_date_sk,ss_store_sk,ss_item_sk,ss_sales_price))\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\"\n" +
                ");");

        starRocksAssert.withTable("CREATE TABLE `time_dim` (\n" +
                "  `t_time_sk` int(11) NOT NULL COMMENT \"\",\n" +
                "  `t_time_id` char(16) NOT NULL COMMENT \"\",\n" +
                "  `t_time` int(11) NULL COMMENT \"\",\n" +
                "  `t_hour` int(11) NULL COMMENT \"\",\n" +
                "  `t_minute` int(11) NULL COMMENT \"\",\n" +
                "  `t_second` int(11) NULL COMMENT \"\",\n" +
                "  `t_am_pm` char(2) NULL COMMENT \"\",\n" +
                "  `t_shift` char(20) NULL COMMENT \"\",\n" +
                "  `t_sub_shift` char(20) NULL COMMENT \"\",\n" +
                "  `t_meal_time` char(20) NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`t_time_sk`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`t_time_sk`) BUCKETS 5\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\"\n" +
                ");");

        starRocksAssert.withTable("CREATE TABLE `warehouse` (\n" +
                "  `w_warehouse_sk` int(11) NOT NULL COMMENT \"\",\n" +
                "  `w_warehouse_id` char(16) NOT NULL COMMENT \"\",\n" +
                "  `w_warehouse_name` varchar(20) NULL COMMENT \"\",\n" +
                "  `w_warehouse_sq_ft` int(11) NULL COMMENT \"\",\n" +
                "  `w_street_number` char(10) NULL COMMENT \"\",\n" +
                "  `w_street_name` varchar(60) NULL COMMENT \"\",\n" +
                "  `w_street_type` char(15) NULL COMMENT \"\",\n" +
                "  `w_suite_number` char(10) NULL COMMENT \"\",\n" +
                "  `w_city` varchar(60) NULL COMMENT \"\",\n" +
                "  `w_county` varchar(30) NULL COMMENT \"\",\n" +
                "  `w_state` char(2) NULL COMMENT \"\",\n" +
                "  `w_zip` char(10) NULL COMMENT \"\",\n" +
                "  `w_country` varchar(20) NULL COMMENT \"\",\n" +
                "  `w_gmt_offset` decimal(5, 2) NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`w_warehouse_sk`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`w_warehouse_sk`) BUCKETS 5\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\"\n" +
                ");");

        starRocksAssert.withTable("CREATE TABLE `web_page` (\n" +
                "  `wp_web_page_sk` int(11) NOT NULL COMMENT \"\",\n" +
                "  `wp_web_page_id` char(16) NOT NULL COMMENT \"\",\n" +
                "  `wp_rec_start_date` date NULL COMMENT \"\",\n" +
                "  `wp_rec_end_date` date NULL COMMENT \"\",\n" +
                "  `wp_creation_date_sk` int(11) NULL COMMENT \"\",\n" +
                "  `wp_access_date_sk` int(11) NULL COMMENT \"\",\n" +
                "  `wp_autogen_flag` char(1) NULL COMMENT \"\",\n" +
                "  `wp_customer_sk` int(11) NULL COMMENT \"\",\n" +
                "  `wp_url` varchar(100) NULL COMMENT \"\",\n" +
                "  `wp_type` char(50) NULL COMMENT \"\",\n" +
                "  `wp_char_count` int(11) NULL COMMENT \"\",\n" +
                "  `wp_link_count` int(11) NULL COMMENT \"\",\n" +
                "  `wp_image_count` int(11) NULL COMMENT \"\",\n" +
                "  `wp_max_ad_count` int(11) NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`wp_web_page_sk`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`wp_web_page_sk`) BUCKETS 5\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\"\n" +
                ");");

        starRocksAssert.withTable("CREATE TABLE `web_returns` (\n" +
                "  `wr_item_sk` int(11) NOT NULL COMMENT \"\",\n" +
                "  `wr_order_number` int(11) NOT NULL COMMENT \"\",\n" +
                "  `wr_returned_date_sk` int(11) NULL COMMENT \"\",\n" +
                "  `wr_returned_time_sk` int(11) NULL COMMENT \"\",\n" +
                "  `wr_refunded_customer_sk` int(11) NULL COMMENT \"\",\n" +
                "  `wr_refunded_cdemo_sk` int(11) NULL COMMENT \"\",\n" +
                "  `wr_refunded_hdemo_sk` int(11) NULL COMMENT \"\",\n" +
                "  `wr_refunded_addr_sk` int(11) NULL COMMENT \"\",\n" +
                "  `wr_returning_customer_sk` int(11) NULL COMMENT \"\",\n" +
                "  `wr_returning_cdemo_sk` int(11) NULL COMMENT \"\",\n" +
                "  `wr_returning_hdemo_sk` int(11) NULL COMMENT \"\",\n" +
                "  `wr_returning_addr_sk` int(11) NULL COMMENT \"\",\n" +
                "  `wr_web_page_sk` int(11) NULL COMMENT \"\",\n" +
                "  `wr_reason_sk` int(11) NULL COMMENT \"\",\n" +
                "  `wr_return_quantity` int(11) NULL COMMENT \"\",\n" +
                "  `wr_return_amt` decimal(7, 2) NULL COMMENT \"\",\n" +
                "  `wr_return_tax` decimal(7, 2) NULL COMMENT \"\",\n" +
                "  `wr_return_amt_inc_tax` decimal(7, 2) NULL COMMENT \"\",\n" +
                "  `wr_fee` decimal(7, 2) NULL COMMENT \"\",\n" +
                "  `wr_return_ship_cost` decimal(7, 2) NULL COMMENT \"\",\n" +
                "  `wr_refunded_cash` decimal(7, 2) NULL COMMENT \"\",\n" +
                "  `wr_reversed_charge` decimal(7, 2) NULL COMMENT \"\",\n" +
                "  `wr_account_credit` decimal(7, 2) NULL COMMENT \"\",\n" +
                "  `wr_net_loss` decimal(7, 2) NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`wr_item_sk`, `wr_order_number`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`wr_item_sk`, `wr_order_number`) BUCKETS 5\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\"\n" +
                ");");

        starRocksAssert.withTable("CREATE TABLE `web_sales` (\n" +
                "  `ws_item_sk` int(11) NOT NULL COMMENT \"\",\n" +
                "  `ws_order_number` int(11) NOT NULL COMMENT \"\",\n" +
                "  `ws_sold_date_sk` int(11) NULL COMMENT \"\",\n" +
                "  `ws_sold_time_sk` int(11) NULL COMMENT \"\",\n" +
                "  `ws_ship_date_sk` int(11) NULL COMMENT \"\",\n" +
                "  `ws_bill_customer_sk` int(11) NULL COMMENT \"\",\n" +
                "  `ws_bill_cdemo_sk` int(11) NULL COMMENT \"\",\n" +
                "  `ws_bill_hdemo_sk` int(11) NULL COMMENT \"\",\n" +
                "  `ws_bill_addr_sk` int(11) NULL COMMENT \"\",\n" +
                "  `ws_ship_customer_sk` int(11) NULL COMMENT \"\",\n" +
                "  `ws_ship_cdemo_sk` int(11) NULL COMMENT \"\",\n" +
                "  `ws_ship_hdemo_sk` int(11) NULL COMMENT \"\",\n" +
                "  `ws_ship_addr_sk` int(11) NULL COMMENT \"\",\n" +
                "  `ws_web_page_sk` int(11) NULL COMMENT \"\",\n" +
                "  `ws_web_site_sk` int(11) NULL COMMENT \"\",\n" +
                "  `ws_ship_mode_sk` int(11) NULL COMMENT \"\",\n" +
                "  `ws_warehouse_sk` int(11) NULL COMMENT \"\",\n" +
                "  `ws_promo_sk` int(11) NULL COMMENT \"\",\n" +
                "  `ws_quantity` int(11) NULL COMMENT \"\",\n" +
                "  `ws_wholesale_cost` decimal(7, 2) NULL COMMENT \"\",\n" +
                "  `ws_list_price` decimal(7, 2) NULL COMMENT \"\",\n" +
                "  `ws_sales_price` decimal(7, 2) NULL COMMENT \"\",\n" +
                "  `ws_ext_discount_amt` decimal(7, 2) NULL COMMENT \"\",\n" +
                "  `ws_ext_sales_price` decimal(7, 2) NULL COMMENT \"\",\n" +
                "  `ws_ext_wholesale_cost` decimal(7, 2) NULL COMMENT \"\",\n" +
                "  `ws_ext_list_price` decimal(7, 2) NULL COMMENT \"\",\n" +
                "  `ws_ext_tax` decimal(7, 2) NULL COMMENT \"\",\n" +
                "  `ws_coupon_amt` decimal(7, 2) NULL COMMENT \"\",\n" +
                "  `ws_ext_ship_cost` decimal(7, 2) NULL COMMENT \"\",\n" +
                "  `ws_net_paid` decimal(7, 2) NULL COMMENT \"\",\n" +
                "  `ws_net_paid_inc_tax` decimal(7, 2) NULL COMMENT \"\",\n" +
                "  `ws_net_paid_inc_ship` decimal(7, 2) NULL COMMENT \"\",\n" +
                "  `ws_net_paid_inc_ship_tax` decimal(7, 2) NULL COMMENT \"\",\n" +
                "  `ws_net_profit` decimal(7, 2) NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`ws_item_sk`, `ws_order_number`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`ws_item_sk`, `ws_order_number`) BUCKETS 5\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\"\n" +
                ");");

        starRocksAssert.withTable("CREATE TABLE `web_site` (\n" +
                "  `web_site_sk` int(11) NOT NULL COMMENT \"\",\n" +
                "  `web_site_id` char(16) NOT NULL COMMENT \"\",\n" +
                "  `web_rec_start_date` date NULL COMMENT \"\",\n" +
                "  `web_rec_end_date` date NULL COMMENT \"\",\n" +
                "  `web_name` varchar(50) NULL COMMENT \"\",\n" +
                "  `web_open_date_sk` int(11) NULL COMMENT \"\",\n" +
                "  `web_close_date_sk` int(11) NULL COMMENT \"\",\n" +
                "  `web_class` varchar(50) NULL COMMENT \"\",\n" +
                "  `web_manager` varchar(40) NULL COMMENT \"\",\n" +
                "  `web_mkt_id` int(11) NULL COMMENT \"\",\n" +
                "  `web_mkt_class` varchar(50) NULL COMMENT \"\",\n" +
                "  `web_mkt_desc` varchar(100) NULL COMMENT \"\",\n" +
                "  `web_market_manager` varchar(40) NULL COMMENT \"\",\n" +
                "  `web_company_id` int(11) NULL COMMENT \"\",\n" +
                "  `web_company_name` char(50) NULL COMMENT \"\",\n" +
                "  `web_street_number` char(10) NULL COMMENT \"\",\n" +
                "  `web_street_name` varchar(60) NULL COMMENT \"\",\n" +
                "  `web_street_type` char(15) NULL COMMENT \"\",\n" +
                "  `web_suite_number` char(10) NULL COMMENT \"\",\n" +
                "  `web_city` varchar(60) NULL COMMENT \"\",\n" +
                "  `web_county` varchar(30) NULL COMMENT \"\",\n" +
                "  `web_state` char(2) NULL COMMENT \"\",\n" +
                "  `web_zip` char(10) NULL COMMENT \"\",\n" +
                "  `web_country` varchar(20) NULL COMMENT \"\",\n" +
                "  `web_gmt_offset` decimal(5, 2) NULL COMMENT \"\",\n" +
                "  `web_tax_percentage` decimal(5, 2) NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`web_site_sk`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`web_site_sk`) BUCKETS 5\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\"\n" +
                ");");

        connectContext.getSessionVariable().setMaxTransformReorderJoins(3);
        connectContext.getSessionVariable().setSqlMode(2);
    }

    private static String from(String name) {
        String path = Objects.requireNonNull(ClassLoader.getSystemClassLoader().getResource("sql")).getPath();
        File file = new File(path + "/tpcds/query" + name + ".sql");
        StringBuilder sb = new StringBuilder();
        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
            String str;
            while ((str = reader.readLine()) != null) {
                sb.append(str).append("\n");
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return sb.toString();
    }

    public static final String Q01 = from("01");
    public static final String Q02 = from("02");
    public static final String Q03 = from("03");
    public static final String Q04 = from("04");
    public static final String Q05 = from("05");
    public static final String Q06 = from("06");
    public static final String Q07 = from("07");
    public static final String Q08 = from("08");
    public static final String Q09 = from("09");
    public static final String Q10 = from("10");
    public static final String Q11 = from("11");
    public static final String Q12 = from("12");
    public static final String Q13 = from("13");
    public static final String Q14_1 = from("14-1");
    public static final String Q14_2 = from("14-2");
    public static final String Q15 = from("15");
    public static final String Q16 = from("16");
    public static final String Q17 = from("17");
    public static final String Q18 = from("18");
    public static final String Q19 = from("19");
    public static final String Q20 = from("20");
    public static final String Q21 = from("21");
    public static final String Q22 = from("22");
    public static final String Q23_1 = from("23-1");
    public static final String Q23_2 = from("23-2");
    public static final String Q24_1 = from("24-1");
    public static final String Q24_2 = from("24-2");
    public static final String Q25 = from("25");
    public static final String Q26 = from("26");
    public static final String Q27 = from("27");
    public static final String Q28 = from("28");
    public static final String Q29 = from("29");
    public static final String Q30 = from("30");
    public static final String Q31 = from("31");
    public static final String Q32 = from("32");
    public static final String Q33 = from("33");
    public static final String Q34 = from("34");
    public static final String Q35 = from("35");
    public static final String Q36 = from("36");
    public static final String Q37 = from("37");
    public static final String Q38 = from("38");
    public static final String Q39_1 = from("39-1");
    public static final String Q39_2 = from("39-2");
    public static final String Q40 = from("40");
    public static final String Q41 = from("41");
    public static final String Q42 = from("42");
    public static final String Q43 = from("43");
    public static final String Q44 = from("44");
    public static final String Q45 = from("45");
    public static final String Q46 = from("46");
    public static final String Q47 = from("47");
    public static final String Q48 = from("48");
    public static final String Q49 = from("49");
    public static final String Q50 = from("50");
    public static final String Q51 = from("51");
    public static final String Q52 = from("52");
    public static final String Q53 = from("53");
    public static final String Q54 = from("54");
    public static final String Q55 = from("55");
    public static final String Q56 = from("56");
    public static final String Q57 = from("57");
    public static final String Q58 = from("58");
    public static final String Q59 = from("59");
    public static final String Q60 = from("60");
    public static final String Q61 = from("61");
    public static final String Q62 = from("62");
    public static final String Q63 = from("63");
    public static final String Q64 = from("64");
    public static final String Q65 = from("65");
    public static final String Q66 = from("66");
    public static final String Q67 = from("67");
    public static final String Q68 = from("68");
    public static final String Q69 = from("69");
    public static final String Q70 = from("70");
    public static final String Q71 = from("71");
    public static final String Q72 = from("72");
    public static final String Q73 = from("73");
    public static final String Q74 = from("74");
    public static final String Q75 = from("75");
    public static final String Q76 = from("76");
    public static final String Q77 = from("77");
    public static final String Q78 = from("78");
    public static final String Q79 = from("79");
    public static final String Q80 = from("80");
    public static final String Q81 = from("81");
    public static final String Q82 = from("82");
    public static final String Q83 = from("83");
    public static final String Q84 = from("84");
    public static final String Q85 = from("85");
    public static final String Q86 = from("86");
    public static final String Q87 = from("87");
    public static final String Q88 = from("88");
    public static final String Q89 = from("89");
    public static final String Q90 = from("90");
    public static final String Q91 = from("91");
    public static final String Q92 = from("92");
    public static final String Q93 = from("93");
    public static final String Q94 = from("94");
    public static final String Q95 = from("95");
    public static final String Q96 = from("96");
    public static final String Q97 = from("97");
    public static final String Q98 = from("98");
    public static final String Q99 = from("99");

    public String getTPCDS(String name) {
        Class<TPCDSPlanTestBase> clazz = TPCDSPlanTestBase.class;
        try {
            Field f = clazz.getField(name);
            return (String) f.get(this);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }
}
