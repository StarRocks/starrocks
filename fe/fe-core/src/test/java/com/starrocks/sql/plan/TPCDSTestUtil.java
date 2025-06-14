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

import com.starrocks.utframe.StarRocksAssert;

public class TPCDSTestUtil {
    public static void prepareTables(StarRocksAssert starRocksAssert) throws Exception {
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
    }
}
