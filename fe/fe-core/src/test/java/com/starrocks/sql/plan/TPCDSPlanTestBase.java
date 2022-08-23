// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.plan;

import com.starrocks.catalog.OlapTable;
import org.junit.BeforeClass;

import java.util.HashMap;
import java.util.Map;

public class TPCDSPlanTestBase extends PlanTestBase {
    protected static final String[] tpcdsTables =
            {"call_center", "catalog_page", "catalog_returns", "catalog_sales", "customer", "customer_address",
                    "customer_demographics", "date_dim", "household_demographics", "income_band", "inventory", "item",
                    "promotion", "reason", "ship_mode", "store", "store_returns", "store_sales", "time_dim",
                    "warehouse", "web_page", "web_returns", "web_sales", "web_site"};
    protected static final int[] tpcdsTablesRowCount =
            {6, 11718, 144067, 1441548, 100000, 50000, 1920800, 73049, 7200, 20, 11745000, 18000, 300, 35, 20, 12,
                    287514, 2880404, 86400, 5, 60, 71763, 719384, 30};

    public void setTPCDSFactor(int factor) {
        for (int i = 0; i < tpcdsTables.length; i++) {
            String t = tpcdsTables[i];
            long v = tpcdsTablesRowCount[i];
            OlapTable table = getOlapTable(t);
            setTableStatistics(table, v * factor);
        }
    }

    public Map<String, Long> getTPCDSTableStats() {
        Map<String, Long> m = new HashMap<>();
        for (int i = 0; i < tpcdsTables.length; i++) {
            String t = tpcdsTables[i];
            long v = tpcdsTablesRowCount[i];
            OlapTable table = getOlapTable(t);
            m.put(t, table.getRowCount());
        }
        return m;
    }

    public void setTPCDSTableStats(Map<String, Long> m) {
        for (int i = 0; i < tpcdsTables.length; i++) {
            String t = tpcdsTables[i];
            if (m.containsKey(t)) {
                long v = m.get(t);
                OlapTable table = getOlapTable(t);
                setTableStatistics(table, v);
            }
        }
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
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"DEFAULT\"\n" +
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
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"DEFAULT\"\n" +
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
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"DEFAULT\"\n" +
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
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"DEFAULT\"\n" +
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
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"DEFAULT\"\n" +
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
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"DEFAULT\"\n" +
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
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"DEFAULT\"\n" +
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
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"DEFAULT\"\n" +
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
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"DEFAULT\"\n" +
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
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"DEFAULT\"\n" +
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
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"DEFAULT\"\n" +
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
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"DEFAULT\"\n" +
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
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"DEFAULT\"\n" +
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
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"DEFAULT\"\n" +
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
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"DEFAULT\"\n" +
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
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"DEFAULT\"\n" +
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
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"DEFAULT\"\n" +
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
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"DEFAULT\"\n" +
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
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"DEFAULT\"\n" +
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
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"DEFAULT\"\n" +
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
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"DEFAULT\"\n" +
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
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"DEFAULT\"\n" +
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
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"DEFAULT\"\n" +
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
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"DEFAULT\"\n" +
                ");");

        connectContext.getSessionVariable().setMaxTransformReorderJoins(3);
        connectContext.getSessionVariable().setSqlMode(2);
    }

    public static final String Q1 = "select\n" +
            "  count(*)\n" +
            "from\n" +
            "  store_sales,\n" +
            "  household_demographics,\n" +
            "  time_dim,\n" +
            "  store\n" +
            "where\n" +
            "  ss_sold_time_sk = time_dim.t_time_sk\n" +
            "  and ss_hdemo_sk = household_demographics.hd_demo_sk\n" +
            "  and ss_store_sk = s_store_sk\n" +
            "  and time_dim.t_hour = 8\n" +
            "  and time_dim.t_minute >= 30\n" +
            "  and household_demographics.hd_dep_count = 5\n" +
            "  and store.s_store_name = 'ese'\n" +
            "order by\n" +
            "  count(*)\n" +
            "limit\n" +
            "  100;\n";

    public static final String Q2 = "select\n" +
            "  i_item_id,\n" +
            "  avg(ss_quantity) agg1,\n" +
            "  avg(ss_list_price) agg2,\n" +
            "  avg(ss_coupon_amt) agg3,\n" +
            "  avg(ss_sales_price) agg4\n" +
            "from\n" +
            "  store_sales,\n" +
            "  customer_demographics,\n" +
            "  date_dim,\n" +
            "  item,\n" +
            "  promotion\n" +
            "where\n" +
            "  ss_sold_date_sk = d_date_sk\n" +
            "  and ss_item_sk = i_item_sk\n" +
            "  and ss_cdemo_sk = cd_demo_sk\n" +
            "  and ss_promo_sk = p_promo_sk\n" +
            "  and cd_gender = 'M'\n" +
            "  and cd_marital_status = 'M'\n" +
            "  and cd_education_status = '4 yr Degree'\n" +
            "  and (\n" +
            "    p_channel_email = 'N'\n" +
            "    or p_channel_event = 'N'\n" +
            "  )\n" +
            "  and d_year = 2001\n" +
            "group by\n" +
            "  i_item_id\n" +
            "order by\n" +
            "  i_item_id\n" +
            "limit\n" +
            "  100;";

    public static final String Q3 = "WITH all_sales AS (\n" +
            "    SELECT\n" +
            "      d_year,\n" +
            "      i_brand_id,\n" +
            "      i_class_id,\n" +
            "      i_category_id,\n" +
            "      i_manufact_id,\n" +
            "      SUM(sales_cnt) AS sales_cnt,\n" +
            "      SUM(sales_amt) AS sales_amt\n" +
            "    FROM\n" +
            "      (\n" +
            "        SELECT\n" +
            "          d_year,\n" +
            "          i_brand_id,\n" +
            "          i_class_id,\n" +
            "          i_category_id,\n" +
            "          i_manufact_id,\n" +
            "          cs_quantity - COALESCE(cr_return_quantity, 0) AS sales_cnt,\n" +
            "          cs_ext_sales_price - COALESCE(cr_return_amount, 0.0) AS sales_amt\n" +
            "        FROM\n" +
            "          catalog_sales\n" +
            "          JOIN item ON i_item_sk = cs_item_sk\n" +
            "          JOIN date_dim ON d_date_sk = cs_sold_date_sk\n" +
            "          LEFT JOIN catalog_returns ON (\n" +
            "            cs_order_number = cr_order_number\n" +
            "            AND cs_item_sk = cr_item_sk\n" +
            "          )\n" +
            "        WHERE\n" +
            "          i_category = 'Shoes'\n" +
            "        UNION\n" +
            "        SELECT\n" +
            "          d_year,\n" +
            "          i_brand_id,\n" +
            "          i_class_id,\n" +
            "          i_category_id,\n" +
            "          i_manufact_id,\n" +
            "          ss_quantity - COALESCE(sr_return_quantity, 0) AS sales_cnt,\n" +
            "          ss_ext_sales_price - COALESCE(sr_return_amt, 0.0) AS sales_amt\n" +
            "        FROM\n" +
            "          store_sales\n" +
            "          JOIN item ON i_item_sk = ss_item_sk\n" +
            "          JOIN date_dim ON d_date_sk = ss_sold_date_sk\n" +
            "          LEFT JOIN store_returns ON (\n" +
            "            ss_ticket_number = sr_ticket_number\n" +
            "            AND ss_item_sk = sr_item_sk\n" +
            "          )\n" +
            "        WHERE\n" +
            "          i_category = 'Shoes'\n" +
            "        UNION\n" +
            "        SELECT\n" +
            "          d_year,\n" +
            "          i_brand_id,\n" +
            "          i_class_id,\n" +
            "          i_category_id,\n" +
            "          i_manufact_id,\n" +
            "          ws_quantity - COALESCE(wr_return_quantity, 0) AS sales_cnt,\n" +
            "          ws_ext_sales_price - COALESCE(wr_return_amt, 0.0) AS sales_amt\n" +
            "        FROM\n" +
            "          web_sales\n" +
            "          JOIN item ON i_item_sk = ws_item_sk\n" +
            "          JOIN date_dim ON d_date_sk = ws_sold_date_sk\n" +
            "          LEFT JOIN web_returns ON (\n" +
            "            ws_order_number = wr_order_number\n" +
            "            AND ws_item_sk = wr_item_sk\n" +
            "          )\n" +
            "        WHERE\n" +
            "          i_category = 'Shoes'\n" +
            "      ) sales_detail\n" +
            "    GROUP BY\n" +
            "      d_year,\n" +
            "      i_brand_id,\n" +
            "      i_class_id,\n" +
            "      i_category_id,\n" +
            "      i_manufact_id\n" +
            "  )\n" +
            "SELECT\n" +
            "  prev_yr.d_year AS prev_year,\n" +
            "  curr_yr.d_year AS year,\n" +
            "  curr_yr.i_brand_id,\n" +
            "  curr_yr.i_class_id,\n" +
            "  curr_yr.i_category_id,\n" +
            "  curr_yr.i_manufact_id,\n" +
            "  prev_yr.sales_cnt AS prev_yr_cnt,\n" +
            "  curr_yr.sales_cnt AS curr_yr_cnt,\n" +
            "  curr_yr.sales_cnt - prev_yr.sales_cnt AS sales_cnt_diff,\n" +
            "  curr_yr.sales_amt - prev_yr.sales_amt AS sales_amt_diff\n" +
            "FROM\n" +
            "  all_sales curr_yr,\n" +
            "  all_sales prev_yr\n" +
            "WHERE\n" +
            "  curr_yr.i_brand_id = prev_yr.i_brand_id\n" +
            "  AND curr_yr.i_class_id = prev_yr.i_class_id\n" +
            "  AND curr_yr.i_category_id = prev_yr.i_category_id\n" +
            "  AND curr_yr.i_manufact_id = prev_yr.i_manufact_id\n" +
            "  AND curr_yr.d_year = 2000\n" +
            "  AND prev_yr.d_year = 2000 -1\n" +
            "  AND CAST(curr_yr.sales_cnt AS DECIMAL(17, 2)) / CAST(prev_yr.sales_cnt AS DECIMAL(17, 2)) < 0.9\n" +
            "ORDER BY\n" +
            "  sales_cnt_diff,\n" +
            "  sales_amt_diff\n" +
            "limit\n" +
            "  100;";

    public static final String Q4 =
            "select  asceding.rnk, i1.i_product_name best_performing, i2.i_product_name worst_performing\n" +
                    "from(select *\n" +
                    "     from (select item_sk,rank() over (order by rank_col asc) rnk\n" +
                    "           from (select ss_item_sk item_sk,avg(ss_net_profit) rank_col\n" +
                    "                 from store_sales ss1\n" +
                    "                 where ss_store_sk = 6\n" +
                    "                 group by ss_item_sk\n" +
                    "                 having avg(ss_net_profit) > 0.9*(select avg(ss_net_profit) rank_col\n" +
                    "                                                  from store_sales\n" +
                    "                                                  where ss_store_sk = 6\n" +
                    "                                                    and ss_hdemo_sk is null\n" +
                    "                                                  group by ss_store_sk))V1)V11\n" +
                    "     where rnk  < 11) asceding,\n" +
                    "    (select *\n" +
                    "     from (select item_sk,rank() over (order by rank_col desc) rnk\n" +
                    "           from (select ss_item_sk item_sk,avg(ss_net_profit) rank_col\n" +
                    "                 from store_sales ss1\n" +
                    "                 where ss_store_sk = 6\n" +
                    "                 group by ss_item_sk\n" +
                    "                 having avg(ss_net_profit) > 0.9*(select avg(ss_net_profit) rank_col\n" +
                    "                                                  from store_sales\n" +
                    "                                                  where ss_store_sk = 6\n" +
                    "                                                    and ss_hdemo_sk is null\n" +
                    "                                                  group by ss_store_sk))V2)V21\n" +
                    "     where rnk  < 11) descending,\n" +
                    "item i1,\n" +
                    "item i2\n" +
                    "where asceding.rnk = descending.rnk\n" +
                    "  and i1.i_item_sk=asceding.item_sk\n" +
                    "  and i2.i_item_sk=descending.item_sk\n" +
                    "order by asceding.rnk\n" +
                    "limit 100;";

    public static final String Q5 = "with inv as\n" +
            "(select w_warehouse_name,w_warehouse_sk,i_item_sk,d_moy\n" +
            "       ,stdev,mean, case mean when 0 then null else stdev/mean end cov\n" +
            " from(select w_warehouse_name,w_warehouse_sk,i_item_sk,d_moy\n" +
            "            ,stddev_samp(inv_quantity_on_hand) stdev,avg(inv_quantity_on_hand) mean\n" +
            "      from inventory\n" +
            "          ,item\n" +
            "          ,warehouse\n" +
            "          ,date_dim\n" +
            "      where inv_item_sk = i_item_sk\n" +
            "        and inv_warehouse_sk = w_warehouse_sk\n" +
            "        and inv_date_sk = d_date_sk\n" +
            "        and d_year =2001\n" +
            "      group by w_warehouse_name,w_warehouse_sk,i_item_sk,d_moy) foo\n" +
            " where case mean when 0 then 0 else stdev/mean end > 1)\n" +
            "select inv1.w_warehouse_sk,inv1.i_item_sk,inv1.d_moy,inv1.mean, inv1.cov\n" +
            "        ,inv2.w_warehouse_sk,inv2.i_item_sk,inv2.d_moy,inv2.mean, inv2.cov\n" +
            "from inv inv1,inv inv2\n" +
            "where inv1.i_item_sk = inv2.i_item_sk\n" +
            "  and inv1.w_warehouse_sk =  inv2.w_warehouse_sk\n" +
            "  and inv1.d_moy=1\n" +
            "  and inv2.d_moy=1+1\n" +
            "order by inv1.w_warehouse_sk,inv1.i_item_sk,inv1.d_moy,inv1.mean,inv1.cov\n" +
            "        ,inv2.d_moy,inv2.mean, inv2.cov\n" +
            ";\n" +
            "with inv as\n" +
            "(select w_warehouse_name,w_warehouse_sk,i_item_sk,d_moy\n" +
            "       ,stdev,mean, case mean when 0 then null else stdev/mean end cov\n" +
            " from(select w_warehouse_name,w_warehouse_sk,i_item_sk,d_moy\n" +
            "            ,stddev_samp(inv_quantity_on_hand) stdev,avg(inv_quantity_on_hand) mean\n" +
            "      from inventory\n" +
            "          ,item\n" +
            "          ,warehouse\n" +
            "          ,date_dim\n" +
            "      where inv_item_sk = i_item_sk\n" +
            "        and inv_warehouse_sk = w_warehouse_sk\n" +
            "        and inv_date_sk = d_date_sk\n" +
            "        and d_year =2001\n" +
            "      group by w_warehouse_name,w_warehouse_sk,i_item_sk,d_moy) foo\n" +
            " where case mean when 0 then 0 else stdev/mean end > 1)\n" +
            "select inv1.w_warehouse_sk,inv1.i_item_sk,inv1.d_moy,inv1.mean, inv1.cov\n" +
            "        ,inv2.w_warehouse_sk,inv2.i_item_sk,inv2.d_moy,inv2.mean, inv2.cov\n" +
            "from inv inv1,inv inv2\n" +
            "where inv1.i_item_sk = inv2.i_item_sk\n" +
            "  and inv1.w_warehouse_sk =  inv2.w_warehouse_sk\n" +
            "  and inv1.d_moy=1\n" +
            "  and inv2.d_moy=1+1\n" +
            "  and inv1.cov > 1.5\n" +
            "order by inv1.w_warehouse_sk,inv1.i_item_sk,inv1.d_moy,inv1.mean,inv1.cov\n" +
            "        ,inv2.d_moy,inv2.mean, inv2.cov\n" +
            ";";

    public static final String Q6 = "with ssr as (\n" +
            "    select\n" +
            "      s_store_id as store_id,\n" +
            "      sum(ss_ext_sales_price) as sales,\n" +
            "      sum(coalesce(sr_return_amt, 0)) as returns,\n" +
            "      sum(ss_net_profit - coalesce(sr_net_loss, 0)) as profit\n" +
            "    from\n" +
            "      store_sales\n" +
            "      left outer join store_returns on (\n" +
            "        ss_item_sk = sr_item_sk\n" +
            "        and ss_ticket_number = sr_ticket_number\n" +
            "      ),\n" +
            "      date_dim,\n" +
            "      store,\n" +
            "      item,\n" +
            "      promotion\n" +
            "    where\n" +
            "      ss_sold_date_sk = d_date_sk\n" +
            "      and d_date between cast('2002-08-04' as date)\n" +
            "      and date_add(cast('2002-08-04' as date), 30)\n" +
            "      and ss_store_sk = s_store_sk\n" +
            "      and ss_item_sk = i_item_sk\n" +
            "      and i_current_price > 50\n" +
            "      and ss_promo_sk = p_promo_sk\n" +
            "      and p_channel_tv = 'N'\n" +
            "    group by\n" +
            "      s_store_id\n" +
            "  ),\n" +
            "  csr as (\n" +
            "    select\n" +
            "      cp_catalog_page_id as catalog_page_id,\n" +
            "      sum(cs_ext_sales_price) as sales,\n" +
            "      sum(coalesce(cr_return_amount, 0)) as returns,\n" +
            "      sum(cs_net_profit - coalesce(cr_net_loss, 0)) as profit\n" +
            "    from\n" +
            "      catalog_sales\n" +
            "      left outer join catalog_returns on (\n" +
            "        cs_item_sk = cr_item_sk\n" +
            "        and cs_order_number = cr_order_number\n" +
            "      ),\n" +
            "      date_dim,\n" +
            "      catalog_page,\n" +
            "      item,\n" +
            "      promotion\n" +
            "    where\n" +
            "      cs_sold_date_sk = d_date_sk\n" +
            "      and d_date between cast('2002-08-04' as date)\n" +
            "      and date_add(cast('2002-08-04' as date), 30)\n" +
            "      and cs_catalog_page_sk = cp_catalog_page_sk\n" +
            "      and cs_item_sk = i_item_sk\n" +
            "      and i_current_price > 50\n" +
            "      and cs_promo_sk = p_promo_sk\n" +
            "      and p_channel_tv = 'N'\n" +
            "    group by\n" +
            "      cp_catalog_page_id\n" +
            "  ),\n" +
            "  wsr as (\n" +
            "    select\n" +
            "      web_site_id,\n" +
            "      sum(ws_ext_sales_price) as sales,\n" +
            "      sum(coalesce(wr_return_amt, 0)) as returns,\n" +
            "      sum(ws_net_profit - coalesce(wr_net_loss, 0)) as profit\n" +
            "    from\n" +
            "      web_sales\n" +
            "      left outer join web_returns on (\n" +
            "        ws_item_sk = wr_item_sk\n" +
            "        and ws_order_number = wr_order_number\n" +
            "      ),\n" +
            "      date_dim,\n" +
            "      web_site,\n" +
            "      item,\n" +
            "      promotion\n" +
            "    where\n" +
            "      ws_sold_date_sk = d_date_sk\n" +
            "      and d_date between cast('2002-08-04' as date)\n" +
            "      and date_add(cast('2002-08-04' as date), 30)\n" +
            "      and ws_web_site_sk = web_site_sk\n" +
            "      and ws_item_sk = i_item_sk\n" +
            "      and i_current_price > 50\n" +
            "      and ws_promo_sk = p_promo_sk\n" +
            "      and p_channel_tv = 'N'\n" +
            "    group by\n" +
            "      web_site_id\n" +
            "  )\n" +
            "select\n" +
            "  channel,\n" +
            "  id,\n" +
            "  sum(sales) as sales,\n" +
            "  sum(returns) as returns,\n" +
            "  sum(profit) as profit\n" +
            "from\n" +
            "  (\n" +
            "    select\n" +
            "      'store channel' as channel,\n" +
            "      'store' || store_id as id,\n" +
            "      sales,\n" +
            "      returns,\n" +
            "      profit\n" +
            "    from\n" +
            "      ssr\n" +
            "    union all\n" +
            "    select\n" +
            "      'globalStateMgr channel' as channel,\n" +
            "      'catalog_page' || catalog_page_id as id,\n" +
            "      sales,\n" +
            "      returns,\n" +
            "      profit\n" +
            "    from\n" +
            "      csr\n" +
            "    union all\n" +
            "    select\n" +
            "      'web channel' as channel,\n" +
            "      'web_site' || web_site_id as id,\n" +
            "      sales,\n" +
            "      returns,\n" +
            "      profit\n" +
            "    from\n" +
            "      wsr\n" +
            "  ) x\n" +
            "group by\n" +
            "  rollup (channel, id)\n" +
            "order by\n" +
            "  channel,\n" +
            "  id\n" +
            "limit\n" +
            "  100;";

    public static final String Q7 = "select\n" +
            "  sum(cs_ext_discount_amt) as \"excess discount amount\"\n" +
            "from\n" +
            "  catalog_sales,\n" +
            "  item,\n" +
            "  date_dim\n" +
            "where\n" +
            "  i_manufact_id = 283\n" +
            "  and i_item_sk = cs_item_sk\n" +
            "  and d_date between '1999-02-22'\n" +
            "  and date_add(cast('1999-02-22' as date), 90)\n" +
            "  and d_date_sk = cs_sold_date_sk\n" +
            "  and cs_ext_discount_amt > (\n" +
            "    select\n" +
            "      1.3 * avg(cs_ext_discount_amt)\n" +
            "    from\n" +
            "      catalog_sales,\n" +
            "      date_dim\n" +
            "    where\n" +
            "      cs_item_sk = i_item_sk\n" +
            "      and d_date between '1999-02-22'\n" +
            "      and date_add(cast('1999-02-22' as date), 90)\n" +
            "      and d_date_sk = cs_sold_date_sk\n" +
            "  )\n" +
            "limit\n" +
            "  100;";

    public static final String Q8 = "select\n" +
            "  i_brand_id brand_id,\n" +
            "  i_brand brand,\n" +
            "  i_manufact_id,\n" +
            "  i_manufact,\n" +
            "  sum(ss_ext_sales_price) ext_price\n" +
            "from\n" +
            "  date_dim,\n" +
            "  store_sales,\n" +
            "  item,\n" +
            "  customer,\n" +
            "  customer_address,\n" +
            "  store\n" +
            "where\n" +
            "  d_date_sk = ss_sold_date_sk\n" +
            "  and ss_item_sk = i_item_sk\n" +
            "  and i_manager_id = 8\n" +
            "  and d_moy = 11\n" +
            "  and d_year = 1999\n" +
            "  and ss_customer_sk = c_customer_sk\n" +
            "  and c_current_addr_sk = ca_address_sk\n" +
            "  and substr(ca_zip, 1, 5) <> substr(s_zip, 1, 5)\n" +
            "  and ss_store_sk = s_store_sk\n" +
            "group by\n" +
            "  i_brand,\n" +
            "  i_brand_id,\n" +
            "  i_manufact_id,\n" +
            "  i_manufact\n" +
            "order by\n" +
            "  ext_price desc,\n" +
            "  i_brand,\n" +
            "  i_brand_id,\n" +
            "  i_manufact_id,\n" +
            "  i_manufact\n" +
            "limit\n" +
            "  100;";

    public static final String Q9 = "select\n" +
            "  i_item_id,\n" +
            "  i_item_desc,\n" +
            "  s_store_id,\n" +
            "  s_store_name,\n" +
            "  min(ss_net_profit) as store_sales_profit,\n" +
            "  min(sr_net_loss) as store_returns_loss,\n" +
            "  min(cs_net_profit) as catalog_sales_profit\n" +
            "from\n" +
            "  store_sales,\n" +
            "  store_returns,\n" +
            "  catalog_sales,\n" +
            "  date_dim d1,\n" +
            "  date_dim d2,\n" +
            "      date_dim d3,\n" +
            "  store,\n" +
            "  item\n" +
            "where\n" +
            "  d1.d_moy = 4\n" +
            "  and d1.d_year = 2002\n" +
            "  and d1.d_date_sk = ss_sold_date_sk\n" +
            "  and i_item_sk = ss_item_sk\n" +
            "  and s_store_sk = ss_store_sk\n" +
            "  and ss_customer_sk = sr_customer_sk\n" +
            "  and ss_item_sk = sr_item_sk\n" +
            "  and ss_ticket_number = sr_ticket_number\n" +
            "  and sr_returned_date_sk = d2.d_date_sk\n" +
            "  and d2.d_moy between 4\n" +
            "  and 10\n" +
            "  and d2.d_year = 2002\n" +
            "  and sr_customer_sk = cs_bill_customer_sk\n" +
            "  and sr_item_sk = cs_item_sk\n" +
            "  and cs_sold_date_sk = d3.d_date_sk\n" +
            "  and d3.d_moy between 4\n" +
            "  and 10\n" +
            "  and d3.d_year = 2002\n" +
            "group by\n" +
            "  i_item_id,\n" +
            "  i_item_desc,\n" +
            "  s_store_id,\n" +
            "  s_store_name\n" +
            "order by\n" +
            "  i_item_id,\n" +
            "  i_item_desc,\n" +
            "  s_store_id,\n" +
            "  s_store_name\n" +
            "limit\n" +
            "  100;";

    public static final String Q10 = "with ws as\n" +
            "  (select d_year AS ws_sold_year, ws_item_sk,\n" +
            "    ws_bill_customer_sk ws_customer_sk,\n" +
            "    sum(ws_quantity) ws_qty,\n" +
            "    sum(ws_wholesale_cost) ws_wc,\n" +
            "    sum(ws_sales_price) ws_sp\n" +
            "   from web_sales\n" +
            "   left join web_returns on wr_order_number=ws_order_number and ws_item_sk=wr_item_sk\n" +
            "   join date_dim on ws_sold_date_sk = d_date_sk\n" +
            "   where wr_order_number is null\n" +
            "   group by d_year, ws_item_sk, ws_bill_customer_sk\n" +
            "   ),\n" +
            "cs as\n" +
            "  (select d_year AS cs_sold_year, cs_item_sk,\n" +
            "    cs_bill_customer_sk cs_customer_sk,\n" +
            "    sum(cs_quantity) cs_qty,\n" +
            "    sum(cs_wholesale_cost) cs_wc,\n" +
            "    sum(cs_sales_price) cs_sp\n" +
            "   from catalog_sales\n" +
            "   left join catalog_returns on cr_order_number=cs_order_number and cs_item_sk=cr_item_sk\n" +
            "   join date_dim on cs_sold_date_sk = d_date_sk\n" +
            "   where cr_order_number is null\n" +
            "   group by d_year, cs_item_sk, cs_bill_customer_sk\n" +
            "   ),\n" +
            "ss as\n" +
            "  (select d_year AS ss_sold_year, ss_item_sk,\n" +
            "    ss_customer_sk,\n" +
            "    sum(ss_quantity) ss_qty,\n" +
            "    sum(ss_wholesale_cost) ss_wc,\n" +
            "    sum(ss_sales_price) ss_sp\n" +
            "   from store_sales\n" +
            "   left join store_returns on sr_ticket_number=ss_ticket_number and ss_item_sk=sr_item_sk\n" +
            "   join date_dim on ss_sold_date_sk = d_date_sk\n" +
            "   where sr_ticket_number is null\n" +
            "   group by d_year, ss_item_sk, ss_customer_sk\n" +
            "   )\n" +
            " select\n" +
            "ss_customer_sk,\n" +
            "round(ss_qty/(coalesce(ws_qty,0)+coalesce(cs_qty,0)),2) ratio,\n" +
            "ss_qty store_qty, ss_wc store_wholesale_cost, ss_sp store_sales_price,\n" +
            "coalesce(ws_qty,0)+coalesce(cs_qty,0) other_chan_qty,\n" +
            "coalesce(ws_wc,0)+coalesce(cs_wc,0) other_chan_wholesale_cost,\n" +
            "coalesce(ws_sp,0)+coalesce(cs_sp,0) other_chan_sales_price\n" +
            "from ss\n" +
            "left join ws on (ws_sold_year=ss_sold_year and ws_item_sk=ss_item_sk and ws_customer_sk=ss_customer_sk)\n" +
            "left join cs on (cs_sold_year=ss_sold_year and cs_item_sk=ss_item_sk and cs_customer_sk=ss_customer_sk)\n" +
            "where (coalesce(ws_qty,0)>0 or coalesce(cs_qty, 0)>0) and ss_sold_year=2001\n" +
            "order by\n" +
            "  ss_customer_sk,\n" +
            "  ss_qty desc, ss_wc desc, ss_sp desc,\n" +
            "  other_chan_qty,\n" +
            "  other_chan_wholesale_cost,\n" +
            "  other_chan_sales_price,\n" +
            "  ratio\n" +
            "limit 100;";

    public static final String Q11 = "select\n" +
            "    sum(ws_net_paid) as total_sum\n" +
            "   ,i_category\n" +
            "   ,i_class\n" +
            "   ,grouping(i_category)+grouping(i_class) as lochierarchy\n" +
            "   ,rank() over (\n" +
            "    partition by grouping(i_category)+grouping(i_class),\n" +
            "    case when grouping(i_class) = 0 then i_category end\n" +
            "    order by sum(ws_net_paid) desc) as rank_within_parent\n" +
            " from\n" +
            "    web_sales\n" +
            "   ,date_dim       d1\n" +
            "   ,item\n" +
            " where\n" +
            "    d1.d_month_seq between 1205 and 1205+11\n" +
            " and d1.d_date_sk = ws_sold_date_sk\n" +
            " and i_item_sk  = ws_item_sk\n" +
            " group by rollup(i_category,i_class)\n" +
            " order by\n" +
            "   lochierarchy desc,\n" +
            "   case when lochierarchy = 0 then i_category end,\n" +
            "   rank_within_parent\n" +
            " limit 100;\n";

    public static final String Q12 = "with customer_total_return as (\n" +
            "    select\n" +
            "      sr_customer_sk as ctr_customer_sk,\n" +
            "      sr_store_sk as ctr_store_sk,\n" +
            "      sum(SR_RETURN_AMT_INC_TAX) as ctr_total_return\n" +
            "    from\n" +
            "      store_returns,\n" +
            "      date_dim\n" +
            "    where\n" +
            "      sr_returned_date_sk = d_date_sk\n" +
            "      and d_year = 1999\n" +
            "    group by\n" +
            "      sr_customer_sk,\n" +
            "      sr_store_sk\n" +
            "  )\n" +
            "select\n" +
            "  c_customer_id\n" +
            "from\n" +
            "  customer_total_return ctr1,\n" +
            "  store,\n" +
            "  customer\n" +
            "where\n" +
            "  ctr1.ctr_total_return > (\n" +
            "    select\n" +
            "      avg(ctr_total_return) * 1.2\n" +
            "    from\n" +
            "      customer_total_return ctr2\n" +
            "    where\n" +
            "      ctr1.ctr_store_sk = ctr2.ctr_store_sk\n" +
            "  )\n" +
            "  and s_store_sk = ctr1.ctr_store_sk\n" +
            "  and s_state = 'TN'\n" +
            "  and ctr1.ctr_customer_sk = c_customer_sk\n" +
            "order by\n" +
            "  c_customer_id\n" +
            "limit\n" +
            "  100;";

    public static final String Q13 = "select\n" +
            "  cc_call_center_id Call_Center,\n" +
            "  cc_name Call_Center_Name,\n" +
            "  cc_manager Manager,\n" +
            "  sum(cr_net_loss) Returns_Loss\n" +
            "from\n" +
            "  call_center,\n" +
            "  catalog_returns,\n" +
            "  date_dim,\n" +
            "  customer,\n" +
            "  customer_address,\n" +
            "  customer_demographics,\n" +
            "  household_demographics\n" +
            "where\n" +
            "  cr_call_center_sk = cc_call_center_sk\n" +
            "  and cr_returned_date_sk = d_date_sk\n" +
            "  and cr_returning_customer_sk = c_customer_sk\n" +
            "  and cd_demo_sk = c_current_cdemo_sk\n" +
            "  and hd_demo_sk = c_current_hdemo_sk\n" +
            "  and ca_address_sk = c_current_addr_sk\n" +
            "  and d_year = 2002\n" +
            "  and d_moy = 11\n" +
            "  and (\n" +
            "    (\n" +
            "      cd_marital_status = 'M'\n" +
            "      and cd_education_status = 'Unknown'\n" +
            "    )\n" +
            "    or(\n" +
            "      cd_marital_status = 'W'\n" +
            "      and cd_education_status = 'Advanced Degree'\n" +
            "    )\n" +
            "  )\n" +
            "  and hd_buy_potential like 'Unknown%'\n" +
            "  and ca_gmt_offset = -6\n" +
            "group by\n" +
            "  cc_call_center_id,\n" +
            "  cc_name,\n" +
            "  cc_manager,\n" +
            "  cd_marital_status,\n" +
            "  cd_education_status\n" +
            "order by\n" +
            "  sum(cr_net_loss) desc;";

    public static final String Q14 = "select\n" +
            "  *\n" +
            "from(\n" +
            "    select\n" +
            "      w_warehouse_name,\n" +
            "      i_item_id,\n" +
            "      sum(\n" +
            "        case\n" +
            "          when (\n" +
            "            cast(d_date as date) < cast ('2000-05-19' as date)\n" +
            "          ) then inv_quantity_on_hand\n" +
            "          else 0\n" +
            "        end\n" +
            "      ) as inv_before,\n" +
            "      sum(\n" +
            "        case\n" +
            "          when (\n" +
            "            cast(d_date as date) >= cast ('2000-05-19' as date)\n" +
            "          ) then inv_quantity_on_hand\n" +
            "          else 0\n" +
            "        end\n" +
            "      ) as inv_after\n" +
            "    from\n" +
            "      inventory,\n" +
            "      warehouse,\n" +
            "      item,\n" +
            "      date_dim\n" +
            "    where\n" +
            "      i_current_price between 0.99\n" +
            "      and 1.49\n" +
            "      and i_item_sk = inv_item_sk\n" +
            "      and inv_warehouse_sk = w_warehouse_sk\n" +
            "      and inv_date_sk = d_date_sk\n" +
            "      and d_date between date_sub(cast ('2000-05-19' as date), 30)\n" +
            "      and date_add(cast ('2000-05-19' as date), 30)\n" +
            "    group by\n" +
            "      w_warehouse_name,\n" +
            "      i_item_id\n" +
            "  ) x\n" +
            "where\n" +
            "  (\n" +
            "    case\n" +
            "      when inv_before > 0 then inv_after / inv_before\n" +
            "      else null\n" +
            "    end\n" +
            "  ) between 2.0 / 3.0\n" +
            "  and 3.0 / 2.0\n" +
            "order by\n" +
            "  w_warehouse_name,\n" +
            "  i_item_id\n" +
            "limit\n" +
            "  100;\n";

    public static final String Q15 = "select\n" +
            "  s_store_name,\n" +
            "  s_store_id,\n" +
            "  sum(\n" +
            "    case\n" +
            "      when (d_day_name = 'Sunday') then ss_sales_price\n" +
            "      else null\n" +
            "    end\n" +
            "  ) sun_sales,\n" +
            "  sum(\n" +
            "    case\n" +
            "      when (d_day_name = 'Monday') then ss_sales_price\n" +
            "      else null\n" +
            "    end\n" +
            "  ) mon_sales,\n" +
            "  sum(\n" +
            "    case\n" +
            "      when (d_day_name = 'Tuesday') then ss_sales_price\n" +
            "      else null\n" +
            "    end\n" +
            "  ) tue_sales,\n" +
            "  sum(\n" +
            "    case\n" +
            "      when (d_day_name = 'Wednesday') then ss_sales_price\n" +
            "      else null\n" +
            "    end\n" +
            "  ) wed_sales,\n" +
            "  sum(\n" +
            "    case\n" +
            "      when (d_day_name = 'Thursday') then ss_sales_price\n" +
            "      else null\n" +
            "    end\n" +
            "  ) thu_sales,\n" +
            "  sum(\n" +
            "    case\n" +
            "      when (d_day_name = 'Friday') then ss_sales_price\n" +
            "      else null\n" +
            "    end\n" +
            "  ) fri_sales,\n" +
            "  sum(\n" +
            "    case\n" +
            "      when (d_day_name = 'Saturday') then ss_sales_price\n" +
            "      else null\n" +
            "    end\n" +
            "  ) sat_sales\n" +
            "from\n" +
            "  date_dim,\n" +
            "  store_sales,\n" +
            "  store\n" +
            "where\n" +
            "  d_date_sk = ss_sold_date_sk\n" +
            "  and s_store_sk = ss_store_sk\n" +
            "  and s_gmt_offset = -5\n" +
            "  and d_year = 2000\n" +
            "group by\n" +
            "  s_store_name,\n" +
            "  s_store_id\n" +
            "order by\n" +
            "  s_store_name,\n" +
            "  s_store_id,\n" +
            "  sun_sales,\n" +
            "  mon_sales,\n" +
            "  tue_sales,\n" +
            "  wed_sales,\n" +
            "  thu_sales,\n" +
            "  fri_sales,\n" +
            "  sat_sales\n" +
            "limit\n" +
            "  100;";

    public static final String Q16 = "select  i_item_id,\n" +
            "        s_state, grouping(s_state) g_state,\n" +
            "        avg(ss_quantity) agg1,\n" +
            "        avg(ss_list_price) agg2,\n" +
            "        avg(ss_coupon_amt) agg3,\n" +
            "        avg(ss_sales_price) agg4\n" +
            " from store_sales, customer_demographics, date_dim, store, item\n" +
            " where ss_sold_date_sk = d_date_sk and\n" +
            "       ss_item_sk = i_item_sk and\n" +
            "       ss_store_sk = s_store_sk and\n" +
            "       ss_cdemo_sk = cd_demo_sk and\n" +
            "       cd_gender = 'M' and\n" +
            "       cd_marital_status = 'U' and\n" +
            "       cd_education_status = 'Secondary' and\n" +
            "       d_year = 2000 and\n" +
            "       s_state in ('TN','TN', 'TN', 'TN', 'TN', 'TN')\n" +
            " group by rollup (i_item_id, s_state)\n" +
            " order by i_item_id\n" +
            "         ,s_state\n" +
            " limit 100;";

    public static final String Q17 = "select\n" +
            "   count(distinct ws_order_number) as \"order count\"\n" +
            "  ,sum(ws_ext_ship_cost) as \"total shipping cost\"\n" +
            "  ,sum(ws_net_profit) as \"total net profit\"\n" +
            "from\n" +
            "   web_sales ws1\n" +
            "  ,date_dim\n" +
            "  ,customer_address\n" +
            "  ,web_site\n" +
            "where\n" +
            "    d_date between '1999-4-01' and\n" +
            "           date_add(cast('1999-4-01' as date), 60)\n" +
            "and ws1.ws_ship_date_sk = d_date_sk\n" +
            "and ws1.ws_ship_addr_sk = ca_address_sk\n" +
            "and ca_state = 'WI'\n" +
            "and ws1.ws_web_site_sk = web_site_sk\n" +
            "and web_company_name = 'pri'\n" +
            "and exists (select *\n" +
            "            from web_sales ws2\n" +
            "            where ws1.ws_order_number = ws2.ws_order_number\n" +
            "              and ws1.ws_warehouse_sk <> ws2.ws_warehouse_sk)\n" +
            "and not exists(select *\n" +
            "               from web_returns wr1\n" +
            "               where ws1.ws_order_number = wr1.wr_order_number)\n" +
            "order by count(distinct ws_order_number)\n" +
            "limit 100;";

    public static final String Q18 = "select  ca_zip, ca_city, sum(ws_sales_price)\n" +
            " from web_sales, customer, customer_address, date_dim, item\n" +
            " where ws_bill_customer_sk = c_customer_sk\n" +
            "    and c_current_addr_sk = ca_address_sk\n" +
            "    and ws_item_sk = i_item_sk\n" +
            "    and ( substr(ca_zip,1,5) in ('85669', '86197','88274','83405','86475', '85392', '85460', '80348', '81792')\n" +
            "          or\n" +
            "          i_item_id in (select i_item_id\n" +
            "                             from item\n" +
            "                             where i_item_sk in (2, 3, 5, 7, 11, 13, 17, 19, 23, 29)\n" +
            "                             )\n" +
            "        )\n" +
            "    and ws_sold_date_sk = d_date_sk\n" +
            "    and d_qoy = 2 and d_year = 2000\n" +
            " group by ca_zip, ca_city\n" +
            " order by ca_zip, ca_city\n" +
            " limit 100;";

    public static final String Q19 = "with ss_items as (\n" +
            "    select\n" +
            "      i_item_id item_id,\n" +
            "      sum(ss_ext_sales_price) ss_item_rev\n" +
            "    from\n" +
            "      store_sales,\n" +
            "      item,\n" +
            "      date_dim\n" +
            "    where\n" +
            "      ss_item_sk = i_item_sk\n" +
            "      and d_date in (\n" +
            "        select\n" +
            "          d_date\n" +
            "        from\n" +
            "          date_dim\n" +
            "        where\n" +
            "          d_week_seq = (\n" +
            "            select\n" +
            "              d_week_seq\n" +
            "            from\n" +
            "              date_dim\n" +
            "            where\n" +
            "              d_date = '2000-02-12'\n" +
            "          )\n" +
            "      )\n" +
            "      and ss_sold_date_sk = d_date_sk\n" +
            "    group by\n" +
            "      i_item_id\n" +
            "  ),\n" +
            "  cs_items as (\n" +
            "    select\n" +
            "      i_item_id item_id,\n" +
            "      sum(cs_ext_sales_price) cs_item_rev\n" +
            "    from\n" +
            "      catalog_sales,\n" +
            "      item,\n" +
            "      date_dim\n" +
            "    where\n" +
            "      cs_item_sk = i_item_sk\n" +
            "      and d_date in (\n" +
            "        select\n" +
            "          d_date\n" +
            "        from\n" +
            "          date_dim\n" +
            "        where\n" +
            "          d_week_seq = (\n" +
            "            select\n" +
            "              d_week_seq\n" +
            "            from\n" +
            "              date_dim\n" +
            "            where\n" +
            "              d_date = '2000-02-12'\n" +
            "          )\n" +
            "      )\n" +
            "      and cs_sold_date_sk = d_date_sk\n" +
            "    group by\n" +
            "      i_item_id\n" +
            "  ),\n" +
            "  ws_items as (\n" +
            "    select\n" +
            "      i_item_id item_id,\n" +
            "      sum(ws_ext_sales_price) ws_item_rev\n" +
            "    from\n" +
            "      web_sales,\n" +
            "      item,\n" +
            "      date_dim\n" +
            "    where\n" +
            "      ws_item_sk = i_item_sk\n" +
            "      and d_date in (\n" +
            "        select\n" +
            "          d_date\n" +
            "        from\n" +
            "          date_dim\n" +
            "        where\n" +
            "          d_week_seq =(\n" +
            "            select\n" +
            "              d_week_seq\n" +
            "            from\n" +
            "              date_dim\n" +
            "            where\n" +
            "              d_date = '2000-02-12'\n" +
            "          )\n" +
            "      )\n" +
            "      and ws_sold_date_sk = d_date_sk\n" +
            "    group by\n" +
            "      i_item_id\n" +
            "  )\n" +
            "select\n" +
            "  ss_items.item_id,\n" +
            "  ss_item_rev,\n" +
            "  ss_item_rev /((ss_item_rev + cs_item_rev + ws_item_rev) / 3) * 100 ss_dev,\n" +
            "  cs_item_rev,\n" +
            "  cs_item_rev /((ss_item_rev + cs_item_rev + ws_item_rev) / 3) * 100 cs_dev,\n" +
            "  ws_item_rev,\n" +
            "  ws_item_rev /((ss_item_rev + cs_item_rev + ws_item_rev) / 3) * 100 " +
            "ws_dev,(ss_item_rev + cs_item_rev + ws_item_rev) / 3 average\n" +
            "from\n" +
            "  ss_items,\n" +
            "  cs_items,\n" +
            "  ws_items\n" +
            "where\n" +
            "  ss_items.item_id = cs_items.item_id\n" +
            "  and ss_items.item_id = ws_items.item_id\n" +
            "  and ss_item_rev between 0.9 * cs_item_rev\n" +
            "  and 1.1 * cs_item_rev\n" +
            "  and ss_item_rev between 0.9 * ws_item_rev\n" +
            "  and 1.1 * ws_item_rev\n" +
            "  and cs_item_rev between 0.9 * ss_item_rev\n" +
            "  and 1.1 * ss_item_rev\n" +
            "  and cs_item_rev between 0.9 * ws_item_rev\n" +
            "  and 1.1 * ws_item_rev\n" +
            "  and ws_item_rev between 0.9 * ss_item_rev\n" +
            "  and 1.1 * ss_item_rev\n" +
            "  and ws_item_rev between 0.9 * cs_item_rev\n" +
            "  and 1.1 * cs_item_rev\n" +
            "order by\n" +
            "  item_id,\n" +
            "  ss_item_rev\n" +
            "limit\n" +
            "  100;";

    public static final String Q20 = "with cs_ui as (\n" +
            "    select\n" +
            "      cs_item_sk,\n" +
            "      sum(cs_ext_list_price) as sale,\n" +
            "      sum(\n" +
            "        cr_refunded_cash + cr_reversed_charge + cr_store_credit\n" +
            "      ) as refund\n" +
            "    from\n" +
            "      catalog_sales,\n" +
            "      catalog_returns\n" +
            "    where\n" +
            "      cs_item_sk = cr_item_sk\n" +
            "      and cs_order_number = cr_order_number\n" +
            "    group by\n" +
            "      cs_item_sk\n" +
            "    having\n" +
            "      sum(cs_ext_list_price) > 2 * sum(\n" +
            "        cr_refunded_cash + cr_reversed_charge + cr_store_credit\n" +
            "      )\n" +
            "  ),\n" +
            "  cross_sales as (\n" +
            "    select\n" +
            "      i_product_name product_name,\n" +
            "      i_item_sk item_sk,\n" +
            "      s_store_name store_name,\n" +
            "      s_zip store_zip,\n" +
            "      ad1.ca_street_number b_street_number,\n" +
            "      ad1.ca_street_name b_street_name,\n" +
            "      ad1.ca_city b_city,\n" +
            "      ad1.ca_zip b_zip,\n" +
            "      ad2.ca_street_number c_street_number,\n" +
            "      ad2.ca_street_name c_street_name,\n" +
            "      ad2.ca_city c_city,\n" +
            "      ad2.ca_zip c_zip,\n" +
            "      d1.d_year as syear,\n" +
            "      d2.d_year as fsyear,\n" +
            "      d3.d_year s2year,\n" +
            "      count(*) cnt,\n" +
            "      sum(ss_wholesale_cost) s1,\n" +
            "      sum(ss_list_price) s2,\n" +
            "      sum(ss_coupon_amt) s3\n" +
            "    FROM\n" +
            "      store_sales,\n" +
            "      store_returns,\n" +
            "      cs_ui,\n" +
            "      date_dim d1,\n" +
            "      date_dim d2,\n" +
            "      date_dim d3,\n" +
            "      store,\n" +
            "      customer,\n" +
            "      customer_demographics cd1,\n" +
            "      customer_demographics cd2,\n" +
            "      promotion,\n" +
            "      household_demographics hd1,\n" +
            "      household_demographics hd2,\n" +
            "      customer_address ad1,\n" +
            "      customer_address ad2,\n" +
            "      income_band ib1,\n" +
            "      income_band ib2,\n" +
            "      item\n" +
            "    WHERE\n" +
            "      ss_store_sk = s_store_sk\n" +
            "      AND ss_sold_date_sk = d1.d_date_sk\n" +
            "      AND ss_customer_sk = c_customer_sk\n" +
            "      AND ss_cdemo_sk = cd1.cd_demo_sk\n" +
            "      AND ss_hdemo_sk = hd1.hd_demo_sk\n" +
            "      AND ss_addr_sk = ad1.ca_address_sk\n" +
            "      and ss_item_sk = i_item_sk\n" +
            "      and ss_item_sk = sr_item_sk\n" +
            "      and ss_ticket_number = sr_ticket_number\n" +
            "      and ss_item_sk = cs_ui.cs_item_sk\n" +
            "      and c_current_cdemo_sk = cd2.cd_demo_sk\n" +
            "      AND c_current_hdemo_sk = hd2.hd_demo_sk\n" +
            "      AND c_current_addr_sk = ad2.ca_address_sk\n" +
            "      and c_first_sales_date_sk = d2.d_date_sk\n" +
            "      and c_first_shipto_date_sk = d3.d_date_sk\n" +
            "      and ss_promo_sk = p_promo_sk\n" +
            "      and hd1.hd_income_band_sk = ib1.ib_income_band_sk\n" +
            "      and hd2.hd_income_band_sk = ib2.ib_income_band_sk\n" +
            "      and cd1.cd_marital_status <> cd2.cd_marital_status\n" +
            "      and i_color in (\n" +
            "        'light',\n" +
            "        'cyan',\n" +
            "        'burnished',\n" +
            "        'green',\n" +
            "        'almond',\n" +
            "        'smoke'\n" +
            "      )\n" +
            "      and i_current_price between 22\n" +
            "      and 22 + 10\n" +
            "      and i_current_price between 22 + 1\n" +
            "      and 22 + 15\n" +
            "    group by\n" +
            "      i_product_name,\n" +
            "      i_item_sk,\n" +
            "      s_store_name,\n" +
            "      i_item_sk,\n" +
            "      s_store_name,\n" +
            "      s_zip,\n" +
            "      ad1.ca_street_number,\n" +
            "      ad1.ca_street_name,\n" +
            "      ad1.ca_city,\n" +
            "      ad1.ca_zip,\n" +
            "      ad2.ca_street_number,\n" +
            "      ad2.ca_street_name,\n" +
            "      ad2.ca_city,\n" +
            "      ad2.ca_zip,\n" +
            "      d1.d_year,\n" +
            "      d2.d_year,\n" +
            "      d3.d_year\n" +
            "  )\n" +
            "select\n" +
            "  cs1.product_name,\n" +
            "  cs1.store_name,\n" +
            "  cs1.store_zip,\n" +
            "  cs1.b_street_number,\n" +
            "  cs1.b_street_name,\n" +
            "  cs1.b_city,\n" +
            "  cs1.b_zip,\n" +
            "  cs1.c_street_number,\n" +
            "  cs1.c_street_name,\n" +
            "  cs1.c_city,\n" +
            "  cs1.c_zip,\n" +
            "  cs1.syear,\n" +
            "  cs1.cnt,\n" +
            "  cs1.s1 as s11,\n" +
            "  cs1.s2 as s21,\n" +
            "  cs1.s3 as s31,\n" +
            "  cs2.s1 as s12,\n" +
            "  cs2.s2 as s22,\n" +
            "  cs2.s3 as s32,\n" +
            "  cs2.syear,\n" +
            "  cs2.cnt\n" +
            "from\n" +
            "  cross_sales cs1,\n" +
            "  cross_sales cs2\n" +
            "where\n" +
            "  cs1.item_sk = cs2.item_sk\n" +
            "  and cs1.syear = 2001\n" +
            "  and cs2.syear = 2001 + 1\n" +
            "  and cs2.cnt <= cs1.cnt\n" +
            "  and cs1.store_name = cs2.store_name\n" +
            "  and cs1.store_zip = cs2.store_zip\n" +
            "order by\n" +
            "  cs1.product_name,\n" +
            "  cs1.store_name,\n" +
            "  cs2.cnt,\n" +
            "  cs1.s1,\n" +
            "  cs2.s1;";

    public static final String Q21 = "select\n" +
            "    sum(ss_net_profit)/sum(ss_ext_sales_price) as gross_margin\n" +
            "   ,i_category\n" +
            "   ,i_class\n" +
            "   ,grouping(i_category)+grouping(i_class) as lochierarchy\n" +
            "   ,rank() over (\n" +
            "    partition by grouping(i_category)+grouping(i_class),\n" +
            "    case when grouping(i_class) = 0 then i_category end\n" +
            "    order by sum(ss_net_profit)/sum(ss_ext_sales_price) asc) as rank_within_parent\n" +
            " from\n" +
            "    store_sales\n" +
            "   ,date_dim       d1\n" +
            "   ,item\n" +
            "   ,store\n" +
            " where\n" +
            "    d1.d_year = 2001\n" +
            " and d1.d_date_sk = ss_sold_date_sk\n" +
            " and i_item_sk  = ss_item_sk\n" +
            " and s_store_sk  = ss_store_sk\n" +
            " and s_state in ('TN','TN','TN','TN',\n" +
            "                 'TN','TN','TN','TN')\n" +
            " group by rollup(i_category,i_class)\n" +
            " order by\n" +
            "   lochierarchy desc\n" +
            "  ,case when lochierarchy = 0 then i_category end\n" +
            "  ,rank_within_parent\n" +
            "  limit 100;";

    public static final String Q22 = "with ss as (\n" +
            "    select\n" +
            "      i_manufact_id,\n" +
            "      sum(ss_ext_sales_price) total_sales\n" +
            "    from\n" +
            "      store_sales,\n" +
            "      date_dim,\n" +
            "      customer_address,\n" +
            "      item\n" +
            "    where\n" +
            "      i_manufact_id in (\n" +
            "        select\n" +
            "          i_manufact_id\n" +
            "        from\n" +
            "          item\n" +
            "        where\n" +
            "          i_category in ('Books')\n" +
            "      )\n" +
            "      and ss_item_sk = i_item_sk\n" +
            "      and ss_sold_date_sk = d_date_sk\n" +
            "      and d_year = 1999\n" +
            "      and d_moy = 4\n" +
            "      and ss_addr_sk = ca_address_sk\n" +
            "      and ca_gmt_offset = -5\n" +
            "    group by\n" +
            "      i_manufact_id\n" +
            "  ),\n" +
            "  cs as (\n" +
            "    select\n" +
            "      i_manufact_id,\n" +
            "      sum(cs_ext_sales_price) total_sales\n" +
            "    from\n" +
            "      catalog_sales,\n" +
            "      date_dim,\n" +
            "      customer_address,\n" +
            "      item\n" +
            "    where\n" +
            "      i_manufact_id in (\n" +
            "        select\n" +
            "          i_manufact_id\n" +
            "        from\n" +
            "          item\n" +
            "        where\n" +
            "          i_category in ('Books')\n" +
            "      )\n" +
            "      and cs_item_sk = i_item_sk\n" +
            "      and cs_sold_date_sk = d_date_sk\n" +
            "      and d_year = 1999\n" +
            "      and d_moy = 4\n" +
            "      and cs_bill_addr_sk = ca_address_sk\n" +
            "      and ca_gmt_offset = -5\n" +
            "    group by\n" +
            "      i_manufact_id\n" +
            "  ),\n" +
            "  ws as (\n" +
            "    select\n" +
            "      i_manufact_id,\n" +
            "      sum(ws_ext_sales_price) total_sales\n" +
            "    from\n" +
            "      web_sales,\n" +
            "      date_dim,\n" +
            "      customer_address,\n" +
            "      item\n" +
            "    where\n" +
            "      i_manufact_id in (\n" +
            "        select\n" +
            "          i_manufact_id\n" +
            "        from\n" +
            "          item\n" +
            "        where\n" +
            "          i_category in ('Books')\n" +
            "      )\n" +
            "      and ws_item_sk = i_item_sk\n" +
            "      and ws_sold_date_sk = d_date_sk\n" +
            "      and d_year = 1999\n" +
            "      and d_moy = 4\n" +
            "      and ws_bill_addr_sk = ca_address_sk\n" +
            "      and ca_gmt_offset = -5\n" +
            "    group by\n" +
            "      i_manufact_id\n" +
            "  )\n" +
            "select\n" +
            "  i_manufact_id,\n" +
            "  sum(total_sales) total_sales\n" +
            "from\n" +
            "  (\n" +
            "    select\n" +
            "      *\n" +
            "    from\n" +
            "      ss\n" +
            "    union all\n" +
            "    select\n" +
            "      *\n" +
            "    from\n" +
            "      cs\n" +
            "    union all\n" +
            "    select\n" +
            "      *\n" +
            "    from\n" +
            "      ws\n" +
            "  ) tmp1\n" +
            "group by\n" +
            "  i_manufact_id\n" +
            "order by\n" +
            "  total_sales\n" +
            "limit\n" +
            "  100;";

    public static final String Q23 = "select\n" +
            "  c_last_name,\n" +
            "  c_first_name,\n" +
            "  ca_city,\n" +
            "  bought_city,\n" +
            "  ss_ticket_number,\n" +
            "  amt,\n" +
            "  profit\n" +
            "from\n" +
            "  (\n" +
            "    select\n" +
            "      ss_ticket_number,\n" +
            "      ss_customer_sk,\n" +
            "      ca_city bought_city,\n" +
            "      sum(ss_coupon_amt) amt,\n" +
            "      sum(ss_net_profit) profit\n" +
            "    from\n" +
            "      store_sales,\n" +
            "      date_dim,\n" +
            "      store,\n" +
            "      household_demographics,\n" +
            "      customer_address\n" +
            "    where\n" +
            "      store_sales.ss_sold_date_sk = date_dim.d_date_sk\n" +
            "      and store_sales.ss_store_sk = store.s_store_sk\n" +
            "      and store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk\n" +
            "      and store_sales.ss_addr_sk = customer_address.ca_address_sk\n" +
            "      and (\n" +
            "        household_demographics.hd_dep_count = 3\n" +
            "        or household_demographics.hd_vehicle_count = 1\n" +
            "      )\n" +
            "      and date_dim.d_dow in (6, 0)\n" +
            "      and date_dim.d_year in (1999, 1999 + 1, 1999 + 2)\n" +
            "      and store.s_city in (\n" +
            "        'Midway',\n" +
            "        'Fairview',\n" +
            "        'Fairview',\n" +
            "        'Midway',\n" +
            "        'Fairview'\n" +
            "      )\n" +
            "    group by\n" +
            "      ss_ticket_number,\n" +
            "      ss_customer_sk,\n" +
            "      ss_addr_sk,\n" +
            "      ca_city\n" +
            "  ) dn,\n" +
            "  customer,\n" +
            "  customer_address current_addr\n" +
            "where\n" +
            "  ss_customer_sk = c_customer_sk\n" +
            "  and customer.c_current_addr_sk = current_addr.ca_address_sk\n" +
            "  and current_addr.ca_city <> bought_city\n" +
            "order by\n" +
            "  c_last_name,\n" +
            "  c_first_name,\n" +
            "  ca_city,\n" +
            "  bought_city,\n" +
            "  ss_ticket_number\n" +
            "limit\n" +
            "  100;";

    public static final String Q24 = "select\n" +
            "  substr(w_warehouse_name, 1, 20),\n" +
            "  sm_type,\n" +
            "  web_name,\n" +
            "  sum(\n" +
            "    case\n" +
            "      when (ws_ship_date_sk - ws_sold_date_sk <= 30) then 1\n" +
            "      else 0\n" +
            "    end\n" +
            "  ) as \"30 days\",\n" +
            "  sum(\n" +
            "    case\n" +
            "      when (ws_ship_date_sk - ws_sold_date_sk > 30)\n" +
            "      and (ws_ship_date_sk - ws_sold_date_sk <= 60) then 1\n" +
            "      else 0\n" +
            "    end\n" +
            "  ) as \"31-60 days\",\n" +
            "  sum(\n" +
            "    case\n" +
            "      when (ws_ship_date_sk - ws_sold_date_sk > 60)\n" +
            "      and (ws_ship_date_sk - ws_sold_date_sk <= 90) then 1\n" +
            "      else 0\n" +
            "    end\n" +
            "  ) as \"61-90 days\",\n" +
            "  sum(\n" +
            "    case\n" +
            "      when (ws_ship_date_sk - ws_sold_date_sk > 90)\n" +
            "      and (ws_ship_date_sk - ws_sold_date_sk <= 120) then 1\n" +
            "      else 0\n" +
            "    end\n" +
            "  ) as \"91-120 days\",\n" +
            "  sum(\n" +
            "    case\n" +
            "      when (ws_ship_date_sk - ws_sold_date_sk > 120) then 1\n" +
            "      else 0\n" +
            "    end\n" +
            "  ) as \">120 days\"\n" +
            "from\n" +
            "  web_sales,\n" +
            "  warehouse,\n" +
            "  ship_mode,\n" +
            "  web_site,\n" +
            "  date_dim\n" +
            "where\n" +
            "  d_month_seq between 1217\n" +
            "  and 1217 + 11\n" +
            "  and ws_ship_date_sk = d_date_sk\n" +
            "  and ws_warehouse_sk = w_warehouse_sk\n" +
            "  and ws_ship_mode_sk = sm_ship_mode_sk\n" +
            "  and ws_web_site_sk = web_site_sk\n" +
            "group by\n" +
            "  substr(w_warehouse_name, 1, 20),\n" +
            "  sm_type,\n" +
            "  web_name\n" +
            "order by\n" +
            "  substr(w_warehouse_name, 1, 20),\n" +
            "  sm_type,\n" +
            "  web_name\n" +
            "limit\n" +
            "  100;";

    public static final String Q25 = "select\n" +
            "   count(distinct cs_order_number) as \"order count\"\n" +
            "  ,sum(cs_ext_ship_cost) as \"total shipping cost\"\n" +
            "  ,sum(cs_net_profit) as \"total net profit\"\n" +
            "from\n" +
            "   catalog_sales cs1\n" +
            "  ,date_dim\n" +
            "  ,customer_address\n" +
            "  ,call_center\n" +
            "where\n" +
            "    d_date between '1999-5-01' and\n" +
            "           date_add(cast('1999-5-01' as date), 60)\n" +
            "and cs1.cs_ship_date_sk = d_date_sk\n" +
            "and cs1.cs_ship_addr_sk = ca_address_sk  \n" +
            "and ca_state = 'ID'\n" +
            "and cs1.cs_call_center_sk = cc_call_center_sk\n" +
            "and cc_county in ('Williamson County','Williamson County','Williamson County','Williamson County',\n" +
            "                  'Williamson County'\n" +
            ")\n" +
            "and exists (select *\n" +
            "            from catalog_sales cs2\n" +
            "            where cs1.cs_order_number = cs2.cs_order_number\n" +
            "              and cs1.cs_warehouse_sk <> cs2.cs_warehouse_sk)\n" +
            "and not exists(select *\n" +
            "               from catalog_returns cr1\n" +
            "               where cs1.cs_order_number = cr1.cr_order_number)\n" +
            "order by count(distinct cs_order_number)\n" +
            "limit 100;";

    public static final String Q26 = "select\n" +
            "  cd_gender,\n" +
            "  cd_marital_status,\n" +
            "  cd_education_status,\n" +
            "  count(*) cnt1,\n" +
            "  cd_purchase_estimate,\n" +
            "  count(*) cnt2,\n" +
            "  cd_credit_rating,\n" +
            "  count(*) cnt3,\n" +
            "  cd_dep_count,\n" +
            "  count(*) cnt4,\n" +
            "  cd_dep_employed_count,\n" +
            "  count(*) cnt5,\n" +
            "  cd_dep_college_count,\n" +
            "  count(*) cnt6\n" +
            " from\n" +
            "  customer c,customer_address ca,customer_demographics\n" +
            " where\n" +
            "  c.c_current_addr_sk = ca.ca_address_sk and\n" +
            "  ca_county in ('Clinton County','Platte County','Franklin County','Louisa County','Harmon County') and\n" +
            "  cd_demo_sk = c.c_current_cdemo_sk and\n" +
            "  exists (select *\n" +
            "          from store_sales,date_dim\n" +
            "          where c.c_customer_sk = ss_customer_sk and\n" +
            "                ss_sold_date_sk = d_date_sk and\n" +
            "                d_year = 2002 and\n" +
            "                d_moy between 3 and 3+3) and\n" +
            "   (exists (select *\n" +
            "            from web_sales,date_dim\n" +
            "            where c.c_customer_sk = ws_bill_customer_sk and\n" +
            "                  ws_sold_date_sk = d_date_sk and\n" +
            "                  d_year = 2002 and\n" +
            "                  d_moy between 3 ANd 3+3) or\n" +
            "    exists (select *\n" +
            "            from catalog_sales,date_dim\n" +
            "            where c.c_customer_sk = cs_ship_customer_sk and\n" +
            "                  cs_sold_date_sk = d_date_sk and\n" +
            "                  d_year = 2002 and\n" +
            "                  d_moy between 3 and 3+3))\n" +
            " group by cd_gender,\n" +
            "          cd_marital_status,\n" +
            "          cd_education_status,\n" +
            "          cd_purchase_estimate,\n" +
            "          cd_credit_rating,\n" +
            "          cd_dep_count,\n" +
            "          cd_dep_employed_count,\n" +
            "          cd_dep_college_count\n" +
            " order by cd_gender,\n" +
            "          cd_marital_status,\n" +
            "          cd_education_status,\n" +
            "          cd_purchase_estimate,\n" +
            "          cd_credit_rating,\n" +
            "          cd_dep_count,\n" +
            "          cd_dep_employed_count,\n" +
            "          cd_dep_college_count\n" +
            "limit 100;\n" +
            "\n";

    public static final String Q27 = "select  *\n" +
            "from (select i_manager_id\n" +
            "             ,sum(ss_sales_price) sum_sales\n" +
            "             ,avg(sum(ss_sales_price)) over (partition by i_manager_id) avg_monthly_sales\n" +
            "      from item\n" +
            "          ,store_sales\n" +
            "          ,date_dim\n" +
            "          ,store\n" +
            "      where ss_item_sk = i_item_sk\n" +
            "        and ss_sold_date_sk = d_date_sk\n" +
            "        and ss_store_sk = s_store_sk\n" +
            "        and d_month_seq in (1181,1181+1,1181+2,1181+3,1181+4,1181+5,1181+6,1181+7,1181+8,1181+9,1181+10,1181+11)\n" +
            "        and ((    i_category in ('Books','Children','Electronics')\n" +
            "              and i_class in ('personal','portable','reference','self-help')\n" +
            "              and i_brand in ('scholaramalgamalg #14','scholaramalgamalg #7',\n" +
            "                          'exportiunivamalg #9','scholaramalgamalg #9'))\n" +
            "           or(    i_category in ('Women','Music','Men')\n" +
            "              and i_class in ('accessories','classical','fragrances','pants')\n" +
            "              and i_brand in ('amalgimporto #1','edu packscholar #1','exportiimporto #1',\n" +
            "                         'importoamalg #1')))\n" +
            "group by i_manager_id, d_moy) tmp1\n" +
            "where case when avg_monthly_sales > 0 " +
            "then abs (sum_sales - avg_monthly_sales) / avg_monthly_sales else null end > 0.1\n" +
            "order by i_manager_id\n" +
            "        ,avg_monthly_sales\n" +
            "        ,sum_sales\n" +
            "limit 100;";

    public static final String Q28 = "select\n" +
            "  cd_gender,\n" +
            "  cd_marital_status,\n" +
            "  cd_education_status,\n" +
            "  count(*) cnt1,\n" +
            "  cd_purchase_estimate,\n" +
            "  count(*) cnt2,\n" +
            "  cd_credit_rating,\n" +
            "  count(*) cnt3\n" +
            "from\n" +
            "  customer c,\n" +
            "  customer_address ca,\n" +
            "  customer_demographics\n" +
            "where\n" +
            "  c.c_current_addr_sk = ca.ca_address_sk\n" +
            "  and ca_state in ('IN', 'VA', 'MS')\n" +
            "  and cd_demo_sk = c.c_current_cdemo_sk\n" +
            "  and exists (\n" +
            "    select\n" +
            "      *\n" +
            "    from\n" +
            "      store_sales,\n" +
            "      date_dim\n" +
            "    where\n" +
            "      c.c_customer_sk = ss_customer_sk\n" +
            "      and ss_sold_date_sk = d_date_sk\n" +
            "      and d_year = 2002\n" +
            "      and d_moy between 2\n" +
            "      and 2 + 2\n" +
            "  )\n" +
            "  and (\n" +
            "    not exists (\n" +
            "      select\n" +
            "        *\n" +
            "      from\n" +
            "        web_sales,\n" +
            "        date_dim\n" +
            "      where\n" +
            "        c.c_customer_sk = ws_bill_customer_sk\n" +
            "        and ws_sold_date_sk = d_date_sk\n" +
            "        and d_year = 2002\n" +
            "        and d_moy between 2\n" +
            "        and 2 + 2\n" +
            "    )\n" +
            "    and not exists (\n" +
            "      select\n" +
            "        *\n" +
            "      from\n" +
            "        catalog_sales,\n" +
            "        date_dim\n" +
            "      where\n" +
            "        c.c_customer_sk = cs_ship_customer_sk\n" +
            "        and cs_sold_date_sk = d_date_sk\n" +
            "        and d_year = 2002\n" +
            "        and d_moy between 2\n" +
            "        and 2 + 2\n" +
            "    )\n" +
            "  )\n" +
            "group by\n" +
            "  cd_gender,\n" +
            "  cd_marital_status,\n" +
            "  cd_education_status,\n" +
            "  cd_purchase_estimate,\n" +
            "  cd_credit_rating\n" +
            "order by\n" +
            "  cd_gender,\n" +
            "  cd_marital_status,\n" +
            "  cd_education_status,\n" +
            "  cd_purchase_estimate,\n" +
            "  cd_credit_rating\n" +
            "limit\n" +
            "  100;";

    public static final String Q29 = "with ss as (\n" +
            "    select\n" +
            "      i_item_id,\n" +
            "      sum(ss_ext_sales_price) total_sales\n" +
            "    from\n" +
            "      store_sales,\n" +
            "      date_dim,\n" +
            "      customer_address,\n" +
            "      item\n" +
            "    where\n" +
            "      i_item_id in (\n" +
            "        select\n" +
            "          i_item_id\n" +
            "        from\n" +
            "          item\n" +
            "        where\n" +
            "          i_category in ('Shoes')\n" +
            "      )\n" +
            "      and ss_item_sk = i_item_sk\n" +
            "      and ss_sold_date_sk = d_date_sk\n" +
            "      and d_year = 2001\n" +
            "      and d_moy = 10\n" +
            "      and ss_addr_sk = ca_address_sk\n" +
            "      and ca_gmt_offset = -6\n" +
            "    group by\n" +
            "      i_item_id\n" +
            "  ),\n" +
            "  cs as (\n" +
            "    select\n" +
            "      i_item_id,\n" +
            "      sum(cs_ext_sales_price) total_sales\n" +
            "    from\n" +
            "      catalog_sales,\n" +
            "      date_dim,\n" +
            "      customer_address,\n" +
            "      item\n" +
            "    where\n" +
            "      i_item_id in (\n" +
            "        select\n" +
            "          i_item_id\n" +
            "        from\n" +
            "          item\n" +
            "        where\n" +
            "          i_category in ('Shoes')\n" +
            "      )\n" +
            "      and cs_item_sk = i_item_sk\n" +
            "      and cs_sold_date_sk = d_date_sk\n" +
            "      and d_year = 2001\n" +
            "      and d_moy = 10\n" +
            "      and cs_bill_addr_sk = ca_address_sk\n" +
            "      and ca_gmt_offset = -6\n" +
            "    group by\n" +
            "      i_item_id\n" +
            "  ),\n" +
            "  ws as (\n" +
            "    select\n" +
            "      i_item_id,\n" +
            "      sum(ws_ext_sales_price) total_sales\n" +
            "    from\n" +
            "      web_sales,\n" +
            "      date_dim,\n" +
            "      customer_address,\n" +
            "      item\n" +
            "    where\n" +
            "      i_item_id in (\n" +
            "        select\n" +
            "          i_item_id\n" +
            "        from\n" +
            "          item\n" +
            "        where\n" +
            "          i_category in ('Shoes')\n" +
            "      )\n" +
            "      and ws_item_sk = i_item_sk\n" +
            "      and ws_sold_date_sk = d_date_sk\n" +
            "      and d_year = 2001\n" +
            "      and d_moy = 10\n" +
            "      and ws_bill_addr_sk = ca_address_sk\n" +
            "      and ca_gmt_offset = -6\n" +
            "    group by\n" +
            "      i_item_id\n" +
            "  )\n" +
            "select\n" +
            "  i_item_id,\n" +
            "  sum(total_sales) total_sales\n" +
            "from\n" +
            "  (\n" +
            "    select\n" +
            "      *\n" +
            "    from\n" +
            "      ss\n" +
            "    union all\n" +
            "    select\n" +
            "      *\n" +
            "    from\n" +
            "      cs\n" +
            "    union all\n" +
            "    select\n" +
            "      *\n" +
            "    from\n" +
            "      ws\n" +
            "  ) tmp1\n" +
            "group by\n" +
            "  i_item_id\n" +
            "order by\n" +
            "  i_item_id,\n" +
            "  total_sales\n" +
            "limit\n" +
            "  100;";

    public static final String Q30 = "with wss as (\n" +
            "    select\n" +
            "      d_week_seq,\n" +
            "      ss_store_sk,\n" +
            "      sum(\n" +
            "        case\n" +
            "          when (d_day_name = 'Sunday') then ss_sales_price\n" +
            "          else null\n" +
            "        end\n" +
            "      ) sun_sales,\n" +
            "      sum(\n" +
            "        case\n" +
            "          when (d_day_name = 'Monday') then ss_sales_price\n" +
            "          else null\n" +
            "        end\n" +
            "      ) mon_sales,\n" +
            "      sum(\n" +
            "        case\n" +
            "          when (d_day_name = 'Tuesday') then ss_sales_price\n" +
            "          else null\n" +
            "        end\n" +
            "      ) tue_sales,\n" +
            "      sum(\n" +
            "        case\n" +
            "          when (d_day_name = 'Wednesday') then ss_sales_price\n" +
            "          else null\n" +
            "        end\n" +
            "      ) wed_sales,\n" +
            "      sum(\n" +
            "        case\n" +
            "          when (d_day_name = 'Thursday') then ss_sales_price\n" +
            "          else null\n" +
            "        end\n" +
            "      ) thu_sales,\n" +
            "      sum(\n" +
            "        case\n" +
            "          when (d_day_name = 'Friday') then ss_sales_price\n" +
            "          else null\n" +
            "        end\n" +
            "      ) fri_sales,\n" +
            "      sum(\n" +
            "        case\n" +
            "          when (d_day_name = 'Saturday') then ss_sales_price\n" +
            "          else null\n" +
            "        end\n" +
            "      ) sat_sales\n" +
            "    from\n" +
            "      store_sales,\n" +
            "      date_dim\n" +
            "    where\n" +
            "      d_date_sk = ss_sold_date_sk\n" +
            "    group by\n" +
            "      d_week_seq,\n" +
            "      ss_store_sk\n" +
            "  )\n" +
            "select\n" +
            "  s_store_name1,\n" +
            "  s_store_id1,\n" +
            "  d_week_seq1,\n" +
            "  sun_sales1 / sun_sales2,\n" +
            "  mon_sales1 / mon_sales2,\n" +
            "  tue_sales1 / tue_sales2,\n" +
            "  wed_sales1 / wed_sales2,\n" +
            "  thu_sales1 / thu_sales2,\n" +
            "  fri_sales1 / fri_sales2,\n" +
            "  sat_sales1 / sat_sales2\n" +
            "from\n" +
            "  (\n" +
            "    select\n" +
            "      s_store_name s_store_name1,\n" +
            "      wss.d_week_seq d_week_seq1,\n" +
            "      s_store_id s_store_id1,\n" +
            "      sun_sales sun_sales1,\n" +
            "      mon_sales mon_sales1,\n" +
            "      tue_sales tue_sales1,\n" +
            "      wed_sales wed_sales1,\n" +
            "      thu_sales thu_sales1,\n" +
            "      fri_sales fri_sales1,\n" +
            "      sat_sales sat_sales1\n" +
            "    from\n" +
            "      wss,\n" +
            "      store,\n" +
            "      date_dim d\n" +
            "    where\n" +
            "      d.d_week_seq = wss.d_week_seq\n" +
            "      and ss_store_sk = s_store_sk\n" +
            "      and d_month_seq between 1206\n" +
            "      and 1206 + 11\n" +
            "  ) y,\n" +
            "  (\n" +
            "    select\n" +
            "      s_store_name s_store_name2,\n" +
            "      wss.d_week_seq d_week_seq2,\n" +
            "      s_store_id s_store_id2,\n" +
            "      sun_sales sun_sales2,\n" +
            "      mon_sales mon_sales2,\n" +
            "      tue_sales tue_sales2,\n" +
            "      wed_sales wed_sales2,\n" +
            "      thu_sales thu_sales2,\n" +
            "      fri_sales fri_sales2,\n" +
            "      sat_sales sat_sales2\n" +
            "    from\n" +
            "      wss,\n" +
            "      store,\n" +
            "      date_dim d\n" +
            "    where\n" +
            "      d.d_week_seq = wss.d_week_seq\n" +
            "      and ss_store_sk = s_store_sk\n" +
            "      and d_month_seq between 1206 + 12\n" +
            "      and 1206 + 23\n" +
            "  ) x\n" +
            "where\n" +
            "  s_store_id1 = s_store_id2\n" +
            "  and d_week_seq1 = d_week_seq2 -52\n" +
            "order by\n" +
            "  s_store_name1,\n" +
            "  s_store_id1,\n" +
            "  d_week_seq1\n" +
            "limit\n" +
            "  100;";

    public static final String Q31 = "select\n" +
            "  i_item_id,\n" +
            "  i_item_desc,\n" +
            "  i_current_price\n" +
            "from\n" +
            "  item,\n" +
            "  inventory,\n" +
            "  date_dim,\n" +
            "  catalog_sales\n" +
            "where\n" +
            "  i_current_price between 26\n" +
            "  and 26 + 30\n" +
            "  and inv_item_sk = i_item_sk\n" +
            "  and d_date_sk = inv_date_sk\n" +
            "  and d_date between cast('2001-06-09' as date)\n" +
            "  and date_add(cast('2001-06-09' as date), 60)\n" +
            "  and i_manufact_id in (744, 884, 722, 693)\n" +
            "  and inv_quantity_on_hand between 100\n" +
            "  and 500\n" +
            "  and cs_item_sk = i_item_sk\n" +
            "group by\n" +
            "  i_item_id,\n" +
            "  i_item_desc,\n" +
            "  i_current_price\n" +
            "order by\n" +
            "  i_item_id\n" +
            "limit\n" +
            "  100;";

    public static final String Q32 = "select i_item_id\n" +
            "      ,i_item_desc\n" +
            "      ,i_category\n" +
            "      ,i_class\n" +
            "      ,i_current_price\n" +
            "      ,sum(ss_ext_sales_price) as itemrevenue\n" +
            "      ,sum(ss_ext_sales_price)*100/sum(sum(ss_ext_sales_price)) over\n" +
            "          (partition by i_class) as revenueratio\n" +
            "from\n" +
            "    store_sales\n" +
            "        ,item\n" +
            "        ,date_dim\n" +
            "where\n" +
            "    ss_item_sk = i_item_sk\n" +
            "    and i_category in ('Shoes', 'Music', 'Men')\n" +
            "    and ss_sold_date_sk = d_date_sk\n" +
            "    and d_date between cast('2000-01-05' as date)\n" +
            "                and date_add(cast('2000-01-05' as date),30)\n" +
            "group by\n" +
            "    i_item_id\n" +
            "        ,i_item_desc\n" +
            "        ,i_category\n" +
            "        ,i_class\n" +
            "        ,i_current_price\n" +
            "order by\n" +
            "    i_category\n" +
            "        ,i_class\n" +
            "        ,i_item_id\n" +
            "        ,i_item_desc\n" +
            "        ,revenueratio;\n";

    public static final String Q33 = "select\n" +
            "  substr(r_reason_desc, 1, 20),\n" +
            "  avg(ws_quantity),\n" +
            "  avg(wr_refunded_cash),\n" +
            "  avg(wr_fee)\n" +
            "from\n" +
            "  web_sales,\n" +
            "  web_returns,\n" +
            "  web_page,\n" +
            "  customer_demographics cd1,\n" +
            "  customer_demographics cd2,\n" +
            "  customer_address,\n" +
            "  date_dim,\n" +
            "  reason\n" +
            "where\n" +
            "  ws_web_page_sk = wp_web_page_sk\n" +
            "  and ws_item_sk = wr_item_sk\n" +
            "  and ws_order_number = wr_order_number\n" +
            "  and ws_sold_date_sk = d_date_sk\n" +
            "  and d_year = 2001\n" +
            "  and cd1.cd_demo_sk = wr_refunded_cdemo_sk\n" +
            "  and cd2.cd_demo_sk = wr_returning_cdemo_sk\n" +
            "  and ca_address_sk = wr_refunded_addr_sk\n" +
            "  and r_reason_sk = wr_reason_sk\n" +
            "  and (\n" +
            "    (\n" +
            "      cd1.cd_marital_status = 'D'\n" +
            "      and cd1.cd_marital_status = cd2.cd_marital_status\n" +
            "      and cd1.cd_education_status = 'Primary'\n" +
            "      and cd1.cd_education_status = cd2.cd_education_status\n" +
            "      and ws_sales_price between 100.00\n" +
            "      and 150.00\n" +
            "    )\n" +
            "    or (\n" +
            "      cd1.cd_marital_status = 'U'\n" +
            "      and cd1.cd_marital_status = cd2.cd_marital_status\n" +
            "      and cd1.cd_education_status = 'Unknown'\n" +
            "      and cd1.cd_education_status = cd2.cd_education_status\n" +
            "      and ws_sales_price between 50.00\n" +
            "      and 100.00\n" +
            "    )\n" +
            "    or (\n" +
            "      cd1.cd_marital_status = 'M'\n" +
            "      and cd1.cd_marital_status = cd2.cd_marital_status\n" +
            "      and cd1.cd_education_status = 'Advanced Degree'\n" +
            "      and cd1.cd_education_status = cd2.cd_education_status\n" +
            "      and ws_sales_price between 150.00\n" +
            "      and 200.00\n" +
            "    )\n" +
            "  )\n" +
            "  and (\n" +
            "    (\n" +
            "      ca_country = 'United States'\n" +
            "      and ca_state in ('SC', 'IN', 'VA')\n" +
            "      and ws_net_profit between 100\n" +
            "      and 200\n" +
            "    )\n" +
            "    or (\n" +
            "      ca_country = 'United States'\n" +
            "      and ca_state in ('WA', 'KS', 'KY')\n" +
            "      and ws_net_profit between 150\n" +
            "      and 300\n" +
            "    )\n" +
            "    or (\n" +
            "      ca_country = 'United States'\n" +
            "      and ca_state in ('SD', 'WI', 'NE')\n" +
            "      and ws_net_profit between 50\n" +
            "      and 250\n" +
            "    )\n" +
            "  )\n" +
            "group by\n" +
            "  r_reason_desc\n" +
            "order by\n" +
            "  substr(r_reason_desc, 1, 20),\n" +
            "  avg(ws_quantity),\n" +
            "  avg(wr_refunded_cash),\n" +
            "  avg(wr_fee)\n" +
            "limit\n" +
            "  100;";

    public static final String Q34 = "select\n" +
            "    sum(ss_net_profit) as total_sum\n" +
            "   ,s_state\n" +
            "   ,s_county\n" +
            "   ,grouping(s_state)+grouping(s_county) as lochierarchy\n" +
            "   ,rank() over (\n" +
            "    partition by grouping(s_state)+grouping(s_county),\n" +
            "    case when grouping(s_county) = 0 then s_state end\n" +
            "    order by sum(ss_net_profit) desc) as rank_within_parent\n" +
            " from\n" +
            "    store_sales\n" +
            "   ,date_dim       d1\n" +
            "   ,store\n" +
            " where\n" +
            "    d1.d_month_seq between 1180 and 1180+11\n" +
            " and d1.d_date_sk = ss_sold_date_sk\n" +
            " and s_store_sk  = ss_store_sk\n" +
            " and s_state in\n" +
            "             ( select s_state\n" +
            "               from  (select s_state as s_state,\n" +
            "                rank() over ( partition by s_state order by sum(ss_net_profit) desc) as ranking\n" +
            "                      from   store_sales, store, date_dim\n" +
            "                      where  d_month_seq between 1180 and 1180+11\n" +
            "                and d_date_sk = ss_sold_date_sk\n" +
            "                and s_store_sk  = ss_store_sk\n" +
            "                      group by s_state\n" +
            "                     ) tmp1\n" +
            "               where ranking <= 5\n" +
            "             )\n" +
            " group by rollup(s_state,s_county)\n" +
            " order by\n" +
            "   lochierarchy desc\n" +
            "  ,case when lochierarchy = 0 then s_state end\n" +
            "  ,rank_within_parent\n" +
            " limit 100;";

    public static final String Q35 = "select\n" +
            "  *\n" +
            "from\n" +
            "  (\n" +
            "    select\n" +
            "      i_category,\n" +
            "      i_class,\n" +
            "      i_brand,\n" +
            "      i_product_name,\n" +
            "      d_year,\n" +
            "      d_qoy,\n" +
            "      d_moy,\n" +
            "      s_store_id,\n" +
            "      sumsales,\n" +
            "      rank() over (\n" +
            "        partition by i_category\n" +
            "        order by\n" +
            "          sumsales desc\n" +
            "      ) rk\n" +
            "    from\n" +
            "      (\n" +
            "        select\n" +
            "          i_category,\n" +
            "          i_class,\n" +
            "          i_brand,\n" +
            "          i_product_name,\n" +
            "          d_year,\n" +
            "          d_qoy,\n" +
            "          d_moy,\n" +
            "          s_store_id,\n" +
            "          sum(coalesce(ss_sales_price * ss_quantity, 0)) sumsales\n" +
            "        from\n" +
            "          store_sales,\n" +
            "          date_dim,\n" +
            "          store,\n" +
            "          item\n" +
            "        where\n" +
            "          ss_sold_date_sk = d_date_sk\n" +
            "          and ss_item_sk = i_item_sk\n" +
            "          and ss_store_sk = s_store_sk\n" +
            "          and d_month_seq between 1194\n" +
            "          and 1194 + 11\n" +
            "        group by\n" +
            "          rollup(\n" +
            "            i_category,\n" +
            "            i_class,\n" +
            "            i_brand,\n" +
            "            i_product_name,\n" +
            "            d_year,\n" +
            "            d_qoy,\n" +
            "            d_moy,\n" +
            "            s_store_id\n" +
            "          )\n" +
            "      ) dw1\n" +
            "  ) dw2\n" +
            "where\n" +
            "  rk <= 100\n" +
            "order by\n" +
            "  i_category,\n" +
            "  i_class,\n" +
            "  i_brand,\n" +
            "  i_product_name,\n" +
            "  d_year,\n" +
            "  d_qoy,\n" +
            "  d_moy,\n" +
            "  s_store_id,\n" +
            "  sumsales,\n" +
            "  rk\n" +
            "limit\n" +
            "  100;";

    public static final String Q36 = "select\n" +
            "  *\n" +
            "from\n" +
            "  (\n" +
            "    select\n" +
            "      avg(ss_list_price) B1_LP,\n" +
            "      count(ss_list_price) B1_CNT,\n" +
            "      count(distinct ss_list_price) B1_CNTD\n" +
            "    from\n" +
            "      store_sales\n" +
            "    where\n" +
            "      ss_quantity between 0\n" +
            "      and 5\n" +
            "      and (\n" +
            "        ss_list_price between 28\n" +
            "        and 28 + 10\n" +
            "        or ss_coupon_amt between 12573\n" +
            "        and 12573 + 1000\n" +
            "        or ss_wholesale_cost between 33\n" +
            "        and 33 + 20\n" +
            "      )\n" +
            "  ) B1,\n" +
            "  (\n" +
            "    select\n" +
            "      avg(ss_list_price) B2_LP,\n" +
            "      count(ss_list_price) B2_CNT,\n" +
            "      count(distinct ss_list_price) B2_CNTD\n" +
            "    from\n" +
            "      store_sales\n" +
            "    where\n" +
            "      ss_quantity between 6\n" +
            "      and 10\n" +
            "      and (\n" +
            "        ss_list_price between 143\n" +
            "        and 143 + 10\n" +
            "        or ss_coupon_amt between 5562\n" +
            "        and 5562 + 1000\n" +
            "        or ss_wholesale_cost between 45\n" +
            "        and 45 + 20\n" +
            "      )\n" +
            "  ) B2,\n" +
            "  (\n" +
            "    select\n" +
            "      avg(ss_list_price) B3_LP,\n" +
            "      count(ss_list_price) B3_CNT,\n" +
            "      count(distinct ss_list_price) B3_CNTD\n" +
            "    from\n" +
            "      store_sales\n" +
            "    where\n" +
            "      ss_quantity between 11\n" +
            "      and 15\n" +
            "      and (\n" +
            "        ss_list_price between 159\n" +
            "        and 159 + 10\n" +
            "        or ss_coupon_amt between 2807\n" +
            "        and 2807 + 1000\n" +
            "        or ss_wholesale_cost between 24\n" +
            "        and 24 + 20\n" +
            "      )\n" +
            "  ) B3,\n" +
            "  (\n" +
            "    select\n" +
            "      avg(ss_list_price) B4_LP,\n" +
            "      count(ss_list_price) B4_CNT,\n" +
            "      count(distinct ss_list_price) B4_CNTD\n" +
            "    from\n" +
            "      store_sales\n" +
            "    where\n" +
            "      ss_quantity between 16\n" +
            "      and 20\n" +
            "      and (\n" +
            "        ss_list_price between 24\n" +
            "        and 24 + 10\n" +
            "        or ss_coupon_amt between 3706\n" +
            "        and 3706 + 1000\n" +
            "        or ss_wholesale_cost between 46\n" +
            "        and 46 + 20\n" +
            "      )\n" +
            "  ) B4,\n" +
            "  (\n" +
            "    select\n" +
            "      avg(ss_list_price) B5_LP,\n" +
            "      count(ss_list_price) B5_CNT,\n" +
            "      count(distinct ss_list_price) B5_CNTD\n" +
            "    from\n" +
            "      store_sales\n" +
            "    where\n" +
            "      ss_quantity between 21\n" +
            "      and 25\n" +
            "      and (\n" +
            "        ss_list_price between 76\n" +
            "        and 76 + 10\n" +
            "        or ss_coupon_amt between 2096\n" +
            "        and 2096 + 1000\n" +
            "        or ss_wholesale_cost between 50\n" +
            "        and 50 + 20\n" +
            "      )\n" +
            "  ) B5,\n" +
            "  (\n" +
            "    select\n" +
            "      avg(ss_list_price) B6_LP,\n" +
            "      count(ss_list_price) B6_CNT,\n" +
            "      count(distinct ss_list_price) B6_CNTD\n" +
            "    from\n" +
            "      store_sales\n" +
            "    where\n" +
            "      ss_quantity between 26\n" +
            "      and 30\n" +
            "      and (\n" +
            "        ss_list_price between 169\n" +
            "        and 169 + 10\n" +
            "        or ss_coupon_amt between 10672\n" +
            "        and 10672 + 1000\n" +
            "        or ss_wholesale_cost between 58\n" +
            "        and 58 + 20\n" +
            "      )\n" +
            "  ) B6\n" +
            "limit\n" +
            "  100;";

    public static final String Q37 = "with customer_total_return as (\n" +
            "    select\n" +
            "      cr_returning_customer_sk as ctr_customer_sk,\n" +
            "      ca_state as ctr_state,\n" +
            "      sum(cr_return_amt_inc_tax) as ctr_total_return\n" +
            "    from\n" +
            "      catalog_returns,\n" +
            "      date_dim,\n" +
            "      customer_address\n" +
            "    where\n" +
            "      cr_returned_date_sk = d_date_sk\n" +
            "      and d_year = 1998\n" +
            "      and cr_returning_addr_sk = ca_address_sk\n" +
            "    group by\n" +
            "      cr_returning_customer_sk,\n" +
            "      ca_state\n" +
            "  )\n" +
            "select\n" +
            "  c_customer_id,\n" +
            "  c_salutation,\n" +
            "  c_first_name,\n" +
            "  c_last_name,\n" +
            "  ca_street_number,\n" +
            "  ca_street_name,\n" +
            "  ca_street_type,\n" +
            "  ca_suite_number,\n" +
            "  ca_city,\n" +
            "  ca_county,\n" +
            "  ca_state,\n" +
            "  ca_zip,\n" +
            "  ca_country,\n" +
            "  ca_gmt_offset,\n" +
            "  ca_location_type,\n" +
            "  ctr_total_return\n" +
            "from\n" +
            "  customer_total_return ctr1,\n" +
            "  customer_address,\n" +
            "  customer\n" +
            "where\n" +
            "  ctr1.ctr_total_return > (\n" +
            "    select\n" +
            "      avg(ctr_total_return) * 1.2\n" +
            "    from\n" +
            "      customer_total_return ctr2\n" +
            "    where\n" +
            "      ctr1.ctr_state = ctr2.ctr_state\n" +
            "  )\n" +
            "  and ca_address_sk = c_current_addr_sk\n" +
            "  and ca_state = 'TX'\n" +
            "  and ctr1.ctr_customer_sk = c_customer_sk\n" +
            "order by\n" +
            "  c_customer_id,\n" +
            "  c_salutation,\n" +
            "  c_first_name,\n" +
            "  c_last_name,\n" +
            "  ca_street_number,\n" +
            "  ca_street_name,\n" +
            "  ca_street_type,\n" +
            "  ca_suite_number,\n" +
            "  ca_city,\n" +
            "  ca_county,\n" +
            "  ca_state,\n" +
            "  ca_zip,\n" +
            "  ca_country,\n" +
            "  ca_gmt_offset,\n" +
            "  ca_location_type,\n" +
            "  ctr_total_return\n" +
            "limit\n" +
            "  100;";

    public static final String Q38 = "with ssci as (\n" +
            "    select\n" +
            "      ss_customer_sk customer_sk,\n" +
            "      ss_item_sk item_sk\n" +
            "    from\n" +
            "      store_sales,\n" +
            "      date_dim\n" +
            "    where\n" +
            "      ss_sold_date_sk = d_date_sk\n" +
            "      and d_month_seq between 1211\n" +
            "      and 1211 + 11\n" +
            "    group by\n" +
            "      ss_customer_sk,\n" +
            "      ss_item_sk\n" +
            "  ),\n" +
            "  csci as(\n" +
            "    select\n" +
            "      cs_bill_customer_sk customer_sk,\n" +
            "      cs_item_sk item_sk\n" +
            "    from\n" +
            "      catalog_sales,\n" +
            "      date_dim\n" +
            "    where\n" +
            "      cs_sold_date_sk = d_date_sk\n" +
            "      and d_month_seq between 1211\n" +
            "      and 1211 + 11\n" +
            "    group by\n" +
            "      cs_bill_customer_sk,\n" +
            "      cs_item_sk\n" +
            "  )\n" +
            "select\n" +
            "  sum(\n" +
            "    case\n" +
            "      when ssci.customer_sk is not null\n" +
            "      and csci.customer_sk is null then 1\n" +
            "      else 0\n" +
            "    end\n" +
            "  ) store_only,\n" +
            "  sum(\n" +
            "    case\n" +
            "      when ssci.customer_sk is null\n" +
            "      and csci.customer_sk is not null then 1\n" +
            "      else 0\n" +
            "    end\n" +
            "  ) catalog_only,\n" +
            "  sum(\n" +
            "    case\n" +
            "      when ssci.customer_sk is not null\n" +
            "      and csci.customer_sk is not null then 1\n" +
            "      else 0\n" +
            "    end\n" +
            "  ) store_and_catalog\n" +
            "from\n" +
            "  ssci full\n" +
            "  outer join csci on (\n" +
            "    ssci.customer_sk = csci.customer_sk\n" +
            "    and ssci.item_sk = csci.item_sk\n" +
            "  )\n" +
            "limit\n" +
            "  100;";

    public static final String Q39 = "select\n" +
            "         w_warehouse_name\n" +
            "    ,w_warehouse_sq_ft\n" +
            "    ,w_city\n" +
            "    ,w_county\n" +
            "    ,w_state\n" +
            "    ,w_country\n" +
            "        ,ship_carriers\n" +
            "        ,year\n" +
            "    ,sum(jan_sales) as jan_sales\n" +
            "    ,sum(feb_sales) as feb_sales\n" +
            "    ,sum(mar_sales) as mar_sales\n" +
            "    ,sum(apr_sales) as apr_sales\n" +
            "    ,sum(may_sales) as may_sales\n" +
            "    ,sum(jun_sales) as jun_sales\n" +
            "    ,sum(jul_sales) as jul_sales\n" +
            "    ,sum(aug_sales) as aug_sales\n" +
            "    ,sum(sep_sales) as sep_sales\n" +
            "    ,sum(oct_sales) as oct_sales\n" +
            "    ,sum(nov_sales) as nov_sales\n" +
            "    ,sum(dec_sales) as dec_sales\n" +
            "    ,sum(jan_sales/w_warehouse_sq_ft) as jan_sales_per_sq_foot\n" +
            "    ,sum(feb_sales/w_warehouse_sq_ft) as feb_sales_per_sq_foot\n" +
            "    ,sum(mar_sales/w_warehouse_sq_ft) as mar_sales_per_sq_foot\n" +
            "    ,sum(apr_sales/w_warehouse_sq_ft) as apr_sales_per_sq_foot\n" +
            "    ,sum(may_sales/w_warehouse_sq_ft) as may_sales_per_sq_foot\n" +
            "    ,sum(jun_sales/w_warehouse_sq_ft) as jun_sales_per_sq_foot\n" +
            "    ,sum(jul_sales/w_warehouse_sq_ft) as jul_sales_per_sq_foot\n" +
            "    ,sum(aug_sales/w_warehouse_sq_ft) as aug_sales_per_sq_foot\n" +
            "    ,sum(sep_sales/w_warehouse_sq_ft) as sep_sales_per_sq_foot\n" +
            "    ,sum(oct_sales/w_warehouse_sq_ft) as oct_sales_per_sq_foot\n" +
            "    ,sum(nov_sales/w_warehouse_sq_ft) as nov_sales_per_sq_foot\n" +
            "    ,sum(dec_sales/w_warehouse_sq_ft) as dec_sales_per_sq_foot\n" +
            "    ,sum(jan_net) as jan_net\n" +
            "    ,sum(feb_net) as feb_net\n" +
            "    ,sum(mar_net) as mar_net\n" +
            "    ,sum(apr_net) as apr_net\n" +
            "    ,sum(may_net) as may_net\n" +
            "    ,sum(jun_net) as jun_net\n" +
            "    ,sum(jul_net) as jul_net\n" +
            "    ,sum(aug_net) as aug_net\n" +
            "    ,sum(sep_net) as sep_net\n" +
            "    ,sum(oct_net) as oct_net\n" +
            "    ,sum(nov_net) as nov_net\n" +
            "    ,sum(dec_net) as dec_net\n" +
            " from (\n" +
            "     select\n" +
            "    w_warehouse_name\n" +
            "    ,w_warehouse_sq_ft\n" +
            "    ,w_city\n" +
            "    ,w_county\n" +
            "    ,w_state\n" +
            "    ,w_country\n" +
            "    ,'FEDEX' || ',' || 'GERMA' as ship_carriers\n" +
            "       ,d_year as year\n" +
            "    ,sum(case when d_moy = 1\n" +
            "        then ws_ext_list_price* ws_quantity else 0 end) as jan_sales\n" +
            "    ,sum(case when d_moy = 2\n" +
            "        then ws_ext_list_price* ws_quantity else 0 end) as feb_sales\n" +
            "    ,sum(case when d_moy = 3\n" +
            "        then ws_ext_list_price* ws_quantity else 0 end) as mar_sales\n" +
            "    ,sum(case when d_moy = 4\n" +
            "        then ws_ext_list_price* ws_quantity else 0 end) as apr_sales\n" +
            "    ,sum(case when d_moy = 5\n" +
            "        then ws_ext_list_price* ws_quantity else 0 end) as may_sales\n" +
            "    ,sum(case when d_moy = 6\n" +
            "        then ws_ext_list_price* ws_quantity else 0 end) as jun_sales\n" +
            "    ,sum(case when d_moy = 7\n" +
            "        then ws_ext_list_price* ws_quantity else 0 end) as jul_sales\n" +
            "    ,sum(case when d_moy = 8\n" +
            "        then ws_ext_list_price* ws_quantity else 0 end) as aug_sales\n" +
            "    ,sum(case when d_moy = 9\n" +
            "        then ws_ext_list_price* ws_quantity else 0 end) as sep_sales\n" +
            "    ,sum(case when d_moy = 10\n" +
            "        then ws_ext_list_price* ws_quantity else 0 end) as oct_sales\n" +
            "    ,sum(case when d_moy = 11\n" +
            "        then ws_ext_list_price* ws_quantity else 0 end) as nov_sales\n" +
            "    ,sum(case when d_moy = 12\n" +
            "        then ws_ext_list_price* ws_quantity else 0 end) as dec_sales\n" +
            "    ,sum(case when d_moy = 1\n" +
            "        then ws_net_profit * ws_quantity else 0 end) as jan_net\n" +
            "    ,sum(case when d_moy = 2\n" +
            "        then ws_net_profit * ws_quantity else 0 end) as feb_net\n" +
            "    ,sum(case when d_moy = 3\n" +
            "        then ws_net_profit * ws_quantity else 0 end) as mar_net\n" +
            "    ,sum(case when d_moy = 4\n" +
            "        then ws_net_profit * ws_quantity else 0 end) as apr_net\n" +
            "    ,sum(case when d_moy = 5\n" +
            "        then ws_net_profit * ws_quantity else 0 end) as may_net\n" +
            "    ,sum(case when d_moy = 6\n" +
            "        then ws_net_profit * ws_quantity else 0 end) as jun_net\n" +
            "    ,sum(case when d_moy = 7\n" +
            "        then ws_net_profit * ws_quantity else 0 end) as jul_net\n" +
            "    ,sum(case when d_moy = 8\n" +
            "        then ws_net_profit * ws_quantity else 0 end) as aug_net\n" +
            "    ,sum(case when d_moy = 9\n" +
            "        then ws_net_profit * ws_quantity else 0 end) as sep_net\n" +
            "    ,sum(case when d_moy = 10\n" +
            "        then ws_net_profit * ws_quantity else 0 end) as oct_net\n" +
            "    ,sum(case when d_moy = 11\n" +
            "        then ws_net_profit * ws_quantity else 0 end) as nov_net\n" +
            "    ,sum(case when d_moy = 12\n" +
            "        then ws_net_profit * ws_quantity else 0 end) as dec_net\n" +
            "     from\n" +
            "          web_sales\n" +
            "         ,warehouse\n" +
            "         ,date_dim\n" +
            "         ,time_dim\n" +
            "      ,ship_mode\n" +
            "     where\n" +
            "            ws_warehouse_sk =  w_warehouse_sk\n" +
            "        and ws_sold_date_sk = d_date_sk\n" +
            "        and ws_sold_time_sk = t_time_sk\n" +
            "    and ws_ship_mode_sk = sm_ship_mode_sk\n" +
            "        and d_year = 2001\n" +
            "    and t_time between 19072 and 19072+28800\n" +
            "    and sm_carrier in ('FEDEX','GERMA')\n" +
            "     group by\n" +
            "        w_warehouse_name\n" +
            "    ,w_warehouse_sq_ft\n" +
            "    ,w_city\n" +
            "    ,w_county\n" +
            "    ,w_state\n" +
            "    ,w_country\n" +
            "       ,d_year\n" +
            " union all\n" +
            "     select\n" +
            "    w_warehouse_name\n" +
            "    ,w_warehouse_sq_ft\n" +
            "    ,w_city\n" +
            "    ,w_county\n" +
            "    ,w_state\n" +
            "    ,w_country\n" +
            "    ,'FEDEX' || ',' || 'GERMA' as ship_carriers\n" +
            "       ,d_year as year\n" +
            "    ,sum(case when d_moy = 1\n" +
            "        then cs_sales_price* cs_quantity else 0 end) as jan_sales\n" +
            "    ,sum(case when d_moy = 2\n" +
            "        then cs_sales_price* cs_quantity else 0 end) as feb_sales\n" +
            "    ,sum(case when d_moy = 3\n" +
            "        then cs_sales_price* cs_quantity else 0 end) as mar_sales\n" +
            "    ,sum(case when d_moy = 4\n" +
            "        then cs_sales_price* cs_quantity else 0 end) as apr_sales\n" +
            "    ,sum(case when d_moy = 5\n" +
            "        then cs_sales_price* cs_quantity else 0 end) as may_sales\n" +
            "    ,sum(case when d_moy = 6\n" +
            "        then cs_sales_price* cs_quantity else 0 end) as jun_sales\n" +
            "    ,sum(case when d_moy = 7\n" +
            "        then cs_sales_price* cs_quantity else 0 end) as jul_sales\n" +
            "    ,sum(case when d_moy = 8\n" +
            "        then cs_sales_price* cs_quantity else 0 end) as aug_sales\n" +
            "    ,sum(case when d_moy = 9\n" +
            "        then cs_sales_price* cs_quantity else 0 end) as sep_sales\n" +
            "    ,sum(case when d_moy = 10\n" +
            "        then cs_sales_price* cs_quantity else 0 end) as oct_sales\n" +
            "    ,sum(case when d_moy = 11\n" +
            "        then cs_sales_price* cs_quantity else 0 end) as nov_sales\n" +
            "    ,sum(case when d_moy = 12\n" +
            "        then cs_sales_price* cs_quantity else 0 end) as dec_sales\n" +
            "    ,sum(case when d_moy = 1\n" +
            "        then cs_net_paid * cs_quantity else 0 end) as jan_net\n" +
            "    ,sum(case when d_moy = 2\n" +
            "        then cs_net_paid * cs_quantity else 0 end) as feb_net\n" +
            "    ,sum(case when d_moy = 3\n" +
            "        then cs_net_paid * cs_quantity else 0 end) as mar_net\n" +
            "    ,sum(case when d_moy = 4\n" +
            "        then cs_net_paid * cs_quantity else 0 end) as apr_net\n" +
            "    ,sum(case when d_moy = 5\n" +
            "        then cs_net_paid * cs_quantity else 0 end) as may_net\n" +
            "    ,sum(case when d_moy = 6\n" +
            "        then cs_net_paid * cs_quantity else 0 end) as jun_net\n" +
            "    ,sum(case when d_moy = 7\n" +
            "        then cs_net_paid * cs_quantity else 0 end) as jul_net\n" +
            "    ,sum(case when d_moy = 8\n" +
            "        then cs_net_paid * cs_quantity else 0 end) as aug_net\n" +
            "    ,sum(case when d_moy = 9\n" +
            "        then cs_net_paid * cs_quantity else 0 end) as sep_net\n" +
            "    ,sum(case when d_moy = 10\n" +
            "        then cs_net_paid * cs_quantity else 0 end) as oct_net\n" +
            "    ,sum(case when d_moy = 11\n" +
            "        then cs_net_paid * cs_quantity else 0 end) as nov_net\n" +
            "    ,sum(case when d_moy = 12\n" +
            "        then cs_net_paid * cs_quantity else 0 end) as dec_net\n" +
            "     from\n" +
            "          catalog_sales\n" +
            "         ,warehouse\n" +
            "         ,date_dim\n" +
            "         ,time_dim\n" +
            "     ,ship_mode\n" +
            "     where\n" +
            "            cs_warehouse_sk =  w_warehouse_sk\n" +
            "        and cs_sold_date_sk = d_date_sk\n" +
            "        and cs_sold_time_sk = t_time_sk\n" +
            "    and cs_ship_mode_sk = sm_ship_mode_sk\n" +
            "        and d_year = 2001\n" +
            "    and t_time between 19072 AND 19072+28800\n" +
            "    and sm_carrier in ('FEDEX','GERMA')\n" +
            "     group by\n" +
            "        w_warehouse_name\n" +
            "    ,w_warehouse_sq_ft\n" +
            "    ,w_city\n" +
            "    ,w_county\n" +
            "    ,w_state\n" +
            "    ,w_country\n" +
            "       ,d_year\n" +
            " ) x\n" +
            " group by\n" +
            "        w_warehouse_name\n" +
            "    ,w_warehouse_sq_ft\n" +
            "    ,w_city\n" +
            "    ,w_county\n" +
            "    ,w_state\n" +
            "    ,w_country\n" +
            "    ,ship_carriers\n" +
            "       ,year\n" +
            " order by w_warehouse_name\n" +
            "limit 100;";

    public static final String Q40 = "select\n" +
            "  cast(amc as decimal(15, 4)) / cast(pmc as decimal(15, 4)) am_pm_ratio\n" +
            "from\n" +
            "  (\n" +
            "    select\n" +
            "      count(*) amc\n" +
            "    from\n" +
            "      web_sales,\n" +
            "      household_demographics,\n" +
            "      time_dim,\n" +
            "      web_page\n" +
            "    where\n" +
            "      ws_sold_time_sk = time_dim.t_time_sk\n" +
            "      and ws_ship_hdemo_sk = household_demographics.hd_demo_sk\n" +
            "      and ws_web_page_sk = web_page.wp_web_page_sk\n" +
            "      and time_dim.t_hour between 9\n" +
            "      and 9 + 1\n" +
            "      and household_demographics.hd_dep_count = 2\n" +
            "      and web_page.wp_char_count between 5000\n" +
            "      and 5200\n" +
            "  ) at,\n" +
            "  (\n" +
            "    select\n" +
            "      count(*) pmc\n" +
            "    from\n" +
            "      web_sales,\n" +
            "      household_demographics,\n" +
            "      time_dim,\n" +
            "      web_page\n" +
            "    where\n" +
            "      ws_sold_time_sk = time_dim.t_time_sk\n" +
            "      and ws_ship_hdemo_sk = household_demographics.hd_demo_sk\n" +
            "      and ws_web_page_sk = web_page.wp_web_page_sk\n" +
            "      and time_dim.t_hour between 15\n" +
            "      and 15 + 1\n" +
            "      and household_demographics.hd_dep_count = 2\n" +
            "      and web_page.wp_char_count between 5000\n" +
            "      and 5200\n" +
            "  ) pt\n" +
            "order by\n" +
            "  am_pm_ratio\n" +
            "limit\n" +
            "  100;";

    public static final String Q41 = "select  i_item_id\n" +
            "       ,i_item_desc\n" +
            "       ,s_state\n" +
            "       ,count(ss_quantity) as store_sales_quantitycount\n" +
            "       ,avg(ss_quantity) as store_sales_quantityave\n" +
            "       ,stddev_samp(ss_quantity) as store_sales_quantitystdev\n" +
            "       ,stddev_samp(ss_quantity)/avg(ss_quantity) as store_sales_quantitycov\n" +
            "       ,count(sr_return_quantity) as store_returns_quantitycount\n" +
            "       ,avg(sr_return_quantity) as store_returns_quantityave\n" +
            "       ,stddev_samp(sr_return_quantity) as store_returns_quantitystdev\n" +
            "       ,stddev_samp(sr_return_quantity)/avg(sr_return_quantity) as store_returns_quantitycov\n" +
            "       ,count(cs_quantity) as catalog_sales_quantitycount ,avg(cs_quantity) as catalog_sales_quantityave\n" +
            "       ,stddev_samp(cs_quantity) as catalog_sales_quantitystdev\n" +
            "       ,stddev_samp(cs_quantity)/avg(cs_quantity) as catalog_sales_quantitycov\n" +
            " from store_sales\n" +
            "     ,store_returns\n" +
            "     ,catalog_sales\n" +
            "     ,date_dim d1\n" +
            "     ,date_dim d2\n" +
            "     ,date_dim d3\n" +
            "     ,store\n" +
            "     ,item\n" +
            " where d1.d_quarter_name = '1999Q1'\n" +
            "   and d1.d_date_sk = ss_sold_date_sk\n" +
            "   and i_item_sk = ss_item_sk\n" +
            "   and s_store_sk = ss_store_sk\n" +
            "   and ss_customer_sk = sr_customer_sk\n" +
            "   and ss_item_sk = sr_item_sk\n" +
            "   and ss_ticket_number = sr_ticket_number\n" +
            "   and sr_returned_date_sk = d2.d_date_sk\n" +
            "   and d2.d_quarter_name in ('1999Q1','1999Q2','1999Q3')\n" +
            "   and sr_customer_sk = cs_bill_customer_sk\n" +
            "   and sr_item_sk = cs_item_sk\n" +
            "   and cs_sold_date_sk = d3.d_date_sk\n" +
            "   and d3.d_quarter_name in ('1999Q1','1999Q2','1999Q3')\n" +
            " group by i_item_id\n" +
            "         ,i_item_desc\n" +
            "         ,s_state\n" +
            " order by i_item_id\n" +
            "         ,i_item_desc\n" +
            "         ,s_state\n" +
            "limit 100;";

    public static final String Q42 = "with v1 as(\n" +
            " select i_category, i_brand,\n" +
            "        s_store_name, s_company_name,\n" +
            "        d_year, d_moy,\n" +
            "        sum(ss_sales_price) sum_sales,\n" +
            "        avg(sum(ss_sales_price)) over\n" +
            "          (partition by i_category, i_brand,\n" +
            "                     s_store_name, s_company_name, d_year)\n" +
            "          avg_monthly_sales,\n" +
            "        rank() over\n" +
            "          (partition by i_category, i_brand,\n" +
            "                     s_store_name, s_company_name\n" +
            "           order by d_year, d_moy) rn\n" +
            " from item, store_sales, date_dim, store\n" +
            " where ss_item_sk = i_item_sk and\n" +
            "       ss_sold_date_sk = d_date_sk and\n" +
            "       ss_store_sk = s_store_sk and\n" +
            "       (\n" +
            "         d_year = 2001 or\n" +
            "         ( d_year = 2001-1 and d_moy =12) or\n" +
            "         ( d_year = 2001+1 and d_moy =1)\n" +
            "       )\n" +
            " group by i_category, i_brand,\n" +
            "          s_store_name, s_company_name,\n" +
            "          d_year, d_moy),\n" +
            " v2 as(\n" +
            " select v1.i_category, v1.i_brand, v1.s_store_name, v1.s_company_name\n" +
            "        ,v1.d_year\n" +
            "        ,v1.avg_monthly_sales\n" +
            "        ,v1.sum_sales, v1_lag.sum_sales psum, v1_lead.sum_sales nsum\n" +
            " from v1, v1 v1_lag, v1 v1_lead\n" +
            " where v1.i_category = v1_lag.i_category and\n" +
            "       v1.i_category = v1_lead.i_category and\n" +
            "       v1.i_brand = v1_lag.i_brand and\n" +
            "       v1.i_brand = v1_lead.i_brand and\n" +
            "       v1.s_store_name = v1_lag.s_store_name and\n" +
            "       v1.s_store_name = v1_lead.s_store_name and\n" +
            "       v1.s_company_name = v1_lag.s_company_name and\n" +
            "       v1.s_company_name = v1_lead.s_company_name and\n" +
            "       v1.rn = v1_lag.rn + 1 and\n" +
            "       v1.rn = v1_lead.rn - 1)\n" +
            "  select  *\n" +
            " from v2\n" +
            " where  d_year = 2001 and\n" +
            "        avg_monthly_sales > 0 and\n" +
            "        case when avg_monthly_sales > 0 " +
            "then abs(sum_sales - avg_monthly_sales) / avg_monthly_sales else null end > 0.1\n" +
            " order by sum_sales - avg_monthly_sales, nsum\n" +
            " limit 100;";

    public static final String Q43 = "with ws_wh as (\n" +
            "    select\n" +
            "      ws1.ws_order_number,\n" +
            "      ws1.ws_warehouse_sk wh1,\n" +
            "      ws2.ws_warehouse_sk wh2\n" +
            "    from\n" +
            "      web_sales ws1,\n" +
            "      web_sales ws2\n" +
            "    where\n" +
            "      ws1.ws_order_number = ws2.ws_order_number\n" +
            "      and ws1.ws_warehouse_sk <> ws2.ws_warehouse_sk\n" +
            "  )\n" +
            "select\n" +
            "  count(distinct ws_order_number) as \"order count\",\n" +
            "  sum(ws_ext_ship_cost) as \"total shipping cost\",\n" +
            "  sum(ws_net_profit) as \"total net profit\"\n" +
            "from\n" +
            "  web_sales ws1,\n" +
            "  date_dim,\n" +
            "  customer_address,\n" +
            "  web_site\n" +
            "where\n" +
            "  d_date between '2002-5-01'\n" +
            "  and date_add(cast('2002-5-01' as date), 60)\n" +
            "  and ws1.ws_ship_date_sk = d_date_sk\n" +
            "  and ws1.ws_ship_addr_sk = ca_address_sk\n" +
            "  and ca_state = 'MA'\n" +
            "  and ws1.ws_web_site_sk = web_site_sk\n" +
            "  and web_company_name = 'pri'\n" +
            "  and ws1.ws_order_number in (\n" +
            "    select\n" +
            "      ws_order_number\n" +
            "    from\n" +
            "      ws_wh\n" +
            "  )\n" +
            "  and ws1.ws_order_number in (\n" +
            "    select\n" +
            "      wr_order_number\n" +
            "    from\n" +
            "      web_returns,\n" +
            "      ws_wh\n" +
            "    where\n" +
            "      wr_order_number = ws_wh.ws_order_number\n" +
            "  )\n" +
            "order by\n" +
            "  count(distinct ws_order_number)\n" +
            "limit\n" +
            "  100;";

    public static final String Q44 = "select\n" +
            "  sum(ws_ext_discount_amt) as \"Excess Discount Amount\"\n" +
            "from\n" +
            "  web_sales,\n" +
            "  item,\n" +
            "  date_dim\n" +
            "where\n" +
            "  i_manufact_id = 914\n" +
            "  and i_item_sk = ws_item_sk\n" +
            "  and d_date between '2001-01-25'\n" +
            "  and date_add(cast('2001-01-25' as date), 90)\n" +
            "  and d_date_sk = ws_sold_date_sk\n" +
            "  and ws_ext_discount_amt > (\n" +
            "    SELECT\n" +
            "      1.3 * avg(ws_ext_discount_amt)\n" +
            "    FROM\n" +
            "      web_sales,\n" +
            "      date_dim\n" +
            "    WHERE\n" +
            "      ws_item_sk = i_item_sk\n" +
            "      and d_date between '2001-01-25'\n" +
            "      and date_add(cast('2001-01-25' as date), 90)\n" +
            "      and d_date_sk = ws_sold_date_sk\n" +
            "  )\n" +
            "order by\n" +
            "  sum(ws_ext_discount_amt)\n" +
            "limit\n" +
            "  100;";

    public static final String Q45 = "select\n" +
            "  dt.d_year,\n" +
            "  item.i_brand_id brand_id,\n" +
            "  item.i_brand brand,\n" +
            "  sum(ss_net_profit) sum_agg\n" +
            "from\n" +
            "  date_dim dt,\n" +
            "  store_sales,\n" +
            "  item\n" +
            "where\n" +
            "  dt.d_date_sk = store_sales.ss_sold_date_sk\n" +
            "  and store_sales.ss_item_sk = item.i_item_sk\n" +
            "  and item.i_manufact_id = 445\n" +
            "  and dt.d_moy = 12\n" +
            "group by\n" +
            "  dt.d_year,\n" +
            "  item.i_brand,\n" +
            "  item.i_brand_id\n" +
            "order by\n" +
            "  dt.d_year,\n" +
            "  sum_agg desc,\n" +
            "  brand_id\n" +
            "limit\n" +
            "  100;";

    public static final String Q46 = "WITH web_v1 as (\n" +
            "select\n" +
            "  ws_item_sk item_sk, d_date,\n" +
            "  sum(sum(ws_sales_price))\n" +
            "      over (partition by ws_item_sk order by d_date rows between unbounded preceding and current row) cume_sales\n" +
            "from web_sales\n" +
            "    ,date_dim\n" +
            "where ws_sold_date_sk=d_date_sk\n" +
            "  and d_month_seq between 1215 and 1215+11\n" +
            "  and ws_item_sk is not NULL\n" +
            "group by ws_item_sk, d_date),\n" +
            "store_v1 as (\n" +
            "select\n" +
            "  ss_item_sk item_sk, d_date,\n" +
            "  sum(sum(ss_sales_price))\n" +
            "      over (partition by ss_item_sk order by d_date rows between unbounded preceding and current row) cume_sales\n" +
            "from store_sales\n" +
            "    ,date_dim\n" +
            "where ss_sold_date_sk=d_date_sk\n" +
            "  and d_month_seq between 1215 and 1215+11\n" +
            "  and ss_item_sk is not NULL\n" +
            "group by ss_item_sk, d_date)\n" +
            " select  *\n" +
            "from (select item_sk\n" +
            "     ,d_date\n" +
            "     ,web_sales\n" +
            "     ,store_sales\n" +
            "     ,max(web_sales)\n" +
            "         over (partition by item_sk order by d_date rows " +
            "between unbounded preceding and current row) web_cumulative\n" +
            "     ,max(store_sales)\n" +
            "         over (partition by item_sk order by d_date rows " +
            "between unbounded preceding and current row) store_cumulative\n" +
            "     from (select case when web.item_sk is not null then web.item_sk else store.item_sk end item_sk\n" +
            "                 ,case when web.d_date is not null then web.d_date else store.d_date end d_date\n" +
            "                 ,web.cume_sales web_sales\n" +
            "                 ,store.cume_sales store_sales\n" +
            "           from web_v1 web full outer join store_v1 store on (web.item_sk = store.item_sk\n" +
            "                                                          and web.d_date = store.d_date)\n" +
            "          )x )y\n" +
            "where web_cumulative > store_cumulative\n" +
            "order by item_sk\n" +
            "        ,d_date\n" +
            "limit 100;";

    public static final String Q47 = "select\n" +
            "  ca_state,\n" +
            "  cd_gender,\n" +
            "  cd_marital_status,\n" +
            "  cd_dep_count,\n" +
            "  count(*) cnt1,\n" +
            "  max(cd_dep_count),\n" +
            "  stddev_samp(cd_dep_count),\n" +
            "  stddev_samp(cd_dep_count),\n" +
            "  cd_dep_employed_count,\n" +
            "  count(*) cnt2,\n" +
            "  max(cd_dep_employed_count),\n" +
            "  stddev_samp(cd_dep_employed_count),\n" +
            "  stddev_samp(cd_dep_employed_count),\n" +
            "  cd_dep_college_count,\n" +
            "  count(*) cnt3,\n" +
            "  max(cd_dep_college_count),\n" +
            "  stddev_samp(cd_dep_college_count),\n" +
            "  stddev_samp(cd_dep_college_count)\n" +
            " from\n" +
            "  customer c,customer_address ca,customer_demographics\n" +
            " where\n" +
            "  c.c_current_addr_sk = ca.ca_address_sk and\n" +
            "  cd_demo_sk = c.c_current_cdemo_sk and\n" +
            "  exists (select *\n" +
            "          from store_sales,date_dim\n" +
            "          where c.c_customer_sk = ss_customer_sk and\n" +
            "                ss_sold_date_sk = d_date_sk and\n" +
            "                d_year = 2000 and\n" +
            "                d_qoy < 4) and\n" +
            "   (exists (select *\n" +
            "            from web_sales,date_dim\n" +
            "            where c.c_customer_sk = ws_bill_customer_sk and\n" +
            "                  ws_sold_date_sk = d_date_sk and\n" +
            "                  d_year = 2000 and\n" +
            "                  d_qoy < 4) or\n" +
            "    exists (select *\n" +
            "            from catalog_sales,date_dim\n" +
            "            where c.c_customer_sk = cs_ship_customer_sk and\n" +
            "                  cs_sold_date_sk = d_date_sk and\n" +
            "                  d_year = 2000 and\n" +
            "                  d_qoy < 4))\n" +
            " group by ca_state,\n" +
            "          cd_gender,\n" +
            "          cd_marital_status,\n" +
            "          cd_dep_count,\n" +
            "          cd_dep_employed_count,\n" +
            "          cd_dep_college_count\n" +
            " order by ca_state,\n" +
            "          cd_gender,\n" +
            "          cd_marital_status,\n" +
            "          cd_dep_count,\n" +
            "          cd_dep_employed_count,\n" +
            "          cd_dep_college_count\n" +
            " limit 100;\n";

    public static final String Q48 = "select\n" +
            "  channel,\n" +
            "  item,\n" +
            "  return_ratio,\n" +
            "  return_rank,\n" +
            "  currency_rank\n" +
            "from\n" +
            "  (\n" +
            "    select\n" +
            "      'web' as channel,\n" +
            "      web.item,\n" +
            "      web.return_ratio,\n" +
            "      web.return_rank,\n" +
            "      web.currency_rank\n" +
            "    from\n" +
            "      (\n" +
            "        select\n" +
            "          item,\n" +
            "          return_ratio,\n" +
            "          currency_ratio,\n" +
            "          rank() over (\n" +
            "            order by\n" +
            "              return_ratio\n" +
            "          ) as return_rank,\n" +
            "          rank() over (\n" +
            "            order by\n" +
            "              currency_ratio\n" +
            "          ) as currency_rank\n" +
            "        from\n" +
            "          (\n" +
            "            select\n" +
            "              ws.ws_item_sk as item,(\n" +
            "                cast(\n" +
            "                  sum(coalesce(wr.wr_return_quantity, 0)) as decimal(15, 4)\n" +
            "                ) / cast(\n" +
            "                  sum(coalesce(ws.ws_quantity, 0)) as decimal(15, 4)\n" +
            "                )\n" +
            "              ) as return_ratio,(\n" +
            "                cast(\n" +
            "                  sum(coalesce(wr.wr_return_amt, 0)) as decimal(15, 4)\n" +
            "                ) / cast(\n" +
            "                  sum(coalesce(ws.ws_net_paid, 0)) as decimal(15, 4)\n" +
            "                )\n" +
            "              ) as currency_ratio\n" +
            "            from\n" +
            "              web_sales ws\n" +
            "              left outer join web_returns wr on (\n" +
            "                ws.ws_order_number = wr.wr_order_number\n" +
            "                and ws.ws_item_sk = wr.wr_item_sk\n" +
            "              ),\n" +
            "              date_dim\n" +
            "            where\n" +
            "              wr.wr_return_amt > 10000\n" +
            "              and ws.ws_net_profit > 1\n" +
            "              and ws.ws_net_paid > 0\n" +
            "              and ws.ws_quantity > 0\n" +
            "              and ws_sold_date_sk = d_date_sk\n" +
            "              and d_year = 2000\n" +
            "              and d_moy = 12\n" +
            "            group by\n" +
            "              ws.ws_item_sk\n" +
            "          ) in_web\n" +
            "      ) web\n" +
            "    where\n" +
            "      (\n" +
            "        web.return_rank <= 10\n" +
            "        or web.currency_rank <= 10\n" +
            "      )\n" +
            "    union\n" +
            "    select\n" +
            "      'globalStateMgr' as channel,\n" +
            "      globalStateMgr.item,\n" +
            "      globalStateMgr.return_ratio,\n" +
            "      globalStateMgr.return_rank,\n" +
            "      globalStateMgr.currency_rank\n" +
            "    from\n" +
            "      (\n" +
            "        select\n" +
            "          item,\n" +
            "          return_ratio,\n" +
            "          currency_ratio,\n" +
            "          rank() over (\n" +
            "            order by\n" +
            "              return_ratio\n" +
            "          ) as return_rank,\n" +
            "          rank() over (\n" +
            "            order by\n" +
            "              currency_ratio\n" +
            "          ) as currency_rank\n" +
            "        from\n" +
            "          (\n" +
            "            select\n" +
            "              cs.cs_item_sk as item,(\n" +
            "                cast(\n" +
            "                  sum(coalesce(cr.cr_return_quantity, 0)) as decimal(15, 4)\n" +
            "                ) / cast(\n" +
            "                  sum(coalesce(cs.cs_quantity, 0)) as decimal(15, 4)\n" +
            "                )\n" +
            "              ) as return_ratio,(\n" +
            "                cast(\n" +
            "                  sum(coalesce(cr.cr_return_amount, 0)) as decimal(15, 4)\n" +
            "                ) / cast(\n" +
            "                  sum(coalesce(cs.cs_net_paid, 0)) as decimal(15, 4)\n" +
            "                )\n" +
            "              ) as currency_ratio\n" +
            "            from\n" +
            "              catalog_sales cs\n" +
            "              left outer join catalog_returns cr on (\n" +
            "                cs.cs_order_number = cr.cr_order_number\n" +
            "                and cs.cs_item_sk = cr.cr_item_sk\n" +
            "              ),\n" +
            "              date_dim\n" +
            "            where\n" +
            "              cr.cr_return_amount > 10000\n" +
            "              and cs.cs_net_profit > 1\n" +
            "              and cs.cs_net_paid > 0\n" +
            "              and cs.cs_quantity > 0\n" +
            "              and cs_sold_date_sk = d_date_sk\n" +
            "              and d_year = 2000\n" +
            "              and d_moy = 12\n" +
            "            group by\n" +
            "              cs.cs_item_sk\n" +
            "          ) in_cat\n" +
            "      ) globalStateMgr\n" +
            "    where\n" +
            "      (\n" +
            "        globalStateMgr.return_rank <= 10\n" +
            "        or globalStateMgr.currency_rank <= 10\n" +
            "      )\n" +
            "    union\n" +
            "    select\n" +
            "      'store' as channel,\n" +
            "      store.item,\n" +
            "      store.return_ratio,\n" +
            "      store.return_rank,\n" +
            "      store.currency_rank\n" +
            "    from\n" +
            "      (\n" +
            "        select\n" +
            "          item,\n" +
            "          return_ratio,\n" +
            "          currency_ratio,\n" +
            "          rank() over (\n" +
            "            order by\n" +
            "              return_ratio\n" +
            "          ) as return_rank,\n" +
            "          rank() over (\n" +
            "            order by\n" +
            "              currency_ratio\n" +
            "          ) as currency_rank\n" +
            "        from\n" +
            "          (\n" +
            "            select\n" +
            "              sts.ss_item_sk as item,(\n" +
            "                cast(\n" +
            "                  sum(coalesce(sr.sr_return_quantity, 0)) as decimal(15, 4)\n" +
            "                ) / cast(\n" +
            "                  sum(coalesce(sts.ss_quantity, 0)) as decimal(15, 4)\n" +
            "                )\n" +
            "              ) as return_ratio,(\n" +
            "                cast(\n" +
            "                  sum(coalesce(sr.sr_return_amt, 0)) as decimal(15, 4)\n" +
            "                ) / cast(\n" +
            "                  sum(coalesce(sts.ss_net_paid, 0)) as decimal(15, 4)\n" +
            "                )\n" +
            "              ) as currency_ratio\n" +
            "            from\n" +
            "              store_sales sts\n" +
            "              left outer join store_returns sr on (\n" +
            "                sts.ss_ticket_number = sr.sr_ticket_number\n" +
            "                and sts.ss_item_sk = sr.sr_item_sk\n" +
            "              ),\n" +
            "              date_dim\n" +
            "            where\n" +
            "              sr.sr_return_amt > 10000\n" +
            "              and sts.ss_net_profit > 1\n" +
            "              and sts.ss_net_paid > 0\n" +
            "              and sts.ss_quantity > 0\n" +
            "              and ss_sold_date_sk = d_date_sk\n" +
            "              and d_year = 2000\n" +
            "              and d_moy = 12\n" +
            "            group by\n" +
            "              sts.ss_item_sk\n" +
            "          ) in_store\n" +
            "      ) store\n" +
            "    where\n" +
            "      (\n" +
            "        store.return_rank <= 10\n" +
            "        or store.currency_rank <= 10\n" +
            "      )\n" +
            "  ) as t1\n" +
            "order by\n" +
            "  1,\n" +
            "  4,\n" +
            "  5,\n" +
            "  2\n" +
            "limit\n" +
            "  100;";

    // @Test
    public static final String Q49 = "select case when (select count(*)\n" +
            "                  from store_sales\n" +
            "                  where ss_quantity between 1 and 20) > 31002\n" +
            "            then (select avg(ss_ext_discount_amt)\n" +
            "                  from store_sales\n" +
            "                  where ss_quantity between 1 and 20)\n" +
            "            else (select avg(ss_net_profit)\n" +
            "                  from store_sales\n" +
            "                  where ss_quantity between 1 and 20) end bucket1 ,\n" +
            "       case when (select count(*)\n" +
            "                  from store_sales\n" +
            "                  where ss_quantity between 21 and 40) > 588\n" +
            "            then (select avg(ss_ext_discount_amt)\n" +
            "                  from store_sales\n" +
            "                  where ss_quantity between 21 and 40)\n" +
            "            else (select avg(ss_net_profit)\n" +
            "                  from store_sales\n" +
            "                  where ss_quantity between 21 and 40) end bucket2,\n" +
            "       case when (select count(*)\n" +
            "                  from store_sales\n" +
            "                  where ss_quantity between 41 and 60) > 2456\n" +
            "            then (select avg(ss_ext_discount_amt)\n" +
            "                  from store_sales\n" +
            "                  where ss_quantity between 41 and 60)\n" +
            "            else (select avg(ss_net_profit)\n" +
            "                  from store_sales\n" +
            "                  where ss_quantity between 41 and 60) end bucket3,\n" +
            "       case when (select count(*)\n" +
            "                  from store_sales\n" +
            "                  where ss_quantity between 61 and 80) > 21645\n" +
            "            then (select avg(ss_ext_discount_amt)\n" +
            "                  from store_sales\n" +
            "                  where ss_quantity between 61 and 80)\n" +
            "            else (select avg(ss_net_profit)\n" +
            "                  from store_sales\n" +
            "                  where ss_quantity between 61 and 80) end bucket4,\n" +
            "       case when (select count(*)\n" +
            "                  from store_sales\n" +
            "                  where ss_quantity between 81 and 100) > 20553\n" +
            "            then (select avg(ss_ext_discount_amt)\n" +
            "                  from store_sales\n" +
            "                  where ss_quantity between 81 and 100)\n" +
            "            else (select avg(ss_net_profit)\n" +
            "                  from store_sales\n" +
            "                  where ss_quantity between 81 and 100) end bucket5\n" +
            "from reason\n" +
            "where r_reason_sk = 1\n" +
            ";\n";

    public static final String Q50 = "with ss as (\n" +
            "    select\n" +
            "      ca_county,\n" +
            "      d_qoy,\n" +
            "      d_year,\n" +
            "      sum(ss_ext_sales_price) as store_sales\n" +
            "    from\n" +
            "      store_sales,\n" +
            "      date_dim,\n" +
            "      customer_address\n" +
            "    where\n" +
            "      ss_sold_date_sk = d_date_sk\n" +
            "      and ss_addr_sk = ca_address_sk\n" +
            "    group by\n" +
            "      ca_county,\n" +
            "      d_qoy,\n" +
            "      d_year\n" +
            "  ),\n" +
            "  ws as (\n" +
            "    select\n" +
            "      ca_county,\n" +
            "      d_qoy,\n" +
            "      d_year,\n" +
            "      sum(ws_ext_sales_price) as web_sales\n" +
            "    from\n" +
            "      web_sales,\n" +
            "      date_dim,\n" +
            "      customer_address\n" +
            "    where\n" +
            "      ws_sold_date_sk = d_date_sk\n" +
            "      and ws_bill_addr_sk = ca_address_sk\n" +
            "    group by\n" +
            "      ca_county,\n" +
            "      d_qoy,\n" +
            "      d_year\n" +
            "  )\n" +
            "select\n" +
            "  ss1.ca_county,\n" +
            "  ss1.d_year,\n" +
            "  ws2.web_sales / ws1.web_sales web_q1_q2_increase,\n" +
            "  ss2.store_sales / ss1.store_sales store_q1_q2_increase,\n" +
            "  ws3.web_sales / ws2.web_sales web_q2_q3_increase,\n" +
            "  ss3.store_sales / ss2.store_sales store_q2_q3_increase\n" +
            "from\n" +
            "  ss ss1,\n" +
            "  ss ss2,\n" +
            "  ss ss3,\n" +
            "  ws ws1,\n" +
            "  ws ws2,\n" +
            "  ws ws3\n" +
            "where\n" +
            "  ss1.d_qoy = 1\n" +
            "  and ss1.d_year = 1999\n" +
            "  and ss1.ca_county = ss2.ca_county\n" +
            "  and ss2.d_qoy = 2\n" +
            "  and ss2.d_year = 1999\n" +
            "  and ss2.ca_county = ss3.ca_county\n" +
            "  and ss3.d_qoy = 3\n" +
            "  and ss3.d_year = 1999\n" +
            "  and ss1.ca_county = ws1.ca_county\n" +
            "  and ws1.d_qoy = 1\n" +
            "  and ws1.d_year = 1999\n" +
            "  and ws1.ca_county = ws2.ca_county\n" +
            "  and ws2.d_qoy = 2\n" +
            "  and ws2.d_year = 1999\n" +
            "  and ws1.ca_county = ws3.ca_county\n" +
            "  and ws3.d_qoy = 3\n" +
            "  and ws3.d_year = 1999\n" +
            "  and case\n" +
            "    when ws1.web_sales > 0 then ws2.web_sales / ws1.web_sales\n" +
            "    else null\n" +
            "  end > case\n" +
            "    when ss1.store_sales > 0 then ss2.store_sales / ss1.store_sales\n" +
            "    else null\n" +
            "  end\n" +
            "  and case\n" +
            "    when ws2.web_sales > 0 then ws3.web_sales / ws2.web_sales\n" +
            "    else null\n" +
            "  end > case\n" +
            "    when ss2.store_sales > 0 then ss3.store_sales / ss2.store_sales\n" +
            "    else null\n" +
            "  end\n" +
            "order by\n" +
            "  ss1.ca_county;";

    public static final String Q51 = "with year_total as (\n" +
            "    select\n" +
            "      c_customer_id customer_id,\n" +
            "      c_first_name customer_first_name,\n" +
            "      c_last_name customer_last_name,\n" +
            "      c_preferred_cust_flag customer_preferred_cust_flag,\n" +
            "      c_birth_country customer_birth_country,\n" +
            "      c_login customer_login,\n" +
            "      c_email_address customer_email_address,\n" +
            "      d_year dyear,\n" +
            "      sum(ss_ext_list_price - ss_ext_discount_amt) year_total,\n" +
            "      's' sale_type\n" +
            "    from\n" +
            "      customer,\n" +
            "      store_sales,\n" +
            "      date_dim\n" +
            "    where\n" +
            "      c_customer_sk = ss_customer_sk\n" +
            "      and ss_sold_date_sk = d_date_sk\n" +
            "    group by\n" +
            "      c_customer_id,\n" +
            "      c_first_name,\n" +
            "      c_last_name,\n" +
            "      c_preferred_cust_flag,\n" +
            "      c_birth_country,\n" +
            "      c_login,\n" +
            "      c_email_address,\n" +
            "      d_year\n" +
            "    union all\n" +
            "    select\n" +
            "      c_customer_id customer_id,\n" +
            "      c_first_name customer_first_name,\n" +
            "      c_last_name customer_last_name,\n" +
            "      c_preferred_cust_flag customer_preferred_cust_flag,\n" +
            "      c_birth_country customer_birth_country,\n" +
            "      c_login customer_login,\n" +
            "      c_email_address customer_email_address,\n" +
            "      d_year dyear,\n" +
            "      sum(ws_ext_list_price - ws_ext_discount_amt) year_total,\n" +
            "      'w' sale_type\n" +
            "    from\n" +
            "      customer,\n" +
            "      web_sales,\n" +
            "      date_dim\n" +
            "    where\n" +
            "      c_customer_sk = ws_bill_customer_sk\n" +
            "      and ws_sold_date_sk = d_date_sk\n" +
            "    group by\n" +
            "      c_customer_id,\n" +
            "      c_first_name,\n" +
            "      c_last_name,\n" +
            "      c_preferred_cust_flag,\n" +
            "      c_birth_country,\n" +
            "      c_login,\n" +
            "      c_email_address,\n" +
            "      d_year\n" +
            "  )\n" +
            "select\n" +
            "  t_s_secyear.customer_id,\n" +
            "  t_s_secyear.customer_first_name,\n" +
            "  t_s_secyear.customer_last_name,\n" +
            "  t_s_secyear.customer_email_address\n" +
            "from\n" +
            "  year_total t_s_firstyear,\n" +
            "  year_total t_s_secyear,\n" +
            "  year_total t_w_firstyear,\n" +
            "  year_total t_w_secyear\n" +
            "where\n" +
            "  t_s_secyear.customer_id = t_s_firstyear.customer_id\n" +
            "  and t_s_firstyear.customer_id = t_w_secyear.customer_id\n" +
            "  and t_s_firstyear.customer_id = t_w_firstyear.customer_id\n" +
            "  and t_s_firstyear.sale_type = 's'\n" +
            "  and t_w_firstyear.sale_type = 'w'\n" +
            "  and t_s_secyear.sale_type = 's'\n" +
            "  and t_w_secyear.sale_type = 'w'\n" +
            "  and t_s_firstyear.dyear = 1999\n" +
            "  and t_s_secyear.dyear = 1999 + 1\n" +
            "  and t_w_firstyear.dyear = 1999\n" +
            "  and t_w_secyear.dyear = 1999 + 1\n" +
            "  and t_s_firstyear.year_total > 0\n" +
            "  and t_w_firstyear.year_total > 0\n" +
            "  and case\n" +
            "    when t_w_firstyear.year_total > 0 then t_w_secyear.year_total / t_w_firstyear.year_total\n" +
            "    else 0.0\n" +
            "  end > case\n" +
            "    when t_s_firstyear.year_total > 0 then t_s_secyear.year_total / t_s_firstyear.year_total\n" +
            "    else 0.0\n" +
            "  end\n" +
            "order by\n" +
            "  t_s_secyear.customer_id,\n" +
            "  t_s_secyear.customer_first_name,\n" +
            "  t_s_secyear.customer_last_name,\n" +
            "  t_s_secyear.customer_email_address\n" +
            "limit\n" +
            "  100;";

    public static final String Q52 = "select\n" +
            "  ss_customer_sk,\n" +
            "  sum(act_sales) sumsales\n" +
            "from\n" +
            "  (\n" +
            "    select\n" +
            "      ss_item_sk,\n" +
            "      ss_ticket_number,\n" +
            "      ss_customer_sk,case\n" +
            "        when sr_return_quantity is not null then (ss_quantity - sr_return_quantity) * ss_sales_price\n" +
            "        else (ss_quantity * ss_sales_price)\n" +
            "      end act_sales\n" +
            "    from\n" +
            "      store_sales\n" +
            "      left outer join store_returns on (\n" +
            "        sr_item_sk = ss_item_sk\n" +
            "        and sr_ticket_number = ss_ticket_number\n" +
            "      ),\n" +
            "      reason\n" +
            "    where\n" +
            "      sr_reason_sk = r_reason_sk\n" +
            "      and r_reason_desc = 'Did not get it on time'\n" +
            "  ) t\n" +
            "group by\n" +
            "  ss_customer_sk\n" +
            "order by\n" +
            "  sumsales,\n" +
            "  ss_customer_sk\n" +
            "limit\n" +
            "  100;";

    public static final String Q53 = "select\n" +
            "     i_item_id\n" +
            "    ,i_item_desc\n" +
            "    ,s_store_id\n" +
            "    ,s_store_name\n" +
            "    ,stddev_samp(ss_quantity)        as store_sales_quantity\n" +
            "    ,stddev_samp(sr_return_quantity) as store_returns_quantity\n" +
            "    ,stddev_samp(cs_quantity)        as catalog_sales_quantity\n" +
            " from\n" +
            "    store_sales\n" +
            "   ,store_returns\n" +
            "   ,catalog_sales\n" +
            "   ,date_dim             d1\n" +
            "   ,date_dim             d2\n" +
            "   ,date_dim             d3\n" +
            "   ,store\n" +
            "   ,item\n" +
            " where\n" +
            "     d1.d_moy               = 4\n" +
            " and d1.d_year              = 1999\n" +
            " and d1.d_date_sk           = ss_sold_date_sk\n" +
            " and i_item_sk              = ss_item_sk\n" +
            " and s_store_sk             = ss_store_sk\n" +
            " and ss_customer_sk         = sr_customer_sk\n" +
            " and ss_item_sk             = sr_item_sk\n" +
            " and ss_ticket_number       = sr_ticket_number\n" +
            " and sr_returned_date_sk    = d2.d_date_sk\n" +
            " and d2.d_moy               between 4 and  4 + 3\n" +
            " and d2.d_year              = 1999\n" +
            " and sr_customer_sk         = cs_bill_customer_sk\n" +
            " and sr_item_sk             = cs_item_sk\n" +
            " and cs_sold_date_sk        = d3.d_date_sk\n" +
            " and d3.d_year              in (1999,1999+1,1999+2)\n" +
            " group by\n" +
            "    i_item_id\n" +
            "   ,i_item_desc\n" +
            "   ,s_store_id\n" +
            "   ,s_store_name\n" +
            " order by\n" +
            "    i_item_id\n" +
            "   ,i_item_desc\n" +
            "   ,s_store_id\n" +
            "   ,s_store_name\n" +
            " limit 100;";

    public static final String Q54 = "select  count(*) from (\n" +
            "    select distinct c_last_name, c_first_name, d_date\n" +
            "    from store_sales, date_dim, customer\n" +
            "          where store_sales.ss_sold_date_sk = date_dim.d_date_sk\n" +
            "      and store_sales.ss_customer_sk = customer.c_customer_sk\n" +
            "      and d_month_seq between 1190 and 1190 + 11\n" +
            "  intersect\n" +
            "    select distinct c_last_name, c_first_name, d_date\n" +
            "    from catalog_sales, date_dim, customer\n" +
            "          where catalog_sales.cs_sold_date_sk = date_dim.d_date_sk\n" +
            "      and catalog_sales.cs_bill_customer_sk = customer.c_customer_sk\n" +
            "      and d_month_seq between 1190 and 1190 + 11\n" +
            "  intersect\n" +
            "    select distinct c_last_name, c_first_name, d_date\n" +
            "    from web_sales, date_dim, customer\n" +
            "          where web_sales.ws_sold_date_sk = date_dim.d_date_sk\n" +
            "      and web_sales.ws_bill_customer_sk = customer.c_customer_sk\n" +
            "      and d_month_seq between 1190 and 1190 + 11\n" +
            ") hot_cust\n" +
            "limit 100;";

    public static final String Q55 = "select  i_product_name\n" +
            "             ,i_brand\n" +
            "             ,i_class\n" +
            "             ,i_category\n" +
            "             ,avg(inv_quantity_on_hand) qoh\n" +
            "       from inventory\n" +
            "           ,date_dim\n" +
            "           ,item\n" +
            "       where inv_date_sk=d_date_sk\n" +
            "              and inv_item_sk=i_item_sk\n" +
            "              and d_month_seq between 1201 and 1201 + 11\n" +
            "       group by rollup(i_product_name\n" +
            "                       ,i_brand\n" +
            "                       ,i_class\n" +
            "                       ,i_category)\n" +
            "order by qoh, i_product_name, i_brand, i_class, i_category\n" +
            "limit 100;\n";

    public static final String Q56 = "select  *\n" +
            "from(\n" +
            "select i_category, i_class, i_brand,\n" +
            "       s_store_name, s_company_name,\n" +
            "       d_moy,\n" +
            "       sum(ss_sales_price) sum_sales,\n" +
            "       avg(sum(ss_sales_price)) over\n" +
            "         (partition by i_category, i_brand, s_store_name, s_company_name)\n" +
            "         avg_monthly_sales\n" +
            "from item, store_sales, date_dim, store\n" +
            "where ss_item_sk = i_item_sk and\n" +
            "      ss_sold_date_sk = d_date_sk and\n" +
            "      ss_store_sk = s_store_sk and\n" +
            "      d_year in (2001) and\n" +
            "        ((i_category in ('Children','Jewelry','Home') and\n" +
            "          i_class in ('infants','birdal','flatware')\n" +
            "         )\n" +
            "      or (i_category in ('Electronics','Music','Books') and\n" +
            "          i_class in ('audio','classical','science')\n" +
            "        ))\n" +
            "group by i_category, i_class, i_brand,\n" +
            "         s_store_name, s_company_name, d_moy) tmp1\n" +
            "where case when (avg_monthly_sales <> 0) " +
            "then (abs(sum_sales - avg_monthly_sales) / avg_monthly_sales) else null end > 0.1\n" +
            "order by sum_sales - avg_monthly_sales, s_store_name\n" +
            "limit 100;";

    public static final String Q57 = "select\n" +
            "  ca_zip,\n" +
            "  sum(cs_sales_price)\n" +
            "from\n" +
            "  catalog_sales,\n" +
            "  customer,\n" +
            "  customer_address,\n" +
            "  date_dim\n" +
            "where\n" +
            "  cs_bill_customer_sk = c_customer_sk\n" +
            "  and c_current_addr_sk = ca_address_sk\n" +
            "  and (\n" +
            "    substr(ca_zip, 1, 5) in (\n" +
            "      '85669',\n" +
            "      '86197',\n" +
            "      '88274',\n" +
            "      '83405',\n" +
            "      '86475',\n" +
            "      '85392',\n" +
            "      '85460',\n" +
            "      '80348',\n" +
            "      '81792'\n" +
            "    )\n" +
            "    or ca_state in ('CA', 'WA', 'GA')\n" +
            "    or cs_sales_price > 500\n" +
            "  )\n" +
            "  and cs_sold_date_sk = d_date_sk\n" +
            "  and d_qoy = 2\n" +
            "  and d_year = 2002\n" +
            "group by\n" +
            "  ca_zip\n" +
            "order by\n" +
            "  ca_zip\n" +
            "limit\n" +
            "  100;";

    public static final String Q58 = "select\n" +
            "  a.ca_state state,\n" +
            "  count(*) cnt\n" +
            "from\n" +
            "  customer_address a,\n" +
            "  customer c,\n" +
            "  store_sales s,\n" +
            "  date_dim d,\n" +
            "  item i\n" +
            "where\n" +
            "  a.ca_address_sk = c.c_current_addr_sk\n" +
            "  and c.c_customer_sk = s.ss_customer_sk\n" +
            "  and s.ss_sold_date_sk = d.d_date_sk\n" +
            "  and s.ss_item_sk = i.i_item_sk\n" +
            "  and d.d_month_seq = (\n" +
            "    select\n" +
            "      distinct (d_month_seq)\n" +
            "    from\n" +
            "      date_dim\n" +
            "    where\n" +
            "      d_year = 1998\n" +
            "      and d_moy = 3\n" +
            "  )\n" +
            "  and i.i_current_price > 1.2 * (\n" +
            "    select\n" +
            "      avg(j.i_current_price)\n" +
            "    from\n" +
            "      item j\n" +
            "    where\n" +
            "      j.i_category = i.i_category\n" +
            "  )\n" +
            "group by\n" +
            "  a.ca_state\n" +
            "having\n" +
            "  count(*) >= 10\n" +
            "order by\n" +
            "  cnt,\n" +
            "  a.ca_state\n" +
            "limit\n" +
            "  100;";

    public static final String Q59 = "select\n" +
            "  dt.d_year,\n" +
            "  item.i_brand_id brand_id,\n" +
            "  item.i_brand brand,\n" +
            "  sum(ss_ext_sales_price) ext_price\n" +
            "from\n" +
            "  date_dim dt,\n" +
            "  store_sales,\n" +
            "  item\n" +
            "where\n" +
            "  dt.d_date_sk = store_sales.ss_sold_date_sk\n" +
            "  and store_sales.ss_item_sk = item.i_item_sk\n" +
            "  and item.i_manager_id = 1\n" +
            "  and dt.d_moy = 11\n" +
            "  and dt.d_year = 2000\n" +
            "group by\n" +
            "  dt.d_year,\n" +
            "  item.i_brand,\n" +
            "  item.i_brand_id\n" +
            "order by\n" +
            "  dt.d_year,\n" +
            "  ext_price desc,\n" +
            "  brand_id\n" +
            "limit\n" +
            "  100;";

    public static final String Q60 = "select\n" +
            "  s_store_name,\n" +
            "  s_company_id,\n" +
            "  s_street_number,\n" +
            "  s_street_name,\n" +
            "  s_street_type,\n" +
            "  s_suite_number,\n" +
            "  s_city,\n" +
            "  s_county,\n" +
            "  s_state,\n" +
            "  s_zip,\n" +
            "  sum(\n" +
            "    case\n" +
            "      when (sr_returned_date_sk - ss_sold_date_sk <= 30) then 1\n" +
            "      else 0\n" +
            "    end\n" +
            "  ) as \"30 days\",\n" +
            "  sum(\n" +
            "    case\n" +
            "      when (sr_returned_date_sk - ss_sold_date_sk > 30)\n" +
            "      and (sr_returned_date_sk - ss_sold_date_sk <= 60) then 1\n" +
            "      else 0\n" +
            "    end\n" +
            "  ) as \"31-60 days\",\n" +
            "  sum(\n" +
            "    case\n" +
            "      when (sr_returned_date_sk - ss_sold_date_sk > 60)\n" +
            "      and (sr_returned_date_sk - ss_sold_date_sk <= 90) then 1\n" +
            "      else 0\n" +
            "    end\n" +
            "  ) as \"61-90 days\",\n" +
            "  sum(\n" +
            "    case\n" +
            "      when (sr_returned_date_sk - ss_sold_date_sk > 90)\n" +
            "      and (sr_returned_date_sk - ss_sold_date_sk <= 120) then 1\n" +
            "      else 0\n" +
            "    end\n" +
            "  ) as \"91-120 days\",\n" +
            "  sum(\n" +
            "    case\n" +
            "      when (sr_returned_date_sk - ss_sold_date_sk > 120) then 1\n" +
            "      else 0\n" +
            "    end\n" +
            "  ) as \">120 days\"\n" +
            "from\n" +
            "  store_sales,\n" +
            "  store_returns,\n" +
            "  store,\n" +
            "  date_dim d1,\n" +
            "  date_dim d2\n" +
            "where\n" +
            "  d2.d_year = 2002\n" +
            "  and d2.d_moy = 8\n" +
            "  and ss_ticket_number = sr_ticket_number\n" +
            "  and ss_item_sk = sr_item_sk\n" +
            "  and ss_sold_date_sk = d1.d_date_sk\n" +
            "  and sr_returned_date_sk = d2.d_date_sk\n" +
            "  and ss_customer_sk = sr_customer_sk\n" +
            "  and ss_store_sk = s_store_sk\n" +
            "group by\n" +
            "  s_store_name,\n" +
            "  s_company_id,\n" +
            "  s_street_number,\n" +
            "  s_street_name,\n" +
            "  s_street_type,\n" +
            "  s_suite_number,\n" +
            "  s_city,\n" +
            "  s_county,\n" +
            "  s_state,\n" +
            "  s_zip\n" +
            "order by\n" +
            "  s_store_name,\n" +
            "  s_company_id,\n" +
            "  s_street_number,\n" +
            "  s_street_name,\n" +
            "  s_street_type,\n" +
            "  s_suite_number,\n" +
            "  s_city,\n" +
            "  s_county,\n" +
            "  s_state,\n" +
            "  s_zip\n" +
            "limit\n" +
            "  100;";

    public static final String Q61 = "select\n" +
            "  dt.d_year,\n" +
            "  item.i_category_id,\n" +
            "  item.i_category,\n" +
            "  sum(ss_ext_sales_price)\n" +
            "from\n" +
            "  date_dim dt,\n" +
            "  store_sales,\n" +
            "  item\n" +
            "where\n" +
            "  dt.d_date_sk = store_sales.ss_sold_date_sk\n" +
            "  and store_sales.ss_item_sk = item.i_item_sk\n" +
            "  and item.i_manager_id = 1\n" +
            "  and dt.d_moy = 11\n" +
            "  and dt.d_year = 1998\n" +
            "group by\n" +
            "  dt.d_year,\n" +
            "  item.i_category_id,\n" +
            "  item.i_category\n" +
            "order by\n" +
            "  sum(ss_ext_sales_price) desc,\n" +
            "  dt.d_year,\n" +
            "  item.i_category_id,\n" +
            "  item.i_category\n" +
            "limit\n" +
            "  100;";

    public static final String Q62 = "select\n" +
            "  distinct(i_product_name)\n" +
            "from\n" +
            "  item i1\n" +
            "where\n" +
            "  i_manufact_id between 668\n" +
            "  and 668 + 40\n" +
            "  and (\n" +
            "    select\n" +
            "      count(*) as item_cnt\n" +
            "    from\n" +
            "      item\n" +
            "    where\n" +
            "      (\n" +
            "        i_manufact = i1.i_manufact\n" +
            "        and (\n" +
            "          (\n" +
            "            i_category = 'Women'\n" +
            "            and (\n" +
            "              i_color = 'cream'\n" +
            "              or i_color = 'ghost'\n" +
            "            )\n" +
            "            and (\n" +
            "              i_units = 'Ton'\n" +
            "              or i_units = 'Gross'\n" +
            "            )\n" +
            "            and (\n" +
            "              i_size = 'economy'\n" +
            "              or i_size = 'small'\n" +
            "            )\n" +
            "          )\n" +
            "          or (\n" +
            "            i_category = 'Women'\n" +
            "            and (\n" +
            "              i_color = 'midnight'\n" +
            "              or i_color = 'burlywood'\n" +
            "            )\n" +
            "            and (\n" +
            "              i_units = 'Tsp'\n" +
            "              or i_units = 'Bundle'\n" +
            "            )\n" +
            "            and (\n" +
            "              i_size = 'medium'\n" +
            "              or i_size = 'extra large'\n" +
            "            )\n" +
            "          )\n" +
            "          or (\n" +
            "            i_category = 'Men'\n" +
            "            and (\n" +
            "              i_color = 'lavender'\n" +
            "              or i_color = 'azure'\n" +
            "            )\n" +
            "            and (\n" +
            "              i_units = 'Each'\n" +
            "              or i_units = 'Lb'\n" +
            "            )\n" +
            "            and (\n" +
            "              i_size = 'large'\n" +
            "              or i_size = 'N/A'\n" +
            "            )\n" +
            "          )\n" +
            "          or (\n" +
            "            i_category = 'Men'\n" +
            "            and (\n" +
            "              i_color = 'chocolate'\n" +
            "              or i_color = 'steel'\n" +
            "            )\n" +
            "            and (\n" +
            "              i_units = 'N/A'\n" +
            "              or i_units = 'Dozen'\n" +
            "            )\n" +
            "            and (\n" +
            "              i_size = 'economy'\n" +
            "              or i_size = 'small'\n" +
            "            )\n" +
            "          )\n" +
            "        )\n" +
            "      )\n" +
            "      or (\n" +
            "        i_manufact = i1.i_manufact\n" +
            "        and (\n" +
            "          (\n" +
            "            i_category = 'Women'\n" +
            "            and (\n" +
            "              i_color = 'floral'\n" +
            "              or i_color = 'royal'\n" +
            "            )\n" +
            "            and (\n" +
            "              i_units = 'Unknown'\n" +
            "              or i_units = 'Tbl'\n" +
            "            )\n" +
            "            and (\n" +
            "              i_size = 'economy'\n" +
            "              or i_size = 'small'\n" +
            "            )\n" +
            "          )\n" +
            "          or (\n" +
            "            i_category = 'Women'\n" +
            "            and (\n" +
            "              i_color = 'navy'\n" +
            "              or i_color = 'forest'\n" +
            "            )\n" +
            "            and (\n" +
            "              i_units = 'Bunch'\n" +
            "              or i_units = 'Dram'\n" +
            "            )\n" +
            "            and (\n" +
            "              i_size = 'medium'\n" +
            "              or i_size = 'extra large'\n" +
            "            )\n" +
            "          )\n" +
            "          or (\n" +
            "            i_category = 'Men'\n" +
            "            and (\n" +
            "              i_color = 'cyan'\n" +
            "              or i_color = 'indian'\n" +
            "            )\n" +
            "            and (\n" +
            "              i_units = 'Carton'\n" +
            "              or i_units = 'Cup'\n" +
            "            )\n" +
            "            and (\n" +
            "              i_size = 'large'\n" +
            "              or i_size = 'N/A'\n" +
            "            )\n" +
            "          )\n" +
            "          or (\n" +
            "            i_category = 'Men'\n" +
            "            and (\n" +
            "              i_color = 'coral'\n" +
            "              or i_color = 'pale'\n" +
            "            )\n" +
            "            and (\n" +
            "              i_units = 'Pallet'\n" +
            "              or i_units = 'Gram'\n" +
            "            )\n" +
            "            and (\n" +
            "              i_size = 'economy'\n" +
            "              or i_size = 'small'\n" +
            "            )\n" +
            "          )\n" +
            "        )\n" +
            "      )\n" +
            "  ) > 0\n" +
            "order by\n" +
            "  i_product_name\n" +
            "limit\n" +
            "  100;";

    public static final String Q63 = "select  s_store_name\n" +
            "      ,sum(ss_net_profit)\n" +
            " from store_sales\n" +
            "     ,date_dim\n" +
            "     ,store,\n" +
            "     (select ca_zip\n" +
            "     from (\n" +
            "      SELECT substr(ca_zip,1,5) ca_zip\n" +
            "      FROM customer_address\n" +
            "      WHERE substr(ca_zip,1,5) IN (\n" +
            "                          '19100','41548','51640','49699','88329','55986',\n" +
            "                          '85119','19510','61020','95452','26235',\n" +
            "                          '51102','16733','42819','27823','90192',\n" +
            "                          '31905','28865','62197','23750','81398',\n" +
            "                          '95288','45114','82060','12313','25218',\n" +
            "                          '64386','46400','77230','69271','43672',\n" +
            "                          '36521','34217','13017','27936','42766',\n" +
            "                          '59233','26060','27477','39981','93402',\n" +
            "                          '74270','13932','51731','71642','17710',\n" +
            "                          '85156','21679','70840','67191','39214',\n" +
            "                          '35273','27293','17128','15458','31615',\n" +
            "                          '60706','67657','54092','32775','14683',\n" +
            "                          '32206','62543','43053','11297','58216',\n" +
            "                          '49410','14710','24501','79057','77038',\n" +
            "                          '91286','32334','46298','18326','67213',\n" +
            "                          '65382','40315','56115','80162','55956',\n" +
            "                          '81583','73588','32513','62880','12201',\n" +
            "                          '11592','17014','83832','61796','57872',\n" +
            "                          '78829','69912','48524','22016','26905',\n" +
            "                          '48511','92168','63051','25748','89786',\n" +
            "                          '98827','86404','53029','37524','14039',\n" +
            "                          '50078','34487','70142','18697','40129',\n" +
            "                          '60642','42810','62667','57183','46414',\n" +
            "                          '58463','71211','46364','34851','54884',\n" +
            "                          '25382','25239','74126','21568','84204',\n" +
            "                          '13607','82518','32982','36953','86001',\n" +
            "                          '79278','21745','64444','35199','83181',\n" +
            "                          '73255','86177','98043','90392','13882',\n" +
            "                          '47084','17859','89526','42072','20233',\n" +
            "                          '52745','75000','22044','77013','24182',\n" +
            "                          '52554','56138','43440','86100','48791',\n" +
            "                          '21883','17096','15965','31196','74903',\n" +
            "                          '19810','35763','92020','55176','54433',\n" +
            "                          '68063','71919','44384','16612','32109',\n" +
            "                          '28207','14762','89933','10930','27616',\n" +
            "                          '56809','14244','22733','33177','29784',\n" +
            "                          '74968','37887','11299','34692','85843',\n" +
            "                          '83663','95421','19323','17406','69264',\n" +
            "                          '28341','50150','79121','73974','92917',\n" +
            "                          '21229','32254','97408','46011','37169',\n" +
            "                          '18146','27296','62927','68812','47734',\n" +
            "                          '86572','12620','80252','50173','27261',\n" +
            "                          '29534','23488','42184','23695','45868',\n" +
            "                          '12910','23429','29052','63228','30731',\n" +
            "                          '15747','25827','22332','62349','56661',\n" +
            "                          '44652','51862','57007','22773','40361',\n" +
            "                          '65238','19327','17282','44708','35484',\n" +
            "                          '34064','11148','92729','22995','18833',\n" +
            "                          '77528','48917','17256','93166','68576',\n" +
            "                          '71096','56499','35096','80551','82424',\n" +
            "                          '17700','32748','78969','46820','57725',\n" +
            "                          '46179','54677','98097','62869','83959',\n" +
            "                          '66728','19716','48326','27420','53458',\n" +
            "                          '69056','84216','36688','63957','41469',\n" +
            "                          '66843','18024','81950','21911','58387',\n" +
            "                          '58103','19813','34581','55347','17171',\n" +
            "                          '35914','75043','75088','80541','26802',\n" +
            "                          '28849','22356','57721','77084','46385',\n" +
            "                          '59255','29308','65885','70673','13306',\n" +
            "                          '68788','87335','40987','31654','67560',\n" +
            "                          '92309','78116','65961','45018','16548',\n" +
            "                          '67092','21818','33716','49449','86150',\n" +
            "                          '12156','27574','43201','50977','52839',\n" +
            "                          '33234','86611','71494','17823','57172',\n" +
            "                          '59869','34086','51052','11320','39717',\n" +
            "                          '79604','24672','70555','38378','91135',\n" +
            "                          '15567','21606','74994','77168','38607',\n" +
            "                          '27384','68328','88944','40203','37893',\n" +
            "                          '42726','83549','48739','55652','27543',\n" +
            "                          '23109','98908','28831','45011','47525',\n" +
            "                          '43870','79404','35780','42136','49317',\n" +
            "                          '14574','99586','21107','14302','83882',\n" +
            "                          '81272','92552','14916','87533','86518',\n" +
            "                          '17862','30741','96288','57886','30304',\n" +
            "                          '24201','79457','36728','49833','35182',\n" +
            "                          '20108','39858','10804','47042','20439',\n" +
            "                          '54708','59027','82499','75311','26548',\n" +
            "                          '53406','92060','41152','60446','33129',\n" +
            "                          '43979','16903','60319','35550','33887',\n" +
            "                          '25463','40343','20726','44429')\n" +
            "     intersect\n" +
            "      select ca_zip\n" +
            "      from (SELECT substr(ca_zip,1,5) ca_zip,count(*) cnt\n" +
            "            FROM customer_address, customer\n" +
            "            WHERE ca_address_sk = c_current_addr_sk and\n" +
            "                  c_preferred_cust_flag='Y'\n" +
            "            group by ca_zip\n" +
            "            having count(*) > 10)A1)A2) V1\n" +
            " where ss_store_sk = s_store_sk\n" +
            "  and ss_sold_date_sk = d_date_sk\n" +
            "  and d_qoy = 1 and d_year = 2000\n" +
            "  and (substr(s_zip,1,2) = substr(V1.ca_zip,1,2))\n" +
            " group by s_store_name\n" +
            " order by s_store_name\n" +
            " limit 100;";

    public static final String Q64 = "select\n" +
            "  i_item_id,\n" +
            "  i_item_desc,\n" +
            "  i_category,\n" +
            "  i_class,\n" +
            "  i_current_price,\n" +
            "  sum(ws_ext_sales_price) as itemrevenue,\n" +
            "  sum(ws_ext_sales_price) * 100 / sum(sum(ws_ext_sales_price)) over (partition by i_class) as revenueratio\n" +
            "from\n" +
            "  web_sales,\n" +
            "  item,\n" +
            "  date_dim\n" +
            "where\n" +
            "  ws_item_sk = i_item_sk\n" +
            "  and i_category in ('Jewelry', 'Books', 'Women')\n" +
            "  and ws_sold_date_sk = d_date_sk\n" +
            "  and d_date between cast('2002-03-22' as date)\n" +
            "  and date_add(cast('2002-03-22' as date), 30)\n" +
            "group by\n" +
            "  i_item_id,\n" +
            "  i_item_desc,\n" +
            "  i_category,\n" +
            "  i_class,\n" +
            "  i_current_price\n" +
            "order by\n" +
            "  i_category,\n" +
            "  i_class,\n" +
            "  i_item_id,\n" +
            "  i_item_desc,\n" +
            "  revenueratio\n" +
            "limit\n" +
            "  100;";

    public static final String Q65 = "select\n" +
            "  i_item_id,\n" +
            "  i_item_desc,\n" +
            "  i_category,\n" +
            "  i_class,\n" +
            "  i_current_price,\n" +
            "  sum(cs_ext_sales_price) as itemrevenue,\n" +
            "  sum(cs_ext_sales_price) * 100 / sum(sum(cs_ext_sales_price)) over (partition by i_class) as revenueratio\n" +
            "from\n" +
            "  catalog_sales,\n" +
            "  item,\n" +
            "  date_dim\n" +
            "where\n" +
            "  cs_item_sk = i_item_sk\n" +
            "  and i_category in ('Children', 'Sports', 'Music')\n" +
            "  and cs_sold_date_sk = d_date_sk\n" +
            "  and d_date between cast('2002-04-01' as date)\n" +
            "  and date_add(cast('2002-04-01' as date), 30)\n" +
            "group by\n" +
            "  i_item_id,\n" +
            "  i_item_desc,\n" +
            "  i_category,\n" +
            "  i_class,\n" +
            "  i_current_price\n" +
            "order by\n" +
            "  i_category,\n" +
            "  i_class,\n" +
            "  i_item_id,\n" +
            "  i_item_desc,\n" +
            "  revenueratio\n" +
            "limit\n" +
            "  100;";

    public static final String Q66 = "select\n" +
            "  *\n" +
            "from\n" +
            "  (\n" +
            "    select\n" +
            "      count(*) h8_30_to_9\n" +
            "    from\n" +
            "      store_sales,\n" +
            "      household_demographics,\n" +
            "      time_dim,\n" +
            "      store\n" +
            "    where\n" +
            "      ss_sold_time_sk = time_dim.t_time_sk\n" +
            "      and ss_hdemo_sk = household_demographics.hd_demo_sk\n" +
            "      and ss_store_sk = s_store_sk\n" +
            "      and time_dim.t_hour = 8\n" +
            "      and time_dim.t_minute >= 30\n" +
            "      and (\n" +
            "        (\n" +
            "          household_demographics.hd_dep_count = 2\n" +
            "          and household_demographics.hd_vehicle_count <= 2 + 2\n" +
            "        )\n" +
            "        or (\n" +
            "          household_demographics.hd_dep_count = 1\n" +
            "          and household_demographics.hd_vehicle_count <= 1 + 2\n" +
            "        )\n" +
            "        or (\n" +
            "          household_demographics.hd_dep_count = 4\n" +
            "          and household_demographics.hd_vehicle_count <= 4 + 2\n" +
            "        )\n" +
            "      )\n" +
            "      and store.s_store_name = 'ese'\n" +
            "  ) s1,\n" +
            "  (\n" +
            "    select\n" +
            "      count(*) h9_to_9_30\n" +
            "    from\n" +
            "      store_sales,\n" +
            "      household_demographics,\n" +
            "      time_dim,\n" +
            "      store\n" +
            "    where\n" +
            "      ss_sold_time_sk = time_dim.t_time_sk\n" +
            "      and ss_hdemo_sk = household_demographics.hd_demo_sk\n" +
            "      and ss_store_sk = s_store_sk\n" +
            "      and time_dim.t_hour = 9\n" +
            "      and time_dim.t_minute < 30\n" +
            "      and (\n" +
            "        (\n" +
            "          household_demographics.hd_dep_count = 2\n" +
            "          and household_demographics.hd_vehicle_count <= 2 + 2\n" +
            "        )\n" +
            "        or (\n" +
            "          household_demographics.hd_dep_count = 1\n" +
            "          and household_demographics.hd_vehicle_count <= 1 + 2\n" +
            "        )\n" +
            "        or (\n" +
            "          household_demographics.hd_dep_count = 4\n" +
            "          and household_demographics.hd_vehicle_count <= 4 + 2\n" +
            "        )\n" +
            "      )\n" +
            "      and store.s_store_name = 'ese'\n" +
            "  ) s2,\n" +
            "  (\n" +
            "    select\n" +
            "      count(*) h9_30_to_10\n" +
            "    from\n" +
            "      store_sales,\n" +
            "      household_demographics,\n" +
            "      time_dim,\n" +
            "      store\n" +
            "    where\n" +
            "      ss_sold_time_sk = time_dim.t_time_sk\n" +
            "      and ss_hdemo_sk = household_demographics.hd_demo_sk\n" +
            "      and ss_store_sk = s_store_sk\n" +
            "      and time_dim.t_hour = 9\n" +
            "      and time_dim.t_minute >= 30\n" +
            "      and (\n" +
            "        (\n" +
            "          household_demographics.hd_dep_count = 2\n" +
            "          and household_demographics.hd_vehicle_count <= 2 + 2\n" +
            "        )\n" +
            "        or (\n" +
            "          household_demographics.hd_dep_count = 1\n" +
            "          and household_demographics.hd_vehicle_count <= 1 + 2\n" +
            "        )\n" +
            "        or (\n" +
            "          household_demographics.hd_dep_count = 4\n" +
            "          and household_demographics.hd_vehicle_count <= 4 + 2\n" +
            "        )\n" +
            "      )\n" +
            "      and store.s_store_name = 'ese'\n" +
            "  ) s3,\n" +
            "  (\n" +
            "    select\n" +
            "      count(*) h10_to_10_30\n" +
            "    from\n" +
            "      store_sales,\n" +
            "      household_demographics,\n" +
            "      time_dim,\n" +
            "      store\n" +
            "    where\n" +
            "      ss_sold_time_sk = time_dim.t_time_sk\n" +
            "      and ss_hdemo_sk = household_demographics.hd_demo_sk\n" +
            "      and ss_store_sk = s_store_sk\n" +
            "      and time_dim.t_hour = 10\n" +
            "      and time_dim.t_minute < 30\n" +
            "      and (\n" +
            "        (\n" +
            "          household_demographics.hd_dep_count = 2\n" +
            "          and household_demographics.hd_vehicle_count <= 2 + 2\n" +
            "        )\n" +
            "        or (\n" +
            "          household_demographics.hd_dep_count = 1\n" +
            "          and household_demographics.hd_vehicle_count <= 1 + 2\n" +
            "        )\n" +
            "        or (\n" +
            "          household_demographics.hd_dep_count = 4\n" +
            "          and household_demographics.hd_vehicle_count <= 4 + 2\n" +
            "        )\n" +
            "      )\n" +
            "      and store.s_store_name = 'ese'\n" +
            "  ) s4,\n" +
            "  (\n" +
            "    select\n" +
            "      count(*) h10_30_to_11\n" +
            "    from\n" +
            "      store_sales,\n" +
            "      household_demographics,\n" +
            "      time_dim,\n" +
            "      store\n" +
            "    where\n" +
            "      ss_sold_time_sk = time_dim.t_time_sk\n" +
            "      and ss_hdemo_sk = household_demographics.hd_demo_sk\n" +
            "      and ss_store_sk = s_store_sk\n" +
            "      and time_dim.t_hour = 10\n" +
            "      and time_dim.t_minute >= 30\n" +
            "      and (\n" +
            "        (\n" +
            "          household_demographics.hd_dep_count = 2\n" +
            "          and household_demographics.hd_vehicle_count <= 2 + 2\n" +
            "        )\n" +
            "        or (\n" +
            "          household_demographics.hd_dep_count = 1\n" +
            "          and household_demographics.hd_vehicle_count <= 1 + 2\n" +
            "        )\n" +
            "        or (\n" +
            "          household_demographics.hd_dep_count = 4\n" +
            "          and household_demographics.hd_vehicle_count <= 4 + 2\n" +
            "        )\n" +
            "      )\n" +
            "      and store.s_store_name = 'ese'\n" +
            "  ) s5,\n" +
            "  (\n" +
            "    select\n" +
            "      count(*) h11_to_11_30\n" +
            "    from\n" +
            "      store_sales,\n" +
            "      household_demographics,\n" +
            "      time_dim,\n" +
            "      store\n" +
            "    where\n" +
            "      ss_sold_time_sk = time_dim.t_time_sk\n" +
            "      and ss_hdemo_sk = household_demographics.hd_demo_sk\n" +
            "      and ss_store_sk = s_store_sk\n" +
            "      and time_dim.t_hour = 11\n" +
            "      and time_dim.t_minute < 30\n" +
            "      and (\n" +
            "        (\n" +
            "          household_demographics.hd_dep_count = 2\n" +
            "          and household_demographics.hd_vehicle_count <= 2 + 2\n" +
            "        )\n" +
            "        or (\n" +
            "          household_demographics.hd_dep_count = 1\n" +
            "          and household_demographics.hd_vehicle_count <= 1 + 2\n" +
            "        )\n" +
            "        or (\n" +
            "          household_demographics.hd_dep_count = 4\n" +
            "          and household_demographics.hd_vehicle_count <= 4 + 2\n" +
            "        )\n" +
            "      )\n" +
            "      and store.s_store_name = 'ese'\n" +
            "  ) s6,\n" +
            "  (\n" +
            "    select\n" +
            "      count(*) h11_30_to_12\n" +
            "    from\n" +
            "      store_sales,\n" +
            "      household_demographics,\n" +
            "      time_dim,\n" +
            "      store\n" +
            "    where\n" +
            "      ss_sold_time_sk = time_dim.t_time_sk\n" +
            "      and ss_hdemo_sk = household_demographics.hd_demo_sk\n" +
            "      and ss_store_sk = s_store_sk\n" +
            "      and time_dim.t_hour = 11\n" +
            "      and time_dim.t_minute >= 30\n" +
            "      and (\n" +
            "        (\n" +
            "          household_demographics.hd_dep_count = 2\n" +
            "          and household_demographics.hd_vehicle_count <= 2 + 2\n" +
            "        )\n" +
            "        or (\n" +
            "          household_demographics.hd_dep_count = 1\n" +
            "          and household_demographics.hd_vehicle_count <= 1 + 2\n" +
            "        )\n" +
            "        or (\n" +
            "          household_demographics.hd_dep_count = 4\n" +
            "          and household_demographics.hd_vehicle_count <= 4 + 2\n" +
            "        )\n" +
            "      )\n" +
            "      and store.s_store_name = 'ese'\n" +
            "  ) s7,\n" +
            "  (\n" +
            "    select\n" +
            "      count(*) h12_to_12_30\n" +
            "    from\n" +
            "      store_sales,\n" +
            "      household_demographics,\n" +
            "      time_dim,\n" +
            "      store\n" +
            "    where\n" +
            "      ss_sold_time_sk = time_dim.t_time_sk\n" +
            "      and ss_hdemo_sk = household_demographics.hd_demo_sk\n" +
            "      and ss_store_sk = s_store_sk\n" +
            "      and time_dim.t_hour = 12\n" +
            "      and time_dim.t_minute < 30\n" +
            "      and (\n" +
            "        (\n" +
            "          household_demographics.hd_dep_count = 2\n" +
            "          and household_demographics.hd_vehicle_count <= 2 + 2\n" +
            "        )\n" +
            "        or (\n" +
            "          household_demographics.hd_dep_count = 1\n" +
            "          and household_demographics.hd_vehicle_count <= 1 + 2\n" +
            "        )\n" +
            "        or (\n" +
            "          household_demographics.hd_dep_count = 4\n" +
            "          and household_demographics.hd_vehicle_count <= 4 + 2\n" +
            "        )\n" +
            "      )\n" +
            "      and store.s_store_name = 'ese'\n" +
            "  ) s8;";

    public static final String Q67 = "select\n" +
            "  i_item_id,\n" +
            "  i_item_desc,\n" +
            "  i_current_price\n" +
            "from\n" +
            "  item,\n" +
            "  inventory,\n" +
            "  date_dim,\n" +
            "  store_sales\n" +
            "where\n" +
            "  i_current_price between 69\n" +
            "  and 69 + 30\n" +
            "  and inv_item_sk = i_item_sk\n" +
            "  and d_date_sk = inv_date_sk\n" +
            "  and d_date between cast('1998-06-06' as date)\n" +
            "  and date_add(cast('1998-06-06' as date), 60)\n" +
            "  and i_manufact_id in (105, 513, 180, 137)\n" +
            "  and inv_quantity_on_hand between 100\n" +
            "  and 500\n" +
            "  and ss_item_sk = i_item_sk\n" +
            "group by\n" +
            "  i_item_id,\n" +
            "  i_item_desc,\n" +
            "  i_current_price\n" +
            "order by\n" +
            "  i_item_id\n" +
            "limit\n" +
            "  100;";

    public static final String Q68 = "with frequent_ss_items as (\n" +
            "    select\n" +
            "      substr(i_item_desc, 1, 30) itemdesc,\n" +
            "      i_item_sk item_sk,\n" +
            "      d_date solddate,\n" +
            "      count(*) cnt\n" +
            "    from\n" +
            "      store_sales,\n" +
            "      date_dim,\n" +
            "      item\n" +
            "    where\n" +
            "      ss_sold_date_sk = d_date_sk\n" +
            "      and ss_item_sk = i_item_sk\n" +
            "      and d_year in (2000, 2000 + 1, 2000 + 2, 2000 + 3)\n" +
            "    group by\n" +
            "      substr(i_item_desc, 1, 30),\n" +
            "      i_item_sk,\n" +
            "      d_date\n" +
            "    having\n" +
            "      count(*) > 4\n" +
            "  ),\n" +
            "  max_store_sales as (\n" +
            "    select\n" +
            "      max(csales) tpcds_cmax\n" +
            "    from\n" +
            "      (\n" +
            "        select\n" +
            "          c_customer_sk,\n" +
            "          sum(ss_quantity * ss_sales_price) csales\n" +
            "        from\n" +
            "          store_sales,\n" +
            "          customer,\n" +
            "          date_dim\n" +
            "        where\n" +
            "          ss_customer_sk = c_customer_sk\n" +
            "          and ss_sold_date_sk = d_date_sk\n" +
            "          and d_year in (2000, 2000 + 1, 2000 + 2, 2000 + 3)\n" +
            "        group by\n" +
            "          c_customer_sk\n" +
            "      ) as t1\n" +
            "  ),\n" +
            "  best_ss_customer as (\n" +
            "    select\n" +
            "      c_customer_sk,\n" +
            "      sum(ss_quantity * ss_sales_price) ssales\n" +
            "    from\n" +
            "      store_sales,\n" +
            "      customer\n" +
            "    where\n" +
            "      ss_customer_sk = c_customer_sk\n" +
            "    group by\n" +
            "      c_customer_sk\n" +
            "    having\n" +
            "      sum(ss_quantity * ss_sales_price) > (95 / 100.0) * (\n" +
            "        select\n" +
            "          *\n" +
            "        from\n" +
            "          max_store_sales\n" +
            "      )\n" +
            "  )\n" +
            "select\n" +
            "  sum(sales)\n" +
            "from\n" +
            "  (\n" +
            "    select\n" +
            "      cs_quantity * cs_list_price sales\n" +
            "    from\n" +
            "      catalog_sales,\n" +
            "      date_dim\n" +
            "    where\n" +
            "      d_year = 2000\n" +
            "      and d_moy = 3\n" +
            "      and cs_sold_date_sk = d_date_sk\n" +
            "      and cs_item_sk in (\n" +
            "        select\n" +
            "          item_sk\n" +
            "        from\n" +
            "          frequent_ss_items\n" +
            "      )\n" +
            "      and cs_bill_customer_sk in (\n" +
            "        select\n" +
            "          c_customer_sk\n" +
            "        from\n" +
            "          best_ss_customer\n" +
            "      )\n" +
            "    union all\n" +
            "    select\n" +
            "      ws_quantity * ws_list_price sales\n" +
            "    from\n" +
            "      web_sales,\n" +
            "      date_dim\n" +
            "    where\n" +
            "      d_year = 2000\n" +
            "      and d_moy = 3\n" +
            "      and ws_sold_date_sk = d_date_sk\n" +
            "      and ws_item_sk in (\n" +
            "        select\n" +
            "          item_sk\n" +
            "        from\n" +
            "          frequent_ss_items\n" +
            "      )\n" +
            "      and ws_bill_customer_sk in (\n" +
            "        select\n" +
            "          c_customer_sk\n" +
            "        from\n" +
            "          best_ss_customer\n" +
            "      )\n" +
            "  ) as t4\n" +
            "limit\n" +
            "  100;\n" +
            "with frequent_ss_items as (\n" +
            "    select\n" +
            "      substr(i_item_desc, 1, 30) itemdesc,\n" +
            "      i_item_sk item_sk,\n" +
            "      d_date solddate,\n" +
            "      count(*) cnt\n" +
            "    from\n" +
            "      store_sales,\n" +
            "      date_dim,\n" +
            "      item\n" +
            "    where\n" +
            "      ss_sold_date_sk = d_date_sk\n" +
            "      and ss_item_sk = i_item_sk\n" +
            "      and d_year in (2000, 2000 + 1, 2000 + 2, 2000 + 3)\n" +
            "    group by\n" +
            "      substr(i_item_desc, 1, 30),\n" +
            "      i_item_sk,\n" +
            "      d_date\n" +
            "    having\n" +
            "      count(*) > 4\n" +
            "  ),\n" +
            "  max_store_sales as (\n" +
            "    select\n" +
            "      max(csales) tpcds_cmax\n" +
            "    from\n" +
            "      (\n" +
            "        select\n" +
            "          c_customer_sk,\n" +
            "          sum(ss_quantity * ss_sales_price) csales\n" +
            "        from\n" +
            "          store_sales,\n" +
            "          customer,\n" +
            "          date_dim\n" +
            "        where\n" +
            "          ss_customer_sk = c_customer_sk\n" +
            "          and ss_sold_date_sk = d_date_sk\n" +
            "          and d_year in (2000, 2000 + 1, 2000 + 2, 2000 + 3)\n" +
            "        group by\n" +
            "          c_customer_sk\n" +
            "      ) as t2\n" +
            "  ),\n" +
            "  best_ss_customer as (\n" +
            "    select\n" +
            "      c_customer_sk,\n" +
            "      sum(ss_quantity * ss_sales_price) ssales\n" +
            "    from\n" +
            "      store_sales,\n" +
            "      customer\n" +
            "    where\n" +
            "      ss_customer_sk = c_customer_sk\n" +
            "    group by\n" +
            "      c_customer_sk\n" +
            "    having\n" +
            "      sum(ss_quantity * ss_sales_price) > (95 / 100.0) * (\n" +
            "        select\n" +
            "          *\n" +
            "        from\n" +
            "          max_store_sales\n" +
            "      )\n" +
            "  )\n" +
            "select\n" +
            "  c_last_name,\n" +
            "  c_first_name,\n" +
            "  sales\n" +
            "from\n" +
            "  (\n" +
            "    select\n" +
            "      c_last_name,\n" +
            "      c_first_name,\n" +
            "      sum(cs_quantity * cs_list_price) sales\n" +
            "    from\n" +
            "      catalog_sales,\n" +
            "      customer,\n" +
            "      date_dim\n" +
            "    where\n" +
            "      d_year = 2000\n" +
            "      and d_moy = 3\n" +
            "      and cs_sold_date_sk = d_date_sk\n" +
            "      and cs_item_sk in (\n" +
            "        select\n" +
            "          item_sk\n" +
            "        from\n" +
            "          frequent_ss_items\n" +
            "      )\n" +
            "      and cs_bill_customer_sk in (\n" +
            "        select\n" +
            "          c_customer_sk\n" +
            "        from\n" +
            "          best_ss_customer\n" +
            "      )\n" +
            "      and cs_bill_customer_sk = c_customer_sk\n" +
            "    group by\n" +
            "      c_last_name,\n" +
            "      c_first_name\n" +
            "    union all\n" +
            "    select\n" +
            "      c_last_name,\n" +
            "      c_first_name,\n" +
            "      sum(ws_quantity * ws_list_price) sales\n" +
            "    from\n" +
            "      web_sales,\n" +
            "      customer,\n" +
            "      date_dim\n" +
            "    where\n" +
            "      d_year = 2000\n" +
            "      and d_moy = 3\n" +
            "      and ws_sold_date_sk = d_date_sk\n" +
            "      and ws_item_sk in (\n" +
            "        select\n" +
            "          item_sk\n" +
            "        from\n" +
            "          frequent_ss_items\n" +
            "      )\n" +
            "      and ws_bill_customer_sk in (\n" +
            "        select\n" +
            "          c_customer_sk\n" +
            "        from\n" +
            "          best_ss_customer\n" +
            "      )\n" +
            "      and ws_bill_customer_sk = c_customer_sk\n" +
            "    group by\n" +
            "      c_last_name,\n" +
            "      c_first_name\n" +
            "  ) as t3\n" +
            "order by\n" +
            "  c_last_name,\n" +
            "  c_first_name,\n" +
            "  sales\n" +
            "limit\n" +
            "  100;";

    public static final String Q69 = "with cross_items as (\n" +
            "    select\n" +
            "      i_item_sk ss_item_sk\n" +
            "    from\n" +
            "      item,\n" +
            "      (\n" +
            "        select\n" +
            "          iss.i_brand_id brand_id,\n" +
            "          iss.i_class_id class_id,\n" +
            "          iss.i_category_id category_id\n" +
            "        from\n" +
            "          store_sales,\n" +
            "          item iss,\n" +
            "          date_dim d1\n" +
            "        where\n" +
            "          ss_item_sk = iss.i_item_sk\n" +
            "          and ss_sold_date_sk = d1.d_date_sk\n" +
            "          and d1.d_year between 1999\n" +
            "          AND 1999 + 2\n" +
            "        intersect\n" +
            "        select\n" +
            "          ics.i_brand_id,\n" +
            "          ics.i_class_id,\n" +
            "          ics.i_category_id\n" +
            "        from\n" +
            "          catalog_sales,\n" +
            "          item ics,\n" +
            "          date_dim d2\n" +
            "        where\n" +
            "          cs_item_sk = ics.i_item_sk\n" +
            "          and cs_sold_date_sk = d2.d_date_sk\n" +
            "          and d2.d_year between 1999\n" +
            "          AND 1999 + 2\n" +
            "        intersect\n" +
            "        select\n" +
            "          iws.i_brand_id,\n" +
            "          iws.i_class_id,\n" +
            "          iws.i_category_id\n" +
            "        from\n" +
            "          web_sales,\n" +
            "          item iws,\n" +
            "          date_dim d3\n" +
            "        where\n" +
            "          ws_item_sk = iws.i_item_sk\n" +
            "          and ws_sold_date_sk = d3.d_date_sk\n" +
            "          and d3.d_year between 1999\n" +
            "          AND 1999 + 2\n" +
            "      ) as t1\n" +
            "    where\n" +
            "      i_brand_id = brand_id\n" +
            "      and i_class_id = class_id\n" +
            "      and i_category_id = category_id\n" +
            "  ),\n" +
            "  avg_sales as (\n" +
            "    select\n" +
            "      avg(quantity * list_price) average_sales\n" +
            "    from\n" +
            "      (\n" +
            "        select\n" +
            "          ss_quantity quantity,\n" +
            "          ss_list_price list_price\n" +
            "        from\n" +
            "          store_sales,\n" +
            "          date_dim\n" +
            "        where\n" +
            "          ss_sold_date_sk = d_date_sk\n" +
            "          and d_year between 1999\n" +
            "          and 1999 + 2\n" +
            "        union all\n" +
            "        select\n" +
            "          cs_quantity quantity,\n" +
            "          cs_list_price list_price\n" +
            "        from\n" +
            "          catalog_sales,\n" +
            "          date_dim\n" +
            "        where\n" +
            "          cs_sold_date_sk = d_date_sk\n" +
            "          and d_year between 1999\n" +
            "          and 1999 + 2\n" +
            "        union all\n" +
            "        select\n" +
            "          ws_quantity quantity,\n" +
            "          ws_list_price list_price\n" +
            "        from\n" +
            "          web_sales,\n" +
            "          date_dim\n" +
            "        where\n" +
            "          ws_sold_date_sk = d_date_sk\n" +
            "          and d_year between 1999\n" +
            "          and 1999 + 2\n" +
            "      ) x\n" +
            "  )\n" +
            "select\n" +
            "  channel,\n" +
            "  i_brand_id,\n" +
            "  i_class_id,\n" +
            "  i_category_id,\n" +
            "  sum(sales),\n" +
            "  sum(number_sales)\n" +
            "from(\n" +
            "    select\n" +
            "      'store' channel,\n" +
            "      i_brand_id,\n" +
            "      i_class_id,\n" +
            "      i_category_id,\n" +
            "      sum(ss_quantity * ss_list_price) sales,\n" +
            "      count(*) number_sales\n" +
            "    from\n" +
            "      store_sales,\n" +
            "      item,\n" +
            "      date_dim\n" +
            "    where\n" +
            "      ss_item_sk in (\n" +
            "        select\n" +
            "          ss_item_sk\n" +
            "        from\n" +
            "          cross_items\n" +
            "      )\n" +
            "      and ss_item_sk = i_item_sk\n" +
            "      and ss_sold_date_sk = d_date_sk\n" +
            "      and d_year = 1999 + 2\n" +
            "      and d_moy = 11\n" +
            "    group by\n" +
            "      i_brand_id,\n" +
            "      i_class_id,\n" +
            "      i_category_id\n" +
            "    having\n" +
            "      sum(ss_quantity * ss_list_price) > (\n" +
            "        select\n" +
            "          average_sales\n" +
            "        from\n" +
            "          avg_sales\n" +
            "      )\n" +
            "    union all\n" +
            "    select\n" +
            "      'globalStateMgr' channel,\n" +
            "      i_brand_id,\n" +
            "      i_class_id,\n" +
            "      i_category_id,\n" +
            "      sum(cs_quantity * cs_list_price) sales,\n" +
            "      count(*) number_sales\n" +
            "    from\n" +
            "      catalog_sales,\n" +
            "      item,\n" +
            "      date_dim\n" +
            "    where\n" +
            "      cs_item_sk in (\n" +
            "        select\n" +
            "          ss_item_sk\n" +
            "        from\n" +
            "          cross_items\n" +
            "      )\n" +
            "      and cs_item_sk = i_item_sk\n" +
            "      and cs_sold_date_sk = d_date_sk\n" +
            "      and d_year = 1999 + 2\n" +
            "      and d_moy = 11\n" +
            "    group by\n" +
            "      i_brand_id,\n" +
            "      i_class_id,\n" +
            "      i_category_id\n" +
            "    having\n" +
            "      sum(cs_quantity * cs_list_price) > (\n" +
            "        select\n" +
            "          average_sales\n" +
            "        from\n" +
            "          avg_sales\n" +
            "      )\n" +
            "    union all\n" +
            "    select\n" +
            "      'web' channel,\n" +
            "      i_brand_id,\n" +
            "      i_class_id,\n" +
            "      i_category_id,\n" +
            "      sum(ws_quantity * ws_list_price) sales,\n" +
            "      count(*) number_sales\n" +
            "    from\n" +
            "      web_sales,\n" +
            "      item,\n" +
            "      date_dim\n" +
            "    where\n" +
            "      ws_item_sk in (\n" +
            "        select\n" +
            "          ss_item_sk\n" +
            "        from\n" +
            "          cross_items\n" +
            "      )\n" +
            "      and ws_item_sk = i_item_sk\n" +
            "      and ws_sold_date_sk = d_date_sk\n" +
            "      and d_year = 1999 + 2\n" +
            "      and d_moy = 11\n" +
            "    group by\n" +
            "      i_brand_id,\n" +
            "      i_class_id,\n" +
            "      i_category_id\n" +
            "    having\n" +
            "      sum(ws_quantity * ws_list_price) > (\n" +
            "        select\n" +
            "          average_sales\n" +
            "        from\n" +
            "          avg_sales\n" +
            "      )\n" +
            "  ) y\n" +
            "group by\n" +
            "  rollup (channel, i_brand_id, i_class_id, i_category_id)\n" +
            "order by\n" +
            "  channel,\n" +
            "  i_brand_id,\n" +
            "  i_class_id,\n" +
            "  i_category_id\n" +
            "limit\n" +
            "  100;\n" +
            "with cross_items as (\n" +
            "    select\n" +
            "      i_item_sk ss_item_sk\n" +
            "    from\n" +
            "      item,\n" +
            "      (\n" +
            "        select\n" +
            "          iss.i_brand_id brand_id,\n" +
            "          iss.i_class_id class_id,\n" +
            "          iss.i_category_id category_id\n" +
            "        from\n" +
            "          store_sales,\n" +
            "          item iss,\n" +
            "          date_dim d1\n" +
            "        where\n" +
            "          ss_item_sk = iss.i_item_sk\n" +
            "          and ss_sold_date_sk = d1.d_date_sk\n" +
            "          and d1.d_year between 1999\n" +
            "          AND 1999 + 2\n" +
            "        intersect\n" +
            "        select\n" +
            "          ics.i_brand_id,\n" +
            "          ics.i_class_id,\n" +
            "          ics.i_category_id\n" +
            "        from\n" +
            "          catalog_sales,\n" +
            "          item ics,\n" +
            "          date_dim d2\n" +
            "        where\n" +
            "          cs_item_sk = ics.i_item_sk\n" +
            "          and cs_sold_date_sk = d2.d_date_sk\n" +
            "          and d2.d_year between 1999\n" +
            "          AND 1999 + 2\n" +
            "        intersect\n" +
            "        select\n" +
            "          iws.i_brand_id,\n" +
            "          iws.i_class_id,\n" +
            "          iws.i_category_id\n" +
            "        from\n" +
            "          web_sales,\n" +
            "          item iws,\n" +
            "          date_dim d3\n" +
            "        where\n" +
            "          ws_item_sk = iws.i_item_sk\n" +
            "          and ws_sold_date_sk = d3.d_date_sk\n" +
            "          and d3.d_year between 1999\n" +
            "          AND 1999 + 2\n" +
            "      ) x\n" +
            "    where\n" +
            "      i_brand_id = brand_id\n" +
            "      and i_class_id = class_id\n" +
            "      and i_category_id = category_id\n" +
            "  ),\n" +
            "  avg_sales as (\n" +
            "    select\n" +
            "      avg(quantity * list_price) average_sales\n" +
            "    from\n" +
            "      (\n" +
            "        select\n" +
            "          ss_quantity quantity,\n" +
            "          ss_list_price list_price\n" +
            "        from\n" +
            "          store_sales,\n" +
            "          date_dim\n" +
            "        where\n" +
            "          ss_sold_date_sk = d_date_sk\n" +
            "          and d_year between 1999\n" +
            "          and 1999 + 2\n" +
            "        union all\n" +
            "        select\n" +
            "          cs_quantity quantity,\n" +
            "          cs_list_price list_price\n" +
            "        from\n" +
            "          catalog_sales,\n" +
            "          date_dim\n" +
            "        where\n" +
            "          cs_sold_date_sk = d_date_sk\n" +
            "          and d_year between 1999\n" +
            "          and 1999 + 2\n" +
            "        union all\n" +
            "        select\n" +
            "          ws_quantity quantity,\n" +
            "          ws_list_price list_price\n" +
            "        from\n" +
            "          web_sales,\n" +
            "          date_dim\n" +
            "        where\n" +
            "          ws_sold_date_sk = d_date_sk\n" +
            "          and d_year between 1999\n" +
            "          and 1999 + 2\n" +
            "      ) x\n" +
            "  )\n" +
            "select\n" +
            "  this_year.channel ty_channel,\n" +
            "  this_year.i_brand_id ty_brand,\n" +
            "  this_year.i_class_id ty_class,\n" +
            "  this_year.i_category_id ty_category,\n" +
            "  this_year.sales ty_sales,\n" +
            "  this_year.number_sales ty_number_sales,\n" +
            "  last_year.channel ly_channel,\n" +
            "  last_year.i_brand_id ly_brand,\n" +
            "  last_year.i_class_id ly_class,\n" +
            "  last_year.i_category_id ly_category,\n" +
            "  last_year.sales ly_sales,\n" +
            "  last_year.number_sales ly_number_sales\n" +
            "from\n" +
            "  (\n" +
            "    select\n" +
            "      'store' channel,\n" +
            "      i_brand_id,\n" +
            "      i_class_id,\n" +
            "      i_category_id,\n" +
            "      sum(ss_quantity * ss_list_price) sales,\n" +
            "      count(*) number_sales\n" +
            "    from\n" +
            "      store_sales,\n" +
            "      item,\n" +
            "      date_dim\n" +
            "    where\n" +
            "      ss_item_sk in (\n" +
            "        select\n" +
            "          ss_item_sk\n" +
            "        from\n" +
            "          cross_items\n" +
            "      )\n" +
            "      and ss_item_sk = i_item_sk\n" +
            "      and ss_sold_date_sk = d_date_sk\n" +
            "      and d_week_seq = (\n" +
            "        select\n" +
            "          d_week_seq\n" +
            "        from\n" +
            "          date_dim\n" +
            "        where\n" +
            "          d_year = 1999 + 1\n" +
            "          and d_moy = 12\n" +
            "          and d_dom = 14\n" +
            "      )\n" +
            "    group by\n" +
            "      i_brand_id,\n" +
            "      i_class_id,\n" +
            "      i_category_id\n" +
            "    having\n" +
            "      sum(ss_quantity * ss_list_price) > (\n" +
            "        select\n" +
            "          average_sales\n" +
            "        from\n" +
            "          avg_sales\n" +
            "      )\n" +
            "  ) this_year,\n" +
            "  (\n" +
            "    select\n" +
            "      'store' channel,\n" +
            "      i_brand_id,\n" +
            "      i_class_id,\n" +
            "      i_category_id,\n" +
            "      sum(ss_quantity * ss_list_price) sales,\n" +
            "      count(*) number_sales\n" +
            "    from\n" +
            "      store_sales,\n" +
            "      item,\n" +
            "      date_dim\n" +
            "    where\n" +
            "      ss_item_sk in (\n" +
            "        select\n" +
            "          ss_item_sk\n" +
            "        from\n" +
            "          cross_items\n" +
            "      )\n" +
            "      and ss_item_sk = i_item_sk\n" +
            "      and ss_sold_date_sk = d_date_sk\n" +
            "      and d_week_seq = (\n" +
            "        select\n" +
            "          d_week_seq\n" +
            "        from\n" +
            "          date_dim\n" +
            "        where\n" +
            "          d_year = 1999\n" +
            "          and d_moy = 12\n" +
            "          and d_dom = 14\n" +
            "      )\n" +
            "    group by\n" +
            "      i_brand_id,\n" +
            "      i_class_id,\n" +
            "      i_category_id\n" +
            "    having\n" +
            "      sum(ss_quantity * ss_list_price) > (\n" +
            "        select\n" +
            "          average_sales\n" +
            "        from\n" +
            "          avg_sales\n" +
            "      )\n" +
            "  ) last_year\n" +
            "where\n" +
            "  this_year.i_brand_id = last_year.i_brand_id\n" +
            "  and this_year.i_class_id = last_year.i_class_id\n" +
            "  and this_year.i_category_id = last_year.i_category_id\n" +
            "order by\n" +
            "  this_year.channel,\n" +
            "  this_year.i_brand_id,\n" +
            "  this_year.i_class_id,\n" +
            "  this_year.i_category_id\n" +
            "limit\n" +
            "  100;";

    public static final String Q70 = "with v1 as(\n" +
            " select i_category, i_brand,\n" +
            "        cc_name,\n" +
            "        d_year, d_moy,\n" +
            "        sum(cs_sales_price) sum_sales,\n" +
            "        avg(sum(cs_sales_price)) over\n" +
            "          (partition by i_category, i_brand,\n" +
            "                     cc_name, d_year)\n" +
            "          avg_monthly_sales,\n" +
            "        rank() over\n" +
            "          (partition by i_category, i_brand,\n" +
            "                     cc_name\n" +
            "           order by d_year, d_moy) rn\n" +
            " from item, catalog_sales, date_dim, call_center\n" +
            " where cs_item_sk = i_item_sk and\n" +
            "       cs_sold_date_sk = d_date_sk and\n" +
            "       cc_call_center_sk= cs_call_center_sk and\n" +
            "       (\n" +
            "         d_year = 2000 or\n" +
            "         ( d_year = 2000-1 and d_moy =12) or\n" +
            "         ( d_year = 2000+1 and d_moy =1)\n" +
            "       )\n" +
            " group by i_category, i_brand,\n" +
            "          cc_name , d_year, d_moy),\n" +
            " v2 as(\n" +
            " select v1.cc_name\n" +
            "        ,v1.d_year, v1.d_moy\n" +
            "        ,v1.avg_monthly_sales\n" +
            "        ,v1.sum_sales, v1_lag.sum_sales psum, v1_lead.sum_sales nsum\n" +
            " from v1, v1 v1_lag, v1 v1_lead\n" +
            " where v1.i_category = v1_lag.i_category and\n" +
            "       v1.i_category = v1_lead.i_category and\n" +
            "       v1.i_brand = v1_lag.i_brand and\n" +
            "       v1.i_brand = v1_lead.i_brand and\n" +
            "       v1. cc_name = v1_lag. cc_name and\n" +
            "       v1. cc_name = v1_lead. cc_name and\n" +
            "       v1.rn = v1_lag.rn + 1 and\n" +
            "       v1.rn = v1_lead.rn - 1)\n" +
            "  select  *\n" +
            " from v2\n" +
            " where  d_year = 2000 and\n" +
            "        avg_monthly_sales > 0 and\n" +
            "        case when avg_monthly_sales > 0 " +
            "then abs(sum_sales - avg_monthly_sales) / avg_monthly_sales else null end > 0.1\n" +
            " order by sum_sales - avg_monthly_sales, psum\n" +
            " limit 100;\n";

    public static final String Q71 = "select\n" +
            "  s_store_name,\n" +
            "  i_item_desc,\n" +
            "  sc.revenue,\n" +
            "  i_current_price,\n" +
            "  i_wholesale_cost,\n" +
            "  i_brand\n" +
            "from\n" +
            "  store,\n" +
            "  item,\n" +
            "  (\n" +
            "    select\n" +
            "      ss_store_sk,\n" +
            "      avg(revenue) as ave\n" +
            "    from\n" +
            "      (\n" +
            "        select\n" +
            "          ss_store_sk,\n" +
            "          ss_item_sk,\n" +
            "          sum(ss_sales_price) as revenue\n" +
            "        from\n" +
            "          store_sales,\n" +
            "          date_dim\n" +
            "        where\n" +
            "          ss_sold_date_sk = d_date_sk\n" +
            "          and d_month_seq between 1186\n" +
            "          and 1186 + 11\n" +
            "        group by\n" +
            "          ss_store_sk,\n" +
            "          ss_item_sk\n" +
            "      ) sa\n" +
            "    group by\n" +
            "      ss_store_sk\n" +
            "  ) sb,\n" +
            "  (\n" +
            "    select\n" +
            "      ss_store_sk,\n" +
            "      ss_item_sk,\n" +
            "      sum(ss_sales_price) as revenue\n" +
            "    from\n" +
            "      store_sales,\n" +
            "      date_dim\n" +
            "    where\n" +
            "      ss_sold_date_sk = d_date_sk\n" +
            "      and d_month_seq between 1186\n" +
            "      and 1186 + 11\n" +
            "    group by\n" +
            "      ss_store_sk,\n" +
            "      ss_item_sk\n" +
            "  ) sc\n" +
            "where\n" +
            "  sb.ss_store_sk = sc.ss_store_sk\n" +
            "  and sc.revenue <= 0.1 * sb.ave\n" +
            "  and s_store_sk = sc.ss_store_sk\n" +
            "  and i_item_sk = sc.ss_item_sk\n" +
            "order by\n" +
            "  s_store_name,\n" +
            "  i_item_desc\n" +
            "limit\n" +
            "  100;";

    public static final String Q72 = "select\n" +
            "  i_brand_id brand_id,\n" +
            "  i_brand brand,\n" +
            "  t_hour,\n" +
            "  t_minute,\n" +
            "  sum(ext_price) ext_price\n" +
            "from\n" +
            "  item,\n" +
            "  (\n" +
            "    select\n" +
            "      ws_ext_sales_price as ext_price,\n" +
            "      ws_sold_date_sk as sold_date_sk,\n" +
            "      ws_item_sk as sold_item_sk,\n" +
            "      ws_sold_time_sk as time_sk\n" +
            "    from\n" +
            "      web_sales,\n" +
            "      date_dim\n" +
            "    where\n" +
            "      d_date_sk = ws_sold_date_sk\n" +
            "      and d_moy = 11\n" +
            "      and d_year = 2001\n" +
            "    union all\n" +
            "    select\n" +
            "      cs_ext_sales_price as ext_price,\n" +
            "      cs_sold_date_sk as sold_date_sk,\n" +
            "      cs_item_sk as sold_item_sk,\n" +
            "      cs_sold_time_sk as time_sk\n" +
            "    from\n" +
            "      catalog_sales,\n" +
            "      date_dim\n" +
            "    where\n" +
            "      d_date_sk = cs_sold_date_sk\n" +
            "      and d_moy = 11\n" +
            "      and d_year = 2001\n" +
            "    union all\n" +
            "    select\n" +
            "      ss_ext_sales_price as ext_price,\n" +
            "      ss_sold_date_sk as sold_date_sk,\n" +
            "      ss_item_sk as sold_item_sk,\n" +
            "      ss_sold_time_sk as time_sk\n" +
            "    from\n" +
            "      store_sales,\n" +
            "      date_dim\n" +
            "    where\n" +
            "      d_date_sk = ss_sold_date_sk\n" +
            "      and d_moy = 11\n" +
            "      and d_year = 2001\n" +
            "  ) tmp,\n" +
            "  time_dim\n" +
            "where\n" +
            "  sold_item_sk = i_item_sk\n" +
            "  and i_manager_id = 1\n" +
            "  and time_sk = t_time_sk\n" +
            "  and (\n" +
            "    t_meal_time = 'breakfast'\n" +
            "    or t_meal_time = 'dinner'\n" +
            "  )\n" +
            "group by\n" +
            "  i_brand,\n" +
            "  i_brand_id,\n" +
            "  t_hour,\n" +
            "  t_minute\n" +
            "order by\n" +
            "  ext_price desc,\n" +
            "  i_brand_id;";

    public static final String Q73 = "select\n" +
            "  c_last_name,\n" +
            "  c_first_name,\n" +
            "  c_salutation,\n" +
            "  c_preferred_cust_flag,\n" +
            "  ss_ticket_number,\n" +
            "  cnt\n" +
            "from\n" +
            "  (\n" +
            "    select\n" +
            "      ss_ticket_number,\n" +
            "      ss_customer_sk,\n" +
            "      count(*) cnt\n" +
            "    from\n" +
            "      store_sales,\n" +
            "      date_dim,\n" +
            "      store,\n" +
            "      household_demographics\n" +
            "    where\n" +
            "      store_sales.ss_sold_date_sk = date_dim.d_date_sk\n" +
            "      and store_sales.ss_store_sk = store.s_store_sk\n" +
            "      and store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk\n" +
            "      and (\n" +
            "        date_dim.d_dom between 1\n" +
            "        and 3\n" +
            "        or date_dim.d_dom between 25\n" +
            "        and 28\n" +
            "      )\n" +
            "      and (\n" +
            "        household_demographics.hd_buy_potential = '501-1000'\n" +
            "        or household_demographics.hd_buy_potential = 'Unknown'\n" +
            "      )\n" +
            "      and household_demographics.hd_vehicle_count > 0\n" +
            "      and (\n" +
            "        case\n" +
            "          when household_demographics.hd_vehicle_count > 0 " +
            "then household_demographics.hd_dep_count / household_demographics.hd_vehicle_count\n" +
            "          else null\n" +
            "        end\n" +
            "      ) > 1.2\n" +
            "      and date_dim.d_year in (2000, 2000 + 1, 2000 + 2)\n" +
            "      and store.s_county in (\n" +
            "        'Williamson County',\n" +
            "        'Williamson County',\n" +
            "        'Williamson County',\n" +
            "        'Williamson County',\n" +
            "        'Williamson County',\n" +
            "        'Williamson County',\n" +
            "        'Williamson County',\n" +
            "        'Williamson County'\n" +
            "      )\n" +
            "    group by\n" +
            "      ss_ticket_number,\n" +
            "      ss_customer_sk\n" +
            "  ) dn,\n" +
            "  customer\n" +
            "where\n" +
            "  ss_customer_sk = c_customer_sk\n" +
            "  and cnt between 15\n" +
            "  and 20\n" +
            "order by\n" +
            "  c_last_name,\n" +
            "  c_first_name,\n" +
            "  c_salutation,\n" +
            "  c_preferred_cust_flag desc,\n" +
            "  ss_ticket_number;";

    public static final String Q74 = "select\n" +
            "  sum (ss_quantity)\n" +
            "from\n" +
            "  store_sales,\n" +
            "  store,\n" +
            "  customer_demographics,\n" +
            "  customer_address,\n" +
            "  date_dim\n" +
            "where\n" +
            "  s_store_sk = ss_store_sk\n" +
            "  and ss_sold_date_sk = d_date_sk\n" +
            "  and d_year = 2001\n" +
            "  and (\n" +
            "    (\n" +
            "      cd_demo_sk = ss_cdemo_sk\n" +
            "      and cd_marital_status = 'W'\n" +
            "      and cd_education_status = '2 yr Degree'\n" +
            "      and ss_sales_price between 100.00\n" +
            "      and 150.00\n" +
            "    )\n" +
            "    or (\n" +
            "      cd_demo_sk = ss_cdemo_sk\n" +
            "      and cd_marital_status = 'S'\n" +
            "      and cd_education_status = 'Advanced Degree'\n" +
            "      and ss_sales_price between 50.00\n" +
            "      and 100.00\n" +
            "    )\n" +
            "    or (\n" +
            "      cd_demo_sk = ss_cdemo_sk\n" +
            "      and cd_marital_status = 'D'\n" +
            "      and cd_education_status = 'Primary'\n" +
            "      and ss_sales_price between 150.00\n" +
            "      and 200.00\n" +
            "    )\n" +
            "  )\n" +
            "  and (\n" +
            "    (\n" +
            "      ss_addr_sk = ca_address_sk\n" +
            "      and ca_country = 'United States'\n" +
            "      and ca_state in ('IL', 'KY', 'OR')\n" +
            "      and ss_net_profit between 0\n" +
            "      and 2000\n" +
            "    )\n" +
            "    or (\n" +
            "      ss_addr_sk = ca_address_sk\n" +
            "      and ca_country = 'United States'\n" +
            "      and ca_state in ('VA', 'FL', 'AL')\n" +
            "      and ss_net_profit between 150\n" +
            "      and 3000\n" +
            "    )\n" +
            "    or (\n" +
            "      ss_addr_sk = ca_address_sk\n" +
            "      and ca_country = 'United States'\n" +
            "      and ca_state in ('OK', 'IA', 'TX')\n" +
            "      and ss_net_profit between 50\n" +
            "      and 25000\n" +
            "    )\n" +
            "  );";

    public static final String Q75 = "with customer_total_return as (\n" +
            "    select\n" +
            "      wr_returning_customer_sk as ctr_customer_sk,\n" +
            "      ca_state as ctr_state,\n" +
            "      sum(wr_return_amt) as ctr_total_return\n" +
            "    from\n" +
            "      web_returns,\n" +
            "      date_dim,\n" +
            "      customer_address\n" +
            "    where\n" +
            "      wr_returned_date_sk = d_date_sk\n" +
            "      and d_year = 2000\n" +
            "      and wr_returning_addr_sk = ca_address_sk\n" +
            "    group by\n" +
            "      wr_returning_customer_sk,\n" +
            "      ca_state\n" +
            "  )\n" +
            "select\n" +
            "  c_customer_id,\n" +
            "  c_salutation,\n" +
            "  c_first_name,\n" +
            "  c_last_name,\n" +
            "  c_preferred_cust_flag,\n" +
            "  c_birth_day,\n" +
            "  c_birth_month,\n" +
            "  c_birth_year,\n" +
            "  c_birth_country,\n" +
            "  c_login,\n" +
            "  c_email_address,\n" +
            "  c_last_review_date,\n" +
            "  ctr_total_return\n" +
            "from\n" +
            "  customer_total_return ctr1,\n" +
            "  customer_address,\n" +
            "  customer\n" +
            "where\n" +
            "  ctr1.ctr_total_return > (\n" +
            "    select\n" +
            "      avg(ctr_total_return) * 1.2\n" +
            "    from\n" +
            "      customer_total_return ctr2\n" +
            "    where\n" +
            "      ctr1.ctr_state = ctr2.ctr_state\n" +
            "  )\n" +
            "  and ca_address_sk = c_current_addr_sk\n" +
            "  and ca_state = 'KS'\n" +
            "  and ctr1.ctr_customer_sk = c_customer_sk\n" +
            "order by\n" +
            "  c_customer_id,\n" +
            "  c_salutation,\n" +
            "  c_first_name,\n" +
            "  c_last_name,\n" +
            "  c_preferred_cust_flag,\n" +
            "  c_birth_day,\n" +
            "  c_birth_month,\n" +
            "  c_birth_year,\n" +
            "  c_birth_country,\n" +
            "  c_login,\n" +
            "  c_email_address,\n" +
            "  c_last_review_date,\n" +
            "  ctr_total_return\n" +
            "limit\n" +
            "  100;";

    public static final String Q76 = "with year_total as (\n" +
            " select c_customer_id customer_id\n" +
            "       ,c_first_name customer_first_name\n" +
            "       ,c_last_name customer_last_name\n" +
            "       ,d_year as year\n" +
            "       ,stddev_samp(ss_net_paid) year_total\n" +
            "       ,'s' sale_type\n" +
            " from customer\n" +
            "     ,store_sales\n" +
            "     ,date_dim\n" +
            " where c_customer_sk = ss_customer_sk\n" +
            "   and ss_sold_date_sk = d_date_sk\n" +
            "   and d_year in (2001,2001+1)\n" +
            " group by c_customer_id\n" +
            "         ,c_first_name\n" +
            "         ,c_last_name\n" +
            "         ,d_year\n" +
            " union all\n" +
            " select c_customer_id customer_id\n" +
            "       ,c_first_name customer_first_name\n" +
            "       ,c_last_name customer_last_name\n" +
            "       ,d_year as year\n" +
            "       ,stddev_samp(ws_net_paid) year_total\n" +
            "       ,'w' sale_type\n" +
            " from customer\n" +
            "     ,web_sales\n" +
            "     ,date_dim\n" +
            " where c_customer_sk = ws_bill_customer_sk\n" +
            "   and ws_sold_date_sk = d_date_sk\n" +
            "   and d_year in (2001,2001+1)\n" +
            " group by c_customer_id\n" +
            "         ,c_first_name\n" +
            "         ,c_last_name\n" +
            "         ,d_year\n" +
            "         )\n" +
            "  select\n" +
            "        t_s_secyear.customer_id, t_s_secyear.customer_first_name, t_s_secyear.customer_last_name\n" +
            " from year_total t_s_firstyear\n" +
            "     ,year_total t_s_secyear\n" +
            "     ,year_total t_w_firstyear\n" +
            "     ,year_total t_w_secyear\n" +
            " where t_s_secyear.customer_id = t_s_firstyear.customer_id\n" +
            "         and t_s_firstyear.customer_id = t_w_secyear.customer_id\n" +
            "         and t_s_firstyear.customer_id = t_w_firstyear.customer_id\n" +
            "         and t_s_firstyear.sale_type = 's'\n" +
            "         and t_w_firstyear.sale_type = 'w'\n" +
            "         and t_s_secyear.sale_type = 's'\n" +
            "         and t_w_secyear.sale_type = 'w'\n" +
            "         and t_s_firstyear.year = 2001\n" +
            "         and t_s_secyear.year = 2001+1\n" +
            "         and t_w_firstyear.year = 2001\n" +
            "         and t_w_secyear.year = 2001+1\n" +
            "         and t_s_firstyear.year_total > 0\n" +
            "         and t_w_firstyear.year_total > 0\n" +
            "         and case when t_w_firstyear.year_total > 0 " +
            "then t_w_secyear.year_total / t_w_firstyear.year_total else null end\n" +
            "           > case when t_s_firstyear.year_total > 0 " +
            "then t_s_secyear.year_total / t_s_firstyear.year_total else null end\n" +
            " order by 3,2,1\n" +
            "limit 100;";

    public static final String Q77 = "select count(*)\n" +
            "from ((select distinct c_last_name, c_first_name, d_date\n" +
            "       from store_sales, date_dim, customer\n" +
            "       where store_sales.ss_sold_date_sk = date_dim.d_date_sk\n" +
            "         and store_sales.ss_customer_sk = customer.c_customer_sk\n" +
            "         and d_month_seq between 1189 and 1189+11)\n" +
            "       except\n" +
            "      (select distinct c_last_name, c_first_name, d_date\n" +
            "       from catalog_sales, date_dim, customer\n" +
            "       where catalog_sales.cs_sold_date_sk = date_dim.d_date_sk\n" +
            "         and catalog_sales.cs_bill_customer_sk = customer.c_customer_sk\n" +
            "         and d_month_seq between 1189 and 1189+11)\n" +
            "       except\n" +
            "      (select distinct c_last_name, c_first_name, d_date\n" +
            "       from web_sales, date_dim, customer\n" +
            "       where web_sales.ws_sold_date_sk = date_dim.d_date_sk\n" +
            "         and web_sales.ws_bill_customer_sk = customer.c_customer_sk\n" +
            "         and d_month_seq between 1189 and 1189+11)\n" +
            ") cool_cust\n" +
            ";\n";

    public static final String Q78 = "with ss as (\n" +
            "    select\n" +
            "      s_store_sk,\n" +
            "      sum(ss_ext_sales_price) as sales,\n" +
            "      sum(ss_net_profit) as profit\n" +
            "    from\n" +
            "      store_sales,\n" +
            "      date_dim,\n" +
            "      store\n" +
            "    where\n" +
            "      ss_sold_date_sk = d_date_sk\n" +
            "      and d_date between cast('2001-08-11' as date)\n" +
            "      and date_add(cast('2001-08-11' as date), 30)\n" +
            "      and ss_store_sk = s_store_sk\n" +
            "    group by\n" +
            "      s_store_sk\n" +
            "  ),\n" +
            "  sr as (\n" +
            "    select\n" +
            "      s_store_sk,\n" +
            "      sum(sr_return_amt) as returns,\n" +
            "      sum(sr_net_loss) as profit_loss\n" +
            "    from\n" +
            "      store_returns,\n" +
            "      date_dim,\n" +
            "      store\n" +
            "    where\n" +
            "      sr_returned_date_sk = d_date_sk\n" +
            "      and d_date between cast('2001-08-11' as date)\n" +
            "      and date_add(cast('2001-08-11' as date), 30)\n" +
            "      and sr_store_sk = s_store_sk\n" +
            "    group by\n" +
            "      s_store_sk\n" +
            "  ),\n" +
            "  cs as (\n" +
            "    select\n" +
            "      cs_call_center_sk,\n" +
            "      sum(cs_ext_sales_price) as sales,\n" +
            "      sum(cs_net_profit) as profit\n" +
            "    from\n" +
            "      catalog_sales,\n" +
            "      date_dim\n" +
            "    where\n" +
            "      cs_sold_date_sk = d_date_sk\n" +
            "      and d_date between cast('2001-08-11' as date)\n" +
            "      and date_add(cast('2001-08-11' as date), 30)\n" +
            "    group by\n" +
            "      cs_call_center_sk\n" +
            "  ),\n" +
            "  cr as (\n" +
            "    select\n" +
            "      cr_call_center_sk,\n" +
            "      sum(cr_return_amount) as returns,\n" +
            "      sum(cr_net_loss) as profit_loss\n" +
            "    from\n" +
            "      catalog_returns,\n" +
            "      date_dim\n" +
            "    where\n" +
            "      cr_returned_date_sk = d_date_sk\n" +
            "      and d_date between cast('2001-08-11' as date)\n" +
            "      and date_add(cast('2001-08-11' as date), 30)\n" +
            "    group by\n" +
            "      cr_call_center_sk\n" +
            "  ),\n" +
            "  ws as (\n" +
            "    select\n" +
            "      wp_web_page_sk,\n" +
            "      sum(ws_ext_sales_price) as sales,\n" +
            "      sum(ws_net_profit) as profit\n" +
            "    from\n" +
            "      web_sales,\n" +
            "      date_dim,\n" +
            "      web_page\n" +
            "    where\n" +
            "      ws_sold_date_sk = d_date_sk\n" +
            "      and d_date between cast('2001-08-11' as date)\n" +
            "      and date_add(cast('2001-08-11' as date), 30)\n" +
            "      and ws_web_page_sk = wp_web_page_sk\n" +
            "    group by\n" +
            "      wp_web_page_sk\n" +
            "  ),\n" +
            "  wr as (\n" +
            "    select\n" +
            "      wp_web_page_sk,\n" +
            "      sum(wr_return_amt) as returns,\n" +
            "      sum(wr_net_loss) as profit_loss\n" +
            "    from\n" +
            "      web_returns,\n" +
            "      date_dim,\n" +
            "      web_page\n" +
            "    where\n" +
            "      wr_returned_date_sk = d_date_sk\n" +
            "      and d_date between cast('2001-08-11' as date)\n" +
            "      and date_add(cast('2001-08-11' as date), 30)\n" +
            "      and wr_web_page_sk = wp_web_page_sk\n" +
            "    group by\n" +
            "      wp_web_page_sk\n" +
            "  )\n" +
            "select\n" +
            "  channel,\n" +
            "  id,\n" +
            "  sum(sales) as sales,\n" +
            "  sum(returns) as returns,\n" +
            "  sum(profit) as profit\n" +
            "from\n" +
            "  (\n" +
            "    select\n" +
            "      'store channel' as channel,\n" +
            "      ss.s_store_sk as id,\n" +
            "      sales,\n" +
            "      coalesce(returns, 0) as returns,\n" +
            "      (profit - coalesce(profit_loss, 0)) as profit\n" +
            "    from\n" +
            "      ss\n" +
            "      left join sr on ss.s_store_sk = sr.s_store_sk\n" +
            "    union all\n" +
            "    select\n" +
            "      'globalStateMgr channel' as channel,\n" +
            "      cs_call_center_sk as id,\n" +
            "      sales,\n" +
            "      returns,\n" +
            "      (profit - profit_loss) as profit\n" +
            "    from\n" +
            "      cs,\n" +
            "      cr\n" +
            "    union all\n" +
            "    select\n" +
            "      'web channel' as channel,\n" +
            "      ws.wp_web_page_sk as id,\n" +
            "      sales,\n" +
            "      coalesce(returns, 0) returns,\n" +
            "      (profit - coalesce(profit_loss, 0)) as profit\n" +
            "    from\n" +
            "      ws\n" +
            "      left join wr on ws.wp_web_page_sk = wr.wp_web_page_sk\n" +
            "  ) x\n" +
            "group by\n" +
            "  rollup (channel, id)\n" +
            "order by\n" +
            "  channel,\n" +
            "  id\n" +
            "limit\n" +
            "  100;\n";

    public static final String Q79 = "select\n" +
            "  c_last_name,\n" +
            "  c_first_name,\n" +
            "  c_salutation,\n" +
            "  c_preferred_cust_flag,\n" +
            "  ss_ticket_number,\n" +
            "  cnt\n" +
            "from\n" +
            "  (\n" +
            "    select\n" +
            "      ss_ticket_number,\n" +
            "      ss_customer_sk,\n" +
            "      count(*) cnt\n" +
            "    from\n" +
            "      store_sales,\n" +
            "      date_dim,\n" +
            "      store,\n" +
            "      household_demographics\n" +
            "    where\n" +
            "      store_sales.ss_sold_date_sk = date_dim.d_date_sk\n" +
            "      and store_sales.ss_store_sk = store.s_store_sk\n" +
            "      and store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk\n" +
            "      and date_dim.d_dom between 1\n" +
            "      and 2\n" +
            "      and (\n" +
            "        household_demographics.hd_buy_potential = '1001-5000'\n" +
            "        or household_demographics.hd_buy_potential = '5001-10000'\n" +
            "      )\n" +
            "      and household_demographics.hd_vehicle_count > 0\n" +
            "      and case\n" +
            "        when household_demographics.hd_vehicle_count > 0 " +
            "then household_demographics.hd_dep_count / household_demographics.hd_vehicle_count\n" +
            "        else null\n" +
            "      end > 1\n" +
            "      and date_dim.d_year in (1999, 1999 + 1, 1999 + 2)\n" +
            "      and store.s_county in (\n" +
            "        'Williamson County',\n" +
            "        'Williamson County',\n" +
            "        'Williamson County',\n" +
            "        'Williamson County'\n" +
            "      )\n" +
            "    group by\n" +
            "      ss_ticket_number,\n" +
            "      ss_customer_sk\n" +
            "  ) dj,\n" +
            "  customer\n" +
            "where\n" +
            "  ss_customer_sk = c_customer_sk\n" +
            "  and cnt between 1\n" +
            "  and 5\n" +
            "order by\n" +
            "  cnt desc,\n" +
            "  c_last_name asc;";

    public static final String Q80 = "select\n" +
            "  c_customer_id as customer_id,\n" +
            "  coalesce(c_last_name, '') || ', ' || coalesce(c_first_name, '') as customername\n" +
            "from\n" +
            "  customer,\n" +
            "  customer_address,\n" +
            "  customer_demographics,\n" +
            "  household_demographics,\n" +
            "  income_band,\n" +
            "  store_returns\n" +
            "where\n" +
            "  ca_city = 'White Oak'\n" +
            "  and c_current_addr_sk = ca_address_sk\n" +
            "  and ib_lower_bound >= 45626\n" +
            "  and ib_upper_bound <= 45626 + 50000\n" +
            "  and ib_income_band_sk = hd_income_band_sk\n" +
            "  and cd_demo_sk = c_current_cdemo_sk\n" +
            "  and hd_demo_sk = c_current_hdemo_sk\n" +
            "  and sr_cdemo_sk = cd_demo_sk\n" +
            "order by\n" +
            "  c_customer_id\n" +
            "limit\n" +
            "  100;";

    public static final String Q81 = "with my_customers as (\n" +
            " select distinct c_customer_sk\n" +
            "        , c_current_addr_sk\n" +
            " from\n" +
            "        ( select cs_sold_date_sk sold_date_sk,\n" +
            "                 cs_bill_customer_sk customer_sk,\n" +
            "                 cs_item_sk item_sk\n" +
            "          from   catalog_sales\n" +
            "          union all\n" +
            "          select ws_sold_date_sk sold_date_sk,\n" +
            "                 ws_bill_customer_sk customer_sk,\n" +
            "                 ws_item_sk item_sk\n" +
            "          from   web_sales\n" +
            "         ) cs_or_ws_sales,\n" +
            "         item,\n" +
            "         date_dim,\n" +
            "         customer\n" +
            " where   sold_date_sk = d_date_sk\n" +
            "         and item_sk = i_item_sk\n" +
            "         and i_category = 'Men'\n" +
            "         and i_class = 'shirts'\n" +
            "         and c_customer_sk = cs_or_ws_sales.customer_sk\n" +
            "         and d_moy = 4\n" +
            "         and d_year = 1998\n" +
            " )\n" +
            " , my_revenue as (\n" +
            " select c_customer_sk,\n" +
            "        sum(ss_ext_sales_price) as revenue\n" +
            " from   my_customers,\n" +
            "        store_sales,\n" +
            "        customer_address,\n" +
            "        store,\n" +
            "        date_dim\n" +
            " where  c_current_addr_sk = ca_address_sk\n" +
            "        and ca_county = s_county\n" +
            "        and ca_state = s_state\n" +
            "        and ss_sold_date_sk = d_date_sk\n" +
            "        and c_customer_sk = ss_customer_sk\n" +
            "        and d_month_seq between (select distinct d_month_seq+1\n" +
            "                                 from   date_dim where d_year = 1998 and d_moy = 4)\n" +
            "                           and  (select distinct d_month_seq+3\n" +
            "                                 from   date_dim where d_year = 1998 and d_moy = 4)\n" +
            " group by c_customer_sk\n" +
            " )\n" +
            " , segments as\n" +
            " (select cast((revenue/50) as int) as segment\n" +
            "  from   my_revenue\n" +
            " )\n" +
            "  select  segment, count(*) as num_customers, segment*50 as segment_base\n" +
            " from segments\n" +
            " group by segment\n" +
            " order by segment, num_customers\n" +
            " limit 100;";

    public static final String Q82 = "select\n" +
            "  i_brand_id brand_id,\n" +
            "  i_brand brand,\n" +
            "  sum(ss_ext_sales_price) ext_price\n" +
            "from\n" +
            "  date_dim,\n" +
            "  store_sales,\n" +
            "  item\n" +
            "where\n" +
            "  d_date_sk = ss_sold_date_sk\n" +
            "  and ss_item_sk = i_item_sk\n" +
            "  and i_manager_id = 20\n" +
            "  and d_moy = 12\n" +
            "  and d_year = 1998\n" +
            "group by\n" +
            "  i_brand,\n" +
            "  i_brand_id\n" +
            "order by\n" +
            "  ext_price desc,\n" +
            "  i_brand_id\n" +
            "limit\n" +
            "  100;";

    public static final String Q83 = "with ss as (\n" +
            "    select\n" +
            "      i_item_id,\n" +
            "      sum(ss_ext_sales_price) total_sales\n" +
            "    from\n" +
            "      store_sales,\n" +
            "      date_dim,\n" +
            "      customer_address,\n" +
            "      item\n" +
            "    where\n" +
            "      i_item_id in (\n" +
            "        select\n" +
            "          i_item_id\n" +
            "        from\n" +
            "          item\n" +
            "        where\n" +
            "          i_color in ('powder', 'goldenrod', 'bisque')\n" +
            "      )\n" +
            "      and ss_item_sk = i_item_sk\n" +
            "      and ss_sold_date_sk = d_date_sk\n" +
            "      and d_year = 1998\n" +
            "      and d_moy = 5\n" +
            "      and ss_addr_sk = ca_address_sk\n" +
            "      and ca_gmt_offset = -5\n" +
            "    group by\n" +
            "      i_item_id\n" +
            "  ),\n" +
            "  cs as (\n" +
            "    select\n" +
            "      i_item_id,\n" +
            "      sum(cs_ext_sales_price) total_sales\n" +
            "    from\n" +
            "      catalog_sales,\n" +
            "      date_dim,\n" +
            "      customer_address,\n" +
            "      item\n" +
            "    where\n" +
            "      i_item_id in (\n" +
            "        select\n" +
            "          i_item_id\n" +
            "        from\n" +
            "          item\n" +
            "        where\n" +
            "          i_color in ('powder', 'goldenrod', 'bisque')\n" +
            "      )\n" +
            "      and cs_item_sk = i_item_sk\n" +
            "      and cs_sold_date_sk = d_date_sk\n" +
            "      and d_year = 1998\n" +
            "      and d_moy = 5\n" +
            "      and cs_bill_addr_sk = ca_address_sk\n" +
            "      and ca_gmt_offset = -5\n" +
            "    group by\n" +
            "      i_item_id\n" +
            "  ),\n" +
            "  ws as (\n" +
            "    select\n" +
            "      i_item_id,\n" +
            "      sum(ws_ext_sales_price) total_sales\n" +
            "    from\n" +
            "      web_sales,\n" +
            "      date_dim,\n" +
            "      customer_address,\n" +
            "      item\n" +
            "    where\n" +
            "      i_item_id in (\n" +
            "        select\n" +
            "          i_item_id\n" +
            "        from\n" +
            "          item\n" +
            "        where\n" +
            "          i_color in ('powder', 'goldenrod', 'bisque')\n" +
            "      )\n" +
            "      and ws_item_sk = i_item_sk\n" +
            "      and ws_sold_date_sk = d_date_sk\n" +
            "      and d_year = 1998\n" +
            "      and d_moy = 5\n" +
            "      and ws_bill_addr_sk = ca_address_sk\n" +
            "      and ca_gmt_offset = -5\n" +
            "    group by\n" +
            "      i_item_id\n" +
            "  )\n" +
            "select\n" +
            "  i_item_id,\n" +
            "  sum(total_sales) total_sales\n" +
            "from\n" +
            "  (\n" +
            "    select\n" +
            "      *\n" +
            "    from\n" +
            "      ss\n" +
            "    union all\n" +
            "    select\n" +
            "      *\n" +
            "    from\n" +
            "      cs\n" +
            "    union all\n" +
            "    select\n" +
            "      *\n" +
            "    from\n" +
            "      ws\n" +
            "  ) tmp1\n" +
            "group by\n" +
            "  i_item_id\n" +
            "order by\n" +
            "  total_sales,\n" +
            "  i_item_id\n" +
            "limit\n" +
            "  100;";

    public static final String Q84 = "with wscs as (\n" +
            "    select\n" +
            "      sold_date_sk,\n" +
            "      sales_price\n" +
            "    from\n" +
            "      (\n" +
            "        select\n" +
            "          ws_sold_date_sk sold_date_sk,\n" +
            "          ws_ext_sales_price sales_price\n" +
            "        from\n" +
            "          web_sales\n" +
            "        union all\n" +
            "        select\n" +
            "          cs_sold_date_sk sold_date_sk,\n" +
            "          cs_ext_sales_price sales_price\n" +
            "        from\n" +
            "          catalog_sales\n" +
            "      ) as t1\n" +
            "  ),\n" +
            "  wswscs as (\n" +
            "    select\n" +
            "      d_week_seq,\n" +
            "      sum(\n" +
            "        case\n" +
            "          when (d_day_name = 'Sunday') then sales_price\n" +
            "          else null\n" +
            "        end\n" +
            "      ) sun_sales,\n" +
            "      sum(\n" +
            "        case\n" +
            "          when (d_day_name = 'Monday') then sales_price\n" +
            "          else null\n" +
            "        end\n" +
            "      ) mon_sales,\n" +
            "      sum(\n" +
            "        case\n" +
            "          when (d_day_name = 'Tuesday') then sales_price\n" +
            "          else null\n" +
            "        end\n" +
            "      ) tue_sales,\n" +
            "      sum(\n" +
            "        case\n" +
            "          when (d_day_name = 'Wednesday') then sales_price\n" +
            "          else null\n" +
            "        end\n" +
            "      ) wed_sales,\n" +
            "      sum(\n" +
            "        case\n" +
            "          when (d_day_name = 'Thursday') then sales_price\n" +
            "          else null\n" +
            "        end\n" +
            "      ) thu_sales,\n" +
            "      sum(\n" +
            "        case\n" +
            "          when (d_day_name = 'Friday') then sales_price\n" +
            "          else null\n" +
            "        end\n" +
            "      ) fri_sales,\n" +
            "      sum(\n" +
            "        case\n" +
            "          when (d_day_name = 'Saturday') then sales_price\n" +
            "          else null\n" +
            "        end\n" +
            "      ) sat_sales\n" +
            "    from\n" +
            "      wscs,\n" +
            "      date_dim\n" +
            "    where\n" +
            "      d_date_sk = sold_date_sk\n" +
            "    group by\n" +
            "      d_week_seq\n" +
            "  )\n" +
            "select\n" +
            "  d_week_seq1,\n" +
            "  round(sun_sales1 / sun_sales2, 2),\n" +
            "  round(mon_sales1 / mon_sales2, 2),\n" +
            "  round(tue_sales1 / tue_sales2, 2),\n" +
            "  round(wed_sales1 / wed_sales2, 2),\n" +
            "  round(thu_sales1 / thu_sales2, 2),\n" +
            "  round(fri_sales1 / fri_sales2, 2),\n" +
            "  round(sat_sales1 / sat_sales2, 2)\n" +
            "from\n" +
            "  (\n" +
            "    select\n" +
            "      wswscs.d_week_seq d_week_seq1,\n" +
            "      sun_sales sun_sales1,\n" +
            "      mon_sales mon_sales1,\n" +
            "      tue_sales tue_sales1,\n" +
            "      wed_sales wed_sales1,\n" +
            "      thu_sales thu_sales1,\n" +
            "      fri_sales fri_sales1,\n" +
            "      sat_sales sat_sales1\n" +
            "    from\n" +
            "      wswscs,\n" +
            "      date_dim\n" +
            "    where\n" +
            "      date_dim.d_week_seq = wswscs.d_week_seq\n" +
            "      and d_year = 2000\n" +
            "  ) y,\n" +
            "  (\n" +
            "    select\n" +
            "      wswscs.d_week_seq d_week_seq2,\n" +
            "      sun_sales sun_sales2,\n" +
            "      mon_sales mon_sales2,\n" +
            "      tue_sales tue_sales2,\n" +
            "      wed_sales wed_sales2,\n" +
            "      thu_sales thu_sales2,\n" +
            "      fri_sales fri_sales2,\n" +
            "      sat_sales sat_sales2\n" +
            "    from\n" +
            "      wswscs,\n" +
            "      date_dim\n" +
            "    where\n" +
            "      date_dim.d_week_seq = wswscs.d_week_seq\n" +
            "      and d_year = 2000 + 1\n" +
            "  ) z\n" +
            "where\n" +
            "  d_week_seq1 = d_week_seq2 -53\n" +
            "order by\n" +
            "  d_week_seq1;";

    public static final String Q85 = "select\n" +
            "  i_item_id,\n" +
            "  avg(cs_quantity) agg1,\n" +
            "  avg(cs_list_price) agg2,\n" +
            "  avg(cs_coupon_amt) agg3,\n" +
            "  avg(cs_sales_price) agg4\n" +
            "from\n" +
            "  catalog_sales,\n" +
            "  customer_demographics,\n" +
            "  date_dim,\n" +
            "  item,\n" +
            "  promotion\n" +
            "where\n" +
            "  cs_sold_date_sk = d_date_sk\n" +
            "  and cs_item_sk = i_item_sk\n" +
            "  and cs_bill_cdemo_sk = cd_demo_sk\n" +
            "  and cs_promo_sk = p_promo_sk\n" +
            "  and cd_gender = 'F'\n" +
            "  and cd_marital_status = 'M'\n" +
            "  and cd_education_status = '4 yr Degree'\n" +
            "  and (\n" +
            "    p_channel_email = 'N'\n" +
            "    or p_channel_event = 'N'\n" +
            "  )\n" +
            "  and d_year = 2000\n" +
            "group by\n" +
            "  i_item_id\n" +
            "order by\n" +
            "  i_item_id\n" +
            "limit\n" +
            "  100;";

    public static final String Q86 = "select\n" +
            "  w_state,\n" +
            "  i_item_id,\n" +
            "  sum(\n" +
            "    case\n" +
            "      when (\n" +
            "        cast(d_date as date) < cast ('2002-05-18' as date)\n" +
            "      ) then cs_sales_price - coalesce(cr_refunded_cash, 0)\n" +
            "      else 0\n" +
            "    end\n" +
            "  ) as sales_before,\n" +
            "  sum(\n" +
            "    case\n" +
            "      when (\n" +
            "        cast(d_date as date) >= cast ('2002-05-18' as date)\n" +
            "      ) then cs_sales_price - coalesce(cr_refunded_cash, 0)\n" +
            "      else 0\n" +
            "    end\n" +
            "  ) as sales_after\n" +
            "from\n" +
            "  catalog_sales\n" +
            "  left outer join catalog_returns on (\n" +
            "    cs_order_number = cr_order_number\n" +
            "    and cs_item_sk = cr_item_sk\n" +
            "  ),\n" +
            "  warehouse,\n" +
            "  item,\n" +
            "  date_dim\n" +
            "where\n" +
            "  i_current_price between 0.99\n" +
            "  and 1.49\n" +
            "  and i_item_sk = cs_item_sk\n" +
            "  and cs_warehouse_sk = w_warehouse_sk\n" +
            "  and cs_sold_date_sk = d_date_sk\n" +
            "  and d_date between date_sub(cast ('2002-05-18' as date), 30)\n" +
            "  and date_add(cast ('2002-05-18' as date), 30)\n" +
            "group by\n" +
            "  w_state,\n" +
            "  i_item_id\n" +
            "order by\n" +
            "  w_state,\n" +
            "  i_item_id\n" +
            "limit\n" +
            "  100;";

    public static final String Q87 = "select\n" +
            "  i_item_desc,\n" +
            "  w_warehouse_name,\n" +
            "  d1.d_week_seq,\n" +
            "  sum(\n" +
            "    case\n" +
            "      when p_promo_sk is null then 1\n" +
            "      else 0\n" +
            "    end\n" +
            "  ) no_promo,\n" +
            "  sum(\n" +
            "    case\n" +
            "      when p_promo_sk is not null then 1\n" +
            "      else 0\n" +
            "    end\n" +
            "  ) promo,\n" +
            "  count(*) total_cnt\n" +
            "from\n" +
            "  catalog_sales\n" +
            "  join inventory on (cs_item_sk = inv_item_sk)\n" +
            "  join warehouse on (w_warehouse_sk = inv_warehouse_sk)\n" +
            "  join item on (i_item_sk = cs_item_sk)\n" +
            "  join customer_demographics on (cs_bill_cdemo_sk = cd_demo_sk)\n" +
            "  join household_demographics on (cs_bill_hdemo_sk = hd_demo_sk)\n" +
            "  join date_dim d1 on (cs_sold_date_sk = d1.d_date_sk)\n" +
            "  join date_dim d2 on (inv_date_sk = d2.d_date_sk)\n" +
            "  join date_dim d3 on (cs_ship_date_sk = d3.d_date_sk)\n" +
            "  left outer join promotion on (cs_promo_sk = p_promo_sk)\n" +
            "  left outer join catalog_returns on (\n" +
            "    cr_item_sk = cs_item_sk\n" +
            "    and cr_order_number = cs_order_number\n" +
            "  )\n" +
            "where\n" +
            "  d1.d_week_seq = d2.d_week_seq\n" +
            "  and inv_quantity_on_hand < cs_quantity\n" +
            "  and d3.d_date > d1.d_date + 5\n" +
            "  and hd_buy_potential = '501-1000'\n" +
            "  and d1.d_year = 1999\n" +
            "  and cd_marital_status = 'S'\n" +
            "group by\n" +
            "  i_item_desc,\n" +
            "  w_warehouse_name,\n" +
            "  d1.d_week_seq\n" +
            "order by\n" +
            "  total_cnt desc,\n" +
            "  i_item_desc,\n" +
            "  w_warehouse_name,\n" +
            "  d_week_seq\n" +
            "limit\n" +
            "  100;";

    public static final String Q88 = "select  * from\n" +
            "(select i_manufact_id,\n" +
            "sum(ss_sales_price) sum_sales,\n" +
            "avg(sum(ss_sales_price)) over (partition by i_manufact_id) avg_quarterly_sales\n" +
            "from item, store_sales, date_dim, store\n" +
            "where ss_item_sk = i_item_sk and\n" +
            "ss_sold_date_sk = d_date_sk and\n" +
            "ss_store_sk = s_store_sk and\n" +
            "d_month_seq in (1197,1197+1,1197+2,1197+3,1197+4,1197+5,1197+6,1197+7,1197+8,1197+9,1197+10,1197+11) and\n" +
            "((i_category in ('Books','Children','Electronics') and\n" +
            "i_class in ('personal','portable','reference','self-help') and\n" +
            "i_brand in ('scholaramalgamalg #14','scholaramalgamalg #7',\n" +
            "        'exportiunivamalg #9','scholaramalgamalg #9'))\n" +
            "or(i_category in ('Women','Music','Men') and\n" +
            "i_class in ('accessories','classical','fragrances','pants') and\n" +
            "i_brand in ('amalgimporto #1','edu packscholar #1','exportiimporto #1',\n" +
            "        'importoamalg #1')))\n" +
            "group by i_manufact_id, d_qoy ) tmp1\n" +
            "where case when avg_quarterly_sales > 0\n" +
            "    then abs (sum_sales - avg_quarterly_sales)/ avg_quarterly_sales\n" +
            "    else null end > 0.1\n" +
            "order by avg_quarterly_sales,\n" +
            "     sum_sales,\n" +
            "     i_manufact_id\n" +
            "limit 100;";

    public static final String Q89 = "select\n" +
            "  c_last_name,\n" +
            "  c_first_name,\n" +
            "      substr(s_city, 1, 30),\n" +
            "  ss_ticket_number,\n" +
            "  amt,\n" +
            "  profit\n" +
            "from\n" +
            "  (\n" +
            "    select\n" +
            "      ss_ticket_number,\n" +
            "      ss_customer_sk,\n" +
            "      store.s_city,\n" +
            "      sum(ss_coupon_amt) amt,\n" +
            "      sum(ss_net_profit) profit\n" +
            "    from\n" +
            "      store_sales,\n" +
            "      date_dim,\n" +
            "      store,\n" +
            "      household_demographics\n" +
            "    where\n" +
            "      store_sales.ss_sold_date_sk = date_dim.d_date_sk\n" +
            "      and store_sales.ss_store_sk = store.s_store_sk\n" +
            "      and store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk\n" +
            "      and (\n" +
            "        household_demographics.hd_dep_count = 0\n" +
            "        or household_demographics.hd_vehicle_count > 4\n" +
            "      )\n" +
            "      and date_dim.d_dow = 1\n" +
            "      and date_dim.d_year in (1999, 1999 + 1, 1999 + 2)\n" +
            "      and store.s_number_employees between 200\n" +
            "      and 295\n" +
            "    group by\n" +
            "      ss_ticket_number,\n" +
            "      ss_customer_sk,\n" +
            "      ss_addr_sk,\n" +
            "      store.s_city\n" +
            "  ) ms,\n" +
            "  customer\n" +
            "where\n" +
            "  ss_customer_sk = c_customer_sk\n" +
            "order by\n" +
            "  c_last_name,\n" +
            "  c_first_name,\n" +
            "  substr(s_city, 1, 30),\n" +
            "  profit\n" +
            "limit\n" +
            "  100;";

    public static final String Q90 = "select  i_item_id,\n" +
            "        ca_country,\n" +
            "        ca_state,\n" +
            "        ca_county,\n" +
            "        avg( cast(cs_quantity as decimal(12,2))) agg1,\n" +
            "        avg( cast(cs_list_price as decimal(12,2))) agg2,\n" +
            "        avg( cast(cs_coupon_amt as decimal(12,2))) agg3,\n" +
            "        avg( cast(cs_sales_price as decimal(12,2))) agg4,\n" +
            "        avg( cast(cs_net_profit as decimal(12,2))) agg5,\n" +
            "        avg( cast(c_birth_year as decimal(12,2))) agg6,\n" +
            "        avg( cast(cd1.cd_dep_count as decimal(12,2))) agg7\n" +
            " from catalog_sales, customer_demographics cd1,\n" +
            "      customer_demographics cd2, customer, customer_address, date_dim, item\n" +
            " where cs_sold_date_sk = d_date_sk and\n" +
            "       cs_item_sk = i_item_sk and\n" +
            "       cs_bill_cdemo_sk = cd1.cd_demo_sk and\n" +
            "       cs_bill_customer_sk = c_customer_sk and\n" +
            "       cd1.cd_gender = 'M' and\n" +
            "       cd1.cd_education_status = 'Primary' and\n" +
            "       c_current_cdemo_sk = cd2.cd_demo_sk and\n" +
            "       c_current_addr_sk = ca_address_sk and\n" +
            "       c_birth_month in (1,2,9,5,11,3) and\n" +
            "       d_year = 1998 and\n" +
            "       ca_state in ('MS','NE','IA'\n" +
            "                   ,'MI','GA','NY','CO')\n" +
            " group by rollup (i_item_id, ca_country, ca_state, ca_county)\n" +
            " order by ca_country,\n" +
            "        ca_state,\n" +
            "        ca_county,\n" +
            "    i_item_id\n" +
            " limit 100;";

    public static final String Q91 = "select avg(ss_quantity)\n" +
            "       ,avg(ss_ext_sales_price)\n" +
            "       ,avg(ss_ext_wholesale_cost)\n" +
            "       ,sum(ss_ext_wholesale_cost)\n" +
            " from store_sales\n" +
            "     ,store\n" +
            "     ,customer_demographics\n" +
            "     ,household_demographics\n" +
            "     ,customer_address\n" +
            "     ,date_dim\n" +
            " where s_store_sk = ss_store_sk\n" +
            " and  ss_sold_date_sk = d_date_sk and d_year = 2001\n" +
            " and((ss_hdemo_sk=hd_demo_sk\n" +
            "  and cd_demo_sk = ss_cdemo_sk\n" +
            "  and cd_marital_status = 'U'\n" +
            "  and cd_education_status = '4 yr Degree'\n" +
            "  and ss_sales_price between 100.00 and 150.00\n" +
            "  and hd_dep_count = 3\n" +
            "     )or\n" +
            "     (ss_hdemo_sk=hd_demo_sk\n" +
            "  and cd_demo_sk = ss_cdemo_sk\n" +
            "  and cd_marital_status = 'S'\n" +
            "  and cd_education_status = 'Unknown'\n" +
            "  and ss_sales_price between 50.00 and 100.00\n" +
            "  and hd_dep_count = 1\n" +
            "     ) or\n" +
            "     (ss_hdemo_sk=hd_demo_sk\n" +
            "  and cd_demo_sk = ss_cdemo_sk\n" +
            "  and cd_marital_status = 'D'\n" +
            "  and cd_education_status = '2 yr Degree'\n" +
            "  and ss_sales_price between 150.00 and 200.00\n" +
            "  and hd_dep_count = 1\n" +
            "     ))\n" +
            " and((ss_addr_sk = ca_address_sk\n" +
            "  and ca_country = 'United States'\n" +
            "  and ca_state in ('CO', 'MI', 'MN')\n" +
            "  and ss_net_profit between 100 and 200\n" +
            "     ) or\n" +
            "     (ss_addr_sk = ca_address_sk\n" +
            "  and ca_country = 'United States'\n" +
            "  and ca_state in ('NC', 'NY', 'TX')\n" +
            "  and ss_net_profit between 150 and 300\n" +
            "     ) or\n" +
            "     (ss_addr_sk = ca_address_sk\n" +
            "  and ca_country = 'United States'\n" +
            "  and ca_state in ('CA', 'NE', 'TN')\n" +
            "  and ss_net_profit between 50 and 250\n" +
            "     ))\n" +
            ";";

    public static final String Q92 = "with ssales as (\n" +
            "    select\n" +
            "      c_last_name,\n" +
            "      c_first_name,\n" +
            "      s_store_name,\n" +
            "      ca_state,\n" +
            "      s_state,\n" +
            "      i_color,\n" +
            "      i_current_price,\n" +
            "      i_manager_id,\n" +
            "      i_units,\n" +
            "      i_size,\n" +
            "      sum(ss_net_profit) netpaid\n" +
            "    from\n" +
            "      store_sales,\n" +
            "      store_returns,\n" +
            "      store,\n" +
            "      item,\n" +
            "      customer,\n" +
            "      customer_address\n" +
            "    where\n" +
            "      ss_ticket_number = sr_ticket_number\n" +
            "      and ss_item_sk = sr_item_sk\n" +
            "      and ss_customer_sk = c_customer_sk\n" +
            "      and ss_item_sk = i_item_sk\n" +
            "      and ss_store_sk = s_store_sk\n" +
            "      and c_current_addr_sk = ca_address_sk\n" +
            "      and c_birth_country <> upper(ca_country)\n" +
            "      and s_zip = ca_zip\n" +
            "      and s_market_id = 10\n" +
            "    group by\n" +
            "      c_last_name,\n" +
            "      c_first_name,\n" +
            "      s_store_name,\n" +
            "      ca_state,\n" +
            "      s_state,\n" +
            "      i_color,\n" +
            "      i_current_price,\n" +
            "      i_manager_id,\n" +
            "      i_units,\n" +
            "      i_size\n" +
            "  )\n" +
            "select\n" +
            "  c_last_name,\n" +
            "  c_first_name,\n" +
            "  s_store_name,\n" +
            "  sum(netpaid) paid\n" +
            "from\n" +
            "  ssales\n" +
            "where\n" +
            "  i_color = 'orchid'\n" +
            "group by\n" +
            "  c_last_name,\n" +
            "  c_first_name,\n" +
            "  s_store_name\n" +
            "having\n" +
            "  sum(netpaid) > (\n" +
            "    select\n" +
            "      0.05 * avg(netpaid)\n" +
            "    from\n" +
            "      ssales\n" +
            "  )\n" +
            "order by\n" +
            "  c_last_name,\n" +
            "  c_first_name,\n" +
            "  s_store_name;\n" +
            "with ssales as (\n" +
            "    select\n" +
            "      c_last_name,\n" +
            "      c_first_name,\n" +
            "      s_store_name,\n" +
            "      ca_state,\n" +
            "      s_state,\n" +
            "      i_color,\n" +
            "      i_current_price,\n" +
            "      i_manager_id,\n" +
            "      i_units,\n" +
            "      i_size,\n" +
            "      sum(ss_net_profit) netpaid\n" +
            "    from\n" +
            "      store_sales,\n" +
            "      store_returns,\n" +
            "      store,\n" +
            "      item,\n" +
            "      customer,\n" +
            "      customer_address\n" +
            "    where\n" +
            "      ss_ticket_number = sr_ticket_number\n" +
            "      and ss_item_sk = sr_item_sk\n" +
            "      and ss_customer_sk = c_customer_sk\n" +
            "      and ss_item_sk = i_item_sk\n" +
            "      and ss_store_sk = s_store_sk\n" +
            "      and c_current_addr_sk = ca_address_sk\n" +
            "      and c_birth_country <> upper(ca_country)\n" +
            "      and s_zip = ca_zip\n" +
            "      and s_market_id = 10\n" +
            "    group by\n" +
            "      c_last_name,\n" +
            "      c_first_name,\n" +
            "      s_store_name,\n" +
            "      ca_state,\n" +
            "      s_state,\n" +
            "      i_color,\n" +
            "      i_current_price,\n" +
            "      i_manager_id,\n" +
            "      i_units,\n" +
            "      i_size\n" +
            "  )\n" +
            "select\n" +
            "  c_last_name,\n" +
            "  c_first_name,\n" +
            "  s_store_name,\n" +
            "  sum(netpaid) paid\n" +
            "from\n" +
            "  ssales\n" +
            "where\n" +
            "  i_color = 'green'\n" +
            "group by\n" +
            "  c_last_name,\n" +
            "  c_first_name,\n" +
            "  s_store_name\n" +
            "having\n" +
            "  sum(netpaid) > (\n" +
            "    select\n" +
            "      0.05 * avg(netpaid)\n" +
            "    from\n" +
            "      ssales\n" +
            "  )\n" +
            "order by\n" +
            "  c_last_name,\n" +
            "  c_first_name,\n" +
            "  s_store_name;";

    public static final String Q93 = "with year_total as (\n" +
            "    select\n" +
            "      c_customer_id customer_id,\n" +
            "      c_first_name customer_first_name,\n" +
            "      c_last_name customer_last_name,\n" +
            "      c_preferred_cust_flag customer_preferred_cust_flag,\n" +
            "      c_birth_country customer_birth_country,\n" +
            "      c_login customer_login,\n" +
            "      c_email_address customer_email_address,\n" +
            "      d_year dyear,\n" +
            "      sum(\n" +
            "        (\n" +
            "          (\n" +
            "            ss_ext_list_price - ss_ext_wholesale_cost - ss_ext_discount_amt\n" +
            "          ) + ss_ext_sales_price\n" +
            "        ) / 2\n" +
            "      ) year_total,\n" +
            "      's' sale_type\n" +
            "    from\n" +
            "      customer,\n" +
            "      store_sales,\n" +
            "      date_dim\n" +
            "    where\n" +
            "      c_customer_sk = ss_customer_sk\n" +
            "      and ss_sold_date_sk = d_date_sk\n" +
            "    group by\n" +
            "      c_customer_id,\n" +
            "      c_first_name,\n" +
            "      c_last_name,\n" +
            "      c_preferred_cust_flag,\n" +
            "      c_birth_country,\n" +
            "      c_login,\n" +
            "      c_email_address,\n" +
            "      d_year\n" +
            "    union all\n" +
            "    select\n" +
            "      c_customer_id customer_id,\n" +
            "      c_first_name customer_first_name,\n" +
            "      c_last_name customer_last_name,\n" +
            "      c_preferred_cust_flag customer_preferred_cust_flag,\n" +
            "      c_birth_country customer_birth_country,\n" +
            "      c_login customer_login,\n" +
            "      c_email_address customer_email_address,\n" +
            "      d_year dyear,\n" +
            "      sum(\n" +
            "        (\n" +
            "          (\n" +
            "            (\n" +
            "              cs_ext_list_price - cs_ext_wholesale_cost - cs_ext_discount_amt\n" +
            "            ) + cs_ext_sales_price\n" +
            "          ) / 2\n" +
            "        )\n" +
            "      ) year_total,\n" +
            "      'c' sale_type\n" +
            "    from\n" +
            "      customer,\n" +
            "      catalog_sales,\n" +
            "      date_dim\n" +
            "    where\n" +
            "      c_customer_sk = cs_bill_customer_sk\n" +
            "      and cs_sold_date_sk = d_date_sk\n" +
            "    group by\n" +
            "      c_customer_id,\n" +
            "      c_first_name,\n" +
            "      c_last_name,\n" +
            "      c_preferred_cust_flag,\n" +
            "      c_birth_country,\n" +
            "      c_login,\n" +
            "      c_email_address,\n" +
            "      d_year\n" +
            "    union all\n" +
            "    select\n" +
            "      c_customer_id customer_id,\n" +
            "      c_first_name customer_first_name,\n" +
            "      c_last_name customer_last_name,\n" +
            "      c_preferred_cust_flag customer_preferred_cust_flag,\n" +
            "      c_birth_country customer_birth_country,\n" +
            "      c_login customer_login,\n" +
            "      c_email_address customer_email_address,\n" +
            "      d_year dyear,\n" +
            "      sum(\n" +
            "        (\n" +
            "          (\n" +
            "            (\n" +
            "              ws_ext_list_price - ws_ext_wholesale_cost - ws_ext_discount_amt\n" +
            "            ) + ws_ext_sales_price\n" +
            "          ) / 2\n" +
            "        )\n" +
            "      ) year_total,\n" +
            "      'w' sale_type\n" +
            "    from\n" +
            "      customer,\n" +
            "      web_sales,\n" +
            "      date_dim\n" +
            "    where\n" +
            "      c_customer_sk = ws_bill_customer_sk\n" +
            "      and ws_sold_date_sk = d_date_sk\n" +
            "    group by\n" +
            "      c_customer_id,\n" +
            "      c_first_name,\n" +
            "      c_last_name,\n" +
            "      c_preferred_cust_flag,\n" +
            "      c_birth_country,\n" +
            "      c_login,\n" +
            "      c_email_address,\n" +
            "      d_year\n" +
            "  )\n" +
            "select\n" +
            "  t_s_secyear.customer_id,\n" +
            "  t_s_secyear.customer_first_name,\n" +
            "  t_s_secyear.customer_last_name,\n" +
            "  t_s_secyear.customer_email_address\n" +
            "from\n" +
            "  year_total t_s_firstyear,\n" +
            "  year_total t_s_secyear,\n" +
            "  year_total t_c_firstyear,\n" +
            "  year_total t_c_secyear,\n" +
            "  year_total t_w_firstyear,\n" +
            "  year_total t_w_secyear\n" +
            "where\n" +
            "  t_s_secyear.customer_id = t_s_firstyear.customer_id\n" +
            "  and t_s_firstyear.customer_id = t_c_secyear.customer_id\n" +
            "  and t_s_firstyear.customer_id = t_c_firstyear.customer_id\n" +
            "  and t_s_firstyear.customer_id = t_w_firstyear.customer_id\n" +
            "  and t_s_firstyear.customer_id = t_w_secyear.customer_id\n" +
            "  and t_s_firstyear.sale_type = 's'\n" +
            "  and t_c_firstyear.sale_type = 'c'\n" +
            "  and t_w_firstyear.sale_type = 'w'\n" +
            "  and t_s_secyear.sale_type = 's'\n" +
            "  and t_c_secyear.sale_type = 'c'\n" +
            "  and t_w_secyear.sale_type = 'w'\n" +
            "  and t_s_firstyear.dyear = 2001\n" +
            "  and t_s_secyear.dyear = 2001 + 1\n" +
            "  and t_c_firstyear.dyear = 2001\n" +
            "  and t_c_secyear.dyear = 2001 + 1\n" +
            "  and t_w_firstyear.dyear = 2001\n" +
            "  and t_w_secyear.dyear = 2001 + 1\n" +
            "  and t_s_firstyear.year_total > 0\n" +
            "  and t_c_firstyear.year_total > 0\n" +
            "  and t_w_firstyear.year_total > 0\n" +
            "  and case\n" +
            "    when t_c_firstyear.year_total > 0 then t_c_secyear.year_total / t_c_firstyear.year_total\n" +
            "    else null\n" +
            "  end > case\n" +
            "    when t_s_firstyear.year_total > 0 then t_s_secyear.year_total / t_s_firstyear.year_total\n" +
            "    else null\n" +
            "  end\n" +
            "  and case\n" +
            "    when t_c_firstyear.year_total > 0 then t_c_secyear.year_total / t_c_firstyear.year_total\n" +
            "    else null\n" +
            "  end > case\n" +
            "    when t_w_firstyear.year_total > 0 then t_w_secyear.year_total / t_w_firstyear.year_total\n" +
            "    else null\n" +
            "  end\n" +
            "order by\n" +
            "  t_s_secyear.customer_id,\n" +
            "  t_s_secyear.customer_first_name,\n" +
            "  t_s_secyear.customer_last_name,\n" +
            "  t_s_secyear.customer_email_address\n" +
            "limit\n" +
            "  100;\n";

    public static final String Q94 = "select\n" +
            "  substr(w_warehouse_name, 1, 20),\n" +
            "  sm_type,\n" +
            "  cc_name,\n" +
            "  sum(\n" +
            "    case\n" +
            "      when (cs_ship_date_sk - cs_sold_date_sk <= 30) then 1\n" +
            "      else 0\n" +
            "    end\n" +
            "  ) as \"30 days\",\n" +
            "  sum(\n" +
            "    case\n" +
            "      when (cs_ship_date_sk - cs_sold_date_sk > 30)\n" +
            "      and (cs_ship_date_sk - cs_sold_date_sk <= 60) then 1\n" +
            "      else 0\n" +
            "    end\n" +
            "  ) as \"31-60 days\",\n" +
            "  sum(\n" +
            "    case\n" +
            "      when (cs_ship_date_sk - cs_sold_date_sk > 60)\n" +
            "      and (cs_ship_date_sk - cs_sold_date_sk <= 90) then 1\n" +
            "      else 0\n" +
            "    end\n" +
            "  ) as \"61-90 days\",\n" +
            "  sum(\n" +
            "    case\n" +
            "      when (cs_ship_date_sk - cs_sold_date_sk > 90)\n" +
            "      and (cs_ship_date_sk - cs_sold_date_sk <= 120) then 1\n" +
            "      else 0\n" +
            "    end\n" +
            "  ) as \"91-120 days\",\n" +
            "  sum(\n" +
            "    case\n" +
            "      when (cs_ship_date_sk - cs_sold_date_sk > 120) then 1\n" +
            "      else 0\n" +
            "    end\n" +
            "  ) as \">120 days\"\n" +
            "from\n" +
            "  catalog_sales,\n" +
            "  warehouse,\n" +
            "  ship_mode,\n" +
            "  call_center,\n" +
            "  date_dim\n" +
            "where\n" +
            "  d_month_seq between 1188\n" +
            "  and 1188 + 11\n" +
            "  and cs_ship_date_sk = d_date_sk\n" +
            "  and cs_warehouse_sk = w_warehouse_sk\n" +
            "  and cs_ship_mode_sk = sm_ship_mode_sk\n" +
            "  and cs_call_center_sk = cc_call_center_sk\n" +
            "group by\n" +
            "  substr(w_warehouse_name, 1, 20),\n" +
            "  sm_type,\n" +
            "  cc_name\n" +
            "order by\n" +
            "  substr(w_warehouse_name, 1, 20),\n" +
            "  sm_type,\n" +
            "  cc_name\n" +
            "limit\n" +
            "  100;";

    public static final String Q95 = "select\n" +
            "  c_last_name,\n" +
            "  c_first_name,\n" +
            "  ca_city,\n" +
            "  bought_city,\n" +
            "  ss_ticket_number,\n" +
            "  extended_price,\n" +
            "  extended_tax,\n" +
            "  list_price\n" +
            "from\n" +
            "  (\n" +
            "    select\n" +
            "      ss_ticket_number,\n" +
            "      ss_customer_sk,\n" +
            "      ca_city bought_city,\n" +
            "      sum(ss_ext_sales_price) extended_price,\n" +
            "      sum(ss_ext_list_price) list_price,\n" +
            "      sum(ss_ext_tax) extended_tax\n" +
            "    from\n" +
            "      store_sales,\n" +
            "      date_dim,\n" +
            "      store,\n" +
            "      household_demographics,\n" +
            "      customer_address\n" +
            "    where\n" +
            "      store_sales.ss_sold_date_sk = date_dim.d_date_sk\n" +
            "      and store_sales.ss_store_sk = store.s_store_sk\n" +
            "      and store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk\n" +
            "      and store_sales.ss_addr_sk = customer_address.ca_address_sk\n" +
            "      and date_dim.d_dom between 1\n" +
            "      and 2\n" +
            "      and (\n" +
            "        household_demographics.hd_dep_count = 8\n" +
            "        or household_demographics.hd_vehicle_count = 3\n" +
            "      )\n" +
            "      and date_dim.d_year in (2000, 2000 + 1, 2000 + 2)\n" +
            "      and store.s_city in ('Midway', 'Fairview')\n" +
            "    group by\n" +
            "      ss_ticket_number,\n" +
            "      ss_customer_sk,\n" +
            "      ss_addr_sk,\n" +
            "      ca_city\n" +
            "  ) dn,\n" +
            "  customer,\n" +
            "  customer_address current_addr\n" +
            "where\n" +
            "  ss_customer_sk = c_customer_sk\n" +
            "  and customer.c_current_addr_sk = current_addr.ca_address_sk\n" +
            "  and current_addr.ca_city <> bought_city\n" +
            "order by\n" +
            "  c_last_name,\n" +
            "  ss_ticket_number\n" +
            "limit\n" +
            "  100;";

    public static final String Q96 = "with sr_items as (\n" +
            "    select\n" +
            "      i_item_id item_id,\n" +
            "      sum(sr_return_quantity) sr_item_qty\n" +
            "    from\n" +
            "      store_returns,\n" +
            "      item,\n" +
            "      date_dim\n" +
            "    where\n" +
            "      sr_item_sk = i_item_sk\n" +
            "      and d_date in (\n" +
            "        select\n" +
            "          d_date\n" +
            "        from\n" +
            "          date_dim\n" +
            "        where\n" +
            "          d_week_seq in (\n" +
            "            select\n" +
            "              d_week_seq\n" +
            "            from\n" +
            "              date_dim\n" +
            "            where\n" +
            "              d_date in ('2000-04-29', '2000-09-09', '2000-11-02')\n" +
            "          )\n" +
            "      )\n" +
            "      and sr_returned_date_sk = d_date_sk\n" +
            "    group by\n" +
            "      i_item_id\n" +
            "  ),\n" +
            "  cr_items as (\n" +
            "    select\n" +
            "      i_item_id item_id,\n" +
            "      sum(cr_return_quantity) cr_item_qty\n" +
            "    from\n" +
            "      catalog_returns,\n" +
            "      item,\n" +
            "      date_dim\n" +
            "    where\n" +
            "      cr_item_sk = i_item_sk\n" +
            "      and d_date in (\n" +
            "        select\n" +
            "          d_date\n" +
            "        from\n" +
            "          date_dim\n" +
            "        where\n" +
            "          d_week_seq in (\n" +
            "            select\n" +
            "              d_week_seq\n" +
            "            from\n" +
            "              date_dim\n" +
            "            where\n" +
            "              d_date in ('2000-04-29', '2000-09-09', '2000-11-02')\n" +
            "          )\n" +
            "      )\n" +
            "      and cr_returned_date_sk = d_date_sk\n" +
            "    group by\n" +
            "      i_item_id\n" +
            "  ),\n" +
            "  wr_items as (\n" +
            "    select\n" +
            "      i_item_id item_id,\n" +
            "      sum(wr_return_quantity) wr_item_qty\n" +
            "    from\n" +
            "      web_returns,\n" +
            "      item,\n" +
            "      date_dim\n" +
            "    where\n" +
            "      wr_item_sk = i_item_sk\n" +
            "      and d_date in (\n" +
            "        select\n" +
            "          d_date\n" +
            "        from\n" +
            "          date_dim\n" +
            "        where\n" +
            "          d_week_seq in (\n" +
            "            select\n" +
            "              d_week_seq\n" +
            "            from\n" +
            "              date_dim\n" +
            "            where\n" +
            "              d_date in ('2000-04-29', '2000-09-09', '2000-11-02')\n" +
            "          )\n" +
            "      )\n" +
            "      and wr_returned_date_sk = d_date_sk\n" +
            "    group by\n" +
            "      i_item_id\n" +
            "  )\n" +
            "select\n" +
            "  sr_items.item_id,\n" +
            "  sr_item_qty,\n" +
            "  sr_item_qty /(sr_item_qty + cr_item_qty + wr_item_qty) / 3.0 * 100 sr_dev,\n" +
            "  cr_item_qty,\n" +
            "  cr_item_qty /(sr_item_qty + cr_item_qty + wr_item_qty) / 3.0 * 100 cr_dev,\n" +
            "  wr_item_qty,\n" +
            "  wr_item_qty /(sr_item_qty + cr_item_qty + wr_item_qty) / 3.0 * 100 " +
            "wr_dev,(sr_item_qty + cr_item_qty + wr_item_qty) / 3.0 average\n" +
            "from\n" +
            "  sr_items,\n" +
            "  cr_items,\n" +
            "  wr_items\n" +
            "where\n" +
            "  sr_items.item_id = cr_items.item_id\n" +
            "  and sr_items.item_id = wr_items.item_id\n" +
            "order by\n" +
            "  sr_items.item_id,\n" +
            "  sr_item_qty\n" +
            "limit\n" +
            "  100;\n";

    public static final String Q97 = "select\n" +
            "  promotions,\n" +
            "  total,\n" +
            "  cast(promotions as decimal(15, 4)) / cast(total as decimal(15, 4)) * 100\n" +
            "from\n" +
            "  (\n" +
            "    select\n" +
            "      sum(ss_ext_sales_price) promotions\n" +
            "    from\n" +
            "      store_sales,\n" +
            "      store,\n" +
            "      promotion,\n" +
            "      date_dim,\n" +
            "      customer,\n" +
            "      customer_address,\n" +
            "      item\n" +
            "    where\n" +
            "      ss_sold_date_sk = d_date_sk\n" +
            "      and ss_store_sk = s_store_sk\n" +
            "      and ss_promo_sk = p_promo_sk\n" +
            "      and ss_customer_sk = c_customer_sk\n" +
            "      and ca_address_sk = c_current_addr_sk\n" +
            "      and ss_item_sk = i_item_sk\n" +
            "      and ca_gmt_offset = -6\n" +
            "      and i_category = 'Sports'\n" +
            "      and (\n" +
            "        p_channel_dmail = 'Y'\n" +
            "        or p_channel_email = 'Y'\n" +
            "        or p_channel_tv = 'Y'\n" +
            "      )\n" +
            "      and s_gmt_offset = -6\n" +
            "      and d_year = 2002\n" +
            "      and d_moy = 11\n" +
            "  ) promotional_sales,\n" +
            "  (\n" +
            "    select\n" +
            "      sum(ss_ext_sales_price) total\n" +
            "    from\n" +
            "      store_sales,\n" +
            "      store,\n" +
            "      date_dim,\n" +
            "      customer,\n" +
            "      customer_address,\n" +
            "      item\n" +
            "    where\n" +
            "      ss_sold_date_sk = d_date_sk\n" +
            "      and ss_store_sk = s_store_sk\n" +
            "      and ss_customer_sk = c_customer_sk\n" +
            "      and ca_address_sk = c_current_addr_sk\n" +
            "      and ss_item_sk = i_item_sk\n" +
            "      and ca_gmt_offset = -6\n" +
            "      and i_category = 'Sports'\n" +
            "      and s_gmt_offset = -6\n" +
            "      and d_year = 2002\n" +
            "      and d_moy = 11\n" +
            "  ) all_sales\n" +
            "order by\n" +
            "  promotions,\n" +
            "  total\n" +
            "limit\n" +
            "  100;\n";

    public static final String Q98 = "with ssr as (\n" +
            "    select\n" +
            "      s_store_id,\n" +
            "      sum(sales_price) as sales,\n" +
            "      sum(profit) as profit,\n" +
            "      sum(return_amt) as returns,\n" +
            "      sum(net_loss) as profit_loss\n" +
            "    from\n" +
            "      (\n" +
            "        select\n" +
            "          ss_store_sk as store_sk,\n" +
            "          ss_sold_date_sk as date_sk,\n" +
            "          ss_ext_sales_price as sales_price,\n" +
            "          ss_net_profit as profit,\n" +
            "          cast(0 as decimal(7, 2)) as return_amt,\n" +
            "          cast(0 as decimal(7, 2)) as net_loss\n" +
            "        from\n" +
            "          store_sales\n" +
            "        union all\n" +
            "        select\n" +
            "          sr_store_sk as store_sk,\n" +
            "          sr_returned_date_sk as date_sk,\n" +
            "          cast(0 as decimal(7, 2)) as sales_price,\n" +
            "          cast(0 as decimal(7, 2)) as profit,\n" +
            "          sr_return_amt as return_amt,\n" +
            "          sr_net_loss as net_loss\n" +
            "        from\n" +
            "          store_returns\n" +
            "      ) salesreturns,\n" +
            "      date_dim,\n" +
            "      store\n" +
            "    where\n" +
            "      date_sk = d_date_sk\n" +
            "      and d_date between cast('2001-08-04' as date)\n" +
            "      and date_add(cast('2001-08-04' as date), 14)\n" +
            "      and store_sk = s_store_sk\n" +
            "    group by\n" +
            "      s_store_id\n" +
            "  ),\n" +
            "  csr as (\n" +
            "    select\n" +
            "      cp_catalog_page_id,\n" +
            "      sum(sales_price) as sales,\n" +
            "      sum(profit) as profit,\n" +
            "      sum(return_amt) as returns,\n" +
            "      sum(net_loss) as profit_loss\n" +
            "    from\n" +
            "      (\n" +
            "        select\n" +
            "          cs_catalog_page_sk as page_sk,\n" +
            "          cs_sold_date_sk as date_sk,\n" +
            "          cs_ext_sales_price as sales_price,\n" +
            "          cs_net_profit as profit,\n" +
            "          cast(0 as decimal(7, 2)) as return_amt,\n" +
            "          cast(0 as decimal(7, 2)) as net_loss\n" +
            "        from\n" +
            "          catalog_sales\n" +
            "        union all\n" +
            "        select\n" +
            "          cr_catalog_page_sk as page_sk,\n" +
            "          cr_returned_date_sk as date_sk,\n" +
            "          cast(0 as decimal(7, 2)) as sales_price,\n" +
            "          cast(0 as decimal(7, 2)) as profit,\n" +
            "          cr_return_amount as return_amt,\n" +
            "          cr_net_loss as net_loss\n" +
            "        from\n" +
            "          catalog_returns\n" +
            "      ) salesreturns,\n" +
            "      date_dim,\n" +
            "      catalog_page\n" +
            "    where\n" +
            "      date_sk = d_date_sk\n" +
            "      and d_date between cast('2001-08-04' as date)\n" +
            "      and date_add(cast('2001-08-04' as date), 14)\n" +
            "      and page_sk = cp_catalog_page_sk\n" +
            "    group by\n" +
            "      cp_catalog_page_id\n" +
            "  ),\n" +
            "  wsr as (\n" +
            "    select\n" +
            "      web_site_id,\n" +
            "      sum(sales_price) as sales,\n" +
            "      sum(profit) as profit,\n" +
            "      sum(return_amt) as returns,\n" +
            "      sum(net_loss) as profit_loss\n" +
            "    from\n" +
            "      (\n" +
            "        select\n" +
            "          ws_web_site_sk as wsr_web_site_sk,\n" +
            "          ws_sold_date_sk as date_sk,\n" +
            "          ws_ext_sales_price as sales_price,\n" +
            "          ws_net_profit as profit,\n" +
            "          cast(0 as decimal(7, 2)) as return_amt,\n" +
            "          cast(0 as decimal(7, 2)) as net_loss\n" +
            "        from\n" +
            "          web_sales\n" +
            "        union all\n" +
            "        select\n" +
            "          ws_web_site_sk as wsr_web_site_sk,\n" +
            "          wr_returned_date_sk as date_sk,\n" +
            "          cast(0 as decimal(7, 2)) as sales_price,\n" +
            "          cast(0 as decimal(7, 2)) as profit,\n" +
            "          wr_return_amt as return_amt,\n" +
            "          wr_net_loss as net_loss\n" +
            "        from\n" +
            "          web_returns\n" +
            "          left outer join web_sales on (\n" +
            "            wr_item_sk = ws_item_sk\n" +
            "            and wr_order_number = ws_order_number\n" +
            "          )\n" +
            "      ) salesreturns,\n" +
            "      date_dim,\n" +
            "      web_site\n" +
            "    where\n" +
            "      date_sk = d_date_sk\n" +
            "      and d_date between cast('2001-08-04' as date)\n" +
            "      and date_add(cast('2001-08-04' as date), 14)\n" +
            "      and wsr_web_site_sk = web_site_sk\n" +
            "    group by\n" +
            "      web_site_id\n" +
            "  )\n" +
            "select\n" +
            "  channel,\n" +
            "  id,\n" +
            "  sum(sales) as sales,\n" +
            "  sum(returns) as returns,\n" +
            "  sum(profit) as profit\n" +
            "from\n" +
            "  (\n" +
            "    select\n" +
            "      'store channel' as channel,\n" +
            "      'store' || s_store_id as id,\n" +
            "      sales,\n" +
            "      returns,\n" +
            "      (profit - profit_loss) as profit\n" +
            "    from\n" +
            "      ssr\n" +
            "    union all\n" +
            "    select\n" +
            "      'globalStateMgr channel' as channel,\n" +
            "      'catalog_page' || cp_catalog_page_id as id,\n" +
            "      sales,\n" +
            "      returns,\n" +
            "      (profit - profit_loss) as profit\n" +
            "    from\n" +
            "      csr\n" +
            "    union all\n" +
            "    select\n" +
            "      'web channel' as channel,\n" +
            "      'web_site' || web_site_id as id,\n" +
            "      sales,\n" +
            "      returns,\n" +
            "      (profit - profit_loss) as profit\n" +
            "    from\n" +
            "      wsr\n" +
            "  ) x\n" +
            "group by\n" +
            "  rollup (channel, id)\n" +
            "order by\n" +
            "  channel,\n" +
            "  id\n" +
            "limit\n" +
            "  100;";

    public static final String Q99 = "select\n" +
            "  channel,\n" +
            "  col_name,\n" +
            "  d_year,\n" +
            "  d_qoy,\n" +
            "  i_category,\n" +
            "  COUNT(*) sales_cnt,\n" +
            "  SUM(ext_sales_price) sales_amt\n" +
            "FROM\n" +
            "  (\n" +
            "    SELECT\n" +
            "      'store' as channel,\n" +
            "      'ss_customer_sk' col_name,\n" +
            "      d_year,\n" +
            "      d_qoy,\n" +
            "      i_category,\n" +
            "      ss_ext_sales_price ext_sales_price\n" +
            "    FROM\n" +
            "      store_sales,\n" +
            "      item,\n" +
            "      date_dim\n" +
            "    WHERE\n" +
            "      ss_customer_sk IS NULL\n" +
            "      AND ss_sold_date_sk = d_date_sk\n" +
            "      AND ss_item_sk = i_item_sk\n" +
            "    UNION ALL\n" +
            "    SELECT\n" +
            "      'web' as channel,\n" +
            "      'ws_ship_hdemo_sk' col_name,\n" +
            "      d_year,\n" +
            "      d_qoy,\n" +
            "      i_category,\n" +
            "      ws_ext_sales_price ext_sales_price\n" +
            "    FROM\n" +
            "      web_sales,\n" +
            "      item,\n" +
            "      date_dim\n" +
            "    WHERE\n" +
            "      ws_ship_hdemo_sk IS NULL\n" +
            "      AND ws_sold_date_sk = d_date_sk\n" +
            "      AND ws_item_sk = i_item_sk\n" +
            "    UNION ALL\n" +
            "    SELECT\n" +
            "      'globalStateMgr' as channel,\n" +
            "      'cs_bill_customer_sk' col_name,\n" +
            "      d_year,\n" +
            "      d_qoy,\n" +
            "      i_category,\n" +
            "      cs_ext_sales_price ext_sales_price\n" +
            "    FROM\n" +
            "      catalog_sales,\n" +
            "      item,\n" +
            "      date_dim\n" +
            "    WHERE\n" +
            "      cs_bill_customer_sk IS NULL\n" +
            "      AND cs_sold_date_sk = d_date_sk\n" +
            "      AND cs_item_sk = i_item_sk\n" +
            "  ) foo\n" +
            "GROUP BY\n" +
            "  channel,\n" +
            "  col_name,\n" +
            "  d_year,\n" +
            "  d_qoy,\n" +
            "  i_category\n" +
            "ORDER BY\n" +
            "  channel,\n" +
            "  col_name,\n" +
            "  d_year,\n" +
            "  d_qoy,\n" +
            "  i_category\n" +
            "limit\n" +
            "  100;";

    public static final String Q80_2 = "with ssr as\n" +
            " (select  s_store_id as store_id,\n" +
            "          sum(ss_ext_sales_price) as sales,\n" +
            "          sum(coalesce(sr_return_amt, 0)) as returns,\n" +
            "          sum(ss_net_profit - coalesce(sr_net_loss, 0)) as profit\n" +
            "  from store_sales left outer join store_returns on\n" +
            "         (ss_item_sk = sr_item_sk and ss_ticket_number = sr_ticket_number),\n" +
            "     date_dim,\n" +
            "     store,\n" +
            "     item,\n" +
            "     promotion\n" +
            " where ss_sold_date_sk = d_date_sk\n" +
            "       and d_date between cast('2000-08-23' as date) \n" +
            "                  and date_add(cast('2000-08-23' as date),  30)\n" +
            "       and ss_store_sk = s_store_sk\n" +
            "       and ss_item_sk = i_item_sk\n" +
            "       and i_current_price > 50\n" +
            "       and ss_promo_sk = p_promo_sk\n" +
            "       and p_channel_tv = 'N'\n" +
            " group by s_store_id)\n" +
            " ,\n" +
            " csr as\n" +
            " (select  cp_catalog_page_id as catalog_page_id,\n" +
            "          sum(cs_ext_sales_price) as sales,\n" +
            "          sum(coalesce(cr_return_amount, 0)) as returns,\n" +
            "          sum(cs_net_profit - coalesce(cr_net_loss, 0)) as profit\n" +
            "  from catalog_sales left outer join catalog_returns on\n" +
            "         (cs_item_sk = cr_item_sk and cs_order_number = cr_order_number),\n" +
            "     date_dim,\n" +
            "     catalog_page,\n" +
            "     item,\n" +
            "     promotion\n" +
            " where cs_sold_date_sk = d_date_sk\n" +
            "       and d_date between cast('2000-08-23' as date)\n" +
            "                  and date_add(cast('2000-08-23' as date),  30)\n" +
            "        and cs_catalog_page_sk = cp_catalog_page_sk\n" +
            "       and cs_item_sk = i_item_sk\n" +
            "       and i_current_price > 50\n" +
            "       and cs_promo_sk = p_promo_sk\n" +
            "       and p_channel_tv = 'N'\n" +
            "group by cp_catalog_page_id)\n" +
            " ,\n" +
            " wsr as\n" +
            " (select  web_site_id,\n" +
            "          sum(ws_ext_sales_price) as sales,\n" +
            "          sum(coalesce(wr_return_amt, 0)) as returns,\n" +
            "          sum(ws_net_profit - coalesce(wr_net_loss, 0)) as profit\n" +
            "  from web_sales left outer join web_returns on\n" +
            "         (ws_item_sk = wr_item_sk and ws_order_number = wr_order_number),\n" +
            "     date_dim,\n" +
            "     web_site,\n" +
            "     item,\n" +
            "     promotion\n" +
            " where ws_sold_date_sk = d_date_sk\n" +
            "       and d_date between cast('2000-08-23' as date)\n" +
            "                  and date_add(cast('2000-08-23' as date),  30)\n" +
            "        and ws_web_site_sk = web_site_sk\n" +
            "       and ws_item_sk = i_item_sk\n" +
            "       and i_current_price > 50\n" +
            "       and ws_promo_sk = p_promo_sk\n" +
            "       and p_channel_tv = 'N'\n" +
            "group by web_site_id)\n" +
            "  select  channel\n" +
            "        , id\n" +
            "        , sum(sales) as sales\n" +
            "        , sum(returns) as returns\n" +
            "        , sum(profit) as profit\n" +
            " from \n" +
            " (select 'store channel' as channel\n" +
            "        , 'store' || store_id as id\n" +
            "        , sales\n" +
            "        , returns\n" +
            "        , profit\n" +
            " from   ssr\n" +
            " union all\n" +
            " select 'globalStateMgr channel' as channel\n" +
            "        , 'catalog_page' || catalog_page_id as id\n" +
            "        , sales\n" +
            "        , returns\n" +
            "        , profit\n" +
            " from  csr\n" +
            " union all\n" +
            " select 'web channel' as channel\n" +
            "        , 'web_site' || web_site_id as id\n" +
            "        , sales\n" +
            "        , returns\n" +
            "        , profit\n" +
            " from   wsr\n" +
            " ) x\n" +
            " group by channel, id\n" +
            " order by channel,id\n" +
            " limit 100; ";

    public static final String Q95_2 = "with ws_wh as\n" +
            "(select ws1.ws_order_number,ws1.ws_warehouse_sk wh1,ws2.ws_warehouse_sk wh2\n" +
            " from web_sales ws1,web_sales ws2\n" +
            " where ws1.ws_order_number = ws2.ws_order_number\n" +
            "   and ws1.ws_warehouse_sk <> ws2.ws_warehouse_sk)\n" +
            " select  \n" +
            "   count(distinct ws_order_number) as \"order count\"\n" +
            "  ,sum(ws_ext_ship_cost) as \"total shipping cost\"\n" +
            "  ,sum(ws_net_profit) as \"total net profit\"\n" +
            "from\n" +
            "   web_sales ws1\n" +
            "  ,date_dim\n" +
            "  ,customer_address\n" +
            "  ,web_site\n" +
            "where\n" +
            "    d_date between '1999-2-01' and \n" +
            "           date_add(cast('1999-2-01' as date), 60)\n" +
            "and ws1.ws_ship_date_sk = d_date_sk\n" +
            "and ws1.ws_ship_addr_sk = ca_address_sk\n" +
            "and ca_state = 'IL'\n" +
            "and ws1.ws_web_site_sk = web_site_sk\n" +
            "and web_company_name = 'pri'\n" +
            "and ws1.ws_order_number in (select ws_order_number\n" +
            "                            from ws_wh)\n" +
            "and ws1.ws_order_number in (select wr_order_number\n" +
            "                            from web_returns,ws_wh\n" +
            "                            where wr_order_number = ws_wh.ws_order_number)\n" +
            "order by count(distinct ws_order_number)\n" +
            "limit 100;";
}
