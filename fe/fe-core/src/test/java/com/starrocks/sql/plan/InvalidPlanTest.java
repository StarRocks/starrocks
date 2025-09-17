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
import com.starrocks.sql.optimizer.rule.transformation.materialization.MVTestBase;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class InvalidPlanTest extends MVTestBase  {
    @BeforeAll
    public static void beforeAll() throws Exception {
        MVTestBase.beforeClass();
        FeConstants.unitTestView = false;
    }

    @Test
    public void testWithLargeBinaryPredicates() throws Exception {
        starRocksAssert.withTable("CREATE TABLE `test_table` (\n" +
                "  `id` bigint(20) NOT NULL,\n" +
                "  `cargo_code` bigint(20) NOT NULL,\n" +
                "  `cargo_upc` varchar(100) NULL,\n" +
                "  `cargo_name` varchar(256) NULL,\n" +
                "  `item1_desc` varchar(256) NULL,\n" +
                "  `upc_nbr` varchar(64) NULL,\n" +
                "  `vendor_nbr` varchar(64) NULL,\n" +
                "  `division` varchar(64) NULL,\n" +
                "  `sub_division` varchar(64) NULL,\n" +
                "  `sub_division_en` varchar(200) NULL,\n" +
                "  `category` varchar(200) NULL,\n" +
                "  `category_cn` varchar(200) NULL,\n" +
                "  `category_en` varchar(200) NULL,\n" +
                "  `sub_category` varchar(200) NULL,\n" +
                "  `sub_category_cn` varchar(200) NULL,\n" +
                "  `sub_category_en` varchar(200) NULL,\n" +
                "  `node_code` varchar(200) NOT NULL,\n" +
                "  `node_type` varchar(200) NULL,\n" +
                "  `storage_area` varchar(256) NULL,\n" +
                "  `fragile_flag` int(11) NULL,\n" +
                "  `storehouse_sell_date` int(11) NULL,\n" +
                "  `custom_box_gauge` int(11) NULL,\n" +
                "  `minimum_order_quantity` int(11) NULL,\n" +
                "  `activation_status` int(11) NULL,\n" +
                "  `day_sold_out_flag` int(11) NULL,\n" +
                "  `new_category_flag` int(11) NULL,\n" +
                "  `updated_time` datetime NULL,\n" +
                "  `calc_node_code_cargo_code` varchar(200) NULL,\n" +
                "  `sell_by_date` int(11) NULL,\n" +
                "  `dc_storage_conditions_code` varchar(200) NULL,\n" +
                "  `dc_storage_conditions` varchar(256) NULL,\n" +
                "  `deleted_flag` int(11) NULL,\n" +
                "  `specification` varchar(255) NULL,\n" +
                "  `lead_time_day` int(11) NULL,\n" +
                "  `lead_time_hour` int(11) NULL,\n" +
                "  `next_status` int(11) NULL,\n" +
                "  `next_switch_time` datetime NULL,\n" +
                "  `cloud_status` int(11) NULL,\n" +
                "  `latest_activate_date` datetime NULL,\n" +
                "  `long_desc` varchar(256) NULL,\n" +
                "  `activate_effect_time` datetime NULL,\n" +
                "  `inactivate_effect_time` datetime NULL,\n" +
                "  `ti` int(11) NULL,\n" +
                "  `hi` int(11) NULL\n" +
                ") ENGINE=OLAP \n" +
                "PRIMARY KEY(`id`)\n" +
                "DISTRIBUTED BY HASH(`id`) BUCKETS 20 \n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ");");
        String sql = ReplayFromDumpTestBase.geContentFromFile("bugs/large_binary_predicate1.sql");
        String plan = getFragmentPlan(sql);
        PlanTestBase.assertContains(plan, " 0:OlapScanNode\n" +
                "     TABLE: test_table\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: 32: deleted_flag = 0, 2: cargo_code IN ");
    }
}
