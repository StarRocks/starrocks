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

package com.starrocks.planner;

import com.starrocks.sql.plan.PlanTestNoneDBBase;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.BeforeClass;
import org.junit.Test;

public class SubfieldPushDownThroughTableFunctionTest extends PlanTestNoneDBBase {
    @BeforeClass
    public static void beforeClass() throws Exception {
        PlanTestNoneDBBase.beforeClass();
        starRocksAssert.withDatabase("test_db0").useDatabase("test_db0");
        String createTableSql1 = "CREATE TABLE `t1` (\n" +
                "  `col_date` date DEFAULT NULL,\n" +
                "  `col_hour` smallint(6) DEFAULT NULL,\n" +
                "  `col_header` struct < \n" +
                "    col_uuid varchar(10000),\n" +
                "    col_timestamp datetime,\n" +
                "    col_context array < struct < col_type varchar(10000), col_id varchar(10000) > >\n" +
                "  > DEFAULT NULL COMMENT \"Header Info.\",\n" +
                "  `col_project` struct < \n" +
                "    col_id varchar(10000),\n" +
                "    col_title varchar(10000),\n" +
                "    col_status boolean,\n" +
                "    col_criteria array < struct < \n" +
                "      col_criteria_id varchar(10000), \n" +
                "      col_condition varchar(10000), \n" +
                "      col_value varchar(10000), \n" +
                "      col_operator varchar(10000) \n" +
                "    > >,\n" +
                "    col_batches array < struct < \n" +
                "      col_batch_id varchar(10000),\n" +
                "      col_batch_name varchar(10000),\n" +
                "      col_batch_status boolean,\n" +
                "      col_budget_id varchar(10000),\n" +
                "      col_pricing struct < \n" +
                "        col_strategy varchar(10000),\n" +
                "        col_currency varchar(10000),\n" +
                "        col_base_bid decimal(15, 2),\n" +
                "        col_max_bid decimal(15, 2),\n" +
                "        col_adjustments array < struct < \n" +
                "          col_adjustment_id varchar(10000),\n" +
                "          col_criteria struct < \n" +
                "            col_criteria_id varchar(10000),\n" +
                "            col_condition varchar(10000),\n" +
                "            col_value varchar(10000),\n" +
                "            col_operator varchar(10000) \n" +
                "          >,\n" +
                "          col_coefficient decimal(15, 2) \n" +
                "        > >\n" +
                "      >,\n" +
                "      col_delivery varchar(10000),\n" +
                "      col_views int(11),\n" +
                "      col_bidding struct < \n" +
                "        col_goal varchar(10000),\n" +
                "        col_metric int(11) \n" +
                "      >,\n" +
                "      col_criteria array < struct < \n" +
                "        col_criteria_id varchar(10000),\n" +
                "        col_condition varchar(10000),\n" +
                "        col_value varchar(10000),\n" +
                "        col_operator varchar(10000) \n" +
                "      > >,\n" +
                "      col_items array < struct < \n" +
                "        col_item_id varchar(10000),\n" +
                "        col_primary_item_id varchar(10000),\n" +
                "        col_criteria array < struct < \n" +
                "          col_criteria_id varchar(10000),\n" +
                "          col_condition varchar(10000),\n" +
                "          col_value varchar(10000),\n" +
                "          col_operator varchar(10000) \n" +
                "        > >,\n" +
                "        col_content_id varchar(10000),\n" +
                "        col_type varchar(10000),\n" +
                "        col_title varchar(10000),\n" +
                "        col_status boolean \n" +
                "      > > \n" +
                "    > >\n" +
                "  >,\n" +
                "  `col_account` varchar(10000) DEFAULT NULL\n" +
                ") \n" +
                "PROPERTIES (\n" +
                "  \"replication_num\" = \"1\"\n" +
                ");";
        starRocksAssert.withTable(createTableSql1);
    }

    @Test
    public void test1() throws Exception {
        String[] sqlList = new String[] {
                "select col_header from t1, generate_series(1, 100);",
                "select t1.* from t1, generate_series(1, 100);"
        };
        for (String sql : sqlList) {
            String plan = UtFrameUtils.getFragmentPlan(connectContext, sql);
            assertCContains(plan, "generate_series");
        }
    }

    @Test
    public void test2() throws Exception {
        String sql = "WITH ranked_data AS (\n" +
                "    SELECT\n" +
                "      *,\n" +
                "      ROW_NUMBER() OVER (\n" +
                "        PARTITION BY CONCAT(col_project.col_id, RAND())\n" +
                "        ORDER BY (col_header.col_timestamp + RAND() * 10000) DESC\n" +
                "      ) AS rank\n" +
                "    FROM\n" +
                "      t1\n" +
                "),\n" +
                "deduped_data AS (\n" +
                "    SELECT\n" +
                "      *\n" +
                "    FROM\n" +
                "      ranked_data\n" +
                "    WHERE\n" +
                "      rank = 1\n" +
                "),\n" +
                "processed_data AS (\n" +
                "    SELECT\n" +
                "      col_project.col_id AS project_id,\n" +
                "      col_project.col_title AS project_title,\n" +
                "      col_batches.col_batch_name AS batch_name,\n" +
                "      col_batches.col_batch_id AS batch_id,\n" +
                "      col_items.col_item_id AS item_id,\n" +
                "      col_items.col_title AS item_title,\n" +
                "      col_items.col_type AS item_type,\n" +
                "      col_criteria.col_value AS criteria_value\n" +
                "    FROM\n" +
                "      deduped_data\n" +
                "    CROSS JOIN UNNEST(col_project.col_batches) AS batch_table (col_batches)\n" +
                "    CROSS JOIN UNNEST(col_batches.col_criteria) AS criteria_table (col_criteria)\n" +
                "    CROSS JOIN UNNEST(col_batches.col_items) AS items_table (col_items)\n" +
                "    WHERE col_criteria.col_operator = 'LTE'\n" +
                ")\n" +
                "SELECT\n" +
                "  SUM(MURMUR_HASH3_32(processed_data.project_title)),\n" +
                "  SUM(MURMUR_HASH3_32(processed_data.batch_name))\n" +
                "FROM\n" +
                "  processed_data;\n";
        String plan = UtFrameUtils.getFragmentPlan(connectContext, sql);
        assertCContains(plan, "  1:Project\n" +
                "  |  <slot 11> : concat(4: col_project.col_id[true], CAST(rand() AS VARCHAR))\n" +
                "  |  <slot 12> : CAST(3: col_header.col_timestamp[true] AS DOUBLE) + rand() * 10000.0\n" +
                "  |  <slot 32> : 4: col_project.col_title[false]\n" +
                "  |  <slot 37> : 4: col_project.col_batches[false]\n" +
                "  |  \n" +
                "  0:OlapScanNode");
    }
}