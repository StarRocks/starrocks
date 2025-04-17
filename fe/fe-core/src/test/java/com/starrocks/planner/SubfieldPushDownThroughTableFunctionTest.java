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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.sql.plan.PlanTestNoneDBBase;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

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

        String createTableSql2 = " CREATE TABLE `t2` (\n" +
                "  `id` bigint(20) NOT NULL COMMENT \"\",\n" +
                "  `col0` varchar(65533) NULL COMMENT \"\",\n" +
                "  `col1` varchar(65533) NULL COMMENT \"\",\n" +
                "  `col2` varchar(65533) NULL COMMENT \"\",\n" +
                "  `col3` varchar(65533) NULL COMMENT \"\",\n" +
                "  `col4` varchar(65533) NULL COMMENT \"\",\n" +
                "  `col5` varchar(65533) NULL COMMENT \"\",\n" +
                "  `col6` varchar(65533) NULL COMMENT \"\",\n" +
                "  `col7` varchar(65533) NULL COMMENT \"\",\n" +
                "  `col8` varchar(65533) NULL COMMENT \"\",\n" +
                "  `col9` varchar(65533) NULL COMMENT \"\",\n" +
                "  `col10` varchar(65533) NULL COMMENT \"\",\n" +
                "  `col11` varchar(65533) NULL COMMENT \"\",\n" +
                "  `col12` int(11) NULL COMMENT \"\",\n" +
                "  `col13` varchar(65533) NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP \n" +
                "PRIMARY KEY(`id`)\n" +
                "COMMENT \"\"\n" +
                "DISTRIBUTED BY HASH(`id`) BUCKETS 3 \n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ");";
        starRocksAssert.withTable(createTableSql1);
        starRocksAssert.withTable(createTableSql2);
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
        Map<Integer, Set<Integer>> expectOuterCols = Maps.newHashMap();
        expectOuterCols.put(7, ImmutableSet.of(32));
        expectOuterCols.put(9, ImmutableSet.of(32, 33, 34));
        expectOuterCols.put(12, ImmutableSet.of(32, 33));
        checkTableFunctionOuterCols(sql, expectOuterCols);
    }

    @Test
    public void test3() throws Exception {
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
                "      col_project, \n" +
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
                "  processed_data.col_project,\n" +
                "  processed_data.project_title,\n" +
                "  processed_data.batch_name\n" +
                "FROM\n" +
                "  processed_data;\n";
        String plan = UtFrameUtils.getFragmentPlan(connectContext, sql);
        System.out.println(plan);
        assertCContains(plan, "  13:Project\n" +
                "  |  <slot 9> : 9: col_project\n" +
                "  |  <slot 21> : 28: expr\n" +
                "  |  <slot 22> : 29: expr");

        Map<Integer, Set<Integer>> expectOuterCols = Maps.newHashMap();
        expectOuterCols.put(7, ImmutableSet.of(9, 28));
        expectOuterCols.put(9, ImmutableSet.of(9, 28, 29, 30));
        expectOuterCols.put(12, ImmutableSet.of(9, 28, 29));
        checkTableFunctionOuterCols(sql, expectOuterCols);
    }

    @Test
    public void test4() throws Exception {
        String sql = "select\n" +
                "  col0\n" +
                " ,col1\n" +
                " ,col2\n" +
                " ,col3\n" +
                " ,col4\n" +
                " ,col5\n" +
                " ,col6\n" +
                " ,col7\n" +
                " ,col8\n" +
                " ,col9\n" +
                " ,col10\n" +
                " ,split(col6,'-')[2] as end_col6\n" +
                " ,t.unnest as day_week\n" +
                " ,case when t.unnest=7 then 1\n" +
                "       else t.unnest+1 end new_day_week\n" +
                " from t2, UNNEST(split(col11,'-')) AS t\n" +
                " where col12=1\n" +
                " and coalesce(col13,'UPDATE')<>'DELETE;";

        String plan = UtFrameUtils.getFragmentPlan(connectContext, sql);
        assertCContains(plan, "  1:Project\n" +
                "  |  <slot 2> : 2: col0\n" +
                "  |  <slot 3> : 3: col1\n" +
                "  |  <slot 4> : 4: col2\n" +
                "  |  <slot 5> : 5: col3\n" +
                "  |  <slot 6> : 6: col4\n" +
                "  |  <slot 7> : 7: col5\n" +
                "  |  <slot 8> : 8: col6\n" +
                "  |  <slot 9> : 9: col7\n" +
                "  |  <slot 10> : 10: col8\n" +
                "  |  <slot 11> : 11: col9\n" +
                "  |  <slot 12> : 12: col10\n" +
                "  |  <slot 17> : split(13: col11, '-')\n" +
                "  |  <slot 20> : split(8: col6, '-')[2]");

        Map<Integer, Set<Integer>> expectOuterCols = Maps.newHashMap();
        expectOuterCols.put(2, ImmutableSet.of(2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 20));
        checkTableFunctionOuterCols(sql, expectOuterCols);
    }

    private void checkTableFunctionOuterCols(String sql, Map<Integer, Set<Integer>> expectOuterCols)
            throws Exception {
        ExecPlan plan = getExecPlan(sql);
        Map<Integer, TableFunctionNode> tableFunctionNodes = plan.getFragments().stream()
                .flatMap(fragment -> fragment.collectNodes().stream())
                .filter(planNode -> planNode instanceof TableFunctionNode)
                .map(planNode -> (TableFunctionNode) planNode)
                .collect(Collectors.toMap(node -> node.getId().asInt(), node -> node));
        for (Map.Entry<Integer, Set<Integer>> e : expectOuterCols.entrySet()) {
            Assert.assertTrue(tableFunctionNodes.containsKey(e.getKey()));
            Set<Integer> actual = new HashSet<>(tableFunctionNodes.get(e.getKey()).getOuterSlots());
            Assert.assertEquals(actual, e.getValue());
        }
    }
}