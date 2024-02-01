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
import com.starrocks.utframe.StarRocksAssert;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class FilterUnusedColumnTest extends PlanTestBase {
    @BeforeClass
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        StarRocksAssert starRocksAssert = new StarRocksAssert(connectContext);
        starRocksAssert.withTable("CREATE TABLE tpcds_100g_date_dim (d_dow INTEGER NOT NULL,\n" +
                "                             d_day_name char(9) NULL,\n" +
                "                             d_current_week char(1) NULL,\n" +
                "                             d_date DATE NULL) \n" +
                "ENGINE=OLAP\n" +
                "DUPLICATE KEY(`d_dow`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`d_dow`) BUCKETS 1\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\"\n" +
                ");");
        // for agg table
        starRocksAssert.withTable("CREATE TABLE `metrics_detail` ( \n" +
                "`tags_id` int(11) NULL COMMENT \"\", \n" +
                "`timestamp` datetime NULL COMMENT \"\", \n" +
                "`value` double SUM NULL COMMENT \"\" \n" +
                ") ENGINE=OLAP \n" +
                "AGGREGATE KEY(`tags_id`, `timestamp`) \n" +
                "COMMENT \"OLAP\" \n" +
                "PARTITION BY RANGE(`timestamp`)\n" +
                "(PARTITION p20200704 VALUES [('0000-01-01 00:00:00'), ('2020-07-05 00:00:00')))\n" +
                "DISTRIBUTED BY HASH(`tags_id`) BUCKETS 1\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"enable_persistent_index\" = \"false\"\n" +
                ");");
        // for primary key table
        starRocksAssert.withTable("CREATE TABLE `primary_table` ( \n" +
                "`tags_id` int(11) NOT NULL COMMENT \"\", \n" +
                "`timestamp` datetime NOT NULL COMMENT \"\", \n" +
                "`k3` varchar(65533) NOT NULL COMMENT \"\" \n" +
                ") ENGINE=OLAP \n" +
                "PRIMARY KEY(`tags_id`, `timestamp`) \n" +
                "COMMENT \"OLAP\" \n" +
                "DISTRIBUTED BY HASH(`tags_id`) BUCKETS 1\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\"\n" +
                ");");

        FeConstants.USE_MOCK_DICT_MANAGER = true;
        connectContext.getSessionVariable().setSqlMode(2);
        connectContext.getSessionVariable().enableTrimOnlyFilteredColumnsInScanStage();
    }

    @Test
    public void testFilterComplexPredicate() throws Exception {
        String sql = "select\n" +
                "            ref_0.d_dow as c1 from tpcds_100g_date_dim as ref_0 \n" +
                "            where ref_0.d_day_name = ref_0.d_day_name limit 137;\n";
        String plan = getThriftPlan(sql);
        Assert.assertTrue(plan.contains("unused_output_column_name:[]"));
    }

    @Test
    public void testFilterSinglePredicate() throws Exception {
        String sql = "select\n" +
                "            ref_0.d_dow as c1 from tpcds_100g_date_dim as ref_0 \n" +
                "            where ref_0.d_day_name = \"dd\" limit 137;\n";
        String plan = getThriftPlan(sql);
        Assert.assertTrue(plan.contains("unused_output_column_name:[d_day_name]"));
    }

    @Test
    public void testFilterSinglePredicateWithoutOutputColumns() throws Exception {
        connectContext.getSessionVariable().enableTrimOnlyFilteredColumnsInScanStage();
        String sql = "select 1 from tpcds_100g_date_dim as ref_0 where ref_0.d_day_name=\"dd\" limit 137";
        String plan = getThriftPlan(sql);
        Assert.assertTrue(plan.contains("unused_output_column_name:[]"));
    }

    @Test
    public void testFilterProjection() throws Exception {
        String sql = "select\n" +
                "            ref_0.d_dow as c1, year(d_date) as year from tpcds_100g_date_dim as ref_0 \n" +
                "            where ref_0.d_date = \'1997-12-31\' limit 137;\n";
        String plan = getThriftPlan(sql);
        Assert.assertTrue(plan.contains("unused_output_column_name:[]"));
    }

    @Test
    public void testFilterAggTable() throws Exception {
        boolean prevEnable = connectContext.getSessionVariable().isEnableFilterUnusedColumnsInScanStage();

        try {
            connectContext.getSessionVariable().setEnableGlobalRuntimeFilter(true);

            // Key columns cannot be pruned in the non-skip-aggr scan stage.
            String sql = "select timestamp from metrics_detail where tags_id > 1";
            String plan = getThriftPlan(sql);
            Assert.assertTrue(plan.contains("unused_output_column_name:[]"));

            sql = "select max(value) from metrics_detail where tags_id > 1";
            plan = getThriftPlan(sql);
            Assert.assertTrue(plan.contains("unused_output_column_name:[]"));

            // Key columns can be pruned in the skip-aggr scan stage.
            sql = "select sum(value) from metrics_detail where tags_id > 1";
            plan = getThriftPlan(sql);
            Assert.assertTrue(plan.contains("unused_output_column_name:[tags_id]"));

            // Value columns cannot be pruned in the non-skip-aggr scan stage.
            sql = "select timestamp from metrics_detail where value is NULL limit 10;";
            plan = getThriftPlan(sql);
            Assert.assertTrue(plan.contains("unused_output_column_name:[]"));
        } finally {
            connectContext.getSessionVariable().setEnableGlobalRuntimeFilter(prevEnable);
        }
    }

    @Test
    public void testFilterAggMV() throws Exception {
        boolean prevEnable = connectContext.getSessionVariable().isEnableFilterUnusedColumnsInScanStage();

        try {
            connectContext.getSessionVariable().setEnableGlobalRuntimeFilter(true);
            starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW tpcds_100g_date_dim_mv as \n" +
                    "SELECT d_dow, d_day_name, max(d_date) \n" +
                    "FROM tpcds_100g_date_dim\n" +
                    "GROUP BY d_dow, d_day_name");

            String sql;
            String plan;

            // Key columns cannot be pruned in the non-skip-aggr scan stage of MV.
            sql = "select d_day_name from tpcds_100g_date_dim where d_dow > 1";
            plan = getThriftPlan(sql);
            assertContains(plan, "unused_output_column_name:[d_dow]");
            assertContains(plan, "rollup_name:tpcds_100g_date_dim");

            // Columns can pruned when using MV.
            sql = "select distinct d_day_name from tpcds_100g_date_dim where d_dow > 1";
            plan = getThriftPlan(sql);
            assertContains(plan, "unused_output_column_name:[d_dow]");
            assertContains(plan, "is_preaggregation:true");
            assertContains(plan, "rollup_name:tpcds_100g_date_dim_mv");

            // Columns can be pruned when not using MV.
            sql = "select d_day_name from tpcds_100g_date_dim where d_dow > 1";
            plan = getThriftPlan(sql);
            assertContains(plan, "unused_output_column_name:[d_dow]");
            assertContains(plan, "rollup_name:tpcds_100g_date_dim");

        } finally {
            connectContext.getSessionVariable().setEnableGlobalRuntimeFilter(prevEnable);
            try {
                starRocksAssert.dropMaterializedView("tpcds_100g_date_dim_mv");
            } catch (Exception e) {
                //
            }
        }
    }

    @Test
    public void testFilterPrimaryKeyTable() throws Exception {
        String sql = "select timestamp\n" +
                "               from primary_table where k3 = \"test\" limit 10;";
        String plan = getThriftPlan(sql);
        Assert.assertTrue(plan.contains("unused_output_column_name:[]"));
    }

    @Test
    public void testFilterDoublePredicateColumn() throws Exception {
        String sql = "select t1a from test_all_type where t1f > 1";
        String plan = getThriftPlan(sql);
        Assert.assertTrue(plan.contains("unused_output_column_name:[]"));

        sql = "select t1a from test_all_type where t1f is null";
        plan = getThriftPlan(sql);
        Assert.assertTrue(plan.contains("unused_output_column_name:[]"));

        sql = "select t1a from test_all_type where t1f in (1.0, 2.0)";
        plan = getThriftPlan(sql);
        Assert.assertTrue(plan.contains("unused_output_column_name:[]"));
    }
}
