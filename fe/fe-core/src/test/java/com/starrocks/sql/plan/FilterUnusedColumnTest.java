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
import com.starrocks.common.FeConstants;
import com.starrocks.qe.SessionVariable;
import com.starrocks.utframe.StarRocksAssert;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class FilterUnusedColumnTest extends PlanTestBase {
    @BeforeAll
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
                "\"enable_persistent_index\" = \"true\"\n" +
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
        // Same columns as primary_table, duplicate keys: the baseline this change aligns primary keys with.
        starRocksAssert.withTable("CREATE TABLE `duplicate_table` ( \n" +
                "`tags_id` int(11) NOT NULL COMMENT \"\", \n" +
                "`timestamp` datetime NOT NULL COMMENT \"\", \n" +
                "`k3` varchar(65533) NOT NULL COMMENT \"\" \n" +
                ") ENGINE=OLAP \n" +
                "DUPLICATE KEY(`tags_id`, `timestamp`) \n" +
                "COMMENT \"OLAP\" \n" +
                "DISTRIBUTED BY HASH(`tags_id`) BUCKETS 1\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\"\n" +
                ");");

        FeConstants.USE_MOCK_DICT_MANAGER = true;
        connectContext.getSessionVariable().setSqlMode(2);
        connectContext.getSessionVariable().enableTrimOnlyFilteredColumnsInScanStage();
        connectContext.getSessionVariable().setEnableRewriteSimpleAggToMetaScan(false);
    }

    @Test
    public void testFilterComplexPredicate() throws Exception {
        String sql = "select\n" +
                "            ref_0.d_dow as c1 from tpcds_100g_date_dim as ref_0 \n" +
                "            where ref_0.d_day_name = ref_0.d_day_name limit 137;\n";
        String plan = getThriftPlan(sql);
        assertContains(plan, "unused_output_column_name:[d_day_name]");
    }

    @Test
    public void testFilterSinglePredicate() throws Exception {
        String sql = "select\n" +
                "            ref_0.d_dow as c1 from tpcds_100g_date_dim as ref_0 \n" +
                "            where ref_0.d_day_name = \"dd\" limit 137;\n";
        String plan = getThriftPlan(sql);
        assertContains(plan, "unused_output_column_name:[d_day_name]");
    }

    @Test
    public void testFilterSinglePredicateWithoutOutputColumns() throws Exception {
        connectContext.getSessionVariable().enableTrimOnlyFilteredColumnsInScanStage();
        String sql = "select 1 from tpcds_100g_date_dim as ref_0 where ref_0.d_day_name=\"dd\" limit 137";
        String plan = getThriftPlan(sql);
        assertContains(plan, "unused_output_column_name:[]");
    }

    @Test
    public void testRowIdCarrierWhenEveryPredicateColumnIsWide() throws Exception {
        SessionVariable sv = connectContext.getSessionVariable();
        boolean prevDict = sv.isEnableLowCardinalityOptimize();
        // UtFrameUtils turns virtual columns off for every FE unit test, while production defaults them on.
        boolean prevVirtual = Config.enable_virtual_columns;
        try {
            // A dict-encoded string is a cheap carrier, so drop the dict to model a high-cardinality column.
            sv.setEnableLowCardinalityOptimize(false);
            Config.enable_virtual_columns = true;
            String sql = "select count(*) from tpcds_100g_date_dim where d_day_name = 'dd'";
            assertContains(getThriftPlan(sql), "unused_output_column_name:[d_day_name]");
            assertContains(getDescTbl(sql), "colName:_row_id_");
        } finally {
            Config.enable_virtual_columns = prevVirtual;
            sv.setEnableLowCardinalityOptimize(prevDict);
        }
    }

    @Test
    public void testCheapPredicateColumnPreferredOverRowIdCarrier() throws Exception {
        SessionVariable sv = connectContext.getSessionVariable();
        boolean prevDict = sv.isEnableLowCardinalityOptimize();
        boolean prevVirtual = Config.enable_virtual_columns;
        try {
            sv.setEnableLowCardinalityOptimize(false);
            Config.enable_virtual_columns = true;
            String sql = "select count(*) from tpcds_100g_date_dim where d_dow > 1 and d_day_name = 'dd'";
            assertContains(getThriftPlan(sql), "unused_output_column_name:[d_day_name]");
            Assertions.assertFalse(getDescTbl(sql).contains("colName:_row_id_"));
        } finally {
            Config.enable_virtual_columns = prevVirtual;
            sv.setEnableLowCardinalityOptimize(prevDict);
        }
    }

    @Test
    public void testRowIdCarrierGatedByVirtualColumnsConfig() throws Exception {
        SessionVariable sv = connectContext.getSessionVariable();
        boolean prevDict = sv.isEnableLowCardinalityOptimize();
        boolean prevVirtual = Config.enable_virtual_columns;
        try {
            sv.setEnableLowCardinalityOptimize(false);
            Config.enable_virtual_columns = false;
            String sql = "select count(*) from tpcds_100g_date_dim where d_day_name = 'dd'";
            assertContains(getThriftPlan(sql), "unused_output_column_name:[]");
            Assertions.assertFalse(getDescTbl(sql).contains("colName:_row_id_"));
        } finally {
            Config.enable_virtual_columns = prevVirtual;
            sv.setEnableLowCardinalityOptimize(prevDict);
        }
    }

    @Test
    public void testDeterministicCarrierWithoutVirtualColumns() throws Exception {
        SessionVariable sv = connectContext.getSessionVariable();
        boolean prevDict = sv.isEnableLowCardinalityOptimize();
        boolean prevVirtual = Config.enable_virtual_columns;
        try {
            sv.setEnableLowCardinalityOptimize(false);
            // Without a synthesized carrier the cheapest predicate column still wins, so t1a stays unread.
            Config.enable_virtual_columns = false;
            String sql = "select count(1) from test_all_type where t1a='a' and t1b=1 and t1c=2";
            assertContains(getThriftPlan(sql), "unused_output_column_name:[t1a, t1c]");
            Assertions.assertFalse(getDescTbl(sql).contains("colName:_row_id_"));
        } finally {
            Config.enable_virtual_columns = prevVirtual;
            sv.setEnableLowCardinalityOptimize(prevDict);
        }
    }

    @Test
    public void testDictEncodedPredicateColumnCarriesRows() throws Exception {
        SessionVariable sv = connectContext.getSessionVariable();
        boolean prevDict = sv.isEnableLowCardinalityOptimize();
        boolean prevVirtual = Config.enable_virtual_columns;
        try {
            // A dict-encoded string is read as fixed-width codes, so it carries the rows and no carrier is added.
            sv.setEnableLowCardinalityOptimize(true);
            Config.enable_virtual_columns = true;
            String sql = "select count(*) from test_all_type where t1a = 'a'";
            assertContains(getThriftPlan(sql), "unused_output_column_name:[]");
            Assertions.assertFalse(getDescTbl(sql).contains("colName:_row_id_"));
        } finally {
            Config.enable_virtual_columns = prevVirtual;
            sv.setEnableLowCardinalityOptimize(prevDict);
        }
    }

    @Test
    public void testRowIdCarrierGatedByQueryCache() throws Exception {
        SessionVariable sv = connectContext.getSessionVariable();
        boolean prevDict = sv.isEnableLowCardinalityOptimize();
        boolean prevCache = sv.isEnableQueryCache();
        boolean prevVirtual = Config.enable_virtual_columns;
        try {
            sv.setEnableLowCardinalityOptimize(false);
            Config.enable_virtual_columns = true;
            sv.setEnableQueryCache(true);
            String sql = "select count(*) from tpcds_100g_date_dim where d_day_name = 'dd'";
            assertContains(getThriftPlan(sql), "unused_output_column_name:[]");
        } finally {
            Config.enable_virtual_columns = prevVirtual;
            sv.setEnableQueryCache(prevCache);
            sv.setEnableLowCardinalityOptimize(prevDict);
        }
    }

    @Test
    public void testFilterProjection() throws Exception {
        String sql = "select\n" +
                "            ref_0.d_dow as c1, year(d_date) as year from tpcds_100g_date_dim as ref_0 \n" +
                "            where ref_0.d_date = \'1997-12-31\' limit 137;\n";
        String plan = getThriftPlan(sql);
        assertContains(plan, "unused_output_column_name:[]");
    }

    @Test
    public void testFilterAggTable() throws Exception {
        boolean prevEnable = connectContext.getSessionVariable().isEnableFilterUnusedColumnsInScanStage();

        try {
            connectContext.getSessionVariable().setEnableGlobalRuntimeFilter(true);

            // Key columns cannot be pruned in the non-skip-aggr scan stage.
            String sql = "select timestamp from metrics_detail where tags_id > 1";
            String plan = getThriftPlan(sql);
            assertContains(plan, "unused_output_column_name:[]");

            sql = "select max(value) from metrics_detail where tags_id > 1";
            plan = getThriftPlan(sql);
            assertContains(plan, "unused_output_column_name:[]");

            // Key columns can be pruned in the skip-aggr scan stage.
            sql = "select sum(value) from metrics_detail where tags_id > 1";
            plan = getThriftPlan(sql);
            assertContains(plan, "unused_output_column_name:[tags_id]");

            // Value columns cannot be pruned in the non-skip-aggr scan stage.
            sql = "select timestamp from metrics_detail where value is NULL limit 10;";
            plan = getThriftPlan(sql);
            assertContains(plan, "unused_output_column_name:[]");
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
        // A primary key table never merges rows, so value columns are as prunable as on a duplicate table.
        {
            String sql = "select timestamp from primary_table where k3 = \"test\" limit 10;";
            String plan = getThriftPlan(sql);
            assertContains(plan, "unused_output_column_name:[k3]");
        }
        {
            // Both predicate columns are unused: tags_id is pushdownable and k3 is the only value column.
            String sql = "select timestamp from primary_table where k3 = \"test\" and tags_id = 1;";
            String plan = getThriftPlan(sql);
            logSysInfo(plan);
            assertContains(plan, "unused_output_column_name:[tags_id, k3]");
        }
        {
            // tags_id is also the output column, so only k3 stays unused.
            String sql = "select tags_id from primary_table where k3 = \"test\" and tags_id = 1;";
            String plan = getThriftPlan(sql);
            logSysInfo(plan);
            assertContains(plan, "unused_output_column_name:[k3]");
        }
        {
            // The non-pushdownable predicate sits in a SELECT node above the scan, so the scan must output its columns.
            String sql = "select k3 from primary_table where timestamp + tags_id = \"test\" and tags_id = 1;";
            String plan = getThriftPlan(sql);
            logSysInfo(plan);
            assertContains(plan, "unused_output_column_name:[]");
        }
        {
            String sql = "select timestamp from primary_table where k3 + tags_id = \"test\" and tags_id = 1;";
            String plan = getThriftPlan(sql);
            logSysInfo(plan);
            assertContains(plan, "unused_output_column_name:[]");
        }
    }

    // Locks the claim of this change: a primary key table prunes exactly what a duplicate key table prunes.
    @Test
    public void testFilterDuplicateKeyTableSameShapesAsPrimaryKey() throws Exception {
        {
            String sql = "select timestamp from duplicate_table where k3 = \"test\" limit 10;";
            assertContains(getThriftPlan(sql), "unused_output_column_name:[k3]");
        }
        {
            String sql = "select timestamp from duplicate_table where k3 = \"test\" and tags_id = 1;";
            assertContains(getThriftPlan(sql), "unused_output_column_name:[tags_id, k3]");
        }
        {
            String sql = "select tags_id from duplicate_table where k3 = \"test\" and tags_id = 1;";
            assertContains(getThriftPlan(sql), "unused_output_column_name:[k3]");
        }
        {
            // Empty here too, so the [] above is the predicate shape talking, not the key type.
            String sql = "select k3 from duplicate_table where timestamp + tags_id = \"test\" and tags_id = 1;";
            assertContains(getThriftPlan(sql), "unused_output_column_name:[]");
        }
        {
            String sql = "select timestamp from duplicate_table where k3 + tags_id = \"test\" and tags_id = 1;";
            assertContains(getThriftPlan(sql), "unused_output_column_name:[]");
        }
    }

    @Test
    public void testFilterDoublePredicateColumn() throws Exception {
        String sql = "select t1a from test_all_type where t1f > 1";
        String plan = getThriftPlan(sql);
        assertContains(plan, "unused_output_column_name:[t1f]");

        sql = "select t1a from test_all_type where t1f is null";
        plan = getThriftPlan(sql);
        assertContains(plan, "unused_output_column_name:[t1f]");

        sql = "select t1a from test_all_type where t1f in (1.0, 2.0)";
        plan = getThriftPlan(sql);
        assertContains(plan, "unused_output_column_name:[t1f]");
    }

    @Test
    public void testEmptyOutputColumns() throws Exception {
        {
            String sql = "select count(1) from test_all_type";
            String plan = getThriftPlan(sql);
            assertContains(plan, "unused_output_column_name:[]");
        }
        {
            String sql = "select count(1) from test_all_type where t1a='a'";
            String plan = getThriftPlan(sql);
            assertContains(plan, "unused_output_column_name:[]");
        }
        {
            // Nothing above the scan needs a value, so the cheapest predicate column (t1b) carries the rows.
            String sql = "select count(1) from test_all_type where t1a='a' and t1b=1 and t1c=2";
            String plan = getThriftPlan(sql);
            assertContains(plan, "unused_output_column_name:[t1a, t1c]");
        }

        {
            String sql = "select 1 from test_all_type";
            String plan = getThriftPlan(sql);
            assertContains(plan, "unused_output_column_name:[]");
        }
        {
            String sql = "select 1 from test_all_type where t1a='a'";
            String plan = getThriftPlan(sql);
            assertContains(plan, "unused_output_column_name:[]");
        }
        {
            String sql = "select 1 from test_all_type where t1a='a' and t1b=1 and t1c=2";
            String plan = getThriftPlan(sql);
            assertContains(plan, "unused_output_column_name:[t1a, t1c]");
        }
    }

    @Test
    public void tesOrPredicate() throws Exception {
        {
            String sql = "select count(1) from test_all_type where t1a='a' or (t1b=1 and t1c=2)";
            String plan = getThriftPlan(sql);
            assertContains(plan, "unused_output_column_name:[t1a, t1c]");
        }
        {
            String sql = "select count(1) from test_all_type where t1a='a' or (t1b=1 and t1c=2 and t1a='b')";
            String plan = getThriftPlan(sql);
            assertContains(plan, "unused_output_column_name:[t1a, t1c]");
        }
        {
            String sql = "select count(1) from test_all_type where t1a='a' or (t1b=1 and t1c=2 and t1d+t1a=3)";
            String plan = getThriftPlan(sql);
            assertContains(plan, "unused_output_column_name:[]");
        }

        // Aggregate-key mode.
        {
            String sql = "select sum(value) from metrics_detail " +
                    "where tags_id=1 or value=1 ";
            String plan = getThriftPlan(sql);
            assertContains(plan, "unused_output_column_name:[]");
        }
        {
            String sql = "select sum(value) from metrics_detail " +
                    "where tags_id=1 or timestamp=1 ";
            String plan = getThriftPlan(sql);
            assertContains(plan, "unused_output_column_name:[tags_id, timestamp]");
        }

        // Primary-key mode.
        {
            String sql = "select k3 from primary_table " +
                    "where tags_id=1 or k3=1 ";
            String plan = getThriftPlan(sql);
            assertContains(plan, "unused_output_column_name:[tags_id]");
        }
        {
            String sql = "select k3 from primary_table " +
                    "where tags_id=1 or timestamp=1 ";
            String plan = getThriftPlan(sql);
            assertContains(plan, "unused_output_column_name:[tags_id, timestamp]");
        }

        // Disable pushdown or predicate.
        {
            String sql = "select /*+SET_VAR(enable_pushdown_or_predicate=false)*/ " +
                    "count(1) from test_all_type where t1a='a' or (t1b=1 and t1c=2)";
            String plan = getThriftPlan(sql);
            assertContains(plan, "unused_output_column_name:[t1a, t1c]");
        }
        {
            String sql = "select /*+SET_VAR(enable_pushdown_or_predicate=false)*/ " +
                    "count(1) from test_all_type where t1a='a' or (t1b=1 and t1c=2 and t1a='b')";
            String plan = getThriftPlan(sql);
            assertContains(plan, "unused_output_column_name:[t1a, t1c]");
        }
    }
}
