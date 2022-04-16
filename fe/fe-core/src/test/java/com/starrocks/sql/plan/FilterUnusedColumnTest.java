// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

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
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"DEFAULT\"\n" +
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
                "\"storage_format\" = \"DEFAULT\"\n" +
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
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"DEFAULT\"\n" +
                ");");
        FeConstants.USE_MOCK_DICT_MANAGER = true;
        connectContext.getSessionVariable().setSqlMode(2);
        
    }

    @Test
    public void testFilterComplexPredicate() throws Exception {
        connectContext.getSessionVariable().enableTrimOnlyFilteredColumnsInScanStage();
        String sql = "select\n" +
                "            ref_0.d_dow as c1 from tpcds_100g_date_dim as ref_0 \n" +
                "            where ref_0.d_day_name = ref_0.d_day_name limit 137;\n";
        String plan = getThriftPlan(sql);
        Assert.assertTrue(plan.contains("unused_output_column_name:[]"));
    }

    @Test
    public void testFilterSinglePredicate() throws Exception {
        connectContext.getSessionVariable().enableTrimOnlyFilteredColumnsInScanStage();
        String sql = "select\n" +
                "            ref_0.d_dow as c1 from tpcds_100g_date_dim as ref_0 \n" +
                "            where ref_0.d_day_name = \"dd\" limit 137;\n";
        String plan = getThriftPlan(sql);
        System.out.println(plan);
        Assert.assertTrue(plan.contains("unused_output_column_name:[d_day_name]"));
    }

    @Test
    public void testFilterProjection() throws Exception {
        connectContext.getSessionVariable().enableTrimOnlyFilteredColumnsInScanStage();
        String sql = "select\n" +
                "            ref_0.d_dow as c1, year(d_date) as year from tpcds_100g_date_dim as ref_0 \n" +
                "            where ref_0.d_date = \'1997-12-31\' limit 137;\n";
        String plan = getThriftPlan(sql);
        Assert.assertTrue(plan.contains("unused_output_column_name:[]"));
    }

    @Test 
    public void testFilterAggTable() throws Exception {
        connectContext.getSessionVariable().enableTrimOnlyFilteredColumnsInScanStage();
        String sql = "select timestamp\n" +
                "               from metrics_detail where value is NULL limit 10;";
        String plan = getThriftPlan(sql);
        Assert.assertTrue(plan.contains("unused_output_column_name:[]"));
    }

    @Test 
    public void testFilterPrimaryKeyTable() throws Exception {
        connectContext.getSessionVariable().enableTrimOnlyFilteredColumnsInScanStage();
        String sql = "select timestamp\n" +
                "               from primary_table where k3 = \"test\" limit 10;";
        String plan = getThriftPlan(sql);
        Assert.assertTrue(plan.contains("unused_output_column_name:[]"));
    }
}
