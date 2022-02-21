// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.plan;

import javax.swing.text.html.parser.DTD;

import com.starrocks.catalog.ScalarType;
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
                "                             d_current_week char(1) NULL) \n" +
                "ENGINE=OLAP\n" +
                "DUPLICATE KEY(`d_dow`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`d_dow`) BUCKETS 1\n" +
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
        Assert.assertTrue(plan.contains("unused_output_column_name:[d_day_name]"));
    }
}
