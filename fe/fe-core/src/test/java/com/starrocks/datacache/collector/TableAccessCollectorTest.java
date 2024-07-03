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

package com.starrocks.datacache.collector;

import com.starrocks.sql.analyzer.AnalyzeTestUtil;
import com.starrocks.sql.plan.ConnectorPlanTestBase;
import com.starrocks.sql.plan.PlanTestBase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

public class TableAccessCollectorTest extends PlanTestBase {

    private final TableAccessCollectorStorage storage = TableAccessCollectorStorage.getInstance();

    @BeforeClass
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        AnalyzeTestUtil.setConnectContext(connectContext);
        ConnectorPlanTestBase.mockHiveCatalog(connectContext);
    }

    @Before
    public void before() {
        connectContext.getSessionVariable().setEnableCollectTableAccessStatistics(true);
    }

    @After
    public void clearDataCacheMgr() {
        connectContext.getSessionVariable().setEnableCollectTableAccessStatistics(false);
    }

    @Test
    public void testIgnorePolicy() throws Exception {
        getFragmentPlan("select * from hive0.datacache_db.normal_table;");
        getFragmentPlan("select * from hive0.datacache_db.single_partition_table;");
        getFragmentPlan("select * from hive0.datacache_db.multi_partition_table;");
        Assert.assertEquals(0, storage.getEstimateMemorySize());
        Assert.assertEquals(0, storage.exportAccessLogs().size());

        connectContext.getSessionVariable().setEnableFullCollectTableAccessStatistics(true);
        getFragmentPlan("select * from hive0.datacache_db.normal_table;");
        Assert.assertEquals(103, storage.getEstimateMemorySize());
        List<AccessLog> accessLogs = storage.exportAccessLogs();
        Assert.assertEquals(0, storage.getEstimateMemorySize());
        Assert.assertEquals(3, accessLogs.size());

        connectContext.getSessionVariable().setEnableFullCollectTableAccessStatistics(false);
    }

    @Test
    public void testPartitionScan() throws Exception {
        getFragmentPlan("select r_name from hive0.datacache_db.normal_table;");
        getFragmentPlan("select r_name from hive0.datacache_db.normal_table;");
        List<AccessLog> accessLogs = storage.exportAccessLogs();
        Assert.assertEquals(1, accessLogs.size());
        AccessLog accessLog = accessLogs.get(0);
        Assert.assertEquals("hive0", accessLog.getCatalogName());
        Assert.assertEquals("datacache_db", accessLog.getDbName());
        Assert.assertEquals("normal_table", accessLog.getTableName());
        Assert.assertEquals("", accessLog.getPartitionName());
        Assert.assertEquals("r_name", accessLog.getColumnName());
        Assert.assertEquals(2, accessLog.getCount());

        getFragmentPlan(
                "select name from hive0.datacache_db.multi_partition_table where l_shipdate='1998-01-04' and l_orderkey=5;");
        Assert.assertEquals(144, storage.getEstimateMemorySize());
        accessLogs = storage.exportAccessLogs();
        Assert.assertEquals(3, accessLogs.size());
    }

    @Test
    public void testComplexType() throws Exception {
        getFragmentPlan("select col_struct.c0 from hive0.subfield_db.subfield;");
        List<AccessLog> accessLogs = storage.exportAccessLogs();
        Assert.assertEquals(1, accessLogs.size());
        Assert.assertEquals("col_struct.c0", accessLogs.get(0).getColumnName());

        getFragmentPlan("select col_struct.c0, col_struct.c1.c11 from hive0.subfield_db.subfield;");
        accessLogs = storage.exportAccessLogs();
        Assert.assertEquals(2, accessLogs.size());
        Assert.assertEquals("col_struct.c1.c11", accessLogs.get(0).getColumnName());
        Assert.assertEquals("col_struct.c0", accessLogs.get(1).getColumnName());

        getFragmentPlan("select col_array[1] from hive0.subfield_db.subfield;");
        accessLogs = storage.exportAccessLogs();
        Assert.assertEquals("col_array", accessLogs.get(0).getColumnName());

        getFragmentPlan("select map_values(col_map)[1].c1 from hive0.subfield_db.subfield;");
        accessLogs = storage.exportAccessLogs();
        Assert.assertEquals("col_map", accessLogs.get(0).getColumnName());

        getFragmentPlan("select map_keys(col_map)[1] from hive0.subfield_db.subfield;");
        accessLogs = storage.exportAccessLogs();
        Assert.assertEquals("map_keys(col_map)", accessLogs.get(0).getColumnName());
    }
}
