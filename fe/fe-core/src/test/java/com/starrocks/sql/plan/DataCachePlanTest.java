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

import com.starrocks.common.structure.Pair;
import com.starrocks.datacache.DataCacheMgr;
import com.starrocks.planner.PlanNodeId;
import com.starrocks.qe.DDLStmtExecutor;
import com.starrocks.qe.DefaultCoordinator;
import com.starrocks.sql.analyzer.AnalyzeTestUtil;
import com.starrocks.sql.ast.ClearDataCacheRulesStmt;
import com.starrocks.sql.ast.CreateDataCacheRuleStmt;
import com.starrocks.sql.ast.DropDataCacheRuleStmt;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.thrift.TScanRangeLocations;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

public class DataCachePlanTest extends PlanTestBase {

    private final DataCacheMgr dataCacheMgr = DataCacheMgr.getInstance();

    @BeforeClass
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        AnalyzeTestUtil.setConnectContext(connectContext);
        ConnectorPlanTestBase.mockHiveCatalog(connectContext);
    }

    @Before
    public void before() {
        connectContext.getSessionVariable().setEnableScanDataCache(true);
    }

    @After
    public void clearDataCacheMgr() {
        dataCacheMgr.clearRules();
    }

    @Test
    public void testForNormalTable() throws Exception {
        // create rule first
        String sql = "create datacache rule hive0.datacache_db.normal_table priority=-1";
        CreateDataCacheRuleStmt stmt = (CreateDataCacheRuleStmt) AnalyzeTestUtil.analyzeSuccess(sql);
        dataCacheMgr.createCacheRule(stmt.getTarget(), stmt.getPredicates(), stmt.getPriority(), null);

        String executeSql = "select * from hive0.datacache_db.normal_table;";
        Pair<String, DefaultCoordinator> pair = UtFrameUtils.getPlanAndStartScheduling(connectContext, executeSql);
        TScanRangeLocations tScanRangeLocations = pair.second.getFragments().get(1).collectScanNodes()
                .get(new PlanNodeId(0)).getScanRangeLocations(100).get(0);
        Assert.assertEquals(-1, tScanRangeLocations.scan_range.hdfs_scan_range.getDatacache_options().getPriority());

        // clear rule
        ClearDataCacheRulesStmt clearDataCacheRulesStmt = new ClearDataCacheRulesStmt(NodePosition.ZERO);
        DDLStmtExecutor.execute(clearDataCacheRulesStmt, connectContext);

        executeSql = "select * from hive0.datacache_db.normal_table;";
        pair = UtFrameUtils.getPlanAndStartScheduling(connectContext, executeSql);
        tScanRangeLocations = pair.second.getFragments().get(1).collectScanNodes()
                .get(new PlanNodeId(0)).getScanRangeLocations(100).get(0);
        Assert.assertFalse(tScanRangeLocations.scan_range.hdfs_scan_range.isSetDatacache_options());
    }

    @Test
    public void testForSinglePartition() throws Exception {
        // create rule first
        String sql = "create datacache rule hive0.datacache_db.single_partition_table where l_shipdate>='1998-01-07' priority=-1";
        CreateDataCacheRuleStmt stmt = (CreateDataCacheRuleStmt) AnalyzeTestUtil.analyzeSuccess(sql);
        dataCacheMgr.createCacheRule(stmt.getTarget(), stmt.getPredicates(), stmt.getPriority(), null);

        String executeSql = "select * from hive0.datacache_db.single_partition_table;";
        Pair<String, DefaultCoordinator> pair = UtFrameUtils.getPlanAndStartScheduling(connectContext, executeSql);
        List<TScanRangeLocations> tScanRangeLocationsList = pair.second.getFragments().get(1).collectScanNodes()
                .get(new PlanNodeId(0)).getScanRangeLocations(100);
        Assert.assertEquals(8, tScanRangeLocationsList.size());
        for (int i = 0; i < tScanRangeLocationsList.size(); i++) {
            TScanRangeLocations tScanRangeLocations = tScanRangeLocationsList.get(i);
            if (i <= 5) {
                Assert.assertFalse(tScanRangeLocations.scan_range.hdfs_scan_range.isSetDatacache_options());
            } else {
                Assert.assertEquals(-1, tScanRangeLocations.scan_range.hdfs_scan_range.getDatacache_options().getPriority());
            }
        }

        // drop cache rule id = 0;
        DropDataCacheRuleStmt dropDataCacheRuleStmt = new DropDataCacheRuleStmt(0, NodePosition.ZERO);
        DDLStmtExecutor.execute(dropDataCacheRuleStmt, connectContext);

        executeSql = "select * from hive0.datacache_db.single_partition_table;";
        pair = UtFrameUtils.getPlanAndStartScheduling(connectContext, executeSql);
        tScanRangeLocationsList = pair.second.getFragments().get(1).collectScanNodes()
                .get(new PlanNodeId(0)).getScanRangeLocations(100);
        Assert.assertEquals(8, tScanRangeLocationsList.size());
        for (TScanRangeLocations tScanRangeLocations : tScanRangeLocationsList) {
            Assert.assertFalse(tScanRangeLocations.scan_range.hdfs_scan_range.isSetDatacache_options());
        }
    }

    @Test
    public void testForMultiPartition() throws Exception {
        // create rule first
        String sql = "create datacache rule hive0.datacache_db.multi_partition_table where l_shipdate>='1998-01-03' " +
                "and l_orderkey=1 priority=-1";
        CreateDataCacheRuleStmt stmt = (CreateDataCacheRuleStmt) AnalyzeTestUtil.analyzeSuccess(sql);
        dataCacheMgr.createCacheRule(stmt.getTarget(), stmt.getPredicates(), stmt.getPriority(), null);

        String executeSql = "select * from hive0.datacache_db.multi_partition_table;";
        Pair<String, DefaultCoordinator> pair = UtFrameUtils.getPlanAndStartScheduling(connectContext, executeSql);
        List<TScanRangeLocations> tScanRangeLocationsList = pair.second.getFragments().get(1).collectScanNodes()
                .get(new PlanNodeId(0)).getScanRangeLocations(100);
        Assert.assertEquals(8, tScanRangeLocationsList.size());
        for (int i = 0; i < tScanRangeLocationsList.size(); i++) {
            TScanRangeLocations tScanRangeLocations = tScanRangeLocationsList.get(i);
            if (i <= 6) {
                Assert.assertFalse(tScanRangeLocations.scan_range.hdfs_scan_range.isSetDatacache_options());
            } else {
                Assert.assertEquals(-1, tScanRangeLocations.scan_range.hdfs_scan_range.getDatacache_options().getPriority());
            }
        }

        // clear rule
        dataCacheMgr.clearRules();
        executeSql = "select * from hive0.datacache_db.multi_partition_table;";
        pair = UtFrameUtils.getPlanAndStartScheduling(connectContext, executeSql);
        tScanRangeLocationsList = pair.second.getFragments().get(1).collectScanNodes()
                .get(new PlanNodeId(0)).getScanRangeLocations(100);
        Assert.assertEquals(8, tScanRangeLocationsList.size());
        for (TScanRangeLocations tScanRangeLocations : tScanRangeLocationsList) {
            Assert.assertFalse(tScanRangeLocations.scan_range.hdfs_scan_range.isSetDatacache_options());
        }
    }

    @Test
    public void testForDisableDataCache() throws Exception {
        connectContext.getSessionVariable().setEnableScanDataCache(false);
        // create rule first
        String sql = "create datacache rule hive0.datacache_db.multi_partition_table where l_shipdate>='1998-01-03' " +
                "and l_orderkey=1 priority=-1";
        CreateDataCacheRuleStmt stmt = (CreateDataCacheRuleStmt) AnalyzeTestUtil.analyzeSuccess(sql);
        dataCacheMgr.createCacheRule(stmt.getTarget(), stmt.getPredicates(), stmt.getPriority(), null);

        String executeSql = "select * from hive0.datacache_db.multi_partition_table;";
        Pair<String, DefaultCoordinator> pair = UtFrameUtils.getPlanAndStartScheduling(connectContext, executeSql);
        List<TScanRangeLocations> tScanRangeLocationsList = pair.second.getFragments().get(1).collectScanNodes()
                .get(new PlanNodeId(0)).getScanRangeLocations(100);
        Assert.assertEquals(8, tScanRangeLocationsList.size());
        for (TScanRangeLocations tScanRangeLocations : tScanRangeLocationsList) {
            Assert.assertFalse(tScanRangeLocations.scan_range.hdfs_scan_range.isSetDatacache_options());
        }
    }

    @Test
    public void testBlackHoleTableSink() throws Exception {
        String sql = "insert into blackhole() select * from hive0.datacache_db.multi_partition_table " +
                "where l_shipdate>='1998-01-03'";
        assertPlanContains(sql, "BLACKHOLE TABLE SINK");
    }
}
