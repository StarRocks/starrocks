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

import com.starrocks.common.Pair;
import com.starrocks.datacache.DataCacheMgr;
import com.starrocks.qe.DDLStmtExecutor;
import com.starrocks.qe.DefaultCoordinator;
import com.starrocks.sql.analyzer.AnalyzeTestUtil;
import com.starrocks.sql.ast.ClearDataCacheRulesStmt;
import com.starrocks.sql.ast.CreateDataCacheRuleStmt;
import com.starrocks.sql.ast.DropDataCacheRuleStmt;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.thrift.TScanRangeParams;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

public class DataCachePlanTest extends PlanTestBase {

    private final DataCacheMgr dataCacheMgr = DataCacheMgr.getInstance();

    @BeforeAll
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        AnalyzeTestUtil.setConnectContext(connectContext);
        ConnectorPlanTestBase.mockHiveCatalog(connectContext);
    }

    @BeforeEach
    public void before() {
        connectContext.getSessionVariable().setEnableScanDataCache(true);
    }

    @AfterEach
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
        List<TScanRangeParams> tScanRangeLocations = collectAllScanRangeParams(pair.second);
        Assertions.assertEquals(-1, tScanRangeLocations.get(0).scan_range.hdfs_scan_range.getDatacache_options().getPriority());

        // clear rule
        ClearDataCacheRulesStmt clearDataCacheRulesStmt = new ClearDataCacheRulesStmt(NodePosition.ZERO);
        DDLStmtExecutor.execute(clearDataCacheRulesStmt, connectContext);

        executeSql = "select * from hive0.datacache_db.normal_table;";
        pair = UtFrameUtils.getPlanAndStartScheduling(connectContext, executeSql);
        tScanRangeLocations = collectAllScanRangeParams(pair.second);
        Assertions.assertFalse(tScanRangeLocations.get(0).scan_range.hdfs_scan_range.isSetDatacache_options());
    }

    @Test
    public void testForSinglePartition() throws Exception {
        // create rule first
        String sql = "create datacache rule hive0.datacache_db.single_partition_table where l_shipdate>='1998-01-07' priority=-1";
        CreateDataCacheRuleStmt stmt = (CreateDataCacheRuleStmt) AnalyzeTestUtil.analyzeSuccess(sql);
        dataCacheMgr.createCacheRule(stmt.getTarget(), stmt.getPredicates(), stmt.getPriority(), null);

        String executeSql = "select * from hive0.datacache_db.single_partition_table;";
        Pair<String, DefaultCoordinator> pair = UtFrameUtils.getPlanAndStartScheduling(connectContext, executeSql);
        List<TScanRangeParams> tScanRangeLocationsList = collectAllScanRangeParams(pair.second);
        Assertions.assertEquals(8, tScanRangeLocationsList.size());
        for (int i = 0; i < tScanRangeLocationsList.size(); i++) {
            TScanRangeParams tScanRangeLocations = tScanRangeLocationsList.get(i);
            if (tScanRangeLocations.scan_range.hdfs_scan_range.partition_id == 6 ||
                    tScanRangeLocations.scan_range.hdfs_scan_range.partition_id == 7) {
                Assertions.assertEquals(-1,
                        tScanRangeLocations.scan_range.hdfs_scan_range.getDatacache_options().getPriority());
            } else {
                Assertions.assertFalse(tScanRangeLocations.scan_range.hdfs_scan_range.isSetDatacache_options());
            }
        }

        // drop cache rule id = 0;
        DropDataCacheRuleStmt dropDataCacheRuleStmt = new DropDataCacheRuleStmt(0, NodePosition.ZERO);
        DDLStmtExecutor.execute(dropDataCacheRuleStmt, connectContext);

        executeSql = "select * from hive0.datacache_db.single_partition_table;";
        pair = UtFrameUtils.getPlanAndStartScheduling(connectContext, executeSql);
        tScanRangeLocationsList = collectAllScanRangeParams(pair.second);
        Assertions.assertEquals(8, tScanRangeLocationsList.size());
        for (TScanRangeParams tScanRangeLocations : tScanRangeLocationsList) {
            Assertions.assertFalse(tScanRangeLocations.scan_range.hdfs_scan_range.isSetDatacache_options());
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
        List<TScanRangeParams> tScanRangeLocationsList = collectAllScanRangeParams(pair.second);
        Assertions.assertEquals(8, tScanRangeLocationsList.size());
        for (int i = 0; i < tScanRangeLocationsList.size(); i++) {
            TScanRangeParams tScanRangeLocations = tScanRangeLocationsList.get(i);
            if (tScanRangeLocations.scan_range.hdfs_scan_range.partition_id == 7) {
                Assertions.assertEquals(-1,
                        tScanRangeLocations.scan_range.hdfs_scan_range.getDatacache_options().getPriority());
            } else {
                Assertions.assertFalse(tScanRangeLocations.scan_range.hdfs_scan_range.isSetDatacache_options());
            }
        }

        // clear rule
        dataCacheMgr.clearRules();
        executeSql = "select * from hive0.datacache_db.multi_partition_table;";
        pair = UtFrameUtils.getPlanAndStartScheduling(connectContext, executeSql);
        tScanRangeLocationsList = collectAllScanRangeParams(pair.second);
        Assertions.assertEquals(8, tScanRangeLocationsList.size());
        for (TScanRangeParams tScanRangeLocations : tScanRangeLocationsList) {
            Assertions.assertFalse(tScanRangeLocations.scan_range.hdfs_scan_range.isSetDatacache_options());
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
        List<TScanRangeParams> tScanRangeLocationsList = collectAllScanRangeParams(pair.second);
        Assertions.assertEquals(8, tScanRangeLocationsList.size());
        for (TScanRangeParams tScanRangeLocations : tScanRangeLocationsList) {
            Assertions.assertFalse(tScanRangeLocations.scan_range.hdfs_scan_range.isSetDatacache_options());
        }
    }

    @Test
    public void testBlackHoleTableSink() throws Exception {
        String sql = "insert into blackhole() select * from hive0.datacache_db.multi_partition_table " +
                "where l_shipdate>='1998-01-03'";
        assertPlanContains(sql, "BLACKHOLE TABLE SINK");

        sql = "insert into blackhole() select * from hive0.datacache_db.multi_partition_table join " +
                "hive0.datacache_db.multi_partition_table as t";
        assertPlanContains(sql, "BLACKHOLE TABLE SINK");
    }
}
