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

package com.starrocks.leader;

import com.google.common.collect.Lists;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.common.FeConstants;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.QueryQueueManager;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.Backend;
import com.starrocks.system.ComputeNode;
import com.starrocks.system.SystemInfoService;
import com.starrocks.thrift.TBackend;
import com.starrocks.thrift.TMasterResult;
import com.starrocks.thrift.TReportRequest;
import com.starrocks.thrift.TResourceUsage;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.thrift.TTablet;
import com.starrocks.thrift.TTabletInfo;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import org.apache.thrift.TException;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ReportHandlerTest {
    private static ConnectContext connectContext;

    @BeforeClass
    public static void beforeClass() throws Exception {
        FeConstants.default_scheduler_interval_millisecond = 1000;
        FeConstants.runningUnitTest = true;

        UtFrameUtils.createMinStarRocksCluster();

        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
        connectContext.setQueryId(UUIDUtil.genUUID());
        StarRocksAssert starRocksAssert = new StarRocksAssert(connectContext);

        starRocksAssert.withDatabase("test").useDatabase("test")
                .withTable("CREATE TABLE test.properties_change_test(k1 int, v1 int) " +
                        "primary key(k1) distributed by hash(k1) properties('replication_num' = '1');")
                .withTable("CREATE TABLE test.binlog_report_handler_test(k1 int, v1 int) " +
                        "duplicate key(k1) distributed by hash(k1) buckets 5 properties('replication_num' = '1', " +
                        "'binlog_enable' = 'true', 'binlog_max_size' = '100');");
    }

    @Test
    public void testHandleSetTabletEnablePersistentIndex() {
        Database db = GlobalStateMgr.getCurrentState().getDb("test");
        long dbId = db.getId();
        long backendId = 10001L;
        List<Long> tabletIds = GlobalStateMgr.getCurrentInvertedIndex().getTabletIdsByBackendId(10001);
        Assert.assertFalse(tabletIds.isEmpty());

        Map<Long, TTablet> backendTablets = new HashMap<Long, TTablet>();
        List<TTabletInfo> tabletInfos = Lists.newArrayList();
        TTablet tablet = new TTablet(tabletInfos);
        for (Long tabletId : tabletIds) {
            TTabletInfo tabletInfo = new TTabletInfo();
            tabletInfo.setTablet_id(tabletId);
            tabletInfo.setSchema_hash(60000);
            tabletInfo.setEnable_persistent_index(true);
            tablet.tablet_infos.add(tabletInfo);
        }
        backendTablets.put(backendId, tablet);

        ReportHandler handler = new ReportHandler();
        handler.testHandleSetTabletEnablePersistentIndex(backendId, backendTablets);
    }

    @Test
    public void testHandleSetTabletBinlogConfig() {
        Database db = GlobalStateMgr.getCurrentState().getDb("test");
        long dbId = db.getId();
        OlapTable olapTable = (OlapTable) db.getTable("binlog_report_handler_test");
        long backendId = 10001L;
        List<Long> tabletIds = GlobalStateMgr.getCurrentInvertedIndex().getTabletIdsByBackendId(10001);
        Assert.assertFalse(tabletIds.isEmpty());

        Map<Long, TTablet> backendTablets = new HashMap<Long, TTablet>();
        List<TTabletInfo> tabletInfos = Lists.newArrayList();
        TTablet tablet = new TTablet(tabletInfos);
        for (Long tabletId : tabletIds) {
            TTabletInfo tabletInfo = new TTabletInfo();
            tabletInfo.setTablet_id(tabletId);
            tabletInfo.setSchema_hash(60000);
            tabletInfo.setBinlog_config_version(-1);
            tablet.tablet_infos.add(tabletInfo);
        }
        backendTablets.put(backendId, tablet);

        ReportHandler handler = new ReportHandler();
        handler.testHandleSetTabletBinlogConfig(backendId, backendTablets);

        for (Long tabletId : tabletIds) {
            TTabletInfo tabletInfo = new TTabletInfo();
            tabletInfo.setTablet_id(tabletId);
            tabletInfo.setSchema_hash(60000);
            tabletInfo.setBinlog_config_version(0);
            tablet.tablet_infos.add(tabletInfo);
        }
        backendTablets.put(backendId, tablet);

        handler.testHandleSetTabletBinlogConfig(backendId, backendTablets);
        Assert.assertTrue(GlobalStateMgr.getCurrentState().getBinlogManager().isBinlogAvailable(dbId, olapTable.getId()));

    }

    private TResourceUsage genResourceUsage(int numRunningQueries, long memLimitBytes, long memUsedBytes,
                                            int cpuUsedPermille) {
        TResourceUsage usage = new TResourceUsage();
        usage.setNum_running_queries(numRunningQueries);
        usage.setMem_limit_bytes(memLimitBytes);
        usage.setMem_used_bytes(memUsedBytes);
        usage.setCpu_used_permille(cpuUsedPermille);
        return usage;
    }

    @Test
    public void testHandleResourceUsageReport() {
        QueryQueueManager queryQueueManager = QueryQueueManager.getInstance();

        Backend backend = new Backend(0, "127.0.0.1", 80);
        ComputeNode computeNode = new ComputeNode(2, "127.0.0.1", 88);

        new MockUp<SystemInfoService>() {
            @Mock
            public ComputeNode getBackendOrComputeNode(long id) {
                if (id == backend.getId()) {
                    return backend;
                }
                if (id == computeNode.getId()) {
                    return computeNode;
                }
                return null;
            }
        };

        new Expectations(queryQueueManager) {
            {
                queryQueueManager.maybeNotifyAfterLock();
                times = 2;
            }
        };

        int numRunningQueries = 1;
        long memLimitBytes = 3;
        long memUsedBytes = 2;
        int cpuUsedPermille = 300;
        TResourceUsage resourceUsage = genResourceUsage(numRunningQueries, memLimitBytes, memUsedBytes, cpuUsedPermille);

        // For backend, sync to FE followers and notify pending queries.
        ReportHandler.testHandleResourceUsageReport(backend.getId(), resourceUsage);
        Assert.assertEquals(numRunningQueries, backend.getNumRunningQueries());
        Assert.assertEquals(memLimitBytes, backend.getMemLimitBytes());
        Assert.assertEquals(memUsedBytes, backend.getMemUsedBytes());
        Assert.assertEquals(cpuUsedPermille, backend.getCpuUsedPermille());

        // For compute node, sync to FE followers and notify pending queries.
        numRunningQueries = 10;
        memLimitBytes = 30;
        memUsedBytes = 20;
        cpuUsedPermille = 310;
        resourceUsage = genResourceUsage(numRunningQueries, memLimitBytes, memUsedBytes, cpuUsedPermille);
        ReportHandler.testHandleResourceUsageReport(computeNode.getId(), resourceUsage);
        Assert.assertEquals(numRunningQueries, computeNode.getNumRunningQueries());
        Assert.assertEquals(memLimitBytes, computeNode.getMemLimitBytes());
        Assert.assertEquals(memUsedBytes, computeNode.getMemUsedBytes());
        Assert.assertEquals(cpuUsedPermille, computeNode.getCpuUsedPermille());

        // Don't sync and notify, because this BE doesn't exist.
        ReportHandler.testHandleResourceUsageReport(/* Not Exist */ 1, resourceUsage);
    }
    
    @Test
    public void testHandleReport() throws TException {
        Backend be = new Backend(10001, "host1", 8000);
        ComputeNode cn = new ComputeNode(10002, "host2", 8000);

        new MockUp<SystemInfoService>() {
            @Mock
            public Backend getBackendWithBePort(String host, int bePort) {
                if (host.equals(be.getHost()) && bePort == be.getBePort()) {
                    return be;
                }
                return null;
            }

            @Mock
            public ComputeNode getComputeNodeWithBePort(String host, int bePort) {
                if (host.equals(cn.getHost()) && bePort == cn.getBePort()) {
                    return cn;
                }
                return null;
            }
        };


        ReportHandler handler = new ReportHandler();
        TResourceUsage resourceUsage = genResourceUsage(1, 2L, 3L, 100);

        {

            TReportRequest req = new TReportRequest();
            req.setResource_usage(resourceUsage);

            TBackend tcn = new TBackend();
            tcn.setHost(cn.getHost());
            tcn.setBe_port(cn.getBePort());
            req.setBackend(tcn);

            TMasterResult res = handler.handleReport(req);
            Assert.assertEquals(TStatusCode.OK, res.getStatus().getStatus_code());
        }

        {

            TReportRequest req = new TReportRequest();
            req.setResource_usage(resourceUsage);

            TBackend tbe = new TBackend();
            tbe.setHost(be.getHost());
            tbe.setBe_port(be.getBePort());
            req.setBackend(tbe);

            TMasterResult res = handler.handleReport(req);
            Assert.assertEquals(TStatusCode.OK, res.getStatus().getStatus_code());
        }

        {

            TReportRequest req = new TReportRequest();

            TBackend tcn = new TBackend();
            tcn.setHost(cn.getHost() + "NotExist");
            tcn.setBe_port(cn.getBePort());
            req.setBackend(tcn);

            TMasterResult res = handler.handleReport(req);
            Assert.assertEquals(TStatusCode.INTERNAL_ERROR, res.getStatus().getStatus_code());
        }
    }
}