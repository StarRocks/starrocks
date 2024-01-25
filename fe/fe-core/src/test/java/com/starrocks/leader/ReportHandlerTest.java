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

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.LocalTablet;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Replica;
import com.starrocks.catalog.Tablet;
import com.starrocks.catalog.TabletInvertedIndex;
import com.starrocks.catalog.TabletMeta;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.scheduler.slot.ResourceUsageMonitor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.Backend;
import com.starrocks.system.Backend.BackendStatus;
import com.starrocks.system.ComputeNode;
import com.starrocks.system.SystemInfoService;
import com.starrocks.task.AgentBatchTask;
import com.starrocks.task.AgentTaskExecutor;
import com.starrocks.task.AgentTaskQueue;
import com.starrocks.thrift.TBackend;
import com.starrocks.thrift.TMasterResult;
import com.starrocks.thrift.TReportRequest;
import com.starrocks.thrift.TResourceUsage;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.thrift.TStorageMedium;
import com.starrocks.thrift.TTablet;
import com.starrocks.thrift.TTabletInfo;
import com.starrocks.thrift.TTaskType;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import org.apache.thrift.TException;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ReportHandlerTest {
    private static ConnectContext connectContext;

    @BeforeClass
    public static void beforeClass() throws Exception {
        Config.alter_scheduler_interval_millisecond = 1000;
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
                        "duplicate key(k1) distributed by hash(k1) buckets 50 properties('replication_num' = '1', " +
                        "'binlog_enable' = 'true', 'binlog_max_size' = '100');")
                .withTable("CREATE TABLE test.primary_index_cache_expire_sec_test(k1 int, v1 int) " +
                        "primary key(k1) distributed by hash(k1) buckets 5 properties('replication_num' = '1', " +
                        "'primary_index_cache_expire_sec' = '3600');");
    }

    @Test
    public void testHandleSetTabletEnablePersistentIndex() {
        Database db = GlobalStateMgr.getCurrentState().getDb("test");
        long dbId = db.getId();
        long backendId = 10001L;
        List<Long> tabletIds = GlobalStateMgr.getCurrentState().getTabletInvertedIndex().getTabletIdsByBackendId(10001);
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
    public void testHandleSetPrimaryIndexCacheExpireSec() {
        Database db = GlobalStateMgr.getCurrentState().getDb("test");
        long dbId = db.getId();
        OlapTable olapTable = (OlapTable) db.getTable("primary_index_cache_expire_sec_test");
        long backendId = 10001L;
        List<Long> tabletIds = GlobalStateMgr.getCurrentState().getTabletInvertedIndex().getTabletIdsByBackendId(10001);
        Assert.assertFalse(tabletIds.isEmpty());

        Map<Long, TTablet> backendTablets = new HashMap<Long, TTablet>();
        List<TTabletInfo> tabletInfos = Lists.newArrayList();
        TTablet tablet = new TTablet(tabletInfos);
        for (Long tabletId : tabletIds) {
            TTabletInfo tabletInfo = new TTabletInfo();
            tabletInfo.setTablet_id(tabletId);
            tabletInfo.setSchema_hash(60000);
            tabletInfo.setPrimary_index_cache_expire_sec(7200);
            tablet.tablet_infos.add(tabletInfo);
        }
        backendTablets.put(backendId, tablet);

        ReportHandler handler = new ReportHandler();
        handler.testHandleSetPrimaryIndexCacheExpireSec(backendId, backendTablets);
    }

    @Test
    public void testHandleSetTabletBinlogConfig() {
        Database db = GlobalStateMgr.getCurrentState().getDb("test");
        long dbId = db.getId();
        OlapTable olapTable = (OlapTable) db.getTable("binlog_report_handler_test");
        long backendId = 10001L;
        List<Long> tabletIds = GlobalStateMgr.getCurrentState().getTabletInvertedIndex().getTabletIdsByBackendId(10001);
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
        ResourceUsageMonitor resourceUsageMonitor = GlobalStateMgr.getCurrentState().getResourceUsageMonitor();

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

        new Expectations(resourceUsageMonitor) {
            {
                resourceUsageMonitor.notifyResourceUsageUpdate();
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

    @Test
    public void testHandleMigration() throws TException {
        List<Long> tabletIds = GlobalStateMgr.getCurrentState().getTabletInvertedIndex().getTabletIdsByBackendId(10001);
        ListMultimap<TStorageMedium, Long> tabletMetaMigrationMap = ArrayListMultimap.create();
        for (Long tabletId : tabletIds) {
            tabletMetaMigrationMap.put(TStorageMedium.SSD, tabletId);
        }
        ReportHandler.handleMigration(tabletMetaMigrationMap, 10001);

        final SystemInfoService currentSystemInfo = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo();
        Backend reportBackend = currentSystemInfo.getBackend(10001);
        BackendStatus backendStatus = reportBackend.getBackendStatus();
        backendStatus.lastSuccessReportTabletsTime = TimeUtils.longToTimeString(Long.MAX_VALUE);

        ReportHandler.handleMigration(tabletMetaMigrationMap, 10001);

        TabletInvertedIndex invertedIndex = GlobalStateMgr.getCurrentState().getTabletInvertedIndex();
        List<TabletMeta> tabletMetaList = invertedIndex.getTabletMetaList(tabletIds);
        for (int i = 0; i < tabletMetaList.size(); i++) {
            long tabletId = tabletIds.get(i);
            TabletMeta tabletMeta = tabletMetaList.get(i);
            Database db = GlobalStateMgr.getCurrentState().getDb("test");
            if (db == null) {
                continue;
            }
            OlapTable table = null;
            db.readLock();
            try {
                table = (OlapTable) db.getTable(tabletMeta.getTableId());
            } finally {
                db.readUnlock();
            }

            Partition partition = table.getPartition(tabletMeta.getPartitionId());
            MaterializedIndex idx = partition.getIndex(tabletMeta.getIndexId());
            LocalTablet tablet = (LocalTablet) idx.getTablet(tabletId);


            for (Replica replica : tablet.getImmutableReplicas()) {
                replica.setMaxRowsetCreationTime(System.currentTimeMillis() / 1000);
            }
        }
        Config.tablet_sched_max_migration_task_sent_once = 1000000;
        Config.primary_key_disk_schedule_time = 0;
        ReportHandler.handleMigration(tabletMetaMigrationMap, 10001);
    }

    @Test
    public void testHandleMigrationTaskControl() {
        long backendId = 10001L;
        // mock the task execution on BE
        new MockUp<AgentTaskExecutor>() {
            @Mock
            public void submit(AgentBatchTask task) {

            }
        };

        OlapTable olapTable = (OlapTable) GlobalStateMgr.getCurrentState()
                .getDb("test").getTable("binlog_report_handler_test");
        ListMultimap<TStorageMedium, Long> tabletMetaMigrationMap = ArrayListMultimap.create();
        List<Long> allTablets = new ArrayList<>();
        for (MaterializedIndex index : olapTable.getPartition("binlog_report_handler_test")
                .getMaterializedIndices(MaterializedIndex.IndexExtState.ALL)) {
            for (Tablet tablet : index.getTablets()) {
                tabletMetaMigrationMap.put(TStorageMedium.HDD, tablet.getId());
                allTablets.add(tablet.getId());
            }
        }

        Assert.assertEquals(50, tabletMetaMigrationMap.size());

        ReportHandler.handleMigration(tabletMetaMigrationMap, backendId);

        Assert.assertEquals(50, AgentTaskQueue.getTaskNum(backendId, TTaskType.STORAGE_MEDIUM_MIGRATE, false));

        // finish 30 tablets migration
        for (int i = 0; i < 30; i++) {
            AgentTaskQueue.removeTask(backendId, TTaskType.STORAGE_MEDIUM_MIGRATE, allTablets.get(49 - i));
        }
        // limit the batch size to 30
        Config.tablet_sched_max_migration_task_sent_once = 30;
        ReportHandler.handleMigration(tabletMetaMigrationMap, backendId);
        Assert.assertEquals(30, AgentTaskQueue.getTaskNum(backendId, TTaskType.STORAGE_MEDIUM_MIGRATE, false));
    }

    @Test
    public void testTabletDropDelay() throws InterruptedException {
        long tabletId = 100001;
        long backendId = 100002;
        Config.tablet_report_drop_tablet_delay_sec = 3;

        boolean ready = ReportHandler.checkReadyToBeDropped(tabletId, backendId);
        Assert.assertFalse(ready);

        Thread.sleep(1000);
        ready = ReportHandler.checkReadyToBeDropped(tabletId, backendId);
        Assert.assertFalse(ready);

        Thread.sleep(3000);
        ready = ReportHandler.checkReadyToBeDropped(tabletId, backendId);
        Assert.assertTrue(ready);

        // check map is cleaned
        ready = ReportHandler.checkReadyToBeDropped(tabletId, backendId);
        Assert.assertFalse(ready);

        Thread.sleep(4000);
        ready = ReportHandler.checkReadyToBeDropped(tabletId, backendId);
        Assert.assertTrue(ready);
    }
}