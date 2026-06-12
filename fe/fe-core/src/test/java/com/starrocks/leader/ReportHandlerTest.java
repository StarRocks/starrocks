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
import com.starrocks.alter.SchemaChangeHandler;
import com.starrocks.catalog.ColocateTableIndex;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.FlatJsonConfig;
import com.starrocks.catalog.LocalTablet;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.OlapTable.OlapTableState;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Replica;
import com.starrocks.catalog.Replica.ReplicaState;
import com.starrocks.catalog.Tablet;
import com.starrocks.catalog.TabletInvertedIndex;
import com.starrocks.catalog.TabletMeta;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.scheduler.slot.ResourceUsageMonitor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.AlterTableStmt;
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
import com.starrocks.thrift.TTabletMetaType;
import com.starrocks.thrift.TTaskType;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import org.apache.thrift.TException;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class ReportHandlerTest {
    private static ConnectContext connectContext;

    @BeforeAll
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
                                "'primary_index_cache_expire_sec' = '3600');")
                    .withTable("CREATE TABLE test.update_schema(k1 int, v1 int) " +
                                "primary key(k1) distributed by hash(k1) buckets 5 properties('replication_num' = '1', " +
                                "'primary_index_cache_expire_sec' = '3600');")
                    .withTable("CREATE TABLE test.flat_json_report_handler_test(k1 int, v1 int) " +
                                "duplicate key(k1) distributed by hash(k1) buckets 5 properties('replication_num' = '1', " +
                                "'flat_json.enable' = 'true');");
    }

    @Test
    public void testHandleSetTabletEnablePersistentIndex() {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        long dbId = db.getId();
        long backendId = 10001L;
        List<Long> tabletIds = GlobalStateMgr.getCurrentState().getTabletInvertedIndex().getTabletIdsByBackendId(10001);
        Assertions.assertFalse(tabletIds.isEmpty());

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
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        long dbId = db.getId();
        OlapTable olapTable = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                    .getTable(db.getFullName(), "primary_index_cache_expire_sec_test");
        long backendId = 10001L;
        List<Long> tabletIds = GlobalStateMgr.getCurrentState().getTabletInvertedIndex().getTabletIdsByBackendId(10001);
        Assertions.assertFalse(tabletIds.isEmpty());

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
    public void testHandleUpdateTableSchema() throws Exception {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        long dbId = db.getId();
        OlapTable olapTable =
                    (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), "update_schema");

        String stmt = "alter table update_schema add column add_v int default '1'";
        StarRocksAssert starRocksAssert = new StarRocksAssert(connectContext);
        AlterTableStmt alterTableStmt =
                    (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(stmt, starRocksAssert.getCtx());
        SchemaChangeHandler schemaChangeHandler = GlobalStateMgr.getCurrentState().getSchemaChangeHandler();
        schemaChangeHandler.process(alterTableStmt.getAlterClauseList(), db, olapTable);
        Assertions.assertEquals(OlapTableState.NORMAL, olapTable.getState());

        long backendId = 10001L;
        List<Long> tabletIds =
                    GlobalStateMgr.getCurrentState().getTabletInvertedIndex().getTabletIdsByBackendId(10001);
        Assertions.assertFalse(tabletIds.isEmpty());

        Map<Long, TTablet> backendTablets = new HashMap<Long, TTablet>();
        List<TTabletInfo> tabletInfos = Lists.newArrayList();
        TTablet tablet = new TTablet(tabletInfos);
        for (Long tabletId : tabletIds) {
            TTabletInfo tabletInfo = new TTabletInfo();
            tabletInfo.setTablet_id(tabletId);
            tabletInfo.setSchema_hash(60000);
            tabletInfo.setTablet_schema_version(0);
            tablet.tablet_infos.add(tabletInfo);
        }
        backendTablets.put(backendId, tablet);

        ReportHandler handler = new ReportHandler();
        handler.testHandleUpdateTableSchema(backendId, backendTablets);
    }

    @Test
    public void testHandleSetTabletBinlogConfig() {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        long dbId = db.getId();
        OlapTable olapTable = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                    .getTable(db.getFullName(), "binlog_report_handler_test");
        long backendId = 10001L;
        List<Long> tabletIds = GlobalStateMgr.getCurrentState().getTabletInvertedIndex().getTabletIdsByBackendId(10001);
        Assertions.assertFalse(tabletIds.isEmpty());

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
        Assertions.assertTrue(GlobalStateMgr.getCurrentState().getBinlogManager().isBinlogAvailable(dbId, olapTable.getId()));

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
    public void testHandleResourceUsageReport() throws TException {
        ResourceUsageMonitor resourceUsageMonitor = GlobalStateMgr.getCurrentState().getResourceUsageMonitor();

        Backend backend = new Backend(0, "127.0.0.1", 80);
        backend.setBePort(90);
        ComputeNode computeNode = new ComputeNode(2, "127.0.0.1", 88);
        computeNode.setBePort(99);

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

        new MockUp<SystemInfoService>() {
            @Mock
            public Backend getBackendWithBePort(String host, int bePort) {
                if (host.equals(backend.getHost()) && bePort == backend.getBePort()) {
                    return backend;
                }
                return null;
            }

            @Mock
            public ComputeNode getComputeNodeWithBePort(String host, int bePort) {
                if (host.equals(computeNode.getHost()) && bePort == computeNode.getBePort()) {
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

        ReportHandler handler = new ReportHandler();
        handler.start();

        {
            int numRunningQueries = 1;
            long memLimitBytes = 3;
            long memUsedBytes = 2;
            int cpuUsedPermille = 300;
            TResourceUsage resourceUsage = genResourceUsage(numRunningQueries, memLimitBytes, memUsedBytes, cpuUsedPermille);
            handler.handleReport(genRequest(resourceUsage, backend));

            Awaitility.await().atMost(5, TimeUnit.SECONDS).until(() -> numRunningQueries == backend.getNumRunningQueries());
            // For backend, sync to FE followers and notify pending queries.
            Assertions.assertEquals(memUsedBytes, backend.getMemUsedBytes());
            Assertions.assertEquals(cpuUsedPermille, backend.getCpuUsedPermille());
        }

        {
            // For compute node, sync to FE followers and notify pending queries.
            int numRunningQueries = 10;
            int memLimitBytes = 30;
            int memUsedBytes = 20;
            int cpuUsedPermille = 310;
            TResourceUsage resourceUsage = genResourceUsage(numRunningQueries, memLimitBytes, memUsedBytes, cpuUsedPermille);
            handler.handleReport(genRequest(resourceUsage, computeNode));

            Awaitility.await().atMost(5, TimeUnit.SECONDS).until(() -> numRunningQueries == computeNode.getNumRunningQueries());
            Assertions.assertEquals(memUsedBytes, computeNode.getMemUsedBytes());
            Assertions.assertEquals(cpuUsedPermille, computeNode.getCpuUsedPermille());

            // Don't sync and notify, because this BE doesn't exist.
            ComputeNode nonExistCN = new ComputeNode(2, "127.10.0.1", 100);
            nonExistCN.setBePort(199);
            handler.handleReport(genRequest(resourceUsage, nonExistCN));
        }

        handler.setStop();
    }

    private TReportRequest genRequest(TResourceUsage resourceUsage, ComputeNode worker) {
        TReportRequest req = new TReportRequest();
        req.setResource_usage(resourceUsage);

        TBackend tcn = new TBackend();
        tcn.setHost(worker.getHost());
        tcn.setBe_port(worker.getBePort());
        req.setBackend(tcn);

        return req;
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
            Assertions.assertEquals(TStatusCode.OK, res.getStatus().getStatus_code());
        }

        {

            TReportRequest req = new TReportRequest();
            req.setResource_usage(resourceUsage);

            TBackend tbe = new TBackend();
            tbe.setHost(be.getHost());
            tbe.setBe_port(be.getBePort());
            req.setBackend(tbe);

            TMasterResult res = handler.handleReport(req);
            Assertions.assertEquals(TStatusCode.OK, res.getStatus().getStatus_code());
        }

        {

            TReportRequest req = new TReportRequest();

            TBackend tcn = new TBackend();
            tcn.setHost(cn.getHost() + "NotExist");
            tcn.setBe_port(cn.getBePort());
            req.setBackend(tcn);

            TMasterResult res = handler.handleReport(req);
            Assertions.assertEquals(TStatusCode.INTERNAL_ERROR, res.getStatus().getStatus_code());
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
        backendStatus.lastSuccessReportTabletsTimeMs = Long.MAX_VALUE;

        ReportHandler.handleMigration(tabletMetaMigrationMap, 10001);

        TabletInvertedIndex invertedIndex = GlobalStateMgr.getCurrentState().getTabletInvertedIndex();
        List<TabletMeta> tabletMetaList = invertedIndex.getTabletMetaList(tabletIds);
        for (int i = 0; i < tabletMetaList.size(); i++) {
            long tabletId = tabletIds.get(i);
            TabletMeta tabletMeta = tabletMetaList.get(i);
            Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
            if (db == null) {
                continue;
            }
            OlapTable table = null;
            Locker locker = new Locker();
            locker.lockDatabase(db.getId(), LockType.READ);
            try {
                table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                            .getTable(db.getId(), tabletMeta.getTableId());
            } finally {
                locker.unLockDatabase(db.getId(), LockType.READ);
            }

            PhysicalPartition partition = table.getPhysicalPartition(tabletMeta.getPhysicalPartitionId());
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
                    .getLocalMetastore().getDb("test").getTable("binlog_report_handler_test");
        ListMultimap<TStorageMedium, Long> tabletMetaMigrationMap = ArrayListMultimap.create();
        List<Long> allTablets = new ArrayList<>();
        for (MaterializedIndex index : olapTable.getPartition("binlog_report_handler_test")
                .getDefaultPhysicalPartition().getLatestMaterializedIndices(MaterializedIndex.IndexExtState.ALL)) {
            for (Tablet tablet : index.getTablets()) {
                tabletMetaMigrationMap.put(TStorageMedium.HDD, tablet.getId());
                allTablets.add(tablet.getId());
            }
        }

        Assertions.assertEquals(50, tabletMetaMigrationMap.size());

        ReportHandler.handleMigration(tabletMetaMigrationMap, backendId);

        Assertions.assertEquals(50, AgentTaskQueue.getTaskNum(backendId, TTaskType.STORAGE_MEDIUM_MIGRATE, false));

        // finish 30 tablets migration
        for (int i = 0; i < 30; i++) {
            AgentTaskQueue.removeTask(backendId, TTaskType.STORAGE_MEDIUM_MIGRATE, allTablets.get(49 - i));
        }
        // limit the batch size to 30
        Config.tablet_sched_max_migration_task_sent_once = 30;
        ReportHandler.handleMigration(tabletMetaMigrationMap, backendId);
        Assertions.assertEquals(30, AgentTaskQueue.getTaskNum(backendId, TTaskType.STORAGE_MEDIUM_MIGRATE, false));
    }

    @Test
    public void testTabletDropDelay() throws InterruptedException {
        long tabletId = 100001;
        long backendId = 100002;
        Config.tablet_report_drop_tablet_delay_sec = 3;

        boolean ready = ReportHandler.checkReadyToBeDropped(tabletId, backendId);
        Assertions.assertFalse(ready);

        Thread.sleep(1000);
        ready = ReportHandler.checkReadyToBeDropped(tabletId, backendId);
        Assertions.assertFalse(ready);

        Thread.sleep(3000);
        ready = ReportHandler.checkReadyToBeDropped(tabletId, backendId);
        Assertions.assertTrue(ready);

        // check map is cleaned
        ready = ReportHandler.checkReadyToBeDropped(tabletId, backendId);
        Assertions.assertFalse(ready);

        Thread.sleep(4000);
        ready = ReportHandler.checkReadyToBeDropped(tabletId, backendId);
        Assertions.assertTrue(ready);
    }

    @Test
    public void testGetPendingTabletReportTaskCnt() throws Exception {
        ReportHandler reportHandler = new ReportHandler();
        Assertions.assertEquals(0, reportHandler.getPendingTabletReportTaskCnt());
        reportHandler.putTabletReportTask(1L, 1L, new HashMap<>());
        Assertions.assertEquals(1, reportHandler.getPendingTabletReportTaskCnt());
        reportHandler.putTabletReportTask(1L, 1L, new HashMap<>());
        Assertions.assertEquals(1, reportHandler.getPendingTabletReportTaskCnt());
        reportHandler.putTabletReportTask(2L, 1L, new HashMap<>());
        Assertions.assertEquals(2, reportHandler.getPendingTabletReportTaskCnt());
    }

    @Test
    public void testOnStoppedDrainsQueuesAndClearsPendingTasks() throws Exception {
        ReportHandler reportHandler = new ReportHandler();
        reportHandler.putTabletReportTask(1L, 1L, new HashMap<>());
        reportHandler.putTabletReportTask(2L, 1L, new HashMap<>());
        Assertions.assertEquals(2, reportHandler.getPendingTabletReportTaskCnt());
        Assertions.assertTrue(reportHandler.getReportQueueSize() > 0);

        reportHandler.onStopped();

        Assertions.assertEquals(0, reportHandler.getPendingTabletReportTaskCnt(),
                "pending tablet report tasks must be cleared after onStopped");
        Assertions.assertEquals(0, reportHandler.getReportQueueSize(),
                "both report queues must be drained after onStopped");
    }

    @Test
    public void testPutTabletReportTaskThrowsAfterStop() {
        ReportHandler reportHandler = new ReportHandler();
        reportHandler.setStop();
        // Must surface as IllegalStateException so LeaderImpl.report() can translate to
        // NOT_MASTER; silent-drop would leave the BE thinking the report succeeded.
        IllegalStateException ex = Assertions.assertThrows(IllegalStateException.class,
                () -> reportHandler.putTabletReportTask(1L, 1L, new HashMap<>()));
        Assertions.assertTrue(ex.getMessage().contains("stopped"),
                "exception message should mention stop reason, got: " + ex.getMessage());
        Assertions.assertEquals(0, reportHandler.getPendingTabletReportTaskCnt());
        Assertions.assertEquals(0, reportHandler.getReportQueueSize());
    }

    private static OlapTable getFlatJsonTable() {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        return (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(db.getFullName(), "flat_json_report_handler_test");
    }

    private static List<Long> getTableTabletIds(OlapTable table) {
        List<Long> ids = new ArrayList<>();
        for (Partition partition : table.getPartitions()) {
            PhysicalPartition physPartition = partition.getDefaultPhysicalPartition();
            for (MaterializedIndex index :
                    physPartition.getLatestMaterializedIndices(MaterializedIndex.IndexExtState.ALL)) {
                for (Tablet tablet : index.getTablets()) {
                    ids.add(tablet.getId());
                }
            }
        }
        return ids;
    }

    private static Map<Long, TTablet> buildBackendTablets(long backendId, List<Long> tabletIds,
                                                          Consumer<TTabletInfo> setup) {
        List<TTabletInfo> infos = Lists.newArrayList();
        TTablet tablet = new TTablet(infos);
        for (Long id : tabletIds) {
            TTabletInfo info = new TTabletInfo();
            info.setTablet_id(id);
            info.setSchema_hash(60000);
            setup.accept(info);
            tablet.tablet_infos.add(info);
        }
        Map<Long, TTablet> backendTablets = new HashMap<>();
        backendTablets.put(backendId, tablet);
        return backendTablets;
    }

    private static List<AgentBatchTask> mockSubmitCapture() {
        List<AgentBatchTask> submitted = new ArrayList<>();
        new MockUp<AgentTaskExecutor>() {
            @Mock
            public void submit(AgentBatchTask task) {
                submitted.add(task);
            }
        };
        return submitted;
    }

    @Test
    public void testHandleSetTabletFlatJsonConfigStaleBe() {
        List<AgentBatchTask> submitted = mockSubmitCapture();
        List<Long> tabletIds = getTableTabletIds(getFlatJsonTable());
        Assertions.assertFalse(tabletIds.isEmpty());

        Map<Long, TTablet> backendTablets = buildBackendTablets(
                10001L, tabletIds, info -> info.setFlat_json_config_version(-1));

        ReportHandler handler = new ReportHandler();
        handler.testHandleSetTabletFlatJsonConfig(10001L, backendTablets);

        Assertions.assertFalse(submitted.isEmpty());
        Assertions.assertTrue(submitted.get(0).getTaskNum() > 0);
    }

    @Test
    public void testHandleSetTabletFlatJsonConfigOrphanBe() {
        // flat_json_config_version not reported by BE (isSet=false) is treated as 0,
        // so a BE that never received the config is reconciled on the next heartbeat.
        FlatJsonConfig config = getFlatJsonTable().getFlatJsonConfig();
        config.incVersion();
        try {
            List<AgentBatchTask> submitted = mockSubmitCapture();
            List<Long> tabletIds = getTableTabletIds(getFlatJsonTable());
            Assertions.assertFalse(tabletIds.isEmpty());

            Map<Long, TTablet> backendTablets = buildBackendTablets(10001L, tabletIds, info -> {});

            ReportHandler handler = new ReportHandler();
            handler.testHandleSetTabletFlatJsonConfig(10001L, backendTablets);

            Assertions.assertFalse(submitted.isEmpty());
        } finally {
            config.setVersion(0);
        }
    }

    @Test
    public void testHandleSetTabletFlatJsonConfigUpToDateBe() {
        List<AgentBatchTask> submitted = mockSubmitCapture();
        List<Long> tabletIds = getTableTabletIds(getFlatJsonTable());
        Assertions.assertFalse(tabletIds.isEmpty());

        Map<Long, TTablet> backendTablets = buildBackendTablets(
                10001L, tabletIds, info -> info.setFlat_json_config_version(0));

        ReportHandler handler = new ReportHandler();
        handler.testHandleSetTabletFlatJsonConfig(10001L, backendTablets);

        Assertions.assertTrue(submitted.isEmpty());
    }

    @Test
    public void testHandleSetTabletFlatJsonConfigAheadBe() {
        List<AgentBatchTask> submitted = mockSubmitCapture();
        List<Long> tabletIds = getTableTabletIds(getFlatJsonTable());
        Assertions.assertFalse(tabletIds.isEmpty());

        Map<Long, TTablet> backendTablets = buildBackendTablets(
                10001L, tabletIds, info -> info.setFlat_json_config_version(1));

        ReportHandler handler = new ReportHandler();
        handler.testHandleSetTabletFlatJsonConfig(10001L, backendTablets);

        Assertions.assertTrue(submitted.isEmpty());
    }

    @Test
    public void testHandleSetTabletFlatJsonConfigTableWithoutFlatJson() {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        OlapTable nonFlatJsonTable = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(db.getFullName(), "properties_change_test");
        List<Long> tabletIds = getTableTabletIds(nonFlatJsonTable);
        Assertions.assertFalse(tabletIds.isEmpty());

        List<AgentBatchTask> submitted = mockSubmitCapture();
        Map<Long, TTablet> backendTablets = buildBackendTablets(
                10001L, tabletIds, info -> info.setFlat_json_config_version(-1));

        ReportHandler handler = new ReportHandler();
        handler.testHandleSetTabletFlatJsonConfig(10001L, backendTablets);

        Assertions.assertTrue(submitted.isEmpty());
    }

    // Concurrent ALTER race: session-2 changes a factor (validated while enabled) while session-1
    // disables flat_json. Under the write lock the change is rebased onto the now-disabled config,
    // and the re-validation there must reject it so no disabled-with-factor state is persisted.
    @Test
    public void testConcurrentDisableRacesFactorChangeViaRebase() {
        SchemaChangeHandler sch = GlobalStateMgr.getCurrentState().getSchemaChangeHandler();
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        OlapTable table = getFlatJsonTable();

        // Restore the shared fixture's config afterwards.
        long origVersion = table.getFlatJsonConfig().getVersion();
        boolean origEnable = table.getFlatJsonConfig().getFlatJsonEnable();
        try {
            table.getFlatJsonConfig().setFlatJsonEnable(true);
            double racingFactor = 0.6;
            double defaultNullFactor = new FlatJsonConfig().getFlatJsonNullFactor();
            Assertions.assertNotEquals(racingFactor, defaultNullFactor);

            mockSubmitCapture();

            // session-1 disables flat_json (and bumps the version) when session-2 takes the write lock.
            final boolean[] injected = {false};
            new MockUp<Locker>() {
                @Mock
                public void lockTablesWithIntensiveDbLock(Long dbId, List<Long> tableList, LockType lockType) {
                    if (lockType == LockType.WRITE && !injected[0]) {
                        injected[0] = true;
                        FlatJsonConfig live = table.getFlatJsonConfig();
                        live.setFlatJsonEnable(false);
                        live.incVersion();
                    }
                }

                @Mock
                public void unLockTablesWithIntensiveDbLock(Long dbId, List<Long> tableList, LockType lockType) {
                }
            };

            // session-2 changes null.factor, validated while flat_json was still enabled.
            Map<String, String> props = new HashMap<>();
            props.put(PropertyAnalyzer.PROPERTIES_FLAT_JSON_NULL_FACTOR, String.valueOf(racingFactor));
            boolean ok = sch.updateFlatJsonConfigMeta(db, table.getId(), props, TTabletMetaType.FLAT_JSON_CONFIG);

            Assertions.assertTrue(injected[0]);
            FlatJsonConfig result = table.getFlatJsonConfig();
            Assertions.assertFalse(ok, "factor change rebased onto a disabled config must be rejected");
            Assertions.assertFalse(result.getFlatJsonEnable());
            Assertions.assertNotEquals(racingFactor, result.getFlatJsonNullFactor());
        } finally {
            table.getFlatJsonConfig().setFlatJsonEnable(origEnable);
            table.getFlatJsonConfig().setVersion(origVersion);
        }
    }

    private static final long VERSION_MISS_VISIBLE = 100L;
    private static final long HOLED_BACKEND_ID = 20001L;
    private static final long ALIVE_SOURCE_BACKEND_ID = 20002L;
    private static final long DEAD_SOURCE_BACKEND_ID = 20003L;

    private static boolean invokeNeedRecoverVersionMiss(OlapTable table, LocalTablet tablet, Replica holed,
                                                        TTabletInfo info) throws Exception {
        Method method = ReportHandler.class.getDeclaredMethod("needRecoverVersionMiss",
                OlapTable.class, LocalTablet.class, Replica.class, long.class, TTabletInfo.class);
        method.setAccessible(true);
        return (boolean) method.invoke(null, table, tablet, holed, VERSION_MISS_VISIBLE, info);
    }

    private static Replica versionMissReplica(long replicaId, long backendId, long lastReportVersion, boolean bad) {
        // version is masked high (>= visible) to mimic the bug; lastReportVersion carries the BE truth.
        Replica replica = new Replica(replicaId, backendId, 200L, 0, 0L, 0L, ReplicaState.NORMAL, -1L, 200L);
        replica.setLastReportVersion(lastReportVersion);
        if (bad) {
            replica.setBad(true);
        }
        return replica;
    }

    private static TTabletInfo versionMissInfo(long reportedVersion) {
        TTabletInfo info = new TTabletInfo();
        info.setVersion(reportedVersion);
        info.setVersion_miss(true);
        return info;
    }

    private void mockVersionMissBackends() {
        Backend alive1 = new Backend(HOLED_BACKEND_ID, "h1", 9050);
        alive1.setAlive(true);
        Backend alive2 = new Backend(ALIVE_SOURCE_BACKEND_ID, "h2", 9050);
        alive2.setAlive(true);
        Backend dead3 = new Backend(DEAD_SOURCE_BACKEND_ID, "h3", 9050);
        dead3.setAlive(false);
        Map<Long, Backend> backends = new HashMap<>();
        backends.put(HOLED_BACKEND_ID, alive1);
        backends.put(ALIVE_SOURCE_BACKEND_ID, alive2);
        backends.put(DEAD_SOURCE_BACKEND_ID, dead3);
        new MockUp<SystemInfoService>() {
            @Mock
            public Backend getBackend(long id) {
                return backends.get(id);
            }
        };
    }

    @Test
    public void testNeedRecoverVersionMiss() throws Exception {
        mockVersionMissBackends();
        OlapTable dupTable = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getDb("test").getTable("binlog_report_handler_test");
        OlapTable pkTable = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getDb("test").getTable("properties_change_test");

        Replica holed = versionMissReplica(1L, HOLED_BACKEND_ID, 50L, false);
        Replica caughtUpSource = versionMissReplica(2L, ALIVE_SOURCE_BACKEND_ID, VERSION_MISS_VISIBLE, false);
        LocalTablet tablet = new LocalTablet(1000L, Lists.newArrayList(holed, caughtUpSource));

        // Debounce: the first version_miss report only records the baseline (not flagged); the second
        // consecutive report at the same frozen continuous version (50 < visible) is flagged, given a
        // genuine caught-up source.
        Assertions.assertFalse(invokeNeedRecoverVersionMiss(dupTable, tablet, holed, versionMissInfo(50L)));
        Assertions.assertEquals(50L, holed.getVersionMissBaselineVersion());
        Assertions.assertTrue(invokeNeedRecoverVersionMiss(dupTable, tablet, holed, versionMissInfo(50L)));

        // A prior healthy report at the same version must NOT count as a confirmation: a healthy report
        // resets the baseline, so the first following version_miss report is not flagged on first sight.
        TTabletInfo healthyAt50 = new TTabletInfo();
        healthyAt50.setVersion(50L);
        healthyAt50.setVersion_miss(false);
        Assertions.assertFalse(invokeNeedRecoverVersionMiss(dupTable, tablet, holed, healthyAt50));
        Assertions.assertEquals(-1L, holed.getVersionMissBaselineVersion());
        Assertions.assertFalse(invokeNeedRecoverVersionMiss(dupTable, tablet, holed, versionMissInfo(50L)));

        // An advancing continuous version is not a frozen hole: a baseline of 40 does not confirm 50.
        holed.setVersionMissBaselineVersion(40L);
        Assertions.assertFalse(invokeNeedRecoverVersionMiss(dupTable, tablet, holed, versionMissInfo(50L)));

        // The remaining cases prime the baseline so the debounce passes and the specific guard is isolated.

        // reportedVersion >= visible -> replica still usable, not flagged.
        holed.setVersionMissBaselineVersion(100L);
        Assertions.assertFalse(invokeNeedRecoverVersionMiss(dupTable, tablet, holed, versionMissInfo(100L)));

        // used==false is handled by needRecover, not here.
        holed.setVersionMissBaselineVersion(50L);
        TTabletInfo usedFalse = versionMissInfo(50L);
        usedFalse.setUsed(false);
        Assertions.assertFalse(invokeNeedRecoverVersionMiss(dupTable, tablet, holed, usedFalse));

        // Already bad -> not flagged.
        Replica holedBad = versionMissReplica(3L, HOLED_BACKEND_ID, 50L, true);
        holedBad.setVersionMissBaselineVersion(50L);
        LocalTablet badTablet = new LocalTablet(1001L, Lists.newArrayList(holedBad, caughtUpSource));
        Assertions.assertFalse(invokeNeedRecoverVersionMiss(dupTable, badTablet, holedBad, versionMissInfo(50L)));

        // Primary-key table -> not flagged.
        holed.setVersionMissBaselineVersion(50L);
        Assertions.assertFalse(invokeNeedRecoverVersionMiss(pkTable, tablet, holed, versionMissInfo(50L)));

        // RF=1 (only the holed replica, no other source) -> not flagged.
        holed.setVersionMissBaselineVersion(50L);
        LocalTablet singleReplica = new LocalTablet(1002L, Lists.newArrayList(holed));
        Assertions.assertFalse(invokeNeedRecoverVersionMiss(dupTable, singleReplica, holed, versionMissInfo(50L)));

        // All replicas holed but FE-masked: the sibling's version is masked high but its lastReportVersion
        // (BE truth) is below visible -> no genuine source -> not flagged.
        holed.setVersionMissBaselineVersion(50L);
        Replica maskedSource = versionMissReplica(4L, ALIVE_SOURCE_BACKEND_ID, 50L, false);
        LocalTablet allHoled = new LocalTablet(1003L, Lists.newArrayList(holed, maskedSource));
        Assertions.assertFalse(invokeNeedRecoverVersionMiss(dupTable, allHoled, holed, versionMissInfo(50L)));

        // The only caught-up source is on a dead backend -> not flagged.
        holed.setVersionMissBaselineVersion(50L);
        Replica deadSource = versionMissReplica(5L, DEAD_SOURCE_BACKEND_ID, VERSION_MISS_VISIBLE, false);
        LocalTablet deadSourceTablet = new LocalTablet(1004L, Lists.newArrayList(holed, deadSource));
        Assertions.assertFalse(invokeNeedRecoverVersionMiss(dupTable, deadSourceTablet, holed, versionMissInfo(50L)));

        // An error-state sibling is not a usable source -> not flagged.
        holed.setVersionMissBaselineVersion(50L);
        Replica errorStateSource = versionMissReplica(6L, ALIVE_SOURCE_BACKEND_ID, VERSION_MISS_VISIBLE, false);
        errorStateSource.setIsErrorState(true);
        LocalTablet errorStateTablet = new LocalTablet(1006L, Lists.newArrayList(holed, errorStateSource));
        Assertions.assertFalse(invokeNeedRecoverVersionMiss(dupTable, errorStateTablet, holed, versionMissInfo(50L)));
    }

    @Test
    public void testNeedRecoverVersionMissSkipsColocate() throws Exception {
        new MockUp<ColocateTableIndex>() {
            @Mock
            public boolean isColocateTable(long tableId) {
                return true;
            }
        };
        mockVersionMissBackends();
        OlapTable dupTable = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getDb("test").getTable("binlog_report_handler_test");
        Replica holed = versionMissReplica(1L, HOLED_BACKEND_ID, 50L, false);
        holed.setVersionMissBaselineVersion(50L);
        Replica caughtUpSource = versionMissReplica(2L, ALIVE_SOURCE_BACKEND_ID, VERSION_MISS_VISIBLE, false);
        LocalTablet tablet = new LocalTablet(1005L, Lists.newArrayList(holed, caughtUpSource));
        // Colocate tables are excluded even when every other condition for recovery holds (baseline primed).
        Assertions.assertFalse(invokeNeedRecoverVersionMiss(dupTable, tablet, holed, versionMissInfo(50L)));
    }
}