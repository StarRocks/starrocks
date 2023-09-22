// This file is made available under Elastic License 2.0.

package com.starrocks.leader;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Tablet;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.QueryQueueManager;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.Backend;
import com.starrocks.system.SystemInfoService;
import com.starrocks.task.AgentBatchTask;
import com.starrocks.task.AgentTaskExecutor;
import com.starrocks.task.AgentTaskQueue;
import com.starrocks.thrift.TResourceUsage;
import com.starrocks.thrift.TStorageMedium;
import com.starrocks.thrift.TTablet;
import com.starrocks.thrift.TTabletInfo;
import com.starrocks.thrift.TTaskType;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
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
                        "duplicate key(k1) distributed by hash(k1) buckets 50 properties('replication_num' = '1');");
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
        Backend backend = new Backend();
        long backendId = 0;
        int numRunningQueries = 1;
        long memLimitBytes = 3;
        long memUsedBytes = 2;
        int cpuUsedPermille = 300;

        new MockUp<SystemInfoService>() {
            @Mock
            public Backend getBackend(long id) {
                if (id == backendId) {
                    return backend;
                }
                return null;
            }
        };

        new Expectations(queryQueueManager) {
            {
                queryQueueManager.maybeNotifyAfterLock();
                times = 1;
            }
        };

        TResourceUsage resourceUsage = genResourceUsage(numRunningQueries, memLimitBytes, memUsedBytes, cpuUsedPermille);
        // Sync to FE followers and notify pending queries.
        ReportHandler.testHandleResourceUsageReport(backendId, resourceUsage);
        Assert.assertEquals(numRunningQueries, backend.getNumRunningQueries());
        Assert.assertEquals(memLimitBytes, backend.getMemLimitBytes());
        Assert.assertEquals(memUsedBytes, backend.getMemUsedBytes());
        Assert.assertEquals(cpuUsedPermille, backend.getCpuUsedPermille());
        // Don't sync and notify, because this BE doesn't exist.
        ReportHandler.testHandleResourceUsageReport(/* Not Exist */ 1, resourceUsage);
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
        ListMultimap<TStorageMedium, Long> tabletMetaMigrationMap = ArrayListMultimap.create();;
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
}