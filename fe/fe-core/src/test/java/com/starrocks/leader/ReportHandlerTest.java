// This file is made available under Elastic License 2.0.

package com.starrocks.leader;

import com.google.common.collect.Lists;
import com.starrocks.catalog.Database;
import com.starrocks.common.FeConstants;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.QueryQueueManager;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.Backend;
import com.starrocks.system.SystemInfoService;
import com.starrocks.thrift.TResourceUsage;
import com.starrocks.thrift.TTablet;
import com.starrocks.thrift.TTabletInfo;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
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
                        "primary key(k1) distributed by hash(k1) properties('replication_num' = '1');");
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

    private TResourceUsage genResourceUsage(int numRunningQueries, long memLimitBytes, long memUsedBytes) {
        TResourceUsage usage = new TResourceUsage();
        usage.setNum_running_queries(numRunningQueries);
        usage.setMem_limit_bytes(memLimitBytes);
        usage.setMem_used_bytes(memUsedBytes);
        return usage;
    }

    @Test
    public void testHandleResourceUsageReport() {
        QueryQueueManager queryQueueManager = QueryQueueManager.getInstance();
        SystemInfoService systemInfoService = GlobalStateMgr.getCurrentSystemInfo();
        Backend backend = new Backend();
        long backendId = 0;
        int numRunningQueries = 1;
        long memLimitBytes = 3;
        long memUsedBytes = 2;
        new Expectations(systemInfoService, queryQueueManager) {
            {
                queryQueueManager.maybeNotifyAfterLock();
                times = 1;
            }

            {
                systemInfoService.getBackend(backendId);
                result = backend;
            }
            {
                systemInfoService.getBackend(anyLong);
                result = null;
            }
        };

        TResourceUsage resourceUsage = genResourceUsage(numRunningQueries, memLimitBytes, memUsedBytes);
        // Sync to FE followers and notify pending queries, because resource usage is changed.
        ReportHandler.testHandleResourceUsageReport(backendId, resourceUsage);
        Assert.assertEquals(numRunningQueries, backend.getNumRunningQueries());
        Assert.assertEquals(memLimitBytes, backend.getMemLimitBytes());
        Assert.assertEquals(memUsedBytes, backend.getMemUsedBytes());
        // Don't sync and notify, because resource usage isn't changed.
        ReportHandler.testHandleResourceUsageReport(backendId, resourceUsage);
        // Don't sync and notify, because this BE doesn't exist.
        ReportHandler.testHandleResourceUsageReport(/* Not Exist */ 1, resourceUsage);
    }
}