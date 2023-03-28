// This file is made available under Elastic License 2.0.

package com.starrocks.leader;

import com.google.common.collect.Lists;
import com.starrocks.catalog.Database;
import com.starrocks.common.FeConstants;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.QueryQueueManager;
import com.starrocks.server.GlobalStateMgr;
<<<<<<< HEAD
import com.starrocks.system.Backend;
=======
import com.starrocks.system.ComputeNode;
import com.starrocks.system.DataNode;
>>>>>>> 52bd9f3d1 ([Refactor]Rename Backend  Class to DataNode (#20438))
import com.starrocks.system.SystemInfoService;
import com.starrocks.thrift.TResourceUsage;
import com.starrocks.thrift.TTablet;
import com.starrocks.thrift.TTabletInfo;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
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
<<<<<<< HEAD
        Backend backend = new Backend();
        long backendId = 0;
        int numRunningQueries = 1;
        long memLimitBytes = 3;
        long memUsedBytes = 2;
        int cpuUsedPermille = 300;
=======

        DataNode backend = new DataNode(0, "127.0.0.1", 80);
        ComputeNode computeNode = new ComputeNode(2, "127.0.0.1", 88);
>>>>>>> 52bd9f3d1 ([Refactor]Rename Backend  Class to DataNode (#20438))

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
<<<<<<< HEAD
=======
    
    @Test
    public void testHandleReport() throws TException {
        DataNode be = new DataNode(10001, "host1", 8000);
        ComputeNode cn = new ComputeNode(10002, "host2", 8000);

        new MockUp<SystemInfoService>() {
            @Mock
            public DataNode getBackendWithBePort(String host, int bePort) {
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
>>>>>>> 52bd9f3d1 ([Refactor]Rename Backend  Class to DataNode (#20438))
}