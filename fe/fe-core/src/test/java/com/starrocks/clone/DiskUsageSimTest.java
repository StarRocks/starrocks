// This file is licensed under the Elastic License 2.0. Copyright 2021-present StarRocks Inc.
package com.starrocks.clone;

import com.starrocks.common.Config;
import com.starrocks.pseudocluster.PseudoBackend;
import com.starrocks.pseudocluster.PseudoCluster;
import com.starrocks.pseudocluster.PseudoClusterUtils;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.SystemInfoService;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.SQLException;

public class DiskUsageSimTest {
    private final long GB = (1L << 30);
    @BeforeClass
    public static void setUp() throws Exception {
        // set some parameters to speedup test
        Config.tablet_sched_checker_interval_seconds = 1;
        Config.tablet_sched_repair_delay_factor_second = 1;
        Config.enable_new_publish_mechanism = true;
        PseudoCluster.getOrCreateWithRandomPort(true, 3);
        GlobalStateMgr.getCurrentState().getTabletChecker().setInterval(1000);
        PseudoCluster cluster = PseudoCluster.getInstance();
        cluster.runSql(null, "create database test");
        cluster.runSql("test",
                "create table test ( pk bigint NOT NULL, v0 string not null, v1 int not null )" +
                        " primary KEY (pk) DISTRIBUTED BY HASH(pk) BUCKETS 1 " +
                        "PROPERTIES(\"replication_num\" = \"3\", \"storage_medium\" = \"SSD\");");
    }

    @AfterClass
    public static void tearDown() throws Exception {
        PseudoCluster.getInstance().shutdown(true);
    }


    @Test
    public void testSetInitialDiskCapacity() throws InterruptedException {
        PseudoCluster cluster = PseudoCluster.getInstance();
        SystemInfoService systemInfoService = GlobalStateMgr.getCurrentSystemInfo();
        // test default value
        Assert.assertEquals(PseudoBackend.DEFAULT_TOTA_CAP_B,
                systemInfoService.getBackend(10001).getTotalCapacityB());
        PseudoBackend backend1 = cluster.getBackend(10001);
        backend1.setInitialCapacity(10 * GB, 8 * GB, 2 * GB);
        PseudoBackend backend2 = cluster.getBackend(10002);
        backend2.setInitialCapacity(20 * GB, 8 * GB, 6 * GB);
        // wait for the disk info to be reported
        // TODO: sleep based on config when disk report interval is configurable
        Thread.sleep(10000);
        Assert.assertEquals(10 * GB,
                systemInfoService.getBackend(10001).getTotalCapacityB());
        Assert.assertEquals(20 * GB,
                systemInfoService.getBackend(10002).getTotalCapacityB());
        Assert.assertEquals(8 * GB + 1,
                systemInfoService.getBackend(10001).getAvailableCapacityB());
        Assert.assertEquals(2 * GB,
                systemInfoService.getBackend(10001).getDataUsedCapacityB());
    }

    @Test
    public void testDiskUsageAfterWrite() throws SQLException, InterruptedException {
        PseudoCluster cluster = PseudoCluster.getInstance();
        PseudoBackend be = cluster.getBackend(10001);
        be.setInitialCapacity(10 * GB, 8 * GB, 2 * GB);
        cluster.runSql("test", "insert into test values (1,\"1\", 1), (2,\"2\",2), (3,\"3\",3);");
        Thread.sleep(10000);
        SystemInfoService systemInfoService = GlobalStateMgr.getCurrentSystemInfo();
        Assert.assertEquals(2 * GB + PseudoBackend.DEFAULT_CHUNK_SIZE_ON_DISK_B,
                systemInfoService.getBackend(10001).getDataUsedCapacityB());
        Assert.assertEquals(8 * GB - PseudoBackend.DEFAULT_CHUNK_SIZE_ON_DISK_B + 1,
                systemInfoService.getBackend(10001).getAvailableCapacityB());
    }

    @Test
    public void testDiskUsageAfterIncrementalClone() throws SQLException, InterruptedException {
        PseudoCluster cluster = PseudoCluster.getInstance();
        PseudoBackend backend1 = cluster.getBackend(10001);
        backend1.setInitialCapacity(10 * GB, 8 * GB, 2 * GB);
        // this will commit 2 transactions
        PseudoClusterUtils.triggerIncrementalCloneOnce(cluster, 10001);
        // TODO: sleep based on config when disk report interval is configurable
        Thread.sleep(10000);
        SystemInfoService systemInfoService = GlobalStateMgr.getCurrentSystemInfo();
        System.out.println(systemInfoService.getBackend(10001).getDataUsedCapacityB());
        Assert.assertEquals(2 * GB + PseudoBackend.DEFAULT_CHUNK_SIZE_ON_DISK_B * 2,
                systemInfoService.getBackend(10001).getDataUsedCapacityB());
        Assert.assertEquals(8 * GB - PseudoBackend.DEFAULT_CHUNK_SIZE_ON_DISK_B * 2 + 1,
                systemInfoService.getBackend(10001).getAvailableCapacityB());
    }
}
