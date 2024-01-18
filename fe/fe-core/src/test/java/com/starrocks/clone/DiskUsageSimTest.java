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

package com.starrocks.clone;

import com.starrocks.common.conf.Config;
import com.starrocks.pseudocluster.PseudoBackend;
import com.starrocks.pseudocluster.PseudoCluster;
import com.starrocks.pseudocluster.PseudoClusterUtils;
import com.starrocks.pseudocluster.Tablet;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.SystemInfoService;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.sql.SQLException;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class DiskUsageSimTest {
    private final long bytesOneGB = (1L << 30);
    @BeforeClass
    public static void setUp() throws Exception {
        // set some parameters to speedup test
        Config.tablet_sched_checker_interval_seconds = 1;
        Config.tablet_sched_repair_delay_factor_second = 1;
        Config.enable_new_publish_mechanism = true;
        PseudoBackend.reportIntervalMs = 2000;
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
    public void test1SetInitialDiskCapacity() throws InterruptedException {
        PseudoCluster cluster = PseudoCluster.getInstance();
        SystemInfoService systemInfoService = GlobalStateMgr.getCurrentSystemInfo();
        // test default value
        Assert.assertEquals(PseudoBackend.DEFAULT_TOTA_CAP_B,
                systemInfoService.getBackend(10001).getTotalCapacityB());
        PseudoBackend backend1 = cluster.getBackend(10001);
        backend1.setInitialCapacity(10 * bytesOneGB, 8 * bytesOneGB, 2 * bytesOneGB);
        PseudoBackend backend2 = cluster.getBackend(10002);
        backend2.setInitialCapacity(20 * bytesOneGB, 8 * bytesOneGB, 6 * bytesOneGB);
        // wait for the disk info to be reported
        Thread.sleep(PseudoBackend.reportIntervalMs + 1000);
        Assert.assertEquals(10 * bytesOneGB,
                systemInfoService.getBackend(10001).getTotalCapacityB());
        Assert.assertEquals(20 * bytesOneGB,
                systemInfoService.getBackend(10002).getTotalCapacityB());
        Assert.assertEquals(8 * bytesOneGB + 1,
                systemInfoService.getBackend(10001).getAvailableCapacityB());
        Assert.assertEquals(2 * bytesOneGB,
                systemInfoService.getBackend(10001).getDataUsedCapacityB());
    }

    @Test
    public void test2DiskUsageAfterWrite() throws SQLException, InterruptedException {
        PseudoCluster cluster = PseudoCluster.getInstance();
        PseudoBackend be = cluster.getBackend(10001);
        be.setInitialCapacity(10 * bytesOneGB, 8 * bytesOneGB, 2 * bytesOneGB);
        cluster.runSql("test", "insert into test values (1,\"1\", 1), (2,\"2\",2), (3,\"3\",3);");
        Thread.sleep(PseudoBackend.reportIntervalMs + 1000);
        SystemInfoService systemInfoService = GlobalStateMgr.getCurrentSystemInfo();
        Assert.assertEquals(2 * bytesOneGB + PseudoBackend.DEFAULT_SIZE_ON_DISK_PER_ROWSET_B,
                systemInfoService.getBackend(10001).getDataUsedCapacityB());
        Assert.assertEquals(8 * bytesOneGB - PseudoBackend.DEFAULT_SIZE_ON_DISK_PER_ROWSET_B + 1,
                systemInfoService.getBackend(10001).getAvailableCapacityB());
    }

    @Test
    public void test3DiskUsageAfterIncrementalClone() throws SQLException, InterruptedException {
        PseudoCluster cluster = PseudoCluster.getInstance();
        // this will commit 2 transactions
        PseudoClusterUtils.triggerIncrementalCloneOnce(cluster, 10001);
        Thread.sleep(PseudoBackend.reportIntervalMs + 1000);
        SystemInfoService systemInfoService = GlobalStateMgr.getCurrentSystemInfo();
        System.out.println(systemInfoService.getBackend(10001).getDataUsedCapacityB());
        Assert.assertEquals(2 * bytesOneGB + PseudoBackend.DEFAULT_SIZE_ON_DISK_PER_ROWSET_B * 3,
                systemInfoService.getBackend(10001).getDataUsedCapacityB());
        Assert.assertEquals(8 * bytesOneGB - PseudoBackend.DEFAULT_SIZE_ON_DISK_PER_ROWSET_B * 3 + 1,
                systemInfoService.getBackend(10001).getAvailableCapacityB());
    }

    @Test
    public void test4DiskUsageAfterFullClone() throws SQLException, InterruptedException {
        PseudoCluster cluster = PseudoCluster.getInstance();
        PseudoBackend be = cluster.getBackend(10001);
        Config.tablet_sched_repair_delay_factor_second = 100;
        Config.tablet_sched_checker_interval_seconds = 100;
        be.setWriteFailureRate(1.0f);
        try {
            cluster.runSql("test", "insert into test values (1111,\"1\", 1), (2,\"2\",2), (3,\"3\",3);");
            cluster.runSql("test", "insert into test values (1222,\"1\", 1), (2,\"2\",2), (3,\"3\",3);");
            cluster.runSql("test", "insert into test values (1333,\"1\", 1), (2,\"2\",2), (3,\"3\",3);");
        } finally {
            be.setWriteFailureRate(0.0f);
        }

        // old versions of other replicas will be GCed, backend 10001 will have to full clone from other be
        long oldVersionExpireSec = Tablet.versionExpireSec;
        PseudoBackend.tabletCheckIntervalMs = 1000;
        Tablet.versionExpireSec = 1;
        try {
            Thread.sleep(Tablet.versionExpireSec * 1000 + PseudoBackend.tabletCheckIntervalMs + 2000);
        } finally {
            Tablet.versionExpireSec = oldVersionExpireSec;
        }

        Config.tablet_sched_repair_delay_factor_second = 1;
        Config.tablet_sched_checker_interval_seconds = 1;
        // wait for full clone finished
        long tabletId = cluster.listTablets("test", "test").get(0);
        Tablet tablet = be.getTablet(tabletId);
        while (true) {
            if (tablet.getCloneExecuted() >= 2) {
                break;
            }
            System.out.printf("wait tablet %d to finish full clone, current clone executed: %d\n",
                    tabletId, tablet.getCloneExecuted());
            Thread.sleep(1000);
        }
        // check disk usage after clone
        Thread.sleep(PseudoBackend.reportIntervalMs + 1000);
        SystemInfoService systemInfoService = GlobalStateMgr.getCurrentSystemInfo();
        Assert.assertEquals(2 * bytesOneGB + PseudoBackend.DEFAULT_SIZE_ON_DISK_PER_ROWSET_B * 6,
                systemInfoService.getBackend(10001).getDataUsedCapacityB());
        Assert.assertEquals(8 * bytesOneGB - PseudoBackend.DEFAULT_SIZE_ON_DISK_PER_ROWSET_B * 6 + 1,
                systemInfoService.getBackend(10001).getAvailableCapacityB());
    }

    @Test
    public void test5DiskUsageAfterTabletDropped() throws SQLException, InterruptedException {
        PseudoCluster cluster = PseudoCluster.getInstance();
        SystemInfoService systemInfoService = GlobalStateMgr.getCurrentSystemInfo();
        cluster.runSql("test", "drop table test force");
        Thread.sleep(PseudoBackend.reportIntervalMs + 1000);
        // The disk usage should return to initial state after the only table dropped.
        Assert.assertEquals(2 * bytesOneGB, systemInfoService.getBackend(10001).getDataUsedCapacityB());
        Assert.assertEquals(8 * bytesOneGB + 1, systemInfoService.getBackend(10001).getAvailableCapacityB());
    }
}
