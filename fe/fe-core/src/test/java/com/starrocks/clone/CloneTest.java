// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.clone;

import com.starrocks.common.Config;
import com.starrocks.pseudocluster.PseudoBackend;
import com.starrocks.pseudocluster.PseudoCluster;
import com.starrocks.pseudocluster.Tablet;
import com.starrocks.server.GlobalStateMgr;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class CloneTest {
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
                "create table test ( pk bigint NOT NULL, v0 string not null, v1 int not null ) primary KEY (pk) DISTRIBUTED BY HASH(pk) BUCKETS 1 PROPERTIES(\"replication_num\" = \"3\", \"storage_medium\" = \"SSD\");");
    }

    @AfterClass
    public static void tearDown() throws Exception {
        PseudoCluster.getInstance().shutdown(true);
    }

    @Test
    public void test1ReplicaWriteFailTriggerRepairClone() throws Exception {
        PseudoCluster cluster = PseudoCluster.getInstance();
        PseudoBackend be = cluster.getBackend(10001);
        long tabletId = cluster.listTablets("test", "test").get(0);
        Tablet tablet = be.getTablet(tabletId);
        be.setWriteFailureRate(1.0f);
        try {
            // 2 replicas commit version 2
            cluster.runSql("test", "insert into test values (1,\"1\", 1), (2,\"2\",2), (3,\"3\",3);");
        } finally {
            be.setWriteFailureRate(0.0f);
        }
        // 3 replicas commit version 3
        cluster.runSql("test", "insert into test values (1,\"1\", 1), (2,\"2\",2), (3,\"3\",3);");
        while (true) {
            if (tablet.getCloneExecuted() == 1) {
                break;
            }
            System.out.printf("wait tablet %d to finish clone\n", tabletId);
            Thread.sleep(1000);
        }
        Assert.assertEquals(3, tablet.maxContinuousVersion());
        Assert.assertEquals(3, tablet.maxVersion());
    }
}
