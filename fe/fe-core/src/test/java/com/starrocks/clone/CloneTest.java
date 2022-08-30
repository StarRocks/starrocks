// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.clone;

import com.starrocks.common.Config;
import com.starrocks.pseudocluster.PseudoCluster;
import com.starrocks.pseudocluster.PseudoClusterUtils;
import com.starrocks.pseudocluster.Tablet;
import com.starrocks.server.GlobalStateMgr;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class CloneTest {
    @BeforeClass
    public static void setUp() throws Exception {
        // set timeout to a really long time so that ut can pass even when load of ut machine is very high
        Config.bdbje_heartbeat_timeout_second = 60;
        Config.bdbje_replica_ack_timeout_second = 60;
        Config.bdbje_lock_timeout_second = 60;
        // set some parameters to speedup test
        Config.tablet_sched_checker_interval_seconds = 1;
        Config.tablet_sched_repair_delay_factor_second = 1;
        Config.enable_new_publish_mechanism = true;
        PseudoCluster.getOrCreateWithRandomPort(true, 3);
        GlobalStateMgr.getCurrentState().getTabletChecker().setInterval(1000);
        PseudoCluster cluster = PseudoCluster.getInstance();
        cluster.runSql(null, "create database test");
        cluster.runSql("test",
                "create table test ( pk bigint NOT NULL, v0 string not null, v1 int not null ) " +
                        "primary KEY (pk) DISTRIBUTED BY HASH(pk) BUCKETS 1 " +
                        "PROPERTIES(\"replication_num\" = \"3\", \"storage_medium\" = \"SSD\");");
    }

    @AfterClass
    public static void tearDown() throws Exception {
        PseudoCluster.getInstance().shutdown(false);
    }

    @Test
    public void test1ReplicaWriteFailTriggerRepairClone() throws Exception {
        PseudoCluster cluster = PseudoCluster.getInstance();
        Tablet tablet = PseudoClusterUtils.triggerIncrementalCloneOnce(cluster, 10001);
        Assert.assertEquals(3, tablet.maxContinuousVersion());
        Assert.assertEquals(3, tablet.maxVersion());
    }
}
