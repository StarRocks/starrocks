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

package com.starrocks.pseudocluster;

import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Replica;
import com.starrocks.common.Config;
import com.starrocks.server.GlobalStateMgr;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class ReplicaMinReadableVersionTest {
    private static final Logger LOG = LogManager.getLogger(ReplicaMinReadableVersionTest.class);
    static PseudoCluster cluster;
    static String tableName = "test";

    @BeforeClass
    public static void setUp() throws Exception {
        Config.tablet_sched_checker_interval_seconds = 2;
        Config.tablet_sched_repair_delay_factor_second = 2;
        Config.alter_scheduler_interval_millisecond = 5000;
        PseudoCluster.getOrCreateWithRandomPort(true, 3);
        cluster = PseudoCluster.getInstance();
        cluster.runSql(null, "create database test");
        String createTable = PseudoCluster.newCreateTableSqlBuilder().setTableName(tableName).setBuckets(1).setReplication(3)
                .build();
        cluster.runSql("test", createTable);
    }

    @AfterClass
    public static void tearDown() throws Exception {
        PseudoCluster.getInstance().shutdown(true);
    }

    @Test
    public void testReplicaMinReadableVersionReported() throws Exception {
        OlapTable olapTable = (OlapTable) GlobalStateMgr.getCurrentState().getDb("test").getTable(tableName);
        long tableId = olapTable.getId();
        PseudoBackend be = cluster.getBackend(10001);
        Tablet tablet = be.getTabletsByTable(tableId).get(0);
        String insert = PseudoCluster.buildInsertSql("test", tableName);
        for (int i = 0; i < 10; i++) {
            cluster.runSql(null, insert, true);
        }
        Assert.assertEquals(11, tablet.maxContinuousVersion());
        // remove a replica, then wait it to be cloned again, the cloned replica will only have version 11,
        // so it's minReadableVersion should change to 11
        cluster.runSql(null, "ADMIN SET REPLICA STATUS PROPERTIES(\"tablet_id\" = \"" + tablet.id +
                "\", \"backend_id\" = \"10001\", \"status\" = \"bad\");", true);
        while (true) {
            Replica replica = GlobalStateMgr.getCurrentState().getTabletInvertedIndex().getReplica(tablet.id, 10001);
            if (replica == null) {
                LOG.info("replica null\n");
            } else {
                LOG.info("replica minversion: %d version: %d\n", replica.getMinReadableVersion(), replica.getVersion());
                if (replica.getMinReadableVersion() == 11) {
                    break;
                }
            }
            Thread.sleep(1000);
        }
    }
}
