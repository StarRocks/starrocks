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

import com.starrocks.common.conf.Config;
import com.starrocks.server.GlobalStateMgr;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;

public class BeRestartTest {
    boolean newPublish = false;

    @Before
    public void setUp() throws Exception {
        // make publish wait time shorter for test, so insert will finish quicker if some BE is shutdown
        Config.quorum_publish_wait_time_ms = 1000;
        Config.heartbeat_timeout_second = 2;
        Config.tablet_sched_checker_interval_seconds = 2;
        Config.tablet_sched_repair_delay_factor_second = 2;
        Config.enable_new_publish_mechanism = newPublish;
        Config.alter_scheduler_interval_millisecond = 5000;
        PseudoCluster.getOrCreateWithRandomPort(true, 3);
        GlobalStateMgr.getCurrentState().getTabletChecker().setInterval(3000);
        PseudoCluster.getInstance().runSql(null, "create database test");
    }

    @After
    public void tearDown() throws Exception {
        PseudoCluster.getInstance().runSql(null, "drop database test force");
        PseudoCluster.getInstance().shutdown(false);
    }

    @Test
    public void testBeRestart() throws Exception {
        PseudoCluster cluster = PseudoCluster.getInstance();
        int numTable = 2;
        final String[] tableNames = new String[numTable];
        final long[] tableIds = new long[numTable];
        final long[] tableVersions = new long[numTable];
        final String[] createTableSqls = new String[numTable];
        final String[] insertSqls = new String[numTable];
        for (int i = 0; i < numTable; i++) {
            String name = "test_" + i;
            tableNames[i] = name;
            createTableSqls[i] = PseudoCluster.newCreateTableSqlBuilder().setTableName(name).build();
            insertSqls[i] = PseudoCluster.buildInsertSql("test", name);
            cluster.runSqls("test", createTableSqls[i], insertSqls[i], insertSqls[i], insertSqls[i]);
            tableIds[i] = GlobalStateMgr.getCurrentState().getDb("test").getTable(name).getId();
            // insert 3 times -> version: 4
            tableVersions[i] = 4;
        }
        Random rand = new Random(0);
        final PseudoBackend be10001 = cluster.getBackend(10001);
        final PseudoBackend be10002 = cluster.getBackend(10002);
        final PseudoBackend be10003 = cluster.getBackend(10003);
        be10001.setShutdown(true);
        for (int i = 0; i < 10; i++) {
            int tableIdx = rand.nextInt(numTable);
            cluster.runSql("test", insertSqls[tableIdx], true);
            tableVersions[tableIdx]++;
            Thread.sleep(500);
        }
        be10001.setShutdown(false);
        for (int i = 0; i < 10; i++) {
            int tableIdx = rand.nextInt(numTable);
            cluster.runSql("test", insertSqls[tableIdx], true);
            tableVersions[tableIdx]++;
            Thread.sleep(500);
        }
        Callable<Boolean> allTabletsReachVersion = new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                for (int i = 0; i < numTable; i++) {
                    final long v = tableVersions[i];
                    List<Tablet> tablets = be10001.getTabletsByTable(tableIds[i]);
                    for (Tablet t : tablets) {
                        if (t.maxContinuousVersion() != v) {
                            return false;
                        }
                    }
                }
                return true;
            }
        };
        while (true) {
            Thread.sleep(1000);
            if (allTabletsReachVersion.call()) {
                break;
            }
            System.out.printf("be tablets: %d/%d/%d incremental/total: %d/%d\n", be10001.getTabletManager().getNumTablet(),
                    be10002.getTabletManager().getNumTablet(), be10003.getTabletManager().getNumTablet(),
                    Tablet.getTotalIncrementalClone(), Tablet.getTotalClone());
        }
    }
}
