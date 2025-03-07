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

import com.starrocks.clone.ColocateTableBalancer;
import com.starrocks.common.Config;
import com.starrocks.server.GlobalStateMgr;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import java.sql.SQLException;
import java.util.Date;
import java.util.Random;

public class DecommissionTest {
    @BeforeClass
    public static void setUp() throws Exception {
        Config.tablet_sched_checker_interval_seconds = 1;
        Config.tablet_sched_repair_delay_factor_second = 1;
        Config.tablet_checker_partition_batch_num = 1;
        Config.enable_new_publish_mechanism = false;
        Config.drop_backend_after_decommission = false;
        Config.sys_log_verbose_modules = new String[] {"com.starrocks.clone"};
        Config.alter_scheduler_interval_millisecond = 5000;
        Config.tablet_sched_slot_num_per_path = 32;
        Config.tablet_sched_colocate_balance_wait_system_stable_time_s = 1;
        PseudoBackend.reportIntervalMs = 1000;
        PseudoCluster.getOrCreateWithRandomPort(true, 4);
        GlobalStateMgr.getCurrentState().getTabletChecker().setInterval(500);
        ColocateTableBalancer.getInstance().setInterval(500);
        GlobalStateMgr.getCurrentState().getTabletScheduler().setInterval(500);
        PseudoCluster.getInstance().runSql(null, "create database test");
    }

    @AfterClass
    public static void tearDown() throws Exception {
        PseudoCluster.getInstance().runSql(null, "drop database test force");
        PseudoCluster.getInstance().shutdown(true);
    }

    @Test
    public void testDecommission() throws Exception {
        PseudoCluster cluster = PseudoCluster.getInstance();
        Config.statistic_collect_query_timeout = 3600;
        int numTable = 3;
        final String[] tableNames = new String[numTable];
        final String[] createTableSqls = new String[numTable];
        final String[] insertSqls = new String[numTable];
        for (int i = 0; i < numTable; i++) {
            tableNames[i] = "test_" + i;
            PseudoCluster.CreateTableSqlBuilder sqlBuilder = PseudoCluster.newCreateTableSqlBuilder().setTableName(tableNames[i])
                    .setBuckets(1);
            createTableSqls[i] = sqlBuilder.build();
            insertSqls[i] = PseudoCluster.buildInsertSql("test", tableNames[i]);
            cluster.runSqls("test", createTableSqls[i], insertSqls[i], insertSqls[i], insertSqls[i]);
        }
        cluster.runSql("test", "create database if not exists _statistics_");
        cluster.runSql("_statistics_", createTableSqls[0]);
        final PseudoBackend decommissionBE = cluster.getBackend(10001);
        int oldTabletNum = decommissionBE.getTabletManager().getNumTablet();
        cluster.runSql(null, String.format("ALTER SYSTEM DECOMMISSION BACKEND \"%s\"", decommissionBE.getHostHeartbeatPort()));
        Random rand = new Random(0);
        while (true) {
            int curTabletNum = decommissionBE.getTabletManager().getNumTablet();
            System.out.printf("%s #tablets: %d/%d: fullClone: %d wait...\n", new Date(), curTabletNum, oldTabletNum,
                    Tablet.getTotalFullClone());
            if (curTabletNum == 0) {
                break;
            }
            cluster.runSql("test", insertSqls[rand.nextInt(numTable)]);
            Thread.sleep(3000);
        }
        System.out.println("decommission finished");

        // decommission from 3 to 2
        final PseudoBackend decommissionBE2 = cluster.getBackend(10002);
        oldTabletNum = decommissionBE2.getTabletManager().getNumTablet();
        Assertions.assertThrows(SQLException.class, () ->
                cluster.runSql(null, String.format("ALTER SYSTEM DECOMMISSION BACKEND \"%s\"",
                        decommissionBE2.getHostHeartbeatPort())));

        for (int i = 0; i < numTable; i++) {
            String tableName = tableNames[i];
            cluster.runSql("test", "drop table " + tableName);
        }
        cluster.runSql(null, String.format("ALTER SYSTEM DECOMMISSION BACKEND \"%s\"",
                decommissionBE2.getHostHeartbeatPort()));
    }
}
