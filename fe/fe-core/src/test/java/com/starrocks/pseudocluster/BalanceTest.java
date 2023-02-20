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

import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.server.GlobalStateMgr;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.util.List;
import java.util.Random;

public class BalanceTest {
    @BeforeClass
    public static void setUp() throws Exception {
        Config.sys_log_verbose_modules = new String[] {"com.starrocks.clone"};
        FeConstants.default_scheduler_interval_millisecond = 5000;
        PseudoCluster.getOrCreateWithRandomPort(true, 3);
        GlobalStateMgr.getCurrentState().getTabletChecker().setInterval(1000);
        PseudoCluster.getInstance().runSql(null, "create database test");
    }

    @AfterClass
    public static void tearDown() throws Exception {
        PseudoCluster.getInstance().runSql(null, "drop database test force");
        PseudoCluster.getInstance().shutdown(false);
    }

    // todo find the cause: there are still -15 tablets should be cloned to backends 10005
    // @Test
    public void testBalance() throws Exception {
        PseudoCluster cluster = PseudoCluster.getInstance();
        int numTable = 10;
        final String[] tableNames = new String[numTable];
        final String[] createTableSqls = new String[numTable];
        final String[] insertSqls = new String[numTable];
        for (int i = 0; i < numTable; i++) {
            tableNames[i] = "test_" + i;
            PseudoCluster.CreateTableSqlBuilder sqlBuilder = PseudoCluster.newCreateTableSqlBuilder().setTableName(tableNames[i]);
            sqlBuilder.setBuckets(6);
            createTableSqls[i] = sqlBuilder.build();
            insertSqls[i] = PseudoCluster.buildInsertSql("test", tableNames[i]);
            cluster.runSqls("test", createTableSqls[i], insertSqls[i], insertSqls[i], insertSqls[i]);
        }
        List<Long> beIds = cluster.addBackends(3);
        Random rand = new Random(0);
        while (true) {
            boolean balanceFinished = true;
            for (Long beId : beIds) {
                PseudoBackend backend = cluster.getBackend(beId);
                // we have 10 tables with 6 tablets and 18 replicas, so the final state should be
                // every backends having 30 tablets
                if (backend.getTabletManager().getNumTablet() != 30) {
                    System.out.printf("there are still %d tablets should be cloned to backends %d\n",
                            30 - backend.getTabletManager().getNumTablet(), backend.getId());
                    balanceFinished = false;
                }
            }
            if (balanceFinished) {
                break;
            }
            cluster.runSql("test", insertSqls[rand.nextInt(numTable)]);
            Thread.sleep(2000);
        }
        System.out.println("balance finished");
    }
}
