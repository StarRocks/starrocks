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

package com.starrocks.alter;

import com.starrocks.common.FeConstants;
import com.starrocks.pseudocluster.PseudoCluster;
import com.starrocks.server.GlobalStateMgr;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class PseudoClusterAlterTest {
    @BeforeClass
    public static void setUp() throws Exception {
        FeConstants.default_scheduler_interval_millisecond = 5000;
        PseudoCluster.getOrCreateWithRandomPort(true, 3);
        PseudoCluster.getInstance().runSql(null, "create database test");
    }

    @AfterClass
    public static void tearDown() throws Exception {
        PseudoCluster.getInstance().runSql(null, "drop database test force");
        PseudoCluster.getInstance().shutdown(true);
    }

    @Test
    public void testAlterTableSimple() throws Exception {
        PseudoCluster cluster = PseudoCluster.getInstance();
        AlterHandler handler = GlobalStateMgr.getCurrentState().getAlterJobMgr().getSchemaChangeHandler();
        long expectAlterFinishNumber = handler.getAlterJobV2Num(AlterJobV2.JobState.FINISHED) + 1;
        String table = "table_simple";
        String createTableSql = PseudoCluster.newCreateTableSqlBuilder().setTableName(table).build();
        String insertSql = PseudoCluster.buildInsertSql("test", table);
        cluster.runSqls("test", createTableSql, insertSql, insertSql, insertSql);
        cluster.runSql("test", "alter table " + table + " add column add_column1 int");
        while (true) {
            long num = handler.getAlterJobV2Num(AlterJobV2.JobState.FINISHED);
            if (num == expectAlterFinishNumber) {
                break;
            }
            System.out.println("wait alter job to finish...");
            Thread.sleep(2000);
        }
    }

    private volatile boolean stopConcurrentInsert = false;
    private volatile Exception errorInConcurrentInsert = null;

    @Test
    public void testAlterTableWithConcurrentInsert() throws Exception {
        PseudoCluster cluster = PseudoCluster.getInstance();
        AlterHandler handler = GlobalStateMgr.getCurrentState().getAlterJobMgr().getSchemaChangeHandler();
        long expectAlterFinishNumber = handler.getAlterJobV2Num(AlterJobV2.JobState.FINISHED) + 1;
        final String table = "table_concurrent_insert";
        final String createTableSql = PseudoCluster.newCreateTableSqlBuilder().setTableName(table).build();
        final String insertSql = PseudoCluster.buildInsertSql("test", table);
        cluster.runSqls("test", createTableSql, insertSql, insertSql, insertSql);
        Thread concurrentInsertThread = new Thread(() -> {
            while (!stopConcurrentInsert) {
                try {
                    Thread.sleep(2000);
                    System.out.println(insertSql);
                    cluster.runSql("test", insertSql);
                } catch (Exception e) {
                    if (e.getMessage().startsWith("Column count doesn't match value count")) {
                        // alter succeed, another column added, so error expected, stop insert
                        break;
                    }
                    errorInConcurrentInsert = e;
                    e.printStackTrace();
                }
            }
        });
        concurrentInsertThread.start();
        cluster.runSql("test", "alter table " + table + " add column add_column1 int");
        while (true) {
            long num = handler.getAlterJobV2Num(AlterJobV2.JobState.FINISHED);
            if (num == expectAlterFinishNumber) {
                break;
            }
            System.out.println("wait alter job to finish...");
            Thread.sleep(2000);
        }
        stopConcurrentInsert = true;
        concurrentInsertThread.join();
        if (errorInConcurrentInsert != null) {
            Assert.fail("error in concurrent insert:" + errorInConcurrentInsert.getMessage());
        }
    }
}
