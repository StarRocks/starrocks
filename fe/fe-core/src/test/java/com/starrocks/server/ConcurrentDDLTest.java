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


package com.starrocks.server;

import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.common.Log4jConfig;
import com.starrocks.common.util.StringUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ShowExecutor;
import com.starrocks.sql.ast.ShowTableStmt;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class ConcurrentDDLTest {
    private static StarRocksAssert starRocksAssert;
    private static ConnectContext connectContext;

    @BeforeClass
    public static void beforeClass() throws Exception {
        Config.sys_log_level = "FATAL";
        Log4jConfig.initLogging();
        UtFrameUtils.createMinStarRocksCluster();
        Config.metadata_journal_queue_size = 100000;
        UtFrameUtils.setUpForPersistTest();

        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);
        starRocksAssert.withDatabase("test");

        // add extra backends
        UtFrameUtils.addMockBackend(10002);
        UtFrameUtils.addMockBackend(10003);
        UtFrameUtils.addMockBackend(10004);
        UtFrameUtils.addMockBackend(10005);

        Config.tablet_sched_disable_colocate_balance = true;
        Config.tablet_sched_repair_delay_factor_second = 1000000;
        Config.task_runs_queue_length = 50000;
    }

    @AfterClass
    public static void teardown() throws Exception {
        UtFrameUtils.tearDownForPersisTest();
    }

    @Test
    public void testConcurrentCreatingColocateTables() throws InterruptedException {
        // Create a CountDownLatch to synchronize the start of all threads
        CountDownLatch startLatch = new CountDownLatch(1);

        // Create and start multiple threads
        int numThreads = 3;
        Thread[] threads = new Thread[numThreads];

        for (int i = 0; i < numThreads; i++) {
            threads[i] = new Thread(() -> {
                try {
                    startLatch.await(); // Wait until all threads are ready to start
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }

                try {
                    System.out.println("start to create table");
                    starRocksAssert.withTable("create table test.test_tbl_" + Thread.currentThread().getId() +
                            " (id int) duplicate key (id)" +
                            " distributed by hash(id) buckets 5183 " +
                            "properties(\"replication_num\"=\"1\", \"colocate_with\"=\"test_cg_001\");");
                    System.out.println("end to create table");
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            threads[i].start();
        }

        List<Long> threadIds = Arrays.stream(threads).map(Thread::getId).collect(Collectors.toList());

        // Start all threads concurrently
        startLatch.countDown();

        // Wait for all threads to finish, table creation finish
        for (Thread thread : threads) {
            thread.join();
        }

        Database db = GlobalStateMgr.getServingState().getDb("test");
        Table table = db.getTable("test_tbl_" + threadIds.get(0));

        List<List<Long>> bucketSeq = GlobalStateMgr.getCurrentState().getColocateTableIndex().getBackendsPerBucketSeq(
                GlobalStateMgr.getCurrentState().getColocateTableIndex().getGroup(table.getId()));
        // check all created colocate tables has same tablet distribution as the bucket seq in colocate group
        for (long threadId : threadIds) {
            table = db.getTable("test_tbl_" + threadId);
            List<Long> tablets = table.getPartitions().stream().findFirst().get().getBaseIndex().getTabletIdsInOrder();
            List<Long> backendIdList = tablets.stream()
                    .map(id -> GlobalStateMgr.getCurrentState().getTabletInvertedIndex().getReplicasByTabletId(id))
                    .map(replicaList -> replicaList.get(0).getBackendId())
                    .collect(Collectors.toList());
            Assert.assertEquals(bucketSeq, backendIdList.stream().map(Arrays::asList).collect(Collectors.toList()));
        }
    }

    @Test
    public void testConcurrentlyDropDbAndCreateTable() throws Exception {
        final String createTableSqlFormat =
                "CREATE TABLE IF NOT EXISTS concurrent_test_db.test_tbl_RRR(k1 int, k2 int, k3 int)" +
                        " distributed by hash(k1) buckets 3 properties('replication_num' = '1');";
        final String createViewSqlFormat = "CREATE VIEW IF NOT EXISTS concurrent_test_db.test_view_RRR" +
                " as select k1,k2 from concurrent_test_db.base_t1;";
        final String createMVSqlFormat = "CREATE MATERIALIZED VIEW IF NOT EXISTS" +
                " concurrent_test_db.test_mv_RRR DISTRIBUTED BY HASH(`k2`) REFRESH MANUAL" +
                " as select k2,k3 from concurrent_test_db.base_t1;";

        // run multi rounds to try to detect potential concurrency problems
        for (int round = 0; round < 5; round++) {
            System.out.println("round-" + round + " begin");
            AtomicBoolean stop = new AtomicBoolean(false);

            // start thread to create and drop db
            Thread controlThread = new Thread(() -> {
                int times = 0;
                Random random = new Random();
                ConnectContext ctx = UtFrameUtils.createDefaultCtx();
                ctx.setThreadLocalInfo();
                while (times < 10) {
                    try {
                        System.out.println("creating table and db time: " + times);
                        starRocksAssert.withDatabase("concurrent_test_db");
                        starRocksAssert.withTable(
                                "CREATE TABLE IF NOT EXISTS concurrent_test_db.base_t1(k1 int, k2 int, k3 int)" +
                                        " distributed by hash(k1) buckets 3 properties('replication_num' = '1');");
                        int time = 300 + random.nextInt(100);
                        // sleep random time before dropping database
                        Thread.sleep(time);
                        System.out.println("dropping table and db");
                        Database db = GlobalStateMgr.getCurrentState().getDb("concurrent_test_db");
                        ShowTableStmt showTableStmt =
                                (ShowTableStmt) UtFrameUtils.parseStmtWithNewParser(
                                        "show tables from concurrent_test_db", connectContext);
                        ShowExecutor showExecutor = new ShowExecutor(connectContext, showTableStmt);
                        starRocksAssert.dropDatabase("concurrent_test_db");
                        System.out.println("concurrent_test_db dropped");
                    } catch (Exception e) {
                        System.out.println("failed, error: " + e.getMessage());
                        e.printStackTrace();
                        Assert.fail();
                    } finally {
                        times++;
                    }
                }
                stop.set(true);
                System.out.println("stop set to true, end all threads");
            });
            controlThread.start();
            Thread.sleep(200);

            // start 50 threads to concurrently create table/mv/views
            Thread[] threads = new Thread[50];
            for (int i = 0; i < 50; i++) {
                threads[i] = new Thread(() -> {
                    connectContext.setThreadLocalInfo();
                    Random random = new Random();
                    while (!stop.get()) {
                        // sleep random time to simulate concurrency
                        int time = random.nextInt(20) + 10;
                        try {
                            Thread.sleep(time);
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                        String randomStr = StringUtils.generateRandomString(24);
                        int idx = time % 3;
                        String sql = null;
                        try {
                            if (idx == 0) { // create table
                                sql = createTableSqlFormat.replaceAll("RRR", randomStr);
                                starRocksAssert.withTable(sql);
                            } else if (idx == 1) { // create view
                                sql = createViewSqlFormat.replaceAll("RRR", randomStr);
                                starRocksAssert.withView(sql);
                            } else { // create mv
                                sql = createMVSqlFormat.replaceAll("RRR", randomStr);
                                starRocksAssert.withMaterializedView(sql);
                            }
                        } catch (Exception e) {
                            // do nothing
                        }
                    }
                });

                threads[i].start();
            } // end create table/view/mv thread

            // Wait for all threads to finish
            controlThread.join();
            for (Thread thread : threads) {
                thread.join();
            }

            // finally replay all the operations to check whether replay is ok
            UtFrameUtils.PseudoJournalReplayer.replayJournalToEnd();
        } // end round
    }

    @Test
    public void testConcurrentCreateSameTable() throws InterruptedException {
        // Create a CountDownLatch to synchronize the start of all threads
        CountDownLatch startLatch = new CountDownLatch(1);

        // Create and start multiple threads
        int numThreads = 5;
        Thread[] threads = new Thread[numThreads];
        AtomicInteger errorCount = new AtomicInteger(0);

        for (int i = 0; i < numThreads; i++) {
            threads[i] = new Thread(() -> {
                try {
                    startLatch.await(); // Wait until all threads are ready to start
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }

                try {
                    System.out.println("start to create table same_tbl");
                    try {
                        starRocksAssert.withTable("create table test.same_tbl " +
                                " (id int) duplicate key (id)" +
                                " distributed by hash(id) buckets 5183 " +
                                "properties(\"replication_num\"=\"1\", \"colocate_with\"=\"test_cg_001\");");
                    } catch (Exception e) {
                        errorCount.incrementAndGet();
                    }
                    System.out.println("end to create table same_tbl");
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            threads[i].start();
        }

        // Start all threads concurrently
        startLatch.countDown();

        // Wait for all threads to finish, table creation finish
        for (Thread thread : threads) {
            thread.join();
        }

        Assert.assertEquals(4, errorCount.get());
    }
}
