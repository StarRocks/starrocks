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
import com.starrocks.qe.ConnectContext;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;

public class ConcurrentDDLTest {
    private static StarRocksAssert starRocksAssert;

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();

        // create connect context
        ConnectContext connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);
        starRocksAssert.withDatabase("test");

        // add extra backends
        UtFrameUtils.addMockBackend(10002);
        UtFrameUtils.addMockBackend(10003);
        UtFrameUtils.addMockBackend(10004);
        UtFrameUtils.addMockBackend(10005);

        Config.tablet_sched_disable_colocate_balance = true;
        Config.tablet_sched_repair_delay_factor_second = 1000000;
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
        System.out.println(bucketSeq);
        // check all created colocate tables has same tablet distribution as the bucket seq in colocate group
        for (long threadId : threadIds) {
            table = db.getTable("test_tbl_" + threadId);
            List<Long> tablets = table.getPartitions().stream().findFirst().get().getBaseIndex().getTabletIdsInOrder();
            List<Long> backendIdList = tablets.stream()
                    .map(id -> GlobalStateMgr.getCurrentState().getTabletInvertedIndex().getReplicasByTabletId(id))
                    .map(replicaList -> replicaList.get(0).getBackendId())
                    .collect(Collectors.toList());
            System.out.println(backendIdList);
            Assert.assertEquals(bucketSeq, backendIdList.stream().map(Arrays::asList).collect(Collectors.toList()));
        }
    }
}
