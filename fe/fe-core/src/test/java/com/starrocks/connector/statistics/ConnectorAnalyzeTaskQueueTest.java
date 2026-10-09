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

package com.starrocks.connector.statistics;

import com.google.common.collect.Sets;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.common.ThreadPoolManager;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.plan.ConnectorPlanTestBase;
import com.starrocks.statistic.AnalyzeStatus;
import com.starrocks.utframe.UtFrameUtils;
import io.trino.hive.$internal.org.apache.commons.lang3.tuple.Triple;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntSupplier;

public class ConnectorAnalyzeTaskQueueTest {
    private static ConnectContext ctx;

    @BeforeAll
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        ctx = UtFrameUtils.createDefaultCtx();
        ConnectorPlanTestBase.mockHiveCatalog(ctx);
    }

    @Test
    public void testAddPendingTaskWithMerge() {
        Table table = GlobalStateMgr.getCurrentState().getMetadataMgr().getTable(ctx, "hive0",
                "partitioned_db", "orders");
        String tableUUID = table.getUUID();
        Triple<String, Database, Table> tableTriple = StatisticsUtils.getTableTripleByUUID(ctx, tableUUID);
        ConnectorAnalyzeTask task1 = new ConnectorAnalyzeTask(tableTriple, Sets.newHashSet("o_orderkey", "o_custkey"));

        ConnectorAnalyzeTaskQueue queue = new ConnectorAnalyzeTaskQueue();
        queue.addPendingTask(tableUUID, task1);
        Assertions.assertEquals(1, queue.getPendingTaskSize());
        // merge task
        ConnectorAnalyzeTask task2 = new ConnectorAnalyzeTask(tableTriple, Sets.newHashSet("o_orderstatus"));
        queue.addPendingTask(tableUUID, task2);
        Assertions.assertEquals(1, queue.getPendingTaskSize());
    }

    @Test
    public void testAddPendingTaskExceedLimit() {
        Table table = GlobalStateMgr.getCurrentState().getMetadataMgr().getTable(ctx, "hive0",
                "partitioned_db", "orders");
        String tableUUID = table.getUUID();
        Triple<String, Database, Table> tableTriple = StatisticsUtils.getTableTripleByUUID(ctx, tableUUID);
        ConnectorAnalyzeTask task1 = new ConnectorAnalyzeTask(tableTriple, Sets.newHashSet("o_orderkey", "o_custkey"));

        Config.connector_table_query_trigger_analyze_max_pending_task_num = 1;
        ConnectorAnalyzeTaskQueue queue = new ConnectorAnalyzeTaskQueue();
        queue.addPendingTask(tableUUID, task1);
        Assertions.assertEquals(1, queue.getPendingTaskSize());
        // add task exceed limit
        ConnectorAnalyzeTask task2 = new ConnectorAnalyzeTask(tableTriple, Sets.newHashSet("o_orderstatus"));
        Assertions.assertFalse(queue.addPendingTask(tableUUID, task2));
        Config.connector_table_query_trigger_analyze_max_pending_task_num = 100;
    }

    @Test
    public void testScheduledPendingTask() {
        Table table = GlobalStateMgr.getCurrentState().getMetadataMgr().getTable(ctx, "hive0",
                "partitioned_db", "orders");
        String tableUUID = table.getUUID();
        Triple<String, Database, Table> tableTriple = StatisticsUtils.getTableTripleByUUID(ctx, tableUUID);
        ConnectorAnalyzeTask task1 = new ConnectorAnalyzeTask(tableTriple, Sets.newHashSet("o_orderkey", "o_custkey"));

        ConnectorAnalyzeTaskQueue queue = new ConnectorAnalyzeTaskQueue();
        queue.addPendingTask(tableUUID, task1);
        Assertions.assertEquals(1, queue.getPendingTaskSize());

        queue.schedulePendingTask();
        Assertions.assertEquals(0, queue.getPendingTaskSize());

        ConnectorAnalyzeTask task2 = new ConnectorAnalyzeTask(tableTriple, Sets.newHashSet("o_custkey", "o_orderstatus"));
        queue.addPendingTask(tableUUID, task2);

        Config.connector_table_query_trigger_analyze_max_running_task_num = 0;
        queue.schedulePendingTask();
        Assertions.assertEquals(1, queue.getPendingTaskSize());
        Config.connector_table_query_trigger_analyze_max_running_task_num = 2;
        // task2 is kept pending until task1 of the same table finishes
        waitUntil(queue::getRunningTaskSize, 0);
        queue.schedulePendingTask();
        Assertions.assertEquals(0, queue.getPendingTaskSize());
    }

    private static class BlockingTask extends ConnectorAnalyzeTask {
        private final CountDownLatch latch = new CountDownLatch(1);
        private final AtomicInteger runCount;

        BlockingTask(Triple<String, Database, Table> tableTriple, Set<String> columns, AtomicInteger runCount) {
            super(tableTriple, columns);
            this.runCount = runCount;
        }

        @Override
        public Optional<AnalyzeStatus> run() {
            runCount.incrementAndGet();
            try {
                latch.await(60, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return Optional.empty();
        }

        void finish() {
            latch.countDown();
        }
    }

    private static void waitUntil(IntSupplier actual, int expected) {
        for (int i = 0; i < 1000 && actual.getAsInt() != expected; i++) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        Assertions.assertEquals(expected, actual.getAsInt());
    }

    private static Triple<String, Database, Table> getOrdersTableTriple() {
        Table table = GlobalStateMgr.getCurrentState().getMetadataMgr().getTable(ctx, "hive0",
                "partitioned_db", "orders");
        return StatisticsUtils.getTableTripleByUUID(ctx, table.getUUID());
    }

    @Test
    public void testScheduleSameTableWhileRunning() {
        Triple<String, Database, Table> tableTriple = getOrdersTableTriple();
        String tableUUID = tableTriple.getRight().getUUID();
        int oldLimit = Config.connector_table_query_trigger_analyze_max_running_task_num;
        Config.connector_table_query_trigger_analyze_max_running_task_num = 2;
        ExecutorService pool = ThreadPoolManager.newDaemonThreadPool(2, 2, 60L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(2), new ThreadPoolExecutor.AbortPolicy(),
                "connector-trigger-analyze-test-pool", false);
        try {
            ConnectorAnalyzeTaskQueue queue = new ConnectorAnalyzeTaskQueue(pool);
            AtomicInteger runCount = new AtomicInteger();
            BlockingTask task1 = new BlockingTask(tableTriple, Sets.newHashSet("o_orderkey"), runCount);
            queue.addPendingTask(tableUUID, task1);
            queue.schedulePendingTask();
            waitUntil(runCount::get, 1);
            Assertions.assertEquals(1, queue.getRunningTaskSize());

            // a task for the same table with remaining columns is accepted while task1 is running,
            // but it must not be dispatched (and must not overwrite the running entry) until task1 finishes
            BlockingTask task2 = new BlockingTask(tableTriple, Sets.newHashSet("o_orderkey", "o_custkey"), runCount);
            Assertions.assertTrue(queue.addPendingTask(tableUUID, task2));
            queue.schedulePendingTask();
            Assertions.assertEquals(1, queue.getPendingTaskSize());
            Assertions.assertEquals(1, queue.getRunningTaskSize());
            Assertions.assertEquals(1, runCount.get());

            task1.finish();
            waitUntil(queue::getRunningTaskSize, 0);

            queue.schedulePendingTask();
            Assertions.assertEquals(0, queue.getPendingTaskSize());
            waitUntil(runCount::get, 2);
            Assertions.assertEquals(1, queue.getRunningTaskSize());

            task2.finish();
            waitUntil(queue::getRunningTaskSize, 0);
        } finally {
            Config.connector_table_query_trigger_analyze_max_running_task_num = oldLimit;
            pool.shutdownNow();
        }
    }

    @Test
    public void testScheduleRejectedBySubmitPool() {
        Triple<String, Database, Table> tableTriple = getOrdersTableTriple();
        String tableUUID = tableTriple.getRight().getUUID();
        int oldLimit = Config.connector_table_query_trigger_analyze_max_running_task_num;
        Config.connector_table_query_trigger_analyze_max_running_task_num = 2;
        try {
            ConnectorAnalyzeTaskQueue queue = new ConnectorAnalyzeTaskQueue(command -> {
                throw new RejectedExecutionException("mock rejected");
            });
            AtomicInteger runCount = new AtomicInteger();
            queue.addPendingTask(tableUUID, new BlockingTask(tableTriple, Sets.newHashSet("o_orderkey"), runCount));

            // rejected task must not leak in running tasks and must be kept pending for the next round
            Assertions.assertDoesNotThrow(queue::schedulePendingTask);
            Assertions.assertEquals(0, queue.getRunningTaskSize());
            Assertions.assertEquals(1, queue.getPendingTaskSize());
            Assertions.assertFalse(queue.isMaxRunningConcurrencyReached());

            // retried in the next round, still not leaked
            Assertions.assertDoesNotThrow(queue::schedulePendingTask);
            Assertions.assertEquals(0, queue.getRunningTaskSize());
            Assertions.assertEquals(1, queue.getPendingTaskSize());
            Assertions.assertEquals(0, runCount.get());
        } finally {
            Config.connector_table_query_trigger_analyze_max_running_task_num = oldLimit;
        }
    }
}
