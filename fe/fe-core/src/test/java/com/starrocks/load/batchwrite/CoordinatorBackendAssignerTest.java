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

package com.starrocks.load.batchwrite;

import com.starrocks.common.Config;
import com.starrocks.system.ComputeNode;
import com.starrocks.utframe.UtFrameUtils;
import com.starrocks.warehouse.cngroup.ComputeResource;
import com.starrocks.warehouse.cngroup.WarehouseComputeResource;
import mockit.Expectations;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class CoordinatorBackendAssignerTest extends BatchWriteTestBase {

    private CoordinatorBackendAssignerImpl assigner;

    @BeforeEach
    public void setup() {
        assigner = new CoordinatorBackendAssignerImpl();
        assigner.start();
    }

    // to be compatible with old api
    private void registerBatchWrite(long loadId, long warehouseId, TableId tableId, int expectParallel) {
        assigner.registerBatchWrite(
                loadId, WarehouseComputeResource.of(warehouseId), tableId, expectParallel);
    }

    private CoordinatorBackendAssignerImpl.WarehouseMeta getWarehouseMeta(long warehouseId) {
        return assigner.getWarehouseMeta(WarehouseComputeResource.of(warehouseId));
    }

    private double currentLoadDiffRatio(long warehouseId) {
        return assigner.currentLoadDiffRatio(WarehouseComputeResource.of(warehouseId));
    }

    @Test
    public void testRegisterBatchWrite() throws Exception {
        registerBatchWrite(
                1L, 1, new TableId(DB_NAME_1, TABLE_NAME_1_1), 1);
        Optional<List<ComputeNode>> nodes1 = assigner.getBackends(1);
        assertTrue(nodes1.isPresent());
        assertEquals(1, nodes1.get().size());
        assertEquals(1, nodes1.get().stream().map(ComputeNode::getId).collect(Collectors.toSet()).size());

        registerBatchWrite(
                2L, 1, new TableId(DB_NAME_1, TABLE_NAME_1_2), 2);
        Optional<List<ComputeNode>> nodes2 = assigner.getBackends(2);
        assertTrue(nodes2.isPresent());
        assertEquals(2, nodes2.get().size());
        assertEquals(2, nodes2.get().stream().map(ComputeNode::getId).collect(Collectors.toSet()).size());

        registerBatchWrite(
                3L, 2, new TableId(DB_NAME_2, TABLE_NAME_2_1), 4);
        Optional<List<ComputeNode>> nodes3 = assigner.getBackends(3);
        assertTrue(nodes3.isPresent());
        assertEquals(4, nodes3.get().size());
        assertEquals(4, nodes3.get().stream().map(ComputeNode::getId).collect(Collectors.toSet()).size());

        registerBatchWrite(
                4L, 2, new TableId(DB_NAME_2, TABLE_NAME_2_2), 10);
        Optional<List<ComputeNode>> nodes4 = assigner.getBackends(4);
        assertTrue(nodes4.isPresent());
        assertEquals(5, nodes4.get().size());
        assertEquals(5, nodes4.get().stream().map(ComputeNode::getId).collect(Collectors.toSet()).size());
    }

    @Test
    public void testUnRegisterBatchWrite() throws Exception {
        registerBatchWrite(
                1L, 1, new TableId(DB_NAME_1, TABLE_NAME_1_1), 1);
        registerBatchWrite(
                2L, 1, new TableId(DB_NAME_1, TABLE_NAME_1_2), 2);
        registerBatchWrite(
                3L, 2, new TableId(DB_NAME_2, TABLE_NAME_2_1), 4);
        registerBatchWrite(
                4L, 2, new TableId(DB_NAME_2, TABLE_NAME_2_2), 10);

        final AtomicLong expectNumScheduledTask = new AtomicLong(assigner.numScheduledTasks());
        assigner.unregisterBatchWrite(1);
        assertFalse(assigner.getBackends(1).isPresent());
        expectNumScheduledTask.incrementAndGet();
        Awaitility.await().atMost(5, TimeUnit.SECONDS)
                .until(() -> assigner.numScheduledTasks() == expectNumScheduledTask.get());
        assertFalse(containsLoadMeta(1, 1));

        assigner.unregisterBatchWrite(2);
        assertFalse(assigner.getBackends(2).isPresent());
        expectNumScheduledTask.incrementAndGet();
        Awaitility.await().atMost(5, TimeUnit.SECONDS)
                .until(() -> assigner.numScheduledTasks() == expectNumScheduledTask.get());
        assertFalse(containsLoadMeta(2, 1));

        assigner.unregisterBatchWrite(3);
        assertFalse(assigner.getBackends(3).isPresent());
        expectNumScheduledTask.incrementAndGet();
        Awaitility.await().atMost(5, TimeUnit.SECONDS)
                .until(() -> assigner.numScheduledTasks() == expectNumScheduledTask.get());
        assertFalse(containsLoadMeta(3, 2));

        assigner.unregisterBatchWrite(4);
        assertFalse(assigner.getBackends(4).isPresent());
        expectNumScheduledTask.incrementAndGet();
        Awaitility.await().atMost(5, TimeUnit.SECONDS)
                .until(() -> assigner.numScheduledTasks() == expectNumScheduledTask.get());
        assertFalse(containsLoadMeta(4, 2));
    }

    @Test
    public void testDetectUnavailableNodesWhenGetBackends() throws Exception {
        registerBatchWrite(
                1L, 1, new TableId(DB_NAME_1, TABLE_NAME_1_1), 1);
        registerBatchWrite(
                2L, 1, new TableId(DB_NAME_1, TABLE_NAME_1_2), 2);
        registerBatchWrite(
                3L, 1, new TableId(DB_NAME_2, TABLE_NAME_2_1), 3);
        registerBatchWrite(
                4L, 1, new TableId(DB_NAME_2, TABLE_NAME_2_2), 4);

        assigner.disablePeriodicalScheduleForTest();
        Optional<List<ComputeNode>> nodes = assigner.getBackends(1);
        assertTrue(nodes.isPresent());
        assertEquals(1, nodes.get().size());
        ComputeNode notAliveNode = nodes.get().get(0);
        assertTrue(notAliveNode.isAvailable());
        notAliveNode.setAlive(false);
        assertFalse(notAliveNode.isAvailable());

        final AtomicLong expectNumScheduledTask = new AtomicLong(assigner.numScheduledTasks());
        Optional<List<ComputeNode>> nodes1 = assigner.getBackends(1);
        assertTrue(nodes1.isPresent());
        assertTrue(nodes1.get().isEmpty());
        expectNumScheduledTask.incrementAndGet();
        Awaitility.await().atMost(5, TimeUnit.SECONDS)
                .until(() -> assigner.numScheduledTasks() == expectNumScheduledTask.get());

        for (int i = 1; i <= 4; i++) {
            Optional<List<ComputeNode>> newNodes = assigner.getBackends(i);
            assertTrue(newNodes.isPresent());
            assertEquals(i, newNodes.get().size());
            assertFalse(newNodes.get().stream().map(ComputeNode::getId)
                    .collect(Collectors.toSet())
                    .contains(notAliveNode.getId()));
        }
        notAliveNode.setAlive(true);
    }

    @Test
    public void testPeriodicalCheck() throws Exception {
        Set<Long> backendIds = new HashSet<>();
        for (int i = 1; i <= 100; i++) {
            registerBatchWrite(
                    i, 1, new TableId(DB_NAME_1, TABLE_NAME_1_1), 4);
            Optional<List<ComputeNode>> nodes = assigner.getBackends(i);
            assertTrue(nodes.isPresent());
            assertEquals(4, nodes.get().size());
            nodes.get().forEach(node -> backendIds.add(node.getId()));
        }
        assertEquals(5, backendIds.size());
        assertTrue(currentLoadDiffRatio(1)
                < Config.merge_commit_be_assigner_balance_factor_threshold);


        // create empty warehouse meta
        registerBatchWrite(
                201, 2, new TableId(DB_NAME_1, TABLE_NAME_1_1), 4);
        long expectNumScheduledTask = assigner.numScheduledTasks() + 1;
        assigner.unregisterBatchWrite(201);
        Awaitility.await().atMost(5, TimeUnit.SECONDS)
                .until(() -> assigner.numScheduledTasks() == expectNumScheduledTask);
        CoordinatorBackendAssignerImpl.WarehouseMeta whMeta = getWarehouseMeta(2);
        assertNotNull(whMeta);
        assertTrue(whMeta.loadMetas.isEmpty());

        assigner.disablePeriodicalScheduleForTest();
        for (int i = 10006; i <= 10010; i++) {
            UtFrameUtils.addMockBackend(i);
        }
        try {
            backendIds.clear();
            assigner.runPeriodicalCheck();
            assertNull(getWarehouseMeta(2));
            assertTrue(currentLoadDiffRatio(1) < Config.merge_commit_be_assigner_balance_factor_threshold);
            for (int i = 1; i <= 100; i++) {
                Optional<List<ComputeNode>> nodes = assigner.getBackends(i);
                assertTrue(nodes.isPresent());
                assertEquals(4, nodes.get().size());
                nodes.get().forEach(node -> backendIds.add(node.getId()));
            }
            assertEquals(10, backendIds.size());
            assertTrue(currentLoadDiffRatio(1) < Config.merge_commit_be_assigner_balance_factor_threshold);
        } finally {
            for (int i = 10006; i <= 10010; i++) {
                UtFrameUtils.dropMockBackend(i);
            }
        }
    }

    @Test
    public void testRegisterBatchWriteFailure() throws Exception {
        new Expectations(assigner) {
            {
                assigner.getAvailableNodes((ComputeResource) any);
                result = new RuntimeException("can't find warehouse");
            }
        };

        try {
            registerBatchWrite(
                    1L, 1, new TableId(DB_NAME_1, TABLE_NAME_1_1), 1);
            fail();
        } catch (Exception e) {
            assertTrue(e instanceof RejectedExecutionException);
            assertTrue(e.getCause().getCause() instanceof RuntimeException);
        }
        assertFalse(assigner.containLoad(1));
    }

    @Test
    public void testTaskComparator() {
        CoordinatorBackendAssignerImpl.Task task1 =
                new CoordinatorBackendAssignerImpl.Task(1,
                        CoordinatorBackendAssignerImpl.EventType.REGISTER_LOAD, () -> {});
        CoordinatorBackendAssignerImpl.Task task2 =
                new CoordinatorBackendAssignerImpl.Task(2,
                        CoordinatorBackendAssignerImpl.EventType.DETECT_UNAVAILABLE_NODES, () -> {});
        CoordinatorBackendAssignerImpl.Task task3 =
                new CoordinatorBackendAssignerImpl.Task(3,
                        CoordinatorBackendAssignerImpl.EventType.UNREGISTER_LOAD, () -> {});
        CoordinatorBackendAssignerImpl.Task task4 =
                new CoordinatorBackendAssignerImpl.Task(4,
                        CoordinatorBackendAssignerImpl.EventType.UNREGISTER_LOAD, () -> {});

        PriorityBlockingQueue<CoordinatorBackendAssignerImpl.Task> queue =
                new PriorityBlockingQueue<>(1, CoordinatorBackendAssignerImpl.TaskComparator.INSTANCE);
        assertTrue(queue.add(task1));
        assertTrue(queue.add(task2));
        assertTrue(queue.add(task3));
        assertTrue(queue.add(task4));

        assertSame(task1, queue.poll());
        assertSame(task2, queue.poll());
        assertSame(task3, queue.poll());
        assertSame(task4, queue.poll());
    }

    @Test
    public void testDuplicateStartIsNoOp() throws Exception {
        // Calling start() while the worker is already running must short-circuit on the
        // "already running" guard instead of spawning a second worker thread that would
        // race the first against taskPriorityQueue and warehouseMetas.
        ExecutorService poolBefore = readSingleExecutor();
        assigner.start();
        ExecutorService poolAfter = readSingleExecutor();
        assertSame(poolBefore, poolAfter, "duplicate start() must not replace the running executor");
    }

    @Test
    public void testStopShutsDownExecutorAndStartRebuilds() throws Exception {
        // stop() interrupts the scheduler worker so its blocking poll() returns and
        // runSchedule exits; the executor must terminate within the drain timeout. A
        // subsequent start() then creates a fresh executor for the re-elected leader.
        ExecutorService originalExecutor = readSingleExecutor();
        assigner.stop();
        Awaitility.await().timeout(5, TimeUnit.SECONDS).until(originalExecutor::isTerminated);

        assigner.start();
        ExecutorService rebuiltExecutor = readSingleExecutor();
        assertNotNull(rebuiltExecutor);
        assertNotSame(originalExecutor, rebuiltExecutor,
                "stop() then start() must rebuild the executor");
        assertFalse(rebuiltExecutor.isShutdown());
    }

    @Test
    public void testStartRefusesWhenPreviousWorkerNotTerminated() throws Exception {
        // Inject a shutdown-but-not-terminated executor to mimic a stop() that timed out
        // because the worker was stuck. start() must refuse rather than spinning up a
        // parallel worker, which would corrupt warehouseMetas.
        ExecutorService blockedExecutor = Executors.newSingleThreadExecutor();
        blockedExecutor.submit(() -> {
            try {
                Thread.sleep(30_000L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        blockedExecutor.shutdown();

        // Tear down the @BeforeEach pool first so it doesn't race the test.
        ExecutorService before = readSingleExecutor();
        assigner.stop();
        Awaitility.await().timeout(5, TimeUnit.SECONDS).until(before::isTerminated);

        // Now plant the blocked executor and call start() - it must throw because the
        // injected executor is shutdown-but-not-terminated.
        writeSingleExecutor(blockedExecutor);

        try {
            assertThrows(IllegalStateException.class, () -> assigner.start());
        } finally {
            blockedExecutor.shutdownNow();
        }
    }

    private ExecutorService readSingleExecutor() throws ReflectiveOperationException {
        java.lang.reflect.Field field =
                CoordinatorBackendAssignerImpl.class.getDeclaredField("singleExecutor");
        field.setAccessible(true);
        return (ExecutorService) field.get(assigner);
    }

    private void writeSingleExecutor(ExecutorService executor) throws ReflectiveOperationException {
        java.lang.reflect.Field field =
                CoordinatorBackendAssignerImpl.class.getDeclaredField("singleExecutor");
        field.setAccessible(true);
        field.set(assigner, executor);
    }

    private boolean containsLoadMeta(long loadId, long warehouseId) {
        CoordinatorBackendAssignerImpl.WarehouseMeta whMeta = getWarehouseMeta(warehouseId);
        if (whMeta == null) {
            return false;
        }
        if (whMeta.loadMetas.containsKey(loadId)) {
            return true;
        }
        for (CoordinatorBackendAssignerImpl.NodeMeta nodeMeta : whMeta.sortedNodeMetas) {
            if (nodeMeta.loadIds.contains(loadId)) {
                return true;
            }
        }
        return false;
    }
}
