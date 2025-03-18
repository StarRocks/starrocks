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
import mockit.Expectations;
import org.awaitility.Awaitility;
import org.junit.Before;
import org.junit.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class CoordinatorBackendAssignerTest extends BatchWriteTestBase {

    private CoordinatorBackendAssignerImpl assigner;

    @Before
    public void setup() {
        assigner = new CoordinatorBackendAssignerImpl();
        assigner.start();
    }

    @Test
    public void testRegisterBatchWrite() throws Exception {
        assigner.registerBatchWrite(
                1L, 1, new TableId(DB_NAME_1, TABLE_NAME_1_1), 1);
        Optional<List<ComputeNode>> nodes1 = assigner.getBackends(1);
        assertTrue(nodes1.isPresent());
        assertEquals(1, nodes1.get().size());
        assertEquals(1, nodes1.get().stream().map(ComputeNode::getId).collect(Collectors.toSet()).size());

        assigner.registerBatchWrite(
                2L, 1, new TableId(DB_NAME_1, TABLE_NAME_1_2), 2);
        Optional<List<ComputeNode>> nodes2 = assigner.getBackends(2);
        assertTrue(nodes2.isPresent());
        assertEquals(2, nodes2.get().size());
        assertEquals(2, nodes2.get().stream().map(ComputeNode::getId).collect(Collectors.toSet()).size());

        assigner.registerBatchWrite(
                3L, 2, new TableId(DB_NAME_2, TABLE_NAME_2_1), 4);
        Optional<List<ComputeNode>> nodes3 = assigner.getBackends(3);
        assertTrue(nodes3.isPresent());
        assertEquals(4, nodes3.get().size());
        assertEquals(4, nodes3.get().stream().map(ComputeNode::getId).collect(Collectors.toSet()).size());

        assigner.registerBatchWrite(
                4L, 2, new TableId(DB_NAME_2, TABLE_NAME_2_2), 10);
        Optional<List<ComputeNode>> nodes4 = assigner.getBackends(4);
        assertTrue(nodes4.isPresent());
        assertEquals(5, nodes4.get().size());
        assertEquals(5, nodes4.get().stream().map(ComputeNode::getId).collect(Collectors.toSet()).size());
    }

    @Test
    public void testUnRegisterBatchWrite() throws Exception {
        assigner.registerBatchWrite(
                1L, 1, new TableId(DB_NAME_1, TABLE_NAME_1_1), 1);
        assigner.registerBatchWrite(
                2L, 1, new TableId(DB_NAME_1, TABLE_NAME_1_2), 2);
        assigner.registerBatchWrite(
                3L, 2, new TableId(DB_NAME_2, TABLE_NAME_2_1), 4);
        assigner.registerBatchWrite(
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
        assigner.registerBatchWrite(
                1L, 1, new TableId(DB_NAME_1, TABLE_NAME_1_1), 1);
        assigner.registerBatchWrite(
                2L, 1, new TableId(DB_NAME_1, TABLE_NAME_1_2), 2);
        assigner.registerBatchWrite(
                3L, 1, new TableId(DB_NAME_2, TABLE_NAME_2_1), 3);
        assigner.registerBatchWrite(
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
            assigner.registerBatchWrite(
                    i, 1, new TableId(DB_NAME_1, TABLE_NAME_1_1), 4);
            Optional<List<ComputeNode>> nodes = assigner.getBackends(i);
            assertTrue(nodes.isPresent());
            assertEquals(4, nodes.get().size());
            nodes.get().forEach(node -> backendIds.add(node.getId()));
        }
        assertEquals(5, backendIds.size());
        assertTrue(assigner.currentLoadDiffRatio(1) < Config.merge_commit_be_assigner_balance_factor_threshold);


        // create empty warehouse meta
        assigner.registerBatchWrite(
                201, 2, new TableId(DB_NAME_1, TABLE_NAME_1_1), 4);
        long expectNumScheduledTask = assigner.numScheduledTasks() + 1;
        assigner.unregisterBatchWrite(201);
        Awaitility.await().atMost(5, TimeUnit.SECONDS)
                .until(() -> assigner.numScheduledTasks() == expectNumScheduledTask);
        CoordinatorBackendAssignerImpl.WarehouseMeta whMeta = assigner.getWarehouseMeta(2);
        assertNotNull(whMeta);
        assertTrue(whMeta.loadMetas.isEmpty());

        assigner.disablePeriodicalScheduleForTest();
        for (int i = 10006; i <= 10010; i++) {
            UtFrameUtils.addMockBackend(i);
        }
        try {
            backendIds.clear();
            assigner.runPeriodicalCheck();
            assertNull(assigner.getWarehouseMeta(2));
            assertTrue(assigner.currentLoadDiffRatio(1) < Config.merge_commit_be_assigner_balance_factor_threshold);
            for (int i = 1; i <= 100; i++) {
                Optional<List<ComputeNode>> nodes = assigner.getBackends(i);
                assertTrue(nodes.isPresent());
                assertEquals(4, nodes.get().size());
                nodes.get().forEach(node -> backendIds.add(node.getId()));
            }
            assertEquals(10, backendIds.size());
            assertTrue(assigner.currentLoadDiffRatio(1) < Config.merge_commit_be_assigner_balance_factor_threshold);
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
                assigner.getAvailableNodes(anyLong);
                result = new RuntimeException("can't find warehouse");
            }
        };

        try {
            assigner.registerBatchWrite(
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

    private boolean containsLoadMeta(long loadId, long warehouseId) {
        CoordinatorBackendAssignerImpl.WarehouseMeta whMeta = assigner.getWarehouseMeta(warehouseId);
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
