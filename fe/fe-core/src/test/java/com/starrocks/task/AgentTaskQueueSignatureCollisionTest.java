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

package com.starrocks.task;

import com.starrocks.common.util.concurrent.MarkedCountDownLatch;
import com.starrocks.thrift.TTabletSchema;
import com.starrocks.thrift.TTaskType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for UpdateTabletSchemaTask signature uniqueness.
 *
 * Background: UpdateTabletSchemaTask previously used tablets.hashCode() as signature.
 * When two different alter jobs operate on the same tablet set (same table/partition),
 * they produced identical signatures, causing:
 * 1. AgentTaskQueue rejected the new task while old task was still queued
 * 2. After old task was removed, new task entered queue with same signature
 * 3. When BE reported old task completion, FE matched it to the new task (wrong job)
 * 4. New job proceeded with wrong txnId -> publish failed with 404
 *
 * Fix: setTxnId() now updates signature to Objects.hash(baseSignature, txnId), ensuring
 * different alter jobs produce different signatures even for the same tablet set.
 */
public class AgentTaskQueueSignatureCollisionTest {

    private static final long BACKEND_ID = 10001L;
    private static final List<Long> TABLETS = Arrays.asList(24947932L, 24947929L);
    private static final TTaskType TASK_TYPE = TTaskType.UPDATE_TABLET_META_INFO;

    @BeforeEach
    public void setUp() {
        AgentTaskQueue.clearAllTasks();
    }

    @AfterEach
    public void tearDown() {
        AgentTaskQueue.clearAllTasks();
    }

    /**
     * Before setTxnId(), tasks with the same tablet list share an initial signature
     * (tablets.hashCode()). After setTxnId() with different txnIds, signatures diverge.
     */
    @Test
    public void testSignatureDivergesAfterSetTxnId() {
        TabletMetadataUpdateAgentTask task1 = TabletMetadataUpdateAgentTaskFactory
                .createTabletSchemaUpdateTask(BACKEND_ID, TABLETS, new TTabletSchema(), true);
        TabletMetadataUpdateAgentTask task2 = TabletMetadataUpdateAgentTaskFactory
                .createTabletSchemaUpdateTask(BACKEND_ID, TABLETS, new TTabletSchema(), true);

        // Before setTxnId: same initial signature (tablets.hashCode())
        assertEquals(task1.getSignature(), task2.getSignature(),
                "Before setTxnId, signatures should be the same (both = tablets.hashCode())");

        // After setTxnId with different txnIds: signatures diverge
        task1.setTxnId(1000L);
        task2.setTxnId(2000L);
        assertNotEquals(task1.getSignature(), task2.getSignature(),
                "After setTxnId with different txnIds, signatures should differ");
    }

    /**
     * With the fix, two tasks for different alter jobs (different txnIds) on the same
     * tablet set can coexist in AgentTaskQueue without collision.
     */
    @Test
    public void testDifferentTxnIdTasksCanCoexistInQueue() {
        TabletMetadataUpdateAgentTask task1 = TabletMetadataUpdateAgentTaskFactory
                .createTabletSchemaUpdateTask(BACKEND_ID, TABLETS, new TTabletSchema(), true);
        task1.setTxnId(1000L);

        TabletMetadataUpdateAgentTask task2 = TabletMetadataUpdateAgentTaskFactory
                .createTabletSchemaUpdateTask(BACKEND_ID, TABLETS, new TTabletSchema(), true);
        task2.setTxnId(2000L);

        // Both tasks can be added (no signature collision)
        assertTrue(AgentTaskQueue.addTask(task1), "First task should be added");
        assertTrue(AgentTaskQueue.addTask(task2), "Second task should also be added (different signature)");

        // Each task is retrievable by its own signature
        AgentTask retrieved1 = AgentTaskQueue.getTask(BACKEND_ID, TASK_TYPE, task1.getSignature());
        AgentTask retrieved2 = AgentTaskQueue.getTask(BACKEND_ID, TASK_TYPE, task2.getSignature());
        assertSame(task1, retrieved1);
        assertSame(task2, retrieved2);
    }

    /**
     * Verifies that the race condition (old task completion matched to new job's task)
     * is no longer possible after the fix.
     *
     * Scenario:
     * 1. Job A creates task1 (txnId=1000) and adds to queue
     * 2. Job A times out, task1 removed from FE queue (but BE still executing)
     * 3. Job B creates task2 (txnId=2000), adds to queue (succeeds: different signature)
     * 4. BE finishes old task (txnId=1000), reports with task1's signature
     * 5. FE looks up by task1's signature -> finds nothing (task1 already removed)
     * 6. task2 is NOT affected -> Job B waits correctly for its own task to complete
     */
    @Test
    public void testOldTaskCompletionDoesNotAffectNewJob() {
        Set<Long> tabletSet = new HashSet<>(TABLETS);

        // Step 1: Job A creates task1
        TabletMetadataUpdateAgentTask task1 = TabletMetadataUpdateAgentTaskFactory
                .createTabletSchemaUpdateTask(BACKEND_ID, TABLETS, new TTabletSchema(), true);
        task1.setTxnId(1000L);
        long signatureA = task1.getSignature();
        MarkedCountDownLatch<Long, Set<Long>> latch1 = new MarkedCountDownLatch<>(1);
        latch1.addMark(BACKEND_ID, tabletSet);
        task1.setLatch(latch1);
        assertTrue(AgentTaskQueue.addTask(task1));

        // Step 2: Job A times out -> remove task from FE queue
        AgentTaskQueue.removeTask(BACKEND_ID, TASK_TYPE, signatureA);
        assertNull(AgentTaskQueue.getTask(BACKEND_ID, TASK_TYPE, signatureA));

        // Step 3: Job B creates task2 with different signature (due to different txnId)
        TabletMetadataUpdateAgentTask task2 = TabletMetadataUpdateAgentTaskFactory
                .createTabletSchemaUpdateTask(BACKEND_ID, TABLETS, new TTabletSchema(), true);
        task2.setTxnId(2000L);
        long signatureB = task2.getSignature();
        assertNotEquals(signatureA, signatureB, "Signatures should differ after fix");
        MarkedCountDownLatch<Long, Set<Long>> latch2 = new MarkedCountDownLatch<>(1);
        latch2.addMark(BACKEND_ID, tabletSet);
        task2.setLatch(latch2);
        assertTrue(AgentTaskQueue.addTask(task2));

        // Step 4: BE finishes OLD task and reports with task1's signature
        // FE looks up by (backendId, taskType, signatureA) -> finds nothing
        AgentTask matched = AgentTaskQueue.getTask(BACKEND_ID, TASK_TYPE, signatureA);
        assertNull(matched, "Old signature should not match any task in queue");

        // Step 5: Job B's latch is NOT affected
        assertEquals(1, latch2.getCount(),
                "Job B's latch should remain at 1 (not incorrectly counted down)");

        // Step 6: Job B's task is still in queue, waiting for its own completion
        AgentTask task2InQueue = AgentTaskQueue.getTask(BACKEND_ID, TASK_TYPE, signatureB);
        assertNotNull(task2InQueue, "Job B's task should still be in queue");
        assertSame(task2, task2InQueue);
    }

    /**
     * Verifies that tasks with the same txnId (e.g., retries of the same job)
     * still produce the same signature, which is the expected dedup behavior.
     */
    @Test
    public void testSameTxnIdProducesSameSignature() {
        TabletMetadataUpdateAgentTask task1 = TabletMetadataUpdateAgentTaskFactory
                .createTabletSchemaUpdateTask(BACKEND_ID, TABLETS, new TTabletSchema(), true);
        task1.setTxnId(1000L);

        TabletMetadataUpdateAgentTask task2 = TabletMetadataUpdateAgentTaskFactory
                .createTabletSchemaUpdateTask(BACKEND_ID, TABLETS, new TTabletSchema(), true);
        task2.setTxnId(1000L);

        assertEquals(task1.getSignature(), task2.getSignature(),
                "Same tablets + same txnId should produce same signature (dedup still works)");

        // AgentTaskQueue correctly deduplicates retries
        assertTrue(AgentTaskQueue.addTask(task1));
        assertFalse(AgentTaskQueue.addTask(task2),
                "task2 with same signature should be rejected (correct dedup)");
        assertSame(task1, AgentTaskQueue.getTask(BACKEND_ID, TASK_TYPE, task1.getSignature()),
                "Queue should still contain task1, not task2");
    }
}
