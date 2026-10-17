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

package com.starrocks.qe.scheduler.slot;

import com.google.common.collect.ImmutableList;
import com.starrocks.common.Config;
import com.starrocks.ha.FrontendNodeType;
import com.starrocks.metric.MetricRepo;
import com.starrocks.server.NodeMgr;
import com.starrocks.system.Frontend;
import com.starrocks.thrift.TStatus;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.thrift.TUniqueId;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.lang.reflect.Field;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

// The fix lives in the shared RequestWorker loop, so each test runs under both query queue v1 and
// v2; Config.enable_query_queue_v2 only swaps the slot selection strategy, which this path never uses.
public class SlotManagerRequestWorkerTest {
    private static final String WORKER_THREAD_NAME = "slot-mgr-req";

    private static boolean oldEnableQueryQueueV2;

    @BeforeAll
    public static void beforeClass() {
        MetricRepo.init();
        oldEnableQueryQueueV2 = Config.enable_query_queue_v2;
    }

    @AfterAll
    public static void afterClass() {
        Config.enable_query_queue_v2 = oldEnableQueryQueueV2;
    }

    @AfterEach
    public void restoreConfig() {
        Config.enable_query_queue_v2 = oldEnableQueryQueueV2;
    }

    private static Set<Thread> workerThreadsSnapshot() {
        Set<Thread> workers = new HashSet<>();
        for (Thread thread : Thread.getAllStackTraces().keySet()) {
            if (WORKER_THREAD_NAME.equals(thread.getName())) {
                workers.add(thread);
            }
        }
        return workers;
    }

    private static Thread awaitNewWorker(Set<Thread> preexisting) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 5000;
        while (System.currentTimeMillis() < deadline) {
            for (Thread thread : workerThreadsSnapshot()) {
                if (!preexisting.contains(thread)) {
                    return thread;
                }
            }
            Thread.sleep(10);
        }
        throw new AssertionError("slot-mgr-req thread did not start within 5s");
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testWorkerSurvivesErrorFromTaskAndKeepsServing(boolean enableQueryQueueV2) throws Exception {
        Config.enable_query_queue_v2 = enableQueryQueueV2;
        Set<Thread> preexisting = workerThreadsSnapshot();
        SlotManager slotManager = new SlotManager(new ResourceUsageMonitor());
        slotManager.start();
        Thread worker = awaitNewWorker(preexisting);

        // Under leader heap exhaustion a task can throw OutOfMemoryError; this stands in for that.
        slotManager.requests.add(() -> {
            throw new OutOfMemoryError("injected error during task processing");
        });

        // The worker must survive the Error and keep serving the next request.
        CountDownLatch probe = new CountDownLatch(1);
        slotManager.requests.add(probe::countDown);
        assertThat(probe.await(5, TimeUnit.SECONDS))
                .as("the later task must still run after a task threw an Error")
                .isTrue();
        assertThat(worker.isAlive()).isTrue();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testExceptionFromOneTaskDoesNotStarveOrReplay(boolean enableQueryQueueV2) throws Exception {
        Config.enable_query_queue_v2 = enableQueryQueueV2;
        Set<Thread> preexisting = workerThreadsSnapshot();
        SlotManager slotManager = new SlotManager(new ResourceUsageMonitor());
        slotManager.start();
        Thread worker = awaitNewWorker(preexisting);

        AtomicInteger poisonRuns = new AtomicInteger();
        slotManager.requests.add(() -> {
            poisonRuns.incrementAndGet();
            throw new RuntimeException("injected exception during task processing");
        });

        // A later task must run, and the failed task must not be replayed on the next batch.
        CountDownLatch probe = new CountDownLatch(1);
        slotManager.requests.add(probe::countDown);
        assertThat(probe.await(5, TimeUnit.SECONDS))
                .as("the later task must still run after a task threw an Exception")
                .isTrue();
        assertThat(poisonRuns.get())
                .as("the failed task must run once, not replay ahead of later tasks")
                .isEqualTo(1);
        assertThat(worker.isAlive()).isTrue();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testWorkerSurvivesErrorFromSchedulingPathAndKeepsServing(boolean enableQueryQueueV2) throws Exception {
        Config.enable_query_queue_v2 = enableQueryQueueV2;
        Set<Thread> preexisting = workerThreadsSnapshot();
        SlotManager slotManager = new SlotManager(new ResourceUsageMonitor());

        // Make the non-task scheduling path throw an Error once. getMinExpiredTimeMs() is called at
        // the top of every worker loop, outside task execution, standing in for an OutOfMemoryError
        // that originates in the scheduling path rather than in a task. This exercises the outer
        // catch(Throwable), which the two task-path tests never reach.
        AtomicBoolean thrown = new AtomicBoolean();
        SlotTracker throwingTracker = new SlotTracker(slotManager, ImmutableList.of()) {
            @Override
            public long getMinExpiredTimeMs() {
                if (thrown.compareAndSet(false, true)) {
                    throw new OutOfMemoryError("injected error in scheduling path");
                }
                return super.getMinExpiredTimeMs();
            }
        };
        Field field = SlotManager.class.getDeclaredField("slotTracker");
        field.setAccessible(true);
        field.set(slotManager, throwingTracker);

        slotManager.start();
        Thread worker = awaitNewWorker(preexisting);

        // The worker must survive the Error from the scheduling path and keep serving.
        CountDownLatch probe = new CountDownLatch(1);
        slotManager.requests.add(probe::countDown);
        assertThat(probe.await(5, TimeUnit.SECONDS))
                .as("the later task must still run after the scheduling path threw an Error")
                .isTrue();
        assertThat(worker.isAlive()).isTrue();
        assertThat(thrown.get()).isTrue();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testFailedSlotRequirementIsReportedToTheRequester(boolean enableQueryQueueV2) throws Exception {
        Config.enable_query_queue_v2 = enableQueryQueueV2;
        Set<Thread> preexisting = workerThreadsSnapshot();
        ReplyCapturingSlotManager slotManager = new ReplyCapturingSlotManager(new ResourceUsageMonitor());

        // Make the slot requirement fail after the frontend checks, standing in for a handler bug or
        // an Error under heap pressure. Without a reply the requester waits out its pending timeout.
        SlotTracker throwingTracker = new SlotTracker(slotManager, ImmutableList.of()) {
            @Override
            public boolean requireSlot(LogicalSlot slot) {
                throw new IllegalStateException("injected failure while requiring a slot");
            }
        };
        Field field = SlotManager.class.getDeclaredField("slotTracker");
        field.setAccessible(true);
        field.set(slotManager, throwingTracker);

        new MockUp<NodeMgr>() {
            @Mock
            public Frontend getFeByName(String name) {
                return new Frontend(FrontendNodeType.FOLLOWER, name, "127.0.0.1", 9010);
            }
        };

        slotManager.start();
        Thread worker = awaitNewWorker(preexisting);

        long nowMs = System.currentTimeMillis();
        LogicalSlot slot = new LogicalSlot(new TUniqueId(1, 1), "FE_NAME", 0L, 0L, 1,
                nowMs + 60_000L, nowMs + 120_000L, nowMs, 1, 1);
        slotManager.requireSlotAsync(slot);

        TStatus reply = slotManager.replies.poll(5, TimeUnit.SECONDS);
        assertThat(reply)
                .as("a failed slot requirement must be reported to the requester")
                .isNotNull();
        assertThat(reply.getStatus_code()).isEqualTo(TStatusCode.INTERNAL_ERROR);
        assertThat(slot.getState()).isEqualTo(LogicalSlot.State.CANCELLED);
        assertThat(worker.isAlive()).isTrue();
    }

    private static class ReplyCapturingSlotManager extends SlotManager {
        private final BlockingQueue<TStatus> replies = new LinkedBlockingQueue<>();

        ReplyCapturingSlotManager(ResourceUsageMonitor resourceUsageMonitor) {
            super(resourceUsageMonitor);
        }

        @Override
        protected void finishSlotRequirementToEndpoint(LogicalSlot slot, TStatus status) {
            replies.add(status);
        }
    }
}
