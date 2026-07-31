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

package com.starrocks.connector.starrocks;

import com.starrocks.common.ErrorCode;
import com.starrocks.qe.ConnectContext;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

public class StarRocksRemoteScanSessionManagerTest {
    @BeforeEach
    public void setUp() {
        StarRocksRemoteScanSessionManager.resetForTest();
    }

    @AfterEach
    public void tearDown() {
        StarRocksRemoteScanSessionManager.resetForTest();
    }

    @Test
    public void testSuccessfulQueryShouldReleaseRemoteScan() {
        ConnectContext context = new ConnectContext();

        Assertions.assertFalse(StarRocksRemoteScanSessionManager.shouldCancelRemoteScan(context));
    }

    @Test
    public void testNullOrKilledContextShouldCancelRemoteScan() {
        Assertions.assertTrue(StarRocksRemoteScanSessionManager.shouldCancelRemoteScan(null));

        ConnectContext context = new ConnectContext();
        context.setKilled();

        Assertions.assertTrue(StarRocksRemoteScanSessionManager.shouldCancelRemoteScan(context));
    }

    @Test
    public void testErrorStateShouldCancelRemoteScan() {
        ConnectContext context = new ConnectContext();
        context.getState().setError("remote scan failed");

        Assertions.assertTrue(StarRocksRemoteScanSessionManager.shouldCancelRemoteScan(context));
    }

    @Test
    public void testErrorCodeShouldCancelRemoteScan() {
        ConnectContext context = new ConnectContext();
        context.getState().setErrorCode(ErrorCode.ERR_QUERY_INTERRUPTED);

        Assertions.assertTrue(StarRocksRemoteScanSessionManager.shouldCancelRemoteScan(context));
    }

    @Test
    public void testFinishListenerOnlyEnqueuesCleanup() {
        ConnectContext context = new ConnectContext();
        ManualCleanupScheduler scheduler = new ManualCleanupScheduler();
        FakeCleanupClient cleanupClient = new FakeCleanupClient();
        StarRocksRemoteScanSessionManager.setCleanupSchedulerForTest(scheduler);
        StarRocksRemoteScanSessionManager.setCleanupClientFactoryForTest(
                (feThriftUrl, scanTransport, feUser, fePassword, feThriftTimeoutMs, feThriftRetryTimes) ->
                        cleanupClient);

        StarRocksRemoteScanSessionManager.registerCleanupOnQueryFinished(context, "127.0.0.1:9020/",
                "arrow-flight", "user", "password", 10, 1, "session-1");

        context.onQueryFinished();

        Assertions.assertEquals(1, scheduler.pendingTaskCount());
        Assertions.assertEquals(0, cleanupClient.releaseCount.get());
        Assertions.assertEquals(0, cleanupClient.cancelCount.get());

        scheduler.runAll();

        Assertions.assertEquals(1, cleanupClient.releaseCount.get());
        Assertions.assertEquals(0, cleanupClient.cancelCount.get());
        Assertions.assertEquals(0, StarRocksRemoteScanSessionManager.registeredCleanupCountForTest());
    }

    @Test
    public void testDuplicateSessionCleanupSuppressedUntilWorkerFinishes() {
        ConnectContext firstContext = new ConnectContext();
        ConnectContext secondContext = new ConnectContext();
        ManualCleanupScheduler scheduler = new ManualCleanupScheduler();
        FakeCleanupClient cleanupClient = new FakeCleanupClient();
        StarRocksRemoteScanSessionManager.setCleanupSchedulerForTest(scheduler);
        StarRocksRemoteScanSessionManager.setCleanupClientFactoryForTest(
                (feThriftUrl, scanTransport, feUser, fePassword, feThriftTimeoutMs, feThriftRetryTimes) ->
                        cleanupClient);

        StarRocksRemoteScanSessionManager.registerCleanupOnQueryFinished(firstContext, "127.0.0.1:9020",
                "arrow-flight", "user", "password", 10, 1, "session-1");
        firstContext.onQueryFinished();
        StarRocksRemoteScanSessionManager.registerCleanupOnQueryFinished(secondContext, "127.0.0.1:9020/",
                "arrow-flight", "user", "password", 10, 1, "session-1");
        secondContext.onQueryFinished();

        Assertions.assertEquals(1, scheduler.pendingTaskCount());
        Assertions.assertEquals(1, StarRocksRemoteScanSessionManager.registeredCleanupCountForTest());

        scheduler.runAll();

        Assertions.assertEquals(1, cleanupClient.releaseCount.get());
        Assertions.assertEquals(0, StarRocksRemoteScanSessionManager.registeredCleanupCountForTest());
    }

    @Test
    public void testCancelSelectedForKilledOrErrorState() {
        ConnectContext killedContext = new ConnectContext();
        killedContext.setKilled();
        ConnectContext successContext = new ConnectContext();
        ManualCleanupScheduler scheduler = new ManualCleanupScheduler();
        FakeCleanupClient cleanupClient = new FakeCleanupClient();
        StarRocksRemoteScanSessionManager.setCleanupSchedulerForTest(scheduler);
        StarRocksRemoteScanSessionManager.setCleanupClientFactoryForTest(
                (feThriftUrl, scanTransport, feUser, fePassword, feThriftTimeoutMs, feThriftRetryTimes) ->
                        cleanupClient);

        StarRocksRemoteScanSessionManager.registerCleanupOnQueryFinished(killedContext, "127.0.0.1:9020",
                "arrow-flight", "user", "password", 10, 1, "session-cancel");
        StarRocksRemoteScanSessionManager.registerCleanupOnQueryFinished(successContext, "127.0.0.1:9020",
                "arrow-flight", "user", "password", 10, 1, "session-release");

        killedContext.onQueryFinished();
        successContext.onQueryFinished();
        scheduler.runAll();

        Assertions.assertEquals(1, cleanupClient.cancelCount.get());
        Assertions.assertEquals(1, cleanupClient.releaseCount.get());
    }

    @Test
    public void testBatchGroupsBySameEndpoint() {
        ConnectContext c1 = new ConnectContext();
        ConnectContext c2 = new ConnectContext();
        ManualCleanupScheduler scheduler = new ManualCleanupScheduler();
        FakeCleanupClient cleanupClient = new FakeCleanupClient();
        StarRocksRemoteScanSessionManager.setCleanupSchedulerForTest(scheduler);
        StarRocksRemoteScanSessionManager.setCleanupClientFactoryForTest(
                (feThriftUrl, scanTransport, feUser, fePassword, feThriftTimeoutMs, feThriftRetryTimes) ->
                        cleanupClient);

        StarRocksRemoteScanSessionManager.registerCleanupOnQueryFinished(c1, "127.0.0.1:9020",
                "arrow-flight", "user", "password", 10, 1, "session-a");
        StarRocksRemoteScanSessionManager.registerCleanupOnQueryFinished(c2, "127.0.0.1:9020",
                "arrow-flight", "user", "password", 10, 1, "session-b");
        c1.onQueryFinished();
        c2.onQueryFinished();

        scheduler.runAll();

        Assertions.assertEquals(1, cleanupClient.batchCallCount.get());
        Assertions.assertEquals(2, cleanupClient.releaseCount.get());
    }

    // H-4: Two cleanup tasks targeting DIFFERENT remote-FE endpoints → dispatchBatch emits TWO separate
    // batchCleanupScanSessions RPCs (one per endpoint), each containing only its endpoint's items.
    @Test
    public void testDispatchBatchSplitsItemsByEndpoint() {
        ConnectContext c1 = new ConnectContext();
        ConnectContext c2 = new ConnectContext();
        ManualCleanupScheduler scheduler = new ManualCleanupScheduler();

        // Track per-endpoint calls: list of (feThriftUrl, sessionIds-in-batch) pairs.
        List<String> calledUrls = new CopyOnWriteArrayList<>();
        List<List<String>> capturedBatches = new CopyOnWriteArrayList<>();

        StarRocksRemoteScanSessionManager.setCleanupSchedulerForTest(scheduler);
        StarRocksRemoteScanSessionManager.setCleanupClientFactoryForTest(
                (feThriftUrl, scanTransport, feUser, fePassword, feThriftTimeoutMs, feThriftRetryTimes) -> items -> {
                    calledUrls.add(feThriftUrl);
                    List<String> ids = new ArrayList<>();
                    for (StarRocksRemoteScanWire.CleanupItem item : items) {
                        ids.add(item.sessionId);
                    }
                    capturedBatches.add(ids);
                });

        StarRocksRemoteScanSessionManager.registerCleanupOnQueryFinished(c1, "fe-a:9020",
                "arrow-flight", "user", "password", 10, 1, "session-on-a");
        StarRocksRemoteScanSessionManager.registerCleanupOnQueryFinished(c2, "fe-b:9020",
                "arrow-flight", "user", "password", 10, 1, "session-on-b");
        c1.onQueryFinished();
        c2.onQueryFinished();

        scheduler.runAll();

        // Exactly two RPCs, one per endpoint.
        Assertions.assertEquals(2, calledUrls.size());
        Set<String> urlSet = new HashSet<>(calledUrls);
        Assertions.assertTrue(urlSet.contains("fe-a:9020"), "Expected fe-a:9020 in " + urlSet);
        Assertions.assertTrue(urlSet.contains("fe-b:9020"), "Expected fe-b:9020 in " + urlSet);

        // Each batch contains exactly one item belonging to the correct endpoint.
        for (int i = 0; i < calledUrls.size(); i++) {
            String url = calledUrls.get(i);
            List<String> batch = capturedBatches.get(i);
            Assertions.assertEquals(1, batch.size(),
                    "Batch for " + url + " should have exactly 1 item");
            if ("fe-a:9020".equals(url)) {
                Assertions.assertEquals("session-on-a", batch.get(0));
            } else {
                Assertions.assertEquals("session-on-b", batch.get(0));
            }
        }
    }

    // H-6: A full batch of BATCH_SIZE = 100 tasks at the same endpoint → dispatched as a single
    // batchCleanupScanSessions RPC with all 100 items.
    // Note: the production BATCH_SIZE constraint is enforced by runLoop calling drainTo(batch, BATCH_SIZE - 1).
    // ManualCleanupScheduler.runAll() bypasses runLoop and calls dispatchBatch directly with all accumulated
    // tasks. dispatchBatch groups by endpoint without capping; so 100 tasks at one endpoint → one RPC
    // containing 100 items. This validates the end-to-end shape for a full-batch scenario.
    @Test
    public void testFullBatchAtSameEndpointProducesOneRpc() {
        ManualCleanupScheduler scheduler = new ManualCleanupScheduler();
        FakeCleanupClient cleanupClient = new FakeCleanupClient();
        StarRocksRemoteScanSessionManager.setCleanupSchedulerForTest(scheduler);
        StarRocksRemoteScanSessionManager.setCleanupClientFactoryForTest(
                (feThriftUrl, scanTransport, feUser, fePassword, feThriftTimeoutMs, feThriftRetryTimes) ->
                        cleanupClient);

        // DEFAULT_BATCH_SIZE = 100 (production constant in StarRocksRemoteScanSessionManager)
        int batchSize = 100;
        for (int i = 0; i < batchSize; i++) {
            ConnectContext ctx = new ConnectContext();
            StarRocksRemoteScanSessionManager.registerCleanupOnQueryFinished(ctx, "fe:9020",
                    "arrow-flight", "user", "password", 10, 1, "session-" + i);
            ctx.onQueryFinished();
        }

        scheduler.runAll();

        Assertions.assertEquals(1, cleanupClient.batchCallCount.get(),
                "All 100 same-endpoint tasks should be dispatched in exactly one RPC");
        Assertions.assertEquals(batchSize, cleanupClient.releaseCount.get(),
                "All 100 tasks should be released");
    }

    // H-7: When cleanupScheduler.schedule(...) returns false (queue full), registerCleanupOnQueryFinished's
    // listener emits a WARN log naming the dropped session id and the endpoint URL.
    // Also: the REGISTERED_CLEANUPS count should drop back to 0 after the failure.
    @Test
    public void testQueueFullEmitsWarnAndClearsRegistry() {
        ConnectContext context = new ConnectContext();
        FullCleanupScheduler fullScheduler = new FullCleanupScheduler();
        StarRocksRemoteScanSessionManager.setCleanupSchedulerForTest(fullScheduler);

        // Attach a capturing appender to the StarRocksRemoteScanSessionManager logger.
        org.apache.logging.log4j.core.Logger coreLogger =
                (org.apache.logging.log4j.core.Logger) LogManager.getLogger(
                        StarRocksRemoteScanSessionManager.class);
        CopyOnWriteArrayList<LogEvent> captured = new CopyOnWriteArrayList<>();
        AbstractAppender capturingAppender = new AbstractAppender(
                "CapturingAppender", null, null, true, Property.EMPTY_ARRAY) {
            @Override
            public void append(LogEvent event) {
                captured.add(event.toImmutable());
            }
        };
        capturingAppender.start();
        coreLogger.addAppender(capturingAppender);

        try {
            StarRocksRemoteScanSessionManager.registerCleanupOnQueryFinished(context,
                    "fe-full:9020", "arrow-flight", "user", "password", 10, 1, "session-dropped");
            // Trigger the listener; schedule() will return false → WARN log expected.
            context.onQueryFinished();
        } finally {
            coreLogger.removeAppender(capturingAppender);
            capturingAppender.stop();
        }

        // WARN log must mention the session id and the endpoint URL.
        boolean foundWarn = captured.stream().anyMatch(e ->
                e.getLevel() == Level.WARN
                        && e.getMessage().getFormattedMessage().contains("session-dropped")
                        && e.getMessage().getFormattedMessage().contains("fe-full:9020"));
        Assertions.assertTrue(foundWarn,
                "Expected WARN log mentioning 'session-dropped' and 'fe-full:9020', "
                        + "but none found in: " + captured);

        // The failed enqueue must remove the key from REGISTERED_CLEANUPS.
        Assertions.assertEquals(0, StarRocksRemoteScanSessionManager.registeredCleanupCountForTest(),
                "REGISTERED_CLEANUPS should be empty after queue-full failure");
    }

    private static class ManualCleanupScheduler implements StarRocksRemoteScanSessionManager.CleanupScheduler {
        private final Queue<StarRocksRemoteScanSessionManager.CleanupTask> tasks = new ArrayDeque<>();

        @Override
        public boolean schedule(StarRocksRemoteScanSessionManager.CleanupTask task) {
            return tasks.offer(task);
        }

        @Override
        public int pendingTaskCount() {
            return tasks.size();
        }

        private void runAll() {
            List<StarRocksRemoteScanSessionManager.CleanupTask> batch = new ArrayList<>(tasks);
            tasks.clear();
            StarRocksRemoteScanSessionManager.dispatchBatch(batch);
        }
    }

    private static class FakeCleanupClient implements StarRocksRemoteScanSessionManager.CleanupClient {
        private final AtomicInteger releaseCount = new AtomicInteger();
        private final AtomicInteger cancelCount = new AtomicInteger();
        private final AtomicInteger batchCallCount = new AtomicInteger();

        @Override
        public void batchCleanupScanSessions(List<StarRocksRemoteScanWire.CleanupItem> items) {
            batchCallCount.incrementAndGet();
            List<StarRocksRemoteScanWire.CleanupItem> list = items == null ? Collections.emptyList() : items;
            for (StarRocksRemoteScanWire.CleanupItem item : list) {
                if (item.cancel) {
                    cancelCount.incrementAndGet();
                } else {
                    releaseCount.incrementAndGet();
                }
            }
        }
    }

    /** A CleanupScheduler whose schedule() always returns false, simulating a full queue. */
    private static class FullCleanupScheduler implements StarRocksRemoteScanSessionManager.CleanupScheduler {
        @Override
        public boolean schedule(StarRocksRemoteScanSessionManager.CleanupTask task) {
            return false;
        }

        @Override
        public int pendingTaskCount() {
            return 0;
        }
    }
}
