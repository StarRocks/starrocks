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

import com.starrocks.catalog.Column;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.sql.ast.KeysType;
import com.starrocks.type.IntegerType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import java.util.function.Function;

/**
 * Tests for the startup activation coordinator: the layer-by-layer scheduling and completion tracking.
 * The activator is a fake that hands back futures the tests complete by hand, so neither a catalog nor a real
 * activation is needed. Every test has a timeout because the failure mode of this class is hanging, not failing.
 */
public class MvStartupActivationCoordinatorTest {

    private Thread awaitOnAnotherThread(MvStartupActivationCoordinator coordinator, AtomicBoolean released) {
        Thread waiter = new Thread(() -> {
            try {
                coordinator.await();
                released.set(true);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }, "activation-waiter");
        waiter.start();
        return waiter;
    }

    @Test
    @Timeout(30)
    public void testCompletionWaitBlocksUntilEveryActivationIsDone() throws Exception {
        MaterializedView mv1 = mv(1);
        MaterializedView mv2 = mv(2);
        CompletableFuture<Void> first = new CompletableFuture<>();
        CompletableFuture<Void> second = new CompletableFuture<>();
        FakeActivator activator = new FakeActivator().returning(mv1, first).returning(mv2, second);

        MvStartupActivationCoordinator coordinator = new MvStartupActivationCoordinator();
        coordinator.schedule(List.of(List.of(mv1, mv2)), activator);
        Assertions.assertTrue(coordinator.isPending());
        Assertions.assertEquals(2, coordinator.getTotalMvs());
        waitUntil(() -> activator.invocations.size() == 2, "the layer was never submitted");

        AtomicBoolean released = new AtomicBoolean(false);
        Thread waiter = awaitOnAnotherThread(coordinator, released);

        waiter.join(500);
        Assertions.assertTrue(waiter.isAlive(), "the completion wait was released while activations were still running");

        // One of two done is still not enough to complete the batch.
        first.complete(null);
        waiter.join(500);
        Assertions.assertTrue(waiter.isAlive(), "the completion wait was released after only part of the batch finished");
        Assertions.assertEquals(1, coordinator.getCompletedMvs());

        second.complete(null);
        waiter.join(WAIT_TIMEOUT_MS);
        Assertions.assertFalse(waiter.isAlive());
        Assertions.assertTrue(released.get(), "the completion wait never released after the batch drained");
        Assertions.assertFalse(coordinator.isPending());
        Assertions.assertEquals(2, coordinator.getCompletedMvs());
    }

    /**
     * An activation task that throws completes its future exceptionally. The completion wait's contract is that the batch
     * has drained, not that every mv activated, so one failure must neither block the completion wait nor hide the rest of
     * the batch.
     */
    @Test
    @Timeout(30)
    public void testExceptionalActivationStillReleasesTheCompletionWait() throws Exception {
        MaterializedView mv1 = mv(1);
        MaterializedView mv2 = mv(2);
        CompletableFuture<Void> failed = new CompletableFuture<>();
        failed.completeExceptionally(new IllegalStateException("activation blew up"));
        FakeActivator activator = new FakeActivator().returning(mv1, failed);

        MvStartupActivationCoordinator coordinator = new MvStartupActivationCoordinator();
        coordinator.schedule(List.of(List.of(mv1, mv2)), activator);

        coordinator.await();

        Assertions.assertFalse(coordinator.isPending());
        Assertions.assertEquals(2, coordinator.getCompletedMvs(),
                "a failed activation was not counted as drained");
    }

    /**
     * A failure in the middle of the batch must not stop the completion wait from waiting for the ones behind it.
     */
    @Test
    @Timeout(30)
    public void testCompletionWaitStillWaitsForTheRestAfterOneFailure() throws Exception {
        MaterializedView mv1 = mv(1);
        MaterializedView mv2 = mv(2);
        CompletableFuture<Void> failed = new CompletableFuture<>();
        failed.completeExceptionally(new IllegalStateException("activation blew up"));
        CompletableFuture<Void> slow = new CompletableFuture<>();
        FakeActivator activator = new FakeActivator().returning(mv1, failed).returning(mv2, slow);

        MvStartupActivationCoordinator coordinator = new MvStartupActivationCoordinator();
        coordinator.schedule(List.of(List.of(mv1, mv2)), activator);

        AtomicBoolean released = new AtomicBoolean(false);
        Thread waiter = awaitOnAnotherThread(coordinator, released);

        waiter.join(500);
        Assertions.assertTrue(waiter.isAlive(), "the completion wait gave up as soon as one activation failed");

        slow.complete(null);
        waiter.join(WAIT_TIMEOUT_MS);
        Assertions.assertTrue(released.get());
        Assertions.assertEquals(2, coordinator.getCompletedMvs());
    }

    @Test
    @Timeout(30)
    public void testAwaitWithoutScheduleReturnsImmediately() throws Exception {
        MvStartupActivationCoordinator coordinator = new MvStartupActivationCoordinator();
        coordinator.await();
        Assertions.assertFalse(coordinator.isPending());
    }

    // ---------------------------------------------------------------------------------------------------------
    // Layered scheduling. The activator below is a fake: it records who called it and hands back futures the
    // test completes by hand, so the layer barrier can be observed without a catalog or a real activation.
    // ---------------------------------------------------------------------------------------------------------

    private static final long WAIT_TIMEOUT_MS = 20_000L;
    // How long a negative check ("not submitted yet") gives the coordinator thread to do the wrong thing.
    private static final long QUIET_PERIOD_MS = 300L;

    private static MaterializedView mv(long id) {
        List<Column> columns = new ArrayList<>();
        columns.add(new Column("c1", IntegerType.INT));
        return new MaterializedView(id, 1L, "mv_" + id, columns, KeysType.DUP_KEYS, null, null, null);
    }

    private static void waitUntil(BooleanSupplier condition, String message) throws InterruptedException {
        long deadline = System.currentTimeMillis() + WAIT_TIMEOUT_MS;
        while (!condition.getAsBoolean()) {
            if (System.currentTimeMillis() > deadline) {
                Assertions.fail(message);
            }
            Thread.sleep(10);
        }
    }

    private static class FakeActivator implements Function<MaterializedView, CompletableFuture<?>> {
        private final Map<Long, CompletableFuture<?>> results = new ConcurrentHashMap<>();
        private final Map<Long, Throwable> throwables = new ConcurrentHashMap<>();
        private final List<Long> invocations = new CopyOnWriteArrayList<>();
        private final List<Thread> threads = new CopyOnWriteArrayList<>();

        FakeActivator returning(MaterializedView mv, CompletableFuture<?> result) {
            results.put(mv.getId(), result);
            return this;
        }

        FakeActivator throwing(MaterializedView mv, Throwable throwable) {
            throwables.put(mv.getId(), throwable);
            return this;
        }

        @Override
        public CompletableFuture<?> apply(MaterializedView mv) {
            invocations.add(mv.getId());
            threads.add(Thread.currentThread());
            Throwable throwable = throwables.get(mv.getId());
            if (throwable instanceof RuntimeException) {
                throw (RuntimeException) throwable;
            }
            if (throwable instanceof Error) {
                throw (Error) throwable;
            }
            CompletableFuture<?> result = results.get(mv.getId());
            return result != null ? result : CompletableFuture.completedFuture(null);
        }

        int invocationCount(MaterializedView mv) {
            return (int) invocations.stream().filter(id -> id == mv.getId()).count();
        }
    }

    /**
     * The caller is the image loading thread, and journal replay only starts once it returns: schedule() must
     * hand the batch over and return while the first layer is still running.
     */
    @Test
    @Timeout(30)
    public void testScheduleReturnsWithoutWaitingForAnyLayer() throws Exception {
        MaterializedView mv1 = mv(1);
        MaterializedView mv2 = mv(2);
        CompletableFuture<Void> slow = new CompletableFuture<>();
        FakeActivator activator = new FakeActivator().returning(mv1, slow);

        MvStartupActivationCoordinator coordinator = new MvStartupActivationCoordinator();
        CompletableFuture<Void> done = coordinator.schedule(List.of(List.of(mv1), List.of(mv2)), activator);

        Assertions.assertFalse(done.isDone(), "schedule() waited for the batch instead of returning");
        Assertions.assertTrue(coordinator.isPending());
        Assertions.assertEquals(2, coordinator.getTotalMvs());
        Assertions.assertEquals(2, coordinator.getTotalLayers());
        Assertions.assertEquals(0, coordinator.getCompletedLayers());

        slow.complete(null);
        done.get(WAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        Assertions.assertFalse(coordinator.isPending());
        Assertions.assertEquals(2, coordinator.getCompletedMvs());
        Assertions.assertEquals(2, coordinator.getCompletedLayers());
    }

    /**
     * The point of the layering: no mv of layer k+1 is handed to the activator while any task of layer k is
     * still running, so no pool task ever waits for a base mv that is queued behind it.
     */
    @Test
    @Timeout(30)
    public void testNextLayerStartsOnlyAfterEveryTaskOfThePreviousLayer() throws Exception {
        MaterializedView mv1 = mv(1);
        MaterializedView mv2 = mv(2);
        MaterializedView mv3 = mv(3);
        CompletableFuture<Void> first = new CompletableFuture<>();
        CompletableFuture<Void> second = new CompletableFuture<>();
        CompletableFuture<Void> third = new CompletableFuture<>();
        FakeActivator activator = new FakeActivator()
                .returning(mv1, first)
                .returning(mv2, second)
                .returning(mv3, third);

        MvStartupActivationCoordinator coordinator = new MvStartupActivationCoordinator();
        CompletableFuture<Void> done = coordinator.schedule(List.of(List.of(mv1, mv2), List.of(mv3)), activator);

        waitUntil(() -> activator.invocationCount(mv1) == 1 && activator.invocationCount(mv2) == 1,
                "layer 1 was never submitted");
        Thread.sleep(QUIET_PERIOD_MS);
        Assertions.assertEquals(0, activator.invocationCount(mv3), "layer 2 was submitted while layer 1 was running");

        // One of two done is still not enough to start the next layer.
        first.complete(null);
        Thread.sleep(QUIET_PERIOD_MS);
        Assertions.assertEquals(0, activator.invocationCount(mv3),
                "layer 2 was submitted after only part of layer 1 finished");
        Assertions.assertEquals(1, coordinator.getCompletedMvs());
        Assertions.assertEquals(0, coordinator.getCompletedLayers());

        second.complete(null);
        waitUntil(() -> activator.invocationCount(mv3) == 1, "layer 2 was never submitted after layer 1 finished");
        Assertions.assertEquals(1, coordinator.getCompletedLayers());
        Assertions.assertFalse(done.isDone(), "the batch finished before its last layer did");
        Assertions.assertTrue(coordinator.isPending());

        third.complete(null);
        done.get(WAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        Assertions.assertEquals(List.of(1L, 2L, 3L), activator.invocations);
        Assertions.assertEquals(3, coordinator.getCompletedMvs());
        Assertions.assertEquals(2, coordinator.getCompletedLayers());
        Assertions.assertFalse(coordinator.isPending());
    }

    /**
     * An activation task that fails is a finished task: the next layer must still be submitted and the completion wait
     * must still release.
     */
    @Test
    @Timeout(30)
    public void testFailedActivationStillLetsTheNextLayerRun() throws Exception {
        MaterializedView mv1 = mv(1);
        MaterializedView mv2 = mv(2);
        MaterializedView mv3 = mv(3);
        CompletableFuture<Void> failed = new CompletableFuture<>();
        failed.completeExceptionally(new IllegalStateException("activation blew up"));
        FakeActivator activator = new FakeActivator().returning(mv1, failed);

        MvStartupActivationCoordinator coordinator = new MvStartupActivationCoordinator();
        CompletableFuture<Void> done = coordinator.schedule(List.of(List.of(mv1, mv2), List.of(mv3)), activator);

        coordinator.await();

        Assertions.assertTrue(done.isDone());
        Assertions.assertFalse(done.isCompletedExceptionally());
        Assertions.assertEquals(1, activator.invocationCount(mv3), "a failure in layer 1 stopped layer 2");
        Assertions.assertEquals(3, coordinator.getCompletedMvs(), "a failed activation was not counted as done");
        Assertions.assertEquals(2, coordinator.getCompletedLayers());
        Assertions.assertFalse(coordinator.isPending());
    }

    /**
     * An activator that throws while submitting one mv fails that mv only; the rest of its layer and the later
     * layers go on as usual, without falling back.
     */
    @Test
    @Timeout(30)
    public void testActivatorExceptionCountsAsOneFailedMv() throws Exception {
        MaterializedView mv1 = mv(1);
        MaterializedView mv2 = mv(2);
        MaterializedView mv3 = mv(3);
        FakeActivator activator = new FakeActivator().throwing(mv1, new IllegalStateException("submit blew up"));

        MvStartupActivationCoordinator coordinator = new MvStartupActivationCoordinator();
        CompletableFuture<Void> done = coordinator.schedule(List.of(List.of(mv1, mv2), List.of(mv3)), activator);
        done.get(WAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);

        Assertions.assertFalse(done.isCompletedExceptionally());
        Assertions.assertEquals(1, activator.invocationCount(mv1));
        Assertions.assertEquals(1, activator.invocationCount(mv2));
        Assertions.assertEquals(1, activator.invocationCount(mv3));
        Assertions.assertEquals(3, coordinator.getCompletedMvs(), "the mv whose submission threw was not counted");
        // Every layer completed normally: the exception did not push the coordinator onto its fallback path.
        Assertions.assertEquals(2, coordinator.getCompletedLayers());
    }

    /**
     * An Error escapes the per-mv handling and breaks the layering. The coordinator must then submit every mv it
     * has not handed to the activator yet, exactly once, wait for those and for what was already running, and
     * still complete.
     */
    @Test
    @Timeout(30)
    public void testActivatorErrorFallsBackToSubmittingTheRestAtOnce() throws Exception {
        MaterializedView mv1 = mv(1);
        MaterializedView mv2 = mv(2);
        MaterializedView mv3 = mv(3);
        MaterializedView mv4 = mv(4);
        MaterializedView mv5 = mv(5);
        CompletableFuture<Void> slow = new CompletableFuture<>();
        FakeActivator activator = new FakeActivator()
                .returning(mv1, slow)
                .throwing(mv2, new Error("coordinator blew up"));

        MvStartupActivationCoordinator coordinator = new MvStartupActivationCoordinator();
        CompletableFuture<Void> done = coordinator.schedule(
                List.of(List.of(mv1, mv2, mv3), List.of(mv4, mv5)), activator);

        waitUntil(() -> activator.invocations.size() == 5, "the remaining mvs were never submitted");
        for (MaterializedView mv : List.of(mv1, mv2, mv3, mv4, mv5)) {
            Assertions.assertEquals(1, activator.invocationCount(mv), mv.getName() + " was not submitted exactly once");
        }

        // mv1 was submitted before the failure and is still running: the batch is not over until it is.
        Thread.sleep(QUIET_PERIOD_MS);
        Assertions.assertFalse(done.isDone(), "the batch finished while an activation was still running");

        slow.complete(null);
        done.get(WAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        Assertions.assertFalse(done.isCompletedExceptionally());
        Assertions.assertEquals(5, activator.invocations.size(), "an mv was submitted again after the fallback");
        Assertions.assertEquals(0, coordinator.getCompletedLayers(), "the failed layer was counted as finished");
        coordinator.await();
        Assertions.assertFalse(coordinator.isPending());
    }

    /**
     * The layers run on the coordinator's own daemon thread: not on the caller, which must not block, and not on
     * a mv-plan-cache pool thread, where the barrier would hold back the tasks it waits for.
     */
    @Test
    @Timeout(30)
    public void testActivationRunsOnTheDedicatedThread() throws Exception {
        FakeActivator activator = new FakeActivator();

        MvStartupActivationCoordinator coordinator = new MvStartupActivationCoordinator();
        coordinator.schedule(List.of(List.of(mv(1)), List.of(mv(2))), activator)
                .get(WAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);

        Assertions.assertEquals(2, activator.threads.size());
        for (Thread thread : activator.threads) {
            Assertions.assertEquals("mv-startup-activation", thread.getName());
            Assertions.assertTrue(thread.isDaemon(), "the coordinator thread would keep the JVM alive");
            Assertions.assertNotSame(Thread.currentThread(), thread);
        }
    }

    /**
     * Only the batch of the process startup is tracked. Unit tests reload the mvs many times in one JVM, so a later
     * batch must still run -- untracked, without touching the coordinator's counts.
     */
    @Test
    @Timeout(30)
    public void testSecondScheduleRunsUntracked() throws Exception {
        MaterializedView mv1 = mv(1);
        MaterializedView mv2 = mv(2);
        MaterializedView mv3 = mv(3);
        CompletableFuture<Void> slow = new CompletableFuture<>();
        FakeActivator activator = new FakeActivator().returning(mv1, slow);

        MvStartupActivationCoordinator coordinator = new MvStartupActivationCoordinator();
        CompletableFuture<Void> first = coordinator.schedule(List.of(List.of(mv1)), activator);
        CompletableFuture<Void> second = coordinator.schedule(List.of(List.of(mv2), List.of(mv3)), activator);
        Assertions.assertEquals(1, coordinator.getTotalMvs(), "the second batch overwrote the tracked one");
        Assertions.assertEquals(1, coordinator.getTotalLayers());

        AtomicBoolean released = new AtomicBoolean(false);
        Thread waiter = awaitOnAnotherThread(coordinator, released);
        waiter.join(500);
        Assertions.assertTrue(waiter.isAlive(), "the completion wait was released while the tracked batch was still running");

        slow.complete(null);
        waiter.join(WAIT_TIMEOUT_MS);
        Assertions.assertTrue(released.get(), "the completion wait never released after the tracked batch drained");
        Assertions.assertTrue(first.isDone());

        second.get(WAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        Assertions.assertEquals(1, activator.invocationCount(mv2), "the second batch did not run");
        Assertions.assertEquals(1, activator.invocationCount(mv3), "the second batch did not run");
        Assertions.assertEquals(1, coordinator.getTotalMvs());
        Assertions.assertEquals(1, coordinator.getCompletedMvs(), "the untracked batch was counted by the coordinator");
        Assertions.assertEquals(1, coordinator.getCompletedLayers(), "the untracked batch was counted by the coordinator");
    }

    @Test
    @Timeout(30)
    public void testScheduleWithoutMvsMakesTheCompletionWaitANoOp() throws Exception {
        FakeActivator activator = new FakeActivator();
        MvStartupActivationCoordinator coordinator = new MvStartupActivationCoordinator();

        Assertions.assertTrue(coordinator.schedule(Collections.emptyList(), activator).isDone());
        Assertions.assertTrue(coordinator.schedule(null, activator).isDone());
        Assertions.assertTrue(coordinator.schedule(List.of(new ArrayList<>()), activator).isDone());

        coordinator.await();
        Assertions.assertFalse(coordinator.isPending());
        Assertions.assertEquals(0, coordinator.getTotalMvs());
        Assertions.assertEquals(0, coordinator.getTotalLayers());
        Assertions.assertTrue(activator.invocations.isEmpty());

        // Nothing was tracked, so the first batch that does carry mvs is still the one the coordinator follows.
        coordinator.schedule(List.of(List.of(mv(1))), activator).get(WAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        Assertions.assertEquals(1, coordinator.getTotalMvs());
        Assertions.assertEquals(1, coordinator.getTotalLayers());
    }
}
