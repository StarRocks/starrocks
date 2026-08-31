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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Tests for the startup readiness gate. The coordinator is handed plain futures, so these exercise the gate
 * itself without needing a catalog or any mv. Every test has a timeout because the failure mode of this class
 * is hanging, not failing.
 */
public class MvStartupActivationCoordinatorTest {

    private List<CompletableFuture<?>> batch(CompletableFuture<?>... futures) {
        List<CompletableFuture<?>> result = new ArrayList<>();
        Collections.addAll(result, futures);
        return result;
    }

    private Thread awaitOnAnotherThread(MvStartupActivationCoordinator coordinator, AtomicBoolean released) {
        Thread waiter = new Thread(() -> {
            try {
                coordinator.await();
                released.set(true);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }, "gate-waiter");
        waiter.start();
        return waiter;
    }

    @Test
    @Timeout(30)
    public void testGateBlocksUntilEveryActivationIsDone() throws Exception {
        CompletableFuture<Void> first = new CompletableFuture<>();
        CompletableFuture<Void> second = new CompletableFuture<>();

        MvStartupActivationCoordinator coordinator = new MvStartupActivationCoordinator();
        coordinator.submit(batch(first, second));
        Assertions.assertTrue(coordinator.isPending());
        Assertions.assertEquals(2, coordinator.getTotalMvs());

        AtomicBoolean released = new AtomicBoolean(false);
        Thread waiter = awaitOnAnotherThread(coordinator, released);

        waiter.join(500);
        Assertions.assertTrue(waiter.isAlive(), "the gate was released while activations were still running");

        // One of two done is still not enough to open the ports.
        first.complete(null);
        waiter.join(500);
        Assertions.assertTrue(waiter.isAlive(), "the gate was released after only part of the batch finished");
        Assertions.assertEquals(1, coordinator.getCompletedMvs());

        second.complete(null);
        waiter.join(20_000);
        Assertions.assertFalse(waiter.isAlive());
        Assertions.assertTrue(released.get(), "the gate never released after the batch drained");
        Assertions.assertFalse(coordinator.isPending());
        Assertions.assertEquals(2, coordinator.getCompletedMvs());
    }

    /**
     * An activation task that throws completes its future exceptionally. The gate's contract is that the batch
     * has drained, not that every mv activated, so one failure must neither block the gate nor hide the rest of
     * the batch.
     */
    @Test
    @Timeout(30)
    public void testExceptionalActivationStillReleasesTheGate() throws Exception {
        CompletableFuture<Void> failed = new CompletableFuture<>();
        failed.completeExceptionally(new IllegalStateException("activation blew up"));
        CompletableFuture<Void> healthy = CompletableFuture.completedFuture(null);

        MvStartupActivationCoordinator coordinator = new MvStartupActivationCoordinator();
        coordinator.submit(batch(failed, healthy));

        coordinator.await();

        Assertions.assertFalse(coordinator.isPending());
        Assertions.assertEquals(2, coordinator.getCompletedMvs(),
                "a failed activation was not counted as drained");
    }

    /**
     * A failure in the middle of the batch must not stop the gate from waiting for the ones behind it.
     */
    @Test
    @Timeout(30)
    public void testGateStillWaitsForTheRestAfterOneFailure() throws Exception {
        CompletableFuture<Void> failed = new CompletableFuture<>();
        failed.completeExceptionally(new IllegalStateException("activation blew up"));
        CompletableFuture<Void> slow = new CompletableFuture<>();

        MvStartupActivationCoordinator coordinator = new MvStartupActivationCoordinator();
        coordinator.submit(batch(failed, slow));

        AtomicBoolean released = new AtomicBoolean(false);
        Thread waiter = awaitOnAnotherThread(coordinator, released);

        waiter.join(500);
        Assertions.assertTrue(waiter.isAlive(), "the gate gave up as soon as one activation failed");

        slow.complete(null);
        waiter.join(20_000);
        Assertions.assertTrue(released.get());
        Assertions.assertEquals(2, coordinator.getCompletedMvs());
    }

    @Test
    @Timeout(30)
    public void testEmptyBatchMakesTheGateANoOp() throws Exception {
        MvStartupActivationCoordinator coordinator = new MvStartupActivationCoordinator();
        coordinator.submit(Collections.emptyList());

        coordinator.await();
        Assertions.assertFalse(coordinator.isPending());
        Assertions.assertEquals(0, coordinator.getTotalMvs());
    }

    @Test
    @Timeout(30)
    public void testNullBatchMakesTheGateANoOp() throws Exception {
        MvStartupActivationCoordinator coordinator = new MvStartupActivationCoordinator();
        coordinator.submit(null);

        coordinator.await();
        Assertions.assertFalse(coordinator.isPending());
    }

    @Test
    @Timeout(30)
    public void testAwaitWithoutSubmitReturnsImmediately() throws Exception {
        MvStartupActivationCoordinator coordinator = new MvStartupActivationCoordinator();
        coordinator.await();
        Assertions.assertFalse(coordinator.isPending());
    }

    @Test
    @Timeout(30)
    public void testSecondSubmitIsIgnored() throws Exception {
        MvStartupActivationCoordinator coordinator = new MvStartupActivationCoordinator();
        coordinator.submit(batch(CompletableFuture.completedFuture(null)));
        coordinator.submit(batch(CompletableFuture.completedFuture(null),
                CompletableFuture.completedFuture(null)));

        coordinator.await();
        Assertions.assertEquals(1, coordinator.getTotalMvs(), "the second submit overwrote the first one");
        Assertions.assertEquals(1, coordinator.getCompletedMvs());
    }
}
