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

package com.starrocks.rpc;

import com.starrocks.common.Config;
import com.starrocks.common.util.concurrent.lock.LockHoldDepth;
import com.starrocks.common.util.concurrent.lock.LockInvariantViolations;
import com.starrocks.common.util.concurrent.lock.LockManager;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.server.GlobalStateMgr;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * The door on a brpc wait: it has to fire when a lock holder is about to block on a response, and
 * stay silent for the two cases that are not waits -- no lock, and a future that is already done.
 */
public class GuardedFutureTest {
    private static final long INTERNAL_DB_ID = 30001L;
    private static final String TRANSPORT = "lake-vacuum";

    private String savedMode;

    @BeforeEach
    public void setUp() {
        GlobalStateMgr.getCurrentState().setLockManager(new LockManager());
        savedMode = Config.lock_blocking_call_validation_mode;
        LockInvariantViolations.clearViolations();
        LockHoldDepth.reset();
    }

    @AfterEach
    public void tearDown() {
        Config.lock_blocking_call_validation_mode = savedMode;
        LockInvariantViolations.clearViolations();
        LockHoldDepth.reset();
    }

    @Test
    public void testWaitingUnderALockIsReported() throws Exception {
        Config.lock_blocking_call_validation_mode = "warn";
        // Still in flight when the wait starts, and answered while the caller sits in get() -- a
        // response that arrives, not one that was already in hand. The delay is generous because
        // what must hold is only that the future is pending at the instant get() is entered; the
        // test still finishes as soon as the value lands.
        CompletableFuture<String> pending = CompletableFuture.supplyAsync(() -> {
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return "response";
        });
        Future<String> guarded = GuardedFuture.guard(pending, TRANSPORT);

        Locker locker = new Locker();
        locker.lockDatabase(INTERNAL_DB_ID, LockType.READ);
        try {
            Assertions.assertEquals("response", guarded.get(30, TimeUnit.SECONDS));
        } finally {
            locker.unLockDatabase(INTERNAL_DB_ID, LockType.READ);
        }

        Assertions.assertEquals(1, LockInvariantViolations.totalViolations());
    }

    /**
     * The report has to name the code that decided to wait under a lock. {@code GuardedFuture} is
     * the transport here, so its own frames are skipped -- the same property every other door has,
     * and the reason the wrapper is in {@code com.starrocks.rpc} rather than in the lock package,
     * whose frames are skipped as the lock layer's own and would shift the answer by one frame.
     */
    @Test
    public void testTheReportNamesTheCallerNotTheWrapper() {
        Config.lock_blocking_call_validation_mode = "warn";
        Future<String> guarded = GuardedFuture.guard(new CompletableFuture<>(), TRANSPORT);

        Locker locker = new Locker();
        locker.lockDatabase(INTERNAL_DB_ID, LockType.READ);
        try {
            Assertions.assertThrows(Exception.class, () -> guarded.get(1, TimeUnit.MILLISECONDS));
        } finally {
            locker.unLockDatabase(INTERNAL_DB_ID, LockType.READ);
        }

        Map<String, Long> bySite = LockInvariantViolations.violationsBySite();
        Assertions.assertEquals(1, bySite.size());
        String site = bySite.keySet().iterator().next();
        Assertions.assertTrue(site.contains(GuardedFutureTest.class.getName()),
                "the report should name the caller, but named: " + site);
        // The trailing dot matters: without it "GuardedFutureTest" would match the wrapper's own name
        // and the assertion would be vacuous.
        Assertions.assertFalse(site.contains(GuardedFuture.class.getName() + "."),
                "the report named the wrapper instead of its caller: " + site);
    }

    /**
     * A response that is already in hand is not a wait, so reporting it would be the same false
     * positive as reporting a cache hit. This is the case that makes the door usable at all: the
     * publish path collects a batch of futures and then walks them, and by the time it reaches the
     * later ones most have long since completed.
     */
    @Test
    public void testACompletedFutureIsNotAWait() throws ExecutionException, InterruptedException {
        Config.lock_blocking_call_validation_mode = "error";
        Future<String> guarded = GuardedFuture.guard(CompletableFuture.completedFuture("response"), TRANSPORT);

        Locker locker = new Locker();
        locker.lockDatabase(INTERNAL_DB_ID, LockType.READ);
        try {
            Assertions.assertEquals("response", guarded.get());
        } finally {
            locker.unLockDatabase(INTERNAL_DB_ID, LockType.READ);
        }
        Assertions.assertEquals(0, LockInvariantViolations.totalViolations());
    }

    @Test
    public void testWaitingWithNoLockHeldIsFine() {
        Config.lock_blocking_call_validation_mode = "error";
        Future<String> guarded = GuardedFuture.guard(new CompletableFuture<>(), TRANSPORT);
        Assertions.assertThrows(Exception.class, () -> guarded.get(1, TimeUnit.MILLISECONDS));
        Assertions.assertEquals(0, LockInvariantViolations.totalViolations());
    }

    /** In error mode the wait is refused, and the message says which request it was. */
    @Test
    public void testErrorModeRefusesAndNamesTheTransport() {
        Config.lock_blocking_call_validation_mode = "error";
        Future<String> guarded = GuardedFuture.guard(new CompletableFuture<>(), TRANSPORT);

        Locker locker = new Locker();
        locker.lockDatabase(INTERNAL_DB_ID, LockType.READ);
        try {
            IllegalStateException e = Assertions.assertThrows(IllegalStateException.class, guarded::get);
            Assertions.assertTrue(e.getMessage().contains(TRANSPORT), e.getMessage());
        } finally {
            locker.unLockDatabase(INTERNAL_DB_ID, LockType.READ);
        }
    }

    /** Everything that is not a wait delegates untouched, including cancellation. */
    @Test
    public void testTheWrapperIsOtherwiseTransparent() {
        CompletableFuture<String> delegate = new CompletableFuture<>();
        Future<String> guarded = GuardedFuture.guard(delegate, TRANSPORT);
        Assertions.assertFalse(guarded.isDone());
        Assertions.assertFalse(guarded.isCancelled());
        Assertions.assertTrue(guarded.cancel(true));
        Assertions.assertTrue(delegate.isCancelled());
        Assertions.assertTrue(guarded.isCancelled());
        Assertions.assertTrue(guarded.isDone());
    }
}
