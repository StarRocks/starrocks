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

package com.starrocks.scheduler.mv;

import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.MvRefreshArbiter;
import com.starrocks.catalog.MvUpdateInfo;
import com.starrocks.catalog.TableProperty;
import com.starrocks.catalog.mv.MVTimelinessArbiter;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.common.StarRocksPlannerException;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MVTimelinessMgrTest {
    private final MaterializedView mv = forceMv();
    private final MVTimelinessMgr mgr = new MVTimelinessMgr();
    private final AtomicInteger computeCount = new AtomicInteger();
    // What the timeliness check returns, given the number of the call (from 1).
    private Function<Integer, MvUpdateInfo> arbiter;
    private volatile boolean inCatalog = true;

    private static MaterializedView forceMv() {
        MaterializedView mv = mock(MaterializedView.class);
        TableProperty tableProperty = mock(TableProperty.class);
        when(tableProperty.getQueryRewriteConsistencyMode())
                .thenReturn(TableProperty.QueryRewriteConsistencyMode.FORCE_MV);
        when(mv.getTableProperty()).thenReturn(tableProperty);
        return mv;
    }

    @BeforeEach
    public void setUp() {
        new MockUp<MVTimelinessMgr>() {
            @Mock
            public boolean isEnableMVTimelinessGlobalCache(MaterializedView ignored) {
                return true;
            }

            @Mock
            public boolean isInCatalog(MaterializedView ignored) {
                return inCatalog;
            }
        };
        new MockUp<MvRefreshArbiter>() {
            @Mock
            public MvUpdateInfo getMVTimelinessUpdateInfo(MaterializedView ignored,
                                                          MVTimelinessArbiter.QueryRewriteParams params) {
                return arbiter.apply(computeCount.incrementAndGet());
            }
        };
    }

    private MvUpdateInfo lookup() {
        return mgr.getMVTimelinessInfo(mv, null);
    }

    @Test
    public void testCachesACompletedCheck() {
        arbiter = call -> MvUpdateInfo.noRefresh(mv);
        MvUpdateInfo first = lookup();
        Assertions.assertSame(first, lookup());
        Assertions.assertEquals(1, computeCount.get());
    }

    @Test
    public void testDoesNotCacheACheckThatARefreshInvalidatedMidway() {
        // The refresh publishes and invalidates the entry while the check is still reading the state before it: the
        // result describes that state, and caching it would keep it until the next refresh.
        arbiter = call -> {
            if (call == 1) {
                mgr.triggerEvent(mv, MVTimelinessMgr.MVChangeEvent.MV_REFRESHED);
                return MvUpdateInfo.fullRefresh(mv);
            }
            return MvUpdateInfo.noRefresh(mv);
        };
        Assertions.assertEquals(MvUpdateInfo.MvToRefreshType.FULL, lookup().getMVToRefreshType());
        Assertions.assertEquals(MvUpdateInfo.MvToRefreshType.NO_REFRESH, lookup().getMVToRefreshType(),
                "the result computed across the refresh was cached");
        Assertions.assertEquals(2, computeCount.get());
    }

    @Test
    public void testDoesNotCacheAFailedCheck() {
        // A failed check would otherwise keep the MV out of rewrite until the next refresh.
        arbiter = call -> call == 1 ? MvUpdateInfo.unknown(mv) : MvUpdateInfo.noRefresh(mv);
        Assertions.assertEquals(MvUpdateInfo.MvToRefreshType.UNKNOWN, lookup().getMVToRefreshType());
        Assertions.assertEquals(MvUpdateInfo.MvToRefreshType.NO_REFRESH, lookup().getMVToRefreshType());
        Assertions.assertSame(lookup(), lookup());
        Assertions.assertEquals(2, computeCount.get());
    }

    @Test
    public void testDoesNotCacheAThrowingCheck() {
        arbiter = call -> {
            if (call == 1) {
                throw new IllegalStateException("partition dropped mid-check");
            }
            return MvUpdateInfo.noRefresh(mv);
        };
        Assertions.assertThrows(IllegalStateException.class, this::lookup);
        Assertions.assertEquals(MvUpdateInfo.MvToRefreshType.NO_REFRESH, lookup().getMVToRefreshType());
        Assertions.assertEquals(2, computeCount.get());
    }

    @Test
    public void testKeepsACheckAcrossAnInvalidationOfAnotherMv() {
        // A refresh of another MV says nothing about this one.
        MaterializedView other = forceMv();
        arbiter = call -> {
            mgr.triggerEvent(other, MVTimelinessMgr.MVChangeEvent.MV_REFRESHED);
            return MvUpdateInfo.noRefresh(mv);
        };
        Assertions.assertSame(lookup(), lookup());
        Assertions.assertEquals(1, computeCount.get());
    }

    @Test
    public void testComputesOnceForConcurrentLookups() throws Exception {
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        arbiter = call -> {
            started.countDown();
            try {
                release.await();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            return MvUpdateInfo.noRefresh(mv);
        };
        AtomicReference<MvUpdateInfo> first = new AtomicReference<>();
        Thread computing = new Thread(() -> first.set(lookup()));
        computing.start();
        Assertions.assertTrue(started.await(10, TimeUnit.SECONDS));
        AtomicReference<MvUpdateInfo> second = new AtomicReference<>();
        Thread waiting = new Thread(() -> second.set(lookup()));
        waiting.start();
        // Let the second lookup reach the in-flight computation: it either waits on it or starts its own.
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (waiting.getState() != Thread.State.TIMED_WAITING && computeCount.get() < 2 && System.nanoTime() < deadline) {
            Thread.sleep(10);
        }
        release.countDown();
        computing.join();
        waiting.join();
        Assertions.assertEquals(1, computeCount.get(), "the concurrent lookup computed the timeliness again");
        Assertions.assertSame(first.get(), second.get());
    }

    @Test
    public void testRefreshOnAnotherThreadWhileACheckIsComputed() throws Exception {
        // The check runs on a query thread and the refresh publishes on another: a query waiting on the in-flight
        // check gets its result, but a query after the refresh must not.
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        arbiter = call -> {
            if (call == 1) {
                started.countDown();
                try {
                    release.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                return MvUpdateInfo.fullRefresh(mv);
            }
            return MvUpdateInfo.noRefresh(mv);
        };
        AtomicReference<MvUpdateInfo> beforeRefresh = new AtomicReference<>();
        Thread computing = new Thread(() -> beforeRefresh.set(lookup()));
        computing.start();
        Assertions.assertTrue(started.await(10, TimeUnit.SECONDS));

        Thread refresh = new Thread(() -> mgr.triggerEvent(mv, MVTimelinessMgr.MVChangeEvent.MV_REFRESHED));
        refresh.start();
        refresh.join();
        // Not blocked by the in-flight check: it computes against the state after the refresh.
        AtomicReference<MvUpdateInfo> afterRefresh = new AtomicReference<>();
        Thread after = new Thread(() -> afterRefresh.set(lookup()));
        after.start();
        after.join(TimeUnit.SECONDS.toMillis(10));
        boolean blocked = after.isAlive();
        release.countDown();
        after.join();
        Assertions.assertFalse(blocked, "a lookup after the refresh waited on the check computed before it");
        Assertions.assertEquals(MvUpdateInfo.MvToRefreshType.NO_REFRESH, afterRefresh.get().getMVToRefreshType());

        computing.join();
        Assertions.assertEquals(MvUpdateInfo.MvToRefreshType.FULL, beforeRefresh.get().getMVToRefreshType());
        Assertions.assertEquals(MvUpdateInfo.MvToRefreshType.NO_REFRESH, lookup().getMVToRefreshType(),
                "the check computed before the refresh replaced the one computed after it");
        Assertions.assertEquals(2, computeCount.get());
    }

    // Blocks the first check until the returned latch is released; later checks return NO_REFRESH.
    private CountDownLatch blockFirstCheck(CountDownLatch started, Function<Integer, MvUpdateInfo> firstResult) {
        CountDownLatch release = new CountDownLatch(1);
        arbiter = call -> {
            if (call == 1) {
                started.countDown();
                try {
                    release.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                return firstResult.apply(call);
            }
            return MvUpdateInfo.noRefresh(mv);
        };
        return release;
    }

    private static void awaitWaiting(List<Thread> waiters) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (waiters.stream().anyMatch(t -> t.getState() != Thread.State.TIMED_WAITING) &&
                System.nanoTime() < deadline) {
            Thread.sleep(10);
        }
    }

    @Test
    public void testWaitersOfAFailedCheckGetItsFailure() throws Exception {
        // Retrying in each waiter would compute the check once per waiter in turn while it keeps failing. The
        // failure is not cached: the next lookup computes it again.
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = blockFirstCheck(started, call -> {
            throw new IllegalStateException("partition dropped mid-check");
        });
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread failing = new Thread(() -> {
            try {
                lookup();
            } catch (Throwable t) {
                failure.set(t);
            }
        });
        failing.start();
        Assertions.assertTrue(started.await(10, TimeUnit.SECONDS));

        int waiterCount = 3;
        List<AtomicReference<Throwable>> waiterFailures = new ArrayList<>();
        List<Thread> waiters = new ArrayList<>();
        for (int i = 0; i < waiterCount; i++) {
            AtomicReference<Throwable> waiterFailure = new AtomicReference<>();
            waiterFailures.add(waiterFailure);
            Thread waiter = new Thread(() -> {
                try {
                    lookup();
                } catch (Throwable t) {
                    waiterFailure.set(t);
                }
            });
            waiters.add(waiter);
            waiter.start();
        }
        awaitWaiting(waiters);
        release.countDown();
        failing.join();
        for (Thread waiter : waiters) {
            waiter.join();
        }

        Assertions.assertInstanceOf(IllegalStateException.class, failure.get());
        for (AtomicReference<Throwable> waiterFailure : waiterFailures) {
            Assertions.assertInstanceOf(IllegalStateException.class, waiterFailure.get());
        }
        Assertions.assertEquals(1, computeCount.get(), "a waiter of the failed check computed it again");
        MvUpdateInfo retried = lookup();
        Assertions.assertEquals(MvUpdateInfo.MvToRefreshType.NO_REFRESH, retried.getMVToRefreshType());
        Assertions.assertSame(retried, lookup(), "the retried check was not cached");
        Assertions.assertEquals(2, computeCount.get());
    }

    // Runs a lookup on a new thread whose statement has the given timeout; the context is handed to the caller.
    private Thread lookupInStatement(int timeoutS, AtomicReference<ConnectContext> context,
                                     AtomicReference<Object> outcome) {
        ConnectContext ctx = new ConnectContext();
        ctx.getSessionVariable().setQueryTimeoutS(timeoutS);
        ctx.setStartTime();
        context.set(ctx);
        return new Thread(() -> {
            ctx.setThreadLocalInfo();
            try {
                outcome.set(lookup());
            } catch (Throwable t) {
                outcome.set(t);
            } finally {
                ConnectContext.remove();
            }
        });
    }

    @Test
    public void testWaiterGivesUpOnceItsBudgetIsSpent() throws Exception {
        // The check computed by another query hangs: the waiter stops at its own timeout instead of hanging with it,
        // and does not compute the check again itself, which would repeat the same slow reads once per waiter.
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = blockFirstCheck(started, call -> MvUpdateInfo.fullRefresh(mv));
        AtomicReference<MvUpdateInfo> first = new AtomicReference<>();
        Thread computing = new Thread(() -> first.set(lookup()));
        computing.start();
        Assertions.assertTrue(started.await(10, TimeUnit.SECONDS));

        AtomicReference<Object> outcome = new AtomicReference<>();
        Thread waiter = lookupInStatement(1, new AtomicReference<>(), outcome);
        waiter.start();
        waiter.join(TimeUnit.SECONDS.toMillis(10));
        boolean hung = waiter.isAlive();
        release.countDown();
        waiter.join();
        computing.join();

        Assertions.assertFalse(hung, "the waiter outlived its statement timeout");
        Assertions.assertInstanceOf(StarRocksPlannerException.class, outcome.get());
        Assertions.assertEquals(1, computeCount.get(), "the waiter computed the check itself");
        Assertions.assertSame(first.get(), lookup());
    }

    @Test
    public void testLookupPastItsBudgetStillTakesACachedCheck() throws Exception {
        arbiter = call -> MvUpdateInfo.noRefresh(mv);
        MvUpdateInfo cached = lookup();
        AtomicReference<ConnectContext> context = new AtomicReference<>();
        AtomicReference<Object> outcome = new AtomicReference<>();
        Thread late = lookupInStatement(1, context, outcome);
        context.get().setStartTime(Instant.now().minusSeconds(3600));
        late.start();
        late.join();
        Assertions.assertSame(cached, outcome.get());
        Assertions.assertEquals(1, computeCount.get());
    }

    @Test
    public void testCancelledWaiterStopsWaiting() throws Exception {
        // KILL QUERY only marks the statement; the waiter must notice it without being interrupted.
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = blockFirstCheck(started, call -> MvUpdateInfo.noRefresh(mv));
        Thread computing = new Thread(this::lookup);
        computing.start();
        Assertions.assertTrue(started.await(10, TimeUnit.SECONDS));

        AtomicReference<ConnectContext> context = new AtomicReference<>();
        AtomicReference<Object> outcome = new AtomicReference<>();
        Thread waiter = lookupInStatement(3600, context, outcome);
        waiter.start();
        awaitWaiting(List.of(waiter));
        context.get().setKilled();
        waiter.join(TimeUnit.SECONDS.toMillis(10));
        boolean hung = waiter.isAlive();
        release.countDown();
        waiter.join();
        computing.join();

        Assertions.assertFalse(hung, "the cancelled waiter kept waiting");
        Assertions.assertInstanceOf(StarRocksPlannerException.class, outcome.get());
        Assertions.assertEquals(1, computeCount.get());
    }

    @Test
    public void testInterruptedWaiterStopsWaiting() throws Exception {
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = blockFirstCheck(started, call -> MvUpdateInfo.noRefresh(mv));
        Thread computing = new Thread(this::lookup);
        computing.start();
        Assertions.assertTrue(started.await(10, TimeUnit.SECONDS));

        AtomicReference<Object> outcome = new AtomicReference<>();
        Thread waiter = lookupInStatement(3600, new AtomicReference<>(), outcome);
        waiter.start();
        awaitWaiting(List.of(waiter));
        waiter.interrupt();
        waiter.join(TimeUnit.SECONDS.toMillis(10));
        boolean hung = waiter.isAlive();
        release.countDown();
        waiter.join();
        computing.join();

        Assertions.assertFalse(hung, "the interrupted waiter kept waiting");
        Assertions.assertInstanceOf(StarRocksPlannerException.class, outcome.get());
        Assertions.assertEquals(1, computeCount.get());
    }

    @Test
    public void testDoesNotCacheTheCheckOfADroppedMv() {
        // DROP removes the MV from the catalog, then evicts its entry; a check that puts its entry after that
        // eviction must not leave it behind.
        arbiter = call -> MvUpdateInfo.noRefresh(mv);
        inCatalog = false;
        lookup();
        lookup();
        Assertions.assertEquals(2, computeCount.get());
        Assertions.assertEquals(0L, mgr.estimateCount().get("mvTimelinessMap"));
    }
}
