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
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

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
        while (waiting.getState() != Thread.State.WAITING && computeCount.get() < 2 && System.nanoTime() < deadline) {
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

    @Test
    public void testWaitersOfAFailedCheckRetryItOnce() throws Exception {
        // A metadata race fails the check while other queries wait on it: one of them computes it again for all,
        // and that result is cached, rather than each waiter computing it for itself.
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
                throw new IllegalStateException("partition dropped mid-check");
            }
            return MvUpdateInfo.noRefresh(mv);
        };
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
        List<AtomicReference<MvUpdateInfo>> results = new ArrayList<>();
        List<Thread> waiters = new ArrayList<>();
        for (int i = 0; i < waiterCount; i++) {
            AtomicReference<MvUpdateInfo> result = new AtomicReference<>();
            results.add(result);
            Thread waiter = new Thread(() -> result.set(lookup()));
            waiters.add(waiter);
            waiter.start();
        }
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (waiters.stream().anyMatch(t -> t.getState() != Thread.State.WAITING) && System.nanoTime() < deadline) {
            Thread.sleep(10);
        }
        release.countDown();
        failing.join();
        for (Thread waiter : waiters) {
            waiter.join();
        }

        Assertions.assertInstanceOf(IllegalStateException.class, failure.get());
        for (AtomicReference<MvUpdateInfo> result : results) {
            Assertions.assertEquals(MvUpdateInfo.MvToRefreshType.NO_REFRESH, result.get().getMVToRefreshType());
        }
        Assertions.assertEquals(2, computeCount.get(), "each waiter of the failed check computed it again");
        Assertions.assertSame(results.get(0).get(), lookup(), "the retried check was not cached");
        Assertions.assertEquals(2, computeCount.get());
    }
}
