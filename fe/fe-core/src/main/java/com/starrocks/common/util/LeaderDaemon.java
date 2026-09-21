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

package com.starrocks.common.util;

import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.LeaderLease;
import com.starrocks.server.WarehouseManager;
import com.starrocks.warehouse.Warehouse;
import com.starrocks.warehouse.cngroup.CRAcquireContext;
import com.starrocks.warehouse.cngroup.ComputeResource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Base class for daemons that may only run while this FE is the leader.
 *
 * Differences from {@link Daemon}:
 * 1. Composition over inheritance: holds an internal {@link Thread} instead of being one,
 *    so the same instance can be {@link #start() started} again after it stops. Required for
 *    safe leader demotion: when this FE later becomes leader again the existing Mgr singletons
 *    must be reusable.
 * 2. Built-in lease check: each iteration captures and revalidates a {@link LeaderLease}
 *    obtained from {@link GlobalStateMgr}. Once the lease is invalidated by a demotion the
 *    daemon stops itself - subclasses do not need to add their own check.
 * 3. Cleanup hook: {@link #onStopped()} runs on the worker thread as its last act, after the run
 *    loop has exited, so subclasses can release leader-session-only state without racing the loop.
 *    Follower state should not retain that data, both to free memory and to avoid leaking stale
 *    leader state into replay paths.
 *
 * A stop request wakes the worker without interrupting business code. The worker finishes its safe
 * step, runs {@link #onStopped()}, and clears {@link #isRunning} only after cleanup succeeds.
 * Demotion requests every stop, then waits for the whole session before starting follower replay.
 * A timeout or cleanup failure terminates the FE; re-activation also checks for remaining workers.
 */
public abstract class LeaderDaemon {
    private static final Logger LOG = LogManager.getLogger(LeaderDaemon.class);
    private static final int DEFAULT_INTERVAL_SECONDS = 30;

    /**
     * Every leader daemon whose worker is currently started and has not finished stopping (its
     * {@link #onStopped()} has not run to completion yet). A daemon adds itself in {@link #start()}
     * and removes itself at the tail of {@link #loop()} after cleanup. The re-activation cleanliness
     * gate ({@code GlobalStateMgr.assertLeaderSessionQuiescedOrExit}) reads this to refuse a new
     * leader session while a previous session's worker still lingers, covering nested daemons
     * uniformly without an explicit per-daemon list.
     */
    private static final Set<LeaderDaemon> RUNNING_INSTANCES = ConcurrentHashMap.newKeySet();

    /** Leader daemons that are still running (worker not fully stopped). Snapshot for the gate. */
    public static List<LeaderDaemon> getRunningInstances() {
        List<LeaderDaemon> result = new ArrayList<>();
        for (LeaderDaemon daemon : RUNNING_INSTANCES) {
            if (daemon.isRunning()) {
                result.add(daemon);
            }
        }
        return result;
    }

    /**
     * Wait for the given daemons' workers and onStopped hooks to finish. Throws on timeout so a
     * failed cleanup dependency cannot be mistaken for a quiesced session.
     */
    public static void awaitQuiesced(List<LeaderDaemon> daemons, long timeoutMs) {
        long deadlineMs = System.currentTimeMillis() + Math.max(1L, timeoutMs);
        for (LeaderDaemon daemon : daemons) {
            while (daemon.isRunning()) {
                if (System.currentTimeMillis() >= deadlineMs) {
                    throw new IllegalStateException("daemon " + daemon.getName() + " has not quiesced within "
                            + timeoutMs + "ms");
                }
                try {
                    Thread.sleep(10L);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("interrupted while waiting for daemons to quiesce", e);
                }
            }
        }
    }

    /**
     * Close a leader-session pool without interrupting active tasks and wait for actual termination.
     * The owning daemon stays registered/isRunning until this returns. GlobalStateMgr bounds the
     * whole session drain and exits on timeout; the daemon thread here never blocks process exit.
     */
    public static void shutdownAndAwaitTermination(String poolName, ExecutorService pool) {
        if (pool == null) {
            return;
        }
        shutdownLeaderExecutor(pool);
        boolean terminated = false;
        while (!terminated) {
            try {
                terminated = pool.awaitTermination(1, TimeUnit.MINUTES);
                if (!terminated) {
                    LOG.warn("{} has not terminated after cooperative shutdown; still draining. "
                            + "The leader-session drain must finish before follower replay or re-activation.", poolName);
                }
            } catch (InterruptedException e) {
                // An unrelated interrupt cannot make a live pool look quiesced. Keep draining.
            }
        }
    }

    /** Reject new work without interrupting running tasks or running delayed work from an old session. */
    public static void shutdownLeaderExecutor(ExecutorService pool) {
        if (pool instanceof ScheduledThreadPoolExecutor) {
            ScheduledThreadPoolExecutor scheduler = (ScheduledThreadPoolExecutor) pool;
            scheduler.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
            scheduler.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
        }
        pool.shutdown();
    }

    private final String name;
    private volatile long intervalMs;
    private final AtomicBoolean isStopRequested = new AtomicBoolean(false);
    private final AtomicBoolean isRunning = new AtomicBoolean(false);
    private final Object stopSignal = new Object();
    private volatile Thread worker;
    private volatile LeaderLease capturedLease = LeaderLease.INVALID;

    /**
     * Last compute resource the subclass acquired through {@link #acquireBackgroundComputeResource()}.
     * Mirrors the same-named field on {@code FrontendDaemon} so lake-side leader daemons that
     * relied on it before migration continue to compile and behave identically. Defaults to
     * {@link WarehouseManager#DEFAULT_RESOURCE} until the subclass acquires one.
     */
    protected ComputeResource computeResource = WarehouseManager.DEFAULT_RESOURCE;

    protected LeaderDaemon(String name) {
        this(name, DEFAULT_INTERVAL_SECONDS * 1000L);
    }

    protected LeaderDaemon(String name, long intervalMs) {
        this.name = name;
        this.intervalMs = intervalMs;
    }

    public final String getName() {
        return name;
    }

    public final long getInterval() {
        return intervalMs;
    }

    public final void setInterval(long intervalMs) {
        this.intervalMs = intervalMs;
    }

    /** Whether a stop has been requested (via {@link #setStop()} or {@link #stopBestEffort()}). */
    public final boolean isStopRequested() {
        return isStopRequested.get();
    }

    public final boolean isRunning() {
        return isRunning.get();
    }

    /**
     * Idempotent. Safe to call after the daemon has stopped - a fresh worker thread will be created.
     */
    public synchronized void start() {
        if (!isRunning.compareAndSet(false, true)) {
            return;
        }
        isStopRequested.set(false);
        capturedLease = LeaderLease.INVALID;
        RUNNING_INSTANCES.add(this);
        Thread t = new Thread(this::loop, name);
        t.setDaemon(true);
        worker = t;
        t.start();
    }

    /**
     * Mark stop requested and wake the worker without interrupting it. Does not wait for the worker to
     * exit; the worker still runs {@link #onStopped()} on its way out. Used for cooperative
     * self-stop from within the loop (e.g. once the lease is lost); demotion uses
     * {@link #stopBestEffort()}.
     */
    public void setStop() {
        requestStop();
    }

    /**
     * Request cooperative stop and return without joining. The worker finishes its current safe
     * business step, runs {@link #onStopped()}, and only then deregisters. Demotion requests every
     * stop before waiting for the whole session to drain, so one slow daemon cannot delay notifying
     * the others. Never interrupt a worker: it may be inside JE or a committed WAL apply.
     */
    public final void stopBestEffort() {
        requestStop();
    }

    private void requestStop() {
        if (!isStopRequested.compareAndSet(false, true)) {
            return;
        }
        synchronized (stopSignal) {
            stopSignal.notifyAll();
        }
        try {
            onStopRequested();
        } catch (Throwable th) {
            LOG.warn("{} onStopRequested failed", name, th);
        }
    }

    /** Check between business steps, never while applying an already committed WAL entry. */
    protected final boolean shouldStop() {
        LeaderLease lease = capturedLease;
        return isStopRequested.get() || (lease.isValid() && !getGlobalStateMgr().isLeaderLeaseValid(lease));
    }

    /** A bounded business delay that stopBestEffort can wake without interrupting the worker. */
    protected final void sleepUntilNextStep(long millis) throws InterruptedException {
        synchronized (stopSignal) {
            if (!shouldStop() && millis > 0) {
                stopSignal.wait(millis);
            }
        }
    }

    private void loop() {
        while (!isStopRequested.get()) {
            try {
                runOneCycle();
            } catch (Throwable e) {
                LOG.error("{} got exception", name, e);
            }
            if (isStopRequested.get()) {
                break;
            }
            // intervalMs <= 0 means "tight drain loop with no inter-cycle delay" (e.g. report-handler,
            // resource-report-handler, routine-load-task-scheduler, whose runAfterLeaseValid() self-paces
            // via a blocking poll/sleep). Object.wait(0) would block the worker forever - it would run
            // exactly one cycle per leader activation and then never drain its queue again - so only wait
            // for a strictly positive interval. setStop()/stopBestEffort() still wake the loop promptly via
            // the isStopRequested checks (and the daemon's own bounded blocking call).
            if (intervalMs > 0) {
                try {
                    synchronized (stopSignal) {
                        // Re-check INSIDE the monitor: requestStop() sets the flag and notifies under
                        // stopSignal, so a notify landing between the flag check above and this wait()
                        // would otherwise be lost, delaying cooperative stop by a full interval.
                        if (isStopRequested.get()) {
                            break;
                        }
                        stopSignal.wait(intervalMs);
                    }
                } catch (InterruptedException ie) {
                    LOG.warn("{} interval wait failed", name, ie);
                }
            }
        }
        LOG.info("{} exits", name);
        // The worker cleans up its own leader-session state after the last cycle. onStopped's pool
        // drain must finish before isRunning can clear.
        try {
            onStopped();
        } catch (Throwable th) {
            LOG.error("{} onStopped failed; leader-session cleanup is incomplete, terminating the process", name, th);
            System.exit(-1);
            return;
        }
        // Deregister BEFORE clearing isRunning: start() CASes on isRunning and then re-adds this
        // singleton, so the old order let a preempted dying worker's late remove() delete the NEW
        // worker's registration (same identity), hiding the daemon from the re-activation cleanliness
        // gate for the whole next leader session. Between remove and set(false) the worker has already
        // run onStopped() (pools drained), so the gate missing it in that instant is harmless.
        RUNNING_INSTANCES.remove(this);
        isRunning.set(false);
    }

    protected void runOneCycle() throws InterruptedException {
        GlobalStateMgr gsm = getGlobalStateMgr();
        while (!gsm.isReady()) {
            Thread.sleep(100);
            if (isStopRequested.get()) {
                return;
            }
        }
        LeaderLease lease = capturedLease;
        if (!lease.isValid()) {
            lease = gsm.captureLeaderLease();
            capturedLease = lease;
        }
        if (!gsm.isLeaderLeaseValid(lease)) {
            LOG.info("{} sees lease invalid, self-stop. lease={}", name, lease);
            // Defensive self-stop; on today's paths this is rarely reached. Demotion sets isReady=false
            // (transferToNonLeader's first statement) BEFORE it invalidates the lease, so workers park in
            // the isReady spin above and are stopped by stopLeaderOnlyDaemonThreads; and a FAILED
            // activation exits the process instead of leaving an invalid lease behind. Keep the check
            // anyway: it costs nothing and self-heals any future path that invalidates the lease while
            // isReady stays true. The worker is running this check, not blocked, so no interrupt is
            // needed; just request stop and the loop breaks on its next isStopRequested check.
            requestStop();
            return;
        }
        runAfterLeaseValid();
    }

    /**
     * The body of each iteration. Runs only after FE is ready and the captured leader lease
     * is still valid. Subclasses must not block indefinitely.
     *
     * Demotion never interrupts a running cycle. Check {@link #shouldStop()} between business steps
     * and after blocking calls, and bound waits or wake them through {@link #onStopRequested()}.
     * Already admitted WAL operations must finish commit/apply before a check may abandon further work.
     * A cycle that cannot drain keeps isRunning true; demotion fails rather than replaying journals
     * concurrently with its unfinished work.
     */
    protected abstract void runAfterLeaseValid() throws InterruptedException;

    /**
     * Seam for tests to provide an isolated {@link GlobalStateMgr} instance. Production code uses
     * the singleton returned by {@link GlobalStateMgr#getServingState()}.
     */
    protected GlobalStateMgr getGlobalStateMgr() {
        return GlobalStateMgr.getServingState();
    }

    /**
     * Re-validate the lease captured at the start of this cycle. Subclasses that perform irreversible
     * external side effects (e.g. deleting object-store data or BE tablets/shards) inside a long cycle
     * should call this before that work and bail out when it returns {@code false}, so a demotion that
     * lands mid-cycle cannot keep acting under a leadership this node has
     * already lost. Same-node re-election bumps the generation, so a stale captured lease fails here too.
     */
    protected final boolean isCapturedLeaseValid() {
        return getGlobalStateMgr().isLeaderLeaseValid(capturedLease);
    }

    /**
     * Hook called on the worker thread as its last act, after the run loop has exited (whether the
     * daemon self-stopped on a lost lease or was stopped for demotion). Subclasses MUST clear all
     * leader-session-only state here (queues, pending maps, executors) so memory is reclaimed promptly
     * and follower state does not retain it. A subclass that owns pools should drain them here via
     * {@link #shutdownAndAwaitTermination(String, ExecutorService)} so that {@code isRunning}, once
     * cleared, implies the owned pools are terminated too. If this hook throws, the process exits
     * without marking the daemon quiesced, because its leader-session cleanup is incomplete.
     */
    protected void onStopped() {
    }

    /**
     * Optional hook called immediately after a stop request is accepted, from the thread that
     * requested the stop. Use it to wake a business wait (for example, offer a sentinel to a result
     * queue or disconnect an owned HTTP connection). It must not interrupt any business thread or
     * reset state still used by the running cycle.
     */
    protected void onStopRequested() {
    }

    /**
     * Refresh {@link #computeResource} from the background warehouse. Migrated from the same
     * helper on {@code FrontendDaemon}; only subclasses that perform background work against a
     * lake compute group need to call it.
     */
    protected void acquireBackgroundComputeResource() {
        final WarehouseManager warehouseManager = GlobalStateMgr.getCurrentState().getWarehouseMgr();
        final Warehouse warehouse = warehouseManager.getBackgroundWarehouse();
        final CRAcquireContext acquireContext = CRAcquireContext.of(warehouse.getId(), computeResource);
        // check resource before each run
        this.computeResource = warehouseManager.acquireComputeResource(acquireContext);
    }
}
