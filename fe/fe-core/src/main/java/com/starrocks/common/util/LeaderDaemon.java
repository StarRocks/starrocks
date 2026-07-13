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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Base class for daemons that may only run while this FE is the leader.
 *
 * Differences from {@link Daemon}:
 * 1. Composition over inheritance: holds an internal {@link Thread} instead of being one,
 *    so the same instance can be {@link #start() started} again after {@link #stopGracefully(long)}.
 *    Required for safe leader demotion: when this FE later becomes leader again the existing
 *    Mgr singletons must be reusable.
 * 2. Built-in lease check: each iteration captures and revalidates a {@link LeaderLease}
 *    obtained from {@link GlobalStateMgr}. Once the lease is invalidated by a demotion the
 *    daemon stops itself - subclasses do not need to add their own check.
 * 3. Cleanup hook: {@link #onStopped()} runs after the worker has actually exited, so
 *    subclasses can release leader-session-only state without racing the loop. Follower state
 *    should not retain that data, both to free memory and to avoid leaking stale leader state
 *    into replay paths. If the worker does not exit within the stop timeout the JVM is
 *    terminated via {@link #onJoinTimeout()} because a concurrent second worker (after a
 *    subsequent re-election) would be strictly more dangerous than a process restart.
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
     * Shut down a leader-session pool and wait — WITHOUT a deadline — until it actually terminates.
     * onStopped() implementations that own pools call this, so that when the worker finally clears
     * {@link #isRunning} at the tail of {@link #loop()} (after onStopped returns), the owned pools are
     * provably terminated too. That makes the daemon's {@code isRunning} the single quiescence signal
     * the re-activation cleanliness gate reads, without the gate having to enumerate pools and without a
     * per-daemon restart guard. A task that never terminates keeps the worker blocked here (isRunning
     * stays true), so the gate exits the process on re-election rather than let a stale pool task race a
     * new leader session. The worker is a JVM daemon thread, so this wait never blocks process exit.
     */
    protected static void shutdownNowAndAwaitTermination(String poolName, ExecutorService pool) {
        if (pool == null) {
            return;
        }
        pool.shutdownNow();
        boolean terminated = false;
        while (!terminated) {
            try {
                terminated = pool.awaitTermination(1, TimeUnit.MINUTES);
                if (!terminated) {
                    LOG.warn("{} has not terminated after shutdownNow; still draining. A stuck task keeps this "
                            + "daemon non-quiesced; the re-activation gate restarts the process if it outlives "
                            + "demotion.", poolName);
                }
            } catch (InterruptedException e) {
                // onStopped runs after the worker's stop-interrupt was already cleared; if re-interrupted, keep
                // draining - leaving a pool half-stopped would defeat the isRunning quiescence signal.
                Thread.interrupted();
            }
        }
    }

    private final String name;
    private volatile long intervalMs;
    private final AtomicBoolean isStopped = new AtomicBoolean(false);
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

    public final boolean isStopped() {
        return isStopped.get();
    }

    public final boolean isRunning() {
        return isRunning.get();
    }

    /**
     * Idempotent. Safe to call after {@link #stopGracefully(long)} - a fresh worker thread
     * will be created.
     */
    public synchronized void start() {
        if (!isRunning.compareAndSet(false, true)) {
            return;
        }
        isStopped.set(false);
        capturedLease = LeaderLease.INVALID;
        RUNNING_INSTANCES.add(this);
        Thread t = new Thread(this::loop, name);
        t.setDaemon(true);
        worker = t;
        t.start();
    }

    /**
     * Mark stopped and wake the worker. Does not wait for the worker to exit.
     * Prefer {@link #stopGracefully(long)} during demotion so cleanup hooks run.
     */
    public void setStop() {
        requestStop(true);
    }

    /**
     * Fire-and-forget stop for leader demotion: request stop (interrupting the worker unless
     * {@link #interruptOnStop()} is overridden to {@code false}) and return immediately WITHOUT
     * joining the worker. The worker exits on its own and runs {@link #onStopped()} + deregisters at
     * the tail of {@link #loop()}; the re-activation cleanliness gate then verifies quiescence and
     * exits the process if this worker is still alive when the node is re-elected. Preferred on the
     * demotion path so the single state-change thread is not blocked waiting for ~40 daemons to drain.
     */
    public final void stopBestEffort() {
        requestStop(interruptOnStop());
    }

    private void requestStop(boolean interruptWorker) {
        if (!isStopped.compareAndSet(false, true)) {
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
        Thread t = worker;
        if (interruptWorker && t != null) {
            t.interrupt();
        }
    }

    /**
     * Coordinated stop for leader demotion:
     *   1. mark stopped, wake interval waits, and (unless {@link #interruptOnStop()} is overridden
     *      to {@code false}) interrupt the worker so it breaks out of any blocking primitive
     *      (queue poll, latch/future await, sleep, interruptible lock) at once;
     *   2. join up to {@code timeoutMs};
     *   3. on timeout, invoke {@link #onJoinTimeout()} (default: terminate the JVM) and return
     *      without clearing {@code worker}/{@code isRunning} - a subsequent {@link #start()}
     *      must not spin up a second worker while the first is still alive;
     *   4. on clean exit, run {@link #onStopped()} so subclasses release leader-session state.
     * Idempotent.
     */
    public final void stopGracefully(long timeoutMs) {
        requestStop(interruptOnStop());
        Thread t = worker;
        if (t != null) {
            try {
                t.join(Math.max(1L, timeoutMs));
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
            if (t.isAlive()) {
                onJoinTimeout();
                // The worker did not exit in time. It stays registered and isRunning stays true, so the
                // re-activation cleanliness gate will observe the straggler and refuse (exit) the next
                // leader session rather than run two workers against the same singleton state.
                return;
            }
        }
        // The worker (if any) has exited and already ran onStopped() + cleared isRunning at the tail of
        // loop(); nothing more to clean here.
        capturedLease = LeaderLease.INVALID;
        worker = null;
    }

    private void loop() {
        while (!isStopped.get()) {
            try {
                runOneCycle();
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                if (isStopped.get()) {
                    break;
                }
            } catch (Throwable e) {
                LOG.error("{} got exception", name, e);
            }
            if (isStopped.get()) {
                break;
            }
            // intervalMs <= 0 means "tight drain loop with no inter-cycle delay" (e.g. report-handler,
            // resource-report-handler, routine-load-task-scheduler, whose runAfterLeaseValid() self-paces
            // via a blocking poll/sleep). Object.wait(0) would block the worker forever - it would run
            // exactly one cycle per leader activation and then never drain its queue again - so only wait
            // for a strictly positive interval. setStop()/stopGracefully() still wake the loop promptly via
            // the isStopped checks (and the daemon's own bounded blocking call).
            if (intervalMs > 0) {
                try {
                    synchronized (stopSignal) {
                        stopSignal.wait(intervalMs);
                    }
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    if (isStopped.get()) {
                        break;
                    }
                }
            }
        }
        LOG.info("{} exits", name);
        // The worker cleans up its own leader-session state as its last act (race-free: nothing else is
        // running for this daemon by now). Clear the interrupt set by the stop request first, so onStopped()
        // may use interruptible blocking primitives (e.g. pool.awaitTermination) that would otherwise throw
        // immediately on the still-set flag and skip the drain.
        Thread.interrupted();
        try {
            onStopped();
        } catch (Throwable th) {
            LOG.warn("{} onStopped failed", name, th);
        }
        isRunning.set(false);
        RUNNING_INSTANCES.remove(this);
    }

    protected void runOneCycle() throws InterruptedException {
        GlobalStateMgr gsm = getGlobalStateMgr();
        while (!gsm.isReady()) {
            Thread.sleep(100);
            if (isStopped.get()) {
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
            setStop();
            return;
        }
        runAfterLeaseValid();
    }

    /**
     * The body of each iteration. Runs only after FE is ready and the captured leader lease
     * is still valid. Subclasses must not block indefinitely.
     *
     * By default leader demotion INTERRUPTS the worker (see {@link #stopGracefully(long)}), so
     * subclasses should block only in interruptible primitives and must let an
     * {@link InterruptedException} propagate (or re-check {@link #isStopped()} and return) - they
     * MUST NOT map it to a business outcome (e.g. cancel a healthy job as "timeout"). If the cycle
     * cannot finish within {@code leader_demotion_drain_timeout_sec} the process is terminated via
     * {@link #onJoinTimeout()} as a last resort.
     *
     * A subclass that runs interrupt-unsafe work on its own thread - a direct BDBJE/JE call
     * (interrupting it can invalidate the environment) or an uninterruptible native/socket read -
     * must override {@link #interruptOnStop()} to return {@code false} and cooperatively bail out
     * by polling {@link #isStopped()} and/or waking its wait in {@link #onStopRequested()}.
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
     * lands mid-cycle (interrupt possibly eaten) cannot keep acting under a leadership this node has
     * already lost. Same-node re-election bumps the generation, so a stale captured lease fails here too.
     */
    protected final boolean isCapturedLeaseValid() {
        return getGlobalStateMgr().isLeaderLeaseValid(capturedLease);
    }

    /**
     * Hook called from {@link #stopGracefully(long)} after the worker thread has actually exited.
     * Subclasses MUST clear all leader-session-only state here (queues, pending maps, executors)
     * so memory is reclaimed promptly and follower state does not retain it. Not called when the
     * join times out - {@link #onJoinTimeout()} fires instead.
     */
    protected void onStopped() {
    }

    /**
     * Optional hook called immediately after a stop request is accepted and before
     * {@link #stopGracefully(long)} waits for the worker thread. Most daemons do not need it:
     * the default stop interrupts the worker, which already breaks any interruptible wait.
     * It matters only for daemons that override {@link #interruptOnStop()} to {@code false} and
     * therefore need to cooperatively wake their own uninterruptible wait (e.g. offer a sentinel
     * to a result queue, or disconnect an in-flight HTTP connection).
     */
    protected void onStopRequested() {
    }

    /**
     * Whether {@link #stopGracefully(long)} may interrupt the worker thread. Default {@code true}:
     * interrupt is the fast, standard way to cancel a blocked cycle. Override to return
     * {@code false} ONLY for daemons whose worker executes interrupt-unsafe work directly on its
     * own thread - a raw BDBJE/JE operation (an interrupt can invalidate the environment) or an
     * uninterruptible native/socket read - and instead bail out cooperatively via
     * {@link #isStopped()} polling and {@link #onStopRequested()}.
     */
    protected boolean interruptOnStop() {
        return true;
    }

    /**
     * Invoked when a {@link #stopGracefully(long)} caller's bounded join elapses before the worker
     * exits. Does NOT terminate the JVM: leader demotion no longer joins here (it fires
     * {@link #setStop()} and moves on), and the surviving worker stays registered with
     * {@code isRunning == true}, so the re-activation cleanliness gate
     * ({@code GlobalStateMgr.assertLeaderSessionQuiescedOrExit}) is the single authority that exits
     * the process if a straggler is still alive when this node is re-elected. Overridable for tests.
     */
    protected void onJoinTimeout() {
        LOG.warn("{} did not exit within stop timeout; left running, the re-activation gate will handle it", name);
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
