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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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

    private final String name;
    private volatile long intervalMs;
    private final AtomicBoolean isStopped = new AtomicBoolean(false);
    private final AtomicBoolean isRunning = new AtomicBoolean(false);
    private volatile Thread worker;
    private volatile LeaderLease capturedLease = LeaderLease.INVALID;

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
        if (!isStopped.compareAndSet(false, true)) {
            return;
        }
        Thread t = worker;
        if (t != null) {
            t.interrupt();
        }
    }

    /**
     * Coordinated stop for leader demotion:
     *   1. mark stopped + interrupt worker;
     *   2. join up to {@code timeoutMs};
     *   3. on timeout, invoke {@link #onJoinTimeout()} (default: terminate the JVM) and return
     *      without clearing {@code worker}/{@code isRunning} - a subsequent {@link #start()}
     *      must not spin up a second worker while the first is still alive;
     *   4. on clean exit, run {@link #onStopped()} so subclasses release leader-session state.
     * Idempotent.
     */
    public final void stopGracefully(long timeoutMs) {
        setStop();
        Thread t = worker;
        if (t != null) {
            try {
                t.join(Math.max(1L, timeoutMs));
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
            if (t.isAlive()) {
                onJoinTimeout();
                // onJoinTimeout normally terminates the JVM. If it returns (tests only) we must
                // not reset worker/isRunning: another start() would then race the stuck worker.
                return;
            }
        }
        try {
            onStopped();
        } catch (Throwable th) {
            LOG.warn("{} onStopped failed", name, th);
        }
        capturedLease = LeaderLease.INVALID;
        worker = null;
        isRunning.set(false);
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
            try {
                Thread.sleep(intervalMs);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                if (isStopped.get()) {
                    break;
                }
            }
        }
        LOG.info("{} exits", name);
        isRunning.set(false);
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
     * is still valid. Subclasses must not block indefinitely - block in interruptible primitives
     * so {@link #setStop()} can wake them.
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
     * Hook called from {@link #stopGracefully(long)} after the worker thread has actually exited.
     * Subclasses MUST clear all leader-session-only state here (queues, pending maps, executors)
     * so memory is reclaimed promptly and follower state does not retain it. Not called when the
     * join times out - {@link #onJoinTimeout()} fires instead.
     */
    protected void onStopped() {
    }

    /**
     * Invoked when the worker thread fails to exit within the stop timeout. Default:
     * {@link System#exit(int)} - a stuck worker combined with a later {@link #start()} would run
     * two workers against the same singleton state, which is strictly worse than a process
     * restart. Overridable for tests only.
     */
    protected void onJoinTimeout() {
        LOG.error("{} did not exit within stop timeout; terminating JVM to avoid concurrent workers", name);
        System.exit(-1);
    }
}
