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

import com.starrocks.common.Config;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class GracefulExitFlag {
    private static final AtomicBoolean GRACEFUL_EXIT = new AtomicBoolean(false);
    // System.nanoTime() when graceful exit was marked (0 = not marked)
    private static final AtomicLong BEGIN_NANO = new AtomicLong(0L);
    // Snapshot of transaction ids that were already in an explicit transaction when graceful exit
    // began, captured by the SIGUSR1 handler from the local connection map BEFORE the graceful-exit
    // flag becomes visible. A follower must not approximate this with TransactionIdGenerator.peekNext
    // (as a numeric boundary would): the generator journals ids in batches of 1000 and follower replay
    // advances nextId to the reserved batch end, so a follower's captured boundary can sit hundreds of
    // ids above the leader's actual position -- a BEGIN forwarded during the accept window would then
    // hold an id below that inflated boundary and wrongly stay exempt after the window. An explicit
    // txnId snapshot is exact on leader and follower alike: any txn begun after graceful exit is simply
    // not in the set. Null means the snapshot has not been captured yet; treat every txn as pre-existing
    // then, so a connection is never wrongly terminated before the handler records the set (and tests
    // that drive isTerminated() directly stay safe).
    private static volatile Set<Long> preSignalTxnIds = null;

    // HTTP admission is independent of JDBC stopAccept(). HealthCheck returns 500 at T0 so the load
    // balancer can detach, but ExecuteSqlAction keeps accepting until the HTTP accept window elapses.
    // tryStartHttpRequest() and the rejecting latch share HTTP_LOCK so a request cannot pass the
    // check without being counted, and drain cannot observe activeHttpRequests==0 while a request
    // admitted during the window is still running.
    private static final Object HTTP_LOCK = new Object();
    private static int activeHttpRequests = 0;
    private static boolean httpRejecting = false;
    // Once drain observes rejecting && active==0, refuse even terminal TransactionLoad ops
    // so waitForDraining cannot System.exit while a later commit increments active.
    private static boolean httpDrainedSealed = false;

    public static void setPreSignalTxnIds(Set<Long> txnIds) {
        preSignalTxnIds = txnIds == null ? null : Collections.unmodifiableSet(new HashSet<>(txnIds));
    }

    public static boolean isPreSignalTxn(long txnId) {
        Set<Long> ids = preSignalTxnIds;
        return ids == null || ids.contains(txnId);
    }

    // Atomically claim the graceful-exit start. Returns true only for the caller that wins the
    // CAS, so exactly one drain thread is spawned even under a burst of SIGUSR1. A repeated call
    // returns false and does not reset BEGIN_NANO (which would reopen the accept-new window).
    public static boolean markGracefulExit() {
        if (GRACEFUL_EXIT.compareAndSet(false, true)) {
            BEGIN_NANO.set(System.nanoTime());
            return true;
        }
        return false;
    }

    public static boolean isGracefulExit() {
        return GRACEFUL_EXIT.get();
    }

    // Existing JDBC connections only: stopAccept() already rejects new MySQL sockets at T0.
    // Keep the previous txn/pre-query exemption clock so a BEGIN on a still-open JDBC session
    // during the LB detach window is not terminated/rolled back (design §11).
    public static boolean shouldAcceptNewRequest() {
        if (!GRACEFUL_EXIT.get()) {
            return true;
        }
        long begin = BEGIN_NANO.get();
        if (begin == 0L) {
            return true;
        }
        return (System.nanoTime() - begin) < TimeUnit.NANOSECONDS.convert(
                Config.graceful_exit_http_accept_window_ms, TimeUnit.MILLISECONDS);
    }

    // Idle JDBC explicit-txn connections are force-closed after the HTTP window plus
    // min_graceful_exit_time_second, matching previous cleanup rules.
    public static boolean isDrainWindowElapsed() {
        if (!GRACEFUL_EXIT.get()) {
            return false;
        }
        long begin = BEGIN_NANO.get();
        if (begin == 0L) {
            return false;
        }
        long elapsed = System.nanoTime() - begin;
        long acceptWindowNanos = TimeUnit.NANOSECONDS.convert(
                Config.graceful_exit_http_accept_window_ms, TimeUnit.MILLISECONDS);
        if (elapsed <= acceptWindowNanos) {
            return false;
        }
        return (elapsed - acceptWindowNanos) > TimeUnit.SECONDS.toNanos(Config.min_graceful_exit_time_second);
    }

    /**
     * Admit one HTTP SQL request. Increments {@code activeHttpRequests} in the same critical
     * section as the rejecting check. Returns false once the HTTP accept window has elapsed;
     * an already-admitted request is never rejected by a later state flip.
     */
    public static boolean tryStartHttpRequest() {
        return tryStartHttpRequest(false);
    }

    // allowWhenRejecting: TransactionLoad prepare/commit/rollback of an already-started txn.
    public static boolean tryStartHttpRequest(boolean allowWhenRejecting) {
        synchronized (HTTP_LOCK) {
            flipHttpRejectingIfNeeded();
            if (httpDrainedSealed || (httpRejecting && !allowWhenRejecting)) {
                return false;
            }
            activeHttpRequests++;
            return true;
        }
    }

    public static void finishHttpRequest() {
        synchronized (HTTP_LOCK) {
            if (activeHttpRequests > 0) {
                activeHttpRequests--;
            }
        }
    }

    public static boolean isHttpRejecting() {
        synchronized (HTTP_LOCK) {
            flipHttpRejectingIfNeeded();
            return httpRejecting;
        }
    }

    public static boolean shouldAcceptNewHttpRequest() {
        return !isHttpRejecting();
    }

    public static int getActiveHttpRequests() {
        synchronized (HTTP_LOCK) {
            return activeHttpRequests;
        }
    }

    // Drain observed rejecting and no in-flight HTTP. Seals admission including terminal ops.
    public static boolean trySealHttpDrained() {
        synchronized (HTTP_LOCK) {
            flipHttpRejectingIfNeeded();
            if (!httpRejecting || activeHttpRequests != 0) {
                return false;
            }
            httpDrainedSealed = true;
            return true;
        }
    }

    // Test helper: HTTP latch/counter survive GRACEFUL_EXIT reflection resets.
    public static void resetHttpAdmissionState() {
        synchronized (HTTP_LOCK) {
            activeHttpRequests = 0;
            httpRejecting = false;
            httpDrainedSealed = false;
        }
    }

    private static void flipHttpRejectingIfNeeded() {
        if (httpRejecting || !GRACEFUL_EXIT.get()) {
            return;
        }
        long begin = BEGIN_NANO.get();
        if (begin == 0L) {
            return;
        }
        long windowNanos = TimeUnit.NANOSECONDS.convert(
                Config.graceful_exit_http_accept_window_ms, TimeUnit.MILLISECONDS);
        if ((System.nanoTime() - begin) >= windowNanos) {
            httpRejecting = true;
        }
    }
}
