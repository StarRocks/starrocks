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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Tracks the materialized view activation tasks submitted while the image was being loaded, and lets the FE
 * startup path wait for them before it binds its service ports.
 * <p>
 * Without this gate the FE opens its ports while those activation tasks still occupy every thread of the
 * shared mv-plan-cache pool -- measured at 76s on a 100-mv cluster. Anything else that needs that pool,
 * plan cache building included, is queued behind them, so the FE is reachable long before it can actually
 * answer with a rewritten plan.
 * <p>
 * <b>Scope:</b> this gate waits for activation only. Plan cache building is queued separately by the
 * {@code setActive()} hook and is NOT awaited here, so a query issued right after the gate opens can still
 * find a candidate mv whose plan future is unfinished. What the gate guarantees is that the pool is drained
 * and those builds can run at full width, not that they have finished.
 * <p>
 * There is deliberately <b>no timeout</b>: a gate that gives up would put the FE back to opening its ports
 * onto a half-built plan cache, which is the failure being fixed. Progress is logged periodically instead, so
 * that an operator or an external orchestrator can tell "still making progress" from "stuck" without a timeout
 * having to guess. It is logged rather than exported as a metric on purpose: the http server that serves
 * /metrics is only started after this gate releases, so a gauge could never be scraped while it mattered.
 * <p>
 * This class only waits; it neither submits nor orders the activation tasks. Submission stays in
 * {@link GlobalStateMgr#processMvRelatedMeta()}, which must not block: it runs on the image loading thread and
 * journal replay only starts after it returns.
 */
public class MvStartupActivationCoordinator {
    private static final Logger LOG = LogManager.getLogger(MvStartupActivationCoordinator.class);

    // Only used to wake the waiter up so that it can log progress; it is not a timeout on the gate itself.
    private static final long PROGRESS_LOG_INTERVAL_MS = 10_000L;

    // null until activation tasks have actually been handed over, which is also the "nothing to wait for"
    // signal for await(): checkpoint threads, a disabled gate and an empty catalog all leave it null.
    private volatile CompletableFuture<Void> pipeline;

    private volatile int totalMvs;
    private final AtomicInteger completedMvs = new AtomicInteger();

    /**
     * Hand over the activation futures of every mv processed after the image load, and return immediately.
     *
     * @param activations one future per mv, in submission order; they are already running
     */
    public synchronized void submit(List<CompletableFuture<?>> activations) {
        if (pipeline != null) {
            LOG.warn("startup mv activation was already being tracked, ignoring the new batch");
            return;
        }
        if (activations == null || activations.isEmpty()) {
            LOG.info("no materialized view to activate on startup, readiness gate is a no-op");
            return;
        }
        totalMvs = activations.size();
        // handle() rather than the raw futures: an activation task that throws completes its future
        // exceptionally, and allOf() would then propagate that instead of waiting for the rest. One mv failing
        // to activate is a normal outcome, and the gate's contract is that the batch has drained -- not that
        // every mv activated.
        CompletableFuture<?>[] counted = new CompletableFuture<?>[activations.size()];
        for (int i = 0; i < activations.size(); i++) {
            counted[i] = activations.get(i).handle((result, e) -> {
                if (e != null) {
                    LOG.warn("startup mv activation task failed", e);
                }
                completedMvs.incrementAndGet();
                return null;
            });
        }
        pipeline = CompletableFuture.allOf(counted);
        LOG.info("tracking startup mv activation of {} materialized views for the readiness gate", totalMvs);
    }

    /**
     * Block until every tracked activation task has finished. By design this never times out; see the class
     * javadoc.
     */
    public void await() throws InterruptedException {
        CompletableFuture<Void> current = pipeline;
        if (current == null) {
            return;
        }
        long startMs = System.currentTimeMillis();
        LOG.info("waiting for the startup mv activation of {} materialized views to drain before serving",
                totalMvs);
        while (true) {
            try {
                current.get(PROGRESS_LOG_INTERVAL_MS, TimeUnit.MILLISECONDS);
                LOG.info("startup mv activation gate released after {}ms, {}/{} materialized views activated",
                        System.currentTimeMillis() - startMs, completedMvs.get(), totalMvs);
                return;
            } catch (TimeoutException e) {
                LOG.info("still waiting for startup mv activation: {}/{} materialized views done, waited {}ms",
                        completedMvs.get(), totalMvs, System.currentTimeMillis() - startMs);
            } catch (ExecutionException e) {
                // handle() above absorbs the per-task failures, so this should not happen; release the gate
                // rather than leaving the FE hanging on a batch that will never complete.
                LOG.error("startup mv activation completed exceptionally, releasing the gate", e);
                return;
            }
        }
    }

    public boolean isPending() {
        CompletableFuture<Void> current = pipeline;
        return current != null && !current.isDone();
    }

    public int getTotalMvs() {
        return totalMvs;
    }

    public int getCompletedMvs() {
        return completedMvs.get();
    }
}
