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

import com.starrocks.catalog.MaterializedView;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

/**
 * Orders and submits materialized view activation tasks on FE startup, layer by layer.
 * The caller is the image-loading thread, so scheduling returns immediately and a dedicated daemon thread
 * waits for each layer. No mv-plan-cache thread waits for a base MV queued behind it in the same pool.
 * If the coordinator fails, it submits the remaining MVs together so that none is left behind.
 * The returned completion future and counters describe activation only; plan-cache builds run separately.
 */
public class MvStartupActivationCoordinator {
    private static final Logger LOG = LogManager.getLogger(MvStartupActivationCoordinator.class);

    // Only used to wake the waiter up so that it can log progress; it is not a timeout on the batch itself.
    private static final long PROGRESS_LOG_INTERVAL_MS = 10_000L;

    private static final String SCHEDULER_THREAD_NAME = "mv-startup-activation";
    // The scheduler thread is only needed while a startup batch drains; let it go once it has been idle this long.
    private static final long SCHEDULER_KEEP_ALIVE_SECONDS = 60L;

    // null until a batch with at least one mv has been scheduled, which is also the "nothing to wait for" signal
    // for await(): checkpoint threads, a disabled post-image async reload and an empty catalog all leave it null.
    // The FE startup path does not wait for this pipeline before opening its ports.
    private volatile CompletableFuture<Void> pipeline;

    private volatile int totalMvs;
    private final AtomicInteger completedMvs = new AtomicInteger();
    private volatile int totalLayers;
    private final AtomicInteger completedLayers = new AtomicInteger();

    // Created on the first schedule() rather than here: every GlobalStateMgr, checkpoint ones included, builds a
    // coordinator, and most of them never schedule anything. Guarded by this.
    private ExecutorService scheduler;

    /**
     * Activate the given mvs layer by layer on the coordinator's own thread, and return immediately.
     * <p>
     * Every mv of a layer is handed to {@code activator}, which is expected to submit its activation task and
     * return a future completed when that task has finished. The next layer is only started once every future of
     * the current one has completed, exceptionally or not. Only the first batch of this coordinator is tracked by
     * the startup batch; a later one still runs, untracked.
     *
     * @param layers    the mvs to activate, in topological layers: every base mv of an mv is in an earlier layer
     * @param activator submits the activation of one mv and returns a future of its completion
     * @return a future completed once every mv of the batch has finished activating; it never completes
     *         exceptionally
     */
    public synchronized CompletableFuture<Void> schedule(List<List<MaterializedView>> layers,
                                                         Function<MaterializedView, CompletableFuture<?>> activator) {
        // Copied because the layers are walked on another thread after this method has returned.
        List<List<MaterializedView>> plan = new ArrayList<>();
        int mvCount = 0;
        if (layers != null) {
            for (List<MaterializedView> layer : layers) {
                List<MaterializedView> copy = layer == null ? new ArrayList<>() : new ArrayList<>(layer);
                plan.add(copy);
                mvCount += copy.size();
            }
        }
        if (mvCount == 0) {
            LOG.info("no materialized view to activate on startup, nothing to schedule");
            return CompletableFuture.completedFuture(null);
        }

        CompletableFuture<Void> done = new CompletableFuture<>();
        // The counters only track the first batch, the one of the process startup. Unit tests reload the
        // mvs many times in one JVM, so a later batch must still run, just without tracking.
        boolean tracked = pipeline == null;
        if (tracked) {
            totalMvs = mvCount;
            totalLayers = plan.size();
            // Published last: await() reads the totals once it sees the pipeline.
            pipeline = done;
            LOG.info("tracking startup mv activation of {} materialized views in {} layers for progress tracking",
                    mvCount, plan.size());
        } else {
            LOG.info("startup mv activation was already being tracked, running this batch of {} materialized views " +
                    "in {} layers untracked", mvCount, plan.size());
        }
        int batchSize = mvCount;
        getOrCreateScheduler().execute(() -> runLayers(plan, activator, batchSize, tracked, done));
        return done;
    }

    private ExecutorService getOrCreateScheduler() {
        if (scheduler == null) {
            // A single thread keeps the batches in order; it is never a mv-plan-cache pool thread.
            ThreadPoolExecutor executor = new ThreadPoolExecutor(1, 1, SCHEDULER_KEEP_ALIVE_SECONDS, TimeUnit.SECONDS,
                    new LinkedBlockingQueue<>(), MvStartupActivationCoordinator::newSchedulerThread);
            executor.allowCoreThreadTimeOut(true);
            scheduler = executor;
        }
        return scheduler;
    }

    private static Thread newSchedulerThread(Runnable runnable) {
        Thread thread = new Thread(runnable, SCHEDULER_THREAD_NAME);
        thread.setDaemon(true);
        return thread;
    }

    private void runLayers(List<List<MaterializedView>> layers, Function<MaterializedView, CompletableFuture<?>> activator,
                           int mvCount, boolean tracked, CompletableFuture<Void> done) {
        long startMs = System.currentTimeMillis();
        int layerCount = layers.size();
        Set<MaterializedView> invoked = Collections.newSetFromMap(new IdentityHashMap<>());
        // The futures of the layer being submitted, so that the fallback can still wait for them.
        List<CompletableFuture<Void>> inFlight = new ArrayList<>();
        int layerIndex = 0;
        try {
            int mvIndex = 0;
            for (; layerIndex < layerCount; layerIndex++) {
                long layerStartMs = System.currentTimeMillis();
                List<MaterializedView> layer = layers.get(layerIndex);
                AtomicInteger failures = new AtomicInteger();
                inFlight.clear();
                for (MaterializedView mv : layer) {
                    mvIndex++;
                    LOG.info("start to reload mv {}/{}: {}({}) in layer {}/{}", mvIndex, mvCount, mv.getName(),
                            mv.getId(), layerIndex + 1, layerCount);
                    inFlight.add(activateSafely(mv, activator, tracked, failures, invoked));
                }
                // activateSafely() has absorbed every failure, so this cannot complete exceptionally: it returns
                // once the whole layer has finished, failed mvs included, and only then is the next layer submitted.
                CompletableFuture.allOf(inFlight.toArray(new CompletableFuture<?>[0])).join();
                LOG.info("finished startup mv activation layer {}/{}: {} mvs, {} failed, cost: {}ms",
                        layerIndex + 1, layerCount, layer.size(), failures.get(),
                        System.currentTimeMillis() - layerStartMs);
                if (tracked) {
                    completedLayers.incrementAndGet();
                }
            }
        } catch (Throwable t) {
            activateRemainingAtOnce(layers, layerIndex, activator, tracked, invoked, inFlight, t);
        } finally {
            LOG.info("startup mv activation pipeline drained: {} mvs in {} layers, cost: {}ms",
                    mvCount, layerCount, System.currentTimeMillis() - startMs);
            done.complete(null);
        }
    }

    /**
     * The degraded path, equal to the flat submission used before the layering: submit every mv from the failed
     * layer onwards that has not been handed to the activator yet, all at once, and wait for them together with
     * whatever the failed layer had already submitted. No mv is left unactivated and the batch still completes.
     */
    private void activateRemainingAtOnce(List<List<MaterializedView>> layers, int failedLayerIndex,
                                         Function<MaterializedView, CompletableFuture<?>> activator, boolean tracked,
                                         Set<MaterializedView> invoked, List<CompletableFuture<Void>> inFlight,
                                         Throwable cause) {
        try {
            List<MaterializedView> remaining = new ArrayList<>();
            for (int i = failedLayerIndex; i < layers.size(); i++) {
                for (MaterializedView mv : layers.get(i)) {
                    if (!invoked.contains(mv)) {
                        remaining.add(mv);
                    }
                }
            }
            LOG.error("startup mv activation coordinator failed at layer {}, submitting the remaining {} mvs at once",
                    failedLayerIndex + 1, remaining.size(), cause);
            AtomicInteger failures = new AtomicInteger();
            List<CompletableFuture<Void>> pending = new ArrayList<>(inFlight);
            for (MaterializedView mv : remaining) {
                try {
                    pending.add(activateSafely(mv, activator, tracked, failures, invoked));
                } catch (Throwable e) {
                    // Keep going: one more broken mv must not leave the ones behind it unactivated.
                    failures.incrementAndGet();
                    if (tracked) {
                        completedMvs.incrementAndGet();
                    }
                    LOG.error("failed to submit the startup activation of mv {}({})", mv.getName(), mv.getId(), e);
                }
            }
            CompletableFuture.allOf(pending.toArray(new CompletableFuture<?>[0])).join();
            LOG.info("finished the fallback startup mv activation: {} mvs, {} failed", remaining.size(), failures.get());
        } catch (Throwable t) {
            LOG.error("fallback startup mv activation failed, some materialized views may stay inactive", t);
        }
    }

    /**
     * Hand one mv to the activator and return a future that completes, normally, once its activation is over.
     * An activator that throws an Exception counts as one failed mv.
     * <p>
     * An Error thrown by the activator deliberately escapes: it means something is wrong beyond this one mv, so
     * runLayers() stops layering and falls back to {@link #activateRemainingAtOnce}, the pre-layering behaviour.
     */
    private CompletableFuture<Void> activateSafely(MaterializedView mv,
                                                   Function<MaterializedView, CompletableFuture<?>> activator,
                                                   boolean tracked, AtomicInteger failures,
                                                   Set<MaterializedView> invoked) {
        invoked.add(mv);
        CompletableFuture<?> activation;
        try {
            activation = activator.apply(mv);
            if (activation == null) {
                activation = CompletableFuture.completedFuture(null);
            }
        } catch (Exception e) {
            // Routed through the same handle() below, which logs it and counts the mv as done and failed.
            activation = CompletableFuture.failedFuture(e);
        }
        // handle() rather than the raw future: one mv failing to activate is a normal outcome. With the raw futures
        // the layer's allOf() would complete exceptionally and its join() would abandon the layering.
        return activation.handle((result, e) -> {
            if (e != null) {
                failures.incrementAndGet();
                LOG.warn("startup activation of mv {}({}) failed", mv.getName(), mv.getId(), e);
            }
            if (tracked) {
                completedMvs.incrementAndGet();
            }
            return null;
        });
    }

    /**
     * Block until every tracked activation task has finished, logging progress periodically.
     */
    public void await() throws InterruptedException {
        CompletableFuture<Void> current = pipeline;
        if (current == null) {
            return;
        }
        long startMs = System.currentTimeMillis();
        LOG.info("waiting for the startup mv activation of {} materialized views in {} layers to drain to finish",
                totalMvs, totalLayers);
        while (true) {
            try {
                current.get(PROGRESS_LOG_INTERVAL_MS, TimeUnit.MILLISECONDS);
                LOG.info("startup mv activation completed after {}ms, {}/{} materialized views activated, " +
                        "{}/{} layers done", System.currentTimeMillis() - startMs,
                        completedMvs.get(), totalMvs, completedLayers.get(), totalLayers);
                return;
            } catch (TimeoutException e) {
                LOG.info("still waiting for startup mv activation: {}/{} materialized views done, " +
                        "{}/{} layers done, waited {}ms", completedMvs.get(), totalMvs,
                        completedLayers.get(), totalLayers, System.currentTimeMillis() - startMs);
            } catch (ExecutionException e) {
                // handle() above absorbs the per-task failures, so this should not happen; complete the batch
                // rather than leaving the FE hanging on a batch that will never complete.
                LOG.error("startup mv activation completed exceptionally, completing the batch", e);
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

    public int getTotalLayers() {
        return totalLayers;
    }

    public int getCompletedLayers() {
        return completedLayers.get();
    }
}
