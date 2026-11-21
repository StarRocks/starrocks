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

package com.starrocks.sql.optimizer;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.common.util.concurrent.Uninterruptibles;
import com.starrocks.catalog.BaseTableInfo;
import com.starrocks.common.profile.Timer;
import com.starrocks.common.profile.Tracers;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.CatalogMgr;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.MetadataMgr;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.starrocks.common.profile.Tracers.Module.EXTERNAL;

/**
 * Warms the metadata of external mv base tables in parallel before mv rewrite preprocessing resolves them one
 * by one.
 *
 * <p>The lookups are the ones the validity check makes ({@code MetadataMgr#getTable(ConnectContext,
 * BaseTableInfo)}, reading the iceberg cache only, see {@code MvUtils#getTableChecked}), run on worker threads
 * bound to the query's context. They therefore use the query-level connector metadata of the query, whose
 * per-query caches answer the validity check's lookups afterwards; the results themselves are not kept.</p>
 *
 * <p>Best effort: a lookup that fails or does not finish in time is simply resolved again on demand.</p>
 */
public class MvBaseTableMetadataPrefetcher {
    private static final Logger LOG = LogManager.getLogger(MvBaseTableMetadataPrefetcher.class);
    private static final long NO_TIMEOUT = Long.MAX_VALUE;

    private MvBaseTableMetadataPrefetcher() {
    }

    /**
     * Warm the metadata of the given (external) mv base tables in parallel.
     *
     * @param timeoutMs the maximum time to wait for the lookups; lookups that have not finished by then keep
     *                  running in the background and their results are simply not waited for.
     *                  {@code Long.MAX_VALUE} means no limit.
     */
    public static void prefetchBaseTables(Collection<BaseTableInfo> baseTableInfos, ConnectContext session,
                                          long timeoutMs) {
        if (!session.getSessionVariable().enableParallelPrefetchMvBaseTableMetadata()) {
            return;
        }
        prefetchBaseTables(baseTableInfos, session, GlobalStateMgr.getCurrentState().getMetadataMgr(), timeoutMs);
    }

    @VisibleForTesting
    static void prefetchBaseTables(Collection<BaseTableInfo> baseTableInfos, ConnectContext session,
                                   MetadataMgr metadataMgr, long timeoutMs) {
        // without a query id the lookups would not share the query-level connector metadata, so there is
        // nothing to warm
        if (!session.getSessionVariable().enableParallelPrefetchMvBaseTableMetadata() || session.getQueryId() == null
                || timeoutMs <= 0) {
            return;
        }

        Set<BaseTableInfo> toPrefetch = new LinkedHashSet<>();
        for (BaseTableInfo baseTableInfo : baseTableInfos) {
            if (baseTableInfo == null || baseTableInfo.isInternalCatalog()
                    || Strings.isNullOrEmpty(baseTableInfo.getCatalogName())
                    || Strings.isNullOrEmpty(baseTableInfo.getDbName())
                    || Strings.isNullOrEmpty(baseTableInfo.getTableName())
                    || CatalogMgr.isInternalCatalog(baseTableInfo.getCatalogName())) {
                continue;
            }
            toPrefetch.add(baseTableInfo);
        }
        if (toPrefetch.isEmpty()) {
            return;
        }

        // the validity check reads the iceberg cache only, so do the lookups the same way: no more remote work
        // than the check itself would do, and the same per-query state afterwards
        try (ConnectContext.ContextScope ignored = ConnectContext.enterOnlyReadIcebergCacheScope(session)) {
            prefetch(toPrefetch, session, metadataMgr, timeoutMs);
        }
    }

    private static void prefetch(Set<BaseTableInfo> baseTableInfos, ConnectContext session, MetadataMgr metadataMgr,
                                 long timeoutMs) {
        UUID queryId = session.getQueryId();
        int poolSize = Math.max(1, Math.min(baseTableInfos.size(),
                session.getSessionVariable().getPrepareMetadataPoolSize()));
        String queryIdString = queryId.toString();
        ExecutorService executorService = Executors.newFixedThreadPool(poolSize,
                new ThreadFactoryBuilder().setNameFormat(
                                String.format("mv-base-table-metadata-%s-%%d", queryIdString))
                        // lookups abandoned on timeout must not keep the FE process alive
                        .setDaemon(true).build());
        Tracers ownerTracers = Tracers.get();
        AtomicBoolean cancelled = new AtomicBoolean();
        long startNanos = System.nanoTime();
        long timeoutNanos = TimeUnit.MILLISECONDS.toNanos(timeoutMs);
        int pending = 0;
        boolean timedOut = false;

        try (Timer ignored = Tracers.watchScope(EXTERNAL, "EXTERNAL.parallel_prefetch_mv_base_tables")) {
            BlockingQueue<PrefetchResult> completed = new LinkedBlockingQueue<>();
            for (BaseTableInfo baseTableInfo : baseTableInfos) {
                submit(executorService, completed, cancelled, metadataMgr, baseTableInfo, session, ownerTracers);
                pending++;
            }
            while (pending > 0) {
                PrefetchResult result;
                try {
                    result = poll(completed, timeoutMs, startNanos, timeoutNanos);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                if (result == null) {
                    timedOut = true;
                    LOG.warn("Prefetch mv base table metadata for query [{}] timed out after {} ms with {} lookups " +
                            "pending; the pending lookups will be retried on demand", queryIdString, timeoutMs, pending);
                    return;
                }
                pending--;
                // tracers are merged on this thread only
                ownerTracers.mergeFrom(result.tracers);
                logPrefetchFailure(queryIdString, result.baseTableInfo, result.failure);
            }
        } finally {
            // lookups that have not started yet are skipped
            cancelled.set(true);
            executorService.shutdown();
            // Wait for running lookups so none outlives the call, except on timeout: then the in-flight lookups
            // are left to finish in the background instead of blocking the query thread beyond its budget. They
            // are not interrupted because connector code may not handle interrupts gracefully.
            if (!timedOut) {
                Uninterruptibles.awaitTerminationUninterruptibly(executorService);
            }
        }
    }

    /**
     * Wait for the next lookup result, or return null once the timeout has elapsed since {@code startNanos}.
     */
    private static PrefetchResult poll(BlockingQueue<PrefetchResult> completed, long timeoutMs, long startNanos,
                                       long timeoutNanos) throws InterruptedException {
        if (timeoutMs == NO_TIMEOUT) {
            return completed.take();
        }
        long remainingNanos = timeoutNanos - (System.nanoTime() - startNanos);
        return remainingNanos <= 0 ? completed.poll() : completed.poll(remainingNanos, TimeUnit.NANOSECONDS);
    }

    private static void submit(ExecutorService executorService, BlockingQueue<PrefetchResult> completed,
                               AtomicBoolean cancelled, MetadataMgr metadataMgr, BaseTableInfo baseTableInfo,
                               ConnectContext session, Tracers ownerTracers) {
        Tracers forked = ownerTracers.fork(true);
        // whenComplete also runs for Errors, so every submitted lookup reports back exactly once.
        CompletableFuture.runAsync(() -> {
            if (!cancelled.get()) {
                prefetchTable(metadataMgr, baseTableInfo, session, forked);
            }
        }, executorService).whenComplete((ignored, throwable) ->
                completed.add(new PrefetchResult(baseTableInfo, forked, throwable)));
    }

    private static void prefetchTable(MetadataMgr metadataMgr, BaseTableInfo baseTableInfo, ConnectContext session,
                                      Tracers forkedTracers) {
        Tracers previousTracers = Tracers.get();
        ConnectContext previousContext = ConnectContext.get();
        Tracers.set(forkedTracers);
        // bind the query's context so the lookup uses the query-level connector metadata of its query id
        session.setThreadLocalInfo();
        try {
            metadataMgr.getTable(session, baseTableInfo);
        } finally {
            if (previousContext == null) {
                ConnectContext.remove();
            } else {
                previousContext.setThreadLocalInfo();
            }
            Tracers.set(previousTracers);
        }
    }

    private static void logPrefetchFailure(String queryId, BaseTableInfo baseTableInfo, Throwable throwable) {
        if (throwable != null) {
            Throwable cause = throwable.getCause() == null ? throwable : throwable.getCause();
            LOG.warn("Failed to prefetch metadata of mv base table {} for query [{}]; " +
                    "the metadata lookup will be retried on demand", baseTableInfo, queryId, cause);
        }
    }

    private record PrefetchResult(BaseTableInfo baseTableInfo, Tracers tracers, Throwable failure) {
    }
}
