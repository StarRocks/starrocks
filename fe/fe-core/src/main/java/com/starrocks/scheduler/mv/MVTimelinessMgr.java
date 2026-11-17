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

import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.MvRefreshArbiter;
import com.starrocks.catalog.MvUpdateInfo;
import com.starrocks.catalog.TableProperty;
import com.starrocks.catalog.mv.MVTimelinessArbiter;
import com.starrocks.common.Config;
import com.starrocks.common.profile.Tracers;
import com.starrocks.memory.MemoryTrackable;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.StarRocksPlannerException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Manage the timeliness of materialized view according to the MVChangeEvent across multiple queries.
 * </p>
 * MV's timeliness is a bit heavy for each query's execution, so we cache the timeliness info for each MV. MV can have
 * different query consistency modes, each mode has different timeliness info needs.
 *
 * Now {@code MVTimelinessMgr} only supports `FORCE_MV` mode's timeliness info cache because it's only affected by
 * the MV's refresh and partition drop in this mode.
 *
 * TODO: we can track more mv/base table's changes to update the timeliness info in the future.
 */
public class MVTimelinessMgr implements MemoryTrackable {
    private static final Logger LOG = LogManager.getLogger(MVTimelinessMgr.class);
    private static final long AWAIT_SLICE_MS = 100;
    private static final Duration AWAIT_SLICE = Duration.ofMillis(AWAIT_SLICE_MS);

    // materialized view -> MvUpdateInfo, completed by the lookup that put the future and awaited by the others. An
    // invalidation removes the future, also one still being computed: later lookups put a new one, so a result read
    // from the state before the invalidation is never handed to them.
    private final Map<MaterializedView, CompletableFuture<MvUpdateInfo>> mvTimelinessMap = Maps.newConcurrentMap();

    public enum MVChangeEvent {
        MV_REFRESHED,
        MV_PARTITION_DROPPED,
    }

    public MvUpdateInfo getMVTimelinessInfo(MaterializedView mv,
                                            MVTimelinessArbiter.QueryRewriteParams queryRewriteParams) {
        if (!isEnableMVTimelinessGlobalCache(mv)) {
            return MvRefreshArbiter.getMVTimelinessUpdateInfo(mv, queryRewriteParams);
        }
        CompletableFuture<MvUpdateInfo> mine = new CompletableFuture<>();
        CompletableFuture<MvUpdateInfo> existing = mvTimelinessMap.putIfAbsent(mv, mine);
        if (existing != null) {
            return awaitInFlight(mv, existing);
        }
        MvUpdateInfo computed;
        try {
            computed = MvRefreshArbiter.getMVTimelinessUpdateInfo(mv, queryRewriteParams);
        } catch (Throwable t) {
            mvTimelinessMap.remove(mv, mine);
            mine.completeExceptionally(t);
            throw t;
        }
        // UNKNOWN is a failed check, not a timeliness: caching it would keep the MV out of rewrite until the next
        // invalidation. The lookups already waiting get it all the same.
        if (computed == null || computed.getMVToRefreshType() == MvUpdateInfo.MvToRefreshType.UNKNOWN) {
            mvTimelinessMap.remove(mv, mine);
        }
        mine.complete(computed);
        // DROP removes the MV from the catalog before it evicts this entry. A future put after that eviction would
        // otherwise stay forever, holding the MV and its MvUpdateInfo.
        if (!isInCatalog(mv)) {
            mvTimelinessMap.remove(mv, mine);
        }
        return computed;
    }

    /**
     * Waits on the check another lookup is computing, in slices so that a cancelled statement stops waiting:
     * KILL QUERY and the query timeout only mark the statement, they do not interrupt this thread.
     *
     * <p>Once the statement's budget is spent the lookup fails rather than computing the check itself: that would
     * repeat the same slow reads -- the base tables' partitions through the connector -- once per waiter, with nothing
     * bounding them either. The failure reaches the caller as a failing check would: query rewrite preprocessing
     * skips the MV for this query, while a query reading the MV through transparent rewrite may fail with it.
     */
    private MvUpdateInfo awaitInFlight(MaterializedView mv, CompletableFuture<MvUpdateInfo> existing) {
        ConnectContext context = ConnectContext.get();
        // Without a statement there is no budget and no cancellation: wait as long as the check takes.
        Instant deadline = context == null
                ? Instant.MAX
                : context.getStartTimeInstant().plusSeconds(context.getExecTimeout());
        while (true) {
            if (context != null && context.isStatementCancelled()) {
                throw new StarRocksPlannerException("statement cancelled while waiting for the timeliness of " +
                        "materialized view " + mv.getName(), ErrorType.USER_ERROR);
            }
            Duration remaining = Duration.between(Instant.now(), deadline);
            boolean budgetSpent = remaining.isNegative() || remaining.isZero();
            // Not Math.min over toMillis(): the remaining time to Instant.MAX overflows a long of millis. With the
            // budget spent, a zero wait still takes a result that is already there.
            long sliceMs = budgetSpent ? 0
                    : remaining.compareTo(AWAIT_SLICE) < 0 ? Math.max(remaining.toMillis(), 1) : AWAIT_SLICE_MS;
            try {
                return existing.get(sliceMs, TimeUnit.MILLISECONDS);
            } catch (TimeoutException e) {
                if (budgetSpent) {
                    LOG.warn("Timed out waiting for the timeliness check of materialized view {} computed by " +
                            "another query", mv.getName());
                    Tracers.count(Tracers.Module.MV, "MVTimelinessAwaitTimeout", 1);
                    throw new StarRocksPlannerException("timed out waiting for the timeliness of materialized view " +
                            mv.getName() + " computed by another query", ErrorType.USER_ERROR);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new StarRocksPlannerException("interrupted while waiting for the timeliness of " +
                        "materialized view " + mv.getName(), ErrorType.USER_ERROR);
            } catch (ExecutionException e) {
                // The failure of the lookup computing it is this lookup's failure too: retrying here would
                // compute it once per waiter in turn when the check keeps failing.
                Throwables.throwIfUnchecked(e.getCause());
                throw new RuntimeException(e.getCause());
            }
        }
    }

    static boolean isInCatalog(MaterializedView mv) {
        return GlobalStateMgr.getCurrentState().getLocalMetastore().mayGetTable(mv.getDbId(), mv.getId()).isPresent();
    }

    public static boolean isEnableMVTimelinessGlobalCache(MaterializedView mv) {
        // To avoid bad cases, we disable the global cache for MV timeliness
        if (!Config.enable_mv_query_context_cache) {
            return false;
        }
        TableProperty.QueryRewriteConsistencyMode consistencyMode =
                mv.getTableProperty().getQueryRewriteConsistencyMode();
        return consistencyMode == TableProperty.QueryRewriteConsistencyMode.FORCE_MV;
    }

    public void triggerEvent(MaterializedView mv, MVChangeEvent event) {
        if (!isEnableMVTimelinessGlobalCache(mv)) {
            return;
        }
        TableProperty.QueryRewriteConsistencyMode consistencyMode =
                mv.getTableProperty().getQueryRewriteConsistencyMode();
        if (consistencyMode == TableProperty.QueryRewriteConsistencyMode.FORCE_MV) {
            if (event == MVChangeEvent.MV_REFRESHED || event == MVChangeEvent.MV_PARTITION_DROPPED) {
                mvTimelinessMap.remove(mv);
            }
        } else {
            mvTimelinessMap.remove(mv);
        }
    }

    public void remove(MaterializedView mv) {
        mvTimelinessMap.remove(mv);
    }

    @Override
    public Map<String, Long> estimateCount() {
        return Map.of("mvTimelinessMap", (long) mvTimelinessMap.size());
    }
}