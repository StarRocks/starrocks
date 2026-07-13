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

package com.starrocks.http.rest.context;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Strings;
import com.starrocks.common.DdlException;
import com.starrocks.context.ContextInternalTables;
import com.starrocks.context.ContextMgr;
import com.starrocks.context.ContextReadExecutor;
import com.starrocks.http.ActionController;
import com.starrocks.http.BaseRequest;
import com.starrocks.http.BaseResponse;
import com.starrocks.http.IllegalArgException;
import com.starrocks.http.rest.RestBaseAction;
import com.starrocks.http.rest.RestBaseResult;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import io.netty.handler.codec.http.HttpMethod;

/**
 * {@code GET /api/context/stats[?contextbase=<name>]}. When {@code contextbase} is supplied,
 * counts rows in each internal table filtered by that contextbase id. Without it, counts are
 * cluster-wide (same numbers as {@code SHOW CONTEXT STATUS} but on a programmatic surface).
 *
 * <p>This is the right shape for capacity planning dashboards that want one row per contextbase.
 * Each metric reports {@code -1} when the underlying table isn't materialized yet, so the dashboard
 * can render "n/a" without the caller having to guess.
 */
public class ContextStatsAction extends RestBaseAction {

    // TTL cache keyed by contextbase name (empty string for cluster-wide). Capacity-planning
    // dashboards poll this frequently — without a cache, each poll fires 5-7 full table COUNT(*)
    // queries which can dominate FE I/O on operator clusters.
    private static final long CACHE_TTL_MS = 5_000L;
    private static final java.util.concurrent.ConcurrentHashMap<String, CachedStats> CACHE =
            new java.util.concurrent.ConcurrentHashMap<>();

    public ContextStatsAction(ActionController controller) {
        super(controller);
    }

    public static void registerAction(ActionController controller) throws IllegalArgException {
        controller.registerHandler(HttpMethod.GET, "/api/context/stats", new ContextStatsAction(controller));
    }

    @Override
    protected void executeWithoutPassword(BaseRequest request, BaseResponse response)
            throws DdlException, com.starrocks.authorization.AccessDeniedException {
        try {
            String contextBase = request.getSingleParameter("contextbase");
            ContextMgr mgr = GlobalStateMgr.getCurrentState().getContextMgr();
            ContextReadExecutor reader = GlobalStateMgr.getCurrentState().getContextReadExecutor();

            String cacheKey = Strings.nullToEmpty(contextBase);
            CachedStats cached = CACHE.get(cacheKey);
            long now = System.currentTimeMillis();
            if (cached != null && now - cached.computedAtMs < CACHE_TTL_MS) {
                // Auth re-check on every hit — the cached payload includes a contextbase scope
                // and we must not skip the per-caller permission gate even when reusing the row.
                // Cluster-wide stats (no contextbase param) gate behind a true admin override
                // (OPERATE / SECURITY); per-base stats gate behind the standard USAGE check that
                // includes admin override, per-base GRANT, and owner match.
                if (Strings.isNullOrEmpty(contextBase)) {
                    ContextRestAuth.checkAdmin(ConnectContext.get());
                } else {
                    ContextRestAuth.checkOnContextBase(ConnectContext.get(), contextBase,
                            ContextRestAuth.BaseAction.USAGE);
                }
                sendResultByJson(request, response, cached.response);
                return;
            }
            StatsResponse resp = new StatsResponse();
            resp.contextbase = contextBase;

            if (Strings.isNullOrEmpty(contextBase)) {
                // Cluster-wide stats are admin-only: emitting them to anyone with FE access leaks
                // counts of every base in the deployment. Gate behind OPERATE / SECURITY rather
                // than CREATE_CONTEXTBASE — the latter is the per-tenant self-service create
                // privilege and must not imply cluster-wide observability.
                ContextRestAuth.checkAdmin(ConnectContext.get());
                resp.collectionCount = mgr.listCollections(null).size();
                resp.workspaceCount = mgr.listWorkspaces(null).size();
                resp.entityCount = reader.countRows(ContextInternalTables.HEADS);
                resp.versionCount = reader.countRows(ContextInternalTables.VERSIONS);
                resp.fragmentCount = reader.countRows(ContextInternalTables.FRAGMENTS);
                resp.refCount = reader.countRows(ContextInternalTables.REFS);
                resp.commitCount = reader.countRows(ContextInternalTables.COMMITS);
                CACHE.put(cacheKey, new CachedStats(resp, now));
                sendResultByJson(request, response, resp);
                return;
            }

            ContextMgr.ContextBaseMeta cb = mgr.getContextBase(contextBase);
            if (cb == null) {
                sendResult(request, response, new RestBaseResult("contextbase not found: " + contextBase));
                return;
            }
            ContextRestAuth.checkOnContextBase(ConnectContext.get(), contextBase,
                    ContextRestAuth.BaseAction.USAGE);
            resp.contextbaseId = cb.getId();
            resp.collectionCount = mgr.listCollections(contextBase).size();

            long workspaceMatches = 0L;
            String wsPrefix = contextBase + ".";
            for (ContextMgr.WorkspaceMeta ws : mgr.listWorkspaces(null)) {
                if (ws.getName() != null && ws.getName().startsWith(wsPrefix)) {
                    workspaceMatches++;
                }
            }
            resp.workspaceCount = workspaceMatches;

            // Filter the heavy tables by `contextbase_id`. The filter clause goes through the same
            // SimpleExecutor path as the unfiltered case, so behaviour during table-not-ready states
            // is identical: -1 surfaces back as "n/a" to the dashboard.
            String filter = "contextbase_id = " + cb.getId();
            resp.entityCount = reader.countWithFilter(ContextInternalTables.HEADS, filter);
            resp.versionCount = reader.countWithFilter(ContextInternalTables.VERSIONS, filter);
            // Fragments and refs don't carry contextbase_id directly; the read executor's per-base
            // helpers join through heads so the count is properly scoped to this contextbase.
            resp.fragmentCount = reader.countFragmentsForContextBase(cb.getId());
            resp.refCount = reader.countRefsForContextBase(cb.getId());
            resp.commitCount = reader.countWithFilter(ContextInternalTables.COMMITS, filter);
            CACHE.put(cacheKey, new CachedStats(resp, now));

            sendResultByJson(request, response, resp);
        } catch (com.starrocks.context.error.ContextException e) {
            sendResultByJson(request, response,
                    ContextErrorResult.fromException(e, ContextRestAuth.currentRequestId()));
        }
    }

    private static final class CachedStats {
        final StatsResponse response;
        final long computedAtMs;

        CachedStats(StatsResponse response, long computedAtMs) {
            this.response = response;
            this.computedAtMs = computedAtMs;
        }
    }

    private static final class StatsResponse {
        public String contextbase;

        @JsonProperty("contextbase_id")
        public long contextbaseId;

        @JsonProperty("collection_count")
        public long collectionCount;

        @JsonProperty("workspace_count")
        public long workspaceCount;

        @JsonProperty("entity_count")
        public long entityCount;

        @JsonProperty("version_count")
        public long versionCount;

        @JsonProperty("fragment_count")
        public long fragmentCount;

        @JsonProperty("ref_count")
        public long refCount;

        @JsonProperty("commit_count")
        public long commitCount;
    }
}
