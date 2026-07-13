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
import com.starrocks.common.DdlException;
import com.starrocks.context.ContextInternalTables;
import com.starrocks.context.ContextMetaManager;
import com.starrocks.context.ContextMgr;
import com.starrocks.context.ContextReadExecutor;
import com.starrocks.http.ActionController;
import com.starrocks.http.BaseRequest;
import com.starrocks.http.BaseResponse;
import com.starrocks.http.IllegalArgException;
import com.starrocks.http.rest.RestBaseAction;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import io.netty.handler.codec.http.HttpMethod;

/**
 * {@code GET /api/context/health}. Aggregates module-wide readiness signals into one JSON blob so
 * operators and dashboards can poll a single endpoint:
 *
 * <ul>
 *   <li>{@code is_leader} — whether this FE is the leader (the only node where the daemons run)</li>
 *   <li>{@code internal_tables_ready} — every TableKeeper reports its table exists in
 *       {@code __internal_context}</li>
 *   <li>{@code metadata_counts} — live counts mirroring {@code SHOW CONTEXT STATUS}</li>
 *   <li>{@code commit_count} — total rows in {@code context_commits} (proxy for "did anything ever
 *       write?" — non-zero on a healthy cluster after the first upsert)</li>
 * </ul>
 *
 * <p>Returns 200 even when subsystems are not ready — the caller decides what's a hard failure
 * vs. a transient warmup state. Returning 503 here would mask the partial-readiness shape that the
 * fields above expose, which is what dashboards actually need.
 */
public class ContextHealthAction extends RestBaseAction {

    // TTL cache: dashboards typically poll this endpoint every 1-5 seconds; without a cache,
    // each poll fires four full-table COUNT(*) queries plus four metadata enumerations. A
    // short TTL means at most one underlying read per TTL_MS window across all callers.
    private static final long CACHE_TTL_MS = 5_000L;
    private static final java.util.concurrent.atomic.AtomicReference<CachedHealth> CACHE =
            new java.util.concurrent.atomic.AtomicReference<>();

    public ContextHealthAction(ActionController controller) {
        super(controller);
    }

    public static void registerAction(ActionController controller) throws IllegalArgException {
        controller.registerHandler(HttpMethod.GET, "/api/context/health", new ContextHealthAction(controller));
    }

    @Override
    protected void executeWithoutPassword(BaseRequest request, BaseResponse response)
            throws DdlException, com.starrocks.authorization.AccessDeniedException {
        // Health surfaces module-wide row counts and table-readiness; this is operator-only
        // data (cluster-wide visibility into module activity) and has no per-base scope to
        // narrow to, so gate behind a true admin override (OPERATE / SECURITY). The previous
        // gate used CREATE_CONTEXTBASE which is the module-create privilege — anyone provisioned
        // for self-service base creation would have inherited cluster-wide health visibility.
        ContextRestAuth.checkAdmin(ConnectContext.get());
        try {
            HealthResponse resp = copyOf(computeWithCache());
            resp.requestId = ContextRestAuth.currentRequestId();
            sendResultByJson(request, response, resp);
        } catch (com.starrocks.context.error.ContextException e) {
            sendResultByJson(request, response,
                    ContextErrorResult.fromException(e, ContextRestAuth.currentRequestId()));
        }
    }

    private static HealthResponse copyOf(HealthResponse src) {
        // request_id is per-call diagnostic data; the readiness counts are cached. Hand the caller
        // a fresh copy so we can stamp their request_id without mutating the shared cache entry.
        HealthResponse out = new HealthResponse();
        out.healthy = src.healthy;
        out.isLeader = src.isLeader;
        out.internalTablesReady = src.internalTablesReady;
        out.contextbaseCount = src.contextbaseCount;
        out.collectionCount = src.collectionCount;
        out.workspaceCount = src.workspaceCount;
        out.retrievalProfileCount = src.retrievalProfileCount;
        out.entityCount = src.entityCount;
        out.versionCount = src.versionCount;
        out.commitCount = src.commitCount;
        out.taskCount = src.taskCount;
        return out;
    }

    private HealthResponse computeWithCache() {
        CachedHealth cached = CACHE.get();
        long now = System.currentTimeMillis();
        if (cached != null && now - cached.computedAtMs < CACHE_TTL_MS) {
            return cached.response;
        }
        HealthResponse resp = new HealthResponse();
        resp.isLeader = GlobalStateMgr.getCurrentState().isLeader();

        ContextMetaManager metaMgr = GlobalStateMgr.getCurrentState().getContextMetaManager();
        resp.internalTablesReady = metaMgr != null && metaMgr.isReady();

        ContextMgr mgr = GlobalStateMgr.getCurrentState().getContextMgr();
        resp.contextbaseCount = mgr.listContextBases().size();
        resp.collectionCount = mgr.listCollections(null).size();
        resp.workspaceCount = mgr.listWorkspaces(null).size();
        resp.retrievalProfileCount = mgr.listRetrievalProfiles().size();

        ContextReadExecutor reader = GlobalStateMgr.getCurrentState().getContextReadExecutor();
        // countRows returns -1 when the table isn't materialized yet — surface that as -1 in JSON
        // so dashboards distinguish "table not ready" from "ready, zero rows".
        resp.entityCount = reader.countRows(ContextInternalTables.HEADS);
        resp.versionCount = reader.countRows(ContextInternalTables.VERSIONS);
        resp.commitCount = reader.countRows(ContextInternalTables.COMMITS);
        resp.taskCount = reader.countRows(ContextInternalTables.TASKS);

        resp.healthy = resp.isLeader && resp.internalTablesReady && resp.commitCount >= 0;
        CACHE.set(new CachedHealth(resp, now));
        return resp;
    }

    private static final class CachedHealth {
        final HealthResponse response;
        final long computedAtMs;

        CachedHealth(HealthResponse response, long computedAtMs) {
            this.response = response;
            this.computedAtMs = computedAtMs;
        }
    }

    private static final class HealthResponse {
        @JsonProperty("request_id")
        public String requestId;
        public boolean healthy;

        @JsonProperty("is_leader")
        public boolean isLeader;

        @JsonProperty("internal_tables_ready")
        public boolean internalTablesReady;

        @JsonProperty("contextbase_count")
        public int contextbaseCount;

        @JsonProperty("collection_count")
        public int collectionCount;

        @JsonProperty("workspace_count")
        public int workspaceCount;

        @JsonProperty("retrieval_profile_count")
        public int retrievalProfileCount;

        @JsonProperty("entity_count")
        public long entityCount;

        @JsonProperty("version_count")
        public long versionCount;

        @JsonProperty("commit_count")
        public long commitCount;

        @JsonProperty("task_count")
        public long taskCount;
    }
}
