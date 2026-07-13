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

import com.starrocks.authorization.AccessDeniedException;
import com.starrocks.authorization.PrivilegeType;
import com.starrocks.context.ContextMgr;
import com.starrocks.context.ContextVisibility;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.Authorizer;

import java.util.Collection;
import java.util.List;

/**
 * REST-side authorization gate for {@code com.starrocks.http.rest.context}. Canonical predicates
 * (admin override, per-base GRANT, owner match, list filters) live in
 * {@link ContextVisibility}; this class is the REST-shaped adapter that the 30+ action handlers
 * call into. The SQL surface ({@code ShowExecutor}, {@code AuthorizerStmtVisitor}) calls
 * {@link ContextVisibility} directly so both transports apply the exact same predicate.
 *
 * <ul>
 *   <li>{@link #checkSystem} — module-create gate (CREATE_CONTEXTBASE). Used by REST endpoints
 *       that create top-level objects (contextbase, retrieval profile).</li>
 *   <li>{@link #checkOnContextBase} — per-base operation gate. Delegates to
 *       {@link ContextVisibility#checkOnContextBase}.</li>
 *   <li>{@link #checkAdmin} — hard admin override, no per-base fallback. Used by
 *       cluster-wide observability endpoints (health, cluster-wide stats) whose payload
 *       aggregates across every base.</li>
 * </ul>
 *
 * <p>All helpers throw {@link AccessDeniedException}; the REST error envelope at
 * {@link com.starrocks.http.rest.RestBaseAction#handleRequest} converts that to an HTTP 401 with
 * a {@code WWW-Authenticate} header.
 */
final class ContextRestAuth {

    private ContextRestAuth() {
    }

    /**
     * REST-side action enum kept for backward compatibility with the 30+ existing action handlers
     * that import {@code ContextRestAuth.BaseAction}. Each value maps 1:1 to
     * {@link ContextVisibility.BaseAction}.
     */
    enum BaseAction {
        USAGE(ContextVisibility.BaseAction.USAGE),
        ALTER(ContextVisibility.BaseAction.ALTER),
        DROP(ContextVisibility.BaseAction.DROP);

        private final ContextVisibility.BaseAction inner;

        BaseAction(ContextVisibility.BaseAction inner) {
            this.inner = inner;
        }

        ContextVisibility.BaseAction inner() {
            return inner;
        }
    }

    /**
     * Module-create gate. Required for endpoints that create top-level objects (contextbase,
     * retrieval profile). Stays bound to SYSTEM-level {@code CREATE_CONTEXTBASE}: that privilege
     * exists specifically so a tenant can be granted "you may create your own bases" without
     * inheriting admin-style visibility into other tenants' data.
     */
    static void checkSystem(ConnectContext ctx) throws AccessDeniedException {
        Authorizer.checkSystemAction(ctx, PrivilegeType.CREATE_CONTEXTBASE);
    }

    /**
     * Cluster-wide admin gate. Used by endpoints whose payload aggregates across every base
     * ({@code /api/context/health}, cluster-wide {@code /api/context/stats}). No per-base
     * fallback — only OPERATE / SECURITY pass.
     */
    static void checkAdmin(ConnectContext ctx) throws AccessDeniedException {
        ContextVisibility.requireAdmin(ctx);
    }

    /**
     * The per-request correlation id surfaced to clients. Reuses {@link ConnectContext#getQueryId()}
     * which {@code RestBaseAction.execute()} populates with a fresh UUID for every request, so
     * audit log entries and the {@code request_id} field on REST responses share the same value.
     * Null-safe: callers may invoke this from error paths where the context has not been set.
     */
    static String currentRequestId() {
        ConnectContext ctx = ConnectContext.get();
        if (ctx == null || ctx.getQueryId() == null) {
            return null;
        }
        return ctx.getQueryId().toString();
    }

    /**
     * Returns true when the caller can see every contextbase regardless of ownership / per-base
     * grants. See {@link ContextVisibility#hasFullVisibility} for the exact criteria.
     */
    static boolean hasFullVisibility(ConnectContext ctx) {
        return ContextVisibility.hasFullVisibility(ctx);
    }

    /**
     * Returns true when {@code ctx} can see at least one contextbase under USAGE.
     */
    static boolean hasAnyContextBaseUsage(ConnectContext ctx) {
        return ContextVisibility.hasAnyContextBaseUsage(ctx);
    }

    /**
     * Returns true when {@code ctx} satisfies {@link #checkOnContextBase} for USAGE on the named
     * base. Swallows the denial as the negative answer.
     */
    static boolean canSeeContextBase(ConnectContext ctx, String contextBaseName) {
        return ContextVisibility.canSeeContextBase(ctx, contextBaseName);
    }

    /**
     * Apply {@link #canSeeContextBase} to {@code metas}, returning only those the caller can read.
     */
    static List<ContextMgr.ContextBaseMeta> filterVisibleBases(ConnectContext ctx,
                                                               Collection<ContextMgr.ContextBaseMeta> metas) {
        return ContextVisibility.filterVisibleBases(ctx, metas);
    }

    /**
     * Filter collections to those whose owning contextbase the caller can read.
     */
    static List<ContextMgr.CollectionMeta> filterVisibleCollections(ConnectContext ctx,
                                                                    Collection<ContextMgr.CollectionMeta> metas) {
        return ContextVisibility.filterVisibleCollections(ctx, metas);
    }

    /**
     * Filter workspaces to those whose contextbase the caller can read.
     */
    static List<ContextMgr.WorkspaceMeta> filterVisibleWorkspaces(ConnectContext ctx,
                                                                  Collection<ContextMgr.WorkspaceMeta> metas) {
        return ContextVisibility.filterVisibleWorkspaces(ctx, metas);
    }

    /**
     * Authorize an operation on a named contextbase. Three paths in order: admin override
     * (OPERATE / SECURITY), per-base GRANT, owner match. See
     * {@link ContextVisibility#checkOnContextBase} for the exact semantics.
     */
    static void checkOnContextBase(ConnectContext ctx, String contextBaseName, BaseAction action)
            throws AccessDeniedException {
        ContextVisibility.checkOnContextBase(ctx, contextBaseName, action.inner());
    }
}
